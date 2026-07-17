package build.crab.prolly.store.redis

import build.crab.prolly.ProllyNative
import build.crab.prolly.remote.NodeMutation
import build.crab.prolly.remote.OptionalBytes
import build.crab.prolly.remote.RemoteProlly
import build.crab.prolly.remote.RootCondition
import build.crab.prolly.remote.RootWrite
import build.crab.prolly.storetest.StoreConformance
import io.lettuce.core.RedisClient
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.api.async.RedisAsyncCommands
import java.nio.file.Files
import java.util.UUID
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RedisStoreTest {
    private val redisUrl = System.getenv("PROLLY_REDIS_URL")

    @BeforeEach fun requireRedis() = assumeTrue(!redisUrl.isNullOrBlank(), "PROLLY_REDIS_URL is not set")

    @Test fun `Redis satisfies conformance and exact binary key families`() = withStore { commands, store, prefix ->
        StoreConformance.run { store }
        val cid = specialBytes(32); val root = specialBytes(9); val namespace = specialBytes(7); val hintKey = specialBytes(5)
        store.putNode(cid, "node".bytes()); store.putRootManifest(root, "manifest".bytes()); store.putHint(namespace, hintKey, "hint".bytes())
        assertEquals("node", commands.get(prefix + "node:".bytes() + cid).get().decodeToString())
        assertEquals("manifest", commands.get(prefix + "root:".bytes() + root).get().decodeToString())
        val length = java.nio.ByteBuffer.allocate(8).putLong(namespace.size.toLong()).array()
        assertEquals("hint", commands.get(prefix + "hint:".bytes() + length + namespace + hintKey).get().decodeToString())
        assertTrue(store.listNodeCids().any { it.contentEquals(cid) })
        store.putRootManifest(byteArrayOf(0xff.toByte()), "last".bytes()); store.putRootManifest(byteArrayOf(0), "first".bytes())
        val names = store.listRootManifests().map { it.name }
        assertEquals(names.sortedWith(BYTE_ARRAY_COMPARATOR).map(ByteArray::hex), names.map(ByteArray::hex))
    }

    @Test fun `Lua CAS has one winner and strict conflicts roll back`() = withStore { _, store, _ ->
        val results = coroutineScope { (0 until 32).map { index -> async { store.compareAndSwapRootManifest("race".bytes(), OptionalBytes.missing(), OptionalBytes.present("winner-$index".bytes())) } }.awaitAll() }
        assertEquals(1, results.count { it.applied })
        val conflict = store.commitTransaction(
            listOf(NodeMutation.Upsert("rollback-node".bytes(), "must-not-write".bytes())),
            listOf(RootCondition("race".bytes(), OptionalBytes.missing())),
            listOf(RootWrite.Put("rollback-root".bytes(), "must-not-publish".bytes())),
        )
        assertFalse(conflict.applied); assertFalse(store.getNode("rollback-node".bytes()).present); assertFalse(store.getRootManifest("rollback-root".bytes()).present)
    }

    @Test fun `Redis reads Rust trees and Rust reads Kotlin trees`() = withStore { _, store, prefix ->
        ProllyNative.useLocalDebugLibrary()
        runRustInterop("write", prefix, "rust-main", "rust-key", "rust-value")
        RemoteProlly.open(store).use { engine ->
            val rustTree = checkNotNull(engine.loadNamedRoot("rust-main".bytes()))
            assertEquals("rust-value", checkNotNull(engine.get(rustTree, "rust-key".bytes())).decodeToString())
            val tree = engine.put(engine.create(), "kotlin-key".bytes(), "kotlin-value".bytes())
            engine.publishNamedRoot("kotlin-main".bytes(), tree)
        }
        runRustInterop("verify", prefix, "kotlin-main", "kotlin-key", "kotlin-value")
    }

    @Test fun `close keeps the injected connection open`() = withStore { commands, store, _ ->
        store.close(); assertEquals("PONG", commands.ping().get())
    }

    @Test fun `cancellation promptly terminates an in-flight command`() = withStore { commands, store, _ ->
        commands.clientPause(1000).get()
        coroutineScope {
            val started = System.nanoTime(); val write = async { store.putNode("cancelled".bytes(), "value".bytes()) }
            delay(50); write.cancelAndJoin()
            assertTrue((System.nanoTime() - started) / 1_000_000 < 500, "Redis command did not cancel promptly")
        }
        store.close(); assertEquals("PONG", commands.ping().get())
    }

    private fun withStore(block: suspend (RedisAsyncCommands<ByteArray, ByteArray>, RedisStore, ByteArray) -> Unit) = runBlocking {
        val client = RedisClient.create(checkNotNull(redisUrl)); val connection = client.connect(ByteArrayCodec())
        val commands = connection.async(); val prefix = "prolly:test:kotlin:${UUID.randomUUID()}:".bytes(); val store = RedisStore(commands, RedisStoreOptions(keyPrefix = prefix))
        try { store.clearNamespace(); block(commands, store, prefix) }
        finally { runCatching { runBlocking { store.clearNamespace() } }; store.close(); connection.close(); client.shutdown() }
    }

    private fun runRustInterop(operation: String, prefix: ByteArray, root: String, key: String, value: String) {
        val repository = generateSequence(java.nio.file.Path.of(System.getProperty("user.dir"))) { it.parent }.first { Files.exists(it.resolve("stores/prolly-store-redis/Cargo.toml")) }
        val process = ProcessBuilder("cargo", "run", "--quiet", "--manifest-path", "stores/prolly-store-redis/Cargo.toml", "--example", "language_interop", "--", operation, redisUrl, prefix.hex(), root, key, value).directory(repository.toFile()).redirectErrorStream(true).start()
        val output = process.inputStream.bufferedReader().use { it.readText() }; check(process.waitFor() == 0) { "Rust Redis interop failed: $output" }
    }
}

private fun String.bytes(): ByteArray = encodeToByteArray()
private fun ByteArray.hex(): String = joinToString("") { "%02x".format(it.toInt() and 0xff) }
private fun specialBytes(length: Int) = ByteArray(length) { byteArrayOf(0, 0x7f, 0x80.toByte(), 0xff.toByte())[it % 4] }
private val BYTE_ARRAY_COMPARATOR = Comparator<ByteArray> { left, right ->
    val common = minOf(left.size, right.size); for (index in 0 until common) { val compared = left[index].toUByte().compareTo(right[index].toUByte()); if (compared != 0) return@Comparator compared }; left.size.compareTo(right.size)
}
