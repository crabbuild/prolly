package build.crab.prolly.store.dynamodb

import build.crab.prolly.ProllyNative
import build.crab.prolly.remote.NodeMutation
import build.crab.prolly.remote.OptionalBytes
import build.crab.prolly.remote.RemoteProlly
import build.crab.prolly.remote.RootCondition
import build.crab.prolly.remote.RootWrite
import build.crab.prolly.remote.StoreException
import build.crab.prolly.storetest.StoreConformance
import java.lang.reflect.Proxy
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.CompletableFuture
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest

class DynamoDbStoreTest {
    private val endpoint = System.getenv("PROLLY_DYNAMODB_ENDPOINT")
    @BeforeEach fun requireDynamoDb() = assumeTrue(!endpoint.isNullOrBlank(), "PROLLY_DYNAMODB_ENDPOINT is not set")

    @Test fun `DynamoDB satisfies conformance and exact binary layout`() = withStore { client, store, table, prefix ->
        StoreConformance.run { store }
        val description = client.describeTable(DescribeTableRequest.builder().tableName(table).build()).get().table()
        assertEquals("pk", description.keySchema().single().attributeName()); assertEquals("B", description.attributeDefinitions().single { it.attributeName() == "pk" }.attributeTypeAsString())
        val cid = specialBytes(32); val root = specialBytes(9); val namespace = specialBytes(7); val hintKey = specialBytes(5)
        store.putNode(cid, "node".bytes()); store.putRootManifest(root, "manifest".bytes()); store.putHint(namespace, hintKey, "hint".bytes())
        assertEquals("node", raw(client, table, prefix + "node:".bytes() + cid).decodeToString())
        assertEquals("manifest", raw(client, table, prefix + "root:".bytes() + root).decodeToString())
        assertEquals("hint", raw(client, table, prefix + "hint:".bytes() + ByteBuffer.allocate(8).putLong(namespace.size.toLong()).array() + namespace + hintKey).decodeToString())
        assertTrue(store.listNodeCids().any { it.contentEquals(cid) })
    }

    @Test fun `conditional CAS has one winner and strict conflicts roll back`() = withStore { _, store, _, _ ->
        val results = coroutineScope { (0 until 32).map { index -> async { store.compareAndSwapRootManifest("race".bytes(), OptionalBytes.missing(), OptionalBytes.present("winner-$index".bytes())) } }.awaitAll() }
        assertEquals(1, results.count { it.applied })
        val conflict = store.commitTransaction(listOf(NodeMutation.Upsert("rollback-node".bytes(), "must-not-write".bytes())), listOf(RootCondition("race".bytes(), OptionalBytes.missing())), listOf(RootWrite.Put("rollback-root".bytes(), "must-not-publish".bytes())))
        assertFalse(conflict.applied); assertFalse(store.getNode("rollback-node".bytes()).present); assertFalse(store.getRootManifest("rollback-root".bytes()).present)
    }

    @Test fun `DynamoDB reads Rust trees and Rust reads Kotlin trees`() = withStore { _, store, table, prefix ->
        ProllyNative.useLocalDebugLibrary(); runRustInterop("write", table, prefix, "rust-main", "rust-key", "rust-value")
        RemoteProlly.open(store).use { engine -> val rustTree = checkNotNull(engine.loadNamedRoot("rust-main".bytes())); assertEquals("rust-value", checkNotNull(engine.get(rustTree, "rust-key".bytes())).decodeToString()); val tree = engine.put(engine.create(), "kotlin-key".bytes(), "kotlin-value".bytes()); engine.publishNamedRoot("kotlin-main".bytes(), tree) }
        runRustInterop("verify", table, prefix, "kotlin-main", "kotlin-key", "kotlin-value")
    }

    @Test fun `close preserves the injected async client`() = withStore { client, store, _, _ -> store.close(); assertTrue(client.listTables(ListTablesRequest.builder().build()).get().sdkHttpResponse().isSuccessful) }

    @Test fun `chunks retries restores order and preflights transaction limits`() = runBlocking {
        val getSizes = mutableListOf<Int>(); val writeSizes = mutableListOf<Int>(); var first = true; var calls = 0
        val client = proxyClient { method, argument -> calls++
            when (method) {
                "batchGetItem" -> { val request = argument as BatchGetItemRequest; val keys = request.requestItems()["table"]!!.keys(); getSizes += keys.size
                    if (first) { first = false; val deferred = keys.last(); BatchGetItemResponse.builder().responses(mapOf("table" to keys.dropLast(1).map(::valueItem))).unprocessedKeys(mapOf("table" to KeysAndAttributes.builder().keys(deferred).build())).build() }
                    else BatchGetItemResponse.builder().responses(mapOf("table" to keys.map(::valueItem))).build()
                }
                "batchWriteItem" -> { val request = argument as BatchWriteItemRequest; writeSizes += request.requestItems()["table"]!!.size; BatchWriteItemResponse.builder().build() }
                else -> error("unexpected SDK call $method")
            }
        }
        val store = DynamoDbStore(client, DynamoDbStoreOptions("table", "p:".bytes())); val cids = (0..100).map { byteArrayOf(it.toByte()) }; val values = store.batchGetNodesOrdered(cids + cids.first())
        assertEquals(listOf(100, 1, 1), getSizes); assertEquals(102, values.size); assertTrue(values.first().value.contentEquals(values.last().value))
        store.batchNodes((0..25).map { NodeMutation.Upsert(byteArrayOf(it.toByte()), "v".bytes()) }); assertEquals(listOf(25, 1), writeSizes)
        calls = 0; val error = runCatching { store.commitTransaction((0..100).map { NodeMutation.Upsert(byteArrayOf(it.toByte()), "v".bytes()) }, emptyList(), emptyList()) }.exceptionOrNull()
        assertTrue(error is StoreException); assertEquals("resource_exhausted", (error as StoreException).error.code); assertEquals(0, calls)
    }

    @Test fun `coroutine cancellation cancels an in-flight SDK future`() = runBlocking {
        val future = CompletableFuture<Any>(); val client = proxyClient { _, _ -> future }; val store = DynamoDbStore(client, DynamoDbStoreOptions("table"))
        coroutineScope { val write = async { store.putNode("cid".bytes(), "value".bytes()) }; kotlinx.coroutines.yield(); write.cancelAndJoin(); assertTrue(future.isCancelled) }; store.close()
    }

    private fun withStore(block: suspend (DynamoDbAsyncClient, DynamoDbStore, String, ByteArray) -> Unit) = runBlocking {
        val client = client(); val table = "prolly_kotlin_${UUID.randomUUID().toString().replace("-", "")}"; val prefix = "prolly:test:kotlin:".bytes(); val store = DynamoDbStore(client, DynamoDbStoreOptions(table, prefix)); store.initializeTable()
        try { block(client, store, table, prefix) } finally { store.close(); runCatching { client.deleteTable(DeleteTableRequest.builder().tableName(table).build()).get() }; client.close() }
    }
    private fun client() = DynamoDbAsyncClient.builder().endpointOverride(URI(checkNotNull(endpoint))).region(Region.US_WEST_2).credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local"))).build()
    private fun runRustInterop(operation: String, table: String, prefix: ByteArray, root: String, key: String, value: String) { val repository = generateSequence(java.nio.file.Path.of(System.getProperty("user.dir"))) { it.parent }.first { Files.exists(it.resolve("stores/prolly-store-dynamodb/Cargo.toml")) }; val process = ProcessBuilder("cargo", "run", "--quiet", "--manifest-path", "stores/prolly-store-dynamodb/Cargo.toml", "--example", "language_interop", "--", operation, endpoint, table, prefix.hex(), root, key, value).directory(repository.toFile()).redirectErrorStream(true).start(); val output = process.inputStream.bufferedReader().use { it.readText() }; check(process.waitFor() == 0) { "Rust DynamoDB interop failed: $output" } }
}

private fun proxyClient(handler: (String, Any?) -> Any): DynamoDbAsyncClient = Proxy.newProxyInstance(DynamoDbAsyncClient::class.java.classLoader, arrayOf(DynamoDbAsyncClient::class.java)) { _, method, args -> when (method.name) { "serviceName" -> "DynamoDb"; "close" -> Unit; else -> handler(method.name, args?.firstOrNull()).let { if (it is CompletableFuture<*>) it else CompletableFuture.completedFuture(it) } } } as DynamoDbAsyncClient
private fun valueItem(key: Map<String, AttributeValue>): Map<String, AttributeValue> { val bytes = key["pk"]!!.b().asByteArray(); return mapOf("pk" to key["pk"]!!, "value" to AttributeValue.builder().b(SdkBytes.fromUtf8String("v${bytes.last().toUByte()}" )).build()) }
private fun raw(client: DynamoDbAsyncClient, table: String, key: ByteArray): ByteArray = client.getItem(GetItemRequest.builder().tableName(table).key(mapOf("pk" to AttributeValue.builder().b(SdkBytes.fromByteArray(key)).build())).consistentRead(true).build()).get().item()["value"]!!.b().asByteArray()
private fun String.bytes() = encodeToByteArray()
private fun ByteArray.hex() = joinToString("") { "%02x".format(it.toInt() and 0xff) }
private fun specialBytes(length: Int) = ByteArray(length) { byteArrayOf(0, 0x7f, 0x80.toByte(), 0xff.toByte())[it % 4] }
