package build.crab.prolly.store.spanner

import build.crab.prolly.remote.NodeMutation
import build.crab.prolly.remote.OptionalBytes
import build.crab.prolly.remote.RootCondition
import build.crab.prolly.remote.RootWrite
import build.crab.prolly.remote.StoreException
import build.crab.prolly.storetest.StoreConformance
import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.InstanceConfigId
import com.google.cloud.spanner.InstanceId
import com.google.cloud.spanner.InstanceInfo
import com.google.cloud.spanner.SpannerExceptionFactory
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.SpannerOptions
import java.util.Base64
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable

class SpannerStoreTest {
    @Test fun `Spanner matches Rust DDL and raw byte layout while satisfying conformance`() = runBlocking {
        assertEquals(listOf(
            "CREATE TABLE ProllyNodes (\n  Cid BYTES(32) NOT NULL,\n  Node BYTES(MAX) NOT NULL\n) PRIMARY KEY (Cid)",
            "CREATE TABLE ProllyHints (\n  Namespace BYTES(MAX) NOT NULL,\n  HintKey BYTES(MAX) NOT NULL,\n  Value BYTES(MAX) NOT NULL\n) PRIMARY KEY (Namespace, HintKey)",
            "CREATE TABLE ProllyRoots (\n  Name BYTES(MAX) NOT NULL,\n  Manifest BYTES(MAX) NOT NULL\n) PRIMARY KEY (Name)",
        ), SPANNER_DDL)
        val client = MemoryClient(); val store = SpannerStore.fromClient(client); StoreConformance.run { store }
        val cid = specialBytes(32); store.putNode(cid, "value".bytes()); assertTrue(client.node(cid)!!.contentEquals("value".bytes())); val mutation = client.lastMutation as SpannerMutation.UpsertNode; assertTrue(mutation.key.contentEquals(cid)); assertTrue(mutation.value.contentEquals("value".bytes()))
        val descriptor = store.descriptor(); assertFalse(descriptor.capabilities.nativeBatchReads); assertTrue(descriptor.capabilities.atomicBatchWrites); assertEquals(null, descriptor.limits.maxTransactionOperations)
    }

    @Test fun `serializable CAS has one winner and strict conflicts roll back`() = runBlocking {
        val client = MemoryClient(); val store = SpannerStore.fromClient(client)
        val results = coroutineScope { (0 until 32).map { index -> async { store.compareAndSwapRootManifest("race".bytes(), OptionalBytes.missing(), OptionalBytes.present("winner-$index".bytes())) } }.awaitAll() }
        assertEquals(1, results.count { it.applied })
        val conflict = store.commitTransaction(listOf(NodeMutation.Upsert("rollback-node".bytes(), "must-not-write".bytes())), listOf(RootCondition("race".bytes(), OptionalBytes.missing())), listOf(RootWrite.Put("rollback-root".bytes(), "must-not-publish".bytes())))
        assertFalse(conflict.applied); assertFalse(store.getNode("rollback-node".bytes()).present); assertFalse(store.getRootManifest("rollback-root".bytes()).present)
    }

    @Test fun `retryable provider failures are redacted`() = runBlocking {
        val secret = "spanner-service-account-secret"; val client = MemoryClient(); client.failure = SpannerExceptionFactory.newSpannerException(ErrorCode.UNAVAILABLE, secret); val failure = runCatching { SpannerStore.fromClient(client).putNode("cid".bytes(), "value".bytes()) }.exceptionOrNull()
        assertTrue(failure is StoreException); assertEquals("unavailable", (failure as StoreException).error.code); assertTrue(failure.error.retryable); assertFalse(failure.message.orEmpty().contains(secret))
    }

    @Test fun `coroutine cancellation and close preserve client ownership`() = runBlocking {
        var closed = false
        val client = object : SpannerItemClient, AutoCloseable {
            override fun close() { closed = true }
            override suspend fun getNode(key: ByteArray) = OptionalBytes.missing()
            override suspend fun getHint(namespace: ByteArray, key: ByteArray) = OptionalBytes.missing()
            override suspend fun getRoot(name: ByteArray) = OptionalBytes.missing()
            override suspend fun listNodeCids() = emptyList<ByteArray>()
            override suspend fun listRoots() = emptyList<SpannerRootRecord>()
            override suspend fun apply(mutations: List<SpannerMutation>): Unit = suspendCancellableCoroutine { }
            override suspend fun <T> readWrite(callback: (SpannerTransaction) -> T): T = error("unused")
        }
        coroutineScope { val write = async { SpannerStore.fromClient(client).putNode("cid".bytes(), "value".bytes()) }; kotlinx.coroutines.yield(); write.cancelAndJoin() }
        val store = SpannerStore.fromClient(client); store.close(); assertFalse(closed)
    }

    @Test @EnabledIfEnvironmentVariable(named = "SPANNER_EMULATOR_HOST", matches = ".+")
    fun `Spanner emulator satisfies conformance`() = runBlocking {
        val project = "prolly-test"; val instance = "prolly-test"; val database = "prolly_kotlin_${System.nanoTime()}"; val host = System.getenv("SPANNER_EMULATOR_HOST")
        val spanner = SpannerOptions.newBuilder().setProjectId(project).setEmulatorHost(host).build().service
        try {
            try { spanner.instanceAdminClient.getInstance(instance) } catch (error: SpannerException) { if (error.errorCode != ErrorCode.NOT_FOUND) throw error; spanner.instanceAdminClient.createInstance(InstanceInfo.newBuilder(InstanceId.of(project, instance)).setInstanceConfigId(InstanceConfigId.of(project, "emulator-config")).setDisplayName(instance).setNodeCount(1).build()).get(30, TimeUnit.SECONDS) }
            spanner.databaseAdminClient.createDatabase(instance, database, SPANNER_DDL).get(30, TimeUnit.SECONDS)
            val store = SpannerStore(spanner.getDatabaseClient(DatabaseId.of(project, instance, database)))
            try { StoreConformance.run { store } } finally { store.close(); spanner.databaseAdminClient.dropDatabase(instance, database) }
        } finally { spanner.close() }
    }
}

private data class State(val nodes: MutableMap<String, ByteArray>, val hints: MutableMap<String, ByteArray>, val roots: MutableMap<String, ByteArray>)
private class MemoryClient : SpannerItemClient {
    private var state = State(linkedMapOf(), linkedMapOf(), linkedMapOf()); var failure: Throwable? = null; var lastMutation: SpannerMutation? = null
    override suspend fun getNode(key: ByteArray) = synchronized(this) { optional(state.nodes[id(key)]) }
    override suspend fun getHint(namespace: ByteArray, key: ByteArray) = synchronized(this) { optional(state.hints["${id(namespace)}:${id(key)}"]) }
    override suspend fun getRoot(name: ByteArray) = synchronized(this) { optional(state.roots[id(name)]) }
    override suspend fun listNodeCids() = synchronized(this) { state.nodes.keys.map(::fromId) }
    override suspend fun listRoots() = synchronized(this) { state.roots.map { SpannerRootRecord(fromId(it.key), it.value.copyOf()) } }
    override suspend fun apply(mutations: List<SpannerMutation>) = synchronized(this) { failure?.let { failure = null; throw it }; val next = cloneState(state); mutations.forEach { apply(next, it) }; state = next; lastMutation = mutations.lastOrNull()?.let(::cloneMutation) }
    override suspend fun <T> readWrite(callback: (SpannerTransaction) -> T): T = synchronized(this) { val next = cloneState(state); val buffered = mutableListOf<SpannerMutation>(); val result = callback(object : SpannerTransaction { override fun getRoot(name: ByteArray) = optional(next.roots[id(name)]); override fun buffer(mutations: List<SpannerMutation>) { buffered += mutations.map(::cloneMutation) } }); buffered.forEach { apply(next, it) }; state = next; result }
    fun node(key: ByteArray) = state.nodes[id(key)]?.copyOf()
}

private fun cloneState(value: State) = State(value.nodes.mapValuesTo(linkedMapOf()) { it.value.copyOf() }, value.hints.mapValuesTo(linkedMapOf()) { it.value.copyOf() }, value.roots.mapValuesTo(linkedMapOf()) { it.value.copyOf() })
private fun apply(state: State, value: SpannerMutation) { when (value) { is SpannerMutation.UpsertNode -> state.nodes[id(value.key)] = value.value.copyOf(); is SpannerMutation.DeleteNode -> state.nodes.remove(id(value.key)); is SpannerMutation.UpsertHint -> state.hints["${id(value.namespace)}:${id(value.key)}"] = value.value.copyOf(); is SpannerMutation.UpsertRoot -> state.roots[id(value.key)] = value.value.copyOf(); is SpannerMutation.DeleteRoot -> state.roots.remove(id(value.key)) } }
private fun cloneMutation(value: SpannerMutation): SpannerMutation = when (value) { is SpannerMutation.UpsertNode -> SpannerMutation.UpsertNode(value.key.copyOf(), value.value.copyOf()); is SpannerMutation.DeleteNode -> SpannerMutation.DeleteNode(value.key.copyOf()); is SpannerMutation.UpsertHint -> SpannerMutation.UpsertHint(value.namespace.copyOf(), value.key.copyOf(), value.value.copyOf()); is SpannerMutation.UpsertRoot -> SpannerMutation.UpsertRoot(value.key.copyOf(), value.value.copyOf()); is SpannerMutation.DeleteRoot -> SpannerMutation.DeleteRoot(value.key.copyOf()) }
private fun optional(value: ByteArray?) = if (value == null) OptionalBytes.missing() else OptionalBytes.present(value)
private fun id(value: ByteArray) = Base64.getEncoder().encodeToString(value)
private fun fromId(value: String) = Base64.getDecoder().decode(value)
private fun String.bytes() = encodeToByteArray()
private fun specialBytes(length: Int) = ByteArray(length) { byteArrayOf(0, 0x7f, 0x80.toByte(), 0xff.toByte())[it % 4] }
