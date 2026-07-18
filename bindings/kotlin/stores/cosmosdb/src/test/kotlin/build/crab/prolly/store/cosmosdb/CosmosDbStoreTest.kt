package build.crab.prolly.store.cosmosdb

import build.crab.prolly.remote.NodeMutation
import build.crab.prolly.remote.OptionalBytes
import build.crab.prolly.remote.RootCondition
import build.crab.prolly.remote.RootWrite
import build.crab.prolly.remote.StoreException
import build.crab.prolly.storetest.StoreConformance
import java.nio.ByteBuffer
import java.util.Base64
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

class CosmosDbStoreTest {
    @Test fun `Cosmos DB satisfies conformance exact documents ETags and one partition`() = runBlocking {
        val client = MemoryClient(); val store = CosmosDbStore.fromClient(client, CosmosDbStoreOptions("prolly:test:".bytes(), "tenant-a"))
        store.validateContainer(); StoreConformance.run { store }
        val cid = specialBytes(32); store.putNode(cid, "value".bytes()); val key = "prolly:test:node:".bytes() + cid; val raw = client.document("tenant-a", "k${key.hex()}")
        assertEquals(CosmosDocument("k${key.hex()}", "tenant-a", "node", key.hex(), Base64.getEncoder().encodeToString("value".bytes())), raw)
        assertEquals(1, client.validations); assertTrue(client.matchedEtags.isNotEmpty()); assertTrue(client.batchPartitions.all { it == "tenant-a" }); assertTrue(store.listNodeCids().any { it.contentEquals(cid) })
    }

    @Test fun `ETag CAS has one winner and failed batches roll back`() = runBlocking {
        val client = MemoryClient(); val store = CosmosDbStore.fromClient(client, CosmosDbStoreOptions(partitionKey = "tenant"))
        val results = coroutineScope { (0 until 32).map { index -> async { store.compareAndSwapRootManifest("race".bytes(), OptionalBytes.missing(), OptionalBytes.present("winner-$index".bytes())) } }.awaitAll() }
        assertEquals(1, results.count { it.applied })
        val conflict = store.commitTransaction(listOf(NodeMutation.Upsert("rollback-node".bytes(), "must-not-write".bytes())), listOf(RootCondition("race".bytes(), OptionalBytes.missing())), listOf(RootWrite.Put("rollback-root".bytes(), "must-not-publish".bytes())))
        assertFalse(conflict.applied); assertFalse(store.getNode("rollback-node".bytes()).present); assertFalse(store.getRootManifest("rollback-root".bytes()).present)
    }

    @Test fun `preflights 101 writes and redacts provider errors`() = runBlocking {
        var calls = 0; val secret = "cosmos-account-secret"
        val client = object : CosmosItemClient {
            override suspend fun read(partition: String, id: String): CosmosReadResult { calls++; throw CosmosStatusException(503) }
            override suspend fun create(partition: String, document: CosmosDocument) { calls++ }
            override suspend fun upsert(partition: String, document: CosmosDocument) { calls++; throw RuntimeException(secret, CosmosStatusException(503)) }
            override suspend fun replace(partition: String, id: String, document: CosmosDocument, etag: String) { calls++ }
            override suspend fun delete(partition: String, id: String, etag: String) { calls++ }
            override suspend fun queryFamily(partition: String, family: String): List<CosmosDocument> { calls++; return emptyList() }
            override suspend fun executeBatch(partition: String, operations: List<CosmosBatchOperation>): CosmosBatchResponse { calls++; return CosmosBatchResponse(true, emptyList()) }
        }
        val store = CosmosDbStore.fromClient(client); val oversized = runCatching { store.commitTransaction((0..100).map { NodeMutation.Upsert(byteArrayOf(it.toByte()), "v".bytes()) }, emptyList(), emptyList()) }.exceptionOrNull()
        assertTrue(oversized is StoreException); assertEquals("resource_exhausted", (oversized as StoreException).error.code); assertEquals(0, calls)
        val failure = runCatching { store.putNode("cid".bytes(), "value".bytes()) }.exceptionOrNull(); assertTrue(failure is StoreException); assertEquals("unavailable", (failure as StoreException).error.code); assertFalse(failure.message.orEmpty().contains(secret))
    }

    @Test fun `coroutine cancellation and close preserve client ownership`() = runBlocking {
        var closed = false
        val client = object : CosmosItemClient, AutoCloseable {
            override fun close() { closed = true }
            override suspend fun read(partition: String, id: String): CosmosReadResult = throw CosmosStatusException(404)
            override suspend fun create(partition: String, document: CosmosDocument) {}
            override suspend fun upsert(partition: String, document: CosmosDocument): Unit = suspendCancellableCoroutine { }
            override suspend fun replace(partition: String, id: String, document: CosmosDocument, etag: String) {}
            override suspend fun delete(partition: String, id: String, etag: String) {}
            override suspend fun queryFamily(partition: String, family: String) = emptyList<CosmosDocument>()
            override suspend fun executeBatch(partition: String, operations: List<CosmosBatchOperation>) = CosmosBatchResponse(true, emptyList())
        }
        coroutineScope { val write = async { CosmosDbStore.fromClient(client).putNode("cid".bytes(), "value".bytes()) }; kotlinx.coroutines.yield(); write.cancelAndJoin() }
        val store = CosmosDbStore.fromClient(client); store.close(); assertFalse(closed)
    }
}

private data class MemoryRecord(val document: CosmosDocument, val etag: String)
private class MemoryClient : CosmosItemClient {
    private val items = LinkedHashMap<String, MemoryRecord>(); private var nextEtag = 1; var validations = 0; val matchedEtags = mutableListOf<String>(); val batchPartitions = mutableListOf<String>()
    override suspend fun validatePartitionKey() { validations++ }
    override suspend fun read(partition: String, id: String): CosmosReadResult = synchronized(this) { items["$partition\u0000$id"]?.let { CosmosReadResult(it.document.copy(), it.etag) } ?: throw CosmosStatusException(404) }
    override suspend fun create(partition: String, document: CosmosDocument) = synchronized(this) { val key = "$partition\u0000${document.id}"; if (items.containsKey(key)) throw CosmosStatusException(409); items[key] = record(document) }
    override suspend fun upsert(partition: String, document: CosmosDocument) = synchronized(this) { items["$partition\u0000${document.id}"] = record(document) }
    override suspend fun replace(partition: String, id: String, document: CosmosDocument, etag: String) = synchronized(this) { val key = "$partition\u0000$id"; val current = items[key] ?: throw CosmosStatusException(404); matchedEtags += etag; if (current.etag != etag) throw CosmosStatusException(412); items[key] = record(document) }
    override suspend fun delete(partition: String, id: String, etag: String) = synchronized(this) { val key = "$partition\u0000$id"; val current = items[key] ?: throw CosmosStatusException(404); if (etag.isNotEmpty()) { matchedEtags += etag; if (current.etag != etag) throw CosmosStatusException(412) }; items.remove(key); Unit }
    override suspend fun queryFamily(partition: String, family: String): List<CosmosDocument> = synchronized(this) { items.filter { it.key.startsWith("$partition\u0000") && it.value.document.family == family }.values.map { it.document.copy() } }
    override suspend fun executeBatch(partition: String, operations: List<CosmosBatchOperation>): CosmosBatchResponse = synchronized(this) { batchPartitions += partition; val working = LinkedHashMap(items.mapValues { MemoryRecord(it.value.document.copy(), it.value.etag) }); val results = mutableListOf<CosmosBatchResult>(); for (operation in operations) { val code = apply(working, partition, operation); results += CosmosBatchResult(code); if (code !in 200..299) { while (results.size < operations.size) results += CosmosBatchResult(424); return@synchronized CosmosBatchResponse(false, results) } }; items.clear(); items.putAll(working); CosmosBatchResponse(true, results) }
    fun document(partition: String, id: String) = items["$partition\u0000$id"]!!.document.copy()
    private fun record(document: CosmosDocument) = MemoryRecord(document.copy(), (nextEtag++).toString())
    private fun apply(target: MutableMap<String, MemoryRecord>, partition: String, operation: CosmosBatchOperation): Int { val id = operation.id.ifEmpty { operation.document!!.id }; val key = "$partition\u0000$id"; val current = target[key]; return when (operation.kind) { "create" -> if (current != null) 409 else { target[key] = record(operation.document!!); 201 }; "upsert" -> { target[key] = record(operation.document!!); 200 }; "replace" -> if (current == null) 404 else if (operation.etag.isNotEmpty() && current.etag != operation.etag) 412 else { if (operation.etag.isNotEmpty()) matchedEtags += operation.etag; target[key] = record(operation.document!!); 200 }; "delete" -> if (current == null) 404 else if (operation.etag.isNotEmpty() && current.etag != operation.etag) 412 else { if (operation.etag.isNotEmpty()) matchedEtags += operation.etag; target.remove(key); 204 }; else -> 400 } }
}

private fun String.bytes() = encodeToByteArray()
private fun ByteArray.hex() = joinToString("") { "%02x".format(it.toInt() and 0xff) }
private fun specialBytes(length: Int) = ByteArray(length) { byteArrayOf(0, 0x7f, 0x80.toByte(), 0xff.toByte())[it % 4] }
