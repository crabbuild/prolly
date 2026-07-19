package build.crab.prolly.store.cosmosdb

import build.crab.prolly.remote.NamedStoreRoot
import build.crab.prolly.remote.NodeEntry
import build.crab.prolly.remote.NodeMutation
import build.crab.prolly.remote.OptionalBytes
import build.crab.prolly.remote.RemoteStore
import build.crab.prolly.remote.RootCasResult
import build.crab.prolly.remote.RootCondition
import build.crab.prolly.remote.RootWrite
import build.crab.prolly.remote.StoreCapabilities
import build.crab.prolly.remote.StoreDescriptor
import build.crab.prolly.remote.StoreError
import build.crab.prolly.remote.StoreException
import build.crab.prolly.remote.StoreLimits
import build.crab.prolly.remote.StoreTransactionConflict
import build.crab.prolly.remote.StoreTransactionResult
import build.crab.prolly.remote.validateStoreDescriptor
import com.azure.cosmos.CosmosAsyncContainer
import com.azure.cosmos.CosmosException
import com.azure.cosmos.models.CosmosBatch
import com.azure.cosmos.models.CosmosBatchItemRequestOptions
import com.azure.cosmos.models.CosmosItemRequestOptions
import com.azure.cosmos.models.CosmosQueryRequestOptions
import com.azure.cosmos.models.PartitionKey
import com.azure.cosmos.models.SqlParameter
import com.azure.cosmos.models.SqlQuerySpec
import java.nio.ByteBuffer
import java.util.Base64
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.future.future
import kotlinx.coroutines.reactor.awaitSingle

data class CosmosDocument(val id: String, val kind: String, val family: String, val key: String, val value: String)
data class CosmosReadResult(val document: CosmosDocument, val etag: String)
data class CosmosBatchOperation(val kind: String, val id: String = "", val document: CosmosDocument? = null, val etag: String = "")
data class CosmosBatchResult(val statusCode: Int)
data class CosmosBatchResponse(val success: Boolean, val results: List<CosmosBatchResult>)
class CosmosStatusException(val statusCode: Int) : RuntimeException("Cosmos status $statusCode")

interface CosmosItemClient {
    suspend fun validatePartitionKey() {}
    suspend fun read(partition: String, id: String): CosmosReadResult
    suspend fun create(partition: String, document: CosmosDocument)
    suspend fun upsert(partition: String, document: CosmosDocument)
    suspend fun replace(partition: String, id: String, document: CosmosDocument, etag: String)
    suspend fun delete(partition: String, id: String, etag: String)
    suspend fun queryFamily(partition: String, family: String): List<CosmosDocument>
    suspend fun executeBatch(partition: String, operations: List<CosmosBatchOperation>): CosmosBatchResponse
}

data class CosmosDbStoreOptions(
    val keyPrefix: ByteArray = "prolly:".encodeToByteArray(),
    val partitionKey: String = "prolly",
    val adapterName: String = "cosmosdb-v1",
    val readParallelism: UInt = 16u,
)

class CosmosDbStore private constructor(
    private val client: CosmosItemClient,
    options: CosmosDbStoreOptions,
) : RemoteStore, AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val javaScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val partition = options.partitionKey.ifBlank { "prolly" }
    private val keyPrefix = options.keyPrefix.copyOf()
    private val storeDescriptor = validateStoreDescriptor(StoreDescriptor(
        2u, options.adapterName.ifBlank { "cosmosdb-v1" }, "cosmosdb", 1u,
        StoreCapabilities(false, false, true, true, false, true, true, true, options.readParallelism),
        StoreLimits(maxTransactionOperations = TRANSACTION_LIMIT.toUInt()),
    ))

    constructor(container: CosmosAsyncContainer, options: CosmosDbStoreOptions = CosmosDbStoreOptions()) : this(CosmosSdkItemClient(container), options)
    constructor(container: CosmosAsyncContainer, partitionKey: String, keyPrefix: ByteArray) : this(container, CosmosDbStoreOptions(keyPrefix, partitionKey))

    companion object { @JvmStatic fun fromClient(client: CosmosItemClient, options: CosmosDbStoreOptions = CosmosDbStoreOptions()) = CosmosDbStore(client, options) }

    suspend fun validateContainer() = operation("validate_container") { client.validatePartitionKey() }
    fun validateContainerAsync(): CompletableFuture<Unit> = javaScope.future { validateContainer() }
    override suspend fun descriptor() = operation("descriptor") { storeDescriptor }
    override suspend fun getNode(cid: ByteArray) = get(familyKey(NODE, cid), "get_node")
    override suspend fun putNode(cid: ByteArray, value: ByteArray) = upsert("node", familyKey(NODE, cid), value, "put_node")
    override suspend fun deleteNode(cid: ByteArray) = delete(familyKey(NODE, cid), "", true, "delete_node")
    override suspend fun batchNodes(operations: List<NodeMutation>) { operations.map(::cloneMutation).forEach { when (it) { is NodeMutation.Upsert -> putNode(it.cid, it.node); is NodeMutation.Delete -> deleteNode(it.cid) } } }
    override suspend fun batchGetNodesOrdered(cids: List<ByteArray>): List<OptionalBytes> = cids.map { getNode(it) }
    override suspend fun listNodeCids(): List<ByteArray> = operation("list_node_cids") { val prefix = keyPrefix + NODE; queryFamily("node").map(::decodeKey).filter { it.startsWithBytes(prefix) && it.size == prefix.size + 32 }.map { it.copyOfRange(prefix.size, it.size) }.sortedWith(BYTE_ARRAY_COMPARATOR) }
    override suspend fun getHint(namespace: ByteArray, key: ByteArray) = get(hintKey(namespace, key), "get_hint")
    override suspend fun putHint(namespace: ByteArray, key: ByteArray, value: ByteArray) = upsert("hint", hintKey(namespace, key), value, "put_hint")
    override suspend fun batchPutNodesWithHint(nodes: List<NodeEntry>, namespace: ByteArray, key: ByteArray, value: ByteArray) { nodes.forEach { putNode(it.cid, it.node) }; putHint(namespace, key, value) }
    override suspend fun getRootManifest(name: ByteArray) = get(familyKey(ROOT, name), "get_root_manifest")
    override suspend fun putRootManifest(name: ByteArray, manifest: ByteArray) = upsert("root", familyKey(ROOT, name), manifest, "put_root_manifest")
    override suspend fun deleteRootManifest(name: ByteArray) = delete(familyKey(ROOT, name), "", true, "delete_root_manifest")

    override suspend fun compareAndSwapRootManifest(name: ByteArray, expected: OptionalBytes, replacement: OptionalBytes): RootCasResult {
        val key = familyKey(ROOT, name); val wanted = OptionalBytes.of(expected.present, expected.value); val next = OptionalBytes.of(replacement.present, replacement.value); val id = documentId(key)
        return operation("compare_and_swap_root_manifest") {
            if (!wanted.present) {
                if (!next.present) { val current = getRaw(key); return@operation RootCasResult(!current.present, current) }
                try { client.create(partition, document(partition, "root", key, next.value)); return@operation RootCasResult(true, next) } catch (error: Throwable) { if (!isConflict(error)) throw error; return@operation RootCasResult(false, getRaw(key)) }
            }
            val current = try { readDocument(key) } catch (error: Throwable) { if (status(error) == 404) return@operation RootCasResult(false, OptionalBytes.missing()); throw error }
            val value = decodeValue(current.document); if (!value.contentEquals(wanted.value)) return@operation RootCasResult(false, OptionalBytes.present(value))
            try { if (next.present) client.replace(partition, id, document(partition, "root", key, next.value), current.etag) else client.delete(partition, id, current.etag); RootCasResult(true, next) }
            catch (error: Throwable) { if (!isConflict(error)) throw error; RootCasResult(false, getRaw(key)) }
        }
    }

    override suspend fun listRootManifests(): List<NamedStoreRoot> = operation("list_root_manifests") { val prefix = keyPrefix + ROOT; queryFamily("root").map { decodeKey(it) to decodeValue(it) }.filter { it.first.startsWithBytes(prefix) }.map { NamedStoreRoot(it.first.copyOfRange(prefix.size, it.first.size), it.second) }.sortedWith { left, right -> BYTE_ARRAY_COMPARATOR.compare(left.name, right.name) } }

    override suspend fun commitTransaction(nodes: List<NodeMutation>, conditions: List<RootCondition>, roots: List<RootWrite>): StoreTransactionResult {
        val ownedNodes = nodes.map(::cloneMutation); val ownedConditions = conditions.map { RootCondition(it.name, OptionalBytes.of(it.expected.present, it.expected.value)) }; val ownedRoots = roots.map(::cloneRootWrite)
        if (ownedNodes.size + ownedRoots.size > TRANSACTION_LIMIT) throw limit(ownedNodes.size + ownedRoots.size)
        return operation("commit_transaction") {
            val byName = ownedConditions.associateBy { it.name.hex() }; val written = ownedRoots.map { it.name.hex() }.toSet(); val operations = mutableListOf<CosmosBatchOperation>()
            for (condition in ownedConditions.filter { it.name.hex() !in written }) { val result = conditionOperations(condition); result.conflict?.let { return@operation StoreTransactionResult.conflict(it) }; operations += result.operations }
            for (root in ownedRoots) { val result = rootOperations(root, byName[root.name.hex()]); result.conflict?.let { return@operation StoreTransactionResult.conflict(it) }; operations += result.operations }
            for (node in ownedNodes) { val key = familyKey(NODE, node.cid); when (node) { is NodeMutation.Upsert -> operations += CosmosBatchOperation("upsert", document = document(partition, "node", key, node.node)); is NodeMutation.Delete -> try { operations += CosmosBatchOperation("delete", documentId(key), etag = readDocument(key).etag) } catch (error: Throwable) { if (status(error) != 404) throw error } } }
            if (operations.size > TRANSACTION_LIMIT) throw limit(operations.size); if (operations.isEmpty()) return@operation StoreTransactionResult.applied()
            val response = client.executeBatch(partition, operations); if (response.success) return@operation StoreTransactionResult.applied()
            for (condition in ownedConditions) { val current = getRaw(familyKey(ROOT, condition.name)); if (!optionalEqual(current, condition.expected)) return@operation StoreTransactionResult.conflict(conflictFor(condition, current)) }
            throw StoreException(StoreError("internal", "Cosmos DB transaction failed", providerCode = response.results.joinToString(",") { it.statusCode.toString() }))
        }
    }

    suspend fun clearNamespace() { if (keyPrefix.isEmpty()) throw StoreException(StoreError("invalid_argument", "refusing to clear an empty Cosmos DB key prefix")); for (family in listOf("node", "root", "hint")) for (entry in queryFamily(family)) { val key = decodeKey(entry); if (key.startsWithBytes(keyPrefix)) delete(key, "", true, "clear_namespace") } }
    override fun close() { if (closed.compareAndSet(false, true)) javaScope.cancel() }

    private data class OperationBuild(val operations: List<CosmosBatchOperation>, val conflict: StoreTransactionConflict? = null)
    private suspend fun conditionOperations(condition: RootCondition): OperationBuild { val key = familyKey(ROOT, condition.name); return try { val current = readDocument(key); val value = decodeValue(current.document); if (!condition.expected.present || !value.contentEquals(condition.expected.value)) OperationBuild(emptyList(), conflictFor(condition, OptionalBytes.present(value))) else OperationBuild(listOf(CosmosBatchOperation("replace", documentId(key), current.document, current.etag))) } catch (error: Throwable) { if (status(error) != 404) throw error; if (condition.expected.present) OperationBuild(emptyList(), conflictFor(condition, OptionalBytes.missing())) else { val placeholder = document(partition, "root", key, byteArrayOf()); OperationBuild(listOf(CosmosBatchOperation("create", document = placeholder), CosmosBatchOperation("delete", placeholder.id))) } } }
    private suspend fun rootOperations(root: RootWrite, condition: RootCondition?): OperationBuild { val key = familyKey(ROOT, root.name); val id = documentId(key); if (condition == null) return when (root) { is RootWrite.Put -> OperationBuild(listOf(CosmosBatchOperation("upsert", document = document(partition, "root", key, root.manifest)))); is RootWrite.Delete -> try { OperationBuild(listOf(CosmosBatchOperation("delete", id, etag = readDocument(key).etag))) } catch (error: Throwable) { if (status(error) == 404) OperationBuild(emptyList()) else throw error } }; return try { val current = readDocument(key); val value = decodeValue(current.document); if (!condition.expected.present || !value.contentEquals(condition.expected.value)) OperationBuild(emptyList(), conflictFor(condition, OptionalBytes.present(value))) else when (root) { is RootWrite.Put -> OperationBuild(listOf(CosmosBatchOperation("replace", id, document(partition, "root", key, root.manifest), current.etag))); is RootWrite.Delete -> OperationBuild(listOf(CosmosBatchOperation("delete", id, etag = current.etag))) } } catch (error: Throwable) { if (status(error) != 404) throw error; if (condition.expected.present) OperationBuild(emptyList(), conflictFor(condition, OptionalBytes.missing())) else when (root) { is RootWrite.Put -> OperationBuild(listOf(CosmosBatchOperation("create", document = document(partition, "root", key, root.manifest)))); is RootWrite.Delete -> { val placeholder = document(partition, "root", key, byteArrayOf()); OperationBuild(listOf(CosmosBatchOperation("create", document = placeholder), CosmosBatchOperation("delete", id))) } } } }
    private suspend fun get(key: ByteArray, name: String) = operation(name) { getRaw(key) }
    private suspend fun getRaw(key: ByteArray): OptionalBytes = try { OptionalBytes.present(decodeValue(readDocument(key).document)) } catch (error: Throwable) { if (status(error) == 404) OptionalBytes.missing() else throw error }
    private suspend fun readDocument(key: ByteArray): CosmosReadResult { val result = client.read(partition, documentId(key)); if (result.document.id != documentId(key) || result.document.kind != partition) throw StoreException(StoreError("invalid_data", "Cosmos DB document identity does not match requested key")); return result.copy(document = result.document.copy()) }
    private suspend fun upsert(family: String, key: ByteArray, value: ByteArray, name: String) { val owned = value.copyOf(); operation(name) { client.upsert(partition, document(partition, family, key, owned)) } }
    private suspend fun delete(key: ByteArray, etag: String, ignoreMissing: Boolean, name: String) { operation(name) { try { client.delete(partition, documentId(key), etag) } catch (error: Throwable) { if (!ignoreMissing || status(error) != 404) throw error } } }
    private suspend fun queryFamily(family: String) = client.queryFamily(partition, family).filter { it.kind == partition && it.family == family }.map(CosmosDocument::copy)
    private fun familyKey(family: ByteArray, suffix: ByteArray) = keyPrefix + family + suffix.copyOf()
    private fun hintKey(namespace: ByteArray, key: ByteArray) = keyPrefix + HINT + ByteBuffer.allocate(8).putLong(namespace.size.toLong()).array() + namespace.copyOf() + key.copyOf()
    private suspend fun <T> operation(name: String, block: suspend () -> T): T { if (closed.get()) throw StoreException(StoreError("internal", "Cosmos DB store is closed")); return try { block() } catch (error: CancellationException) { throw error } catch (error: StoreException) { throw error } catch (error: Throwable) { throw cosmosError(name, error) } }
}

private class CosmosSdkItemClient(private val container: CosmosAsyncContainer) : CosmosItemClient {
    override suspend fun validatePartitionKey() { val paths = container.read().awaitSingle().properties.partitionKeyDefinition.paths; if (paths.size != 1 || paths[0] != "/kind") throw StoreException(StoreError("invalid_argument", "Cosmos DB container partition key must be /kind")) }
    override suspend fun read(partition: String, id: String): CosmosReadResult { val response = container.readItem(id, PartitionKey(partition), CosmosDocument::class.java).awaitSingle(); return CosmosReadResult(response.item, response.eTag ?: "") }
    override suspend fun create(partition: String, document: CosmosDocument) { container.createItem(document, PartitionKey(partition), CosmosItemRequestOptions()).awaitSingle() }
    override suspend fun upsert(partition: String, document: CosmosDocument) { container.upsertItem(document, PartitionKey(partition), CosmosItemRequestOptions()).awaitSingle() }
    override suspend fun replace(partition: String, id: String, document: CosmosDocument, etag: String) { container.replaceItem(document, id, PartitionKey(partition), CosmosItemRequestOptions().setIfMatchETag(etag)).awaitSingle() }
    override suspend fun delete(partition: String, id: String, etag: String) { val options = CosmosItemRequestOptions(); if (etag.isNotEmpty()) options.setIfMatchETag(etag); container.deleteItem(id, PartitionKey(partition), options).awaitSingle() }
    override suspend fun queryFamily(partition: String, family: String): List<CosmosDocument> { val query = SqlQuerySpec("SELECT * FROM c WHERE c.kind = @kind AND c.family = @family", SqlParameter("@kind", partition), SqlParameter("@family", family)); return container.queryItems(query, CosmosQueryRequestOptions().setPartitionKey(PartitionKey(partition)), CosmosDocument::class.java).collectList().awaitSingle() }
    override suspend fun executeBatch(partition: String, operations: List<CosmosBatchOperation>): CosmosBatchResponse { val batch = CosmosBatch.createCosmosBatch(PartitionKey(partition)); operations.forEach { operation -> val options = CosmosBatchItemRequestOptions(); if (operation.etag.isNotEmpty()) options.setIfMatchETag(operation.etag); when (operation.kind) { "create" -> batch.createItemOperation(operation.document!!, options); "upsert" -> batch.upsertItemOperation(operation.document!!, options); "replace" -> batch.replaceItemOperation(operation.id, operation.document!!, options); "delete" -> batch.deleteItemOperation(operation.id, options); else -> throw StoreException(StoreError("invalid_argument", "unknown Cosmos DB batch operation")) } }; val response = container.executeCosmosBatch(batch).awaitSingle(); return CosmosBatchResponse(response.isSuccessStatusCode, response.results.map { CosmosBatchResult(it.statusCode) }) }
}

private const val TRANSACTION_LIMIT = 100
private val NODE = "node:".encodeToByteArray(); private val ROOT = "root:".encodeToByteArray(); private val HINT = "hint:".encodeToByteArray()
private fun document(partition: String, family: String, key: ByteArray, value: ByteArray) = CosmosDocument(documentId(key), partition, family, key.hex(), Base64.getEncoder().encodeToString(value))
private fun documentId(key: ByteArray) = "k${key.hex()}"
private fun decodeKey(value: CosmosDocument): ByteArray { if (value.key.length % 2 != 0 || !value.key.matches(Regex("[0-9a-f]*"))) throw StoreException(StoreError("invalid_data", "Cosmos DB document key is not valid hex")); return ByteArray(value.key.length / 2) { value.key.substring(it * 2, it * 2 + 2).toInt(16).toByte() } }
private fun decodeValue(value: CosmosDocument): ByteArray = try { Base64.getDecoder().decode(value.value) } catch (error: IllegalArgumentException) { throw StoreException(StoreError("invalid_data", "Cosmos DB document value is not valid base64"), error) }
private fun cloneMutation(value: NodeMutation): NodeMutation = when (value) { is NodeMutation.Upsert -> NodeMutation.Upsert(value.cid, value.node); is NodeMutation.Delete -> NodeMutation.Delete(value.cid) }
private fun cloneRootWrite(value: RootWrite): RootWrite = when (value) { is RootWrite.Put -> RootWrite.Put(value.name, value.manifest); is RootWrite.Delete -> RootWrite.Delete(value.name) }
private fun conflictFor(condition: RootCondition, current: OptionalBytes) = StoreTransactionConflict(condition.name, condition.expected, current)
private fun optionalEqual(left: OptionalBytes, right: OptionalBytes) = left.present == right.present && (!left.present || left.value.contentEquals(right.value))
private fun ByteArray.startsWithBytes(prefix: ByteArray) = size >= prefix.size && prefix.indices.all { this[it] == prefix[it] }
private fun ByteArray.hex() = joinToString("") { "%02x".format(it.toInt() and 0xff) }
private val BYTE_ARRAY_COMPARATOR = Comparator<ByteArray> { left, right -> val common = minOf(left.size, right.size); for (index in 0 until common) { val compared = left[index].toUByte().compareTo(right[index].toUByte()); if (compared != 0) return@Comparator compared }; left.size.compareTo(right.size) }
private fun status(error: Throwable): Int = when (error) { is CosmosStatusException -> error.statusCode; is CosmosException -> error.statusCode; else -> error.cause?.let(::status) ?: 0 }
private fun isConflict(error: Throwable) = status(error) in setOf(404, 409, 412)
private fun limit(count: Int) = StoreException(StoreError("resource_exhausted", "Cosmos DB transaction has $count operations, exceeding the $TRANSACTION_LIMIT operation limit"))
private fun cosmosError(operation: String, error: Throwable): StoreException { val code = status(error); val retryable = code == 408 || code == 429 || code >= 500; return StoreException(StoreError(if (retryable) "unavailable" else "internal", "Cosmos DB operation failed", retryable, if (code == 0) null else "cosmos:$code:$operation"), error) }
