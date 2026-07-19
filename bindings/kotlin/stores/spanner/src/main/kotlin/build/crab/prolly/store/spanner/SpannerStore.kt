package build.crab.prolly.store.spanner

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
import com.google.api.gax.rpc.ApiException
import com.google.cloud.ByteArray as CloudByteArray
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.TransactionContext
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext

val SPANNER_DDL = listOf(
    "CREATE TABLE ProllyNodes (\n  Cid BYTES(32) NOT NULL,\n  Node BYTES(MAX) NOT NULL\n) PRIMARY KEY (Cid)",
    "CREATE TABLE ProllyHints (\n  Namespace BYTES(MAX) NOT NULL,\n  HintKey BYTES(MAX) NOT NULL,\n  Value BYTES(MAX) NOT NULL\n) PRIMARY KEY (Namespace, HintKey)",
    "CREATE TABLE ProllyRoots (\n  Name BYTES(MAX) NOT NULL,\n  Manifest BYTES(MAX) NOT NULL\n) PRIMARY KEY (Name)",
)

sealed interface SpannerMutation {
    data class UpsertNode(val key: ByteArray, val value: ByteArray) : SpannerMutation
    data class DeleteNode(val key: ByteArray) : SpannerMutation
    data class UpsertHint(val namespace: ByteArray, val key: ByteArray, val value: ByteArray) : SpannerMutation
    data class UpsertRoot(val key: ByteArray, val value: ByteArray) : SpannerMutation
    data class DeleteRoot(val key: ByteArray) : SpannerMutation
}
data class SpannerRootRecord(val name: ByteArray, val manifest: ByteArray)
interface SpannerTransaction { fun getRoot(name: ByteArray): OptionalBytes; fun buffer(mutations: List<SpannerMutation>) }
interface SpannerItemClient {
    suspend fun getNode(key: ByteArray): OptionalBytes
    suspend fun getHint(namespace: ByteArray, key: ByteArray): OptionalBytes
    suspend fun getRoot(name: ByteArray): OptionalBytes
    suspend fun listNodeCids(): List<ByteArray>
    suspend fun listRoots(): List<SpannerRootRecord>
    suspend fun apply(mutations: List<SpannerMutation>)
    suspend fun <T> readWrite(callback: (SpannerTransaction) -> T): T
}

data class SpannerStoreOptions(val adapterName: String = "spanner-v1", val readParallelism: UInt = 16u)

class SpannerStore private constructor(private val client: SpannerItemClient, options: SpannerStoreOptions) : RemoteStore, AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val storeDescriptor = validateStoreDescriptor(StoreDescriptor(
        2u, options.adapterName.ifBlank { "spanner-v1" }, "spanner", 1u,
        StoreCapabilities(false, true, true, true, true, true, true, true, options.readParallelism),
        StoreLimits(),
    ))

    constructor(database: DatabaseClient, dispatcher: CoroutineDispatcher = Dispatchers.IO, options: SpannerStoreOptions = SpannerStoreOptions()) : this(SpannerSdkItemClient(database, dispatcher), options)

    companion object {
        @JvmStatic fun fromClient(client: SpannerItemClient, options: SpannerStoreOptions = SpannerStoreOptions()) = SpannerStore(client, options)
        @JvmStatic @JvmOverloads fun fromExecutor(database: DatabaseClient, executor: Executor, options: SpannerStoreOptions = SpannerStoreOptions()) = SpannerStore(database, executor.asCoroutineDispatcher(), options)
    }

    override suspend fun descriptor() = operation("descriptor") { storeDescriptor }
    override suspend fun getNode(cid: ByteArray) = operation("get_node") { ownOptional(client.getNode(cid.copyOf())) }
    override suspend fun putNode(cid: ByteArray, value: ByteArray) = apply(listOf(SpannerMutation.UpsertNode(cid.copyOf(), value.copyOf())), "put_node")
    override suspend fun deleteNode(cid: ByteArray) = apply(listOf(SpannerMutation.DeleteNode(cid.copyOf())), "delete_node")
    override suspend fun batchNodes(operations: List<NodeMutation>) = apply(operations.map(::nodeMutation), "batch_nodes")
    override suspend fun batchGetNodesOrdered(cids: List<ByteArray>): List<OptionalBytes> = cids.map { getNode(it) }
    override suspend fun listNodeCids() = operation("list_node_cids") { client.listNodeCids().filter { it.size == 32 }.map(ByteArray::copyOf).sortedWith(BYTE_ARRAY_COMPARATOR) }
    override suspend fun getHint(namespace: ByteArray, key: ByteArray) = operation("get_hint") { ownOptional(client.getHint(namespace.copyOf(), key.copyOf())) }
    override suspend fun putHint(namespace: ByteArray, key: ByteArray, value: ByteArray) = apply(listOf(SpannerMutation.UpsertHint(namespace.copyOf(), key.copyOf(), value.copyOf())), "put_hint")
    override suspend fun batchPutNodesWithHint(nodes: List<NodeEntry>, namespace: ByteArray, key: ByteArray, value: ByteArray) = apply(nodes.map { SpannerMutation.UpsertNode(it.cid.copyOf(), it.node.copyOf()) } + SpannerMutation.UpsertHint(namespace.copyOf(), key.copyOf(), value.copyOf()), "batch_nodes_hint")
    override suspend fun getRootManifest(name: ByteArray) = operation("get_root_manifest") { ownOptional(client.getRoot(name.copyOf())) }
    override suspend fun putRootManifest(name: ByteArray, manifest: ByteArray) = apply(listOf(SpannerMutation.UpsertRoot(name.copyOf(), manifest.copyOf())), "put_root_manifest")
    override suspend fun deleteRootManifest(name: ByteArray) = apply(listOf(SpannerMutation.DeleteRoot(name.copyOf())), "delete_root_manifest")

    override suspend fun compareAndSwapRootManifest(name: ByteArray, expected: OptionalBytes, replacement: OptionalBytes): RootCasResult {
        val key = name.copyOf(); val wanted = ownOptional(expected); val next = ownOptional(replacement)
        return operation("compare_and_swap_root_manifest") { client.readWrite { transaction -> val current = ownOptional(transaction.getRoot(key)); if (!optionalEqual(current, wanted)) RootCasResult(false, current) else { transaction.buffer(listOf(if (next.present) SpannerMutation.UpsertRoot(key, next.value) else SpannerMutation.DeleteRoot(key))); RootCasResult(true, next) } } }
    }

    override suspend fun listRootManifests() = operation("list_root_manifests") { client.listRoots().map { NamedStoreRoot(it.name, it.manifest) }.sortedWith { left, right -> BYTE_ARRAY_COMPARATOR.compare(left.name, right.name) } }

    override suspend fun commitTransaction(nodes: List<NodeMutation>, conditions: List<RootCondition>, roots: List<RootWrite>): StoreTransactionResult {
        val ownedNodes = nodes.map(::cloneNodeMutation); val ownedConditions = conditions.map { RootCondition(it.name, ownOptional(it.expected)) }; val ownedRoots = roots.map(::cloneRootWrite)
        return operation("commit_transaction") { client.readWrite { transaction -> for (condition in ownedConditions) { val current = ownOptional(transaction.getRoot(condition.name)); if (!optionalEqual(current, condition.expected)) return@readWrite StoreTransactionResult.conflict(StoreTransactionConflict(condition.name, condition.expected, current)) }; transaction.buffer(ownedNodes.map(::nodeMutation) + ownedRoots.map(::rootMutation)); StoreTransactionResult.applied() } }
    }

    override fun close() { closed.set(true) }
    private suspend fun apply(mutations: List<SpannerMutation>, name: String) { if (mutations.isEmpty()) return; operation(name) { client.apply(mutations.map(::cloneMutation)) } }
    private suspend fun <T> operation(name: String, block: suspend () -> T): T { if (closed.get()) throw StoreException(StoreError("internal", "Spanner store is closed")); return try { block() } catch (error: CancellationException) { throw error } catch (error: StoreException) { throw error } catch (error: Throwable) { throw spannerError(name, error) } }
}

private class SpannerSdkItemClient(private val database: DatabaseClient, private val dispatcher: CoroutineDispatcher) : SpannerItemClient {
    override suspend fun getNode(key: ByteArray) = read("ProllyNodes", Key.of(cloud(key)), "Node")
    override suspend fun getHint(namespace: ByteArray, key: ByteArray) = read("ProllyHints", Key.of(cloud(namespace), cloud(key)), "Value")
    override suspend fun getRoot(name: ByteArray) = read("ProllyRoots", Key.of(cloud(name)), "Manifest")
    override suspend fun listNodeCids(): List<ByteArray> = withContext(dispatcher) { database.singleUse().executeQuery(Statement.of("SELECT Cid FROM ProllyNodes ORDER BY Cid")).use { rows -> buildList { while (rows.next()) add(rows.getBytes("Cid").toByteArray()) } } }
    override suspend fun listRoots(): List<SpannerRootRecord> = withContext(dispatcher) { database.singleUse().executeQuery(Statement.of("SELECT Name, Manifest FROM ProllyRoots ORDER BY Name")).use { rows -> buildList { while (rows.next()) add(SpannerRootRecord(rows.getBytes("Name").toByteArray(), rows.getBytes("Manifest").toByteArray())) } } }
    override suspend fun apply(mutations: List<SpannerMutation>) { withContext(dispatcher) { database.write(mutations.map(::sdkMutation)) } }
    @Suppress("UNCHECKED_CAST")
    override suspend fun <T> readWrite(callback: (SpannerTransaction) -> T): T = withContext(dispatcher) { database.readWriteTransaction().run { context -> val buffered = mutableListOf<SpannerMutation>(); val result = callback(SdkTransaction(context, buffered)); if (buffered.isNotEmpty()) context.buffer(buffered.map(::sdkMutation)); result } as T }
    private suspend fun read(table: String, key: Key, column: String): OptionalBytes = withContext(dispatcher) { val row = database.singleUse().readRow(table, key, listOf(column)); if (row == null) OptionalBytes.missing() else OptionalBytes.present(row.getBytes(column).toByteArray()) }
}
private class SdkTransaction(private val context: TransactionContext, private val mutations: MutableList<SpannerMutation>) : SpannerTransaction {
    override fun getRoot(name: ByteArray): OptionalBytes { val row = context.readRow("ProllyRoots", Key.of(cloud(name)), listOf("Manifest")); return if (row == null) OptionalBytes.missing() else OptionalBytes.present(row.getBytes("Manifest").toByteArray()) }
    override fun buffer(mutations: List<SpannerMutation>) { this.mutations += mutations.map(::cloneMutation) }
}

private fun sdkMutation(value: SpannerMutation): Mutation = when (value) {
    is SpannerMutation.UpsertNode -> Mutation.newInsertOrUpdateBuilder("ProllyNodes").set("Cid").to(cloud(value.key)).set("Node").to(cloud(value.value)).build()
    is SpannerMutation.DeleteNode -> Mutation.delete("ProllyNodes", Key.of(cloud(value.key)))
    is SpannerMutation.UpsertHint -> Mutation.newInsertOrUpdateBuilder("ProllyHints").set("Namespace").to(cloud(value.namespace)).set("HintKey").to(cloud(value.key)).set("Value").to(cloud(value.value)).build()
    is SpannerMutation.UpsertRoot -> Mutation.newInsertOrUpdateBuilder("ProllyRoots").set("Name").to(cloud(value.key)).set("Manifest").to(cloud(value.value)).build()
    is SpannerMutation.DeleteRoot -> Mutation.delete("ProllyRoots", Key.of(cloud(value.key)))
}
private fun cloud(value: ByteArray) = CloudByteArray.copyFrom(value)
private fun nodeMutation(value: NodeMutation): SpannerMutation = when (value) { is NodeMutation.Upsert -> SpannerMutation.UpsertNode(value.cid.copyOf(), value.node.copyOf()); is NodeMutation.Delete -> SpannerMutation.DeleteNode(value.cid.copyOf()) }
private fun rootMutation(value: RootWrite): SpannerMutation = when (value) { is RootWrite.Put -> SpannerMutation.UpsertRoot(value.name.copyOf(), value.manifest.copyOf()); is RootWrite.Delete -> SpannerMutation.DeleteRoot(value.name.copyOf()) }
private fun cloneNodeMutation(value: NodeMutation): NodeMutation = when (value) { is NodeMutation.Upsert -> NodeMutation.Upsert(value.cid, value.node); is NodeMutation.Delete -> NodeMutation.Delete(value.cid) }
private fun cloneRootWrite(value: RootWrite): RootWrite = when (value) { is RootWrite.Put -> RootWrite.Put(value.name, value.manifest); is RootWrite.Delete -> RootWrite.Delete(value.name) }
private fun cloneMutation(value: SpannerMutation): SpannerMutation = when (value) { is SpannerMutation.UpsertNode -> SpannerMutation.UpsertNode(value.key.copyOf(), value.value.copyOf()); is SpannerMutation.DeleteNode -> SpannerMutation.DeleteNode(value.key.copyOf()); is SpannerMutation.UpsertHint -> SpannerMutation.UpsertHint(value.namespace.copyOf(), value.key.copyOf(), value.value.copyOf()); is SpannerMutation.UpsertRoot -> SpannerMutation.UpsertRoot(value.key.copyOf(), value.value.copyOf()); is SpannerMutation.DeleteRoot -> SpannerMutation.DeleteRoot(value.key.copyOf()) }
private fun ownOptional(value: OptionalBytes) = OptionalBytes.of(value.present, value.value)
private fun optionalEqual(left: OptionalBytes, right: OptionalBytes) = left.present == right.present && (!left.present || left.value.contentEquals(right.value))
private val BYTE_ARRAY_COMPARATOR = Comparator<ByteArray> { left, right -> val common = minOf(left.size, right.size); for (index in 0 until common) { val compared = left[index].toUByte().compareTo(right[index].toUByte()); if (compared != 0) return@Comparator compared }; left.size.compareTo(right.size) }
private fun providerCode(error: Throwable): String? = when (error) { is SpannerException -> error.errorCode.name; is ApiException -> error.statusCode.code.name; else -> error.cause?.let(::providerCode) }
private fun spannerError(operation: String, error: Throwable): StoreException { val code = providerCode(error); val retryable = code in setOf("ABORTED", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED", "UNAVAILABLE"); val storeCode = if (code == "RESOURCE_EXHAUSTED") "resource_exhausted" else if (retryable) "unavailable" else "internal"; return StoreException(StoreError(storeCode, "Spanner operation failed", retryable, code?.let { "grpc:$it:$operation" }), error) }
