package build.crab.prolly.remote

import build.crab.prolly.BytesListResultRecord
import build.crab.prolly.ConfigRecord
import build.crab.prolly.DiffRecord
import build.crab.prolly.EntryRecord
import build.crab.prolly.ForeignRemoteStore
import build.crab.prolly.MutationRecord
import build.crab.prolly.NamedBytesListResultRecord
import build.crab.prolly.NamedBytesRecord
import build.crab.prolly.NamedRootRecord
import build.crab.prolly.NamedRootUpdateRecord
import build.crab.prolly.NodeEntryRecord
import build.crab.prolly.NodeMutationRecord
import build.crab.prolly.NodePublicationRecord
import build.crab.prolly.OptionalBytesListResultRecord
import build.crab.prolly.OptionalBytesRecord
import build.crab.prolly.OptionalBytesResultRecord
import build.crab.prolly.RangeCursorRecord
import build.crab.prolly.RangePageRecord
import build.crab.prolly.RemoteNativeProllyEngine
import build.crab.prolly.RemoteNativeProllyTransaction
import build.crab.prolly.RootCasResultRecord
import build.crab.prolly.RootConditionRecord
import build.crab.prolly.RootWriteRecord
import build.crab.prolly.StoreCapabilitiesRecord
import build.crab.prolly.StoreDescriptorRecord
import build.crab.prolly.StoreDescriptorResultRecord
import build.crab.prolly.StoreErrorRecord
import build.crab.prolly.StoreLimitsRecord
import build.crab.prolly.StoreTransactionConflictRecord
import build.crab.prolly.TransactionResultRecord
import build.crab.prolly.TransactionUpdateRecord
import build.crab.prolly.TreeRecord
import build.crab.prolly.TreeStatsRecord
import build.crab.prolly.UnitResultRecord
import build.crab.prolly.defaultConfig
import build.crab.prolly.openRemoteProllyEngine
import kotlinx.coroutines.CancellationException

class RemoteProlly private constructor(
    private val native: RemoteNativeProllyEngine,
) : AutoCloseable {
    companion object {
        suspend fun open(
            store: RemoteStore,
            config: ConfigRecord = defaultConfig(),
        ): RemoteProlly = RemoteProlly(openRemoteProllyEngine(ForeignRemoteStoreAdapter(store), config))
    }

    fun create(): TreeRecord = native.create()

    suspend fun get(tree: TreeRecord, key: ByteArray): ByteArray? =
        native.get(tree, key.copyOf())?.copyOf()

    suspend fun getMany(tree: TreeRecord, keys: List<ByteArray>): List<ByteArray?> =
        native.getMany(tree, keys.map(ByteArray::copyOf)).map { it?.copyOf() }

    suspend fun put(tree: TreeRecord, key: ByteArray, value: ByteArray): TreeRecord =
        native.put(tree, key.copyOf(), value.copyOf())

    suspend fun delete(tree: TreeRecord, key: ByteArray): TreeRecord =
        native.delete(tree, key.copyOf())

    suspend fun batch(tree: TreeRecord, mutations: List<MutationRecord>): TreeRecord =
        native.batch(tree, mutations)

    suspend fun range(tree: TreeRecord, start: ByteArray, end: ByteArray?): List<EntryRecord> =
        native.range(tree, start.copyOf(), end?.copyOf()).map { entry ->
            EntryRecord(entry.key.copyOf(), entry.value.copyOf())
        }

    suspend fun prefix(tree: TreeRecord, prefix: ByteArray): List<EntryRecord> =
        native.prefix(tree, prefix.copyOf()).map { entry ->
            EntryRecord(entry.key.copyOf(), entry.value.copyOf())
        }

    suspend fun rangePage(
        tree: TreeRecord,
        cursor: RangeCursorRecord?,
        end: ByteArray?,
        limit: ULong,
    ): RangePageRecord = native.rangePage(tree, cursor, end?.copyOf(), limit)

    suspend fun diff(base: TreeRecord, other: TreeRecord): List<DiffRecord> = native.diff(base, other)

    suspend fun merge(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        resolver: String? = null,
    ): TreeRecord = native.merge(base, left, right, resolver)

    suspend fun collectStats(tree: TreeRecord): TreeStatsRecord = native.collectStats(tree)

    suspend fun loadNamedRoot(name: ByteArray): TreeRecord? = native.loadNamedRoot(name.copyOf())

    suspend fun listNamedRoots(): List<NamedRootRecord> = native.listNamedRoots()

    suspend fun publishNamedRoot(name: ByteArray, tree: TreeRecord) {
        native.publishNamedRoot(name.copyOf(), tree)
    }

    suspend fun publishNamedRootAtMillis(name: ByteArray, tree: TreeRecord, timestampMillis: ULong) {
        native.publishNamedRootAtMillis(name.copyOf(), tree, timestampMillis)
    }

    suspend fun deleteNamedRoot(name: ByteArray) {
        native.deleteNamedRoot(name.copyOf())
    }

    suspend fun compareAndSwapNamedRoot(
        name: ByteArray,
        expected: TreeRecord?,
        replacement: TreeRecord?,
    ): NamedRootUpdateRecord = native.compareAndSwapNamedRoot(name.copyOf(), expected, replacement)

    suspend fun beginTransaction(): RemoteProllyTransaction =
        RemoteProllyTransaction(native.beginTransaction())

    override fun close() {
        native.close()
    }
}

class RemoteProllyTransaction internal constructor(
    private val native: RemoteNativeProllyTransaction,
) : AutoCloseable {
    suspend fun create(): TreeRecord = native.create()

    suspend fun get(tree: TreeRecord, key: ByteArray): ByteArray? =
        native.get(tree, key.copyOf())?.copyOf()

    suspend fun put(tree: TreeRecord, key: ByteArray, value: ByteArray): TreeRecord =
        native.put(tree, key.copyOf(), value.copyOf())

    suspend fun delete(tree: TreeRecord, key: ByteArray): TreeRecord =
        native.delete(tree, key.copyOf())

    suspend fun batch(tree: TreeRecord, mutations: List<MutationRecord>): TreeRecord =
        native.batch(tree, mutations)

    suspend fun loadNamedRoot(name: ByteArray): TreeRecord? = native.loadNamedRoot(name.copyOf())

    suspend fun publishNamedRoot(name: ByteArray, tree: TreeRecord) {
        native.publishNamedRoot(name.copyOf(), tree)
    }

    suspend fun publishNamedRootAtMillis(name: ByteArray, tree: TreeRecord, timestampMillis: ULong) {
        native.publishNamedRootAtMillis(name.copyOf(), tree, timestampMillis)
    }

    suspend fun deleteNamedRoot(name: ByteArray) {
        native.deleteNamedRoot(name.copyOf())
    }

    suspend fun compareAndSwapNamedRoot(
        name: ByteArray,
        expected: TreeRecord?,
        replacement: TreeRecord?,
    ): NamedRootUpdateRecord = native.compareAndSwapNamedRoot(name.copyOf(), expected, replacement)

    suspend fun commit(): TransactionUpdateRecord = native.commit()

    suspend fun rollback() {
        native.rollback()
    }

    override fun close() {
        native.close()
    }
}

internal class ForeignRemoteStoreAdapter(
    private val store: RemoteStore,
) : ForeignRemoteStore {
    override suspend fun descriptor(): StoreDescriptorResultRecord = try {
        StoreDescriptorResultRecord(
            value = validateStoreDescriptor(store.descriptor()).toNative(),
            error = null,
        )
    } catch (error: Throwable) {
        StoreDescriptorResultRecord(value = null, error = error.toNativeError())
    }

    override suspend fun getNode(cid: ByteArray): OptionalBytesResultRecord = try {
        OptionalBytesResultRecord(store.getNode(cid.copyOf()).toNative(), null)
    } catch (error: Throwable) {
        OptionalBytesResultRecord(OptionalBytes.missing().toNative(), error.toNativeError())
    }

    override suspend fun putNode(cid: ByteArray, value: ByteArray): UnitResultRecord =
        unitResult { store.putNode(cid.copyOf(), value.copyOf()) }

    override suspend fun deleteNode(cid: ByteArray): UnitResultRecord =
        unitResult { store.deleteNode(cid.copyOf()) }

    override suspend fun batchNodes(ops: List<NodeMutationRecord>): UnitResultRecord =
        unitResult { store.batchNodes(ops.map(NodeMutationRecord::toDomain)) }

    override suspend fun publishNodes(publication: NodePublicationRecord): UnitResultRecord =
        unitResult {
            store.publishNodes(
                NodePublication(
                    nodes = publication.nodes.map { NodeEntry(it.key, it.value) },
                    hint = publication.hint?.let { NodePublicationHint(it.namespace, it.key, it.value) },
                    origin = PublicationOrigin(publication.origin.code),
                ),
            )
        }

    override suspend fun batchGetNodesOrdered(cids: List<ByteArray>): OptionalBytesListResultRecord = try {
        OptionalBytesListResultRecord(
            values = store.batchGetNodesOrdered(cids.map(ByteArray::copyOf)).map(OptionalBytes::toNative),
            error = null,
        )
    } catch (error: Throwable) {
        OptionalBytesListResultRecord(emptyList(), error.toNativeError())
    }

    override suspend fun listNodeCids(): BytesListResultRecord = try {
        BytesListResultRecord(store.listNodeCids().map(ByteArray::copyOf), null)
    } catch (error: Throwable) {
        BytesListResultRecord(emptyList(), error.toNativeError())
    }

    override suspend fun getHint(namespace: ByteArray, key: ByteArray): OptionalBytesResultRecord = try {
        OptionalBytesResultRecord(
            store.getHint(namespace.copyOf(), key.copyOf()).toNative(),
            null,
        )
    } catch (error: Throwable) {
        OptionalBytesResultRecord(OptionalBytes.missing().toNative(), error.toNativeError())
    }

    override suspend fun putHint(
        namespace: ByteArray,
        key: ByteArray,
        value: ByteArray,
    ): UnitResultRecord = unitResult {
        store.putHint(namespace.copyOf(), key.copyOf(), value.copyOf())
    }

    override suspend fun batchPutNodesWithHint(
        nodes: List<NodeEntryRecord>,
        namespace: ByteArray,
        key: ByteArray,
        value: ByteArray,
    ): UnitResultRecord = unitResult {
        store.batchPutNodesWithHint(
            nodes.map { NodeEntry(it.key.copyOf(), it.value.copyOf()) },
            namespace.copyOf(),
            key.copyOf(),
            value.copyOf(),
        )
    }

    override suspend fun getRootManifest(name: ByteArray): OptionalBytesResultRecord = try {
        OptionalBytesResultRecord(store.getRootManifest(name.copyOf()).toNative(), null)
    } catch (error: Throwable) {
        OptionalBytesResultRecord(OptionalBytes.missing().toNative(), error.toNativeError())
    }

    override suspend fun putRootManifest(name: ByteArray, manifest: ByteArray): UnitResultRecord =
        unitResult { store.putRootManifest(name.copyOf(), manifest.copyOf()) }

    override suspend fun deleteRootManifest(name: ByteArray): UnitResultRecord =
        unitResult { store.deleteRootManifest(name.copyOf()) }

    override suspend fun compareAndSwapRootManifest(
        name: ByteArray,
        expected: OptionalBytesRecord,
        new: OptionalBytesRecord,
    ): RootCasResultRecord = try {
        val result = store.compareAndSwapRootManifest(
            name.copyOf(),
            expected.toDomain(),
            new.toDomain(),
        )
        RootCasResultRecord(result.applied, result.current.toNative(), null)
    } catch (error: Throwable) {
        RootCasResultRecord(false, OptionalBytes.missing().toNative(), error.toNativeError())
    }

    override suspend fun listRootManifests(): NamedBytesListResultRecord = try {
        NamedBytesListResultRecord(
            values = store.listRootManifests().map { root ->
                NamedBytesRecord(root.name, root.manifest)
            },
            error = null,
        )
    } catch (error: Throwable) {
        NamedBytesListResultRecord(emptyList(), error.toNativeError())
    }

    override suspend fun commitTransaction(
        nodes: List<NodeMutationRecord>,
        conditions: List<RootConditionRecord>,
        roots: List<RootWriteRecord>,
    ): TransactionResultRecord = try {
        val result = store.commitTransaction(
            nodes.map(NodeMutationRecord::toDomain),
            conditions.map { RootCondition(it.name.copyOf(), it.expected.toDomain()) },
            roots.map(RootWriteRecord::toDomain),
        )
        TransactionResultRecord(
            applied = result.applied,
            conflict = result.conflict?.let { conflict ->
                StoreTransactionConflictRecord(
                    conflict.name,
                    conflict.expected.toNative(),
                    conflict.current.toNative(),
                )
            },
            error = null,
        )
    } catch (error: Throwable) {
        TransactionResultRecord(false, null, error.toNativeError())
    }

    private suspend fun unitResult(operation: suspend () -> Unit): UnitResultRecord = try {
        operation()
        UnitResultRecord(null)
    } catch (error: Throwable) {
        UnitResultRecord(error.toNativeError())
    }
}

private fun StoreDescriptor.toNative(): StoreDescriptorRecord = StoreDescriptorRecord(
    protocolMajor,
    adapterName,
    provider,
    schemaVersion,
    StoreCapabilitiesRecord(
        capabilities.nativeBatchReads,
        capabilities.atomicBatchWrites,
        capabilities.nodeScan,
        capabilities.hints,
        capabilities.atomicNodesAndHint,
        capabilities.rootScan,
        capabilities.rootCompareAndSwap,
        capabilities.transactions,
        capabilities.readParallelism,
    ),
    StoreLimitsRecord(
        limits.maxBatchReadItems,
        limits.maxBatchWriteItems,
        limits.maxTransactionOperations,
        limits.maxNodeBytes,
    ),
)

private fun OptionalBytes.toNative(): OptionalBytesRecord = OptionalBytesRecord(present, owned())

private fun OptionalBytesRecord.toDomain(): OptionalBytes = OptionalBytes.from(present, value.copyOf())

private fun NodeMutationRecord.toDomain(): NodeMutation {
    val optional = value.toDomain()
    return if (optional.present) NodeMutation.Upsert(key.copyOf(), optional.value) else NodeMutation.Delete(key.copyOf())
}

private fun RootWriteRecord.toDomain(): RootWrite {
    val optional = replacement.toDomain()
    return if (optional.present) RootWrite.Put(name.copyOf(), optional.value) else RootWrite.Delete(name.copyOf())
}

private fun Throwable.toNativeError(): StoreErrorRecord {
    if (this is CancellationException) throw this
    val error = (this as? StoreException)?.error
        ?: StoreError("internal", "remote store callback failed")
    return StoreErrorRecord(error.code, error.message, error.retryable, error.providerCode)
}
