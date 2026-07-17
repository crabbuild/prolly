package build.crab.prolly.storetest

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
import build.crab.prolly.remote.StoreTransactionConflict
import build.crab.prolly.remote.StoreTransactionResult
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.future.asCompletableFuture
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

class MemoryRemoteStore : RemoteStore {
    private val nodes = ConcurrentHashMap<BytesKey, ByteArray>()
    private val hints = ConcurrentHashMap<HintKey, ByteArray>()
    private val roots = ConcurrentHashMap<BytesKey, ByteArray>()
    private val writeMutex = Mutex()

    @Volatile
    private var readsBlocked = false
    @Volatile
    private var started = CompletableDeferred<Unit>()
    @Volatile
    private var cancelled = CompletableDeferred<Boolean>()

    val nodeCount: Int get() = nodes.size

    override suspend fun descriptor(): StoreDescriptor = StoreDescriptor(
        protocolMajor = 1u,
        adapterName = "jvm-test-memory",
        provider = "memory",
        schemaVersion = 1u,
        capabilities = StoreCapabilities(
            nativeBatchReads = true,
            atomicBatchWrites = true,
            nodeScan = true,
            hints = true,
            atomicNodesAndHint = true,
            rootScan = true,
            rootCompareAndSwap = true,
            transactions = true,
            readParallelism = 4u,
        ),
    )

    override suspend fun getNode(cid: ByteArray): OptionalBytes {
        maybeBlockRead()
        return optional(nodes[BytesKey(cid)])
    }

    override suspend fun putNode(cid: ByteArray, value: ByteArray) {
        nodes[BytesKey(cid)] = value.copyOf()
    }

    override suspend fun deleteNode(cid: ByteArray) {
        nodes.remove(BytesKey(cid))
    }

    override suspend fun batchNodes(operations: List<NodeMutation>) {
        writeMutex.withLock { applyNodes(operations) }
    }

    override suspend fun batchGetNodesOrdered(cids: List<ByteArray>): List<OptionalBytes> {
        maybeBlockRead()
        return cids.map { optional(nodes[BytesKey(it)]) }
    }

    override suspend fun listNodeCids(): List<ByteArray> = nodes.keys.sorted().map(BytesKey::bytes)

    override suspend fun getHint(namespace: ByteArray, key: ByteArray): OptionalBytes =
        optional(hints[HintKey(namespace, key)])

    override suspend fun putHint(namespace: ByteArray, key: ByteArray, value: ByteArray) {
        hints[HintKey(namespace, key)] = value.copyOf()
    }

    override suspend fun batchPutNodesWithHint(
        nodes: List<NodeEntry>,
        namespace: ByteArray,
        key: ByteArray,
        value: ByteArray,
    ) {
        writeMutex.withLock {
            nodes.forEach { this.nodes[BytesKey(it.cid)] = it.node }
            hints[HintKey(namespace, key)] = value.copyOf()
        }
    }

    override suspend fun getRootManifest(name: ByteArray): OptionalBytes = optional(roots[BytesKey(name)])

    override suspend fun putRootManifest(name: ByteArray, manifest: ByteArray) {
        roots[BytesKey(name)] = manifest.copyOf()
    }

    override suspend fun deleteRootManifest(name: ByteArray) {
        roots.remove(BytesKey(name))
    }

    override suspend fun compareAndSwapRootManifest(
        name: ByteArray,
        expected: OptionalBytes,
        replacement: OptionalBytes,
    ): RootCasResult = writeMutex.withLock {
        val key = BytesKey(name)
        val current = roots[key]
        if (!matches(current, expected)) return@withLock RootCasResult(false, optional(current))
        if (replacement.present) roots[key] = replacement.value else roots.remove(key)
        RootCasResult(true, OptionalBytes.missing())
    }

    override suspend fun listRootManifests(): List<NamedStoreRoot> =
        roots.entries.sortedBy { it.key }.map { NamedStoreRoot(it.key.bytes(), it.value) }

    override suspend fun commitTransaction(
        nodes: List<NodeMutation>,
        conditions: List<RootCondition>,
        roots: List<RootWrite>,
    ): StoreTransactionResult = writeMutex.withLock {
        for (condition in conditions) {
            val current = this.roots[BytesKey(condition.name)]
            if (!matches(current, condition.expected)) {
                return@withLock StoreTransactionResult.conflict(
                    StoreTransactionConflict(condition.name, condition.expected, optional(current)),
                )
            }
        }
        applyNodes(nodes)
        roots.forEach { write ->
            when (write) {
                is RootWrite.Put -> this.roots[BytesKey(write.name)] = write.manifest
                is RootWrite.Delete -> this.roots.remove(BytesKey(write.name))
            }
        }
        StoreTransactionResult.applied()
    }

    fun blockReads() {
        started = CompletableDeferred()
        cancelled = CompletableDeferred()
        readsBlocked = true
    }

    fun readStarted(): CompletableFuture<Void> = started.asCompletableFuture().thenApply { null }

    fun readCancelled(): CompletableFuture<Boolean> = cancelled.asCompletableFuture()

    private suspend fun maybeBlockRead() {
        if (!readsBlocked) return
        started.complete(Unit)
        try {
            awaitCancellation()
        } finally {
            cancelled.complete(true)
            readsBlocked = false
        }
    }

    private fun applyNodes(operations: List<NodeMutation>) {
        operations.forEach { operation ->
            when (operation) {
                is NodeMutation.Upsert -> nodes[BytesKey(operation.cid)] = operation.node
                is NodeMutation.Delete -> nodes.remove(BytesKey(operation.cid))
            }
        }
    }

    private fun optional(value: ByteArray?): OptionalBytes =
        if (value == null) OptionalBytes.missing() else OptionalBytes.present(value)

    private fun matches(current: ByteArray?, expected: OptionalBytes): Boolean =
        if (!expected.present) current == null else current?.contentEquals(expected.value) == true
}

private class HintKey(namespace: ByteArray, key: ByteArray) {
    private val namespace = namespace.copyOf()
    private val key = key.copyOf()
    override fun equals(other: Any?): Boolean =
        other is HintKey && namespace.contentEquals(other.namespace) && key.contentEquals(other.key)
    override fun hashCode(): Int = 31 * namespace.contentHashCode() + key.contentHashCode()
}

private class BytesKey(value: ByteArray) : Comparable<BytesKey> {
    private val value = value.copyOf()
    fun bytes(): ByteArray = value.copyOf()
    override fun equals(other: Any?): Boolean = other is BytesKey && value.contentEquals(other.value)
    override fun hashCode(): Int = value.contentHashCode()
    override fun compareTo(other: BytesKey): Int = value.compareUnsigned(other.value)
}

private fun ByteArray.compareUnsigned(other: ByteArray): Int {
    val size = minOf(size, other.size)
    for (index in 0 until size) {
        val compared = this[index].toUByte().compareTo(other[index].toUByte())
        if (compared != 0) return compared
    }
    return this.size.compareTo(other.size)
}
