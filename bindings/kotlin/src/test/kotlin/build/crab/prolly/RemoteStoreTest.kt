package build.crab.prolly

import build.crab.prolly.remote.OptionalBytes
import build.crab.prolly.remote.NamedStoreRoot
import build.crab.prolly.remote.NodeEntry
import build.crab.prolly.remote.NodeMutation
import build.crab.prolly.remote.RemoteProlly
import build.crab.prolly.remote.RemoteStore
import build.crab.prolly.remote.RootCasResult
import build.crab.prolly.remote.RootCondition
import build.crab.prolly.remote.RootWrite
import build.crab.prolly.remote.StoreCapabilities
import build.crab.prolly.remote.StoreDescriptor
import build.crab.prolly.remote.StoreTransactionConflict
import build.crab.prolly.remote.StoreTransactionResult
import build.crab.prolly.remote.validateStoreDescriptor
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class RemoteStoreTest {
    @Test
    fun protocolPreservesPresentEmptyOrderedReadsAndRootConflicts() = runBlocking {
        val store = MemoryRemoteStore()
        val descriptor: StoreDescriptor = store.descriptor()
        validateStoreDescriptor(descriptor)
        assertThrows<IllegalArgumentException> {
            validateStoreDescriptor(descriptor.copy(protocolMajor = 2u))
        }

        val cid = "cid".bytes()
        store.putNode(cid, byteArrayOf())
        val ordered = store.batchGetNodesOrdered(listOf(cid, "missing".bytes(), cid))
        assertTrue(ordered[0].present)
        assertEquals(0, ordered[0].value.size)
        assertFalse(ordered[1].present)
        assertTrue(ordered[2].present)

        val name = "main".bytes()
        val first = store.compareAndSwapRootManifest(
            name,
            OptionalBytes.missing(),
            OptionalBytes.present("one".bytes()),
        )
        assertTrue(first.applied)
        val conflict = store.compareAndSwapRootManifest(
            name,
            OptionalBytes.missing(),
            OptionalBytes.present("two".bytes()),
        )
        assertFalse(conflict.applied)
        assertArrayEquals("one".bytes(), conflict.current.value)
        val deleted = store.compareAndSwapRootManifest(
            name,
            OptionalBytes.present("one".bytes()),
            OptionalBytes.missing(),
        )
        assertTrue(deleted.applied)

        val rolledBack = store.commitTransaction(
            emptyList(),
            listOf(RootCondition(name, OptionalBytes.present("wrong".bytes()))),
            listOf(RootWrite.Put(name, "replacement".bytes())),
        )
        assertFalse(rolledBack.applied)
        assertFalse(store.getRootManifest(name).present)
    }

    @Test
    fun remoteEngineRunsRustAgainstSuspendingKotlinStore() = runBlocking {
        ProllyNative.useLocalDebugLibrary()
        val store = MemoryRemoteStore()
        RemoteProlly.open(store).use { engine ->
            val empty = engine.create()
            val tree = engine.put(empty, "key".bytes(), "value".bytes())
            assertArrayEquals("value".bytes(), engine.get(tree, "key".bytes()))
            assertTrue(store.nodeCount > 0)
        }
    }
}

private fun String.bytes(): ByteArray = encodeToByteArray()

private class MemoryRemoteStore : RemoteStore {
    private val nodes = linkedMapOf<BytesKey, ByteArray>()
    private val hints = linkedMapOf<Pair<BytesKey, BytesKey>, ByteArray>()
    private val roots = linkedMapOf<BytesKey, ByteArray>()

    val nodeCount: Int get() = nodes.size

    override suspend fun descriptor(): StoreDescriptor = StoreDescriptor(
        protocolMajor = 1u,
        adapterName = "kotlin-test-memory",
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

    override suspend fun getNode(cid: ByteArray): OptionalBytes = optional(nodes[BytesKey(cid)])

    override suspend fun putNode(cid: ByteArray, value: ByteArray) {
        nodes[BytesKey(cid)] = value.copyOf()
    }

    override suspend fun deleteNode(cid: ByteArray) {
        nodes.remove(BytesKey(cid))
    }

    override suspend fun batchNodes(operations: List<NodeMutation>) {
        operations.forEach { operation ->
            when (operation) {
                is NodeMutation.Upsert -> nodes[BytesKey(operation.cid)] = operation.node
                is NodeMutation.Delete -> nodes.remove(BytesKey(operation.cid))
            }
        }
    }

    override suspend fun batchGetNodesOrdered(cids: List<ByteArray>): List<OptionalBytes> =
        cids.map { getNode(it) }

    override suspend fun listNodeCids(): List<ByteArray> =
        nodes.keys.sorted().map(BytesKey::bytes)

    override suspend fun getHint(namespace: ByteArray, key: ByteArray): OptionalBytes =
        optional(hints[BytesKey(namespace) to BytesKey(key)])

    override suspend fun putHint(namespace: ByteArray, key: ByteArray, value: ByteArray) {
        hints[BytesKey(namespace) to BytesKey(key)] = value.copyOf()
    }

    override suspend fun batchPutNodesWithHint(
        nodes: List<NodeEntry>,
        namespace: ByteArray,
        key: ByteArray,
        value: ByteArray,
    ) {
        nodes.forEach { this.nodes[BytesKey(it.cid)] = it.node }
        putHint(namespace, key, value)
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
    ): RootCasResult {
        val key = BytesKey(name)
        val current = roots[key]
        if (!matches(current, expected)) return RootCasResult(false, optional(current))
        if (replacement.present) roots[key] = replacement.value else roots.remove(key)
        return RootCasResult(true, OptionalBytes.missing())
    }

    override suspend fun listRootManifests(): List<NamedStoreRoot> =
        roots.entries.sortedBy { it.key }.map { NamedStoreRoot(it.key.bytes(), it.value) }

    override suspend fun commitTransaction(
        nodes: List<NodeMutation>,
        conditions: List<RootCondition>,
        roots: List<RootWrite>,
    ): StoreTransactionResult {
        for (condition in conditions) {
            val current = this.roots[BytesKey(condition.name)]
            if (!matches(current, condition.expected)) {
                return StoreTransactionResult.conflict(
                    StoreTransactionConflict(condition.name, condition.expected, optional(current)),
                )
            }
        }
        batchNodes(nodes)
        roots.forEach { write ->
            when (write) {
                is RootWrite.Put -> this.roots[BytesKey(write.name)] = write.manifest
                is RootWrite.Delete -> this.roots.remove(BytesKey(write.name))
            }
        }
        return StoreTransactionResult.applied()
    }

    private fun optional(value: ByteArray?): OptionalBytes =
        if (value == null) OptionalBytes.missing() else OptionalBytes.present(value)

    private fun matches(current: ByteArray?, expected: OptionalBytes): Boolean =
        if (!expected.present) current == null else current?.contentEquals(expected.value) == true
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
