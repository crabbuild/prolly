package build.crab.prolly

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ProllySmokeTest {
    @Test
    fun memoryEngineCrudAndRange() {
        ProllyNative.useLocalDebugLibrary()

        ProllyEngine.memory(defaultConfig()).use { engine ->
            var tree = engine.create()
            tree = engine.put(tree, "a".toByteArray(), "1".toByteArray())

            assertArrayEquals("1".toByteArray(), engine.get(tree, "a".toByteArray()))

            val entries = engine.range(tree, ByteArray(0), null)
            assertEquals(1, entries.size)
            assertArrayEquals("a".toByteArray(), entries[0].key)
            assertArrayEquals("1".toByteArray(), entries[0].value)
        }
    }

    @Test
    fun customStoreCallbacksDriveEngine() {
        ProllyNative.useLocalDebugLibrary()

        val sourceStore = MemoryHostStore()
        ProllyEngine.customStore(sourceStore, defaultConfig()).use { source ->
            val empty = source.create()
            val tree = source.batch(
                empty,
                listOf(
                    MutationRecord(MutationKind.UPSERT, "a".toByteArray(), "1".toByteArray()),
                    MutationRecord(MutationKind.UPSERT, "b".toByteArray(), "2".toByteArray()),
                ),
            )

            assertArrayEquals("1".toByteArray(), source.get(tree, "a".toByteArray()))
            assertEquals(3, source.getMany(tree, listOf("a".toByteArray(), "missing".toByteArray(), "b".toByteArray())).size)
            assertTrue(source.publishPrefixPathHint(tree, "a".toByteArray()))
            assertTrue(source.hydratePrefixPathHint(tree, "a".toByteArray()))

            source.publishNamedRootAtMillis("main".toByteArray(), tree, 7UL)
            val loaded = source.loadNamedRoot("main".toByteArray())
            assertArrayEquals(tree.root, loaded?.root)
            assertEquals(1, source.listNamedRoots().size)

            val cids = source.listNodeCids()
            assertTrue(cids.isNotEmpty())
            assertEquals(0UL, source.planStoreGc(listOf(tree)).reclaimableNodes)
            assertEquals(
                0UL,
                source.planStoreGcForRetention(
                    NamedRootRetentionRecord(NamedRootRetentionKind.ALL, emptyList(), ByteArray(0), null, null),
                ).reclaimableNodes,
            )

            val destinationStore = MemoryHostStore()
            ProllyEngine.customStore(destinationStore, defaultConfig()).use { destination ->
                val plan = source.planMissingNodes(tree, destination)
                assertTrue(plan.missingNodes > 0UL)
                val copied = source.copyMissingNodes(tree, destination)
                assertEquals(plan.missingNodes, copied.copiedNodes)
                assertArrayEquals("2".toByteArray(), destination.get(tree, "b".toByteArray()))
            }

            val update = source.compareAndSwapNamedRoot("main".toByteArray(), tree, null)
            assertTrue(update.applied)
            assertFalse(update.conflict)
            assertEquals(null, source.loadNamedRoot("main".toByteArray()))
        }
    }

    private class MemoryHostStore : HostStoreCallback {
        private val nodes = linkedMapOf<List<Byte>, ByteArray>()
        private val hints = linkedMapOf<Pair<List<Byte>, List<Byte>>, ByteArray>()
        private val roots = linkedMapOf<List<Byte>, RootManifestRecord>()

        override fun get(key: ByteArray): HostStoreBytesResultRecord =
            HostStoreBytesResultRecord(nodes[key.key()]?.copyOf(), null)

        override fun put(key: ByteArray, value: ByteArray): HostStoreUnitResultRecord {
            nodes[key.key()] = value.copyOf()
            return HostStoreUnitResultRecord(null)
        }

        override fun delete(key: ByteArray): HostStoreUnitResultRecord {
            nodes.remove(key.key())
            return HostStoreUnitResultRecord(null)
        }

        override fun batch(ops: List<MutationRecord>): HostStoreUnitResultRecord {
            for (op in ops) {
                when (op.kind) {
                    MutationKind.UPSERT -> nodes[op.key.key()] = requireNotNull(op.value).copyOf()
                    MutationKind.DELETE -> nodes.remove(op.key.key())
                }
            }
            return HostStoreUnitResultRecord(null)
        }

        override fun batchGetOrdered(keys: List<ByteArray>): HostStoreBatchGetResultRecord =
            HostStoreBatchGetResultRecord(keys.map { nodes[it.key()]?.copyOf() }, null)

        override fun prefersBatchReads(): HostStoreBoolResultRecord =
            HostStoreBoolResultRecord(true, null)

        override fun supportsHints(): HostStoreBoolResultRecord =
            HostStoreBoolResultRecord(true, null)

        override fun getHint(namespace: ByteArray, key: ByteArray): HostStoreBytesResultRecord =
            HostStoreBytesResultRecord(hints[namespace.key() to key.key()]?.copyOf(), null)

        override fun putHint(namespace: ByteArray, key: ByteArray, value: ByteArray): HostStoreUnitResultRecord {
            hints[namespace.key() to key.key()] = value.copyOf()
            return HostStoreUnitResultRecord(null)
        }

        override fun listNodeCids(): HostStoreListBytesResultRecord =
            HostStoreListBytesResultRecord(nodes.keys.map { it.bytes() }, null)

        override fun getRoot(name: ByteArray): HostStoreRootResultRecord =
            HostStoreRootResultRecord(roots[name.key()], null)

        override fun putRoot(name: ByteArray, manifest: RootManifestRecord): HostStoreUnitResultRecord {
            roots[name.key()] = manifest
            return HostStoreUnitResultRecord(null)
        }

        override fun deleteRoot(name: ByteArray): HostStoreUnitResultRecord {
            roots.remove(name.key())
            return HostStoreUnitResultRecord(null)
        }

        override fun compareAndSwapRoot(
            name: ByteArray,
            expected: RootManifestRecord?,
            replacement: RootManifestRecord?,
        ): HostStoreRootCasResultRecord {
            val key = name.key()
            val current = roots[key]
            return if (sameManifest(current, expected)) {
                if (replacement == null) {
                    roots.remove(key)
                } else {
                    roots[key] = replacement
                }
                HostStoreRootCasResultRecord(true, null, null)
            } else {
                HostStoreRootCasResultRecord(false, current, null)
            }
        }

        override fun listRoots(): HostStoreListRootsResultRecord =
            HostStoreListRootsResultRecord(
                roots.map { (name, manifest) -> HostStoreNamedRootManifestRecord(name.bytes(), manifest) },
                null,
            )

        private fun sameManifest(left: RootManifestRecord?, right: RootManifestRecord?): Boolean =
            when {
                left == null || right == null -> left == right
                else -> rootManifestToBytes(left).contentEquals(rootManifestToBytes(right))
            }

        private fun ByteArray.key(): List<Byte> = toList()

        private fun List<Byte>.bytes(): ByteArray = ByteArray(size) { index -> this[index] }
    }
}
