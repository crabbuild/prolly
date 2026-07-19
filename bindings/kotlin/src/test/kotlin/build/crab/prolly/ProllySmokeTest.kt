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

            engine.readSession(tree).use { session ->
                assertArrayEquals("1".toByteArray(), session.get("a".toByteArray()))
                assertEquals(null, session.get("missing".toByteArray()))

                val values = session.getMany(
                    listOf(
                        "a".toByteArray(),
                        "missing".toByteArray(),
                        "a".toByteArray(),
                    ),
                )
                assertArrayEquals("1".toByteArray(), values[0])
                assertEquals(null, values[1])
                assertArrayEquals("1".toByteArray(), values[2])

                val visited = mutableListOf<String>()
                val visitor = object : EntryVisitorCallback {
                    override fun visit(entry: EntryRecord): Boolean {
                        visited += entry.key.decodeToString()
                        return true
                    }
                }
                val outcome = session.scanRange(ByteArray(0), null, visitor)
                assertEquals(listOf("a"), visited)
                assertEquals(1UL, outcome.visited)
                assertFalse(outcome.stopped)
            }

            val entries = engine.range(tree, ByteArray(0), null)
            assertEquals(1, entries.size)
            assertArrayEquals("a".toByteArray(), entries[0].key)
            assertArrayEquals("1".toByteArray(), entries[0].value)
        }
    }

    @Test
    fun memoryEngineDeleteRangeUsesHalfOpenBounds() {
        ProllyNative.useLocalDebugLibrary()

        ProllyEngine.memory(defaultConfig()).use { engine ->
            var tree = engine.create()
            for (key in listOf("a", "b", "c", "d", "e", "f")) {
                tree = engine.put(tree, key.toByteArray(), key.toByteArray())
            }
            val deleted = engine.deleteRange(tree, "b".toByteArray(), "e".toByteArray())
            assertEquals(listOf("a", "e", "f"), engine.range(deleted, ByteArray(0), null).map { it.key.decodeToString() })
            val withStats = engine.deleteRangeWithStats(tree, "b".toByteArray(), "e".toByteArray())
            assertEquals(listOf("a", "e", "f"), engine.range(withStats.tree, ByteArray(0), null).map { it.key.decodeToString() })
        }
    }

    @Test
    fun memoryTransactionCommitsRollsBackAndConflicts() {
        ProllyNative.useLocalDebugLibrary()

        ProllyEngine.memory(defaultConfig()).use { engine ->
            val sourceName = "tickets/source/current".toByteArray()
            val rollbackName = "tickets/source/rolled-back".toByteArray()
            val conflictName = "tickets/source/conflict".toByteArray()

            engine.beginTransaction().use { transaction ->
                val tree = transaction.put(
                    transaction.create(),
                    "ticket/123/status".toByteArray(),
                    "open".toByteArray(),
                )
                transaction.publishNamedRoot(sourceName, tree)
                val update = transaction.commit()
                assertTrue(update.applied)
                assertFalse(update.conflict)
                assertTrue(update.nodesWritten > 0UL)
                assertArrayEquals(tree.root, engine.loadNamedRoot(sourceName)?.root)
            }

            engine.beginTransaction().use { transaction ->
                val rolledBack = transaction.put(
                    transaction.create(),
                    "ticket/456/status".toByteArray(),
                    "closed".toByteArray(),
                )
                transaction.publishNamedRoot(rollbackName, rolledBack)
                transaction.rollback()
                assertEquals(null, engine.loadNamedRoot(rollbackName))
            }

            engine.beginTransaction().use { stale ->
                assertEquals(null, stale.loadNamedRoot(conflictName))
                val winner = engine.put(
                    engine.create(),
                    "ticket/789/status".toByteArray(),
                    "open".toByteArray(),
                )
                engine.publishNamedRoot(conflictName, winner)
                val loser = stale.put(
                    stale.create(),
                    "ticket/789/status".toByteArray(),
                    "closed".toByteArray(),
                )
                stale.publishNamedRoot(conflictName, loser)
                val conflict = stale.commit()
                assertFalse(conflict.applied)
                assertTrue(conflict.conflict)
                assertArrayEquals(conflictName, conflict.conflictDetail?.name)
                assertArrayEquals(winner.root, engine.loadNamedRoot(conflictName)?.root)
            }
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

        override fun publishNodes(publication: NodePublicationRecord): HostStoreUnitResultRecord {
            for (node in publication.nodes) {
                nodes[node.key.key()] = node.value.copyOf()
            }
            publication.hint?.let { hint ->
                hints[hint.namespace.key() to hint.key.key()] = hint.value.copyOf()
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
