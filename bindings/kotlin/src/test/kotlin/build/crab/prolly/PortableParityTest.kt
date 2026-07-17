package build.crab.prolly

import build.crab.prolly.api.Engine
import build.crab.prolly.api.ProximityRecord
import build.crab.prolly.api.verifyKeyProof
import build.crab.prolly.api.verifyProximityMembershipProof
import build.crab.prolly.api.verifyProximityStructureProof
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class PortableParityTest {
    @Test
    fun versionedSnapshotsExposeOrderedNavigationAndBoundedPages() {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.versionedMap("versioned-ordered".bytes()).use { versioned ->
                versioned.initialize()
                versioned.apply(listOf(
                    MutationRecord(MutationKind.UPSERT, "a".bytes(), "one".bytes()),
                    MutationRecord(MutationKind.UPSERT, "ab".bytes(), "two".bytes()),
                    MutationRecord(MutationKind.UPSERT, "b".bytes(), "three".bytes()),
                    MutationRecord(MutationKind.UPSERT, "c".bytes(), "four".bytes()),
                ))
                versioned.snapshot()!!.use { snapshot ->
                    assertEquals(true, snapshot.containsKey("ab".bytes()))
                    assertArrayEquals("one".bytes(), snapshot.getMany(listOf("a".bytes(), "missing".bytes()))[0])
                    assertArrayEquals("a".bytes(), snapshot.firstEntry()!!.key)
                    assertArrayEquals("c".bytes(), snapshot.lastEntry()!!.key)
                    assertArrayEquals("ab".bytes(), snapshot.lowerBound("aa".bytes())!!.key)
                    assertArrayEquals("b".bytes(), snapshot.upperBound("ab".bytes())!!.key)
                    assertEquals(listOf("a", "ab"), snapshot.prefix("a".bytes()).map { String(it.key) })
                    assertEquals(listOf("ab", "b"), snapshot.range("ab".bytes(), "c".bytes()).map { String(it.key) })
                    val prefixPage = snapshot.prefixPage("a".bytes(), limit = 1uL)
                    assertEquals(listOf("a"), prefixPage.entries.map { String(it.key) })
                    assertEquals(true, prefixPage.nextCursor != null)
                    val first = snapshot.rangePage(end = "c".bytes(), limit = 2uL)
                    assertEquals(listOf("a", "ab"), first.entries.map { String(it.key) })
                    val second = snapshot.rangePage(first.nextCursor, "c".bytes(), 2uL)
                    assertEquals(listOf("b"), second.entries.map { String(it.key) })
                    assertEquals(listOf("c", "b"), snapshot.reversePage(start = "a".bytes(), limit = 2uL).entries.map { String(it.key) })
                    assertEquals(listOf("ab", "a"), snapshot.prefixReversePage("a".bytes(), limit = 2uL).entries.map { String(it.key) })
                }
            }
        }
    }

    @Test
    fun versionedIndexedAndProximityMapsUseThePortableApi() {
        ProllyNative.useLocalDebugLibrary()

        Engine.memory().use { engine ->
            engine.versionedMap("users".bytes()).use { versioned ->
                versioned.initialize()
                versioned.put("u1".bytes(), "Ada".bytes())
                assertArrayEquals("Ada".bytes(), versioned.get("u1".bytes()))
            }

            engine.indexRegistry().use { registry ->
                registry.register(
                    "by_team".bytes(),
                    1uL,
                    "team-v1",
                    IndexProjectionRecord.ALL,
                    object : SecondaryIndexExtractorCallback {
                        override fun extract(primaryKey: ByteArray, sourceValue: ByteArray) =
                            listOf(IndexEntryRecord(sourceValue.copyOf(), null))
                    },
                )
                engine.indexedMap("members".bytes(), registry).use { indexed ->
                    indexed.put("u1".bytes(), "red".bytes())
                    indexed.ensureIndex("by_team".bytes())
                    indexed.snapshot().use { snapshot ->
                        snapshot.index("by_team".bytes()).use { index ->
                            val records = index.records("red".bytes())
                            assertEquals(1, records.size)
                            assertArrayEquals("u1".bytes(), records.single().primaryKey)
                            var escaped: build.crab.prolly.api.ScopedBytes? = null
                            val key = index.withExactPage("red".bytes()) { rows ->
                                escaped = rows.single().primaryKey
                                rows.single().primaryKey.bytes()
                            }
                            assertArrayEquals("u1".bytes(), key)
                            assertThrows<IllegalStateException> { escaped!!.bytes() }
                            assertArrayEquals("by_team".bytes(), index.name)
                            assertEquals(1, index.prefixReversePage("r".bytes()).matches.size)
                        }
                    }
                    assertArrayEquals("members".bytes(), indexed.id)
                    val applied = indexed.apply(
                        listOf(MutationRecord(MutationKind.UPSERT, "u2".bytes(), "red".bytes())),
                    )
                    val conditional = indexed.applyIf(
                        applied.sourceVersion,
                        listOf(MutationRecord(MutationKind.UPSERT, "u3".bytes(), "blue".bytes())),
                    )
                    assertEquals(true, conditional.current != null)
                    indexed.snapshotAt(applied.sourceVersion).use { historical ->
                        assertEquals(2, historical.index("by_team".bytes()).use {
                            it.exactPage("red".bytes()).matches.size
                        })
                    }
                    indexed.snapshot().use { current ->
                        indexed.snapshotById(current.id).use { reopened ->
                            assertArrayEquals(current.id.sourceVersion, reopened.id.sourceVersion)
                            assertArrayEquals(current.id.catalogVersion, reopened.id.catalogVersion)
                        }
                    }
                }
            }

            engine.buildProximity(
                2u,
                listOf(ProximityRecord("a".bytes(), listOf(0.0f, 0.0f), "alpha".bytes())),
            ).use { proximity ->
                assertArrayEquals("a".bytes(), proximity.searchExact(listOf(0.1f, 0.1f), 1uL).neighbors.single().key)
                val key = proximity.withSearchView(listOf(0.1f, 0.1f), 1u) { rows ->
                    rows.single().key.bytes()
                }
                assertArrayEquals("a".bytes(), key)
            }
        }
    }

    @Test
    fun suspendWritesCopyMutableInputsBeforeDispatch() = runBlocking {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.versionedMap("async".bytes()).use { versioned ->
                versioned.initialize()
                val key = "k".bytes()
                versioned.putAsync(key, "v".bytes())
                key[0] = 'x'.code.toByte()
                assertArrayEquals("v".bytes(), versioned.get("k".bytes()))
            }
        }
    }

    @Test
    fun proofsSessionsAndMaintenanceAreApplicationFacing() {
        ProllyNative.useLocalDebugLibrary()
        Engine.memory().use { engine ->
            engine.versionedMap("proofs".bytes()).use { versioned ->
                versioned.initialize()
                versioned.put("k".bytes(), "v".bytes())
                versioned.snapshot()!!.use { snapshot ->
                    val verified = verifyKeyProof(snapshot.proveKey("k".bytes()))
                    assertEquals(true, verified.valid)
                    assertArrayEquals("v".bytes(), verified.value)
                    assertEquals(1uL, snapshot.stats().totalKeyValuePairs)
                    assertEquals(true, snapshot.export().nodes.isNotEmpty())
                    snapshot.read().use { session ->
                        assertArrayEquals("v".bytes(), session.get("k".bytes()))
                    }
                }
                assertEquals(true, versioned.verifyCatalog().versionCount >= 2uL)
                assertEquals(true, versioned.backup().isNotEmpty())
                assertEquals(true, versioned.planGc().reachability.liveCids.isNotEmpty())
            }

            engine.indexRegistry().use { registry ->
                registry.register(
                    "by_value".bytes(), 1uL, "value-v1", IndexProjectionRecord.ALL,
                    object : SecondaryIndexExtractorCallback {
                        override fun extract(primaryKey: ByteArray, sourceValue: ByteArray) =
                            listOf(IndexEntryRecord(sourceValue.copyOf(), null))
                    },
                )
                engine.indexedMap("indexed-maintenance".bytes(), registry).use { indexed ->
                    val version = indexed.put("k".bytes(), "term".bytes())
                    indexed.ensureIndex("by_value".bytes())
                    assertEquals(true, indexed.verifyIndex("by_value".bytes(), version.sourceVersion).valid)
                    assertEquals(true, indexed.metrics().buildAttempts >= 1uL)
                    assertEquals(true, indexed.exportCurrent().isNotEmpty())
                    assertEquals(true, indexed.keepLast(1uL).retainedSourceVersions.isNotEmpty())
                }
            }

            engine.buildProximity(
                2u,
                listOf(ProximityRecord("p".bytes(), listOf(0.0f, 0.0f), "payload".bytes())),
            ).use { proximity ->
                val verified = verifyProximityMembershipProof(
                    proximity.proveMembership("p".bytes()),
                    proximity.descriptor,
                )
                assertArrayEquals("payload".bytes(), verified.record!!.value)
                assertEquals(1uL, proximity.verify().recordCount)
                assertEquals(1uL, proximity.count)
                assertEquals(true, proximity.containsKey("p".bytes()))
                assertEquals(2u, proximity.config.dimensions)
                assertEquals(
                    1uL,
                    verifyProximityStructureProof(
                        proximity.proveStructure(), proximity.descriptor,
                    ).summary.recordCount,
                )
                val (mutated, stats) = proximity.mutate(
                    listOf(ProximityMutationRecord("q".bytes(), listOf(1.0f, 1.0f), "second".bytes())),
                )
                mutated.use { assertEquals(2uL, it.count) }
                assertEquals(true, stats.recordsRebuilt >= 1uL)
                proximity.read().use { retained ->
                    assertArrayEquals(
                        "p".bytes(),
                        retained.searchExact(listOf(0.0f, 0.0f), 1uL).neighbors.single().key,
                    )
                    assertArrayEquals(
                        "p".bytes(),
                        retained.withSearchView(listOf(0.0f, 0.0f), 1u) { rows ->
                            rows.single().key.bytes()
                        },
                    )
                }
                proximity.proveSearchExact(listOf(0.0f, 0.0f), 1uL).use { proof ->
                    val verifiedSearch = proof.verify(proximity.descriptor)
                    assertArrayEquals("p".bytes(), verifiedSearch.result.neighbors.single().key)
                    assertEquals(true, verifiedSearch.replayedEvents > 0uL)
                }
            }
        }
    }

    private fun String.bytes() = encodeToByteArray()
}
