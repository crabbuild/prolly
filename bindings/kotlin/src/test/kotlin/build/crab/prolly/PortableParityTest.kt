package build.crab.prolly

import build.crab.prolly.api.Engine
import build.crab.prolly.api.ProximityRecord
import build.crab.prolly.api.verifyKeyProof
import build.crab.prolly.api.verifyProximityMembershipProof
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class PortableParityTest {
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
