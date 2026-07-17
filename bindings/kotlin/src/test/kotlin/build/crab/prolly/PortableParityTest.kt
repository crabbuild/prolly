package build.crab.prolly

import build.crab.prolly.api.Engine
import build.crab.prolly.api.ProximityRecord
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

    private fun String.bytes() = encodeToByteArray()
}
