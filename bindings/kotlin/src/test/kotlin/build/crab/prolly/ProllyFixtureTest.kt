package build.crab.prolly

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

class ProllyFixtureTest {
    private val fixtures: JsonNode by lazy {
        ObjectMapper().readTree(Files.readString(fixturePath()))
    }

    @Test
    fun nodeFixturesDecodeEncodeAndHash() {
        ProllyNative.useLocalDebugLibrary()

        for (fixture in fixtures["node_fixtures"]) {
            val bytes = hex(fixture["bytes"].asText())
            val node = nodeFromBytes(bytes)
            assertArrayEquals(bytes, nodeToBytes(node))
            assertArrayEquals(hex(fixture["cid"].asText()), nodeCid(node))
        }
    }

    @Test
    fun boundaryAndKeyFixturesMatchRust() {
        ProllyNative.useLocalDebugLibrary()

        for (fixture in fixtures["boundary_fixtures"]) {
            assertEquals(
                fixture["is_boundary"].asBoolean(),
                isBoundaryConfig(
                    configFromFixture(fixture["config"]),
                    fixture["count"].asText().toULong(),
                    hex(fixture["key"].asText()),
                    hex(fixture["value"].asText()),
                ),
            )
        }

        for (fixture in fixtures["key_fixtures"]["prefix_end"]) {
            val prefix = hex(fixture["prefix"].asText())
            val actual = prefixEnd(prefix)
            if (fixture["end"].isNull) {
                assertNull(actual)
            } else {
                assertArrayEquals(hex(fixture["end"].asText()), actual)
            }
            val bounds = prefixRange(prefix)
            assertArrayEquals(prefix, bounds.start)
            if (fixture["end"].isNull) {
                assertNull(bounds.end)
            } else {
                assertArrayEquals(hex(fixture["end"].asText()), bounds.end)
            }
        }

        for (fixture in fixtures["key_fixtures"]["numeric"]) {
            val actual = when (fixture["kind"].asText()) {
                "u64" -> u64Key(fixture["value"].asText().toULong())
                "u128" -> u128Key(fixture["value"].asText())
                "i64" -> i64Key(fixture["value"].asText().toLong())
                "i128" -> i128Key(fixture["value"].asText())
                "timestamp_millis" -> timestampMillisKey(fixture["value"].asText().toULong())
                else -> null
            }
            if (actual != null) {
                assertArrayEquals(hex(fixture["encoded"].asText()), actual)
            }
        }

        for (fixture in fixtures["key_fixtures"]["segments"]) {
            var encoded = byteArrayOf()
            val segmentBytes = mutableListOf<ByteArray>()
            for (segment in fixture["segments"]) {
                val bytes = hex(segment.asText())
                segmentBytes += bytes
                encoded += encodeSegment(bytes)
            }
            assertArrayEquals(hex(fixture["encoded"].asText()), encoded)
            assertArrayEquals(hex(fixture["encoded"].asText()), keyFromSegments(segmentBytes))
            val prefix = keyFromSegments(segmentBytes.take(1))
            assertArrayEquals(hex(fixture["encoded"].asText()), keyFromPrefixedSegments(prefix, segmentBytes.drop(1)))
            val actual = decodeSegments(hex(fixture["encoded"].asText())).map { it.hex() }
            val expected = fixture["decoded"].map { it.asText() }
            assertEquals(expected, actual)
        }

        for (fixture in fixtures["key_fixtures"]["debug"]) {
            assertEquals(fixture["debug"].asText(), debugKey(hex(fixture["key"].asText())))
        }
    }

    @Test
    fun treeAndDiffFixturesMatchRust() {
        ProllyNative.useLocalDebugLibrary()

        for (fixture in fixtures["tree_fixtures"]) {
            ProllyEngine.memory(configFromFixture(fixture["config"])).use { engine ->
                val tree = buildTree(engine, fixture["entries"])
                assertArrayEquals(hex(fixture["root"].asText()), tree.root)

                for (lookup in fixture["lookups"]) {
                    val actual = engine.get(tree, hex(lookup["key"].asText()))
                    if (lookup["value"].isNull) {
                        assertNull(actual)
                    } else {
                        assertArrayEquals(hex(lookup["value"].asText()), actual)
                    }
                }

                for (rangeFixture in fixture["ranges"]) {
                    val end = if (rangeFixture["end"].isNull) null else hex(rangeFixture["end"].asText())
                    val actual = engine.range(tree, hex(rangeFixture["start"].asText()), end)
                    assertEntries(rangeFixture["entries"], actual)
                }
            }
        }

        val diffFixture = fixtures["diff_fixtures"][0]
        ProllyEngine.memory(configFromFixture(diffFixture["config"])).use { engine ->
            val base = buildTree(
                engine,
                listOf(
                    "61" to "31",
                    "62" to "32",
                    "63" to "33",
                ),
            )
            val other = buildTree(
                engine,
                listOf(
                    "61" to "31",
                    "62" to "3232",
                    "64" to "34",
                ),
            )
            assertArrayEquals(hex(diffFixture["base_root"].asText()), base.root)
            assertArrayEquals(hex(diffFixture["other_root"].asText()), other.root)

            val actual = engine.diff(base, other)
            assertEquals(diffFixture["diffs"].size(), actual.size)
            for ((index, diff) in actual.withIndex()) {
                val expected = diffFixture["diffs"][index]
                assertEquals(expected["kind"].asText(), diff.kind.name.lowercase())
                assertArrayEquals(hex(expected["key"].asText()), diff.key)
                assertOptionalHex(expected["value"], diff.value)
                assertOptionalHex(expected["old"], diff.oldValue)
                assertOptionalHex(expected["new"], diff.newValue)
            }
        }
    }

    @Test
    fun codecFixturesRoundTrip() {
        ProllyNative.useLocalDebugLibrary()

        for (fixture in fixtures["value_fixtures"]) {
            val bytes = hex(fixture["bytes"].asText())
            val schema = fixture["schema_name"].asText()
            val version = fixture["version"].asLong().toULong()
            assertArrayEquals(bytes, versionedValueToBytes(versionedValueFromBytes(bytes)))
            assertTrue(versionedValueBytesMatchesSchema(bytes, schema, version))
            assertFalse(versionedValueBytesMatchesSchema(bytes, schema, version + 1UL))
            versionedValueBytesRequireSchema(bytes, schema, version)
            assertThrows<ProllyBindingException> {
                versionedValueBytesRequireSchema(bytes, schema, version + 1UL)
            }
        }
        for (fixture in fixtures["blob_fixtures"]) {
            val bytes = hex(fixture["bytes"].asText())
            assertArrayEquals(bytes, valueRefToBytes(valueRefFromBytes(bytes)))
            val expectedKind = if (fixture["kind"].asText() == "inline") ValueRefKind.INLINE else ValueRefKind.BLOB
            assertEquals(expectedKind, valueRefFromStoredBytes(bytes).kind)
            assertTrue(valueRefInlineRequiresEscape(bytes))
        }
        val plain = "plain".toByteArray()
        val rawInline = valueRefFromStoredBytes(plain)
        assertEquals(ValueRefKind.INLINE, rawInline.kind)
        assertArrayEquals(plain, rawInline.value ?: ByteArray(0))
        assertFalse(valueRefInlineRequiresEscape(plain))
        for (fixture in fixtures["manifest_fixtures"]) {
            val bytes = hex(fixture["bytes"].asText())
            assertArrayEquals(bytes, rootManifestToBytes(rootManifestFromBytes(bytes)))
        }
    }

    private fun buildTree(engine: ProllyEngine, entries: JsonNode): TreeRecord {
        var tree = engine.create()
        for (entry in entries) {
            tree = engine.put(tree, hex(entry["key"].asText()), hex(entry["value"].asText()))
        }
        return tree
    }

    private fun buildTree(engine: ProllyEngine, entries: List<Pair<String, String>>): TreeRecord {
        var tree = engine.create()
        for ((key, value) in entries) {
            tree = engine.put(tree, hex(key), hex(value))
        }
        return tree
    }

    private fun assertEntries(expected: JsonNode, actual: List<EntryRecord>) {
        assertEquals(expected.size(), actual.size)
        for ((index, entry) in actual.withIndex()) {
            assertArrayEquals(hex(expected[index]["key"].asText()), entry.key)
            assertArrayEquals(hex(expected[index]["value"].asText()), entry.value)
        }
    }

    private fun assertOptionalHex(expected: JsonNode, actual: ByteArray?) {
        if (expected.isNull) {
            assertNull(actual)
        } else {
            assertArrayEquals(hex(expected.asText()), actual)
        }
    }

    private fun configFromFixture(node: JsonNode): ConfigRecord {
        val encoding = node["encoding"]
        return ConfigRecord(
            node["min_chunk_size"].asText().toULong(),
            node["max_chunk_size"].asText().toULong(),
            node["chunking_factor"].asText().toUInt(),
            node["hash_seed"].asText().toULong(),
            EncodingRecord(
                when (encoding["kind"].asText()) {
                    "raw" -> EncodingKind.RAW
                    "cbor" -> EncodingKind.CBOR
                    "json" -> EncodingKind.JSON
                    "custom" -> EncodingKind.CUSTOM
                    else -> error("unknown encoding kind ${encoding["kind"].asText()}")
                },
                if (encoding["custom_name"].isNull) null else encoding["custom_name"].asText(),
            ),
            if (node["node_cache_max_nodes"].isNull) null else node["node_cache_max_nodes"].asText().toULong(),
            if (node["node_cache_max_bytes"].isNull) null else node["node_cache_max_bytes"].asText().toULong(),
        )
    }

    private fun fixturePath(): Path {
        val candidates = listOf(
            Paths.get("crates/prolly/conformance/prolly-fixtures.v1.json"),
            Paths.get("../../conformance/prolly-fixtures.v1.json"),
        ).map { it.toAbsolutePath().normalize() }
        return candidates.firstOrNull { Files.exists(it) }
            ?: error("could not locate prolly-fixtures.v1.json")
    }

    private fun hex(value: String): ByteArray {
        assertEquals(0, value.length % 2)
        return ByteArray(value.length / 2) { index ->
            value.substring(index * 2, index * 2 + 2).toInt(16).toByte()
        }
    }

    private fun ByteArray.hex(): String =
        joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
}
