package build.crab.prolly;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ProllyFixtureTest {
    private static final HexFormat HEX = HexFormat.of();
    private static JsonNode fixtures;

    @BeforeAll
    static void loadFixtures() throws Exception {
        Prolly.useLocalDebugLibrary();
        fixtures = new ObjectMapper().readTree(Files.readString(fixturePath()));
    }

    @Test
    void nodeFixturesDecodeEncodeAndHash() throws Exception {
        for (JsonNode fixture : fixtures.get("node_fixtures")) {
            byte[] bytes = hex(fixture.get("bytes").asText());
            assertArrayEquals(bytes, Prolly.nodeBytesRoundTrip(bytes));
            assertArrayEquals(hex(fixture.get("cid").asText()), Prolly.nodeCidFromBytes(bytes));
            assertArrayEquals(hex(fixture.get("cid").asText()), Prolly.cidFromBytes(bytes));
        }
    }

    @Test
    void boundaryAndKeyFixturesMatchRust() throws Exception {
        for (JsonNode fixture : fixtures.get("boundary_fixtures")) {
            assertEquals(
                    fixture.get("is_boundary").asBoolean(),
                    Prolly.isBoundaryConfig(
                            configFromFixture(fixture.get("config")),
                            fixture.get("count").asLong(),
                            hex(fixture.get("key").asText()),
                            hex(fixture.get("value").asText())));
        }

        for (JsonNode fixture : fixtures.get("key_fixtures").get("prefix_end")) {
            JsonNode expected = fixture.get("end");
            byte[] prefix = hex(fixture.get("prefix").asText());
            byte[] actual = Prolly.prefixEnd(prefix);
            if (expected.isNull()) {
                assertNull(actual);
            } else {
                assertArrayEquals(hex(expected.asText()), actual);
            }
            RangeBoundsRecord bounds = Prolly.prefixRange(prefix);
            assertArrayEquals(prefix, bounds.getStart());
            if (expected.isNull()) {
                assertNull(bounds.getEnd());
            } else {
                assertArrayEquals(hex(expected.asText()), bounds.getEnd());
            }
        }

        for (JsonNode fixture : fixtures.get("key_fixtures").get("numeric")) {
            byte[] actual = switch (fixture.get("kind").asText()) {
                case "u64" -> Prolly.u64Key(fixture.get("value").asText());
                case "u128" -> Prolly.u128Key(fixture.get("value").asText());
                case "i64" -> Prolly.i64Key(Long.parseLong(fixture.get("value").asText()));
                case "i128" -> Prolly.i128Key(fixture.get("value").asText());
                case "timestamp_millis" -> Prolly.timestampMillisKey(fixture.get("value").asText());
                default -> null;
            };
            if (actual != null) {
                assertArrayEquals(hex(fixture.get("encoded").asText()), actual);
            }
        }

        for (JsonNode fixture : fixtures.get("key_fixtures").get("segments")) {
            ByteArrayOutputStream encoded = new ByteArrayOutputStream();
            List<byte[]> segmentBytes = new ArrayList<>();
            for (JsonNode segment : fixture.get("segments")) {
                byte[] bytes = hex(segment.asText());
                segmentBytes.add(bytes);
                encoded.writeBytes(Prolly.encodeSegment(bytes));
            }
            assertArrayEquals(hex(fixture.get("encoded").asText()), encoded.toByteArray());
            assertArrayEquals(hex(fixture.get("encoded").asText()), Prolly.keyFromSegments(segmentBytes));
            byte[] prefix = Prolly.keyFromSegments(segmentBytes.subList(0, 1));
            assertArrayEquals(
                    hex(fixture.get("encoded").asText()),
                    Prolly.keyFromPrefixedSegments(prefix, segmentBytes.subList(1, segmentBytes.size())));
            List<byte[]> segments = Prolly.decodeSegments(hex(fixture.get("encoded").asText()));
            assertEquals(fixture.get("decoded").size(), segments.size());
            for (int i = 0; i < segments.size(); i++) {
                assertArrayEquals(hex(fixture.get("decoded").get(i).asText()), segments.get(i));
            }
        }

        for (JsonNode fixture : fixtures.get("key_fixtures").get("debug")) {
            assertEquals(fixture.get("debug").asText(), Prolly.debugKey(hex(fixture.get("key").asText())));
        }
    }

    @Test
    void treeAndDiffFixturesMatchRust() throws Exception {
        for (JsonNode fixture : fixtures.get("tree_fixtures")) {
            try (Prolly prolly = Prolly.memory(configFromFixture(fixture.get("config")))) {
                TreeRecord tree = buildTree(prolly, fixture.get("entries"));
                assertArrayEquals(hex(fixture.get("root").asText()), tree.getRoot());

                for (JsonNode lookup : fixture.get("lookups")) {
                    Optional<byte[]> actual = prolly.get(tree, hex(lookup.get("key").asText()));
                    if (lookup.get("value").isNull()) {
                        assertTrue(actual.isEmpty());
                    } else {
                        assertArrayEquals(hex(lookup.get("value").asText()), actual.orElseThrow());
                    }
                }

                for (JsonNode rangeFixture : fixture.get("ranges")) {
                    Optional<byte[]> end = rangeFixture.get("end").isNull()
                            ? Optional.empty()
                            : Optional.of(hex(rangeFixture.get("end").asText()));
                    assertEntries(rangeFixture.get("entries"), prolly.range(tree, hex(rangeFixture.get("start").asText()), end));
                }
            }
        }

        JsonNode diffFixture = fixtures.get("diff_fixtures").get(0);
        try (Prolly prolly = Prolly.memory(configFromFixture(diffFixture.get("config")))) {
            TreeRecord base = buildTree(
                    prolly,
                    new String[][] {
                        {"61", "31"},
                        {"62", "32"},
                        {"63", "33"}
                    });
            TreeRecord other = buildTree(
                    prolly,
                    new String[][] {
                        {"61", "31"},
                        {"62", "3232"},
                        {"64", "34"}
                    });
            assertArrayEquals(hex(diffFixture.get("base_root").asText()), base.getRoot());
            assertArrayEquals(hex(diffFixture.get("other_root").asText()), other.getRoot());

            List<DiffRecord> actual = prolly.diff(base, other);
            assertEquals(diffFixture.get("diffs").size(), actual.size());
            for (int i = 0; i < actual.size(); i++) {
                JsonNode expected = diffFixture.get("diffs").get(i);
                DiffRecord diff = actual.get(i);
                assertEquals(expected.get("kind").asText(), diff.getKind().name().toLowerCase(Locale.ROOT));
                assertArrayEquals(hex(expected.get("key").asText()), diff.getKey());
                assertOptionalHex(expected.get("value"), diff.getValue());
                assertOptionalHex(expected.get("old"), diff.getOldValue());
                assertOptionalHex(expected.get("new"), diff.getNewValue());
            }
        }
    }

    @Test
    void codecFixturesRoundTrip() throws Exception {
        for (JsonNode fixture : fixtures.get("value_fixtures")) {
            byte[] bytes = hex(fixture.get("bytes").asText());
            String schema = fixture.get("schema_name").asText();
            long version = fixture.get("version").asLong();
            assertArrayEquals(bytes, Prolly.versionedValueBytesRoundTrip(bytes));
            assertTrue(Prolly.versionedValueBytesMatchesSchema(bytes, schema, version));
            assertFalse(Prolly.versionedValueBytesMatchesSchema(bytes, schema, version + 1));
            Prolly.versionedValueBytesRequireSchema(bytes, schema, version);
            assertThrows(
                    ProllyBindingException.class,
                    () -> Prolly.versionedValueBytesRequireSchema(bytes, schema, version + 1));
        }
        for (JsonNode fixture : fixtures.get("blob_fixtures")) {
            byte[] bytes = hex(fixture.get("bytes").asText());
            assertArrayEquals(bytes, Prolly.valueRefBytesRoundTrip(bytes));
            ValueRef.Kind expectedKind =
                    fixture.get("kind").asText().equals("inline") ? ValueRef.Kind.INLINE : ValueRef.Kind.BLOB;
            assertEquals(expectedKind, Prolly.valueRefFromStoredBytes(bytes).kind());
            assertTrue(Prolly.valueRefInlineRequiresEscape(bytes));
        }
        byte[] plain = "plain".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ValueRef rawInline = Prolly.valueRefFromStoredBytes(plain);
        assertEquals(ValueRef.Kind.INLINE, rawInline.kind());
        assertArrayEquals(plain, rawInline.value().orElseThrow());
        assertFalse(Prolly.valueRefInlineRequiresEscape(plain));
        for (JsonNode fixture : fixtures.get("manifest_fixtures")) {
            byte[] bytes = hex(fixture.get("bytes").asText());
            assertArrayEquals(bytes, Prolly.rootManifestBytesRoundTrip(bytes));
        }
    }

    private static TreeRecord buildTree(Prolly prolly, JsonNode entries) throws Exception {
        TreeRecord tree = prolly.create();
        for (JsonNode entry : entries) {
            tree = prolly.put(tree, hex(entry.get("key").asText()), hex(entry.get("value").asText()));
        }
        return tree;
    }

    private static TreeRecord buildTree(Prolly prolly, String[][] entries) throws Exception {
        TreeRecord tree = prolly.create();
        for (String[] entry : entries) {
            tree = prolly.put(tree, hex(entry[0]), hex(entry[1]));
        }
        return tree;
    }

    private static void assertEntries(JsonNode expected, List<Entry> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < actual.size(); i++) {
            assertArrayEquals(hex(expected.get(i).get("key").asText()), actual.get(i).key());
            assertArrayEquals(hex(expected.get(i).get("value").asText()), actual.get(i).value());
        }
    }

    private static void assertOptionalHex(JsonNode expected, byte[] actual) {
        if (expected.isNull()) {
            assertNull(actual);
        } else {
            assertArrayEquals(hex(expected.asText()), actual);
        }
    }

    private static ConfigRecord configFromFixture(JsonNode fixture) {
        JsonNode encoding = fixture.get("encoding");
        return Prolly.config(
                fixture.get("min_chunk_size").asLong(),
                fixture.get("max_chunk_size").asLong(),
                fixture.get("chunking_factor").asInt(),
                fixture.get("hash_seed").asLong(),
                encoding.get("kind").asText(),
                encoding.get("custom_name").isNull() ? null : encoding.get("custom_name").asText(),
                fixture.get("node_cache_max_nodes").isNull() ? null : fixture.get("node_cache_max_nodes").asLong(),
                fixture.get("node_cache_max_bytes").isNull() ? null : fixture.get("node_cache_max_bytes").asLong());
    }

    private static Path fixturePath() {
        for (Path candidate : List.of(
                Paths.get("crates/prolly/conformance/prolly-fixtures.v1.json"),
                Paths.get("../../conformance/prolly-fixtures.v1.json"))) {
            Path normalized = candidate.toAbsolutePath().normalize();
            if (Files.exists(normalized)) {
                return normalized;
            }
        }
        throw new IllegalStateException("could not locate prolly-fixtures.v1.json");
    }

    private static byte[] hex(String value) {
        return HEX.parseHex(value);
    }
}
