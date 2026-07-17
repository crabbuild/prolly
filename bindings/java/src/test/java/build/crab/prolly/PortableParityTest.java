package build.crab.prolly;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import build.crab.prolly.javaapi.Engine;
import build.crab.prolly.javaapi.IndexEntry;
import build.crab.prolly.javaapi.IndexProjection;
import build.crab.prolly.javaapi.ProximityRecord;
import build.crab.prolly.javaapi.SearchRequest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class PortableParityTest {
    @Test
    void versionedIndexedAndProximityParity() throws Exception {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory()) {
            try (var versioned = engine.versionedMap(bytes("users"))) {
                versioned.initialize();
                versioned.put(bytes("u1"), bytes("Ada"));
                assertArrayEquals(bytes("Ada"), versioned.get(bytes("u1")).orElseThrow());
            }

            try (var registry = engine.indexRegistry()) {
                registry.register(bytes("by_team"), 1, "team-v1", IndexProjection.ALL,
                        (key, value) -> List.of(new IndexEntry(value, null)));
                try (var indexed = engine.indexedMap(bytes("members"), registry)) {
                    indexed.put(bytes("u1"), bytes("red"));
                    indexed.put(bytes("u2"), bytes("red"));
                    indexed.ensureIndex(bytes("by_team"));
                    try (var snapshot = indexed.snapshot();
                         var index = snapshot.index(bytes("by_team"));
                         var page = index.exactPage(bytes("red"), 2)) {
                        assertEquals(2, page.rows().size());
                        ByteBuffer primaryKey = page.rows().get(0).primaryKey();
                        byte[] key = new byte[primaryKey.remaining()];
                        primaryKey.get(key);
                        assertArrayEquals(bytes("u1"), key);
                        ByteBuffer secondPrimaryKey = page.rows().get(1).primaryKey();
                        byte[] secondKey = new byte[secondPrimaryKey.remaining()];
                        secondPrimaryKey.get(secondKey);
                        assertArrayEquals(bytes("u2"), secondKey);
                    }
                }
            }

            try (var proximity = engine.buildProximity(2,
                    List.of(new ProximityRecord(bytes("a"), new float[] {0, 0}, bytes("alpha"))));
                 var session = proximity.read()) {
                var result = session.search(SearchRequest.exact(new float[] {0.1f, 0.1f}, 1));
                assertArrayEquals(bytes("a"), result.neighbors().get(0).key());
            }
        }
    }

    @Test
    void completableFutureOwnsMutableInputsBeforeScheduling() throws Exception {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var map = engine.versionedMap(bytes("async"))) {
            map.initialize();
            byte[] key = bytes("original-key");
            byte[] value = bytes("original-value");
            var future = map.putAsync(key, value);
            key[0] = 'x';
            value[0] = 'x';
            future.get();
            assertArrayEquals(bytes("original-value"), map.get(bytes("original-key")).orElseThrow());
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
