package build.crab.prolly;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import build.crab.prolly.javaapi.Engine;
import build.crab.prolly.javaapi.IndexEntry;
import build.crab.prolly.javaapi.IndexProjection;
import build.crab.prolly.javaapi.ProximityRecord;
import build.crab.prolly.javaapi.ProximityMutation;
import build.crab.prolly.javaapi.Proofs;
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
                assertTrue(session.contains(bytes("a")));
                assertArrayEquals(bytes("alpha"), session.get(bytes("a")).getValue());
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

    @Test
    void proofsSessionsAndMaintenanceAreApplicationFacing() throws Exception {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var versioned = engine.versionedMap(bytes("proofs"))) {
            versioned.initialize();
            versioned.put(bytes("k"), bytes("v"));
            try (var snapshot = versioned.snapshot(); var session = snapshot.read()) {
                var verified = Proofs.verify(snapshot.proveKey(bytes("k")));
                assertTrue(verified.getValid());
                assertArrayEquals(bytes("v"), verified.getValue());
                assertEquals(1, snapshot.entryCount());
                assertFalse(snapshot.export().getNodes().isEmpty());
                assertArrayEquals(bytes("v"), session.get(bytes("k")).orElseThrow());
            }
            assertTrue(versioned.catalogVersionCount() >= 2);
            assertFalse(versioned.backup().length == 0);
            assertFalse(versioned.planGc().getReachability().getLiveCids().isEmpty());

            try (var registry = engine.indexRegistry()) {
                registry.register(bytes("by_value"), 1, "value-v1", IndexProjection.ALL,
                        (key, value) -> List.of(new IndexEntry(value, null)));
                try (var indexed = engine.indexedMap(bytes("indexed-maintenance"), registry)) {
                    var version = indexed.put(bytes("k"), bytes("term"));
                    indexed.ensureIndex(bytes("by_value"));
                    assertTrue(indexed.verifyIndex(bytes("by_value"), version.sourceVersion()).getValid());
                    assertTrue(indexed.buildAttempts() >= 1);
                    assertFalse(indexed.exportCurrent().length == 0);
                    assertFalse(indexed.keepLast(1).getRetainedSourceVersions().isEmpty());
                }
            }

            try (var proximity = engine.buildProximity(2,
                    List.of(new ProximityRecord(bytes("p"), new float[] {0, 0}, bytes("payload"))))) {
                var membership = Proofs.verify(
                        proximity.proveMembership(bytes("p")), proximity.descriptor());
                assertArrayEquals(bytes("payload"), membership.getRecord().getValue());
            assertEquals(1, proximity.verifiedRecordCount());
            assertEquals(1, proximity.count());
            assertTrue(proximity.contains(bytes("p")));
            assertEquals(2, proximity.config().dimensions());
            assertEquals(1, Proofs.verify(
                    proximity.proveStructure(), proximity.descriptor())
                    .summary().recordCount());
            var mutation = proximity.mutate(List.of(ProximityMutation.upsert(
                    bytes("q"), new float[]{1, 1}, bytes("second"))));
            assertEquals(2, mutation.map().count());
            assertTrue(mutation.stats().recordsRebuilt() >= 1);
                try (var searchProof = proximity.proveSearch(
                        SearchRequest.exact(new float[] {0, 0}, 1))) {
                    var verifiedSearch = searchProof.verify(proximity.descriptor());
                    assertArrayEquals(bytes("p"),
                            verifiedSearch.getResult().getNeighbors().get(0).getKey());
                    assertTrue(searchProof.replayedEvents(verifiedSearch) > 0);
                }
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
