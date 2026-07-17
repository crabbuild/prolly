package build.crab.prolly;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import build.crab.prolly.javaapi.Engine;
import build.crab.prolly.javaapi.IndexEntry;
import build.crab.prolly.javaapi.IndexProjection;
import build.crab.prolly.javaapi.IndexedMutation;
import build.crab.prolly.javaapi.IndexedUpdateKind;
import build.crab.prolly.javaapi.MapMutation;
import build.crab.prolly.javaapi.MapUpdateKind;
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
    void versionedSnapshotsExposeOrderedNavigationAndBoundedPages() {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var map = engine.versionedMap(bytes("versioned-ordered"))) {
            map.initialize();
            map.apply(List.of(
                    MapMutation.upsert(bytes("a"), bytes("one")),
                    MapMutation.upsert(bytes("ab"), bytes("two")),
                    MapMutation.upsert(bytes("b"), bytes("three")),
                    MapMutation.upsert(bytes("c"), bytes("four"))));
            try (var snapshot = map.snapshot()) {
                assertTrue(snapshot.containsKey(bytes("ab")));
                assertArrayEquals(bytes("one"), snapshot.getMany(List.of(bytes("a"), bytes("missing"))).get(0).orElseThrow());
                assertArrayEquals(bytes("a"), snapshot.firstEntry().orElseThrow().getKey());
                assertArrayEquals(bytes("c"), snapshot.lastEntry().orElseThrow().getKey());
                assertArrayEquals(bytes("ab"), snapshot.lowerBound(bytes("aa")).orElseThrow().getKey());
                assertArrayEquals(bytes("b"), snapshot.upperBound(bytes("ab")).orElseThrow().getKey());
                assertEquals(List.of("a", "ab"), snapshot.prefix(bytes("a")).stream().map(entry -> new String(entry.getKey())).toList());
                assertEquals(List.of("ab", "b"), snapshot.range(bytes("ab"), bytes("c")).stream().map(entry -> new String(entry.getKey())).toList());
                var prefixPage = snapshot.prefixPage(bytes("a"), null, 1);
                assertEquals(List.of("a"), prefixPage.getEntries().stream().map(entry -> new String(entry.getKey())).toList());
                assertNotNull(prefixPage.getNextCursor());
                var first = snapshot.rangePage(null, bytes("c"), 2);
                assertEquals(List.of("a", "ab"), first.getEntries().stream().map(entry -> new String(entry.getKey())).toList());
                var second = snapshot.rangePage(first.getNextCursor(), bytes("c"), 2);
                assertEquals(List.of("b"), second.getEntries().stream().map(entry -> new String(entry.getKey())).toList());
                assertEquals(List.of("c", "b"), snapshot.reversePage(null, bytes("a"), 2).getEntries().stream().map(entry -> new String(entry.getKey())).toList());
                assertEquals(List.of("ab", "a"), snapshot.prefixReversePage(bytes("a"), null, 2).getEntries().stream().map(entry -> new String(entry.getKey())).toList());
            }
        }
    }

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
                    assertTrue(indexed.verifyIndex(bytes("by_value"), version.sourceVersion()).valid());
                    assertTrue(indexed.buildAttempts() >= 1);
                    assertFalse(indexed.exportCurrent().length == 0);
                    assertFalse(indexed.keepLast(1).retainedSourceVersions().isEmpty());
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

    @Test
    void indexedBatchCasAndHistoricalSnapshotsAreJavaNative() throws Exception {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var registry = engine.indexRegistry()) {
            registry.register(bytes("by_value"), 1, "value-v1", IndexProjection.ALL,
                    (key, value) -> List.of(new IndexEntry(value, null)));
            try (var indexed = engine.indexedMap(bytes("indexed-lifecycle"), registry)) {
                assertArrayEquals(bytes("indexed-lifecycle"), indexed.id());
                var first = indexed.apply(List.of(
                        IndexedMutation.upsert(bytes("u1"), bytes("red")),
                        IndexedMutation.upsert(bytes("u2"), bytes("red"))));
                indexed.ensureIndex(bytes("by_value"));
                try (var firstSnapshot = indexed.snapshot()) {
                    var firstId = firstSnapshot.id();
                    assertArrayEquals(first.sourceVersion(), firstId.sourceVersion());
                    var applied = indexed.applyIf(first.sourceVersion(), List.of(
                            IndexedMutation.upsert(bytes("u3"), bytes("blue"))));
                    assertEquals(IndexedUpdateKind.APPLIED, applied.kind());
                    assertTrue(applied.current().isPresent());
                    var conflict = indexed.applyIf(first.sourceVersion(), List.of(
                            IndexedMutation.delete(bytes("u1"))));
                    assertEquals(IndexedUpdateKind.CONFLICT, conflict.kind());
                    try (var historical = indexed.snapshotAt(first.sourceVersion());
                         var reopened = indexed.snapshotById(firstId)) {
                        assertArrayEquals(firstId.sourceVersion(), historical.id().sourceVersion());
                        assertArrayEquals(firstId.catalogVersion(), reopened.id().catalogVersion());
                    }
                }
            }
        }
    }

    @Test
    void indexedMaintenanceAndOwnedPagesExposeCompleteJavaRecords() throws Exception {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var registry = engine.indexRegistry()) {
            registry.register(bytes("by_value"), 1, "value-v1", IndexProjection.ALL,
                    (key, value) -> List.of(new IndexEntry(value, null)));
            try (var indexed = engine.indexedMap(bytes("indexed-records"), registry)) {
                var version = indexed.apply(List.of(
                        IndexedMutation.upsert(bytes("u1"), bytes("red")),
                        IndexedMutation.upsert(bytes("u2"), bytes("red")),
                        IndexedMutation.upsert(bytes("u3"), bytes("rose"))));
                indexed.ensureIndex(bytes("by_value"));
                assertArrayEquals(bytes("indexed-records"), indexed.health().sourceMapId());
                assertEquals(1, indexed.health().activeIndexes().size());
                assertTrue(indexed.repairIndex(bytes("by_value"), version.sourceVersion()).valid());
                assertTrue(indexed.metrics().buildAttempts() >= 1);

                try (var snapshot = indexed.snapshot(); var index = snapshot.index(bytes("by_value"))) {
                    assertArrayEquals(bytes("by_value"), index.name());
                    assertEquals(2, index.exact(bytes("red")).size());
                    assertEquals(3, index.prefix(bytes("r")).size());
                    assertEquals(3, index.range(bytes("red"), bytes("s")).size());
                    assertArrayEquals(bytes("u1"), index.exactPage(bytes("red"), null, 1).matches().get(0).primaryKey());
                    assertArrayEquals(bytes("u2"), index.exactReversePage(bytes("red"), null, 1).matches().get(0).primaryKey());
                    assertArrayEquals(bytes("u1"), index.prefixPage(bytes("r"), null, 1).matches().get(0).primaryKey());
                    assertArrayEquals(bytes("u3"), index.prefixReversePage(bytes("r"), null, 1).matches().get(0).primaryKey());
                    assertArrayEquals(bytes("u1"), index.rangePage(bytes("red"), bytes("s"), null, 1).matches().get(0).primaryKey());
                    assertArrayEquals(bytes("u3"), index.rangeReversePage(bytes("red"), bytes("s"), null, 1).matches().get(0).primaryKey());
                }

                byte[] bundle = indexed.exportCurrent();
                var next = indexed.put(bytes("u4"), bytes("blue"));
                assertArrayEquals(version.sourceVersion(), indexed.importCurrent(bundle, next.sourceVersion()).sourceVersion());
                assertFalse(indexed.keepLast(1).retainedSourceVersions().isEmpty());
                indexed.deactivateIndex(bytes("by_value"));
                assertTrue(indexed.health().activeIndexes().isEmpty());
            }
        }
    }

    @Test
    void indexedCompletableFutureOwnsBatchInputsBeforeScheduling() throws Exception {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var registry = engine.indexRegistry()) {
            registry.register(bytes("by_value"), 1, "value-v1", IndexProjection.ALL,
                    (key, value) -> List.of(new IndexEntry(value, null)));
            try (var indexed = engine.indexedMap(bytes("indexed-async"), registry)) {
                var mutation = IndexedMutation.upsert(bytes("original-key"), bytes("original-value"));
                var future = indexed.applyAsync(List.of(mutation));
                mutation.key()[0] = 'x';
                mutation.value()[0] = 'x';
                future.get();
                assertArrayEquals(bytes("original-value"), indexed.get(bytes("original-key")).orElseThrow());
            }
        }
    }

    @Test
    void versionedMapExposesIdentityAndHistoricalSnapshotLifecycle() {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var map = engine.versionedMap(bytes("versioned-lifecycle"))) {
            assertArrayEquals(bytes("versioned-lifecycle"), map.id());
            assertFalse(map.isInitialized());
            var initial = map.initialize();
            assertTrue(map.isInitialized());
            assertArrayEquals(initial.id(), map.headId().orElseThrow());
            var first = map.put(bytes("k"), bytes("v1"));
            map.put(bytes("k"), bytes("v2"));
            assertArrayEquals(map.head().orElseThrow().id(), map.headId().orElseThrow());
            assertArrayEquals(first.id(), map.version(first.id()).orElseThrow().id());
            assertTrue(map.versions().size() >= 3);
            try (var historical = map.snapshotAt(first.id())) {
                assertArrayEquals(first.id(), historical.id());
                assertArrayEquals(first.id(), historical.version().id());
                assertArrayEquals(bytes("v1"), historical.get(bytes("k")).orElseThrow());
            }
        }
    }

    @Test
    void versionedMapExposesOwnedBatchCasAndVersionPinnedPointReads() {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var map = engine.versionedMap(bytes("versioned-cas"))) {
            map.initialize();
            var first = map.apply(List.of(
                    MapMutation.upsert(bytes("a"), bytes("one")),
                    MapMutation.upsert(bytes("b"), bytes("two"))));
            assertTrue(map.containsKey(bytes("a")));
            assertArrayEquals(bytes("one"), map.getMany(List.of(bytes("a"), bytes("missing"))).get(0).orElseThrow());
            assertTrue(map.getMany(List.of(bytes("missing"))).get(0).isEmpty());
            var applied = map.putIf(first.id(), bytes("a"), bytes("updated"));
            assertEquals(MapUpdateKind.APPLIED, applied.kind());
            assertEquals(MapUpdateKind.CONFLICT, map.deleteIf(first.id(), bytes("b")).kind());
            var historical = map.getManyAt(first.id(), List.of(bytes("a"), bytes("b")));
            assertArrayEquals(bytes("one"), historical.get(0).orElseThrow());
            assertArrayEquals(bytes("two"), historical.get(1).orElseThrow());
            assertArrayEquals(bytes("one"), map.getAt(first.id(), bytes("a")).orElseThrow());
            assertEquals(MapUpdateKind.APPLIED,
                    map.applyIf(applied.current().id(), List.of(MapMutation.delete(bytes("b")))).kind());
        }
    }

    @Test
    void versionedBackupRestoresAndRetentionReturnsCompleteVersionSets() {
        Prolly.useLocalDebugLibrary();
        try (Engine sourceEngine = Engine.memory(); Engine targetEngine = Engine.memory();
                var source = sourceEngine.versionedMap(bytes("versioned-backup"));
                var target = targetEngine.versionedMap(bytes("versioned-backup"))) {
            source.initialize();
            source.put(bytes("k"), bytes("v1"));
            source.put(bytes("k"), bytes("v2"));
            var restored = target.restoreBackup(source.backup());
            assertArrayEquals(source.headId().orElseThrow(), restored.id());
            assertArrayEquals(bytes("v2"), target.get(bytes("k")).orElseThrow());
            var pruned = source.keepLast(1);
            assertFalse(pruned.retained().isEmpty());
            assertFalse(pruned.removed().isEmpty());
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
