package build.crab.prolly;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import build.crab.prolly.javaapi.Engine;
import build.crab.prolly.javaapi.CompositeAcceleratorConfig;
import build.crab.prolly.javaapi.HnswBuildLimits;
import build.crab.prolly.javaapi.HnswConfig;
import build.crab.prolly.javaapi.IndexEntry;
import build.crab.prolly.javaapi.IndexProjection;
import build.crab.prolly.javaapi.IndexedMutation;
import build.crab.prolly.javaapi.IndexedUpdateKind;
import build.crab.prolly.javaapi.MapMutation;
import build.crab.prolly.javaapi.MapEntry;
import build.crab.prolly.javaapi.MapUpdateKind;
import build.crab.prolly.javaapi.ParallelConfig;
import build.crab.prolly.javaapi.ProximityRecord;
import build.crab.prolly.javaapi.ProximityCancellationToken;
import build.crab.prolly.javaapi.ProximityMutation;
import build.crab.prolly.javaapi.ProductQuantizationConfig;
import build.crab.prolly.javaapi.Proofs;
import build.crab.prolly.javaapi.SearchRequest;
import build.crab.prolly.javaapi.ScopedBytes;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class PortableParityTest {
    @Test
    void retainedSearchRuntimeReusesValidatedContent() throws Exception {
        Prolly.useLocalDebugLibrary();
        var records = new ArrayList<ProximityRecord>();
        for (int index = 0; index < 16; index++) {
            records.add(new ProximityRecord(
                    bytes(String.format("vector-%02d", index)),
                    new float[] {index, 0},
                    bytes(String.format("value-%02d", index))));
        }
        try (Engine engine = Engine.memory();
             var proximity = engine.buildProximity(2, records);
             var index = proximity.buildHnsw().index();
             var runtime = engine.proximitySearchRuntime()) {
            var request = SearchRequest.fixedBudget(
                    new float[] {0, 0},
                    3,
                    SearchRequest.SearchBudget.unlimited(),
                    SearchRequest.SearchFilter.all(),
                    SearchRequest.Kernel.AUTO_DETERMINISTIC,
                    SearchRequest.Backend.HNSW);
            var cold = index.searchWithRuntime(proximity, request, runtime);
            assertTrue(cold.stats().physicalBytesRead() > 0);
            var coldStats = runtime.stats();
            assertTrue(coldStats.physicalReads() > 0);
            var warm = index.searchWithRuntime(proximity, request, runtime);
            assertEquals(0, warm.stats().physicalBytesRead());
            assertArrayEquals(cold.neighbors().get(0).key(), warm.neighbors().get(0).key());
            assertEquals(coldStats, runtime.stats());
            runtime.clear();
            assertTrue(index.searchWithRuntime(proximity, request, runtime)
                    .stats().physicalBytesRead() > 0);
        }
    }

    @Test
    void proximityFutureUsesNativeCooperativeCancellation() throws Exception {
        Prolly.useLocalDebugLibrary();
        var records = new ArrayList<ProximityRecord>();
        for (int index = 0; index < 256; index++) {
            records.add(new ProximityRecord(
                    bytes(String.format("vector-%04d", index)),
                    new float[] {index, index % 7},
                    bytes(Integer.toString(index))));
        }
        try (Engine engine = Engine.memory();
             var proximity = engine.buildProximity(2, records);
             var runtime = engine.proximitySearchRuntime();
             var cancellation = new ProximityCancellationToken()) {
            cancellation.cancel();
            var result = proximity.searchAsync(
                    SearchRequest.exact(new float[] {0, 0}, 10),
                    runtime,
                    cancellation).get();
            assertEquals("cancelled", result.completion());
            assertTrue(result.neighbors().isEmpty());
            try (var session = proximity.read()) {
                var sessionResult = session.searchAsync(
                        SearchRequest.exact(new float[] {0, 0}, 10),
                        runtime,
                        cancellation).get();
                assertEquals("cancelled", sessionResult.completion());
                assertTrue(sessionResult.neighbors().isEmpty());
            }
        }
    }

    @Test
    void productQuantizerLifecycleIsPortableAndBounded() throws Exception {
        Prolly.useLocalDebugLibrary();
        var records = new ArrayList<ProximityRecord>();
        for (int index = 0; index < 16; index++) {
            records.add(new ProximityRecord(
                    bytes(String.format("vector-%02d", index)),
                    new float[] {index, index % 3, 0, 1},
                    bytes(String.format("value-%02d", index))));
        }
        try (Engine engine = Engine.memory(); var proximity = engine.buildProximity(4, records)) {
            var config = new ProductQuantizationConfig(2, 4, 2, 4, -1L, 16);
            var built = proximity.buildPq(config, 2);
            assertEquals(16L, built.stats().encodedVectors());
            var request = SearchRequest.fixedBudget(
                    new float[] {0, 0, 0, 1},
                    3,
                    SearchRequest.SearchBudget.unlimited(),
                    SearchRequest.SearchFilter.all(),
                    SearchRequest.Kernel.AUTO_DETERMINISTIC,
                    SearchRequest.Backend.PRODUCT_QUANTIZED);
            byte[] manifest;
            try (var index = built.index()) {
                assertEquals(config, index.config());
                assertArrayEquals(proximity.descriptor(), index.sourceDescriptor());
                assertTrue(index.quality().meanSquaredError() >= 0.0);
                var result = index.search(proximity, request);
                assertEquals("product_quantized", result.backend());
                assertArrayEquals(bytes("vector-00"), result.neighbors().get(0).key());
                manifest = index.manifest();
                try (var proof = index.proveSearch(proximity, request)) {
                    assertEquals(
                            SearchBackendRecord.PRODUCT_QUANTIZED,
                            proof.verify(proximity.descriptor()).getResult().getBackend());
                }
            }
            try (var loaded = proximity.loadPq(manifest)) {
                assertArrayEquals(manifest, loaded.manifest());
            }
        }
    }

    @Test
    void hnswAcceleratorLifecycleIsPortable() throws Exception {
        Prolly.useLocalDebugLibrary();
        var records = new ArrayList<ProximityRecord>();
        for (int index = 0; index < 16; index++) {
            records.add(new ProximityRecord(
                    bytes(String.format("vector-%02d", index)),
                    new float[] {index, 0},
                    bytes(String.format("value-%02d", index))));
        }
        try (Engine engine = Engine.memory(); var proximity = engine.buildProximity(2, records)) {
            var defaults = HnswConfig.defaults();
            var fullWidthSeed = new HnswConfig(
                    defaults.maxConnections(),
                    defaults.efConstruction(),
                    defaults.efSearch(),
                    defaults.levelBits(),
                    defaults.overfetchMultiplier(),
                    -1L,
                    defaults.routingVectorEncoding());
            var built = proximity.buildHnsw(fullWidthSeed, HnswBuildLimits.defaults());
            assertEquals(16L, built.stats().records());
            var request = SearchRequest.fixedBudget(
                    new float[] {0, 0},
                    3,
                    SearchRequest.SearchBudget.unlimited(),
                    SearchRequest.SearchFilter.all(),
                    SearchRequest.Kernel.AUTO_DETERMINISTIC,
                    SearchRequest.Backend.HNSW);
            byte[] manifest;
            try (var index = built.index()) {
                assertEquals(-1L, index.config().seed(), "Java long preserves all Rust u64 seed bits");
                assertTrue(index.isCanonical());
                assertArrayEquals(proximity.descriptor(), index.sourceDescriptor());
                var result = index.search(proximity, request);
                assertEquals("hnsw", result.backend());
                assertArrayEquals(bytes("vector-00"), result.neighbors().get(0).key());
                try (var cancellation = new ProximityCancellationToken()) {
                    cancellation.cancel();
                    var cancelled = index.searchCancellable(
                            proximity, request, null, cancellation);
                    assertEquals("cancelled", cancelled.completion());
                    assertTrue(cancelled.neighbors().isEmpty());
                }
                manifest = index.manifest();
                try (var proof = index.proveSearch(proximity, request)) {
                    assertEquals(
                            SearchBackendRecord.HNSW,
                            proof.verify(proximity.descriptor()).getResult().getBackend());
                }
            }
            try (var loaded = proximity.loadHnsw(manifest)) {
                assertArrayEquals(manifest, loaded.manifest());
            }
        }
    }

    @Test
    void compositeAndCatalogLifecycleIsPortableAndBounded() throws Exception {
        Prolly.useLocalDebugLibrary();
        var records = new ArrayList<ProximityRecord>();
        for (int index = 0; index < 16; index++) {
            records.add(new ProximityRecord(
                    bytes(String.format("vector-%02d", index)),
                    new float[] {index, 0},
                    bytes(String.format("value-%02d", index))));
        }
        try (Engine engine = Engine.memory(); var baseMap = engine.buildProximity(2, records)) {
            var baseBuild = baseMap.buildHnsw();
            try (var base = baseBuild.index()) {
                var mutation = baseMap.mutate(List.of(ProximityMutation.upsert(
                        bytes("vector-00"), new float[] {0.25f, 0}, bytes("updated"))));
                try (var current = mutation.map()) {
                    var built = current.buildCompositeHnsw(baseMap, base);
                    assertTrue(built.reasons().isEmpty());
                    assertEquals(1L, built.stats().vectorUpdatedRecords());
                    var request = SearchRequest.fixedBudget(
                            new float[] {0, 0},
                            3,
                            SearchRequest.SearchBudget.unlimited(),
                            SearchRequest.SearchFilter.all(),
                            SearchRequest.Kernel.AUTO_DETERMINISTIC,
                            SearchRequest.Backend.COMPOSITE);
                    byte[] compositeManifest;
                    try (var composite = built.accelerator()) {
                        assertArrayEquals(current.descriptor(), composite.currentSourceDescriptor());
                        assertArrayEquals(baseMap.descriptor(), composite.baseSourceDescriptor());
                        assertEquals("HNSW", composite.baseKind());
                        assertEquals(1L, composite.deltaCount());
                        assertEquals(1L, composite.shadowCount());
                        assertEquals("composite", composite.search(current, request).backend());
                        try (var proof = composite.proveSearch(current, request)) {
                            assertEquals(
                                    SearchBackendRecord.COMPOSITE,
                                    proof.verify(current.descriptor()).getResult().getBackend());
                        }
                        compositeManifest = composite.manifest();
                        try (var catalog = current.buildAcceleratorCatalog(null, null, composite)) {
                            assertEquals(1, catalog.entries().size());
                            assertEquals("composite", catalog.search(current, request).backend());
                            var catalogManifest = catalog.manifest();
                            try (var loaded = current.loadAcceleratorCatalog(catalogManifest)) {
                                assertArrayEquals(catalogManifest, loaded.manifest());
                            }
                        }
                    }
                    try (var loaded = current.loadComposite(compositeManifest)) {
                        assertArrayEquals(compositeManifest, loaded.manifest());
                    }

                    var defaults = CompositeAcceleratorConfig.defaults();
                    var forced = new CompositeAcceleratorConfig(
                            0,
                            defaults.maxShadowRecords(),
                            defaults.maxDeltaRatioPpm(),
                            defaults.maxShadowRatioPpm(),
                            defaults.baseOverfetchMultiplier());
                    var rebuilt = current.buildOrRebuildCompositeHnsw(baseMap, base, forced);
                    assertEquals(
                            build.crab.prolly.javaapi.CompositeBuildOrRebuildOutcome.Kind.HNSW_REBUILT,
                            rebuilt.kind());
                    assertFalse(rebuilt.reasons().isEmpty());
                    try (var rebuiltIndex = rebuilt.hnsw()) {
                        assertArrayEquals(current.descriptor(), rebuiltIndex.sourceDescriptor());
                    }
                }
            }
        }
    }

    @Test
    void richProximitySearchPreservesPolicyFilterStatsSessionAndProof() throws Exception {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory();
             var proximity = engine.buildProximity(2, List.of(
                     new ProximityRecord(bytes("a"), new float[] {0, 0}, bytes("alpha")),
                     new ProximityRecord(bytes("ab"), new float[] {1, 0}, bytes("alphabet")),
                     new ProximityRecord(bytes("b"), new float[] {0.1f, 0}, bytes("beta"))))) {
            var request = SearchRequest.fixedBudget(
                    new float[] {0, 0},
                    3,
                    new SearchRequest.SearchBudget(1_000L, 1_000_000L, 1_000L, 1_000L),
                    SearchRequest.SearchFilter.prefix(bytes("a")),
                    SearchRequest.Kernel.SCALAR_DETERMINISTIC,
                    SearchRequest.Backend.AUTO);

            var result = proximity.search(request);
            assertEquals(List.of("a", "ab"), result.neighbors().stream()
                    .map(neighbor -> new String(neighbor.key(), StandardCharsets.UTF_8)).toList());
            assertTrue(result.stats().distanceEvaluations() > 0);
            assertTrue(result.planFormatVersion() > 0);
            var scanned = new ArrayList<String>();
            assertEquals(2, proximity.scanRecords(record -> {
                scanned.add(new String(record.key(), StandardCharsets.UTF_8));
                return scanned.size() < 2;
            }));
            assertEquals(List.of("a", "ab"), scanned);
            try (var session = proximity.read()) {
                assertEquals(List.of("a", "ab"), session.search(request).neighbors().stream()
                        .map(neighbor -> new String(neighbor.key(), StandardCharsets.UTF_8)).toList());
                var retained = new ArrayList<String>();
                assertEquals(3, session.scanRecords(record -> {
                    retained.add(new String(record.key(), StandardCharsets.UTF_8));
                    return true;
                }));
                assertEquals(List.of("a", "ab", "b"), retained);
            }
            try (var proof = proximity.proveSearch(request)) {
                assertEquals(List.of("a", "ab"), proof.verify(proximity.descriptor())
                        .getResult().getNeighbors().stream()
                        .map(neighbor -> new String(neighbor.getKey(), StandardCharsets.UTF_8)).toList());
            }
        }
    }

    @Test
    void versionedBulkPublicationUsesNativePerformancePaths() {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var map = engine.versionedMap(bytes("bulk-publication"))) {
            var initialized = map.initializeSorted(List.of(
                    new MapEntry(bytes("a"), bytes("one")),
                    new MapEntry(bytes("b"), bytes("two"))));
            assertEquals(MapUpdateKind.APPLIED, initialized.kind());
            map.append(List.of(MapMutation.upsert(bytes("c"), bytes("three"))));
            var parallel = map.parallelApply(List.of(
                    MapMutation.upsert(bytes("b"), bytes("updated")),
                    MapMutation.upsert(bytes("d"), bytes("four"))), new ParallelConfig(1, 1));
            assertEquals(2L, parallel.stats().inputMutations());
            var rebuilt = map.rebuildSortedIf(parallel.version().id(), List.of(
                    new MapEntry(bytes("x"), bytes("nine")),
                    new MapEntry(bytes("y"), bytes("ten"))));
            assertEquals(MapUpdateKind.APPLIED, rebuilt.kind());
            var iterRebuilt = map.rebuildFromEntriesIf(rebuilt.current().id(), List.of(
                    new MapEntry(bytes("q"), bytes("queue")),
                    new MapEntry(bytes("p"), bytes("priority"))));
            assertEquals(MapUpdateKind.APPLIED, iterRebuilt.kind());
            assertArrayEquals(bytes("priority"), map.get(bytes("p")).orElseThrow());
        }
    }

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
                        byte[] key = page.rows().get(0).primaryKey().copy();
                        assertArrayEquals(bytes("u1"), key);
                        byte[] secondKey = page.rows().get(1).primaryKey().copy();
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
            versioned.put(bytes("ka"), bytes("v2"));
            try (var snapshot = versioned.snapshot(); var session = snapshot.read()) {
                var verified = Proofs.verify(snapshot.proveKey(bytes("k")));
                assertTrue(verified.getValid());
                assertArrayEquals(bytes("v"), verified.getValue());
                var multi = Proofs.verify(snapshot.proveKeys(List.of(bytes("k"), bytes("missing"))));
                assertEquals(List.of(true, false), multi.getResults().stream().map(result -> result.getExists()).toList());
                assertEquals(List.of("k", "ka"), Proofs.verify(snapshot.proveRange(bytes("k"), bytes("l"))).getEntries().stream().map(entry -> new String(entry.getKey())).toList());
                assertEquals(List.of("k", "ka"), Proofs.verify(snapshot.provePrefix(bytes("k"))).getEntries().stream().map(entry -> new String(entry.getKey())).toList());
                var provedPage = snapshot.proveRangePage(null, bytes("l"), 1);
                assertTrue(Proofs.verify(provedPage.getProof()).getValid());
                assertEquals(List.of("k"), provedPage.getPage().getEntries().stream().map(entry -> new String(entry.getKey())).toList());
                assertEquals(2, snapshot.entryCount());
                assertFalse(snapshot.export().getNodes().isEmpty());
                assertArrayEquals(bytes("v"), session.get(bytes("k")).orElseThrow());
                var escaped = new AtomicReference<ScopedBytes>();
                var seen = new ArrayList<String>();
                var scan = session.scanRangeView(bytes("k"), bytes("l"), entry -> {
                    escaped.compareAndSet(null, entry.key());
                    seen.add(new String(entry.key().copy()) + "=" + new String(entry.value().copy()));
                    return true;
                });
                assertEquals(2, scan.visited());
                assertFalse(scan.stopped());
                assertEquals(List.of("k=v", "ka=v2"), seen);
                assertThrows(IllegalStateException.class, () -> escaped.get().copy());
                assertEquals(
                        new build.crab.prolly.javaapi.ReadScanOutcome(1, true),
                        session.scanRangeView(bytes("k"), bytes("l"), entry -> false));
            }
            assertTrue(versioned.catalogVersionCount() >= 2);
            assertFalse(versioned.backup().length == 0);
            assertFalse(versioned.planGc().reachability().liveCids().isEmpty());

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

    @Test
    void versionedComparisonsPinVersionsAndPageDiffs() {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var map = engine.versionedMap(bytes("comparison"))) {
            var base = map.initialize();
            var target = map.put(bytes("k"), bytes("v"));
            try (var comparison = map.compare(base.id(), target.id())) {
                assertArrayEquals(base.id(), comparison.base().id());
                assertArrayEquals(target.id(), comparison.target().id());
                assertEquals(List.of("k"), comparison.diff().stream().map(diff -> new String(diff.getKey())).toList());
                assertEquals(List.of("k"), comparison.diffPage(1).getDiffs().stream().map(diff -> new String(diff.getKey())).toList());
            }
        }
    }

    @Test
    void versionedHistoryNavigationDiffAndRollbackStayNative() {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var map = engine.versionedMap(bytes("history-navigation"))) {
            map.initialize();
            map.put(bytes("a"), bytes("one"));
            map.put(bytes("ab"), bytes("two"));
            var base = map.put(bytes("b"), bytes("three"));
            var target = map.put(bytes("a"), bytes("updated"));

            assertEquals(List.of("a", "ab", "b"), map.range(bytes("a"), bytes("c")).stream().map(row -> new String(row.getKey())).toList());
            assertEquals(List.of("a", "ab"), map.prefix(bytes("a")).stream().map(row -> new String(row.getKey())).toList());
            assertArrayEquals(bytes("one"), map.rangeAt(base.id(), bytes("a"), bytes("b")).get(0).getValue());
            assertEquals(List.of("a", "ab"), map.prefixAt(base.id(), bytes("a")).stream().map(row -> new String(row.getKey())).toList());
            assertEquals(List.of("a", "ab"), map.rangePage(null, null, 2).getEntries().stream().map(row -> new String(row.getKey())).toList());
            assertEquals(List.of("a"), map.prefixPage(bytes("a"), null, 1).getEntries().stream().map(row -> new String(row.getKey())).toList());
            var historicalPage = map.prefixPageAt(base.id(), bytes("a"), null, 1);
            assertEquals(List.of("a"), historicalPage.getEntries().stream().map(row -> new String(row.getKey())).toList());
            assertNotNull(historicalPage.getNextCursor());
            assertEquals(List.of("a"), map.diff(base.id(), target.id()).stream().map(row -> new String(row.getKey())).toList());
            assertEquals(List.of("a"), map.changesSince(base.id()).stream().map(row -> new String(row.getKey())).toList());

            var rolledBack = map.rollbackTo(base.id());
            assertArrayEquals(rolledBack.id(), map.headId().orElseThrow());
            assertArrayEquals(bytes("one"), map.get(bytes("a")).orElseThrow());
            assertTrue(map.changesSince(base.id()).isEmpty());
        }
    }

    @Test
    void versionedTimestampedWritesExposeCompleteMaintenanceAndRetentionRecords() {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var map = engine.versionedMap(bytes("maintenance-complete"))) {
            var first = map.applyAtMillis(List.of(MapMutation.upsert(bytes("k"), bytes("one"))), 1_000);
            var second = map.applyIfAtMillis(first.id(), List.of(MapMutation.upsert(bytes("k"), bytes("two"))), 2_000).current();
            var third = map.applyAtMillis(List.of(MapMutation.upsert(bytes("k"), bytes("three"))), 3_000);

        assertEquals(1_000L, first.createdAtMillis().orElseThrow());
        assertEquals(2_000L, second.createdAtMillis().orElseThrow());
            assertEquals(NamedRootRetentionKind.PREFIX, map.retentionPolicy().getKind());
            var verification = map.verifyCatalog();
            assertArrayEquals(third.id(), verification.getHead());
            assertEquals(3, map.catalogVersionCount());
            var plan = map.planGc();
            assertTrue(plan.reachability().liveNodes() > 0);
            assertTrue(plan.candidateNodes() >= plan.reclaimableNodes());

            var aged = map.keepForAt(3_000, 1_500);
            assertTrue(aged.retained().stream().anyMatch(id -> java.util.Arrays.equals(id, second.id())));
            assertTrue(aged.removed().stream().anyMatch(id -> java.util.Arrays.equals(id, first.id())));
            var explicit = map.keepVersions(List.of(second.id()));
            assertTrue(explicit.retained().stream().anyMatch(id -> java.util.Arrays.equals(id, third.id())));
            var pruned = map.pruneVersions(0);
            assertEquals(1, pruned.retained().size());
            assertArrayEquals(third.id(), pruned.retained().get(0));
            assertTrue(pruned.removed().stream().anyMatch(id -> java.util.Arrays.equals(id, second.id())));
            assertFalse(map.keepFor(10_000).retained().isEmpty());
            assertTrue(map.sweepGc().deletedNodes() >= 0);
        }
    }

    @Test
    void versionedSubscriptionsResumeAndPollOwnedDiffs() {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var map = engine.versionedMap(bytes("subscription"))) {
            var initial = map.initialize();
            try (var subscription = map.subscribe()) {
                assertArrayEquals(initial.id(), subscription.lastSeen().orElseThrow());
                assertTrue(subscription.poll().isEmpty());
                var current = map.put(bytes("k"), bytes("v"));
                var event = subscription.poll().orElseThrow();
                assertArrayEquals(initial.id(), event.getPrevious());
                assertArrayEquals(current.id(), event.getCurrent().getId());
                assertEquals(List.of("k"), event.getDiffs().stream().map(diff -> new String(diff.getKey())).toList());
                assertArrayEquals(current.id(), subscription.lastSeen().orElseThrow());
            }
        }
    }

    @Test
    void multiMapTransactionsAreAtomicAndReadStagedValues() {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory()) {
            try (var tx = engine.beginVersionedTransaction()) {
                tx.put(bytes("a"), bytes("k"), bytes("one"));
                tx.put(bytes("b"), bytes("k"), bytes("two"));
                assertArrayEquals(bytes("one"), tx.get(bytes("a"), bytes("k")).orElseThrow());
                var committed = tx.commit();
                assertTrue(committed.applied());
                assertEquals(2, committed.versions().size());
            }
            try (var a = engine.versionedMap(bytes("a")); var b = engine.versionedMap(bytes("b"))) {
                assertArrayEquals(bytes("one"), a.get(bytes("k")).orElseThrow());
                assertArrayEquals(bytes("two"), b.get(bytes("k")).orElseThrow());
            }
            try (var tx = engine.beginVersionedTransaction()) {
                tx.put(bytes("a"), bytes("discard"), bytes("x"));
                tx.rollback();
            }
            try (var a = engine.versionedMap(bytes("a"))) { assertTrue(a.get(bytes("discard")).isEmpty()); }
        }
    }

    @Test
    void pinnedMergesPageConflictsAndCasPublish() {
        Prolly.useLocalDebugLibrary();
        try (Engine engine = Engine.memory(); var map = engine.versionedMap(bytes("merge"))) {
            var base = map.initialize();
            var candidate = map.put(bytes("k"), bytes("candidate"));
            map.put(bytes("k"), bytes("head"));
            try (var merge = map.prepareMerge(base.id(), candidate.id())) {
                assertArrayEquals(base.id(), merge.base().id());
                assertArrayEquals(candidate.id(), merge.candidate().id());
                assertEquals(List.of("k"), merge.conflictPage(null, 1).getConflicts().stream().map(row -> new String(row.getKey())).toList());
                assertArrayEquals(candidate.id(), merge.publish("prefer_right").current().id());
            }
            assertArrayEquals(bytes("candidate"), map.get(bytes("k")).orElseThrow());
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
