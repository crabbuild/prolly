package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

public final class ProximityMap implements AutoCloseable {
    private build.crab.prolly.api.ProximityMap nativeMap;
    ProximityMap(build.crab.prolly.api.ProximityMap nativeMap) { this.nativeMap = nativeMap; }
    build.crab.prolly.api.ProximityMap open() {
        if (nativeMap == null) throw new IllegalStateException("proximity map is closed");
        return nativeMap;
    }
    public byte[] descriptor() { return open().getDescriptor().clone(); }
    public long count() { return JavaPortableBridge.count(open()); }
    public ProximityConfig config() {
        var value = JavaPortableBridge.config(open());
        return new ProximityConfig(
                value.getDimensions(), value.getMetric(), value.getLogChunkSize(),
                value.getLevelHashSeed(), value.getMinPageBytes(), value.getTargetPageBytes(),
                value.getMaxPageBytes(), value.getOverflowHashSeed(),
                value.getInlineThresholdBytes(), value.getScalarQuantizationGroupSize());
    }
    public build.crab.prolly.ExactProximityRecordRecord get(byte[] key) { return open().get(key.clone()); }
    public boolean contains(byte[] key) { return open().containsKey(key.clone()); }
    public long scanRecords(Predicate<ProximityRecord> visitor) {
        return JavaPortableBridge.scanRecords(open(), record -> visitor.test(new ProximityRecord(
                record.getKey().clone(), toFloatArray(record.getVector()), record.getValue().clone())));
    }
    private static float[] toFloatArray(List<Float> values) {
        var result = new float[values.size()];
        for (int index = 0; index < values.size(); index++) result[index] = values.get(index);
        return result;
    }
    public HnswBuildResult buildHnsw() {
        return buildHnsw(HnswConfig.defaults(), HnswBuildLimits.defaults());
    }
    public HnswBuildResult buildHnsw(HnswConfig config, HnswBuildLimits limits) {
        var result = JavaPortableBridge.buildHnsw(open(), config.toNative(), limits.toNative());
        return new HnswBuildResult(
                new HnswIndex(result.getIndex()),
                HnswBuildStats.fromNative(result.getStats()));
    }
    public HnswIndex loadHnsw(byte[] manifest) {
        return new HnswIndex(JavaPortableBridge.loadHnsw(open(), manifest.clone()));
    }
    public ProductQuantizationBuildResult buildPq() {
        return buildPq(ProductQuantizationConfig.defaults(), 1, ProductQuantizationBuildLimits.defaults());
    }
    public ProductQuantizationBuildResult buildPq(
            ProductQuantizationConfig config, long workerThreads) {
        return buildPq(config, workerThreads, ProductQuantizationBuildLimits.defaults());
    }
    public ProductQuantizationBuildResult buildPq(
            ProductQuantizationConfig config,
            long workerThreads,
            ProductQuantizationBuildLimits limits) {
        if (workerThreads <= 0) throw new IllegalArgumentException("workerThreads must be positive");
        var result = JavaPortableBridge.buildPq(
                open(), config.toNative(), workerThreads, limits.toNative());
        return new ProductQuantizationBuildResult(
                new ProductQuantizer(result.getIndex()),
                ProductQuantizationBuildStats.fromNative(result.getStats()));
    }
    public ProductQuantizer loadPq(byte[] manifest) {
        return new ProductQuantizer(JavaPortableBridge.loadPq(open(), manifest.clone()));
    }
    public CompositeBuildOutcome buildCompositeHnsw(ProximityMap baseMap, HnswIndex base) {
        return buildCompositeHnsw(
                baseMap, base, CompositeAcceleratorConfig.defaults(), CompositeBuildLimits.defaults());
    }
    public CompositeBuildOutcome buildCompositeHnsw(
            ProximityMap baseMap,
            HnswIndex base,
            CompositeAcceleratorConfig config,
            CompositeBuildLimits limits) {
        return compositeOutcome(JavaPortableBridge.buildCompositeHnsw(
                open(), baseMap.open(), base.open(), config.toNative(), limits.toNative()));
    }
    public CompositeBuildOutcome buildCompositePq(ProximityMap baseMap, ProductQuantizer base) {
        return buildCompositePq(
                baseMap, base, CompositeAcceleratorConfig.defaults(), CompositeBuildLimits.defaults());
    }
    public CompositeBuildOutcome buildCompositePq(
            ProximityMap baseMap,
            ProductQuantizer base,
            CompositeAcceleratorConfig config,
            CompositeBuildLimits limits) {
        return compositeOutcome(JavaPortableBridge.buildCompositePq(
                open(), baseMap.open(), base.open(), config.toNative(), limits.toNative()));
    }
    public CompositeBuildOrRebuildOutcome buildOrRebuildCompositeHnsw(
            ProximityMap baseMap,
            HnswIndex base,
            CompositeAcceleratorConfig config) {
        return compositeRebuildOutcome(JavaPortableBridge.buildOrRebuildCompositeHnsw(
                open(), baseMap.open(), base.open(), config.toNative(),
                CompositeBuildLimits.defaults().toNative(),
                CompositeRebuildOptions.defaults().toNative()));
    }
    public CompositeBuildOrRebuildOutcome buildOrRebuildCompositePq(
            ProximityMap baseMap,
            ProductQuantizer base,
            CompositeAcceleratorConfig config) {
        return compositeRebuildOutcome(JavaPortableBridge.buildOrRebuildCompositePq(
                open(), baseMap.open(), base.open(), config.toNative(),
                CompositeBuildLimits.defaults().toNative(),
                CompositeRebuildOptions.defaults().toNative()));
    }
    public CompositeAccelerator loadComposite(byte[] manifest) {
        return new CompositeAccelerator(JavaPortableBridge.loadComposite(open(), manifest.clone()));
    }
    public AcceleratorCatalog buildAcceleratorCatalog(
            HnswIndex hnsw, ProductQuantizer pq, CompositeAccelerator composite) {
        return new AcceleratorCatalog(JavaPortableBridge.buildAcceleratorCatalog(
                open(), hnsw == null ? null : hnsw.open(), pq == null ? null : pq.open(),
                composite == null ? null : composite.open()));
    }
    public AcceleratorCatalog loadAcceleratorCatalog(byte[] manifest) {
        return new AcceleratorCatalog(
                JavaPortableBridge.loadAcceleratorCatalog(open(), manifest.clone()));
    }
    private static CompositeBuildOutcome compositeOutcome(
            build.crab.prolly.api.JavaCompositeBuildOutcome value) {
        return new CompositeBuildOutcome(
                value.getAccelerator() == null ? null : new CompositeAccelerator(value.getAccelerator()),
                value.getReasons().stream().map(FullRebuildReason::fromNative).toList(),
                CompositeBuildStats.fromNative(value.getStats()));
    }
    private static CompositeBuildOrRebuildOutcome compositeRebuildOutcome(
            build.crab.prolly.api.JavaCompositeBuildOrRebuildOutcome value) {
        return new CompositeBuildOrRebuildOutcome(
                CompositeBuildOrRebuildOutcome.Kind.valueOf(value.getKind()),
                value.getComposite() == null ? null : new CompositeAccelerator(value.getComposite()),
                value.getHnsw() == null ? null : new HnswIndex(value.getHnsw()),
                value.getPq() == null ? null : new ProductQuantizer(value.getPq()),
                value.getReasons().stream().map(FullRebuildReason::fromNative).toList(),
                CompositeBuildStats.fromNative(value.getCompositeStats()),
                value.getHnswStats() == null ? null : HnswBuildStats.fromNative(value.getHnswStats()),
                value.getPqStats() == null
                        ? null : ProductQuantizationBuildStats.fromNative(value.getPqStats()));
    }
    public ProximityReadSession read() { return new ProximityReadSession(this, open().read()); }
    public SearchResult search(SearchRequest request) {
        return fromNative(JavaPortableBridge.search(open(), request.toNative()));
    }
    public SearchResult searchWithRuntime(
            SearchRequest request, ProximitySearchRuntime runtime) {
        return fromNative(JavaPortableBridge.searchWithRuntime(
                open(), request.toNative(), runtime.open()));
    }
    static SearchResult fromNative(build.crab.prolly.ProximitySearchResultRecord result) {
        var stats = JavaPortableBridge.searchStats(result);
        return new SearchResult(result.getNeighbors().stream().map(neighbor ->
                new SearchResult.Neighbor(
                        neighbor.getKey().clone(), neighbor.getValue().clone(), neighbor.getDistance()))
                .toList(), new SearchResult.Stats(
                        stats.getLevelsVisited(), stats.getNodesRead(), stats.getBytesRead(),
                        stats.getPhysicalBytesRead(), stats.getCommittedBytes(),
                        stats.getDistanceEvaluations(), stats.getQuantizedDistanceEvaluations(),
                        stats.getRerankedCandidates(), stats.getFrontierPeak(),
                        stats.getCandidateHandlesPeak(), stats.getCandidateRetainedBytesPeak()),
                result.getCompletion().name().toLowerCase(),
                result.getBackend().name().toLowerCase(),
                JavaPortableBridge.searchPlanFormatVersion(result));
    }
    public CompletableFuture<SearchResult> searchAsync(SearchRequest request) {
        return searchAsync(request, null, null);
    }
    public SearchResult searchCancellable(
            SearchRequest request,
            ProximitySearchRuntime runtime,
            ProximityCancellationToken cancellation) {
        return fromNative(JavaPortableBridge.searchCancellable(
                open(), request.toNative(), runtime == null ? null : runtime.open(),
                cancellation.open()));
    }
    public CompletableFuture<SearchResult> searchAsync(
            SearchRequest request,
            ProximitySearchRuntime runtime,
            ProximityCancellationToken cancellation) {
        var owned = request.ownedCopy();
        return cancellableFuture(cancellation, token -> searchCancellable(owned, runtime, token));
    }
    static <T> CompletableFuture<T> cancellableFuture(
            ProximityCancellationToken cancellation,
            Function<ProximityCancellationToken, T> operation) {
        var token = cancellation == null ? new ProximityCancellationToken() : cancellation;
        var future = new CompletableFuture<T>();
        future.whenComplete((ignored, error) -> {
            if (future.isCancelled()) token.cancel();
        });
        CompletableFuture.runAsync(() -> {
            try {
                future.complete(operation.apply(token));
            } catch (Throwable error) {
                future.completeExceptionally(error);
            } finally {
                if (cancellation == null) token.close();
            }
        });
        return future;
    }
    public build.crab.prolly.ProximityMembershipProofRecord proveMembership(byte[] key) {
        return open().proveMembership(key.clone());
    }
    public ProximitySearchProof proveSearch(SearchRequest request) {
        return new ProximitySearchProof(JavaPortableBridge.proveSearch(open(), request.toNative()));
    }
    public build.crab.prolly.ProximityStructuralProofRecord proveStructure() {
        return JavaPortableBridge.proveStructure(open());
    }
    public MutationResult mutate(List<ProximityMutation> mutations) {
        var nativeMutations = mutations.stream().map(mutation -> {
            List<Float> vector = null;
            if (mutation.vector() != null) {
                vector = new ArrayList<>(mutation.vector().length);
                for (float value : mutation.vector()) vector.add(value);
            }
            return new build.crab.prolly.ProximityMutationRecord(
                    mutation.key(), vector, mutation.value());
        }).toList();
        var result = JavaPortableBridge.mutate(open(), nativeMutations);
        var stats = result.getStats();
        return new MutationResult(new ProximityMap(result.getMap()), new ProximityMutationStats(
                stats.getDirectoryEntriesScanned(), stats.getDirectoryNodesRead(),
                stats.getDirectoryNodesRebuilt(), stats.getDirectoryNodesWritten(),
                stats.getDirectoryNodesReused(), stats.getDirectoryLevelsRebuilt(),
                stats.getDirectoryRightEdgeRebuilt(), stats.getNodesRead(), stats.getNodesWritten(),
                stats.getNodesReused(), stats.getRecordsRebuilt(), stats.getDistanceEvaluations(),
                stats.getFullProximityRebuild()));
    }
    public ProximityMap rebuild(List<ProximityMutation> mutations) {
        var nativeMutations = mutations.stream().map(mutation -> {
            List<Float> vector = null;
            if (mutation.vector() != null) {
                vector = new ArrayList<>(mutation.vector().length);
                for (float value : mutation.vector()) vector.add(value);
            }
            return new build.crab.prolly.ProximityMutationRecord(
                    mutation.key(), vector, mutation.value());
        }).toList();
        return new ProximityMap(JavaPortableBridge.rebuild(open(), nativeMutations));
    }
    public record MutationResult(
            ProximityMap map,
            ProximityMutationStats stats) {}
    public ProximityVerification verify() {
        var value = JavaPortableBridge.verify(open());
        return new ProximityVerification(
                value.getRecordCount(), value.getProximityNodeCount(), value.getExternalVectorCount(),
                value.getQuantizedNodeCount(), value.getScalarQuantizerCount(),
                value.getOverflowPageCount(), value.getOverflowDirectoryCount(),
                value.getMaximumLevel(), value.getMaximumNodeBytes(), value.getDistanceChecks());
    }
    public long verifiedRecordCount() {
        return verify().recordCount();
    }
    public void clearCache() { open().clearCache(); }
    @Override public void close() {
        if (nativeMap != null) { nativeMap.close(); nativeMap = null; }
    }
}
