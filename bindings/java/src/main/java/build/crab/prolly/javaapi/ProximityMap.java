package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
    public ProximityReadSession read() { return new ProximityReadSession(this, open().read()); }
    public SearchResult search(SearchRequest request) {
        List<Float> query = new ArrayList<>(request.vector().length);
        for (float value : request.vector()) query.add(value);
        var result = JavaPortableBridge.searchExact(open(), query, request.topK());
        return fromNative(result);
    }
    static SearchResult fromNative(build.crab.prolly.ProximitySearchResultRecord result) {
        return new SearchResult(result.getNeighbors().stream().map(neighbor ->
                new SearchResult.Neighbor(
                        neighbor.getKey().clone(), neighbor.getValue().clone(), neighbor.getDistance()))
                .toList(), result.getCompletion().name().toLowerCase(), result.getBackend().name().toLowerCase());
    }
    public CompletableFuture<SearchResult> searchAsync(SearchRequest request) {
        var owned = SearchRequest.exact(request.vector(), request.topK());
        return CompletableFuture.supplyAsync(() -> search(owned));
    }
    public build.crab.prolly.ProximityMembershipProofRecord proveMembership(byte[] key) {
        return open().proveMembership(key.clone());
    }
    public ProximitySearchProof proveSearch(SearchRequest request) {
        List<Float> query = new ArrayList<>(request.vector().length);
        for (float value : request.vector()) query.add(value);
        return new ProximitySearchProof(
                JavaPortableBridge.proveSearch(open(), query, request.topK()));
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
