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
    public build.crab.prolly.ProximityVerificationRecord verify() { return open().verify(); }
    public long verifiedRecordCount() {
        return JavaPortableBridge.recordCount(verify());
    }
    public void clearCache() { open().clearCache(); }
    @Override public void close() {
        if (nativeMap != null) { nativeMap.close(); nativeMap = null; }
    }
}
