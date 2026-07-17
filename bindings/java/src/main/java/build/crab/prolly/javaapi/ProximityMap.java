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
    public ProximityReadSession read() { return new ProximityReadSession(this, open().read()); }
    public SearchResult search(SearchRequest request) {
        List<Float> query = new ArrayList<>(request.vector().length);
        for (float value : request.vector()) query.add(value);
        var result = JavaPortableBridge.searchExact(open(), query, request.topK());
        return new SearchResult(result.getNeighbors().stream().map(neighbor ->
                new SearchResult.Neighbor(
                        neighbor.getKey().clone(), neighbor.getValue().clone(), neighbor.getDistance()))
                .toList(), result.getCompletion().name().toLowerCase(), result.getBackend().name().toLowerCase());
    }
    public CompletableFuture<SearchResult> searchAsync(SearchRequest request) {
        var owned = SearchRequest.exact(request.vector(), request.topK());
        return CompletableFuture.supplyAsync(() -> search(owned));
    }
    @Override public void close() {
        if (nativeMap != null) { nativeMap.close(); nativeMap = null; }
    }
}
