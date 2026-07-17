package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;
import java.util.concurrent.CompletableFuture;

public final class HnswIndex implements AutoCloseable {
    private build.crab.prolly.api.HnswIndex nativeIndex;

    HnswIndex(build.crab.prolly.api.HnswIndex nativeIndex) {
        this.nativeIndex = nativeIndex;
    }

    build.crab.prolly.api.HnswIndex open() {
        if (nativeIndex == null) throw new IllegalStateException("HNSW index is closed");
        return nativeIndex;
    }

    public byte[] manifest() { return open().getManifest().clone(); }
    public byte[] sourceDescriptor() { return open().getSourceDescriptor().clone(); }
    public HnswConfig config() { return HnswConfig.fromNative(JavaPortableBridge.hnswConfig(open())); }
    public boolean isCanonical() { return open().isCanonical(); }
    public SearchResult search(ProximityMap map, SearchRequest request) {
        return ProximityMap.fromNative(
                JavaPortableBridge.hnswSearch(open(), map.open(), request.toNative()));
    }
    public SearchResult searchWithRuntime(
            ProximityMap map, SearchRequest request, ProximitySearchRuntime runtime) {
        return ProximityMap.fromNative(JavaPortableBridge.hnswSearchWithRuntime(
                open(), map.open(), request.toNative(), runtime.open()));
    }
    public SearchResult searchCancellable(
            ProximityMap map,
            SearchRequest request,
            ProximitySearchRuntime runtime,
            ProximityCancellationToken cancellation) {
        return ProximityMap.fromNative(JavaPortableBridge.hnswSearchCancellable(
                open(), map.open(), request.toNative(), runtime == null ? null : runtime.open(),
                cancellation.open()));
    }
    public CompletableFuture<SearchResult> searchAsync(ProximityMap map, SearchRequest request) {
        return searchAsync(map, request, null, null);
    }
    public CompletableFuture<SearchResult> searchAsync(
            ProximityMap map,
            SearchRequest request,
            ProximitySearchRuntime runtime,
            ProximityCancellationToken cancellation) {
        var owned = request.ownedCopy();
        return ProximityMap.cancellableFuture(
                cancellation, token -> searchCancellable(map, owned, runtime, token));
    }
    public ProximitySearchProof proveSearch(ProximityMap map, SearchRequest request) {
        return new ProximitySearchProof(
                JavaPortableBridge.hnswProveSearch(open(), map.open(), request.toNative()));
    }

    @Override public void close() {
        if (nativeIndex != null) {
            nativeIndex.close();
            nativeIndex = null;
        }
    }
}
