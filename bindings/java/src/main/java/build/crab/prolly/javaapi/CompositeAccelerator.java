package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;
import java.util.concurrent.CompletableFuture;

public final class CompositeAccelerator implements AutoCloseable {
    private build.crab.prolly.api.CompositeAccelerator nativeIndex;

    CompositeAccelerator(build.crab.prolly.api.CompositeAccelerator nativeIndex) {
        this.nativeIndex = nativeIndex;
    }
    build.crab.prolly.api.CompositeAccelerator open() {
        if (nativeIndex == null) throw new IllegalStateException("composite accelerator is closed");
        return nativeIndex;
    }
    public byte[] manifest() { return open().getManifest().clone(); }
    public byte[] currentSourceDescriptor() { return open().getCurrentSourceDescriptor().clone(); }
    public byte[] baseSourceDescriptor() { return open().getBaseSourceDescriptor().clone(); }
    public String baseKind() { return open().getBaseKind().name(); }
    public long deltaCount() { return JavaPortableBridge.compositeDeltaCount(open()); }
    public long shadowCount() { return JavaPortableBridge.compositeShadowCount(open()); }
    public CompositeAcceleratorConfig config() {
        return CompositeAcceleratorConfig.fromNative(JavaPortableBridge.compositeConfig(open()));
    }
    public CompositeBuildStats buildStats() {
        return CompositeBuildStats.fromNative(JavaPortableBridge.compositeBuildStats(open()));
    }
    public SearchResult search(ProximityMap map, SearchRequest request) {
        return ProximityMap.fromNative(
                JavaPortableBridge.compositeSearch(open(), map.open(), request.toNative()));
    }
    public SearchResult searchWithRuntime(
            ProximityMap map, SearchRequest request, ProximitySearchRuntime runtime) {
        return ProximityMap.fromNative(JavaPortableBridge.compositeSearchWithRuntime(
                open(), map.open(), request.toNative(), runtime.open()));
    }
    public SearchResult searchCancellable(
            ProximityMap map,
            SearchRequest request,
            ProximitySearchRuntime runtime,
            ProximityCancellationToken cancellation) {
        return ProximityMap.fromNative(JavaPortableBridge.compositeSearchCancellable(
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
                JavaPortableBridge.compositeProveSearch(open(), map.open(), request.toNative()));
    }
    @Override public void close() {
        if (nativeIndex != null) { nativeIndex.close(); nativeIndex = null; }
    }
}
