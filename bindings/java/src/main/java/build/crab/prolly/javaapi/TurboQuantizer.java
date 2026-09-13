package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;
import java.util.concurrent.CompletableFuture;

public final class TurboQuantizer implements AutoCloseable {
    private build.crab.prolly.api.TurboQuantizer nativeIndex;

    TurboQuantizer(build.crab.prolly.api.TurboQuantizer nativeIndex) {
        this.nativeIndex = nativeIndex;
    }

    build.crab.prolly.api.TurboQuantizer open() {
        if (nativeIndex == null) throw new IllegalStateException("TurboQuantizer is closed");
        return nativeIndex;
    }

    public byte[] manifest() { return open().getManifest().clone(); }
    public byte[] sourceDescriptor() { return open().getSourceDescriptor().clone(); }
    public TurboQuantizationConfig config() {
        return TurboQuantizationConfig.fromNative(JavaPortableBridge.turboquantConfig(open()));
    }
    public TurboQuantizationQuality quality() {
        return TurboQuantizationQuality.fromNative(JavaPortableBridge.turboquantQuality(open()));
    }
    public TurboQuantizationVerification verify(ProximityMap map) {
        return TurboQuantizationVerification.fromNative(
                JavaPortableBridge.turboquantVerify(open(), map.open()));
    }
    public SearchResult search(ProximityMap map, SearchRequest request) {
        return ProximityMap.fromNative(
                JavaPortableBridge.turboquantSearch(open(), map.open(), request.toNative()));
    }
    public SearchResult searchWithRuntime(
            ProximityMap map, SearchRequest request, ProximitySearchRuntime runtime) {
        return ProximityMap.fromNative(JavaPortableBridge.turboquantSearchWithRuntime(
                open(), map.open(), request.toNative(), runtime.open()));
    }
    public SearchResult searchCancellable(
            ProximityMap map,
            SearchRequest request,
            ProximitySearchRuntime runtime,
            ProximityCancellationToken cancellation) {
        return ProximityMap.fromNative(JavaPortableBridge.turboquantSearchCancellable(
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
                JavaPortableBridge.turboquantProveSearch(open(), map.open(), request.toNative()));
    }

    @Override public void close() {
        if (nativeIndex != null) {
            nativeIndex.close();
            nativeIndex = null;
        }
    }
}
