package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class AcceleratorCatalog implements AutoCloseable {
    private build.crab.prolly.api.AcceleratorCatalog nativeCatalog;

    AcceleratorCatalog(build.crab.prolly.api.AcceleratorCatalog nativeCatalog) {
        this.nativeCatalog = nativeCatalog;
    }
    build.crab.prolly.api.AcceleratorCatalog open() {
        if (nativeCatalog == null) throw new IllegalStateException("accelerator catalog is closed");
        return nativeCatalog;
    }
    public byte[] manifest() { return open().getManifest().clone(); }
    public byte[] sourceDescriptor() { return open().getSourceDescriptor().clone(); }
    public List<AcceleratorCatalogEntry> entries() {
        return JavaPortableBridge.catalogEntries(open()).stream().map(value ->
                new AcceleratorCatalogEntry(
                        AcceleratorCatalogEntry.Kind.valueOf(value.getKind()),
                        value.getConfigurationFingerprint(), value.getManifest())).toList();
    }
    public SearchResult search(ProximityMap map, SearchRequest request) {
        return ProximityMap.fromNative(
                JavaPortableBridge.catalogSearch(open(), map.open(), request.toNative()));
    }
    public SearchResult searchWithRuntime(
            ProximityMap map, SearchRequest request, ProximitySearchRuntime runtime) {
        return ProximityMap.fromNative(JavaPortableBridge.catalogSearchWithRuntime(
                open(), map.open(), request.toNative(), runtime.open()));
    }
    public SearchResult searchCancellable(
            ProximityMap map,
            SearchRequest request,
            ProximitySearchRuntime runtime,
            ProximityCancellationToken cancellation) {
        return ProximityMap.fromNative(JavaPortableBridge.catalogSearchCancellable(
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
                JavaPortableBridge.catalogProveSearch(open(), map.open(), request.toNative()));
    }
    @Override public void close() {
        if (nativeCatalog != null) { nativeCatalog.close(); nativeCatalog = null; }
    }
}
