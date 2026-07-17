package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;

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
