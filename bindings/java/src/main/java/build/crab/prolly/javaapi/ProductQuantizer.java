package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;

public final class ProductQuantizer implements AutoCloseable {
    private build.crab.prolly.api.ProductQuantizer nativeIndex;

    ProductQuantizer(build.crab.prolly.api.ProductQuantizer nativeIndex) {
        this.nativeIndex = nativeIndex;
    }

    private build.crab.prolly.api.ProductQuantizer open() {
        if (nativeIndex == null) throw new IllegalStateException("product quantizer is closed");
        return nativeIndex;
    }

    public byte[] manifest() { return open().getManifest().clone(); }
    public byte[] sourceDescriptor() { return open().getSourceDescriptor().clone(); }
    public ProductQuantizationConfig config() {
        return ProductQuantizationConfig.fromNative(JavaPortableBridge.pqConfig(open()));
    }
    public ProductQuantizationQuality quality() {
        return ProductQuantizationQuality.fromNative(JavaPortableBridge.pqQuality(open()));
    }
    public SearchResult search(ProximityMap map, SearchRequest request) {
        return ProximityMap.fromNative(
                JavaPortableBridge.pqSearch(open(), map.open(), request.toNative()));
    }
    public ProximitySearchProof proveSearch(ProximityMap map, SearchRequest request) {
        return new ProximitySearchProof(
                JavaPortableBridge.pqProveSearch(open(), map.open(), request.toNative()));
    }

    @Override public void close() {
        if (nativeIndex != null) {
            nativeIndex.close();
            nativeIndex = null;
        }
    }
}
