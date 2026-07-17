package build.crab.prolly.javaapi;

public record AcceleratorCatalogEntry(Kind kind, byte[] configurationFingerprint, byte[] manifest) {
    public enum Kind { HNSW, PRODUCT_QUANTIZED, COMPOSITE }
    public AcceleratorCatalogEntry {
        configurationFingerprint = configurationFingerprint.clone();
        manifest = manifest.clone();
    }
}
