package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexedVersion;

public record IndexedVersion(byte[] sourceVersion, byte[] catalogVersion, long indexCount) {
    static IndexedVersion fromNative(JavaIndexedVersion value) {
        return new IndexedVersion(
                value.getSourceVersion().clone(),
                value.getCatalogVersion() == null ? null : value.getCatalogVersion().clone(),
                value.getIndexCount());
    }
}
