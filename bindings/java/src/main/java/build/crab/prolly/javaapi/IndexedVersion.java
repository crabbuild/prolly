package build.crab.prolly.javaapi;

import build.crab.prolly.IndexedVersionRecord;

public record IndexedVersion(byte[] sourceVersion, byte[] catalogVersion) {
    static IndexedVersion fromNative(IndexedVersionRecord value) {
        return new IndexedVersion(
                value.getSourceVersion().clone(),
                value.getCatalogVersion() == null ? null : value.getCatalogVersion().clone());
    }
}
