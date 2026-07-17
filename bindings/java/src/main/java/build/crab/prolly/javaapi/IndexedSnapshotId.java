package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexedSnapshotId;

public record IndexedSnapshotId(byte[] sourceVersion, byte[] catalogVersion) {
    public IndexedSnapshotId {
        sourceVersion = sourceVersion.clone();
        catalogVersion = catalogVersion.clone();
    }

    static IndexedSnapshotId fromNative(JavaIndexedSnapshotId value) {
        return new IndexedSnapshotId(value.getSourceVersion(), value.getCatalogVersion());
    }

    JavaIndexedSnapshotId toNative() {
        return new JavaIndexedSnapshotId(sourceVersion.clone(), catalogVersion.clone());
    }
}
