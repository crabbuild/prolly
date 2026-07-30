package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexedSnapshotId;

public record IndexedSnapshotId(byte[] snapshot) {
    public IndexedSnapshotId {
        snapshot = snapshot.clone();
    }

    static IndexedSnapshotId fromNative(JavaIndexedSnapshotId value) {
        return new IndexedSnapshotId(value.getSnapshot());
    }

    JavaIndexedSnapshotId toNative() {
        return new JavaIndexedSnapshotId(snapshot.clone());
    }
}
