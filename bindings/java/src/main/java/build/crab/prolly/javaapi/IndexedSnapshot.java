package build.crab.prolly.javaapi;

public final class IndexedSnapshot implements AutoCloseable {
    private build.crab.prolly.api.IndexedSnapshot nativeSnapshot;
    IndexedSnapshot(build.crab.prolly.api.IndexedSnapshot nativeSnapshot) {
        this.nativeSnapshot = nativeSnapshot;
    }
    private build.crab.prolly.api.IndexedSnapshot open() {
        if (nativeSnapshot == null) throw new IllegalStateException("indexed snapshot is closed");
        return nativeSnapshot;
    }
    public IndexedSnapshotId id() {
        return IndexedSnapshotId.fromNative(
                build.crab.prolly.api.JavaPortableBridge.snapshotId(open()));
    }
    public SecondaryIndex index(byte[] name) {
        return new SecondaryIndex(open().index(name.clone()));
    }
    @Override public void close() {
        if (nativeSnapshot != null) { nativeSnapshot.close(); nativeSnapshot = null; }
    }
}
