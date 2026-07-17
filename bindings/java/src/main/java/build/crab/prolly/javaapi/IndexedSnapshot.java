package build.crab.prolly.javaapi;

public final class IndexedSnapshot implements AutoCloseable {
    private build.crab.prolly.api.IndexedSnapshot nativeSnapshot;
    IndexedSnapshot(build.crab.prolly.api.IndexedSnapshot nativeSnapshot) {
        this.nativeSnapshot = nativeSnapshot;
    }
    public SecondaryIndex index(byte[] name) {
        if (nativeSnapshot == null) throw new IllegalStateException("indexed snapshot is closed");
        return new SecondaryIndex(nativeSnapshot.index(name.clone()));
    }
    @Override public void close() {
        if (nativeSnapshot != null) { nativeSnapshot.close(); nativeSnapshot = null; }
    }
}
