package build.crab.prolly.javaapi;

import java.util.List;
import java.util.Optional;

public final class MapSnapshot implements AutoCloseable {
    private build.crab.prolly.api.MapSnapshot nativeSnapshot;

    MapSnapshot(build.crab.prolly.api.MapSnapshot nativeSnapshot) {
        this.nativeSnapshot = nativeSnapshot;
    }

    private build.crab.prolly.api.MapSnapshot open() {
        if (nativeSnapshot == null) throw new IllegalStateException("map snapshot is closed");
        return nativeSnapshot;
    }

    public byte[] id() { return open().getId().clone(); }
    public Optional<byte[]> get(byte[] key) {
        byte[] value = open().get(key.clone());
        return Optional.ofNullable(value == null ? null : value.clone());
    }
    public build.crab.prolly.KeyProofRecord proveKey(byte[] key) {
        return open().proveKey(key.clone());
    }
    public build.crab.prolly.MultiKeyProofRecord proveKeys(List<byte[]> keys) {
        return open().proveKeys(keys.stream().map(byte[]::clone).toList());
    }
    public build.crab.prolly.TreeStatsRecord stats() { return open().stats(); }
    public long entryCount() {
        return build.crab.prolly.api.JavaPortableBridge.totalKeyValuePairs(stats());
    }
    public build.crab.prolly.SnapshotBundleRecord export() { return open().export(); }
    public ReadSession read() { return new ReadSession(open().read()); }

    @Override public void close() {
        if (nativeSnapshot != null) { nativeSnapshot.close(); nativeSnapshot = null; }
    }
}
