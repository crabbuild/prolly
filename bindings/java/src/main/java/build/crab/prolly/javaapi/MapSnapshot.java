package build.crab.prolly.javaapi;

import java.util.List;
import java.util.Optional;

import build.crab.prolly.EntryRecord;
import build.crab.prolly.RangeCursorRecord;
import build.crab.prolly.RangePageRecord;
import build.crab.prolly.ReverseCursorRecord;
import build.crab.prolly.ReversePageRecord;
import build.crab.prolly.api.JavaPortableBridge;

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
    public MapVersion version() { return MapVersion.fromNative(open().getVersion()); }
    public Optional<byte[]> get(byte[] key) {
        byte[] value = open().get(key.clone());
        return Optional.ofNullable(value == null ? null : value.clone());
    }
    public List<Optional<byte[]>> getMany(List<byte[]> keys) {
        return open().getMany(keys.stream().map(byte[]::clone).toList()).stream()
                .map(value -> Optional.ofNullable(value == null ? null : value.clone())).toList();
    }
    public boolean containsKey(byte[] key) { return open().containsKey(key.clone()); }
    public Optional<EntryRecord> firstEntry() { return Optional.ofNullable(open().firstEntry()); }
    public Optional<EntryRecord> lastEntry() { return Optional.ofNullable(open().lastEntry()); }
    public Optional<EntryRecord> lowerBound(byte[] key) {
        return Optional.ofNullable(open().lowerBound(key.clone()));
    }
    public Optional<EntryRecord> upperBound(byte[] key) {
        return Optional.ofNullable(open().upperBound(key.clone()));
    }
    public List<EntryRecord> range(byte[] start, byte[] end) {
        return open().range(start.clone(), end == null ? null : end.clone());
    }
    public List<EntryRecord> prefix(byte[] prefix) { return open().prefix(prefix.clone()); }
    public RangePageRecord rangePage(RangeCursorRecord cursor, byte[] end, long limit) {
        return JavaPortableBridge.mapSnapshotRangePage(
                open(), cursor, end == null ? null : end.clone(), limit);
    }
    public RangePageRecord prefixPage(byte[] prefix, RangeCursorRecord cursor, long limit) {
        return JavaPortableBridge.mapSnapshotPrefixPage(open(), prefix.clone(), cursor, limit);
    }
    public ReversePageRecord reversePage(ReverseCursorRecord cursor, byte[] start, long limit) {
        return JavaPortableBridge.mapSnapshotReversePage(open(), cursor, start.clone(), limit);
    }
    public ReversePageRecord prefixReversePage(byte[] prefix, ReverseCursorRecord cursor, long limit) {
        return JavaPortableBridge.mapSnapshotPrefixReversePage(
                open(), prefix.clone(), cursor, limit);
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
