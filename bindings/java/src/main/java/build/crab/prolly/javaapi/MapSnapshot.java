package build.crab.prolly.javaapi;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import build.crab.prolly.EntryRecord;
import build.crab.prolly.RangeCursorRecord;
import build.crab.prolly.RangePageRecord;
import build.crab.prolly.RangeProofRecord;
import build.crab.prolly.ProvedRangePageRecord;
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
    public RangeProofRecord proveRange(byte[] start, byte[] end) {
        return open().proveRange(start.clone(), end == null ? null : end.clone());
    }
    public RangeProofRecord provePrefix(byte[] prefix) {
        return open().provePrefix(prefix.clone());
    }
    public ProvedRangePageRecord proveRangePage(
            RangeCursorRecord cursor, byte[] end, long limit) {
        return JavaPortableBridge.mapSnapshotProveRangePage(
                open(), cursor, end == null ? null : end.clone(), limit);
    }
    public build.crab.prolly.TreeStatsRecord stats() { return open().stats(); }
    public long entryCount() {
        return build.crab.prolly.api.JavaPortableBridge.totalKeyValuePairs(stats());
    }
    public build.crab.prolly.SnapshotBundleRecord export() { return open().export(); }
    public CompletableFuture<build.crab.prolly.SnapshotBundleRecord> exportAsync() {
        return CompletableFuture.supplyAsync(this::export);
    }
    public ReadSession read() { return new ReadSession(open().read()); }

    public CompletableFuture<Optional<byte[]>> getAsync(byte[] key) {
        byte[] owned = key.clone();
        return CompletableFuture.supplyAsync(() -> get(owned));
    }
    public CompletableFuture<List<Optional<byte[]>>> getManyAsync(List<byte[]> keys) {
        var owned = keys.stream().map(byte[]::clone).toList();
        return CompletableFuture.supplyAsync(() -> getMany(owned));
    }
    public CompletableFuture<List<EntryRecord>> rangeAsync(byte[] start, byte[] end) {
        byte[] ownedStart = start.clone(); byte[] ownedEnd = end == null ? null : end.clone();
        return CompletableFuture.supplyAsync(() -> range(ownedStart, ownedEnd));
    }
    public CompletableFuture<List<EntryRecord>> prefixAsync(byte[] prefix) {
        byte[] owned = prefix.clone();
        return CompletableFuture.supplyAsync(() -> prefix(owned));
    }
    public CompletableFuture<RangePageRecord> rangePageAsync(
            RangeCursorRecord cursor, byte[] end, long limit) {
        byte[] ownedEnd = end == null ? null : end.clone();
        return CompletableFuture.supplyAsync(() -> rangePage(cursor, ownedEnd, limit));
    }
    public CompletableFuture<RangePageRecord> prefixPageAsync(
            byte[] prefix, RangeCursorRecord cursor, long limit) {
        byte[] owned = prefix.clone();
        return CompletableFuture.supplyAsync(() -> prefixPage(owned, cursor, limit));
    }
    public CompletableFuture<build.crab.prolly.KeyProofRecord> proveKeyAsync(byte[] key) {
        byte[] owned = key.clone();
        return CompletableFuture.supplyAsync(() -> proveKey(owned));
    }
    public CompletableFuture<build.crab.prolly.MultiKeyProofRecord> proveKeysAsync(List<byte[]> keys) {
        var owned = keys.stream().map(byte[]::clone).toList();
        return CompletableFuture.supplyAsync(() -> proveKeys(owned));
    }
    public CompletableFuture<RangeProofRecord> proveRangeAsync(byte[] start, byte[] end) {
        byte[] ownedStart = start.clone(); byte[] ownedEnd = end == null ? null : end.clone();
        return CompletableFuture.supplyAsync(() -> proveRange(ownedStart, ownedEnd));
    }
    public CompletableFuture<RangeProofRecord> provePrefixAsync(byte[] prefix) {
        byte[] owned = prefix.clone();
        return CompletableFuture.supplyAsync(() -> provePrefix(owned));
    }
    public CompletableFuture<build.crab.prolly.TreeStatsRecord> statsAsync() {
        return CompletableFuture.supplyAsync(this::stats);
    }

    @Override public void close() {
        if (nativeSnapshot != null) { nativeSnapshot.close(); nativeSnapshot = null; }
    }
}
