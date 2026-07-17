package build.crab.prolly.javaapi;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public final class IndexedMap implements AutoCloseable {
    private build.crab.prolly.api.IndexedMap nativeMap;

    IndexedMap(build.crab.prolly.api.IndexedMap nativeMap) { this.nativeMap = nativeMap; }

    private build.crab.prolly.api.IndexedMap open() {
        if (nativeMap == null) throw new IllegalStateException("indexed map is closed");
        return nativeMap;
    }

    public Optional<byte[]> get(byte[] key) {
        byte[] value = open().get(key.clone());
        return Optional.ofNullable(value == null ? null : value.clone());
    }
    public IndexedVersion put(byte[] key, byte[] value) {
        return IndexedVersion.fromNative(open().put(key.clone(), value.clone()));
    }
    public IndexedVersion delete(byte[] key) {
        return IndexedVersion.fromNative(open().delete(key.clone()));
    }
    public IndexBuildResult ensureIndex(byte[] name) {
        return IndexBuildResult.fromNative(open().ensureIndex(name.clone()));
    }
    public IndexedSnapshot snapshot() { return new IndexedSnapshot(open().snapshot()); }
    public build.crab.prolly.IndexedMapHealthRecord health() { return open().health(); }
    public build.crab.prolly.IndexedMapMetricsRecord metrics() { return open().metrics(); }
    public long buildAttempts() {
        return build.crab.prolly.api.JavaPortableBridge.buildAttempts(metrics());
    }
    public build.crab.prolly.IndexVerificationRecord verifyIndex(byte[] name, byte[] sourceVersion) {
        return open().verifyIndex(name.clone(), sourceVersion.clone());
    }
    public java.util.List<build.crab.prolly.IndexVerificationRecord> verifyAll(byte[] sourceVersion) {
        return open().verifyAll(sourceVersion.clone());
    }
    public build.crab.prolly.IndexVerificationRecord repairIndex(byte[] name, byte[] sourceVersion) {
        return open().repairIndex(name.clone(), sourceVersion.clone());
    }
    public byte[] exportCurrent() { return open().exportCurrent().clone(); }
    public build.crab.prolly.IndexedRetentionRecord keepLast(long count) {
        return build.crab.prolly.api.JavaPortableBridge.keepLast(open(), count);
    }

    public CompletableFuture<IndexedVersion> putAsync(byte[] key, byte[] value) {
        var nativeHandle = open(); byte[] ownedKey = key.clone(); byte[] ownedValue = value.clone();
        return CompletableFuture.supplyAsync(
                () -> IndexedVersion.fromNative(nativeHandle.put(ownedKey, ownedValue)));
    }
    public CompletableFuture<IndexBuildResult> ensureIndexAsync(byte[] name) {
        var nativeHandle = open(); byte[] ownedName = name.clone();
        return CompletableFuture.supplyAsync(
                () -> IndexBuildResult.fromNative(nativeHandle.ensureIndex(ownedName)));
    }

    @Override public void close() {
        if (nativeMap != null) { nativeMap.close(); nativeMap = null; }
    }
}
