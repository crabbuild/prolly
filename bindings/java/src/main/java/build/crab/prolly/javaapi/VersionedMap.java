package build.crab.prolly.javaapi;

import java.util.Optional;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class VersionedMap implements AutoCloseable {
    private build.crab.prolly.api.VersionedMap nativeMap;

    VersionedMap(build.crab.prolly.api.VersionedMap nativeMap) {
        this.nativeMap = nativeMap;
    }

    private build.crab.prolly.api.VersionedMap open() {
        if (nativeMap == null) throw new IllegalStateException("versioned map is closed");
        return nativeMap;
    }

    public MapVersion initialize() {
        return MapVersion.fromNative(open().initialize());
    }

    public byte[] id() { return open().getId().clone(); }

    public boolean isInitialized() { return open().isInitialized(); }

    public Optional<MapVersion> head() {
        var value = open().head();
        return Optional.ofNullable(value == null ? null : MapVersion.fromNative(value));
    }

    public Optional<byte[]> headId() {
        byte[] value = open().headId();
        return Optional.ofNullable(value == null ? null : value.clone());
    }

    public Optional<MapVersion> version(byte[] id) {
        var value = open().version(id.clone());
        return Optional.ofNullable(value == null ? null : MapVersion.fromNative(value));
    }

    public List<MapVersion> versions() {
        return open().versions().stream().map(MapVersion::fromNative).toList();
    }

    public Optional<byte[]> get(byte[] key) {
        byte[] value = open().get(key.clone());
        return Optional.ofNullable(value == null ? null : value.clone());
    }

    public MapVersion put(byte[] key, byte[] value) {
        return MapVersion.fromNative(open().put(key.clone(), value.clone()));
    }

    public MapVersion delete(byte[] key) {
        return MapVersion.fromNative(open().delete(key.clone()));
    }

    public MapSnapshot snapshot() {
        var snapshot = open().snapshot();
        return snapshot == null ? null : new MapSnapshot(snapshot);
    }

    public MapSnapshot snapshotAt(byte[] id) {
        var snapshot = open().snapshotAt(id.clone());
        return snapshot == null ? null : new MapSnapshot(snapshot);
    }

    public byte[] backup() { return open().backup().clone(); }
    public build.crab.prolly.MapCatalogVerificationRecord verifyCatalog() {
        return open().verifyCatalog();
    }
    public long catalogVersionCount() {
        return build.crab.prolly.api.JavaPortableBridge.versionCount(verifyCatalog());
    }
    public build.crab.prolly.GcPlanRecord planGc() { return open().planGc(); }
    public build.crab.prolly.GcSweepRecord sweepGc() { return open().sweepGc(); }
    public build.crab.prolly.VersionPruneRecord keepLast(long count) {
        return build.crab.prolly.api.JavaPortableBridge.keepLast(open(), count);
    }

    public CompletableFuture<MapVersion> initializeAsync() {
        var nativeHandle = open();
        return CompletableFuture.supplyAsync(() -> MapVersion.fromNative(nativeHandle.initialize()));
    }

    public CompletableFuture<Optional<byte[]>> getAsync(byte[] key) {
        var nativeHandle = open();
        byte[] ownedKey = key.clone();
        return CompletableFuture.supplyAsync(() -> {
            byte[] value = nativeHandle.get(ownedKey);
            return Optional.ofNullable(value == null ? null : value.clone());
        });
    }

    public CompletableFuture<MapVersion> putAsync(byte[] key, byte[] value) {
        var nativeHandle = open();
        byte[] ownedKey = key.clone();
        byte[] ownedValue = value.clone();
        return CompletableFuture.supplyAsync(
                () -> MapVersion.fromNative(nativeHandle.put(ownedKey, ownedValue)));
    }

    public CompletableFuture<MapVersion> deleteAsync(byte[] key) {
        var nativeHandle = open();
        byte[] ownedKey = key.clone();
        return CompletableFuture.supplyAsync(() -> MapVersion.fromNative(nativeHandle.delete(ownedKey)));
    }

    @Override
    public void close() {
        if (nativeMap != null) {
            nativeMap.close();
            nativeMap = null;
        }
    }
}
