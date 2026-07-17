package build.crab.prolly.javaapi;

import java.util.Optional;
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
