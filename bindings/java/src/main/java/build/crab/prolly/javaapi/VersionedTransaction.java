package build.crab.prolly.javaapi;

import java.util.List;
import java.util.Optional;

/** Atomic, read-your-writes transaction spanning multiple versioned maps. */
public final class VersionedTransaction implements AutoCloseable {
    private build.crab.prolly.api.VersionedTransaction nativeTransaction;
    VersionedTransaction(build.crab.prolly.api.VersionedTransaction nativeTransaction) {
        this.nativeTransaction = nativeTransaction;
    }
    private build.crab.prolly.api.VersionedTransaction open() {
        if (nativeTransaction == null) throw new IllegalStateException("versioned transaction is completed");
        return nativeTransaction;
    }
    public Optional<MapVersion> head(byte[] mapId) {
        var value = open().head(mapId.clone());
        return Optional.ofNullable(value == null ? null : MapVersion.fromNative(value));
    }
    public Optional<byte[]> get(byte[] mapId, byte[] key) {
        byte[] value = open().get(mapId.clone(), key.clone());
        return Optional.ofNullable(value == null ? null : value.clone());
    }
    public MapVersion apply(byte[] mapId, List<MapMutation> mutations) {
        return MapVersion.fromNative(open().apply(mapId.clone(), mutations.stream().map(MapMutation::toNative).toList()));
    }
    public MapUpdate applyIf(byte[] mapId, byte[] expected, List<MapMutation> mutations) {
        return MapUpdate.fromNative(open().applyIf(
                mapId.clone(), expected == null ? null : expected.clone(),
                mutations.stream().map(MapMutation::toNative).toList()));
    }
    public MapVersion put(byte[] mapId, byte[] key, byte[] value) {
        return MapVersion.fromNative(open().put(mapId.clone(), key.clone(), value.clone()));
    }
    public MapVersion delete(byte[] mapId, byte[] key) {
        return MapVersion.fromNative(open().delete(mapId.clone(), key.clone()));
    }
    public VersionedTransactionCommit commit() {
        var result = VersionedTransactionCommit.fromNative(open().commit());
        close();
        return result;
    }
    public void rollback() { open().rollback(); close(); }
    @Override public void close() {
        if (nativeTransaction != null) { nativeTransaction.close(); nativeTransaction = null; }
    }
}
