package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public interface HostStore {
    Optional<byte[]> get(byte[] key) throws Exception;

    void put(byte[] key, byte[] value) throws Exception;

    void delete(byte[] key) throws Exception;

    default void batch(List<MutationRecord> ops) throws Exception {
        for (MutationRecord op : ops) {
            if (op.getKind() == MutationKind.UPSERT) {
                byte[] value = op.getValue();
                if (value == null) {
                    throw new IllegalArgumentException("upsert mutation requires a value");
                }
                put(op.getKey(), value);
            } else {
                delete(op.getKey());
            }
        }
    }

    default List<Optional<byte[]>> batchGetOrdered(List<byte[]> keys) throws Exception {
        List<Optional<byte[]>> values = new ArrayList<>(keys.size());
        for (byte[] key : keys) {
            values.add(get(key));
        }
        return values;
    }

    default boolean prefersBatchReads() {
        return false;
    }

    default boolean supportsHints() {
        return false;
    }

    default Optional<byte[]> getHint(byte[] namespace, byte[] key) throws Exception {
        return Optional.empty();
    }

    default void putHint(byte[] namespace, byte[] key, byte[] value) throws Exception {
    }

    default List<byte[]> listNodeCids() throws Exception {
        return List.of();
    }

    default Optional<RootManifestRecord> getRoot(byte[] name) throws Exception {
        return Optional.empty();
    }

    default void putRoot(byte[] name, RootManifestRecord manifest) throws Exception {
    }

    default void deleteRoot(byte[] name) throws Exception {
    }

    default HostStoreRootCasResult compareAndSwapRoot(
            byte[] name,
            RootManifestRecord expected,
            RootManifestRecord replacement) throws Exception {
        RootManifestRecord current = getRoot(name).orElse(null);
        return current == expected
                ? HostStoreRootCasResult.success()
                : HostStoreRootCasResult.conflict(current);
    }

    default List<HostStoreNamedRootManifestRecord> listRoots() throws Exception {
        return List.of();
    }
}
