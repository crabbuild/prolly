package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class HostStoreAdapter implements HostStoreCallback {
    private final HostStore store;

    HostStoreAdapter(HostStore store) {
        this.store = store;
    }

    @Override
    public HostStoreBytesResultRecord get(byte[] key) {
        try {
            return new HostStoreBytesResultRecord(
                    store.get(cloneBytes(key)).map(HostStoreAdapter::cloneBytes).orElse(null),
                    null);
        } catch (Exception error) {
            return new HostStoreBytesResultRecord(null, message(error));
        }
    }

    @Override
    public HostStoreUnitResultRecord put(byte[] key, byte[] value) {
        try {
            store.put(cloneBytes(key), cloneBytes(value));
            return unit();
        } catch (Exception error) {
            return unit(error);
        }
    }

    @Override
    public HostStoreUnitResultRecord delete(byte[] key) {
        try {
            store.delete(cloneBytes(key));
            return unit();
        } catch (Exception error) {
            return unit(error);
        }
    }

    @Override
    public HostStoreUnitResultRecord batch(List<MutationRecord> ops) {
        try {
            store.batch(cloneMutations(ops));
            return unit();
        } catch (Exception error) {
            return unit(error);
        }
    }

    @Override
    public HostStoreBatchGetResultRecord batchGetOrdered(List<byte[]> keys) {
        try {
            List<Optional<byte[]>> values = store.batchGetOrdered(cloneByteArrays(keys));
            List<byte[]> lowered = new ArrayList<>(values.size());
            for (Optional<byte[]> value : values) {
                lowered.add(value.map(HostStoreAdapter::cloneBytes).orElse(null));
            }
            return new HostStoreBatchGetResultRecord(lowered, null);
        } catch (Exception error) {
            return new HostStoreBatchGetResultRecord(List.of(), message(error));
        }
    }

    @Override
    public HostStoreBoolResultRecord prefersBatchReads() {
        try {
            return new HostStoreBoolResultRecord(store.prefersBatchReads(), null);
        } catch (Exception error) {
            return new HostStoreBoolResultRecord(false, message(error));
        }
    }

    @Override
    public HostStoreBoolResultRecord supportsHints() {
        try {
            return new HostStoreBoolResultRecord(store.supportsHints(), null);
        } catch (Exception error) {
            return new HostStoreBoolResultRecord(false, message(error));
        }
    }

    @Override
    public HostStoreBytesResultRecord getHint(byte[] namespace, byte[] key) {
        try {
            return new HostStoreBytesResultRecord(
                    store.getHint(cloneBytes(namespace), cloneBytes(key))
                            .map(HostStoreAdapter::cloneBytes)
                            .orElse(null),
                    null);
        } catch (Exception error) {
            return new HostStoreBytesResultRecord(null, message(error));
        }
    }

    @Override
    public HostStoreUnitResultRecord putHint(byte[] namespace, byte[] key, byte[] value) {
        try {
            store.putHint(cloneBytes(namespace), cloneBytes(key), cloneBytes(value));
            return unit();
        } catch (Exception error) {
            return unit(error);
        }
    }

    @Override
    public HostStoreListBytesResultRecord listNodeCids() {
        try {
            return new HostStoreListBytesResultRecord(cloneByteArrays(store.listNodeCids()), null);
        } catch (Exception error) {
            return new HostStoreListBytesResultRecord(List.of(), message(error));
        }
    }

    @Override
    public HostStoreRootResultRecord getRoot(byte[] name) {
        try {
            return new HostStoreRootResultRecord(store.getRoot(cloneBytes(name)).orElse(null), null);
        } catch (Exception error) {
            return new HostStoreRootResultRecord(null, message(error));
        }
    }

    @Override
    public HostStoreUnitResultRecord putRoot(byte[] name, RootManifestRecord manifest) {
        try {
            store.putRoot(cloneBytes(name), manifest);
            return unit();
        } catch (Exception error) {
            return unit(error);
        }
    }

    @Override
    public HostStoreUnitResultRecord deleteRoot(byte[] name) {
        try {
            store.deleteRoot(cloneBytes(name));
            return unit();
        } catch (Exception error) {
            return unit(error);
        }
    }

    @Override
    public HostStoreRootCasResultRecord compareAndSwapRoot(
            byte[] name,
            RootManifestRecord expected,
            RootManifestRecord replacement) {
        try {
            HostStoreRootCasResult result =
                    store.compareAndSwapRoot(cloneBytes(name), expected, replacement);
            return new HostStoreRootCasResultRecord(result.applied(), result.current(), null);
        } catch (Exception error) {
            return new HostStoreRootCasResultRecord(false, null, message(error));
        }
    }

    @Override
    public HostStoreListRootsResultRecord listRoots() {
        try {
            return new HostStoreListRootsResultRecord(store.listRoots(), null);
        } catch (Exception error) {
            return new HostStoreListRootsResultRecord(List.of(), message(error));
        }
    }

    private static HostStoreUnitResultRecord unit() {
        return new HostStoreUnitResultRecord(null);
    }

    private static HostStoreUnitResultRecord unit(Exception error) {
        return new HostStoreUnitResultRecord(message(error));
    }

    private static String message(Exception error) {
        String message = error.getMessage();
        return message == null ? error.toString() : message;
    }

    private static byte[] cloneBytes(byte[] value) {
        return value == null ? null : value.clone();
    }

    private static List<byte[]> cloneByteArrays(List<byte[]> values) {
        List<byte[]> cloned = new ArrayList<>(values.size());
        for (byte[] value : values) {
            cloned.add(cloneBytes(value));
        }
        return cloned;
    }

    private static List<MutationRecord> cloneMutations(List<MutationRecord> mutations) {
        List<MutationRecord> cloned = new ArrayList<>(mutations.size());
        for (MutationRecord mutation : mutations) {
            cloned.add(new MutationRecord(
                    mutation.getKind(),
                    cloneBytes(mutation.getKey()),
                    cloneBytes(mutation.getValue())));
        }
        return cloned;
    }
}
