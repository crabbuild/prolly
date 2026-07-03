package build.crab.prolly;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ProllySmokeTest {
    @Test
    void memoryEngineCrudAndRange() throws Exception {
        Prolly.useLocalDebugLibrary();

        try (Prolly prolly = Prolly.memory()) {
            TreeRecord tree = prolly.create();
            tree = prolly.put(tree, "a".getBytes(), "1".getBytes());

            assertArrayEquals("1".getBytes(), prolly.get(tree, "a".getBytes()).orElseThrow());

            List<Entry> entries = prolly.range(tree, new byte[0], Optional.empty());
            assertEquals(1, entries.size());
            assertArrayEquals("a".getBytes(), entries.get(0).key());
            assertArrayEquals("1".getBytes(), entries.get(0).value());
        }
    }

    @Test
    void customStoreCallbacksDriveEngine() throws Exception {
        Prolly.useLocalDebugLibrary();

        MemoryHostStore sourceStore = new MemoryHostStore();
        try (Prolly source = Prolly.customStore(sourceStore)) {
            TreeRecord empty = source.create();
            TreeRecord tree = source.batch(
                    empty,
                    List.of(
                            Prolly.upsert("a".getBytes(), "1".getBytes()),
                            Prolly.upsert("b".getBytes(), "2".getBytes())));

            assertArrayEquals("1".getBytes(), source.get(tree, "a".getBytes()).orElseThrow());
            List<byte[]> values = source.getMany(tree, List.of("a".getBytes(), "missing".getBytes(), "b".getBytes()));
            assertArrayEquals("1".getBytes(), values.get(0));
            assertEquals(null, values.get(1));
            assertArrayEquals("2".getBytes(), values.get(2));
            assertTrue(source.publishPrefixPathHint(tree, "a".getBytes()));
            assertTrue(source.hydratePrefixPathHint(tree, "a".getBytes()));

            source.publishNamedRootAtMillis("main".getBytes(), tree, 7);
            TreeRecord loaded = source.loadNamedRoot("main".getBytes()).orElseThrow();
            assertArrayEquals(tree.getRoot(), loaded.getRoot());
            assertEquals(1, source.listNamedRoots().size());

            List<byte[]> cids = source.listNodeCids();
            assertFalse(cids.isEmpty());
            assertEquals(0, source.planStoreGc(List.of(tree)).reclaimableNodes());

            MemoryHostStore destinationStore = new MemoryHostStore();
            try (Prolly destination = Prolly.customStore(destinationStore)) {
                MissingNodePlan plan = source.planMissingNodes(tree, destination);
                assertTrue(plan.missingNodes() > 0);
                MissingNodeCopy copied = source.copyMissingNodes(tree, destination);
                assertEquals(plan.missingNodes(), copied.copiedNodes());
                assertArrayEquals("2".getBytes(), destination.get(tree, "b".getBytes()).orElseThrow());
            }

            NamedRootUpdateRecord update =
                    source.compareAndSwapNamedRoot("main".getBytes(), Optional.of(tree), Optional.empty());
            assertTrue(update.getApplied());
            assertFalse(update.getConflict());
            assertTrue(source.loadNamedRoot("main".getBytes()).isEmpty());
        }
    }

    private static final class MemoryHostStore implements HostStore {
        private final Map<Key, byte[]> nodes = new HashMap<>();
        private final Map<List<Key>, byte[]> hints = new HashMap<>();
        private final Map<Key, RootManifestRecord> roots = new HashMap<>();

        @Override
        public Optional<byte[]> get(byte[] key) {
            return Optional.ofNullable(nodes.get(new Key(key))).map(byte[]::clone);
        }

        @Override
        public void put(byte[] key, byte[] value) {
            nodes.put(new Key(key), value.clone());
        }

        @Override
        public void delete(byte[] key) {
            nodes.remove(new Key(key));
        }

        @Override
        public boolean prefersBatchReads() {
            return true;
        }

        @Override
        public boolean supportsHints() {
            return true;
        }

        @Override
        public Optional<byte[]> getHint(byte[] namespace, byte[] key) {
            return Optional.ofNullable(hints.get(List.of(new Key(namespace), new Key(key)))).map(byte[]::clone);
        }

        @Override
        public void putHint(byte[] namespace, byte[] key, byte[] value) {
            hints.put(List.of(new Key(namespace), new Key(key)), value.clone());
        }

        @Override
        public List<byte[]> listNodeCids() {
            List<byte[]> cids = new ArrayList<>(nodes.size());
            for (Key key : nodes.keySet()) {
                cids.add(key.bytes());
            }
            return cids;
        }

        @Override
        public Optional<RootManifestRecord> getRoot(byte[] name) {
            return Optional.ofNullable(roots.get(new Key(name)));
        }

        @Override
        public void putRoot(byte[] name, RootManifestRecord manifest) {
            roots.put(new Key(name), manifest);
        }

        @Override
        public void deleteRoot(byte[] name) {
            roots.remove(new Key(name));
        }

        @Override
        public HostStoreRootCasResult compareAndSwapRoot(
                byte[] name,
                RootManifestRecord expected,
                RootManifestRecord replacement) throws Exception {
            Key rootName = new Key(name);
            RootManifestRecord current = roots.get(rootName);
            if (sameManifest(current, expected)) {
                if (replacement == null) {
                    roots.remove(rootName);
                } else {
                    roots.put(rootName, replacement);
                }
                return HostStoreRootCasResult.success();
            }
            return HostStoreRootCasResult.conflict(current);
        }

        @Override
        public List<HostStoreNamedRootManifestRecord> listRoots() {
            List<HostStoreNamedRootManifestRecord> values = new ArrayList<>(roots.size());
            for (Map.Entry<Key, RootManifestRecord> entry : roots.entrySet()) {
                values.add(new HostStoreNamedRootManifestRecord(entry.getKey().bytes(), entry.getValue()));
            }
            return values;
        }

        private static boolean sameManifest(RootManifestRecord left, RootManifestRecord right) throws Exception {
            if (left == null || right == null) {
                return left == right;
            }
            return Arrays.equals(ProllyKt.rootManifestToBytes(left), ProllyKt.rootManifestToBytes(right));
        }
    }

    private record Key(byte[] bytes) {
        private Key {
            bytes = bytes.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Key key && Arrays.equals(bytes, key.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }
    }
}
