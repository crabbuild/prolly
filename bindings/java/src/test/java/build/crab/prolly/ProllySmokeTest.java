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
    void streamingVisitorsPreserveOrderAndEarlyStop() throws Exception {
        Prolly.useLocalDebugLibrary();
        try (Prolly prolly = Prolly.memory()) {
            TreeRecord base = prolly.create();
            for (String key : List.of("a", "b", "c", "d")) {
                base = prolly.put(base, key.getBytes(), key.getBytes());
            }

            List<String> keys = new ArrayList<>();
            ScanOutcome range = prolly.scanRange(base, "b".getBytes(), Optional.empty(), entry -> {
                keys.add(new String(entry.key()));
                return keys.size() < 2;
            });
            assertEquals(List.of("b", "c"), keys);
            assertEquals(new ScanOutcome(2, true), range);

            List<String> reverse = new ArrayList<>();
            ScanOutcome reverseOutcome = prolly.scanPrefixReverse(base, new byte[0], entry -> {
                reverse.add(new String(entry.key()));
                return true;
            });
            assertEquals(List.of("d", "c", "b", "a"), reverse);
            assertEquals(new ScanOutcome(4, false), reverseOutcome);

            TreeRecord left = prolly.put(base, "b".getBytes(), "left".getBytes());
            TreeRecord right = prolly.put(base, "b".getBytes(), "right".getBytes());
            List<String> kinds = new ArrayList<>();
            assertEquals(1, prolly.scanDiff(base, right, diff -> {
                kinds.add(diff.getKind().name().toLowerCase());
                return true;
            }).visited());
            assertEquals(List.of("changed"), kinds);
            List<String> conflicts = new ArrayList<>();
            assertEquals(1, prolly.scanConflicts(base, left, right, conflict -> {
                conflicts.add(new String(conflict.getKey()));
                return true;
            }).visited());
            assertEquals(List.of("b"), conflicts);
        }
    }

    @Test
    void memoryEngineDeleteRangeUsesHalfOpenBounds() throws Exception {
        Prolly.useLocalDebugLibrary();
        try (Prolly prolly = Prolly.memory()) {
            TreeRecord tree = prolly.create();
            for (String key : List.of("a", "b", "c", "d", "e", "f")) {
                tree = prolly.put(tree, key.getBytes(), key.getBytes());
            }
            TreeRecord deleted = prolly.deleteRange(tree, "b".getBytes(), "e".getBytes());
            assertEquals(List.of("a", "e", "f"), prolly.range(deleted, new byte[0], Optional.empty()).stream().map(entry -> new String(entry.key())).toList());
            WriteResult withStats = prolly.deleteRangeWithStats(tree, "b".getBytes(), "e".getBytes());
            assertEquals(List.of("a", "e", "f"), prolly.range(withStats.tree(), new byte[0], Optional.empty()).stream().map(entry -> new String(entry.key())).toList());
        }
    }

    @Test
    void memoryTransactionCommitsRollsBackAndConflicts() throws Exception {
        Prolly.useLocalDebugLibrary();

        try (Prolly prolly = Prolly.memory()) {
            byte[] sourceName = "tickets/source/current".getBytes();
            byte[] rollbackName = "tickets/source/rolled-back".getBytes();
            byte[] conflictName = "tickets/source/conflict".getBytes();

            try (Transaction transaction = prolly.beginTransaction()) {
                TreeRecord tree = transaction.put(
                        transaction.create(),
                        "ticket/123/status".getBytes(),
                        "open".getBytes());
                transaction.publishNamedRoot(sourceName, tree);
                TransactionUpdate update = transaction.commit();
                assertTrue(update.applied());
                assertFalse(update.conflict());
                assertTrue(update.nodesWritten() > 0);
                assertEquals(1, update.rootsWritten());
                assertArrayEquals(tree.getRoot(), prolly.loadNamedRoot(sourceName).orElseThrow().getRoot());
            }

            try (Transaction transaction = prolly.beginTransaction()) {
                TreeRecord rolledBack = transaction.put(
                        transaction.create(),
                        "ticket/456/status".getBytes(),
                        "closed".getBytes());
                transaction.publishNamedRoot(rollbackName, rolledBack);
                transaction.rollback();
                assertTrue(prolly.loadNamedRoot(rollbackName).isEmpty());
            }

            try (Transaction stale = prolly.beginTransaction()) {
                assertTrue(stale.loadNamedRoot(conflictName).isEmpty());
                TreeRecord winner = prolly.put(
                        prolly.create(),
                        "ticket/789/status".getBytes(),
                        "open".getBytes());
                prolly.publishNamedRoot(conflictName, winner);
                TreeRecord loser = stale.put(
                        stale.create(),
                        "ticket/789/status".getBytes(),
                        "closed".getBytes());
                stale.publishNamedRoot(conflictName, loser);
                TransactionUpdate conflict = stale.commit();
                assertFalse(conflict.applied());
                assertTrue(conflict.conflict());
                assertArrayEquals(conflictName, conflict.conflictDetail().name());
                assertArrayEquals(winner.getRoot(), prolly.loadNamedRoot(conflictName).orElseThrow().getRoot());
            }
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
