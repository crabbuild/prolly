package build.crab.prolly.examples;

import build.crab.prolly.BlobGcPlan;
import build.crab.prolly.BlobGcSweep;
import build.crab.prolly.BlobStore;
import build.crab.prolly.DiffKind;
import build.crab.prolly.DiffRecord;
import build.crab.prolly.Entry;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import build.crab.prolly.ValueRef;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public final class CookbookScenarios {
    private CookbookScenarios() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        basicMap();
        diffMerge();
        fileBlobStore();
        secondaryIndex();
        CookbookApps.runAll();
    }

    public static void basicMap() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            TreeRecord tree = prolly.create();
            tree = prolly.put(tree, bytes("user:001"), bytes("Ada"));
            tree = prolly.put(tree, bytes("user:002"), bytes("Grace"));
            tree = prolly.put(tree, bytes("user:003"), bytes("Linus"));

            requireBytes(bytes("Ada"), prolly.get(tree, bytes("user:001")).orElseThrow(), "user:001");

            tree = prolly.delete(tree, bytes("user:003"));
            require(prolly.get(tree, bytes("user:003")).isEmpty(), "user:003 should be deleted");

            List<Entry> users = prolly.range(tree, bytes("user:"), Optional.of(bytes("user;")));
            require(users.size() == 2, "expected two users");
            requireBytes(bytes("user:001"), users.get(0).key(), "first key");
            requireBytes(bytes("Ada"), users.get(0).value(), "first value");
            requireBytes(bytes("user:002"), users.get(1).key(), "second key");
            requireBytes(bytes("Grace"), users.get(1).value(), "second value");

            System.out.printf("basic_map: %d users in range%n", users.size());
        }
    }

    public static void diffMerge() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            TreeRecord base = prolly.create();
            base = prolly.put(base, bytes("doc:title"), bytes("Draft"));

            TreeRecord left = prolly.put(base, bytes("doc:body"), bytes("Hello"));
            TreeRecord right = prolly.put(base, bytes("doc:tags"), bytes("example"));

            List<DiffRecord> leftChanges = prolly.diff(base, left);
            require(leftChanges.size() == 1, "expected one left-side change");
            requireBytes(bytes("doc:body"), leftChanges.get(0).getKey(), "diff key");

            TreeRecord merged = prolly.merge(base, left, right, "prefer_right");
            requireBytes(bytes("Hello"), prolly.get(merged, bytes("doc:body")).orElseThrow(), "merged body");
            requireBytes(bytes("example"), prolly.get(merged, bytes("doc:tags")).orElseThrow(), "merged tags");

            System.out.printf("diff_merge: merged %d left-side change%n", leftChanges.size());
        }
    }

    public static void fileBlobStore() throws Exception {
        try (Prolly prolly = Prolly.memory(); BlobStore blobStore = BlobStore.memory()) {
            TreeRecord tree = prolly.create();
            byte[] first = repeated((byte) 7, 64);
            byte[] second = repeated((byte) 9, 64);

            tree = prolly.putLargeValue(blobStore, tree, bytes("doc/body"), first, Prolly.largeValueConfig(8));
            require(prolly.getValueRef(tree, bytes("doc/body")).orElseThrow().kind() == ValueRef.Kind.BLOB, "expected blob ref");

            TreeRecord updated =
                    prolly.putLargeValue(blobStore, tree, bytes("doc/body"), second, Prolly.largeValueConfig(8));
            requireBytes(second, prolly.getLargeValue(blobStore, updated, bytes("doc/body")).orElseThrow(), "large value");

            BlobGcPlan plan = prolly.planBlobStoreGc(blobStore, List.of(updated));
            require(plan.reclaimableBlobCount() == 1, "expected one reclaimable blob");
            BlobGcSweep sweep = prolly.sweepBlobStoreGc(blobStore, List.of(updated));
            require(sweep.deletedBlobs() == 1, "expected one deleted blob");

            System.out.printf("file_blob_store: reclaimed %d bytes%n", sweep.deletedBlobBytes());
        }
    }

    public static void secondaryIndex() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            TreeRecord empty = prolly.create();

            TreeRecord sourceV1 = putUser(prolly, empty, user("acme", "u001", "active", "Ada"));
            sourceV1 = putUser(prolly, sourceV1, user("acme", "u002", "invited", "Grace"));
            TreeRecord indexV1 = buildStatusIndex(prolly, sourceV1);

            TreeRecord sourceV2 = putUser(prolly, sourceV1, user("acme", "u002", "active", "Grace"));
            sourceV2 = putUser(prolly, sourceV2, user("globex", "u003", "active", "Linus"));

            List<DiffRecord> sourceChanges = prolly.diff(sourceV1, sourceV2);
            require(sourceChanges.size() == 2, "expected two source changes");

            TreeRecord indexV2 = applySourceDiff(prolly, indexV1, sourceChanges);
            TreeRecord rebuiltIndexV2 = buildStatusIndex(prolly, sourceV2);
            requireBytes(indexV2.getRoot(), rebuiltIndexV2.getRoot(), "incremental index root");

            require(usersByStatus(prolly, indexV2, "acme", "active").size() == 2, "expected two acme active users");
            require(usersByStatus(prolly, indexV2, "acme", "invited").isEmpty(), "expected no acme invited users");
            require(usersByStatus(prolly, indexV2, "globex", "active").size() == 1, "expected one globex active user");

            System.out.printf("secondary_index: applied %d source diffs%n", sourceChanges.size());
        }
    }

    public static void largeValues() throws Exception {
        fileBlobStore();
    }

    private record User(String tenant, String id, String status, String name) {
    }

    private static User user(String tenant, String id, String status, String name) {
        return new User(tenant, id, status, name);
    }

    private static byte[] userKey(User user) {
        return bytes("source/tenant/" + user.tenant() + "/user/" + user.id());
    }

    private static byte[] encodeUser(User user) {
        return bytes(String.join("|", user.tenant(), user.id(), user.status(), user.name()));
    }

    private static User decodeUser(byte[] value) {
        String[] parts = new String(value, StandardCharsets.UTF_8).split("\\|", 4);
        return user(parts[0], parts[1], parts[2], parts[3]);
    }

    private static byte[] statusIndexPrefix(String tenant, String status) {
        return bytes("index/user-by-status/tenant/" + tenant + "/status/" + status + "/");
    }

    private static byte[] statusIndexKey(User user) {
        return bytes(new String(statusIndexPrefix(user.tenant(), user.status()), StandardCharsets.UTF_8) + user.id());
    }

    private static TreeRecord putUser(Prolly prolly, TreeRecord tree, User user) throws Exception {
        return prolly.put(tree, userKey(user), encodeUser(user));
    }

    private static TreeRecord buildStatusIndex(Prolly prolly, TreeRecord source) throws Exception {
        TreeRecord index = prolly.create();
        for (Entry entry : prolly.range(source, bytes("source/"), Optional.of(bytes("source0")))) {
            index = prolly.put(index, statusIndexKey(decodeUser(entry.value())), bytes("1"));
        }
        return index;
    }

    private static TreeRecord applySourceDiff(Prolly prolly, TreeRecord index, List<DiffRecord> changes)
            throws Exception {
        for (DiffRecord change : changes) {
            if (change.getKind() == DiffKind.ADDED) {
                index = prolly.put(index, statusIndexKey(decodeUser(change.getValue())), bytes("1"));
            } else if (change.getKind() == DiffKind.REMOVED) {
                index = prolly.delete(index, statusIndexKey(decodeUser(change.getValue())));
            } else if (change.getKind() == DiffKind.CHANGED) {
                byte[] oldKey = statusIndexKey(decodeUser(change.getOldValue()));
                byte[] newKey = statusIndexKey(decodeUser(change.getNewValue()));
                if (!Arrays.equals(oldKey, newKey)) {
                    index = prolly.delete(index, oldKey);
                    index = prolly.put(index, newKey, bytes("1"));
                }
            }
        }
        return index;
    }

    private static List<Entry> usersByStatus(Prolly prolly, TreeRecord index, String tenant, String status)
            throws Exception {
        byte[] start = statusIndexPrefix(tenant, status);
        return prolly.range(index, start, Optional.ofNullable(Prolly.prefixEnd(start)));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] repeated(byte value, int len) {
        byte[] out = new byte[len];
        Arrays.fill(out, value);
        return out;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static void requireBytes(byte[] expected, byte[] actual, String label) {
        if (!Arrays.equals(expected, actual)) {
            throw new IllegalStateException(label + " mismatch");
        }
    }
}
