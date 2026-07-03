package build.crab.prolly.examples;

import build.crab.prolly.DiffKind;
import build.crab.prolly.DiffRecord;
import build.crab.prolly.Entry;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public final class SecondaryIndex {
    private SecondaryIndex() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        secondaryIndex();
    }

    private static void secondaryIndex() throws Exception {
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
