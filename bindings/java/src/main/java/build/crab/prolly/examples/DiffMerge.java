package build.crab.prolly.examples;

import build.crab.prolly.DiffRecord;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public final class DiffMerge {
    private DiffMerge() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        diffMerge();
    }

    private static void diffMerge() throws Exception {
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
