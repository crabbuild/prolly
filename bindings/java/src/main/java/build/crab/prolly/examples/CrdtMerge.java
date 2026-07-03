package build.crab.prolly.examples;

import build.crab.prolly.Prolly;
import build.crab.prolly.TimestampedValueRecord;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public final class CrdtMerge {
    private CrdtMerge() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        crdtMerge();
    }

    private static void crdtMerge() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            byte[] baseValue = Prolly.timestampedValueToBytes(Prolly.timestampedValue(bytes("base"), 1));
            byte[] leftValue = Prolly.timestampedValueToBytes(Prolly.timestampedValue(bytes("left"), 2));
            byte[] rightValue = Prolly.timestampedValueToBytes(Prolly.timestampedValue(bytes("right"), 3));

            TreeRecord base = prolly.put(prolly.create(), bytes("counter/global"), baseValue);
            TreeRecord left = prolly.put(base, bytes("counter/global"), leftValue);
            TreeRecord right = prolly.put(base, bytes("counter/global"), rightValue);
            TreeRecord merged = prolly.crdtMerge(base, left, right, Prolly.crdtConfigLww("update_wins"));
            TimestampedValueRecord decoded =
                    Prolly.timestampedValueFromBytes(prolly.get(merged, bytes("counter/global")).orElseThrow());
            List<byte[]> mergedSet = Prolly.multiValueSetMerge(List.of(bytes("candidate-b")), List.of(bytes("candidate-a"), bytes("candidate-b")));

            requireBytes(bytes("right"), decoded.getValue(), "CRDT value");
            require(Prolly.timestampedValueTimestamp(decoded) == 3, "CRDT timestamp");
            requireBytes(bytes("candidate-a"), mergedSet.get(0), "first multi-value item");

            System.out.println("crdt_merge: last-writer-wins and multi-value helpers passed");
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
