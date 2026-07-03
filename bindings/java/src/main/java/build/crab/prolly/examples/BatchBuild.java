package build.crab.prolly.examples;

import build.crab.prolly.Entry;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public final class BatchBuild {
    private BatchBuild() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        batchBuild();
    }

    private static void batchBuild() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            List<Entry> entries = new ArrayList<>();
            for (int idx = 64; idx >= 1; idx--) {
                entries.add(new Entry(bytes(String.format("event/%04d", idx)), bytes("payload-" + idx)));
            }
            TreeRecord tree = prolly.buildFromEntries(entries);
            List<Entry> rows = prolly.range(tree, bytes("event/"), Optional.of(bytes("event0")));
            require(rows.size() == 64, "expected 64 rows");
            requireBytes(bytes("event/0001"), rows.get(0).key(), "first event key");
            require(prolly.collectStatsJson(tree).contains("num_nodes"), "stats should include num_nodes");

            System.out.printf("batch_build: imported %d events%n", rows.size());
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
