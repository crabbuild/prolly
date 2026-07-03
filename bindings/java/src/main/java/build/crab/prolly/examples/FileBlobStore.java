package build.crab.prolly.examples;

import build.crab.prolly.BlobGcPlan;
import build.crab.prolly.BlobGcSweep;
import build.crab.prolly.BlobStore;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import build.crab.prolly.ValueRef;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public final class FileBlobStore {
    private FileBlobStore() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        fileBlobStore();
    }

    private static void fileBlobStore() throws Exception {
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
