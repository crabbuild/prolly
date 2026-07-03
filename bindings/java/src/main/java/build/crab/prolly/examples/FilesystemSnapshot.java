package build.crab.prolly.examples;

import build.crab.prolly.BlobStore;
import build.crab.prolly.Entry;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

public final class FilesystemSnapshot {
    private FilesystemSnapshot() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        filesystemSnapshot();
    }

    private static void filesystemSnapshot() throws Exception {
        try (Prolly prolly = Prolly.memory(); BlobStore blobStore = BlobStore.memory()) {
            TreeRecord tree = prolly.create();
            Map<String, String> files = Map.of(
                    "README.md", "# Demo\n",
                    "src/lib.rs", "pub fn answer() -> u8 { 42 }\n");
            for (Map.Entry<String, String> file : files.entrySet()) {
                tree = prolly.putLargeValue(blobStore, tree, bytes("path/" + file.getKey()), bytes(file.getValue()), Prolly.largeValueConfig(4));
            }
            prolly.publishNamedRoot(bytes("refs/heads/main"), tree);
            TreeRecord loaded = prolly.loadNamedRoot(bytes("refs/heads/main")).orElseThrow();
            requireBytes(bytes("# Demo\n"), prolly.getLargeValue(blobStore, loaded, bytes("path/README.md")).orElseThrow(), "README.md");

            System.out.println("filesystem_snapshot: published branch with blob-backed file contents");
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void requireBytes(byte[] expected, byte[] actual, String label) {
        if (!Arrays.equals(expected, actual)) {
            throw new IllegalStateException(label + " mismatch");
        }
    }
}
