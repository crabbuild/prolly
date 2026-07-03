package build.crab.prolly.examples;

import build.crab.prolly.MutationRecord;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public final class DurableSqlite {
    private DurableSqlite() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        durableSqlite();
    }

    private static void durableSqlite() throws Exception {
        Path dir = Files.createTempDirectory("prolly-java-");
        try (Prolly prolly = Prolly.sqlite(dir.resolve("app.prolly.sqlite"))) {
            TreeRecord tree = prolly.batch(prolly.create(), List.of(
                    upsertText("user/1", "Ada"),
                    upsertText("user/2", "Grace")));
            prolly.publishNamedRoot(bytes("users/main"), tree);
            TreeRecord loaded = prolly.loadNamedRoot(bytes("users/main")).orElseThrow();
            requireBytes(tree.getRoot(), loaded.getRoot(), "loaded SQLite root");
            requireBytes(bytes("Ada"), prolly.get(loaded, bytes("user/1")).orElseThrow(), "sqlite user");
        } finally {
            deleteTree(dir);
        }
        System.out.println("durable_sqlite: named root survived through SQLite store API");
    }

    private static MutationRecord upsertText(String key, String value) {
        return Prolly.upsert(bytes(key), bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void requireBytes(byte[] expected, byte[] actual, String label) {
        if (!Arrays.equals(expected, actual)) {
            throw new IllegalStateException(label + " mismatch");
        }
    }

    private static void deleteTree(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return;
        }
        try (var paths = Files.walk(dir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
