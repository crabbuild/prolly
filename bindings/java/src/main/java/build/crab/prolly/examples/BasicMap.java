package build.crab.prolly.examples;

import build.crab.prolly.Entry;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public final class BasicMap {
    private BasicMap() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        basicMap();
    }

    private static void basicMap() throws Exception {
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
