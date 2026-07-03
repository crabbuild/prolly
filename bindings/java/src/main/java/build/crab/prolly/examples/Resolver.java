package build.crab.prolly.examples;

import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class Resolver {
    private Resolver() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        resolver();
    }

    private static void resolver() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            TreeRecord base = prolly.put(prolly.create(), bytes("settings/theme"), bytes("light"));
            TreeRecord leftDelete = prolly.delete(base, bytes("settings/theme"));
            TreeRecord rightUpdate = prolly.put(base, bytes("settings/theme"), bytes("dark"));

            TreeRecord updateWins = prolly.merge(base, leftDelete, rightUpdate, "update_wins");
            TreeRecord deleteWins = prolly.merge(base, leftDelete, rightUpdate, "delete_wins");

            requireBytes(bytes("dark"), prolly.get(updateWins, bytes("settings/theme")).orElseThrow(), "update-wins setting");
            require(prolly.get(deleteWins, bytes("settings/theme")).isEmpty(), "delete-wins should remove setting");

            System.out.println("resolver: demonstrated update-wins and delete-wins policies");
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
