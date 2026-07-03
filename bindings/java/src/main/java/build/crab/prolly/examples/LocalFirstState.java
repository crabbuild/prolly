package build.crab.prolly.examples;

import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public final class LocalFirstState {
    private LocalFirstState() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        localFirstState();
    }

    private static void localFirstState() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            byte[] main = bytes("app/demo/root/main");
            TreeRecord base = prolly.batch(prolly.create(), List.of(
                    Prolly.upsert(bytes("entity/user/001"), bytes("Ada")),
                    Prolly.upsert(bytes("index/user/name/Ada/001"), new byte[0])));
            prolly.publishNamedRoot(main, base);

            TreeRecord device = prolly.batch(base, List.of(
                    Prolly.upsert(bytes("entity/task/900"), bytes("offline draft")),
                    Prolly.upsert(bytes("index/task/status/open/900"), new byte[0])));
            TreeRecord canonical = prolly.put(base, bytes("entity/user/002"), bytes("Grace"));
            prolly.publishNamedRoot(main, canonical);

            TreeRecord current = prolly.loadNamedRoot(main).orElseThrow();
            TreeRecord merged = prolly.merge(base, current, device, "prefer_right");
            var update = prolly.compareAndSwapNamedRoot(main, Optional.of(current), Optional.of(merged));

            require(update.getApplied(), "main root CAS failed");
            requireBytes(bytes("Grace"), prolly.get(merged, bytes("entity/user/002")).orElseThrow(), "canonical user");
            requireBytes(bytes("offline draft"), prolly.get(merged, bytes("entity/task/900")).orElseThrow(), "device task");

            System.out.println("local_first_state: merged offline branch into main");
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
