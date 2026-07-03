package build.crab.prolly.examples;

import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class ConversationMemory {
    private ConversationMemory() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        conversationMemory();
    }

    private static void conversationMemory() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            byte[] main = bytes("conversation/c42/root/main");
            byte[] attemptName = bytes("conversation/c42/attempt/extractor/a1");
            TreeRecord base = prolly.put(prolly.create(), bytes("conversation/c42/memory/m001"), bytes("user|likes terse summaries|0.91"));
            prolly.publishNamedRoot(main, base);
            TreeRecord attempt = prolly.put(base, bytes("conversation/c42/memory/m002"), bytes("user|uses Java|0.87"));
            prolly.publishNamedRoot(attemptName, attempt);
            TreeRecord canonical = prolly.put(base, bytes("conversation/c42/memory/m003"), bytes("user|prefers local-first apps|0.82"));
            prolly.publishNamedRoot(main, canonical);

            TreeRecord merged = prolly.merge(
                    base,
                    prolly.loadNamedRoot(main).orElseThrow(),
                    prolly.loadNamedRoot(attemptName).orElseThrow(),
                    "prefer_right");
            var update = prolly.compareAndSwapNamedRoot(main, Optional.of(canonical), Optional.of(merged));
            int count = prolly.range(merged, bytes("conversation/c42/memory/"), Optional.of(bytes("conversation/c42/memory0"))).size();

            require(update.getApplied(), "main root CAS failed");
            require(count == 3, "expected three memories");

            System.out.println("conversation_memory: accepted extractor attempt into canonical memory");
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
}
