package build.crab.prolly.examples;

import build.crab.prolly.MutationRecord;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public final class AgentEventLog {
    private AgentEventLog() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        agentEventLog();
    }

    private static void agentEventLog() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            byte[] root = bytes("agent-log/run-7/root/events/current");
            TreeRecord tree = prolly.batch(prolly.create(), List.of(
                    upsertText("agent-log/run-7/event/1783036805000/0001", "user|Summarize the plan"),
                    upsertText("agent-log/run-7/event/1783036805000/0002", "tool-call|search-docs"),
                    upsertText("agent-log/run-7/event/1783036806000/0003", "assistant|Plan ready")));
            prolly.publishNamedRoot(root, tree);

            var page = prolly.rangePage(prolly.loadNamedRoot(root).orElseThrow(), null, Optional.empty(), 2);
            require(page.getEntries().size() == 2, "expected first event page");
            require(page.getNextCursor() != null, "expected next cursor");

            System.out.printf("agent_event_log: first page has %d events%n", page.getEntries().size());
        }
    }

    private static MutationRecord upsertText(String key, String value) {
        return Prolly.upsert(bytes(key), bytes(value));
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
