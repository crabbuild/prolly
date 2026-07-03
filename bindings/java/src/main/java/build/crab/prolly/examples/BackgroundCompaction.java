package build.crab.prolly.examples;

import build.crab.prolly.Entry;
import build.crab.prolly.GcPlan;
import build.crab.prolly.MutationRecord;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class BackgroundCompaction {
    private BackgroundCompaction() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        backgroundCompaction();
    }

    private static void backgroundCompaction() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            List<MutationRecord> mutations = new ArrayList<>();
            for (int idx = 1; idx <= 6; idx++) {
                mutations.add(upsertText(String.format("event/%04d", idx), "raw-event-" + idx));
            }
            TreeRecord events = prolly.batch(prolly.create(), mutations);
            prolly.publishNamedRoot(bytes("compaction/run/r7/root/events/0001"), events);
            TreeRecord compacted = prolly.batch(events, List.of(
                    Prolly.deleteMutation(bytes("event/0001")),
                    Prolly.deleteMutation(bytes("event/0002")),
                    Prolly.deleteMutation(bytes("event/0003")),
                    Prolly.deleteMutation(bytes("event/0004")),
                    upsertText("event/0004-summary", "summary of events 1..4")));
            prolly.publishNamedRoot(bytes("compaction/run/r7/root/events/current"), compacted);

            GcPlan plan = prolly.planStoreGc(List.of(events, compacted));
            List<Entry> remaining = prolly.range(compacted, bytes("event/"), Optional.of(bytes("event0")));
            require(remaining.size() == 3, "expected compacted log records");
            require(plan.reclaimableNodes() >= 0, "invalid GC plan");

            System.out.printf("background_compaction: compacted log to %d records%n", remaining.size());
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
