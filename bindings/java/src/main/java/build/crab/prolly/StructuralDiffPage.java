package build.crab.prolly;

import java.util.List;
import java.util.Optional;

public final class StructuralDiffPage {
    private final List<DiffRecord> diffs;
    private final Optional<String> nextCursorJson;
    private final DiffTraversalStats stats;

    StructuralDiffPage(StructuralDiffPageRecord record) {
        this.diffs = List.copyOf(record.getDiffs());
        this.nextCursorJson = Optional.ofNullable(record.getNextCursorJson());
        this.stats = new DiffTraversalStats(record.getStats());
    }

    public List<DiffRecord> diffs() {
        return diffs;
    }

    public Optional<String> nextCursorJson() {
        return nextCursorJson;
    }

    public DiffTraversalStats stats() {
        return stats;
    }
}
