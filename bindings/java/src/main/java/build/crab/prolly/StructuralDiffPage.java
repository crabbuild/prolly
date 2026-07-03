package build.crab.prolly;

import java.util.List;
import java.util.Optional;

public final class StructuralDiffPage {
    private final List<DiffRecord> diffs;
    private final Optional<String> nextCursorJson;
    private final DiffTraversalStats stats;
    private final Optional<StructuralDiffCursorRecord> nextCursor;

    StructuralDiffPage(StructuralDiffPageRecord record) {
        this.diffs = List.copyOf(record.getDiffs());
        this.nextCursorJson = Optional.ofNullable(record.getNextCursorJson());
        this.stats = new DiffTraversalStats(record.getStats());
        this.nextCursor = Optional.ofNullable(record.getNextCursor());
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

    public Optional<StructuralDiffCursorRecord> nextCursor() {
        return nextCursor;
    }
}
