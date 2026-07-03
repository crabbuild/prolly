package build.crab.prolly;

public final class DiffTraversalStats {
    private final long comparedNodes;
    private final long reusedSubtrees;
    private final long addedSubtrees;
    private final long removedSubtrees;
    private final long collectedFallbacks;
    private final long emittedDiffs;

    DiffTraversalStats(DiffTraversalStatsRecord record) {
        this.comparedNodes = ProllyJavaAdapters.diffStatsComparedNodes(record);
        this.reusedSubtrees = ProllyJavaAdapters.diffStatsReusedSubtrees(record);
        this.addedSubtrees = ProllyJavaAdapters.diffStatsAddedSubtrees(record);
        this.removedSubtrees = ProllyJavaAdapters.diffStatsRemovedSubtrees(record);
        this.collectedFallbacks = ProllyJavaAdapters.diffStatsCollectedFallbacks(record);
        this.emittedDiffs = ProllyJavaAdapters.diffStatsEmittedDiffs(record);
    }

    public long comparedNodes() {
        return comparedNodes;
    }

    public long reusedSubtrees() {
        return reusedSubtrees;
    }

    public long addedSubtrees() {
        return addedSubtrees;
    }

    public long removedSubtrees() {
        return removedSubtrees;
    }

    public long collectedFallbacks() {
        return collectedFallbacks;
    }

    public long emittedDiffs() {
        return emittedDiffs;
    }
}
