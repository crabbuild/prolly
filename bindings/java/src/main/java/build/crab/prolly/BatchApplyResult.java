package build.crab.prolly;

public final class BatchApplyResult {
    private final TreeRecord tree;
    private final BatchApplyStats stats;

    BatchApplyResult(BatchApplyResultRecord record) {
        this.tree = ProllyJavaAdapters.batchResultTree(record);
        this.stats = new BatchApplyStats(ProllyJavaAdapters.batchResultStats(record));
    }

    public TreeRecord tree() {
        return tree;
    }

    public BatchApplyStats stats() {
        return stats;
    }
}
