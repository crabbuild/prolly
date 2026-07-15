package build.crab.prolly;

public final class CanonicalWriteResult {
    private final TreeRecord tree;
    private final CanonicalWriteStats stats;

    CanonicalWriteResult(CanonicalWriteResultRecord record) {
        this.tree = ProllyJavaAdapters.canonicalWriteResultTree(record);
        this.stats = new CanonicalWriteStats(ProllyJavaAdapters.canonicalWriteResultStats(record));
    }

    public TreeRecord tree() {
        return tree;
    }

    public CanonicalWriteStats stats() {
        return stats;
    }
}
