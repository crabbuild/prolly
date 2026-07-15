package build.crab.prolly;

public final class WriteResult {
    private final TreeRecord tree;
    private final WriteStats stats;

    WriteResult(WriteResultRecord record) {
        this.tree = ProllyJavaAdapters.writeResultTree(record);
        this.stats = new WriteStats(ProllyJavaAdapters.writeResultStats(record));
    }

    public TreeRecord tree() {
        return tree;
    }

    public WriteStats stats() {
        return stats;
    }
}
