package build.crab.prolly;

public final class GcSweep {
    private final GcPlan plan;
    private final long deletedNodes;
    private final long deletedBytes;

    GcSweep(GcSweepRecord record) {
        this.plan = new GcPlan(record.getPlan());
        this.deletedNodes = ProllyJavaAdapters.gcSweepDeletedNodes(record);
        this.deletedBytes = ProllyJavaAdapters.gcSweepDeletedBytes(record);
    }

    public GcPlan plan() {
        return plan;
    }

    public long deletedNodes() {
        return deletedNodes;
    }

    public long deletedBytes() {
        return deletedBytes;
    }
}
