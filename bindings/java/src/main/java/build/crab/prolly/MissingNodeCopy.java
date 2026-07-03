package build.crab.prolly;

public final class MissingNodeCopy {
    private final MissingNodePlan plan;
    private final long copiedNodes;
    private final long copiedBytes;

    MissingNodeCopy(MissingNodeCopyRecord record) {
        this.plan = new MissingNodePlan(record.getPlan());
        this.copiedNodes = ProllyJavaAdapters.missingNodeCopyCopiedNodes(record);
        this.copiedBytes = ProllyJavaAdapters.missingNodeCopyCopiedBytes(record);
    }

    public MissingNodePlan plan() {
        return plan;
    }

    public long copiedNodes() {
        return copiedNodes;
    }

    public long copiedBytes() {
        return copiedBytes;
    }
}
