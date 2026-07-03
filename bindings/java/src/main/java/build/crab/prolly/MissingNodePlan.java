package build.crab.prolly;

import java.util.List;

public final class MissingNodePlan {
    private final List<byte[]> requiredCids;
    private final long requiredNodes;
    private final long requiredBytes;
    private final List<byte[]> missingCids;
    private final long missingNodes;
    private final long missingBytes;

    MissingNodePlan(MissingNodePlanRecord record) {
        this.requiredCids = GcReachability.cloneByteArrays(record.getRequiredCids());
        this.requiredNodes = ProllyJavaAdapters.missingNodePlanRequiredNodes(record);
        this.requiredBytes = ProllyJavaAdapters.missingNodePlanRequiredBytes(record);
        this.missingCids = GcReachability.cloneByteArrays(record.getMissingCids());
        this.missingNodes = ProllyJavaAdapters.missingNodePlanMissingNodes(record);
        this.missingBytes = ProllyJavaAdapters.missingNodePlanMissingBytes(record);
    }

    public List<byte[]> requiredCids() {
        return GcReachability.cloneByteArrays(requiredCids);
    }

    public long requiredNodes() {
        return requiredNodes;
    }

    public long requiredBytes() {
        return requiredBytes;
    }

    public List<byte[]> missingCids() {
        return GcReachability.cloneByteArrays(missingCids);
    }

    public long missingNodes() {
        return missingNodes;
    }

    public long missingBytes() {
        return missingBytes;
    }
}
