package build.crab.prolly;

import java.util.List;

public final class GcPlan {
    private final GcReachability reachability;
    private final long candidateNodes;
    private final List<byte[]> reclaimableCids;
    private final long reclaimableNodes;
    private final long reclaimableBytes;
    private final long missingCandidates;

    GcPlan(GcPlanRecord record) {
        this.reachability = new GcReachability(record.getReachability());
        this.candidateNodes = ProllyJavaAdapters.gcPlanCandidateNodes(record);
        this.reclaimableCids = GcReachability.cloneByteArrays(record.getReclaimableCids());
        this.reclaimableNodes = ProllyJavaAdapters.gcPlanReclaimableNodes(record);
        this.reclaimableBytes = ProllyJavaAdapters.gcPlanReclaimableBytes(record);
        this.missingCandidates = ProllyJavaAdapters.gcPlanMissingCandidates(record);
    }

    public GcReachability reachability() {
        return reachability;
    }

    public long candidateNodes() {
        return candidateNodes;
    }

    public List<byte[]> reclaimableCids() {
        return GcReachability.cloneByteArrays(reclaimableCids);
    }

    public long reclaimableNodes() {
        return reclaimableNodes;
    }

    public long reclaimableBytes() {
        return reclaimableBytes;
    }

    public long missingCandidates() {
        return missingCandidates;
    }
}
