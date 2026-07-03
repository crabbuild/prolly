package build.crab.prolly;

import java.util.List;

public final class BlobGcPlan {
    private final BlobGcReachability reachability;
    private final long candidateBlobs;
    private final List<BlobRef> reclaimableBlobs;
    private final long reclaimableBlobCount;
    private final long reclaimableBlobBytes;
    private final long missingCandidates;

    BlobGcPlan(BlobGcPlanRecord record) {
        this.reachability = new BlobGcReachability(record.getReachability());
        this.candidateBlobs = ProllyJavaAdapters.blobGcPlanCandidateBlobs(record);
        this.reclaimableBlobs = BlobGcReachability.blobRefs(record.getReclaimableBlobs());
        this.reclaimableBlobCount = ProllyJavaAdapters.blobGcPlanReclaimableBlobCount(record);
        this.reclaimableBlobBytes = ProllyJavaAdapters.blobGcPlanReclaimableBlobBytes(record);
        this.missingCandidates = ProllyJavaAdapters.blobGcPlanMissingCandidates(record);
    }

    public BlobGcReachability reachability() {
        return reachability;
    }

    public long candidateBlobs() {
        return candidateBlobs;
    }

    public List<BlobRef> reclaimableBlobs() {
        return List.copyOf(reclaimableBlobs);
    }

    public long reclaimableBlobCount() {
        return reclaimableBlobCount;
    }

    public long reclaimableBlobBytes() {
        return reclaimableBlobBytes;
    }

    public long missingCandidates() {
        return missingCandidates;
    }
}
