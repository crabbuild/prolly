package build.crab.prolly.javaapi;

import java.util.List;

public record BlobGcPlan(
        BlobGcReachability reachability,
        long candidateBlobs,
        List<BlobRef> reclaimableBlobs,
        long reclaimableBlobCount,
        long reclaimableBlobBytes,
        long missingCandidates) {
    public BlobGcPlan {
        reclaimableBlobs = List.copyOf(reclaimableBlobs);
    }

    static BlobGcPlan fromBridge(build.crab.prolly.api.JavaBlobGcPlan value) {
        return new BlobGcPlan(
                BlobGcReachability.fromBridge(value.getReachability()),
                value.getCandidateBlobs(),
                value.getReclaimableBlobs().stream().map(BlobRef::fromBridge).toList(),
                value.getReclaimableBlobCount(), value.getReclaimableBlobBytes(),
                value.getMissingCandidates());
    }
}
