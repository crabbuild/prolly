package build.crab.prolly.javaapi;

import java.util.List;

public record GcPlan(
        GcReachability reachability,
        long candidateNodes,
        List<byte[]> reclaimableCids,
        long reclaimableNodes,
        long reclaimableBytes,
        long missingCandidates) {
    static GcPlan fromBridge(build.crab.prolly.api.JavaGcPlan value) {
        return new GcPlan(
                GcReachability.fromBridge(value.getReachability()),
                value.getCandidateNodes(),
                value.getReclaimableCids().stream().map(byte[]::clone).toList(),
                value.getReclaimableNodes(), value.getReclaimableBytes(), value.getMissingCandidates());
    }

    @Override public List<byte[]> reclaimableCids() {
        return reclaimableCids.stream().map(byte[]::clone).toList();
    }
}
