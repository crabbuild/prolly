package build.crab.prolly.javaapi;

import java.util.List;

public record GcReachability(
        List<byte[]> liveCids,
        long liveNodes,
        long liveBytes,
        long leafNodes,
        long internalNodes) {
    static GcReachability fromBridge(build.crab.prolly.api.JavaGcReachability value) {
        return new GcReachability(
                value.getLiveCids().stream().map(byte[]::clone).toList(),
                value.getLiveNodes(), value.getLiveBytes(), value.getLeafNodes(), value.getInternalNodes());
    }

    @Override public List<byte[]> liveCids() {
        return liveCids.stream().map(byte[]::clone).toList();
    }
}
