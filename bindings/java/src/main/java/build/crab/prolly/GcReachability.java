package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;

public final class GcReachability {
    private final List<byte[]> liveCids;
    private final long liveNodes;
    private final long liveBytes;
    private final long leafNodes;
    private final long internalNodes;

    GcReachability(GcReachabilityRecord record) {
        this.liveCids = cloneByteArrays(record.getLiveCids());
        this.liveNodes = ProllyJavaAdapters.gcReachabilityLiveNodes(record);
        this.liveBytes = ProllyJavaAdapters.gcReachabilityLiveBytes(record);
        this.leafNodes = ProllyJavaAdapters.gcReachabilityLeafNodes(record);
        this.internalNodes = ProllyJavaAdapters.gcReachabilityInternalNodes(record);
    }

    public List<byte[]> liveCids() {
        return cloneByteArrays(liveCids);
    }

    public long liveNodes() {
        return liveNodes;
    }

    public long liveBytes() {
        return liveBytes;
    }

    public long leafNodes() {
        return leafNodes;
    }

    public long internalNodes() {
        return internalNodes;
    }

    static List<byte[]> cloneByteArrays(List<byte[]> values) {
        List<byte[]> cloned = new ArrayList<>(values.size());
        for (byte[] value : values) {
            cloned.add(value.clone());
        }
        return List.copyOf(cloned);
    }
}
