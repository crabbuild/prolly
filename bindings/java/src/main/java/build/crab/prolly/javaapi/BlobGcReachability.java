package build.crab.prolly.javaapi;

import java.util.List;

public record BlobGcReachability(
        List<BlobRef> liveBlobs,
        long liveBlobCount,
        long liveBlobBytes,
        long scannedNodes,
        long scannedValues) {
    public BlobGcReachability {
        liveBlobs = List.copyOf(liveBlobs);
    }

    static BlobGcReachability fromBridge(build.crab.prolly.api.JavaBlobGcReachability value) {
        return new BlobGcReachability(
                value.getLiveBlobs().stream().map(BlobRef::fromBridge).toList(),
                value.getLiveBlobCount(), value.getLiveBlobBytes(),
                value.getScannedNodes(), value.getScannedValues());
    }
}
