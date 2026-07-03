package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;

public final class BlobGcReachability {
    private final List<BlobRef> liveBlobs;
    private final long liveBlobCount;
    private final long liveBlobBytes;
    private final long scannedNodes;
    private final long scannedValues;

    BlobGcReachability(BlobGcReachabilityRecord record) {
        this.liveBlobs = blobRefs(record.getLiveBlobs());
        this.liveBlobCount = ProllyJavaAdapters.blobGcReachabilityLiveBlobCount(record);
        this.liveBlobBytes = ProllyJavaAdapters.blobGcReachabilityLiveBlobBytes(record);
        this.scannedNodes = ProllyJavaAdapters.blobGcReachabilityScannedNodes(record);
        this.scannedValues = ProllyJavaAdapters.blobGcReachabilityScannedValues(record);
    }

    public List<BlobRef> liveBlobs() {
        return List.copyOf(liveBlobs);
    }

    public long liveBlobCount() {
        return liveBlobCount;
    }

    public long liveBlobBytes() {
        return liveBlobBytes;
    }

    public long scannedNodes() {
        return scannedNodes;
    }

    public long scannedValues() {
        return scannedValues;
    }

    static List<BlobRef> blobRefs(List<BlobRefRecord> records) {
        List<BlobRef> refs = new ArrayList<>(records.size());
        for (BlobRefRecord record : records) {
            refs.add(new BlobRef(record));
        }
        return List.copyOf(refs);
    }
}
