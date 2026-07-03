package build.crab.prolly;

public final class BlobGcSweep {
    private final BlobGcPlan plan;
    private final long deletedBlobs;
    private final long deletedBlobBytes;

    BlobGcSweep(BlobGcSweepRecord record) {
        this.plan = new BlobGcPlan(record.getPlan());
        this.deletedBlobs = ProllyJavaAdapters.blobGcSweepDeletedBlobs(record);
        this.deletedBlobBytes = ProllyJavaAdapters.blobGcSweepDeletedBlobBytes(record);
    }

    public BlobGcPlan plan() {
        return plan;
    }

    public long deletedBlobs() {
        return deletedBlobs;
    }

    public long deletedBlobBytes() {
        return deletedBlobBytes;
    }
}
