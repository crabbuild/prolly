package build.crab.prolly.javaapi;

public record BlobGcSweep(BlobGcPlan plan, long deletedBlobs, long deletedBlobBytes) {
    static BlobGcSweep fromBridge(build.crab.prolly.api.JavaBlobGcSweep value) {
        return new BlobGcSweep(
                BlobGcPlan.fromBridge(value.getPlan()),
                value.getDeletedBlobs(), value.getDeletedBlobBytes());
    }
}
