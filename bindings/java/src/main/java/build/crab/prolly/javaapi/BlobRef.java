package build.crab.prolly.javaapi;

public record BlobRef(byte[] cid, long length) {
    public BlobRef {
        cid = cid.clone();
    }

    static BlobRef fromBridge(build.crab.prolly.api.JavaBlobRef value) {
        return new BlobRef(value.getCid(), value.getLen());
    }

    @Override public byte[] cid() { return cid.clone(); }
}
