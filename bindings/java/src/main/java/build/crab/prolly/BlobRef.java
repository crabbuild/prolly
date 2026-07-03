package build.crab.prolly;

import java.util.Arrays;

public final class BlobRef {
    private final byte[] cid;
    private final long len;

    public BlobRef(byte[] cid, long len) {
        this.cid = cid.clone();
        this.len = len;
    }

    BlobRef(BlobRefRecord record) {
        this(record.getCid(), ProllyJavaAdapters.blobRefLen(record));
    }

    BlobRefRecord toRecord() {
        return ProllyJavaAdapters.blobRefRecord(cid.clone(), len);
    }

    public byte[] cid() {
        return cid.clone();
    }

    public long len() {
        return len;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof BlobRef that)) {
            return false;
        }
        return len == that.len && Arrays.equals(cid, that.cid);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(cid) + Long.hashCode(len);
    }
}
