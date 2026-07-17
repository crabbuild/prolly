package build.crab.prolly.javaapi;

public sealed interface ValueRefView permits ValueRefView.Inline, ValueRefView.Blob {
    record Inline(ScopedBytes value) implements ValueRefView {}

    /** Blob length is the unsigned bit pattern of the Rust u64 length. */
    record Blob(byte[] cid, long length) implements ValueRefView {
        public Blob { cid = cid.clone(); }
        @Override public byte[] cid() { return cid.clone(); }
    }

    static ValueRefView fromNative(build.crab.prolly.api.ValueRefView value) {
        if (value instanceof build.crab.prolly.api.ValueRefView.Inline inline) {
            return new Inline(new ScopedBytes(inline.getValue()));
        }
        if (value instanceof build.crab.prolly.api.ValueRefView.Blob blob) {
            return new Blob(blob.getCid(), blob.getLength());
        }
        throw new IllegalArgumentException("unknown value reference view");
    }
}
