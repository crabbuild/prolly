package build.crab.prolly.javaapi;

public final class BytesValueCodec implements ValueCodec<byte[]> {
    public static final BytesValueCodec INSTANCE = new BytesValueCodec();

    private BytesValueCodec() {}

    @Override public byte[] encode(byte[] value) { return value.clone(); }
    @Override public byte[] decode(byte[] bytes) { return bytes.clone(); }
}
