package build.crab.prolly.javaapi;

public final class BytesKeyCodec implements KeyCodec<byte[]> {
    public static final BytesKeyCodec INSTANCE = new BytesKeyCodec();

    private BytesKeyCodec() {}

    @Override public byte[] encodeKey(byte[] key) { return key.clone(); }
    @Override public byte[] decodeKey(byte[] bytes) { return bytes.clone(); }
}
