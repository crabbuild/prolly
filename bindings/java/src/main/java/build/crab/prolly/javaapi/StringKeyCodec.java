package build.crab.prolly.javaapi;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

public final class StringKeyCodec implements KeyCodec<String> {
    public static final StringKeyCodec INSTANCE = new StringKeyCodec();

    private StringKeyCodec() {}

    @Override
    public byte[] encodeKey(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String decodeKey(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException error) {
            throw new IllegalArgumentException("key is not valid UTF-8", error);
        }
    }
}
