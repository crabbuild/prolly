package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;
import build.crab.prolly.api.JavaTurboQuantizationConfig;

public record TurboQuantizationConfig(int bitWidth, long rerankMultiplier, long seed) {
    public TurboQuantizationConfig {
        if (bitWidth < 0 || bitWidth > 255) {
            throw new IllegalArgumentException("bitWidth must fit an unsigned 8-bit value");
        }
        if (rerankMultiplier < 0 || rerankMultiplier > 0xffff_ffffL) {
            throw new IllegalArgumentException(
                    "rerankMultiplier must fit an unsigned 32-bit value");
        }
    }

    public static TurboQuantizationConfig defaults() {
        return fromNative(JavaPortableBridge.defaultTurboquantConfig());
    }

    static TurboQuantizationConfig fromNative(JavaTurboQuantizationConfig value) {
        return new TurboQuantizationConfig(
                value.getBitWidth(), value.getRerankMultiplier(), value.getSeed());
    }

    JavaTurboQuantizationConfig toNative() {
        return new JavaTurboQuantizationConfig(bitWidth, rerankMultiplier, seed);
    }
}
