package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaTurboQuantizationVerification;

public record TurboQuantizationVerification(
        long encodedVectors,
        long zeroVectors,
        TurboQuantizationQuality quality) {
    static TurboQuantizationVerification fromNative(JavaTurboQuantizationVerification value) {
        return new TurboQuantizationVerification(
                value.getEncodedVectors(), value.getZeroVectors(),
                TurboQuantizationQuality.fromNative(value.getQuality()));
    }
}
