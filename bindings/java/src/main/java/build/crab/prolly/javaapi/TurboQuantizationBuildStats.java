package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaTurboQuantizationBuildStats;

public record TurboQuantizationBuildStats(
        long encodedVectors,
        long zeroVectors,
        long transformedComponents,
        long butterflyOperations,
        long inputBytes,
        long encodedOutputBytes,
        long peakTemporaryBytes) {
    static TurboQuantizationBuildStats fromNative(JavaTurboQuantizationBuildStats value) {
        return new TurboQuantizationBuildStats(
                value.getEncodedVectors(), value.getZeroVectors(), value.getTransformedComponents(),
                value.getButterflyOperations(), value.getInputBytes(),
                value.getEncodedOutputBytes(), value.getPeakTemporaryBytes());
    }
}
