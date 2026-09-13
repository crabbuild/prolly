package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;
import build.crab.prolly.api.JavaTurboQuantizationBuildLimits;

public record TurboQuantizationBuildLimits(
        Long maxRecords,
        Long maxInputBytes,
        Long maxTemporaryBytes,
        Long maxTransformOperations,
        Long maxEncodedOutputBytes,
        Long maxWorkerThreads) {
    public TurboQuantizationBuildLimits {
        requireOptional(maxRecords, "maxRecords");
        requireOptional(maxInputBytes, "maxInputBytes");
        requireOptional(maxTemporaryBytes, "maxTemporaryBytes");
        requireOptional(maxTransformOperations, "maxTransformOperations");
        requireOptional(maxEncodedOutputBytes, "maxEncodedOutputBytes");
        requireOptional(maxWorkerThreads, "maxWorkerThreads");
    }

    public static TurboQuantizationBuildLimits defaults() {
        return fromNative(JavaPortableBridge.defaultTurboquantBuildLimits());
    }

    static TurboQuantizationBuildLimits fromNative(JavaTurboQuantizationBuildLimits value) {
        return new TurboQuantizationBuildLimits(
                value.getMaxRecords(), value.getMaxInputBytes(), value.getMaxTemporaryBytes(),
                value.getMaxTransformOperations(), value.getMaxEncodedOutputBytes(),
                value.getMaxWorkerThreads());
    }

    JavaTurboQuantizationBuildLimits toNative() {
        return new JavaTurboQuantizationBuildLimits(
                maxRecords, maxInputBytes, maxTemporaryBytes, maxTransformOperations,
                maxEncodedOutputBytes, maxWorkerThreads);
    }

    private static void requireOptional(Long value, String name) {
        if (value != null && value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }
}
