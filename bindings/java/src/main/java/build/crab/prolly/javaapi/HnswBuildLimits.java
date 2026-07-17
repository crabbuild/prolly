package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaHnswBuildLimits;
import build.crab.prolly.api.JavaPortableBridge;

public record HnswBuildLimits(
        Long maxRecords,
        Long maxOwnedBytes,
        Long maxDistanceEvaluations,
        long workerThreads,
        Long maxEncodedGraphBytes) {
    public HnswBuildLimits {
        requireNonNegative(maxRecords, "maxRecords");
        requireNonNegative(maxOwnedBytes, "maxOwnedBytes");
        requireNonNegative(maxDistanceEvaluations, "maxDistanceEvaluations");
        if (workerThreads < 0) throw new IllegalArgumentException("workerThreads must be non-negative");
        requireNonNegative(maxEncodedGraphBytes, "maxEncodedGraphBytes");
    }

    public static HnswBuildLimits defaults() {
        var value = JavaPortableBridge.defaultHnswBuildLimits();
        return new HnswBuildLimits(
                value.getMaxRecords(), value.getMaxOwnedBytes(), value.getMaxDistanceEvaluations(),
                value.getWorkerThreads(), value.getMaxEncodedGraphBytes());
    }

    JavaHnswBuildLimits toNative() {
        return new JavaHnswBuildLimits(
                maxRecords, maxOwnedBytes, maxDistanceEvaluations, workerThreads,
                maxEncodedGraphBytes);
    }

    private static void requireNonNegative(Long value, String name) {
        if (value != null && value < 0) throw new IllegalArgumentException(name + " must be non-negative");
    }
}
