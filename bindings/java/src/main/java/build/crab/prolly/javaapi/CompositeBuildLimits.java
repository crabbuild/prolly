package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaCompositeBuildLimits;
import build.crab.prolly.api.JavaPortableBridge;

public record CompositeBuildLimits(
        Long maxDiffEntries,
        Long maxOwnedBytes,
        Long maxEncodedOutputBytes,
        Long maxDistanceEvaluations) {
    public static CompositeBuildLimits defaults() {
        var value = JavaPortableBridge.defaultCompositeBuildLimits();
        return new CompositeBuildLimits(
                value.getMaxDiffEntries(), value.getMaxOwnedBytes(),
                value.getMaxEncodedOutputBytes(), value.getMaxDistanceEvaluations());
    }
    JavaCompositeBuildLimits toNative() {
        return new JavaCompositeBuildLimits(
                maxDiffEntries, maxOwnedBytes, maxEncodedOutputBytes, maxDistanceEvaluations);
    }
}
