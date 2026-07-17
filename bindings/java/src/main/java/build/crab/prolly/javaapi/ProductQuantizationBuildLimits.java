package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;
import build.crab.prolly.api.JavaProductQuantizationBuildLimits;

public record ProductQuantizationBuildLimits(
        Long maxTrainingVectors,
        Long maxTrainingBytes,
        Long maxTemporaryCodeBytes,
        Long maxDistanceEvaluations,
        Long maxEncodedOutputBytes,
        Long maxWorkerThreads) {
    public ProductQuantizationBuildLimits {
        requireOptional(maxTrainingVectors, "maxTrainingVectors");
        requireOptional(maxTrainingBytes, "maxTrainingBytes");
        requireOptional(maxTemporaryCodeBytes, "maxTemporaryCodeBytes");
        requireOptional(maxDistanceEvaluations, "maxDistanceEvaluations");
        requireOptional(maxEncodedOutputBytes, "maxEncodedOutputBytes");
        requireOptional(maxWorkerThreads, "maxWorkerThreads");
    }

    public static ProductQuantizationBuildLimits defaults() {
        var value = JavaPortableBridge.defaultPqBuildLimits();
        return fromNative(value);
    }

    static ProductQuantizationBuildLimits fromNative(JavaProductQuantizationBuildLimits value) {
        return new ProductQuantizationBuildLimits(
                value.getMaxTrainingVectors(), value.getMaxTrainingBytes(),
                value.getMaxTemporaryCodeBytes(), value.getMaxDistanceEvaluations(),
                value.getMaxEncodedOutputBytes(), value.getMaxWorkerThreads());
    }

    JavaProductQuantizationBuildLimits toNative() {
        return new JavaProductQuantizationBuildLimits(
                maxTrainingVectors, maxTrainingBytes, maxTemporaryCodeBytes,
                maxDistanceEvaluations, maxEncodedOutputBytes, maxWorkerThreads);
    }

    private static void requireOptional(Long value, String name) {
        if (value != null && value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }
}
