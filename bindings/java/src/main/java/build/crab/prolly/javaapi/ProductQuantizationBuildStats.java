package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaProductQuantizationBuildStats;

public record ProductQuantizationBuildStats(
        long trainingDistanceEvaluations,
        long encodingDistanceEvaluations,
        long encodedVectors,
        long trainingVectors,
        long trainingBytes,
        long encodedOutputBytes) {
    static ProductQuantizationBuildStats fromNative(JavaProductQuantizationBuildStats value) {
        return new ProductQuantizationBuildStats(
                value.getTrainingDistanceEvaluations(), value.getEncodingDistanceEvaluations(),
                value.getEncodedVectors(), value.getTrainingVectors(), value.getTrainingBytes(),
                value.getEncodedOutputBytes());
    }
}
