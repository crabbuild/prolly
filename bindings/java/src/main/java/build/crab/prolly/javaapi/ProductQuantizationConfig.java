package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;
import build.crab.prolly.api.JavaProductQuantizationConfig;

public record ProductQuantizationConfig(
        long subquantizers,
        int centroidsPerSubquantizer,
        int trainingIterations,
        long rerankMultiplier,
        long seed,
        long maxTrainingVectors) {
    public ProductQuantizationConfig {
        requireUInt(subquantizers, "subquantizers");
        requireUShort(centroidsPerSubquantizer, "centroidsPerSubquantizer");
        requireUShort(trainingIterations, "trainingIterations");
        requireUInt(rerankMultiplier, "rerankMultiplier");
        if (maxTrainingVectors < 0) {
            throw new IllegalArgumentException("maxTrainingVectors must be non-negative");
        }
    }

    public static ProductQuantizationConfig defaults() {
        return fromNative(JavaPortableBridge.defaultPqConfig());
    }

    static ProductQuantizationConfig fromNative(JavaProductQuantizationConfig value) {
        return new ProductQuantizationConfig(
                value.getSubquantizers(), value.getCentroidsPerSubquantizer(),
                value.getTrainingIterations(), value.getRerankMultiplier(), value.getSeed(),
                value.getMaxTrainingVectors());
    }

    JavaProductQuantizationConfig toNative() {
        return new JavaProductQuantizationConfig(
                subquantizers, centroidsPerSubquantizer, trainingIterations,
                rerankMultiplier, seed, maxTrainingVectors);
    }

    private static void requireUInt(long value, String name) {
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException(name + " must fit an unsigned 32-bit value");
        }
    }

    private static void requireUShort(int value, String name) {
        if (value < 0 || value > 65_535) {
            throw new IllegalArgumentException(name + " must fit an unsigned 16-bit value");
        }
    }
}
