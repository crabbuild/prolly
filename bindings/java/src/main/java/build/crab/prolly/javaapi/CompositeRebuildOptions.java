package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaCompositeRebuildOptions;
import build.crab.prolly.api.JavaPortableBridge;

public record CompositeRebuildOptions(
        HnswBuildLimits hnswLimits,
        long pqWorkerThreads,
        ProductQuantizationBuildLimits pqLimits) {
    public static CompositeRebuildOptions defaults() {
        var value = JavaPortableBridge.defaultCompositeRebuildOptions();
        return new CompositeRebuildOptions(
                new HnswBuildLimits(
                        value.getHnswLimits().getMaxRecords(), value.getHnswLimits().getMaxOwnedBytes(),
                        value.getHnswLimits().getMaxDistanceEvaluations(),
                        value.getHnswLimits().getWorkerThreads(),
                        value.getHnswLimits().getMaxEncodedGraphBytes()),
                value.getPqWorkerThreads(),
                ProductQuantizationBuildLimits.fromNative(value.getPqLimits()));
    }
    JavaCompositeRebuildOptions toNative() {
        return new JavaCompositeRebuildOptions(
                hnswLimits.toNative(), pqWorkerThreads, pqLimits.toNative());
    }
}
