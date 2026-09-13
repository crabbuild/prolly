package build.crab.prolly.javaapi;

import java.util.List;

public record CompositeBuildOrRebuildOutcome(
        Kind kind,
        CompositeAccelerator composite,
        HnswIndex hnsw,
        ProductQuantizer pq,
        TurboQuantizer turboquant,
        List<FullRebuildReason> reasons,
        CompositeBuildStats compositeStats,
        HnswBuildStats hnswStats,
        ProductQuantizationBuildStats pqStats,
        TurboQuantizationBuildStats turboquantStats) {
    public enum Kind {
        COMPOSITE,
        NO_ACCELERATOR_REQUIRED,
        HNSW_REBUILT,
        PRODUCT_QUANTIZED_REBUILT,
        TURBO_QUANTIZED_REBUILT
    }
}
