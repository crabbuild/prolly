package build.crab.prolly.javaapi;

public record ProximityConfig(
        int dimensions,
        String metric,
        int logChunkSize,
        long levelHashSeed,
        int minPageBytes,
        int targetPageBytes,
        int maxPageBytes,
        long overflowHashSeed,
        int inlineThresholdBytes,
        Integer scalarQuantizationGroupSize) {}
