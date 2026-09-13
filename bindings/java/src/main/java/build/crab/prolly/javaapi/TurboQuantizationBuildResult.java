package build.crab.prolly.javaapi;

public record TurboQuantizationBuildResult(
        TurboQuantizer index,
        TurboQuantizationBuildStats stats) {}
