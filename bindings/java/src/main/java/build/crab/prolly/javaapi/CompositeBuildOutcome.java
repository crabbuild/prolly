package build.crab.prolly.javaapi;

import java.util.List;

public record CompositeBuildOutcome(
        CompositeAccelerator accelerator,
        List<FullRebuildReason> reasons,
        CompositeBuildStats stats) {}
