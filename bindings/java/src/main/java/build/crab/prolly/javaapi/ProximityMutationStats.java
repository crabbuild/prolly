package build.crab.prolly.javaapi;

public record ProximityMutationStats(
        long directoryEntriesScanned,
        long directoryNodesRead,
        long directoryNodesRebuilt,
        long directoryNodesWritten,
        long directoryNodesReused,
        long directoryLevelsRebuilt,
        boolean directoryRightEdgeRebuilt,
        long nodesRead,
        long nodesWritten,
        long nodesReused,
        long recordsRebuilt,
        long distanceEvaluations,
        boolean fullProximityRebuild) {}
