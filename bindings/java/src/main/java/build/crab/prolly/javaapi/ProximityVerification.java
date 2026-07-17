package build.crab.prolly.javaapi;

public record ProximityVerification(
        long recordCount,
        long proximityNodeCount,
        long externalVectorCount,
        long quantizedNodeCount,
        long scalarQuantizerCount,
        long overflowPageCount,
        long overflowDirectoryCount,
        int maximumLevel,
        long maximumNodeBytes,
        long distanceChecks) {}
