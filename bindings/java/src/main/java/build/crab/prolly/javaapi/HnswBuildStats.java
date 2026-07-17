package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaHnswBuildStats;

public record HnswBuildStats(
        long records,
        long distanceEvaluations,
        long directedEdges,
        int maximumLevel,
        long ownedBytes,
        long encodedGraphBytes) {
    static HnswBuildStats fromNative(JavaHnswBuildStats value) {
        return new HnswBuildStats(
                value.getRecords(), value.getDistanceEvaluations(), value.getDirectedEdges(),
                value.getMaximumLevel(), value.getOwnedBytes(), value.getEncodedGraphBytes());
    }
}
