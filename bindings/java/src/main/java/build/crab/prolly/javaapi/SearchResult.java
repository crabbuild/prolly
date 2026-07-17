package build.crab.prolly.javaapi;

import java.util.List;

public record SearchResult(
        List<Neighbor> neighbors,
        Stats stats,
        String completion,
        String backend,
        int planFormatVersion) {
    public record Neighbor(byte[] key, byte[] value, double distance) {
        public Neighbor {
            key = key.clone();
            value = value.clone();
        }
        @Override public byte[] key() { return key.clone(); }
        @Override public byte[] value() { return value.clone(); }
    }
    public record Stats(
            long levelsVisited,
            long nodesRead,
            long bytesRead,
            long physicalBytesRead,
            long committedBytes,
            long distanceEvaluations,
            long quantizedDistanceEvaluations,
            long rerankedCandidates,
            long frontierPeak,
            long candidateHandlesPeak,
            long candidateRetainedBytesPeak) {}
}
