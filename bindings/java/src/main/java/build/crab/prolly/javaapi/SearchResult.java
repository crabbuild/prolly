package build.crab.prolly.javaapi;

import java.util.List;

public record SearchResult(List<Neighbor> neighbors, String completion, String backend) {
    public record Neighbor(byte[] key, byte[] value, double distance) {}
}
