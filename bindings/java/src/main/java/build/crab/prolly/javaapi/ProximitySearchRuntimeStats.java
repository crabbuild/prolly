package build.crab.prolly.javaapi;

public record ProximitySearchRuntimeStats(long physicalReads, long physicalBytesRead) {
    static ProximitySearchRuntimeStats fromNative(
            build.crab.prolly.api.JavaProximitySearchRuntimeStats value) {
        return new ProximitySearchRuntimeStats(
                value.getPhysicalReads(), value.getPhysicalBytesRead());
    }
}
