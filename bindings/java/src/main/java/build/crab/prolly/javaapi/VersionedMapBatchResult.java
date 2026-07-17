package build.crab.prolly.javaapi;

public record VersionedMapBatchResult(MapVersion version, BatchApplyStats stats) {
    static VersionedMapBatchResult fromBridge(build.crab.prolly.api.JavaVersionedMapBatchResult value) {
        return new VersionedMapBatchResult(
                MapVersion.fromNative(value.getVersion()), BatchApplyStats.fromBridge(value.getStats()));
    }
}
