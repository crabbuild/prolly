package build.crab.prolly.javaapi;

public record MapUpdate(
        MapUpdateKind kind,
        byte[] previous,
        MapVersion current) {
    public MapUpdate {
        previous = previous == null ? null : previous.clone();
    }

    static MapUpdate fromBridge(build.crab.prolly.api.JavaMapUpdate value) {
        return new MapUpdate(
                MapUpdateKind.valueOf(value.getKind().toUpperCase()),
                value.getPrevious(),
                value.getCurrent() == null ? null : MapVersion.fromNative(value.getCurrent()));
    }

    static MapUpdate fromNative(build.crab.prolly.MapUpdateRecord value) {
        return new MapUpdate(
                MapUpdateKind.valueOf(value.getKind().name()),
                value.getPrevious(),
                value.getCurrent() == null ? null : MapVersion.fromNative(value.getCurrent()));
    }
}
