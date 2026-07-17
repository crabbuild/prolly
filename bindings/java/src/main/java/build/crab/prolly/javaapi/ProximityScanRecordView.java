package build.crab.prolly.javaapi;

/** Callback-scoped zero-copy view over one bounded proximity-map record. */
public record ProximityScanRecordView(
        ScopedBytes key, ProximityVectorView vector, ScopedBytes value) {
    static ProximityScanRecordView fromNative(build.crab.prolly.api.ProximityScanRecordView value) {
        return new ProximityScanRecordView(
                new ScopedBytes(value.getKey()),
                new ProximityVectorView(value.getVector()),
                new ScopedBytes(value.getValue()));
    }
}
