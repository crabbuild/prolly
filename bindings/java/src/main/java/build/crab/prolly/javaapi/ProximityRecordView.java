package build.crab.prolly.javaapi;

/** Exact proximity record view valid only during the supplying callback. */
public record ProximityRecordView(ProximityVectorView vector, ScopedBytes value) {
    static ProximityRecordView fromNative(build.crab.prolly.api.ProximityRecordView value) {
        return new ProximityRecordView(
                new ProximityVectorView(value.getVector()), new ScopedBytes(value.getValue()));
    }
}
