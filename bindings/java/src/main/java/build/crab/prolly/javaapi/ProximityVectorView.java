package build.crab.prolly.javaapi;

/** Callback-scoped, zero-copy view over an exact proximity vector. */
public final class ProximityVectorView {
    private final build.crab.prolly.api.ProximityVectorView nativeView;
    ProximityVectorView(build.crab.prolly.api.ProximityVectorView nativeView) {
        this.nativeView = nativeView;
    }
    public int dimensions() { return nativeView.getDimensions(); }
    public float component(int index) { return nativeView.component(index); }
    public float[] copy() { return nativeView.floats(); }
}
