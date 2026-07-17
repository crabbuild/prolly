package build.crab.prolly.javaapi;

/** A read-only view into a native packed page, valid only during its scan callback. */
public final class ScopedBytes {
    private final build.crab.prolly.api.ScopedBytes nativeView;

    ScopedBytes(build.crab.prolly.api.ScopedBytes nativeView) {
        this.nativeView = nativeView;
    }

    public int size() { return nativeView.getSize(); }
    public byte get(int index) { return nativeView.get(index); }
    public byte[] copy() { return nativeView.bytes(); }
}
