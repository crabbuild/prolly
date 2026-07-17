package build.crab.prolly.javaapi;

public interface ValueCodec<V> {
    /** Returns a fresh byte array whose ownership is transferred to the typed map call. */
    byte[] encode(V value);

    V decode(byte[] bytes);
}
