package build.crab.prolly.javaapi;

public interface KeyCodec<K> {
    /** Returns a fresh byte array whose ownership is transferred to the typed map call. */
    byte[] encodeKey(K key);

    K decodeKey(byte[] bytes);
}
