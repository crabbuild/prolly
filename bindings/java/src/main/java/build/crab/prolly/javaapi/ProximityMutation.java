package build.crab.prolly.javaapi;

public record ProximityMutation(byte[] key, float[] vector, byte[] value) {
    public ProximityMutation {
        key = key.clone();
        vector = vector == null ? null : vector.clone();
        value = value == null ? null : value.clone();
        if ((vector == null) != (value == null)) {
            throw new IllegalArgumentException("vector and value must both be present or absent");
        }
    }

    public static ProximityMutation upsert(byte[] key, float[] vector, byte[] value) {
        return new ProximityMutation(key, vector, value);
    }

    public static ProximityMutation delete(byte[] key) {
        return new ProximityMutation(key, null, null);
    }

    @Override public byte[] key() { return key.clone(); }
    @Override public float[] vector() { return vector == null ? null : vector.clone(); }
    @Override public byte[] value() { return value == null ? null : value.clone(); }
}
