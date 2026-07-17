package build.crab.prolly.javaapi;

public record IndexedMutation(Kind kind, byte[] key, byte[] value) {
    public enum Kind { UPSERT, DELETE }

    public IndexedMutation {
        key = key.clone();
        value = value == null ? null : value.clone();
        if (kind == Kind.UPSERT && value == null) {
            throw new IllegalArgumentException("indexed upsert requires a value");
        }
    }

    public static IndexedMutation upsert(byte[] key, byte[] value) {
        return new IndexedMutation(Kind.UPSERT, key, value);
    }

    public static IndexedMutation delete(byte[] key) {
        return new IndexedMutation(Kind.DELETE, key, null);
    }
}
