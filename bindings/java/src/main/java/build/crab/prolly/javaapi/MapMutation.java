package build.crab.prolly.javaapi;

public record MapMutation(Kind kind, byte[] key, byte[] value) {
    public enum Kind { UPSERT, DELETE }

    public MapMutation {
        key = key.clone();
        value = value == null ? null : value.clone();
        if (kind == Kind.UPSERT && value == null) {
            throw new IllegalArgumentException("map upsert requires a value");
        }
    }

    public static MapMutation upsert(byte[] key, byte[] value) {
        return new MapMutation(Kind.UPSERT, key, value);
    }

    public static MapMutation delete(byte[] key) {
        return new MapMutation(Kind.DELETE, key, null);
    }

    build.crab.prolly.api.JavaMapMutation toBridge() {
        return new build.crab.prolly.api.JavaMapMutation(
                kind.name().toLowerCase(), key.clone(), value == null ? null : value.clone());
    }
}
