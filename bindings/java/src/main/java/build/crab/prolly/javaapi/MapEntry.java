package build.crab.prolly.javaapi;

public record MapEntry(byte[] key, byte[] value) {
    public MapEntry {
        key = key.clone();
        value = value.clone();
    }

    build.crab.prolly.api.JavaMapEntry toBridge() {
        return new build.crab.prolly.api.JavaMapEntry(key.clone(), value.clone());
    }
}
