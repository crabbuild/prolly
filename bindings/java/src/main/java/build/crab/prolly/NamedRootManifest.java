package build.crab.prolly;

public record NamedRootManifest(
        byte[] name,
        RootManifest manifest) {
    public NamedRootManifest {
        name = name.clone();
    }
}
