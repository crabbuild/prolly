package build.crab.prolly;

public record HostStoreRootCasResult(boolean applied, RootManifestRecord current) {
    public static HostStoreRootCasResult success() {
        return new HostStoreRootCasResult(true, null);
    }

    public static HostStoreRootCasResult conflict(RootManifestRecord current) {
        return new HostStoreRootCasResult(false, current);
    }
}
