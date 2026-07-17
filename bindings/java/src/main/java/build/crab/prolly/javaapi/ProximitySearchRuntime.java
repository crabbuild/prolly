package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;

public final class ProximitySearchRuntime implements AutoCloseable {
    private build.crab.prolly.api.ProximitySearchRuntime nativeRuntime;

    ProximitySearchRuntime(build.crab.prolly.api.ProximitySearchRuntime nativeRuntime) {
        this.nativeRuntime = nativeRuntime;
    }

    build.crab.prolly.api.ProximitySearchRuntime open() {
        if (nativeRuntime == null) throw new IllegalStateException("proximity search runtime is closed");
        return nativeRuntime;
    }

    public ProximitySearchRuntimePolicy policy() {
        return ProximitySearchRuntimePolicy.fromNative(
                JavaPortableBridge.proximitySearchRuntimePolicy(open()));
    }

    public ProximitySearchRuntimeStats stats() {
        return ProximitySearchRuntimeStats.fromNative(
                JavaPortableBridge.proximitySearchRuntimeStats(open()));
    }

    public void clear() {
        open().clear();
    }

    @Override
    public void close() {
        if (nativeRuntime != null) {
            nativeRuntime.close();
            nativeRuntime = null;
        }
    }
}
