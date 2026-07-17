package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;
import java.util.List;
import java.util.Objects;

public final class Engine implements AutoCloseable {
    private build.crab.prolly.api.Engine nativeEngine;

    private Engine(build.crab.prolly.api.Engine nativeEngine) {
        this.nativeEngine = nativeEngine;
    }

    public static Engine memory() {
        return new Engine(JavaPortableBridge.memory());
    }

    private build.crab.prolly.api.Engine open() {
        if (nativeEngine == null) throw new IllegalStateException("prolly engine is closed");
        return nativeEngine;
    }

    public VersionedMap versionedMap(byte[] id) {
        return new VersionedMap(open().versionedMap(id.clone()));
    }

    public IndexRegistry indexRegistry() {
        return new IndexRegistry(open().indexRegistry());
    }

    public IndexedMap indexedMap(byte[] id, IndexRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        return new IndexedMap(open().indexedMap(id.clone(), registry.nativeRegistry()));
    }

    public ProximityMap buildProximity(int dimensions, List<ProximityRecord> records) {
        if (dimensions <= 0) throw new IllegalArgumentException("dimensions must be positive");
        var nativeRecords = records.stream().map(ProximityRecord::toNative).toList();
        return new ProximityMap(JavaPortableBridge.buildProximity(open(), dimensions, nativeRecords));
    }

    @Override
    public void close() {
        if (nativeEngine != null) {
            nativeEngine.close();
            nativeEngine = null;
        }
    }
}
