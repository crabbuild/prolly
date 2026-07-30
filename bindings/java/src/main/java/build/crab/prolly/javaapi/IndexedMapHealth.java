package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexedMapHealth;
import java.util.List;
import java.util.Optional;

public record IndexedMapHealth(
        byte[] sourceMapId,
        Optional<byte[]> sourceVersion,
        Optional<byte[]> stateVersion,
        List<ActiveIndexHealth> activeIndexes,
        boolean closureValid,
        long retainedSnapshots,
        long durablePins) {
    static IndexedMapHealth fromNative(JavaIndexedMapHealth value) {
        return new IndexedMapHealth(
                value.getSourceMapId().clone(),
                Optional.ofNullable(value.getSourceVersion()).map(byte[]::clone),
                Optional.ofNullable(value.getStateVersion()).map(byte[]::clone),
                value.getActiveIndexes().stream().map(ActiveIndexHealth::fromNative).toList(),
                value.getClosureValid(), value.getRetainedSnapshots(), value.getDurablePins());
    }
}
