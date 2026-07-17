package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexedMapHealth;
import java.util.List;
import java.util.Optional;

public record IndexedMapHealth(
        byte[] sourceMapId,
        Optional<byte[]> sourceVersion,
        Optional<byte[]> catalogVersion,
        List<ActiveIndexHealth> activeIndexes,
        boolean supportsTransactions) {
    static IndexedMapHealth fromNative(JavaIndexedMapHealth value) {
        return new IndexedMapHealth(
                value.getSourceMapId().clone(),
                Optional.ofNullable(value.getSourceVersion()).map(byte[]::clone),
                Optional.ofNullable(value.getCatalogVersion()).map(byte[]::clone),
                value.getActiveIndexes().stream().map(ActiveIndexHealth::fromNative).toList(),
                value.getSupportsTransactions());
    }
}
