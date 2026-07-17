package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexedUpdate;
import java.util.Optional;

public record IndexedUpdate(
        IndexedUpdateKind kind,
        Optional<byte[]> previousSourceVersion,
        Optional<IndexedVersion> current) {
    static IndexedUpdate fromNative(JavaIndexedUpdate value) {
        return new IndexedUpdate(
                IndexedUpdateKind.fromWire(value.getKind()),
                Optional.ofNullable(value.getPreviousSourceVersion()).map(byte[]::clone),
                Optional.ofNullable(value.getCurrent()).map(IndexedVersion::fromNative));
    }
}
