package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexedRetention;
import java.util.List;

public record IndexedRetention(
        List<byte[]> retainedSourceVersions,
        List<byte[]> removedSourceVersions,
        List<byte[]> retainedIndexVersions,
        List<byte[]> removedIndexVersions,
        List<byte[]> removedCatalogVersions,
        long removedCheckpointRecords,
        List<byte[]> removedNamedRoots) {
    private static List<byte[]> copy(List<byte[]> values) {
        return values.stream().map(byte[]::clone).toList();
    }

    static IndexedRetention fromNative(JavaIndexedRetention value) {
        return new IndexedRetention(
                copy(value.getRetainedSourceVersions()), copy(value.getRemovedSourceVersions()),
                copy(value.getRetainedIndexVersions()), copy(value.getRemovedIndexVersions()),
                copy(value.getRemovedCatalogVersions()), value.getRemovedCheckpointRecords(),
                copy(value.getRemovedNamedRoots()));
    }
}
