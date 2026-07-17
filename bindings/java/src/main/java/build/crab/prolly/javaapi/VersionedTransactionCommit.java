package build.crab.prolly.javaapi;

import java.util.List;
import java.util.Optional;

public record VersionedTransactionCommit(
        boolean applied,
        List<MapVersion> versions,
        byte[] conflictMapId,
        MapVersion conflictCurrent) {
    static VersionedTransactionCommit fromNative(build.crab.prolly.VersionedTransactionCommitRecord value) {
        return new VersionedTransactionCommit(
                value.getApplied(),
                value.getVersions().stream().map(MapVersion::fromNative).toList(),
                value.getConflictMapId(),
                value.getConflictCurrent() == null ? null : MapVersion.fromNative(value.getConflictCurrent()));
    }
    public VersionedTransactionCommit {
        conflictMapId = conflictMapId == null ? null : conflictMapId.clone();
        versions = List.copyOf(versions);
    }
    @Override public byte[] conflictMapId() { return conflictMapId == null ? null : conflictMapId.clone(); }
    public Optional<MapVersion> conflict() { return Optional.ofNullable(conflictCurrent); }
}
