package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexedMutation;
import build.crab.prolly.api.JavaPortableBridge;
import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import build.crab.prolly.IndexEntryRecord;
import build.crab.prolly.SecondaryIndexExtractorCallback;

public final class IndexedMap implements AutoCloseable {
    private build.crab.prolly.api.IndexedMap nativeMap;
    private final List<SecondaryIndexExtractorCallback> extractors = new ArrayList<>();

    IndexedMap(build.crab.prolly.api.IndexedMap nativeMap) { this.nativeMap = nativeMap; }

    private build.crab.prolly.api.IndexedMap open() {
        if (nativeMap == null) throw new IllegalStateException("indexed map is closed");
        return nativeMap;
    }

    private static List<JavaIndexedMutation> ownMutations(List<IndexedMutation> mutations) {
        return mutations.stream().map(mutation -> new JavaIndexedMutation(
                mutation.kind().name().toLowerCase(java.util.Locale.ROOT),
                mutation.key().clone(),
                mutation.value() == null ? null : mutation.value().clone())).toList();
    }

    public byte[] id() { return JavaPortableBridge.indexedId(open()).clone(); }

    public Optional<byte[]> get(byte[] key) {
        byte[] value = open().get(key.clone());
        return Optional.ofNullable(value == null ? null : value.clone());
    }

    public IndexedVersion put(byte[] key, byte[] value) {
        return IndexedVersion.fromNative(JavaPortableBridge.putIndexed(open(), key.clone(), value.clone()));
    }

    public IndexedVersion apply(List<IndexedMutation> mutations) {
        return IndexedVersion.fromNative(JavaPortableBridge.applyIndexed(open(), ownMutations(mutations)));
    }

    public IndexedUpdate applyIf(byte[] expectedSource, List<IndexedMutation> mutations) {
        return IndexedUpdate.fromNative(JavaPortableBridge.applyIndexedIf(
                open(), expectedSource == null ? null : expectedSource.clone(), ownMutations(mutations)));
    }

    public IndexedVersion delete(byte[] key) {
        return IndexedVersion.fromNative(JavaPortableBridge.deleteIndexed(open(), key.clone()));
    }

    public IndexBuildResult ensureIndex(byte[] name) {
        return IndexBuildResult.fromNative(JavaPortableBridge.ensureIndex(open(), name.clone()));
    }

    public IndexedSnapshot snapshot() { return new IndexedSnapshot(open().snapshot()); }

    public IndexedSnapshot snapshotAt(byte[] sourceVersion) {
        return new IndexedSnapshot(JavaPortableBridge.snapshotAt(open(), sourceVersion.clone()));
    }

    public IndexedSnapshot snapshotById(IndexedSnapshotId id) {
        return new IndexedSnapshot(JavaPortableBridge.snapshotById(open(), id.toNative()));
    }

    public IndexedMapHealth health() {
        return IndexedMapHealth.fromNative(JavaPortableBridge.indexedHealth(open()));
    }

    public IndexedMapMetrics metrics() {
        return IndexedMapMetrics.fromNative(JavaPortableBridge.indexedMetrics(open()));
    }

    public long buildAttempts() { return metrics().buildAttempts(); }

    public IndexVerification verifyIndex(byte[] name, byte[] sourceVersion) {
        return IndexVerification.fromNative(JavaPortableBridge.verifyIndex(
                open(), name.clone(), sourceVersion.clone()));
    }

    public List<IndexVerification> verifyAll(byte[] sourceVersion) {
        return JavaPortableBridge.verifyAll(open(), sourceVersion.clone()).stream()
                .map(IndexVerification::fromNative).toList();
    }

    public IndexVerification repairIndex(byte[] name, byte[] sourceVersion) {
        return IndexVerification.fromNative(JavaPortableBridge.repairIndex(
                open(), name.clone(), sourceVersion.clone()));
    }

    public IndexBuildResult replaceIndex(
            byte[] name,
            long generation,
            String extractorId,
            IndexProjection projection,
            IndexExtractor extractor) {
        return replaceIndex(name, generation, extractorId, projection, null, extractor);
    }

    public IndexBuildResult replaceIndex(
            byte[] name,
            long generation,
            String extractorId,
            IndexProjection projection,
            SecondaryIndexLimits limits,
            IndexExtractor extractor) {
        Objects.requireNonNull(extractor, "extractor");
        SecondaryIndexExtractorCallback callback = (primaryKey, sourceValue) ->
                extractor.extract(primaryKey.clone(), sourceValue.clone()).stream()
                        .map(entry -> new IndexEntryRecord(entry.term(), entry.projection()))
                        .toList();
        extractors.add(callback);
        return IndexBuildResult.fromNative(JavaPortableBridge.replaceIndex(
                open(), name.clone(), generation, extractorId,
                Objects.requireNonNull(projection).nativeValue,
                limits == null ? null : limits.toNative(), callback));
    }

    public IndexedVersion deactivateIndex(byte[] name) {
        return IndexedVersion.fromNative(JavaPortableBridge.deactivateIndex(open(), name.clone()));
    }

    public byte[] exportCurrent() { return open().exportCurrent().clone(); }

    public IndexedVersion importCurrent(byte[] bundle, byte[] expectedSource) {
        return IndexedVersion.fromNative(JavaPortableBridge.importCurrent(
                open(), bundle.clone(), expectedSource == null ? null : expectedSource.clone()));
    }

    public IndexedRetention keepLast(long count) {
        return IndexedRetention.fromNative(JavaPortableBridge.keepLast(open(), count));
    }

    public GcPlan planGc() {
        return GcPlan.fromBridge(JavaPortableBridge.indexedPlanGc(open()));
    }

    public CompletableFuture<Optional<byte[]>> getAsync(byte[] key) {
        var nativeHandle = open(); byte[] ownedKey = key.clone();
        return CompletableFuture.supplyAsync(() -> {
            byte[] value = nativeHandle.get(ownedKey);
            return Optional.ofNullable(value == null ? null : value.clone());
        });
    }

    public CompletableFuture<IndexedVersion> putAsync(byte[] key, byte[] value) {
        var nativeHandle = open(); byte[] ownedKey = key.clone(); byte[] ownedValue = value.clone();
        return CompletableFuture.supplyAsync(() -> IndexedVersion.fromNative(
                JavaPortableBridge.putIndexed(nativeHandle, ownedKey, ownedValue)));
    }

    public CompletableFuture<IndexedVersion> applyAsync(List<IndexedMutation> mutations) {
        var nativeHandle = open(); var owned = ownMutations(mutations);
        return CompletableFuture.supplyAsync(() -> IndexedVersion.fromNative(
                JavaPortableBridge.applyIndexed(nativeHandle, owned)));
    }

    public CompletableFuture<IndexedUpdate> applyIfAsync(
            byte[] expectedSource, List<IndexedMutation> mutations) {
        var nativeHandle = open();
        byte[] ownedExpected = expectedSource == null ? null : expectedSource.clone();
        var owned = ownMutations(mutations);
        return CompletableFuture.supplyAsync(() -> IndexedUpdate.fromNative(
                JavaPortableBridge.applyIndexedIf(nativeHandle, ownedExpected, owned)));
    }

    public CompletableFuture<IndexedVersion> deleteAsync(byte[] key) {
        var nativeHandle = open(); byte[] ownedKey = key.clone();
        return CompletableFuture.supplyAsync(() -> IndexedVersion.fromNative(
                JavaPortableBridge.deleteIndexed(nativeHandle, ownedKey)));
    }

    public CompletableFuture<IndexBuildResult> ensureIndexAsync(byte[] name) {
        var nativeHandle = open(); byte[] ownedName = name.clone();
        return CompletableFuture.supplyAsync(() -> IndexBuildResult.fromNative(
                JavaPortableBridge.ensureIndex(nativeHandle, ownedName)));
    }

    public CompletableFuture<IndexedVersion> deactivateIndexAsync(byte[] name) {
        var nativeHandle = open(); byte[] ownedName = name.clone();
        return CompletableFuture.supplyAsync(() -> IndexedVersion.fromNative(
                JavaPortableBridge.deactivateIndex(nativeHandle, ownedName)));
    }

    public CompletableFuture<IndexedVersion> importCurrentAsync(byte[] bundle, byte[] expectedSource) {
        var nativeHandle = open(); byte[] ownedBundle = bundle.clone();
        byte[] ownedExpected = expectedSource == null ? null : expectedSource.clone();
        return CompletableFuture.supplyAsync(() -> IndexedVersion.fromNative(
                JavaPortableBridge.importCurrent(nativeHandle, ownedBundle, ownedExpected)));
    }

    @Override public void close() {
        if (nativeMap != null) { nativeMap.close(); nativeMap = null; }
    }
}
