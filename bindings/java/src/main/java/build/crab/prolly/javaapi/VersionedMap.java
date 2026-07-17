package build.crab.prolly.javaapi;

import java.util.Optional;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import build.crab.prolly.DiffRecord;
import build.crab.prolly.EntryRecord;
import build.crab.prolly.RangeCursorRecord;
import build.crab.prolly.RangePageRecord;

public final class VersionedMap implements AutoCloseable {
    private build.crab.prolly.api.VersionedMap nativeMap;

    VersionedMap(build.crab.prolly.api.VersionedMap nativeMap) {
        this.nativeMap = nativeMap;
    }

    private build.crab.prolly.api.VersionedMap open() {
        if (nativeMap == null) throw new IllegalStateException("versioned map is closed");
        return nativeMap;
    }

    public MapVersion initialize() {
        return MapVersion.fromNative(open().initialize());
    }

    public MapUpdate initializeSorted(List<MapEntry> entries) {
        return MapUpdate.fromBridge(build.crab.prolly.api.JavaPortableBridge.initializeVersionedSorted(
                open(), entries.stream().map(MapEntry::toBridge).toList()));
    }

    public byte[] id() { return open().getId().clone(); }

    public boolean isInitialized() { return open().isInitialized(); }

    public Optional<MapVersion> head() {
        var value = open().head();
        return Optional.ofNullable(value == null ? null : MapVersion.fromNative(value));
    }

    public Optional<byte[]> headId() {
        byte[] value = open().headId();
        return Optional.ofNullable(value == null ? null : value.clone());
    }

    public Optional<MapVersion> version(byte[] id) {
        var value = open().version(id.clone());
        return Optional.ofNullable(value == null ? null : MapVersion.fromNative(value));
    }

    public List<MapVersion> versions() {
        return open().versions().stream().map(MapVersion::fromNative).toList();
    }

    public Optional<byte[]> get(byte[] key) {
        byte[] value = open().get(key.clone());
        return Optional.ofNullable(value == null ? null : value.clone());
    }

    public boolean containsKey(byte[] key) { return open().containsKey(key.clone()); }

    public List<Optional<byte[]>> getMany(List<byte[]> keys) {
        return open().getMany(keys.stream().map(byte[]::clone).toList()).stream()
                .map(value -> Optional.ofNullable(value == null ? null : value.clone()))
                .toList();
    }

    public Optional<byte[]> getAt(byte[] id, byte[] key) {
        byte[] value = open().getAt(id.clone(), key.clone());
        return Optional.ofNullable(value == null ? null : value.clone());
    }

    public List<Optional<byte[]>> getManyAt(byte[] id, List<byte[]> keys) {
        return open().getManyAt(id.clone(), keys.stream().map(byte[]::clone).toList()).stream()
                .map(value -> Optional.ofNullable(value == null ? null : value.clone()))
                .toList();
    }

    public List<EntryRecord> range(byte[] start, byte[] end) {
        return open().range(start.clone(), end == null ? null : end.clone());
    }

    public List<EntryRecord> prefix(byte[] prefix) {
        return open().prefix(prefix.clone());
    }

    public List<EntryRecord> rangeAt(byte[] id, byte[] start, byte[] end) {
        return open().rangeAt(id.clone(), start.clone(), end == null ? null : end.clone());
    }

    public List<EntryRecord> prefixAt(byte[] id, byte[] prefix) {
        return open().prefixAt(id.clone(), prefix.clone());
    }

    public RangePageRecord rangePage(RangeCursorRecord cursor, byte[] end, long limit) {
        return build.crab.prolly.api.JavaPortableBridge.versionedRangePage(
                open(), cursor, end == null ? null : end.clone(), limit);
    }

    public RangePageRecord prefixPage(byte[] prefix, RangeCursorRecord cursor, long limit) {
        return build.crab.prolly.api.JavaPortableBridge.versionedPrefixPage(
                open(), prefix.clone(), cursor, limit);
    }

    public RangePageRecord rangePageAt(
            byte[] id, RangeCursorRecord cursor, byte[] end, long limit) {
        return build.crab.prolly.api.JavaPortableBridge.versionedRangePageAt(
                open(), id.clone(), cursor, end == null ? null : end.clone(), limit);
    }

    public RangePageRecord prefixPageAt(
            byte[] id, byte[] prefix, RangeCursorRecord cursor, long limit) {
        return build.crab.prolly.api.JavaPortableBridge.versionedPrefixPageAt(
                open(), id.clone(), prefix.clone(), cursor, limit);
    }

    public List<DiffRecord> diff(byte[] base, byte[] target) {
        return open().diff(base.clone(), target.clone());
    }

    public List<DiffRecord> changesSince(byte[] base) {
        return open().changesSince(base.clone());
    }

    public MapVersion rollbackTo(byte[] id) {
        return MapVersion.fromNative(open().rollbackTo(id.clone()));
    }

    public MapVersion put(byte[] key, byte[] value) {
        return MapVersion.fromNative(open().put(key.clone(), value.clone()));
    }

    public MapVersion apply(List<MapMutation> mutations) {
        return MapVersion.fromNative(build.crab.prolly.api.JavaPortableBridge.applyVersioned(
                open(), mutations.stream().map(MapMutation::toBridge).toList()));
    }

    public MapVersion append(List<MapMutation> mutations) {
        return MapVersion.fromNative(build.crab.prolly.api.JavaPortableBridge.appendVersioned(
                open(), mutations.stream().map(MapMutation::toBridge).toList()));
    }

    public VersionedMapBatchResult parallelApply(
            List<MapMutation> mutations, ParallelConfig config) {
        return VersionedMapBatchResult.fromBridge(
                build.crab.prolly.api.JavaPortableBridge.parallelApplyVersioned(
                        open(), mutations.stream().map(MapMutation::toBridge).toList(),
                        config.maxThreads(), config.parallelismThreshold()));
    }

    public MapUpdate rebuildSortedIf(byte[] expected, List<MapEntry> entries) {
        return MapUpdate.fromBridge(build.crab.prolly.api.JavaPortableBridge.rebuildVersionedSortedIf(
                open(), expected == null ? null : expected.clone(),
                entries.stream().map(MapEntry::toBridge).toList()));
    }

    public MapUpdate rebuildFromEntriesIf(byte[] expected, List<MapEntry> entries) {
        return MapUpdate.fromBridge(build.crab.prolly.api.JavaPortableBridge.rebuildVersionedFromEntriesIf(
                open(), expected == null ? null : expected.clone(),
                entries.stream().map(MapEntry::toBridge).toList()));
    }

    public MapUpdate rebuildFromIterIf(byte[] expected, List<MapEntry> entries) {
        return rebuildFromEntriesIf(expected, entries);
    }

    public MapVersion applyAtMillis(List<MapMutation> mutations, long timestampMillis) {
        return MapVersion.fromNative(build.crab.prolly.api.JavaPortableBridge.applyVersionedAtMillis(
                open(), mutations.stream().map(MapMutation::toBridge).toList(), timestampMillis));
    }

    public MapUpdate applyIf(byte[] expected, List<MapMutation> mutations) {
        return MapUpdate.fromBridge(build.crab.prolly.api.JavaPortableBridge.applyVersionedIf(
                open(), expected == null ? null : expected.clone(),
                mutations.stream().map(MapMutation::toBridge).toList()));
    }

    public MapUpdate applyIfAtMillis(
            byte[] expected, List<MapMutation> mutations, long timestampMillis) {
        return MapUpdate.fromBridge(build.crab.prolly.api.JavaPortableBridge.applyVersionedIfAtMillis(
                open(), expected == null ? null : expected.clone(),
                mutations.stream().map(MapMutation::toBridge).toList(), timestampMillis));
    }

    public MapUpdate putIf(byte[] expected, byte[] key, byte[] value) {
        return MapUpdate.fromBridge(build.crab.prolly.api.JavaPortableBridge.putVersionedIf(
                open(), expected == null ? null : expected.clone(), key.clone(), value.clone()));
    }

    public MapUpdate deleteIf(byte[] expected, byte[] key) {
        return MapUpdate.fromBridge(build.crab.prolly.api.JavaPortableBridge.deleteVersionedIf(
                open(), expected == null ? null : expected.clone(), key.clone()));
    }

    public MapVersion delete(byte[] key) {
        return MapVersion.fromNative(open().delete(key.clone()));
    }

    public MapSnapshot snapshot() {
        var snapshot = open().snapshot();
        return snapshot == null ? null : new MapSnapshot(snapshot);
    }

    public MapSnapshot snapshotAt(byte[] id) {
        var snapshot = open().snapshotAt(id.clone());
        return snapshot == null ? null : new MapSnapshot(snapshot);
    }

    public MapComparison compare(byte[] base, byte[] target) {
        return new MapComparison(open().compare(base.clone(), target.clone()));
    }

    public MapComparison compareToHead(byte[] base) {
        return new MapComparison(open().compareToHead(base.clone()));
    }

    public MapSubscription subscribe() { return new MapSubscription(open().subscribe()); }

    public MapSubscription subscribeFrom(byte[] lastSeen) {
        return new MapSubscription(open().subscribeFrom(lastSeen == null ? null : lastSeen.clone()));
    }

    public MapMerge prepareMerge(byte[] base, byte[] candidate) {
        return new MapMerge(open().prepareMerge(base.clone(), candidate.clone()));
    }

    public byte[] backup() { return open().backup().clone(); }
    public MapVersion restoreBackup(byte[] bytes) {
        return MapVersion.fromNative(open().restoreBackup(bytes.clone()));
    }
    public MapVersion importAsHead(build.crab.prolly.SnapshotBundleRecord bundle) {
        return MapVersion.fromNative(open().importAsHead(
                build.crab.prolly.api.JavaPortableBridge.ownedSnapshotBundle(bundle)));
    }
    public MapVersion importAsHead(
            build.crab.prolly.SnapshotBundleRecord bundle, long timestampMillis) {
        return MapVersion.fromNative(build.crab.prolly.api.JavaPortableBridge.importAsHeadAtMillis(
                open(), bundle, timestampMillis));
    }
    public build.crab.prolly.MapCatalogVerificationRecord verifyCatalog() {
        return open().verifyCatalog();
    }
    public long catalogVersionCount() {
        return build.crab.prolly.api.JavaPortableBridge.versionCount(verifyCatalog());
    }
    public GcPlan planGc() {
        return GcPlan.fromBridge(build.crab.prolly.api.JavaPortableBridge.versionedPlanGc(open()));
    }
    public GcSweep sweepGc() {
        return GcSweep.fromBridge(build.crab.prolly.api.JavaPortableBridge.versionedSweepGc(open()));
    }
    public VersionPrune keepLast(long count) {
        return VersionPrune.fromBridge(build.crab.prolly.api.JavaPortableBridge.keepLast(open(), count));
    }
    public VersionPrune pruneVersions(long keepLatest) {
        return VersionPrune.fromBridge(build.crab.prolly.api.JavaPortableBridge.pruneVersions(open(), keepLatest));
    }
    public VersionPrune keepForAt(long nowMillis, long maxAgeMillis) {
        return VersionPrune.fromBridge(build.crab.prolly.api.JavaPortableBridge.keepForAt(
                open(), nowMillis, maxAgeMillis));
    }
    public VersionPrune keepFor(long maxAgeMillis) {
        return VersionPrune.fromBridge(build.crab.prolly.api.JavaPortableBridge.keepFor(open(), maxAgeMillis));
    }
    public VersionPrune keepVersions(List<byte[]> ids) {
        return VersionPrune.fromBridge(build.crab.prolly.api.JavaPortableBridge.keepVersions(
                open(), ids.stream().map(byte[]::clone).toList()));
    }
    public build.crab.prolly.NamedRootRetentionRecord retentionPolicy() {
        return open().retentionPolicy();
    }

    public CompletableFuture<MapVersion> initializeAsync() {
        var nativeHandle = open();
        return CompletableFuture.supplyAsync(() -> MapVersion.fromNative(nativeHandle.initialize()));
    }

    public CompletableFuture<Optional<byte[]>> getAsync(byte[] key) {
        var nativeHandle = open();
        byte[] ownedKey = key.clone();
        return CompletableFuture.supplyAsync(() -> {
            byte[] value = nativeHandle.get(ownedKey);
            return Optional.ofNullable(value == null ? null : value.clone());
        });
    }

    public CompletableFuture<Optional<MapVersion>> headAsync() {
        var nativeHandle = open();
        return CompletableFuture.supplyAsync(() -> {
            var value = nativeHandle.head();
            return Optional.ofNullable(value == null ? null : MapVersion.fromNative(value));
        });
    }

    public CompletableFuture<Optional<MapVersion>> versionAsync(byte[] id) {
        var nativeHandle = open(); byte[] ownedId = id.clone();
        return CompletableFuture.supplyAsync(() -> {
            var value = nativeHandle.version(ownedId);
            return Optional.ofNullable(value == null ? null : MapVersion.fromNative(value));
        });
    }

    public CompletableFuture<MapVersion> putAsync(byte[] key, byte[] value) {
        var nativeHandle = open();
        byte[] ownedKey = key.clone();
        byte[] ownedValue = value.clone();
        return CompletableFuture.supplyAsync(
                () -> MapVersion.fromNative(nativeHandle.put(ownedKey, ownedValue)));
    }

    public CompletableFuture<MapVersion> applyAsync(List<MapMutation> mutations) {
        var nativeHandle = open();
        var owned = mutations.stream()
                .map(value -> new MapMutation(value.kind(), value.key(), value.value())).toList();
        return CompletableFuture.supplyAsync(() -> MapVersion.fromNative(
                build.crab.prolly.api.JavaPortableBridge.applyVersioned(
                        nativeHandle, owned.stream().map(MapMutation::toBridge).toList())));
    }

    public CompletableFuture<MapVersion> deleteAsync(byte[] key) {
        var nativeHandle = open();
        byte[] ownedKey = key.clone();
        return CompletableFuture.supplyAsync(() -> MapVersion.fromNative(nativeHandle.delete(ownedKey)));
    }

    public CompletableFuture<MapSnapshot> snapshotAsync() {
        var nativeHandle = open();
        return CompletableFuture.supplyAsync(() -> {
            var value = nativeHandle.snapshot();
            return value == null ? null : new MapSnapshot(value);
        });
    }

    public CompletableFuture<MapSnapshot> snapshotAtAsync(byte[] id) {
        var nativeHandle = open(); byte[] ownedId = id.clone();
        return CompletableFuture.supplyAsync(() -> {
            var value = nativeHandle.snapshotAt(ownedId);
            return value == null ? null : new MapSnapshot(value);
        });
    }

    public CompletableFuture<MapVersion> importAsHeadAsync(
            build.crab.prolly.SnapshotBundleRecord bundle) {
        var nativeHandle = open();
        var owned = build.crab.prolly.api.JavaPortableBridge.ownedSnapshotBundle(bundle);
        return CompletableFuture.supplyAsync(
                () -> MapVersion.fromNative(nativeHandle.importAsHead(owned)));
    }

    public CompletableFuture<MapVersion> importAsHeadAsync(
            build.crab.prolly.SnapshotBundleRecord bundle, long timestampMillis) {
        var nativeHandle = open();
        var owned = build.crab.prolly.api.JavaPortableBridge.ownedSnapshotBundle(bundle);
        return CompletableFuture.supplyAsync(() -> MapVersion.fromNative(
                build.crab.prolly.api.JavaPortableBridge.importAsHeadAtMillis(
                        nativeHandle, owned, timestampMillis)));
    }

    public CompletableFuture<MapSubscription> subscribeAsync() {
        var nativeHandle = open();
        return CompletableFuture.supplyAsync(() -> new MapSubscription(nativeHandle.subscribe()));
    }

    public CompletableFuture<MapSubscription> subscribeFromAsync(byte[] lastSeen) {
        var nativeHandle = open();
        byte[] owned = lastSeen == null ? null : lastSeen.clone();
        return CompletableFuture.supplyAsync(
                () -> new MapSubscription(nativeHandle.subscribeFrom(owned)));
    }

    @Override
    public void close() {
        if (nativeMap != null) {
            nativeMap.close();
            nativeMap = null;
        }
    }
}
