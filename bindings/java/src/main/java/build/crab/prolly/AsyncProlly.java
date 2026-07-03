package build.crab.prolly;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public final class AsyncProlly implements AutoCloseable {
    private final Prolly inner;
    private final Executor executor;

    private AsyncProlly(Prolly inner, Executor executor) {
        this.inner = inner;
        this.executor = executor;
    }

    public static CompletableFuture<AsyncProlly> memory() {
        return memory(ForkJoinPool.commonPool());
    }

    public static CompletableFuture<AsyncProlly> memory(ConfigRecord config) {
        return memory(config, ForkJoinPool.commonPool());
    }

    public static CompletableFuture<AsyncProlly> memory(Executor executor) {
        return memory(ProllyKt.defaultConfig(), executor);
    }

    public static CompletableFuture<AsyncProlly> memory(ConfigRecord config, Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return new AsyncProlly(Prolly.memory(config), executor);
            } catch (ProllyBindingException exception) {
                throw new CompletionException(exception);
            }
        }, executor);
    }

    public static CompletableFuture<AsyncProlly> file(Path path) {
        return file(path, ForkJoinPool.commonPool());
    }

    public static CompletableFuture<AsyncProlly> file(Path path, Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return new AsyncProlly(Prolly.file(path), executor);
            } catch (ProllyBindingException exception) {
                throw new CompletionException(exception);
            }
        }, executor);
    }

    public static CompletableFuture<AsyncProlly> sqlite(Path path) {
        return sqlite(path, ForkJoinPool.commonPool());
    }

    public static CompletableFuture<AsyncProlly> sqlite(Path path, Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return new AsyncProlly(Prolly.sqlite(path), executor);
            } catch (ProllyBindingException exception) {
                throw new CompletionException(exception);
            }
        }, executor);
    }

    public static CompletableFuture<AsyncProlly> sqliteInMemory() {
        return sqliteInMemory(ForkJoinPool.commonPool());
    }

    public static CompletableFuture<AsyncProlly> sqliteInMemory(Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return new AsyncProlly(Prolly.sqliteInMemory(), executor);
            } catch (ProllyBindingException exception) {
                throw new CompletionException(exception);
            }
        }, executor);
    }

    public static AsyncProlly wrap(Prolly prolly) {
        return wrap(prolly, ForkJoinPool.commonPool());
    }

    public static AsyncProlly wrap(Prolly prolly, Executor executor) {
        return new AsyncProlly(prolly, executor);
    }

    Prolly inner() {
        return inner;
    }

    public CompletableFuture<TreeRecord> create() {
        return supply(inner::create);
    }

    public CompletableFuture<Optional<byte[]>> get(TreeRecord tree, byte[] key) {
        return supply(() -> inner.get(tree, key));
    }

    public CompletableFuture<Optional<ValueRef>> getValueRef(TreeRecord tree, byte[] key) {
        return supply(() -> inner.getValueRef(tree, key));
    }

    public CompletableFuture<Optional<byte[]>> getLargeValue(AsyncBlobStore blobStore, TreeRecord tree, byte[] key) {
        return supply(() -> inner.getLargeValue(blobStore.inner(), tree, key));
    }

    public CompletableFuture<Optional<byte[]>> getLargeValue(BlobStore blobStore, TreeRecord tree, byte[] key) {
        return supply(() -> inner.getLargeValue(blobStore, tree, key));
    }

    public CompletableFuture<List<byte[]>> getMany(TreeRecord tree, List<byte[]> keys) {
        return supply(() -> inner.getMany(tree, keys));
    }

    public CompletableFuture<KeyProof> proveKey(TreeRecord tree, byte[] key) {
        return supply(() -> inner.proveKey(tree, key));
    }

    public CompletableFuture<MultiKeyProof> proveKeys(TreeRecord tree, List<byte[]> keys) {
        return supply(() -> inner.proveKeys(tree, keys));
    }

    public CompletableFuture<RangeProof> proveRange(TreeRecord tree, byte[] start, Optional<byte[]> end) {
        return supply(() -> inner.proveRange(tree, start, end));
    }

    public CompletableFuture<RangeProof> provePrefix(TreeRecord tree, byte[] prefix) {
        return supply(() -> inner.provePrefix(tree, prefix));
    }

    public CompletableFuture<ProvedRangePage> proveRangePage(
            TreeRecord tree,
            RangeCursorRecord cursor,
            Optional<byte[]> end,
            long limit) {
        return supply(() -> inner.proveRangePage(tree, cursor, end, limit));
    }

    public CompletableFuture<TreeRecord> put(TreeRecord tree, byte[] key, byte[] value) {
        return supply(() -> inner.put(tree, key, value));
    }

    public CompletableFuture<TreeRecord> putLargeValue(
            AsyncBlobStore blobStore,
            TreeRecord tree,
            byte[] key,
            byte[] value,
            LargeValueConfig config) {
        return supply(() -> inner.putLargeValue(blobStore.inner(), tree, key, value, config));
    }

    public CompletableFuture<TreeRecord> putLargeValue(
            BlobStore blobStore,
            TreeRecord tree,
            byte[] key,
            byte[] value,
            LargeValueConfig config) {
        return supply(() -> inner.putLargeValue(blobStore, tree, key, value, config));
    }

    public CompletableFuture<TreeRecord> delete(TreeRecord tree, byte[] key) {
        return supply(() -> inner.delete(tree, key));
    }

    public CompletableFuture<TreeRecord> batch(TreeRecord tree, List<MutationRecord> mutations) {
        return supply(() -> inner.batch(tree, mutations));
    }

    public CompletableFuture<BatchApplyResult> batchWithStats(TreeRecord tree, List<MutationRecord> mutations) {
        return supply(() -> inner.batchWithStats(tree, mutations));
    }

    public CompletableFuture<TreeRecord> buildFromEntries(List<Entry> entries) {
        return supply(() -> inner.buildFromEntries(entries));
    }

    public CompletableFuture<TreeRecord> buildFromSortedEntries(List<Entry> entries) {
        return supply(() -> inner.buildFromSortedEntries(entries));
    }

    public CompletableFuture<TreeRecord> appendBatch(TreeRecord tree, List<MutationRecord> mutations) {
        return supply(() -> inner.appendBatch(tree, mutations));
    }

    public CompletableFuture<BatchApplyResult> appendBatchWithStats(TreeRecord tree, List<MutationRecord> mutations) {
        return supply(() -> inner.appendBatchWithStats(tree, mutations));
    }

    public CompletableFuture<TreeRecord> parallelBatch(
            TreeRecord tree,
            List<MutationRecord> mutations,
            ParallelConfigRecord config) {
        return supply(() -> inner.parallelBatch(tree, mutations, config));
    }

    public CompletableFuture<List<Entry>> range(TreeRecord tree, byte[] start, Optional<byte[]> end) {
        return supply(() -> inner.range(tree, start, end));
    }

    public CompletableFuture<List<Entry>> rangeAfter(TreeRecord tree, byte[] afterKey, Optional<byte[]> end) {
        return supply(() -> inner.rangeAfter(tree, afterKey, end));
    }

    public CompletableFuture<List<Entry>> rangeFromCursor(TreeRecord tree, RangeCursorRecord cursor, Optional<byte[]> end) {
        return supply(() -> inner.rangeFromCursor(tree, cursor, end));
    }

    public CompletableFuture<RangePageRecord> rangePage(
            TreeRecord tree,
            RangeCursorRecord cursor,
            Optional<byte[]> end,
            long limit) {
        return supply(() -> inner.rangePage(tree, cursor, end, limit));
    }

    public CompletableFuture<List<DiffRecord>> diff(TreeRecord base, TreeRecord other) {
        return supply(() -> inner.diff(base, other));
    }

    public CompletableFuture<List<DiffRecord>> rangeDiff(TreeRecord base, TreeRecord other, byte[] start, Optional<byte[]> end) {
        return supply(() -> inner.rangeDiff(base, other, start, end));
    }

    public CompletableFuture<List<DiffRecord>> diffFromCursor(
            TreeRecord base,
            TreeRecord other,
            RangeCursorRecord cursor,
            Optional<byte[]> end) {
        return supply(() -> inner.diffFromCursor(base, other, cursor, end));
    }

    public CompletableFuture<DiffPageRecord> diffPage(
            TreeRecord base,
            TreeRecord other,
            RangeCursorRecord cursor,
            Optional<byte[]> end,
            long limit) {
        return supply(() -> inner.diffPage(base, other, cursor, end, limit));
    }

    public CompletableFuture<ConflictPageRecord> conflictPage(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            RangeCursorRecord cursor,
            long limit) {
        return supply(() -> inner.conflictPage(base, left, right, cursor, limit));
    }

    public CompletableFuture<TreeRecord> merge(TreeRecord base, TreeRecord left, TreeRecord right, String resolver) {
        return supply(() -> inner.merge(base, left, right, resolver));
    }

    public CompletableFuture<TreeRecord> mergeWithResolver(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            MergeResolverCallback resolver) {
        return supply(() -> inner.mergeWithResolver(base, left, right, resolver));
    }

    public CompletableFuture<TreeRecord> mergeWithPolicy(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            MergePolicyRegistry policy) {
        return supply(() -> inner.mergeWithPolicy(base, left, right, policy));
    }

    public CompletableFuture<TreeRecord> crdtMerge(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            CrdtConfigRecord config) {
        return supply(() -> inner.crdtMerge(base, left, right, config));
    }

    public CompletableFuture<TreeRecord> crdtMergeWithResolver(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            CrdtDeletePolicyKind deletePolicy,
            CrdtResolverCallback resolver) {
        return supply(() -> inner.crdtMergeWithResolver(base, left, right, deletePolicy, resolver));
    }

    public CompletableFuture<MergeExplanationRecord> mergeExplain(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            String resolver) {
        return supply(() -> inner.mergeExplain(base, left, right, resolver));
    }

    public CompletableFuture<MergeExplanationRecord> mergeExplainWithResolver(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            MergeResolverCallback resolver) {
        return supply(() -> inner.mergeExplainWithResolver(base, left, right, resolver));
    }

    public CompletableFuture<MergeExplanationRecord> mergeExplainWithPolicy(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            MergePolicyRegistry policy) {
        return supply(() -> inner.mergeExplainWithPolicy(base, left, right, policy));
    }

    public CompletableFuture<TreeRecord> mergeRange(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            byte[] start,
            Optional<byte[]> end,
            String resolver) {
        return supply(() -> inner.mergeRange(base, left, right, start, end, resolver));
    }

    public CompletableFuture<TreeRecord> mergeRangeWithResolver(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            byte[] start,
            Optional<byte[]> end,
            MergeResolverCallback resolver) {
        return supply(() -> inner.mergeRangeWithResolver(base, left, right, start, end, resolver));
    }

    public CompletableFuture<TreeRecord> mergeRangeWithPolicy(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            byte[] start,
            Optional<byte[]> end,
            MergePolicyRegistry policy) {
        return supply(() -> inner.mergeRangeWithPolicy(base, left, right, start, end, policy));
    }

    public CompletableFuture<TreeRecord> mergePrefix(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            byte[] prefix,
            String resolver) {
        return supply(() -> inner.mergePrefix(base, left, right, prefix, resolver));
    }

    public CompletableFuture<TreeRecord> mergePrefixWithResolver(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            byte[] prefix,
            MergeResolverCallback resolver) {
        return supply(() -> inner.mergePrefixWithResolver(base, left, right, prefix, resolver));
    }

    public CompletableFuture<TreeRecord> mergePrefixWithPolicy(
            TreeRecord base,
            TreeRecord left,
            TreeRecord right,
            byte[] prefix,
            MergePolicyRegistry policy) {
        return supply(() -> inner.mergePrefixWithPolicy(base, left, right, prefix, policy));
    }

    public CompletableFuture<Optional<TreeRecord>> loadNamedRoot(byte[] name) {
        return supply(() -> inner.loadNamedRoot(name));
    }

    public CompletableFuture<NamedRootSelectionRecord> loadNamedRoots(List<byte[]> names) {
        return supply(() -> inner.loadNamedRoots(names));
    }

    public CompletableFuture<NamedRootSelectionRecord> loadRetainedNamedRoots(NamedRootRetentionRecord retention) {
        return supply(() -> inner.loadRetainedNamedRoots(retention));
    }

    public CompletableFuture<List<NamedRootRecord>> listNamedRoots() {
        return supply(inner::listNamedRoots);
    }

    public CompletableFuture<List<NamedRootManifest>> listNamedRootManifests() {
        return supply(inner::listNamedRootManifests);
    }

    public CompletableFuture<Void> publishNamedRoot(byte[] name, TreeRecord tree) {
        return run(() -> inner.publishNamedRoot(name, tree));
    }

    public CompletableFuture<Void> publishNamedRootAtMillis(byte[] name, TreeRecord tree, long timestampMillis) {
        return run(() -> inner.publishNamedRootAtMillis(name, tree, timestampMillis));
    }

    public CompletableFuture<Void> deleteNamedRoot(byte[] name) {
        return run(() -> inner.deleteNamedRoot(name));
    }

    public CompletableFuture<NamedRootUpdateRecord> compareAndSwapNamedRoot(
            byte[] name,
            Optional<TreeRecord> expected,
            Optional<TreeRecord> replacement) {
        return supply(() -> inner.compareAndSwapNamedRoot(name, expected, replacement));
    }

    public CompletableFuture<NamedRootUpdateRecord> compareAndSwapNamedRootAtMillis(
            byte[] name,
            Optional<TreeRecord> expected,
            Optional<TreeRecord> replacement,
            long timestampMillis) {
        return supply(() -> inner.compareAndSwapNamedRootAtMillis(name, expected, replacement, timestampMillis));
    }

    public CompletableFuture<Void> publishSnapshot(SnapshotNamespaceRecord namespace, byte[] id, TreeRecord tree) {
        return run(() -> inner.publishSnapshot(namespace, id, tree));
    }

    public CompletableFuture<Void> publishSnapshotAtMillis(
            SnapshotNamespaceRecord namespace,
            byte[] id,
            TreeRecord tree,
            long timestampMillis) {
        return run(() -> inner.publishSnapshotAtMillis(namespace, id, tree, timestampMillis));
    }

    public CompletableFuture<Optional<TreeRecord>> loadSnapshot(SnapshotNamespaceRecord namespace, byte[] id) {
        return supply(() -> inner.loadSnapshot(namespace, id));
    }

    public CompletableFuture<SnapshotSelection> loadSnapshots(SnapshotNamespaceRecord namespace, List<byte[]> ids) {
        return supply(() -> inner.loadSnapshots(namespace, ids));
    }

    public CompletableFuture<List<SnapshotRoot>> listSnapshots(SnapshotNamespaceRecord namespace) {
        return supply(() -> inner.listSnapshots(namespace));
    }

    public CompletableFuture<Void> deleteSnapshot(SnapshotNamespaceRecord namespace, byte[] id) {
        return run(() -> inner.deleteSnapshot(namespace, id));
    }

    public CompletableFuture<NamedRootUpdateRecord> compareAndSwapSnapshot(
            SnapshotNamespaceRecord namespace,
            byte[] id,
            Optional<TreeRecord> expected,
            Optional<TreeRecord> replacement) {
        return supply(() -> inner.compareAndSwapSnapshot(namespace, id, expected, replacement));
    }

    public CompletableFuture<NamedRootUpdateRecord> compareAndSwapSnapshotAtMillis(
            SnapshotNamespaceRecord namespace,
            byte[] id,
            Optional<TreeRecord> expected,
            Optional<TreeRecord> replacement,
            long timestampMillis) {
        return supply(() -> inner.compareAndSwapSnapshotAtMillis(namespace, id, expected, replacement, timestampMillis));
    }

    public CompletableFuture<String> collectStatsJson(TreeRecord tree) {
        return supply(() -> inner.collectStatsJson(tree));
    }

    public CompletableFuture<String> statsDiffJson(TreeRecord before, TreeRecord after) {
        return supply(() -> inner.statsDiffJson(before, after));
    }

    public CompletableFuture<String> debugTreeJson(TreeRecord tree) {
        return supply(() -> inner.debugTreeJson(tree));
    }

    public CompletableFuture<String> debugTreeText(TreeRecord tree) {
        return supply(() -> inner.debugTreeText(tree));
    }

    public CompletableFuture<String> debugCompareTreesJson(TreeRecord left, TreeRecord right) {
        return supply(() -> inner.debugCompareTreesJson(left, right));
    }

    public CompletableFuture<String> debugCompareTreesText(TreeRecord left, TreeRecord right) {
        return supply(() -> inner.debugCompareTreesText(left, right));
    }

    public CompletableFuture<CacheStats> cacheStats() {
        return supply(inner::cacheStats);
    }

    public CompletableFuture<Void> clearCache() {
        return run(inner::clearCache);
    }

    public CompletableFuture<Long> pinTreeRoot(TreeRecord tree) {
        return supply(() -> inner.pinTreeRoot(tree));
    }

    public CompletableFuture<Long> pinTreePath(TreeRecord tree, byte[] key) {
        return supply(() -> inner.pinTreePath(tree, key));
    }

    public CompletableFuture<Long> unpinAllCacheNodes() {
        return supply(inner::unpinAllCacheNodes);
    }

    public CompletableFuture<Metrics> metrics() {
        return supply(inner::metrics);
    }

    public CompletableFuture<Void> resetMetrics() {
        return run(inner::resetMetrics);
    }

    public CompletableFuture<Boolean> publishPrefixPathHint(TreeRecord tree, byte[] prefix) {
        return supply(() -> inner.publishPrefixPathHint(tree, prefix));
    }

    public CompletableFuture<Boolean> hydratePrefixPathHint(TreeRecord tree, byte[] prefix) {
        return supply(() -> inner.hydratePrefixPathHint(tree, prefix));
    }

    public CompletableFuture<Boolean> publishChangedSpansHint(
            TreeRecord base,
            TreeRecord changed,
            List<ChangedSpanRecord> spans) {
        return supply(() -> inner.publishChangedSpansHint(base, changed, spans));
    }

    public CompletableFuture<ChangedSpanHintRecord> loadChangedSpansHint(TreeRecord base, TreeRecord changed) {
        return supply(() -> inner.loadChangedSpansHint(base, changed));
    }

    public CompletableFuture<StructuralDiffPage> structuralDiffPage(
            TreeRecord base,
            TreeRecord other,
            String cursorJson,
            long limit) {
        return supply(() -> inner.structuralDiffPage(base, other, cursorJson, limit));
    }

    public CompletableFuture<GcReachability> markReachable(List<TreeRecord> roots) {
        return supply(() -> inner.markReachable(roots));
    }

    public CompletableFuture<List<byte[]>> listNodeCids() {
        return supply(inner::listNodeCids);
    }

    public CompletableFuture<GcPlan> planGc(List<TreeRecord> roots, List<byte[]> candidateCids) {
        return supply(() -> inner.planGc(roots, candidateCids));
    }

    public CompletableFuture<GcSweep> sweepGc(List<TreeRecord> roots, List<byte[]> candidateCids) {
        return supply(() -> inner.sweepGc(roots, candidateCids));
    }

    public CompletableFuture<GcPlan> planStoreGc(List<TreeRecord> roots) {
        return supply(() -> inner.planStoreGc(roots));
    }

    public CompletableFuture<GcSweep> sweepStoreGc(List<TreeRecord> roots) {
        return supply(() -> inner.sweepStoreGc(roots));
    }

    public CompletableFuture<GcPlan> planStoreGcForRetention(NamedRootRetentionRecord retention) {
        return supply(() -> inner.planStoreGcForRetention(retention));
    }

    public CompletableFuture<GcSweep> sweepStoreGcForRetention(NamedRootRetentionRecord retention) {
        return supply(() -> inner.sweepStoreGcForRetention(retention));
    }

    public CompletableFuture<BlobGcReachability> markReachableBlobs(List<TreeRecord> roots) {
        return supply(() -> inner.markReachableBlobs(roots));
    }

    public CompletableFuture<BlobGcPlan> planBlobGc(
            AsyncBlobStore blobStore,
            List<TreeRecord> roots,
            List<BlobRef> candidateBlobs) {
        return supply(() -> inner.planBlobGc(blobStore.inner(), roots, candidateBlobs));
    }

    public CompletableFuture<BlobGcSweep> sweepBlobGc(
            AsyncBlobStore blobStore,
            List<TreeRecord> roots,
            List<BlobRef> candidateBlobs) {
        return supply(() -> inner.sweepBlobGc(blobStore.inner(), roots, candidateBlobs));
    }

    public CompletableFuture<BlobGcPlan> planBlobStoreGc(AsyncBlobStore blobStore, List<TreeRecord> roots) {
        return supply(() -> inner.planBlobStoreGc(blobStore.inner(), roots));
    }

    public CompletableFuture<BlobGcSweep> sweepBlobStoreGc(AsyncBlobStore blobStore, List<TreeRecord> roots) {
        return supply(() -> inner.sweepBlobStoreGc(blobStore.inner(), roots));
    }

    public CompletableFuture<MissingNodePlan> planMissingNodes(TreeRecord tree, AsyncProlly destination) {
        return supply(() -> inner.planMissingNodes(tree, destination.inner()));
    }

    public CompletableFuture<MissingNodeCopy> copyMissingNodes(TreeRecord tree, AsyncProlly destination) {
        return supply(() -> inner.copyMissingNodes(tree, destination.inner()));
    }

    @Override
    public void close() {
        inner.close();
    }

    private CompletableFuture<Void> run(ThrowingRunnable runnable) {
        return CompletableFuture.runAsync(() -> {
            try {
                runnable.run();
            } catch (Exception exception) {
                throw new CompletionException(exception);
            }
        }, executor);
    }

    private <T> CompletableFuture<T> supply(ThrowingSupplier<T> supplier) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return supplier.get();
            } catch (Exception exception) {
                throw new CompletionException(exception);
            }
        }, executor);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
