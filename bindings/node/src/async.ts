import {
  loadNative,
  type NativeBatchApplyResultRecord,
  type NativeBlobGcPlanRecord,
  type NativeBlobGcReachabilityRecord,
  type NativeBlobGcSweepRecord,
  type NativeBlobRefRecord,
  type NativeCacheStatsRecord,
  type NativeChangedSpanHintRecord,
  type NativeChangedSpanRecord,
  type NativeConflictPageRecord,
  type NativeCursorWindowRecord,
  type NativeCrdtConfigRecord,
  type NativeCrdtResolver,
  type NativeDiffPageRecord,
  type NativeDiffRecord,
  type NativeEntryRecord,
  type NativeGcPlanRecord,
  type NativeGcReachabilityRecord,
  type NativeGcSweepRecord,
  type NativeLargeValueConfigRecord,
  type NativeKeyProofRecord,
  type NativeMergeResolver,
  type NativeMergeExplanationRecord,
  type NativeMergePolicyRegistry,
  type NativeMetricsRecord,
  type NativeMissingNodeCopyRecord,
  type NativeMissingNodePlanRecord,
  type NativeModule,
  type NativeMultiKeyProofRecord,
  type NativeMutationRecord,
  type NativeNamedRootManifestRecord,
  type NativeNamedRootRecord,
  type NativeNamedRootRetentionRecord,
  type NativeNamedRootSelectionRecord,
  type NativeNamedRootUpdateRecord,
  type NativeParallelConfigRecord,
  type NativeProvedRangePageRecord,
  type NativeProllyBlobStore,
  type NativeProllyEngine,
  type NativeRangeCursorRecord,
  type NativeRangePageRecord,
  type NativeRangeProofRecord,
  type NativeReverseCursorRecord,
  type NativeReversePageRecord,
  type NativeSnapshotNamespaceRecord,
  type NativeSnapshotBundleRecord,
  type NativeSnapshotRecord,
  type NativeSnapshotSelectionRecord,
  type NativeStatsComparisonRecord,
  type NativeStructuralDiffCursorRecord,
  type NativeStructuralDiffPageRecord,
  type NativeTreeDebugComparisonRecord,
  type NativeTreeDebugViewRecord,
  type NativeTreeStatsRecord,
  type NativeTreeRecord,
  type NativeValueRefRecord,
} from "./native.ts";

export class AsyncProllyBlobStore {
  readonly inner: NativeProllyBlobStore;

  private constructor(inner: NativeProllyBlobStore) {
    this.inner = inner;
  }

  static async memory(): Promise<AsyncProllyBlobStore> {
    const native = await loadNative();
    return new AsyncProllyBlobStore(native.NativeProllyBlobStore.memory());
  }

  static async file(path: string): Promise<AsyncProllyBlobStore> {
    const native = await loadNative();
    return new AsyncProllyBlobStore(native.NativeProllyBlobStore.file(path));
  }

  static async fromNative(store: NativeProllyBlobStore): Promise<AsyncProllyBlobStore> {
    return new AsyncProllyBlobStore(store);
  }

  putBlob(bytes: Uint8Array): Promise<NativeBlobRefRecord> {
    return defer(() => this.inner.putBlob(bytes));
  }

  getBlob(reference: NativeBlobRefRecord): Promise<Uint8Array | null> {
    return defer(() => this.inner.getBlob(reference));
  }

  deleteBlob(reference: NativeBlobRefRecord): Promise<void> {
    return defer(() => this.inner.deleteBlob(reference));
  }

  listBlobRefs(): Promise<NativeBlobRefRecord[]> {
    return defer(() => this.inner.listBlobRefs());
  }

  blobCount(): Promise<string> {
    return defer(() => this.inner.blobCount());
  }
}

export class AsyncMergePolicyRegistry {
  readonly inner: NativeMergePolicyRegistry;

  private constructor(inner: NativeMergePolicyRegistry) {
    this.inner = inner;
  }

  static async create(): Promise<AsyncMergePolicyRegistry> {
    const native = await loadNative();
    return new AsyncMergePolicyRegistry(new native.NativeMergePolicyRegistry());
  }

  static async fromNative(policy: NativeMergePolicyRegistry): Promise<AsyncMergePolicyRegistry> {
    return new AsyncMergePolicyRegistry(policy);
  }

  len(): Promise<string> {
    return defer(() => this.inner.len());
  }

  isEmpty(): Promise<boolean> {
    return defer(() => this.inner.isEmpty());
  }

  hasDefault(): Promise<boolean> {
    return defer(() => this.inner.hasDefault());
  }

  setDefaultResolverName(name: string): Promise<void> {
    return defer(() => this.inner.setDefaultResolverName(name));
  }

  setDefaultResolver(resolver: NativeMergeResolver): Promise<void> {
    return defer(() => this.inner.setDefaultResolver(resolver));
  }

  pushPrefixResolverName(prefix: Uint8Array, name: string): Promise<void> {
    return defer(() => this.inner.pushPrefixResolverName(prefix, name));
  }

  pushPrefixResolver(prefix: Uint8Array, resolver: NativeMergeResolver): Promise<void> {
    return defer(() => this.inner.pushPrefixResolver(prefix, resolver));
  }

  pushExactResolverName(key: Uint8Array, name: string): Promise<void> {
    return defer(() => this.inner.pushExactResolverName(key, name));
  }

  pushExactResolver(key: Uint8Array, resolver: NativeMergeResolver): Promise<void> {
    return defer(() => this.inner.pushExactResolver(key, resolver));
  }
}

export class AsyncProllyEngine {
  readonly inner: NativeProllyEngine;

  private constructor(inner: NativeProllyEngine) {
    this.inner = inner;
  }

  static async memory(): Promise<AsyncProllyEngine> {
    const native = await loadNative();
    return new AsyncProllyEngine(native.NativeProllyEngine.memory());
  }

  static async memoryWithConfigJson(configJson: string): Promise<AsyncProllyEngine> {
    const native = await loadNative();
    return new AsyncProllyEngine(native.NativeProllyEngine.memoryWithConfigJson(configJson));
  }

  static async file(path: string): Promise<AsyncProllyEngine> {
    const native = await loadNative();
    return new AsyncProllyEngine(native.NativeProllyEngine.file(path));
  }

  static async fileWithConfigJson(path: string, configJson: string): Promise<AsyncProllyEngine> {
    const native = await loadNative();
    return new AsyncProllyEngine(native.NativeProllyEngine.fileWithConfigJson(path, configJson));
  }

  static async sqlite(path: string): Promise<AsyncProllyEngine> {
    const native = await loadNative();
    return new AsyncProllyEngine(native.NativeProllyEngine.sqlite(path));
  }

  static async sqliteWithConfigJson(path: string, configJson: string): Promise<AsyncProllyEngine> {
    const native = await loadNative();
    return new AsyncProllyEngine(native.NativeProllyEngine.sqliteWithConfigJson(path, configJson));
  }

  static async sqliteInMemory(): Promise<AsyncProllyEngine> {
    const native = await loadNative();
    return new AsyncProllyEngine(native.NativeProllyEngine.sqliteInMemory());
  }

  static async sqliteInMemoryWithConfigJson(configJson: string): Promise<AsyncProllyEngine> {
    const native = await loadNative();
    return new AsyncProllyEngine(native.NativeProllyEngine.sqliteInMemoryWithConfigJson(configJson));
  }

  static async fromNative(engine: NativeProllyEngine): Promise<AsyncProllyEngine> {
    return new AsyncProllyEngine(engine);
  }

  create(): Promise<NativeTreeRecord> {
    return defer(() => this.inner.create());
  }

  put(tree: NativeTreeRecord, key: Uint8Array, value: Uint8Array): Promise<NativeTreeRecord> {
    return defer(() => this.inner.put(tree, key, value));
  }

  delete(tree: NativeTreeRecord, key: Uint8Array): Promise<NativeTreeRecord> {
    return defer(() => this.inner.delete(tree, key));
  }

  get(tree: NativeTreeRecord, key: Uint8Array): Promise<Uint8Array | null> {
    return defer(() => this.inner.get(tree, key));
  }

  getValueRef(tree: NativeTreeRecord, key: Uint8Array): Promise<NativeValueRefRecord | null> {
    return defer(() => this.inner.getValueRef(tree, key));
  }

  getLargeValue(
    blobStore: NativeProllyBlobStore | AsyncProllyBlobStore,
    tree: NativeTreeRecord,
    key: Uint8Array,
  ): Promise<Uint8Array | null> {
    return defer(() => this.inner.getLargeValue(unwrapBlobStore(blobStore), tree, key));
  }

  putLargeValue(
    blobStore: NativeProllyBlobStore | AsyncProllyBlobStore,
    tree: NativeTreeRecord,
    key: Uint8Array,
    value: Uint8Array,
    config: NativeLargeValueConfigRecord,
  ): Promise<NativeTreeRecord> {
    return defer(() => this.inner.putLargeValue(unwrapBlobStore(blobStore), tree, key, value, config));
  }

  getMany(tree: NativeTreeRecord, keys: Uint8Array[]): Promise<Array<Uint8Array | null>> {
    return defer(() => this.inner.getMany(tree, keys));
  }

  proveKey(tree: NativeTreeRecord, key: Uint8Array): Promise<NativeKeyProofRecord> {
    return defer(() => this.inner.proveKey(tree, key));
  }

  proveKeys(tree: NativeTreeRecord, keys: Uint8Array[]): Promise<NativeMultiKeyProofRecord> {
    return defer(() => this.inner.proveKeys(tree, keys));
  }

  proveRange(tree: NativeTreeRecord, start: Uint8Array, end?: Uint8Array | null): Promise<NativeRangeProofRecord> {
    return defer(() => this.inner.proveRange(tree, start, end));
  }

  provePrefix(tree: NativeTreeRecord, prefix: Uint8Array): Promise<NativeRangeProofRecord> {
    return defer(() => this.inner.provePrefix(tree, prefix));
  }

  proveRangePage(
    tree: NativeTreeRecord,
    cursor?: NativeRangeCursorRecord | null,
    end?: Uint8Array | null,
    limit = "1024",
  ): Promise<NativeProvedRangePageRecord> {
    return defer(() => this.inner.proveRangePage(tree, cursor, end, limit));
  }

  batch(tree: NativeTreeRecord, mutations: NativeMutationRecord[]): Promise<NativeTreeRecord> {
    return defer(() => this.inner.batch(tree, mutations));
  }

  batchWithStats(tree: NativeTreeRecord, mutations: NativeMutationRecord[]): Promise<NativeBatchApplyResultRecord> {
    return defer(() => this.inner.batchWithStats(tree, mutations));
  }

  parallelBatch(
    tree: NativeTreeRecord,
    mutations: NativeMutationRecord[],
    config: NativeParallelConfigRecord,
  ): Promise<NativeTreeRecord> {
    return defer(() => this.inner.parallelBatch(tree, mutations, config));
  }

  parallelBatchWithStats(
    tree: NativeTreeRecord,
    mutations: NativeMutationRecord[],
    config: NativeParallelConfigRecord,
  ): Promise<NativeBatchApplyResultRecord> {
    return defer(() => this.inner.parallelBatchWithStats(tree, mutations, config));
  }

  buildFromEntries(entries: NativeEntryRecord[]): Promise<NativeTreeRecord> {
    return defer(() => this.inner.buildFromEntries(entries));
  }

  buildFromSortedEntries(entries: NativeEntryRecord[]): Promise<NativeTreeRecord> {
    return defer(() => this.inner.buildFromSortedEntries(entries));
  }

  appendBatch(tree: NativeTreeRecord, mutations: NativeMutationRecord[]): Promise<NativeTreeRecord> {
    return defer(() => this.inner.appendBatch(tree, mutations));
  }

  appendBatchWithStats(tree: NativeTreeRecord, mutations: NativeMutationRecord[]): Promise<NativeBatchApplyResultRecord> {
    return defer(() => this.inner.appendBatchWithStats(tree, mutations));
  }

  range(tree: NativeTreeRecord, start: Uint8Array, end?: Uint8Array | null): Promise<NativeEntryRecord[]> {
    return defer(() => this.inner.range(tree, start, end));
  }

  prefix(tree: NativeTreeRecord, prefix: Uint8Array): Promise<NativeEntryRecord[]> {
    return defer(() => this.inner.prefix(tree, prefix));
  }

  prefixPage(
    tree: NativeTreeRecord,
    prefix: Uint8Array,
    cursor?: NativeRangeCursorRecord | null,
    limit = "1024",
  ): Promise<NativeRangePageRecord> {
    return defer(() => this.inner.prefixPage(tree, prefix, cursor, limit));
  }

  prefixReversePage(
    tree: NativeTreeRecord,
    prefix: Uint8Array,
    cursor?: NativeReverseCursorRecord | null,
    limit = "1024",
  ): Promise<NativeReversePageRecord> {
    return defer(() => this.inner.prefixReversePage(tree, prefix, cursor, limit));
  }

  rangeAfter(tree: NativeTreeRecord, afterKey: Uint8Array, end?: Uint8Array | null): Promise<NativeEntryRecord[]> {
    return defer(() => this.inner.rangeAfter(tree, afterKey, end));
  }

  rangeFromCursor(
    tree: NativeTreeRecord,
    cursor?: NativeRangeCursorRecord | null,
    end?: Uint8Array | null,
  ): Promise<NativeEntryRecord[]> {
    return defer(() => this.inner.rangeFromCursor(tree, cursor, end));
  }

  firstEntry(tree: NativeTreeRecord): Promise<NativeEntryRecord | null> {
    return defer(() => this.inner.firstEntry(tree));
  }

  lastEntry(tree: NativeTreeRecord): Promise<NativeEntryRecord | null> {
    return defer(() => this.inner.lastEntry(tree));
  }

  lowerBound(tree: NativeTreeRecord, key: Uint8Array): Promise<NativeEntryRecord | null> {
    return defer(() => this.inner.lowerBound(tree, key));
  }

  upperBound(tree: NativeTreeRecord, key: Uint8Array): Promise<NativeEntryRecord | null> {
    return defer(() => this.inner.upperBound(tree, key));
  }

  rangePage(
    tree: NativeTreeRecord,
    cursor?: NativeRangeCursorRecord | null,
    end?: Uint8Array | null,
    limit = "1024",
  ): Promise<NativeRangePageRecord> {
    return defer(() => this.inner.rangePage(tree, cursor, end, limit));
  }

  reversePage(
    tree: NativeTreeRecord,
    cursor?: NativeReverseCursorRecord | null,
    start: Uint8Array = new Uint8Array(),
    limit = "1024",
  ): Promise<NativeReversePageRecord> {
    return defer(() => this.inner.reversePage(tree, cursor, start, limit));
  }

  cursorWindow(
    tree: NativeTreeRecord,
    key: Uint8Array,
    end?: Uint8Array | null,
    limit = "1024",
  ): Promise<NativeCursorWindowRecord> {
    return defer(() => this.inner.cursorWindow(tree, key, end, limit));
  }

  diff(base: NativeTreeRecord, other: NativeTreeRecord): Promise<NativeDiffRecord[]> {
    return defer(() => this.inner.diff(base, other));
  }

  rangeDiff(
    base: NativeTreeRecord,
    other: NativeTreeRecord,
    start: Uint8Array,
    end?: Uint8Array | null,
  ): Promise<NativeDiffRecord[]> {
    return defer(() => this.inner.rangeDiff(base, other, start, end));
  }

  diffFromCursor(
    base: NativeTreeRecord,
    other: NativeTreeRecord,
    cursor?: NativeRangeCursorRecord | null,
    end?: Uint8Array | null,
  ): Promise<NativeDiffRecord[]> {
    return defer(() => this.inner.diffFromCursor(base, other, cursor, end));
  }

  diffPage(
    base: NativeTreeRecord,
    other: NativeTreeRecord,
    cursor?: NativeRangeCursorRecord | null,
    end?: Uint8Array | null,
    limit = "1024",
  ): Promise<NativeDiffPageRecord> {
    return defer(() => this.inner.diffPage(base, other, cursor, end, limit));
  }

  conflictPage(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    cursor?: NativeRangeCursorRecord | null,
    limit = "1024",
  ): Promise<NativeConflictPageRecord> {
    return defer(() => this.inner.conflictPage(base, left, right, cursor, limit));
  }

  merge(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    resolver?: string | null,
  ): Promise<NativeTreeRecord> {
    return defer(() => this.inner.merge(base, left, right, resolver));
  }

  mergeWithResolver(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    resolver: NativeMergeResolver,
  ): Promise<NativeTreeRecord> {
    return defer(() => this.inner.mergeWithResolver(base, left, right, resolver));
  }

  mergeWithPolicy(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    policy: NativeMergePolicyRegistry | AsyncMergePolicyRegistry,
  ): Promise<NativeTreeRecord> {
    return defer(() => this.inner.mergeWithPolicy(base, left, right, unwrapPolicy(policy)));
  }

  crdtMerge(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    config: NativeCrdtConfigRecord,
  ): Promise<NativeTreeRecord> {
    return defer(() => this.inner.crdtMerge(base, left, right, config));
  }

  crdtMergeWithResolver(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    deletePolicy: "delete_wins" | "update_wins",
    resolver: NativeCrdtResolver,
  ): Promise<NativeTreeRecord> {
    return defer(() => this.inner.crdtMergeWithResolver(base, left, right, deletePolicy, resolver));
  }

  mergeExplain(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    resolver?: string | null,
  ): Promise<NativeMergeExplanationRecord> {
    return defer(() => this.inner.mergeExplain(base, left, right, resolver));
  }

  mergeExplainWithResolver(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    resolver: NativeMergeResolver,
  ): Promise<NativeMergeExplanationRecord> {
    return defer(() => this.inner.mergeExplainWithResolver(base, left, right, resolver));
  }

  mergeExplainWithPolicy(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    policy: NativeMergePolicyRegistry | AsyncMergePolicyRegistry,
  ): Promise<NativeMergeExplanationRecord> {
    return defer(() => this.inner.mergeExplainWithPolicy(base, left, right, unwrapPolicy(policy)));
  }

  mergeRange(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    start: Uint8Array,
    end?: Uint8Array | null,
    resolver?: string | null,
  ): Promise<NativeTreeRecord> {
    return defer(() => this.inner.mergeRange(base, left, right, start, end, resolver));
  }

  mergeRangeWithResolver(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    start: Uint8Array,
    end: Uint8Array | null | undefined,
    resolver: NativeMergeResolver,
  ): Promise<NativeTreeRecord> {
    return defer(() => this.inner.mergeRangeWithResolver(base, left, right, start, end, resolver));
  }

  mergeRangeWithPolicy(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    start: Uint8Array,
    end: Uint8Array | null | undefined,
    policy: NativeMergePolicyRegistry | AsyncMergePolicyRegistry,
  ): Promise<NativeTreeRecord> {
    return defer(() => this.inner.mergeRangeWithPolicy(base, left, right, start, end, unwrapPolicy(policy)));
  }

  mergePrefix(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    prefix: Uint8Array,
    resolver?: string | null,
  ): Promise<NativeTreeRecord> {
    return defer(() => this.inner.mergePrefix(base, left, right, prefix, resolver));
  }

  mergePrefixWithResolver(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    prefix: Uint8Array,
    resolver: NativeMergeResolver,
  ): Promise<NativeTreeRecord> {
    return defer(() => this.inner.mergePrefixWithResolver(base, left, right, prefix, resolver));
  }

  mergePrefixWithPolicy(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    prefix: Uint8Array,
    policy: NativeMergePolicyRegistry | AsyncMergePolicyRegistry,
  ): Promise<NativeTreeRecord> {
    return defer(() => this.inner.mergePrefixWithPolicy(base, left, right, prefix, unwrapPolicy(policy)));
  }

  loadNamedRoot(name: Uint8Array): Promise<NativeTreeRecord | null> {
    return defer(() => this.inner.loadNamedRoot(name));
  }

  loadNamedRoots(names: Uint8Array[]): Promise<NativeNamedRootSelectionRecord> {
    return defer(() => this.inner.loadNamedRoots(names));
  }

  loadRetainedNamedRoots(retention: NativeNamedRootRetentionRecord): Promise<NativeNamedRootSelectionRecord> {
    return defer(() => this.inner.loadRetainedNamedRoots(retention));
  }

  listNamedRoots(): Promise<NativeNamedRootRecord[]> {
    return defer(() => this.inner.listNamedRoots());
  }

  listNamedRootManifests(): Promise<NativeNamedRootManifestRecord[]> {
    return defer(() => this.inner.listNamedRootManifests());
  }

  publishNamedRoot(name: Uint8Array, tree: NativeTreeRecord): Promise<void> {
    return defer(() => this.inner.publishNamedRoot(name, tree));
  }

  publishNamedRootAtMillis(name: Uint8Array, tree: NativeTreeRecord, timestampMillis: string): Promise<void> {
    return defer(() => this.inner.publishNamedRootAtMillis(name, tree, timestampMillis));
  }

  deleteNamedRoot(name: Uint8Array): Promise<void> {
    return defer(() => this.inner.deleteNamedRoot(name));
  }

  compareAndSwapNamedRoot(
    name: Uint8Array,
    expected?: NativeTreeRecord | null,
    replacement?: NativeTreeRecord | null,
  ): Promise<NativeNamedRootUpdateRecord> {
    return defer(() => this.inner.compareAndSwapNamedRoot(name, expected, replacement));
  }

  publishSnapshot(namespace: NativeSnapshotNamespaceRecord, id: Uint8Array, tree: NativeTreeRecord): Promise<void> {
    return defer(() => this.inner.publishSnapshot(namespace, id, tree));
  }

  publishSnapshotAtMillis(
    namespace: NativeSnapshotNamespaceRecord,
    id: Uint8Array,
    tree: NativeTreeRecord,
    timestampMillis: string,
  ): Promise<void> {
    return defer(() => this.inner.publishSnapshotAtMillis(namespace, id, tree, timestampMillis));
  }

  loadSnapshot(namespace: NativeSnapshotNamespaceRecord, id: Uint8Array): Promise<NativeTreeRecord | null> {
    return defer(() => this.inner.loadSnapshot(namespace, id));
  }

  loadSnapshots(namespace: NativeSnapshotNamespaceRecord, ids: Uint8Array[]): Promise<NativeSnapshotSelectionRecord> {
    return defer(() => this.inner.loadSnapshots(namespace, ids));
  }

  listSnapshots(namespace: NativeSnapshotNamespaceRecord): Promise<NativeSnapshotRecord[]> {
    return defer(() => this.inner.listSnapshots(namespace));
  }

  deleteSnapshot(namespace: NativeSnapshotNamespaceRecord, id: Uint8Array): Promise<void> {
    return defer(() => this.inner.deleteSnapshot(namespace, id));
  }

  compareAndSwapSnapshot(
    namespace: NativeSnapshotNamespaceRecord,
    id: Uint8Array,
    expected?: NativeTreeRecord | null,
    replacement?: NativeTreeRecord | null,
  ): Promise<NativeNamedRootUpdateRecord> {
    return defer(() => this.inner.compareAndSwapSnapshot(namespace, id, expected, replacement));
  }

  compareAndSwapSnapshotAtMillis(
    namespace: NativeSnapshotNamespaceRecord,
    id: Uint8Array,
    expected: NativeTreeRecord | null | undefined,
    replacement: NativeTreeRecord | null | undefined,
    timestampMillis: string,
  ): Promise<NativeNamedRootUpdateRecord> {
    return defer(() => this.inner.compareAndSwapSnapshotAtMillis(namespace, id, expected, replacement, timestampMillis));
  }

  collectStatsJson(tree: NativeTreeRecord): Promise<string> {
    return defer(() => this.inner.collectStatsJson(tree));
  }

  collectStats(tree: NativeTreeRecord): Promise<NativeTreeStatsRecord> {
    return defer(() => this.inner.collectStats(tree));
  }

  statsDiffJson(before: NativeTreeRecord, after: NativeTreeRecord): Promise<string> {
    return defer(() => this.inner.statsDiffJson(before, after));
  }

  statsDiff(before: NativeTreeRecord, after: NativeTreeRecord): Promise<NativeStatsComparisonRecord> {
    return defer(() => this.inner.statsDiff(before, after));
  }

  debugTreeJson(tree: NativeTreeRecord): Promise<string> {
    return defer(() => this.inner.debugTreeJson(tree));
  }

  debugTree(tree: NativeTreeRecord): Promise<NativeTreeDebugViewRecord> {
    return defer(() => this.inner.debugTree(tree));
  }

  debugTreeText(tree: NativeTreeRecord): Promise<string> {
    return defer(() => this.inner.debugTreeText(tree));
  }

  debugCompareTreesJson(left: NativeTreeRecord, right: NativeTreeRecord): Promise<string> {
    return defer(() => this.inner.debugCompareTreesJson(left, right));
  }

  debugCompareTrees(left: NativeTreeRecord, right: NativeTreeRecord): Promise<NativeTreeDebugComparisonRecord> {
    return defer(() => this.inner.debugCompareTrees(left, right));
  }

  debugCompareTreesText(left: NativeTreeRecord, right: NativeTreeRecord): Promise<string> {
    return defer(() => this.inner.debugCompareTreesText(left, right));
  }

  cacheStats(): Promise<NativeCacheStatsRecord> {
    return defer(() => this.inner.cacheStats());
  }

  clearCache(): Promise<void> {
    return defer(() => this.inner.clearCache());
  }

  pinTreeRoot(tree: NativeTreeRecord): Promise<string> {
    return defer(() => this.inner.pinTreeRoot(tree));
  }

  pinTreePath(tree: NativeTreeRecord, key: Uint8Array): Promise<string> {
    return defer(() => this.inner.pinTreePath(tree, key));
  }

  unpinAllCacheNodes(): Promise<string> {
    return defer(() => this.inner.unpinAllCacheNodes());
  }

  metrics(): Promise<NativeMetricsRecord> {
    return defer(() => this.inner.metrics());
  }

  resetMetrics(): Promise<void> {
    return defer(() => this.inner.resetMetrics());
  }

  publishPrefixPathHint(tree: NativeTreeRecord, prefix: Uint8Array): Promise<boolean> {
    return defer(() => this.inner.publishPrefixPathHint(tree, prefix));
  }

  hydratePrefixPathHint(tree: NativeTreeRecord, prefix: Uint8Array): Promise<boolean> {
    return defer(() => this.inner.hydratePrefixPathHint(tree, prefix));
  }

  publishChangedSpansHint(
    base: NativeTreeRecord,
    changed: NativeTreeRecord,
    spans: NativeChangedSpanRecord[],
  ): Promise<boolean> {
    return defer(() => this.inner.publishChangedSpansHint(base, changed, spans));
  }

  loadChangedSpansHint(base: NativeTreeRecord, changed: NativeTreeRecord): Promise<NativeChangedSpanHintRecord | null> {
    return defer(() => this.inner.loadChangedSpansHint(base, changed));
  }

  structuralDiffPage(
    base: NativeTreeRecord,
    other: NativeTreeRecord,
    cursorJson?: string | null,
    limit = "1024",
  ): Promise<NativeStructuralDiffPageRecord> {
    return defer(() => this.inner.structuralDiffPage(base, other, cursorJson, limit));
  }

  structuralDiffPageWithCursor(
    base: NativeTreeRecord,
    other: NativeTreeRecord,
    cursor?: NativeStructuralDiffCursorRecord | null,
    limit = "1024",
  ): Promise<NativeStructuralDiffPageRecord> {
    return defer(() => this.inner.structuralDiffPageWithCursor(base, other, cursor, limit));
  }

  markReachable(roots: NativeTreeRecord[]): Promise<NativeGcReachabilityRecord> {
    return defer(() => this.inner.markReachable(roots));
  }

  markReachableBlobs(roots: NativeTreeRecord[]): Promise<NativeBlobGcReachabilityRecord> {
    return defer(() => this.inner.markReachableBlobs(roots));
  }

  listNodeCids(): Promise<Uint8Array[]> {
    return defer(() => this.inner.listNodeCids());
  }

  planGc(roots: NativeTreeRecord[], candidateCids: Uint8Array[]): Promise<NativeGcPlanRecord> {
    return defer(() => this.inner.planGc(roots, candidateCids));
  }

  sweepGc(roots: NativeTreeRecord[], candidateCids: Uint8Array[]): Promise<NativeGcSweepRecord> {
    return defer(() => this.inner.sweepGc(roots, candidateCids));
  }

  planStoreGc(roots: NativeTreeRecord[]): Promise<NativeGcPlanRecord> {
    return defer(() => this.inner.planStoreGc(roots));
  }

  sweepStoreGc(roots: NativeTreeRecord[]): Promise<NativeGcSweepRecord> {
    return defer(() => this.inner.sweepStoreGc(roots));
  }

  planStoreGcForRetention(retention: NativeNamedRootRetentionRecord): Promise<NativeGcPlanRecord> {
    return defer(() => this.inner.planStoreGcForRetention(retention));
  }

  sweepStoreGcForRetention(retention: NativeNamedRootRetentionRecord): Promise<NativeGcSweepRecord> {
    return defer(() => this.inner.sweepStoreGcForRetention(retention));
  }

  planBlobGc(
    blobStore: NativeProllyBlobStore | AsyncProllyBlobStore,
    roots: NativeTreeRecord[],
    candidateBlobs: NativeBlobRefRecord[],
  ): Promise<NativeBlobGcPlanRecord> {
    return defer(() => this.inner.planBlobGc(unwrapBlobStore(blobStore), roots, candidateBlobs));
  }

  sweepBlobGc(
    blobStore: NativeProllyBlobStore | AsyncProllyBlobStore,
    roots: NativeTreeRecord[],
    candidateBlobs: NativeBlobRefRecord[],
  ): Promise<NativeBlobGcSweepRecord> {
    return defer(() => this.inner.sweepBlobGc(unwrapBlobStore(blobStore), roots, candidateBlobs));
  }

  planBlobStoreGc(
    blobStore: NativeProllyBlobStore | AsyncProllyBlobStore,
    roots: NativeTreeRecord[],
  ): Promise<NativeBlobGcPlanRecord> {
    return defer(() => this.inner.planBlobStoreGc(unwrapBlobStore(blobStore), roots));
  }

  sweepBlobStoreGc(
    blobStore: NativeProllyBlobStore | AsyncProllyBlobStore,
    roots: NativeTreeRecord[],
  ): Promise<NativeBlobGcSweepRecord> {
    return defer(() => this.inner.sweepBlobStoreGc(unwrapBlobStore(blobStore), roots));
  }

  planMissingNodes(
    tree: NativeTreeRecord,
    destination: NativeProllyEngine | AsyncProllyEngine,
  ): Promise<NativeMissingNodePlanRecord> {
    return defer(() => this.inner.planMissingNodes(tree, unwrapEngine(destination)));
  }

  copyMissingNodes(
    tree: NativeTreeRecord,
    destination: NativeProllyEngine | AsyncProllyEngine,
  ): Promise<NativeMissingNodeCopyRecord> {
    return defer(() => this.inner.copyMissingNodes(tree, unwrapEngine(destination)));
  }

  exportSnapshot(tree: NativeTreeRecord): Promise<NativeSnapshotBundleRecord> {
    return defer(() => this.inner.exportSnapshot(tree));
  }

  importSnapshot(bundle: NativeSnapshotBundleRecord): Promise<NativeTreeRecord> {
    return defer(() => this.inner.importSnapshot(bundle));
  }
}

export async function loadAsyncNative(): Promise<NativeModule> {
  return loadNative();
}

function unwrapBlobStore(blobStore: NativeProllyBlobStore | AsyncProllyBlobStore): NativeProllyBlobStore {
  return blobStore instanceof AsyncProllyBlobStore ? blobStore.inner : blobStore;
}

function unwrapEngine(engine: NativeProllyEngine | AsyncProllyEngine): NativeProllyEngine {
  return engine instanceof AsyncProllyEngine ? engine.inner : engine;
}

function unwrapPolicy(policy: NativeMergePolicyRegistry | AsyncMergePolicyRegistry): NativeMergePolicyRegistry {
  return policy instanceof AsyncMergePolicyRegistry ? policy.inner : policy;
}

function defer<T>(fn: () => T): Promise<T> {
  return Promise.resolve().then(fn);
}
