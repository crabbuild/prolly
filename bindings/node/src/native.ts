export interface NativeTreeRecord {
  root?: Uint8Array | null;
  config?: NativeConfigRecord | null;
}

export interface NativeEntryRecord {
  key: Uint8Array;
  value: Uint8Array;
}

export interface NativeDiffRecord {
  kind: string;
  key: Uint8Array;
  value?: Uint8Array | null;
  old?: Uint8Array | null;
  newValue?: Uint8Array | null;
}

export interface NativeMutationRecord {
  kind: "upsert" | "delete";
  key: Uint8Array;
  value?: Uint8Array | null;
}

export interface NativeEncodingRecord {
  kind: "raw" | "cbor" | "json" | "custom";
  customName?: string | null;
}

export interface NativeConfigRecord {
  minChunkSize: string;
  maxChunkSize: string;
  chunkingFactor: number;
  hashSeed: string;
  encoding: NativeEncodingRecord;
  nodeCacheMaxNodes?: string | null;
  nodeCacheMaxBytes?: string | null;
}

export interface NativeParallelConfigRecord {
  maxThreads: string;
  parallelismThreshold: string;
}

export interface NativeBatchApplyStatsRecord {
  inputMutations: string;
  effectiveMutations: string;
  preprocessInputSorted: boolean;
  affectedLeaves: string;
  changedLeaves: string;
  sparseLeafApplies: string;
  writtenNodes: string;
  writtenBytes: string;
  usedAppendFastPath: boolean;
  usedBatchedRoute: boolean;
  usedCoalescedRebuild: boolean;
  usedDeferredRebalancing: boolean;
  usedBottomUpRebuild: boolean;
  cacheWrittenNodes: boolean;
}

export interface NativeBatchApplyResultRecord {
  tree: NativeTreeRecord;
  stats: NativeBatchApplyStatsRecord;
}

export interface NativeRangeCursorRecord {
  afterKey?: Uint8Array | null;
}

export interface NativeReverseCursorRecord {
  beforeKey?: Uint8Array | null;
}

export interface NativeRangeBoundsRecord {
  start: Uint8Array;
  end?: Uint8Array | null;
}

export interface NativeRangePageRecord {
  entries: NativeEntryRecord[];
  nextCursor?: NativeRangeCursorRecord | null;
}

export interface NativeReversePageRecord {
  entries: NativeEntryRecord[];
  nextCursor?: NativeReverseCursorRecord | null;
}

export interface NativeCursorWindowRecord {
  positionKey?: Uint8Array | null;
  positionValue?: Uint8Array | null;
  found: boolean;
  entries: NativeEntryRecord[];
  nextCursor?: NativeRangeCursorRecord | null;
}

export interface NativeDiffPageRecord {
  diffs: NativeDiffRecord[];
  nextCursor?: NativeRangeCursorRecord | null;
}

export interface NativeConflictRecord {
  key: Uint8Array;
  base?: Uint8Array | null;
  left?: Uint8Array | null;
  right?: Uint8Array | null;
}

export interface NativeResolutionRecord {
  kind: "value" | "delete" | "unresolved";
  value?: Uint8Array | null;
}

export type NativeMergeResolver = (conflict: NativeConflictRecord) => NativeResolutionRecord;

export interface NativeCrdtResolutionRecord {
  kind: "value" | "delete";
  value?: Uint8Array | null;
}

export type NativeCrdtResolver = (conflict: NativeConflictRecord) => NativeCrdtResolutionRecord;

export interface NativeConflictPageRecord {
  conflicts: NativeConflictRecord[];
  nextCursor?: NativeRangeCursorRecord | null;
}

export interface NativeDiffTraversalStatsRecord {
  comparedNodes: string;
  reusedSubtrees: string;
  addedSubtrees: string;
  removedSubtrees: string;
  collectedFallbacks: string;
  emittedDiffs: string;
}

export interface NativeStructuralDiffPageRecord {
  diffs: NativeDiffRecord[];
  nextCursorJson?: string | null;
  stats: NativeDiffTraversalStatsRecord;
  nextCursor?: NativeStructuralDiffCursorRecord | null;
}

export interface NativeStructuralDiffCursorRecord {
  baseRoot?: Uint8Array | null;
  otherRoot?: Uint8Array | null;
  markers: NativeStructuralDiffMarkerRecord[];
  pending: NativeDiffRecord[];
}

export interface NativeStructuralDiffMarkerRecord {
  kind: "compare" | "added" | "removed" | string;
  baseCid?: Uint8Array | null;
  otherCid?: Uint8Array | null;
  spanEnd?: Uint8Array | null;
  cid?: Uint8Array | null;
}

export interface NativeMergeTraceRecord {
  events: NativeMergeTraceEventRecord[];
}

export interface NativeMergeTraceEventRecord {
  kind: string;
  fastPath?: string | null;
  cid?: Uint8Array | null;
  reuseReason?: string | null;
  level?: string | null;
  entries?: string | null;
  firstKey?: Uint8Array | null;
  lastKey?: Uint8Array | null;
  stage?: string | null;
  key?: Uint8Array | null;
  resolution?: string | null;
  fallbackReason?: string | null;
  diffStats?: NativeDiffTraversalStatsRecord | null;
  rightChanges?: string | null;
  mutations?: string | null;
  appendOnly?: boolean | null;
}

export interface NativeMergeExplanationRecord {
  result?: NativeTreeRecord | null;
  error?: string | null;
  traceJson: string;
  trace: NativeMergeTraceRecord;
}

export interface NativeNamedRootRecord {
  name: Uint8Array;
  tree: NativeTreeRecord;
}

export interface NativeRootManifestRecord {
  tree: NativeTreeRecord;
  createdAtMillis?: string | null;
  updatedAtMillis?: string | null;
}

export interface NativeNamedRootManifestRecord {
  name: Uint8Array;
  manifest: NativeRootManifestRecord;
}

export interface NativeNamedRootSelectionRecord {
  roots: NativeNamedRootRecord[];
  missingNames: Uint8Array[];
}

export interface NativeNamedRootUpdateRecord {
  applied: boolean;
  conflict: boolean;
  current?: NativeTreeRecord | null;
}

export interface NativeSnapshotNamespaceRecord {
  kind: "branch" | "tag" | "checkpoint" | "custom";
  customPrefix?: Uint8Array | null;
}

export interface NativeSnapshotRecord {
  id: Uint8Array;
  name: Uint8Array;
  tree: NativeTreeRecord;
  createdAtMillis?: string | null;
  updatedAtMillis?: string | null;
}

export interface NativeSnapshotSelectionRecord {
  snapshots: NativeSnapshotRecord[];
  missingIds: Uint8Array[];
}

export interface NativeKeyProofRecord {
  root?: Uint8Array | null;
  key: Uint8Array;
  pathNodeBytes: Uint8Array[];
}

export interface NativeKeyProofVerificationRecord {
  valid: boolean;
  exists: boolean;
  absence: boolean;
  root?: Uint8Array | null;
  key: Uint8Array;
  value?: Uint8Array | null;
}

export interface NativeMultiKeyProofRecord {
  root?: Uint8Array | null;
  keys: Uint8Array[];
  pathNodeBytes: Uint8Array[];
}

export interface NativeMultiKeyProofVerificationRecord {
  valid: boolean;
  root?: Uint8Array | null;
  results: NativeKeyProofVerificationRecord[];
}

export interface NativeRangeProofRecord {
  root?: Uint8Array | null;
  start: Uint8Array;
  end?: Uint8Array | null;
  pathNodeBytes: Uint8Array[];
}

export interface NativeRangeProofVerificationRecord {
  valid: boolean;
  root?: Uint8Array | null;
  start: Uint8Array;
  end?: Uint8Array | null;
  entries: NativeEntryRecord[];
}

export interface NativeRangePageProofRecord {
  root?: Uint8Array | null;
  after?: Uint8Array | null;
  end?: Uint8Array | null;
  pathNodeBytes: Uint8Array[];
}

export interface NativeRangePageProofVerificationRecord {
  valid: boolean;
  root?: Uint8Array | null;
  after?: Uint8Array | null;
  end?: Uint8Array | null;
  entries: NativeEntryRecord[];
}

export interface NativeProvedRangePageRecord {
  page: NativeRangePageRecord;
  proof: NativeRangePageProofRecord;
}

export interface NativeDiffPageProofRecord {
  base: NativeRangePageProofRecord;
  other: NativeRangePageProofRecord;
  lookaheadBase?: NativeKeyProofRecord | null;
  lookaheadOther?: NativeKeyProofRecord | null;
  requestedEnd?: Uint8Array | null;
  limit: string;
}

export interface NativeDiffPageProofVerificationRecord {
  valid: boolean;
  baseValid: boolean;
  otherValid: boolean;
  lookaheadValid: boolean;
  baseRoot?: Uint8Array | null;
  otherRoot?: Uint8Array | null;
  after?: Uint8Array | null;
  requestedEnd?: Uint8Array | null;
  proofEnd?: Uint8Array | null;
  limit: string;
  diffs: NativeDiffRecord[];
  nextCursor?: NativeRangeCursorRecord | null;
}

export interface NativeProvedDiffPageRecord {
  page: NativeDiffPageRecord;
  proof: NativeDiffPageProofRecord;
}

export interface NativeProofBundleSummaryRecord {
  version: string;
  kind: string;
  root?: Uint8Array | null;
  otherRoot?: Uint8Array | null;
  keyCount: string;
  pathNodeCount: string;
  start?: Uint8Array | null;
  end?: Uint8Array | null;
  after?: Uint8Array | null;
  requestedEnd?: Uint8Array | null;
  limit?: string | null;
  hasLookahead: boolean;
}

export interface NativeProofBundleVerificationRecord {
  summary: NativeProofBundleSummaryRecord;
  valid: boolean;
  existsCount: string;
  absenceCount: string;
  entryCount: string;
  diffCount: string;
  nextCursor?: NativeRangeCursorRecord | null;
}

export interface NativeAuthenticatedProofEnvelopeRecord {
  algorithm: string;
  keyId: Uint8Array;
  proofBundle: Uint8Array;
  context: Uint8Array;
  issuedAtMillis?: string | null;
  expiresAtMillis?: string | null;
  nonce: Uint8Array;
  signature: Uint8Array;
}

export interface NativeAuthenticatedProofEnvelopeVerificationRecord {
  valid: boolean;
  signatureValid: boolean;
  timeValid: boolean;
  notYetValid: boolean;
  expired: boolean;
  algorithm: string;
  keyId: Uint8Array;
  proofBundle: Uint8Array;
  context: Uint8Array;
  issuedAtMillis?: string | null;
  expiresAtMillis?: string | null;
  nonce: Uint8Array;
}

export interface NativeAuthenticatedProofBundleVerificationRecord {
  valid: boolean;
  envelope: NativeAuthenticatedProofEnvelopeVerificationRecord;
  proof?: NativeProofBundleVerificationRecord | null;
  proofError?: string | null;
}

export interface NativeNamedRootRetentionRecord {
  kind: "all" | "exact" | "prefix" | "newest_by_name" | "updated_since";
  names: Uint8Array[];
  prefix?: Uint8Array | null;
  count?: string | null;
  minUpdatedAtMillis?: string | null;
}

export interface NativeCacheStatsRecord {
  cachedNodes: string;
  cachedBytes: string;
  pinnedNodes: string;
  pinnedBytes: string;
}

export interface NativeMetricsRecord {
  nodeCacheHits: string;
  nodeCacheMisses: string;
  nodeCacheEvictions: string;
  nodesRead: string;
  bytesRead: string;
  nodesWritten: string;
  bytesWritten: string;
  storeGetCalls: string;
  storeBatchGetCalls: string;
  storeBatchGetKeys: string;
  storePutCalls: string;
  storeBatchPutCalls: string;
  storeBatchPutNodes: string;
}

export interface NativeTreeStatsRecord {
  num_nodes: number;
  num_leaves: number;
  num_internal_nodes: number;
  tree_height: number;
  total_key_value_pairs: number;
  total_tree_size_bytes: number;
  avg_node_size_bytes: number;
  min_node_size_bytes: number;
  max_node_size_bytes: number;
  avg_entries_per_node: number;
  nodes_per_level: Record<string, number>;
  avg_node_size_per_level: Record<string, number>;
  avg_entries_per_level: Record<string, number>;
  min_entries_per_level: Record<string, number>;
  max_entries_per_level: Record<string, number>;
  avg_fanout: number;
  min_fanout: number;
  max_fanout: number;
  avg_fill_factor: number;
  avg_leaf_fill_factor: number;
  avg_internal_fill_factor: number;
  avg_key_size_bytes: number;
  avg_value_size_bytes: number;
  min_key_size_bytes: number;
  max_key_size_bytes: number;
  min_value_size_bytes: number;
  max_value_size_bytes: number;
  total_keys_size_bytes: number;
  total_values_size_bytes: number;
}

export interface NativeStatsDiffRecord {
  num_nodes_diff: number;
  num_leaves_diff: number;
  num_internal_nodes_diff: number;
  tree_height_diff: number;
  total_key_value_pairs_diff: number;
  total_tree_size_bytes_diff: number;
  avg_node_size_bytes_diff: number;
  min_node_size_bytes_diff: number;
  max_node_size_bytes_diff: number;
  avg_entries_per_node_diff: number;
  avg_fanout_diff: number;
  min_fanout_diff: number;
  max_fanout_diff: number;
  avg_fill_factor_diff: number;
  avg_leaf_fill_factor_diff: number;
  avg_internal_fill_factor_diff: number;
  avg_key_size_bytes_diff: number;
  avg_value_size_bytes_diff: number;
  min_key_size_bytes_diff: number;
  max_key_size_bytes_diff: number;
  min_value_size_bytes_diff: number;
  max_value_size_bytes_diff: number;
  total_keys_size_bytes_diff: number;
  total_values_size_bytes_diff: number;
}

export interface NativeStatsPercentageChangeRecord {
  num_nodes_pct: number;
  num_leaves_pct: number;
  num_internal_nodes_pct: number;
  tree_height_pct: number;
  total_key_value_pairs_pct: number;
  total_tree_size_bytes_pct: number;
  avg_node_size_bytes_pct: number;
  min_node_size_bytes_pct: number;
  max_node_size_bytes_pct: number;
  avg_entries_per_node_pct: number;
  avg_fanout_pct: number;
  min_fanout_pct: number;
  max_fanout_pct: number;
  avg_fill_factor_pct: number;
  avg_leaf_fill_factor_pct: number;
  avg_internal_fill_factor_pct: number;
  avg_key_size_bytes_pct: number;
  avg_value_size_bytes_pct: number;
  min_key_size_bytes_pct: number;
  max_key_size_bytes_pct: number;
  min_value_size_bytes_pct: number;
  max_value_size_bytes_pct: number;
  total_keys_size_bytes_pct: number;
  total_values_size_bytes_pct: number;
}

export interface NativeStatsComparisonRecord {
  before: NativeTreeStatsRecord;
  after: NativeTreeStatsRecord;
  absolute: NativeStatsDiffRecord;
  percentage: NativeStatsPercentageChangeRecord;
}

export interface NativeTreeDebugNodeRecord {
  cid: Uint8Array;
  leaf: boolean;
  level: number;
  entry_count: number;
  max_entries: number;
  fill_factor: number;
  encoded_bytes: number;
  first_key?: Uint8Array | null;
  last_key?: Uint8Array | null;
}

export interface NativeTreeDebugLevelRecord {
  level: number;
  nodes: NativeTreeDebugNodeRecord[];
}

export interface NativeTreeDebugViewRecord {
  levels: NativeTreeDebugLevelRecord[];
}

export type NativeTreeDebugNodeStatus = "Shared" | "LeftOnly" | "RightOnly";

export interface NativeTreeDebugComparedNodeRecord {
  status: NativeTreeDebugNodeStatus;
  node: NativeTreeDebugNodeRecord;
}

export interface NativeTreeDebugComparisonLevelRecord {
  level: number;
  shared_nodes: number;
  left_only_nodes: number;
  right_only_nodes: number;
  shared_bytes: number;
  left_only_bytes: number;
  right_only_bytes: number;
  nodes: NativeTreeDebugComparedNodeRecord[];
}

export interface NativeTreeDebugComparisonRecord {
  shared_nodes: number;
  left_only_nodes: number;
  right_only_nodes: number;
  shared_bytes: number;
  left_only_bytes: number;
  right_only_bytes: number;
  levels: NativeTreeDebugComparisonLevelRecord[];
}

export interface NativeChangedSpanRecord {
  start: Uint8Array;
  end?: Uint8Array | null;
}

export interface NativeChangedSpanHintRecord {
  baseRoot?: Uint8Array | null;
  changedRoot?: Uint8Array | null;
  spans: NativeChangedSpanRecord[];
}

export interface NativeGcReachabilityRecord {
  liveCids: Uint8Array[];
  liveNodes: string;
  liveBytes: string;
  leafNodes: string;
  internalNodes: string;
}

export interface NativeGcPlanRecord {
  reachability: NativeGcReachabilityRecord;
  candidateNodes: string;
  reclaimableCids: Uint8Array[];
  reclaimableNodes: string;
  reclaimableBytes: string;
  missingCandidates: string;
}

export interface NativeGcSweepRecord {
  plan: NativeGcPlanRecord;
  deletedNodes: string;
  deletedBytes: string;
}

export interface NativeMissingNodePlanRecord {
  requiredCids: Uint8Array[];
  requiredNodes: string;
  requiredBytes: string;
  missingCids: Uint8Array[];
  missingNodes: string;
  missingBytes: string;
}

export interface NativeMissingNodeCopyRecord {
  plan: NativeMissingNodePlanRecord;
  copiedNodes: string;
  copiedBytes: string;
}

export interface NativeSnapshotBundleNodeRecord {
  cid: Uint8Array;
  bytes: Uint8Array;
}

export interface NativeSnapshotBundleRecord {
  formatVersion: number;
  tree: NativeTreeRecord;
  nodes: NativeSnapshotBundleNodeRecord[];
}

export interface NativeSnapshotBundleSummaryRecord {
  formatVersion: number;
  root?: Uint8Array | null;
  nodeCount: string;
  byteCount: string;
  minNodeBytes: string;
  maxNodeBytes: string;
}

export interface NativeSnapshotBundleVerificationRecord {
  valid: boolean;
  summary: NativeSnapshotBundleSummaryRecord;
  reachableNodes: string;
  reachableBytes: string;
  missingCids: Uint8Array[];
  extraCids: Uint8Array[];
}

export interface NativeBlobRefRecord {
  cid: Uint8Array;
  len: string;
}

export interface NativeLargeValueConfigRecord {
  inlineThreshold: string;
}

export interface NativeValueRefRecord {
  kind: "inline" | "blob";
  value?: Uint8Array | null;
  blob?: NativeBlobRefRecord | null;
}

export interface NativeBlobGcReachabilityRecord {
  liveBlobs: NativeBlobRefRecord[];
  liveBlobCount: string;
  liveBlobBytes: string;
  scannedNodes: string;
  scannedValues: string;
}

export interface NativeBlobGcPlanRecord {
  reachability: NativeBlobGcReachabilityRecord;
  candidateBlobs: string;
  reclaimableBlobs: NativeBlobRefRecord[];
  reclaimableBlobCount: string;
  reclaimableBlobBytes: string;
  missingCandidates: string;
}

export interface NativeBlobGcSweepRecord {
  plan: NativeBlobGcPlanRecord;
  deletedBlobs: string;
  deletedBlobBytes: string;
}

export interface NativeCrdtConfigRecord {
  strategy: "last_writer_wins" | "multi_value";
  deletePolicy: "delete_wins" | "update_wins";
}

export interface NativeTimestampedValueRecord {
  value: Uint8Array;
  timestamp: string;
}

export interface NativeTombstoneMetadataRecord {
  key: string;
  value: Uint8Array;
}

export interface NativeTombstoneRecord {
  actor: Uint8Array;
  timestampMillis: string;
  causalMetadata: NativeTombstoneMetadataRecord[];
}

export interface NativeProllyBlobStore {
  putBlob(bytes: Uint8Array): NativeBlobRefRecord;
  getBlob(reference: NativeBlobRefRecord): Uint8Array | null;
  deleteBlob(reference: NativeBlobRefRecord): void;
  listBlobRefs(): NativeBlobRefRecord[];
  blobCount(): string;
}

export interface NativeHostStoreEmptyRequest {}

export interface NativeHostStoreKeyRequest {
  key: Uint8Array;
}

export interface NativeHostStorePutRequest {
  key: Uint8Array;
  value: Uint8Array;
}

export interface NativeHostStoreBatchRequest {
  ops: NativeMutationRecord[];
}

export interface NativeHostStoreBatchGetRequest {
  keys: Uint8Array[];
}

export interface NativeHostStoreHintRequest {
  namespace: Uint8Array;
  key: Uint8Array;
}

export interface NativeHostStorePutHintRequest {
  namespace: Uint8Array;
  key: Uint8Array;
  value: Uint8Array;
}

export interface NativeHostStoreRootRequest {
  name: Uint8Array;
}

export interface NativeHostStorePutRootRequest {
  name: Uint8Array;
  manifest: Uint8Array;
}

export interface NativeHostStoreCasRootRequest {
  name: Uint8Array;
  expected?: Uint8Array | null;
  replacement?: Uint8Array | null;
}

export interface NativeHostStoreBytesResult {
  value?: Uint8Array | null;
  error?: string | null;
}

export interface NativeHostStoreUnitResult {
  error?: string | null;
}

export interface NativeHostStoreBoolResult {
  value: boolean;
  error?: string | null;
}

export interface NativeHostStoreBatchGetResult {
  values: NativeHostStoreBytesResult[];
  error?: string | null;
}

export interface NativeHostStoreListBytesResult {
  values: Uint8Array[];
  error?: string | null;
}

export interface NativeHostStoreRootResult {
  value?: Uint8Array | null;
  error?: string | null;
}

export interface NativeHostStoreNamedRootManifest {
  name: Uint8Array;
  manifest: Uint8Array;
}

export interface NativeHostStoreListRootsResult {
  values: NativeHostStoreNamedRootManifest[];
  error?: string | null;
}

export interface NativeHostStoreCasResult {
  applied: boolean;
  current?: Uint8Array | null;
  error?: string | null;
}

export interface NativeHostStore {
  // Marker interface for the Node-API host-store callback object.
}

export interface NativeMergePolicyRegistry {
  len(): string;
  isEmpty(): boolean;
  hasDefault(): boolean;
  setDefaultResolverName(name: string): void;
  setDefaultResolver(resolver: NativeMergeResolver): void;
  pushPrefixResolverName(prefix: Uint8Array, name: string): void;
  pushPrefixResolver(prefix: Uint8Array, resolver: NativeMergeResolver): void;
  pushExactResolverName(key: Uint8Array, name: string): void;
  pushExactResolver(key: Uint8Array, resolver: NativeMergeResolver): void;
}

export interface NativeProllyEngine {
  create(): NativeTreeRecord;
  put(tree: NativeTreeRecord, key: Uint8Array, value: Uint8Array): NativeTreeRecord;
  delete(tree: NativeTreeRecord, key: Uint8Array): NativeTreeRecord;
  get(tree: NativeTreeRecord, key: Uint8Array): Uint8Array | null;
  getValueRef(tree: NativeTreeRecord, key: Uint8Array): NativeValueRefRecord | null;
  getLargeValue(blobStore: NativeProllyBlobStore, tree: NativeTreeRecord, key: Uint8Array): Uint8Array | null;
  putLargeValue(
    blobStore: NativeProllyBlobStore,
    tree: NativeTreeRecord,
    key: Uint8Array,
    value: Uint8Array,
    config: NativeLargeValueConfigRecord,
  ): NativeTreeRecord;
  getMany(tree: NativeTreeRecord, keys: Uint8Array[]): Array<Uint8Array | null>;
  proveKey(tree: NativeTreeRecord, key: Uint8Array): NativeKeyProofRecord;
  proveKeys(tree: NativeTreeRecord, keys: Uint8Array[]): NativeMultiKeyProofRecord;
  proveRange(tree: NativeTreeRecord, start: Uint8Array, end?: Uint8Array | null): NativeRangeProofRecord;
  provePrefix(tree: NativeTreeRecord, prefix: Uint8Array): NativeRangeProofRecord;
  proveRangePage(
    tree: NativeTreeRecord,
    cursor?: NativeRangeCursorRecord | null,
    end?: Uint8Array | null,
    limit?: string,
  ): NativeProvedRangePageRecord;
  proveDiffPage(
    base: NativeTreeRecord,
    other: NativeTreeRecord,
    cursor?: NativeRangeCursorRecord | null,
    end?: Uint8Array | null,
    limit?: string,
  ): NativeProvedDiffPageRecord;
  batch(tree: NativeTreeRecord, mutations: NativeMutationRecord[]): NativeTreeRecord;
  batchWithStats(tree: NativeTreeRecord, mutations: NativeMutationRecord[]): NativeBatchApplyResultRecord;
  parallelBatch(
    tree: NativeTreeRecord,
    mutations: NativeMutationRecord[],
    config: NativeParallelConfigRecord,
  ): NativeTreeRecord;
  parallelBatchWithStats(
    tree: NativeTreeRecord,
    mutations: NativeMutationRecord[],
    config: NativeParallelConfigRecord,
  ): NativeBatchApplyResultRecord;
  buildFromEntries(entries: NativeEntryRecord[]): NativeTreeRecord;
  buildFromSortedEntries(entries: NativeEntryRecord[]): NativeTreeRecord;
  appendBatch(tree: NativeTreeRecord, mutations: NativeMutationRecord[]): NativeTreeRecord;
  appendBatchWithStats(tree: NativeTreeRecord, mutations: NativeMutationRecord[]): NativeBatchApplyResultRecord;
  firstEntry(tree: NativeTreeRecord): NativeEntryRecord | null;
  lastEntry(tree: NativeTreeRecord): NativeEntryRecord | null;
  lowerBound(tree: NativeTreeRecord, key: Uint8Array): NativeEntryRecord | null;
  upperBound(tree: NativeTreeRecord, key: Uint8Array): NativeEntryRecord | null;
  range(tree: NativeTreeRecord, start: Uint8Array, end?: Uint8Array | null): NativeEntryRecord[];
  prefix(tree: NativeTreeRecord, prefix: Uint8Array): NativeEntryRecord[];
  prefixPage(
    tree: NativeTreeRecord,
    prefix: Uint8Array,
    cursor?: NativeRangeCursorRecord | null,
    limit?: string,
  ): NativeRangePageRecord;
  prefixReversePage(
    tree: NativeTreeRecord,
    prefix: Uint8Array,
    cursor?: NativeReverseCursorRecord | null,
    limit?: string,
  ): NativeReversePageRecord;
  rangeAfter(tree: NativeTreeRecord, afterKey: Uint8Array, end?: Uint8Array | null): NativeEntryRecord[];
  rangeFromCursor(
    tree: NativeTreeRecord,
    cursor?: NativeRangeCursorRecord | null,
    end?: Uint8Array | null,
  ): NativeEntryRecord[];
  rangePage(
    tree: NativeTreeRecord,
    cursor?: NativeRangeCursorRecord | null,
    end?: Uint8Array | null,
    limit?: string,
  ): NativeRangePageRecord;
  reversePage(
    tree: NativeTreeRecord,
    cursor?: NativeReverseCursorRecord | null,
    start: Uint8Array,
    limit?: string,
  ): NativeReversePageRecord;
  cursorWindow(
    tree: NativeTreeRecord,
    key: Uint8Array,
    end?: Uint8Array | null,
    limit?: string,
  ): NativeCursorWindowRecord;
  diff(base: NativeTreeRecord, other: NativeTreeRecord): NativeDiffRecord[];
  rangeDiff(
    base: NativeTreeRecord,
    other: NativeTreeRecord,
    start: Uint8Array,
    end?: Uint8Array | null,
  ): NativeDiffRecord[];
  diffFromCursor(
    base: NativeTreeRecord,
    other: NativeTreeRecord,
    cursor?: NativeRangeCursorRecord | null,
    end?: Uint8Array | null,
  ): NativeDiffRecord[];
  diffPage(
    base: NativeTreeRecord,
    other: NativeTreeRecord,
    cursor?: NativeRangeCursorRecord | null,
    end?: Uint8Array | null,
    limit?: string,
  ): NativeDiffPageRecord;
  conflictPage(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    cursor?: NativeRangeCursorRecord | null,
    limit?: string,
  ): NativeConflictPageRecord;
  merge(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    resolver?: string | null,
  ): NativeTreeRecord;
  mergeWithResolver(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    resolver: NativeMergeResolver,
  ): NativeTreeRecord;
  mergeWithPolicy(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    policy: NativeMergePolicyRegistry,
  ): NativeTreeRecord;
  crdtMerge(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    config: NativeCrdtConfigRecord,
  ): NativeTreeRecord;
  crdtMergeWithResolver(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    deletePolicy: "delete_wins" | "update_wins",
    resolver: NativeCrdtResolver,
  ): NativeTreeRecord;
  mergeExplain(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    resolver?: string | null,
  ): NativeMergeExplanationRecord;
  mergeExplainWithResolver(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    resolver: NativeMergeResolver,
  ): NativeMergeExplanationRecord;
  mergeExplainWithPolicy(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    policy: NativeMergePolicyRegistry,
  ): NativeMergeExplanationRecord;
  mergeRange(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    start: Uint8Array,
    end?: Uint8Array | null,
    resolver?: string | null,
  ): NativeTreeRecord;
  mergeRangeWithResolver(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    start: Uint8Array,
    end: Uint8Array | null | undefined,
    resolver: NativeMergeResolver,
  ): NativeTreeRecord;
  mergeRangeWithPolicy(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    start: Uint8Array,
    end: Uint8Array | null | undefined,
    policy: NativeMergePolicyRegistry,
  ): NativeTreeRecord;
  mergePrefix(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    prefix: Uint8Array,
    resolver?: string | null,
  ): NativeTreeRecord;
  mergePrefixWithResolver(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    prefix: Uint8Array,
    resolver: NativeMergeResolver,
  ): NativeTreeRecord;
  mergePrefixWithPolicy(
    base: NativeTreeRecord,
    left: NativeTreeRecord,
    right: NativeTreeRecord,
    prefix: Uint8Array,
    policy: NativeMergePolicyRegistry,
  ): NativeTreeRecord;
  loadNamedRoot(name: Uint8Array): NativeTreeRecord | null;
  loadNamedRoots(names: Uint8Array[]): NativeNamedRootSelectionRecord;
  loadRetainedNamedRoots(retention: NativeNamedRootRetentionRecord): NativeNamedRootSelectionRecord;
  listNamedRoots(): NativeNamedRootRecord[];
  listNamedRootManifests(): NativeNamedRootManifestRecord[];
  publishNamedRoot(name: Uint8Array, tree: NativeTreeRecord): void;
  publishNamedRootAtMillis(name: Uint8Array, tree: NativeTreeRecord, timestampMillis: string): void;
  deleteNamedRoot(name: Uint8Array): void;
  compareAndSwapNamedRoot(
    name: Uint8Array,
    expected?: NativeTreeRecord | null,
    replacement?: NativeTreeRecord | null,
  ): NativeNamedRootUpdateRecord;
  publishSnapshot(namespace: NativeSnapshotNamespaceRecord, id: Uint8Array, tree: NativeTreeRecord): void;
  publishSnapshotAtMillis(
    namespace: NativeSnapshotNamespaceRecord,
    id: Uint8Array,
    tree: NativeTreeRecord,
    timestampMillis: string,
  ): void;
  loadSnapshot(namespace: NativeSnapshotNamespaceRecord, id: Uint8Array): NativeTreeRecord | null;
  loadSnapshots(namespace: NativeSnapshotNamespaceRecord, ids: Uint8Array[]): NativeSnapshotSelectionRecord;
  listSnapshots(namespace: NativeSnapshotNamespaceRecord): NativeSnapshotRecord[];
  deleteSnapshot(namespace: NativeSnapshotNamespaceRecord, id: Uint8Array): void;
  compareAndSwapSnapshot(
    namespace: NativeSnapshotNamespaceRecord,
    id: Uint8Array,
    expected?: NativeTreeRecord | null,
    replacement?: NativeTreeRecord | null,
  ): NativeNamedRootUpdateRecord;
  compareAndSwapSnapshotAtMillis(
    namespace: NativeSnapshotNamespaceRecord,
    id: Uint8Array,
    expected: NativeTreeRecord | null | undefined,
    replacement: NativeTreeRecord | null | undefined,
    timestampMillis: string,
  ): NativeNamedRootUpdateRecord;
  collectStatsJson(tree: NativeTreeRecord): string;
  collectStats(tree: NativeTreeRecord): NativeTreeStatsRecord;
  statsDiffJson(before: NativeTreeRecord, after: NativeTreeRecord): string;
  statsDiff(before: NativeTreeRecord, after: NativeTreeRecord): NativeStatsComparisonRecord;
  debugTreeJson(tree: NativeTreeRecord): string;
  debugTree(tree: NativeTreeRecord): NativeTreeDebugViewRecord;
  debugTreeText(tree: NativeTreeRecord): string;
  debugCompareTreesJson(left: NativeTreeRecord, right: NativeTreeRecord): string;
  debugCompareTrees(left: NativeTreeRecord, right: NativeTreeRecord): NativeTreeDebugComparisonRecord;
  debugCompareTreesText(left: NativeTreeRecord, right: NativeTreeRecord): string;
  cacheStats(): NativeCacheStatsRecord;
  clearCache(): void;
  pinTreeRoot(tree: NativeTreeRecord): string;
  pinTreePath(tree: NativeTreeRecord, key: Uint8Array): string;
  unpinAllCacheNodes(): string;
  metrics(): NativeMetricsRecord;
  resetMetrics(): void;
  publishPrefixPathHint(tree: NativeTreeRecord, prefix: Uint8Array): boolean;
  hydratePrefixPathHint(tree: NativeTreeRecord, prefix: Uint8Array): boolean;
  publishChangedSpansHint(
    base: NativeTreeRecord,
    changed: NativeTreeRecord,
    spans: NativeChangedSpanRecord[],
  ): boolean;
  loadChangedSpansHint(base: NativeTreeRecord, changed: NativeTreeRecord): NativeChangedSpanHintRecord | null;
  structuralDiffPage(
    base: NativeTreeRecord,
    other: NativeTreeRecord,
    cursorJson?: string | null,
    limit?: string,
  ): NativeStructuralDiffPageRecord;
  structuralDiffPageWithCursor(
    base: NativeTreeRecord,
    other: NativeTreeRecord,
    cursor?: NativeStructuralDiffCursorRecord | null,
    limit?: string,
  ): NativeStructuralDiffPageRecord;
  markReachable(roots: NativeTreeRecord[]): NativeGcReachabilityRecord;
  markReachableBlobs(roots: NativeTreeRecord[]): NativeBlobGcReachabilityRecord;
  listNodeCids(): Uint8Array[];
  planGc(roots: NativeTreeRecord[], candidateCids: Uint8Array[]): NativeGcPlanRecord;
  sweepGc(roots: NativeTreeRecord[], candidateCids: Uint8Array[]): NativeGcSweepRecord;
  planStoreGc(roots: NativeTreeRecord[]): NativeGcPlanRecord;
  sweepStoreGc(roots: NativeTreeRecord[]): NativeGcSweepRecord;
  planStoreGcForRetention(retention: NativeNamedRootRetentionRecord): NativeGcPlanRecord;
  sweepStoreGcForRetention(retention: NativeNamedRootRetentionRecord): NativeGcSweepRecord;
  planBlobGc(
    blobStore: NativeProllyBlobStore,
    roots: NativeTreeRecord[],
    candidateBlobs: NativeBlobRefRecord[],
  ): NativeBlobGcPlanRecord;
  sweepBlobGc(
    blobStore: NativeProllyBlobStore,
    roots: NativeTreeRecord[],
    candidateBlobs: NativeBlobRefRecord[],
  ): NativeBlobGcSweepRecord;
  planBlobStoreGc(blobStore: NativeProllyBlobStore, roots: NativeTreeRecord[]): NativeBlobGcPlanRecord;
  sweepBlobStoreGc(blobStore: NativeProllyBlobStore, roots: NativeTreeRecord[]): NativeBlobGcSweepRecord;
  planMissingNodes(tree: NativeTreeRecord, destination: NativeProllyEngine): NativeMissingNodePlanRecord;
  copyMissingNodes(tree: NativeTreeRecord, destination: NativeProllyEngine): NativeMissingNodeCopyRecord;
  exportSnapshot(tree: NativeTreeRecord): NativeSnapshotBundleRecord;
  importSnapshot(bundle: NativeSnapshotBundleRecord): NativeTreeRecord;
}

export interface NativeModule {
  NativeProllyBlobStore: {
    memory(): NativeProllyBlobStore;
    file(path: string): NativeProllyBlobStore;
  };
  NativeMergePolicyRegistry: {
    new (): NativeMergePolicyRegistry;
  };
  NativeHostStore: {
    new (
      get: (arg: NativeHostStoreKeyRequest) => NativeHostStoreBytesResult,
      put: (arg: NativeHostStorePutRequest) => NativeHostStoreUnitResult,
      delete_: (arg: NativeHostStoreKeyRequest) => NativeHostStoreUnitResult,
      batch: (arg: NativeHostStoreBatchRequest) => NativeHostStoreUnitResult,
      batchGetOrdered: (arg: NativeHostStoreBatchGetRequest) => NativeHostStoreBatchGetResult,
      prefersBatchReads: (arg: NativeHostStoreEmptyRequest) => NativeHostStoreBoolResult,
      supportsHints: (arg: NativeHostStoreEmptyRequest) => NativeHostStoreBoolResult,
      getHint: (arg: NativeHostStoreHintRequest) => NativeHostStoreBytesResult,
      putHint: (arg: NativeHostStorePutHintRequest) => NativeHostStoreUnitResult,
      listNodeCids: (arg: NativeHostStoreEmptyRequest) => NativeHostStoreListBytesResult,
      getRoot: (arg: NativeHostStoreRootRequest) => NativeHostStoreRootResult,
      putRoot: (arg: NativeHostStorePutRootRequest) => NativeHostStoreUnitResult,
      deleteRoot: (arg: NativeHostStoreRootRequest) => NativeHostStoreUnitResult,
      compareAndSwapRoot: (arg: NativeHostStoreCasRootRequest) => NativeHostStoreCasResult,
      listRoots: (arg: NativeHostStoreEmptyRequest) => NativeHostStoreListRootsResult,
    ): NativeHostStore;
  };
  NativeProllyEngine: {
    memory(): NativeProllyEngine;
    memoryWithConfigJson(configJson: string): NativeProllyEngine;
    file(path: string): NativeProllyEngine;
    fileWithConfigJson(path: string, configJson: string): NativeProllyEngine;
    sqlite(path: string): NativeProllyEngine;
    sqliteWithConfigJson(path: string, configJson: string): NativeProllyEngine;
    sqliteInMemory(): NativeProllyEngine;
    sqliteInMemoryWithConfigJson(configJson: string): NativeProllyEngine;
    customStore(store: NativeHostStore): NativeProllyEngine;
    customStoreWithConfigJson(store: NativeHostStore, configJson: string): NativeProllyEngine;
  };
  cidFromBytes(bytes: Uint8Array): Uint8Array;
  nodeBytesRoundTrip(bytes: Uint8Array): Uint8Array;
  nodeCidFromBytes(bytes: Uint8Array): Uint8Array;
  verifyKeyProof(proof: NativeKeyProofRecord): NativeKeyProofVerificationRecord;
  verifyMultiKeyProof(proof: NativeMultiKeyProofRecord): NativeMultiKeyProofVerificationRecord;
  verifyRangeProof(proof: NativeRangeProofRecord): NativeRangeProofVerificationRecord;
  verifyRangePageProof(proof: NativeRangePageProofRecord): NativeRangePageProofVerificationRecord;
  verifyDiffPageProof(proof: NativeDiffPageProofRecord): NativeDiffPageProofVerificationRecord;
  keyProofToBytes(proof: NativeKeyProofRecord): Uint8Array;
  keyProofFromBytes(bytes: Uint8Array): NativeKeyProofRecord;
  multiKeyProofToBytes(proof: NativeMultiKeyProofRecord): Uint8Array;
  multiKeyProofFromBytes(bytes: Uint8Array): NativeMultiKeyProofRecord;
  rangeProofToBytes(proof: NativeRangeProofRecord): Uint8Array;
  rangeProofFromBytes(bytes: Uint8Array): NativeRangeProofRecord;
  rangePageProofToBytes(proof: NativeRangePageProofRecord): Uint8Array;
  rangePageProofFromBytes(bytes: Uint8Array): NativeRangePageProofRecord;
  diffPageProofToBytes(proof: NativeDiffPageProofRecord): Uint8Array;
  diffPageProofFromBytes(bytes: Uint8Array): NativeDiffPageProofRecord;
  inspectProofBundle(bytes: Uint8Array): NativeProofBundleSummaryRecord;
  verifyProofBundle(bytes: Uint8Array): NativeProofBundleVerificationRecord;
  signProofBundleHmacSha256(
    proofBundle: Uint8Array,
    keyId: Uint8Array,
    secret: Uint8Array,
    context: Uint8Array,
    issuedAtMillis: string | null | undefined,
    expiresAtMillis: string | null | undefined,
    nonce: Uint8Array,
  ): NativeAuthenticatedProofEnvelopeRecord;
  verifyAuthenticatedProofEnvelope(
    envelope: NativeAuthenticatedProofEnvelopeRecord,
    secret: Uint8Array,
    nowMillis: string | null | undefined,
  ): NativeAuthenticatedProofEnvelopeVerificationRecord;
  verifyAuthenticatedProofBundle(
    envelopeBytes: Uint8Array,
    secret: Uint8Array,
    nowMillis: string | null | undefined,
  ): NativeAuthenticatedProofBundleVerificationRecord;
  authenticatedProofEnvelopeToBytes(envelope: NativeAuthenticatedProofEnvelopeRecord): Uint8Array;
  authenticatedProofEnvelopeFromBytes(bytes: Uint8Array): NativeAuthenticatedProofEnvelopeRecord;
  keyProofFromNodeBytes(
    root: Uint8Array | null | undefined,
    key: Uint8Array,
    pathNodeBytes: Uint8Array[],
  ): NativeKeyProofRecord;
  multiKeyProofFromNodeBytes(
    root: Uint8Array | null | undefined,
    keys: Uint8Array[],
    pathNodeBytes: Uint8Array[],
  ): NativeMultiKeyProofRecord;
  rangeProofFromNodeBytes(
    root: Uint8Array | null | undefined,
    start: Uint8Array,
    end: Uint8Array | null | undefined,
    pathNodeBytes: Uint8Array[],
  ): NativeRangeProofRecord;
  rangePageProofFromNodeBytes(
    root: Uint8Array | null | undefined,
    after: Uint8Array | null | undefined,
    end: Uint8Array | null | undefined,
    pathNodeBytes: Uint8Array[],
  ): NativeRangePageProofRecord;
  isBoundaryConfigJson(configJson: string, count: string, key: Uint8Array, value: Uint8Array): boolean;
  prefixEnd(prefix: Uint8Array): Uint8Array | null;
  prefixRange(prefix: Uint8Array): NativeRangeBoundsRecord;
  u64Key(value: string): Uint8Array;
  u128Key(value: string): Uint8Array;
  i64Key(value: string): Uint8Array;
  i128Key(value: string): Uint8Array;
  timestampMillisKey(value: string): Uint8Array;
  encodeSegment(segment: Uint8Array): Uint8Array;
  keyFromSegments(segments: Uint8Array[]): Uint8Array;
  keyFromPrefixedSegments(prefix: Uint8Array, segments: Uint8Array[]): Uint8Array;
  rangeCursorStart(): NativeRangeCursorRecord;
  rangeCursorAfterKey(key: Uint8Array): NativeRangeCursorRecord;
  reverseCursorEnd(): NativeReverseCursorRecord;
  reverseCursorBeforeKey(key: Uint8Array): NativeReverseCursorRecord;
  upsertMutation(key: Uint8Array, value: Uint8Array): NativeMutationRecord;
  deleteMutation(key: Uint8Array): NativeMutationRecord;
  resolutionValue(value: Uint8Array): NativeResolutionRecord;
  resolutionDelete(): NativeResolutionRecord;
  resolutionUnresolved(): NativeResolutionRecord;
  resolvePreferLeft(conflict: NativeConflictRecord): NativeResolutionRecord;
  resolvePreferRight(conflict: NativeConflictRecord): NativeResolutionRecord;
  resolveDeleteWins(conflict: NativeConflictRecord): NativeResolutionRecord;
  resolveUpdateWins(conflict: NativeConflictRecord): NativeResolutionRecord;
  crdtResolutionValue(value: Uint8Array): NativeCrdtResolutionRecord;
  crdtResolutionDelete(): NativeCrdtResolutionRecord;
  changedSpan(start: Uint8Array, end: Uint8Array | null | undefined): NativeChangedSpanRecord;
  changedSpanFromKey(key: Uint8Array): NativeChangedSpanRecord;
  changedSpanForPrefix(prefix: Uint8Array): NativeChangedSpanRecord;
  retainAllNamedRoots(): NativeNamedRootRetentionRecord;
  retainExactNamedRoots(names: Uint8Array[]): NativeNamedRootRetentionRecord;
  retainNamedRootPrefix(prefix: Uint8Array): NativeNamedRootRetentionRecord;
  retainNewestNamedRoots(prefix: Uint8Array, count: string): NativeNamedRootRetentionRecord;
  retainNamedRootsUpdatedSince(prefix: Uint8Array, minUpdatedAtMillis: string): NativeNamedRootRetentionRecord;
  decodeSegments(key: Uint8Array): Uint8Array[];
  debugKey(key: Uint8Array): string;
  snapshotNamespaceBranch(): NativeSnapshotNamespaceRecord;
  snapshotNamespaceTag(): NativeSnapshotNamespaceRecord;
  snapshotNamespaceCheckpoint(): NativeSnapshotNamespaceRecord;
  snapshotNamespaceCustom(prefix: Uint8Array): NativeSnapshotNamespaceRecord;
  snapshotRootName(namespace: NativeSnapshotNamespaceRecord, id: Uint8Array): Uint8Array;
  snapshotIdFromName(namespace: NativeSnapshotNamespaceRecord, name: Uint8Array): Uint8Array | null;
  versionedValueBytesRoundTrip(bytes: Uint8Array): Uint8Array;
  versionedValueBytesMatchesSchema(bytes: Uint8Array, schema: string, version: string): boolean;
  versionedValueBytesRequireSchema(bytes: Uint8Array, schema: string, version: string): void;
  valueRefBytesRoundTrip(bytes: Uint8Array): Uint8Array;
  valueRefFromStoredBytes(bytes: Uint8Array): NativeValueRefRecord;
  valueRefInlineRequiresEscape(value: Uint8Array): boolean;
  blobRefValidateBytes(reference: NativeBlobRefRecord, bytes: Uint8Array): void;
  rootManifestBytesRoundTrip(bytes: Uint8Array): Uint8Array;
  snapshotBundleToBytes(bundle: NativeSnapshotBundleRecord): Uint8Array;
  snapshotBundleFromBytes(bytes: Uint8Array): NativeSnapshotBundleRecord;
  snapshotBundleDigest(bundle: NativeSnapshotBundleRecord): Uint8Array;
  snapshotBundleDigestBytes(bytes: Uint8Array): Uint8Array;
  snapshotBundleSummary(bundle: NativeSnapshotBundleRecord): NativeSnapshotBundleSummaryRecord;
  snapshotBundleSummaryFromBytes(bytes: Uint8Array): NativeSnapshotBundleSummaryRecord;
  verifySnapshotBundle(bundle: NativeSnapshotBundleRecord): NativeSnapshotBundleVerificationRecord;
  verifySnapshotBundleBytes(bytes: Uint8Array): NativeSnapshotBundleVerificationRecord;
  defaultConfig(): NativeConfigRecord;
  encodingRaw(): NativeEncodingRecord;
  encodingCbor(): NativeEncodingRecord;
  encodingJson(): NativeEncodingRecord;
  encodingCustom(name: string): NativeEncodingRecord;
  treeConfig(
    minChunkSize: string,
    maxChunkSize: string,
    chunkingFactor: number,
    hashSeed: string,
    encoding: NativeEncodingRecord,
    nodeCacheMaxNodes?: string | null,
    nodeCacheMaxBytes?: string | null,
  ): NativeConfigRecord;
  largeValueConfig(inlineThreshold: string): NativeLargeValueConfigRecord;
  defaultLargeValueConfig(): NativeLargeValueConfigRecord;
  parallelConfig(maxThreads: string, parallelismThreshold: string): NativeParallelConfigRecord;
  parallelConfigSequential(): NativeParallelConfigRecord;
  defaultParallelConfig(): NativeParallelConfigRecord;
  crdtConfigLww(deletePolicy: "delete_wins" | "update_wins"): NativeCrdtConfigRecord;
  crdtConfigMultiValue(deletePolicy: "delete_wins" | "update_wins"): NativeCrdtConfigRecord;
  timestampedValueToBytes(record: NativeTimestampedValueRecord): Uint8Array;
  timestampedValueFromBytes(bytes: Uint8Array): NativeTimestampedValueRecord;
  timestampedValueNow(value: Uint8Array): NativeTimestampedValueRecord;
  multiValueSetToBytes(values: Uint8Array[]): Uint8Array;
  multiValueSetFromBytes(bytes: Uint8Array): Uint8Array[];
  multiValueSetMerge(left: Uint8Array[], right: Uint8Array[]): Uint8Array[];
  tombstoneToBytes(record: NativeTombstoneRecord): Uint8Array;
  tombstoneFromBytes(bytes: Uint8Array): NativeTombstoneRecord;
  tombstoneFromStoredBytes(bytes: Uint8Array): NativeTombstoneRecord | null;
  isTombstoneValue(bytes: Uint8Array): boolean;
  tombstoneUpsertMutation(key: Uint8Array, tombstone: NativeTombstoneRecord): NativeMutationRecord;
  tombstoneCompactionMutation(key: Uint8Array, storedValue: Uint8Array): NativeMutationRecord | null;
}

export async function loadNative(): Promise<NativeModule> {
  const { createRequire } = await import("node:module");
  const require = createRequire(import.meta.url);

  try {
    return require("../index.cjs") as NativeModule;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "MODULE_NOT_FOUND") {
      throw new Error("Prolly native Node-API module is not built. Run `npm run build:native` first.");
    }
    throw error;
  }
}
