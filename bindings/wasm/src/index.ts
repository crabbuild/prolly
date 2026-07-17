export interface WasmEntryRecord {
  key: Uint8Array;
  value: Uint8Array;
}

export interface WasmScanOutcomeRecord {
  visited: string;
  stopped: boolean;
}

export type WasmEntryVisitor = (entry: WasmEntryRecord) => boolean;
export type WasmDiffVisitor = (diff: WasmDiffRecord) => boolean;
export type WasmConflictVisitor = (conflict: WasmConflictRecord) => boolean;

export type WasmOptionalEntryRecord = WasmEntryRecord | null;

export interface WasmMutationRecord {
  kind: "upsert" | "delete";
  key: Uint8Array;
  value?: Uint8Array | null;
}

export interface WasmParallelConfigRecord {
  maxThreads: string | number;
  parallelismThreshold: string | number;
}

export interface WasmBatchApplyStatsRecord {
  inputMutations: number;
  effectiveMutations: number;
  preprocessInputSorted: boolean;
  affectedLeaves: number;
  changedLeaves: number;
  sparseLeafApplies: number;
  writtenNodes: number;
  writtenBytes: number;
  usedAppendFastPath: boolean;
  usedBatchedRoute: boolean;
  usedCoalescedRebuild: boolean;
  usedDeferredRebalancing: boolean;
  usedBottomUpRebuild: boolean;
  cacheWrittenNodes: boolean;
}

export interface WasmBatchApplyResultRecord {
  tree: unknown;
  stats: WasmBatchApplyStatsRecord;
}

export interface WasmWriteStatsRecord {
  inputMutations: number;
  effectiveMutations: number;
  entriesStreamed: number;
  nodesRead: number;
  nodesWritten: number;
  nodesReused: number;
  bytesRead: number;
  bytesWritten: number;
  resyncDistanceEntries: number;
  resyncDistanceNodes: number;
  usedKeyStableFastPath: boolean;
  usedBatchedValueUpdatePath: boolean;
}

export interface WasmWriteResultRecord {
  tree: unknown;
  stats: WasmWriteStatsRecord;
}

export interface WasmSnapshotBundleNodeRecord {
  cid: Uint8Array;
  bytes: Uint8Array;
}

export interface WasmSnapshotBundleRecord {
  formatVersion: number;
  tree: unknown;
  nodes: WasmSnapshotBundleNodeRecord[];
  nodeCount: number;
  byteCount: number;
}

export interface WasmSnapshotBundleSummaryRecord {
  formatVersion: number;
  root?: Uint8Array | null;
  nodeCount: string;
  byteCount: string;
  minNodeBytes: string;
  maxNodeBytes: string;
}

export interface WasmSnapshotBundleVerificationRecord {
  valid: boolean;
  summary: WasmSnapshotBundleSummaryRecord;
  reachableNodes: string;
  reachableBytes: string;
  missingCids: Uint8Array[];
  extraCids: Uint8Array[];
}

export interface WasmRangeCursorRecord {
  afterKey?: Uint8Array | null;
}

export interface WasmReverseCursorRecord {
  beforeKey?: Uint8Array | null;
}

export interface WasmRangeBoundsRecord {
  start: Uint8Array;
  end?: Uint8Array | null;
}

export interface WasmRangePageRecord {
  entries: WasmEntryRecord[];
  nextCursor?: WasmRangeCursorRecord | null;
}

export interface WasmReversePageRecord {
  entries: WasmEntryRecord[];
  nextCursor?: WasmReverseCursorRecord | null;
}

export interface WasmCursorWindowRecord {
  positionKey?: Uint8Array | null;
  positionValue?: Uint8Array | null;
  found: boolean;
  entries: WasmEntryRecord[];
  nextCursor?: WasmRangeCursorRecord | null;
}

export interface WasmDiffRecord {
  kind: "added" | "removed" | "changed";
  key: Uint8Array;
  value?: Uint8Array | null;
  old?: Uint8Array | null;
  newValue?: Uint8Array | null;
}

export interface WasmDiffPageRecord {
  diffs: WasmDiffRecord[];
  nextCursor?: WasmRangeCursorRecord | null;
}

export interface WasmDiffTraversalStatsRecord {
  comparedNodes: number;
  reusedSubtrees: number;
  addedSubtrees: number;
  removedSubtrees: number;
  collectedFallbacks: number;
  emittedDiffs: number;
}

export interface WasmStructuralDiffPageRecord {
  diffs: WasmDiffRecord[];
  nextCursorJson?: string | null;
  stats: WasmDiffTraversalStatsRecord;
  nextCursor?: WasmStructuralDiffCursorRecord | null;
}

export interface WasmStructuralDiffCursorRecord {
  baseRoot?: Uint8Array | null;
  otherRoot?: Uint8Array | null;
  markers: WasmStructuralDiffMarkerRecord[];
  pending: WasmDiffRecord[];
}

export interface WasmStructuralDiffMarkerRecord {
  kind: "compare" | "added" | "removed" | string;
  baseCid?: Uint8Array | null;
  otherCid?: Uint8Array | null;
  spanEnd?: Uint8Array | null;
  cid?: Uint8Array | null;
}

export interface WasmConflictRecord {
  key: Uint8Array;
  base?: Uint8Array | null;
  left?: Uint8Array | null;
  right?: Uint8Array | null;
}

export interface WasmConflictPageRecord {
  conflicts: WasmConflictRecord[];
  nextCursor?: WasmRangeCursorRecord | null;
}

export interface WasmMergeExplanationRecord {
  result?: unknown | null;
  error?: string | null;
  traceJson: string;
  trace: WasmMergeTraceRecord;
}

export interface WasmMergeTraceRecord {
  events: WasmMergeTraceEventRecord[];
}

export interface WasmMergeTraceEventRecord {
  kind: string;
  fastPath?: string;
  cid?: Uint8Array;
  reuseReason?: string;
  level?: number;
  entries?: number;
  firstKey?: Uint8Array;
  lastKey?: Uint8Array;
  stage?: string;
  key?: Uint8Array;
  resolution?: string;
  fallbackReason?: string;
  diffStats?: WasmDiffTraversalStatsRecord;
  rightChanges?: number;
  mutations?: number;
  appendOnly?: boolean;
}

export interface WasmTreeStatsRecord {
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

export interface WasmStatsDiffRecord {
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

export interface WasmStatsPercentageChangeRecord {
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

export interface WasmStatsComparisonRecord {
  before: WasmTreeStatsRecord;
  after: WasmTreeStatsRecord;
  absolute: WasmStatsDiffRecord;
  percentage: WasmStatsPercentageChangeRecord;
}

export interface WasmTreeDebugNodeRecord {
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

export interface WasmTreeDebugLevelRecord {
  level: number;
  nodes: WasmTreeDebugNodeRecord[];
}

export interface WasmTreeDebugViewRecord {
  levels: WasmTreeDebugLevelRecord[];
}

export type WasmTreeDebugNodeStatus = "Shared" | "LeftOnly" | "RightOnly";

export interface WasmTreeDebugComparedNodeRecord {
  status: WasmTreeDebugNodeStatus;
  node: WasmTreeDebugNodeRecord;
}

export interface WasmTreeDebugComparisonLevelRecord {
  level: number;
  shared_nodes: number;
  left_only_nodes: number;
  right_only_nodes: number;
  shared_bytes: number;
  left_only_bytes: number;
  right_only_bytes: number;
  nodes: WasmTreeDebugComparedNodeRecord[];
}

export interface WasmTreeDebugComparisonRecord {
  shared_nodes: number;
  left_only_nodes: number;
  right_only_nodes: number;
  shared_bytes: number;
  left_only_bytes: number;
  right_only_bytes: number;
  levels: WasmTreeDebugComparisonLevelRecord[];
}

export interface WasmKeyProofRecord {
  root?: Uint8Array | null;
  key: Uint8Array;
  pathNodeBytes: Uint8Array[];
}

export interface WasmKeyProofVerificationRecord {
  valid: boolean;
  exists: boolean;
  absence: boolean;
  root?: Uint8Array | null;
  key: Uint8Array;
  value?: Uint8Array | null;
}

export interface WasmMultiKeyProofRecord {
  root?: Uint8Array | null;
  keys: Uint8Array[];
  pathNodeBytes: Uint8Array[];
}

export interface WasmMultiKeyProofVerificationRecord {
  valid: boolean;
  root?: Uint8Array | null;
  results: WasmKeyProofVerificationRecord[];
}

export interface WasmRangeProofRecord {
  root?: Uint8Array | null;
  start: Uint8Array;
  end?: Uint8Array | null;
  pathNodeBytes: Uint8Array[];
}

export interface WasmRangeProofVerificationRecord {
  valid: boolean;
  root?: Uint8Array | null;
  start: Uint8Array;
  end?: Uint8Array | null;
  entries: WasmEntryRecord[];
}

export interface WasmRangePageProofVerificationRecord {
  valid: boolean;
  root?: Uint8Array | null;
  after?: Uint8Array | null;
  end?: Uint8Array | null;
  entries: WasmEntryRecord[];
}

export interface WasmProofBundleSummaryRecord {
  version: string;
  kind: "key" | "multi_key" | "range" | "range_page" | "diff_page";
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

export interface WasmProofBundleVerificationRecord {
  summary: WasmProofBundleSummaryRecord;
  valid: boolean;
  existsCount: string;
  absenceCount: string;
  entryCount: string;
  diffCount: string;
  nextCursor?: WasmRangeCursor | null;
}

export interface WasmAuthenticatedProofEnvelopeRecord {
  algorithm: string;
  keyId: Uint8Array;
  proofBundle: Uint8Array;
  context: Uint8Array;
  issuedAtMillis?: string | null;
  expiresAtMillis?: string | null;
  nonce: Uint8Array;
  signature: Uint8Array;
}

export interface WasmAuthenticatedProofEnvelopeVerificationRecord {
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

export interface WasmAuthenticatedProofBundleVerificationRecord {
  valid: boolean;
  envelope: WasmAuthenticatedProofEnvelopeVerificationRecord;
  proof?: WasmProofBundleVerificationRecord | null;
  proofError?: string | null;
}

export type WasmSnapshotNamespaceKind = "branch" | "tag" | "checkpoint" | "custom";

export type WasmResolverName =
  | "prefer_left"
  | "prefer_right"
  | "delete_wins"
  | "update_wins";

type RawProllyWasmModule = typeof import("../pkg/prolly_wasm.js");

export type WasmTree = import("../pkg/prolly_wasm.js").WasmTree;
export type WasmConfig = import("../pkg/prolly_wasm.js").WasmConfig;
export type WasmRangeCursor = import("../pkg/prolly_wasm.js").WasmRangeCursor;
export type WasmReverseCursor = import("../pkg/prolly_wasm.js").WasmReverseCursor;
export type RawWasmTransaction = import("../pkg/prolly_wasm.js").WasmTransaction;
export type RawWasmProllyEngine = import("../pkg/prolly_wasm.js").WasmProllyEngine;

export interface WasmRootManifestRecord {
  tree: WasmTree;
  createdAtMillis?: number | null;
  updatedAtMillis?: number | null;
}

export interface WasmNamedRootUpdateRecord {
  applied: boolean;
  conflict: boolean;
  current?: WasmTree | null;
}

export interface WasmTransactionConflictRecord {
  name: Uint8Array;
  expected?: WasmRootManifestRecord | null;
  current?: WasmRootManifestRecord | null;
}

export interface WasmTransactionUpdateRecord {
  applied: boolean;
  conflict: boolean;
  nodesWritten: number;
  rootsWritten: number;
  conflictDetail?: WasmTransactionConflictRecord | null;
}

export interface WasmTransactionInstance
  extends Omit<RawWasmTransaction, "loadNamedRoot" | "compareAndSwapNamedRoot" | "commit"> {
  loadNamedRoot(name: Uint8Array): WasmTree | null;
  compareAndSwapNamedRoot(
    name: Uint8Array,
    expected?: WasmTree | null,
    replacement?: WasmTree | null,
  ): WasmNamedRootUpdateRecord;
  commit(): WasmTransactionUpdateRecord;
}

export interface WasmProllyEngineInstance
  extends Omit<
    RawWasmProllyEngine,
    | "beginTransaction"
    | "firstEntry"
    | "lastEntry"
    | "deleteRange"
    | "deleteRangeWithStats"
    | "loadNamedRoot"
    | "compareAndSwapNamedRoot"
    | "lowerBound"
    | "upperBound"
    | "prefix"
    | "prefixPage"
    | "prefixReversePage"
    | "reversePage"
    | "scanRange"
    | "scanPrefix"
    | "scanRangeReverse"
    | "scanPrefixReverse"
    | "scanDiff"
    | "scanRangeDiff"
    | "scanConflicts"
  > {
  beginTransaction(): WasmTransactionInstance;
  firstEntry(tree: WasmTree): WasmOptionalEntryRecord;
  lastEntry(tree: WasmTree): WasmOptionalEntryRecord;
  deleteRange(tree: WasmTree, start: Uint8Array, rangeEnd: Uint8Array): WasmTree;
  deleteRangeWithStats(
    tree: WasmTree,
    start: Uint8Array,
    rangeEnd: Uint8Array,
  ): WasmWriteResultRecord;
  loadNamedRoot(name: Uint8Array): WasmTree | null;
  compareAndSwapNamedRoot(
    name: Uint8Array,
    expected?: WasmTree | null,
    replacement?: WasmTree | null,
  ): WasmNamedRootUpdateRecord;
  lowerBound(tree: WasmTree, key: Uint8Array): WasmOptionalEntryRecord;
  upperBound(tree: WasmTree, key: Uint8Array): WasmOptionalEntryRecord;
  prefix(tree: WasmTree, prefix: Uint8Array): WasmEntryRecord[];
  prefixPage(
    tree: WasmTree,
    prefix: Uint8Array,
    cursor?: WasmRangeCursor | null,
    limit?: number,
  ): WasmRangePageRecord;
  prefixReversePage(
    tree: WasmTree,
    prefix: Uint8Array,
    cursor?: WasmReverseCursor | null,
    limit?: number,
  ): WasmReversePageRecord;
  reversePage(
    tree: WasmTree,
    cursor: WasmReverseCursor | null | undefined,
    start: Uint8Array,
    limit: number,
  ): WasmReversePageRecord;
  scanRange(
    tree: WasmTree,
    start: Uint8Array,
    end: Uint8Array | null | undefined,
    visitor: WasmEntryVisitor,
  ): WasmScanOutcomeRecord;
  scanPrefix(
    tree: WasmTree,
    prefix: Uint8Array,
    visitor: WasmEntryVisitor,
  ): WasmScanOutcomeRecord;
  scanRangeReverse(
    tree: WasmTree,
    start: Uint8Array,
    end: Uint8Array | null | undefined,
    visitor: WasmEntryVisitor,
  ): WasmScanOutcomeRecord;
  scanPrefixReverse(
    tree: WasmTree,
    prefix: Uint8Array,
    visitor: WasmEntryVisitor,
  ): WasmScanOutcomeRecord;
  scanDiff(
    base: WasmTree,
    other: WasmTree,
    visitor: WasmDiffVisitor,
  ): WasmScanOutcomeRecord;
  scanRangeDiff(
    base: WasmTree,
    other: WasmTree,
    start: Uint8Array,
    end: Uint8Array | null | undefined,
    visitor: WasmDiffVisitor,
  ): WasmScanOutcomeRecord;
  scanConflicts(
    base: WasmTree,
    left: WasmTree,
    right: WasmTree,
    visitor: WasmConflictVisitor,
  ): WasmScanOutcomeRecord;
}

export interface WasmProllyEngineConstructor {
  memory(): WasmProllyEngineInstance;
  memoryWithConfig(config: WasmConfig): WasmProllyEngineInstance;
  memoryWithConfigJson(json: string): WasmProllyEngineInstance;
}

export type ProllyWasmModule = Omit<RawProllyWasmModule, "WasmProllyEngine"> & {
  WasmProllyEngine: WasmProllyEngineConstructor;
};

export async function loadProllyWasm(
  modulePath = "../pkg/prolly_wasm.js",
  wasmInput?: WebAssembly.Module | BufferSource,
): Promise<ProllyWasmModule> {
  const module = (await import(modulePath)) as ProllyWasmModule;
  if (wasmInput && "initSync" in module) {
    module.initSync({ module: wasmInput });
    return module;
  }
  await module.default();
  return module;
}

type PortableNative = Record<string, any>;

function ownedPortableBytes(value: Uint8Array): Uint8Array {
  return Uint8Array.from(value);
}

function portableAbortError(): Error {
  const error = new Error("prolly WASM operation aborted");
  error.name = "AbortError";
  return error;
}

async function portablePromise<T>(signal: AbortSignal | undefined, operation: () => T): Promise<T> {
  if (signal?.aborted) throw portableAbortError();
  await Promise.resolve();
  if (signal?.aborted) throw portableAbortError();
  const result = operation();
  if (signal?.aborted) throw portableAbortError();
  return result;
}

export interface PortableIndexEntry {
  term: Uint8Array;
  projection?: Uint8Array;
}

export interface PortableIndexRegistration {
  name: Uint8Array;
  generation: bigint;
  extractorId: string;
  projection: "keys_only" | "include" | "all";
  extract(primaryKey: Uint8Array, sourceValue: Uint8Array): PortableIndexEntry[];
}

export interface PortableIndexedVersion {
  sourceVersion: Uint8Array;
  catalogVersion?: Uint8Array;
  indexCount: bigint;
}

export interface PortableIndexBuildResult {
  sourceVersion: Uint8Array;
  indexVersion: Uint8Array;
  catalogVersion: Uint8Array;
  generation: bigint;
  entries: bigint;
  attempts: bigint;
  activated: boolean;
}

export type PortableIndexedMutation =
  | { kind: "upsert"; key: Uint8Array; value: Uint8Array }
  | { kind: "delete"; key: Uint8Array };

export interface PortableIndexedUpdate {
  kind: "applied" | "unchanged" | "conflict";
  previousSourceVersion?: Uint8Array;
  current?: PortableIndexedVersion;
}

export interface PortableIndexedSnapshotId {
  sourceVersion: Uint8Array;
  catalogVersion: Uint8Array;
}

export interface PortableIndexVerification {
  name: Uint8Array;
  sourceVersion: Uint8Array;
  expectedIndexVersion: Uint8Array;
  actualIndexVersion: Uint8Array;
  expectedEntries: bigint;
  actualEntries: bigint;
  semanticDifferences: bigint;
  valid: boolean;
  canonical: boolean;
}

export interface PortableIndexedMapHealth {
  sourceMapId: Uint8Array;
  sourceVersion?: Uint8Array;
  catalogVersion?: Uint8Array;
  activeIndexes: Array<{
    name: Uint8Array; generation: bigint; fingerprint: Uint8Array;
    projection: "keys_only" | "include" | "all";
    indexMapId: Uint8Array; indexVersion: Uint8Array;
  }>;
  supportsTransactions: boolean;
}

export interface PortableIndexedMapMetrics {
  normalizedSourceMutations: bigint;
  recordsExtracted: bigint;
  termsEmitted: bigint;
  projectedBytes: bigint;
  physicalUpserts: bigint;
  physicalDeletes: bigint;
  unchangedEmissionsSkipped: bigint;
  sourceNodesWritten: bigint;
  indexNodesWritten: bigint;
  catalogNodesWritten: bigint;
  retries: bigint;
  buildAttempts: bigint;
  verificationOutcomes: bigint;
  retainedRoots: bigint;
}

export interface PortableIndexedRetention {
  retainedSourceVersions: Uint8Array[];
  removedSourceVersions: Uint8Array[];
  retainedIndexVersions: Uint8Array[];
  removedIndexVersions: Uint8Array[];
  removedCatalogVersions: Uint8Array[];
  removedCheckpointRecords: bigint;
  removedNamedRoots: Uint8Array[];
}

export interface PortableIndexMatch {
  term: Uint8Array;
  primaryKey: Uint8Array;
  projection?: Uint8Array;
}

export interface PortableIndexedSource extends PortableIndexMatch {
  sourceValue: Uint8Array;
}

export interface PortableIndexPage {
  matches: PortableIndexMatch[];
  nextCursor?: Uint8Array;
}

export interface PortableIndexPageOptions {
  pageSize?: number;
  signal?: AbortSignal;
}

export interface PortableProximityRecord {
  key: Uint8Array;
  vector: Float32Array;
  value?: Uint8Array;
}

export interface PortableSearchRequest {
  vector: Float32Array;
  topK: number;
  policy: "exact" | "fixed_budget" | "adaptive";
  adaptiveQuality?: "fast" | "balanced" | "high_recall";
  budget?: PortableSearchBudget;
  filter?: PortableSearchFilter;
  kernel?: "scalar_deterministic" | "simd_deterministic" | "auto_deterministic";
  backend?: "native" | "product_quantized" | "hnsw" | "composite" | "auto";
  hnswEfSearch?: number;
  pqRerankMultiplier?: number;
  signal?: AbortSignal;
}

export interface PortableSearchBudget {
  maxNodes?: bigint;
  maxCommittedBytes?: bigint;
  maxDistanceEvaluations?: bigint;
  maxFrontierEntries?: bigint;
}

export type PortableSearchFilter =
  | { kind: "all" }
  | { kind: "key_range"; start?: Uint8Array; rangeEnd?: Uint8Array }
  | { kind: "prefix"; prefix: Uint8Array }
  | { kind: "eligible_keys"; eligibleKeys: Uint8Array[] };

export interface PortableSearchResult {
  neighbors: Array<{ key: Uint8Array; value: Uint8Array; distance: number }>;
  stats: {
    levelsVisited: bigint;
    nodesRead: bigint;
    bytesRead: bigint;
    physicalBytesRead: bigint;
    committedBytes: bigint;
    distanceEvaluations: bigint;
    quantizedDistanceEvaluations: bigint;
    rerankedCandidates: bigint;
    frontierPeak: bigint;
    candidateHandlesPeak: bigint;
    candidateRetainedBytesPeak: bigint;
  };
  completion: string;
  backend: string;
  planFormatVersion: number;
}

export interface PortableNeighborView {
  key: Uint8Array;
  value: Uint8Array;
  distance: number;
  rank: number;
}

export interface HnswConfig {
  maxConnections: number;
  efConstruction: number;
  efSearch: number;
  levelBits: number;
  overfetchMultiplier: number;
  seed: bigint;
  routingVectorEncoding: "full_f32";
}

export interface HnswBuildLimits {
  maxRecords?: bigint;
  maxOwnedBytes?: bigint;
  maxDistanceEvaluations?: bigint;
  workerThreads: bigint;
  maxEncodedGraphBytes?: bigint;
}

export interface HnswBuildStats {
  records: bigint;
  distanceEvaluations: bigint;
  directedEdges: bigint;
  maximumLevel: number;
  ownedBytes: bigint;
  encodedGraphBytes: bigint;
}

export interface HnswBuildOptions {
  config?: HnswConfig;
  limits?: HnswBuildLimits;
  signal?: AbortSignal;
}

export interface HnswBuildResult {
  index: WasmHnswIndex;
  stats: HnswBuildStats;
}

export function defaultHnswConfig(): HnswConfig {
  return {
    maxConnections: 16,
    efConstruction: 128,
    efSearch: 64,
    levelBits: 4,
    overfetchMultiplier: 8,
    seed: 0n,
    routingVectorEncoding: "full_f32",
  };
}

export function defaultHnswBuildLimits(): HnswBuildLimits {
  return { workerThreads: 1n };
}

export interface ProductQuantizationConfig {
  subquantizers: number;
  centroidsPerSubquantizer: number;
  trainingIterations: number;
  rerankMultiplier: number;
  seed: bigint;
  maxTrainingVectors: bigint;
}

export interface ProductQuantizationBuildLimits {
  maxTrainingVectors?: bigint;
  maxTrainingBytes?: bigint;
  maxTemporaryCodeBytes?: bigint;
  maxDistanceEvaluations?: bigint;
  maxEncodedOutputBytes?: bigint;
  maxWorkerThreads?: bigint;
}

export interface ProductQuantizationBuildStats {
  trainingDistanceEvaluations: bigint;
  encodingDistanceEvaluations: bigint;
  encodedVectors: bigint;
  trainingVectors: bigint;
  trainingBytes: bigint;
  encodedOutputBytes: bigint;
}

export interface ProductQuantizationQuality {
  meanSquaredError: number;
  maximumSquaredError: number;
}

export interface ProductQuantizationBuildOptions {
  config?: ProductQuantizationConfig;
  workerThreads?: bigint;
  limits?: ProductQuantizationBuildLimits;
  signal?: AbortSignal;
}

export interface ProductQuantizationBuildResult {
  index: WasmProductQuantizer;
  stats: ProductQuantizationBuildStats;
}

export function defaultPqConfig(): ProductQuantizationConfig {
  return {
    subquantizers: 8,
    centroidsPerSubquantizer: 256,
    trainingIterations: 12,
    rerankMultiplier: 8,
    seed: 0n,
    maxTrainingVectors: 65_536n,
  };
}

export function defaultPqBuildLimits(): ProductQuantizationBuildLimits {
  return {};
}

export interface CompositeAcceleratorConfig {
  maxDeltaRecords: bigint;
  maxShadowRecords: bigint;
  maxDeltaRatioPpm: number;
  maxShadowRatioPpm: number;
  baseOverfetchMultiplier: number;
}
export interface CompositeBuildLimits {
  maxDiffEntries?: bigint; maxOwnedBytes?: bigint;
  maxEncodedOutputBytes?: bigint; maxDistanceEvaluations?: bigint;
}
export interface CompositeBuildStats {
  diffEntries: bigint; insertedRecords: bigint; vectorUpdatedRecords: bigint;
  valueOnlyRecords: bigint; deletedRecords: bigint; deltaRecords: bigint;
  shadowRecords: bigint; ownedBytesPeak: bigint; encodedOutputBytes: bigint;
  distanceEvaluations: bigint;
}
export interface FullRebuildReason {
  kind: "delta_records" | "shadow_records" | "delta_ratio" | "shadow_ratio";
  actual: bigint; maximum: bigint;
}
export interface CompositeRebuildOptions {
  hnswLimits?: HnswBuildLimits;
  pqWorkerThreads?: bigint;
  pqLimits?: ProductQuantizationBuildLimits;
}
export interface CompositeBuildOptions {
  config?: CompositeAcceleratorConfig; limits?: CompositeBuildLimits; signal?: AbortSignal;
}
export interface CompositeBuildOrRebuildOptions extends CompositeBuildOptions { rebuild?: CompositeRebuildOptions; }
export interface CompositeBuildOutcome {
  accelerator?: WasmCompositeAccelerator; reasons: FullRebuildReason[]; stats: CompositeBuildStats;
}
export interface CompositeBuildOrRebuildOutcome {
  kind: "composite" | "no_accelerator_required" | "hnsw_rebuilt" | "product_quantized_rebuilt";
  composite?: WasmCompositeAccelerator; hnsw?: WasmHnswIndex; pq?: WasmProductQuantizer;
  reasons: FullRebuildReason[]; compositeStats: CompositeBuildStats;
  hnswStats?: HnswBuildStats; pqStats?: ProductQuantizationBuildStats;
}
export interface AcceleratorCatalogEntry {
  kind: "hnsw" | "product_quantized" | "composite";
  configurationFingerprint: Uint8Array; manifest: Uint8Array;
}
export function defaultCompositeAcceleratorConfig(): CompositeAcceleratorConfig {
  return { maxDeltaRecords: 4_096n, maxShadowRecords: 8_192n, maxDeltaRatioPpm: 100_000,
    maxShadowRatioPpm: 200_000, baseOverfetchMultiplier: 2 };
}
export function defaultCompositeBuildLimits(): CompositeBuildLimits { return {}; }

function requireUnsignedInteger(value: number, name: string, maximum: number): number {
  if (!Number.isInteger(value) || value < 0 || value > maximum) {
    throw new RangeError(`${name} must be an unsigned integer no greater than ${maximum}`);
  }
  return value;
}

function requireUnsignedBigInt(value: bigint, name: string): bigint {
  if (value < 0n || value > 0xffff_ffff_ffff_ffffn) {
    throw new RangeError(`${name} must fit an unsigned 64-bit integer`);
  }
  return value;
}

function ownHnswConfig(value: HnswConfig | undefined): object {
  const config = value ?? defaultHnswConfig();
  if (config.routingVectorEncoding !== "full_f32") {
    throw new TypeError("unsupported HNSW routing-vector encoding");
  }
  const seed = requireUnsignedBigInt(config.seed, "seed");
  if (seed > 0xffff_ffff_ffff_ffffn) throw new RangeError("seed must fit u64");
  return {
    maxConnections: requireUnsignedInteger(config.maxConnections, "maxConnections", 0xffff),
    efConstruction: requireUnsignedInteger(config.efConstruction, "efConstruction", 0xffff_ffff),
    efSearch: requireUnsignedInteger(config.efSearch, "efSearch", 0xffff_ffff),
    levelBits: requireUnsignedInteger(config.levelBits, "levelBits", 0xff),
    overfetchMultiplier: requireUnsignedInteger(
      config.overfetchMultiplier, "overfetchMultiplier", 0xffff_ffff,
    ),
    seed: seed.toString(),
    routingVectorEncoding: config.routingVectorEncoding,
  };
}

function ownHnswBuildLimits(value: HnswBuildLimits | undefined): object {
  const limits = value ?? defaultHnswBuildLimits();
  const optional = (candidate: bigint | undefined, name: string) =>
    candidate == null ? undefined : requireUnsignedBigInt(candidate, name).toString();
  return {
    maxRecords: optional(limits.maxRecords, "maxRecords"),
    maxOwnedBytes: optional(limits.maxOwnedBytes, "maxOwnedBytes"),
    maxDistanceEvaluations: optional(limits.maxDistanceEvaluations, "maxDistanceEvaluations"),
    workerThreads: requireUnsignedBigInt(limits.workerThreads, "workerThreads").toString(),
    maxEncodedGraphBytes: optional(limits.maxEncodedGraphBytes, "maxEncodedGraphBytes"),
  };
}

function ownPqConfig(value: ProductQuantizationConfig | undefined): object {
  const config = value ?? defaultPqConfig();
  return {
    subquantizers: requireUnsignedInteger(config.subquantizers, "subquantizers", 0xffff_ffff),
    centroidsPerSubquantizer: requireUnsignedInteger(
      config.centroidsPerSubquantizer, "centroidsPerSubquantizer", 0xffff,
    ),
    trainingIterations: requireUnsignedInteger(
      config.trainingIterations, "trainingIterations", 0xffff,
    ),
    rerankMultiplier: requireUnsignedInteger(
      config.rerankMultiplier, "rerankMultiplier", 0xffff_ffff,
    ),
    seed: requireUnsignedBigInt(config.seed, "seed").toString(),
    maxTrainingVectors: requireUnsignedBigInt(
      config.maxTrainingVectors, "maxTrainingVectors",
    ).toString(),
  };
}

function ownPqBuildLimits(value: ProductQuantizationBuildLimits | undefined): object {
  const limits = value ?? defaultPqBuildLimits();
  const optional = (candidate: bigint | undefined, name: string) =>
    candidate == null ? undefined : requireUnsignedBigInt(candidate, name).toString();
  return {
    maxTrainingVectors: optional(limits.maxTrainingVectors, "maxTrainingVectors"),
    maxTrainingBytes: optional(limits.maxTrainingBytes, "maxTrainingBytes"),
    maxTemporaryCodeBytes: optional(limits.maxTemporaryCodeBytes, "maxTemporaryCodeBytes"),
    maxDistanceEvaluations: optional(limits.maxDistanceEvaluations, "maxDistanceEvaluations"),
    maxEncodedOutputBytes: optional(limits.maxEncodedOutputBytes, "maxEncodedOutputBytes"),
    maxWorkerThreads: optional(limits.maxWorkerThreads, "maxWorkerThreads"),
  };
}

function ownCompositeConfig(value: CompositeAcceleratorConfig | undefined): object {
  const config = value ?? defaultCompositeAcceleratorConfig();
  return {
    maxDeltaRecords: requireUnsignedBigInt(config.maxDeltaRecords, "maxDeltaRecords").toString(),
    maxShadowRecords: requireUnsignedBigInt(config.maxShadowRecords, "maxShadowRecords").toString(),
    maxDeltaRatioPpm: requireUnsignedInteger(config.maxDeltaRatioPpm, "maxDeltaRatioPpm", 1_000_000),
    maxShadowRatioPpm: requireUnsignedInteger(config.maxShadowRatioPpm, "maxShadowRatioPpm", 1_000_000),
    baseOverfetchMultiplier: requireUnsignedInteger(config.baseOverfetchMultiplier, "baseOverfetchMultiplier", 0xffff_ffff),
  };
}

function ownCompositeBuildLimits(value: CompositeBuildLimits | undefined): object {
  const limits = value ?? defaultCompositeBuildLimits();
  const optional = (candidate: bigint | undefined, name: string) =>
    candidate == null ? undefined : requireUnsignedBigInt(candidate, name).toString();
  return {
    maxDiffEntries: optional(limits.maxDiffEntries, "maxDiffEntries"),
    maxOwnedBytes: optional(limits.maxOwnedBytes, "maxOwnedBytes"),
    maxEncodedOutputBytes: optional(limits.maxEncodedOutputBytes, "maxEncodedOutputBytes"),
    maxDistanceEvaluations: optional(limits.maxDistanceEvaluations, "maxDistanceEvaluations"),
  };
}

function ownCompositeRebuildOptions(value: CompositeRebuildOptions | undefined): object {
  const options = value ?? {};
  const workerThreads = requireUnsignedBigInt(options.pqWorkerThreads ?? 1n, "pqWorkerThreads");
  if (workerThreads !== 1n) {
    throw new RangeError("browser-safe WASM composite PQ rebuild requires pqWorkerThreads = 1");
  }
  return {
    hnswLimits: ownHnswBuildLimits(options.hnswLimits),
    pqWorkerThreads: workerThreads.toString(),
    pqLimits: ownPqBuildLimits(options.pqLimits),
  };
}

function ownPortableSearchRequest(request: PortableSearchRequest): object {
  if (!Number.isSafeInteger(request.topK) || request.topK <= 0) {
    throw new RangeError("topK must be a positive safe integer");
  }
  if (request.policy === "adaptive" && request.adaptiveQuality == null) {
    throw new TypeError("adaptive search requires adaptiveQuality");
  }
  const filter = request.filter ?? { kind: "all" as const };
  let ownedFilter: object;
  switch (filter.kind) {
    case "all":
      ownedFilter = { kind: "all" };
      break;
    case "key_range":
      ownedFilter = {
        kind: "key_range",
        start: filter.start == null ? undefined : ownedPortableBytes(filter.start),
        rangeEnd: filter.rangeEnd == null ? undefined : ownedPortableBytes(filter.rangeEnd),
      };
      break;
    case "prefix":
      ownedFilter = { kind: "prefix", prefix: ownedPortableBytes(filter.prefix) };
      break;
    case "eligible_keys":
      ownedFilter = { kind: "eligible_keys", eligibleKeys: filter.eligibleKeys.map(ownedPortableBytes) };
      break;
  }
  const budget = request.budget ?? {};
  return {
    query: new Float32Array(request.vector),
    k: request.topK,
    policy: request.policy,
    adaptiveQuality: request.adaptiveQuality,
    budget: {
      maxNodes: budget.maxNodes?.toString(),
      maxCommittedBytes: budget.maxCommittedBytes?.toString(),
      maxDistanceEvaluations: budget.maxDistanceEvaluations?.toString(),
      maxFrontierEntries: budget.maxFrontierEntries?.toString(),
    },
    filter: ownedFilter,
    kernel: request.kernel ?? "auto_deterministic",
    backend: request.backend ?? "native",
    hnswEfSearch: request.hnswEfSearch,
    pqRerankMultiplier: request.pqRerankMultiplier,
  };
}

export interface PortableProximityConfig {
  dimensions: number;
  metric: "l2_squared" | "cosine" | "inner_product";
  logChunkSize: number;
  levelHashSeed: bigint;
  minPageBytes: number;
  targetPageBytes: number;
  maxPageBytes: number;
  overflowHashSeed: bigint;
  inlineThresholdBytes: number;
  scalarQuantizationGroupSize?: number;
}

export interface PortableProximityMutation {
  key: Uint8Array;
  vector?: Float32Array;
  value?: Uint8Array;
}

export interface PortableProximityVerification {
  recordCount: bigint;
  proximityNodeCount: bigint;
  externalVectorCount: bigint;
  quantizedNodeCount: bigint;
  scalarQuantizerCount: bigint;
  overflowPageCount: bigint;
  overflowDirectoryCount: bigint;
  maximumLevel: number;
  maximumNodeBytes: bigint;
  distanceChecks: bigint;
}

export class WasmViewExpiredError extends Error {
  constructor() {
    super("scoped WASM view has expired");
    this.name = "WasmViewExpiredError";
  }
}

function scopedPortableBytes(
  value: Uint8Array,
  scope: { alive: boolean },
  memory?: WebAssembly.Memory,
): Uint8Array {
  const borrowedBuffer = value.buffer;
  const check = () => {
    if (!scope.alive || (memory != null && memory.buffer !== borrowedBuffer)) {
      throw new WasmViewExpiredError();
    }
  };
  const mutators = new Set<PropertyKey>(["copyWithin", "fill", "reverse", "set", "sort"]);
  const iterators = new Set<PropertyKey>([Symbol.iterator, "entries", "keys", "values"]);
  return new Proxy(value, {
    get(target, property) {
      check();
      if (property === "buffer") {
        throw new TypeError("the backing WASM memory of a scoped view is not exposed; copy the view instead");
      }
      if (mutators.has(property)) {
        return () => { throw new TypeError("scoped WASM views are read-only"); };
      }
      if (property === "subarray") {
        return (begin?: number, end?: number) => scopedPortableBytes(target.subarray(begin, end), scope, memory);
      }
      if (iterators.has(property)) {
        return () => {
          const iteratorFactory = Reflect.get(target, property, target) as () => Iterator<unknown>;
          const iterator = iteratorFactory.call(target);
          return {
            next(): IteratorResult<unknown> {
              check();
              return iterator.next();
            },
            [Symbol.iterator]() { return this; },
          };
        };
      }
      const member = Reflect.get(target, property, target);
      return typeof member === "function"
        ? (...args: unknown[]) => {
          check();
          return member.apply(target, args);
        }
        : member;
    },
    set() { throw new TypeError("scoped WASM views are read-only"); },
  });
}

export class Engine implements Disposable {
  #module?: PortableNative;
  #native?: any;

  private constructor(module: PortableNative, native: any) {
    this.#module = module;
    this.#native = native;
  }

  static memory(module: PortableNative): Engine {
    return new Engine(module, module.WasmProllyEngine.memory());
  }

  static file(_module: PortableNative, _path: string): never {
    throw new Error("filesystem engines are unsupported in WASM");
  }

  static sqlite(_module: PortableNative, _path: string): never {
    throw new Error("SQLite engines are unsupported in WASM");
  }

  #open(): any {
    if (this.#native == null) throw new Error("WASM engine is closed");
    return this.#native;
  }

  versionedMap(id: Uint8Array): WasmVersionedMap {
    return new WasmVersionedMap(
      this.#open().versionedMap(ownedPortableBytes(id)), this.#module?.wasmMemory(),
    );
  }

  beginVersionedTransaction(): WasmVersionedTransaction {
    return new WasmVersionedTransaction(this.#open().beginVersionedTransaction());
  }

  indexRegistry(): WasmIndexRegistry {
    return new WasmIndexRegistry(this.#open().indexRegistry());
  }

  indexedMap(id: Uint8Array, registry: WasmIndexRegistry): WasmIndexedMap {
    return new WasmIndexedMap(
      this.#open().indexedMap(ownedPortableBytes(id), registry.nativeHandle()),
    );
  }

  buildProximity(
    dimensions: number,
    records: PortableProximityRecord[],
    signal?: AbortSignal,
  ): Promise<WasmProximityMap> {
    const native = this.#open();
    const owned = records.map((record) => ({
      key: ownedPortableBytes(record.key),
      vector: new Float32Array(record.vector),
      value: ownedPortableBytes(record.value ?? new Uint8Array()),
    }));
    return portablePromise(signal, () =>
      new WasmProximityMap(native.buildProximity(dimensions, owned), this.#module?.wasmMemory()));
  }

  close(): void {
    this.#native?.free?.();
    this.#native = undefined;
    this.#module = undefined;
  }

  [Symbol.dispose](): void { this.close(); }
}

export interface WasmMapVersion {
  id: Uint8Array;
  createdAtMillis?: bigint;
  isHead: boolean;
}

export interface WasmMapMutation {
  kind: "upsert" | "delete";
  key: Uint8Array;
  value?: Uint8Array;
}

export interface WasmMapUpdate {
  kind: "applied" | "unchanged" | "conflict";
  previous?: Uint8Array;
  current?: WasmMapVersion;
}

export interface WasmVersionPrune {
  retained: Uint8Array[];
  removed: Uint8Array[];
}

export interface WasmVersionedParallelConfig { maxThreads: bigint; parallelismThreshold: bigint; }
export interface WasmVersionedBatchApplyStats {
  inputMutations: bigint;
  effectiveMutations: bigint;
  preprocessInputSorted: boolean;
  affectedLeaves: bigint;
  changedLeaves: bigint;
  sparseLeafApplies: bigint;
  writtenNodes: bigint;
  writtenBytes: bigint;
  usedAppendFastPath: boolean;
  usedBatchedRoute: boolean;
  usedCoalescedRebuild: boolean;
  usedDeferredRebalancing: boolean;
  usedBottomUpRebuild: boolean;
  cacheWrittenNodes: boolean;
}
export interface WasmVersionedMapBatchResult {
  version: WasmMapVersion;
  stats: WasmVersionedBatchApplyStats;
}

export interface WasmCatalogVerification {
  head: Uint8Array;
  versionCount: bigint;
  reachableNodes: bigint;
  reachableBytes: bigint;
}

export interface WasmGcReachability {
  liveCids: Uint8Array[];
  liveNodes: bigint;
  liveBytes: bigint;
  leafNodes: bigint;
  internalNodes: bigint;
}

export interface WasmGcPlan {
  reachability: WasmGcReachability;
  candidateNodes: bigint;
  reclaimableCids: Uint8Array[];
  reclaimableNodes: bigint;
  reclaimableBytes: bigint;
  missingCandidates: bigint;
}

export interface WasmGcSweep {
  plan: WasmGcPlan;
  deletedNodes: bigint;
  deletedBytes: bigint;
}

export interface WasmNamedRootRetention {
  kind: "all" | "exact" | "prefix" | "newest_by_name" | "updated_since";
  names: Uint8Array[];
  prefix?: Uint8Array;
  count?: bigint;
  minUpdatedAtMillis?: bigint;
}

export interface WasmMapDiff {
  kind: "added" | "removed" | "changed";
  key: Uint8Array;
  value?: Uint8Array;
  old?: Uint8Array;
  newValue?: Uint8Array;
}

export interface WasmDiffPage {
  diffs: WasmMapDiff[];
  nextCursor?: WasmRangeCursorRecord | null;
}

export interface WasmMapChangeEvent {
  previous?: Uint8Array;
  current: WasmMapVersion;
  diffs: WasmMapDiff[];
}

export interface WasmVersionedTransactionCommit {
  applied: boolean;
  versions: WasmMapVersion[];
  conflictMapId?: Uint8Array;
  conflictCurrent?: WasmMapVersion;
}

function wasmMapVersion(value: any): WasmMapVersion {
  return {
    id: value.id,
    createdAtMillis: value.createdAtMillis == null ? undefined : BigInt(value.createdAtMillis),
    isHead: value.isHead,
  };
}

function wasmMapUpdate(value: any): WasmMapUpdate {
  return {
    kind: value.kind,
    previous: value.previous ?? undefined,
    current: value.current == null ? undefined : wasmMapVersion(value.current),
  };
}

function wasmVersionedBatchResult(value: any): WasmVersionedMapBatchResult {
  return {
    version: wasmMapVersion(value.version),
    stats: {
      inputMutations: BigInt(value.stats.inputMutations),
      effectiveMutations: BigInt(value.stats.effectiveMutations),
      preprocessInputSorted: value.stats.preprocessInputSorted,
      affectedLeaves: BigInt(value.stats.affectedLeaves),
      changedLeaves: BigInt(value.stats.changedLeaves),
      sparseLeafApplies: BigInt(value.stats.sparseLeafApplies),
      writtenNodes: BigInt(value.stats.writtenNodes),
      writtenBytes: BigInt(value.stats.writtenBytes),
      usedAppendFastPath: value.stats.usedAppendFastPath,
      usedBatchedRoute: value.stats.usedBatchedRoute,
      usedCoalescedRebuild: value.stats.usedCoalescedRebuild,
      usedDeferredRebalancing: value.stats.usedDeferredRebalancing,
      usedBottomUpRebuild: value.stats.usedBottomUpRebuild,
      cacheWrittenNodes: value.stats.cacheWrittenNodes,
    },
  };
}

function wasmCatalogVerification(value: any): WasmCatalogVerification {
  return {
    head: value.head,
    versionCount: BigInt(value.versionCount),
    reachableNodes: BigInt(value.reachableNodes),
    reachableBytes: BigInt(value.reachableBytes),
  };
}

function wasmGcPlan(value: any): WasmGcPlan {
  return {
    reachability: {
      liveCids: value.reachability.liveCids,
      liveNodes: BigInt(value.reachability.liveNodes),
      liveBytes: BigInt(value.reachability.liveBytes),
      leafNodes: BigInt(value.reachability.leafNodes),
      internalNodes: BigInt(value.reachability.internalNodes),
    },
    candidateNodes: BigInt(value.candidateNodes),
    reclaimableCids: value.reclaimableCids,
    reclaimableNodes: BigInt(value.reclaimableNodes),
    reclaimableBytes: BigInt(value.reclaimableBytes),
    missingCandidates: BigInt(value.missingCandidates),
  };
}

function wasmNamedRootRetention(value: any): WasmNamedRootRetention {
  return {
    kind: value.kind,
    names: value.names,
    prefix: value.prefix,
    count: value.count == null ? undefined : BigInt(value.count),
    minUpdatedAtMillis: value.minUpdatedAtMillis == null ? undefined : BigInt(value.minUpdatedAtMillis),
  };
}

function ownedWasmMutations(mutations: readonly WasmMapMutation[]): WasmMapMutation[] {
  return mutations.map((mutation) => ({
    kind: mutation.kind,
    key: ownedPortableBytes(mutation.key),
    value: mutation.value == null ? undefined : ownedPortableBytes(mutation.value),
  }));
}

function ownedWasmEntries(entries: readonly WasmEntryRecord[]): WasmEntryRecord[] {
  return entries.map((entry) => ({
    key: ownedPortableBytes(entry.key),
    value: ownedPortableBytes(entry.value),
  }));
}

export class WasmVersionedMap implements Disposable {
  #native?: any;
  #memory?: WebAssembly.Memory;
  constructor(native: any, memory?: WebAssembly.Memory) { this.#native = native; this.#memory = memory; }
  #open(): any {
    if (this.#native == null) throw new Error("WASM versioned map is closed");
    return this.#native;
  }
  id(): Uint8Array { return this.#open().id(); }
  isInitialized(signal?: AbortSignal): Promise<boolean> {
    const native = this.#open();
    return portablePromise(signal, () => native.isInitialized());
  }
  initialize(signal?: AbortSignal): Promise<WasmMapVersion> {
    const native = this.#open();
    return portablePromise(signal, () => wasmMapVersion(native.initialize()));
  }
  initializeSorted(entries: readonly WasmEntryRecord[], signal?: AbortSignal): Promise<WasmMapUpdate> {
    const native = this.#open(); const owned = ownedWasmEntries(entries);
    return portablePromise(signal, () => wasmMapUpdate(native.initializeSorted(owned)));
  }
  head(signal?: AbortSignal): Promise<WasmMapVersion | undefined> {
    const native = this.#open();
    return portablePromise(signal, () => {
      const value = native.head();
      return value == null ? undefined : wasmMapVersion(value);
    });
  }
  headId(signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open();
    return portablePromise(signal, () => native.headId() ?? undefined);
  }
  version(id: Uint8Array, signal?: AbortSignal): Promise<WasmMapVersion | undefined> {
    const native = this.#open(); id = ownedPortableBytes(id);
    return portablePromise(signal, () => {
      const value = native.version(id);
      return value == null ? undefined : wasmMapVersion(value);
    });
  }
  versions(signal?: AbortSignal): Promise<WasmMapVersion[]> {
    const native = this.#open();
    return portablePromise(signal, () => native.versions().map(wasmMapVersion));
  }
  get(key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open(); key = ownedPortableBytes(key);
    return portablePromise(signal, () => native.get(key) ?? undefined);
  }
  containsKey(key: Uint8Array, signal?: AbortSignal): Promise<boolean> {
    const native = this.#open(); key = ownedPortableBytes(key);
    return portablePromise(signal, () => native.containsKey(key));
  }
  getMany(keys: readonly Uint8Array[], signal?: AbortSignal): Promise<Array<Uint8Array | undefined>> {
    const native = this.#open(); const owned = keys.map(ownedPortableBytes);
    return portablePromise(signal, () => native.getMany(owned).map((value: Uint8Array | null) => value ?? undefined));
  }
  getAt(id: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open(); id = ownedPortableBytes(id); key = ownedPortableBytes(key);
    return portablePromise(signal, () => native.getAt(id, key) ?? undefined);
  }
  getManyAt(id: Uint8Array, keys: readonly Uint8Array[], signal?: AbortSignal): Promise<Array<Uint8Array | undefined>> {
    const native = this.#open(); id = ownedPortableBytes(id); const owned = keys.map(ownedPortableBytes);
    return portablePromise(signal, () => native.getManyAt(id, owned).map((value: Uint8Array | null) => value ?? undefined));
  }
  range(start: Uint8Array = new Uint8Array(), end?: Uint8Array, signal?: AbortSignal): Promise<WasmEntryRecord[]> {
    const native = this.#open(); start = ownedPortableBytes(start);
    const ownedEnd = end == null ? undefined : ownedPortableBytes(end);
    return portablePromise(signal, () => native.range(start, ownedEnd));
  }
  prefix(prefix: Uint8Array, signal?: AbortSignal): Promise<WasmEntryRecord[]> {
    const native = this.#open(); prefix = ownedPortableBytes(prefix);
    return portablePromise(signal, () => native.prefix(prefix));
  }
  rangeAt(id: Uint8Array, start: Uint8Array = new Uint8Array(), end?: Uint8Array, signal?: AbortSignal): Promise<WasmEntryRecord[]> {
    const native = this.#open(); id = ownedPortableBytes(id); start = ownedPortableBytes(start);
    const ownedEnd = end == null ? undefined : ownedPortableBytes(end);
    return portablePromise(signal, () => native.rangeAt(id, start, ownedEnd));
  }
  prefixAt(id: Uint8Array, prefix: Uint8Array, signal?: AbortSignal): Promise<WasmEntryRecord[]> {
    const native = this.#open(); id = ownedPortableBytes(id); prefix = ownedPortableBytes(prefix);
    return portablePromise(signal, () => native.prefixAt(id, prefix));
  }
  rangePage(cursor?: WasmRangeCursorRecord, end?: Uint8Array, limit = 256, signal?: AbortSignal): Promise<WasmRangePageRecord> {
    const native = this.#open(); const ownedEnd = end == null ? undefined : ownedPortableBytes(end);
    return portablePromise(signal, () => native.rangePage(cursor, ownedEnd, limit));
  }
  prefixPage(prefix: Uint8Array, cursor?: WasmRangeCursorRecord, limit = 256, signal?: AbortSignal): Promise<WasmRangePageRecord> {
    const native = this.#open(); prefix = ownedPortableBytes(prefix);
    return portablePromise(signal, () => native.prefixPage(prefix, cursor, limit));
  }
  rangePageAt(id: Uint8Array, cursor?: WasmRangeCursorRecord, end?: Uint8Array, limit = 256, signal?: AbortSignal): Promise<WasmRangePageRecord> {
    const native = this.#open(); id = ownedPortableBytes(id);
    const ownedEnd = end == null ? undefined : ownedPortableBytes(end);
    return portablePromise(signal, () => native.rangePageAt(id, cursor, ownedEnd, limit));
  }
  prefixPageAt(id: Uint8Array, prefix: Uint8Array, cursor?: WasmRangeCursorRecord, limit = 256, signal?: AbortSignal): Promise<WasmRangePageRecord> {
    const native = this.#open(); id = ownedPortableBytes(id); prefix = ownedPortableBytes(prefix);
    return portablePromise(signal, () => native.prefixPageAt(id, prefix, cursor, limit));
  }
  diff(base: Uint8Array, target: Uint8Array, signal?: AbortSignal): Promise<WasmMapDiff[]> {
    const native = this.#open(); base = ownedPortableBytes(base); target = ownedPortableBytes(target);
    return portablePromise(signal, () => native.diff(base, target));
  }
  changesSince(base: Uint8Array, signal?: AbortSignal): Promise<WasmMapDiff[]> {
    const native = this.#open(); base = ownedPortableBytes(base);
    return portablePromise(signal, () => native.changesSince(base));
  }
  rollbackTo(id: Uint8Array, signal?: AbortSignal): Promise<WasmMapVersion> {
    const native = this.#open(); id = ownedPortableBytes(id);
    return portablePromise(signal, () => wasmMapVersion(native.rollbackTo(id)));
  }
  put(key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<WasmMapVersion> {
    const native = this.#open(); key = ownedPortableBytes(key); value = ownedPortableBytes(value);
    return portablePromise(signal, () => wasmMapVersion(native.put(key, value)));
  }
  apply(mutations: readonly WasmMapMutation[], signal?: AbortSignal): Promise<WasmMapVersion> {
    const native = this.#open(); const owned = ownedWasmMutations(mutations);
    return portablePromise(signal, () => wasmMapVersion(native.apply(owned)));
  }
  append(mutations: readonly WasmMapMutation[], signal?: AbortSignal): Promise<WasmMapVersion> {
    const native = this.#open(); const owned = ownedWasmMutations(mutations);
    return portablePromise(signal, () => wasmMapVersion(native.append(owned)));
  }
  parallelApply(mutations: readonly WasmMapMutation[], config: WasmVersionedParallelConfig, signal?: AbortSignal): Promise<WasmVersionedMapBatchResult> {
    const native = this.#open(); const owned = ownedWasmMutations(mutations);
    const ownedConfig = {
      maxThreads: config.maxThreads.toString(),
      parallelismThreshold: config.parallelismThreshold.toString(),
    };
    return portablePromise(signal, () => wasmVersionedBatchResult(native.parallelApply(owned, ownedConfig)));
  }
  rebuildSortedIf(expected: Uint8Array | undefined, entries: readonly WasmEntryRecord[], signal?: AbortSignal): Promise<WasmMapUpdate> {
    const native = this.#open(); const ownedExpected = expected == null ? undefined : ownedPortableBytes(expected);
    const owned = ownedWasmEntries(entries);
    return portablePromise(signal, () => wasmMapUpdate(native.rebuildSortedIf(ownedExpected, owned)));
  }
  rebuildFromEntriesIf(expected: Uint8Array | undefined, entries: readonly WasmEntryRecord[], signal?: AbortSignal): Promise<WasmMapUpdate> {
    const native = this.#open(); const ownedExpected = expected == null ? undefined : ownedPortableBytes(expected);
    const owned = ownedWasmEntries(entries);
    return portablePromise(signal, () => wasmMapUpdate(native.rebuildFromEntriesIf(ownedExpected, owned)));
  }
  rebuildFromIterIf(expected: Uint8Array | undefined, entries: readonly WasmEntryRecord[], signal?: AbortSignal): Promise<WasmMapUpdate> {
    return this.rebuildFromEntriesIf(expected, entries, signal);
  }
  applyAtMillis(mutations: readonly WasmMapMutation[], timestampMillis: bigint, signal?: AbortSignal): Promise<WasmMapVersion> {
    const native = this.#open(); const owned = ownedWasmMutations(mutations);
    return portablePromise(signal, () => wasmMapVersion(native.applyAtMillis(owned, timestampMillis)));
  }
  applyIf(expected: Uint8Array | undefined, mutations: readonly WasmMapMutation[], signal?: AbortSignal): Promise<WasmMapUpdate> {
    const native = this.#open();
    const ownedExpected = expected == null ? undefined : ownedPortableBytes(expected);
    const owned = ownedWasmMutations(mutations);
    return portablePromise(signal, () => wasmMapUpdate(native.applyIf(ownedExpected, owned)));
  }
  applyIfAtMillis(expected: Uint8Array | undefined, mutations: readonly WasmMapMutation[], timestampMillis: bigint, signal?: AbortSignal): Promise<WasmMapUpdate> {
    const native = this.#open();
    const ownedExpected = expected == null ? undefined : ownedPortableBytes(expected);
    const owned = ownedWasmMutations(mutations);
    return portablePromise(signal, () => wasmMapUpdate(native.applyIfAtMillis(ownedExpected, owned, timestampMillis)));
  }
  putIf(expected: Uint8Array | undefined, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<WasmMapUpdate> {
    const native = this.#open();
    const ownedExpected = expected == null ? undefined : ownedPortableBytes(expected);
    key = ownedPortableBytes(key); value = ownedPortableBytes(value);
    return portablePromise(signal, () => wasmMapUpdate(native.putIf(ownedExpected, key, value)));
  }
  deleteIf(expected: Uint8Array | undefined, key: Uint8Array, signal?: AbortSignal): Promise<WasmMapUpdate> {
    const native = this.#open();
    const ownedExpected = expected == null ? undefined : ownedPortableBytes(expected); key = ownedPortableBytes(key);
    return portablePromise(signal, () => wasmMapUpdate(native.deleteIf(ownedExpected, key)));
  }
  delete(key: Uint8Array, signal?: AbortSignal): Promise<WasmMapVersion> {
    const native = this.#open(); key = ownedPortableBytes(key);
    return portablePromise(signal, () => wasmMapVersion(native.delete(key)));
  }
  snapshot(signal?: AbortSignal): Promise<WasmMapSnapshot | undefined> {
    const native = this.#open();
    return portablePromise(signal, () => {
      const value = native.snapshot();
      return value == null ? undefined : new WasmMapSnapshot(value, this.#memory);
    });
  }
  snapshotAt(id: Uint8Array, signal?: AbortSignal): Promise<WasmMapSnapshot | undefined> {
    const native = this.#open(); id = ownedPortableBytes(id);
    return portablePromise(signal, () => {
      const value = native.snapshotAt(id);
      return value == null ? undefined : new WasmMapSnapshot(value, this.#memory);
    });
  }
  compare(base: Uint8Array, target: Uint8Array): WasmMapComparison {
    return new WasmMapComparison(this.#open().compare(
      ownedPortableBytes(base), ownedPortableBytes(target),
    ));
  }
  compareToHead(base: Uint8Array): WasmMapComparison {
    return new WasmMapComparison(this.#open().compareToHead(ownedPortableBytes(base)));
  }
  subscribe(): WasmMapSubscription { return new WasmMapSubscription(this.#open().subscribe()); }
  subscribeFrom(lastSeen?: Uint8Array): WasmMapSubscription {
    return new WasmMapSubscription(this.#open().subscribeFrom(
      lastSeen == null ? undefined : ownedPortableBytes(lastSeen),
    ));
  }
  prepareMerge(base: Uint8Array, candidate: Uint8Array): WasmMapMerge {
    return new WasmMapMerge(this.#open().prepareMerge(
      ownedPortableBytes(base), ownedPortableBytes(candidate),
    ));
  }
  backup(signal?: AbortSignal): Promise<Uint8Array> {
    const native = this.#open();
    return portablePromise(signal, () => native.backup());
  }
  restoreBackup(bytes: Uint8Array, signal?: AbortSignal): Promise<WasmMapVersion> {
    const native = this.#open(); bytes = ownedPortableBytes(bytes);
    return portablePromise(signal, () => wasmMapVersion(native.restoreBackup(bytes)));
  }
  keepLast(count: number, signal?: AbortSignal): Promise<WasmVersionPrune> {
    if (!Number.isSafeInteger(count) || count < 0 || count > 0xffff_ffff) {
      return Promise.reject(new RangeError("keepLast count must be a non-negative uint32"));
    }
    const native = this.#open();
    return portablePromise(signal, () => native.keepLast(count));
  }
  pruneVersions(keepLatest: bigint): WasmVersionPrune {
    return this.#open().pruneVersions(keepLatest);
  }
  keepForAt(nowMillis: bigint, maxAgeMillis: bigint): WasmVersionPrune {
    return this.#open().keepForAt(nowMillis, maxAgeMillis);
  }
  keepFor(maxAgeMillis: bigint): WasmVersionPrune {
    return this.#open().keepFor(maxAgeMillis);
  }
  keepVersions(ids: readonly Uint8Array[]): WasmVersionPrune {
    return this.#open().keepVersions(ids.map(ownedPortableBytes));
  }
  retentionPolicy(): WasmNamedRootRetention {
    return wasmNamedRootRetention(this.#open().retentionPolicy());
  }
  verifyCatalog(): WasmCatalogVerification {
    return wasmCatalogVerification(this.#open().verifyCatalog());
  }
  planGc(): WasmGcPlan {
    return wasmGcPlan(this.#open().planGc());
  }
  sweepGc(): WasmGcSweep {
    const value = this.#open().sweepGc();
    return {
      plan: wasmGcPlan(value.plan),
      deletedNodes: BigInt(value.deletedNodes),
      deletedBytes: BigInt(value.deletedBytes),
    };
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; this.#memory = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmVersionedTransaction implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  #open(): any { if (this.#native == null) throw new Error("WASM versioned transaction is completed"); return this.#native; }
  head(mapId: Uint8Array, signal?: AbortSignal): Promise<WasmMapVersion | undefined> {
    const native = this.#open(); mapId = ownedPortableBytes(mapId);
    return portablePromise(signal, () => { const value = native.head(mapId); return value == null ? undefined : wasmMapVersion(value); });
  }
  get(mapId: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open(); mapId = ownedPortableBytes(mapId); key = ownedPortableBytes(key);
    return portablePromise(signal, () => native.get(mapId, key) ?? undefined);
  }
  apply(mapId: Uint8Array, mutations: readonly WasmMapMutation[], signal?: AbortSignal): Promise<WasmMapVersion> {
    const native = this.#open(); mapId = ownedPortableBytes(mapId); const owned = ownedWasmMutations(mutations);
    return portablePromise(signal, () => wasmMapVersion(native.apply(mapId, owned)));
  }
  applyIf(mapId: Uint8Array, expected: Uint8Array | undefined, mutations: readonly WasmMapMutation[], signal?: AbortSignal): Promise<WasmMapUpdate> {
    const native = this.#open(); mapId = ownedPortableBytes(mapId);
    const ownedExpected = expected == null ? undefined : ownedPortableBytes(expected);
    const owned = ownedWasmMutations(mutations);
    return portablePromise(signal, () => wasmMapUpdate(native.applyIf(mapId, ownedExpected, owned)));
  }
  put(mapId: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<WasmMapVersion> {
    const native = this.#open(); mapId = ownedPortableBytes(mapId); key = ownedPortableBytes(key); value = ownedPortableBytes(value);
    return portablePromise(signal, () => wasmMapVersion(native.put(mapId, key, value)));
  }
  delete(mapId: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<WasmMapVersion> {
    const native = this.#open(); mapId = ownedPortableBytes(mapId); key = ownedPortableBytes(key);
    return portablePromise(signal, () => wasmMapVersion(native.delete(mapId, key)));
  }
  commit(signal?: AbortSignal): Promise<WasmVersionedTransactionCommit> {
    const native = this.#open(); this.#native = undefined;
    return portablePromise(signal, () => {
      const value = native.commit();
      return {
        applied: value.applied,
        versions: value.versions.map(wasmMapVersion),
        conflictMapId: value.conflictMapId ?? undefined,
        conflictCurrent: value.conflictCurrent == null ? undefined : wasmMapVersion(value.conflictCurrent),
      };
    });
  }
  rollback(signal?: AbortSignal): Promise<void> {
    const native = this.#open(); this.#native = undefined;
    return portablePromise(signal, () => native.rollback());
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmMapComparison implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  #open(): any { if (this.#native == null) throw new Error("WASM map comparison is closed"); return this.#native; }
  base(): WasmMapVersion { return wasmMapVersion(this.#open().base()); }
  target(): WasmMapVersion { return wasmMapVersion(this.#open().target()); }
  diff(): WasmMapDiff[] { return this.#open().diff(); }
  diffPage(cursor?: WasmRangeCursorRecord, end?: Uint8Array, limit = 256): WasmDiffPage {
    return this.#open().diffPage(
      cursor, end == null ? undefined : ownedPortableBytes(end), limit,
    );
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmMapSubscription implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  #open(): any { if (this.#native == null) throw new Error("WASM map subscription is closed"); return this.#native; }
  lastSeen(): Uint8Array | undefined { return this.#open().lastSeen() ?? undefined; }
  poll(): WasmMapChangeEvent | undefined {
    const event = this.#open().poll();
    return event == null ? undefined : {
      previous: event.previous ?? undefined,
      current: wasmMapVersion(event.current),
      diffs: event.diffs,
    };
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmMapMerge implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  #open(): any { if (this.#native == null) throw new Error("WASM map merge is closed"); return this.#native; }
  base(): WasmMapVersion { return wasmMapVersion(this.#open().base()); }
  head(): WasmMapVersion { return wasmMapVersion(this.#open().head()); }
  candidate(): WasmMapVersion { return wasmMapVersion(this.#open().candidate()); }
  merge(resolver?: string): any { return this.#open().merge(resolver); }
  conflictPage(cursor?: WasmRangeCursorRecord, limit = 256): WasmConflictPageRecord {
    return this.#open().conflictPage(cursor, limit);
  }
  publish(resolver?: string): WasmMapUpdate {
    return wasmMapUpdate(this.#open().publish(resolver));
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmMapSnapshot implements Disposable {
  #native?: any;
  #memory?: WebAssembly.Memory;
  constructor(native: any, memory?: WebAssembly.Memory) { this.#native = native; this.#memory = memory; }
  #open(): any { if (this.#native == null) throw new Error("WASM map snapshot is closed"); return this.#native; }
  id(): Uint8Array { return this.#open().id(); }
  version(): WasmMapVersion { return wasmMapVersion(this.#open().version()); }
  get(key: Uint8Array): Uint8Array | undefined { return this.#open().get(ownedPortableBytes(key)) ?? undefined; }
  getMany(keys: readonly Uint8Array[]): Array<Uint8Array | undefined> {
    return this.#open().getMany(keys.map(ownedPortableBytes)).map(
      (value: Uint8Array | null) => value ?? undefined,
    );
  }
  containsKey(key: Uint8Array): boolean { return this.#open().containsKey(ownedPortableBytes(key)); }
  firstEntry(): WasmEntryRecord | undefined { return this.#open().firstEntry() ?? undefined; }
  lastEntry(): WasmEntryRecord | undefined { return this.#open().lastEntry() ?? undefined; }
  lowerBound(key: Uint8Array): WasmEntryRecord | undefined {
    return this.#open().lowerBound(ownedPortableBytes(key)) ?? undefined;
  }
  upperBound(key: Uint8Array): WasmEntryRecord | undefined {
    return this.#open().upperBound(ownedPortableBytes(key)) ?? undefined;
  }
  range(start: Uint8Array = new Uint8Array(), end?: Uint8Array): WasmEntryRecord[] {
    return this.#open().range(
      ownedPortableBytes(start), end == null ? undefined : ownedPortableBytes(end),
    );
  }
  prefix(prefix: Uint8Array): WasmEntryRecord[] {
    return this.#open().prefix(ownedPortableBytes(prefix));
  }
  rangePage(
    cursor?: WasmRangeCursorRecord,
    end?: Uint8Array,
    limit = 256,
  ): WasmRangePageRecord {
    return this.#open().rangePage(
      cursor, end == null ? undefined : ownedPortableBytes(end), limit,
    );
  }
  prefixPage(
    prefix: Uint8Array,
    cursor?: WasmRangeCursorRecord,
    limit = 256,
  ): WasmRangePageRecord {
    return this.#open().prefixPage(ownedPortableBytes(prefix), cursor, limit);
  }
  reversePage(
    cursor?: WasmReverseCursorRecord,
    start: Uint8Array = new Uint8Array(),
    limit = 256,
  ): WasmReversePageRecord {
    return this.#open().reversePage(cursor, ownedPortableBytes(start), limit);
  }
  prefixReversePage(
    prefix: Uint8Array,
    cursor?: WasmReverseCursorRecord,
    limit = 256,
  ): WasmReversePageRecord {
    return this.#open().prefixReversePage(ownedPortableBytes(prefix), cursor, limit);
  }
  proveKey(key: Uint8Array): WasmKeyProof { return new WasmKeyProof(this.#open().proveKey(ownedPortableBytes(key))); }
  proveKeys(keys: readonly Uint8Array[]): WasmMultiKeyProof {
    return new WasmMultiKeyProof(this.#open().proveKeys(keys.map(ownedPortableBytes)));
  }
  proveRange(start: Uint8Array = new Uint8Array(), end?: Uint8Array): WasmRangeProof {
    return new WasmRangeProof(this.#open().proveRange(
      ownedPortableBytes(start), end == null ? undefined : ownedPortableBytes(end),
    ));
  }
  provePrefix(prefix: Uint8Array): WasmRangeProof {
    return new WasmRangeProof(this.#open().provePrefix(ownedPortableBytes(prefix)));
  }
  proveRangePage(
    cursor?: WasmRangeCursorRecord,
    end?: Uint8Array,
    limit = 256,
  ): WasmProvedRangePage {
    return new WasmProvedRangePage(this.#open().proveRangePage(
      cursor, end == null ? undefined : ownedPortableBytes(end), limit,
    ));
  }
  stats(): { itemCount: bigint; byteCount: bigint } {
    const value = this.#open().stats(); return { itemCount: BigInt(value.itemCount), byteCount: BigInt(value.byteCount) };
  }
  exportSummary(): { itemCount: bigint; byteCount: bigint } {
    const value = this.#open().export(); return { itemCount: BigInt(value.itemCount), byteCount: BigInt(value.byteCount) };
  }
  read(): WasmReadSession { return new WasmReadSession(this.#open().read(), this.#memory); }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmReadSession implements Disposable {
  #native?: any;
  #memory?: WebAssembly.Memory;
  #scanActive = false;
  constructor(native: any, memory?: WebAssembly.Memory) { this.#native = native; this.#memory = memory; }
  get(key: Uint8Array): Uint8Array | undefined {
    if (this.#native == null) throw new Error("WASM read session is closed");
    if (this.#scanActive) throw new Error("WASM read session cannot be re-entered during a zero-copy scan callback");
    return this.#native.get(ownedPortableBytes(key)) ?? undefined;
  }
  scanRangeView(
    start: Uint8Array,
    end: Uint8Array | undefined,
    visit: (entry: WasmEntryRecord) => boolean,
  ): { visited: bigint; stopped: boolean } {
    if (this.#native == null) throw new Error("WASM read session is closed");
    if (this.#scanActive) throw new Error("WASM read session cannot be re-entered during a zero-copy scan callback");
    if (typeof visit !== "function") throw new TypeError("scan visitor must be a function");
    this.#scanActive = true;
    try {
      const outcome = this.#native.scanRangeView(
        ownedPortableBytes(start), end == null ? undefined : ownedPortableBytes(end),
        (entry: WasmEntryRecord) => {
          const scope = { alive: true };
          try {
            return visit({
              key: scopedPortableBytes(entry.key, scope, this.#memory),
              value: scopedPortableBytes(entry.value, scope, this.#memory),
            });
          } finally {
            scope.alive = false;
          }
        },
      );
      return { visited: BigInt(outcome.visited), stopped: outcome.stopped };
    } finally {
      this.#scanActive = false;
    }
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmKeyProof implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  verify(): { valid: boolean; exists: boolean; value?: Uint8Array } {
    if (this.#native == null) throw new Error("WASM key proof is closed");
    return this.#native.verify();
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmMultiKeyProof implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  verify(): WasmMultiKeyProofVerificationRecord {
    if (this.#native == null) throw new Error("WASM multi-key proof is closed");
    return this.#native.verify();
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmRangeProof implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  verify(): WasmRangeProofVerificationRecord {
    if (this.#native == null) throw new Error("WASM range proof is closed");
    return this.#native.verify();
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmProvedRangePage implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  page(): WasmRangePageRecord {
    if (this.#native == null) throw new Error("WASM proved range page is closed");
    return this.#native.page();
  }
  verify(): WasmRangePageProofVerificationRecord {
    if (this.#native == null) throw new Error("WASM proved range page is closed");
    return this.#native.verify();
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmIndexRegistry implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  nativeHandle(): any {
    if (this.#native == null) throw new Error("WASM index registry is closed");
    return this.#native;
  }
  register(registration: PortableIndexRegistration): void {
    this.nativeHandle().register(
      ownedPortableBytes(registration.name), registration.generation,
      registration.extractorId, registration.projection,
      (key: Uint8Array, value: Uint8Array) => registration.extract(key, value).map((entry) => ({
        term: ownedPortableBytes(entry.term),
        projection: entry.projection == null ? undefined : ownedPortableBytes(entry.projection),
      })),
    );
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

function portableIndexedVersion(value: any): PortableIndexedVersion {
  return {
    sourceVersion: value.sourceVersion,
    catalogVersion: value.catalogVersion,
    indexCount: BigInt(value.indexCount),
  };
}

function portableIndexedUpdate(value: any): PortableIndexedUpdate {
  return {
    kind: value.kind,
    previousSourceVersion: value.previousSourceVersion,
    current: value.current == null ? undefined : portableIndexedVersion(value.current),
  };
}

function portableIndexVerification(value: any): PortableIndexVerification {
  return {
    name: value.name, sourceVersion: value.sourceVersion,
    expectedIndexVersion: value.expectedIndexVersion, actualIndexVersion: value.actualIndexVersion,
    expectedEntries: BigInt(value.expectedEntries), actualEntries: BigInt(value.actualEntries),
    semanticDifferences: BigInt(value.semanticDifferences), valid: value.valid, canonical: value.canonical,
  };
}

function ownPortableIndexedMutations(mutations: readonly PortableIndexedMutation[]) {
  return mutations.map((mutation) => ({
    kind: mutation.kind,
    key: ownedPortableBytes(mutation.key),
    value: mutation.kind === "upsert" ? ownedPortableBytes(mutation.value) : undefined,
  }));
}

export class WasmIndexedMap implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  #open(): any {
    if (this.#native == null) throw new Error("WASM indexed map is closed");
    return this.#native;
  }
  id(): Uint8Array { return this.#open().id(); }
  get(key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open(); key = ownedPortableBytes(key);
    return portablePromise(signal, () => native.get(key) ?? undefined);
  }
  put(key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<PortableIndexedVersion> {
    const native = this.#open(); key = ownedPortableBytes(key); value = ownedPortableBytes(value);
    return portablePromise(signal, () => portableIndexedVersion(native.put(key, value)));
  }
  apply(mutations: readonly PortableIndexedMutation[], signal?: AbortSignal): Promise<PortableIndexedVersion> {
    const native = this.#open(); const owned = ownPortableIndexedMutations(mutations);
    return portablePromise(signal, () => portableIndexedVersion(native.apply(owned)));
  }
  applyIf(expectedSource: Uint8Array | undefined, mutations: readonly PortableIndexedMutation[], signal?: AbortSignal): Promise<PortableIndexedUpdate> {
    const native = this.#open();
    expectedSource = expectedSource == null ? undefined : ownedPortableBytes(expectedSource);
    const owned = ownPortableIndexedMutations(mutations);
    return portablePromise(signal, () => portableIndexedUpdate(native.applyIf(expectedSource, owned)));
  }
  delete(key: Uint8Array, signal?: AbortSignal): Promise<PortableIndexedVersion> {
    const native = this.#open(); key = ownedPortableBytes(key);
    return portablePromise(signal, () => portableIndexedVersion(native.delete(key)));
  }
  ensureIndex(name: Uint8Array, signal?: AbortSignal): Promise<PortableIndexBuildResult> {
    const native = this.#open(); name = ownedPortableBytes(name);
    return portablePromise(signal, () => {
      const value = native.ensureIndex(name);
      return {
        sourceVersion: value.sourceVersion, indexVersion: value.indexVersion,
        catalogVersion: value.catalogVersion, generation: BigInt(value.generation),
        entries: BigInt(value.entries), attempts: BigInt(value.attempts), activated: value.activated,
      };
    });
  }
  snapshot(signal?: AbortSignal): Promise<WasmIndexedSnapshot> {
    const native = this.#open();
    return portablePromise(signal, () => new WasmIndexedSnapshot(native.snapshot()));
  }
  snapshotAt(sourceVersion: Uint8Array, signal?: AbortSignal): Promise<WasmIndexedSnapshot> {
    const native = this.#open(); sourceVersion = ownedPortableBytes(sourceVersion);
    return portablePromise(signal, () => new WasmIndexedSnapshot(native.snapshotAt(sourceVersion)));
  }
  snapshotById(id: PortableIndexedSnapshotId, signal?: AbortSignal): Promise<WasmIndexedSnapshot> {
    const native = this.#open();
    const source = ownedPortableBytes(id.sourceVersion);
    const catalog = ownedPortableBytes(id.catalogVersion);
    return portablePromise(signal, () => new WasmIndexedSnapshot(native.snapshotById(source, catalog)));
  }
  health(): PortableIndexedMapHealth {
    const value = this.#open().health();
    return {
      sourceMapId: value.sourceMapId, sourceVersion: value.sourceVersion,
      catalogVersion: value.catalogVersion, supportsTransactions: value.supportsTransactions,
      activeIndexes: value.activeIndexes.map((index: any) => ({
        name: index.name, generation: BigInt(index.generation), fingerprint: index.fingerprint,
        projection: index.projection, indexMapId: index.indexMapId, indexVersion: index.indexVersion,
      })),
    };
  }
  metrics(): PortableIndexedMapMetrics {
    const value = this.#open().metrics();
    return Object.fromEntries(Object.entries(value).map(([key, count]) => [key, BigInt(count as string)])) as unknown as PortableIndexedMapMetrics;
  }
  verifyIndex(name: Uint8Array, sourceVersion: Uint8Array): PortableIndexVerification {
    return portableIndexVerification(this.#open().verifyIndex(ownedPortableBytes(name), ownedPortableBytes(sourceVersion)));
  }
  verifyAll(sourceVersion: Uint8Array): PortableIndexVerification[] {
    return this.#open().verifyAll(ownedPortableBytes(sourceVersion)).map(portableIndexVerification);
  }
  repairIndex(name: Uint8Array, sourceVersion: Uint8Array): PortableIndexVerification {
    return portableIndexVerification(this.#open().repairIndex(ownedPortableBytes(name), ownedPortableBytes(sourceVersion)));
  }
  deactivateIndex(name: Uint8Array, signal?: AbortSignal): Promise<PortableIndexedVersion> {
    const native = this.#open(); name = ownedPortableBytes(name);
    return portablePromise(signal, () => portableIndexedVersion(native.deactivateIndex(name)));
  }
  exportCurrent(): Uint8Array { return this.#open().exportCurrent(); }
  importCurrent(bundle: Uint8Array, expectedSource?: Uint8Array, signal?: AbortSignal): Promise<PortableIndexedVersion> {
    const native = this.#open(); bundle = ownedPortableBytes(bundle);
    expectedSource = expectedSource == null ? undefined : ownedPortableBytes(expectedSource);
    return portablePromise(signal, () => portableIndexedVersion(native.importCurrent(bundle, expectedSource)));
  }
  keepLast(count: bigint): PortableIndexedRetention {
    const value = this.#open().keepLast(count);
    return {
      retainedSourceVersions: value.retainedSourceVersions,
      removedSourceVersions: value.removedSourceVersions,
      retainedIndexVersions: value.retainedIndexVersions,
      removedIndexVersions: value.removedIndexVersions,
      removedCatalogVersions: value.removedCatalogVersions,
      removedCheckpointRecords: BigInt(value.removedCheckpointRecords),
      removedNamedRoots: value.removedNamedRoots,
    };
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmIndexedSnapshot implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  #open(): any {
    if (this.#native == null) throw new Error("WASM indexed snapshot is closed");
    return this.#native;
  }
  id(): PortableIndexedSnapshotId { return this.#open().id(); }
  index(name: Uint8Array): WasmSecondaryIndex {
    return new WasmSecondaryIndex(this.#open().index(ownedPortableBytes(name)));
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmSecondaryIndex implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  #open(): any {
    if (this.#native == null) throw new Error("WASM secondary index is closed");
    return this.#native;
  }
  name(): Uint8Array { return this.#open().name(); }
  exact(term: Uint8Array, signal?: AbortSignal): Promise<PortableIndexMatch[]> {
    const native = this.#open(); term = ownedPortableBytes(term);
    return portablePromise(signal, () => native.exact(term));
  }
  prefix(prefix: Uint8Array, signal?: AbortSignal): Promise<PortableIndexMatch[]> {
    const native = this.#open(); prefix = ownedPortableBytes(prefix);
    return portablePromise(signal, () => native.prefix(prefix));
  }
  range(start: Uint8Array, end?: Uint8Array, signal?: AbortSignal): Promise<PortableIndexMatch[]> {
    const native = this.#open(); start = ownedPortableBytes(start);
    end = end == null ? undefined : ownedPortableBytes(end);
    return portablePromise(signal, () => native.range(start, end));
  }
  records(term: Uint8Array, signal?: AbortSignal): Promise<PortableIndexedSource[]> {
    const native = this.#open(); term = ownedPortableBytes(term);
    return portablePromise(signal, () => native.records(term));
  }
  exactPage(term: Uint8Array, cursor: Uint8Array | undefined, limit: bigint, signal?: AbortSignal): Promise<PortableIndexPage> {
    const native = this.#open(); term = ownedPortableBytes(term);
    cursor = cursor == null ? undefined : ownedPortableBytes(cursor);
    return portablePromise(signal, () => native.exactPage(term, cursor, limit));
  }
  exactReversePage(term: Uint8Array, cursor: Uint8Array | undefined, limit: bigint, signal?: AbortSignal): Promise<PortableIndexPage> {
    const native = this.#open(); term = ownedPortableBytes(term);
    cursor = cursor == null ? undefined : ownedPortableBytes(cursor);
    return portablePromise(signal, () => native.exactReversePage(term, cursor, limit));
  }
  prefixPage(prefix: Uint8Array, cursor: Uint8Array | undefined, limit: bigint, signal?: AbortSignal): Promise<PortableIndexPage> {
    const native = this.#open(); prefix = ownedPortableBytes(prefix);
    cursor = cursor == null ? undefined : ownedPortableBytes(cursor);
    return portablePromise(signal, () => native.prefixPage(prefix, cursor, limit));
  }
  prefixReversePage(prefix: Uint8Array, cursor: Uint8Array | undefined, limit: bigint, signal?: AbortSignal): Promise<PortableIndexPage> {
    const native = this.#open(); prefix = ownedPortableBytes(prefix);
    cursor = cursor == null ? undefined : ownedPortableBytes(cursor);
    return portablePromise(signal, () => native.prefixReversePage(prefix, cursor, limit));
  }
  rangePage(start: Uint8Array, end: Uint8Array | undefined, cursor: Uint8Array | undefined, limit: bigint, signal?: AbortSignal): Promise<PortableIndexPage> {
    const native = this.#open(); start = ownedPortableBytes(start);
    end = end == null ? undefined : ownedPortableBytes(end);
    cursor = cursor == null ? undefined : ownedPortableBytes(cursor);
    return portablePromise(signal, () => native.rangePage(start, end, cursor, limit));
  }
  rangeReversePage(start: Uint8Array, end: Uint8Array | undefined, cursor: Uint8Array | undefined, limit: bigint, signal?: AbortSignal): Promise<PortableIndexPage> {
    const native = this.#open(); start = ownedPortableBytes(start);
    end = end == null ? undefined : ownedPortableBytes(end);
    cursor = cursor == null ? undefined : ownedPortableBytes(cursor);
    return portablePromise(signal, () => native.rangeReversePage(start, end, cursor, limit));
  }
  async *#pages(
    next: (cursor: Uint8Array | undefined, limit: bigint) => Promise<PortableIndexPage>,
    options: PortableIndexPageOptions,
  ): AsyncIterable<PortableIndexMatch[]> {
    const pageSize = options.pageSize ?? 256;
    if (!Number.isSafeInteger(pageSize) || pageSize <= 0) {
      throw new RangeError("WASM index pageSize must be a positive safe integer");
    }
    let cursor: Uint8Array | undefined;
    do {
      const page = await next(cursor, BigInt(pageSize));
      yield page.matches;
      cursor = page.nextCursor;
    } while (cursor != null);
  }
  exactPages(term: Uint8Array, options: PortableIndexPageOptions = {}): AsyncIterable<PortableIndexMatch[]> {
    term = ownedPortableBytes(term);
    return this.#pages((cursor, limit) => this.exactPage(term, cursor, limit, options.signal), options);
  }
  exactReversePages(term: Uint8Array, options: PortableIndexPageOptions = {}): AsyncIterable<PortableIndexMatch[]> {
    term = ownedPortableBytes(term);
    return this.#pages((cursor, limit) => this.exactReversePage(term, cursor, limit, options.signal), options);
  }
  prefixPages(prefix: Uint8Array, options: PortableIndexPageOptions = {}): AsyncIterable<PortableIndexMatch[]> {
    prefix = ownedPortableBytes(prefix);
    return this.#pages((cursor, limit) => this.prefixPage(prefix, cursor, limit, options.signal), options);
  }
  prefixReversePages(prefix: Uint8Array, options: PortableIndexPageOptions = {}): AsyncIterable<PortableIndexMatch[]> {
    prefix = ownedPortableBytes(prefix);
    return this.#pages((cursor, limit) => this.prefixReversePage(prefix, cursor, limit, options.signal), options);
  }
  rangePages(start: Uint8Array, end?: Uint8Array, options: PortableIndexPageOptions = {}): AsyncIterable<PortableIndexMatch[]> {
    start = ownedPortableBytes(start); end = end == null ? undefined : ownedPortableBytes(end);
    return this.#pages((cursor, limit) => this.rangePage(start, end, cursor, limit, options.signal), options);
  }
  rangeReversePages(start: Uint8Array, end?: Uint8Array, options: PortableIndexPageOptions = {}): AsyncIterable<PortableIndexMatch[]> {
    start = ownedPortableBytes(start); end = end == null ? undefined : ownedPortableBytes(end);
    return this.#pages((cursor, limit) => this.rangeReversePage(start, end, cursor, limit, options.signal), options);
  }
  async exactView(
    term: Uint8Array,
    visit: (row: PortableIndexMatch) => boolean | void,
    signal?: AbortSignal,
  ): Promise<void> {
    for await (const rows of this.exactPages(term, { signal })) {
      for (const row of rows) {
        const scope = { alive: true };
        try {
          if (visit({
            term: scopedPortableBytes(row.term, scope),
            primaryKey: scopedPortableBytes(row.primaryKey, scope),
            projection: row.projection == null
              ? undefined : scopedPortableBytes(row.projection, scope),
          }) === false) return;
        } finally {
          scope.alive = false;
        }
      }
    }
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

function wasmCompositeRebuildOutcome(result: any): CompositeBuildOrRebuildOutcome {
  return {
    kind: result.kind,
    composite: result.composite == null ? undefined : new WasmCompositeAccelerator(result.composite),
    hnsw: result.hnsw == null ? undefined : new WasmHnswIndex(result.hnsw),
    pq: result.pq == null ? undefined : new WasmProductQuantizer(result.pq),
    reasons: result.reasons,
    compositeStats: result.compositeStats,
    hnswStats: result.hnswStats,
    pqStats: result.pqStats,
  };
}

export class WasmProximityMap implements Disposable {
  #native?: any;
  #memory?: WebAssembly.Memory;
  constructor(native: any, memory?: WebAssembly.Memory) {
    this.#native = native;
    this.#memory = memory;
  }
  nativeHandle(): any {
    if (this.#native == null) throw new Error("WASM proximity map is closed");
    return this.#native;
  }
  read(): WasmProximityReadSession {
    return new WasmProximityReadSession(this.nativeHandle().read(), this.#memory);
  }
  search(request: PortableSearchRequest): Promise<PortableSearchResult> {
    return this.read().search(request);
  }
  get(key: Uint8Array): { vector: Float32Array; value: Uint8Array } | undefined {
    return this.nativeHandle().get(ownedPortableBytes(key)) ?? undefined;
  }
  contains(key: Uint8Array): boolean { return this.nativeHandle().contains(ownedPortableBytes(key)); }
  scanRecords(visitor: (record: PortableProximityRecord) => boolean): bigint {
    return BigInt(this.nativeHandle().scanRecords((record: PortableProximityRecord) => visitor({
      key: ownedPortableBytes(record.key),
      vector: new Float32Array(record.vector),
      value: ownedPortableBytes(record.value ?? new Uint8Array()),
    })));
  }
  withSearchView<R>(
    query: Float32Array,
    k: number,
    visitor: (neighbors: PortableNeighborView[]) => R,
  ): R {
    const session = this.read();
    try { return session.withSearchView(query, k, visitor); }
    finally { session.close(); }
  }
  count(): bigint { return BigInt(this.nativeHandle().count()); }
  config(): PortableProximityConfig { return this.nativeHandle().config(); }
  buildHnsw(options: HnswBuildOptions = {}): Promise<HnswBuildResult> {
    const native = this.nativeHandle();
    const config = ownHnswConfig(options.config);
    const limits = ownHnswBuildLimits(options.limits);
    return portablePromise(options.signal, () => {
      const result = native.buildHnsw(config, limits);
      return {
        index: new WasmHnswIndex(result.index),
        stats: {
          records: BigInt(result.stats.records),
          distanceEvaluations: BigInt(result.stats.distanceEvaluations),
          directedEdges: BigInt(result.stats.directedEdges),
          maximumLevel: result.stats.maximumLevel,
          ownedBytes: BigInt(result.stats.ownedBytes),
          encodedGraphBytes: BigInt(result.stats.encodedGraphBytes),
        },
      };
    });
  }
  loadHnsw(manifest: Uint8Array): WasmHnswIndex {
    return new WasmHnswIndex(this.nativeHandle().loadHnsw(ownedPortableBytes(manifest)));
  }
  buildPq(options: ProductQuantizationBuildOptions = {}): Promise<ProductQuantizationBuildResult> {
    const native = this.nativeHandle();
    const config = ownPqConfig(options.config);
    const workerThreads = requireUnsignedBigInt(
      options.workerThreads ?? 1n, "workerThreads",
    ).toString();
    const limits = ownPqBuildLimits(options.limits);
    return portablePromise(options.signal, () => {
      const result = native.buildPq(config, workerThreads, limits);
      return {
        index: new WasmProductQuantizer(result.index),
        stats: {
          trainingDistanceEvaluations: BigInt(result.stats.trainingDistanceEvaluations),
          encodingDistanceEvaluations: BigInt(result.stats.encodingDistanceEvaluations),
          encodedVectors: BigInt(result.stats.encodedVectors),
          trainingVectors: BigInt(result.stats.trainingVectors),
          trainingBytes: BigInt(result.stats.trainingBytes),
          encodedOutputBytes: BigInt(result.stats.encodedOutputBytes),
        },
      };
    });
  }
  loadPq(manifest: Uint8Array): WasmProductQuantizer {
    return new WasmProductQuantizer(this.nativeHandle().loadPq(ownedPortableBytes(manifest)));
  }
  buildCompositeHnsw(baseMap: WasmProximityMap, base: WasmHnswIndex, options: CompositeBuildOptions = {}): Promise<CompositeBuildOutcome> {
    const native = this.nativeHandle();
    return portablePromise(options.signal, () => {
      const result = native.buildCompositeHnsw(baseMap.nativeHandle(), base.nativeHandle(), ownCompositeConfig(options.config), ownCompositeBuildLimits(options.limits));
      return { accelerator: result.accelerator == null ? undefined : new WasmCompositeAccelerator(result.accelerator), reasons: result.reasons, stats: result.stats };
    });
  }
  buildCompositePq(baseMap: WasmProximityMap, base: WasmProductQuantizer, options: CompositeBuildOptions = {}): Promise<CompositeBuildOutcome> {
    const native = this.nativeHandle();
    return portablePromise(options.signal, () => {
      const result = native.buildCompositePq(baseMap.nativeHandle(), base.nativeHandle(), ownCompositeConfig(options.config), ownCompositeBuildLimits(options.limits));
      return { accelerator: result.accelerator == null ? undefined : new WasmCompositeAccelerator(result.accelerator), reasons: result.reasons, stats: result.stats };
    });
  }
  buildOrRebuildCompositeHnsw(baseMap: WasmProximityMap, base: WasmHnswIndex, options: CompositeBuildOrRebuildOptions = {}): Promise<CompositeBuildOrRebuildOutcome> {
    const native = this.nativeHandle();
    return portablePromise(options.signal, () => wasmCompositeRebuildOutcome(native.buildOrRebuildCompositeHnsw(
      baseMap.nativeHandle(), base.nativeHandle(), ownCompositeConfig(options.config), ownCompositeBuildLimits(options.limits), ownCompositeRebuildOptions(options.rebuild),
    )));
  }
  buildOrRebuildCompositePq(baseMap: WasmProximityMap, base: WasmProductQuantizer, options: CompositeBuildOrRebuildOptions = {}): Promise<CompositeBuildOrRebuildOutcome> {
    const native = this.nativeHandle();
    return portablePromise(options.signal, () => wasmCompositeRebuildOutcome(native.buildOrRebuildCompositePq(
      baseMap.nativeHandle(), base.nativeHandle(), ownCompositeConfig(options.config), ownCompositeBuildLimits(options.limits), ownCompositeRebuildOptions(options.rebuild),
    )));
  }
  loadComposite(manifest: Uint8Array): WasmCompositeAccelerator {
    return new WasmCompositeAccelerator(this.nativeHandle().loadComposite(ownedPortableBytes(manifest)));
  }
  buildAcceleratorCatalog(options: { hnsw?: WasmHnswIndex; pq?: WasmProductQuantizer; composite?: WasmCompositeAccelerator } = {}): WasmAcceleratorCatalog {
    return new WasmAcceleratorCatalog(this.nativeHandle().buildAcceleratorCatalog(
      options.hnsw?.manifest(), options.pq?.manifest(), options.composite?.manifest(),
    ));
  }
  loadAcceleratorCatalog(manifest: Uint8Array): WasmAcceleratorCatalog {
    return new WasmAcceleratorCatalog(this.nativeHandle().loadAcceleratorCatalog(ownedPortableBytes(manifest)));
  }
  mutate(mutations: PortableProximityMutation[]): {
    map: WasmProximityMap;
    stats: {
      directoryEntriesScanned: bigint; directoryNodesRead: bigint;
      directoryNodesRebuilt: bigint; directoryNodesWritten: bigint;
      directoryNodesReused: bigint; directoryLevelsRebuilt: bigint;
      directoryRightEdgeRebuilt: boolean;
      recordsRebuilt: bigint; nodesRead: bigint; nodesWritten: bigint; nodesReused: bigint;
      distanceEvaluations: bigint; fullProximityRebuild: boolean;
    };
  } {
    const result = this.nativeHandle().mutate(mutations.map((mutation) => ({
      key: ownedPortableBytes(mutation.key),
      vector: mutation.vector == null ? undefined : new Float32Array(mutation.vector),
      value: mutation.value == null ? undefined : ownedPortableBytes(mutation.value),
    })));
    return { map: new WasmProximityMap(result.map, this.#memory), stats: result.stats };
  }
  rebuild(mutations: PortableProximityMutation[]): WasmProximityMap {
    return new WasmProximityMap(this.nativeHandle().rebuild(mutations.map((mutation) => ({
      key: ownedPortableBytes(mutation.key),
      vector: mutation.vector == null ? undefined : new Float32Array(mutation.vector),
      value: mutation.value == null ? undefined : ownedPortableBytes(mutation.value),
    }))), this.#memory);
  }
  descriptor(): Uint8Array { return this.nativeHandle().descriptor(); }
  verify(): PortableProximityVerification { return this.nativeHandle().verify(); }
  proveMembership(key: Uint8Array): WasmProximityProof {
    return new WasmProximityProof(this.nativeHandle().proveMembership(ownedPortableBytes(key)));
  }
  proveSearch(request: PortableSearchRequest): WasmProximitySearchProof {
    return new WasmProximitySearchProof(
      this.nativeHandle().proveSearch(ownPortableSearchRequest(request)),
    );
  }
  proveStructure(): WasmProximityStructuralProof {
    return new WasmProximityStructuralProof(this.nativeHandle().proveStructure());
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmHnswIndex implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  #open(): any {
    if (this.#native == null) throw new Error("WASM HNSW index is closed");
    return this.#native;
  }
  nativeHandle(): any { return this.#open(); }
  manifest(): Uint8Array { return ownedPortableBytes(this.#open().manifest()); }
  sourceDescriptor(): Uint8Array { return ownedPortableBytes(this.#open().sourceDescriptor()); }
  config(): HnswConfig {
    const value = this.#open().config();
    return {
      maxConnections: value.maxConnections,
      efConstruction: value.efConstruction,
      efSearch: value.efSearch,
      levelBits: value.levelBits,
      overfetchMultiplier: value.overfetchMultiplier,
      seed: BigInt(value.seed),
      routingVectorEncoding: value.routingVectorEncoding,
    };
  }
  isCanonical(): boolean { return this.#open().isCanonical(); }
  search(map: WasmProximityMap, request: PortableSearchRequest): Promise<PortableSearchResult> {
    const native = this.#open();
    const nativeMap = map.nativeHandle();
    const owned = ownPortableSearchRequest(request);
    return portablePromise(request.signal, () => native.search(nativeMap, owned));
  }
  proveSearch(map: WasmProximityMap, request: PortableSearchRequest): WasmProximitySearchProof {
    return new WasmProximitySearchProof(
      this.#open().proveSearch(map.nativeHandle(), ownPortableSearchRequest(request)),
    );
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmProductQuantizer implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  #open(): any {
    if (this.#native == null) throw new Error("WASM product quantizer is closed");
    return this.#native;
  }
  nativeHandle(): any { return this.#open(); }
  manifest(): Uint8Array { return ownedPortableBytes(this.#open().manifest()); }
  sourceDescriptor(): Uint8Array { return ownedPortableBytes(this.#open().sourceDescriptor()); }
  config(): ProductQuantizationConfig {
    const value = this.#open().config();
    return {
      subquantizers: value.subquantizers,
      centroidsPerSubquantizer: value.centroidsPerSubquantizer,
      trainingIterations: value.trainingIterations,
      rerankMultiplier: value.rerankMultiplier,
      seed: BigInt(value.seed),
      maxTrainingVectors: BigInt(value.maxTrainingVectors),
    };
  }
  quality(): ProductQuantizationQuality {
    const value = this.#open().quality();
    return {
      meanSquaredError: value.meanSquaredError,
      maximumSquaredError: value.maximumSquaredError,
    };
  }
  search(map: WasmProximityMap, request: PortableSearchRequest): Promise<PortableSearchResult> {
    const native = this.#open();
    const nativeMap = map.nativeHandle();
    const owned = ownPortableSearchRequest(request);
    return portablePromise(request.signal, () => native.search(nativeMap, owned));
  }
  proveSearch(map: WasmProximityMap, request: PortableSearchRequest): WasmProximitySearchProof {
    return new WasmProximitySearchProof(
      this.#open().proveSearch(map.nativeHandle(), ownPortableSearchRequest(request)),
    );
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmCompositeAccelerator implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  nativeHandle(): any {
    if (this.#native == null) throw new Error("WASM composite accelerator is closed");
    return this.#native;
  }
  manifest(): Uint8Array { return ownedPortableBytes(this.nativeHandle().manifest()); }
  currentSourceDescriptor(): Uint8Array { return ownedPortableBytes(this.nativeHandle().currentSourceDescriptor()); }
  baseSourceDescriptor(): Uint8Array { return ownedPortableBytes(this.nativeHandle().baseSourceDescriptor()); }
  baseKind(): "hnsw" | "product_quantized" { return this.nativeHandle().baseKind(); }
  deltaCount(): bigint { return BigInt(this.nativeHandle().deltaCount()); }
  shadowCount(): bigint { return BigInt(this.nativeHandle().shadowCount()); }
  config(): CompositeAcceleratorConfig { return this.nativeHandle().config(); }
  buildStats(): CompositeBuildStats { return this.nativeHandle().buildStats(); }
  search(map: WasmProximityMap, request: PortableSearchRequest): Promise<PortableSearchResult> {
    const native = this.nativeHandle(); const nativeMap = map.nativeHandle(); const owned = ownPortableSearchRequest(request);
    return portablePromise(request.signal, () => native.search(nativeMap, owned));
  }
  proveSearch(map: WasmProximityMap, request: PortableSearchRequest): WasmProximitySearchProof {
    return new WasmProximitySearchProof(this.nativeHandle().proveSearch(map.nativeHandle(), ownPortableSearchRequest(request)));
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmAcceleratorCatalog implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  #open(): any { if (this.#native == null) throw new Error("WASM accelerator catalog is closed"); return this.#native; }
  manifest(): Uint8Array { return ownedPortableBytes(this.#open().manifest()); }
  sourceDescriptor(): Uint8Array { return ownedPortableBytes(this.#open().sourceDescriptor()); }
  entries(): AcceleratorCatalogEntry[] { return this.#open().entries().map((entry: any) => ({
    kind: entry.kind, configurationFingerprint: ownedPortableBytes(entry.configurationFingerprint), manifest: ownedPortableBytes(entry.manifest),
  })); }
  search(map: WasmProximityMap, request: PortableSearchRequest): Promise<PortableSearchResult> {
    const native = this.#open(); const nativeMap = map.nativeHandle(); const owned = ownPortableSearchRequest(request);
    return portablePromise(request.signal, () => native.search(nativeMap, owned));
  }
  proveSearch(map: WasmProximityMap, request: PortableSearchRequest): WasmProximitySearchProof {
    return new WasmProximitySearchProof(this.#open().proveSearch(map.nativeHandle(), ownPortableSearchRequest(request)));
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmProximityStructuralProof implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  verify(expected?: Uint8Array): {
    descriptor: Uint8Array; objectCount: bigint; summary: PortableProximityVerification;
  } {
    if (this.#native == null) throw new Error("WASM proximity structural proof is closed");
    return this.#native.verify(expected == null ? undefined : ownedPortableBytes(expected));
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmProximityProof implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  verify(expected?: Uint8Array): { value?: Uint8Array } {
    if (this.#native == null) throw new Error("WASM proximity proof is closed");
    return { value: this.#native.verify(expected == null ? undefined : ownedPortableBytes(expected)) ?? undefined };
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmProximitySearchProof implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  verify(expected?: Uint8Array): {
    result: PortableSearchResult;
    claim: string;
    terminalLowerBound?: number;
    replayedEvents: bigint;
  } {
    if (this.#native == null) throw new Error("WASM proximity search proof is closed");
    const value = this.#native.verify(
      expected == null ? undefined : ownedPortableBytes(expected),
    );
    return {
      result: value.result,
      claim: value.claim,
      terminalLowerBound: value.terminalLowerBound,
      replayedEvents: BigInt(value.replayedEvents),
    };
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmProximityReadSession implements Disposable {
  #native?: any;
  #memory?: WebAssembly.Memory;
  constructor(native: any, memory?: WebAssembly.Memory) {
    this.#native = native;
    this.#memory = memory;
  }
  get(key: Uint8Array): { vector: Float32Array; value: Uint8Array } | undefined {
    if (this.#native == null) throw new Error("WASM proximity session is closed");
    return this.#native.get(ownedPortableBytes(key)) ?? undefined;
  }
  contains(key: Uint8Array): boolean {
    if (this.#native == null) throw new Error("WASM proximity session is closed");
    return this.#native.contains(ownedPortableBytes(key));
  }
  scanRecords(visitor: (record: PortableProximityRecord) => boolean): bigint {
    if (this.#native == null) throw new Error("WASM proximity session is closed");
    return BigInt(this.#native.scanRecords((record: PortableProximityRecord) => visitor({
      key: ownedPortableBytes(record.key),
      vector: new Float32Array(record.vector),
      value: ownedPortableBytes(record.value ?? new Uint8Array()),
    })));
  }
  withSearchView<R>(
    query: Float32Array,
    k: number,
    visitor: (neighbors: PortableNeighborView[]) => R,
  ): R {
    if (this.#native == null) throw new Error("WASM proximity session is closed");
    if (!Number.isSafeInteger(k) || k <= 0 || k > 0xffff_ffff) {
      throw new RangeError("k must be a positive unsigned 32-bit integer");
    }
    const scope = { alive: true };
    try {
      return this.#native.withSearchView(
        new Float32Array(query),
        k,
        (rows: Array<{ key: Uint8Array; value: Uint8Array; distance: number; rank: number }>) =>
          visitor(rows.map((row) => ({
            key: scopedPortableBytes(row.key, scope, this.#memory),
            value: scopedPortableBytes(row.value, scope, this.#memory),
            distance: row.distance,
            rank: row.rank,
          }))),
      ) as R;
    } finally {
      scope.alive = false;
    }
  }
  search(request: PortableSearchRequest): Promise<PortableSearchResult> {
    if (this.#native == null) return Promise.reject(new Error("WASM proximity session is closed"));
    const native = this.#native;
    const owned = ownPortableSearchRequest(request);
    return portablePromise(request.signal, () => native.search(owned));
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; this.#memory = undefined; }
  [Symbol.dispose](): void { this.close(); }
}
