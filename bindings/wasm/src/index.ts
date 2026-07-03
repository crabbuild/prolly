export interface WasmEntryRecord {
  key: Uint8Array;
  value: Uint8Array;
}

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
export type RawWasmProllyEngine = import("../pkg/prolly_wasm.js").WasmProllyEngine;

export interface WasmProllyEngineInstance
  extends Omit<RawWasmProllyEngine, "firstEntry" | "lastEntry" | "lowerBound" | "upperBound" | "prefix" | "prefixPage" | "prefixReversePage" | "reversePage"> {
  firstEntry(tree: WasmTree): WasmOptionalEntryRecord;
  lastEntry(tree: WasmTree): WasmOptionalEntryRecord;
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
