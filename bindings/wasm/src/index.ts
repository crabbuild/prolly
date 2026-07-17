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

export interface PortableIndexMatch {
  term: Uint8Array;
  primaryKey: Uint8Array;
  projection?: Uint8Array;
}

export interface PortableIndexedSource extends PortableIndexMatch {
  sourceValue: Uint8Array;
}

export interface PortableProximityRecord {
  key: Uint8Array;
  vector: Float32Array;
  value?: Uint8Array;
}

export interface PortableSearchRequest {
  vector: Float32Array;
  topK: number;
  policy: "exact";
  signal?: AbortSignal;
}

export interface PortableSearchResult {
  neighbors: Array<{ key: Uint8Array; value: Uint8Array; distance: number }>;
  completion: string;
  backend: string;
}

export class WasmViewExpiredError extends Error {
  constructor() {
    super("scoped WASM view has expired");
    this.name = "WasmViewExpiredError";
  }
}

function scopedPortableBytes(value: Uint8Array, scope: { alive: boolean }): Uint8Array {
  return new Proxy(value, {
    get(target, property) {
      if (!scope.alive) throw new WasmViewExpiredError();
      const member = Reflect.get(target, property, target);
      return typeof member === "function" ? member.bind(target) : member;
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
    return new WasmVersionedMap(this.#open().versionedMap(ownedPortableBytes(id)));
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
      new WasmProximityMap(native.buildProximity(dimensions, owned)));
  }

  close(): void {
    this.#native?.free?.();
    this.#native = undefined;
    this.#module = undefined;
  }

  [Symbol.dispose](): void { this.close(); }
}

export class WasmVersionedMap implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  #open(): any {
    if (this.#native == null) throw new Error("WASM versioned map is closed");
    return this.#native;
  }
  initialize(signal?: AbortSignal): Promise<unknown> {
    const native = this.#open();
    return portablePromise(signal, () => native.initialize());
  }
  get(key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open(); key = ownedPortableBytes(key);
    return portablePromise(signal, () => native.get(key) ?? undefined);
  }
  put(key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<unknown> {
    const native = this.#open(); key = ownedPortableBytes(key); value = ownedPortableBytes(value);
    return portablePromise(signal, () => native.put(key, value));
  }
  delete(key: Uint8Array, signal?: AbortSignal): Promise<unknown> {
    const native = this.#open(); key = ownedPortableBytes(key);
    return portablePromise(signal, () => native.delete(key));
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

export class WasmIndexedMap implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  #open(): any {
    if (this.#native == null) throw new Error("WASM indexed map is closed");
    return this.#native;
  }
  get(key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open(); key = ownedPortableBytes(key);
    return portablePromise(signal, () => native.get(key) ?? undefined);
  }
  put(key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<unknown> {
    const native = this.#open(); key = ownedPortableBytes(key); value = ownedPortableBytes(value);
    return portablePromise(signal, () => native.put(key, value));
  }
  delete(key: Uint8Array, signal?: AbortSignal): Promise<unknown> {
    const native = this.#open(); key = ownedPortableBytes(key);
    return portablePromise(signal, () => native.delete(key));
  }
  ensureIndex(name: Uint8Array, signal?: AbortSignal): Promise<unknown> {
    const native = this.#open(); name = ownedPortableBytes(name);
    return portablePromise(signal, () => native.ensureIndex(name));
  }
  snapshot(signal?: AbortSignal): Promise<WasmIndexedSnapshot> {
    const native = this.#open();
    return portablePromise(signal, () => new WasmIndexedSnapshot(native.snapshot()));
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmIndexedSnapshot implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  index(name: Uint8Array): WasmSecondaryIndex {
    if (this.#native == null) throw new Error("WASM indexed snapshot is closed");
    return new WasmSecondaryIndex(this.#native.index(ownedPortableBytes(name)));
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
  exact(term: Uint8Array, signal?: AbortSignal): Promise<PortableIndexMatch[]> {
    const native = this.#open(); term = ownedPortableBytes(term);
    return portablePromise(signal, () => native.exact(term));
  }
  records(term: Uint8Array, signal?: AbortSignal): Promise<PortableIndexedSource[]> {
    const native = this.#open(); term = ownedPortableBytes(term);
    return portablePromise(signal, () => native.records(term));
  }
  async exactView(
    term: Uint8Array,
    visit: (row: PortableIndexMatch) => boolean | void,
    signal?: AbortSignal,
  ): Promise<void> {
    for (const row of await this.exact(term, signal)) {
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
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmProximityMap implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  nativeHandle(): any {
    if (this.#native == null) throw new Error("WASM proximity map is closed");
    return this.#native;
  }
  read(): WasmProximityReadSession {
    return new WasmProximityReadSession(this.nativeHandle().read());
  }
  search(request: PortableSearchRequest): Promise<PortableSearchResult> {
    return this.read().search(request);
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class WasmProximityReadSession implements Disposable {
  #native?: any;
  constructor(native: any) { this.#native = native; }
  search(request: PortableSearchRequest): Promise<PortableSearchResult> {
    if (this.#native == null) return Promise.reject(new Error("WASM proximity session is closed"));
    const native = this.#native;
    const vector = new Float32Array(request.vector);
    return portablePromise(request.signal, () => native.search(vector, request.topK));
  }
  close(): void { this.#native?.free?.(); this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}
