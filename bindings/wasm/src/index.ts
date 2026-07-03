export interface WasmEntryRecord {
  key: Uint8Array;
  value: Uint8Array;
}

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

export interface WasmRangeCursorRecord {
  afterKey?: Uint8Array | null;
}

export interface WasmRangeBoundsRecord {
  start: Uint8Array;
  end?: Uint8Array | null;
}

export interface WasmRangePageRecord {
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

export type ProllyWasmModule = typeof import("../pkg/prolly_wasm.js");

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
