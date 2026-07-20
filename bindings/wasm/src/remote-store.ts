/** Browser-safe version-2 asynchronous store protocol. */
export const STORE_PROTOCOL_MAJOR = 2 as const;
export const GENERAL = 0 as const;
export const POINT_UPSERT = 1 as const;
export const POINT_DELETE = 2 as const;
export const BATCH_MUTATION = 3 as const;
export const TREE_BUILD = 4 as const;
export const MERGE = 5 as const;
export const RANGE_DELETE = 6 as const;
export const REPLICATION = 7 as const;
export const MAINTENANCE = 8 as const;
export type StoreErrorCode = "invalid_argument" | "invalid_data" | "unavailable" | "permission_denied" | "resource_exhausted" | "unsupported" | "cancelled" | "internal";

export class StoreError extends Error {
  readonly code: StoreErrorCode;
  readonly retryable: boolean;
  readonly providerCode?: string;
  override readonly cause?: unknown;
  constructor(code: StoreErrorCode, message: string, options: { retryable?: boolean; providerCode?: string; cause?: unknown } = {}) {
    super(message); this.name = "StoreError"; this.code = code; this.retryable = options.retryable ?? false;
    this.providerCode = options.providerCode; this.cause = options.cause;
  }
}

export interface StoreCapabilities { readonly nativeBatchReads: boolean; readonly atomicBatchWrites: boolean; readonly nodeScan: boolean; readonly hints: boolean; readonly atomicNodesAndHint: boolean; readonly rootScan: boolean; readonly rootCompareAndSwap: boolean; readonly transactions: boolean; readonly readParallelism: number; }
export interface StoreLimits { readonly maxBatchReadItems?: number; readonly maxBatchWriteItems?: number; readonly maxTransactionOperations?: number; readonly maxNodeBytes?: number; }
export interface StoreDescriptor { readonly protocolMajor: number; readonly adapterName: string; readonly provider: string; readonly schemaVersion: number; readonly capabilities: StoreCapabilities; readonly limits: StoreLimits; }
export interface OptionalBytes { readonly present: boolean; readonly value: Uint8Array; }
export type NodeMutation = { readonly kind: "upsert"; readonly cid: Uint8Array; readonly node: Uint8Array } | { readonly kind: "delete"; readonly cid: Uint8Array };
export interface NodeEntry { readonly cid: Uint8Array; readonly node: Uint8Array; }
export interface NodePublicationHint { readonly namespace: Uint8Array; readonly key: Uint8Array; readonly value: Uint8Array; }
export interface NodePublication { readonly nodes: readonly NodeEntry[]; readonly hint?: NodePublicationHint; readonly origin: number; }
export interface NamedStoreRoot { readonly name: Uint8Array; readonly manifest: Uint8Array; }
export interface RootCasResult { readonly applied: boolean; readonly current: OptionalBytes; }
export interface RootCondition { readonly name: Uint8Array; readonly expected: OptionalBytes; }
export type RootWrite = { readonly kind: "put"; readonly name: Uint8Array; readonly manifest: Uint8Array } | { readonly kind: "delete"; readonly name: Uint8Array };
export interface StoreTransactionConflict { readonly name: Uint8Array; readonly expected: OptionalBytes; readonly current: OptionalBytes; }
export type StoreTransactionResult = { readonly applied: true; readonly conflict?: undefined } | { readonly applied: false; readonly conflict: StoreTransactionConflict };

export interface RemoteStore {
  descriptor(signal?: AbortSignal): Promise<StoreDescriptor>;
  getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes>;
  putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void>;
  deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void>;
  batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void>;
  publishNodes(publication: NodePublication, signal?: AbortSignal): Promise<void>;
  batchGetNodesOrdered(cids: readonly Uint8Array[], signal?: AbortSignal): Promise<OptionalBytes[]>;
  listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]>;
  getHint(namespace: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes>;
  putHint(namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void>;
  batchPutNodesWithHint(nodes: readonly NodeEntry[], namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void>;
  getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes>;
  putRootManifest(name: Uint8Array, manifest: Uint8Array, signal?: AbortSignal): Promise<void>;
  deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void>;
  compareAndSwapRootManifest(name: Uint8Array, expected: OptionalBytes, replacement: OptionalBytes, signal?: AbortSignal): Promise<RootCasResult>;
  listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]>;
  commitTransaction(nodes: readonly NodeMutation[], conditions: readonly RootCondition[], roots: readonly RootWrite[], signal?: AbortSignal): Promise<StoreTransactionResult>;
}

export function validateStoreDescriptor(value: StoreDescriptor): StoreDescriptor {
  if (value.protocolMajor !== STORE_PROTOCOL_MAJOR || !value.adapterName.trim() || !value.provider.trim() || value.schemaVersion < 1 || !Number.isSafeInteger(value.capabilities.readParallelism) || value.capabilities.readParallelism < 1) throw new StoreError("invalid_argument", "invalid browser store descriptor");
  for (const limit of Object.values(value.limits)) if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 1)) throw new StoreError("invalid_argument", "invalid browser store limit");
  return Object.freeze({ ...value, capabilities: Object.freeze({ ...value.capabilities }), limits: Object.freeze({ ...value.limits }) });
}
export function normalizePublicationOriginCode(code: number): number { return Number.isInteger(code) && code >= GENERAL && code <= MAINTENANCE ? code : GENERAL; }
export async function publishNodesWithGeneralPath(store: RemoteStore, publication: NodePublication, signal?: AbortSignal): Promise<void> {
  normalizePublicationOriginCode(publication.origin);
  if (publication.hint) {
    return store.batchPutNodesWithHint(publication.nodes, publication.hint.namespace, publication.hint.key, publication.hint.value, signal);
  }
  return store.batchNodes(publication.nodes.map(({ cid, node }) => ({ kind: "upsert" as const, cid, node })), signal);
}
export function ownBytes(value: Uint8Array): Uint8Array { return Uint8Array.from(value); }
export function missingBytes(): OptionalBytes { return { present: false, value: new Uint8Array() }; }
export function presentBytes(value: Uint8Array): OptionalBytes { return { present: true, value: ownBytes(value) }; }
export function normalizeOptionalBytes(value: OptionalBytes): OptionalBytes { if (!value.present && value.value.byteLength !== 0) throw new StoreError("invalid_data", "absent optional bytes must have an empty value"); return value.present ? presentBytes(value.value) : missingBytes(); }
export function throwIfAborted(signal?: AbortSignal): void { if (signal?.aborted) throw new StoreError("cancelled", "browser store operation was cancelled", { cause: signal.reason }); }
export function equalBytes(left: Uint8Array, right: Uint8Array): boolean { return left.byteLength === right.byteLength && left.every((value, index) => value === right[index]); }
export function optionalEqual(left: OptionalBytes, right: OptionalBytes): boolean { return left.present === right.present && (!left.present || equalBytes(left.value, right.value)); }
export function compareBytes(left: Uint8Array, right: Uint8Array): number { const size = Math.min(left.length, right.length); for (let index = 0; index < size; index++) { const compared = left[index]! - right[index]!; if (compared) return compared; } return left.length - right.length; }
export function hex(value: Uint8Array): string { return [...value].map((byte) => byte.toString(16).padStart(2, "0")).join(""); }
