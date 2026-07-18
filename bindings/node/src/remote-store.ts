export const STORE_PROTOCOL_MAJOR = 1 as const;

export type StoreErrorCode =
  | "invalid_argument"
  | "invalid_data"
  | "unavailable"
  | "permission_denied"
  | "resource_exhausted"
  | "unsupported"
  | "cancelled"
  | "internal";

export class StoreError extends Error {
  readonly code: StoreErrorCode;
  readonly retryable: boolean;
  readonly providerCode?: string;
  override readonly cause?: unknown;

  constructor(
    code: StoreErrorCode,
    message: string,
    options: {
      retryable?: boolean;
      providerCode?: string;
      cause?: unknown;
    } = {},
  ) {
    super(message);
    this.name = "StoreError";
    this.code = code;
    this.retryable = options.retryable ?? false;
    this.providerCode = options.providerCode;
    this.cause = options.cause;
  }
}

export interface StoreCapabilities {
  readonly nativeBatchReads: boolean;
  readonly atomicBatchWrites: boolean;
  readonly nodeScan: boolean;
  readonly hints: boolean;
  readonly atomicNodesAndHint: boolean;
  readonly rootScan: boolean;
  readonly rootCompareAndSwap: boolean;
  readonly transactions: boolean;
  readonly readParallelism: number;
}

export interface StoreLimits {
  readonly maxBatchReadItems?: number;
  readonly maxBatchWriteItems?: number;
  readonly maxTransactionOperations?: number;
  readonly maxNodeBytes?: number;
}

export interface StoreDescriptor {
  readonly protocolMajor: number;
  readonly adapterName: string;
  readonly provider: string;
  readonly schemaVersion: number;
  readonly capabilities: StoreCapabilities;
  readonly limits: StoreLimits;
}

export interface OptionalBytes {
  readonly present: boolean;
  readonly value: Uint8Array;
}

export type NodeMutation =
  | { readonly kind: "upsert"; readonly cid: Uint8Array; readonly node: Uint8Array }
  | { readonly kind: "delete"; readonly cid: Uint8Array };

export interface NodeEntry {
  readonly cid: Uint8Array;
  readonly node: Uint8Array;
}

export interface NamedStoreRoot {
  readonly name: Uint8Array;
  readonly manifest: Uint8Array;
}

export interface RootCasResult {
  readonly applied: boolean;
  readonly current: OptionalBytes;
}

export interface RootCondition {
  readonly name: Uint8Array;
  readonly expected: OptionalBytes;
}

export type RootWrite =
  | { readonly kind: "put"; readonly name: Uint8Array; readonly manifest: Uint8Array }
  | { readonly kind: "delete"; readonly name: Uint8Array };

export interface StoreTransactionConflict {
  readonly name: Uint8Array;
  readonly expected: OptionalBytes;
  readonly current: OptionalBytes;
}

export type StoreTransactionResult =
  | { readonly applied: true; readonly conflict?: undefined }
  | { readonly applied: false; readonly conflict: StoreTransactionConflict };

export interface RemoteStore {
  descriptor(signal?: AbortSignal): Promise<StoreDescriptor>;
  getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes>;
  putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void>;
  deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void>;
  batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void>;
  batchGetNodesOrdered(cids: readonly Uint8Array[], signal?: AbortSignal): Promise<OptionalBytes[]>;
  listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]>;
  getHint(namespace: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes>;
  putHint(
    namespace: Uint8Array,
    key: Uint8Array,
    value: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void>;
  batchPutNodesWithHint(
    nodes: readonly NodeEntry[],
    namespace: Uint8Array,
    key: Uint8Array,
    value: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void>;
  getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes>;
  putRootManifest(name: Uint8Array, manifest: Uint8Array, signal?: AbortSignal): Promise<void>;
  deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void>;
  compareAndSwapRootManifest(
    name: Uint8Array,
    expected: OptionalBytes,
    replacement: OptionalBytes,
    signal?: AbortSignal,
  ): Promise<RootCasResult>;
  listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]>;
  commitTransaction(
    nodes: readonly NodeMutation[],
    conditions: readonly RootCondition[],
    roots: readonly RootWrite[],
    signal?: AbortSignal,
  ): Promise<StoreTransactionResult>;
}

export function validateStoreDescriptor(descriptor: StoreDescriptor): StoreDescriptor {
  const invalid = (message: string): never => {
    throw new StoreError("invalid_argument", `invalid remote store descriptor: ${message}`);
  };

  if (descriptor.protocolMajor !== STORE_PROTOCOL_MAJOR) {
    invalid(`protocol major must be ${STORE_PROTOCOL_MAJOR}, got ${descriptor.protocolMajor}`);
  }
  if (descriptor.adapterName.trim().length === 0) {
    invalid("adapter name must not be empty");
  }
  if (descriptor.provider.trim().length === 0) {
    invalid("provider must not be empty");
  }
  if (!Number.isSafeInteger(descriptor.schemaVersion) || descriptor.schemaVersion < 1) {
    invalid("schema version must be at least 1");
  }
  if (
    !Number.isSafeInteger(descriptor.capabilities.readParallelism) ||
    descriptor.capabilities.readParallelism < 1
  ) {
    invalid("read parallelism must be at least 1");
  }
  if (descriptor.capabilities.atomicNodesAndHint && !descriptor.capabilities.hints) {
    invalid("atomic nodes and hint requires hints support");
  }

  for (const [name, limit] of Object.entries(descriptor.limits)) {
    if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 1)) {
      invalid(`${camelCaseToWords(name)} must be at least 1 when present`);
    }
  }

  return Object.freeze({
    ...descriptor,
    capabilities: Object.freeze({ ...descriptor.capabilities }),
    limits: Object.freeze({ ...descriptor.limits }),
  });
}

export function missingBytes(): OptionalBytes {
  return { present: false, value: new Uint8Array() };
}

export function presentBytes(value: Uint8Array): OptionalBytes {
  return { present: true, value: ownBytes(value) };
}

export function normalizeOptionalBytes(value: OptionalBytes): OptionalBytes {
  if (!value.present && value.value.byteLength !== 0) {
    throw new StoreError("invalid_data", "absent optional bytes must have an empty value");
  }
  return value.present ? presentBytes(value.value) : missingBytes();
}

export function upsertNode(cid: Uint8Array, node: Uint8Array): NodeMutation {
  return { kind: "upsert", cid: ownBytes(cid), node: ownBytes(node) };
}

export function deleteNode(cid: Uint8Array): NodeMutation {
  return { kind: "delete", cid: ownBytes(cid) };
}

export function putRoot(name: Uint8Array, manifest: Uint8Array): RootWrite {
  return { kind: "put", name: ownBytes(name), manifest: ownBytes(manifest) };
}

export function deleteRoot(name: Uint8Array): RootWrite {
  return { kind: "delete", name: ownBytes(name) };
}

export function ownBytes(value: Uint8Array): Uint8Array {
  return Uint8Array.from(value);
}

export function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw new StoreError("cancelled", "store operation was cancelled", {
      cause: signal.reason,
    });
  }
}

function camelCaseToWords(value: string): string {
  return value.replaceAll(/([a-z])([A-Z])/g, "$1 $2").toLowerCase();
}
