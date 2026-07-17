import {
  loadNative,
  type NativeConfigRecord,
  type NativeEntryRecord,
  type NativeMutationRecord,
  type NativeNamedRootUpdateRecord,
  type NativeRemoteOptionalBytesRecord,
  type NativeRemoteProllyEngine,
  type NativeRemoteProllyTransaction,
  type NativeRemoteStoreDescriptorRecord,
  type NativeRemoteStoreErrorRecord,
  type NativeRemoteStoreRequest,
  type NativeRemoteStoreResponse,
  type NativeTreeRecord,
} from "./native.ts";
import { abortError, ownedBytes } from "./packed.ts";
import {
  StoreError,
  normalizeOptionalBytes,
  ownBytes,
  validateStoreDescriptor,
  type NodeMutation,
  type OptionalBytes,
  type RemoteStore,
  type RootCondition,
  type RootWrite,
  type StoreDescriptor,
  type StoreErrorCode,
} from "./remote-store.ts";

class RemoteOperationDispatcher {
  readonly #store: RemoteStore;
  readonly #controllers = new Map<string, AbortController>();
  #nextRequestId = 0n;

  constructor(store: RemoteStore) {
    this.#store = store;
  }

  readonly dispatch = (request: NativeRemoteStoreRequest): Promise<NativeRemoteStoreResponse> => {
    const controller = this.#controllers.get(request.requestId);
    if (controller === undefined) {
      return Promise.resolve({
        error: {
          code: "cancelled",
          message: "remote store operation is no longer active",
          retryable: false,
        },
      });
    }
    return dispatchRemoteStore(this.#store, request, controller.signal);
  };

  run<T>(signal: AbortSignal | undefined, operation: (requestId: string) => Promise<T>): Promise<T> {
    if (signal?.aborted) return Promise.reject(abortError());

    const requestId = (++this.#nextRequestId).toString();
    const controller = new AbortController();
    this.#controllers.set(requestId, controller);

    let rejectAborted: (error: Error) => void = () => {};
    const aborted = new Promise<never>((_, reject) => {
      rejectAborted = reject;
    });
    const onAbort = () => {
      controller.abort(signal?.reason);
      rejectAborted(abortError());
    };
    signal?.addEventListener("abort", onAbort, { once: true });

    let native: Promise<T>;
    try {
      native = operation(requestId);
    } catch (error) {
      signal?.removeEventListener("abort", onAbort);
      this.#controllers.delete(requestId);
      throw error;
    }

    const tracked = native.finally(() => {
      signal?.removeEventListener("abort", onAbort);
      this.#controllers.delete(requestId);
    });
    return signal === undefined ? tracked : Promise.race([tracked, aborted]);
  }
}

export class RemoteAsyncProllyEngine implements AsyncDisposable {
  #native?: NativeRemoteProllyEngine;
  readonly #dispatcher: RemoteOperationDispatcher;

  private constructor(native: NativeRemoteProllyEngine, dispatcher: RemoteOperationDispatcher) {
    this.#native = native;
    this.#dispatcher = dispatcher;
  }

  static async open(
    store: RemoteStore,
    config?: NativeConfigRecord,
    signal?: AbortSignal,
  ): Promise<RemoteAsyncProllyEngine> {
    const module = await loadNative();
    const dispatcher = new RemoteOperationDispatcher(store);
    const native = await dispatcher.run(signal, (requestId) =>
      module.NativeRemoteProllyEngine.open(dispatcher.dispatch, config, requestId),
    );
    return new RemoteAsyncProllyEngine(native, dispatcher);
  }

  #open(): NativeRemoteProllyEngine {
    if (this.#native === undefined) throw new Error("remote prolly engine is closed");
    return this.#native;
  }

  create(): NativeTreeRecord {
    return this.#open().create();
  }

  get(
    tree: NativeTreeRecord,
    key: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array | null> {
    const native = this.#open();
    const ownedKey = ownedBytes(key);
    return this.#dispatcher.run(signal, (requestId) => native.get(tree, ownedKey, requestId));
  }

  getMany(
    tree: NativeTreeRecord,
    keys: readonly Uint8Array[],
    signal?: AbortSignal,
  ): Promise<Array<Uint8Array | null>> {
    const native = this.#open();
    const ownedKeys = keys.map(ownedBytes);
    return this.#dispatcher.run(signal, (requestId) =>
      native.getMany(tree, ownedKeys, requestId),
    );
  }

  put(
    tree: NativeTreeRecord,
    key: Uint8Array,
    value: Uint8Array,
    signal?: AbortSignal,
  ): Promise<NativeTreeRecord> {
    const native = this.#open();
    const ownedKey = ownedBytes(key);
    const ownedValue = ownedBytes(value);
    return this.#dispatcher.run(signal, (requestId) =>
      native.put(tree, ownedKey, ownedValue, requestId),
    );
  }

  delete(tree: NativeTreeRecord, key: Uint8Array, signal?: AbortSignal): Promise<NativeTreeRecord> {
    const native = this.#open();
    const ownedKey = ownedBytes(key);
    return this.#dispatcher.run(signal, (requestId) => native.delete(tree, ownedKey, requestId));
  }

  batch(
    tree: NativeTreeRecord,
    mutations: readonly NativeMutationRecord[],
    signal?: AbortSignal,
  ): Promise<NativeTreeRecord> {
    const native = this.#open();
    const ownedMutations = mutations.map((mutation) => ({
        kind: mutation.kind,
        key: ownedBytes(mutation.key),
        value: mutation.value == null ? mutation.value : ownedBytes(mutation.value),
      }));
    return this.#dispatcher.run(signal, (requestId) =>
      native.batch(tree, ownedMutations, requestId),
    );
  }

  range(
    tree: NativeTreeRecord,
    start: Uint8Array,
    end?: Uint8Array | null,
    signal?: AbortSignal,
  ): Promise<NativeEntryRecord[]> {
    const native = this.#open();
    const ownedStart = ownedBytes(start);
    const ownedEnd = end == null ? end : ownedBytes(end);
    return this.#dispatcher.run(signal, (requestId) =>
      native.range(tree, ownedStart, ownedEnd, requestId),
    );
  }

  loadNamedRoot(name: Uint8Array, signal?: AbortSignal): Promise<NativeTreeRecord | null> {
    const native = this.#open();
    const ownedName = ownedBytes(name);
    return this.#dispatcher.run(signal, (requestId) => native.loadNamedRoot(ownedName, requestId));
  }

  publishNamedRoot(
    name: Uint8Array,
    tree: NativeTreeRecord,
    signal?: AbortSignal,
  ): Promise<void> {
    const native = this.#open();
    const ownedName = ownedBytes(name);
    return this.#dispatcher.run(signal, (requestId) =>
      native.publishNamedRoot(ownedName, tree, requestId),
    );
  }

  compareAndSwapNamedRoot(
    name: Uint8Array,
    expected?: NativeTreeRecord | null,
    replacement?: NativeTreeRecord | null,
    signal?: AbortSignal,
  ): Promise<NativeNamedRootUpdateRecord> {
    const native = this.#open();
    const ownedName = ownedBytes(name);
    return this.#dispatcher.run(signal, (requestId) =>
      native.compareAndSwapNamedRoot(ownedName, expected, replacement, requestId),
    );
  }

  async beginTransaction(signal?: AbortSignal): Promise<RemoteAsyncProllyTransaction> {
    const native = this.#open();
    const transaction = await this.#dispatcher.run(signal, (requestId) =>
      native.beginTransaction(requestId),
    );
    return new RemoteAsyncProllyTransaction(transaction, this.#dispatcher);
  }

  close(): void {
    this.#native?.close();
    this.#native = undefined;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    this.close();
  }
}

export class RemoteAsyncProllyTransaction implements AsyncDisposable {
  #native?: NativeRemoteProllyTransaction;
  readonly #dispatcher: RemoteOperationDispatcher;

  constructor(native: NativeRemoteProllyTransaction, dispatcher: RemoteOperationDispatcher) {
    this.#native = native;
    this.#dispatcher = dispatcher;
  }

  #open(): NativeRemoteProllyTransaction {
    if (this.#native === undefined) throw new Error("remote prolly transaction is closed");
    return this.#native;
  }

  create(signal?: AbortSignal): Promise<NativeTreeRecord> {
    const native = this.#open();
    return this.#dispatcher.run(signal, (requestId) => native.create(requestId));
  }

  get(
    tree: NativeTreeRecord,
    key: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array | null> {
    const native = this.#open();
    const ownedKey = ownedBytes(key);
    return this.#dispatcher.run(signal, (requestId) => native.get(tree, ownedKey, requestId));
  }

  put(
    tree: NativeTreeRecord,
    key: Uint8Array,
    value: Uint8Array,
    signal?: AbortSignal,
  ): Promise<NativeTreeRecord> {
    const native = this.#open();
    const ownedKey = ownedBytes(key);
    const ownedValue = ownedBytes(value);
    return this.#dispatcher.run(signal, (requestId) =>
      native.put(tree, ownedKey, ownedValue, requestId),
    );
  }

  publishNamedRoot(
    name: Uint8Array,
    tree: NativeTreeRecord,
    signal?: AbortSignal,
  ): Promise<void> {
    const native = this.#open();
    const ownedName = ownedBytes(name);
    return this.#dispatcher.run(signal, (requestId) =>
      native.publishNamedRoot(ownedName, tree, requestId),
    );
  }

  commit(signal?: AbortSignal) {
    const native = this.#open();
    return this.#dispatcher.run(signal, (requestId) => native.commit(requestId));
  }

  rollback(signal?: AbortSignal): Promise<void> {
    const native = this.#open();
    return this.#dispatcher.run(signal, (requestId) => native.rollback(requestId));
  }

  close(): void {
    this.#native?.close();
    this.#native = undefined;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    this.close();
  }
}

export async function dispatchRemoteStore(
  store: RemoteStore,
  request: NativeRemoteStoreRequest,
  signal?: AbortSignal,
): Promise<NativeRemoteStoreResponse> {
  try {
    const values = request.bytes ?? [];
    switch (request.operation) {
      case "descriptor":
        return {
          descriptor: descriptorToNative(validateStoreDescriptor(await store.descriptor(signal))),
        };
      case "getNode":
        return {
          optionalBytes: optionalToNative(await store.getNode(required(values, 0), signal)),
        };
      case "putNode":
        await store.putNode(required(values, 0), required(values, 1), signal);
        return {};
      case "deleteNode":
        await store.deleteNode(required(values, 0), signal);
        return {};
      case "batchNodes":
        await store.batchNodes((request.mutations ?? []).map(mutationFromNative), signal);
        return {};
      case "batchGetNodesOrdered":
        return {
          optionalValues: (await store.batchGetNodesOrdered(values, signal)).map(optionalToNative),
        };
      case "listNodeCids":
        return { bytesValues: (await store.listNodeCids(signal)).map(ownBytes) };
      case "getHint":
        return {
          optionalBytes: optionalToNative(
            await store.getHint(required(values, 0), required(values, 1), signal),
          ),
        };
      case "putHint":
        await store.putHint(required(values, 0), required(values, 1), required(values, 2), signal);
        return {};
      case "batchPutNodesWithHint":
        await store.batchPutNodesWithHint(
          (request.entries ?? []).map((entry) => ({
            cid: ownBytes(entry.cid),
            node: ownBytes(entry.node),
          })),
          required(values, 0),
          required(values, 1),
          required(values, 2),
          signal,
        );
        return {};
      case "getRootManifest":
        return {
          optionalBytes: optionalToNative(await store.getRootManifest(required(values, 0), signal)),
        };
      case "putRootManifest":
        await store.putRootManifest(required(values, 0), required(values, 1), signal);
        return {};
      case "deleteRootManifest":
        await store.deleteRootManifest(required(values, 0), signal);
        return {};
      case "compareAndSwapRootManifest": {
        const optional = request.optionalBytes ?? [];
        const result = await store.compareAndSwapRootManifest(
          required(values, 0),
          optionalFromNative(required(optional, 0)),
          optionalFromNative(required(optional, 1)),
          signal,
        );
        return {
          rootCas: {
            applied: result.applied,
            current: optionalToNative(result.current),
          },
        };
      }
      case "listRootManifests":
        return {
          namedRoots: (await store.listRootManifests(signal)).map((root) => ({
            name: ownBytes(root.name),
            manifest: ownBytes(root.manifest),
          })),
        };
      case "commitTransaction": {
        const result = await store.commitTransaction(
          (request.mutations ?? []).map(mutationFromNative),
          (request.conditions ?? []).map(conditionFromNative),
          (request.roots ?? []).map(rootWriteFromNative),
          signal,
        );
        return {
          transaction: result.applied
            ? { applied: true }
            : {
                applied: false,
                conflict: {
                  name: ownBytes(result.conflict.name),
                  expected: optionalToNative(result.conflict.expected),
                  current: optionalToNative(result.conflict.current),
                },
              },
        };
      }
      default:
        throw new StoreError("invalid_argument", `unknown remote store operation ${request.operation}`);
    }
  } catch (error) {
    return { error: errorToNative(error) };
  }
}

function descriptorToNative(value: StoreDescriptor): NativeRemoteStoreDescriptorRecord {
  return {
    protocolMajor: value.protocolMajor,
    adapterName: value.adapterName,
    provider: value.provider,
    schemaVersion: value.schemaVersion,
    capabilities: { ...value.capabilities },
    limits: {
      ...value.limits,
      maxNodeBytes:
        value.limits.maxNodeBytes === undefined ? undefined : String(value.limits.maxNodeBytes),
    },
  };
}

function optionalToNative(value: OptionalBytes): NativeRemoteOptionalBytesRecord {
  const normalized = normalizeOptionalBytes(value);
  return { present: normalized.present, value: ownBytes(normalized.value) };
}

function optionalFromNative(value: NativeRemoteOptionalBytesRecord): OptionalBytes {
  return normalizeOptionalBytes({ present: value.present, value: ownBytes(value.value) });
}

function mutationFromNative(value: {
  cid: Uint8Array;
  value: NativeRemoteOptionalBytesRecord;
}): NodeMutation {
  const optional = optionalFromNative(value.value);
  return optional.present
    ? { kind: "upsert", cid: ownBytes(value.cid), node: optional.value }
    : { kind: "delete", cid: ownBytes(value.cid) };
}

function conditionFromNative(value: {
  name: Uint8Array;
  expected: NativeRemoteOptionalBytesRecord;
}): RootCondition {
  return { name: ownBytes(value.name), expected: optionalFromNative(value.expected) };
}

function rootWriteFromNative(value: {
  name: Uint8Array;
  replacement: NativeRemoteOptionalBytesRecord;
}): RootWrite {
  const replacement = optionalFromNative(value.replacement);
  return replacement.present
    ? { kind: "put", name: ownBytes(value.name), manifest: replacement.value }
    : { kind: "delete", name: ownBytes(value.name) };
}

function errorToNative(error: unknown): NativeRemoteStoreErrorRecord {
  if (error instanceof StoreError) {
    return {
      code: error.code,
      message: error.message,
      retryable: error.retryable,
      providerCode: error.providerCode,
    };
  }
  return {
    code: "internal" satisfies StoreErrorCode,
    message: "remote store callback failed",
    retryable: false,
  };
}

function required<T>(values: readonly T[], index: number): T {
  const value = values[index];
  if (value === undefined) {
    throw new StoreError("invalid_data", `remote store request omitted argument ${index}`);
  }
  return value;
}
