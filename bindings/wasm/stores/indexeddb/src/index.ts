import {
  STORE_PROTOCOL_MAJOR, StoreError, compareBytes, missingBytes, normalizeOptionalBytes, optionalEqual, ownBytes, presentBytes,
  publishNodesWithGeneralPath, throwIfAborted, validateStoreDescriptor,
  type NamedStoreRoot, type NodeEntry, type NodeMutation, type NodePublication, type OptionalBytes, type RemoteStore,
  type RootCasResult, type RootCondition, type RootWrite, type StoreDescriptor, type StoreTransactionResult,
} from "@trail/prolly-wasm/remote-store";

const DATABASE_VERSION = 1;
const NODES = "nodes";
const HINTS = "hints";
const ROOTS = "roots";

export interface OpenIndexedDbOptions { readonly indexedDB?: IDBFactory; }
export interface IndexedDbStoreOptions { readonly adapterName?: string; readonly readParallelism?: number; }

export function openIndexedDbDatabase(name: string, options: OpenIndexedDbOptions = {}): Promise<IDBDatabase> {
  if (!name.trim()) return Promise.reject(new StoreError("invalid_argument", "IndexedDB database name is required"));
  const factory = options.indexedDB ?? globalThis.indexedDB;
  if (!factory) return Promise.reject(new StoreError("unsupported", "IndexedDB is unavailable in this browser"));
  return new Promise((resolve, reject) => {
    const request = factory.open(name, DATABASE_VERSION);
    request.onupgradeneeded = () => {
      const database = request.result;
      if (!database.objectStoreNames.contains(NODES)) database.createObjectStore(NODES);
      if (!database.objectStoreNames.contains(HINTS)) database.createObjectStore(HINTS);
      if (!database.objectStoreNames.contains(ROOTS)) database.createObjectStore(ROOTS);
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(mapIndexedDbError("open", request.error));
    request.onblocked = () => reject(new StoreError("unavailable", "IndexedDB open is blocked by another connection", { retryable: true, providerCode: "indexeddb:blocked" }));
  });
}

export class IndexedDbStore implements RemoteStore {
  readonly #database: IDBDatabase;
  readonly #storeDescriptor: StoreDescriptor;
  readonly #pending = new Set<Promise<unknown>>();
  #accepting = true;

  constructor(database: IDBDatabase, options: IndexedDbStoreOptions = {}) {
    if (!database) throw new StoreError("invalid_argument", "IndexedDB database is required");
    for (const name of [NODES, HINTS, ROOTS]) if (!database.objectStoreNames.contains(name)) throw new StoreError("invalid_argument", `IndexedDB database is missing ${name} object store`);
    this.#database = database;
    this.#storeDescriptor = validateStoreDescriptor({
      protocolMajor: STORE_PROTOCOL_MAJOR, adapterName: options.adapterName?.trim() || "indexeddb-v1", provider: "indexeddb", schemaVersion: 1,
      capabilities: { nativeBatchReads: true, atomicBatchWrites: true, nodeScan: true, hints: true, atomicNodesAndHint: true, rootScan: true, rootCompareAndSwap: true, transactions: true, readParallelism: options.readParallelism ?? 16 }, limits: {},
    });
  }

  async close(): Promise<void> { this.#accepting = false; await Promise.allSettled([...this.#pending]); }
  async descriptor(signal?: AbortSignal): Promise<StoreDescriptor> { return this.#run("descriptor", signal, async () => this.#storeDescriptor); }
  async getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { return this.#readOptional(NODES, binaryKey(cid), signal); }
  async putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#write(NODES, binaryKey(cid), value, signal); }
  async deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#delete(NODES, binaryKey(cid), signal); }

  async batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void> {
    const owned = operations.map(cloneNodeMutation);
    return this.#runTransaction("batch_nodes", [NODES], "readwrite", signal, async (transaction) => { applyNodes(transaction.objectStore(NODES), owned); });
  }

  async publishNodes(publication: NodePublication, signal?: AbortSignal): Promise<void> { return publishNodesWithGeneralPath(this, publication, signal); }

  async batchGetNodesOrdered(cids: readonly Uint8Array[], signal?: AbortSignal): Promise<OptionalBytes[]> {
    const keys = cids.map(binaryKey);
    return this.#runTransaction("batch_get_nodes_ordered", [NODES], "readonly", signal, async (transaction) => {
      const store = transaction.objectStore(NODES); const values = [];
      for (const key of keys) values.push(optionalFrom(await request(store.get(key))));
      return values;
    });
  }

  async listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]> {
    return this.#runTransaction("list_node_cids", [NODES], "readonly", signal, async (transaction) =>
      (await request(transaction.objectStore(NODES).getAllKeys())).map(bytesFromKey).filter((cid) => cid.byteLength === 32).sort(compareBytes));
  }

  async getHint(namespace: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { return this.#readOptional(HINTS, [binaryKey(namespace), binaryKey(key)], signal); }
  async putHint(namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#write(HINTS, [binaryKey(namespace), binaryKey(key)], value, signal); }

  async batchPutNodesWithHint(nodes: readonly NodeEntry[], namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> {
    const owned = nodes.map(({ cid, node }) => ({ cid: ownBytes(cid), node: ownBytes(node) })); const ns = binaryKey(namespace); const hintKey = binaryKey(key); const hint = binaryValue(value);
    return this.#runTransaction("batch_put_nodes_with_hint", [NODES, HINTS], "readwrite", signal, async (transaction) => {
      const nodeStore = transaction.objectStore(NODES); for (const node of owned) nodeStore.put(binaryValue(node.node), binaryKey(node.cid));
      transaction.objectStore(HINTS).put(hint, [ns, hintKey]);
    });
  }

  async getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { return this.#readOptional(ROOTS, binaryKey(name), signal); }
  async putRootManifest(name: Uint8Array, manifest: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#write(ROOTS, binaryKey(name), manifest, signal); }
  async deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#delete(ROOTS, binaryKey(name), signal); }

  async compareAndSwapRootManifest(name: Uint8Array, expected: OptionalBytes, replacement: OptionalBytes, signal?: AbortSignal): Promise<RootCasResult> {
    const key = binaryKey(name); const wanted = normalizeOptionalBytes(expected); const next = normalizeOptionalBytes(replacement);
    return this.#runTransaction("compare_and_swap_root_manifest", [ROOTS], "readwrite", signal, async (transaction) => {
      const store = transaction.objectStore(ROOTS); const current = optionalFrom(await request(store.get(key)));
      if (!optionalEqual(current, wanted)) return { applied: false, current };
      if (next.present) store.put(binaryValue(next.value), key); else store.delete(key);
      return { applied: true, current: next.present ? presentBytes(next.value) : missingBytes() };
    });
  }

  async listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]> {
    return this.#runTransaction("list_root_manifests", [ROOTS], "readonly", signal, async (transaction) => {
      const store = transaction.objectStore(ROOTS); const keys = await request(store.getAllKeys()); const values = await request(store.getAll());
      return keys.map((key, index) => ({ name: bytesFromKey(key), manifest: bytesFromValue(values[index]) })).sort((left, right) => compareBytes(left.name, right.name));
    });
  }

  async commitTransaction(nodes: readonly NodeMutation[], conditions: readonly RootCondition[], roots: readonly RootWrite[], signal?: AbortSignal): Promise<StoreTransactionResult> {
    const ownedNodes = nodes.map(cloneNodeMutation); const ownedConditions = conditions.map(({ name, expected }) => ({ name: ownBytes(name), expected: normalizeOptionalBytes(expected) })); const ownedRoots = roots.map(cloneRootWrite);
    return this.#runTransaction("commit_transaction", [NODES, ROOTS], "readwrite", signal, async (transaction) => {
      const rootStore = transaction.objectStore(ROOTS);
      for (const condition of ownedConditions) {
        const current = optionalFrom(await request(rootStore.get(binaryKey(condition.name))));
        if (!optionalEqual(current, condition.expected)) return { applied: false, conflict: { name: ownBytes(condition.name), expected: normalizeOptionalBytes(condition.expected), current } };
      }
      applyNodes(transaction.objectStore(NODES), ownedNodes);
      for (const root of ownedRoots) if (root.kind === "put") rootStore.put(binaryValue(root.manifest), binaryKey(root.name)); else rootStore.delete(binaryKey(root.name));
      return { applied: true };
    });
  }

  async #readOptional(store: string, key: IDBValidKey, signal?: AbortSignal): Promise<OptionalBytes> { return this.#runTransaction(`get_${store}`, [store], "readonly", signal, async (transaction) => optionalFrom(await request(transaction.objectStore(store).get(key)))); }
  async #write(store: string, key: IDBValidKey, value: Uint8Array, signal?: AbortSignal): Promise<void> { const owned = binaryValue(value); return this.#runTransaction(`put_${store}`, [store], "readwrite", signal, async (transaction) => { transaction.objectStore(store).put(owned, key); }); }
  async #delete(store: string, key: IDBValidKey, signal?: AbortSignal): Promise<void> { return this.#runTransaction(`delete_${store}`, [store], "readwrite", signal, async (transaction) => { transaction.objectStore(store).delete(key); }); }

  async #runTransaction<T>(operation: string, stores: string[], mode: IDBTransactionMode, signal: AbortSignal | undefined, call: (transaction: IDBTransaction) => Promise<T>): Promise<T> {
    return this.#run(operation, signal, async () => {
      const transaction = this.#database.transaction(stores, mode); const complete = transactionDone(transaction);
      const abort = () => transaction.abort(); signal?.addEventListener("abort", abort, { once: true });
      try { const result = await call(transaction); await complete; throwIfAborted(signal); return result; }
      catch (error) { try { transaction.abort(); } catch {} throw error; }
      finally { signal?.removeEventListener("abort", abort); }
    });
  }

  async #run<T>(operation: string, signal: AbortSignal | undefined, call: () => Promise<T>): Promise<T> {
    throwIfAborted(signal); if (!this.#accepting) throw new StoreError("internal", "IndexedDB store is closed");
    const pending = call().catch((error) => { if (signal?.aborted) throw new StoreError("cancelled", "IndexedDB operation was cancelled", { cause: signal.reason }); throw mapIndexedDbError(operation, error); });
    this.#pending.add(pending); try { return await pending; } finally { this.#pending.delete(pending); }
  }
}

function request<T>(value: IDBRequest<T>): Promise<T> { return new Promise((resolve, reject) => { value.onsuccess = () => resolve(value.result); value.onerror = () => reject(value.error); }); }
function transactionDone(transaction: IDBTransaction): Promise<void> { return new Promise((resolve, reject) => { transaction.oncomplete = () => resolve(); transaction.onerror = () => reject(transaction.error); transaction.onabort = () => reject(transaction.error ?? new DOMException("transaction aborted", "AbortError")); }); }
function binaryKey(value: Uint8Array): ArrayBuffer { const owned = ownBytes(value); return owned.buffer.slice(owned.byteOffset, owned.byteOffset + owned.byteLength) as ArrayBuffer; }
function binaryValue(value: Uint8Array): ArrayBuffer { return binaryKey(value); }
function bytesFromKey(value: IDBValidKey): Uint8Array { if (value instanceof ArrayBuffer) return new Uint8Array(value.slice(0)); if (ArrayBuffer.isView(value)) return ownBytes(new Uint8Array(value.buffer, value.byteOffset, value.byteLength)); throw new StoreError("invalid_data", "IndexedDB key is not binary"); }
function bytesFromValue(value: unknown): Uint8Array { if (value instanceof ArrayBuffer) return new Uint8Array(value.slice(0)); if (ArrayBuffer.isView(value)) return ownBytes(new Uint8Array(value.buffer, value.byteOffset, value.byteLength)); throw new StoreError("invalid_data", "IndexedDB value is not binary"); }
function optionalFrom(value: unknown): OptionalBytes { return value === undefined ? missingBytes() : presentBytes(bytesFromValue(value)); }
function cloneNodeMutation(value: NodeMutation): NodeMutation { return value.kind === "upsert" ? { kind: "upsert", cid: ownBytes(value.cid), node: ownBytes(value.node) } : { kind: "delete", cid: ownBytes(value.cid) }; }
function cloneRootWrite(value: RootWrite): RootWrite { return value.kind === "put" ? { kind: "put", name: ownBytes(value.name), manifest: ownBytes(value.manifest) } : { kind: "delete", name: ownBytes(value.name) }; }
function applyNodes(store: IDBObjectStore, operations: readonly NodeMutation[]): void { for (const operation of operations) if (operation.kind === "upsert") store.put(binaryValue(operation.node), binaryKey(operation.cid)); else store.delete(binaryKey(operation.cid)); }
function mapIndexedDbError(operation: string, error: unknown): StoreError { if (error instanceof StoreError) return error; const name = typeof error === "object" && error && "name" in error ? String(error.name) : undefined; const exhausted = name === "QuotaExceededError"; const retryable = name === "AbortError" || name === "TimeoutError"; return new StoreError(exhausted ? "resource_exhausted" : retryable ? "unavailable" : "internal", "IndexedDB provider operation failed", { retryable, providerCode: name ? `indexeddb:${name}:${operation}` : undefined, cause: error }); }
