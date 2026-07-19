import {
  STORE_PROTOCOL_MAJOR, StoreError, compareBytes, hex, missingBytes, normalizeOptionalBytes, optionalEqual, ownBytes, presentBytes,
  publishNodesWithGeneralPath, throwIfAborted, validateStoreDescriptor,
  type NamedStoreRoot, type NodeEntry, type NodeMutation, type NodePublication, type OptionalBytes, type RemoteStore,
  type RootCasResult, type RootCondition, type RootWrite, type StoreDescriptor, type StoreTransactionResult,
} from "@trail/prolly-wasm/remote-store";

export interface OpfsWritableFile { write(data: string): Promise<void>; close(): Promise<void>; abort?(): Promise<void>; }
export interface OpfsFileHandle { getFile(): Promise<{ text(): Promise<string> }>; createWritable(): Promise<OpfsWritableFile>; }
export interface OpfsDirectoryHandle { getFileHandle(name: string, options: { create: boolean }): Promise<OpfsFileHandle>; }
export interface OpfsLockManager { request<T>(name: string, options: { mode: "exclusive" }, callback: () => Promise<T>): Promise<T>; }
export interface OpfsStoreOptions { readonly fileName?: string; readonly adapterName?: string; readonly readParallelism?: number; readonly lockManager?: OpfsLockManager; }

type State = { nodes: Map<string, Uint8Array>; hints: Map<string, Uint8Array>; roots: Map<string, Uint8Array> };
const directoryQueues = new WeakMap<object, Promise<void>>();

export class OpfsStore implements RemoteStore {
  readonly #directory: OpfsDirectoryHandle;
  readonly #fileName: string;
  readonly #lockManager?: OpfsLockManager;
  readonly #storeDescriptor: StoreDescriptor;
  #state?: State;
  #tail: Promise<void> = Promise.resolve();
  #accepting = true;

  constructor(directory: OpfsDirectoryHandle, options: OpfsStoreOptions = {}) {
    if (!directory?.getFileHandle) throw new StoreError("invalid_argument", "OPFS directory handle is required");
    this.#directory = directory; this.#fileName = options.fileName?.trim() || "prolly-store-v1.json";
    this.#lockManager = options.lockManager ?? (globalThis.navigator?.locks as OpfsLockManager | undefined);
    this.#storeDescriptor = validateStoreDescriptor({
      protocolMajor: STORE_PROTOCOL_MAJOR, adapterName: options.adapterName?.trim() || "opfs-v1", provider: "opfs", schemaVersion: 1,
      capabilities: { nativeBatchReads: true, atomicBatchWrites: true, nodeScan: true, hints: true, atomicNodesAndHint: true, rootScan: true, rootCompareAndSwap: true, transactions: true, readParallelism: options.readParallelism ?? 1 }, limits: {},
    });
  }

  async close(): Promise<void> { this.#accepting = false; await this.#tail; }
  async descriptor(signal?: AbortSignal): Promise<StoreDescriptor> { return this.#enqueue("descriptor", signal, async () => this.#storeDescriptor); }
  async getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { const key = hex(cid); return this.#read("get_node", signal, (state) => optional(state.nodes.get(key))); }
  async putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { const key = hex(cid); const node = ownBytes(value); return this.#write("put_node", signal, (state) => state.nodes.set(key, node)); }
  async deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void> { const key = hex(cid); return this.#write("delete_node", signal, (state) => state.nodes.delete(key)); }

  async batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void> {
    const owned = operations.map(cloneNodeMutation); return this.#write("batch_nodes", signal, (state) => applyNodes(state, owned));
  }
  async publishNodes(publication: NodePublication, signal?: AbortSignal): Promise<void> { return publishNodesWithGeneralPath(this, publication, signal); }
  async batchGetNodesOrdered(cids: readonly Uint8Array[], signal?: AbortSignal): Promise<OptionalBytes[]> { const keys = cids.map(hex); return this.#read("batch_get_nodes_ordered", signal, (state) => keys.map((key) => optional(state.nodes.get(key)))); }
  async listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]> { return this.#read("list_node_cids", signal, (state) => [...state.nodes.keys()].map(fromHex).filter((cid) => cid.byteLength === 32).sort(compareBytes)); }
  async getHint(namespace: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { const storageKey = hintKey(namespace, key); return this.#read("get_hint", signal, (state) => optional(state.hints.get(storageKey))); }
  async putHint(namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { const storageKey = hintKey(namespace, key); const hint = ownBytes(value); return this.#write("put_hint", signal, (state) => state.hints.set(storageKey, hint)); }
  async batchPutNodesWithHint(nodes: readonly NodeEntry[], namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { const owned = nodes.map(({ cid, node }) => ({ cid: ownBytes(cid), node: ownBytes(node) })); const storageKey = hintKey(namespace, key); const hint = ownBytes(value); return this.#write("batch_put_nodes_with_hint", signal, (state) => { for (const node of owned) state.nodes.set(hex(node.cid), node.node); state.hints.set(storageKey, hint); }); }
  async getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { const key = hex(name); return this.#read("get_root_manifest", signal, (state) => optional(state.roots.get(key))); }
  async putRootManifest(name: Uint8Array, manifest: Uint8Array, signal?: AbortSignal): Promise<void> { const key = hex(name); const value = ownBytes(manifest); return this.#write("put_root_manifest", signal, (state) => state.roots.set(key, value)); }
  async deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void> { const key = hex(name); return this.#write("delete_root_manifest", signal, (state) => state.roots.delete(key)); }

  async compareAndSwapRootManifest(name: Uint8Array, expected: OptionalBytes, replacement: OptionalBytes, signal?: AbortSignal): Promise<RootCasResult> {
    const key = hex(name); const wanted = normalizeOptionalBytes(expected); const next = normalizeOptionalBytes(replacement);
    return this.#update<RootCasResult>("compare_and_swap_root_manifest", signal, (state) => {
      const current = optional(state.roots.get(key)); if (!optionalEqual(current, wanted)) return { result: { applied: false, current }, changed: false };
      if (next.present) state.roots.set(key, ownBytes(next.value)); else state.roots.delete(key);
      return { result: { applied: true, current: next.present ? presentBytes(next.value) : missingBytes() }, changed: true };
    });
  }

  async listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]> { return this.#read("list_root_manifests", signal, (state) => [...state.roots].map(([name, manifest]) => ({ name: fromHex(name), manifest: ownBytes(manifest) })).sort((left, right) => compareBytes(left.name, right.name))); }

  async commitTransaction(nodes: readonly NodeMutation[], conditions: readonly RootCondition[], roots: readonly RootWrite[], signal?: AbortSignal): Promise<StoreTransactionResult> {
    const ownedNodes = nodes.map(cloneNodeMutation); const ownedConditions = conditions.map(({ name, expected }) => ({ name: ownBytes(name), expected: normalizeOptionalBytes(expected) })); const ownedRoots = roots.map(cloneRootWrite);
    return this.#update("commit_transaction", signal, (state) => {
      for (const condition of ownedConditions) { const current = optional(state.roots.get(hex(condition.name))); if (!optionalEqual(current, condition.expected)) return { result: { applied: false, conflict: { name: ownBytes(condition.name), expected: normalizeOptionalBytes(condition.expected), current } } as StoreTransactionResult, changed: false }; }
      applyNodes(state, ownedNodes); for (const root of ownedRoots) if (root.kind === "put") state.roots.set(hex(root.name), ownBytes(root.manifest)); else state.roots.delete(hex(root.name));
      return { result: { applied: true } as StoreTransactionResult, changed: true };
    });
  }

  async #read<T>(operation: string, signal: AbortSignal | undefined, call: (state: State) => T): Promise<T> { return this.#enqueue(operation, signal, async () => call(await this.#load())); }
  async #write(operation: string, signal: AbortSignal | undefined, call: (state: State) => unknown): Promise<void> { return this.#update(operation, signal, (state) => { call(state); return { result: undefined, changed: true }; }); }
  async #update<T>(operation: string, signal: AbortSignal | undefined, call: (state: State) => { result: T; changed: boolean }): Promise<T> {
    return this.#enqueue(operation, signal, async () => { const current = await this.#load(); const draft = cloneState(current); const { result, changed } = call(draft); if (changed) { await this.#save(draft); throwIfAborted(signal); this.#state = draft; } return result; });
  }

  async #enqueue<T>(operation: string, signal: AbortSignal | undefined, call: () => Promise<T>): Promise<T> {
    throwIfAborted(signal); if (!this.#accepting) throw new StoreError("internal", "OPFS store is closed");
    const preceding = directoryQueues.get(this.#directory) ?? Promise.resolve();
    const pending = preceding.then(async () => {
      throwIfAborted(signal); this.#state = undefined;
      return this.#lockManager ? this.#lockManager.request(`prolly-opfs:${this.#fileName}`, { mode: "exclusive" }, call) : call();
    });
    this.#tail = pending.then(() => undefined, () => undefined); directoryQueues.set(this.#directory, this.#tail);
    try { return await pending; } catch (error) { if (error instanceof StoreError) throw error; throw mapOpfsError(operation, error); }
  }

  async #load(): Promise<State> {
    if (this.#state) return this.#state;
    const handle = await this.#directory.getFileHandle(this.#fileName, { create: true }); const content = await (await handle.getFile()).text();
    this.#state = content ? deserialize(content) : emptyState(); return this.#state;
  }

  async #save(state: State): Promise<void> {
    const handle = await this.#directory.getFileHandle(this.#fileName, { create: true }); const writable = await handle.createWritable();
    try { await writable.write(serialize(state)); await writable.close(); }
    catch (error) { await writable.abort?.().catch(() => undefined); throw error; }
  }
}

function emptyState(): State { return { nodes: new Map(), hints: new Map(), roots: new Map() }; }
function cloneState(state: State): State { const clone = (source: Map<string, Uint8Array>) => new Map([...source].map(([key, value]) => [key, ownBytes(value)])); return { nodes: clone(state.nodes), hints: clone(state.hints), roots: clone(state.roots) }; }
function optional(value?: Uint8Array): OptionalBytes { return value === undefined ? missingBytes() : presentBytes(value); }
function hintKey(namespace: Uint8Array, key: Uint8Array): string { return `${hex(namespace)}:${hex(key)}`; }
function fromHex(value: string): Uint8Array { if (value.length % 2 || !/^[0-9a-f]*$/.test(value)) throw new StoreError("invalid_data", "OPFS store contains an invalid binary key"); return Uint8Array.from(value.match(/../g)?.map((byte) => Number.parseInt(byte, 16)) ?? []); }
function cloneNodeMutation(value: NodeMutation): NodeMutation { return value.kind === "upsert" ? { kind: "upsert", cid: ownBytes(value.cid), node: ownBytes(value.node) } : { kind: "delete", cid: ownBytes(value.cid) }; }
function cloneRootWrite(value: RootWrite): RootWrite { return value.kind === "put" ? { kind: "put", name: ownBytes(value.name), manifest: ownBytes(value.manifest) } : { kind: "delete", name: ownBytes(value.name) }; }
function applyNodes(state: State, nodes: readonly NodeMutation[]): void { for (const node of nodes) if (node.kind === "upsert") state.nodes.set(hex(node.cid), ownBytes(node.node)); else state.nodes.delete(hex(node.cid)); }
function encode(value: Uint8Array): string { let binary = ""; for (const byte of value) binary += String.fromCharCode(byte); return btoa(binary); }
function decode(value: string): Uint8Array { const binary = atob(value); return Uint8Array.from(binary, (character) => character.charCodeAt(0)); }
function serialize(state: State): string { const entries = (values: Map<string, Uint8Array>) => [...values].map(([key, value]) => [key, encode(value)]); return JSON.stringify({ version: 1, nodes: entries(state.nodes), hints: entries(state.hints), roots: entries(state.roots) }); }
function deserialize(content: string): State { try { const value = JSON.parse(content); if (value.version !== 1) throw new Error("version"); const entries = (items: unknown) => { if (!Array.isArray(items)) throw new Error("entries"); return new Map(items.map((item) => { if (!Array.isArray(item) || item.length !== 2 || typeof item[0] !== "string" || typeof item[1] !== "string") throw new Error("entry"); return [item[0], decode(item[1])]; })); }; return { nodes: entries(value.nodes), hints: entries(value.hints), roots: entries(value.roots) }; } catch (error) { throw new StoreError("invalid_data", "OPFS store file is invalid", { cause: error }); } }
function mapOpfsError(operation: string, error: unknown): StoreError { const name = typeof error === "object" && error && "name" in error ? String(error.name) : undefined; const exhausted = name === "QuotaExceededError"; const retryable = name === "NoModificationAllowedError" || name === "InvalidStateError"; return new StoreError(exhausted ? "resource_exhausted" : retryable ? "unavailable" : "internal", "OPFS provider operation failed", { retryable, providerCode: name ? `opfs:${name}:${operation}` : undefined, cause: error }); }
