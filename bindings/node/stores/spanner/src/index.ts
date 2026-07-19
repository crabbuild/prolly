import type { Database, Transaction } from "@google-cloud/spanner";

import {
  StoreError,
  deleteNode,
  missingBytes,
  normalizeOptionalBytes,
  ownBytes,
  presentBytes,
  publishNodesWithGeneralPath,
  throwIfAborted,
  validateStoreDescriptor,
  type NamedStoreRoot,
  type NodeEntry,
  type NodeMutation,
  type NodePublication,
  type OptionalBytes,
  type RemoteStore,
  type RootCasResult,
  type RootCondition,
  type RootWrite,
  type StoreDescriptor,
  type StoreTransactionResult,
} from "@trail/prolly-node/remote-store";

export const SPANNER_DDL = [
  "CREATE TABLE ProllyNodes (\n  Cid BYTES(32) NOT NULL,\n  Node BYTES(MAX) NOT NULL\n) PRIMARY KEY (Cid)",
  "CREATE TABLE ProllyHints (\n  Namespace BYTES(MAX) NOT NULL,\n  HintKey BYTES(MAX) NOT NULL,\n  Value BYTES(MAX) NOT NULL\n) PRIMARY KEY (Namespace, HintKey)",
  "CREATE TABLE ProllyRoots (\n  Name BYTES(MAX) NOT NULL,\n  Manifest BYTES(MAX) NOT NULL\n) PRIMARY KEY (Name)",
] as const;

export type SpannerMutation =
  | { readonly kind: "upsertNode"; readonly key: Uint8Array; readonly value: Uint8Array }
  | { readonly kind: "deleteNode"; readonly key: Uint8Array }
  | { readonly kind: "upsertHint"; readonly namespace: Uint8Array; readonly key: Uint8Array; readonly value: Uint8Array }
  | { readonly kind: "upsertRoot"; readonly key: Uint8Array; readonly value: Uint8Array }
  | { readonly kind: "deleteRoot"; readonly key: Uint8Array };

export interface SpannerRootRecord { readonly name: Uint8Array; readonly manifest: Uint8Array; }
export interface SpannerTransaction {
  getRoot(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes>;
  buffer(mutations: readonly SpannerMutation[]): void;
}
export interface SpannerItemClient {
  getNode(key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes>;
  getHint(namespace: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes>;
  getRoot(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes>;
  listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]>;
  listRoots(signal?: AbortSignal): Promise<SpannerRootRecord[]>;
  apply(mutations: readonly SpannerMutation[], signal?: AbortSignal): Promise<void>;
  readWrite<T>(callback: (transaction: SpannerTransaction) => Promise<T>, signal?: AbortSignal): Promise<T>;
}

export interface SpannerStoreOptions { readonly adapterName?: string; readonly readParallelism?: number; }

export class SpannerStore implements RemoteStore {
  readonly #client: SpannerItemClient;
  readonly #descriptor: StoreDescriptor;
  readonly #pending = new Set<Promise<unknown>>();
  #accepting = true;

  constructor(source: Database | SpannerItemClient, options: SpannerStoreOptions = {}) {
    if (source == null) throw new StoreError("invalid_argument", "Spanner client is required");
    this.#client = isItemClient(source) ? source : new SpannerSdkItemClient(source);
    this.#descriptor = validateStoreDescriptor({
      protocolMajor: 2,
      adapterName: options.adapterName?.trim() || "spanner-v1",
      provider: "spanner",
      schemaVersion: 1,
      capabilities: { nativeBatchReads: false, atomicBatchWrites: true, nodeScan: true, hints: true, atomicNodesAndHint: true, rootScan: true, rootCompareAndSwap: true, transactions: true, readParallelism: options.readParallelism ?? 16 },
      limits: {},
    });
  }

  static fromClient(client: SpannerItemClient, options: SpannerStoreOptions = {}): SpannerStore { return new SpannerStore(client, options); }
  async close(): Promise<void> { this.#accepting = false; await Promise.allSettled([...this.#pending]); }
  async descriptor(signal?: AbortSignal): Promise<StoreDescriptor> { return this.#run("descriptor", signal, async () => this.#descriptor); }
  async getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { const key = ownBytes(cid); return this.#run("get_node", signal, () => this.#client.getNode(key, signal)); }
  async putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#apply([{ kind: "upsertNode", key: ownBytes(cid), value: ownBytes(value) }], "put_node", signal); }
  async deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#apply([{ kind: "deleteNode", key: ownBytes(cid) }], "delete_node", signal); }
  async batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void> { return this.#apply(operations.map(nodeMutation), "batch_nodes", signal); }
  async publishNodes(publication: NodePublication, signal?: AbortSignal): Promise<void> { return publishNodesWithGeneralPath(this, publication, signal); }
  async batchGetNodesOrdered(cids: readonly Uint8Array[], signal?: AbortSignal): Promise<OptionalBytes[]> { const result: OptionalBytes[] = []; for (const cid of cids) result.push(await this.getNode(cid, signal)); return result; }
  async listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]> { return this.#run("list_node_cids", signal, async () => (await this.#client.listNodeCids(signal)).filter((value) => value.byteLength === 32).map(ownBytes).sort(compareBytes)); }
  async getHint(namespace: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { const ownedNamespace = ownBytes(namespace); const ownedKey = ownBytes(key); return this.#run("get_hint", signal, () => this.#client.getHint(ownedNamespace, ownedKey, signal)); }
  async putHint(namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#apply([{ kind: "upsertHint", namespace: ownBytes(namespace), key: ownBytes(key), value: ownBytes(value) }], "put_hint", signal); }
  async batchPutNodesWithHint(nodes: readonly NodeEntry[], namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#apply([...nodes.map(({ cid, node }) => ({ kind: "upsertNode" as const, key: ownBytes(cid), value: ownBytes(node) })), { kind: "upsertHint", namespace: ownBytes(namespace), key: ownBytes(key), value: ownBytes(value) }], "batch_nodes_hint", signal); }
  async getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { const key = ownBytes(name); return this.#run("get_root_manifest", signal, () => this.#client.getRoot(key, signal)); }
  async putRootManifest(name: Uint8Array, manifest: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#apply([{ kind: "upsertRoot", key: ownBytes(name), value: ownBytes(manifest) }], "put_root_manifest", signal); }
  async deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#apply([{ kind: "deleteRoot", key: ownBytes(name) }], "delete_root_manifest", signal); }

  async compareAndSwapRootManifest(name: Uint8Array, expected: OptionalBytes, replacement: OptionalBytes, signal?: AbortSignal): Promise<RootCasResult> {
    const key = ownBytes(name); const wanted = normalizeOptionalBytes(expected); const next = normalizeOptionalBytes(replacement);
    return this.#run("compare_and_swap_root_manifest", signal, () => this.#client.readWrite(async (transaction) => {
      const current = normalizeOptionalBytes(await transaction.getRoot(key, signal));
      if (!optionalEqual(current, wanted)) return { applied: false, current };
      transaction.buffer([next.present ? { kind: "upsertRoot", key, value: ownBytes(next.value) } : { kind: "deleteRoot", key }]);
      return { applied: true, current: normalizeOptionalBytes(next) };
    }, signal));
  }

  async listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]> { return this.#run("list_root_manifests", signal, async () => (await this.#client.listRoots(signal)).map(({ name, manifest }) => ({ name: ownBytes(name), manifest: ownBytes(manifest) })).sort((left, right) => compareBytes(left.name, right.name))); }

  async commitTransaction(nodes: readonly NodeMutation[], conditions: readonly RootCondition[], roots: readonly RootWrite[], signal?: AbortSignal): Promise<StoreTransactionResult> {
    const ownedNodes = nodes.map(cloneNodeMutation); const ownedConditions = conditions.map(({ name, expected }) => ({ name: ownBytes(name), expected: normalizeOptionalBytes(expected) })); const ownedRoots = roots.map(cloneRootWrite);
    return this.#run("commit_transaction", signal, () => this.#client.readWrite(async (transaction) => {
      for (const condition of ownedConditions) {
        const current = normalizeOptionalBytes(await transaction.getRoot(condition.name, signal));
        if (!optionalEqual(current, condition.expected)) return { applied: false, conflict: { name: ownBytes(condition.name), expected: normalizeOptionalBytes(condition.expected), current } };
      }
      transaction.buffer([...ownedNodes.map(nodeMutation), ...ownedRoots.map(rootMutation)]);
      return { applied: true };
    }, signal));
  }

  async #apply(mutations: readonly SpannerMutation[], operation: string, signal?: AbortSignal): Promise<void> { if (mutations.length === 0) return; return this.#run(operation, signal, () => this.#client.apply(mutations.map(cloneMutation), signal)); }
  async #run<T>(operation: string, signal: AbortSignal | undefined, call: () => Promise<T>): Promise<T> { throwIfAborted(signal); if (!this.#accepting) throw new StoreError("internal", "Spanner store is closed"); const pending = call().then((value) => { throwIfAborted(signal); return value; }).catch((error: unknown) => { if (signal?.aborted) throw new StoreError("cancelled", "Spanner operation was cancelled", { cause: signal.reason }); throw mapSpannerError(operation, error); }); this.#pending.add(pending); try { return await pending; } finally { this.#pending.delete(pending); } }
}

class SpannerSdkItemClient implements SpannerItemClient {
  readonly #database: Database;
  constructor(database: Database) { this.#database = database; }
  async getNode(key: Uint8Array): Promise<OptionalBytes> { return readValue(this.#database, "SELECT Node FROM ProllyNodes WHERE Cid = @key", "Node", { key: Buffer.from(key) }); }
  async getHint(namespace: Uint8Array, key: Uint8Array): Promise<OptionalBytes> { return readValue(this.#database, "SELECT Value FROM ProllyHints WHERE Namespace = @namespace AND HintKey = @key", "Value", { namespace: Buffer.from(namespace), key: Buffer.from(key) }); }
  async getRoot(name: Uint8Array): Promise<OptionalBytes> { return readValue(this.#database, "SELECT Manifest FROM ProllyRoots WHERE Name = @key", "Manifest", { key: Buffer.from(name) }); }
  async listNodeCids(): Promise<Uint8Array[]> { const [rows] = await this.#database.run({ sql: "SELECT Cid FROM ProllyNodes ORDER BY Cid", json: true }); return rows.map((row) => ownBytes(field(row, "Cid"))); }
  async listRoots(): Promise<SpannerRootRecord[]> { const [rows] = await this.#database.run({ sql: "SELECT Name, Manifest FROM ProllyRoots ORDER BY Name", json: true }); return rows.map((row) => ({ name: ownBytes(field(row, "Name")), manifest: ownBytes(field(row, "Manifest")) })); }
  async apply(mutations: readonly SpannerMutation[]): Promise<void> { await this.#database.runTransactionAsync(async (transaction) => { bufferSdk(transaction, mutations); await transaction.commit(); }); }
  async readWrite<T>(callback: (transaction: SpannerTransaction) => Promise<T>): Promise<T> { return this.#database.runTransactionAsync(async (transaction) => { const buffered: SpannerMutation[] = []; const result = await callback({ getRoot: (name) => readValue(transaction, "SELECT Manifest FROM ProllyRoots WHERE Name = @key", "Manifest", { key: Buffer.from(name) }), buffer: (mutations) => buffered.push(...mutations.map(cloneMutation)) }); bufferSdk(transaction, buffered); await transaction.commit(); return result; }); }
}

async function readValue(runner: Database | Transaction, sql: string, column: string, params: Record<string, Buffer>): Promise<OptionalBytes> { const [rows] = await runner.run({ sql, params, json: true }); if (rows.length === 0) return missingBytes(); return presentBytes(field(rows[0]!, column)); }
function field(row: unknown, name: string): Uint8Array { const record = row as Record<string, unknown> & { toJSON?: () => Record<string, unknown> }; const value = record[name] ?? record.toJSON?.()[name]; if (!(value instanceof Uint8Array)) throw new StoreError("invalid_data", `Spanner ${name} column is not bytes`); return value; }
function bufferSdk(transaction: Transaction, mutations: readonly SpannerMutation[]): void { for (const mutation of mutations) switch (mutation.kind) { case "upsertNode": transaction.upsert("ProllyNodes", { Cid: Buffer.from(mutation.key), Node: Buffer.from(mutation.value) }); break; case "deleteNode": transaction.deleteRows("ProllyNodes", [byteKey(mutation.key)]); break; case "upsertHint": transaction.upsert("ProllyHints", { Namespace: Buffer.from(mutation.namespace), HintKey: Buffer.from(mutation.key), Value: Buffer.from(mutation.value) }); break; case "upsertRoot": transaction.upsert("ProllyRoots", { Name: Buffer.from(mutation.key), Manifest: Buffer.from(mutation.value) }); break; case "deleteRoot": transaction.deleteRows("ProllyRoots", [byteKey(mutation.key)]); break; } }
// Spanner 8.0 accepts Buffer key parts at runtime but its Key declaration only lists strings.
function byteKey(value: Uint8Array): string[] { return [Buffer.from(value)] as unknown as string[]; }
function nodeMutation(value: NodeMutation): SpannerMutation { return value.kind === "upsert" ? { kind: "upsertNode", key: ownBytes(value.cid), value: ownBytes(value.node) } : { kind: "deleteNode", key: ownBytes(value.cid) }; }
function rootMutation(value: RootWrite): SpannerMutation { return value.kind === "put" ? { kind: "upsertRoot", key: ownBytes(value.name), value: ownBytes(value.manifest) } : { kind: "deleteRoot", key: ownBytes(value.name) }; }
function cloneNodeMutation(value: NodeMutation): NodeMutation { return value.kind === "upsert" ? { kind: "upsert", cid: ownBytes(value.cid), node: ownBytes(value.node) } : deleteNode(value.cid); }
function cloneRootWrite(value: RootWrite): RootWrite { return value.kind === "put" ? { kind: "put", name: ownBytes(value.name), manifest: ownBytes(value.manifest) } : { kind: "delete", name: ownBytes(value.name) }; }
function cloneMutation(value: SpannerMutation): SpannerMutation { switch (value.kind) { case "upsertNode": return { kind: value.kind, key: ownBytes(value.key), value: ownBytes(value.value) }; case "deleteNode": return { kind: value.kind, key: ownBytes(value.key) }; case "upsertHint": return { kind: value.kind, namespace: ownBytes(value.namespace), key: ownBytes(value.key), value: ownBytes(value.value) }; case "upsertRoot": return { kind: value.kind, key: ownBytes(value.key), value: ownBytes(value.value) }; case "deleteRoot": return { kind: value.kind, key: ownBytes(value.key) }; } }
function optionalEqual(left: OptionalBytes, right: OptionalBytes): boolean { return left.present === right.present && (!left.present || Buffer.from(left.value).equals(Buffer.from(right.value))); }
function compareBytes(left: Uint8Array, right: Uint8Array): number { return Buffer.compare(Buffer.from(left), Buffer.from(right)); }
function isItemClient(value: Database | SpannerItemClient): value is SpannerItemClient { return typeof (value as SpannerItemClient).readWrite === "function" && typeof (value as SpannerItemClient).listRoots === "function"; }
function statusCode(error: unknown): number { if (typeof error !== "object" || error === null) return 0; const code = (error as { code?: unknown }).code; return typeof code === "number" ? code : 0; }
function mapSpannerError(operation: string, error: unknown): StoreError { if (error instanceof StoreError) return error; const code = statusCode(error); if (code === 1) return new StoreError("cancelled", "Spanner operation was cancelled", { providerCode: `grpc:${code}:${operation}`, cause: error }); if (code === 8) return new StoreError("resource_exhausted", "Spanner operation exhausted provider resources", { retryable: true, providerCode: `grpc:${code}:${operation}`, cause: error }); const retryable = [4, 10, 14].includes(code); return new StoreError(retryable ? "unavailable" : "internal", "Spanner operation failed", { retryable, providerCode: code === 0 ? undefined : `grpc:${code}:${operation}`, cause: error }); }
