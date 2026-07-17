import type { PGliteInterface, Results, Transaction } from "@electric-sql/pglite";
import {
  StoreError, compareBytes, equalBytes, hex, missingBytes, normalizeOptionalBytes, optionalEqual, ownBytes,
  presentBytes, throwIfAborted, validateStoreDescriptor,
  type NamedStoreRoot, type NodeEntry, type NodeMutation, type OptionalBytes, type RemoteStore,
  type RootCasResult, type RootCondition, type RootWrite, type StoreDescriptor, type StoreTransactionResult,
} from "@trail/prolly-wasm/remote-store";

export const PGLITE_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS prolly_nodes (cid bytea PRIMARY KEY, node bytea NOT NULL);
CREATE TABLE IF NOT EXISTS prolly_hints (namespace bytea NOT NULL, key bytea NOT NULL, value bytea NOT NULL, PRIMARY KEY(namespace, key));
CREATE TABLE IF NOT EXISTS prolly_roots (name bytea PRIMARY KEY, manifest bytea NOT NULL);
`;
const SELECT_NODE = "SELECT node AS value FROM prolly_nodes WHERE cid = $1";
const UPSERT_NODE = "INSERT INTO prolly_nodes (cid, node) VALUES ($1, $2) ON CONFLICT(cid) DO UPDATE SET node = excluded.node";
const DELETE_NODE = "DELETE FROM prolly_nodes WHERE cid = $1";
const SELECT_HINT = "SELECT value FROM prolly_hints WHERE namespace = $1 AND key = $2";
const UPSERT_HINT = "INSERT INTO prolly_hints (namespace, key, value) VALUES ($1, $2, $3) ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value";
const SELECT_ROOT = "SELECT manifest AS value FROM prolly_roots WHERE name = $1";
const SELECT_ROOT_FOR_UPDATE = `${SELECT_ROOT} FOR UPDATE`;
const UPSERT_ROOT = "INSERT INTO prolly_roots (name, manifest) VALUES ($1, $2) ON CONFLICT(name) DO UPDATE SET manifest = excluded.manifest";
const DELETE_ROOT = "DELETE FROM prolly_roots WHERE name = $1";
const LOCK_ROOT = "SELECT pg_advisory_xact_lock(hashtextextended(encode($1::bytea, 'hex'), 0))";

export interface BrowserPGliteStoreOptions { readonly adapterName?: string; readonly readParallelism?: number; }

export class BrowserPGliteStore implements RemoteStore {
  readonly #database: PGliteInterface;
  readonly #storeDescriptor: StoreDescriptor;
  readonly #pending = new Set<Promise<unknown>>();
  #accepting = true;

  constructor(database: PGliteInterface, options: BrowserPGliteStoreOptions = {}) {
    if (!database) throw new StoreError("invalid_argument", "PGlite database is required"); this.#database = database;
    this.#storeDescriptor = validateStoreDescriptor({ protocolMajor: 1, adapterName: options.adapterName?.trim() || "browser-pglite-v1", provider: "pglite", schemaVersion: 1, capabilities: { nativeBatchReads: true, atomicBatchWrites: true, nodeScan: true, hints: true, atomicNodesAndHint: true, rootScan: true, rootCompareAndSwap: true, transactions: true, readParallelism: options.readParallelism ?? 16 }, limits: {} });
  }

  async initializeSchema(signal?: AbortSignal): Promise<void> { return this.#run("initialize_schema", signal, async () => { await this.#database.exec(PGLITE_SCHEMA_SQL); }); }
  async close(): Promise<void> { this.#accepting = false; await Promise.allSettled([...this.#pending]); }
  async descriptor(signal?: AbortSignal): Promise<StoreDescriptor> { return this.#run("descriptor", signal, async () => this.#storeDescriptor); }
  async getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { const key = ownBytes(cid); return this.#run("get_node", signal, () => queryOptional(this.#database, SELECT_NODE, [key], signal)); }
  async putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { const key = ownBytes(cid); const node = ownBytes(value); return this.#run("put_node", signal, async () => { await query(this.#database, UPSERT_NODE, [key, node], signal); }); }
  async deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void> { const key = ownBytes(cid); return this.#run("delete_node", signal, async () => { await query(this.#database, DELETE_NODE, [key], signal); }); }
  async batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void> { const owned = operations.map(cloneNodeMutation); return this.#run("batch_nodes", signal, () => this.#database.transaction(async (transaction) => applyNodeMutations(transaction, owned, signal))); }
  async batchGetNodesOrdered(cids: readonly Uint8Array[], signal?: AbortSignal): Promise<OptionalBytes[]> { const keys = cids.map(ownBytes); return this.#run("batch_get_nodes_ordered", signal, async () => { if (!keys.length) return []; const placeholders = keys.map((_, index) => `$${index + 1}`).join(", "); const rows = (await query<{ cid: Uint8Array; node: Uint8Array }>(this.#database, `SELECT cid, node FROM prolly_nodes WHERE cid IN (${placeholders})`, keys, signal)).rows; const values = new Map(rows.map(({ cid, node }) => [hex(cid), ownBytes(node)])); return keys.map((key) => values.has(hex(key)) ? presentBytes(values.get(hex(key))!) : missingBytes()); }); }
  async listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]> { return this.#run("list_node_cids", signal, async () => (await query<{ cid: Uint8Array }>(this.#database, "SELECT cid FROM prolly_nodes ORDER BY cid", [], signal)).rows.map(({ cid }) => ownBytes(cid)).filter((cid) => cid.byteLength === 32)); }
  async getHint(namespace: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { const ns = ownBytes(namespace); const hintKey = ownBytes(key); return this.#run("get_hint", signal, () => queryOptional(this.#database, SELECT_HINT, [ns, hintKey], signal)); }
  async putHint(namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { const ns = ownBytes(namespace); const hintKey = ownBytes(key); const hint = ownBytes(value); return this.#run("put_hint", signal, async () => { await query(this.#database, UPSERT_HINT, [ns, hintKey, hint], signal); }); }
  async batchPutNodesWithHint(nodes: readonly NodeEntry[], namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { const owned = nodes.map(({ cid, node }) => ({ cid: ownBytes(cid), node: ownBytes(node) })); const ns = ownBytes(namespace); const hintKey = ownBytes(key); const hint = ownBytes(value); return this.#run("batch_put_nodes_with_hint", signal, () => this.#database.transaction(async (transaction) => { for (const node of owned) await query(transaction, UPSERT_NODE, [node.cid, node.node], signal); await query(transaction, UPSERT_HINT, [ns, hintKey, hint], signal); })); }
  async getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { const key = ownBytes(name); return this.#run("get_root_manifest", signal, () => queryOptional(this.#database, SELECT_ROOT, [key], signal)); }
  async putRootManifest(name: Uint8Array, manifest: Uint8Array, signal?: AbortSignal): Promise<void> { const key = ownBytes(name); const value = ownBytes(manifest); return this.#run("put_root_manifest", signal, async () => { await query(this.#database, UPSERT_ROOT, [key, value], signal); }); }
  async deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void> { const key = ownBytes(name); return this.#run("delete_root_manifest", signal, async () => { await query(this.#database, DELETE_ROOT, [key], signal); }); }

  async compareAndSwapRootManifest(name: Uint8Array, expected: OptionalBytes, replacement: OptionalBytes, signal?: AbortSignal): Promise<RootCasResult> { const key = ownBytes(name); const wanted = normalizeOptionalBytes(expected); const next = normalizeOptionalBytes(replacement); return this.#run("compare_and_swap_root_manifest", signal, () => this.#database.transaction(async (transaction) => { await query(transaction, LOCK_ROOT, [key], signal); const current = await queryOptional(transaction, SELECT_ROOT_FOR_UPDATE, [key], signal); if (!optionalEqual(current, wanted)) return { applied: false, current }; await writeRoot(transaction, key, next, signal); return { applied: true, current: next.present ? presentBytes(next.value) : missingBytes() }; })); }
  async listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]> { return this.#run("list_root_manifests", signal, async () => (await query<{ name: Uint8Array; manifest: Uint8Array }>(this.#database, "SELECT name, manifest FROM prolly_roots ORDER BY name", [], signal)).rows.map(({ name, manifest }) => ({ name: ownBytes(name), manifest: ownBytes(manifest) }))); }

  async commitTransaction(nodes: readonly NodeMutation[], conditions: readonly RootCondition[], roots: readonly RootWrite[], signal?: AbortSignal): Promise<StoreTransactionResult> { const ownedNodes = nodes.map(cloneNodeMutation); const ownedConditions = conditions.map(({ name, expected }) => ({ name: ownBytes(name), expected: normalizeOptionalBytes(expected) })); const ownedRoots = roots.map(cloneRootWrite); return this.#run("commit_transaction", signal, () => this.#database.transaction(async (transaction) => { for (const name of uniqueSortedNames(ownedConditions.map(({ name }) => name))) await query(transaction, LOCK_ROOT, [name], signal); for (const condition of ownedConditions) { const current = await queryOptional(transaction, SELECT_ROOT_FOR_UPDATE, [condition.name], signal); if (!optionalEqual(current, condition.expected)) return { applied: false, conflict: { name: ownBytes(condition.name), expected: normalizeOptionalBytes(condition.expected), current } }; } await applyNodeMutations(transaction, ownedNodes, signal); for (const root of ownedRoots) await writeRoot(transaction, root.name, root.kind === "put" ? presentBytes(root.manifest) : missingBytes(), signal); return { applied: true }; })); }

  async #run<T>(operation: string, signal: AbortSignal | undefined, call: () => Promise<T>): Promise<T> { throwIfAborted(signal); if (!this.#accepting) throw new StoreError("internal", "PGlite store is closed"); const pending = call().then((value) => { throwIfAborted(signal); return value; }).catch((error) => { if (signal?.aborted) throw new StoreError("cancelled", "PGlite operation was cancelled", { cause: signal.reason }); throw mapPGliteError(operation, error); }); this.#pending.add(pending); try { return await pending; } finally { this.#pending.delete(pending); } }
}

type Queryable = Pick<PGliteInterface, "query"> | Pick<Transaction, "query">;
async function query<T = Record<string, unknown>>(client: Queryable, text: string, values: readonly unknown[], signal?: AbortSignal): Promise<Results<T>> { throwIfAborted(signal); const result = await client.query<T>(text, [...values]); throwIfAborted(signal); return result; }
async function queryOptional(client: Queryable, text: string, values: readonly unknown[], signal?: AbortSignal): Promise<OptionalBytes> { const value = (await query<{ value: Uint8Array }>(client, text, values, signal)).rows[0]?.value; return value === undefined ? missingBytes() : presentBytes(value); }
async function applyNodeMutations(client: Queryable, operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void> { for (const operation of operations) if (operation.kind === "upsert") await query(client, UPSERT_NODE, [operation.cid, operation.node], signal); else await query(client, DELETE_NODE, [operation.cid], signal); }
async function writeRoot(client: Queryable, name: Uint8Array, value: OptionalBytes, signal?: AbortSignal): Promise<void> { if (value.present) await query(client, UPSERT_ROOT, [name, value.value], signal); else await query(client, DELETE_ROOT, [name], signal); }
function cloneNodeMutation(value: NodeMutation): NodeMutation { return value.kind === "upsert" ? { kind: "upsert", cid: ownBytes(value.cid), node: ownBytes(value.node) } : { kind: "delete", cid: ownBytes(value.cid) }; }
function cloneRootWrite(value: RootWrite): RootWrite { return value.kind === "put" ? { kind: "put", name: ownBytes(value.name), manifest: ownBytes(value.manifest) } : { kind: "delete", name: ownBytes(value.name) }; }
function uniqueSortedNames(names: readonly Uint8Array[]): Uint8Array[] { const sorted = names.map(ownBytes).sort(compareBytes); return sorted.filter((name, index) => index === 0 || !equalBytes(name, sorted[index - 1]!)); }
function providerCode(error: unknown): string | undefined { if (typeof error !== "object" || !error || !("code" in error) || typeof error.code !== "string") return undefined; return /^[0-9A-Z]{5}$/.test(error.code) ? error.code : undefined; }
function mapPGliteError(operation: string, error: unknown): StoreError { if (error instanceof StoreError) return error; const code = providerCode(error); const retryable = code === "40001" || code === "40P01" || code === "55P03"; return new StoreError(retryable ? "unavailable" : "internal", "PGlite provider operation failed", { retryable, providerCode: code ? `pglite:${code}:${operation}` : undefined, cause: error }); }
