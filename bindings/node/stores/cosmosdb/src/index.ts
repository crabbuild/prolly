import { BulkOperationType, type Container, type OperationInput } from "@azure/cosmos";

import {
  StoreError,
  deleteNode,
  missingBytes,
  normalizeOptionalBytes,
  ownBytes,
  presentBytes,
  throwIfAborted,
  upsertNode,
  validateStoreDescriptor,
  type NamedStoreRoot,
  type NodeEntry,
  type NodeMutation,
  type OptionalBytes,
  type RemoteStore,
  type RootCasResult,
  type RootCondition,
  type RootWrite,
  type StoreDescriptor,
  type StoreTransactionResult,
} from "@trail/prolly-node/remote-store";

const TRANSACTION_LIMIT = 100;
const NODE = Buffer.from("node:");
const ROOT = Buffer.from("root:");
const HINT = Buffer.from("hint:");

export interface CosmosDocument {
  readonly [key: string]: string;
  readonly id: string;
  readonly kind: string;
  readonly family: "node" | "root" | "hint";
  readonly key: string;
  readonly value: string;
}

export interface CosmosReadResult { readonly document: CosmosDocument; readonly etag: string; }
export interface CosmosBatchOperation { readonly kind: "create" | "upsert" | "replace" | "delete"; readonly id?: string; readonly document?: CosmosDocument; readonly etag?: string; }
export interface CosmosBatchResult { readonly statusCode: number; }
export interface CosmosBatchResponse { readonly success: boolean; readonly results: readonly CosmosBatchResult[]; }

export interface CosmosItemClient {
  read(partition: string, id: string, signal?: AbortSignal): Promise<CosmosReadResult>;
  create(partition: string, document: CosmosDocument, signal?: AbortSignal): Promise<void>;
  upsert(partition: string, document: CosmosDocument, signal?: AbortSignal): Promise<void>;
  replace(partition: string, id: string, document: CosmosDocument, etag: string, signal?: AbortSignal): Promise<void>;
  delete(partition: string, id: string, etag: string, signal?: AbortSignal): Promise<void>;
  queryFamily(partition: string, family: CosmosDocument["family"], signal?: AbortSignal): Promise<CosmosDocument[]>;
  executeBatch(partition: string, operations: readonly CosmosBatchOperation[], signal?: AbortSignal): Promise<CosmosBatchResponse>;
  validatePartitionKey?(): Promise<void>;
}

export interface CosmosDbStoreOptions {
  readonly keyPrefix?: Uint8Array;
  readonly partitionKey?: string;
  readonly adapterName?: string;
  readonly readParallelism?: number;
}

export class CosmosDbStore implements RemoteStore {
  readonly #client: CosmosItemClient;
  readonly #partition: string;
  readonly #keyPrefix: Buffer;
  readonly #descriptor: StoreDescriptor;
  readonly #pending = new Set<Promise<unknown>>();
  #accepting = true;

  constructor(source: Container | CosmosItemClient, options: CosmosDbStoreOptions = {}) {
    const client = isItemClient(source) ? source : new CosmosSdkItemClient(source);
    if (client == null) throw new StoreError("invalid_argument", "Cosmos DB item client is required");
    this.#client = client;
    this.#partition = options.partitionKey?.trim() || "prolly";
    this.#keyPrefix = Buffer.from(options.keyPrefix ?? Buffer.from("prolly:"));
    this.#descriptor = validateStoreDescriptor({
      protocolMajor: 1, adapterName: options.adapterName?.trim() || "cosmosdb-v1", provider: "cosmosdb", schemaVersion: 1,
      capabilities: { nativeBatchReads: false, atomicBatchWrites: false, nodeScan: true, hints: true, atomicNodesAndHint: false, rootScan: true, rootCompareAndSwap: true, transactions: true, readParallelism: options.readParallelism ?? 16 },
      limits: { maxTransactionOperations: TRANSACTION_LIMIT },
    });
  }

  static fromClient(client: CosmosItemClient, options: CosmosDbStoreOptions = {}): CosmosDbStore {
    return new CosmosDbStore(client, options);
  }

  async validateContainer(signal?: AbortSignal): Promise<void> { return this.#run("validate_container", signal, async () => { throwIfAborted(signal); if (this.#client.validatePartitionKey === undefined) return; await this.#client.validatePartitionKey(); }); }
  async close(): Promise<void> { this.#accepting = false; await Promise.allSettled([...this.#pending]); }
  async descriptor(signal?: AbortSignal): Promise<StoreDescriptor> { return this.#run("descriptor", signal, async () => this.#descriptor); }
  async getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { return this.#get(this.#familyKey(NODE, cid), "get_node", signal); }
  async putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#upsert("node", this.#familyKey(NODE, cid), value, "put_node", signal); }
  async deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#delete(this.#familyKey(NODE, cid), "", true, "delete_node", signal); }
  async batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void> { for (const operation of operations.map(cloneMutation)) { if (operation.kind === "upsert") await this.putNode(operation.cid, operation.node, signal); else await this.deleteNode(operation.cid, signal); } }
  async batchGetNodesOrdered(cids: readonly Uint8Array[], signal?: AbortSignal): Promise<OptionalBytes[]> { const result: OptionalBytes[] = []; for (const cid of cids) result.push(await this.getNode(cid, signal)); return result; }

  async listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]> {
    return this.#run("list_node_cids", signal, async () => { const prefix = Buffer.concat([this.#keyPrefix, NODE]); return (await this.#queryFamily("node", signal)).map(decodeKey).filter((key) => startsWith(key, prefix) && key.length === prefix.length + 32).map((key) => ownBytes(key.subarray(prefix.length))).sort(compareBytes); });
  }
  async getHint(namespace: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { return this.#get(this.#hintKey(namespace, key), "get_hint", signal); }
  async putHint(namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#upsert("hint", this.#hintKey(namespace, key), value, "put_hint", signal); }
  async batchPutNodesWithHint(nodes: readonly NodeEntry[], namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { for (const node of nodes) await this.putNode(node.cid, node.node, signal); await this.putHint(namespace, key, value, signal); }
  async getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { return this.#get(this.#familyKey(ROOT, name), "get_root_manifest", signal); }
  async putRootManifest(name: Uint8Array, manifest: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#upsert("root", this.#familyKey(ROOT, name), manifest, "put_root_manifest", signal); }
  async deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#delete(this.#familyKey(ROOT, name), "", true, "delete_root_manifest", signal); }

  async compareAndSwapRootManifest(name: Uint8Array, expected: OptionalBytes, replacement: OptionalBytes, signal?: AbortSignal): Promise<RootCasResult> {
    const logicalKey = this.#familyKey(ROOT, name); const wanted = normalizeOptionalBytes(expected); const next = normalizeOptionalBytes(replacement); const id = documentId(logicalKey);
    return this.#run("compare_and_swap_root_manifest", signal, async () => {
      if (!wanted.present) {
        if (!next.present) { const current = await this.#getRaw(logicalKey, signal); return { applied: !current.present, current }; }
        try { await this.#client.create(this.#partition, document(this.#partition, "root", logicalKey, next.value), signal); return { applied: true, current: normalizeOptionalBytes(next) }; }
        catch (error: unknown) { if (!isConflict(error)) throw error; return { applied: false, current: await this.#getRaw(logicalKey, signal) }; }
      }
      let current: CosmosReadResult;
      try { current = await this.#readDocument(logicalKey, signal); }
      catch (error: unknown) { if (status(error) === 404) return { applied: false, current: missingBytes() }; throw error; }
      const value = decodeValue(current.document); if (!Buffer.from(value).equals(Buffer.from(wanted.value))) return { applied: false, current: presentBytes(value) };
      try {
        if (next.present) await this.#client.replace(this.#partition, id, document(this.#partition, "root", logicalKey, next.value), current.etag, signal);
        else await this.#client.delete(this.#partition, id, current.etag, signal);
        return { applied: true, current: normalizeOptionalBytes(next) };
      } catch (error: unknown) { if (!isConflict(error)) throw error; return { applied: false, current: await this.#getRaw(logicalKey, signal) }; }
    });
  }

  async listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]> {
    return this.#run("list_root_manifests", signal, async () => { const prefix = Buffer.concat([this.#keyPrefix, ROOT]); return (await this.#queryFamily("root", signal)).map((entry) => ({ key: decodeKey(entry), value: decodeValue(entry) })).filter(({ key }) => startsWith(key, prefix)).map(({ key, value }) => ({ name: ownBytes(key.subarray(prefix.length)), manifest: value })).sort((left, right) => compareBytes(left.name, right.name)); });
  }

  async commitTransaction(nodes: readonly NodeMutation[], conditions: readonly RootCondition[], roots: readonly RootWrite[], signal?: AbortSignal): Promise<StoreTransactionResult> {
    const ownedNodes = nodes.map(cloneMutation); const ownedConditions = conditions.map(({ name, expected }) => ({ name: ownBytes(name), expected: normalizeOptionalBytes(expected) })); const ownedRoots = roots.map(cloneRootWrite);
    if (ownedNodes.length + ownedRoots.length > TRANSACTION_LIMIT) throw limitError(ownedNodes.length + ownedRoots.length);
    return this.#run("commit_transaction", signal, async () => {
      const conditionsByName = new Map(ownedConditions.map((value) => [hex(value.name), value])); const written = new Set(ownedRoots.map(({ name }) => hex(name))); const operations: CosmosBatchOperation[] = [];
      for (const condition of ownedConditions) if (!written.has(hex(condition.name))) { const result = await this.#conditionOperations(condition, signal); if (result.conflict !== undefined) return { applied: false, conflict: result.conflict }; operations.push(...result.operations); }
      for (const root of ownedRoots) { const result = await this.#rootOperations(root, conditionsByName.get(hex(root.name)), signal); if (result.conflict !== undefined) return { applied: false, conflict: result.conflict }; operations.push(...result.operations); }
      for (const node of ownedNodes) { const key = this.#familyKey(NODE, node.cid); if (node.kind === "upsert") operations.push({ kind: "upsert", document: document(this.#partition, "node", key, node.node) }); else { try { const current = await this.#readDocument(key, signal); operations.push({ kind: "delete", id: documentId(key), etag: current.etag }); } catch (error: unknown) { if (status(error) !== 404) throw error; } } }
      if (operations.length > TRANSACTION_LIMIT) throw limitError(operations.length); if (operations.length === 0) return { applied: true };
      const response = await this.#client.executeBatch(this.#partition, operations, signal); if (response.success) return { applied: true };
      for (const condition of ownedConditions) { const current = await this.#getRaw(this.#familyKey(ROOT, condition.name), signal); if (!optionalEqual(current, condition.expected)) return { applied: false, conflict: conflictFor(condition, current) }; }
      throw new StoreError("internal", "Cosmos DB transaction failed", { providerCode: response.results.map(({ statusCode }) => statusCode).join(",") });
    });
  }

  async clearNamespace(signal?: AbortSignal): Promise<void> { if (this.#keyPrefix.length === 0) throw new StoreError("invalid_argument", "refusing to clear an empty Cosmos DB key prefix"); for (const family of ["node", "root", "hint"] as const) for (const entry of await this.#queryFamily(family, signal)) { const key = decodeKey(entry); if (startsWith(key, this.#keyPrefix)) await this.#delete(key, "", true, "clear_namespace", signal); } }

  async #conditionOperations(condition: RootCondition, signal?: AbortSignal): Promise<{ operations: CosmosBatchOperation[]; conflict?: { name: Uint8Array; expected: OptionalBytes; current: OptionalBytes } }> {
    const key = this.#familyKey(ROOT, condition.name); try { const current = await this.#readDocument(key, signal); const value = decodeValue(current.document); if (!condition.expected.present || !Buffer.from(value).equals(Buffer.from(condition.expected.value))) return { operations: [], conflict: conflictFor(condition, presentBytes(value)) }; return { operations: [{ kind: "replace", id: documentId(key), document: current.document, etag: current.etag }] }; }
    catch (error: unknown) { if (status(error) !== 404) throw error; if (condition.expected.present) return { operations: [], conflict: conflictFor(condition, missingBytes()) }; const placeholder = document(this.#partition, "root", key, new Uint8Array()); return { operations: [{ kind: "create", document: placeholder }, { kind: "delete", id: placeholder.id }] }; }
  }

  async #rootOperations(root: RootWrite, condition: RootCondition | undefined, signal?: AbortSignal): Promise<{ operations: CosmosBatchOperation[]; conflict?: { name: Uint8Array; expected: OptionalBytes; current: OptionalBytes } }> {
    const key = this.#familyKey(ROOT, root.name); const id = documentId(key);
    if (condition === undefined) { if (root.kind === "put") return { operations: [{ kind: "upsert", document: document(this.#partition, "root", key, root.manifest) }] }; try { const current = await this.#readDocument(key, signal); return { operations: [{ kind: "delete", id, etag: current.etag }] }; } catch (error: unknown) { if (status(error) === 404) return { operations: [] }; throw error; } }
    try { const current = await this.#readDocument(key, signal); const value = decodeValue(current.document); if (!condition.expected.present || !Buffer.from(value).equals(Buffer.from(condition.expected.value))) return { operations: [], conflict: conflictFor(condition, presentBytes(value)) }; return root.kind === "put" ? { operations: [{ kind: "replace", id, document: document(this.#partition, "root", key, root.manifest), etag: current.etag }] } : { operations: [{ kind: "delete", id, etag: current.etag }] }; }
    catch (error: unknown) { if (status(error) !== 404) throw error; if (condition.expected.present) return { operations: [], conflict: conflictFor(condition, missingBytes()) }; if (root.kind === "put") return { operations: [{ kind: "create", document: document(this.#partition, "root", key, root.manifest) }] }; const placeholder = document(this.#partition, "root", key, new Uint8Array()); return { operations: [{ kind: "create", document: placeholder }, { kind: "delete", id }] }; }
  }

  async #get(key: Buffer, operation: string, signal?: AbortSignal): Promise<OptionalBytes> { return this.#run(operation, signal, () => this.#getRaw(key, signal)); }
  async #getRaw(key: Buffer, signal?: AbortSignal): Promise<OptionalBytes> { try { return presentBytes(decodeValue((await this.#readDocument(key, signal)).document)); } catch (error: unknown) { if (status(error) === 404) return missingBytes(); throw error; } }
  async #readDocument(key: Uint8Array, signal?: AbortSignal): Promise<CosmosReadResult> { const result = await this.#client.read(this.#partition, documentId(key), signal); if (result.document.id !== documentId(key) || result.document.kind !== this.#partition) throw new StoreError("invalid_data", "Cosmos DB document identity does not match requested key"); return { document: cloneDocument(result.document), etag: result.etag }; }
  async #upsert(family: CosmosDocument["family"], key: Buffer, value: Uint8Array, operation: string, signal?: AbortSignal): Promise<void> { const owned = ownBytes(value); return this.#run(operation, signal, async () => { await this.#client.upsert(this.#partition, document(this.#partition, family, key, owned), signal); }); }
  async #delete(key: Uint8Array, etag: string, ignoreMissing: boolean, operation: string, signal?: AbortSignal): Promise<void> { return this.#run(operation, signal, async () => { try { await this.#client.delete(this.#partition, documentId(key), etag, signal); } catch (error: unknown) { if (!ignoreMissing || status(error) !== 404) throw error; } }); }
  async #queryFamily(family: CosmosDocument["family"], signal?: AbortSignal): Promise<CosmosDocument[]> { return (await this.#client.queryFamily(this.#partition, family, signal)).filter((entry) => entry.kind === this.#partition && entry.family === family).map(cloneDocument); }
  #familyKey(family: Buffer, suffix: Uint8Array): Buffer { return Buffer.concat([this.#keyPrefix, family, Buffer.from(ownBytes(suffix))]); }
  #hintKey(namespace: Uint8Array, key: Uint8Array): Buffer { const length = Buffer.alloc(8); length.writeBigUInt64BE(BigInt(namespace.byteLength)); return Buffer.concat([this.#keyPrefix, HINT, length, Buffer.from(ownBytes(namespace)), Buffer.from(ownBytes(key))]); }
  async #run<T>(operation: string, signal: AbortSignal | undefined, call: () => Promise<T>): Promise<T> { throwIfAborted(signal); if (!this.#accepting) throw new StoreError("internal", "Cosmos DB store is closed"); const pending = call().catch((error: unknown) => { if (signal?.aborted) throw new StoreError("cancelled", "Cosmos DB operation was cancelled", { cause: signal.reason }); throw mapCosmosError(operation, error); }); this.#pending.add(pending); try { return await pending; } finally { this.#pending.delete(pending); } }
}

class CosmosSdkItemClient implements CosmosItemClient {
  readonly #container: Container;
  constructor(container: Container) { if (container == null) throw new StoreError("invalid_argument", "Cosmos DB container is required"); this.#container = container; }
  async validatePartitionKey(): Promise<void> { const { resource } = await this.#container.read(); const paths = resource?.partitionKey?.paths; if (paths?.length !== 1 || paths[0] !== "/kind") throw new StoreError("invalid_argument", "Cosmos DB container partition key must be /kind"); }
  async read(partition: string, id: string, signal?: AbortSignal): Promise<CosmosReadResult> { const response = await this.#container.item(id, partition).read<CosmosDocument>({ abortSignal: signal }); if (response.resource === undefined) throw Object.assign(new Error("not found"), { code: 404 }); return { document: response.resource, etag: response.etag ?? response.resource._etag ?? "" }; }
  async create(_partition: string, value: CosmosDocument, signal?: AbortSignal): Promise<void> { await this.#container.items.create(value, { abortSignal: signal }); }
  async upsert(_partition: string, value: CosmosDocument, signal?: AbortSignal): Promise<void> { await this.#container.items.upsert(value, { abortSignal: signal }); }
  async replace(partition: string, id: string, value: CosmosDocument, etag: string, signal?: AbortSignal): Promise<void> { await this.#container.item(id, partition).replace(value, { abortSignal: signal, accessCondition: { type: "IfMatch", condition: etag } }); }
  async delete(partition: string, id: string, etag: string, signal?: AbortSignal): Promise<void> { await this.#container.item(id, partition).delete({ abortSignal: signal, accessCondition: etag === "" ? undefined : { type: "IfMatch", condition: etag } }); }
  async queryFamily(partition: string, family: CosmosDocument["family"], signal?: AbortSignal): Promise<CosmosDocument[]> { const response = await this.#container.items.query<CosmosDocument>({ query: "SELECT * FROM c WHERE c.kind = @kind AND c.family = @family", parameters: [{ name: "@kind", value: partition }, { name: "@family", value: family }] }, { partitionKey: partition, abortSignal: signal }).fetchAll(); return response.resources; }
  async executeBatch(partition: string, operations: readonly CosmosBatchOperation[], signal?: AbortSignal): Promise<CosmosBatchResponse> { const inputs: OperationInput[] = operations.map((operation) => { switch (operation.kind) { case "create": return { operationType: BulkOperationType.Create, resourceBody: operation.document! }; case "upsert": return { operationType: BulkOperationType.Upsert, resourceBody: operation.document! }; case "replace": return { operationType: BulkOperationType.Replace, id: operation.id!, resourceBody: operation.document!, ifMatch: operation.etag }; case "delete": return { operationType: BulkOperationType.Delete, id: operation.id!, ifMatch: operation.etag }; } }); const response = await this.#container.items.batch(inputs, partition, { abortSignal: signal }); const results = (response.result ?? []).map(({ statusCode }) => ({ statusCode })); return { success: results.every(({ statusCode }) => statusCode >= 200 && statusCode < 300), results }; }
}

function document(partition: string, family: CosmosDocument["family"], key: Uint8Array, value: Uint8Array): CosmosDocument { return { id: documentId(key), kind: partition, family, key: hex(key), value: Buffer.from(value).toString("base64") }; }
function documentId(key: Uint8Array): string { return `k${hex(key)}`; }
function decodeKey(value: CosmosDocument): Buffer { if (!/^(?:[0-9a-f]{2})*$/.test(value.key)) throw new StoreError("invalid_data", "Cosmos DB document key is not valid hex"); return Buffer.from(value.key, "hex"); }
function decodeValue(value: CosmosDocument): Uint8Array { try { return ownBytes(Buffer.from(value.value, "base64")); } catch (error: unknown) { throw new StoreError("invalid_data", "Cosmos DB document value is not valid base64", { cause: error }); } }
function cloneDocument(value: CosmosDocument): CosmosDocument { return { id: value.id, kind: value.kind, family: value.family, key: value.key, value: value.value }; }
function cloneMutation(value: NodeMutation): NodeMutation { return value.kind === "upsert" ? upsertNode(value.cid, value.node) : deleteNode(value.cid); }
function cloneRootWrite(value: RootWrite): RootWrite { const name = ownBytes(value.name); return value.kind === "put" ? { kind: "put", name, manifest: ownBytes(value.manifest) } : { kind: "delete", name }; }
function conflictFor(condition: RootCondition, current: OptionalBytes) { return { name: ownBytes(condition.name), expected: normalizeOptionalBytes(condition.expected), current }; }
function optionalEqual(left: OptionalBytes, right: OptionalBytes): boolean { return left.present === right.present && (!left.present || Buffer.from(left.value).equals(Buffer.from(right.value))); }
function startsWith(value: Uint8Array, prefix: Uint8Array): boolean { return value.length >= prefix.length && Buffer.from(value.subarray(0, prefix.length)).equals(Buffer.from(prefix)); }
function compareBytes(left: Uint8Array, right: Uint8Array): number { return Buffer.compare(Buffer.from(left), Buffer.from(right)); }
function hex(value: Uint8Array): string { return Buffer.from(value).toString("hex"); }
function status(error: unknown): number | undefined { if (typeof error !== "object" || error === null) return undefined; if ("statusCode" in error && typeof error.statusCode === "number") return error.statusCode; if ("code" in error && typeof error.code === "number") return error.code; return undefined; }
function isConflict(error: unknown): boolean { return [404, 409, 412].includes(status(error) ?? 0); }
function limitError(count: number): StoreError { return new StoreError("resource_exhausted", `Cosmos DB transaction has ${count} operations, exceeding the ${TRANSACTION_LIMIT} operation limit`); }
function mapCosmosError(operation: string, error: unknown): StoreError { if (error instanceof StoreError) return error; const code = status(error); const retryable = code === 408 || code === 429 || (code !== undefined && code >= 500); return new StoreError(retryable ? "unavailable" : "internal", "Cosmos DB operation failed", { retryable, providerCode: code === undefined ? undefined : `cosmos:${code}:${operation}`, cause: error }); }
function isItemClient(value: Container | CosmosItemClient): value is CosmosItemClient { return typeof (value as CosmosItemClient).executeBatch === "function" && typeof (value as CosmosItemClient).queryFamily === "function"; }
