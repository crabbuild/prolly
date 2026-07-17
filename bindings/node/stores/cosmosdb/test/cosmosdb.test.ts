import assert from "node:assert/strict";
import test from "node:test";

import { CosmosClient } from "@azure/cosmos";
import { StoreError, missingBytes, presentBytes, upsertNode } from "@trail/prolly-node/remote-store";
import { runStoreConformance } from "@trail/prolly-storetest";

import { CosmosDbStore, type CosmosBatchOperation, type CosmosBatchResponse, type CosmosDocument, type CosmosItemClient, type CosmosReadResult } from "../src/index.ts";

test("Cosmos DB SDK contract", async (suite) => {
  await suite.test("satisfies conformance with exact documents, ETags, and one partition", async () => {
    const client = new MemoryClient(); const store = CosmosDbStore.fromClient(client, { partitionKey: "tenant-a", keyPrefix: bytes("prolly:test:") });
    await store.validateContainer(); await runStoreConformance(() => store);
    const cid = specialBytes(32); await store.putNode(cid, bytes("value"));
    const logicalKey = Buffer.concat([Buffer.from("prolly:test:node:"), cid]); const raw = client.document("tenant-a", `k${logicalKey.toString("hex")}`);
    assert.deepEqual(raw, { id: `k${logicalKey.toString("hex")}`, kind: "tenant-a", family: "node", key: logicalKey.toString("hex"), value: Buffer.from("value").toString("base64") });
    assert.equal(client.validations, 1); assert.ok(client.matchedEtags.length > 0); assert.ok(client.batchPartitions.every((value) => value === "tenant-a"));
    const listed = await store.listNodeCids(); assert.ok(listed.some((value) => Buffer.from(value).equals(cid)));
  });

  await suite.test("ETag CAS has one winner and failed strict batches roll back", async () => {
    const client = new MemoryClient(); const store = CosmosDbStore.fromClient(client, { partitionKey: "tenant" });
    const results = await Promise.all(Array.from({ length: 32 }, (_, index) => store.compareAndSwapRootManifest(bytes("race"), missingBytes(), presentBytes(bytes(`winner-${index}`)))));
    assert.equal(results.filter(({ applied }) => applied).length, 1);
    const conflict = await store.commitTransaction([upsertNode(bytes("rollback-node"), bytes("must-not-write"))], [{ name: bytes("race"), expected: missingBytes() }], [{ kind: "put", name: bytes("rollback-root"), manifest: bytes("must-not-publish") }]);
    assert.equal(conflict.applied, false); assert.equal((await store.getNode(bytes("rollback-node"))).present, false); assert.equal((await store.getRootManifest(bytes("rollback-root"))).present, false);
  });

  await suite.test("preflights 101 writes before the client and redacts provider errors", async () => {
    let calls = 0; const secret = "cosmos-account-secret";
    const client: CosmosItemClient = {
      async read(): Promise<never> { calls++; throw Object.assign(new Error(secret), { code: 503 }); }, async create(): Promise<void> { calls++; }, async upsert(): Promise<void> { calls++; throw Object.assign(new Error(secret), { code: 503 }); }, async replace(): Promise<void> { calls++; }, async delete(): Promise<void> { calls++; }, async queryFamily(): Promise<CosmosDocument[]> { calls++; return []; }, async executeBatch(): Promise<CosmosBatchResponse> { calls++; return { success: true, results: [] }; },
    };
    const store = CosmosDbStore.fromClient(client);
    await assert.rejects(store.commitTransaction(Array.from({ length: 101 }, (_, index) => upsertNode(Uint8Array.of(index), bytes("value"))), [], []), (error: unknown) => { assert.ok(error instanceof StoreError); assert.equal(error.code, "resource_exhausted"); return true; }); assert.equal(calls, 0);
    await assert.rejects(store.putNode(bytes("cid"), bytes("value")), (error: unknown) => { assert.ok(error instanceof StoreError); assert.equal(error.code, "unavailable"); assert.doesNotMatch(error.message, new RegExp(secret)); return true; });
  });

  await suite.test("propagates AbortSignal and close does not own the client", async () => {
    let closed = false;
    const client: CosmosItemClient & { close(): void } = {
      close(): void { closed = true; }, async read(): Promise<never> { throw Object.assign(new Error("missing"), { code: 404 }); }, async create(): Promise<void> {},
      async upsert(_partition, _document, signal): Promise<void> { await new Promise((_resolve, reject) => signal?.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })), { once: true })); },
      async replace(): Promise<void> {}, async delete(): Promise<void> {}, async queryFamily(): Promise<CosmosDocument[]> { return []; }, async executeBatch(): Promise<CosmosBatchResponse> { return { success: true, results: [] }; },
    };
    const store = CosmosDbStore.fromClient(client); const controller = new AbortController(); const write = store.putNode(bytes("cid"), bytes("value"), controller.signal); controller.abort("test");
    await assert.rejects(write, (error: unknown) => { assert.ok(error instanceof StoreError); assert.equal(error.code, "cancelled"); return true; }); await store.close(); assert.equal(closed, false);
  });
});

const endpoint = process.env.PROLLY_COSMOS_ENDPOINT; const key = process.env.PROLLY_COSMOS_KEY; const databaseId = process.env.PROLLY_COSMOS_DATABASE;
test("Cosmos DB live conformance", { skip: endpoint === undefined || key === undefined || databaseId === undefined }, async () => {
  const client = new CosmosClient({ endpoint: endpoint!, key: key! }); const database = client.database(databaseId!); const containerId = `prolly-node-${Date.now()}`;
  const { container } = await database.containers.createIfNotExists({ id: containerId, partitionKey: { paths: ["/kind"] } }); const store = new CosmosDbStore(container, { partitionKey: `node-${Date.now()}` });
  try { await store.validateContainer(); await runStoreConformance(() => store); } finally { await store.close(); await container.delete(); client.dispose(); }
});

interface RecordValue { document: CosmosDocument; etag: string; }
class MemoryClient implements CosmosItemClient {
  readonly #items = new Map<string, RecordValue>(); #etag = 1; validations = 0; readonly matchedEtags: string[] = []; readonly batchPartitions: string[] = [];
  async validatePartitionKey(): Promise<void> { this.validations++; }
  async read(partition: string, id: string): Promise<CosmosReadResult> { const value = this.#items.get(`${partition}\0${id}`); if (value === undefined) throw statusError(404); return { document: clone(value.document), etag: value.etag }; }
  async create(partition: string, document: CosmosDocument): Promise<void> { const key = `${partition}\0${document.id}`; if (this.#items.has(key)) throw statusError(409); this.#items.set(key, this.#record(document)); }
  async upsert(partition: string, document: CosmosDocument): Promise<void> { this.#items.set(`${partition}\0${document.id}`, this.#record(document)); }
  async replace(partition: string, id: string, document: CosmosDocument, etag: string): Promise<void> { const key = `${partition}\0${id}`; const current = this.#items.get(key); if (current === undefined) throw statusError(404); this.matchedEtags.push(etag); if (current.etag !== etag) throw statusError(412); this.#items.set(key, this.#record(document)); }
  async delete(partition: string, id: string, etag: string): Promise<void> { const key = `${partition}\0${id}`; const current = this.#items.get(key); if (current === undefined) throw statusError(404); if (etag !== "") { this.matchedEtags.push(etag); if (current.etag !== etag) throw statusError(412); } this.#items.delete(key); }
  async queryFamily(partition: string, family: CosmosDocument["family"]): Promise<CosmosDocument[]> { return [...this.#items.entries()].filter(([key, value]) => key.startsWith(`${partition}\0`) && value.document.family === family).map(([, value]) => clone(value.document)); }
  async executeBatch(partition: string, operations: readonly CosmosBatchOperation[]): Promise<CosmosBatchResponse> {
    this.batchPartitions.push(partition); const working = new Map([...this.#items].map(([key, value]) => [key, { document: clone(value.document), etag: value.etag }])); const results: { statusCode: number }[] = [];
    for (const operation of operations) { const statusCode = this.#apply(working, partition, operation); results.push({ statusCode }); if (statusCode < 200 || statusCode >= 300) { while (results.length < operations.length) results.push({ statusCode: 424 }); return { success: false, results }; } }
    this.#items.clear(); for (const [key, value] of working) this.#items.set(key, value); return { success: true, results };
  }
  document(partition: string, id: string): CosmosDocument { return clone(this.#items.get(`${partition}\0${id}`)!.document); }
  #record(document: CosmosDocument): RecordValue { return { document: clone(document), etag: String(this.#etag++) }; }
  #apply(items: Map<string, RecordValue>, partition: string, operation: CosmosBatchOperation): number { const id = operation.id ?? operation.document!.id; const key = `${partition}\0${id}`; const current = items.get(key); switch (operation.kind) { case "create": if (current !== undefined) return 409; items.set(key, this.#record(operation.document!)); return 201; case "upsert": items.set(key, this.#record(operation.document!)); return 200; case "replace": if (current === undefined) return 404; if (operation.etag !== undefined) { this.matchedEtags.push(operation.etag); if (current.etag !== operation.etag) return 412; } items.set(key, this.#record(operation.document!)); return 200; case "delete": if (current === undefined) return 404; if (operation.etag !== undefined) { this.matchedEtags.push(operation.etag); if (current.etag !== operation.etag) return 412; } items.delete(key); return 204; } }
}

function clone(value: CosmosDocument): CosmosDocument { return { id: value.id, kind: value.kind, family: value.family, key: value.key, value: value.value }; }
function statusError(code: number): Error { return Object.assign(new Error(`status ${code}`), { code }); }
function bytes(value: string): Uint8Array { return Uint8Array.from(Buffer.from(value)); }
function specialBytes(length: number): Buffer { const pattern = [0, 0x7f, 0x80, 0xff]; return Buffer.from(Array.from({ length }, (_, index) => pattern[index % 4]!)); }
