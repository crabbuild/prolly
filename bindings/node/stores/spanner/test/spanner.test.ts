import assert from "node:assert/strict";
import test from "node:test";

import { Spanner } from "@google-cloud/spanner";
import { StoreError, missingBytes, presentBytes, upsertNode } from "@trail/prolly-node/remote-store";
import { runStoreConformance } from "@trail/prolly-storetest";

import { SPANNER_DDL, SpannerStore, type SpannerItemClient, type SpannerMutation, type SpannerRootRecord, type SpannerTransaction } from "../src/index.ts";

test("Spanner SDK contract", async (suite) => {
  await suite.test("matches Rust DDL and raw-byte layout while satisfying conformance", async () => {
    assert.deepEqual(SPANNER_DDL, [
      "CREATE TABLE ProllyNodes (\n  Cid BYTES(32) NOT NULL,\n  Node BYTES(MAX) NOT NULL\n) PRIMARY KEY (Cid)",
      "CREATE TABLE ProllyHints (\n  Namespace BYTES(MAX) NOT NULL,\n  HintKey BYTES(MAX) NOT NULL,\n  Value BYTES(MAX) NOT NULL\n) PRIMARY KEY (Namespace, HintKey)",
      "CREATE TABLE ProllyRoots (\n  Name BYTES(MAX) NOT NULL,\n  Manifest BYTES(MAX) NOT NULL\n) PRIMARY KEY (Name)",
    ]);
    const client = new MemoryClient(); const store = SpannerStore.fromClient(client); await runStoreConformance(() => store);
    const cid = specialBytes(32); await store.putNode(cid, bytes("value")); assert.deepEqual(client.node(cid), bytes("value")); assert.deepEqual(client.lastMutation, { kind: "upsertNode", key: cid, value: bytes("value") });
    const descriptor = await store.descriptor(); assert.equal(descriptor.capabilities.nativeBatchReads, false); assert.equal(descriptor.capabilities.atomicBatchWrites, true); assert.equal(descriptor.limits.maxTransactionOperations, undefined);
  });

  await suite.test("serializable CAS has one winner and strict conflicts roll back", async () => {
    const client = new MemoryClient(); const store = SpannerStore.fromClient(client);
    const results = await Promise.all(Array.from({ length: 32 }, (_, index) => store.compareAndSwapRootManifest(bytes("race"), missingBytes(), presentBytes(bytes(`winner-${index}`)))));
    assert.equal(results.filter(({ applied }) => applied).length, 1);
    const conflict = await store.commitTransaction([upsertNode(bytes("rollback-node"), bytes("must-not-write"))], [{ name: bytes("race"), expected: missingBytes() }], [{ kind: "put", name: bytes("rollback-root"), manifest: bytes("must-not-publish") }]);
    assert.equal(conflict.applied, false); assert.equal((await store.getNode(bytes("rollback-node"))).present, false); assert.equal((await store.getRootManifest(bytes("rollback-root"))).present, false);
  });

  await suite.test("maps retryable errors without leaking provider details", async () => {
    const secret = "spanner-service-account-secret";
    const client = new MemoryClient(); client.failure = Object.assign(new Error(secret), { code: 14 }); const store = SpannerStore.fromClient(client);
    await assert.rejects(store.putNode(bytes("cid"), bytes("value")), (error: unknown) => { assert.ok(error instanceof StoreError); assert.equal(error.code, "unavailable"); assert.equal(error.retryable, true); assert.doesNotMatch(error.message, new RegExp(secret)); return true; });
  });

  await suite.test("propagates cancellation and preserves client ownership", async () => {
    let closed = false;
    const client: SpannerItemClient & { close(): void } = {
      close(): void { closed = true; }, async getNode() { return missingBytes(); }, async getHint() { return missingBytes(); }, async getRoot() { return missingBytes(); }, async listNodeCids() { return []; }, async listRoots() { return []; },
      async apply(_mutations, signal) { await new Promise((_resolve, reject) => signal?.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { code: 1 })), { once: true })); },
      async readWrite<T>(_callback: (transaction: SpannerTransaction) => Promise<T>): Promise<T> { throw new Error("unused"); },
    };
    const store = SpannerStore.fromClient(client); const controller = new AbortController(); const write = store.putNode(bytes("cid"), bytes("value"), controller.signal); controller.abort("test");
    await assert.rejects(write, (error: unknown) => { assert.ok(error instanceof StoreError); assert.equal(error.code, "cancelled"); return true; }); await store.close(); assert.equal(closed, false);
  });
});

test("Spanner emulator conformance", { skip: process.env.SPANNER_EMULATOR_HOST === undefined }, async () => {
  const projectId = "prolly-test"; const instanceId = "prolly-test"; const databaseId = `prolly_node_${Date.now()}`; const spanner = new Spanner({ projectId }); const instance = spanner.instance(instanceId);
  const [exists] = await instance.exists(); if (!exists) { const [, operation] = await spanner.createInstance(instanceId, { config: "emulator-config", nodes: 1 }); await operation.promise(); }
  const [database, operation] = await instance.createDatabase(databaseId, { schema: [...SPANNER_DDL] }); await operation.promise(); const store = new SpannerStore(database);
  try { await runStoreConformance(() => store); } finally { await store.close(); await database.close(); await database.delete(); spanner.close(); }
});

interface State { nodes: Map<string, Uint8Array>; hints: Map<string, Uint8Array>; roots: Map<string, Uint8Array>; }
class MemoryClient implements SpannerItemClient {
  #state: State = emptyState(); #tail: Promise<void> = Promise.resolve(); failure?: Error; lastMutation?: SpannerMutation;
  async getNode(key: Uint8Array): Promise<ReturnType<typeof missingBytes>> { return optional(this.#state.nodes.get(id(key))); }
  async getHint(namespace: Uint8Array, key: Uint8Array): Promise<ReturnType<typeof missingBytes>> { return optional(this.#state.hints.get(`${id(namespace)}:${id(key)}`)); }
  async getRoot(name: Uint8Array): Promise<ReturnType<typeof missingBytes>> { return optional(this.#state.roots.get(id(name))); }
  async listNodeCids(): Promise<Uint8Array[]> { return [...this.#state.nodes.keys()].map(fromId); }
  async listRoots(): Promise<SpannerRootRecord[]> { return [...this.#state.roots].map(([name, manifest]) => ({ name: fromId(name), manifest: own(manifest) })); }
  async apply(mutations: readonly SpannerMutation[]): Promise<void> { if (this.failure !== undefined) { const failure = this.failure; this.failure = undefined; throw failure; } const next = cloneState(this.#state); for (const mutation of mutations) apply(next, mutation); this.#state = next; this.lastMutation = mutations.at(-1); }
  async readWrite<T>(callback: (transaction: SpannerTransaction) => Promise<T>): Promise<T> {
    let release!: () => void; const previous = this.#tail; this.#tail = new Promise<void>((resolve) => { release = resolve; }); await previous;
    try { const next = cloneState(this.#state); const buffered: SpannerMutation[] = []; const result = await callback({ getRoot: async (name) => optional(next.roots.get(id(name))), buffer: (mutations) => buffered.push(...mutations.map(cloneMutation)) }); for (const mutation of buffered) apply(next, mutation); this.#state = next; return result; } finally { release(); }
  }
  node(key: Uint8Array): Uint8Array | undefined { const value = this.#state.nodes.get(id(key)); return value === undefined ? undefined : own(value); }
}

function emptyState(): State { return { nodes: new Map(), hints: new Map(), roots: new Map() }; }
function cloneState(state: State): State { return { nodes: new Map([...state.nodes].map(([key, value]) => [key, own(value)])), hints: new Map([...state.hints].map(([key, value]) => [key, own(value)])), roots: new Map([...state.roots].map(([key, value]) => [key, own(value)])) }; }
function apply(state: State, mutation: SpannerMutation): void { switch (mutation.kind) { case "upsertNode": state.nodes.set(id(mutation.key), own(mutation.value)); break; case "deleteNode": state.nodes.delete(id(mutation.key)); break; case "upsertHint": state.hints.set(`${id(mutation.namespace)}:${id(mutation.key)}`, own(mutation.value)); break; case "upsertRoot": state.roots.set(id(mutation.key), own(mutation.value)); break; case "deleteRoot": state.roots.delete(id(mutation.key)); break; } }
function cloneMutation(value: SpannerMutation): SpannerMutation { switch (value.kind) { case "upsertNode": return { kind: value.kind, key: own(value.key), value: own(value.value) }; case "deleteNode": return { kind: value.kind, key: own(value.key) }; case "upsertHint": return { kind: value.kind, namespace: own(value.namespace), key: own(value.key), value: own(value.value) }; case "upsertRoot": return { kind: value.kind, key: own(value.key), value: own(value.value) }; case "deleteRoot": return { kind: value.kind, key: own(value.key) }; } }
function optional(value: Uint8Array | undefined): ReturnType<typeof missingBytes> { return value === undefined ? missingBytes() : presentBytes(value); }
function id(value: Uint8Array): string { return Buffer.from(value).toString("base64"); }
function fromId(value: string): Uint8Array { return own(Buffer.from(value, "base64")); }
function own(value: Uint8Array): Uint8Array { return Uint8Array.from(value); }
function bytes(value: string): Uint8Array { return own(Buffer.from(value)); }
function specialBytes(length: number): Uint8Array { const pattern = [0, 0x7f, 0x80, 0xff]; return Uint8Array.from(Array.from({ length }, (_, index) => pattern[index % pattern.length]!)); }
