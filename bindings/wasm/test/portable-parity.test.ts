import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";
import assert from "node:assert/strict";

const api = await import("../src/index.ts");
const generatedModulePath = resolve(import.meta.dirname, "../pkg/prolly_wasm.js");
const generatedWasmPath = resolve(import.meta.dirname, "../pkg/prolly_wasm_bg.wasm");
const generatedPresent = existsSync(generatedModulePath) && existsSync(generatedWasmPath);
let wasm: any;
if (generatedPresent) {
  wasm = await import("../pkg/prolly_wasm.js");
  wasm.initSync({ module: readFileSync(generatedWasmPath) });
}

const bytes = (value: string): Uint8Array => new TextEncoder().encode(value);

test("WASM exposes portable maps and explicit native exclusions", { skip: !generatedPresent }, async () => {
  assert.equal(typeof api.Engine, "function");
  const engine = api.Engine.memory(wasm);
  assert.throws(() => api.Engine.file(wasm, "db.prolly"), /unsupported.*wasm/i);
  assert.throws(() => api.Engine.sqlite(wasm, "db.sqlite"), /unsupported.*wasm/i);

  const versioned = engine.versionedMap(bytes("users"));
  await versioned.initialize();
  await versioned.put(bytes("u1"), bytes("Ada"));
  assert.equal(Buffer.from(await versioned.get(bytes("u1")) ?? []).toString(), "Ada");

  const registry = engine.indexRegistry();
  registry.register({
    name: bytes("by_team"), generation: 1n, extractorId: "team-v1", projection: "all",
    extract: (_key: Uint8Array, source: Uint8Array) => [{ term: Uint8Array.from(source) }],
  });
  const indexed = engine.indexedMap(bytes("members"), registry);
  await indexed.put(bytes("u1"), bytes("red"));
  await indexed.ensureIndex(bytes("by_team"));
  const snapshot = await indexed.snapshot();
  const index = snapshot.index(bytes("by_team"));
  const rows = await index.records(bytes("red"));
  assert.equal(Buffer.from(rows[0].primaryKey).toString(), "u1");
  await indexed.put(bytes("u2"), bytes("red"));
  assert.equal((await index.records(bytes("red"))).length, 1, "snapshot remains pinned");
  const freshSnapshot = await indexed.snapshot();
  const freshIndex = freshSnapshot.index(bytes("by_team"));
  assert.equal((await freshIndex.records(bytes("red"))).length, 2);
  let escaped: Uint8Array | undefined;
  await index.exactView(bytes("red"), (row: any) => {
    escaped = row.primaryKey;
    assert.equal(Buffer.from(row.primaryKey).toString(), "u1");
    return false;
  });
  assert.throws(() => escaped?.byteLength, /expired/i);

  const proximity = await engine.buildProximity(2, [
    { key: bytes("a"), vector: new Float32Array([0, 0]), value: bytes("alpha") },
  ]);
  const session = proximity.read();
  assert.equal(session.contains(bytes("a")), true);
  assert.equal(Buffer.from(session.get(bytes("a"))?.value ?? []).toString(), "alpha");
  const result = await session.search({
    vector: new Float32Array([0.1, 0.1]), topK: 1, policy: "exact",
  });
  assert.equal(Buffer.from(result.neighbors[0].key).toString(), "a");

  session.close();
  proximity.close();
  freshIndex.close();
  freshSnapshot.close();
  index.close();
  snapshot.close();
  indexed.close();
  registry.close();
  versioned.close();
  engine.close();
});

test("WASM promise wrappers own inputs and honor AbortSignal", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const map = engine.versionedMap(bytes("async"));
  await map.initialize();
  const key = bytes("original-key");
  const value = bytes("original-value");
  const pending = map.put(key, value);
  key.fill(0);
  value.fill(0);
  await pending;
  assert.equal(Buffer.from(await map.get(bytes("original-key")) ?? []).toString(), "original-value");

  const controller = new AbortController();
  controller.abort();
  await assert.rejects(map.get(bytes("original-key"), controller.signal), /abort/i);
  map.close();
  engine.close();
});

test("WASM proofs, retained sessions, and maintenance stay in Rust", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const versioned = engine.versionedMap(bytes("proofs"));
  await versioned.initialize();
  await versioned.put(bytes("k"), bytes("v"));
  const snapshot = await versioned.snapshot();
  assert.ok(snapshot);
  assert.equal(Buffer.from(snapshot.proveKey(bytes("k")).verify().value ?? []).toString(), "v");
  assert.equal(snapshot.stats().itemCount, 1n);
  assert.ok(snapshot.exportSummary().itemCount > 0n);
  const session = snapshot.read();
  assert.equal(Buffer.from(session.get(bytes("k")) ?? []).toString(), "v");
  assert.ok(versioned.verifyCatalog().itemCount >= 2n);
  assert.ok((await versioned.backup()).byteLength > 0);
  assert.ok(versioned.planGc().itemCount > 0n);

  const registry = engine.indexRegistry();
  registry.register({ name: bytes("by_value"), generation: 1n, extractorId: "value-v1", projection: "all",
    extract: (_key: Uint8Array, value: Uint8Array) => [{ term: Uint8Array.from(value) }] });
  const indexed = engine.indexedMap(bytes("indexed-maintenance"), registry);
  const version: any = await indexed.put(bytes("k"), bytes("term"));
  await indexed.ensureIndex(bytes("by_value"));
  assert.equal(indexed.verifyIndex(bytes("by_value"), version.sourceVersion), true);
  assert.ok(indexed.metrics().buildAttempts >= 1n);
  assert.ok(indexed.exportCurrent().byteLength > 0);

  const proximity = await engine.buildProximity(2, [
    { key: bytes("p"), vector: new Float32Array([0, 0]), value: bytes("payload") },
  ]);
  assert.equal(Buffer.from(proximity.proveMembership(bytes("p")).verify(proximity.descriptor()).value ?? []).toString(), "payload");
  assert.equal(proximity.verify().recordCount, 1n);
  const searchProof = proximity.proveSearch(new Float32Array([0, 0]), 1);
  const verifiedSearch = searchProof.verify(proximity.descriptor());
  assert.equal(Buffer.from(verifiedSearch.result.neighbors[0].key).toString(), "p");
  assert.ok(verifiedSearch.replayedEvents > 0n);
  engine.close();
});
