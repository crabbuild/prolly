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
