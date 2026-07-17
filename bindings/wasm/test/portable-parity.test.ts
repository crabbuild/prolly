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
  assert.equal(proximity.count(), 1n);
  assert.equal(proximity.contains(bytes("a")), true);
  assert.equal(proximity.config().dimensions, 2);
  assert.equal(proximity.proveStructure().verify(proximity.descriptor()).summary.recordCount, 1n);
  const mutation = proximity.mutate([
    { key: bytes("b"), vector: new Float32Array([1, 1]), value: bytes("beta") },
  ]);
  assert.equal(mutation.map.count(), 2n);
  assert.ok(mutation.stats.recordsRebuilt >= 1n);

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

test("WASM versioned maps expose identity and historical snapshot lifecycle", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const map = engine.versionedMap(bytes("versioned-lifecycle"));
  assert.equal(Buffer.from(map.id()).toString(), "versioned-lifecycle");
  assert.equal(await map.isInitialized(), false);
  const initial = await map.initialize();
  assert.equal(await map.isInitialized(), true);
  assert.deepEqual(await map.headId(), initial.id);
  const first = await map.put(bytes("k"), bytes("v1"));
  await map.put(bytes("k"), bytes("v2"));
  assert.deepEqual((await map.head())?.id, await map.headId());
  assert.deepEqual((await map.version(first.id))?.id, first.id);
  assert.ok((await map.versions()).length >= 3);
  const historical = await map.snapshotAt(first.id);
  assert.ok(historical);
  assert.deepEqual(historical.id(), first.id);
  assert.deepEqual(historical.version().id, first.id);
  assert.equal(Buffer.from(historical.get(bytes("k")) ?? []).toString(), "v1");
  engine.close();
});

test("WASM versioned maps expose owned batch CAS and version-pinned point reads", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const map = engine.versionedMap(bytes("versioned-cas"));
  await map.initialize();
  const first = await map.apply([
    { kind: "upsert", key: bytes("a"), value: bytes("one") },
    { kind: "upsert", key: bytes("b"), value: bytes("two") },
  ]);
  assert.equal(await map.containsKey(bytes("a")), true);
  assert.deepEqual((await map.getMany([bytes("a"), bytes("missing")])).map((value: Uint8Array | undefined) => value == null ? undefined : Buffer.from(value).toString()), ["one", undefined]);
  const applied = await map.putIf(first.id, bytes("a"), bytes("updated"));
  assert.equal(applied.kind, "applied");
  const conflict = await map.deleteIf(first.id, bytes("b"));
  assert.equal(conflict.kind, "conflict");
  const values = await map.getManyAt(first.id, [bytes("a"), bytes("b")]);
  assert.deepEqual(values.map((value: Uint8Array | undefined) => Buffer.from(value ?? []).toString()), ["one", "two"]);
  assert.equal(Buffer.from(await map.getAt(first.id, bytes("a")) ?? []).toString(), "one");
  const batch = await map.applyIf(applied.current!.id, [{ kind: "delete", key: bytes("b") }]);
  assert.equal(batch.kind, "applied");
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
  assert.equal(indexed.verifyIndex(bytes("by_value"), version.sourceVersion).valid, true);
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

test("WASM indexed maps expose batch CAS and historical snapshots", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const registry = engine.indexRegistry();
  registry.register({ name: bytes("by_value"), generation: 1n, extractorId: "value-v1", projection: "all",
    extract: (_key: Uint8Array, value: Uint8Array) => [{ term: Uint8Array.from(value) }] });
  const indexed = engine.indexedMap(bytes("indexed-lifecycle"), registry);
  assert.equal(Buffer.from(indexed.id()).toString(), "indexed-lifecycle");
  const first = await indexed.apply([
    { kind: "upsert", key: bytes("u1"), value: bytes("red") },
    { kind: "upsert", key: bytes("u2"), value: bytes("red") },
  ]);
  await indexed.ensureIndex(bytes("by_value"));
  const firstSnapshot = await indexed.snapshot();
  const firstId = firstSnapshot.id();
  assert.deepEqual(firstId.sourceVersion, first.sourceVersion);
  const applied = await indexed.applyIf(first.sourceVersion, [
    { kind: "upsert", key: bytes("u3"), value: bytes("blue") },
  ]);
  assert.equal(applied.kind, "applied");
  const conflict = await indexed.applyIf(first.sourceVersion, [
    { kind: "delete", key: bytes("u1") },
  ]);
  assert.equal(conflict.kind, "conflict");
  const historical = await indexed.snapshotAt(first.sourceVersion);
  assert.deepEqual(historical.id().sourceVersion, firstId.sourceVersion);
  const reopened = await indexed.snapshotById(firstId);
  assert.deepEqual(reopened.id(), firstId);
  engine.close();
});

test("WASM indexed maintenance and every bounded page direction are complete", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const registry = engine.indexRegistry();
  registry.register({ name: bytes("by_value"), generation: 1n, extractorId: "value-v1", projection: "all",
    extract: (_key: Uint8Array, value: Uint8Array) => [{ term: Uint8Array.from(value) }] });
  const indexed = engine.indexedMap(bytes("indexed-records"), registry);
  const version = await indexed.apply([
    { kind: "upsert", key: bytes("u1"), value: bytes("red") },
    { kind: "upsert", key: bytes("u2"), value: bytes("red") },
    { kind: "upsert", key: bytes("u3"), value: bytes("rose") },
  ]);
  await indexed.ensureIndex(bytes("by_value"));
  assert.equal(Buffer.from(indexed.health().sourceMapId).toString(), "indexed-records");
  assert.equal(indexed.verifyIndex(bytes("by_value"), version.sourceVersion).valid, true);
  assert.equal(indexed.verifyAll(version.sourceVersion).length, 1);
  assert.equal(indexed.repairIndex(bytes("by_value"), version.sourceVersion).canonical, true);
  assert.ok(indexed.metrics().buildAttempts >= 1n);

  const index = (await indexed.snapshot()).index(bytes("by_value"));
  assert.equal(Buffer.from(index.name()).toString(), "by_value");
  assert.equal((await index.prefix(bytes("r"))).length, 3);
  assert.equal((await index.range(bytes("red"), bytes("s"))).length, 3);
  assert.equal(Buffer.from((await index.exactPage(bytes("red"), undefined, 1n)).matches[0].primaryKey).toString(), "u1");
  assert.equal(Buffer.from((await index.exactReversePage(bytes("red"), undefined, 1n)).matches[0].primaryKey).toString(), "u2");
  assert.equal(Buffer.from((await index.prefixPage(bytes("r"), undefined, 1n)).matches[0].primaryKey).toString(), "u1");
  assert.equal(Buffer.from((await index.prefixReversePage(bytes("r"), undefined, 1n)).matches[0].primaryKey).toString(), "u3");
  assert.equal(Buffer.from((await index.rangePage(bytes("red"), bytes("s"), undefined, 1n)).matches[0].primaryKey).toString(), "u1");
  assert.equal(Buffer.from((await index.rangeReversePage(bytes("red"), bytes("s"), undefined, 1n)).matches[0].primaryKey).toString(), "u3");

  const bundle = indexed.exportCurrent();
  const next = await indexed.put(bytes("u4"), bytes("blue"));
  assert.deepEqual((await indexed.importCurrent(bundle, next.sourceVersion)).sourceVersion, version.sourceVersion);
  assert.ok(indexed.keepLast(1n).retainedSourceVersions.length >= 1);
  await indexed.deactivateIndex(bytes("by_value"));
  assert.equal(indexed.health().activeIndexes.length, 0);
  engine.close();
});
