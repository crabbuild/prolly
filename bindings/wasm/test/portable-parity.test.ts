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

test("WASM retained search runtime reuses validated content", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  try {
    const proximity = await engine.buildProximity(2, Array.from({ length: 16 }, (_, index) => ({
      key: bytes(`runtime-vector-${index.toString().padStart(2, "0")}`),
      vector: new Float32Array([index, 0]),
      value: bytes(`runtime-value-${index.toString().padStart(2, "0")}`),
    })));
    const runtime = engine.proximitySearchRuntime();
    try {
      const request = { vector: new Float32Array([0, 0]), topK: 3, policy: "exact" as const };
      const cold = await proximity.searchWithRuntime(request, runtime);
      const warm = await proximity.searchWithRuntime(request, runtime);
      assert.ok(cold.stats.physicalBytesRead > 0n);
      assert.equal(warm.stats.physicalBytesRead, 0n);
      assert.ok(runtime.stats().physicalReads > 0n);
      runtime.clear();
      assert.ok((await proximity.searchWithRuntime(request, runtime)).stats.physicalBytesRead > 0n);
    } finally {
      runtime.close();
      proximity.close();
    }
  } finally {
    engine.close();
  }
});

test("WASM HNSW accelerator lifecycle is portable", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  try {
    const proximity = await engine.buildProximity(2, Array.from({ length: 16 }, (_, index) => ({
      key: bytes(`vector-${index.toString().padStart(2, "0")}`),
      vector: new Float32Array([index, 0]),
      value: bytes(`value-${index.toString().padStart(2, "0")}`),
    })));
    const defaults = api.defaultHnswConfig();
    const built = await proximity.buildHnsw({
      config: defaults,
      limits: api.defaultHnswBuildLimits(),
    });
    assert.equal(built.stats.records, 16n);
    const request = {
      vector: new Float32Array([0, 0]),
      topK: 3,
      policy: "fixed_budget" as const,
      backend: "hnsw" as const,
    };
    const index = built.index;
    assert.equal(index.isCanonical(), true);
    assert.deepEqual(index.sourceDescriptor(), proximity.descriptor());
    assert.deepEqual(index.config(), defaults);
    const result = await index.search(proximity, request);
    assert.equal(result.backend, "hnsw");
    assert.equal(Buffer.from(result.neighbors[0].key).toString(), "vector-00");
    const manifest = index.manifest();
    const proof = index.proveSearch(proximity, request);
    assert.equal(proof.verify(proximity.descriptor()).result.backend, "hnsw");
    proof.close();
    index.close();
    const loaded = proximity.loadHnsw(manifest);
    assert.deepEqual(loaded.manifest(), manifest);
    loaded.close();
    proximity.close();
  } finally {
    engine.close();
  }
});

test("WASM product quantizer lifecycle is portable and bounded", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  try {
    const proximity = await engine.buildProximity(4, Array.from({ length: 16 }, (_, index) => ({
      key: bytes(`pq-vector-${index.toString().padStart(2, "0")}`),
      vector: new Float32Array([index, index % 3, index % 5, index % 7]),
      value: bytes(`pq-value-${index.toString().padStart(2, "0")}`),
    })));
    const config = {
      subquantizers: 2,
      centroidsPerSubquantizer: 4,
      trainingIterations: 2,
      rerankMultiplier: 4,
      seed: 0xffff_ffff_ffff_ffffn,
      maxTrainingVectors: 16n,
    };
    const built = await proximity.buildPq({
      config,
      workerThreads: 1n,
      limits: api.defaultPqBuildLimits(),
    });
    assert.equal(built.stats.encodedVectors, 16n);
    assert.equal(built.stats.trainingVectors, 16n);
    const request = {
      vector: new Float32Array([0, 0, 0, 0]),
      topK: 3,
      policy: "fixed_budget" as const,
      backend: "product_quantized" as const,
    };
    const index = built.index;
    assert.deepEqual(index.sourceDescriptor(), proximity.descriptor());
    assert.deepEqual(index.config(), config);
    assert.ok(index.quality().meanSquaredError >= 0);
    const result = await index.search(proximity, request);
    assert.equal(result.backend, "product_quantized");
    assert.equal(Buffer.from(result.neighbors[0].key).toString(), "pq-vector-00");
    const manifest = index.manifest();
    const proof = index.proveSearch(proximity, request);
    assert.equal(proof.verify(proximity.descriptor()).result.backend, "product_quantized");
    proof.close();
    index.close();
    const loaded = proximity.loadPq(manifest);
    assert.deepEqual(loaded.manifest(), manifest);
    loaded.close();
    await assert.rejects(
      proximity.buildPq({ config, workerThreads: 2n }),
      /browser-safe WASM product quantization requires workerThreads = 1/,
    );
    proximity.close();
  } finally {
    engine.close();
  }
});

test("WASM composite and catalog lifecycle is portable and bounded", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  try {
    const baseMap = await engine.buildProximity(2, Array.from({ length: 16 }, (_, index) => ({
      key: bytes(`composite-vector-${index.toString().padStart(2, "0")}`),
      vector: new Float32Array([index, 0]),
      value: bytes(`composite-value-${index.toString().padStart(2, "0")}`),
    })));
    const base = (await baseMap.buildHnsw()).index;
    const current = baseMap.mutate([{
      key: bytes("composite-vector-00"), vector: new Float32Array([0.25, 0]), value: bytes("updated"),
    }]).map;
    const built = await current.buildCompositeHnsw(baseMap, base);
    assert.equal(built.reasons.length, 0);
    assert.equal(built.stats.vectorUpdatedRecords, 1n);
    const composite = built.accelerator!;
    assert.deepEqual(composite.currentSourceDescriptor(), current.descriptor());
    assert.deepEqual(composite.baseSourceDescriptor(), baseMap.descriptor());
    assert.equal(composite.baseKind(), "hnsw");
    assert.equal(composite.deltaCount(), 1n);
    assert.equal(composite.shadowCount(), 1n);
    const request = { vector: new Float32Array([0, 0]), topK: 3, policy: "fixed_budget" as const, backend: "composite" as const };
    assert.equal((await composite.search(current, request)).backend, "composite");
    const proof = composite.proveSearch(current, request);
    assert.equal(proof.verify(current.descriptor()).result.backend, "composite");
    proof.close();
    const manifest = composite.manifest();
    const catalog = current.buildAcceleratorCatalog({ composite });
    assert.equal(catalog.entries().length, 1);
    assert.equal((await catalog.search(current, request)).backend, "composite");
    const catalogManifest = catalog.manifest();
    catalog.close();
    const loadedCatalog = current.loadAcceleratorCatalog(catalogManifest);
    assert.deepEqual(loadedCatalog.manifest(), catalogManifest);
    loadedCatalog.close();
    composite.close();
    const loaded = current.loadComposite(manifest);
    assert.deepEqual(loaded.manifest(), manifest);
    loaded.close();
    const forced = { ...api.defaultCompositeAcceleratorConfig(), maxDeltaRecords: 0n };
    const rebuilt = await current.buildOrRebuildCompositeHnsw(baseMap, base, { config: forced });
    assert.equal(rebuilt.kind, "hnsw_rebuilt");
    assert.ok(rebuilt.reasons.length > 0);
    assert.deepEqual(rebuilt.hnsw!.sourceDescriptor(), current.descriptor());
    rebuilt.hnsw!.close();
    await assert.rejects(
      current.buildOrRebuildCompositeHnsw(baseMap, base, { config: forced, rebuild: { pqWorkerThreads: 2n } }),
      /browser-safe WASM composite PQ rebuild requires pqWorkerThreads = 1/,
    );
    current.close(); base.close(); baseMap.close();
  } finally { engine.close(); }
});

test("WASM rich proximity search preserves policy filter stats session and proof", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  try {
    const proximity = await engine.buildProximity(2, [
      { key: bytes("a"), vector: new Float32Array([0, 0]), value: bytes("alpha") },
      { key: bytes("ab"), vector: new Float32Array([1, 0]), value: bytes("alphabet") },
      { key: bytes("b"), vector: new Float32Array([0.1, 0]), value: bytes("beta") },
    ]);
    const request = {
      vector: new Float32Array([0, 0]),
      topK: 3,
      policy: "fixed_budget" as const,
      budget: {
        maxNodes: 1_000n,
        maxCommittedBytes: 1_000_000n,
        maxDistanceEvaluations: 1_000n,
        maxFrontierEntries: 1_000n,
      },
      filter: { kind: "prefix" as const, prefix: bytes("a") },
      kernel: "scalar_deterministic" as const,
      backend: "auto" as const,
    };
    const result = await proximity.search(request);
    assert.deepEqual(result.neighbors.map((neighbor) => Buffer.from(neighbor.key).toString()), ["a", "ab"]);
    assert.ok(result.stats.distanceEvaluations > 0n);
    assert.ok(result.planFormatVersion > 0);
    const scanned: string[] = [];
    assert.equal(proximity.scanRecords((record) => {
      scanned.push(Buffer.from(record.key).toString());
      return scanned.length < 2;
    }), 2n);
    assert.deepEqual(scanned, ["a", "ab"]);
    let expiredKey: Uint8Array | undefined;
    const viewed = proximity.withSearchView(new Float32Array([0, 0]), 2, (neighbors) => {
      expiredKey = neighbors[0]!.key;
      return neighbors.map((neighbor) => Buffer.from(neighbor.key).toString());
    });
    assert.deepEqual(viewed, ["a", "b"]);
    assert.throws(() => expiredKey![0], /expired/i);
    const session = proximity.read();
    assert.deepEqual(
      (await session.search(request)).neighbors.map((neighbor) => Buffer.from(neighbor.key).toString()),
      ["a", "ab"],
    );
    const retained: string[] = [];
    assert.equal(session.scanRecords((record) => {
      retained.push(Buffer.from(record.key).toString());
      return true;
    }), 3n);
    assert.deepEqual(retained, ["a", "ab", "b"]);
    session.close();
    const proof = proximity.proveSearch(request);
    assert.deepEqual(
      proof.verify(proximity.descriptor()).result.neighbors
        .map((neighbor) => Buffer.from(neighbor.key).toString()),
      ["a", "ab"],
    );
    proof.close();
  } finally {
    engine.close();
  }
});

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

test("WASM versioned snapshots expose ordered navigation and bounded pages", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const map = engine.versionedMap(bytes("versioned-ordered"));
  await map.initialize();
  await map.apply([
    { kind: "upsert", key: bytes("a"), value: bytes("one") },
    { kind: "upsert", key: bytes("ab"), value: bytes("two") },
    { kind: "upsert", key: bytes("b"), value: bytes("three") },
    { kind: "upsert", key: bytes("c"), value: bytes("four") },
  ]);
  const snapshot = await map.snapshot();
  assert.ok(snapshot);
  assert.equal(snapshot.containsKey(bytes("ab")), true);
  assert.deepEqual(snapshot.getMany([bytes("a"), bytes("missing")]).map((value: Uint8Array | undefined) => value == null ? undefined : Buffer.from(value).toString()), ["one", undefined]);
  assert.equal(Buffer.from(snapshot.firstEntry()!.key).toString(), "a");
  assert.equal(Buffer.from(snapshot.lastEntry()!.key).toString(), "c");
  assert.equal(Buffer.from(snapshot.lowerBound(bytes("aa"))!.key).toString(), "ab");
  assert.equal(Buffer.from(snapshot.upperBound(bytes("ab"))!.key).toString(), "b");
  assert.deepEqual(snapshot.prefix(bytes("a")).map((entry: any) => Buffer.from(entry.key).toString()), ["a", "ab"]);
  assert.deepEqual(snapshot.range(bytes("ab"), bytes("c")).map((entry: any) => Buffer.from(entry.key).toString()), ["ab", "b"]);

  const prefixPage = snapshot.prefixPage(bytes("a"), undefined, 1);
  assert.deepEqual(prefixPage.entries.map((entry: any) => Buffer.from(entry.key).toString()), ["a"]);
  assert.ok(prefixPage.nextCursor);

  const first = snapshot.rangePage(undefined, bytes("c"), 2);
  assert.deepEqual(first.entries.map((entry: any) => Buffer.from(entry.key).toString()), ["a", "ab"]);
  assert.ok(first.nextCursor);
  const second = snapshot.rangePage(first.nextCursor, bytes("c"), 2);
  assert.deepEqual(second.entries.map((entry: any) => Buffer.from(entry.key).toString()), ["b"]);
  const reverse = snapshot.reversePage(undefined, bytes("a"), 2);
  assert.deepEqual(reverse.entries.map((entry: any) => Buffer.from(entry.key).toString()), ["c", "b"]);
  const prefixed = snapshot.prefixReversePage(bytes("a"), undefined, 2);
  assert.deepEqual(prefixed.entries.map((entry: any) => Buffer.from(entry.key).toString()), ["ab", "a"]);
  map.close();
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

test("WASM versioned backups restore and retention returns complete version sets", { skip: !generatedPresent }, async () => {
  const sourceEngine = api.Engine.memory(wasm);
  const targetEngine = api.Engine.memory(wasm);
  const source = sourceEngine.versionedMap(bytes("versioned-backup"));
  await source.initialize();
  await source.put(bytes("k"), bytes("v1"));
  await source.put(bytes("k"), bytes("v2"));
  const backup = await source.backup();
  const target = targetEngine.versionedMap(bytes("versioned-backup"));
  const restored = await target.restoreBackup(backup);
  assert.deepEqual(restored.id, await source.headId());
  assert.equal(Buffer.from(await target.get(bytes("k")) ?? []).toString(), "v2");
  const pruned = await source.keepLast(1);
  assert.ok(pruned.retained.length >= 1);
  assert.ok(pruned.removed.length >= 1);
  sourceEngine.close();
  targetEngine.close();
});

test("WASM proofs, retained sessions, and maintenance stay in Rust", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const versioned = engine.versionedMap(bytes("proofs"));
  await versioned.initialize();
  await versioned.put(bytes("k"), bytes("v"));
  await versioned.put(bytes("ka"), bytes("v2"));
  const snapshot = await versioned.snapshot();
  assert.ok(snapshot);
  assert.equal(Buffer.from(snapshot.proveKey(bytes("k")).verify().value ?? []).toString(), "v");
  const multi = snapshot.proveKeys([bytes("k"), bytes("missing")]).verify();
  assert.equal(multi.valid, true);
  assert.deepEqual(multi.results.map((result: any) => result.exists), [true, false]);
  assert.deepEqual(snapshot.proveRange(bytes("k"), bytes("l")).verify().entries.map((entry: any) => Buffer.from(entry.key).toString()), ["k", "ka"]);
  assert.deepEqual(snapshot.provePrefix(bytes("k")).verify().entries.map((entry: any) => Buffer.from(entry.key).toString()), ["k", "ka"]);
  const provedPage = snapshot.proveRangePage(undefined, bytes("l"), 1);
  assert.equal(provedPage.verify().valid, true);
  assert.deepEqual(provedPage.page().entries.map((entry: any) => Buffer.from(entry.key).toString()), ["k"]);
  assert.equal(snapshot.stats().itemCount, 2n);
  assert.ok(snapshot.exportSummary().itemCount > 0n);
  const session = snapshot.read();
  assert.equal(Buffer.from(session.get(bytes("k")) ?? []).toString(), "v");
  let escaped: Uint8Array | undefined;
  const seen: string[] = [];
  const scan = session.scanRangeView(bytes("k"), bytes("l"), (entry: any) => {
    escaped ??= entry.key;
    seen.push(`${Buffer.from(entry.key)}=${Buffer.from(entry.value)}`);
    return true;
  });
  assert.deepEqual(scan, { visited: 2n, stopped: false });
  assert.deepEqual(seen, ["k=v", "ka=v2"]);
  assert.throws(() => Buffer.from(escaped!));
  assert.deepEqual(
    session.scanRangeView(bytes("k"), bytes("l"), () => false),
    { visited: 1n, stopped: true },
  );
  const memoryGuarded = session.scanRangeView(bytes("k"), bytes("l"), (entry: any) => {
    wasm.wasmMemory().grow(1);
    assert.throws(() => entry.key[0]);
    return false;
  });
  assert.deepEqual(memoryGuarded, { visited: 1n, stopped: true });
  assert.ok(versioned.verifyCatalog().versionCount >= 2n);
  assert.ok((await versioned.backup()).byteLength > 0);
  assert.ok(versioned.planGc().reachability.liveNodes > 0n);

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
  const searchProof = proximity.proveSearch({
    vector: new Float32Array([0, 0]), topK: 1, policy: "exact",
  });
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

test("WASM versioned comparisons pin versions and page diffs", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const map = engine.versionedMap(bytes("comparison"));
  const base = await map.initialize();
  const target = await map.put(bytes("k"), bytes("v"));
  const comparison = map.compare(base.id, target.id);
  assert.deepEqual(comparison.base().id, base.id);
  assert.deepEqual(comparison.target().id, target.id);
  assert.deepEqual(comparison.diff().map((diff: any) => Buffer.from(diff.key).toString()), ["k"]);
  assert.deepEqual(comparison.diffPage(undefined, undefined, 1).diffs.map((diff: any) => Buffer.from(diff.key).toString()), ["k"]);
  comparison.close();
  engine.close();
});

test("WASM versioned history navigation, diffs, and rollback stay native", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const map = engine.versionedMap(bytes("history-navigation"));
  await map.initialize();
  await map.put(bytes("a"), bytes("one"));
  await map.put(bytes("ab"), bytes("two"));
  const base = await map.put(bytes("b"), bytes("three"));
  const target = await map.put(bytes("a"), bytes("updated"));
  const keys = (rows: readonly { key: Uint8Array }[]) => rows.map((row) => Buffer.from(row.key).toString());

  assert.deepEqual(keys(await map.range(bytes("a"), bytes("c"))), ["a", "ab", "b"]);
  assert.deepEqual(keys(await map.prefix(bytes("a"))), ["a", "ab"]);
  assert.equal(Buffer.from((await map.rangeAt(base.id, bytes("a"), bytes("b")))[0].value).toString(), "one");
  assert.deepEqual(keys(await map.prefixAt(base.id, bytes("a"))), ["a", "ab"]);
  assert.deepEqual(keys((await map.rangePage(undefined, undefined, 2)).entries), ["a", "ab"]);
  assert.deepEqual(keys((await map.prefixPage(bytes("a"), undefined, 1)).entries), ["a"]);
  const historicalPage = await map.prefixPageAt(base.id, bytes("a"), undefined, 1);
  assert.deepEqual(keys(historicalPage.entries), ["a"]);
  assert.ok(historicalPage.nextCursor != null);
  assert.deepEqual((await map.diff(base.id, target.id)).map((row: any) => Buffer.from(row.key).toString()), ["a"]);
  assert.deepEqual((await map.changesSince(base.id)).map((row: any) => Buffer.from(row.key).toString()), ["a"]);

  const rolledBack = await map.rollbackTo(base.id);
  assert.deepEqual(await map.headId(), rolledBack.id);
  assert.equal(Buffer.from(await map.get(bytes("a"))).toString(), "one");
  assert.deepEqual(await map.changesSince(base.id), []);
  engine.close();
});

test("WASM versioned timestamped writes expose complete maintenance records", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const map = engine.versionedMap(bytes("maintenance-complete"));
  const first = await map.applyAtMillis([{ kind: "upsert", key: bytes("k"), value: bytes("one") }], 1_000n);
  const second = (await map.applyIfAtMillis(first.id, [{ kind: "upsert", key: bytes("k"), value: bytes("two") }], 2_000n)).current!;
  const third = await map.applyAtMillis([{ kind: "upsert", key: bytes("k"), value: bytes("three") }], 3_000n);

  assert.equal(first.createdAtMillis, 1_000n);
  assert.equal(second.createdAtMillis, 2_000n);
  assert.equal(map.retentionPolicy().kind, "prefix");
  const verification = map.verifyCatalog();
  assert.deepEqual(verification.head, third.id);
  assert.equal(verification.versionCount, 3n);
  const plan = map.planGc();
  assert.ok(plan.reachability.liveNodes > 0n);
  assert.ok(plan.candidateNodes >= plan.reclaimableNodes);

  const aged = map.keepForAt(3_000n, 1_500n);
  assert.ok(aged.retained.some((id: Uint8Array) => Buffer.from(id).equals(Buffer.from(second.id))));
  assert.ok(aged.removed.some((id: Uint8Array) => Buffer.from(id).equals(Buffer.from(first.id))));
  assert.ok(map.keepVersions([second.id]).retained.some((id: Uint8Array) => Buffer.from(id).equals(Buffer.from(third.id))));
  const pruned = map.pruneVersions(0n);
  assert.deepEqual(pruned.retained, [third.id]);
  assert.ok(pruned.removed.some((id: Uint8Array) => Buffer.from(id).equals(Buffer.from(second.id))));
  assert.ok(map.keepFor(10_000n).retained.length >= 1);
  assert.ok(map.sweepGc().deletedNodes >= 0n);
  engine.close();
});

test("WASM versioned bulk publication uses native performance paths", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const map = engine.versionedMap(bytes("bulk-publication"));
  const initialized = await map.initializeSorted([{ key: bytes("a"), value: bytes("one") }, { key: bytes("b"), value: bytes("two") }]);
  assert.equal(initialized.kind, "applied");
  await map.append([{ kind: "upsert", key: bytes("c"), value: bytes("three") }]);
  const parallel = await map.parallelApply([
    { kind: "upsert", key: bytes("b"), value: bytes("updated") },
    { kind: "upsert", key: bytes("d"), value: bytes("four") },
  ], { maxThreads: 1n, parallelismThreshold: 1n });
  assert.equal(parallel.stats.inputMutations, 2n);
  const rebuilt = await map.rebuildSortedIf(parallel.version.id, [{ key: bytes("x"), value: bytes("nine") }, { key: bytes("y"), value: bytes("ten") }]);
  assert.equal(rebuilt.kind, "applied");
  const iterRebuilt = await map.rebuildFromEntriesIf(rebuilt.current!.id, [{ key: bytes("q"), value: bytes("queue") }, { key: bytes("p"), value: bytes("priority") }]);
  assert.equal(iterRebuilt.kind, "applied");
  assert.equal(Buffer.from(await map.get(bytes("p")) ?? []).toString(), "priority");
  engine.close();
});

test("WASM versioned subscriptions resume and poll owned diffs", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const map = engine.versionedMap(bytes("subscription"));
  const initial = await map.initialize();
  const subscription = map.subscribe();
  assert.deepEqual(subscription.lastSeen(), initial.id);
  assert.equal(subscription.poll(), undefined);
  const current = await map.put(bytes("k"), bytes("v"));
  const event = subscription.poll();
  assert.deepEqual(event?.previous, initial.id);
  assert.deepEqual(event?.current.id, current.id);
  assert.deepEqual(event?.diffs.map((diff: any) => Buffer.from(diff.key).toString()), ["k"]);
  assert.deepEqual(subscription.lastSeen(), current.id);
  subscription.close();
  engine.close();
});

test("WASM multi-map transactions are atomic and read staged values", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const tx = engine.beginVersionedTransaction();
  await tx.put(bytes("a"), bytes("k"), bytes("one"));
  await tx.put(bytes("b"), bytes("k"), bytes("two"));
  assert.equal(Buffer.from((await tx.get(bytes("a"), bytes("k")))!).toString(), "one");
  const committed = await tx.commit();
  assert.equal(committed.applied, true);
  assert.equal(committed.versions.length, 2);
  assert.equal(Buffer.from((await engine.versionedMap(bytes("a")).get(bytes("k")))!).toString(), "one");
  assert.equal(Buffer.from((await engine.versionedMap(bytes("b")).get(bytes("k")))!).toString(), "two");
  const rolledBack = engine.beginVersionedTransaction();
  await rolledBack.put(bytes("a"), bytes("discard"), bytes("x"));
  await rolledBack.rollback();
  assert.equal(await engine.versionedMap(bytes("a")).get(bytes("discard")), undefined);
  engine.close();
});

test("WASM pinned merges page conflicts and CAS publish", { skip: !generatedPresent }, async () => {
  const engine = api.Engine.memory(wasm);
  const map = engine.versionedMap(bytes("merge"));
  const base = await map.initialize();
  const candidate = await map.put(bytes("k"), bytes("candidate"));
  await map.put(bytes("k"), bytes("head"));
  const merge = map.prepareMerge(base.id, candidate.id);
  assert.deepEqual(merge.base().id, base.id);
  assert.deepEqual(merge.candidate().id, candidate.id);
  assert.deepEqual(merge.conflictPage(undefined, 1).conflicts.map((row) => Buffer.from(row.key).toString()), ["k"]);
  assert.deepEqual(merge.publish("prefer_right").current?.id, candidate.id);
  assert.equal(Buffer.from((await map.get(bytes("k")))!).toString(), "candidate");
  merge.close(); engine.close();
});
