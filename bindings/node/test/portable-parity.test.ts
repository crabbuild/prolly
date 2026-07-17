import test from "node:test";
import assert from "node:assert/strict";

import { Engine, exactSearch } from "../src/index.ts";

const bytes = (value: string): Buffer => Buffer.from(value);

test("portable versioned, indexed, and proximity maps stay native", async () => {
  const engine = await Engine.memory();
  try {
    const versioned = engine.versionedMap(bytes("users"));
    await versioned.initialize();
    await versioned.put(bytes("u1"), bytes("Ada"));
    assert.equal(Buffer.from(await versioned.get(bytes("u1")) ?? []).toString(), "Ada");

    const registry = engine.indexRegistry();
    registry.register({
      name: bytes("by_team"),
      generation: 1n,
      extractorId: "team-v1",
      projection: "all",
      extract: (_key, source) => [{ term: Buffer.from(source) }],
    });
    const indexed = engine.indexedMap(bytes("members"), registry);
    await indexed.put(bytes("u1"), bytes("red"));
    await indexed.put(bytes("u2"), bytes("red"));
    await indexed.ensureIndex(bytes("by_team"));
    const index = (await indexed.snapshot()).index(bytes("by_team"));
    const records = await index.records(bytes("red"));
    assert.equal(Buffer.from(records[0].primaryKey).toString(), "u1");
    assert.equal(Buffer.from(records[0].sourceValue).toString(), "red");
    const pageKeys: string[] = [];
    for await (const page of index.exactPages(bytes("red"), { pageSize: 1 })) {
      pageKeys.push(...page.map((row) => Buffer.from(row.primaryKey).toString()));
    }
    assert.deepEqual(pageKeys, ["u1", "u2"]);

    let escaped: Uint8Array | undefined;
    await index.exactView(bytes("red"), (row) => {
      escaped = row.primaryKey;
      assert.equal(Buffer.from(row.primaryKey).toString(), "u1");
      return false;
    });
    assert.throws(() => escaped?.byteLength, /expired/i);

    const proximity = await engine.buildProximity(2, [
      { key: bytes("a"), vector: new Float32Array([0, 0]), value: bytes("alpha") },
    ]);
    const proximitySession = proximity.read();
    assert.equal(proximitySession.contains(bytes("a")), true);
    assert.equal(Buffer.from(proximitySession.get(bytes("a"))?.value ?? []).toString(), "alpha");
    assert.ok(proximitySession.fastHandle() > 0n);
    const result = await proximitySession.search(exactSearch(new Float32Array([0.1, 0.1]), 1));
    assert.equal(Buffer.from(result.neighbors[0].key).toString(), "a");
    proximitySession.close();
  } finally {
    engine.close();
  }
});

test("portable promises honor AbortSignal and own inputs", async () => {
  const engine = await Engine.memory();
  try {
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
  } finally {
    engine.close();
  }
});

test("versioned maps expose identity and historical snapshot lifecycle", async () => {
  const engine = await Engine.memory();
  try {
    const map = engine.versionedMap(bytes("versioned-lifecycle"));
    assert.equal(Buffer.from(map.id()).toString(), "versioned-lifecycle");
    assert.equal(await map.isInitialized(), false);
    const initial = await map.initialize();
    assert.equal(await map.isInitialized(), true);
    assert.deepEqual(await map.headId(), initial.id);
    const first = await map.put(bytes("k"), bytes("v1"));
    await map.put(bytes("k"), bytes("v2"));
    assert.deepEqual((await map.head())?.id, (await map.headId()));
    assert.deepEqual((await map.version(first.id))?.id, first.id);
    assert.ok((await map.versions()).length >= 3);
    const historical = await map.snapshotAt(first.id);
    assert.ok(historical);
    assert.deepEqual(historical.id(), first.id);
    assert.deepEqual(historical.version().id, first.id);
    assert.equal(Buffer.from(await historical.get(bytes("k")) ?? []).toString(), "v1");
  } finally {
    engine.close();
  }
});

test("proofs, retained sessions, and maintenance stay native", async () => {
  const engine = await Engine.memory();
  try {
    const versioned = engine.versionedMap(bytes("proofs"));
    await versioned.initialize();
    await versioned.put(bytes("k"), bytes("v"));
    const snapshot = await versioned.snapshot();
    assert.ok(snapshot);
    const proof = snapshot.proveKey(bytes("k"));
    assert.equal(Buffer.from(proof.verify().value ?? []).toString(), "v");
    assert.equal(snapshot.stats().itemCount, 1n);
    assert.ok(snapshot.exportSummary().itemCount > 0n);
    const session = snapshot.read();
    assert.equal(Buffer.from(session.get(bytes("k")) ?? []).toString(), "v");
    assert.ok((await versioned.verifyCatalog()).itemCount >= 2n);
    assert.ok((await versioned.backup()).byteLength > 0);
    assert.ok((await versioned.planGc()).itemCount > 0n);

    const registry = engine.indexRegistry();
    registry.register({
      name: bytes("by_value"), generation: 1n, extractorId: "value-v1", projection: "all",
      extract: (_key, value) => [{ term: Buffer.from(value) }],
    });
    const indexed = engine.indexedMap(bytes("indexed-maintenance"), registry);
    const version = await indexed.put(bytes("k"), bytes("term"));
    await indexed.ensureIndex(bytes("by_value"));
    assert.equal(indexed.verifyIndex(bytes("by_value"), version.sourceVersion).valid, true);
    assert.ok(indexed.metrics().buildAttempts >= 1n);
    assert.ok(indexed.exportCurrent().byteLength > 0);
    assert.ok(indexed.keepLast(1n).retainedSourceVersions.length >= 1);

    const proximity = await engine.buildProximity(2, [
      { key: bytes("p"), vector: new Float32Array([0, 0]), value: bytes("payload") },
    ]);
    const membership = proximity.proveMembership(bytes("p")).verify(proximity.descriptor());
    assert.equal(Buffer.from(membership.value ?? []).toString(), "payload");
    assert.equal(proximity.verify().recordCount, 1n);
    assert.equal(proximity.count(), 1n);
    assert.equal(proximity.contains(bytes("p")), true);
    assert.equal(proximity.config().dimensions, 2);
    const structural = proximity.proveStructure().verify(proximity.descriptor());
    assert.equal(structural.summary.recordCount, 1n);
    const mutation = proximity.mutate([
      { key: bytes("q"), vector: new Float32Array([1, 1]), value: bytes("second") },
    ]);
    assert.equal(mutation.map.count(), 2n);
    assert.ok(mutation.stats.recordsRebuilt >= 1n);
    const searchProof = proximity.proveSearch(exactSearch(new Float32Array([0, 0]), 1));
    const verifiedSearch = searchProof.verify(proximity.descriptor());
    assert.equal(Buffer.from(verifiedSearch.result.neighbors[0]!.key).toString(), "p");
    assert.ok(verifiedSearch.replayedEvents > 0n);
    searchProof.close();
  } finally {
    engine.close();
  }
});

test("indexed maps expose batch CAS and historical snapshot lifecycle", async () => {
  const engine = await Engine.memory();
  try {
    const registry = engine.indexRegistry();
    registry.register({
      name: bytes("by_value"), generation: 1n, extractorId: "value-v1", projection: "all",
      extract: (_key, value) => [{ term: Buffer.from(value) }],
    });
    const indexed = engine.indexedMap(bytes("indexed-lifecycle"), registry);
    assert.equal(Buffer.from(indexed.id()).toString(), "indexed-lifecycle");

    const first = await indexed.apply([
      { kind: "upsert", key: bytes("u1"), value: bytes("red") },
      { kind: "upsert", key: bytes("u2"), value: bytes("red") },
    ]);
    await indexed.ensureIndex(bytes("by_value"));
    const firstSnapshot = await indexed.snapshot();
    const firstSnapshotId = firstSnapshot.id();
    assert.deepEqual(firstSnapshotId.sourceVersion, first.sourceVersion);

    const applied = await indexed.applyIf(first.sourceVersion, [
      { kind: "upsert", key: bytes("u3"), value: bytes("blue") },
    ]);
    assert.equal(applied.kind, "applied");
    assert.ok(applied.current);
    const conflict = await indexed.applyIf(first.sourceVersion, [
      { kind: "delete", key: bytes("u1") },
    ]);
    assert.equal(conflict.kind, "conflict");

    const historical = await indexed.snapshotAt(first.sourceVersion);
    assert.deepEqual(historical.id().sourceVersion, firstSnapshotId.sourceVersion);
    const reopened = await indexed.snapshotById(firstSnapshotId);
    assert.deepEqual(reopened.id(), firstSnapshotId);
  } finally {
    engine.close();
  }
});

test("indexed maintenance returns complete portable records", async () => {
  const engine = await Engine.memory();
  try {
    const registry = engine.indexRegistry();
    registry.register({
      name: bytes("by_value"), generation: 1n, extractorId: "value-v1", projection: "all",
      extract: (_key, value) => [{ term: Buffer.from(value) }],
    });
    const indexed = engine.indexedMap(bytes("indexed-records"), registry);
    const version = await indexed.put(bytes("u1"), bytes("red"));
    await indexed.ensureIndex(bytes("by_value"));

    const health = indexed.health();
    assert.equal(Buffer.from(health.sourceMapId).toString(), "indexed-records");
    assert.equal(health.activeIndexes.length, 1);
    assert.equal(health.activeIndexes[0]!.generation, 1n);
    const verification = indexed.verifyIndex(bytes("by_value"), version.sourceVersion);
    assert.equal(verification.valid, true);
    assert.equal(verification.canonical, true);
    assert.equal(indexed.verifyAll(version.sourceVersion).length, 1);
    assert.equal(indexed.repairIndex(bytes("by_value"), version.sourceVersion).valid, true);
    assert.ok(indexed.metrics().buildAttempts >= 1n);

    const bundle = indexed.exportCurrent();
    const next = await indexed.put(bytes("u2"), bytes("blue"));
    const imported = await indexed.importCurrent(bundle, next.sourceVersion);
    assert.deepEqual(imported.sourceVersion, version.sourceVersion);
    const retained = indexed.keepLast(1n);
    assert.ok(retained.retainedSourceVersions.length >= 1);
    await indexed.deactivateIndex(bytes("by_value"));
    assert.equal(indexed.health().activeIndexes.length, 0);
  } finally {
    engine.close();
  }
});

test("secondary indexes expose identity and every bounded page direction", async () => {
  const engine = await Engine.memory();
  try {
    const registry = engine.indexRegistry();
    registry.register({
      name: bytes("by_value"), generation: 1n, extractorId: "value-v1", projection: "all",
      extract: (_key, value) => [{ term: Buffer.from(value) }],
    });
    const indexed = engine.indexedMap(bytes("indexed-pages"), registry);
    await indexed.apply([
      { kind: "upsert", key: bytes("u1"), value: bytes("red") },
      { kind: "upsert", key: bytes("u2"), value: bytes("red") },
      { kind: "upsert", key: bytes("u3"), value: bytes("rose") },
    ]);
    await indexed.ensureIndex(bytes("by_value"));
    const index = (await indexed.snapshot()).index(bytes("by_value"));
    assert.equal(Buffer.from(index.name()).toString(), "by_value");

    const keys = async (pages: AsyncIterable<readonly { primaryKey: Uint8Array }[]>) => {
      const result: string[] = [];
      for await (const page of pages) {
        result.push(...page.map((row) => Buffer.from(row.primaryKey).toString()));
      }
      return result;
    };
    assert.deepEqual(await keys(index.exactPages(bytes("red"), { pageSize: 1 })), ["u1", "u2"]);
    assert.deepEqual(await keys(index.exactReversePages(bytes("red"), { pageSize: 1 })), ["u2", "u1"]);
    assert.deepEqual(await keys(index.prefixPages(bytes("r"), { pageSize: 1 })), ["u1", "u2", "u3"]);
    assert.deepEqual(await keys(index.prefixReversePages(bytes("r"), { pageSize: 1 })), ["u3", "u2", "u1"]);
    assert.deepEqual(await keys(index.rangePages(bytes("red"), bytes("s"), { pageSize: 1 })), ["u1", "u2", "u3"]);
    assert.deepEqual(await keys(index.rangeReversePages(bytes("red"), bytes("s"), { pageSize: 1 })), ["u3", "u2", "u1"]);
    assert.equal(Buffer.from((await index.exactPage(bytes("red"), undefined, 1n)).matches[0]!.primaryKey).toString(), "u1");
    assert.equal(Buffer.from((await index.exactReversePage(bytes("red"), undefined, 1n)).matches[0]!.primaryKey).toString(), "u2");
    assert.equal(Buffer.from((await index.prefixPage(bytes("r"), undefined, 1n)).matches[0]!.primaryKey).toString(), "u1");
    assert.equal(Buffer.from((await index.prefixReversePage(bytes("r"), undefined, 1n)).matches[0]!.primaryKey).toString(), "u3");
    assert.equal(Buffer.from((await index.rangePage(bytes("red"), bytes("s"), undefined, 1n)).matches[0]!.primaryKey).toString(), "u1");
    assert.equal(Buffer.from((await index.rangeReversePage(bytes("red"), bytes("s"), undefined, 1n)).matches[0]!.primaryKey).toString(), "u3");
  } finally {
    engine.close();
  }
});
