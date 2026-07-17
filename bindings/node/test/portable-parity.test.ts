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
    const result = await proximity.read().search(exactSearch(new Float32Array([0.1, 0.1]), 1));
    assert.equal(Buffer.from(result.neighbors[0].key).toString(), "a");
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
