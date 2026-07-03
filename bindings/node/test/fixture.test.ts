import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";
import assert from "node:assert/strict";

import {
  Config,
  MemoryStore,
  Prolly,
  ProllyNode,
  Tree,
  ValueRef,
  VersionedValue,
  changedSpan,
  changedSpanForPrefix,
  changedSpanFromKey,
  decodeSegments,
  encodingCbor,
  encodingCustom,
  encodingJson,
  encodingRaw,
  encodeSegment,
  fromHex,
  i128Key,
  i64Key,
  isBoundaryConfig,
  keyFromPrefixedSegments,
  keyFromSegments,
  largeValueConfig,
  parallelConfig,
  parallelConfigSequential,
  prefixEnd,
  prefixRange,
  toHex,
  treeConfig,
  u128Key,
  u64Key,
} from "../src/index.ts";

const fixturePath = resolve(import.meta.dirname, "../../../conformance/prolly-fixtures.v1.json");

function fixtures(): any {
  return JSON.parse(readFileSync(fixturePath, "utf8"));
}

test("node fixtures decode, encode, and hash", () => {
  for (const fixture of fixtures().node_fixtures) {
    const node = ProllyNode.fromBytes(fromHex(fixture.bytes));
    assert.equal(toHex(node.toBytes()), fixture.bytes);
    assert.equal(node.cid().hex(), fixture.cid);
  }
});

test("boundary and key fixtures match Rust", () => {
  const loaded = fixtures();
  assert.equal(encodingRaw().kind, "raw");
  assert.equal(encodingCbor().kind, "cbor");
  assert.equal(encodingJson().kind, "json");
  const customEncoding = encodingCustom("postcard");
  assert.deepEqual(customEncoding, { kind: "custom", customName: "postcard" });
  const constructedConfig = treeConfig({
    minChunkSize: 2,
    maxChunkSize: 64,
    chunkingFactor: 32,
    hashSeed: 7n,
    encoding: customEncoding,
    nodeCacheMaxNodes: 16,
    nodeCacheMaxBytes: 4096,
  });
  assert.equal(constructedConfig.encoding, "custom");
  assert.equal(constructedConfig.customEncodingName, "postcard");
  assert.throws(() =>
    treeConfig({
      minChunkSize: 2,
      maxChunkSize: 64,
      chunkingFactor: 32,
      hashSeed: 7n,
      encoding: { kind: "custom" },
    }),
  );
  assert.deepEqual(largeValueConfig(8), { inlineThreshold: 8 });
  assert.deepEqual(parallelConfig(2, 24), { maxThreads: 2, parallelismThreshold: 24 });
  assert.equal(parallelConfigSequential().maxThreads, 1);

  for (const fixture of loaded.boundary_fixtures) {
    assert.equal(
      isBoundaryConfig(
        Config.fromFixture(fixture.config),
        fixture.count,
        fromHex(fixture.key),
        fromHex(fixture.value),
      ),
      fixture.is_boundary,
    );
  }

  for (const fixture of loaded.key_fixtures.prefix_end) {
    const prefix = fromHex(fixture.prefix);
    const actual = prefixEnd(prefix);
    assert.equal(actual ? toHex(actual) : null, fixture.end);
    const bounds = prefixRange(prefix);
    assert.equal(toHex(bounds.start), fixture.prefix);
    assert.equal(bounds.end ? toHex(bounds.end) : null, fixture.end);
  }

  for (const fixture of loaded.key_fixtures.numeric) {
    const value = BigInt(fixture.value);
    if (fixture.kind === "u64") assert.equal(toHex(u64Key(value)), fixture.encoded);
    if (fixture.kind === "u128") assert.equal(toHex(u128Key(value)), fixture.encoded);
    if (fixture.kind === "i64") assert.equal(toHex(i64Key(value)), fixture.encoded);
    if (fixture.kind === "i128") assert.equal(toHex(i128Key(value)), fixture.encoded);
  }

  for (const fixture of loaded.key_fixtures.segments) {
    const segments = fixture.segments.map((segment: string) => fromHex(segment));
    const encoded = Buffer.concat(segments.map(encodeSegment));
    assert.equal(toHex(encoded), fixture.encoded);
    assert.equal(toHex(keyFromSegments(segments)), fixture.encoded);
    assert.equal(toHex(keyFromPrefixedSegments(keyFromSegments(segments.slice(0, 1)), segments.slice(1))), fixture.encoded);
    assert.deepEqual(decodeSegments(fromHex(fixture.encoded)).map(toHex), fixture.decoded);
  }

  assert.equal(toHex(changedSpan(fromHex("6b"), fromHex("6c")).end!), "6c");
  assert.equal(toHex(changedSpanFromKey(fromHex("6b")).end!), "6b00");
  assert.equal(toHex(changedSpanForPrefix(fromHex("6b")).end!), "6c");
});

test("tree fixture supports get, range, and diff", () => {
  const loaded = fixtures();
  const fixture = loaded.tree_fixtures[0];
  const store = MemoryStore.fromFixture(fixture);
  const tree = Tree.fromFixture(fixture);
  const prolly = new Prolly(store, tree.config);

  for (const lookup of fixture.lookups) {
    const actual = prolly.get(tree, fromHex(lookup.key));
    assert.equal(actual ? toHex(actual) : null, lookup.value);
  }

  for (const rangeFixture of fixture.ranges) {
    const actual = prolly.range(
      tree,
      fromHex(rangeFixture.start),
      rangeFixture.end === null ? undefined : fromHex(rangeFixture.end),
    );
    assert.deepEqual(
      actual.map(([key, value]) => ({ key: toHex(key), value: toHex(value) })),
      rangeFixture.entries,
    );
  }

  const diffFixture = loaded.diff_fixtures[0];
  const diffStore = MemoryStore.fromFixture(diffFixture);
  const diffProlly = new Prolly(diffStore, Config.fromFixture(diffFixture.config));
  const base = Tree.fromFixture({ root: diffFixture.base_root, config: diffFixture.config });
  const other = Tree.fromFixture({ root: diffFixture.other_root, config: diffFixture.config });
  assert.deepEqual(diffProlly.diff(base, other), diffFixture.diffs);
});

test("value and blob fixtures decode", () => {
  const loaded = fixtures();
  for (const fixture of loaded.value_fixtures) {
    const value = VersionedValue.fromBytes(fromHex(fixture.bytes));
    assert.equal(value.schema, fixture.schema_name);
    assert.equal(value.version, BigInt(fixture.version));
    assert.equal(value.encoding, fixture.encoding.kind);
    assert.equal(toHex(value.payload), fixture.payload);
    assert.equal(toHex(value.toBytes()), fixture.bytes);
  }

  for (const fixture of loaded.blob_fixtures) {
    const valueRef = ValueRef.fromBytes(fromHex(fixture.bytes));
    assert.equal(valueRef.kind, fixture.kind);
    assert.equal(toHex(valueRef.toBytes()), fixture.bytes);
    if (fixture.kind === "inline") {
      assert.equal(toHex(valueRef.value ?? new Uint8Array()), fixture.value);
    } else {
      assert.equal(valueRef.blobRef?.cid.hex(), fixture.cid);
      assert.equal(valueRef.blobRef?.length, BigInt(fixture.len));
    }
  }
});
