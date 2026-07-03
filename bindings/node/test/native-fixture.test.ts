import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";
import assert from "node:assert/strict";

import { loadNative, type NativeModule, type NativeProllyEngine } from "../src/native.ts";

const fixturePath = resolve(import.meta.dirname, "../../../conformance/prolly-fixtures.v1.json");

let native: NativeModule | null = null;
try {
  native = await loadNative();
} catch {
  native = null;
}

function fixtures(): any {
  return JSON.parse(readFileSync(fixturePath, "utf8"));
}

function fromHex(value: string): Buffer {
  return Buffer.from(value, "hex");
}

function toHex(value: Uint8Array | null | undefined): string | null {
  return value == null ? null : Buffer.from(value).toString("hex");
}

function configJson(config: any): string {
  return JSON.stringify(config);
}

test("native node fixtures decode, encode, and hash", { skip: native === null }, () => {
  assert.ok(native);
  for (const fixture of fixtures().node_fixtures) {
    const bytes = fromHex(fixture.bytes);
    assert.equal(toHex(native.nodeBytesRoundTrip(bytes)), fixture.bytes);
    assert.equal(toHex(native.nodeCidFromBytes(bytes)), fixture.cid);
    assert.equal(toHex(native.cidFromBytes(bytes)), fixture.cid);
  }
});

test("native boundary and key fixtures match Rust", { skip: native === null }, () => {
  assert.ok(native);
  const loaded = fixtures();

  for (const fixture of loaded.boundary_fixtures) {
    assert.equal(
      native.isBoundaryConfigJson(
        configJson(fixture.config),
        String(fixture.count),
        fromHex(fixture.key),
        fromHex(fixture.value),
      ),
      fixture.is_boundary,
    );
  }

  for (const fixture of loaded.key_fixtures.prefix_end) {
    const prefix = fromHex(fixture.prefix);
    assert.equal(toHex(native.prefixEnd(prefix)), fixture.end);
    const bounds = native.prefixRange(prefix);
    assert.equal(toHex(bounds.start), fixture.prefix);
    assert.equal(toHex(bounds.end), fixture.end);
  }

  for (const fixture of loaded.key_fixtures.numeric) {
    if (fixture.kind === "u64") assert.equal(toHex(native.u64Key(fixture.value)), fixture.encoded);
    if (fixture.kind === "u128") assert.equal(toHex(native.u128Key(fixture.value)), fixture.encoded);
    if (fixture.kind === "i64") assert.equal(toHex(native.i64Key(fixture.value)), fixture.encoded);
    if (fixture.kind === "i128") assert.equal(toHex(native.i128Key(fixture.value)), fixture.encoded);
    if (fixture.kind === "timestamp_millis") {
      assert.equal(toHex(native.timestampMillisKey(fixture.value)), fixture.encoded);
    }
  }

  for (const fixture of loaded.key_fixtures.segments) {
    const segments = fixture.segments.map((segment: string) => fromHex(segment));
    const encoded = Buffer.concat(segments.map((segment: Buffer) => native.encodeSegment(segment)));
    assert.equal(toHex(encoded), fixture.encoded);
    assert.equal(toHex(native.keyFromSegments(segments)), fixture.encoded);
    assert.equal(
      toHex(native.keyFromPrefixedSegments(native.keyFromSegments(segments.slice(0, 1)), segments.slice(1))),
      fixture.encoded,
    );
    assert.deepEqual(native.decodeSegments(fromHex(fixture.encoded)).map(toHex), fixture.decoded);
  }

  for (const fixture of loaded.key_fixtures.debug) {
    assert.equal(native.debugKey(fromHex(fixture.key)), fixture.debug);
  }
});

test("native tree and diff fixtures match Rust", { skip: native === null }, () => {
  assert.ok(native);
  const loaded = fixtures();

  for (const fixture of loaded.tree_fixtures) {
    const engine = native.NativeProllyEngine.memoryWithConfigJson(configJson(fixture.config));
    const tree = buildTree(engine, fixture.entries);
    assert.equal(toHex(tree.root), fixture.root);

    for (const lookup of fixture.lookups) {
      assert.equal(toHex(engine.get(tree, fromHex(lookup.key))), lookup.value);
    }

    for (const rangeFixture of fixture.ranges) {
      const entries = engine.range(
        tree,
        fromHex(rangeFixture.start),
        rangeFixture.end === null ? null : fromHex(rangeFixture.end),
      );
      assert.deepEqual(
        entries.map((entry) => ({ key: toHex(entry.key), value: toHex(entry.value) })),
        rangeFixture.entries,
      );
    }
  }

  const diffFixture = loaded.diff_fixtures[0];
  const diffEngine = native.NativeProllyEngine.memoryWithConfigJson(configJson(diffFixture.config));
  const base = buildTree(diffEngine, [
    { key: "61", value: "31" },
    { key: "62", value: "32" },
    { key: "63", value: "33" },
  ]);
  const other = buildTree(diffEngine, [
    { key: "61", value: "31" },
    { key: "62", value: "3232" },
    { key: "64", value: "34" },
  ]);
  assert.equal(toHex(base.root), diffFixture.base_root);
  assert.equal(toHex(other.root), diffFixture.other_root);
  assert.deepEqual(
    diffEngine.diff(base, other).map((diff) => ({
      kind: diff.kind,
      key: toHex(diff.key),
      value: toHex(diff.value),
      old: toHex(diff.old),
      new: toHex(diff.newValue),
    })),
    diffFixture.diffs,
  );
});

test("native codec fixtures round trip", { skip: native === null }, () => {
  assert.ok(native);
  const loaded = fixtures();

  for (const fixture of loaded.value_fixtures) {
    const bytes = fromHex(fixture.bytes);
    const version = String(fixture.version);
    const nextVersion = String(BigInt(fixture.version) + 1n);
    assert.equal(toHex(native.versionedValueBytesRoundTrip(bytes)), fixture.bytes);
    assert.equal(native.versionedValueBytesMatchesSchema(bytes, fixture.schema_name, version), true);
    assert.equal(native.versionedValueBytesMatchesSchema(bytes, fixture.schema_name, nextVersion), false);
    assert.doesNotThrow(() => native.versionedValueBytesRequireSchema(bytes, fixture.schema_name, version));
    assert.throws(() => native.versionedValueBytesRequireSchema(bytes, fixture.schema_name, nextVersion));
  }
  for (const fixture of loaded.blob_fixtures) {
    const bytes = fromHex(fixture.bytes);
    assert.equal(toHex(native.valueRefBytesRoundTrip(bytes)), fixture.bytes);
    assert.equal(native.valueRefFromStoredBytes(bytes).kind, fixture.kind);
    assert.equal(native.valueRefInlineRequiresEscape(bytes), true);
  }
  const rawInline = native.valueRefFromStoredBytes(Buffer.from("plain"));
  assert.equal(rawInline.kind, "inline");
  assert.equal(toHex(rawInline.value), "706c61696e");
  assert.equal(native.valueRefInlineRequiresEscape(Buffer.from("plain")), false);
  for (const fixture of loaded.manifest_fixtures) {
    assert.equal(toHex(native.rootManifestBytesRoundTrip(fromHex(fixture.bytes))), fixture.bytes);
  }
});

function buildTree(engine: NativeProllyEngine, entries: Array<{ key: string; value: string }>) {
  let tree = engine.create();
  for (const entry of entries) {
    tree = engine.put(tree, fromHex(entry.key), fromHex(entry.value));
  }
  return tree;
}
