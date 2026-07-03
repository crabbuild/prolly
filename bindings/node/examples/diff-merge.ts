import assert from "node:assert/strict";

import { loadNative } from "../src/native.ts";

const native = await loadNative();
const bytes = (value: string): Buffer => Buffer.from(value);
const text = (value: Uint8Array | null | undefined): string | null =>
  value == null ? null : Buffer.from(value).toString();

const engine = native.NativeProllyEngine.memory();
let base = engine.create();
base = engine.put(base, bytes("doc:title"), bytes("Draft"));

const left = engine.put(base, bytes("doc:body"), bytes("Hello"));
const right = engine.put(base, bytes("doc:tags"), bytes("example"));

const leftChanges = engine.diff(base, left);
assert.equal(leftChanges.length, 1);
assert.equal(text(leftChanges[0].key), "doc:body");

const merged = engine.merge(base, left, right, "prefer_right");
assert.equal(text(engine.get(merged, bytes("doc:body"))), "Hello");
assert.equal(text(engine.get(merged, bytes("doc:tags"))), "example");

console.log(`diff_merge: merged ${leftChanges.length} left-side change`);
