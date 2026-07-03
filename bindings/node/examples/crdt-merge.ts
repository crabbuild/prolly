import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { loadNative, type NativeModule, type NativeProllyEngine, type NativeTreeRecord } from "../src/native.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

const bytes = (value: string): Uint8Array => enc.encode(value);
const text = (value: Uint8Array | null | undefined): string | null =>
  value == null ? null : dec.decode(value);
const upsert = (key: string, value: string | Uint8Array) => ({
  kind: "upsert" as const,
  key: bytes(key),
  value: typeof value === "string" ? bytes(value) : value,
});
const del = (key: string) => ({ kind: "delete" as const, key: bytes(key) });
const rootHex = (tree: NativeTreeRecord): string => Buffer.from(tree.root ?? []).toString("hex");

async function withNative<T>(fn: (native: NativeModule) => T): Promise<T> {
  const native = await loadNative();
  return fn(native);
}

async function crdtMerge(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const empty = engine.create();
    const baseValue = native.timestampedValueToBytes({ value: bytes("base"), timestamp: "1" });
    const leftValue = native.timestampedValueToBytes({ value: bytes("left"), timestamp: "2" });
    const rightValue = native.timestampedValueToBytes({ value: bytes("right"), timestamp: "3" });

    const base = engine.put(empty, bytes("counter/global"), baseValue);
    const left = engine.put(base, bytes("counter/global"), leftValue);
    const right = engine.put(base, bytes("counter/global"), rightValue);
    const merged = engine.crdtMerge(base, left, right, native.crdtConfigLww("update_wins"));
    const decoded = native.timestampedValueFromBytes(engine.get(merged, bytes("counter/global"))!);
    const mergedSet = native.multiValueSetMerge([bytes("candidate-b")], [bytes("candidate-a"), bytes("candidate-b")]);

    assert.equal(text(decoded.value), "right");
    assert.equal(decoded.timestamp, "3");
    assert.deepEqual(mergedSet.map(text), ["candidate-a", "candidate-b"]);

    console.log("crdt_merge: last-writer-wins and multi-value helpers passed");
  });
}

await crdtMerge();
