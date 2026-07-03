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

async function batchBuild(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const entries = Array.from({ length: 64 }, (_, idx) => {
      const value = 64 - idx;
      return { key: bytes(`event/${value.toString().padStart(4, "0")}`), value: bytes(`payload-${value}`) };
    });
    const tree = engine.buildFromEntries(entries);
    const rows = engine.range(tree, bytes("event/"), bytes("event0"));
    const stats = engine.collectStatsJson(tree);

    assert.equal(rows.length, 64);
    assert.equal(text(rows[0].key), "event/0001");
    assert.match(stats, /num_nodes/);

    console.log(`batch_build: imported ${rows.length} events`);
  });
}

await batchBuild();
