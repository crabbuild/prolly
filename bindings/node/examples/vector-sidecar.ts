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

async function vectorSidecar(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const sidecar = new Map([
      ["vec-1", [0.9, 0.1]],
      ["vec-2", [0.8, 0.2]],
      ["vec-stale", [1.0, 0.0]],
    ]);
    const tree = engine.batch(engine.create(), [
      upsert("vector-sidecar/corpus/docs/chunk/doc-1/0001", "vec-1|doc-1|parser-v1"),
      upsert("vector-sidecar/corpus/docs/chunk/doc-2/0001", "vec-2|doc-2|parser-v1"),
    ]);
    const allowed = new Set(
      engine
        .range(tree, bytes("vector-sidecar/corpus/docs/chunk/"), bytes("vector-sidecar/corpus/docs/chunk0"))
        .map((entry) => text(entry.value)!.split("|", 1)[0]),
    );
    const hits = [...sidecar.keys()].sort().filter((vectorId) => allowed.has(vectorId));
    assert.deepEqual(hits, ["vec-1", "vec-2"]);

    console.log(`vector_sidecar: filtered sidecar hits to ${hits.length} snapshot vectors`);
  });
}

await vectorSidecar();
