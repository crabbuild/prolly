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

async function filesystemSnapshot(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const blobStore = native.NativeProllyBlobStore.memory();
    let tree = engine.create();
    for (const [path, contents] of [
      ["README.md", "# Demo\n"],
      ["src/lib.rs", "pub fn answer() -> u8 { 42 }\n"],
    ] as const) {
      tree = engine.putLargeValue(blobStore, tree, bytes(`path/${path}`), bytes(contents), { inlineThreshold: "4" });
    }
    engine.publishNamedRoot(bytes("refs/heads/main"), tree);
    const loaded = engine.loadNamedRoot(bytes("refs/heads/main"))!;
    assert.equal(text(engine.getLargeValue(blobStore, loaded, bytes("path/README.md"))), "# Demo\n");

    console.log("filesystem_snapshot: published branch with blob-backed file contents");
  });
}

await filesystemSnapshot();
