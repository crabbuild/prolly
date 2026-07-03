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

async function durableSqlite(): Promise<void> {
  await withNative((native) => {
    const dir = mkdtempSync(join(tmpdir(), "prolly-node-"));
    try {
      const engine = native.NativeProllyEngine.sqlite(join(dir, "app.prolly.sqlite"));
      const tree = engine.batch(engine.create(), [upsert("user/1", "Ada"), upsert("user/2", "Grace")]);
      engine.publishNamedRoot(bytes("users/main"), tree);
      const loaded = engine.loadNamedRoot(bytes("users/main"))!;
      assert.deepEqual(loaded.root, tree.root);
      assert.equal(text(engine.get(loaded, bytes("user/1"))), "Ada");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }

    console.log("durable_sqlite: named root survived through SQLite store API");
  });
}

await durableSqlite();
