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

async function localFirstState(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const main = bytes("app/demo/root/main");
    const base = engine.batch(engine.create(), [
      upsert("entity/user/001", "Ada"),
      upsert("index/user/name/Ada/001", new Uint8Array()),
    ]);
    engine.publishNamedRoot(main, base);

    const device = engine.batch(base, [
      upsert("entity/task/900", "offline draft"),
      upsert("index/task/status/open/900", new Uint8Array()),
    ]);
    const canonical = engine.put(base, bytes("entity/user/002"), bytes("Grace"));
    engine.publishNamedRoot(main, canonical);

    const current = engine.loadNamedRoot(main)!;
    const merged = engine.merge(base, current, device, "prefer_right");
    const update = engine.compareAndSwapNamedRoot(main, current, merged);

    assert.equal(update.applied, true);
    assert.equal(text(engine.get(merged, bytes("entity/user/002"))), "Grace");
    assert.equal(text(engine.get(merged, bytes("entity/task/900"))), "offline draft");

    console.log("local_first_state: merged offline branch into main");
  });
}

await localFirstState();
