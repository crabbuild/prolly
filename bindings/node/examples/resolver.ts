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

async function resolver(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const base = engine.put(engine.create(), bytes("settings/theme"), bytes("light"));
    const leftDelete = engine.delete(base, bytes("settings/theme"));
    const rightUpdate = engine.put(base, bytes("settings/theme"), bytes("dark"));

    const updateWins = engine.merge(base, leftDelete, rightUpdate, "update_wins");
    const deleteWins = engine.merge(base, leftDelete, rightUpdate, "delete_wins");

    assert.equal(text(engine.get(updateWins, bytes("settings/theme"))), "dark");
    assert.equal(engine.get(deleteWins, bytes("settings/theme")), null);

    console.log("resolver: demonstrated update-wins and delete-wins policies");
  });
}

await resolver();
