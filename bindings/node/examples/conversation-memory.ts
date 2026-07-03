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

async function conversationMemory(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const main = bytes("conversation/c42/root/main");
    const attemptName = bytes("conversation/c42/attempt/extractor/a1");

    const base = engine.put(engine.create(), bytes("conversation/c42/memory/m001"), bytes("user|likes terse summaries|0.91"));
    engine.publishNamedRoot(main, base);
    const attempt = engine.put(base, bytes("conversation/c42/memory/m002"), bytes("user|uses TypeScript|0.87"));
    engine.publishNamedRoot(attemptName, attempt);
    const canonical = engine.put(base, bytes("conversation/c42/memory/m003"), bytes("user|prefers local-first apps|0.82"));
    engine.publishNamedRoot(main, canonical);

    const merged = engine.merge(base, engine.loadNamedRoot(main)!, engine.loadNamedRoot(attemptName)!, "prefer_right");
    const update = engine.compareAndSwapNamedRoot(main, canonical, merged);

    assert.equal(update.applied, true);
    assert.equal(engine.range(merged, bytes("conversation/c42/memory/"), bytes("conversation/c42/memory0")).length, 3);

    console.log("conversation_memory: accepted extractor attempt into canonical memory");
  });
}

await conversationMemory();
