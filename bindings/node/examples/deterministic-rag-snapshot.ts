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

async function deterministicRagSnapshot(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const indexRoot = bytes("rag/corpus/docs/root/index/current");
    const indexV1 = engine.batch(engine.create(), [
      upsert("rag/corpus/docs/chunk/doc-1/0001", "vector:v1|CrabDB stores deterministic roots"),
      upsert("rag/corpus/docs/chunk/doc-2/0001", "vector:v2|Prolly trees diff by key"),
    ]);
    engine.publishNamedRoot(indexRoot, indexV1);

    const answers = engine.put(engine.create(), bytes("rag/answer/q1"), bytes(`query:q1|snapshot:${rootHex(indexV1)}|citation:doc-1/0001`));
    engine.publishNamedRoot(bytes("rag/corpus/docs/root/answers"), answers);

    const indexV2 = engine.put(indexV1, bytes("rag/corpus/docs/chunk/doc-3/0001"), bytes("vector:v3|New content"));
    engine.publishNamedRoot(indexRoot, indexV2);

    assert.equal(engine.range(indexV1, bytes("rag/corpus/docs/chunk/"), bytes("rag/corpus/docs/chunk0")).length, 2);
    assert.equal(engine.range(engine.loadNamedRoot(indexRoot)!, bytes("rag/corpus/docs/chunk/"), bytes("rag/corpus/docs/chunk0")).length, 3);

    console.log("deterministic_rag_snapshot: replay kept original index root");
  });
}

await deterministicRagSnapshot();
