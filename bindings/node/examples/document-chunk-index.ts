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

async function documentChunkIndex(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const blobStore = native.NativeProllyBlobStore.memory();
    const textKey = bytes("doc-index/corpus/text/parser-v1/doc-1/chunk-0001");
    const metadataKey = bytes("doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000");

    let tree = engine.create();
    tree = engine.putLargeValue(blobStore, tree, textKey, bytes("CrabDB stores large chunk text outside prolly leaves.".repeat(8)), { inlineThreshold: "32" });
    tree = engine.put(tree, metadataKey, bytes("doc-1|chunk-0001|0|384|vector-0001"));

    assert.equal(engine.range(tree, bytes("doc-index/corpus/parser/"), bytes("doc-index/corpus/parser0")).length, 1);
    assert.ok(text(engine.getLargeValue(blobStore, tree, textKey))?.startsWith("CrabDB stores"));

    console.log("document_chunk_index: metadata and blob-backed chunk text are linked");
  });
}

await documentChunkIndex();
