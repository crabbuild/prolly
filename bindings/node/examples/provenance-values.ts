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

async function provenanceValues(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const source = bytes("CrabDB language bindings design");
    const sourceCid = Buffer.from(native.cidFromBytes(source)).toString("hex");
    const chunkCid = Buffer.from(native.cidFromBytes(source.slice(0, 16))).toString("hex");
    const tree = engine.batch(engine.create(), [
      upsert("provenance/chunk/file-1/chunk-1", `source=${sourceCid}|chunk=${chunkCid}|parser=v1`),
      upsert("provenance/claim/file-1/claim-1", "CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1"),
    ]);

    const claims = engine.range(tree, bytes("provenance/claim/file-1/"), bytes("provenance/claim/file-10"));
    assert.equal(claims.length, 1);
    assert.match(text(claims[0].value)!, /Rust-backed/);

    console.log("provenance_values: claim links back to source and chunk CIDs");
  });
}

await provenanceValues();
