import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { loadProllyWasm } from "../src/index.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();
const here = dirname(fileURLToPath(import.meta.url));
const pkgJs = join(here, "../pkg/prolly_wasm.js");
const pkgWasm = join(here, "../pkg/prolly_wasm_bg.wasm");

const bytes = (value: string): Uint8Array => enc.encode(value);
const text = (value: Uint8Array | null | undefined): string | null =>
  value == null ? null : dec.decode(value);
const upsert = (key: string, value: string | Uint8Array) => ({
  kind: "upsert",
  key: bytes(key),
  value: typeof value === "string" ? bytes(value) : value,
});
const del = (key: string) => ({ kind: "delete", key: bytes(key) });

async function loadWasm() {
  if (!existsSync(pkgJs) || !existsSync(pkgWasm)) {
    throw new Error("WASM package is not built. Run npm --prefix crates/prolly/bindings/wasm run build:wasm first.");
  }
  return loadProllyWasm("../pkg/prolly_wasm.js", readFileSync(pkgWasm));
}

async function provenanceValues(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const source = bytes("CrabDB language bindings design");
  const sourceCid = Buffer.from(wasm.cidFromBytes(source)).toString("hex");
  const chunkCid = Buffer.from(wasm.cidFromBytes(source.slice(0, 16))).toString("hex");
  const tree = engine.batch(engine.create(), [
    upsert("provenance/chunk/file-1/chunk-1", `source=${sourceCid}|chunk=${chunkCid}|parser=v1`),
    upsert("provenance/claim/file-1/claim-1", "CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1"),
  ]);
  const claims = engine.range(tree, bytes("provenance/claim/file-1/"), bytes("provenance/claim/file-10"));

  assert.equal(claims.length, 1);
  assert.match(text(claims[0].value)!, /Rust-backed/);

  console.log("provenance_values: claim links back to source and chunk CIDs");
}

await provenanceValues();
