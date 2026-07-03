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

async function documentChunkIndex(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const tree = engine.batch(engine.create(), [
    upsert("doc-index/corpus/text/parser-v1/doc-1/chunk-0001", "CrabDB stores chunk text in IndexedDB or OPFS sidecars."),
    upsert("doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000", "doc-1|chunk-0001|0|384|vector-0001"),
  ]);

  assert.equal(engine.range(tree, bytes("doc-index/corpus/parser/"), bytes("doc-index/corpus/parser0")).length, 1);
  assert.match(text(engine.get(tree, bytes("doc-index/corpus/text/parser-v1/doc-1/chunk-0001")))!, /CrabDB stores/);

  console.log("document_chunk_index: metadata and browser-side chunk text are linked");
}

await documentChunkIndex();
