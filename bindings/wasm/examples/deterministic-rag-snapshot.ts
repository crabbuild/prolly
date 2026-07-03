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

async function deterministicRagSnapshot(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const roots = new Map<string, unknown>();
  const indexV1 = engine.batch(engine.create(), [
    upsert("rag/corpus/docs/chunk/doc-1/0001", "vector:v1|CrabDB stores deterministic roots"),
    upsert("rag/corpus/docs/chunk/doc-2/0001", "vector:v2|Prolly trees diff by key"),
  ]);
  roots.set("rag/corpus/docs/root/index/current", indexV1);
  const answers = engine.put(engine.create(), bytes("rag/answer/q1"), bytes("query:q1|snapshot:pinned|citation:doc-1/0001"));
  roots.set("rag/corpus/docs/root/answers", answers);
  const indexV2 = engine.put(indexV1, bytes("rag/corpus/docs/chunk/doc-3/0001"), bytes("vector:v3|New content"));
  roots.set("rag/corpus/docs/root/index/current", indexV2);

  assert.equal(engine.range(indexV1, bytes("rag/corpus/docs/chunk/"), bytes("rag/corpus/docs/chunk0")).length, 2);
  assert.equal(engine.range(roots.get("rag/corpus/docs/root/index/current"), bytes("rag/corpus/docs/chunk/"), bytes("rag/corpus/docs/chunk0")).length, 3);

  console.log("deterministic_rag_snapshot: replay kept original index root");
}

await deterministicRagSnapshot();
