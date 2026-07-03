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

async function vectorSidecar(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const sidecar = new Map([
    ["vec-1", [0.9, 0.1]],
    ["vec-2", [0.8, 0.2]],
    ["vec-stale", [1.0, 0.0]],
  ]);
  const tree = engine.batch(engine.create(), [
    upsert("vector-sidecar/corpus/docs/chunk/doc-1/0001", "vec-1|doc-1|parser-v1"),
    upsert("vector-sidecar/corpus/docs/chunk/doc-2/0001", "vec-2|doc-2|parser-v1"),
  ]);
  const allowed = new Set(
    engine
      .range(tree, bytes("vector-sidecar/corpus/docs/chunk/"), bytes("vector-sidecar/corpus/docs/chunk0"))
      .map((entry) => text(entry.value)!.split("|", 1)[0]),
  );
  const hits = [...sidecar.keys()].sort().filter((vectorId) => allowed.has(vectorId));
  assert.deepEqual(hits, ["vec-1", "vec-2"]);

  console.log(`vector_sidecar: filtered sidecar hits to ${hits.length} snapshot vectors`);
}

await vectorSidecar();
