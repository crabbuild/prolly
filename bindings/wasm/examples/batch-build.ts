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

async function batchBuild(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const entries = Array.from({ length: 64 }, (_, idx) => {
    const value = 64 - idx;
    return { key: bytes(`event/${value.toString().padStart(4, "0")}`), value: bytes(`payload-${value}`) };
  });
  const tree = engine.buildFromEntries(entries);
  const rows = engine.range(tree, bytes("event/"), bytes("event0"));
  const stats = engine.collectStatsJson(tree);

  assert.equal(rows.length, 64);
  assert.equal(text(rows[0].key), "event/0001");
  assert.match(stats, /num_nodes/);

  console.log(`batch_build: imported ${rows.length} events`);
}

await batchBuild();
