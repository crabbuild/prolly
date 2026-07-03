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

async function resolver(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const base = engine.put(engine.create(), bytes("settings/theme"), bytes("light"));
  const leftDelete = engine.delete(base, bytes("settings/theme"));
  const rightUpdate = engine.put(base, bytes("settings/theme"), bytes("dark"));

  const updateWins = engine.merge(base, leftDelete, rightUpdate, "update_wins");
  const deleteWins = engine.merge(base, leftDelete, rightUpdate, "delete_wins");

  assert.equal(text(engine.get(updateWins, bytes("settings/theme"))), "dark");
  assert.equal(engine.get(deleteWins, bytes("settings/theme")), null);

  console.log("resolver: demonstrated update-wins and delete-wins policies");
}

await resolver();
