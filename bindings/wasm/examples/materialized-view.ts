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

async function materializedView(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const sourceV1 = engine.batch(engine.create(), [
    upsert("orders/source/tenant/acme/order/o1", "acme|o1|paid|1200"),
    upsert("orders/source/tenant/acme/order/o2", "acme|o2|open|500"),
  ]);
  const sourceV2 = engine.put(sourceV1, bytes("orders/source/tenant/acme/order/o2"), bytes("acme|o2|paid|500"));
  const rows = engine.range(sourceV2, bytes("orders/source/"), bytes("orders/source0"));
  const paidTotal = rows
    .map((entry) => text(entry.value)!.split("|"))
    .filter((order) => order[2] === "paid")
    .reduce((sum, order) => sum + Number(order[3]), 0);
  const view = engine.put(engine.create(), bytes("orders/view/by-status/tenant/acme/status/paid"), bytes(String(paidTotal)));

  assert.equal(text(engine.get(view, bytes("orders/view/by-status/tenant/acme/status/paid"))), "1700");

  console.log(`materialized_view: folded ${engine.diff(sourceV1, sourceV2).length} source diff`);
}

await materializedView();
