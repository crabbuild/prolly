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

async function backgroundCompaction(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const events = engine.batch(
    engine.create(),
    Array.from({ length: 6 }, (_, idx) => upsert(`event/${(idx + 1).toString().padStart(4, "0")}`, `raw-event-${idx + 1}`)),
  );
  const compacted = engine.batch(events, [
    del("event/0001"),
    del("event/0002"),
    del("event/0003"),
    del("event/0004"),
    upsert("event/0004-summary", "summary of events 1..4"),
  ]);
  const remaining = engine.range(compacted, bytes("event/"), bytes("event0"));

  assert.equal(remaining.length, 3);

  console.log(`background_compaction: compacted log to ${remaining.length} records`);
}

await backgroundCompaction();
