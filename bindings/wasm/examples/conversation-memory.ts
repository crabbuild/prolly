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

async function conversationMemory(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const roots = new Map<string, unknown>();
  const base = engine.put(engine.create(), bytes("conversation/c42/memory/m001"), bytes("user|likes terse summaries|0.91"));
  roots.set("conversation/c42/root/main", base);
  const attempt = engine.put(base, bytes("conversation/c42/memory/m002"), bytes("user|uses WASM|0.87"));
  roots.set("conversation/c42/attempt/extractor/a1", attempt);
  const canonical = engine.put(base, bytes("conversation/c42/memory/m003"), bytes("user|prefers local-first apps|0.82"));
  roots.set("conversation/c42/root/main", canonical);

  const merged = engine.merge(base, roots.get("conversation/c42/root/main"), roots.get("conversation/c42/attempt/extractor/a1"), "prefer_right");
  roots.set("conversation/c42/root/main", merged);
  assert.equal(engine.range(merged, bytes("conversation/c42/memory/"), bytes("conversation/c42/memory0")).length, 3);

  console.log("conversation_memory: accepted extractor attempt into canonical memory");
}

await conversationMemory();
