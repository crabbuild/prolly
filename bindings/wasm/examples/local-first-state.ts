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

async function localFirstState(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const roots = new Map<string, unknown>();
  const base = engine.batch(engine.create(), [
    upsert("entity/user/001", "Ada"),
    upsert("index/user/name/Ada/001", new Uint8Array()),
  ]);
  roots.set("app/demo/root/main", base);

  const device = engine.batch(base, [
    upsert("entity/task/900", "offline draft"),
    upsert("index/task/status/open/900", new Uint8Array()),
  ]);
  const canonical = engine.put(base, bytes("entity/user/002"), bytes("Grace"));
  roots.set("app/demo/root/main", canonical);

  const merged = engine.merge(base, roots.get("app/demo/root/main"), device, "prefer_right");
  roots.set("app/demo/root/main", merged);

  assert.equal(text(engine.get(merged, bytes("entity/user/002"))), "Grace");
  assert.equal(text(engine.get(merged, bytes("entity/task/900"))), "offline draft");

  console.log("local_first_state: merged offline branch into main");
}

await localFirstState();
