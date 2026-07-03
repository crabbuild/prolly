import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { loadProllyWasm } from "../src/index.ts";

const here = dirname(fileURLToPath(import.meta.url));
const pkgWasm = join(here, "../pkg/prolly_wasm_bg.wasm");
if (!existsSync(pkgWasm)) throw new Error("Run `npm --prefix crates/prolly/bindings/wasm run build:wasm` first.");

const wasm = await loadProllyWasm("../pkg/prolly_wasm.js", readFileSync(pkgWasm));
const bytes = (value: string): Uint8Array => new TextEncoder().encode(value);
const text = (value: Uint8Array | null | undefined): string | null =>
  value == null ? null : new TextDecoder().decode(value);

const engine = wasm.WasmProllyEngine.memory();
let base = engine.create();
base = engine.put(base, bytes("doc:title"), bytes("Draft"));

const left = engine.put(base, bytes("doc:body"), bytes("Hello"));
const right = engine.put(base, bytes("doc:tags"), bytes("example"));

const leftChanges = engine.diff(base, left);
assert.equal(leftChanges.length, 1);

const merged = engine.merge(base, left, right, "prefer_right");
assert.equal(text(engine.get(merged, bytes("doc:body"))), "Hello");
assert.equal(text(engine.get(merged, bytes("doc:tags"))), "example");

console.log(`diff_merge: merged ${leftChanges.length} left-side change`);
