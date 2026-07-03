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
let tree = engine.create();
tree = engine.put(tree, bytes("user:001"), bytes("Ada"));
tree = engine.put(tree, bytes("user:002"), bytes("Grace"));
tree = engine.put(tree, bytes("user:003"), bytes("Linus"));

assert.equal(text(engine.get(tree, bytes("user:001"))), "Ada");
tree = engine.delete(tree, bytes("user:003"));
assert.equal(engine.get(tree, bytes("user:003")), null);

const users = engine.range(tree, bytes("user:"), bytes("user;"));
assert.equal(users.length, 2);

console.log(`basic_map: ${users.length} users in range`);
