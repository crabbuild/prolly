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

// Browser WASM intentionally has no native file/blob store. This scenario uses
// a content-addressed sidecar namespace in the tree, which maps naturally to
// IndexedDB or OPFS in an application.
tree = engine.put(tree, bytes("blob/doc/body/v1"), new Uint8Array(64).fill(7));
tree = engine.put(tree, bytes("doc/body"), bytes("blob/doc/body/v1"));
assert.equal(text(engine.get(tree, bytes("doc/body"))), "blob/doc/body/v1");

let updated = engine.put(tree, bytes("blob/doc/body/v2"), new Uint8Array(64).fill(9));
updated = engine.put(updated, bytes("doc/body"), bytes("blob/doc/body/v2"));
const orphaned = engine.diff(tree, updated).filter((diff) => text(diff.key).startsWith("blob/"));
assert.equal(orphaned.length, 1);

console.log(`file_blob_store: tracked ${orphaned.length} browser-side orphan candidate`);
