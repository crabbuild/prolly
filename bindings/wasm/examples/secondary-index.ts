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
const text = (value: Uint8Array | null | undefined): string => value == null ? "" : new TextDecoder().decode(value);

type User = [tenant: string, userId: string, status: string, displayName: string];

const engine = wasm.WasmProllyEngine.memory();

function encodeUser(...user: User): Uint8Array {
  return bytes(user.join("|"));
}

function decodeUser(value: Uint8Array): User {
  return text(value).split("|", 4) as User;
}

function userKey(user: User): Uint8Array {
  return bytes(`source/tenant/${user[0]}/user/${user[1]}`);
}

function statusPrefix(tenant: string, status: string): Uint8Array {
  return bytes(`index/user-by-status/tenant/${tenant}/status/${status}/`);
}

function statusKey(user: User): Uint8Array {
  return bytes(`index/user-by-status/tenant/${user[0]}/status/${user[2]}/${user[1]}`);
}

function putUser(tree: any, user: User): any {
  return engine.put(tree, userKey(user), encodeUser(...user));
}

function buildStatusIndex(source: any): any {
  let index = engine.create();
  for (const entry of engine.range(source, bytes("source/"), bytes("source0"))) {
    index = engine.put(index, statusKey(decodeUser(entry.value)), bytes("1"));
  }
  return index;
}

function usersByStatus(index: any, tenant: string, status: string): unknown[] {
  const start = statusPrefix(tenant, status);
  return engine.range(index, start, bytes(`${text(start)}~`));
}

const empty = engine.create();
let sourceV1 = putUser(empty, ["acme", "u001", "active", "Ada"]);
sourceV1 = putUser(sourceV1, ["acme", "u002", "invited", "Grace"]);
const indexV1 = buildStatusIndex(sourceV1);

let sourceV2 = putUser(sourceV1, ["acme", "u002", "active", "Grace"]);
sourceV2 = putUser(sourceV2, ["globex", "u003", "active", "Linus"]);

const sourceChanges = engine.diff(sourceV1, sourceV2);
assert.equal(sourceChanges.length, 2);
const rebuiltIndexV2 = buildStatusIndex(sourceV2);
assert.notDeepEqual(indexV1.root, rebuiltIndexV2.root);
assert.equal(usersByStatus(rebuiltIndexV2, "acme", "active").length, 2);
assert.equal(usersByStatus(rebuiltIndexV2, "globex", "active").length, 1);

console.log(`secondary_index: applied ${sourceChanges.length} source diffs`);
