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

async function agentEventLog(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const tree = engine.batch(engine.create(), [
    upsert("agent-log/run-7/event/1783036805000/0001", "user|Summarize the plan"),
    upsert("agent-log/run-7/event/1783036805000/0002", "tool-call|search-docs"),
    upsert("agent-log/run-7/event/1783036806000/0003", "assistant|Plan ready"),
  ]);

  const page = engine.rangePage(tree, null, null, 2);
  assert.equal(page.entries.length, 2);
  assert.ok(page.nextCursor);

  console.log(`agent_event_log: first page has ${page.entries.length} events`);
}

await agentEventLog();
