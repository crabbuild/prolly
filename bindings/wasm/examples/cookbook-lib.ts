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
    throw new Error("WASM package is not built. Run `npm --prefix crates/prolly/bindings/wasm run build:wasm` first.");
  }
  return loadProllyWasm("../pkg/prolly_wasm.js", readFileSync(pkgWasm));
}

export async function batchBuild(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const entries = Array.from({ length: 64 }, (_, idx) => {
    const value = 64 - idx;
    return { key: bytes(`event/${value.toString().padStart(4, "0")}`), value: bytes(`payload-${value}`) };
  });
  const tree = engine.buildFromEntries(entries);
  const rows = engine.range(tree, bytes("event/"), bytes("event0"));
  const stats = engine.collectStatsJson(tree);

  assert.equal(rows.length, 64);
  assert.equal(text(rows[0].key), "event/0001");
  assert.match(stats, /num_nodes/);

  console.log(`batch_build: imported ${rows.length} events`);
}

export async function localFirstState(): Promise<void> {
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

export async function resolver(): Promise<void> {
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

export async function conversationMemory(): Promise<void> {
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

export async function agentEventLog(): Promise<void> {
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

export async function backgroundCompaction(): Promise<void> {
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

export async function deterministicRagSnapshot(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const roots = new Map<string, unknown>();
  const indexV1 = engine.batch(engine.create(), [
    upsert("rag/corpus/docs/chunk/doc-1/0001", "vector:v1|CrabDB stores deterministic roots"),
    upsert("rag/corpus/docs/chunk/doc-2/0001", "vector:v2|Prolly trees diff by key"),
  ]);
  roots.set("rag/corpus/docs/root/index/current", indexV1);
  const answers = engine.put(engine.create(), bytes("rag/answer/q1"), bytes("query:q1|snapshot:pinned|citation:doc-1/0001"));
  roots.set("rag/corpus/docs/root/answers", answers);
  const indexV2 = engine.put(indexV1, bytes("rag/corpus/docs/chunk/doc-3/0001"), bytes("vector:v3|New content"));
  roots.set("rag/corpus/docs/root/index/current", indexV2);

  assert.equal(engine.range(indexV1, bytes("rag/corpus/docs/chunk/"), bytes("rag/corpus/docs/chunk0")).length, 2);
  assert.equal(engine.range(roots.get("rag/corpus/docs/root/index/current"), bytes("rag/corpus/docs/chunk/"), bytes("rag/corpus/docs/chunk0")).length, 3);

  console.log("deterministic_rag_snapshot: replay kept original index root");
}

export async function documentChunkIndex(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const tree = engine.batch(engine.create(), [
    upsert("doc-index/corpus/text/parser-v1/doc-1/chunk-0001", "CrabDB stores chunk text in IndexedDB or OPFS sidecars."),
    upsert("doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000", "doc-1|chunk-0001|0|384|vector-0001"),
  ]);

  assert.equal(engine.range(tree, bytes("doc-index/corpus/parser/"), bytes("doc-index/corpus/parser0")).length, 1);
  assert.match(text(engine.get(tree, bytes("doc-index/corpus/text/parser-v1/doc-1/chunk-0001")))!, /CrabDB stores/);

  console.log("document_chunk_index: metadata and browser-side chunk text are linked");
}

export async function vectorSidecar(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const sidecar = new Map([
    ["vec-1", [0.9, 0.1]],
    ["vec-2", [0.8, 0.2]],
    ["vec-stale", [1.0, 0.0]],
  ]);
  const tree = engine.batch(engine.create(), [
    upsert("vector-sidecar/corpus/docs/chunk/doc-1/0001", "vec-1|doc-1|parser-v1"),
    upsert("vector-sidecar/corpus/docs/chunk/doc-2/0001", "vec-2|doc-2|parser-v1"),
  ]);
  const allowed = new Set(
    engine
      .range(tree, bytes("vector-sidecar/corpus/docs/chunk/"), bytes("vector-sidecar/corpus/docs/chunk0"))
      .map((entry) => text(entry.value)!.split("|", 1)[0]),
  );
  const hits = [...sidecar.keys()].sort().filter((vectorId) => allowed.has(vectorId));
  assert.deepEqual(hits, ["vec-1", "vec-2"]);

  console.log(`vector_sidecar: filtered sidecar hits to ${hits.length} snapshot vectors`);
}

export async function provenanceValues(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const source = bytes("CrabDB language bindings design");
  const sourceCid = Buffer.from(wasm.cidFromBytes(source)).toString("hex");
  const chunkCid = Buffer.from(wasm.cidFromBytes(source.slice(0, 16))).toString("hex");
  const tree = engine.batch(engine.create(), [
    upsert("provenance/chunk/file-1/chunk-1", `source=${sourceCid}|chunk=${chunkCid}|parser=v1`),
    upsert("provenance/claim/file-1/claim-1", "CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1"),
  ]);
  const claims = engine.range(tree, bytes("provenance/claim/file-1/"), bytes("provenance/claim/file-10"));

  assert.equal(claims.length, 1);
  assert.match(text(claims[0].value)!, /Rust-backed/);

  console.log("provenance_values: claim links back to source and chunk CIDs");
}

export async function materializedView(): Promise<void> {
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

export async function browserStorage(): Promise<void> {
  const wasm = await loadWasm();
  const engine = wasm.WasmProllyEngine.memory();
  const indexedDbRoots = new Map<string, unknown>();
  let tree = engine.create();
  tree = engine.put(tree, bytes("outbox/0001"), bytes("pending sync"));
  indexedDbRoots.set("browser/install-1/root/outbox", tree);

  const loaded = indexedDbRoots.get("browser/install-1/root/outbox");
  assert.equal(text(engine.get(loaded, bytes("outbox/0001"))), "pending sync");

  console.log("browser_storage: simulated IndexedDB named root and offline outbox");
}

export async function runAll(): Promise<void> {
  await batchBuild();
  await localFirstState();
  await resolver();
  await conversationMemory();
  await agentEventLog();
  await backgroundCompaction();
  await deterministicRagSnapshot();
  await documentChunkIndex();
  await vectorSidecar();
  await provenanceValues();
  await materializedView();
  await browserStorage();
}
