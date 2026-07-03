import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { loadNative, type NativeModule, type NativeProllyEngine, type NativeTreeRecord } from "../src/native.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

const bytes = (value: string): Uint8Array => enc.encode(value);
const text = (value: Uint8Array | null | undefined): string | null =>
  value == null ? null : dec.decode(value);
const upsert = (key: string, value: string | Uint8Array) => ({
  kind: "upsert" as const,
  key: bytes(key),
  value: typeof value === "string" ? bytes(value) : value,
});
const del = (key: string) => ({ kind: "delete" as const, key: bytes(key) });
const rootHex = (tree: NativeTreeRecord): string => Buffer.from(tree.root ?? []).toString("hex");

async function withNative<T>(fn: (native: NativeModule) => T): Promise<T> {
  const native = await loadNative();
  return fn(native);
}

export async function batchBuild(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
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
  });
}

export async function localFirstState(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const main = bytes("app/demo/root/main");
    const base = engine.batch(engine.create(), [
      upsert("entity/user/001", "Ada"),
      upsert("index/user/name/Ada/001", new Uint8Array()),
    ]);
    engine.publishNamedRoot(main, base);

    const device = engine.batch(base, [
      upsert("entity/task/900", "offline draft"),
      upsert("index/task/status/open/900", new Uint8Array()),
    ]);
    const canonical = engine.put(base, bytes("entity/user/002"), bytes("Grace"));
    engine.publishNamedRoot(main, canonical);

    const current = engine.loadNamedRoot(main)!;
    const merged = engine.merge(base, current, device, "prefer_right");
    const update = engine.compareAndSwapNamedRoot(main, current, merged);

    assert.equal(update.applied, true);
    assert.equal(text(engine.get(merged, bytes("entity/user/002"))), "Grace");
    assert.equal(text(engine.get(merged, bytes("entity/task/900"))), "offline draft");

    console.log("local_first_state: merged offline branch into main");
  });
}

export async function resolver(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const base = engine.put(engine.create(), bytes("settings/theme"), bytes("light"));
    const leftDelete = engine.delete(base, bytes("settings/theme"));
    const rightUpdate = engine.put(base, bytes("settings/theme"), bytes("dark"));

    const updateWins = engine.merge(base, leftDelete, rightUpdate, "update_wins");
    const deleteWins = engine.merge(base, leftDelete, rightUpdate, "delete_wins");

    assert.equal(text(engine.get(updateWins, bytes("settings/theme"))), "dark");
    assert.equal(engine.get(deleteWins, bytes("settings/theme")), null);

    console.log("resolver: demonstrated update-wins and delete-wins policies");
  });
}

export async function crdtMerge(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const empty = engine.create();
    const baseValue = native.timestampedValueToBytes({ value: bytes("base"), timestamp: "1" });
    const leftValue = native.timestampedValueToBytes({ value: bytes("left"), timestamp: "2" });
    const rightValue = native.timestampedValueToBytes({ value: bytes("right"), timestamp: "3" });

    const base = engine.put(empty, bytes("counter/global"), baseValue);
    const left = engine.put(base, bytes("counter/global"), leftValue);
    const right = engine.put(base, bytes("counter/global"), rightValue);
    const merged = engine.crdtMerge(base, left, right, native.crdtConfigLww("update_wins"));
    const decoded = native.timestampedValueFromBytes(engine.get(merged, bytes("counter/global"))!);
    const mergedSet = native.multiValueSetMerge([bytes("candidate-b")], [bytes("candidate-a"), bytes("candidate-b")]);

    assert.equal(text(decoded.value), "right");
    assert.equal(decoded.timestamp, "3");
    assert.deepEqual(mergedSet.map(text), ["candidate-a", "candidate-b"]);

    console.log("crdt_merge: last-writer-wins and multi-value helpers passed");
  });
}

export async function conversationMemory(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const main = bytes("conversation/c42/root/main");
    const attemptName = bytes("conversation/c42/attempt/extractor/a1");

    const base = engine.put(engine.create(), bytes("conversation/c42/memory/m001"), bytes("user|likes terse summaries|0.91"));
    engine.publishNamedRoot(main, base);
    const attempt = engine.put(base, bytes("conversation/c42/memory/m002"), bytes("user|uses TypeScript|0.87"));
    engine.publishNamedRoot(attemptName, attempt);
    const canonical = engine.put(base, bytes("conversation/c42/memory/m003"), bytes("user|prefers local-first apps|0.82"));
    engine.publishNamedRoot(main, canonical);

    const merged = engine.merge(base, engine.loadNamedRoot(main)!, engine.loadNamedRoot(attemptName)!, "prefer_right");
    const update = engine.compareAndSwapNamedRoot(main, canonical, merged);

    assert.equal(update.applied, true);
    assert.equal(engine.range(merged, bytes("conversation/c42/memory/"), bytes("conversation/c42/memory0")).length, 3);

    console.log("conversation_memory: accepted extractor attempt into canonical memory");
  });
}

export async function agentEventLog(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const root = bytes("agent-log/run-7/root/events/current");
    const tree = engine.batch(engine.create(), [
      upsert("agent-log/run-7/event/1783036805000/0001", "user|Summarize the plan"),
      upsert("agent-log/run-7/event/1783036805000/0002", "tool-call|search-docs"),
      upsert("agent-log/run-7/event/1783036806000/0003", "assistant|Plan ready"),
    ]);
    engine.publishNamedRoot(root, tree);

    const page = engine.rangePage(engine.loadNamedRoot(root)!, null, null, "2");
    assert.equal(page.entries.length, 2);
    assert.ok(page.nextCursor);

    console.log(`agent_event_log: first page has ${page.entries.length} events`);
  });
}

export async function backgroundCompaction(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const events = engine.batch(
      engine.create(),
      Array.from({ length: 6 }, (_, idx) => upsert(`event/${(idx + 1).toString().padStart(4, "0")}`, `raw-event-${idx + 1}`)),
    );
    engine.publishNamedRoot(bytes("compaction/run/r7/root/events/0001"), events);

    const compacted = engine.batch(events, [
      del("event/0001"),
      del("event/0002"),
      del("event/0003"),
      del("event/0004"),
      upsert("event/0004-summary", "summary of events 1..4"),
    ]);
    engine.publishNamedRoot(bytes("compaction/run/r7/root/events/current"), compacted);

    const plan = engine.planStoreGc([events, compacted]);
    const remaining = engine.range(compacted, bytes("event/"), bytes("event0"));
    assert.equal(remaining.length, 3);
    assert.ok(Number(plan.reclaimableNodes) >= 0);

    console.log(`background_compaction: compacted log to ${remaining.length} records`);
  });
}

export async function deterministicRagSnapshot(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const indexRoot = bytes("rag/corpus/docs/root/index/current");
    const indexV1 = engine.batch(engine.create(), [
      upsert("rag/corpus/docs/chunk/doc-1/0001", "vector:v1|CrabDB stores deterministic roots"),
      upsert("rag/corpus/docs/chunk/doc-2/0001", "vector:v2|Prolly trees diff by key"),
    ]);
    engine.publishNamedRoot(indexRoot, indexV1);

    const answers = engine.put(engine.create(), bytes("rag/answer/q1"), bytes(`query:q1|snapshot:${rootHex(indexV1)}|citation:doc-1/0001`));
    engine.publishNamedRoot(bytes("rag/corpus/docs/root/answers"), answers);

    const indexV2 = engine.put(indexV1, bytes("rag/corpus/docs/chunk/doc-3/0001"), bytes("vector:v3|New content"));
    engine.publishNamedRoot(indexRoot, indexV2);

    assert.equal(engine.range(indexV1, bytes("rag/corpus/docs/chunk/"), bytes("rag/corpus/docs/chunk0")).length, 2);
    assert.equal(engine.range(engine.loadNamedRoot(indexRoot)!, bytes("rag/corpus/docs/chunk/"), bytes("rag/corpus/docs/chunk0")).length, 3);

    console.log("deterministic_rag_snapshot: replay kept original index root");
  });
}

export async function documentChunkIndex(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const blobStore = native.NativeProllyBlobStore.memory();
    const textKey = bytes("doc-index/corpus/text/parser-v1/doc-1/chunk-0001");
    const metadataKey = bytes("doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000");

    let tree = engine.create();
    tree = engine.putLargeValue(blobStore, tree, textKey, bytes("CrabDB stores large chunk text outside prolly leaves.".repeat(8)), { inlineThreshold: "32" });
    tree = engine.put(tree, metadataKey, bytes("doc-1|chunk-0001|0|384|vector-0001"));

    assert.equal(engine.range(tree, bytes("doc-index/corpus/parser/"), bytes("doc-index/corpus/parser0")).length, 1);
    assert.ok(text(engine.getLargeValue(blobStore, tree, textKey))?.startsWith("CrabDB stores"));

    console.log("document_chunk_index: metadata and blob-backed chunk text are linked");
  });
}

export async function vectorSidecar(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
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
  });
}

export async function provenanceValues(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const source = bytes("CrabDB language bindings design");
    const sourceCid = Buffer.from(native.cidFromBytes(source)).toString("hex");
    const chunkCid = Buffer.from(native.cidFromBytes(source.slice(0, 16))).toString("hex");
    const tree = engine.batch(engine.create(), [
      upsert("provenance/chunk/file-1/chunk-1", `source=${sourceCid}|chunk=${chunkCid}|parser=v1`),
      upsert("provenance/claim/file-1/claim-1", "CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1"),
    ]);

    const claims = engine.range(tree, bytes("provenance/claim/file-1/"), bytes("provenance/claim/file-10"));
    assert.equal(claims.length, 1);
    assert.match(text(claims[0].value)!, /Rust-backed/);

    console.log("provenance_values: claim links back to source and chunk CIDs");
  });
}

type Order = { tenant: string; id: string; status: string; cents: number };
const orderKey = (order: Order): Uint8Array => bytes(`orders/source/tenant/${order.tenant}/order/${order.id}`);
const encodeOrder = (order: Order): Uint8Array => bytes(`${order.tenant}|${order.id}|${order.status}|${order.cents}`);
const decodeOrder = (value: Uint8Array): Order => {
  const [tenant, id, status, cents] = text(value)!.split("|");
  return { tenant, id, status, cents: Number(cents) };
};
const viewKey = (tenant: string, status: string): Uint8Array => bytes(`orders/view/by-status/tenant/${tenant}/status/${status}`);

function buildRevenueView(engine: NativeProllyEngine, source: NativeTreeRecord): NativeTreeRecord {
  const totals = new Map<string, number>();
  for (const entry of engine.range(source, bytes("orders/source/"), bytes("orders/source0"))) {
    const order = decodeOrder(entry.value);
    const key = `${order.tenant}|${order.status}`;
    totals.set(key, (totals.get(key) ?? 0) + order.cents);
  }
  return engine.batch(
    engine.create(),
    [...totals.entries()].sort().map(([key, cents]) => {
      const [tenant, status] = key.split("|");
      return { kind: "upsert" as const, key: viewKey(tenant, status), value: bytes(String(cents)) };
    }),
  );
}

export async function materializedView(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const o1 = { tenant: "acme", id: "o1", status: "paid", cents: 1200 };
    const o2 = { tenant: "acme", id: "o2", status: "open", cents: 500 };
    const sourceV1 = engine.batch(engine.create(), [
      { kind: "upsert", key: orderKey(o1), value: encodeOrder(o1) },
      { kind: "upsert", key: orderKey(o2), value: encodeOrder(o2) },
    ]);
    const paidO2 = { ...o2, status: "paid" };
    const sourceV2 = engine.put(sourceV1, orderKey(paidO2), encodeOrder(paidO2));
    const viewV2 = buildRevenueView(engine, sourceV2);

    assert.equal(text(engine.get(viewV2, viewKey("acme", "paid"))), "1700");
    assert.equal(engine.get(viewV2, viewKey("acme", "open")), null);

    console.log(`materialized_view: folded ${engine.diff(sourceV1, sourceV2).length} source diff`);
  });
}

export async function filesystemSnapshot(): Promise<void> {
  await withNative((native) => {
    const engine = native.NativeProllyEngine.memory();
    const blobStore = native.NativeProllyBlobStore.memory();
    let tree = engine.create();
    for (const [path, contents] of [
      ["README.md", "# Demo\n"],
      ["src/lib.rs", "pub fn answer() -> u8 { 42 }\n"],
    ] as const) {
      tree = engine.putLargeValue(blobStore, tree, bytes(`path/${path}`), bytes(contents), { inlineThreshold: "4" });
    }
    engine.publishNamedRoot(bytes("refs/heads/main"), tree);
    const loaded = engine.loadNamedRoot(bytes("refs/heads/main"))!;
    assert.equal(text(engine.getLargeValue(blobStore, loaded, bytes("path/README.md"))), "# Demo\n");

    console.log("filesystem_snapshot: published branch with blob-backed file contents");
  });
}

export async function durableSqlite(): Promise<void> {
  await withNative((native) => {
    const dir = mkdtempSync(join(tmpdir(), "prolly-node-"));
    try {
      const engine = native.NativeProllyEngine.sqlite(join(dir, "app.prolly.sqlite"));
      const tree = engine.batch(engine.create(), [upsert("user/1", "Ada"), upsert("user/2", "Grace")]);
      engine.publishNamedRoot(bytes("users/main"), tree);
      const loaded = engine.loadNamedRoot(bytes("users/main"))!;
      assert.deepEqual(loaded.root, tree.root);
      assert.equal(text(engine.get(loaded, bytes("user/1"))), "Ada");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }

    console.log("durable_sqlite: named root survived through SQLite store API");
  });
}

export const scenarios = {
  batch_build: batchBuild,
  local_first_state: localFirstState,
  resolver,
  crdt_merge: crdtMerge,
  conversation_memory: conversationMemory,
  agent_event_log: agentEventLog,
  background_compaction: backgroundCompaction,
  deterministic_rag_snapshot: deterministicRagSnapshot,
  document_chunk_index: documentChunkIndex,
  vector_sidecar: vectorSidecar,
  provenance_values: provenanceValues,
  materialized_view: materializedView,
  filesystem_snapshot: filesystemSnapshot,
  durable_sqlite: durableSqlite,
};

export async function runAll(): Promise<void> {
  for (const scenario of Object.values(scenarios)) {
    await scenario();
  }
}
