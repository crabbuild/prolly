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

async function backgroundCompaction(): Promise<void> {
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

await backgroundCompaction();
