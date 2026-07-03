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

async function agentEventLog(): Promise<void> {
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

await agentEventLog();
