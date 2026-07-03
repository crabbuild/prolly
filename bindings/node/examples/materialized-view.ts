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

async function materializedView(): Promise<void> {
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

await materializedView();
