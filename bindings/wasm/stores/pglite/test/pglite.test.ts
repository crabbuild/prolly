import assert from "node:assert/strict";
import test from "node:test";
import "fake-indexeddb/auto";
import { PGlite } from "@electric-sql/pglite";
import { missingBytes, presentBytes } from "@trail/prolly-wasm/remote-store";
import { BrowserPGliteStore } from "../src/index.ts";

const bytes = (value: string): Uint8Array => new TextEncoder().encode(value);
const text = (value: Uint8Array): string => new TextDecoder().decode(value);

test("browser PGlite implements transactions, cancellation, ownership, and IndexedDB reopen", async () => {
  const name = `prolly-pglite-${Date.now()}-${Math.random()}`;
  const first = new PGlite(`idb://${name}`); await first.waitReady;
  const store = new BrowserPGliteStore(first); await store.initializeSchema();
  try {
    assert.equal((await store.descriptor()).provider, "pglite");
    await store.putNode(bytes("durable"), bytes("value"));
    const contenders = await Promise.all(Array.from({ length: 32 }, (_, index) =>
      store.compareAndSwapRootManifest(bytes("main"), missingBytes(), presentBytes(Uint8Array.of(index))),
    ));
    assert.equal(contenders.filter(({ applied }) => applied).length, 1);
    const conflict = await store.commitTransaction(
      [{ kind: "upsert", cid: bytes("rollback"), node: bytes("bad") }],
      [{ name: bytes("main"), expected: missingBytes() }], [],
    );
    assert.equal(conflict.applied, false);
    assert.equal((await store.getNode(bytes("rollback"))).present, false);
    const controller = new AbortController(); controller.abort();
    await assert.rejects(store.getNode(bytes("cancel"), controller.signal), (error: any) => error.code === "cancelled");
    await store.close();
    assert.equal((await first.query<{ value: number }>("SELECT 1 value")).rows[0]!.value, 1);
  } finally { await first.close(); }

  const second = new PGlite(`idb://${name}`); await second.waitReady;
  const reopened = new BrowserPGliteStore(second); await reopened.initializeSchema();
  try { assert.equal(text((await reopened.getNode(bytes("durable"))).value), "value"); }
  finally { await reopened.close(); await second.close(); indexedDB.deleteDatabase(`/pglite/${name}`); }
});
