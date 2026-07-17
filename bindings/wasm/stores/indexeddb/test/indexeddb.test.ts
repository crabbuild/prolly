import assert from "node:assert/strict";
import test from "node:test";
import { indexedDB } from "fake-indexeddb";
import { missingBytes, presentBytes } from "@trail/prolly-wasm/remote-store";
import { IndexedDbStore, openIndexedDbDatabase } from "../src/index.ts";

const bytes = (value: string): Uint8Array => new TextEncoder().encode(value);
const text = (value: Uint8Array): string => new TextDecoder().decode(value);

test("IndexedDB implements atomic protocol behavior and persists across reopen", async () => {
  const name = `prolly-indexeddb-${Date.now()}-${Math.random()}`;
  const database = await openIndexedDbDatabase(name, { indexedDB });
  const store = new IndexedDbStore(database);
  try {
    assert.equal((await store.descriptor()).provider, "indexeddb");
    await store.putNode(new Uint8Array(32).fill(7), bytes("node"));
    await store.batchPutNodesWithHint(
      [1, 2, 3].map((value) => ({ cid: new Uint8Array(32).fill(value), node: bytes(`n${value}`) })),
      bytes("ns"), bytes("key"), bytes("hint"),
    );
    assert.equal(text((await store.getHint(bytes("ns"), bytes("key"))).value), "hint");

    const contenders = await Promise.all(Array.from({ length: 32 }, (_, index) =>
      store.compareAndSwapRootManifest(bytes("main"), missingBytes(), presentBytes(Uint8Array.of(index))),
    ));
    assert.equal(contenders.filter(({ applied }) => applied).length, 1);
    const conflict = await store.commitTransaction(
      [{ kind: "upsert", cid: bytes("rollback"), node: bytes("bad") }],
      [{ name: bytes("main"), expected: missingBytes() }],
      [{ kind: "put", name: bytes("other"), manifest: bytes("bad") }],
    );
    assert.equal(conflict.applied, false);
    assert.equal((await store.getNode(bytes("rollback"))).present, false);

    const controller = new AbortController(); controller.abort("cancelled");
    await assert.rejects(store.getNode(bytes("cancel"), controller.signal), (error: any) => error.code === "cancelled");
    await store.close();
    assert.equal((await new Promise<IDBRequest>((resolve) => resolve(database.transaction("nodes").objectStore("nodes").count()))).readyState, "pending");
  } finally {
    database.close();
  }

  const reopened = await openIndexedDbDatabase(name, { indexedDB });
  try {
    const store = new IndexedDbStore(reopened);
    assert.equal(text((await store.getNode(new Uint8Array(32).fill(7))).value), "node");
    await store.close();
  } finally {
    reopened.close();
    indexedDB.deleteDatabase(name);
  }
});
