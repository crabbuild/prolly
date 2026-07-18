import assert from "node:assert/strict";
import test from "node:test";

import { RemoteAsyncProllyEngine } from "../src/remote-async.ts";
import { FakeRemoteStore } from "./support/fake-remote-store.ts";
import { StoreError, type OptionalBytes } from "../src/remote-store.ts";

const bytes = (value: string): Uint8Array => Buffer.from(value);

test("remote async engine drives the Rust tree through a JavaScript promise store", async () => {
  const store = new FakeRemoteStore();
  const engine = await RemoteAsyncProllyEngine.open(store);
  try {
    const empty = engine.create();
    const tree = await engine.put(empty, bytes("key"), bytes("value"));
    assert.equal(Buffer.from((await engine.get(tree, bytes("key"))) ?? []).toString(), "value");
    assert.ok(store.nodes.size > 0);
  } finally {
    engine.close();
  }
});

test("remote async engine preserves ordered batches, ranges, and named-root CAS", async () => {
  const engine = await RemoteAsyncProllyEngine.open(new FakeRemoteStore());
  try {
    const empty = engine.create();
    const tree = await engine.batch(empty, [
      { kind: "upsert", key: bytes("b"), value: bytes("2") },
      { kind: "upsert", key: bytes("a"), value: bytes("1") },
    ]);
    assert.deepEqual(
      (await engine.getMany(tree, [bytes("b"), bytes("missing"), bytes("a")])).map((value) =>
        value == null ? null : Buffer.from(value).toString(),
      ),
      ["2", null, "1"],
    );
    assert.deepEqual(
      (await engine.range(tree, bytes("a"), bytes("c"))).map((entry) =>
        Buffer.from(entry.key).toString(),
      ),
      ["a", "b"],
    );

    await engine.publishNamedRoot(bytes("main"), tree);
    assert.equal(
      Buffer.from((await engine.loadNamedRoot(bytes("main")))?.root ?? []).byteLength,
      32,
    );
    const conflict = await engine.compareAndSwapNamedRoot(bytes("main"), empty, null);
    assert.equal(conflict.conflict, true);
    const applied = await engine.compareAndSwapNamedRoot(bytes("main"), tree, null);
    assert.equal(applied.applied, true);
  } finally {
    engine.close();
  }
});

test("remote async transactions publish atomically and reject use after completion", async () => {
  const engine = await RemoteAsyncProllyEngine.open(new FakeRemoteStore());
  try {
    const transaction = await engine.beginTransaction();
    const empty = await transaction.create();
    const tree = await transaction.put(empty, bytes("atomic"), bytes("value"));
    await transaction.publishNamedRoot(bytes("tx"), tree);
    const update = await transaction.commit();
    assert.equal(update.applied, true);
    assert.equal(
      Buffer.from((await engine.get((await engine.loadNamedRoot(bytes("tx")))!, bytes("atomic"))) ?? [])
        .toString(),
      "value",
    );
    await assert.rejects(transaction.get(tree, bytes("atomic")), /already committed or rolled back/);

    const rolledBack = await engine.beginTransaction();
    await rolledBack.rollback();
    await assert.rejects(rolledBack.create(), /already committed or rolled back/);
  } finally {
    engine.close();
  }
});

test("AbortSignal cancels the provider request and ignores late completion safely", async () => {
  class DelayedStore extends FakeRemoteStore {
    delayReads = false;
    cancelled = false;
    readonly started = Promise.withResolvers<void>();

    override async getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> {
      if (!this.delayReads) return super.getNode(cid, signal);
      return this.delay(() => super.getNode(cid), signal);
    }

    override async batchGetNodesOrdered(
      cids: readonly Uint8Array[],
      signal?: AbortSignal,
    ): Promise<OptionalBytes[]> {
      if (!this.delayReads) return super.batchGetNodesOrdered(cids, signal);
      return this.delay(() => super.batchGetNodesOrdered(cids), signal);
    }

    private delay<T>(operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
      this.started.resolve();
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          operation().then(resolve, reject);
        }, 100);
        signal?.addEventListener(
          "abort",
          () => {
            this.cancelled = true;
            clearTimeout(timer);
            reject(new StoreError("cancelled", "delayed read cancelled"));
          },
          { once: true },
        );
      });
    }
  }

  const store = new DelayedStore();
  const writer = await RemoteAsyncProllyEngine.open(store);
  const tree = await writer.put(writer.create(), bytes("key"), bytes("value"));
  writer.close();
  const engine = await RemoteAsyncProllyEngine.open(store);
  try {
    store.delayReads = true;
    const controller = new AbortController();
    const pending = engine.get(tree, bytes("key"), controller.signal);
    await store.started.promise;
    controller.abort("caller stopped");
    await assert.rejects(pending, /aborted/i);
    assert.equal(store.cancelled, true);
  } finally {
    engine.close();
  }
});
