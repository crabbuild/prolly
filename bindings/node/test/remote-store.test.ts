import assert from "node:assert/strict";
import test from "node:test";

import {
  STORE_PROTOCOL_MAJOR,
  deleteNode,
  deleteRoot,
  missingBytes,
  normalizeOptionalBytes,
  presentBytes,
  putRoot,
  throwIfAborted,
  upsertNode,
  validateStoreDescriptor,
  type StoreDescriptor,
} from "../src/remote-store.ts";

const descriptor = (): StoreDescriptor => ({
  protocolMajor: STORE_PROTOCOL_MAJOR,
  adapterName: "test-memory",
  provider: "memory",
  schemaVersion: 1,
  capabilities: {
    nativeBatchReads: true,
    atomicBatchWrites: true,
    nodeScan: true,
    hints: true,
    atomicNodesAndHint: true,
    rootScan: true,
    rootCompareAndSwap: true,
    transactions: true,
    readParallelism: 4,
  },
  limits: {},
});

test("remote store descriptors enforce protocol invariants", () => {
  assert.doesNotThrow(() => validateStoreDescriptor(descriptor()));
  assert.throws(
    () => validateStoreDescriptor({ ...descriptor(), protocolMajor: 2 }),
    /protocol major must be 1, got 2/,
  );
  assert.throws(
    () => validateStoreDescriptor({ ...descriptor(), adapterName: " " }),
    /adapter name must not be empty/,
  );
  assert.throws(
    () => validateStoreDescriptor({ ...descriptor(), provider: "" }),
    /provider must not be empty/,
  );
  assert.throws(
    () => validateStoreDescriptor({ ...descriptor(), schemaVersion: 0 }),
    /schema version must be at least 1/,
  );
  assert.throws(
    () => validateStoreDescriptor({
      ...descriptor(),
      capabilities: { ...descriptor().capabilities, readParallelism: 0 },
    }),
    /read parallelism must be at least 1/,
  );
  assert.throws(
    () => validateStoreDescriptor({
      ...descriptor(),
      capabilities: {
        ...descriptor().capabilities,
        hints: false,
        atomicNodesAndHint: true,
      },
    }),
    /atomic nodes and hint requires hints support/,
  );
  for (const limit of [
    "maxBatchReadItems",
    "maxBatchWriteItems",
    "maxTransactionOperations",
    "maxNodeBytes",
  ] as const) {
    assert.throws(
      () => validateStoreDescriptor({ ...descriptor(), limits: { [limit]: 0 } }),
      /must be at least 1 when present/,
    );
  }
});

test("optional bytes distinguish missing from a present empty value and own inputs", () => {
  assert.deepEqual(missingBytes(), { present: false, value: new Uint8Array() });

  const input = Uint8Array.of(1, 2, 3);
  const present = presentBytes(input);
  input.fill(9);

  assert.equal(present.present, true);
  assert.deepEqual(present.value, Uint8Array.of(1, 2, 3));
  assert.deepEqual(presentBytes(new Uint8Array()), {
    present: true,
    value: new Uint8Array(),
  });
});

test("protocol helpers own mutation bytes and reject malformed optional values", () => {
  const cid = Uint8Array.of(1);
  const node = Uint8Array.of(2);
  const name = Uint8Array.of(3);
  const manifest = Uint8Array.of(4);

  const upsert = upsertNode(cid, node);
  const deletion = deleteNode(cid);
  const rootPut = putRoot(name, manifest);
  const rootDelete = deleteRoot(name);
  cid.fill(9);
  node.fill(9);
  name.fill(9);
  manifest.fill(9);

  assert.deepEqual(upsert, {
    kind: "upsert",
    cid: Uint8Array.of(1),
    node: Uint8Array.of(2),
  });
  assert.deepEqual(deletion, { kind: "delete", cid: Uint8Array.of(1) });
  assert.deepEqual(rootPut, {
    kind: "put",
    name: Uint8Array.of(3),
    manifest: Uint8Array.of(4),
  });
  assert.deepEqual(rootDelete, { kind: "delete", name: Uint8Array.of(3) });
  assert.throws(
    () => normalizeOptionalBytes({ present: false, value: Uint8Array.of(1) }),
    /absent optional bytes must have an empty value/,
  );
});

test("abort guards return a structured cancellation error", () => {
  assert.doesNotThrow(() => throwIfAborted());
  const controller = new AbortController();
  controller.abort("caller stopped");
  assert.throws(
    () => throwIfAborted(controller.signal),
    (error: unknown) =>
      error instanceof Error &&
      error.name === "StoreError" &&
      "code" in error &&
      error.code === "cancelled" &&
      error.cause === "caller stopped",
  );
});

test("the core package exports the remote protocol without provider modules", async () => {
  const core = await import("../src/index.ts");
  assert.equal(core.STORE_PROTOCOL_MAJOR, 1);
  assert.equal(core.presentBytes(Uint8Array.of(7)).present, true);
});
