import assert from "node:assert/strict";
import test from "node:test";
import {
  GENERAL,
  MAINTENANCE,
  POINT_UPSERT,
  STORE_PROTOCOL_MAJOR,
  normalizePublicationOriginCode,
  publishNodesWithGeneralPath,
  validateStoreDescriptor,
  type NodeEntry,
  type NodeMutation,
  type RemoteStore,
} from "../src/remote-store.ts";

test("browser publication origins preserve known codes and normalize unknown codes", () => {
  for (let code = GENERAL; code <= MAINTENANCE; code++) {
    assert.equal(normalizePublicationOriginCode(code), code);
  }
  assert.equal(normalizePublicationOriginCode(0xffff_ffff), GENERAL);
  assert.equal(normalizePublicationOriginCode(Number.NaN), GENERAL);
  assert.throws(() => validateStoreDescriptor({
    protocolMajor: 1,
    adapterName: "legacy",
    provider: "memory",
    schemaVersion: 1,
    capabilities: { nativeBatchReads: false, atomicBatchWrites: true, nodeScan: false, hints: false, atomicNodesAndHint: false, rootScan: false, rootCompareAndSwap: false, transactions: false, readParallelism: 1 },
    limits: {},
  }));
  assert.equal(STORE_PROTOCOL_MAJOR, 2);
});

test("browser publication default dispatches exactly once and preserves hint bytes", async () => {
  const batches: NodeMutation[][] = [];
  const hinted: Array<{ nodes: readonly NodeEntry[]; namespace: Uint8Array; key: Uint8Array; value: Uint8Array }> = [];
  const store = {
    async batchNodes(nodes: readonly NodeMutation[]) { batches.push([...nodes]); },
    async batchPutNodesWithHint(nodes: readonly NodeEntry[], namespace: Uint8Array, key: Uint8Array, value: Uint8Array) {
      hinted.push({ nodes, namespace, key, value });
    },
  } as unknown as RemoteStore;
  const node = { cid: Uint8Array.of(1), node: Uint8Array.of(2) };
  const hint = { namespace: Uint8Array.of(3), key: Uint8Array.of(4), value: Uint8Array.of(5) };

  await publishNodesWithGeneralPath(store, { nodes: [node], hint, origin: POINT_UPSERT });

  assert.equal(batches.length, 0);
  assert.equal(hinted.length, 1);
  assert.deepEqual(hinted[0], { nodes: [node], ...hint });
});
