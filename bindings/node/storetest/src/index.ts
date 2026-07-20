import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import {
  StoreError,
  deleteRoot,
  missingBytes,
  normalizeOptionalBytes,
  presentBytes,
  upsertNode,
  validateStoreDescriptor,
  type OptionalBytes,
  type RemoteStore,
} from "../../src/remote-store.ts";

interface OptionalCase {
  readonly name: string;
  readonly present: boolean;
  readonly hex: string;
}

interface CasesFile {
  readonly protocol_major: number;
  readonly cases: readonly OptionalCase[];
}

export type RemoteStoreFactory = () => RemoteStore | Promise<RemoteStore>;

export async function runStoreConformance(factory: RemoteStoreFactory): Promise<void> {
  const store = await factory();
  const descriptor = validateStoreDescriptor(await store.descriptor());
  assert.equal(descriptor.protocolMajor, 2);

  const cases = await loadCases();
  assert.equal(cases.protocol_major, descriptor.protocolMajor);
  for (const fixture of cases.cases) {
    const cid = bytes(`fixture:${fixture.name}`);
    if (fixture.present) {
      await store.putNode(cid, Uint8Array.from(Buffer.from(fixture.hex, "hex")));
    } else {
      await store.deleteNode(cid);
    }
    assertOptional(await store.getNode(cid), fixture.present, fixture.hex);
  }

  const duplicate = bytes("ordered:duplicate");
  const missing = bytes("ordered:missing");
  await store.putNode(duplicate, bytes("value"));
  const ordered = await store.batchGetNodesOrdered([duplicate, missing, duplicate]);
  assert.equal(ordered.length, 3);
  assertOptional(ordered[0]!, true, Buffer.from("value").toString("hex"));
  assertOptional(ordered[1]!, false, "");
  assertOptional(ordered[2]!, true, Buffer.from("value").toString("hex"));

  const ownedCid = bytes("owned:cid");
  const ownedValue = bytes("owned:value");
  await store.batchNodes([upsertNode(ownedCid, ownedValue)]);
  ownedCid.fill(0);
  ownedValue.fill(0);
  assertOptional(await store.getNode(bytes("owned:cid")), true, Buffer.from("owned:value").toString("hex"));

  if (descriptor.capabilities.nodeScan) {
    const cids = await store.listNodeCids();
    assert.deepEqual(cids.map(hex), [...cids].sort(compareBytes).map(hex));
  }

  if (descriptor.capabilities.hints) {
    const namespace = bytes("hint:namespace");
    const key = bytes("hint:key");
    await store.putHint(namespace, key, bytes("hint:value"));
    assertOptional(await store.getHint(namespace, key), true, Buffer.from("hint:value").toString("hex"));
    await store.batchPutNodesWithHint(
      [{ cid: bytes("hint:node"), node: bytes("hint:node-value") }],
      namespace,
      bytes("hint:batch-key"),
      bytes("hint:batch-value"),
    );
    assertOptional(await store.getNode(bytes("hint:node")), true, Buffer.from("hint:node-value").toString("hex"));
  }

  const rootName = bytes("root:cas");
  if (descriptor.capabilities.rootCompareAndSwap) {
    const created = await store.compareAndSwapRootManifest(
      rootName,
      missingBytes(),
      presentBytes(bytes("manifest:one")),
    );
    assert.equal(created.applied, true);
    const conflict = await store.compareAndSwapRootManifest(
      rootName,
      missingBytes(),
      presentBytes(bytes("manifest:two")),
    );
    assert.equal(conflict.applied, false);
    assertOptional(conflict.current, true, Buffer.from("manifest:one").toString("hex"));
    const deleted = await store.compareAndSwapRootManifest(
      rootName,
      presentBytes(bytes("manifest:one")),
      missingBytes(),
    );
    assert.equal(deleted.applied, true);
  }

  if (descriptor.capabilities.transactions) {
    const txRoot = bytes("root:transaction");
    const txNode = bytes("transaction:node");
    const conflict = await store.commitTransaction(
      [upsertNode(txNode, bytes("must-not-write"))],
      [{ name: txRoot, expected: presentBytes(bytes("wrong")) }],
      [{ kind: "put", name: txRoot, manifest: bytes("must-not-publish") }],
    );
    assert.equal(conflict.applied, false);
    assertOptional(await store.getNode(txNode), false, "");
    assertOptional(await store.getRootManifest(txRoot), false, "");

    const applied = await store.commitTransaction(
      [upsertNode(txNode, bytes("written"))],
      [{ name: txRoot, expected: missingBytes() }],
      [{ kind: "put", name: txRoot, manifest: bytes("published") }],
    );
    assert.equal(applied.applied, true);
    assertOptional(await store.getNode(txNode), true, Buffer.from("written").toString("hex"));
    assertOptional(await store.getRootManifest(txRoot), true, Buffer.from("published").toString("hex"));
    await store.commitTransaction([], [], [deleteRoot(txRoot)]);
  }

  const controller = new AbortController();
  controller.abort("conformance cancellation");
  await assert.rejects(store.descriptor(controller.signal), (error: unknown) => {
    assert.ok(error instanceof StoreError);
    assert.equal(error.code, "cancelled");
    return true;
  });

  assert.throws(
    () => normalizeOptionalBytes({ present: false, value: Uint8Array.of(1) }),
    /absent optional bytes/,
  );
}

async function loadCases(): Promise<CasesFile> {
  const url = new URL("../../../../conformance/store-protocol-v1/cases.json", import.meta.url);
  return JSON.parse(await readFile(url, "utf8")) as CasesFile;
}

function assertOptional(value: OptionalBytes, present: boolean, expectedHex: string): void {
  const normalized = normalizeOptionalBytes(value);
  assert.equal(normalized.present, present);
  assert.equal(hex(normalized.value), expectedHex);
}

function bytes(value: string): Uint8Array {
  return Uint8Array.from(Buffer.from(value));
}

function hex(value: Uint8Array): string {
  return Buffer.from(value).toString("hex");
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
  return Buffer.compare(Buffer.from(left), Buffer.from(right));
}
