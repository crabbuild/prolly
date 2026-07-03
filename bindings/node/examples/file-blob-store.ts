import assert from "node:assert/strict";

import { loadNative } from "../src/native.ts";

const native = await loadNative();
const bytes = (value: string): Buffer => Buffer.from(value);

const engine = native.NativeProllyEngine.memory();
const blobStore = native.NativeProllyBlobStore.memory();
const policy = { inlineThreshold: "8" };

let tree = engine.create();
tree = engine.putLargeValue(blobStore, tree, bytes("doc/body"), Buffer.alloc(64, 7), policy);
assert.equal(engine.getValueRef(tree, bytes("doc/body"))?.kind, "blob");

const updated = engine.putLargeValue(blobStore, tree, bytes("doc/body"), Buffer.alloc(64, 9), policy);
assert.deepEqual(Buffer.from(engine.getLargeValue(blobStore, updated, bytes("doc/body")) ?? []), Buffer.alloc(64, 9));

const plan = engine.planBlobStoreGc(blobStore, [updated]);
assert.equal(plan.reclaimableBlobCount, "1");
const sweep = engine.sweepBlobStoreGc(blobStore, [updated]);
assert.equal(sweep.deletedBlobs, "1");

console.log(`file_blob_store: reclaimed ${sweep.deletedBlobBytes} bytes`);
