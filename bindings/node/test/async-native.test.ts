import test from "node:test";
import assert from "node:assert/strict";

import { AsyncMergePolicyRegistry, AsyncProllyBlobStore, AsyncProllyEngine } from "../src/async.ts";
import { loadNative } from "../src/native.ts";

let native: Awaited<ReturnType<typeof loadNative>> | null = null;
try {
  native = await loadNative();
} catch {
  native = null;
}

const bytes = (value: string): Buffer => Buffer.from(value);
const text = (value: Uint8Array | null | undefined): string | null =>
  value == null ? null : Buffer.from(value).toString();

test("async native wrapper preserves core tree behavior", { skip: native === null }, async () => {
  assert.ok(native);
  const engine = await AsyncProllyEngine.fromNative(native.NativeProllyEngine.memory());
  const empty = await engine.create();
  const tree = await engine.batch(empty, [
    { kind: "upsert", key: bytes("a"), value: bytes("1") },
    { kind: "upsert", key: bytes("b"), value: bytes("2") },
  ]);

  assert.equal(text(await engine.get(tree, bytes("a"))), "1");
  assert.deepEqual((await engine.getMany(tree, [bytes("b"), bytes("missing")])).map(text), ["2", null]);
  const proof = await engine.proveKey(tree, bytes("a"));
  assert.equal(text(native.verifyKeyProof(proof).value), "1");
  const multiProof = await engine.proveKeys(tree, [bytes("a"), bytes("missing"), bytes("b")]);
  const multiVerified = native.verifyMultiKeyProof(multiProof);
  assert.equal(multiVerified.valid, true);
  assert.deepEqual(multiVerified.results.map((result) => text(result.value)), ["1", null, "2"]);
  const rangeProof = await engine.proveRange(tree, bytes("a"), bytes("c"));
  const rangeVerified = native.verifyRangeProof(rangeProof);
  assert.equal(rangeVerified.valid, true);
  assert.deepEqual(rangeVerified.entries.map((entry) => text(entry.value)), ["1", "2"]);
  const prefixProof = await engine.provePrefix(tree, bytes("a"));
  const prefixVerified = native.verifyRangeProof(prefixProof);
  assert.equal(prefixVerified.valid, true);
  assert.deepEqual(prefixVerified.entries.map((entry) => text(entry.value)), ["1"]);
  const provedPage = await engine.proveRangePage(tree, { afterKey: bytes("a") }, null, "1");
  const pageVerified = native.verifyRangePageProof(provedPage.proof);
  assert.equal(pageVerified.valid, true);
  assert.deepEqual(pageVerified.entries.map((entry) => text(entry.value)), ["2"]);
  assert.deepEqual((await engine.rangeAfter(tree, bytes("a"), null)).map((entry) => text(entry.key)), ["b"]);
  const batchStats = await engine.batchWithStats(empty, [
    { kind: "upsert", key: bytes("b"), value: bytes("2") },
    { kind: "upsert", key: bytes("a"), value: bytes("1") },
    { kind: "upsert", key: bytes("a"), value: bytes("11") },
  ]);
  assert.equal(text(await engine.get(batchStats.tree, bytes("a"))), "11");
  assert.equal(batchStats.stats.inputMutations, "3");
  assert.equal(batchStats.stats.effectiveMutations, "2");

  const parallelTree = await engine.parallelBatch(
    empty,
    [
      { kind: "upsert", key: bytes("p"), value: bytes("parallel") },
      { kind: "upsert", key: bytes("q"), value: bytes("async") },
    ],
    { ...native.defaultParallelConfig(), maxThreads: "1", parallelismThreshold: "1" },
  );
  assert.equal(text(await engine.get(parallelTree, bytes("q"))), "async");

  const changed = await engine.put(tree, bytes("b"), bytes("22"));
  const diffs = await engine.diff(tree, changed);
  assert.equal(diffs.length, 1);
  assert.equal(diffs[0].kind, "changed");

  const page = await engine.rangePage(changed, null, null, "1");
  assert.equal(page.entries.length, 1);
  assert.ok(page.nextCursor);
});

test("async native wrapper covers advanced engine and blob APIs", { skip: native === null }, async () => {
  assert.ok(native);
  const engine = await AsyncProllyEngine.fromNative(native.NativeProllyEngine.memory());
  const blobStore = await AsyncProllyBlobStore.fromNative(native.NativeProllyBlobStore.memory());

  const directRef = await blobStore.putBlob(bytes("direct"));
  assert.equal(text(await blobStore.getBlob(directRef)), "direct");
  assert.equal(await blobStore.blobCount(), "1");
  assert.equal((await blobStore.listBlobRefs()).length, 1);
  await blobStore.deleteBlob(directRef);
  assert.equal(await blobStore.blobCount(), "0");

  const empty = await engine.create();
  const largeValue = Buffer.alloc(64, 7);
  const tree = await engine.putLargeValue(blobStore, empty, bytes("big"), largeValue, { inlineThreshold: "8" });
  const valueRef = await engine.getValueRef(tree, bytes("big"));
  assert.equal(valueRef?.kind, "blob");
  assert.deepEqual(Buffer.from((await engine.getLargeValue(blobStore, tree, bytes("big"))) ?? []), largeValue);
  const blobPlan = await engine.planBlobStoreGc(blobStore, [tree]);
  assert.equal(blobPlan.reachability.liveBlobCount, "1");

  const base = await engine.put(empty, bytes("k"), bytes("base"));
  const left = await engine.put(base, bytes("k"), bytes("left"));
  const right = await engine.put(base, bytes("k"), bytes("right"));
  const merged = await engine.merge(base, left, right, "prefer_right");
  assert.equal(text(await engine.get(merged, bytes("k"))), "right");
  const explanation = await engine.mergeExplain(base, left, right, "prefer_right");
  assert.ok(explanation.result);
  assert.equal(explanation.error ?? null, null);
  const resolver = (conflict: { left?: Uint8Array | null; right?: Uint8Array | null }) => ({
    kind: "value" as const,
    value: Buffer.concat([Buffer.from(conflict.left ?? []), bytes("|"), Buffer.from(conflict.right ?? [])]),
  });
  const callbackMerged = await engine.mergeWithResolver(base, left, right, resolver);
  assert.equal(text(await engine.get(callbackMerged, bytes("k"))), "left|right");
  const callbackExplanation = await engine.mergeExplainWithResolver(base, left, right, resolver);
  assert.ok(callbackExplanation.result);
  assert.equal(callbackExplanation.error ?? null, null);
  const policy = await AsyncMergePolicyRegistry.create();
  await policy.setDefaultResolver(resolver);
  const policyMerged = await engine.mergeWithPolicy(base, left, right, policy);
  assert.equal(text(await engine.get(policyMerged, bytes("k"))), "left|right");
  const crdtCallbackMerged = await engine.crdtMergeWithResolver(base, left, right, "update_wins", resolver);
  assert.equal(text(await engine.get(crdtCallbackMerged, bytes("k"))), "left|right");

  await engine.publishNamedRootAtMillis(bytes("main"), merged, "42");
  assert.ok(await engine.loadNamedRoot(bytes("main")));
  assert.equal((await engine.listNamedRoots()).length, 1);
  const selection = await engine.loadNamedRoots([bytes("main"), bytes("missing")]);
  assert.equal(selection.roots.length, 1);
  assert.equal(selection.missingNames.length, 1);
  const retainedPlan = await engine.planStoreGcForRetention({
    kind: "all",
    names: [],
    prefix: Buffer.alloc(0),
  });
  assert.ok(Number(retainedPlan.reachability.liveNodes) > 0);

  const branch = native.snapshotNamespaceBranch();
  await engine.publishSnapshotAtMillis(branch, bytes("main"), merged, "77");
  assert.ok(await engine.loadSnapshot(branch, bytes("main")));
  assert.equal((await engine.listSnapshots(branch)).length, 1);
  const snapshotSelection = await engine.loadSnapshots(branch, [bytes("main"), bytes("missing")]);
  assert.equal(snapshotSelection.snapshots.length, 1);
  assert.equal(snapshotSelection.missingIds.length, 1);
  const snapshotUpdate = await engine.compareAndSwapSnapshot(branch, bytes("main"), merged, null);
  assert.equal(snapshotUpdate.applied, true);
  assert.equal(snapshotUpdate.conflict, false);
  assert.equal(await engine.loadSnapshot(branch, bytes("main")), null);

  const update = await engine.compareAndSwapNamedRoot(bytes("main"), merged, null);
  assert.equal(update.applied, true);
  assert.equal(update.conflict, false);
  assert.equal(await engine.loadNamedRoot(bytes("main")), null);

  assert.match(await engine.collectStatsJson(tree), /"num_nodes"/);
  assert.match(await engine.debugTreeText(tree), /level/);
  assert.equal(Number(await engine.pinTreeRoot(tree)) > 0, true);
  const cacheStats = await engine.cacheStats();
  assert.equal(Number(cacheStats.pinnedNodes) > 0, true);
  assert.equal(Number(await engine.unpinAllCacheNodes()) > 0, true);
  await engine.clearCache();
  const metrics = await engine.metrics();
  assert.equal(Number(metrics.nodesWritten) > 0, true);
  await engine.resetMetrics();

  const reachability = await engine.markReachable([tree]);
  assert.equal(Number(reachability.liveNodes) > 0, true);
  const nodeCids = await engine.listNodeCids();
  assert.equal(nodeCids.length > 0, true);
  const gcPlan = await engine.planGc([tree], nodeCids);
  assert.equal(gcPlan.candidateNodes, String(nodeCids.length));

  const destination = await AsyncProllyEngine.fromNative(native.NativeProllyEngine.memory());
  const missing = await engine.planMissingNodes(tree, destination);
  assert.equal(Number(missing.missingNodes) > 0, true);
  const copied = await engine.copyMissingNodes(tree, destination);
  assert.equal(copied.copiedNodes, missing.missingNodes);
  assert.deepEqual(Buffer.from((await destination.getLargeValue(blobStore, tree, bytes("big"))) ?? []), largeValue);
});
