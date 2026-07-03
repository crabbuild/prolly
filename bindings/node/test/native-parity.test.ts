import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

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
const keyOf = (value: Uint8Array): string => Buffer.from(value).toString("hex");

function makeHostStore(nativeModule: any): any {
  const nodes = new Map<string, Buffer>();
  const roots = new Map<string, Buffer>();
  const hints = new Map<string, Buffer>();
  const hintKey = (namespace: Uint8Array, key: Uint8Array): string => `${keyOf(namespace)}:${keyOf(key)}`;

  return new nativeModule.NativeHostStore(
    ({ key }: { key: Uint8Array }) => {
      const value = nodes.get(keyOf(key));
      return value == null ? { ok: false } : { ok: true, value };
    },
    ({ key, value }: { key: Uint8Array; value: Uint8Array }) => {
      nodes.set(keyOf(key), Buffer.from(value));
      return {};
    },
    ({ key }: { key: Uint8Array }) => {
      nodes.delete(keyOf(key));
      return {};
    },
    ({ ops }: { ops: Array<{ kind: string; key: Uint8Array; value?: Uint8Array | null }> }) => {
      for (const op of ops) {
        if (op.kind === "upsert") nodes.set(keyOf(op.key), Buffer.from(op.value ?? []));
        if (op.kind === "delete") nodes.delete(keyOf(op.key));
      }
      return {};
    },
    ({ keys }: { keys: Uint8Array[] }) => ({
      values: keys.map((key) => {
        const value = nodes.get(keyOf(key));
        return value == null ? { ok: false } : { ok: true, value };
      }),
    }),
    () => ({ value: true }),
    () => ({ value: true }),
    ({ namespace, key }: { namespace: Uint8Array; key: Uint8Array }) => {
      const value = hints.get(hintKey(namespace, key));
      return value == null ? { ok: false } : { ok: true, value };
    },
    ({ namespace, key, value }: { namespace: Uint8Array; key: Uint8Array; value: Uint8Array }) => {
      hints.set(hintKey(namespace, key), Buffer.from(value));
      return {};
    },
    () => ({
      values: [...nodes.keys()].sort().map((key) => Buffer.from(key, "hex")),
    }),
    ({ name }: { name: Uint8Array }) => {
      const value = roots.get(keyOf(name));
      return value == null ? {} : { value };
    },
    ({ name, manifest }: { name: Uint8Array; manifest: Uint8Array }) => {
      roots.set(keyOf(name), Buffer.from(manifest));
      return {};
    },
    ({ name }: { name: Uint8Array }) => {
      roots.delete(keyOf(name));
      return {};
    },
    ({
      name,
      expected,
      replacement,
    }: {
      name: Uint8Array;
      expected?: Uint8Array | null;
      replacement?: Uint8Array | null;
    }) => {
      const key = keyOf(name);
      const current = roots.get(key) ?? null;
      const expectedBuffer = expected == null ? null : Buffer.from(expected);
      const matches =
        (current == null && expectedBuffer == null) ||
        (current != null && expectedBuffer != null && current.equals(expectedBuffer));
      if (!matches) return current == null ? { applied: false } : { applied: false, current };
      if (replacement == null) roots.delete(key);
      else roots.set(key, Buffer.from(replacement));
      return { applied: true };
    },
    () => ({
      values: [...roots.entries()]
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([name, manifest]) => ({ name: Buffer.from(name, "hex"), manifest })),
    }),
  );
}

test("native batch, getMany, pages, and diff pages use Rust engine", { skip: native === null }, () => {
  assert.ok(native);
  const defaultConfig = native.defaultConfig();
  assert.equal(defaultConfig.encoding.kind, "raw");
  assert.equal(native.encodingRaw().kind, "raw");
  assert.equal(native.encodingCbor().kind, "cbor");
  assert.equal(native.encodingJson().kind, "json");
  const customEncoding = native.encodingCustom("postcard");
  assert.deepEqual(customEncoding, { kind: "custom", customName: "postcard" });
  assert.deepEqual(native.treeConfig("2", "64", 32, "7", customEncoding, "16", "4096"), {
    minChunkSize: "2",
    maxChunkSize: "64",
    chunkingFactor: 32,
    hashSeed: "7",
    encoding: customEncoding,
    nodeCacheMaxNodes: "16",
    nodeCacheMaxBytes: "4096",
  });
  assert.throws(() =>
    native.treeConfig("2", "64", 32, "7", { kind: "custom" } as any, null, null),
  );

  const engine = native.NativeProllyEngine.memory();
  const empty = engine.create();
  assert.deepEqual(native.upsertMutation(bytes("probe"), bytes("value")), {
    kind: "upsert",
    key: bytes("probe"),
    value: bytes("value"),
  });
  assert.deepEqual(native.deleteMutation(bytes("probe")), {
    kind: "delete",
    key: bytes("probe"),
  });
  const tree = engine.batch(empty, [
    native.upsertMutation(bytes("a"), bytes("1")),
    native.upsertMutation(bytes("b"), bytes("2")),
    native.upsertMutation(bytes("a"), bytes("11")),
    native.deleteMutation(bytes("missing")),
  ]);

  assert.deepEqual(engine.getMany(tree, [bytes("a"), bytes("missing"), bytes("b")]).map(text), [
    "11",
    null,
    "2",
  ]);

  const proof = engine.proveKey(tree, bytes("a"));
  const verifiedProof = native.verifyKeyProof(proof);
  assert.equal(verifiedProof.valid, true);
  assert.equal(verifiedProof.exists, true);
  assert.equal(verifiedProof.absence, false);
  assert.equal(text(verifiedProof.value), "11");
  const decodedProof = native.keyProofFromNodeBytes(proof.root, proof.key, proof.pathNodeBytes);
  assert.equal(text(native.verifyKeyProof(decodedProof).value), "11");
  const proofBytes = native.keyProofToBytes(proof);
  const proofSummary = native.inspectProofBundle(proofBytes);
  assert.equal(proofSummary.kind, "key");
  assert.deepEqual(proofSummary.root, proof.root);
  assert.equal(proofSummary.keyCount, "1");
  assert.equal(proofSummary.pathNodeCount, String(proof.pathNodeBytes.length));
  const proofBundleVerified = native.verifyProofBundle(proofBytes);
  assert.equal(proofBundleVerified.valid, true);
  assert.equal(proofBundleVerified.summary.kind, "key");
  assert.equal(proofBundleVerified.existsCount, "1");
  assert.equal(proofBundleVerified.absenceCount, "0");
  const decodedProofFromBytes = native.keyProofFromBytes(proofBytes);
  assert.equal(text(native.verifyKeyProof(decodedProofFromBytes).value), "11");
  const absentProof = engine.proveKey(tree, bytes("missing"));
  const verifiedAbsence = native.verifyKeyProof(absentProof);
  assert.equal(verifiedAbsence.valid, true);
  assert.equal(verifiedAbsence.exists, false);
  assert.equal(verifiedAbsence.absence, true);
  assert.equal(verifiedAbsence.value == null, true);
  const tamperedProof = {
    ...proof,
    root: Buffer.from(proof.root ?? []),
  };
  tamperedProof.root[0] ^= 0xff;
  assert.equal(native.verifyKeyProof(tamperedProof).valid, false);
  assert.equal(native.verifyProofBundle(native.keyProofToBytes(tamperedProof)).valid, false);
  const multiProof = engine.proveKeys(tree, [bytes("a"), bytes("missing"), bytes("b")]);
  const multiVerified = native.verifyMultiKeyProof(multiProof);
  assert.equal(multiVerified.valid, true);
  assert.equal(multiVerified.results.length, 3);
  assert.equal(text(multiVerified.results[0].value), "11");
  assert.equal(multiVerified.results[1].absence, true);
  assert.equal(text(multiVerified.results[2].value), "2");
  const decodedMultiProof = native.multiKeyProofFromNodeBytes(
    multiProof.root,
    multiProof.keys,
    multiProof.pathNodeBytes,
  );
  assert.equal(text(native.verifyMultiKeyProof(decodedMultiProof).results[2].value), "2");
  const decodedMultiProofFromBytes = native.multiKeyProofFromBytes(native.multiKeyProofToBytes(multiProof));
  assert.equal(text(native.verifyMultiKeyProof(decodedMultiProofFromBytes).results[2].value), "2");
  const rangeProof = engine.proveRange(tree, bytes("a"), bytes("c"));
  const rangeVerified = native.verifyRangeProof(rangeProof);
  assert.equal(rangeVerified.valid, true);
  assert.deepEqual(rangeVerified.entries.map((entry) => text(entry.key)), ["a", "b"]);
  assert.deepEqual(rangeVerified.entries.map((entry) => text(entry.value)), ["11", "2"]);
  const decodedRangeProof = native.rangeProofFromNodeBytes(
    rangeProof.root,
    rangeProof.start,
    rangeProof.end,
    rangeProof.pathNodeBytes,
  );
  assert.equal(text(native.verifyRangeProof(decodedRangeProof).entries[1].value), "2");
  const decodedRangeProofFromBytes = native.rangeProofFromBytes(native.rangeProofToBytes(rangeProof));
  assert.equal(text(native.verifyRangeProof(decodedRangeProofFromBytes).entries[1].value), "2");
  const prefixProof = engine.provePrefix(tree, bytes("a"));
  const prefixVerified = native.verifyRangeProof(prefixProof);
  assert.equal(prefixVerified.valid, true);
  assert.deepEqual(prefixVerified.entries.map((entry) => text(entry.value)), ["11"]);
  const provedPage = engine.proveRangePage(tree, { afterKey: bytes("a") }, null, "1");
  assert.deepEqual(provedPage.page.entries.map((entry) => text(entry.key)), ["b"]);
  assert.equal(provedPage.page.nextCursor == null, true);
  assert.equal(text(provedPage.proof.after), "a");
  assert.equal(provedPage.proof.end == null, true);
  const pageVerified = native.verifyRangePageProof(provedPage.proof);
  assert.equal(pageVerified.valid, true);
  assert.deepEqual(pageVerified.entries.map((entry) => text(entry.value)), ["2"]);
  const decodedPageProof = native.rangePageProofFromNodeBytes(
    provedPage.proof.root,
    provedPage.proof.after,
    provedPage.proof.end,
    provedPage.proof.pathNodeBytes,
  );
  assert.equal(text(native.verifyRangePageProof(decodedPageProof).entries[0].key), "b");
  const decodedPageProofFromBytes = native.rangePageProofFromBytes(
    native.rangePageProofToBytes(provedPage.proof),
  );
  assert.equal(text(native.verifyRangePageProof(decodedPageProofFromBytes).entries[0].key), "b");

  let other = engine.delete(tree, bytes("a"));
  other = engine.put(other, bytes("b"), bytes("22"));
  other = engine.put(other, bytes("d"), bytes("4"));
  const provedDiffPage = engine.proveDiffPage(tree, other, null, null, "1");
  assert.equal(provedDiffPage.page.diffs.length, 1);
  assert.equal(provedDiffPage.page.diffs[0].kind, "removed");
  assert.equal(text(provedDiffPage.page.diffs[0].key), "a");
  assert.equal(text(provedDiffPage.page.nextCursor?.afterKey), "a");
  assert.equal(text(provedDiffPage.proof.base.end), "b");
  assert.equal(text(provedDiffPage.proof.lookaheadBase?.key), "b");
  const diffPageVerified = native.verifyDiffPageProof(provedDiffPage.proof);
  assert.equal(diffPageVerified.valid, true);
  assert.equal(diffPageVerified.lookaheadValid, true);
  assert.deepEqual(diffPageVerified.diffs, provedDiffPage.page.diffs);
  assert.deepEqual(diffPageVerified.nextCursor, provedDiffPage.page.nextCursor);
  const diffPageProofBytes = native.diffPageProofToBytes(provedDiffPage.proof);
  assert.deepEqual(diffPageProofBytes, native.diffPageProofToBytes(provedDiffPage.proof));
  const diffPageProofSummary = native.inspectProofBundle(diffPageProofBytes);
  assert.equal(diffPageProofSummary.kind, "diff_page");
  assert.deepEqual(diffPageProofSummary.root, tree.root);
  assert.deepEqual(diffPageProofSummary.otherRoot, other.root);
  assert.equal(diffPageProofSummary.limit, "1");
  assert.equal(diffPageProofSummary.hasLookahead, true);
  const diffPageBundleVerified = native.verifyProofBundle(diffPageProofBytes);
  assert.equal(diffPageBundleVerified.valid, true);
  assert.equal(diffPageBundleVerified.summary.kind, "diff_page");
  assert.equal(diffPageBundleVerified.diffCount, "1");
  assert.equal(text(diffPageBundleVerified.nextCursor?.afterKey), "a");
  const decodedDiffPageProof = native.diffPageProofFromBytes(diffPageProofBytes);
  assert.deepEqual(
    native.verifyDiffPageProof(decodedDiffPageProof).diffs,
    provedDiffPage.page.diffs,
  );

  const signedEnvelope = native.signProofBundleHmacSha256(
    native.keyProofToBytes(proof),
    bytes("node-key"),
    bytes("shared secret"),
    bytes("tenant=t1"),
    "1700000000000",
    "1700000100000",
    bytes("nonce-1"),
  );
  const signedEnvelopeBytes = native.authenticatedProofEnvelopeToBytes(signedEnvelope);
  assert.deepEqual(signedEnvelopeBytes, native.authenticatedProofEnvelopeToBytes(signedEnvelope));
  const decodedEnvelope = native.authenticatedProofEnvelopeFromBytes(signedEnvelopeBytes);
  const envelopeVerified = native.verifyAuthenticatedProofEnvelope(
    decodedEnvelope,
    bytes("shared secret"),
    "1700000050000",
  );
  assert.equal(envelopeVerified.valid, true);
  assert.equal(envelopeVerified.signatureValid, true);
  assert.equal(text(envelopeVerified.keyId), "node-key");
  assert.equal(text(envelopeVerified.context), "tenant=t1");
  assert.equal(
    text(native.verifyKeyProof(native.keyProofFromBytes(envelopeVerified.proofBundle)).value),
    "11",
  );
  const authenticatedBundle = native.verifyAuthenticatedProofBundle(
    signedEnvelopeBytes,
    bytes("shared secret"),
    "1700000050000",
  );
  assert.equal(authenticatedBundle.valid, true);
  assert.equal(authenticatedBundle.envelope.valid, true);
  assert.equal(authenticatedBundle.proofError == null, true);
  assert.equal(authenticatedBundle.proof?.existsCount, "1");
  const wrongEnvelope = native.verifyAuthenticatedProofEnvelope(
    decodedEnvelope,
    bytes("wrong secret"),
    "1700000050000",
  );
  assert.equal(wrongEnvelope.valid, false);
  assert.equal(wrongEnvelope.signatureValid, false);
  const wrongBundle = native.verifyAuthenticatedProofBundle(
    signedEnvelopeBytes,
    bytes("wrong secret"),
    "1700000050000",
  );
  assert.equal(wrongBundle.valid, false);
  assert.equal(wrongBundle.envelope.valid, false);
  assert.equal(wrongBundle.proof == null, true);

  const built = engine.buildFromEntries([
    { key: bytes("c"), value: bytes("3") },
    { key: bytes("a"), value: bytes("1") },
    { key: bytes("b"), value: bytes("2") },
  ]);
  const sortedBuilt = engine.buildFromSortedEntries([
    { key: bytes("a"), value: bytes("1") },
    { key: bytes("b"), value: bytes("2") },
    { key: bytes("c"), value: bytes("3") },
  ]);
  assert.deepEqual(built.root, sortedBuilt.root);
  assert.throws(() =>
    engine.buildFromSortedEntries([
      { key: bytes("b"), value: bytes("2") },
      { key: bytes("a"), value: bytes("1") },
    ]),
  );
  const batchStats = engine.batchWithStats(empty, [
    { kind: "upsert", key: bytes("b"), value: bytes("2") },
    { kind: "upsert", key: bytes("a"), value: bytes("1") },
    { kind: "upsert", key: bytes("a"), value: bytes("11") },
  ]);
  assert.equal(text(engine.get(batchStats.tree, bytes("a"))), "11");
  assert.equal(batchStats.stats.inputMutations, "3");
  assert.equal(batchStats.stats.effectiveMutations, "2");
  assert.equal(batchStats.stats.preprocessInputSorted, false);

  const parallelConfig = native.defaultParallelConfig();
  assert.equal(parallelConfig.parallelismThreshold, "100");
  assert.deepEqual(native.parallelConfig("1", "1"), {
    maxThreads: "1",
    parallelismThreshold: "1",
  });
  const sequentialParallelConfig = native.parallelConfigSequential();
  assert.equal(sequentialParallelConfig.maxThreads, "1");
  assert.equal(BigInt(sequentialParallelConfig.parallelismThreshold), (1n << 64n) - 1n);
  const parallelTree = engine.parallelBatch(
    empty,
    [
      { kind: "upsert", key: bytes("p"), value: bytes("parallel") },
      { kind: "upsert", key: bytes("q"), value: bytes("batch") },
    ],
    native.parallelConfig("1", "1"),
  );
  assert.equal(text(engine.get(parallelTree, bytes("q"))), "batch");
  const parallelStats = engine.parallelBatchWithStats(
    empty,
    [
      { kind: "upsert", key: bytes("r"), value: bytes("route") },
      { kind: "upsert", key: bytes("s"), value: bytes("stats") },
    ],
    native.parallelConfig("1", "1"),
  );
  assert.equal(text(engine.get(parallelStats.tree, bytes("s"))), "stats");
  assert.equal(parallelStats.stats.inputMutations, "2");
  assert.equal(parallelStats.stats.effectiveMutations, "2");
  assert.notEqual(parallelStats.stats.writtenNodes, "0");

  const appended = engine.appendBatch(built, [
    { kind: "upsert", key: bytes("d"), value: bytes("4") },
    { kind: "upsert", key: bytes("e"), value: bytes("5") },
    { kind: "upsert", key: bytes("d"), value: bytes("44") },
  ]);
  assert.equal(text(engine.get(appended, bytes("d"))), "44");
  const appendedStats = engine.appendBatchWithStats(built, [
    { kind: "upsert", key: bytes("d"), value: bytes("4") },
    { kind: "upsert", key: bytes("e"), value: bytes("5") },
    { kind: "upsert", key: bytes("d"), value: bytes("44") },
  ]);
  assert.equal(text(engine.get(appendedStats.tree, bytes("d"))), "44");
  assert.equal(appendedStats.stats.inputMutations, "3");
  assert.equal(appendedStats.stats.effectiveMutations, "2");
  assert.equal(appendedStats.stats.preprocessInputSorted, false);
  assert.equal(appendedStats.stats.usedAppendFastPath, true);
  assert.notEqual(appendedStats.stats.writtenNodes, "0");

  const firstPage = engine.rangePage(tree, null, null, "1");
  assert.equal(firstPage.entries.length, 1);
  assert.equal(text(firstPage.entries[0].key), "a");
  assert.ok(firstPage.nextCursor);

  const afterA = engine.rangeAfter(tree, bytes("a"), null);
  assert.deepEqual(afterA.map((entry) => text(entry.key)), ["b"]);
  assert.equal(native.rangeCursorStart().afterKey ?? null, null);
  const afterACursor = native.rangeCursorAfterKey(bytes("a"));
  assert.equal(text(afterACursor.afterKey), "a");
  const fromCursor = engine.rangeFromCursor(tree, afterACursor, null);
  assert.deepEqual(fromCursor.map((entry) => text(entry.key)), afterA.map((entry) => text(entry.key)));
  assert.deepEqual(engine.prefix(tree, bytes("a")).map((entry) => text(entry.value)), ["11"]);
  const prefixPage = engine.prefixPage(tree, bytes("a"), null, "1");
  assert.deepEqual(prefixPage.entries.map((entry) => text(entry.value)), ["11"]);
  assert.ok(prefixPage.nextCursor);
  assert.equal(text(engine.firstEntry(tree)?.key), "a");
  assert.equal(text(engine.firstEntry(tree)?.value), "11");
  assert.equal(text(engine.lastEntry(tree)?.key), "b");
  assert.equal(text(engine.lowerBound(tree, bytes("aa"))?.key), "b");
  assert.equal(engine.upperBound(tree, bytes("b")) ?? null, null);

  const window = engine.cursorWindow(tree, bytes("aa"), null, "1");
  assert.equal(text(window.positionKey), "a");
  assert.equal(text(window.positionValue), "11");
  assert.equal(window.found, false);
  assert.deepEqual(window.entries.map((entry) => text(entry.key)), ["b"]);
  assert.equal(text(window.nextCursor?.afterKey), "b");

  const exactProbe = engine.cursorWindow(tree, bytes("a"), null, "0");
  assert.equal(exactProbe.found, true);
  assert.equal(text(exactProbe.positionKey), "a");
  assert.equal(exactProbe.entries.length, 0);
  assert.equal(exactProbe.nextCursor == null, true);

  const secondPage = engine.rangePage(tree, firstPage.nextCursor, null, "1");
  assert.equal(secondPage.entries.length, 1);
  assert.equal(text(secondPage.entries[0].key), "b");
  if (secondPage.nextCursor != null) {
    const thirdPage = engine.rangePage(tree, secondPage.nextCursor, null, "1");
    assert.equal(thirdPage.entries.length, 0);
    assert.equal(thirdPage.nextCursor == null, true);
  }

  assert.equal(native.reverseCursorEnd().beforeKey ?? null, null);
  const beforeCCursor = native.reverseCursorBeforeKey(bytes("c"));
  assert.equal(text(beforeCCursor.beforeKey), "c");
  const reverseFirst = engine.reversePage(built, null, Buffer.alloc(0), "2");
  assert.deepEqual(reverseFirst.entries.map((entry) => text(entry.key)), ["c", "b"]);
  assert.equal(text(reverseFirst.nextCursor?.beforeKey), "b");
  const reverseSecond = engine.reversePage(built, reverseFirst.nextCursor, Buffer.alloc(0), "2");
  assert.deepEqual(reverseSecond.entries.map((entry) => text(entry.key)), ["a"]);
  assert.equal(reverseSecond.nextCursor == null, true);
  const reverseBounded = engine.reversePage(built, null, bytes("b"), "8");
  assert.deepEqual(reverseBounded.entries.map((entry) => text(entry.key)), ["c", "b"]);
  const prefixReverse = engine.prefixReversePage(built, bytes("b"), null, "8");
  assert.deepEqual(prefixReverse.entries.map((entry) => text(entry.key)), ["b"]);
  assert.equal(prefixReverse.nextCursor == null, true);

  const changed = engine.put(tree, bytes("b"), bytes("22"));
  const diffPage = engine.diffPage(tree, changed, null, null, "1");
  assert.equal(diffPage.diffs.length, 1);
  assert.equal(diffPage.diffs[0].kind, "changed");
  if (diffPage.nextCursor != null) {
    const secondDiffPage = engine.diffPage(tree, changed, diffPage.nextCursor, null, "1");
    assert.equal(secondDiffPage.diffs.length, 0);
    assert.equal(secondDiffPage.nextCursor == null, true);
  }

  const changedForCursor = engine.batch(built, [
    { kind: "upsert", key: bytes("b"), value: bytes("22") },
    { kind: "upsert", key: bytes("c"), value: bytes("33") },
  ]);
  const resumedDiffs = engine.diffFromCursor(built, changedForCursor, { afterKey: bytes("a") }, bytes("c"));
  assert.deepEqual(resumedDiffs.map((diff) => [diff.kind, text(diff.key)]), [["changed", "b"]]);

  const conflictBase = engine.batch(empty, [
    { kind: "upsert", key: bytes("a"), value: bytes("base-a") },
    { kind: "upsert", key: bytes("b"), value: bytes("base-b") },
  ]);
  const conflictLeft = engine.batch(conflictBase, [
    { kind: "upsert", key: bytes("a"), value: bytes("left-a") },
    { kind: "upsert", key: bytes("b"), value: bytes("left-b") },
  ]);
  const conflictRight = engine.batch(conflictBase, [
    { kind: "upsert", key: bytes("a"), value: bytes("right-a") },
    { kind: "upsert", key: bytes("b"), value: bytes("right-b") },
  ]);

  const firstConflictPage = engine.conflictPage(conflictBase, conflictLeft, conflictRight, null, "1");
  assert.equal(firstConflictPage.conflicts.length, 1);
  assert.equal(text(firstConflictPage.conflicts[0].key), "a");
  assert.equal(text(firstConflictPage.conflicts[0].base), "base-a");
  assert.equal(text(firstConflictPage.conflicts[0].left), "left-a");
  assert.equal(text(firstConflictPage.conflicts[0].right), "right-a");
  assert.ok(firstConflictPage.nextCursor);

  const secondConflictPage = engine.conflictPage(
    conflictBase,
    conflictLeft,
    conflictRight,
    firstConflictPage.nextCursor,
    "1",
  );
  assert.equal(secondConflictPage.conflicts.length, 1);
  assert.equal(text(secondConflictPage.conflicts[0].key), "b");
  assert.equal(secondConflictPage.nextCursor == null, true);
});

test("native merge and named-root CAS use Rust engine", { skip: native === null }, () => {
  assert.ok(native);
  const engine = native.NativeProllyEngine.memory();
  const empty = engine.create();
  const base = engine.put(empty, bytes("k"), bytes("base"));
  const left = engine.put(base, bytes("k"), bytes("left"));
  const right = engine.put(base, bytes("k"), bytes("right"));

  const explanation = engine.mergeExplain(base, left, right, "prefer_right");
  assert.ok(explanation.result);
  assert.equal(explanation.error == null, true);
  assert.match(explanation.traceJson, /events/);
  assert.ok(explanation.trace.events.length > 0);
  assert.ok(
    explanation.trace.events.some(
      (event) => event.kind === "resolver_called" && event.resolution === "value",
    ),
  );

  const merged = engine.merge(base, left, right, "prefer_right");
  assert.equal(text(engine.get(merged, bytes("k"))), "right");

  assert.equal(native.resolutionDelete().kind, "delete");
  assert.equal(native.resolutionUnresolved().kind, "unresolved");
  let updateConflict: Parameters<typeof native.resolvePreferLeft>[0] | null = null;
  const preferRightCallbackMerged = engine.mergeWithResolver(base, left, right, (conflict) => {
    updateConflict = conflict;
    return native.resolvePreferRight(conflict);
  });
  assert.equal(text(engine.get(preferRightCallbackMerged, bytes("k"))), "right");
  assert.ok(updateConflict);
  assert.equal(text(native.resolvePreferLeft(updateConflict).value), "left");
  assert.equal(native.resolveDeleteWins(updateConflict).kind, "unresolved");

  let deleteConflict: Parameters<typeof native.resolveDeleteWins>[0] | null = null;
  const updateWinsCallbackMerged = engine.mergeWithResolver(
    base,
    engine.delete(base, bytes("k")),
    right,
    (conflict) => {
      deleteConflict = conflict;
      return native.resolveUpdateWins(conflict);
    },
  );
  assert.equal(text(engine.get(updateWinsCallbackMerged, bytes("k"))), "right");
  assert.ok(deleteConflict);
  assert.equal(native.resolveDeleteWins(deleteConflict).kind, "delete");
  const resolver = (conflict: { left?: Uint8Array | null; right?: Uint8Array | null }) =>
    native.resolutionValue(
      Buffer.concat([Buffer.from(conflict.left ?? []), bytes("|"), Buffer.from(conflict.right ?? [])]),
    );
  const callbackMerged = engine.mergeWithResolver(base, left, right, resolver);
  assert.equal(text(engine.get(callbackMerged, bytes("k"))), "left|right");

  const policyBase = engine.batch(empty, [
    { kind: "upsert", key: bytes("doc/title"), value: bytes("base-title") },
    { kind: "upsert", key: bytes("k"), value: bytes("base-k") },
  ]);
  const policyLeft = engine.batch(policyBase, [
    { kind: "upsert", key: bytes("doc/title"), value: bytes("left-title") },
    { kind: "upsert", key: bytes("k"), value: bytes("left-k") },
  ]);
  const policyRight = engine.batch(policyBase, [
    { kind: "upsert", key: bytes("doc/title"), value: bytes("right-title") },
    { kind: "upsert", key: bytes("k"), value: bytes("right-k") },
  ]);
  const policy = new native.NativeMergePolicyRegistry();
  assert.equal(policy.isEmpty(), true);
  assert.equal(policy.hasDefault(), false);
  policy.setDefaultResolverName("prefer_left");
  policy.pushPrefixResolver(bytes("doc/"), resolver);
  policy.pushExactResolverName(bytes("k"), "prefer_right");
  assert.equal(policy.len(), "2");
  assert.equal(policy.hasDefault(), true);

  const policyMerged = engine.mergeWithPolicy(policyBase, policyLeft, policyRight, policy);
  assert.equal(text(engine.get(policyMerged, bytes("doc/title"))), "left-title|right-title");
  assert.equal(text(engine.get(policyMerged, bytes("k"))), "right-k");
  const policyExplanation = engine.mergeExplainWithPolicy(policyBase, policyLeft, policyRight, policy);
  assert.ok(policyExplanation.result);
  assert.equal(policyExplanation.error == null, true);
  const policyRange = engine.mergeRangeWithPolicy(policyBase, policyLeft, policyRight, bytes("doc/"), bytes("doc0"), policy);
  assert.equal(text(engine.get(policyRange, bytes("doc/title"))), "left-title|right-title");
  const policyPrefix = engine.mergePrefixWithPolicy(policyBase, policyLeft, policyRight, bytes("doc/"), policy);
  assert.equal(text(engine.get(policyPrefix, bytes("doc/title"))), "left-title|right-title");

  const name = bytes("main");
  engine.publishNamedRootAtMillis(name, merged, "42");
  assert.ok(engine.loadNamedRoot(name));
  assert.equal(engine.listNamedRoots().length, 1);
  const manifests = engine.listNamedRootManifests();
  assert.equal(manifests.length, 1);
  assert.equal(text(manifests[0].name), "main");
  assert.deepEqual(
    Buffer.from(manifests[0].manifest.tree.root ?? new Uint8Array()),
    Buffer.from(merged.root ?? new Uint8Array()),
  );
  assert.equal(manifests[0].manifest.createdAtMillis, "42");
  assert.equal(manifests[0].manifest.updatedAtMillis, "42");

  const selection = engine.loadNamedRoots([name, bytes("missing")]);
  assert.equal(selection.roots.length, 1);
  assert.equal(selection.missingNames.length, 1);

  const retainAll = native.retainAllNamedRoots();
  assert.equal(retainAll.kind, "all");
  const retainExact = native.retainExactNamedRoots([bytes("main"), bytes("missing")]);
  assert.equal(retainExact.kind, "exact");
  assert.equal(retainExact.names.length, 2);
  const retainPrefix = native.retainNamedRootPrefix(bytes("ma"));
  assert.equal(retainPrefix.kind, "prefix");
  assert.equal(keyOf(retainPrefix.prefix!), "6d61");
  const retainNewest = native.retainNewestNamedRoots(bytes("checkpoint/"), "2");
  assert.equal(retainNewest.kind, "newest_by_name");
  assert.equal(retainNewest.count, "2");
  const retainUpdated = native.retainNamedRootsUpdatedSince(bytes("checkpoint/"), "42");
  assert.equal(retainUpdated.kind, "updated_since");
  assert.equal(retainUpdated.minUpdatedAtMillis, "42");

  const retained = engine.loadRetainedNamedRoots(retainAll);
  assert.equal(retained.roots.length, 1);
  const retainedPlan = engine.planStoreGcForRetention(retainAll);
  assert.ok(Number(retainedPlan.reachability.liveNodes) > 0);

  const branch = native.snapshotNamespaceBranch();
  const tag = native.snapshotNamespaceTag();
  const custom = native.snapshotNamespaceCustom(bytes("refs/custom/"));
  assert.equal(text(native.snapshotRootName(branch, bytes("main"))), "refs/heads/main");
  assert.equal(text(native.snapshotIdFromName(branch, bytes("refs/heads/main"))), "main");
  assert.equal(text(native.snapshotRootName(custom, bytes("draft"))), "refs/custom/draft");

  engine.publishSnapshotAtMillis(branch, bytes("main"), merged, "77");
  assert.ok(engine.loadSnapshot(branch, bytes("main")));
  engine.publishSnapshot(tag, bytes("v1"), merged);
  const branchSnapshots = engine.listSnapshots(branch);
  assert.equal(branchSnapshots.length, 1);
  assert.equal(text(branchSnapshots[0].id), "main");
  assert.equal(text(branchSnapshots[0].name), "refs/heads/main");
  assert.equal(branchSnapshots[0].updatedAtMillis, "77");
  const tagSnapshots = engine.listSnapshots(tag);
  assert.equal(tagSnapshots.length, 1);
  assert.equal(text(tagSnapshots[0].id), "v1");
  const snapshotSelection = engine.loadSnapshots(branch, [bytes("main"), bytes("missing")]);
  assert.equal(snapshotSelection.snapshots.length, 1);
  assert.equal(snapshotSelection.missingIds.length, 1);
  const conflict = engine.compareAndSwapSnapshot(branch, bytes("main"), null, null);
  assert.equal(conflict.applied, false);
  assert.equal(conflict.conflict, true);
  assert.ok(conflict.current);
  const snapshotUpdate = engine.compareAndSwapSnapshotAtMillis(branch, bytes("main"), merged, null, "88");
  assert.equal(snapshotUpdate.applied, true);
  assert.equal(snapshotUpdate.conflict, false);
  assert.equal(engine.loadSnapshot(branch, bytes("main")), null);

  const update = engine.compareAndSwapNamedRoot(name, merged, null);
  assert.equal(update.applied, true);
  assert.equal(update.conflict, false);
  assert.equal(engine.loadNamedRoot(name), null);
});

test("native file and SQLite stores reopen Rust nodes", { skip: native === null }, () => {
  assert.ok(native);
  const root = mkdtempSync(join(tmpdir(), "prolly-node-"));
  try {
    const filePath = join(root, "nodes");
    const firstFile = native.NativeProllyEngine.file(filePath);
    const fileTree = firstFile.put(firstFile.create(), bytes("k"), bytes("v"));
    const reopenedFile = native.NativeProllyEngine.file(filePath);
    assert.equal(text(reopenedFile.get(fileTree, bytes("k"))), "v");

    const sqlitePath = join(root, "prolly.db");
    const firstSqlite = native.NativeProllyEngine.sqlite(sqlitePath);
    const sqliteTree = firstSqlite.put(firstSqlite.create(), bytes("k"), bytes("v"));
    const reopenedSqlite = native.NativeProllyEngine.sqlite(sqlitePath);
    assert.equal(text(reopenedSqlite.get(sqliteTree, bytes("k"))), "v");

    const transient = native.NativeProllyEngine.sqliteInMemory();
    const transientTree = transient.put(transient.create(), bytes("transient"), bytes("ok"));
    assert.equal(text(transient.get(transientTree, bytes("transient"))), "ok");
  } finally {
    rmSync(root, { force: true, recursive: true });
  }
});

test("native custom store callbacks drive Rust engine", { skip: native === null }, () => {
  assert.ok(native);
  const hostStore = makeHostStore(native);
  const engine = native.NativeProllyEngine.customStore(hostStore);
  const empty = engine.create();
  const tree = engine.batch(empty, [
    { kind: "upsert", key: bytes("a"), value: bytes("1") },
    { kind: "upsert", key: bytes("b"), value: bytes("2") },
  ]);

  assert.deepEqual(engine.getMany(tree, [bytes("a"), bytes("missing")]).map(text), ["1", null]);
  assert.equal(engine.publishPrefixPathHint(tree, bytes("a")), true);
  assert.equal(engine.hydratePrefixPathHint(tree, bytes("a")), true);

  const name = bytes("main");
  engine.publishNamedRootAtMillis(name, tree, "42");
  assert.ok(engine.loadNamedRoot(name));
  assert.equal(engine.listNamedRoots().length, 1);
  const manifests = engine.listNamedRootManifests();
  assert.equal(manifests.length, 1);
  assert.equal(text(manifests[0].name), "main");
  assert.deepEqual(
    Buffer.from(manifests[0].manifest.tree.root ?? new Uint8Array()),
    Buffer.from(tree.root ?? new Uint8Array()),
  );
  assert.equal(manifests[0].manifest.createdAtMillis, "42");
  assert.equal(manifests[0].manifest.updatedAtMillis, "42");

  const cids = engine.listNodeCids();
  assert.ok(cids.length > 0);
  assert.equal(engine.planStoreGc([tree]).reclaimableNodes, "0");

  const destination = native.NativeProllyEngine.customStore(makeHostStore(native));
  const missing = engine.planMissingNodes(tree, destination);
  assert.ok(Number(missing.missingNodes) > 0);
  const copied = engine.copyMissingNodes(tree, destination);
  assert.equal(copied.copiedNodes, missing.missingNodes);
  assert.equal(text(destination.get(tree, bytes("b"))), "2");

  const deleted = engine.compareAndSwapNamedRoot(name, tree, null);
  assert.equal(deleted.applied, true);
  assert.equal(engine.loadNamedRoot(name), null);
});

test("native operational APIs expose stats, debug, metrics, cache, and hints", { skip: native === null }, () => {
  assert.ok(native);
  const engine = native.NativeProllyEngine.memory();
  const empty = engine.create();
  const tree = engine.put(empty, bytes("k"), bytes("v"));

  assert.match(engine.collectStatsJson(tree), /"num_nodes"/);
  const typedStats = engine.collectStats(tree);
  assert.equal(typedStats.total_key_value_pairs, 1);
  assert.ok(typedStats.nodes_per_level["0"] > 0);
  assert.match(engine.statsDiffJson(empty, tree), /"absolute"/);
  const typedDiffStats = engine.statsDiff(empty, tree);
  assert.equal(typedDiffStats.before.total_key_value_pairs, 0);
  assert.equal(typedDiffStats.after.total_key_value_pairs, 1);
  assert.equal(typedDiffStats.absolute.total_key_value_pairs_diff, 1);
  assert.match(engine.debugTreeJson(tree), /"levels"/);
  const debugTree = engine.debugTree(tree);
  assert.ok(debugTree.levels.length > 0);
  assert.ok(debugTree.levels[0].nodes.length > 0);
  assert.match(engine.debugTreeText(tree), /level/);
  assert.match(engine.debugCompareTreesJson(empty, tree), /"right_only_nodes"/);
  const debugComparison = engine.debugCompareTrees(empty, tree);
  assert.ok(debugComparison.right_only_nodes > 0);
  assert.equal(debugComparison.left_only_nodes, 0);
  assert.ok(debugComparison.levels.some((level) => level.nodes.some((node) => node.status === "RightOnly")));
  assert.match(engine.debugCompareTreesText(empty, tree), /right/);

  const pinnedPathCount = Number(engine.pinTreePath(tree, bytes("k")));
  assert.ok(pinnedPathCount > 0);
  assert.ok(Number(engine.unpinAllCacheNodes()) >= 0);
  const pinnedRootCount = Number(engine.pinTreeRoot(tree));
  assert.ok(pinnedRootCount > 0);
  assert.ok(Number(engine.cacheStats().cachedNodes) > 0);
  assert.ok(Number(engine.unpinAllCacheNodes()) >= 0);
  engine.clearCache();

  assert.ok(Number(engine.metrics().nodesWritten) > 0);
  engine.resetMetrics();
  assert.equal(engine.metrics().nodesWritten, "0");

  assert.equal(engine.publishPrefixPathHint(tree, bytes("k")), false);
  assert.equal(engine.hydratePrefixPathHint(tree, bytes("k")), false);
  assert.equal(keyOf(native.changedSpanFromKey(bytes("k")).end!), "6b00");
  assert.equal(keyOf(native.changedSpanForPrefix(bytes("k")).end!), "6c");
  assert.equal(
    engine.publishChangedSpansHint(empty, tree, [native.changedSpan(bytes("k"), bytes("l"))]),
    false,
  );
  assert.equal(engine.loadChangedSpansHint(empty, tree), null);

  const structuralPage = engine.structuralDiffPage(empty, tree, null, "1");
  assert.ok(structuralPage.diffs.length > 0);
  assert.ok(Number(structuralPage.stats.emittedDiffs) > 0);
  const cursorPage = engine.structuralDiffPage(empty, tree, null, "0");
  assert.ok(cursorPage.nextCursor);
  assert.ok(cursorPage.nextCursorJson);
  const resumedStructuralPage = engine.structuralDiffPageWithCursor(empty, tree, cursorPage.nextCursor, "1");
  assert.ok(resumedStructuralPage.diffs.length > 0);

  const reachability = engine.markReachable([tree]);
  assert.ok(Number(reachability.liveNodes) > 0);
  assert.ok(reachability.liveCids.length > 0);
  const nodeCids = engine.listNodeCids();
  assert.ok(nodeCids.length > 0);

  const gcPlan = engine.planGc([tree], nodeCids);
  assert.equal(Number(gcPlan.candidateNodes), nodeCids.length);
  assert.equal(gcPlan.reclaimableNodes, "0");
  const gcSweep = engine.sweepGc([tree], nodeCids);
  assert.equal(gcSweep.deletedNodes, "0");
  assert.equal(engine.planStoreGc([tree]).reclaimableNodes, "0");
  assert.equal(engine.sweepStoreGc([tree]).deletedNodes, "0");
  engine.publishNamedRootAtMillis(bytes("live"), tree, "100");
  const retainedGc = { kind: "all", names: [], prefix: Buffer.alloc(0) };
  assert.equal(engine.planStoreGcForRetention(retainedGc).reclaimableNodes, "0");
  assert.equal(engine.sweepStoreGcForRetention(retainedGc).deletedNodes, "0");

  const destination = native.NativeProllyEngine.memory();
  const missing = engine.planMissingNodes(tree, destination);
  assert.ok(Number(missing.missingNodes) > 0);
  const copied = engine.copyMissingNodes(tree, destination);
  assert.equal(copied.copiedNodes, missing.missingNodes);
  assert.equal(engine.planMissingNodes(tree, destination).missingNodes, "0");
  assert.equal(text(destination.get(tree, bytes("k"))), "v");

  const bundle = engine.exportSnapshot(tree);
  assert.equal(bundle.formatVersion, 1);
  assert.ok(bundle.tree.config);
  assert.ok(bundle.nodes.length > 0);
  const bundleBytes = native.snapshotBundleToBytes(bundle);
  assert.deepEqual(
    Buffer.from(native.snapshotBundleDigest(bundle)),
    Buffer.from(native.cidFromBytes(bundleBytes)),
  );
  assert.deepEqual(
    Buffer.from(native.snapshotBundleDigestBytes(bundleBytes)),
    Buffer.from(native.snapshotBundleDigest(bundle)),
  );
  const summary = native.snapshotBundleSummary(bundle);
  assert.equal(summary.formatVersion, 1);
  assert.equal(summary.nodeCount, String(bundle.nodes.length));
  assert.ok(Number(summary.byteCount) > 0);
  assert.ok(summary.root);
  assert.deepEqual(native.snapshotBundleSummaryFromBytes(bundleBytes), summary);
  const verification = native.verifySnapshotBundle(bundle);
  assert.equal(verification.valid, true);
  assert.equal(verification.summary.nodeCount, summary.nodeCount);
  assert.equal(verification.missingCids.length, 0);
  assert.equal(verification.extraCids.length, 0);
  const byteVerification = native.verifySnapshotBundleBytes(bundleBytes);
  assert.equal(byteVerification.valid, true);
  assert.deepEqual(byteVerification.summary, summary);
  const incompleteBundle = { ...bundle, nodes: bundle.nodes.slice(0, -1) };
  const incompleteVerification = native.verifySnapshotBundle(incompleteBundle);
  assert.equal(incompleteVerification.valid, false);
  assert.ok(incompleteVerification.missingCids.length > 0);
  const decodedBundle = native.snapshotBundleFromBytes(bundleBytes);
  assert.equal(decodedBundle.formatVersion, 1);
  assert.equal(decodedBundle.nodes.length, bundle.nodes.length);
  const snapshotDestination = native.NativeProllyEngine.memory();
  const importedTree = snapshotDestination.importSnapshot(decodedBundle);
  assert.equal(text(snapshotDestination.get(importedTree, bytes("k"))), "v");
});

test("native CRDT, multi-value, and tombstone helpers use Rust engine", { skip: native === null }, () => {
  assert.ok(native);
  const engine = native.NativeProllyEngine.memory();
  const empty = engine.create();

  const baseValue = native.timestampedValueToBytes({ value: bytes("base"), timestamp: "1" });
  const leftValue = native.timestampedValueToBytes({ value: bytes("left"), timestamp: "2" });
  const rightValue = native.timestampedValueToBytes({ value: bytes("right"), timestamp: "3" });

  const base = engine.put(empty, bytes("k"), baseValue);
  const left = engine.put(base, bytes("k"), leftValue);
  const right = engine.put(base, bytes("k"), rightValue);
  const lww = native.crdtConfigLww("update_wins");
  assert.deepEqual(lww, { strategy: "last_writer_wins", deletePolicy: "update_wins" });

  const merged = engine.crdtMerge(base, left, right, lww);
  const mergedValue = native.timestampedValueFromBytes(engine.get(merged, bytes("k"))!);
  assert.equal(text(mergedValue.value), "right");
  assert.equal(mergedValue.timestamp, "3");

  const callbackMerged = engine.crdtMergeWithResolver(base, left, right, "update_wins", (conflict) =>
    native.crdtResolutionValue(
      Buffer.concat([Buffer.from(conflict.left ?? []), bytes("|"), Buffer.from(conflict.right ?? [])]),
    ),
  );
  assert.deepEqual(
    Buffer.from(engine.get(callbackMerged, bytes("k"))!),
    Buffer.concat([Buffer.from(leftValue), bytes("|"), Buffer.from(rightValue)]),
  );
  const deleted = engine.crdtMergeWithResolver(
    base,
    engine.delete(base, bytes("k")),
    engine.put(base, bytes("k"), rightValue),
    "update_wins",
    () => native.crdtResolutionDelete(),
  );
  assert.equal(engine.get(deleted, bytes("k")), null);

  const now = native.timestampedValueNow(bytes("now"));
  assert.equal(text(now.value), "now");
  assert.ok(Number(now.timestamp) > 0);

  const multiConfig = native.crdtConfigMultiValue("delete_wins");
  assert.deepEqual(multiConfig, { strategy: "multi_value", deletePolicy: "delete_wins" });
  const set = native.multiValueSetFromBytes(
    native.multiValueSetToBytes([bytes("b"), bytes("a"), bytes("a")]),
  );
  assert.deepEqual(set.map(text), ["a", "b"]);
  assert.deepEqual(native.multiValueSetMerge([bytes("b")], [bytes("a"), bytes("b")]).map(text), [
    "a",
    "b",
  ]);

  const tombstone = {
    actor: bytes("actor"),
    timestampMillis: "7",
    causalMetadata: [{ key: "clock", value: bytes("7") }],
  };
  const tombstoneBytes = native.tombstoneToBytes(tombstone);
  assert.equal(native.isTombstoneValue(tombstoneBytes), true);
  assert.equal(native.tombstoneFromBytes(tombstoneBytes).timestampMillis, "7");
  assert.equal(native.tombstoneFromStoredBytes(tombstoneBytes)?.causalMetadata[0].key, "clock");

  const upsert = native.tombstoneUpsertMutation(bytes("deleted"), tombstone);
  assert.equal(upsert.kind, "upsert");
  assert.equal(text(upsert.key), "deleted");
  assert.ok(upsert.value);

  const compaction = native.tombstoneCompactionMutation(bytes("deleted"), tombstoneBytes);
  assert.equal(compaction?.kind, "delete");
  assert.equal(text(compaction?.key), "deleted");
  assert.equal(compaction?.value == null, true);
});

test("native blob stores, large values, and blob GC use Rust engine", { skip: native === null }, () => {
  assert.ok(native);
  const engine = native.NativeProllyEngine.memory();
  const blobStore = native.NativeProllyBlobStore.memory();

  assert.equal(blobStore.blobCount(), "0");
  const directRef = blobStore.putBlob(bytes("direct"));
  assert.doesNotThrow(() => native.blobRefValidateBytes(directRef, bytes("direct")));
  assert.throws(() => native.blobRefValidateBytes(directRef, bytes("wrong")));
  assert.equal(text(blobStore.getBlob(directRef)), "direct");
  blobStore.deleteBlob(directRef);
  assert.equal(blobStore.blobCount(), "0");

  const empty = engine.create();
  const largeValue = Buffer.alloc(64, 42);
  const tree = engine.putLargeValue(blobStore, empty, bytes("big"), largeValue, native.largeValueConfig("8"));
  const valueRef = engine.getValueRef(tree, bytes("big"));
  assert.equal(valueRef?.kind, "blob");
  assert.ok(valueRef?.blob);
  assert.deepEqual(engine.getLargeValue(blobStore, tree, bytes("big")), largeValue);

  const reachable = engine.markReachableBlobs([tree]);
  assert.equal(reachable.liveBlobCount, "1");
  assert.equal(reachable.liveBlobs.length, 1);
  assert.equal(engine.planBlobGc(blobStore, [tree], reachable.liveBlobs).reclaimableBlobCount, "0");

  blobStore.putBlob(bytes("orphan"));
  assert.equal(blobStore.listBlobRefs().length, 2);
  assert.equal(engine.planBlobStoreGc(blobStore, [tree]).reclaimableBlobCount, "1");
  assert.equal(engine.sweepBlobStoreGc(blobStore, [tree]).deletedBlobs, "1");
  assert.equal(blobStore.blobCount(), "1");

  const withoutBig = engine.delete(tree, bytes("big"));
  assert.equal(engine.planBlobStoreGc(blobStore, [withoutBig]).reclaimableBlobCount, "1");
  assert.equal(engine.sweepBlobStoreGc(blobStore, [withoutBig]).deletedBlobs, "1");
  assert.equal(blobStore.blobCount(), "0");
});
