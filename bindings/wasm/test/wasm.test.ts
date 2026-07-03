import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";
import assert from "node:assert/strict";

const fixturePath = resolve(import.meta.dirname, "../../../conformance/prolly-fixtures.v1.json");

function fixtures(): any {
  return JSON.parse(readFileSync(fixturePath, "utf8"));
}

function fromHex(value: string): Uint8Array {
  return Uint8Array.from(Buffer.from(value, "hex"));
}

function utf8(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function toHex(value: Uint8Array | null | undefined): string | null {
  return value == null ? null : Buffer.from(value).toString("hex");
}

let wasm: any = null;
let runtimeError: unknown = null;
const generatedModulePath = resolve(import.meta.dirname, "../pkg/prolly_wasm.js");
const generatedWasmPath = resolve(import.meta.dirname, "../pkg/prolly_wasm_bg.wasm");
const generatedPresent = existsSync(generatedModulePath) && existsSync(generatedWasmPath);
try {
  if (generatedPresent) {
    wasm = await import("../pkg/prolly_wasm.js");
    wasm.initSync({ module: readFileSync(generatedWasmPath) });
  }
} catch (error) {
  runtimeError = error;
  wasm = null;
}

test("wasm package declares browser memory-scope API", () => {
  const source = readFileSync(resolve(import.meta.dirname, "../src/index.ts"), "utf8");
  assert.match(source, /loadProllyWasm/);
  assert.match(source, /WasmEntryRecord/);
  assert.match(source, /WasmParallelConfigRecord/);
  assert.match(source, /WasmBatchApplyStatsRecord/);
  assert.match(source, /WasmBatchApplyResultRecord/);
  assert.match(source, /WasmRangePageRecord/);
  assert.match(source, /WasmRangeBoundsRecord/);
  assert.match(source, /WasmDiffPageRecord/);
  assert.match(source, /WasmStructuralDiffPageRecord/);
  assert.match(source, /WasmConflictPageRecord/);
  assert.match(source, /WasmMergeExplanationRecord/);
  assert.match(source, /WasmKeyProofRecord/);
  assert.match(source, /WasmKeyProofVerificationRecord/);
  assert.match(source, /WasmProofBundleSummaryRecord/);
  assert.match(source, /WasmProofBundleVerificationRecord/);
  assert.match(source, /WasmAuthenticatedProofBundleVerificationRecord/);
  assert.match(source, /WasmRangeProofRecord/);
  assert.match(source, /WasmRangeProofVerificationRecord/);
  assert.match(source, /WasmSnapshotNamespaceKind/);
});

test("wasm fixtures decode, build, and query through Rust memory engine", { skip: !generatedPresent }, () => {
  assert.ifError(runtimeError);
  assert.ok(wasm);
  const loaded = fixtures();

  for (const fixture of loaded.node_fixtures) {
    const bytes = fromHex(fixture.bytes);
    assert.equal(toHex(wasm.nodeBytesRoundTrip(bytes)), fixture.bytes);
    assert.equal(toHex(wasm.nodeCidFromBytes(bytes)), fixture.cid);
    assert.equal(toHex(wasm.cidFromBytes(bytes)), fixture.cid);
  }

  for (const fixture of loaded.boundary_fixtures) {
    assert.equal(
      wasm.isBoundaryConfigJson(
        JSON.stringify(fixture.config),
        fixture.count,
        fromHex(fixture.key),
        fromHex(fixture.value),
      ),
      fixture.is_boundary,
    );
  }

  for (const fixture of loaded.key_fixtures.prefix_end) {
    const prefix = fromHex(fixture.prefix);
    assert.equal(toHex(wasm.prefixEnd(prefix)), fixture.end);
    const bounds = wasm.prefixRange(prefix);
    assert.equal(toHex(bounds.start), fixture.prefix);
    assert.equal(toHex(bounds.end), fixture.end);
  }

  for (const fixture of loaded.key_fixtures.numeric) {
    if (fixture.kind === "u64") assert.equal(toHex(wasm.u64Key(fixture.value)), fixture.encoded);
    if (fixture.kind === "u128") assert.equal(toHex(wasm.u128Key(fixture.value)), fixture.encoded);
    if (fixture.kind === "i64") assert.equal(toHex(wasm.i64Key(fixture.value)), fixture.encoded);
    if (fixture.kind === "i128") assert.equal(toHex(wasm.i128Key(fixture.value)), fixture.encoded);
    if (fixture.kind === "timestamp_millis") assert.equal(toHex(wasm.timestampMillisKey(fixture.value)), fixture.encoded);
  }

  for (const fixture of loaded.key_fixtures.segments) {
    const encoded = Buffer.concat(fixture.segments.map((segment: string) => wasm.encodeSegment(fromHex(segment))));
    assert.equal(toHex(encoded), fixture.encoded);
    assert.deepEqual(wasm.decodeSegments(fromHex(fixture.encoded)).map(toHex), fixture.decoded);
  }

  for (const fixture of loaded.key_fixtures.debug) {
    assert.equal(wasm.debugKey(fromHex(fixture.key)), fixture.debug);
  }

  assert.equal(Buffer.from(wasm.snapshotRootName("branch", utf8("main"))).toString(), "refs/heads/main");
  assert.equal(Buffer.from(wasm.snapshotIdFromName("branch", utf8("refs/heads/main"))).toString(), "main");
  assert.equal(Buffer.from(wasm.snapshotRootName("custom", utf8("draft"), utf8("refs/custom/"))).toString(), "refs/custom/draft");

  const treeFixture = loaded.tree_fixtures[0];
  const engine = wasm.WasmProllyEngine.memoryWithConfigJson(JSON.stringify(treeFixture.config));
  const tree = engine.buildFromSortedEntries(
    treeFixture.entries.map((entry: any) => ({
      key: fromHex(entry.key),
      value: fromHex(entry.value),
    })),
  );
  assert.equal(toHex(tree.root), treeFixture.root);

  for (const lookup of treeFixture.lookups) {
    assert.equal(toHex(engine.get(tree, fromHex(lookup.key))), lookup.value);
  }

  const presentLookup = treeFixture.lookups.find((lookup: any) => lookup.value !== null);
  const proof = engine.proveKey(tree, fromHex(presentLookup.key));
  const verifiedProof = wasm.verifyKeyProof(proof.root, proof.key, proof.pathNodeBytes);
  assert.equal(verifiedProof.valid, true);
  assert.equal(verifiedProof.exists, true);
  assert.equal(toHex(verifiedProof.value), presentLookup.value);
  const decodedProof = wasm.keyProofFromNodeBytes(proof.root, proof.key, proof.pathNodeBytes);
  assert.equal(toHex(wasm.verifyKeyProof(decodedProof.root, decodedProof.key, decodedProof.pathNodeBytes).value), presentLookup.value);
  const keyProofBytes = wasm.keyProofToBytes(proof.root, proof.key, proof.pathNodeBytes);
  const keyProofSummary = wasm.inspectProofBundle(keyProofBytes);
  assert.equal(keyProofSummary.kind, "key");
  assert.equal(toHex(keyProofSummary.root), treeFixture.root);
  assert.equal(keyProofSummary.keyCount, "1");
  assert.equal(keyProofSummary.pathNodeCount, String(proof.pathNodeBytes.length));
  const keyProofBundleVerified = wasm.verifyProofBundle(keyProofBytes);
  assert.equal(keyProofBundleVerified.valid, true);
  assert.equal(keyProofBundleVerified.summary.kind, "key");
  assert.equal(keyProofBundleVerified.existsCount, "1");
  assert.equal(keyProofBundleVerified.absenceCount, "0");
  const decodedProofFromBytes = wasm.keyProofFromBytes(keyProofBytes);
  assert.equal(
    toHex(wasm.verifyKeyProof(
      decodedProofFromBytes.root,
      decodedProofFromBytes.key,
      decodedProofFromBytes.pathNodeBytes,
    ).value),
    presentLookup.value,
  );
  const absentProof = engine.proveKey(tree, utf8("definitely-missing"));
  const verifiedAbsence = wasm.verifyKeyProof(absentProof.root, absentProof.key, absentProof.pathNodeBytes);
  assert.equal(verifiedAbsence.valid, true);
  assert.equal(verifiedAbsence.exists, false);
  assert.equal(verifiedAbsence.absence, true);
  assert.equal(verifiedAbsence.value, null);
  const tamperedRoot = new Uint8Array(proof.root);
  tamperedRoot[0] ^= 0xff;
  assert.equal(wasm.verifyKeyProof(tamperedRoot, proof.key, proof.pathNodeBytes).valid, false);
  assert.equal(
    wasm.verifyProofBundle(wasm.keyProofToBytes(tamperedRoot, proof.key, proof.pathNodeBytes)).valid,
    false,
  );
  const multiProof = engine.proveKeys(tree, [
    fromHex(presentLookup.key),
    utf8("definitely-missing"),
  ]);
  const multiVerified = wasm.verifyMultiKeyProof(multiProof.root, multiProof.keys, multiProof.pathNodeBytes);
  assert.equal(multiVerified.valid, true);
  assert.equal(multiVerified.results.length, 2);
  assert.equal(toHex(multiVerified.results[0].value), presentLookup.value);
  assert.equal(multiVerified.results[1].absence, true);
  const decodedMultiProof = wasm.multiKeyProofFromNodeBytes(
    multiProof.root,
    multiProof.keys,
    multiProof.pathNodeBytes,
  );
  assert.equal(
    toHex(wasm.verifyMultiKeyProof(
      decodedMultiProof.root,
      decodedMultiProof.keys,
      decodedMultiProof.pathNodeBytes,
    ).results[0].value),
    presentLookup.value,
  );
  const decodedMultiProofFromBytes = wasm.multiKeyProofFromBytes(
    wasm.multiKeyProofToBytes(multiProof.root, multiProof.keys, multiProof.pathNodeBytes),
  );
  assert.equal(
    toHex(wasm.verifyMultiKeyProof(
      decodedMultiProofFromBytes.root,
      decodedMultiProofFromBytes.keys,
      decodedMultiProofFromBytes.pathNodeBytes,
    ).results[0].value),
    presentLookup.value,
  );

  const rangeFixture = treeFixture.ranges[0];
  const range = engine.range(
    tree,
    fromHex(rangeFixture.start),
    rangeFixture.end === null ? null : fromHex(rangeFixture.end),
  );
  assert.deepEqual(
    range.map((entry: any) => ({ key: toHex(entry.key), value: toHex(entry.value) })),
    rangeFixture.entries,
  );
  const rangeProof = engine.proveRange(
    tree,
    fromHex(rangeFixture.start),
    rangeFixture.end === null ? null : fromHex(rangeFixture.end),
  );
  const rangeVerified = wasm.verifyRangeProof(
    rangeProof.root,
    rangeProof.start,
    rangeProof.end,
    rangeProof.pathNodeBytes,
  );
  assert.equal(rangeVerified.valid, true);
  assert.deepEqual(
    rangeVerified.entries.map((entry: any) => ({ key: toHex(entry.key), value: toHex(entry.value) })),
    rangeFixture.entries,
  );
  const decodedRangeProof = wasm.rangeProofFromNodeBytes(
    rangeProof.root,
    rangeProof.start,
    rangeProof.end,
    rangeProof.pathNodeBytes,
  );
  assert.deepEqual(
    wasm.verifyRangeProof(
      decodedRangeProof.root,
      decodedRangeProof.start,
      decodedRangeProof.end,
      decodedRangeProof.pathNodeBytes,
    ).entries.map((entry: any) => ({ key: toHex(entry.key), value: toHex(entry.value) })),
    rangeFixture.entries,
  );
  const decodedRangeProofFromBytes = wasm.rangeProofFromBytes(
    wasm.rangeProofToBytes(rangeProof.root, rangeProof.start, rangeProof.end, rangeProof.pathNodeBytes),
  );
  assert.deepEqual(
    wasm.verifyRangeProof(
      decodedRangeProofFromBytes.root,
      decodedRangeProofFromBytes.start,
      decodedRangeProofFromBytes.end,
      decodedRangeProofFromBytes.pathNodeBytes,
    ).entries.map((entry: any) => ({ key: toHex(entry.key), value: toHex(entry.value) })),
    rangeFixture.entries,
  );
  const prefixProof = engine.provePrefix(tree, fromHex(presentLookup.key).slice(0, 1));
  const prefixVerified = wasm.verifyRangeProof(
    prefixProof.root,
    prefixProof.start,
    prefixProof.end,
    prefixProof.pathNodeBytes,
  );
  assert.equal(prefixVerified.valid, true);
  assert.ok(prefixVerified.entries.some((entry: any) => toHex(entry.key) === presentLookup.key));
  const provedPage = engine.proveRangePage(tree, null, null, 1);
  const pageVerified = wasm.verifyRangePageProof(
    provedPage.proof.root,
    provedPage.proof.after,
    provedPage.proof.end,
    provedPage.proof.pathNodeBytes,
  );
  assert.equal(pageVerified.valid, true);
  assert.deepEqual(
    pageVerified.entries.map((entry: any) => ({ key: toHex(entry.key), value: toHex(entry.value) })),
    provedPage.page.entries.map((entry: any) => ({ key: toHex(entry.key), value: toHex(entry.value) })),
  );
  const decodedPageProof = wasm.rangePageProofFromNodeBytes(
    provedPage.proof.root,
    provedPage.proof.after,
    provedPage.proof.end,
    provedPage.proof.pathNodeBytes,
  );
  assert.deepEqual(
    wasm.verifyRangePageProof(
      decodedPageProof.root,
      decodedPageProof.after,
      decodedPageProof.end,
      decodedPageProof.pathNodeBytes,
    ).entries.map((entry: any) => toHex(entry.key)),
    provedPage.page.entries.map((entry: any) => toHex(entry.key)),
  );
  const decodedPageProofFromBytes = wasm.rangePageProofFromBytes(
    wasm.rangePageProofToBytes(
      provedPage.proof.root,
      provedPage.proof.after,
      provedPage.proof.end,
      provedPage.proof.pathNodeBytes,
    ),
  );
  assert.deepEqual(
    wasm.verifyRangePageProof(
      decodedPageProofFromBytes.root,
      decodedPageProofFromBytes.after,
      decodedPageProofFromBytes.end,
      decodedPageProofFromBytes.pathNodeBytes,
    ).entries.map((entry: any) => toHex(entry.key)),
    provedPage.page.entries.map((entry: any) => toHex(entry.key)),
  );
  let otherTree = engine.put(tree, fromHex(rangeFixture.entries[0].key), utf8("changed-0"));
  otherTree = engine.put(otherTree, fromHex(rangeFixture.entries[1].key), utf8("changed-1"));
  const provedDiffPage = engine.proveDiffPage(tree, otherTree, null, null, 1);
  assert.equal(provedDiffPage.page.diffs.length, 1);
  assert.equal(provedDiffPage.page.diffs[0].kind, "changed");
  assert.equal(toHex(provedDiffPage.page.diffs[0].key), rangeFixture.entries[0].key);
  assert.equal(toHex(provedDiffPage.page.nextCursor.afterKey), rangeFixture.entries[0].key);
  assert.equal(toHex(provedDiffPage.proof.base.end), rangeFixture.entries[1].key);
  assert.equal(toHex(provedDiffPage.proof.lookaheadBase.key), rangeFixture.entries[1].key);
  const diffPageVerified = wasm.verifyDiffPageProof(provedDiffPage.proof);
  assert.equal(diffPageVerified.valid, true);
  assert.equal(diffPageVerified.lookaheadValid, true);
  assert.deepEqual(
    diffPageVerified.diffs.map((diff: any) => ({ kind: diff.kind, key: toHex(diff.key) })),
    provedDiffPage.page.diffs.map((diff: any) => ({ kind: diff.kind, key: toHex(diff.key) })),
  );
  assert.equal(toHex(diffPageVerified.nextCursor.afterKey), rangeFixture.entries[0].key);
  const diffPageProofBytes = wasm.diffPageProofToBytes(provedDiffPage.proof);
  assert.deepEqual(diffPageProofBytes, wasm.diffPageProofToBytes(provedDiffPage.proof));
  const diffPageSummary = wasm.inspectProofBundle(diffPageProofBytes);
  assert.equal(diffPageSummary.kind, "diff_page");
  assert.equal(toHex(diffPageSummary.root), treeFixture.root);
  assert.equal(toHex(diffPageSummary.otherRoot), toHex(otherTree.root));
  assert.equal(diffPageSummary.limit, "1");
  assert.equal(diffPageSummary.hasLookahead, true);
  const diffPageBundleVerified = wasm.verifyProofBundle(diffPageProofBytes);
  assert.equal(diffPageBundleVerified.valid, true);
  assert.equal(diffPageBundleVerified.summary.kind, "diff_page");
  assert.equal(diffPageBundleVerified.diffCount, "1");
  assert.equal(toHex(diffPageBundleVerified.nextCursor.afterKey), rangeFixture.entries[0].key);
  const decodedDiffPageProof = wasm.diffPageProofFromBytes(diffPageProofBytes);
  assert.deepEqual(
    wasm.verifyDiffPageProof(decodedDiffPageProof).diffs.map((diff: any) => toHex(diff.key)),
    provedDiffPage.page.diffs.map((diff: any) => toHex(diff.key)),
  );
  const signedEnvelope = wasm.signProofBundleHmacSha256(
    wasm.keyProofToBytes(proof.root, proof.key, proof.pathNodeBytes),
    utf8("wasm-key"),
    utf8("shared secret"),
    utf8("tenant=t1"),
    "1700000000000",
    "1700000100000",
    utf8("nonce-1"),
  );
  const signedEnvelopeBytes = wasm.authenticatedProofEnvelopeToBytes(
    signedEnvelope.algorithm,
    signedEnvelope.keyId,
    signedEnvelope.proofBundle,
    signedEnvelope.context,
    signedEnvelope.issuedAtMillis,
    signedEnvelope.expiresAtMillis,
    signedEnvelope.nonce,
    signedEnvelope.signature,
  );
  assert.deepEqual(
    signedEnvelopeBytes,
    wasm.authenticatedProofEnvelopeToBytes(
      signedEnvelope.algorithm,
      signedEnvelope.keyId,
      signedEnvelope.proofBundle,
      signedEnvelope.context,
      signedEnvelope.issuedAtMillis,
      signedEnvelope.expiresAtMillis,
      signedEnvelope.nonce,
      signedEnvelope.signature,
    ),
  );
  const decodedEnvelope = wasm.authenticatedProofEnvelopeFromBytes(signedEnvelopeBytes);
  const envelopeVerified = wasm.verifyAuthenticatedProofEnvelope(
    decodedEnvelope.algorithm,
    decodedEnvelope.keyId,
    decodedEnvelope.proofBundle,
    decodedEnvelope.context,
    decodedEnvelope.issuedAtMillis,
    decodedEnvelope.expiresAtMillis,
    decodedEnvelope.nonce,
    decodedEnvelope.signature,
    utf8("shared secret"),
    "1700000050000",
  );
  assert.equal(envelopeVerified.valid, true);
  assert.equal(envelopeVerified.signatureValid, true);
  assert.equal(Buffer.from(envelopeVerified.keyId).toString(), "wasm-key");
  assert.equal(Buffer.from(envelopeVerified.context).toString(), "tenant=t1");
  const decodedSignedProof = wasm.keyProofFromBytes(envelopeVerified.proofBundle);
  assert.equal(
    wasm.verifyKeyProof(
      decodedSignedProof.root,
      decodedSignedProof.key,
      decodedSignedProof.pathNodeBytes,
    ).valid,
    true,
  );
  const authenticatedBundle = wasm.verifyAuthenticatedProofBundle(
    signedEnvelopeBytes,
    utf8("shared secret"),
    "1700000050000",
  );
  assert.equal(authenticatedBundle.valid, true);
  assert.equal(authenticatedBundle.envelope.valid, true);
  assert.equal(authenticatedBundle.proofError, null);
  assert.equal(authenticatedBundle.proof.existsCount, "1");
  const wrongEnvelope = wasm.verifyAuthenticatedProofEnvelope(
    decodedEnvelope.algorithm,
    decodedEnvelope.keyId,
    decodedEnvelope.proofBundle,
    decodedEnvelope.context,
    decodedEnvelope.issuedAtMillis,
    decodedEnvelope.expiresAtMillis,
    decodedEnvelope.nonce,
    decodedEnvelope.signature,
    utf8("wrong secret"),
    "1700000050000",
  );
  assert.equal(wrongEnvelope.valid, false);
  const wrongBundle = wasm.verifyAuthenticatedProofBundle(
    signedEnvelopeBytes,
    utf8("wrong secret"),
    "1700000050000",
  );
  assert.equal(wrongBundle.valid, false);
  assert.equal(wrongBundle.envelope.valid, false);
  assert.equal(wrongBundle.proof, null);

  const statsBase = engine.buildFromSortedEntries([
    { key: utf8("a"), value: utf8("1") },
    { key: utf8("b"), value: utf8("2") },
    { key: utf8("c"), value: utf8("3") },
  ]);
  const batchStats = engine.batchWithStats(engine.create(), [
    { kind: "upsert", key: utf8("b"), value: utf8("2") },
    { kind: "upsert", key: utf8("a"), value: utf8("1") },
    { kind: "upsert", key: utf8("a"), value: utf8("11") },
  ]);
  assert.equal(Buffer.from(engine.get(batchStats.tree, utf8("a"))).toString(), "11");
  assert.equal(batchStats.stats.inputMutations, 3);
  assert.equal(batchStats.stats.effectiveMutations, 2);
  assert.equal(batchStats.stats.preprocessInputSorted, false);

  const parallelConfig = wasm.defaultParallelConfig();
  assert.equal(parallelConfig.parallelismThreshold, "100");
  const parallelTree = engine.parallelBatch(engine.create(), [
    { kind: "upsert", key: utf8("p"), value: utf8("parallel") },
    { kind: "upsert", key: utf8("q"), value: utf8("wasm") },
  ], { ...parallelConfig, maxThreads: 1, parallelismThreshold: 1 });
  assert.equal(Buffer.from(engine.get(parallelTree, utf8("q"))).toString(), "wasm");

  const appendedStats = engine.appendBatchWithStats(statsBase, [
    { kind: "upsert", key: utf8("d"), value: utf8("4") },
    { kind: "upsert", key: utf8("e"), value: utf8("5") },
    { kind: "upsert", key: utf8("d"), value: utf8("44") },
  ]);
  assert.equal(Buffer.from(engine.get(appendedStats.tree, utf8("d"))).toString(), "44");
  assert.equal(appendedStats.stats.inputMutations, 3);
  assert.equal(appendedStats.stats.effectiveMutations, 2);
  assert.equal(appendedStats.stats.preprocessInputSorted, false);
  assert.equal(appendedStats.stats.usedAppendFastPath, true);
  assert.ok(appendedStats.stats.writtenNodes > 0);

  const base = engine.put(engine.create(), utf8("k"), utf8("base"));
  const left = engine.put(base, utf8("k"), utf8("left"));
  const right = engine.put(base, utf8("k"), utf8("right"));

  const diffPage = engine.diffPage(base, right, null, null, 1);
  assert.equal(diffPage.diffs.length, 1);
  assert.equal(diffPage.diffs[0].kind, "changed");

  const rangeDiff = engine.rangeDiff(base, right, utf8("a"), null);
  assert.equal(rangeDiff.length, 1);
  assert.equal(rangeDiff[0].kind, "changed");

  const cursorChanged = engine.batch(statsBase, [
    { kind: "upsert", key: utf8("b"), value: utf8("22") },
    { kind: "upsert", key: utf8("c"), value: utf8("33") },
  ]);
  const resumedDiffs = engine.diffFromCursor(statsBase, cursorChanged, new wasm.WasmRangeCursor(utf8("a")), utf8("c"));
  assert.deepEqual(
    resumedDiffs.map((diff) => [diff.kind, Buffer.from(diff.key).toString()]),
    [["changed", "b"]],
  );

  const structuralDiffPage = engine.structuralDiffPage(base, right, null, 1);
  assert.equal(structuralDiffPage.diffs.length, 1);
  assert.equal(structuralDiffPage.stats.emittedDiffs, 1);

  const conflictPage = engine.conflictPage(base, left, right, null, 1);
  assert.equal(conflictPage.conflicts.length, 1);
  assert.equal(Buffer.from(conflictPage.conflicts[0].key).toString(), "k");
  assert.equal(Buffer.from(conflictPage.conflicts[0].left).toString(), "left");
  assert.equal(Buffer.from(conflictPage.conflicts[0].right).toString(), "right");

  const merged = engine.merge(base, left, right, "prefer_right");
  assert.equal(Buffer.from(engine.get(merged, utf8("k"))).toString(), "right");
  assert.equal(Buffer.from(engine.get(engine.mergeRange(base, left, right, utf8("a"), null, "prefer_right"), utf8("k"))).toString(), "right");
  assert.equal(Buffer.from(engine.get(engine.mergePrefix(base, left, right, utf8("k"), "prefer_left"), utf8("k"))).toString(), "left");

  const explanation = engine.mergeExplain(base, left, right, "prefer_right");
  assert.ok(explanation.result);
  assert.equal(explanation.error, null);
  assert.ok(JSON.parse(explanation.traceJson).events.length >= 1);

  assert.ok(JSON.parse(engine.collectStatsJson(merged)).num_nodes >= 1);
  assert.ok(JSON.parse(engine.statsDiffJson(base, merged)));
  assert.ok(JSON.parse(engine.debugTreeJson(merged)).levels.length >= 1);
  assert.match(engine.debugTreeText(merged), /level/i);
  assert.ok(JSON.parse(engine.debugCompareTreesJson(base, merged)));
  assert.match(engine.debugCompareTreesText(base, merged), /left_only/i);
});
