package build.crab.prolly;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ProllyParityTest {
    @Test
    void batchGetManyPagesAndDiffPagesWorkThroughJavaFacade() throws Exception {
        Prolly.useLocalDebugLibrary();

        try (Prolly prolly = Prolly.memory()) {
            assertEquals(EncodingKind.RAW, Prolly.defaultConfig().getEncoding().getKind());
            assertEquals(EncodingKind.RAW, Prolly.encodingRaw().getKind());
            assertEquals(EncodingKind.CBOR, Prolly.encodingCbor().getKind());
            assertEquals(EncodingKind.JSON, Prolly.encodingJson().getKind());
            EncodingRecord customEncoding = Prolly.encodingCustom("postcard");
            assertEquals(EncodingKind.CUSTOM, customEncoding.getKind());
            assertEquals("postcard", customEncoding.getCustomName());
            ConfigRecord constructedConfig = Prolly.treeConfig(2, 64, 32, 7, customEncoding, 16L, 4096L);
            assertEquals(EncodingKind.CUSTOM, constructedConfig.getEncoding().getKind());
            assertEquals(16L, Prolly.configNodeCacheMaxNodes(constructedConfig));
            assertEquals(1L, Prolly.parallelConfigMaxThreads(Prolly.parallelConfigSequential()));

            TreeRecord empty = prolly.create();
            TreeRecord tree = prolly.batch(
                    empty,
                    List.of(
                            Prolly.upsert(bytes("a"), bytes("1")),
                            Prolly.upsert(bytes("b"), bytes("2")),
                            Prolly.upsert(bytes("a"), bytes("11")),
                            Prolly.deleteMutation(bytes("missing"))));

            List<byte[]> values = prolly.getMany(tree, List.of(bytes("a"), bytes("missing"), bytes("b")));
            assertArrayEquals(bytes("11"), values.get(0));
            assertNull(values.get(1));
            assertArrayEquals(bytes("2"), values.get(2));

            KeyProof proof = prolly.proveKey(tree, bytes("a"));
            KeyProofVerification verifiedProof = Prolly.verifyKeyProof(proof);
            assertTrue(verifiedProof.valid());
            assertTrue(verifiedProof.exists());
            assertFalse(verifiedProof.absence());
            assertArrayEquals(bytes("11"), verifiedProof.value());
            KeyProof decodedProof = Prolly.keyProofFromNodeBytes(
                    proof.root(),
                    proof.key(),
                    proof.pathNodeBytes());
            assertArrayEquals(bytes("11"), Prolly.verifyKeyProof(decodedProof).value());
            byte[] keyProofBytes = Prolly.keyProofToBytes(proof);
            ProofBundleSummary keyProofSummary = Prolly.inspectProofBundle(keyProofBytes);
            assertEquals("key", keyProofSummary.kind());
            assertArrayEquals(proof.root(), keyProofSummary.root());
            assertEquals(1L, keyProofSummary.keyCount());
            assertEquals((long) proof.pathNodeBytes().size(), keyProofSummary.pathNodeCount());
            ProofBundleVerification keyBundleVerified = Prolly.verifyProofBundle(keyProofBytes);
            assertTrue(keyBundleVerified.valid());
            assertEquals("key", keyBundleVerified.summary().kind());
            assertEquals(1L, keyBundleVerified.existsCount());
            assertEquals(0L, keyBundleVerified.absenceCount());
            KeyProof decodedProofFromBytes = Prolly.keyProofFromBytes(keyProofBytes);
            assertArrayEquals(bytes("11"), Prolly.verifyKeyProof(decodedProofFromBytes).value());
            KeyProof absentProof = prolly.proveKey(tree, bytes("missing"));
            KeyProofVerification verifiedAbsence = Prolly.verifyKeyProof(absentProof);
            assertTrue(verifiedAbsence.valid());
            assertFalse(verifiedAbsence.exists());
            assertTrue(verifiedAbsence.absence());
            assertNull(verifiedAbsence.value());
            byte[] tamperedRoot = proof.root();
            tamperedRoot[0] ^= (byte) 0xff;
            assertFalse(Prolly.verifyKeyProof(new KeyProof(
                            tamperedRoot,
                            proof.key(),
                            proof.pathNodeBytes()))
                    .valid());
            MultiKeyProof multiProof = prolly.proveKeys(
                    tree,
                    List.of(bytes("a"), bytes("missing"), bytes("b")));
            MultiKeyProofVerification multiVerified = Prolly.verifyMultiKeyProof(multiProof);
            assertTrue(multiVerified.valid());
            assertEquals(3, multiVerified.results().size());
            assertArrayEquals(bytes("11"), multiVerified.results().get(0).value());
            assertTrue(multiVerified.results().get(1).absence());
            assertArrayEquals(bytes("2"), multiVerified.results().get(2).value());
            MultiKeyProof decodedMulti = Prolly.multiKeyProofFromNodeBytes(
                    multiProof.root(),
                    multiProof.keys(),
                    multiProof.pathNodeBytes());
            assertArrayEquals(bytes("2"), Prolly.verifyMultiKeyProof(decodedMulti).results().get(2).value());
            MultiKeyProof decodedMultiFromBytes =
                    Prolly.multiKeyProofFromBytes(Prolly.multiKeyProofToBytes(multiProof));
            assertArrayEquals(bytes("2"), Prolly.verifyMultiKeyProof(decodedMultiFromBytes).results().get(2).value());
            RangeProof rangeProof = prolly.proveRange(tree, bytes("a"), Optional.of(bytes("c")));
            RangeProofVerification verifiedRange = Prolly.verifyRangeProof(rangeProof);
            assertTrue(verifiedRange.valid());
            assertEquals(2, verifiedRange.entries().size());
            assertEquals(new Entry(bytes("a"), bytes("11")), verifiedRange.entries().get(0));
            RangeProof decodedRange = Prolly.rangeProofFromNodeBytes(
                    rangeProof.root(),
                    rangeProof.start(),
                    rangeProof.end(),
                    rangeProof.pathNodeBytes());
            assertEquals(new Entry(bytes("b"), bytes("2")), Prolly.verifyRangeProof(decodedRange).entries().get(1));
            RangeProof decodedRangeFromBytes = Prolly.rangeProofFromBytes(Prolly.rangeProofToBytes(rangeProof));
            assertEquals(new Entry(bytes("b"), bytes("2")), Prolly.verifyRangeProof(decodedRangeFromBytes).entries().get(1));
            RangeProof prefixProof = prolly.provePrefix(tree, bytes("a"));
            RangeProofVerification verifiedPrefix = Prolly.verifyRangeProof(prefixProof);
            assertTrue(verifiedPrefix.valid());
            assertEquals(List.of(new Entry(bytes("a"), bytes("11"))), verifiedPrefix.entries());
            ProvedRangePage provedPage =
                    prolly.proveRangePage(tree, new RangeCursorRecord(bytes("a")), Optional.empty(), 1);
            assertEquals(1, provedPage.page().getEntries().size());
            assertArrayEquals(bytes("b"), provedPage.page().getEntries().get(0).getKey());
            RangePageProofVerification pageVerified = Prolly.verifyRangePageProof(provedPage.proof());
            assertTrue(pageVerified.valid());
            assertEquals(List.of(new Entry(bytes("b"), bytes("2"))), pageVerified.entries());
            RangePageProof decodedPage = Prolly.rangePageProofFromNodeBytes(
                    provedPage.proof().root(),
                    provedPage.proof().after(),
                    provedPage.proof().end(),
                    provedPage.proof().pathNodeBytes());
            assertEquals(new Entry(bytes("b"), bytes("2")), Prolly.verifyRangePageProof(decodedPage).entries().get(0));
            RangePageProof decodedPageFromBytes =
                    Prolly.rangePageProofFromBytes(Prolly.rangePageProofToBytes(provedPage.proof()));
            assertEquals(new Entry(bytes("b"), bytes("2")), Prolly.verifyRangePageProof(decodedPageFromBytes).entries().get(0));
            TreeRecord diffProofOther = prolly.delete(tree, bytes("a"));
            diffProofOther = prolly.put(diffProofOther, bytes("b"), bytes("22"));
            diffProofOther = prolly.put(diffProofOther, bytes("d"), bytes("4"));
            ProvedDiffPage provedDiffPage =
                    prolly.proveDiffPage(tree, diffProofOther, null, Optional.empty(), 1);
            assertEquals(1, provedDiffPage.page().getDiffs().size());
            DiffRecord firstProvedDiff = provedDiffPage.page().getDiffs().get(0);
            assertEquals(DiffKind.REMOVED, firstProvedDiff.getKind());
            assertArrayEquals(bytes("a"), firstProvedDiff.getKey());
            assertArrayEquals(bytes("11"), firstProvedDiff.getValue());
            assertArrayEquals(bytes("a"), provedDiffPage.page().getNextCursor().getAfterKey());
            assertArrayEquals(bytes("b"), provedDiffPage.proof().base().end());
            assertNotNull(provedDiffPage.proof().lookaheadBase());
            assertNotNull(provedDiffPage.proof().lookaheadOther());
            assertArrayEquals(bytes("b"), provedDiffPage.proof().lookaheadBase().key());
            assertArrayEquals(bytes("b"), provedDiffPage.proof().lookaheadOther().key());
            DiffPageProofVerification diffPageVerified = Prolly.verifyDiffPageProof(provedDiffPage.proof());
            assertTrue(diffPageVerified.valid());
            assertTrue(diffPageVerified.baseValid());
            assertTrue(diffPageVerified.otherValid());
            assertTrue(diffPageVerified.lookaheadValid());
            assertEquals(1, diffPageVerified.limit());
            assertEquals(1, diffPageVerified.diffs().size());
            DiffRecord firstVerifiedDiff = diffPageVerified.diffs().get(0);
            assertEquals(DiffKind.REMOVED, firstVerifiedDiff.getKind());
            assertArrayEquals(bytes("a"), firstVerifiedDiff.getKey());
            assertArrayEquals(bytes("11"), firstVerifiedDiff.getValue());
            assertArrayEquals(bytes("a"), diffPageVerified.nextCursor().getAfterKey());
            byte[] diffPageProofBytes = Prolly.diffPageProofToBytes(provedDiffPage.proof());
            assertArrayEquals(diffPageProofBytes, Prolly.diffPageProofToBytes(provedDiffPage.proof()));
            ProofBundleSummary diffPageProofSummary = Prolly.inspectProofBundle(diffPageProofBytes);
            assertEquals("diff_page", diffPageProofSummary.kind());
            assertArrayEquals(tree.getRoot(), diffPageProofSummary.root());
            assertArrayEquals(diffProofOther.getRoot(), diffPageProofSummary.otherRoot());
            assertEquals(1L, diffPageProofSummary.limit());
            assertTrue(diffPageProofSummary.hasLookahead());
            ProofBundleVerification diffPageBundleVerified = Prolly.verifyProofBundle(diffPageProofBytes);
            assertTrue(diffPageBundleVerified.valid());
            assertEquals("diff_page", diffPageBundleVerified.summary().kind());
            assertEquals(1L, diffPageBundleVerified.diffCount());
            assertArrayEquals(bytes("a"), diffPageBundleVerified.nextCursor().getAfterKey());
            DiffPageProof decodedDiffPageProof = Prolly.diffPageProofFromBytes(diffPageProofBytes);
            DiffPageProofVerification decodedDiffPageVerified = Prolly.verifyDiffPageProof(decodedDiffPageProof);
            assertTrue(decodedDiffPageVerified.valid());
            assertEquals(DiffKind.REMOVED, decodedDiffPageVerified.diffs().get(0).getKind());
            assertArrayEquals(bytes("a"), decodedDiffPageVerified.diffs().get(0).getKey());
            AuthenticatedProofEnvelope signedEnvelope = Prolly.signProofBundleHmacSha256(
                    Prolly.keyProofToBytes(proof),
                    bytes("java-key"),
                    bytes("shared secret"),
                    bytes("tenant=t1"),
                    1_700_000_000_000L,
                    1_700_000_100_000L,
                    bytes("nonce-1"));
            byte[] signedEnvelopeBytes = Prolly.authenticatedProofEnvelopeToBytes(signedEnvelope);
            assertArrayEquals(signedEnvelopeBytes, Prolly.authenticatedProofEnvelopeToBytes(signedEnvelope));
            AuthenticatedProofEnvelope decodedEnvelope =
                    Prolly.authenticatedProofEnvelopeFromBytes(signedEnvelopeBytes);
            AuthenticatedProofEnvelopeVerification envelopeVerified =
                    Prolly.verifyAuthenticatedProofEnvelope(
                            decodedEnvelope,
                            bytes("shared secret"),
                            1_700_000_050_000L);
            assertTrue(envelopeVerified.valid());
            assertTrue(envelopeVerified.signatureValid());
            assertArrayEquals(bytes("java-key"), envelopeVerified.keyId());
            assertArrayEquals(bytes("tenant=t1"), envelopeVerified.context());
            assertArrayEquals(
                    bytes("11"),
                    Prolly.verifyKeyProof(Prolly.keyProofFromBytes(envelopeVerified.proofBundle())).value());
            AuthenticatedProofBundleVerification authenticatedBundle =
                    Prolly.verifyAuthenticatedProofBundle(
                            signedEnvelopeBytes,
                            bytes("shared secret"),
                            1_700_000_050_000L);
            assertTrue(authenticatedBundle.valid());
            assertTrue(authenticatedBundle.envelope().valid());
            assertNull(authenticatedBundle.proofError());
            assertNotNull(authenticatedBundle.proof());
            assertEquals(1L, authenticatedBundle.proof().existsCount());
            AuthenticatedProofEnvelopeVerification wrongEnvelope =
                    Prolly.verifyAuthenticatedProofEnvelope(
                            decodedEnvelope,
                            bytes("wrong secret"),
                            1_700_000_050_000L);
            assertFalse(wrongEnvelope.valid());
            AuthenticatedProofBundleVerification wrongBundle =
                    Prolly.verifyAuthenticatedProofBundle(
                            signedEnvelopeBytes,
                            bytes("wrong secret"),
                            1_700_000_050_000L);
            assertFalse(wrongBundle.valid());
            assertFalse(wrongBundle.envelope().valid());
            assertNull(wrongBundle.proof());

            TreeRecord built = prolly.buildFromEntries(List.of(
                    new Entry(bytes("c"), bytes("3")),
                    new Entry(bytes("a"), bytes("1")),
                    new Entry(bytes("b"), bytes("2"))));
            TreeRecord sortedBuilt = prolly.buildFromSortedEntries(List.of(
                    new Entry(bytes("a"), bytes("1")),
                    new Entry(bytes("b"), bytes("2")),
                    new Entry(bytes("c"), bytes("3"))));
            assertArrayEquals(built.getRoot(), sortedBuilt.getRoot());
            assertTrue(throwsAny(() -> prolly.buildFromSortedEntries(List.of(
                    new Entry(bytes("b"), bytes("2")),
                    new Entry(bytes("a"), bytes("1"))))));
            assertEquals(MutationKind.UPSERT, Prolly.upsertMutation(bytes("probe"), bytes("value")).getKind());
            assertEquals(MutationKind.DELETE, Prolly.deleteMutation(bytes("probe")).getKind());
            BatchApplyResult batchStats = prolly.batchWithStats(
                    empty,
                    List.of(
                            Prolly.upsertMutation(bytes("b"), bytes("2")),
                            Prolly.upsertMutation(bytes("a"), bytes("1")),
                            Prolly.upsertMutation(bytes("a"), bytes("11"))));
            assertArrayEquals(bytes("11"), prolly.get(batchStats.tree(), bytes("a")).orElseThrow());
            assertEquals(3, batchStats.stats().inputMutations());
            assertEquals(2, batchStats.stats().effectiveMutations());
            assertFalse(batchStats.stats().preprocessInputSorted());

            TreeRecord parallelTree = prolly.parallelBatch(
                    empty,
                    List.of(
                            Prolly.upsert(bytes("p"), bytes("parallel")),
                            Prolly.upsert(bytes("q"), bytes("java"))),
                    Prolly.parallelConfig(1, 1));
            assertArrayEquals(bytes("java"), prolly.get(parallelTree, bytes("q")).orElseThrow());
            BatchApplyResult parallelStats = prolly.parallelBatchWithStats(
                    empty,
                    List.of(
                            Prolly.upsert(bytes("r"), bytes("route")),
                            Prolly.upsert(bytes("s"), bytes("stats"))),
                    Prolly.parallelConfig(1, 1));
            assertArrayEquals(bytes("stats"), prolly.get(parallelStats.tree(), bytes("s")).orElseThrow());
            assertEquals(2, parallelStats.stats().inputMutations());
            assertEquals(2, parallelStats.stats().effectiveMutations());
            assertTrue(parallelStats.stats().writtenNodes() > 0);

            TreeRecord appended = prolly.appendBatch(
                    built,
                    List.of(
                            Prolly.upsert(bytes("d"), bytes("4")),
                            Prolly.upsert(bytes("e"), bytes("5")),
                            Prolly.upsert(bytes("d"), bytes("44"))));
            assertArrayEquals(bytes("44"), prolly.get(appended, bytes("d")).orElseThrow());
            BatchApplyResult appendedStats = prolly.appendBatchWithStats(
                    built,
                    List.of(
                            Prolly.upsert(bytes("d"), bytes("4")),
                            Prolly.upsert(bytes("e"), bytes("5")),
                            Prolly.upsert(bytes("d"), bytes("44"))));
            assertArrayEquals(bytes("44"), prolly.get(appendedStats.tree(), bytes("d")).orElseThrow());
            assertEquals(3, appendedStats.stats().inputMutations());
            assertEquals(2, appendedStats.stats().effectiveMutations());
            assertFalse(appendedStats.stats().preprocessInputSorted());
            assertTrue(appendedStats.stats().usedAppendFastPath());
            assertTrue(appendedStats.stats().writtenNodes() > 0);

            RangePageRecord firstPage = prolly.rangePage(tree, null, Optional.empty(), 1);
            assertEquals(1, firstPage.getEntries().size());
            assertArrayEquals(bytes("a"), firstPage.getEntries().get(0).getKey());
            assertNotNull(firstPage.getNextCursor());

            List<Entry> afterA = prolly.rangeAfter(tree, bytes("a"), Optional.empty());
            assertEquals(1, afterA.size());
            assertArrayEquals(bytes("b"), afterA.get(0).key());
            assertNull(Prolly.rangeCursorStart().getAfterKey());
            RangeCursorRecord afterACursor = Prolly.rangeCursorAfterKey(bytes("a"));
            assertArrayEquals(bytes("a"), afterACursor.getAfterKey());
            List<Entry> fromCursor = prolly.rangeFromCursor(tree, afterACursor, Optional.empty());
            assertEquals(1, fromCursor.size());
            assertArrayEquals(afterA.get(0).key(), fromCursor.get(0).key());
            assertArrayEquals(bytes("a"), prolly.firstEntry(tree).orElseThrow().key());
            assertArrayEquals(bytes("11"), prolly.firstEntry(tree).orElseThrow().value());
            assertArrayEquals(bytes("b"), prolly.lastEntry(tree).orElseThrow().key());
            assertArrayEquals(bytes("b"), prolly.lowerBound(tree, bytes("aa")).orElseThrow().key());
            assertTrue(prolly.upperBound(tree, bytes("b")).isEmpty());
            List<Entry> prefixEntries = prolly.prefix(tree, bytes("a"));
            assertEquals(1, prefixEntries.size());
            assertArrayEquals(bytes("11"), prefixEntries.get(0).value());
            RangePageRecord prefixPage = prolly.prefixPage(tree, bytes("a"), null, 1);
            assertEquals(1, prefixPage.getEntries().size());
            assertArrayEquals(bytes("11"), prefixPage.getEntries().get(0).getValue());
            assertNotNull(prefixPage.getNextCursor());

            CursorWindowRecord window = prolly.cursorWindow(tree, bytes("aa"), Optional.empty(), 1);
            assertArrayEquals(bytes("a"), window.getPositionKey());
            assertArrayEquals(bytes("11"), window.getPositionValue());
            assertFalse(window.getFound());
            assertEquals(1, window.getEntries().size());
            assertArrayEquals(bytes("b"), window.getEntries().get(0).getKey());
            assertArrayEquals(bytes("b"), window.getNextCursor().getAfterKey());

            CursorWindowRecord exactProbe = prolly.cursorWindow(tree, bytes("a"), Optional.empty(), 0);
            assertTrue(exactProbe.getFound());
            assertArrayEquals(bytes("a"), exactProbe.getPositionKey());
            assertEquals(0, exactProbe.getEntries().size());
            assertNull(exactProbe.getNextCursor());

            RangePageRecord secondPage = prolly.rangePage(tree, firstPage.getNextCursor(), Optional.empty(), 1);
            assertEquals(1, secondPage.getEntries().size());
            assertArrayEquals(bytes("b"), secondPage.getEntries().get(0).getKey());
            if (secondPage.getNextCursor() != null) {
                RangePageRecord thirdPage = prolly.rangePage(tree, secondPage.getNextCursor(), Optional.empty(), 1);
                assertEquals(0, thirdPage.getEntries().size());
                assertNull(thirdPage.getNextCursor());
            }

            assertNull(Prolly.reverseCursorEnd().getBeforeKey());
            ReverseCursorRecord beforeCCursor = Prolly.reverseCursorBeforeKey(bytes("c"));
            assertArrayEquals(bytes("c"), beforeCCursor.getBeforeKey());
            ReversePageRecord reverseFirst = prolly.reversePage(built, null, new byte[0], 2);
            assertEquals(2, reverseFirst.getEntries().size());
            assertArrayEquals(bytes("c"), reverseFirst.getEntries().get(0).getKey());
            assertArrayEquals(bytes("b"), reverseFirst.getEntries().get(1).getKey());
            assertArrayEquals(bytes("b"), reverseFirst.getNextCursor().getBeforeKey());
            ReversePageRecord reverseSecond = prolly.reversePage(built, reverseFirst.getNextCursor(), new byte[0], 2);
            assertEquals(1, reverseSecond.getEntries().size());
            assertArrayEquals(bytes("a"), reverseSecond.getEntries().get(0).getKey());
            assertNull(reverseSecond.getNextCursor());
            ReversePageRecord prefixReverse = prolly.prefixReversePage(built, bytes("b"), null, 8);
            assertEquals(1, prefixReverse.getEntries().size());
            assertArrayEquals(bytes("b"), prefixReverse.getEntries().get(0).getKey());
            assertNull(prefixReverse.getNextCursor());

            TreeRecord changed = prolly.put(tree, bytes("b"), bytes("22"));
            DiffPageRecord diffPage = prolly.diffPage(tree, changed, null, Optional.empty(), 1);
            assertEquals(1, diffPage.getDiffs().size());
            assertEquals(DiffKind.CHANGED, diffPage.getDiffs().get(0).getKind());
            if (diffPage.getNextCursor() != null) {
                DiffPageRecord secondDiffPage = prolly.diffPage(tree, changed, diffPage.getNextCursor(), Optional.empty(), 1);
                assertEquals(0, secondDiffPage.getDiffs().size());
                assertNull(secondDiffPage.getNextCursor());
            }

            TreeRecord changedForCursor = prolly.batch(
                    built,
                    List.of(
                            Prolly.upsert(bytes("b"), bytes("22")),
                            Prolly.upsert(bytes("c"), bytes("33"))));
            List<DiffRecord> resumedDiffs = prolly.diffFromCursor(
                    built,
                    changedForCursor,
                    new RangeCursorRecord(bytes("a")),
                    Optional.of(bytes("c")));
            assertEquals(1, resumedDiffs.size());
            assertEquals(DiffKind.CHANGED, resumedDiffs.get(0).getKind());
            assertArrayEquals(bytes("b"), resumedDiffs.get(0).getKey());

            TreeRecord conflictBase = prolly.batch(
                    empty,
                    List.of(
                            Prolly.upsert(bytes("a"), bytes("base-a")),
                            Prolly.upsert(bytes("b"), bytes("base-b"))));
            TreeRecord conflictLeft = prolly.batch(
                    conflictBase,
                    List.of(
                            Prolly.upsert(bytes("a"), bytes("left-a")),
                            Prolly.upsert(bytes("b"), bytes("left-b"))));
            TreeRecord conflictRight = prolly.batch(
                    conflictBase,
                    List.of(
                            Prolly.upsert(bytes("a"), bytes("right-a")),
                            Prolly.upsert(bytes("b"), bytes("right-b"))));
            ConflictPageRecord conflictPage = prolly.conflictPage(conflictBase, conflictLeft, conflictRight, null, 1);
            assertEquals(1, conflictPage.getConflicts().size());
            ConflictRecord firstConflict = conflictPage.getConflicts().get(0);
            assertArrayEquals(bytes("a"), firstConflict.getKey());
            assertArrayEquals(bytes("base-a"), firstConflict.getBase());
            assertArrayEquals(bytes("left-a"), firstConflict.getLeft());
            assertArrayEquals(bytes("right-a"), firstConflict.getRight());
            assertNotNull(conflictPage.getNextCursor());

            ConflictPageRecord secondConflictPage = prolly.conflictPage(
                    conflictBase, conflictLeft, conflictRight, conflictPage.getNextCursor(), 1);
            assertEquals(1, secondConflictPage.getConflicts().size());
            assertArrayEquals(bytes("b"), secondConflictPage.getConflicts().get(0).getKey());
            assertNull(secondConflictPage.getNextCursor());
        }
    }

    @Test
    void mergeAndNamedRootCasWorkThroughJavaFacade() throws Exception {
        Prolly.useLocalDebugLibrary();

        try (Prolly prolly = Prolly.memory()) {
            TreeRecord empty = prolly.create();
            TreeRecord base = prolly.put(empty, bytes("k"), bytes("base"));
            TreeRecord left = prolly.put(base, bytes("k"), bytes("left"));
            TreeRecord right = prolly.put(base, bytes("k"), bytes("right"));

            MergeExplanationRecord explanation = prolly.mergeExplain(base, left, right, "prefer_right");
            assertNotNull(explanation.getResult());
            assertNull(explanation.getError());
            assertTrue(explanation.getTraceJson().contains("events"));
            assertFalse(explanation.getTrace().getEvents().isEmpty());
            assertTrue(explanation.getTrace().getEvents().stream()
                    .anyMatch(event -> event.getKind() == MergeTraceEventKind.RESOLVER_CALLED
                            && event.getResolution() == MergeTraceResolutionKind.VALUE));

            TreeRecord merged = prolly.merge(base, left, right, "prefer_right");
            assertArrayEquals(bytes("right"), prolly.get(merged, bytes("k")).orElseThrow());

            assertEquals(ResolutionKind.DELETE, Prolly.resolutionDelete().getKind());
            assertEquals(ResolutionKind.UNRESOLVED, Prolly.resolutionUnresolved().getKind());
            ConflictRecord updateConflict =
                    new ConflictRecord(bytes("k"), bytes("base"), bytes("left"), bytes("right"));
            assertArrayEquals(bytes("left"), Prolly.resolvePreferLeft(updateConflict).getValue());
            assertEquals(ResolutionKind.UNRESOLVED, Prolly.resolveDeleteWins(updateConflict).getKind());
            ConflictRecord deleteConflict =
                    new ConflictRecord(bytes("k"), bytes("base"), null, bytes("right"));
            assertEquals(ResolutionKind.DELETE, Prolly.resolveDeleteWins(deleteConflict).getKind());
            assertArrayEquals(bytes("right"), Prolly.resolveUpdateWins(deleteConflict).getValue());
            TreeRecord preferRightCallback = prolly.mergeWithResolver(base, left, right, Prolly::resolvePreferRight);
            assertArrayEquals(bytes("right"), prolly.get(preferRightCallback, bytes("k")).orElseThrow());
            MergeResolverCallback resolver =
                    conflict -> Prolly.resolutionValue(concat(conflict.getLeft(), bytes("|"), conflict.getRight()));
            TreeRecord callbackMerged = prolly.mergeWithResolver(base, left, right, resolver);
            assertArrayEquals(bytes("left|right"), prolly.get(callbackMerged, bytes("k")).orElseThrow());

            TreeRecord policyBase = prolly.batch(
                    empty,
                    List.of(
                            Prolly.upsert(bytes("doc/title"), bytes("base-title")),
                            Prolly.upsert(bytes("k"), bytes("base-k"))));
            TreeRecord policyLeft = prolly.batch(
                    policyBase,
                    List.of(
                            Prolly.upsert(bytes("doc/title"), bytes("left-title")),
                            Prolly.upsert(bytes("k"), bytes("left-k"))));
            TreeRecord policyRight = prolly.batch(
                    policyBase,
                    List.of(
                            Prolly.upsert(bytes("doc/title"), bytes("right-title")),
                            Prolly.upsert(bytes("k"), bytes("right-k"))));
            try (MergePolicyRegistry policy = Prolly.mergePolicyRegistry()) {
                assertTrue(policy.isEmpty());
                assertFalse(policy.hasDefault());
                policy.setDefaultResolverName("prefer_left");
                policy.pushPrefixResolver(bytes("doc/"), resolver);
                policy.pushExactResolverName(bytes("k"), "prefer_right");
                assertTrue(policy.hasDefault());

                TreeRecord policyMerged = prolly.mergeWithPolicy(policyBase, policyLeft, policyRight, policy);
                assertArrayEquals(bytes("left-title|right-title"), prolly.get(policyMerged, bytes("doc/title")).orElseThrow());
                assertArrayEquals(bytes("right-k"), prolly.get(policyMerged, bytes("k")).orElseThrow());
                assertNotNull(prolly.mergeExplainWithPolicy(policyBase, policyLeft, policyRight, policy).getResult());
                TreeRecord policyRange = prolly.mergeRangeWithPolicy(
                        policyBase,
                        policyLeft,
                        policyRight,
                        bytes("doc/"),
                        Optional.of(bytes("doc0")),
                        policy);
                assertArrayEquals(bytes("left-title|right-title"), prolly.get(policyRange, bytes("doc/title")).orElseThrow());
                TreeRecord policyPrefix =
                        prolly.mergePrefixWithPolicy(policyBase, policyLeft, policyRight, bytes("doc/"), policy);
                assertArrayEquals(bytes("left-title|right-title"), prolly.get(policyPrefix, bytes("doc/title")).orElseThrow());
            }

            byte[] name = bytes("main");
            prolly.publishNamedRootAtMillis(name, merged, 42);
            assertTrue(prolly.loadNamedRoot(name).isPresent());
            assertEquals(1, prolly.listNamedRoots().size());
            List<NamedRootManifest> manifests = prolly.listNamedRootManifests();
            assertEquals(1, manifests.size());
            assertArrayEquals(name, manifests.get(0).name());
            assertArrayEquals(merged.getRoot(), manifests.get(0).manifest().tree().getRoot());
            assertEquals(42L, manifests.get(0).manifest().createdAtMillis());
            assertEquals(42L, manifests.get(0).manifest().updatedAtMillis());

            NamedRootSelectionRecord selection = prolly.loadNamedRoots(List.of(name, bytes("missing")));
            assertEquals(1, selection.getRoots().size());
            assertEquals(1, selection.getMissingNames().size());

            NamedRootRetentionRecord retainAll = Prolly.retainAllNamedRoots();
            assertEquals(NamedRootRetentionKind.ALL, retainAll.getKind());
            NamedRootRetentionRecord retainExact = Prolly.retainExactNamedRoots(List.of(name, bytes("missing")));
            assertEquals(NamedRootRetentionKind.EXACT, retainExact.getKind());
            assertEquals(2, retainExact.getNames().size());
            NamedRootRetentionRecord retainPrefix = Prolly.retainNamedRootPrefix(bytes("ma"));
            assertEquals(NamedRootRetentionKind.PREFIX, retainPrefix.getKind());
            assertArrayEquals(bytes("ma"), retainPrefix.getPrefix());
            NamedRootRetentionRecord retainNewest = Prolly.retainNewestNamedRoots(bytes("checkpoint/"), 2);
            assertEquals(NamedRootRetentionKind.NEWEST_BY_NAME, retainNewest.getKind());
            assertArrayEquals(bytes("checkpoint/"), retainNewest.getPrefix());
            NamedRootRetentionRecord retainUpdated = Prolly.retainNamedRootsUpdatedSince(bytes("checkpoint/"), 42);
            assertEquals(NamedRootRetentionKind.UPDATED_SINCE, retainUpdated.getKind());
            assertArrayEquals(bytes("checkpoint/"), retainUpdated.getPrefix());

            NamedRootSelectionRecord retained = prolly.loadRetainedNamedRoots(retainAll);
            assertEquals(1, retained.getRoots().size());
            GcPlan retainedPlan = prolly.planStoreGcForRetention(retainAll);
            assertTrue(retainedPlan.reachability().liveNodes() > 0);

            SnapshotNamespaceRecord branch = Prolly.snapshotNamespaceBranch();
            SnapshotNamespaceRecord tag = Prolly.snapshotNamespaceTag();
            SnapshotNamespaceRecord custom = Prolly.snapshotNamespaceCustom(bytes("refs/custom/"));
            assertArrayEquals(bytes("refs/heads/main"), Prolly.snapshotRootName(branch, bytes("main")));
            assertArrayEquals(bytes("main"), Prolly.snapshotIdFromName(branch, bytes("refs/heads/main")).orElseThrow());
            assertArrayEquals(bytes("refs/custom/draft"), Prolly.snapshotRootName(custom, bytes("draft")));

            prolly.publishSnapshotAtMillis(branch, bytes("main"), merged, 77);
            assertTrue(prolly.loadSnapshot(branch, bytes("main")).isPresent());
            prolly.publishSnapshot(tag, bytes("v1"), merged);
            List<SnapshotRoot> branchSnapshots = prolly.listSnapshots(branch);
            assertEquals(1, branchSnapshots.size());
            assertArrayEquals(bytes("main"), branchSnapshots.get(0).id());
            assertArrayEquals(bytes("refs/heads/main"), branchSnapshots.get(0).name());
            assertEquals(77, branchSnapshots.get(0).updatedAtMillis());
            List<SnapshotRoot> tagSnapshots = prolly.listSnapshots(tag);
            assertEquals(1, tagSnapshots.size());
            assertArrayEquals(bytes("v1"), tagSnapshots.get(0).id());
            SnapshotSelection snapshotSelection = prolly.loadSnapshots(branch, List.of(bytes("main"), bytes("missing")));
            assertEquals(1, snapshotSelection.snapshots().size());
            assertEquals(1, snapshotSelection.missingIds().size());
            NamedRootUpdateRecord snapshotConflict =
                    prolly.compareAndSwapSnapshot(branch, bytes("main"), Optional.empty(), Optional.empty());
            assertFalse(snapshotConflict.getApplied());
            assertTrue(snapshotConflict.getConflict());
            assertNotNull(snapshotConflict.getCurrent());
            NamedRootUpdateRecord snapshotUpdate =
                    prolly.compareAndSwapSnapshotAtMillis(branch, bytes("main"), Optional.of(merged), Optional.empty(), 88);
            assertTrue(snapshotUpdate.getApplied());
            assertFalse(snapshotUpdate.getConflict());
            assertTrue(prolly.loadSnapshot(branch, bytes("main")).isEmpty());

            NamedRootUpdateRecord update = prolly.compareAndSwapNamedRoot(name, Optional.of(merged), Optional.empty());
            assertTrue(update.getApplied());
            assertFalse(update.getConflict());
            assertTrue(prolly.loadNamedRoot(name).isEmpty());
        }
    }

    @Test
    void crdtTombstoneAndMultiValueHelpersWorkThroughJavaFacade() throws Exception {
        Prolly.useLocalDebugLibrary();

        try (Prolly prolly = Prolly.memory()) {
            TreeRecord empty = prolly.create();
            byte[] baseValue = Prolly.timestampedValueToBytes(Prolly.timestampedValue(bytes("base"), 1));
            byte[] leftValue = Prolly.timestampedValueToBytes(Prolly.timestampedValue(bytes("left"), 2));
            byte[] rightValue = Prolly.timestampedValueToBytes(Prolly.timestampedValue(bytes("right"), 3));

            TreeRecord base = prolly.put(empty, bytes("k"), baseValue);
            TreeRecord left = prolly.put(base, bytes("k"), leftValue);
            TreeRecord right = prolly.put(base, bytes("k"), rightValue);

            CrdtConfigRecord lww = Prolly.crdtConfigLww("update_wins");
            assertEquals(CrdtMergeStrategyKind.LAST_WRITER_WINS, lww.getStrategy());
            assertEquals(CrdtDeletePolicyKind.UPDATE_WINS, lww.getDeletePolicy());
            TreeRecord merged = prolly.crdtMerge(base, left, right, lww);
            TimestampedValueRecord mergedValue =
                    Prolly.timestampedValueFromBytes(prolly.get(merged, bytes("k")).orElseThrow());
            assertArrayEquals(bytes("right"), mergedValue.getValue());
            assertEquals(3, Prolly.timestampedValueTimestamp(mergedValue));

            CrdtResolverCallback resolver =
                    conflict -> Prolly.crdtResolutionValue(concat(conflict.getLeft(), bytes("|"), conflict.getRight()));
            TreeRecord callbackMerged =
                    prolly.crdtMergeWithResolver(base, left, right, CrdtDeletePolicyKind.UPDATE_WINS, resolver);
            assertArrayEquals(
                    concat(leftValue, bytes("|"), rightValue),
                    prolly.get(callbackMerged, bytes("k")).orElseThrow());

            TreeRecord deleteLeft = prolly.delete(base, bytes("k"));
            TreeRecord updateRight = prolly.put(base, bytes("k"), bytes("right"));
            TreeRecord deleted = prolly.crdtMergeWithResolver(
                    base,
                    deleteLeft,
                    updateRight,
                    CrdtDeletePolicyKind.UPDATE_WINS,
                    conflict -> Prolly.crdtResolutionDelete());
            assertTrue(prolly.get(deleted, bytes("k")).isEmpty());

            TimestampedValueRecord now = Prolly.timestampedValueNow(bytes("now"));
            assertArrayEquals(bytes("now"), now.getValue());
            assertTrue(Prolly.timestampedValueTimestamp(now) > 0);

            CrdtConfigRecord multiConfig = Prolly.crdtConfigMultiValue("delete_wins");
            assertEquals(CrdtMergeStrategyKind.MULTI_VALUE, multiConfig.getStrategy());
            assertEquals(CrdtDeletePolicyKind.DELETE_WINS, multiConfig.getDeletePolicy());

            List<byte[]> set = Prolly.multiValueSetFromBytes(
                    Prolly.multiValueSetToBytes(List.of(bytes("b"), bytes("a"), bytes("a"))));
            assertEquals(2, set.size());
            assertArrayEquals(bytes("a"), set.get(0));
            assertArrayEquals(bytes("b"), set.get(1));
            List<byte[]> mergedSet = Prolly.multiValueSetMerge(List.of(bytes("b")), List.of(bytes("a"), bytes("b")));
            assertEquals(2, mergedSet.size());
            assertArrayEquals(bytes("a"), mergedSet.get(0));
            assertArrayEquals(bytes("b"), mergedSet.get(1));

            TombstoneRecord tombstone = Prolly.tombstone(
                    bytes("actor"),
                    7,
                    List.of(Prolly.tombstoneMetadata("clock", bytes("7"))));
            byte[] tombstoneBytes = Prolly.tombstoneToBytes(tombstone);
            assertTrue(Prolly.isTombstoneValue(tombstoneBytes));
            assertEquals(7, Prolly.tombstoneTimestampMillis(Prolly.tombstoneFromBytes(tombstoneBytes)));
            assertEquals("clock", Prolly.tombstoneFromStoredBytes(tombstoneBytes)
                    .orElseThrow()
                    .getCausalMetadata()
                    .get(0)
                    .getKey());

            MutationRecord upsert = Prolly.tombstoneUpsertMutation(bytes("deleted"), tombstone);
            assertEquals(MutationKind.UPSERT, upsert.getKind());
            assertArrayEquals(bytes("deleted"), upsert.getKey());
            assertNotNull(upsert.getValue());

            MutationRecord compaction = Prolly.tombstoneCompactionMutation(bytes("deleted"), tombstoneBytes)
                    .orElseThrow();
            assertEquals(MutationKind.DELETE, compaction.getKind());
            assertArrayEquals(bytes("deleted"), compaction.getKey());
            assertNull(compaction.getValue());
        }
    }

    @Test
    void sqliteEnginePersistsNodesAcrossReopenThroughJavaFacade() throws Exception {
        Prolly.useLocalDebugLibrary();
        Path path = Files.createTempFile("prolly-java", ".db");
        Files.deleteIfExists(path);

        TreeRecord tree;
        try (Prolly first = Prolly.sqlite(path)) {
            tree = first.put(first.create(), bytes("k"), bytes("v"));
        }

        try (Prolly reopened = Prolly.sqlite(path)) {
            assertArrayEquals(bytes("v"), reopened.get(tree, bytes("k")).orElseThrow());
        } finally {
            Files.deleteIfExists(path);
        }
    }

    @Test
    void operationalApisWorkThroughJavaFacade() throws Exception {
        Prolly.useLocalDebugLibrary();

        try (Prolly prolly = Prolly.memory()) {
            TreeRecord empty = prolly.create();
            TreeRecord tree = prolly.put(empty, bytes("k"), bytes("v"));

            assertTrue(prolly.collectStatsJson(tree).contains("\"num_nodes\""));
            TreeStatsRecord typedStats = prolly.collectStats(tree);
            assertEquals(1L, Prolly.treeStatsTotalKeyValuePairs(typedStats));
            assertTrue(Prolly.treeStatsLevelCount(typedStats, 0) > 0);
            assertTrue(prolly.statsDiffJson(empty, tree).contains("\"absolute\""));
            StatsComparisonRecord typedDiffStats = prolly.statsDiff(empty, tree);
            assertEquals(0L, Prolly.statsComparisonBeforeTotalKeyValuePairs(typedDiffStats));
            assertEquals(1L, Prolly.statsComparisonAfterTotalKeyValuePairs(typedDiffStats));
            assertEquals(1L, Prolly.statsDiffTotalKeyValuePairs(typedDiffStats));
            assertTrue(prolly.debugTreeJson(tree).contains("\"levels\""));
            TreeDebugViewRecord debugTree = prolly.debugTree(tree);
            assertTrue(Prolly.treeDebugViewLevelCount(debugTree) > 0);
            assertTrue(Prolly.treeDebugViewFirstLevelNodeCount(debugTree) > 0);
            assertTrue(prolly.debugTreeText(tree).contains("level"));
            assertTrue(prolly.debugCompareTreesJson(empty, tree).contains("\"right_only_nodes\""));
            TreeDebugComparisonRecord debugComparison = prolly.debugCompareTrees(empty, tree);
            assertEquals(0L, Prolly.treeDebugComparisonLeftOnlyNodes(debugComparison));
            assertTrue(Prolly.treeDebugComparisonRightOnlyNodes(debugComparison) > 0);
            assertTrue(Prolly.treeDebugComparisonHasRightOnlyNode(debugComparison));
            assertTrue(prolly.debugCompareTreesText(empty, tree).contains("right"));

            assertTrue(prolly.pinTreePath(tree, bytes("k")) > 0);
            assertTrue(prolly.unpinAllCacheNodes() >= 0);
            assertTrue(prolly.pinTreeRoot(tree) > 0);
            assertTrue(prolly.cacheStats().cachedNodes() > 0);
            assertTrue(prolly.unpinAllCacheNodes() >= 0);
            prolly.clearCache();

            assertTrue(prolly.metrics().nodesWritten() > 0);
            prolly.resetMetrics();
            assertEquals(0, prolly.metrics().nodesWritten());

            assertFalse(prolly.publishPrefixPathHint(tree, bytes("k")));
            assertFalse(prolly.hydratePrefixPathHint(tree, bytes("k")));
            assertArrayEquals(bytes("k\0"), Prolly.changedSpanFromKey(bytes("k")).getEnd());
            assertArrayEquals(bytes("l"), Prolly.changedSpanForPrefix(bytes("k")).getEnd());
            assertFalse(prolly.publishChangedSpansHint(
                    empty,
                    tree,
                    List.of(Prolly.changedSpan(bytes("k"), bytes("l")))));
            assertNull(prolly.loadChangedSpansHint(empty, tree));

            StructuralDiffPage structuralPage = prolly.structuralDiffPage(empty, tree, null, 1);
            assertFalse(structuralPage.diffs().isEmpty());
            assertTrue(structuralPage.stats().emittedDiffs() > 0);
            StructuralDiffPage structuralCursorPage = prolly.structuralDiffPage(empty, tree, null, 0);
            assertTrue(structuralCursorPage.nextCursor().isPresent());
            assertTrue(structuralCursorPage.nextCursorJson().isPresent());
            StructuralDiffPage resumedStructuralPage = prolly.structuralDiffPageWithCursor(
                    empty,
                    tree,
                    structuralCursorPage.nextCursor().orElseThrow(),
                    1);
            assertFalse(resumedStructuralPage.diffs().isEmpty());

            GcReachability reachability = prolly.markReachable(List.of(tree));
            assertTrue(reachability.liveNodes() > 0);
            assertFalse(reachability.liveCids().isEmpty());
            List<byte[]> nodeCids = prolly.listNodeCids();
            assertFalse(nodeCids.isEmpty());
            GcPlan gcPlan = prolly.planGc(List.of(tree), nodeCids);
            assertEquals(nodeCids.size(), gcPlan.candidateNodes());
            assertEquals(0, gcPlan.reclaimableNodes());
            assertEquals(0, prolly.sweepGc(List.of(tree), nodeCids).deletedNodes());
            assertEquals(0, prolly.planStoreGc(List.of(tree)).reclaimableNodes());
            assertEquals(0, prolly.sweepStoreGc(List.of(tree)).deletedNodes());
            prolly.publishNamedRootAtMillis(bytes("live"), tree, 100);
            assertEquals(0, prolly.planStoreGcForRetention(Prolly.retainAllNamedRoots()).reclaimableNodes());
            assertEquals(0, prolly.sweepStoreGcForRetention(Prolly.retainAllNamedRoots()).deletedNodes());

            try (Prolly destination = Prolly.memory()) {
                MissingNodePlan missing = prolly.planMissingNodes(tree, destination);
                assertTrue(missing.missingNodes() > 0);
                MissingNodeCopy copied = prolly.copyMissingNodes(tree, destination);
                assertEquals(missing.missingNodes(), copied.copiedNodes());
                assertEquals(0, prolly.planMissingNodes(tree, destination).missingNodes());
                assertArrayEquals(bytes("v"), destination.get(tree, bytes("k")).orElseThrow());
            }

            SnapshotBundleRecord snapshotBundle = prolly.exportSnapshot(tree);
            assertEquals(1, Prolly.snapshotBundleFormatVersion(snapshotBundle));
            assertTrue(Prolly.snapshotBundleNodeCount(snapshotBundle) > 0);
            byte[] snapshotBundleBytes = Prolly.snapshotBundleToBytes(snapshotBundle);
            byte[] snapshotBundleDigest = Prolly.snapshotBundleDigest(snapshotBundle);
            assertArrayEquals(Prolly.cidFromBytes(snapshotBundleBytes), snapshotBundleDigest);
            assertArrayEquals(snapshotBundleDigest, Prolly.snapshotBundleDigestBytes(snapshotBundleBytes));
            SnapshotBundleSummaryRecord snapshotSummary = Prolly.snapshotBundleSummary(snapshotBundle);
            assertEquals(1, Prolly.snapshotBundleSummaryFormatVersion(snapshotSummary));
            assertEquals(Prolly.snapshotBundleNodeCount(snapshotBundle), Prolly.snapshotBundleSummaryNodeCount(snapshotSummary));
            assertTrue(Prolly.snapshotBundleSummaryByteCount(snapshotSummary) > 0);
            SnapshotBundleSummaryRecord byteSnapshotSummary = Prolly.snapshotBundleSummaryFromBytes(snapshotBundleBytes);
            assertEquals(
                    Prolly.snapshotBundleSummaryNodeCount(snapshotSummary),
                    Prolly.snapshotBundleSummaryNodeCount(byteSnapshotSummary));
            SnapshotBundleVerificationRecord snapshotVerification = Prolly.verifySnapshotBundle(snapshotBundle);
            assertTrue(Prolly.snapshotBundleVerificationValid(snapshotVerification));
            assertEquals(0, Prolly.snapshotBundleVerificationMissingCidCount(snapshotVerification));
            assertEquals(0, Prolly.snapshotBundleVerificationExtraCidCount(snapshotVerification));
            assertTrue(Prolly.snapshotBundleVerificationValid(Prolly.verifySnapshotBundleBytes(snapshotBundleBytes)));
            SnapshotBundleRecord decodedSnapshotBundle =
                    Prolly.snapshotBundleFromBytes(snapshotBundleBytes);
            try (Prolly destination = Prolly.memory()) {
                TreeRecord importedTree = destination.importSnapshot(decodedSnapshotBundle);
                assertArrayEquals(bytes("v"), destination.get(importedTree, bytes("k")).orElseThrow());
            }
        }
    }

    @Test
    void blobStoresLargeValuesAndBlobGcWorkThroughJavaFacade() throws Exception {
        Prolly.useLocalDebugLibrary();

        try (Prolly prolly = Prolly.memory(); BlobStore blobStore = BlobStore.memory()) {
            assertEquals(0, blobStore.blobCount());
            BlobRef directRef = blobStore.putBlob(bytes("direct"));
            assertArrayEquals(bytes("direct"), blobStore.getBlob(directRef).orElseThrow());
            Prolly.blobRefValidateBytes(directRef, bytes("direct"));
            assertThrows(
                    ProllyBindingException.class,
                    () -> Prolly.blobRefValidateBytes(directRef, bytes("wrong")));
            blobStore.deleteBlob(directRef);
            assertEquals(0, blobStore.blobCount());

            TreeRecord empty = prolly.create();
            byte[] largeValue = repeated((byte) 42, 64);
            TreeRecord tree = prolly.putLargeValue(
                    blobStore,
                    empty,
                    bytes("big"),
                    largeValue,
                    Prolly.largeValueConfig(8));
            ValueRef valueRef = prolly.getValueRef(tree, bytes("big")).orElseThrow();
            assertEquals(ValueRef.Kind.BLOB, valueRef.kind());
            assertTrue(valueRef.blob().isPresent());
            assertArrayEquals(largeValue, prolly.getLargeValue(blobStore, tree, bytes("big")).orElseThrow());

            BlobGcReachability reachable = prolly.markReachableBlobs(List.of(tree));
            assertEquals(1, reachable.liveBlobCount());
            assertEquals(1, reachable.liveBlobs().size());
            assertEquals(0, prolly.planBlobGc(blobStore, List.of(tree), reachable.liveBlobs()).reclaimableBlobCount());

            blobStore.putBlob(bytes("orphan"));
            assertEquals(2, blobStore.listBlobRefs().size());
            assertEquals(1, prolly.planBlobStoreGc(blobStore, List.of(tree)).reclaimableBlobCount());
            assertEquals(1, prolly.sweepBlobStoreGc(blobStore, List.of(tree)).deletedBlobs());
            assertEquals(1, blobStore.blobCount());

            TreeRecord withoutBig = prolly.delete(tree, bytes("big"));
            assertEquals(1, prolly.planBlobStoreGc(blobStore, List.of(withoutBig)).reclaimableBlobCount());
            assertEquals(1, prolly.sweepBlobStoreGc(blobStore, List.of(withoutBig)).deletedBlobs());
            assertEquals(0, blobStore.blobCount());
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes();
    }

    private static byte[] repeated(byte value, int count) {
        byte[] bytes = new byte[count];
        for (int i = 0; i < count; i++) {
            bytes[i] = value;
        }
        return bytes;
    }

    private static byte[] concat(byte[]... chunks) {
        int length = 0;
        for (byte[] chunk : chunks) {
            length += chunk.length;
        }
        byte[] result = new byte[length];
        int offset = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, result, offset, chunk.length);
            offset += chunk.length;
        }
        return result;
    }

    private static boolean throwsAny(ThrowingRunnable runnable) {
        try {
            runnable.run();
            return false;
        } catch (Exception expected) {
            return true;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
