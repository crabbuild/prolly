package build.crab.prolly

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.Files
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.startCoroutine

class ProllyParityTest {
    private object JoinResolver : MergeResolverCallback {
        override fun resolve(conflict: ConflictRecord): ResolutionRecord {
            val left = conflict.left
            val right = conflict.right
            return when {
                left != null && right != null ->
                    resolutionValue(left + byteArrayOf('|'.code.toByte()) + right)
                left != null ->
                    resolutionValue(left)
                right != null ->
                    resolutionValue(right)
                else ->
                    resolutionDelete()
            }
        }
    }

    private object CrdtJoinResolver : CrdtResolverCallback {
        override fun resolve(conflict: ConflictRecord): CrdtResolutionRecord {
            val left = conflict.left
            val right = conflict.right
            return when {
                left != null && right != null ->
                    crdtResolutionValue(left + byteArrayOf('|'.code.toByte()) + right)
                left != null ->
                    crdtResolutionValue(left)
                right != null ->
                    crdtResolutionValue(right)
                else ->
                    crdtResolutionDelete()
            }
        }
    }

    @Test
    fun batchGetManyPagesAndDiffPagesWorkThroughGeneratedKotlinApi() {
        ProllyNative.useLocalDebugLibrary()

        ProllyEngine.memory(defaultConfig()).use { engine ->
            assertEquals(EncodingKind.RAW, defaultConfig().encoding.kind)
            assertEquals(EncodingKind.RAW, encodingRaw().kind)
            assertEquals(EncodingKind.CBOR, encodingCbor().kind)
            assertEquals(EncodingKind.JSON, encodingJson().kind)
            val customEncoding = encodingCustom("postcard")
            assertEquals(EncodingKind.CUSTOM, customEncoding.kind)
            assertEquals("postcard", customEncoding.customName)
            val constructedConfig = treeConfig(2uL, 64uL, 32u, 7uL, customEncoding, 16uL, 4096uL)
            assertEquals(EncodingKind.CUSTOM, constructedConfig.encoding.kind)
            assertEquals(16uL, constructedConfig.nodeCacheMaxNodes)
            assertThrows<ProllyBindingException> {
                treeConfig(
                    2uL,
                    64uL,
                    32u,
                    7uL,
                    EncodingRecord(EncodingKind.CUSTOM, null),
                    null,
                    null,
                )
            }
            assertEquals(8uL, largeValueConfig(8uL).inlineThreshold)
            assertEquals(1uL, parallelConfigSequential().maxThreads)

            val empty = engine.create()
            assertEquals(MutationKind.UPSERT, upsertMutation("probe".bytes(), "value".bytes()).kind)
            assertEquals(MutationKind.DELETE, deleteMutation("probe".bytes()).kind)
            val tree = engine.batch(
                empty,
                listOf(
                    upsertMutation("a".bytes(), "1".bytes()),
                    upsertMutation("b".bytes(), "2".bytes()),
                    upsertMutation("a".bytes(), "11".bytes()),
                    deleteMutation("missing".bytes()),
                ),
            )

            val values = engine.getMany(tree, listOf("a".bytes(), "missing".bytes(), "b".bytes()))
            assertArrayEquals("11".bytes(), values[0])
            assertNull(values[1])
            assertArrayEquals("2".bytes(), values[2])

            val proof = engine.proveKey(tree, "a".bytes())
            val keyBundle = keyProofToBytes(proof)
            val keySummary = inspectProofBundle(keyBundle)
            assertEquals("key", keySummary.kind)
            assertArrayEquals(proof.root, keySummary.root)
            assertEquals(1uL, keySummary.keyCount)
            assertEquals(proof.path.size.toULong(), keySummary.pathNodeCount)
            val keyBundleVerified = verifyProofBundle(keyBundle)
            assertTrue(keyBundleVerified.valid)
            assertEquals("key", keyBundleVerified.summary.kind)
            assertEquals(1uL, keyBundleVerified.existsCount)
            assertEquals(0uL, keyBundleVerified.absenceCount)
            assertArrayEquals("11".bytes(), verifyKeyProof(keyProofFromBytes(keyBundle)).value)

            val multiProof = engine.proveKeys(tree, listOf("a".bytes(), "missing".bytes(), "b".bytes()))
            val multiVerified = verifyMultiKeyProof(multiProof)
            assertTrue(multiVerified.valid)
            assertEquals(3, multiVerified.results.size)
            assertArrayEquals("11".bytes(), multiVerified.results[0].value)
            assertTrue(multiVerified.results[1].absence)
            assertArrayEquals("2".bytes(), multiVerified.results[2].value)
            val decodedMulti = multiKeyProofFromNodeBytes(
                multiProof.root,
                multiProof.keys,
                multiKeyProofPathNodeBytes(multiProof),
            )
            assertArrayEquals("2".bytes(), verifyMultiKeyProof(decodedMulti).results[2].value)
            val decodedMultiFromBytes = multiKeyProofFromBytes(multiKeyProofToBytes(multiProof))
            assertArrayEquals("2".bytes(), verifyMultiKeyProof(decodedMultiFromBytes).results[2].value)
            val rangeProof = engine.proveRange(tree, "a".bytes(), "c".bytes())
            val rangeVerified = verifyRangeProof(rangeProof)
            assertTrue(rangeVerified.valid)
            assertEquals(2, rangeVerified.entries.size)
            assertArrayEquals("11".bytes(), rangeVerified.entries[0].value)
            val decodedRange = rangeProofFromNodeBytes(
                rangeProof.root,
                rangeProof.start,
                rangeProof.end,
                rangeProofPathNodeBytes(rangeProof),
            )
            assertArrayEquals("2".bytes(), verifyRangeProof(decodedRange).entries[1].value)
            val decodedRangeFromBytes = rangeProofFromBytes(rangeProofToBytes(rangeProof))
            assertArrayEquals("2".bytes(), verifyRangeProof(decodedRangeFromBytes).entries[1].value)
            val prefixProof = engine.provePrefix(tree, "a".bytes())
            val prefixVerified = verifyRangeProof(prefixProof)
            assertTrue(prefixVerified.valid)
            assertEquals(1, prefixVerified.entries.size)
            assertArrayEquals("11".bytes(), prefixVerified.entries[0].value)
            val provedPage = engine.proveRangePage(tree, RangeCursorRecord("a".bytes()), null, 1uL)
            val pageVerified = verifyRangePageProof(provedPage.proof)
            assertTrue(pageVerified.valid)
            assertEquals(1, pageVerified.entries.size)
            assertArrayEquals("2".bytes(), pageVerified.entries[0].value)
            val decodedPage = rangePageProofFromNodeBytes(
                provedPage.proof.root,
                provedPage.proof.after,
                provedPage.proof.end,
                rangePageProofPathNodeBytes(provedPage.proof),
            )
            assertArrayEquals("b".bytes(), verifyRangePageProof(decodedPage).entries[0].key)
            val decodedPageFromBytes = rangePageProofFromBytes(rangePageProofToBytes(provedPage.proof))
            assertArrayEquals("b".bytes(), verifyRangePageProof(decodedPageFromBytes).entries[0].key)
            var diffProofOther = engine.delete(tree, "a".bytes())
            diffProofOther = engine.put(diffProofOther, "b".bytes(), "22".bytes())
            diffProofOther = engine.put(diffProofOther, "d".bytes(), "4".bytes())
            val provedDiffPage = engine.proveDiffPage(tree, diffProofOther, null, null, 1uL)
            assertEquals(1, provedDiffPage.page.diffs.size)
            assertEquals(DiffKind.REMOVED, provedDiffPage.page.diffs[0].kind)
            assertArrayEquals("a".bytes(), provedDiffPage.page.diffs[0].key)
            assertArrayEquals("11".bytes(), provedDiffPage.page.diffs[0].value)
            assertArrayEquals("a".bytes(), provedDiffPage.page.nextCursor?.afterKey)
            assertArrayEquals("b".bytes(), provedDiffPage.proof.lookaheadBase?.key)
            assertArrayEquals("b".bytes(), provedDiffPage.proof.lookaheadOther?.key)
            val diffPageVerified = verifyDiffPageProof(provedDiffPage.proof)
            assertTrue(diffPageVerified.valid)
            assertTrue(diffPageVerified.baseValid)
            assertTrue(diffPageVerified.otherValid)
            assertTrue(diffPageVerified.lookaheadValid)
            assertEquals(1uL, diffPageVerified.limit)
            assertEquals(1, diffPageVerified.diffs.size)
            assertEquals(DiffKind.REMOVED, diffPageVerified.diffs[0].kind)
            assertArrayEquals("a".bytes(), diffPageVerified.diffs[0].key)
            assertArrayEquals("11".bytes(), diffPageVerified.diffs[0].value)
            assertArrayEquals("a".bytes(), diffPageVerified.nextCursor?.afterKey)
            val diffPageProofBytes = diffPageProofToBytes(provedDiffPage.proof)
            assertArrayEquals(diffPageProofBytes, diffPageProofToBytes(provedDiffPage.proof))
            val diffPageSummary = inspectProofBundle(diffPageProofBytes)
            assertEquals("diff_page", diffPageSummary.kind)
            assertArrayEquals(tree.root, diffPageSummary.root)
            assertArrayEquals(diffProofOther.root, diffPageSummary.otherRoot)
            assertEquals(1uL, diffPageSummary.limit)
            assertTrue(diffPageSummary.hasLookahead)
            val diffPageBundleVerified = verifyProofBundle(diffPageProofBytes)
            assertTrue(diffPageBundleVerified.valid)
            assertEquals("diff_page", diffPageBundleVerified.summary.kind)
            assertEquals(1uL, diffPageBundleVerified.diffCount)
            assertArrayEquals("a".bytes(), diffPageBundleVerified.nextCursor?.afterKey)
            val decodedDiffPageProof = diffPageProofFromBytes(diffPageProofBytes)
            val decodedDiffPageVerified = verifyDiffPageProof(decodedDiffPageProof)
            assertTrue(decodedDiffPageVerified.valid)
            assertEquals(DiffKind.REMOVED, decodedDiffPageVerified.diffs[0].kind)
            assertArrayEquals("a".bytes(), decodedDiffPageVerified.diffs[0].key)
            val signedEnvelope = signProofBundleHmacSha256(
                keyProofToBytes(proof),
                "kotlin-key".bytes(),
                "shared secret".bytes(),
                "tenant=t1".bytes(),
                1_700_000_000_000uL,
                1_700_000_100_000uL,
                "nonce-1".bytes(),
            )
            val signedEnvelopeBytes = authenticatedProofEnvelopeToBytes(signedEnvelope)
            assertArrayEquals(signedEnvelopeBytes, authenticatedProofEnvelopeToBytes(signedEnvelope))
            val decodedEnvelope = authenticatedProofEnvelopeFromBytes(signedEnvelopeBytes)
            val envelopeVerified = verifyAuthenticatedProofEnvelope(
                decodedEnvelope,
                "shared secret".bytes(),
                1_700_000_050_000uL,
            )
            assertTrue(envelopeVerified.valid)
            assertTrue(envelopeVerified.signatureValid)
            assertArrayEquals("kotlin-key".bytes(), envelopeVerified.keyId)
            assertArrayEquals("tenant=t1".bytes(), envelopeVerified.context)
            assertArrayEquals(
                "11".bytes(),
                verifyKeyProof(keyProofFromBytes(envelopeVerified.proofBundle)).value,
            )
            val authenticatedBundle = verifyAuthenticatedProofBundle(
                signedEnvelopeBytes,
                "shared secret".bytes(),
                1_700_000_050_000uL,
            )
            assertTrue(authenticatedBundle.valid)
            assertTrue(authenticatedBundle.envelope.valid)
            assertNull(authenticatedBundle.proofError)
            assertEquals(1uL, authenticatedBundle.proof?.existsCount)
            val wrongEnvelope = verifyAuthenticatedProofEnvelope(
                decodedEnvelope,
                "wrong secret".bytes(),
                1_700_000_050_000uL,
            )
            assertFalse(wrongEnvelope.valid)
            val wrongAuthenticatedBundle = verifyAuthenticatedProofBundle(
                signedEnvelopeBytes,
                "wrong secret".bytes(),
                1_700_000_050_000uL,
            )
            assertFalse(wrongAuthenticatedBundle.valid)
            assertFalse(wrongAuthenticatedBundle.envelope.valid)
            assertNull(wrongAuthenticatedBundle.proof)

            val built = engine.buildFromEntries(
                listOf(
                    EntryRecord("c".bytes(), "3".bytes()),
                    EntryRecord("a".bytes(), "1".bytes()),
                    EntryRecord("b".bytes(), "2".bytes()),
                ),
            )
            val sortedBuilt = engine.buildFromSortedEntries(
                listOf(
                    EntryRecord("a".bytes(), "1".bytes()),
                    EntryRecord("b".bytes(), "2".bytes()),
                    EntryRecord("c".bytes(), "3".bytes()),
                ),
            )
            assertArrayEquals(built.root, sortedBuilt.root)
            assertTrue(
                runCatching {
                    engine.buildFromSortedEntries(
                        listOf(
                            EntryRecord("b".bytes(), "2".bytes()),
                            EntryRecord("a".bytes(), "1".bytes()),
                        ),
                    )
                }.isFailure,
            )
            val batchStats = engine.batchWithStats(
                empty,
                listOf(
                    MutationRecord(MutationKind.UPSERT, "b".bytes(), "2".bytes()),
                    MutationRecord(MutationKind.UPSERT, "a".bytes(), "1".bytes()),
                    MutationRecord(MutationKind.UPSERT, "a".bytes(), "11".bytes()),
                ),
            )
            assertArrayEquals("11".bytes(), engine.get(batchStats.tree, "a".bytes()))
            assertEquals(3UL, batchStats.stats.inputMutations)
            assertEquals(2UL, batchStats.stats.effectiveMutations)
            assertFalse(batchStats.stats.preprocessInputSorted)

            val parallelTree = engine.parallelBatch(
                empty,
                listOf(
                    MutationRecord(MutationKind.UPSERT, "p".bytes(), "parallel".bytes()),
                    MutationRecord(MutationKind.UPSERT, "q".bytes(), "kotlin".bytes()),
                ),
                ParallelConfigRecord(1uL, 1uL),
            )
            assertArrayEquals("kotlin".bytes(), engine.get(parallelTree, "q".bytes()))
            val parallelStats = engine.parallelBatchWithStats(
                empty,
                listOf(
                    MutationRecord(MutationKind.UPSERT, "r".bytes(), "route".bytes()),
                    MutationRecord(MutationKind.UPSERT, "s".bytes(), "stats".bytes()),
                ),
                ParallelConfigRecord(1uL, 1uL),
            )
            assertArrayEquals("stats".bytes(), engine.get(parallelStats.tree, "s".bytes()))
            assertEquals(2UL, parallelStats.stats.inputMutations)
            assertEquals(2UL, parallelStats.stats.effectiveMutations)
            assertTrue(parallelStats.stats.writtenNodes > 0UL)

            val appended = engine.appendBatch(
                built,
                listOf(
                    MutationRecord(MutationKind.UPSERT, "d".bytes(), "4".bytes()),
                    MutationRecord(MutationKind.UPSERT, "e".bytes(), "5".bytes()),
                    MutationRecord(MutationKind.UPSERT, "d".bytes(), "44".bytes()),
                ),
            )
            assertArrayEquals("44".bytes(), engine.get(appended, "d".bytes()))
            val appendedStats = engine.appendBatchWithStats(
                built,
                listOf(
                    MutationRecord(MutationKind.UPSERT, "d".bytes(), "4".bytes()),
                    MutationRecord(MutationKind.UPSERT, "e".bytes(), "5".bytes()),
                    MutationRecord(MutationKind.UPSERT, "d".bytes(), "44".bytes()),
                ),
            )
            assertArrayEquals("44".bytes(), engine.get(appendedStats.tree, "d".bytes()))
            assertEquals(3UL, appendedStats.stats.inputMutations)
            assertEquals(2UL, appendedStats.stats.effectiveMutations)
            assertFalse(appendedStats.stats.preprocessInputSorted)
            assertTrue(appendedStats.stats.usedAppendFastPath)
            assertTrue(appendedStats.stats.writtenNodes > 0UL)

            val firstPage = engine.rangePage(tree, null, null, 1UL)
            assertEquals(1, firstPage.entries.size)
            assertArrayEquals("a".bytes(), firstPage.entries[0].key)
            assertNotNull(firstPage.nextCursor)

            val afterA = engine.rangeAfter(tree, "a".bytes(), null)
            assertEquals(1, afterA.size)
            assertArrayEquals("b".bytes(), afterA[0].key)
            assertNull(rangeCursorStart().afterKey)
            val afterACursor = rangeCursorAfterKey("a".bytes())
            assertArrayEquals("a".bytes(), afterACursor.afterKey)
            val fromCursor = engine.rangeFromCursor(tree, afterACursor, null)
            assertEquals(1, fromCursor.size)
            assertArrayEquals(afterA[0].key, fromCursor[0].key)
            assertArrayEquals("a".bytes(), engine.firstEntry(tree)?.key)
            assertArrayEquals("11".bytes(), engine.firstEntry(tree)?.value)
            assertArrayEquals("b".bytes(), engine.lastEntry(tree)?.key)
            assertArrayEquals("b".bytes(), engine.lowerBound(tree, "aa".bytes())?.key)
            assertNull(engine.upperBound(tree, "b".bytes()))
            val prefixEntries = engine.prefix(tree, "a".bytes())
            assertEquals(1, prefixEntries.size)
            assertArrayEquals("11".bytes(), prefixEntries[0].value)
            val prefixPage = engine.prefixPage(tree, "a".bytes(), null, 1UL)
            assertEquals(1, prefixPage.entries.size)
            assertArrayEquals("11".bytes(), prefixPage.entries[0].value)
            assertNotNull(prefixPage.nextCursor)

            val window = engine.cursorWindow(tree, "aa".bytes(), null, 1UL)
            assertArrayEquals("a".bytes(), window.positionKey)
            assertArrayEquals("11".bytes(), window.positionValue)
            assertFalse(window.found)
            assertEquals(1, window.entries.size)
            assertArrayEquals("b".bytes(), window.entries[0].key)
            assertArrayEquals("b".bytes(), window.nextCursor?.afterKey)

            val exactProbe = engine.cursorWindow(tree, "a".bytes(), null, 0UL)
            assertTrue(exactProbe.found)
            assertArrayEquals("a".bytes(), exactProbe.positionKey)
            assertEquals(0, exactProbe.entries.size)
            assertNull(exactProbe.nextCursor)

            val secondPage = engine.rangePage(tree, firstPage.nextCursor, null, 1UL)
            assertEquals(1, secondPage.entries.size)
            assertArrayEquals("b".bytes(), secondPage.entries[0].key)
            if (secondPage.nextCursor != null) {
                val thirdPage = engine.rangePage(tree, secondPage.nextCursor, null, 1UL)
                assertEquals(0, thirdPage.entries.size)
                assertNull(thirdPage.nextCursor)
            }

            assertNull(reverseCursorEnd().beforeKey)
            val beforeCCursor = reverseCursorBeforeKey("c".bytes())
            assertArrayEquals("c".bytes(), beforeCCursor.beforeKey)
            val reverseFirst = engine.reversePage(built, null, ByteArray(0), 2UL)
            assertEquals(2, reverseFirst.entries.size)
            assertArrayEquals("c".bytes(), reverseFirst.entries[0].key)
            assertArrayEquals("b".bytes(), reverseFirst.entries[1].key)
            assertArrayEquals("b".bytes(), reverseFirst.nextCursor?.beforeKey)
            val reverseSecond = engine.reversePage(built, reverseFirst.nextCursor, ByteArray(0), 2UL)
            assertEquals(1, reverseSecond.entries.size)
            assertArrayEquals("a".bytes(), reverseSecond.entries[0].key)
            assertNull(reverseSecond.nextCursor)
            val prefixReverse = engine.prefixReversePage(built, "b".bytes(), null, 8UL)
            assertEquals(1, prefixReverse.entries.size)
            assertArrayEquals("b".bytes(), prefixReverse.entries[0].key)
            assertNull(prefixReverse.nextCursor)

            val changed = engine.put(tree, "b".bytes(), "22".bytes())
            val diffPage = engine.diffPage(tree, changed, null, null, 1UL)
            assertEquals(1, diffPage.diffs.size)
            assertEquals(DiffKind.CHANGED, diffPage.diffs[0].kind)
            if (diffPage.nextCursor != null) {
                val secondDiffPage = engine.diffPage(tree, changed, diffPage.nextCursor, null, 1UL)
                assertEquals(0, secondDiffPage.diffs.size)
                assertNull(secondDiffPage.nextCursor)
            }

            val changedForCursor = engine.batch(
                built,
                listOf(
                    MutationRecord(MutationKind.UPSERT, "b".bytes(), "22".bytes()),
                    MutationRecord(MutationKind.UPSERT, "c".bytes(), "33".bytes()),
                ),
            )
            val resumedDiffs = engine.diffFromCursor(
                built,
                changedForCursor,
                RangeCursorRecord("a".bytes()),
                "c".bytes(),
            )
            assertEquals(1, resumedDiffs.size)
            assertEquals(DiffKind.CHANGED, resumedDiffs[0].kind)
            assertArrayEquals("b".bytes(), resumedDiffs[0].key)

            val conflictBase = engine.batch(
                empty,
                listOf(
                    MutationRecord(MutationKind.UPSERT, "a".bytes(), "base-a".bytes()),
                    MutationRecord(MutationKind.UPSERT, "b".bytes(), "base-b".bytes()),
                ),
            )
            val conflictLeft = engine.batch(
                conflictBase,
                listOf(
                    MutationRecord(MutationKind.UPSERT, "a".bytes(), "left-a".bytes()),
                    MutationRecord(MutationKind.UPSERT, "b".bytes(), "left-b".bytes()),
                ),
            )
            val conflictRight = engine.batch(
                conflictBase,
                listOf(
                    MutationRecord(MutationKind.UPSERT, "a".bytes(), "right-a".bytes()),
                    MutationRecord(MutationKind.UPSERT, "b".bytes(), "right-b".bytes()),
                ),
            )
            val conflictPage = engine.conflictPage(conflictBase, conflictLeft, conflictRight, null, 1UL)
            assertEquals(1, conflictPage.conflicts.size)
            assertArrayEquals("a".bytes(), conflictPage.conflicts[0].key)
            assertArrayEquals("base-a".bytes(), conflictPage.conflicts[0].base)
            assertArrayEquals("left-a".bytes(), conflictPage.conflicts[0].left)
            assertArrayEquals("right-a".bytes(), conflictPage.conflicts[0].right)
            assertNotNull(conflictPage.nextCursor)

            val secondConflictPage = engine.conflictPage(
                conflictBase,
                conflictLeft,
                conflictRight,
                conflictPage.nextCursor,
                1UL,
            )
            assertEquals(1, secondConflictPage.conflicts.size)
            assertArrayEquals("b".bytes(), secondConflictPage.conflicts[0].key)
            assertNull(secondConflictPage.nextCursor)

            val base = engine.put(empty, "k".bytes(), "base".bytes())
            val left = engine.put(base, "k".bytes(), "left".bytes())
            val right = engine.put(base, "k".bytes(), "right".bytes())
            val callbackMerged = engine.mergeWithResolver(base, left, right, JoinResolver)
            assertArrayEquals("left|right".bytes(), engine.get(callbackMerged, "k".bytes()))
            assertNotNull(engine.mergeExplainWithResolver(base, left, right, JoinResolver).result)

            val policyBase = engine.batch(
                empty,
                listOf(
                    MutationRecord(MutationKind.UPSERT, "doc/title".bytes(), "base-title".bytes()),
                    MutationRecord(MutationKind.UPSERT, "k".bytes(), "base-k".bytes()),
                ),
            )
            val policyLeft = engine.batch(
                policyBase,
                listOf(
                    MutationRecord(MutationKind.UPSERT, "doc/title".bytes(), "left-title".bytes()),
                    MutationRecord(MutationKind.UPSERT, "k".bytes(), "left-k".bytes()),
                ),
            )
            val policyRight = engine.batch(
                policyBase,
                listOf(
                    MutationRecord(MutationKind.UPSERT, "doc/title".bytes(), "right-title".bytes()),
                    MutationRecord(MutationKind.UPSERT, "k".bytes(), "right-k".bytes()),
                ),
            )
            MergePolicyRegistry().use { policy ->
                assertTrue(policy.isEmpty())
                assertFalse(policy.hasDefault())
                policy.setDefaultResolverName("prefer_left")
                policy.pushPrefixResolver("doc/".bytes(), JoinResolver)
                policy.pushExactResolverName("k".bytes(), "prefer_right")
                assertEquals(2UL, policy.len())
                assertTrue(policy.hasDefault())

                val policyMerged = engine.mergeWithPolicy(policyBase, policyLeft, policyRight, policy)
                assertArrayEquals("left-title|right-title".bytes(), engine.get(policyMerged, "doc/title".bytes()))
                assertArrayEquals("right-k".bytes(), engine.get(policyMerged, "k".bytes()))
                assertNotNull(engine.mergeExplainWithPolicy(policyBase, policyLeft, policyRight, policy).result)
                val policyRange = engine.mergeRangeWithPolicy(
                    policyBase,
                    policyLeft,
                    policyRight,
                    "doc/".bytes(),
                    "doc0".bytes(),
                    policy,
                )
                assertArrayEquals("left-title|right-title".bytes(), engine.get(policyRange, "doc/title".bytes()))
                val policyPrefix = engine.mergePrefixWithPolicy(
                    policyBase,
                    policyLeft,
                    policyRight,
                    "doc/".bytes(),
                    policy,
                )
                assertArrayEquals("left-title|right-title".bytes(), engine.get(policyPrefix, "doc/title".bytes()))
            }
        }
    }

    @Test
    fun suspendWrapperPreservesCoreBehavior() {
        ProllyNative.useLocalDebugLibrary()

        runSuspend {
            AsyncProllyEngine.memory().use { engine ->
                val empty = engine.create()
                val tree = engine.batch(
                    empty,
                    listOf(
                        MutationRecord(MutationKind.UPSERT, "a".bytes(), "1".bytes()),
                        MutationRecord(MutationKind.UPSERT, "b".bytes(), "2".bytes()),
                    ),
                )

                assertArrayEquals("1".bytes(), engine.get(tree, "a".bytes()))
                val afterA = engine.rangeAfter(tree, "a".bytes(), null)
                assertEquals(1, afterA.size)
                assertArrayEquals("b".bytes(), afterA[0].key)

                val changed = engine.put(tree, "b".bytes(), "22".bytes())
                assertEquals(1, engine.diff(tree, changed).size)
                assertNotNull(engine.rangePage(changed, null, null, 1UL).nextCursor)
                val reversePage = engine.reversePage(changed, null, ByteArray(0), 2UL)
                assertArrayEquals("b".bytes(), reversePage.entries[0].key)
                assertArrayEquals("a".bytes(), reversePage.entries[1].key)
            }
        }
    }

    @Test
    fun suspendWrapperCoversAdvancedApis() {
        ProllyNative.useLocalDebugLibrary()

        runSuspend {
            AsyncProllyEngine.memory().use { engine ->
                AsyncProllyBlobStore.memory().use { blobStore ->
                    val directRef = blobStore.putBlob("direct".bytes())
                    assertArrayEquals("direct".bytes(), blobStore.getBlob(directRef))
                    assertEquals(1UL, blobStore.blobCount())
                    blobStore.deleteBlob(directRef)
                    assertEquals(0UL, blobStore.blobCount())

                    val empty = engine.create()
                    val largeValue = ByteArray(32) { 7 }
                    val largeTree = engine.putLargeValue(
                        blobStore,
                        empty,
                        "big".bytes(),
                        largeValue,
                        LargeValueConfigRecord(8UL),
                    )
                    assertEquals(ValueRefKind.BLOB, engine.getValueRef(largeTree, "big".bytes())?.kind)
                    assertArrayEquals(largeValue, engine.getLargeValue(blobStore, largeTree, "big".bytes()))
                    assertEquals(1UL, engine.planBlobStoreGc(blobStore, listOf(largeTree)).reachability.liveBlobCount)

                    val base = engine.put(empty, "k".bytes(), "base".bytes())
                    val left = engine.put(base, "k".bytes(), "left".bytes())
                    val right = engine.put(base, "k".bytes(), "right".bytes())
                    val merged = engine.merge(base, left, right, "prefer_right")
                    assertArrayEquals("right".bytes(), engine.get(merged, "k".bytes()))
                    assertNotNull(engine.mergeExplain(base, left, right, "prefer_right").result)
                    val callbackMerged = engine.mergeWithResolver(base, left, right, JoinResolver)
                    assertArrayEquals("left|right".bytes(), engine.get(callbackMerged, "k".bytes()))
                    assertNotNull(engine.mergeExplainWithResolver(base, left, right, JoinResolver).result)
                    MergePolicyRegistry().use { policy ->
                        policy.setDefaultResolver(JoinResolver)
                        val policyMerged = engine.mergeWithPolicy(base, left, right, policy)
                        assertArrayEquals("left|right".bytes(), engine.get(policyMerged, "k".bytes()))
                    }

                    engine.publishNamedRootAtMillis("main".bytes(), merged, 42UL)
                    assertNotNull(engine.loadNamedRoot("main".bytes()))
                    assertEquals(1, engine.listNamedRoots().size)
                    val update = engine.compareAndSwapNamedRootAtMillis("main".bytes(), merged, null, 43UL)
                    assertTrue(update.applied)
                    assertFalse(update.conflict)

                    assertTrue(engine.collectStatsJson(largeTree).json.contains("\"num_nodes\""))
                    val typedStats = engine.collectStats(largeTree)
                    assertTrue(typedStats.numNodes > 0UL)
                    assertEquals(1UL, typedStats.totalKeyValuePairs)
                    val typedDiffStats = engine.statsDiff(empty, largeTree)
                    assertEquals(0UL, typedDiffStats.before.totalKeyValuePairs)
                    assertEquals(1UL, typedDiffStats.after.totalKeyValuePairs)
                    val debugTree = engine.debugTree(largeTree)
                    assertTrue(debugTree.levels.isNotEmpty())
                    val debugComparison = engine.debugCompareTrees(empty, largeTree)
                    assertTrue(debugComparison.rightOnlyNodes > 0UL)
                    assertTrue(engine.pinTreeRoot(largeTree) > 0UL)
                    assertTrue(engine.cacheStats().pinnedNodes > 0UL)
                    assertTrue(engine.unpinAllCacheNodes() > 0UL)
                    engine.clearCache()

                    val reachability = engine.markReachable(listOf(largeTree))
                    assertTrue(reachability.liveNodes > 0UL)
                    val nodeCids = engine.listNodeCids()
                    assertTrue(nodeCids.isNotEmpty())
                    assertEquals(nodeCids.size.toULong(), engine.planGc(listOf(largeTree), nodeCids).candidateNodes)

                    AsyncProllyEngine.memory().use { destination ->
                        val missing = engine.planMissingNodes(largeTree, destination)
                        assertTrue(missing.missingNodes > 0UL)
                        val copied = engine.copyMissingNodes(largeTree, destination)
                        assertEquals(missing.missingNodes, copied.copiedNodes)
                    }
                }
            }
        }
    }

    @Test
    fun mergeAndNamedRootCasWorkThroughGeneratedKotlinApi() {
        ProllyNative.useLocalDebugLibrary()

        ProllyEngine.memory(defaultConfig()).use { engine ->
            val empty = engine.create()
            val base = engine.put(empty, "k".bytes(), "base".bytes())
            val left = engine.put(base, "k".bytes(), "left".bytes())
            val right = engine.put(base, "k".bytes(), "right".bytes())

            val explanation = engine.mergeExplain(base, left, right, "prefer_right")
            assertNotNull(explanation.result)
            assertNull(explanation.error)
            assertTrue(explanation.traceJson.contains("events"))
            assertTrue(explanation.trace.events.isNotEmpty())
            assertTrue(
                explanation.trace.events.any { event ->
                    event.kind == MergeTraceEventKind.RESOLVER_CALLED &&
                        event.resolution == MergeTraceResolutionKind.VALUE
                }
            )

            val merged = engine.merge(base, left, right, "prefer_right")
            assertArrayEquals("right".bytes(), engine.get(merged, "k".bytes()))

            val updateConflict = ConflictRecord("k".bytes(), "base".bytes(), "left".bytes(), "right".bytes())
            assertArrayEquals("left".bytes(), resolvePreferLeft(updateConflict).value)
            assertEquals(ResolutionKind.UNRESOLVED, resolveDeleteWins(updateConflict).kind)
            val deleteConflict = ConflictRecord("k".bytes(), "base".bytes(), null, "right".bytes())
            assertEquals(ResolutionKind.DELETE, resolveDeleteWins(deleteConflict).kind)
            assertArrayEquals("right".bytes(), resolveUpdateWins(deleteConflict).value)
            val preferRightCallback = object : MergeResolverCallback {
                override fun resolve(conflict: ConflictRecord): ResolutionRecord = resolvePreferRight(conflict)
            }
            val preferRightMerged = engine.mergeWithResolver(base, left, right, preferRightCallback)
            assertArrayEquals("right".bytes(), engine.get(preferRightMerged, "k".bytes()))

            val name = "main".bytes()
            engine.publishNamedRootAtMillis(name, merged, 42UL)
            assertNotNull(engine.loadNamedRoot(name))
            assertEquals(1, engine.listNamedRoots().size)
            val manifests = engine.listNamedRootManifests()
            assertEquals(1, manifests.size)
            assertArrayEquals(name, manifests[0].name)
            assertArrayEquals(merged.root, manifests[0].manifest.tree.root)
            assertEquals(42UL, manifests[0].manifest.createdAtMillis)
            assertEquals(42UL, manifests[0].manifest.updatedAtMillis)

            val selection = engine.loadNamedRoots(listOf(name, "missing".bytes()))
            assertEquals(1, selection.roots.size)
            assertEquals(1, selection.missingNames.size)

            val retainAll = retainAllNamedRoots()
            assertEquals(NamedRootRetentionKind.ALL, retainAll.kind)
            val retainExact = retainExactNamedRoots(listOf(name, "missing".bytes()))
            assertEquals(NamedRootRetentionKind.EXACT, retainExact.kind)
            assertEquals(2, retainExact.names.size)
            val retainPrefix = retainNamedRootPrefix("ma".bytes())
            assertEquals(NamedRootRetentionKind.PREFIX, retainPrefix.kind)
            assertArrayEquals("ma".bytes(), retainPrefix.prefix)
            val retainNewest = retainNewestNamedRoots("checkpoint/".bytes(), 2UL)
            assertEquals(NamedRootRetentionKind.NEWEST_BY_NAME, retainNewest.kind)
            assertArrayEquals("checkpoint/".bytes(), retainNewest.prefix)
            assertEquals(2UL, retainNewest.count)
            val retainUpdated = retainNamedRootsUpdatedSince("checkpoint/".bytes(), 42UL)
            assertEquals(NamedRootRetentionKind.UPDATED_SINCE, retainUpdated.kind)
            assertArrayEquals("checkpoint/".bytes(), retainUpdated.prefix)
            assertEquals(42UL, retainUpdated.minUpdatedAtMillis)

            val retained = engine.loadRetainedNamedRoots(retainAll)
            assertEquals(1, retained.roots.size)
            assertEquals(1UL, engine.planStoreGcForRetention(retainAll).reachability.liveNodes)
            assertEquals(1UL, engine.sweepStoreGcForRetention(retainAll).plan.reachability.liveNodes)

            val update = engine.compareAndSwapNamedRoot(name, merged, null)
            assertTrue(update.applied)
            assertFalse(update.conflict)
            assertNull(engine.loadNamedRoot(name))
        }
    }

    @Test
    fun crdtTombstoneAndMultiValueHelpersWorkThroughGeneratedKotlinApi() {
        ProllyNative.useLocalDebugLibrary()

        ProllyEngine.memory(defaultConfig()).use { engine ->
            val empty = engine.create()
            val baseValue = timestampedValueToBytes(TimestampedValueRecord("base".bytes(), 1UL))
            val leftValue = timestampedValueToBytes(TimestampedValueRecord("left".bytes(), 2UL))
            val rightValue = timestampedValueToBytes(TimestampedValueRecord("right".bytes(), 3UL))

            val base = engine.put(empty, "k".bytes(), baseValue)
            val left = engine.put(base, "k".bytes(), leftValue)
            val right = engine.put(base, "k".bytes(), rightValue)

            val lww = crdtConfigLww(CrdtDeletePolicyKind.UPDATE_WINS)
            assertEquals(CrdtMergeStrategyKind.LAST_WRITER_WINS, lww.strategy)
            assertEquals(CrdtDeletePolicyKind.UPDATE_WINS, lww.deletePolicy)
            val merged = engine.crdtMerge(base, left, right, lww)
            val mergedValue = timestampedValueFromBytes(engine.get(merged, "k".bytes())!!)
            assertArrayEquals("right".bytes(), mergedValue.value)
            assertEquals(3UL, mergedValue.timestamp)

            val callbackMerged = engine.crdtMergeWithResolver(
                base,
                left,
                right,
                CrdtDeletePolicyKind.UPDATE_WINS,
                CrdtJoinResolver,
            )
            assertArrayEquals(leftValue + byteArrayOf('|'.code.toByte()) + rightValue, engine.get(callbackMerged, "k".bytes()))

            val deleteLeft = engine.delete(base, "k".bytes())
            val updateRight = engine.put(base, "k".bytes(), "right".bytes())
            val deleted = engine.crdtMergeWithResolver(
                base,
                deleteLeft,
                updateRight,
                CrdtDeletePolicyKind.UPDATE_WINS,
                object : CrdtResolverCallback {
                    override fun resolve(conflict: ConflictRecord): CrdtResolutionRecord =
                        crdtResolutionDelete()
                },
            )
            assertNull(engine.get(deleted, "k".bytes()))

            val now = timestampedValueNow("now".bytes())
            assertArrayEquals("now".bytes(), now.value)
            assertTrue(now.timestamp > 0UL)

            val multiConfig = crdtConfigMultiValue(CrdtDeletePolicyKind.DELETE_WINS)
            assertEquals(CrdtMergeStrategyKind.MULTI_VALUE, multiConfig.strategy)
            assertEquals(CrdtDeletePolicyKind.DELETE_WINS, multiConfig.deletePolicy)
            val set = multiValueSetFromBytes(multiValueSetToBytes(listOf("b".bytes(), "a".bytes(), "a".bytes())))
            assertEquals(2, set.size)
            assertArrayEquals("a".bytes(), set[0])
            assertArrayEquals("b".bytes(), set[1])
            val mergedSet = multiValueSetMerge(listOf("b".bytes()), listOf("a".bytes(), "b".bytes()))
            assertEquals(2, mergedSet.size)
            assertArrayEquals("a".bytes(), mergedSet[0])
            assertArrayEquals("b".bytes(), mergedSet[1])

            val tombstone = TombstoneRecord(
                "actor".bytes(),
                7UL,
                listOf(TombstoneMetadataRecord("clock", "7".bytes())),
            )
            val tombstoneBytes = tombstoneToBytes(tombstone)
            assertTrue(isTombstoneValue(tombstoneBytes))
            assertEquals(7UL, tombstoneFromBytes(tombstoneBytes).timestampMillis)
            assertEquals("clock", tombstoneFromStoredBytes(tombstoneBytes)?.causalMetadata?.get(0)?.key)

            val upsert = tombstoneUpsertMutation("deleted".bytes(), tombstone)
            assertEquals(MutationKind.UPSERT, upsert.kind)
            assertArrayEquals("deleted".bytes(), upsert.key)
            assertNotNull(upsert.value)

            val compaction = tombstoneCompactionMutation("deleted".bytes(), tombstoneBytes)
            assertEquals(MutationKind.DELETE, compaction?.kind)
            assertArrayEquals("deleted".bytes(), compaction?.key)
            assertNull(compaction?.value)
        }
    }

    @Test
    fun sqliteEnginePersistsNodesAcrossReopenThroughGeneratedKotlinApi() {
        ProllyNative.useLocalDebugLibrary()
        val path = Files.createTempFile("prolly-kotlin", ".db")
        Files.deleteIfExists(path)

        val tree = ProllyEngine.sqlite(path.toString(), defaultConfig()).use { first ->
            first.put(first.create(), "k".bytes(), "v".bytes())
        }

        try {
            ProllyEngine.sqlite(path.toString(), defaultConfig()).use { reopened ->
                assertArrayEquals("v".bytes(), reopened.get(tree, "k".bytes()))
            }
        } finally {
            Files.deleteIfExists(path)
        }
    }

    @Test
    fun operationalApisWorkThroughGeneratedKotlinApi() {
        ProllyNative.useLocalDebugLibrary()

        ProllyEngine.memory(defaultConfig()).use { engine ->
            val empty = engine.create()
            val tree = engine.put(empty, "k".bytes(), "v".bytes())

            assertTrue(engine.collectStatsJson(tree).json.contains("\"num_nodes\""))
            val typedStats = engine.collectStats(tree)
            assertEquals(1UL, typedStats.totalKeyValuePairs)
            assertTrue(typedStats.nodesPerLevel.any { it.level == 0.toUByte() && it.value > 0UL })
            assertTrue(engine.statsDiffJson(empty, tree).json.contains("\"absolute\""))
            val typedDiffStats = engine.statsDiff(empty, tree)
            assertEquals(0UL, typedDiffStats.before.totalKeyValuePairs)
            assertEquals(1UL, typedDiffStats.after.totalKeyValuePairs)
            assertEquals(1L, typedDiffStats.absolute.totalKeyValuePairsDiff)
            assertTrue(engine.debugTreeJson(tree).json.contains("\"levels\""))
            val debugTree = engine.debugTree(tree)
            assertTrue(debugTree.levels.isNotEmpty())
            assertTrue(debugTree.levels.first().nodes.isNotEmpty())
            assertTrue(engine.debugTreeText(tree).contains("level"))
            assertTrue(engine.debugCompareTreesJson(empty, tree).json.contains("\"right_only_nodes\""))
            val debugComparison = engine.debugCompareTrees(empty, tree)
            assertEquals(0UL, debugComparison.leftOnlyNodes)
            assertTrue(debugComparison.rightOnlyNodes > 0UL)
            assertTrue(debugComparison.levels.any { level ->
                level.nodes.any { node -> node.status == TreeDebugNodeStatusKind.RIGHT_ONLY }
            })
            assertTrue(engine.debugCompareTreesText(empty, tree).contains("right"))

            assertTrue(engine.pinTreePath(tree, "k".bytes()) > 0UL)
            assertTrue(engine.unpinAllCacheNodes() >= 0UL)
            assertTrue(engine.pinTreeRoot(tree) > 0UL)
            assertTrue(engine.cacheStats().cachedNodes > 0UL)
            assertTrue(engine.unpinAllCacheNodes() >= 0UL)
            engine.clearCache()

            assertTrue(engine.metrics().nodesWritten > 0UL)
            engine.resetMetrics()
            assertEquals(0UL, engine.metrics().nodesWritten)

            assertFalse(engine.publishPrefixPathHint(tree, "k".bytes()))
            assertFalse(engine.hydratePrefixPathHint(tree, "k".bytes()))
            assertArrayEquals(byteArrayOf('k'.code.toByte(), 0), changedSpanFromKey("k".bytes()).end)
            assertArrayEquals("l".bytes(), changedSpanForPrefix("k".bytes()).end)
            assertFalse(
                engine.publishChangedSpansHint(
                    empty,
                    tree,
                    listOf(changedSpan("k".bytes(), "l".bytes())),
                ),
            )
            assertNull(engine.loadChangedSpansHint(empty, tree))

            val structuralPage = engine.structuralDiffPage(empty, tree, null, 1UL)
            assertTrue(structuralPage.diffs.isNotEmpty())
            assertTrue(structuralPage.stats.emittedDiffs > 0UL)
            val structuralCursorPage = engine.structuralDiffPage(empty, tree, null, 0UL)
            assertNotNull(structuralCursorPage.nextCursor)
            assertNotNull(structuralCursorPage.nextCursorJson)
            val resumedStructuralPage = engine.structuralDiffPageWithCursor(
                empty,
                tree,
                structuralCursorPage.nextCursor,
                1UL,
            )
            assertTrue(resumedStructuralPage.diffs.isNotEmpty())

            val reachability = engine.markReachable(listOf(tree))
            assertTrue(reachability.liveNodes > 0UL)
            assertTrue(reachability.liveCids.isNotEmpty())
            val nodeCids = engine.listNodeCids()
            assertTrue(nodeCids.isNotEmpty())
            val gcPlan = engine.planGc(listOf(tree), nodeCids)
            assertEquals(nodeCids.size.toULong(), gcPlan.candidateNodes)
            assertEquals(0UL, gcPlan.reclaimableNodes)
            assertEquals(0UL, engine.sweepGc(listOf(tree), nodeCids).deletedNodes)
            assertEquals(0UL, engine.planStoreGc(listOf(tree)).reclaimableNodes)
            assertEquals(0UL, engine.sweepStoreGc(listOf(tree)).deletedNodes)

            ProllyEngine.memory(defaultConfig()).use { destination ->
                val missing = engine.planMissingNodes(tree, destination)
                assertTrue(missing.missingNodes > 0UL)
                val copied = engine.copyMissingNodes(tree, destination)
                assertEquals(missing.missingNodes, copied.copiedNodes)
                assertEquals(0UL, engine.planMissingNodes(tree, destination).missingNodes)
                assertArrayEquals("v".bytes(), destination.get(tree, "k".bytes()))
            }

            val snapshotBundle = engine.exportSnapshot(tree)
            assertEquals(1u, snapshotBundle.formatVersion)
            assertTrue(snapshotBundle.nodes.isNotEmpty())
            val snapshotBundleBytes = snapshotBundleToBytes(snapshotBundle)
            val snapshotBundleDigest = snapshotBundleDigest(snapshotBundle)
            assertArrayEquals(cidFromBytes(snapshotBundleBytes), snapshotBundleDigest)
            assertArrayEquals(snapshotBundleDigest, snapshotBundleDigestBytes(snapshotBundleBytes))
            val snapshotSummary = snapshotBundleSummary(snapshotBundle)
            assertEquals(1u, snapshotSummary.formatVersion)
            assertEquals(snapshotBundle.nodes.size.toULong(), snapshotSummary.nodeCount)
            assertTrue(snapshotSummary.byteCount > 0u)
            assertSnapshotSummaryEquals(snapshotSummary, snapshotBundleSummaryFromBytes(snapshotBundleBytes))
            val snapshotVerification = verifySnapshotBundle(snapshotBundle)
            assertTrue(snapshotVerification.valid)
            assertSnapshotSummaryEquals(snapshotSummary, snapshotVerification.summary)
            assertTrue(snapshotVerification.missingCids.isEmpty())
            assertTrue(snapshotVerification.extraCids.isEmpty())
            val byteSnapshotVerification = verifySnapshotBundleBytes(snapshotBundleBytes)
            assertTrue(byteSnapshotVerification.valid)
            assertSnapshotSummaryEquals(snapshotSummary, byteSnapshotVerification.summary)
            assertTrue(byteSnapshotVerification.missingCids.isEmpty())
            assertTrue(byteSnapshotVerification.extraCids.isEmpty())
            val incompleteSnapshotVerification =
                verifySnapshotBundle(snapshotBundle.copy(nodes = snapshotBundle.nodes.dropLast(1)))
            assertFalse(incompleteSnapshotVerification.valid)
            assertTrue(incompleteSnapshotVerification.missingCids.isNotEmpty())
            val decodedSnapshotBundle = snapshotBundleFromBytes(snapshotBundleBytes)
            ProllyEngine.memory(defaultConfig()).use { destination ->
                val importedTree = destination.importSnapshot(decodedSnapshotBundle)
                assertArrayEquals("v".bytes(), destination.get(importedTree, "k".bytes()))
            }
        }
    }

    @Test
    fun blobStoresLargeValuesAndBlobGcWorkThroughGeneratedKotlinApi() {
        ProllyNative.useLocalDebugLibrary()

        ProllyEngine.memory(defaultConfig()).use { engine ->
            ProllyBlobStore.memory().use { blobStore ->
                assertEquals(0UL, blobStore.blobCount())
                val directRef = blobStore.putBlob("direct".bytes())
                assertArrayEquals("direct".bytes(), blobStore.getBlob(directRef))
                blobRefValidateBytes(directRef, "direct".bytes())
                assertThrows<ProllyBindingException> {
                    blobRefValidateBytes(directRef, "wrong".bytes())
                }
                blobStore.deleteBlob(directRef)
                assertEquals(0UL, blobStore.blobCount())

                val empty = engine.create()
                val largeValue = ByteArray(64) { 42 }
                val tree = engine.putLargeValue(
                    blobStore,
                    empty,
                    "big".bytes(),
                    largeValue,
                    LargeValueConfigRecord(8UL),
                )
                val valueRef = engine.getValueRef(tree, "big".bytes())
                assertEquals(ValueRefKind.BLOB, valueRef?.kind)
                assertNotNull(valueRef?.blob)
                assertArrayEquals(largeValue, engine.getLargeValue(blobStore, tree, "big".bytes()))

                val reachable = engine.markReachableBlobs(listOf(tree))
                assertEquals(1UL, reachable.liveBlobCount)
                assertEquals(1, reachable.liveBlobs.size)
                assertEquals(0UL, engine.planBlobGc(blobStore, listOf(tree), reachable.liveBlobs).reclaimableBlobCount)

                blobStore.putBlob("orphan".bytes())
                assertEquals(2, blobStore.listBlobRefs().size)
                assertEquals(1UL, engine.planBlobStoreGc(blobStore, listOf(tree)).reclaimableBlobCount)
                assertEquals(1UL, engine.sweepBlobStoreGc(blobStore, listOf(tree)).deletedBlobs)
                assertEquals(1UL, blobStore.blobCount())

                val withoutBig = engine.delete(tree, "big".bytes())
                assertEquals(1UL, engine.planBlobStoreGc(blobStore, listOf(withoutBig)).reclaimableBlobCount)
                assertEquals(1UL, engine.sweepBlobStoreGc(blobStore, listOf(withoutBig)).deletedBlobs)
                assertEquals(0UL, blobStore.blobCount())
            }
        }
    }

    private fun String.bytes(): ByteArray = toByteArray()

    private fun assertSnapshotSummaryEquals(
        expected: SnapshotBundleSummaryRecord,
        actual: SnapshotBundleSummaryRecord,
    ) {
        assertEquals(expected.formatVersion, actual.formatVersion)
        if (expected.root == null || actual.root == null) {
            assertEquals(expected.root, actual.root)
        } else {
            assertArrayEquals(expected.root, actual.root)
        }
        assertEquals(expected.nodeCount, actual.nodeCount)
        assertEquals(expected.byteCount, actual.byteCount)
        assertEquals(expected.minNodeBytes, actual.minNodeBytes)
        assertEquals(expected.maxNodeBytes, actual.maxNodeBytes)
    }

    private fun <T> runSuspend(block: suspend () -> T): T {
        var value: Any? = null
        var failure: Throwable? = null
        block.startCoroutine(
            object : Continuation<T> {
                override val context = EmptyCoroutineContext

                override fun resumeWith(result: Result<T>) {
                    result.fold(
                        onSuccess = { value = it },
                        onFailure = { failure = it },
                    )
                }
            },
        )
        failure?.let { throw it }
        @Suppress("UNCHECKED_CAST")
        return value as T
    }
}
