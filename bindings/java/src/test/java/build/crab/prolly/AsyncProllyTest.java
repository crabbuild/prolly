package build.crab.prolly;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class AsyncProllyTest {
    @Test
    void completableFutureWrapperPreservesCoreBehavior() throws Exception {
        Prolly.useLocalDebugLibrary();

        try (AsyncProlly prolly = AsyncProlly.memory().get(5, TimeUnit.SECONDS)) {
            TreeRecord empty = prolly.create().get(5, TimeUnit.SECONDS);
            TreeRecord tree = prolly.batch(
                            empty,
                            List.of(
                                    Prolly.upsert(bytes("a"), bytes("1")),
                                    Prolly.upsert(bytes("b"), bytes("2"))))
                    .get(5, TimeUnit.SECONDS);

            assertArrayEquals(bytes("1"), prolly.get(tree, bytes("a")).get(5, TimeUnit.SECONDS).orElseThrow());
            KeyProof proof = prolly.proveKey(tree, bytes("a")).get(5, TimeUnit.SECONDS);
            assertArrayEquals(bytes("1"), Prolly.verifyKeyProof(proof).value());
            MultiKeyProof multiProof = prolly.proveKeys(tree, List.of(bytes("a"), bytes("missing"), bytes("b")))
                    .get(5, TimeUnit.SECONDS);
            MultiKeyProofVerification multiVerified = Prolly.verifyMultiKeyProof(multiProof);
            assertTrue(multiVerified.valid());
            assertArrayEquals(bytes("2"), multiVerified.results().get(2).value());
            RangeProof rangeProof = prolly.proveRange(tree, bytes("a"), Optional.of(bytes("c")))
                    .get(5, TimeUnit.SECONDS);
            RangeProofVerification rangeVerified = Prolly.verifyRangeProof(rangeProof);
            assertTrue(rangeVerified.valid());
            assertEquals(2, rangeVerified.entries().size());
            RangeProof prefixProof = prolly.provePrefix(tree, bytes("a")).get(5, TimeUnit.SECONDS);
            RangeProofVerification prefixVerified = Prolly.verifyRangeProof(prefixProof);
            assertTrue(prefixVerified.valid());
            assertEquals(1, prefixVerified.entries().size());
            ProvedRangePage provedPage = prolly.proveRangePage(
                            tree,
                            new RangeCursorRecord(bytes("a")),
                            Optional.empty(),
                            1)
                    .get(5, TimeUnit.SECONDS);
            RangePageProofVerification pageVerified = Prolly.verifyRangePageProof(provedPage.proof());
            assertTrue(pageVerified.valid());
            assertEquals(List.of(new Entry(bytes("b"), bytes("2"))), pageVerified.entries());
            assertArrayEquals(bytes("b"), prolly.rangeAfter(tree, bytes("a"), Optional.empty())
                    .get(5, TimeUnit.SECONDS)
                    .get(0)
                    .key());
            BatchApplyResult batchStats = prolly.batchWithStats(
                            empty,
                            List.of(
                                    Prolly.upsert(bytes("b"), bytes("2")),
                                    Prolly.upsert(bytes("a"), bytes("1")),
                                    Prolly.upsert(bytes("a"), bytes("11"))))
                    .get(5, TimeUnit.SECONDS);
            assertArrayEquals(bytes("11"), prolly.get(batchStats.tree(), bytes("a"))
                    .get(5, TimeUnit.SECONDS)
                    .orElseThrow());
            assertEquals(3, batchStats.stats().inputMutations());
            assertEquals(2, batchStats.stats().effectiveMutations());

            TreeRecord parallelTree = prolly.parallelBatch(
                            empty,
                            List.of(
                                    Prolly.upsert(bytes("p"), bytes("parallel")),
                                    Prolly.upsert(bytes("q"), bytes("async"))),
                            Prolly.parallelConfig(1, 1))
                    .get(5, TimeUnit.SECONDS);
            assertArrayEquals(bytes("async"), prolly.get(parallelTree, bytes("q"))
                    .get(5, TimeUnit.SECONDS)
                    .orElseThrow());
            BatchApplyResult parallelStats = prolly.parallelBatchWithStats(
                            empty,
                            List.of(
                                    Prolly.upsert(bytes("r"), bytes("route")),
                                    Prolly.upsert(bytes("s"), bytes("stats"))),
                            Prolly.parallelConfig(1, 1))
                    .get(5, TimeUnit.SECONDS);
            assertArrayEquals(bytes("stats"), prolly.get(parallelStats.tree(), bytes("s"))
                    .get(5, TimeUnit.SECONDS)
                    .orElseThrow());
            assertEquals(2, parallelStats.stats().inputMutations());
            assertEquals(2, parallelStats.stats().effectiveMutations());
            assertTrue(parallelStats.stats().writtenNodes() > 0);

            TreeRecord changed = prolly.put(tree, bytes("b"), bytes("22")).get(5, TimeUnit.SECONDS);
            assertEquals(1, prolly.diff(tree, changed).get(5, TimeUnit.SECONDS).size());
            assertArrayEquals(bytes("a"), prolly.firstEntry(tree).get(5, TimeUnit.SECONDS).orElseThrow().key());
            assertArrayEquals(bytes("b"), prolly.lastEntry(tree).get(5, TimeUnit.SECONDS).orElseThrow().key());
            assertArrayEquals(bytes("b"), prolly.lowerBound(tree, bytes("aa")).get(5, TimeUnit.SECONDS).orElseThrow().key());
            assertTrue(prolly.upperBound(tree, bytes("b")).get(5, TimeUnit.SECONDS).isEmpty());
            List<Entry> prefixEntries = prolly.prefix(tree, bytes("a")).get(5, TimeUnit.SECONDS);
            assertEquals(1, prefixEntries.size());
            assertArrayEquals(bytes("1"), prefixEntries.get(0).value());
            RangePageRecord prefixPage = prolly.prefixPage(tree, bytes("a"), null, 1).get(5, TimeUnit.SECONDS);
            assertEquals(1, prefixPage.getEntries().size());
            assertArrayEquals(bytes("1"), prefixPage.getEntries().get(0).getValue());
            CursorWindowRecord window = prolly.cursorWindow(tree, bytes("aa"), Optional.empty(), 1)
                    .get(5, TimeUnit.SECONDS);
            assertArrayEquals(bytes("a"), window.getPositionKey());
            assertFalse(window.getFound());
            assertEquals(1, window.getEntries().size());
            assertArrayEquals(bytes("b"), window.getEntries().get(0).getKey());
            assertNotNull(prolly.rangePage(changed, null, Optional.empty(), 1).get(5, TimeUnit.SECONDS).getNextCursor());
            ReversePageRecord reversePage = prolly.reversePage(changed, null, new byte[0], 2).get(5, TimeUnit.SECONDS);
            assertArrayEquals(bytes("b"), reversePage.getEntries().get(0).getKey());
            assertArrayEquals(bytes("a"), reversePage.getEntries().get(1).getKey());
            ReversePageRecord prefixReversePage =
                    prolly.prefixReversePage(changed, bytes("a"), null, 2).get(5, TimeUnit.SECONDS);
            assertEquals(1, prefixReversePage.getEntries().size());
            assertArrayEquals(bytes("a"), prefixReversePage.getEntries().get(0).getKey());
        }
    }

    @Test
    void completableFutureWrapperCoversAdvancedApis() throws Exception {
        Prolly.useLocalDebugLibrary();

        try (AsyncProlly prolly = await(AsyncProlly.memory());
                AsyncBlobStore blobStore = await(AsyncBlobStore.memory())) {
            BlobRef directRef = await(blobStore.putBlob(bytes("direct")));
            assertArrayEquals(bytes("direct"), await(blobStore.getBlob(directRef)).orElseThrow());
            assertEquals(1, await(blobStore.blobCount()));
            await(blobStore.deleteBlob(directRef));
            assertEquals(0, await(blobStore.blobCount()));

            TreeRecord empty = await(prolly.create());
            byte[] largeValue = repeated((byte) 7, 32);
            TreeRecord largeTree = await(prolly.putLargeValue(
                    blobStore,
                    empty,
                    bytes("big"),
                    largeValue,
                    Prolly.largeValueConfig(8)));
            assertEquals(ValueRef.Kind.BLOB, await(prolly.getValueRef(largeTree, bytes("big"))).orElseThrow().kind());
            assertArrayEquals(largeValue, await(prolly.getLargeValue(blobStore, largeTree, bytes("big"))).orElseThrow());
            assertEquals(1, await(prolly.planBlobStoreGc(blobStore, List.of(largeTree))).reachability().liveBlobCount());

            TreeRecord base = await(prolly.put(empty, bytes("k"), bytes("base")));
            TreeRecord left = await(prolly.put(base, bytes("k"), bytes("left")));
            TreeRecord right = await(prolly.put(base, bytes("k"), bytes("right")));
            TreeRecord merged = await(prolly.merge(base, left, right, "prefer_right"));
            assertArrayEquals(bytes("right"), await(prolly.get(merged, bytes("k"))).orElseThrow());
            assertNotNull(await(prolly.mergeExplain(base, left, right, "prefer_right")).getResult());
            MergeResolverCallback resolver =
                    conflict -> Prolly.resolutionValue(concat(conflict.getLeft(), bytes("|"), conflict.getRight()));
            TreeRecord callbackMerged = await(prolly.mergeWithResolver(base, left, right, resolver));
            assertArrayEquals(bytes("left|right"), await(prolly.get(callbackMerged, bytes("k"))).orElseThrow());
            assertNotNull(await(prolly.mergeExplainWithResolver(base, left, right, resolver)).getResult());
            try (MergePolicyRegistry policy = Prolly.mergePolicyRegistry()) {
                policy.setDefaultResolver(resolver);
                TreeRecord policyMerged = await(prolly.mergeWithPolicy(base, left, right, policy));
                assertArrayEquals(bytes("left|right"), await(prolly.get(policyMerged, bytes("k"))).orElseThrow());
            }
            CrdtResolverCallback crdtResolver =
                    conflict -> Prolly.crdtResolutionValue(concat(conflict.getLeft(), bytes("|"), conflict.getRight()));
            TreeRecord crdtCallbackMerged =
                    await(prolly.crdtMergeWithResolver(base, left, right, CrdtDeletePolicyKind.UPDATE_WINS, crdtResolver));
            assertArrayEquals(bytes("left|right"), await(prolly.get(crdtCallbackMerged, bytes("k"))).orElseThrow());

            await(prolly.publishNamedRootAtMillis(bytes("main"), merged, 42));
            assertTrue(await(prolly.loadNamedRoot(bytes("main"))).isPresent());
            assertEquals(1, await(prolly.listNamedRoots()).size());
            List<NamedRootManifest> manifests = await(prolly.listNamedRootManifests());
            assertEquals(1, manifests.size());
            assertArrayEquals(bytes("main"), manifests.get(0).name());
            assertArrayEquals(merged.getRoot(), manifests.get(0).manifest().tree().getRoot());
            assertEquals(42L, manifests.get(0).manifest().createdAtMillis());
            assertEquals(42L, manifests.get(0).manifest().updatedAtMillis());
            assertTrue(await(prolly.planStoreGcForRetention(Prolly.retainAllNamedRoots()))
                    .reachability()
                    .liveNodes() > 0);
            NamedRootUpdateRecord update =
                    await(prolly.compareAndSwapNamedRootAtMillis(bytes("main"), Optional.of(merged), Optional.empty(), 43));
            assertTrue(update.getApplied());
            assertEquals(false, update.getConflict());

            SnapshotNamespaceRecord branch = Prolly.snapshotNamespaceBranch();
            await(prolly.publishSnapshotAtMillis(branch, bytes("main"), merged, 77));
            assertTrue(await(prolly.loadSnapshot(branch, bytes("main"))).isPresent());
            assertEquals(1, await(prolly.listSnapshots(branch)).size());
            SnapshotSelection snapshotSelection =
                    await(prolly.loadSnapshots(branch, List.of(bytes("main"), bytes("missing"))));
            assertEquals(1, snapshotSelection.snapshots().size());
            assertEquals(1, snapshotSelection.missingIds().size());
            NamedRootUpdateRecord snapshotUpdate =
                    await(prolly.compareAndSwapSnapshot(branch, bytes("main"), Optional.of(merged), Optional.empty()));
            assertTrue(snapshotUpdate.getApplied());
            assertEquals(false, snapshotUpdate.getConflict());
            assertTrue(await(prolly.loadSnapshot(branch, bytes("main"))).isEmpty());

            assertTrue(await(prolly.collectStatsJson(largeTree)).contains("\"num_nodes\""));
            TreeStatsRecord typedStats = await(prolly.collectStats(largeTree));
            assertTrue(Prolly.treeStatsTotalKeyValuePairs(typedStats) > 0);
            assertTrue(Prolly.treeStatsLevelCount(typedStats, 0) > 0);
            StatsComparisonRecord typedDiffStats = await(prolly.statsDiff(empty, largeTree));
            assertEquals(0L, Prolly.statsComparisonBeforeTotalKeyValuePairs(typedDiffStats));
            assertTrue(Prolly.statsComparisonAfterTotalKeyValuePairs(typedDiffStats) > 0);
            assertTrue(Prolly.statsDiffTotalKeyValuePairs(typedDiffStats) > 0);
            TreeDebugViewRecord debugTree = await(prolly.debugTree(largeTree));
            assertTrue(Prolly.treeDebugViewLevelCount(debugTree) > 0);
            TreeDebugComparisonRecord debugComparison = await(prolly.debugCompareTrees(empty, largeTree));
            assertTrue(Prolly.treeDebugComparisonRightOnlyNodes(debugComparison) > 0);
            assertTrue(await(prolly.pinTreeRoot(largeTree)) > 0);
            assertTrue(await(prolly.cacheStats()).pinnedNodes() > 0);
            assertTrue(await(prolly.unpinAllCacheNodes()) > 0);
            await(prolly.clearCache());

            GcReachability reachability = await(prolly.markReachable(List.of(largeTree)));
            assertTrue(reachability.liveNodes() > 0);
            List<byte[]> nodeCids = await(prolly.listNodeCids());
            assertTrue(nodeCids.size() > 0);
            assertEquals(nodeCids.size(), await(prolly.planGc(List.of(largeTree), nodeCids)).candidateNodes());

            try (AsyncProlly destination = await(AsyncProlly.memory())) {
                MissingNodePlan missing = await(prolly.planMissingNodes(largeTree, destination));
                assertTrue(missing.missingNodes() > 0);
                MissingNodeCopy copied = await(prolly.copyMissingNodes(largeTree, destination));
                assertEquals(missing.missingNodes(), copied.copiedNodes());
            }

            SnapshotBundleRecord snapshotBundle = await(prolly.exportSnapshot(largeTree));
            assertEquals(1, Prolly.snapshotBundleFormatVersion(snapshotBundle));
            assertTrue(Prolly.snapshotBundleNodeCount(snapshotBundle) > 0);
            byte[] snapshotBundleBytes = Prolly.snapshotBundleToBytes(snapshotBundle);
            assertArrayEquals(
                    Prolly.cidFromBytes(snapshotBundleBytes),
                    Prolly.snapshotBundleDigestBytes(snapshotBundleBytes));
            SnapshotBundleVerificationRecord snapshotVerification =
                    Prolly.verifySnapshotBundleBytes(snapshotBundleBytes);
            assertTrue(Prolly.snapshotBundleVerificationValid(snapshotVerification));
            assertEquals(
                    Prolly.snapshotBundleNodeCount(snapshotBundle),
                    Prolly.snapshotBundleSummaryNodeCount(snapshotVerification.getSummary()));
            SnapshotBundleRecord decodedSnapshotBundle = Prolly.snapshotBundleFromBytes(snapshotBundleBytes);
            try (AsyncProlly snapshotDestination = await(AsyncProlly.memory())) {
                TreeRecord importedTree = await(snapshotDestination.importSnapshot(decodedSnapshotBundle));
                assertArrayEquals(
                        largeValue,
                        await(snapshotDestination.getLargeValue(blobStore, importedTree, bytes("big"))).orElseThrow());
            }
        }
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(5, TimeUnit.SECONDS);
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
}
