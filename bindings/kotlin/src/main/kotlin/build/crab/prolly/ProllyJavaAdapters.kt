package build.crab.prolly

object ProllyJavaAdapters {
    @JvmStatic
    fun config(
        minChunkSize: Long,
        maxChunkSize: Long,
        chunkingFactor: Int,
        hashSeed: Long,
        encodingKind: String,
        customEncodingName: String?,
        nodeCacheMaxNodes: Long?,
        nodeCacheMaxBytes: Long?,
    ): ConfigRecord =
        ConfigRecord(
            minChunkSize.toULong(),
            maxChunkSize.toULong(),
            chunkingFactor.toUInt(),
            hashSeed.toULong(),
            EncodingRecord(encodingKind(encodingKind), customEncodingName),
            nodeCacheMaxNodes?.toULong(),
            nodeCacheMaxBytes?.toULong(),
        )

    @JvmStatic
    fun treeConfig(
        minChunkSize: Long,
        maxChunkSize: Long,
        chunkingFactor: Int,
        hashSeed: Long,
        encoding: EncodingRecord,
        nodeCacheMaxNodes: Long?,
        nodeCacheMaxBytes: Long?,
    ): ConfigRecord =
        build.crab.prolly.treeConfig(
            minChunkSize.toULong(),
            maxChunkSize.toULong(),
            chunkingFactor.toUInt(),
            hashSeed.toULong(),
            encoding,
            nodeCacheMaxNodes?.toULong(),
            nodeCacheMaxBytes?.toULong(),
        )

    @JvmStatic
    fun configNodeCacheMaxNodes(record: ConfigRecord): Long? =
        record.nodeCacheMaxNodes?.toLong()

    @JvmStatic
    fun isBoundaryConfig(config: ConfigRecord, count: Long, key: ByteArray, value: ByteArray): Boolean =
        build.crab.prolly.isBoundaryConfig(config, count.toULong(), key, value)

    @JvmStatic
    fun u64Key(value: String): ByteArray =
        build.crab.prolly.u64Key(value.toULong())

    @JvmStatic
    fun u128Key(value: String): ByteArray =
        build.crab.prolly.u128Key(value)

    @JvmStatic
    fun i128Key(value: String): ByteArray =
        build.crab.prolly.i128Key(value)

    @JvmStatic
    fun timestampMillisKey(value: String): ByteArray =
        build.crab.prolly.timestampMillisKey(value.toULong())

    @JvmStatic
    fun upsertMutation(key: ByteArray, value: ByteArray): MutationRecord =
        MutationRecord(MutationKind.UPSERT, key, value)

    @JvmStatic
    fun deleteMutation(key: ByteArray): MutationRecord =
        MutationRecord(MutationKind.DELETE, key, null)

    @JvmStatic
    fun parallelConfig(maxThreads: Long, parallelismThreshold: Long): ParallelConfigRecord =
        ParallelConfigRecord(maxThreads.toULong(), parallelismThreshold.toULong())

    @JvmStatic
    fun parallelConfigSequential(): ParallelConfigRecord =
        build.crab.prolly.parallelConfigSequential()

    @JvmStatic
    fun parallelConfigMaxThreads(record: ParallelConfigRecord): Long =
        record.maxThreads.toLong()

    @JvmStatic
    fun firstEntry(engine: ProllyEngine, tree: TreeRecord): EntryRecord? =
        engine.firstEntry(tree)

    @JvmStatic
    fun lastEntry(engine: ProllyEngine, tree: TreeRecord): EntryRecord? =
        engine.lastEntry(tree)

    @JvmStatic
    fun lowerBound(engine: ProllyEngine, tree: TreeRecord, key: ByteArray): EntryRecord? =
        engine.lowerBound(tree, key)

    @JvmStatic
    fun upperBound(engine: ProllyEngine, tree: TreeRecord, key: ByteArray): EntryRecord? =
        engine.upperBound(tree, key)

    @JvmStatic
    fun prefix(engine: ProllyEngine, tree: TreeRecord, prefix: ByteArray): List<EntryRecord> =
        engine.prefix(tree, prefix)

    @JvmStatic
    fun prefixPage(
        engine: ProllyEngine,
        tree: TreeRecord,
        prefix: ByteArray,
        cursor: RangeCursorRecord?,
        limit: Long,
    ): RangePageRecord =
        engine.prefixPage(tree, prefix, cursor, limit.toULong())

    @JvmStatic
    fun prefixReversePage(
        engine: ProllyEngine,
        tree: TreeRecord,
        prefix: ByteArray,
        cursor: ReverseCursorRecord?,
        limit: Long,
    ): ReversePageRecord =
        engine.prefixReversePage(tree, prefix, cursor, limit.toULong())

    @JvmStatic
    fun rangePage(
        engine: ProllyEngine,
        tree: TreeRecord,
        cursor: RangeCursorRecord?,
        end: ByteArray?,
        limit: Long,
    ): RangePageRecord =
        engine.rangePage(tree, cursor, end, limit.toULong())

    @JvmStatic
    fun reversePage(
        engine: ProllyEngine,
        tree: TreeRecord,
        cursor: ReverseCursorRecord?,
        start: ByteArray,
        limit: Long,
    ): ReversePageRecord =
        engine.reversePage(tree, cursor, start, limit.toULong())

    @JvmStatic
    fun cursorWindow(
        engine: ProllyEngine,
        tree: TreeRecord,
        key: ByteArray,
        end: ByteArray?,
        limit: Long,
    ): CursorWindowRecord =
        engine.cursorWindow(tree, key, end, limit.toULong())

    @JvmStatic
    fun proveRangePage(
        engine: ProllyEngine,
        tree: TreeRecord,
        cursor: RangeCursorRecord?,
        end: ByteArray?,
        limit: Long,
    ): ProvedRangePageRecord =
        engine.proveRangePage(tree, cursor, end, limit.toULong())

    @JvmStatic
    fun proveDiffPage(
        engine: ProllyEngine,
        base: TreeRecord,
        other: TreeRecord,
        cursor: RangeCursorRecord?,
        end: ByteArray?,
        limit: Long,
    ): ProvedDiffPageRecord =
        engine.proveDiffPage(base, other, cursor, end, limit.toULong())

    @JvmStatic
    fun diffPage(
        engine: ProllyEngine,
        base: TreeRecord,
        other: TreeRecord,
        cursor: RangeCursorRecord?,
        end: ByteArray?,
        limit: Long,
    ): DiffPageRecord =
        engine.diffPage(base, other, cursor, end, limit.toULong())

    @JvmStatic
    fun conflictPage(
        engine: ProllyEngine,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        cursor: RangeCursorRecord?,
        limit: Long,
    ): ConflictPageRecord =
        engine.conflictPage(base, left, right, cursor, limit.toULong())

    @JvmStatic
    fun structuralDiffPage(
        engine: ProllyEngine,
        base: TreeRecord,
        other: TreeRecord,
        cursorJson: String?,
        limit: Long,
    ): StructuralDiffPageRecord =
        engine.structuralDiffPage(base, other, cursorJson, limit.toULong())

    @JvmStatic
    fun structuralDiffPageWithCursor(
        engine: ProllyEngine,
        base: TreeRecord,
        other: TreeRecord,
        cursor: StructuralDiffCursorRecord?,
        limit: Long,
    ): StructuralDiffPageRecord =
        engine.structuralDiffPageWithCursor(base, other, cursor, limit.toULong())

    @JvmStatic
    fun publishNamedRootAtMillis(
        engine: ProllyEngine,
        name: ByteArray,
        tree: TreeRecord,
        timestampMillis: Long,
    ) {
        engine.publishNamedRootAtMillis(name, tree, timestampMillis.toULong())
    }

    @JvmStatic
    fun compareAndSwapNamedRootAtMillis(
        engine: ProllyEngine,
        name: ByteArray,
        expected: TreeRecord?,
        replacement: TreeRecord?,
        timestampMillis: Long,
    ): NamedRootUpdateRecord =
        engine.compareAndSwapNamedRootAtMillis(name, expected, replacement, timestampMillis.toULong())

    @JvmStatic
    fun snapshotNamespaceBranch(): SnapshotNamespaceRecord =
        build.crab.prolly.snapshotNamespaceBranch()

    @JvmStatic
    fun snapshotNamespaceTag(): SnapshotNamespaceRecord =
        build.crab.prolly.snapshotNamespaceTag()

    @JvmStatic
    fun snapshotNamespaceCheckpoint(): SnapshotNamespaceRecord =
        build.crab.prolly.snapshotNamespaceCheckpoint()

    @JvmStatic
    fun snapshotNamespaceCustom(prefix: ByteArray): SnapshotNamespaceRecord =
        build.crab.prolly.snapshotNamespaceCustom(prefix)

    @JvmStatic
    fun snapshotRootName(namespace: SnapshotNamespaceRecord, id: ByteArray): ByteArray =
        build.crab.prolly.snapshotRootName(namespace, id)

    @JvmStatic
    fun snapshotIdFromName(namespace: SnapshotNamespaceRecord, name: ByteArray): ByteArray? =
        build.crab.prolly.snapshotIdFromName(namespace, name)

    @JvmStatic
    fun publishSnapshotAtMillis(
        engine: ProllyEngine,
        namespace: SnapshotNamespaceRecord,
        id: ByteArray,
        tree: TreeRecord,
        timestampMillis: Long,
    ) {
        engine.publishSnapshotAtMillis(namespace, id, tree, timestampMillis.toULong())
    }

    @JvmStatic
    fun compareAndSwapSnapshotAtMillis(
        engine: ProllyEngine,
        namespace: SnapshotNamespaceRecord,
        id: ByteArray,
        expected: TreeRecord?,
        replacement: TreeRecord?,
        timestampMillis: Long,
    ): NamedRootUpdateRecord =
        engine.compareAndSwapSnapshotAtMillis(namespace, id, expected, replacement, timestampMillis.toULong())

    @JvmStatic
    fun snapshotCreatedAtMillis(record: SnapshotRecord): Long? =
        record.createdAtMillis?.toLong()

    @JvmStatic
    fun snapshotUpdatedAtMillis(record: SnapshotRecord): Long? =
        record.updatedAtMillis?.toLong()

    @JvmStatic
    fun treeStatsNumNodes(record: TreeStatsRecord): Long =
        record.numNodes.toLong()

    @JvmStatic
    fun treeStatsTotalKeyValuePairs(record: TreeStatsRecord): Long =
        record.totalKeyValuePairs.toLong()

    @JvmStatic
    fun treeStatsLevelCount(record: TreeStatsRecord, level: Int): Long =
        record.nodesPerLevel.firstOrNull { it.level.toInt() == level }?.value?.toLong() ?: 0L

    @JvmStatic
    fun statsComparisonBeforeTotalKeyValuePairs(record: StatsComparisonRecord): Long =
        record.before.totalKeyValuePairs.toLong()

    @JvmStatic
    fun statsComparisonAfterTotalKeyValuePairs(record: StatsComparisonRecord): Long =
        record.after.totalKeyValuePairs.toLong()

    @JvmStatic
    fun statsDiffTotalKeyValuePairs(record: StatsComparisonRecord): Long =
        record.absolute.totalKeyValuePairsDiff

    @JvmStatic
    fun treeDebugViewLevelCount(record: TreeDebugViewRecord): Long =
        record.levels.size.toLong()

    @JvmStatic
    fun treeDebugViewFirstLevelNodeCount(record: TreeDebugViewRecord): Long =
        record.levels.firstOrNull()?.nodes?.size?.toLong() ?: 0L

    @JvmStatic
    fun treeDebugComparisonLeftOnlyNodes(record: TreeDebugComparisonRecord): Long =
        record.leftOnlyNodes.toLong()

    @JvmStatic
    fun treeDebugComparisonRightOnlyNodes(record: TreeDebugComparisonRecord): Long =
        record.rightOnlyNodes.toLong()

    @JvmStatic
    fun treeDebugComparisonHasRightOnlyNode(record: TreeDebugComparisonRecord): Boolean =
        record.levels.any { level ->
            level.nodes.any { node -> node.status == TreeDebugNodeStatusKind.RIGHT_ONLY }
        }

    @JvmStatic
    fun rootManifestCreatedAtMillis(record: RootManifestRecord): Long? =
        record.createdAtMillis?.toLong()

    @JvmStatic
    fun rootManifestUpdatedAtMillis(record: RootManifestRecord): Long? =
        record.updatedAtMillis?.toLong()

    @JvmStatic
    fun versionedValueBytesMatchesSchema(bytes: ByteArray, schema: String, version: Long): Boolean =
        build.crab.prolly.versionedValueBytesMatchesSchema(bytes, schema, version.toULong())

    @JvmStatic
    fun versionedValueBytesRequireSchema(bytes: ByteArray, schema: String, version: Long) {
        build.crab.prolly.versionedValueBytesRequireSchema(bytes, schema, version.toULong())
    }

    @JvmStatic
    fun keyProofPathNodeBytes(proof: KeyProofRecord): List<ByteArray> =
        build.crab.prolly.keyProofPathNodeBytes(proof)

    @JvmStatic
    fun keyProofToBytes(proof: KeyProofRecord): ByteArray =
        build.crab.prolly.keyProofToBytes(proof)

    @JvmStatic
    fun keyProofFromBytes(bytes: ByteArray): KeyProofRecord =
        build.crab.prolly.keyProofFromBytes(bytes)

    @JvmStatic
    fun keyProofFromNodeBytes(
        root: ByteArray?,
        key: ByteArray,
        pathNodeBytes: List<ByteArray>,
    ): KeyProofRecord =
        build.crab.prolly.keyProofFromNodeBytes(root, key, pathNodeBytes)

    @JvmStatic
    fun verifyKeyProof(proof: KeyProofRecord): KeyProofVerificationRecord =
        build.crab.prolly.verifyKeyProof(proof)

    @JvmStatic
    fun multiKeyProofPathNodeBytes(proof: MultiKeyProofRecord): List<ByteArray> =
        build.crab.prolly.multiKeyProofPathNodeBytes(proof)

    @JvmStatic
    fun multiKeyProofToBytes(proof: MultiKeyProofRecord): ByteArray =
        build.crab.prolly.multiKeyProofToBytes(proof)

    @JvmStatic
    fun multiKeyProofFromBytes(bytes: ByteArray): MultiKeyProofRecord =
        build.crab.prolly.multiKeyProofFromBytes(bytes)

    @JvmStatic
    fun multiKeyProofFromNodeBytes(
        root: ByteArray?,
        keys: List<ByteArray>,
        pathNodeBytes: List<ByteArray>,
    ): MultiKeyProofRecord =
        build.crab.prolly.multiKeyProofFromNodeBytes(root, keys, pathNodeBytes)

    @JvmStatic
    fun verifyMultiKeyProof(proof: MultiKeyProofRecord): MultiKeyProofVerificationRecord =
        build.crab.prolly.verifyMultiKeyProof(proof)

    @JvmStatic
    fun rangeProofPathNodeBytes(proof: RangeProofRecord): List<ByteArray> =
        build.crab.prolly.rangeProofPathNodeBytes(proof)

    @JvmStatic
    fun rangeProofToBytes(proof: RangeProofRecord): ByteArray =
        build.crab.prolly.rangeProofToBytes(proof)

    @JvmStatic
    fun rangeProofFromBytes(bytes: ByteArray): RangeProofRecord =
        build.crab.prolly.rangeProofFromBytes(bytes)

    @JvmStatic
    fun rangeProofFromNodeBytes(
        root: ByteArray?,
        start: ByteArray,
        end: ByteArray?,
        pathNodeBytes: List<ByteArray>,
    ): RangeProofRecord =
        build.crab.prolly.rangeProofFromNodeBytes(root, start, end, pathNodeBytes)

    @JvmStatic
    fun verifyRangeProof(proof: RangeProofRecord): RangeProofVerificationRecord =
        build.crab.prolly.verifyRangeProof(proof)

    @JvmStatic
    fun rangePageProofPathNodeBytes(proof: RangePageProofRecord): List<ByteArray> =
        build.crab.prolly.rangePageProofPathNodeBytes(proof)

    @JvmStatic
    fun rangePageProofToBytes(proof: RangePageProofRecord): ByteArray =
        build.crab.prolly.rangePageProofToBytes(proof)

    @JvmStatic
    fun rangePageProofFromBytes(bytes: ByteArray): RangePageProofRecord =
        build.crab.prolly.rangePageProofFromBytes(bytes)

    @JvmStatic
    fun rangePageProofFromNodeBytes(
        root: ByteArray?,
        after: ByteArray?,
        end: ByteArray?,
        pathNodeBytes: List<ByteArray>,
    ): RangePageProofRecord =
        build.crab.prolly.rangePageProofFromNodeBytes(root, after, end, pathNodeBytes)

    @JvmStatic
    fun verifyRangePageProof(proof: RangePageProofRecord): RangePageProofVerificationRecord =
        build.crab.prolly.verifyRangePageProof(proof)

    @JvmStatic
    fun diffPageProofRecord(
        base: RangePageProofRecord,
        other: RangePageProofRecord,
        lookaheadBase: KeyProofRecord?,
        lookaheadOther: KeyProofRecord?,
        requestedEnd: ByteArray?,
        limit: Long,
    ): DiffPageProofRecord =
        DiffPageProofRecord(base, other, lookaheadBase, lookaheadOther, requestedEnd, limit.toULong())

    @JvmStatic
    fun diffPageProofLimit(record: DiffPageProofRecord): Long =
        record.limit.toLong()

    @JvmStatic
    fun diffPageProofVerificationLimit(record: DiffPageProofVerificationRecord): Long =
        record.limit.toLong()

    @JvmStatic
    fun diffPageProofToBytes(proof: DiffPageProofRecord): ByteArray =
        build.crab.prolly.diffPageProofToBytes(proof)

    @JvmStatic
    fun diffPageProofFromBytes(bytes: ByteArray): DiffPageProofRecord =
        build.crab.prolly.diffPageProofFromBytes(bytes)

    @JvmStatic
    fun verifyDiffPageProof(proof: DiffPageProofRecord): DiffPageProofVerificationRecord =
        build.crab.prolly.verifyDiffPageProof(proof)

    @JvmStatic
    fun inspectProofBundle(bytes: ByteArray): ProofBundleSummaryRecord =
        build.crab.prolly.inspectProofBundle(bytes)

    @JvmStatic
    fun verifyProofBundle(bytes: ByteArray): ProofBundleVerificationRecord =
        build.crab.prolly.verifyProofBundle(bytes)

    @JvmStatic
    fun proofBundleSummaryVersion(record: ProofBundleSummaryRecord): Long =
        record.version.toLong()

    @JvmStatic
    fun proofBundleSummaryKeyCount(record: ProofBundleSummaryRecord): Long =
        record.keyCount.toLong()

    @JvmStatic
    fun proofBundleSummaryPathNodeCount(record: ProofBundleSummaryRecord): Long =
        record.pathNodeCount.toLong()

    @JvmStatic
    fun proofBundleSummaryLimit(record: ProofBundleSummaryRecord): Long? =
        record.limit?.toLong()

    @JvmStatic
    fun proofBundleVerificationExistsCount(record: ProofBundleVerificationRecord): Long =
        record.existsCount.toLong()

    @JvmStatic
    fun proofBundleVerificationAbsenceCount(record: ProofBundleVerificationRecord): Long =
        record.absenceCount.toLong()

    @JvmStatic
    fun proofBundleVerificationEntryCount(record: ProofBundleVerificationRecord): Long =
        record.entryCount.toLong()

    @JvmStatic
    fun proofBundleVerificationDiffCount(record: ProofBundleVerificationRecord): Long =
        record.diffCount.toLong()

    @JvmStatic
    fun authenticatedProofEnvelopeRecord(
        algorithm: String,
        keyId: ByteArray,
        proofBundle: ByteArray,
        context: ByteArray,
        issuedAtMillis: Long?,
        expiresAtMillis: Long?,
        nonce: ByteArray,
        signature: ByteArray,
    ): AuthenticatedProofEnvelopeRecord =
        AuthenticatedProofEnvelopeRecord(
            algorithm,
            keyId,
            proofBundle,
            context,
            issuedAtMillis?.toULong(),
            expiresAtMillis?.toULong(),
            nonce,
            signature,
        )

    @JvmStatic
    fun authenticatedProofEnvelopeIssuedAtMillis(record: AuthenticatedProofEnvelopeRecord): Long? =
        record.issuedAtMillis?.toLong()

    @JvmStatic
    fun authenticatedProofEnvelopeExpiresAtMillis(record: AuthenticatedProofEnvelopeRecord): Long? =
        record.expiresAtMillis?.toLong()

    @JvmStatic
    fun authenticatedProofEnvelopeVerificationIssuedAtMillis(
        record: AuthenticatedProofEnvelopeVerificationRecord,
    ): Long? =
        record.issuedAtMillis?.toLong()

    @JvmStatic
    fun authenticatedProofEnvelopeVerificationExpiresAtMillis(
        record: AuthenticatedProofEnvelopeVerificationRecord,
    ): Long? =
        record.expiresAtMillis?.toLong()

    @JvmStatic
    fun signProofBundleHmacSha256(
        proofBundle: ByteArray,
        keyId: ByteArray,
        secret: ByteArray,
        context: ByteArray,
        issuedAtMillis: Long?,
        expiresAtMillis: Long?,
        nonce: ByteArray,
    ): AuthenticatedProofEnvelopeRecord =
        build.crab.prolly.signProofBundleHmacSha256(
            proofBundle,
            keyId,
            secret,
            context,
            issuedAtMillis?.toULong(),
            expiresAtMillis?.toULong(),
            nonce,
        )

    @JvmStatic
    fun verifyAuthenticatedProofEnvelope(
        envelope: AuthenticatedProofEnvelopeRecord,
        secret: ByteArray,
        nowMillis: Long?,
    ): AuthenticatedProofEnvelopeVerificationRecord =
        build.crab.prolly.verifyAuthenticatedProofEnvelope(envelope, secret, nowMillis?.toULong())

    @JvmStatic
    fun verifyAuthenticatedProofBundle(
        envelopeBytes: ByteArray,
        secret: ByteArray,
        nowMillis: Long?,
    ): AuthenticatedProofBundleVerificationRecord =
        build.crab.prolly.verifyAuthenticatedProofBundle(envelopeBytes, secret, nowMillis?.toULong())

    @JvmStatic
    fun authenticatedProofEnvelopeToBytes(envelope: AuthenticatedProofEnvelopeRecord): ByteArray =
        build.crab.prolly.authenticatedProofEnvelopeToBytes(envelope)

    @JvmStatic
    fun authenticatedProofEnvelopeFromBytes(bytes: ByteArray): AuthenticatedProofEnvelopeRecord =
        build.crab.prolly.authenticatedProofEnvelopeFromBytes(bytes)

    @JvmStatic
    fun pinTreeRoot(engine: ProllyEngine, tree: TreeRecord): Long =
        engine.pinTreeRoot(tree).toLong()

    @JvmStatic
    fun pinTreePath(engine: ProllyEngine, tree: TreeRecord, key: ByteArray): Long =
        engine.pinTreePath(tree, key).toLong()

    @JvmStatic
    fun unpinAllCacheNodes(engine: ProllyEngine): Long =
        engine.unpinAllCacheNodes().toLong()

    @JvmStatic
    fun cacheStatsCachedNodes(record: CacheStatsRecord): Long =
        record.cachedNodes.toLong()

    @JvmStatic
    fun cacheStatsCachedBytes(record: CacheStatsRecord): Long =
        record.cachedBytes.toLong()

    @JvmStatic
    fun cacheStatsPinnedNodes(record: CacheStatsRecord): Long =
        record.pinnedNodes.toLong()

    @JvmStatic
    fun cacheStatsPinnedBytes(record: CacheStatsRecord): Long =
        record.pinnedBytes.toLong()

    @JvmStatic
    fun metricsNodeCacheHits(record: MetricsRecord): Long =
        record.nodeCacheHits.toLong()

    @JvmStatic
    fun metricsNodeCacheMisses(record: MetricsRecord): Long =
        record.nodeCacheMisses.toLong()

    @JvmStatic
    fun metricsNodeCacheEvictions(record: MetricsRecord): Long =
        record.nodeCacheEvictions.toLong()

    @JvmStatic
    fun metricsNodesRead(record: MetricsRecord): Long =
        record.nodesRead.toLong()

    @JvmStatic
    fun metricsBytesRead(record: MetricsRecord): Long =
        record.bytesRead.toLong()

    @JvmStatic
    fun metricsNodesWritten(record: MetricsRecord): Long =
        record.nodesWritten.toLong()

    @JvmStatic
    fun metricsBytesWritten(record: MetricsRecord): Long =
        record.bytesWritten.toLong()

    @JvmStatic
    fun metricsStoreGetCalls(record: MetricsRecord): Long =
        record.storeGetCalls.toLong()

    @JvmStatic
    fun metricsStoreBatchGetCalls(record: MetricsRecord): Long =
        record.storeBatchGetCalls.toLong()

    @JvmStatic
    fun metricsStoreBatchGetKeys(record: MetricsRecord): Long =
        record.storeBatchGetKeys.toLong()

    @JvmStatic
    fun metricsStorePutCalls(record: MetricsRecord): Long =
        record.storePutCalls.toLong()

    @JvmStatic
    fun metricsStoreBatchPutCalls(record: MetricsRecord): Long =
        record.storeBatchPutCalls.toLong()

    @JvmStatic
    fun metricsStoreBatchPutNodes(record: MetricsRecord): Long =
        record.storeBatchPutNodes.toLong()

    @JvmStatic
    fun diffStatsComparedNodes(record: DiffTraversalStatsRecord): Long =
        record.comparedNodes.toLong()

    @JvmStatic
    fun diffStatsReusedSubtrees(record: DiffTraversalStatsRecord): Long =
        record.reusedSubtrees.toLong()

    @JvmStatic
    fun diffStatsAddedSubtrees(record: DiffTraversalStatsRecord): Long =
        record.addedSubtrees.toLong()

    @JvmStatic
    fun diffStatsRemovedSubtrees(record: DiffTraversalStatsRecord): Long =
        record.removedSubtrees.toLong()

    @JvmStatic
    fun diffStatsCollectedFallbacks(record: DiffTraversalStatsRecord): Long =
        record.collectedFallbacks.toLong()

    @JvmStatic
    fun diffStatsEmittedDiffs(record: DiffTraversalStatsRecord): Long =
        record.emittedDiffs.toLong()

    @JvmStatic
    fun batchResultTree(record: BatchApplyResultRecord): TreeRecord =
        record.tree

    @JvmStatic
    fun batchResultStats(record: BatchApplyResultRecord): BatchApplyStatsRecord =
        record.stats

    @JvmStatic
    fun batchStatsInputMutations(record: BatchApplyStatsRecord): Long =
        record.inputMutations.toLong()

    @JvmStatic
    fun batchStatsEffectiveMutations(record: BatchApplyStatsRecord): Long =
        record.effectiveMutations.toLong()

    @JvmStatic
    fun batchStatsAffectedLeaves(record: BatchApplyStatsRecord): Long =
        record.affectedLeaves.toLong()

    @JvmStatic
    fun batchStatsChangedLeaves(record: BatchApplyStatsRecord): Long =
        record.changedLeaves.toLong()

    @JvmStatic
    fun batchStatsSparseLeafApplies(record: BatchApplyStatsRecord): Long =
        record.sparseLeafApplies.toLong()

    @JvmStatic
    fun batchStatsWrittenNodes(record: BatchApplyStatsRecord): Long =
        record.writtenNodes.toLong()

    @JvmStatic
    fun batchStatsWrittenBytes(record: BatchApplyStatsRecord): Long =
        record.writtenBytes.toLong()

    @JvmStatic
    fun gcReachabilityLiveNodes(record: GcReachabilityRecord): Long =
        record.liveNodes.toLong()

    @JvmStatic
    fun gcReachabilityLiveBytes(record: GcReachabilityRecord): Long =
        record.liveBytes.toLong()

    @JvmStatic
    fun gcReachabilityLeafNodes(record: GcReachabilityRecord): Long =
        record.leafNodes.toLong()

    @JvmStatic
    fun gcReachabilityInternalNodes(record: GcReachabilityRecord): Long =
        record.internalNodes.toLong()

    @JvmStatic
    fun gcPlanCandidateNodes(record: GcPlanRecord): Long =
        record.candidateNodes.toLong()

    @JvmStatic
    fun gcPlanReclaimableNodes(record: GcPlanRecord): Long =
        record.reclaimableNodes.toLong()

    @JvmStatic
    fun gcPlanReclaimableBytes(record: GcPlanRecord): Long =
        record.reclaimableBytes.toLong()

    @JvmStatic
    fun gcPlanMissingCandidates(record: GcPlanRecord): Long =
        record.missingCandidates.toLong()

    @JvmStatic
    fun gcSweepDeletedNodes(record: GcSweepRecord): Long =
        record.deletedNodes.toLong()

    @JvmStatic
    fun gcSweepDeletedBytes(record: GcSweepRecord): Long =
        record.deletedBytes.toLong()

    @JvmStatic
    fun missingNodePlanRequiredNodes(record: MissingNodePlanRecord): Long =
        record.requiredNodes.toLong()

    @JvmStatic
    fun missingNodePlanRequiredBytes(record: MissingNodePlanRecord): Long =
        record.requiredBytes.toLong()

    @JvmStatic
    fun missingNodePlanMissingNodes(record: MissingNodePlanRecord): Long =
        record.missingNodes.toLong()

    @JvmStatic
    fun missingNodePlanMissingBytes(record: MissingNodePlanRecord): Long =
        record.missingBytes.toLong()

    @JvmStatic
    fun missingNodeCopyCopiedNodes(record: MissingNodeCopyRecord): Long =
        record.copiedNodes.toLong()

    @JvmStatic
    fun missingNodeCopyCopiedBytes(record: MissingNodeCopyRecord): Long =
        record.copiedBytes.toLong()

    @JvmStatic
    fun blobRefRecord(cid: ByteArray, len: Long): BlobRefRecord =
        BlobRefRecord(cid, len.toULong())

    @JvmStatic
    fun blobRefLen(record: BlobRefRecord): Long =
        record.len.toLong()

    @JvmStatic
    fun largeValueConfig(inlineThreshold: Long): LargeValueConfigRecord =
        LargeValueConfigRecord(inlineThreshold.toULong())

    @JvmStatic
    fun largeValueConfigInlineThreshold(record: LargeValueConfigRecord): Long =
        record.inlineThreshold.toLong()

    @JvmStatic
    fun blobStoreBlobCount(store: ProllyBlobStore): Long =
        store.blobCount().toLong()

    @JvmStatic
    fun blobGcReachabilityLiveBlobCount(record: BlobGcReachabilityRecord): Long =
        record.liveBlobCount.toLong()

    @JvmStatic
    fun blobGcReachabilityLiveBlobBytes(record: BlobGcReachabilityRecord): Long =
        record.liveBlobBytes.toLong()

    @JvmStatic
    fun blobGcReachabilityScannedNodes(record: BlobGcReachabilityRecord): Long =
        record.scannedNodes.toLong()

    @JvmStatic
    fun blobGcReachabilityScannedValues(record: BlobGcReachabilityRecord): Long =
        record.scannedValues.toLong()

    @JvmStatic
    fun blobGcPlanCandidateBlobs(record: BlobGcPlanRecord): Long =
        record.candidateBlobs.toLong()

    @JvmStatic
    fun blobGcPlanReclaimableBlobCount(record: BlobGcPlanRecord): Long =
        record.reclaimableBlobCount.toLong()

    @JvmStatic
    fun blobGcPlanReclaimableBlobBytes(record: BlobGcPlanRecord): Long =
        record.reclaimableBlobBytes.toLong()

    @JvmStatic
    fun blobGcPlanMissingCandidates(record: BlobGcPlanRecord): Long =
        record.missingCandidates.toLong()

    @JvmStatic
    fun blobGcSweepDeletedBlobs(record: BlobGcSweepRecord): Long =
        record.deletedBlobs.toLong()

    @JvmStatic
    fun blobGcSweepDeletedBlobBytes(record: BlobGcSweepRecord): Long =
        record.deletedBlobBytes.toLong()

    @JvmStatic
    fun crdtConfigLww(deletePolicy: String): CrdtConfigRecord =
        build.crab.prolly.crdtConfigLww(crdtDeletePolicyKind(deletePolicy))

    @JvmStatic
    fun crdtConfigMultiValue(deletePolicy: String): CrdtConfigRecord =
        build.crab.prolly.crdtConfigMultiValue(crdtDeletePolicyKind(deletePolicy))

    @JvmStatic
    fun timestampedValue(value: ByteArray, timestamp: Long): TimestampedValueRecord =
        TimestampedValueRecord(value, timestamp.toULong())

    @JvmStatic
    fun timestampedValueTimestamp(record: TimestampedValueRecord): Long =
        record.timestamp.toLong()

    @JvmStatic
    fun tombstoneMetadata(key: String, value: ByteArray): TombstoneMetadataRecord =
        TombstoneMetadataRecord(key, value)

    @JvmStatic
    fun tombstone(
        actor: ByteArray,
        timestampMillis: Long,
        causalMetadata: List<TombstoneMetadataRecord>,
    ): TombstoneRecord =
        TombstoneRecord(actor, timestampMillis.toULong(), causalMetadata)

    @JvmStatic
    fun tombstoneTimestampMillis(record: TombstoneRecord): Long =
        record.timestampMillis.toLong()

    @JvmStatic
    fun retentionAll(): NamedRootRetentionRecord =
        build.crab.prolly.retainAllNamedRoots()

    @JvmStatic
    fun retentionExact(names: List<ByteArray>): NamedRootRetentionRecord =
        build.crab.prolly.retainExactNamedRoots(names)

    @JvmStatic
    fun retentionPrefix(prefix: ByteArray): NamedRootRetentionRecord =
        build.crab.prolly.retainNamedRootPrefix(prefix)

    @JvmStatic
    fun retentionNewestByName(count: Long): NamedRootRetentionRecord =
        retentionNewestByName(ByteArray(0), count)

    @JvmStatic
    fun retentionNewestByName(prefix: ByteArray, count: Long): NamedRootRetentionRecord =
        build.crab.prolly.retainNewestNamedRoots(prefix, count.toULong())

    @JvmStatic
    fun retentionUpdatedSince(minUpdatedAtMillis: Long): NamedRootRetentionRecord =
        retentionUpdatedSince(ByteArray(0), minUpdatedAtMillis)

    @JvmStatic
    fun retentionUpdatedSince(prefix: ByteArray, minUpdatedAtMillis: Long): NamedRootRetentionRecord =
        build.crab.prolly.retainNamedRootsUpdatedSince(prefix, minUpdatedAtMillis.toULong())

    @JvmStatic
    fun snapshotBundleFormatVersion(record: SnapshotBundleRecord): Long =
        record.formatVersion.toLong()

    @JvmStatic
    fun snapshotBundleNodeCount(record: SnapshotBundleRecord): Long =
        record.nodes.size.toLong()

    @JvmStatic
    fun snapshotBundleSummaryFormatVersion(record: SnapshotBundleSummaryRecord): Long =
        record.formatVersion.toLong()

    @JvmStatic
    fun snapshotBundleSummaryNodeCount(record: SnapshotBundleSummaryRecord): Long =
        record.nodeCount.toLong()

    @JvmStatic
    fun snapshotBundleSummaryByteCount(record: SnapshotBundleSummaryRecord): Long =
        record.byteCount.toLong()

    @JvmStatic
    fun snapshotBundleVerificationValid(record: SnapshotBundleVerificationRecord): Boolean =
        record.valid

    @JvmStatic
    fun snapshotBundleVerificationMissingCidCount(record: SnapshotBundleVerificationRecord): Long =
        record.missingCids.size.toLong()

    @JvmStatic
    fun snapshotBundleVerificationExtraCidCount(record: SnapshotBundleVerificationRecord): Long =
        record.extraCids.size.toLong()

    private fun encodingKind(kind: String): EncodingKind =
        when (kind) {
            "raw" -> EncodingKind.RAW
            "cbor" -> EncodingKind.CBOR
            "json" -> EncodingKind.JSON
            "custom" -> EncodingKind.CUSTOM
            else -> error("unknown encoding kind $kind")
        }

    private fun crdtDeletePolicyKind(kind: String): CrdtDeletePolicyKind =
        when (kind) {
            "delete_wins" -> CrdtDeletePolicyKind.DELETE_WINS
            "update_wins" -> CrdtDeletePolicyKind.UPDATE_WINS
            else -> error("unknown CRDT delete policy $kind")
        }
}
