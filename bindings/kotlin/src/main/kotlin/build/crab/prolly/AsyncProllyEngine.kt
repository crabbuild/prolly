package build.crab.prolly

class AsyncProllyEngine private constructor(
    internal val engine: ProllyEngine,
) : AutoCloseable {
    companion object {
        suspend fun memory(config: ConfigRecord = defaultConfig()): AsyncProllyEngine =
            AsyncProllyEngine(ProllyEngine.memory(config))

        suspend fun file(path: String, config: ConfigRecord = defaultConfig()): AsyncProllyEngine =
            AsyncProllyEngine(ProllyEngine.file(path, config))

        suspend fun sqlite(path: String, config: ConfigRecord = defaultConfig()): AsyncProllyEngine =
            AsyncProllyEngine(ProllyEngine.sqlite(path, config))

        suspend fun sqliteInMemory(config: ConfigRecord = defaultConfig()): AsyncProllyEngine =
            AsyncProllyEngine(ProllyEngine.sqliteInMemory(config))

        fun wrap(engine: ProllyEngine): AsyncProllyEngine = AsyncProllyEngine(engine)
    }

    suspend fun create(): TreeRecord = engine.create()

    suspend fun get(tree: TreeRecord, key: ByteArray): ByteArray? = engine.get(tree, key)

    suspend fun getValueRef(tree: TreeRecord, key: ByteArray): ValueRefRecord? = engine.getValueRef(tree, key)

    suspend fun getLargeValue(
        blobStore: AsyncProllyBlobStore,
        tree: TreeRecord,
        key: ByteArray,
    ): ByteArray? = engine.getLargeValue(blobStore.store, tree, key)

    suspend fun getMany(tree: TreeRecord, keys: List<ByteArray>): List<ByteArray?> = engine.getMany(tree, keys)

    suspend fun proveKeys(tree: TreeRecord, keys: List<ByteArray>): MultiKeyProofRecord =
        engine.proveKeys(tree, keys)

    suspend fun put(tree: TreeRecord, key: ByteArray, value: ByteArray): TreeRecord = engine.put(tree, key, value)

    suspend fun putLargeValue(
        blobStore: AsyncProllyBlobStore,
        tree: TreeRecord,
        key: ByteArray,
        value: ByteArray,
        config: LargeValueConfigRecord,
    ): TreeRecord = engine.putLargeValue(blobStore.store, tree, key, value, config)

    suspend fun delete(tree: TreeRecord, key: ByteArray): TreeRecord = engine.delete(tree, key)

    suspend fun batch(tree: TreeRecord, mutations: List<MutationRecord>): TreeRecord = engine.batch(tree, mutations)

    suspend fun batchWithStats(tree: TreeRecord, mutations: List<MutationRecord>): BatchApplyResultRecord =
        engine.batchWithStats(tree, mutations)

    suspend fun buildFromEntries(entries: List<EntryRecord>): TreeRecord = engine.buildFromEntries(entries)

    suspend fun buildFromSortedEntries(entries: List<EntryRecord>): TreeRecord = engine.buildFromSortedEntries(entries)

    suspend fun appendBatch(tree: TreeRecord, mutations: List<MutationRecord>): TreeRecord =
        engine.appendBatch(tree, mutations)

    suspend fun appendBatchWithStats(tree: TreeRecord, mutations: List<MutationRecord>): BatchApplyResultRecord =
        engine.appendBatchWithStats(tree, mutations)

    suspend fun parallelBatch(
        tree: TreeRecord,
        mutations: List<MutationRecord>,
        config: ParallelConfigRecord,
    ): TreeRecord = engine.parallelBatch(tree, mutations, config)

    suspend fun parallelBatchWithStats(
        tree: TreeRecord,
        mutations: List<MutationRecord>,
        config: ParallelConfigRecord,
    ): BatchApplyResultRecord = engine.parallelBatchWithStats(tree, mutations, config)

    suspend fun firstEntry(tree: TreeRecord): EntryRecord? = engine.firstEntry(tree)

    suspend fun lastEntry(tree: TreeRecord): EntryRecord? = engine.lastEntry(tree)

    suspend fun lowerBound(tree: TreeRecord, key: ByteArray): EntryRecord? =
        engine.lowerBound(tree, key)

    suspend fun upperBound(tree: TreeRecord, key: ByteArray): EntryRecord? =
        engine.upperBound(tree, key)

    suspend fun prefix(tree: TreeRecord, prefix: ByteArray): List<EntryRecord> =
        engine.prefix(tree, prefix)

    suspend fun prefixPage(
        tree: TreeRecord,
        prefix: ByteArray,
        cursor: RangeCursorRecord?,
        limit: ULong,
    ): RangePageRecord = engine.prefixPage(tree, prefix, cursor, limit)

    suspend fun prefixReversePage(
        tree: TreeRecord,
        prefix: ByteArray,
        cursor: ReverseCursorRecord?,
        limit: ULong,
    ): ReversePageRecord = engine.prefixReversePage(tree, prefix, cursor, limit)

    suspend fun range(tree: TreeRecord, start: ByteArray, end: ByteArray?): List<EntryRecord> =
        engine.range(tree, start, end)

    suspend fun rangeAfter(tree: TreeRecord, afterKey: ByteArray, end: ByteArray?): List<EntryRecord> =
        engine.rangeAfter(tree, afterKey, end)

    suspend fun rangeFromCursor(tree: TreeRecord, cursor: RangeCursorRecord?, end: ByteArray?): List<EntryRecord> =
        engine.rangeFromCursor(tree, cursor, end)

    suspend fun rangePage(
        tree: TreeRecord,
        cursor: RangeCursorRecord?,
        end: ByteArray?,
        limit: ULong,
    ): RangePageRecord = engine.rangePage(tree, cursor, end, limit)

    suspend fun reversePage(
        tree: TreeRecord,
        cursor: ReverseCursorRecord?,
        start: ByteArray,
        limit: ULong,
    ): ReversePageRecord = engine.reversePage(tree, cursor, start, limit)

    suspend fun cursorWindow(
        tree: TreeRecord,
        key: ByteArray,
        end: ByteArray?,
        limit: ULong,
    ): CursorWindowRecord = engine.cursorWindow(tree, key, end, limit)

    suspend fun diff(base: TreeRecord, other: TreeRecord): List<DiffRecord> = engine.diff(base, other)

    suspend fun rangeDiff(base: TreeRecord, other: TreeRecord, start: ByteArray, end: ByteArray?): List<DiffRecord> =
        engine.rangeDiff(base, other, start, end)

    suspend fun diffFromCursor(
        base: TreeRecord,
        other: TreeRecord,
        cursor: RangeCursorRecord?,
        end: ByteArray?,
    ): List<DiffRecord> = engine.diffFromCursor(base, other, cursor, end)

    suspend fun diffPage(
        base: TreeRecord,
        other: TreeRecord,
        cursor: RangeCursorRecord?,
        end: ByteArray?,
        limit: ULong,
    ): DiffPageRecord = engine.diffPage(base, other, cursor, end, limit)

    suspend fun conflictPage(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        cursor: RangeCursorRecord?,
        limit: ULong,
    ): ConflictPageRecord = engine.conflictPage(base, left, right, cursor, limit)

    suspend fun merge(base: TreeRecord, left: TreeRecord, right: TreeRecord, resolver: String?): TreeRecord =
        engine.merge(base, left, right, resolver)

    suspend fun mergeWithResolver(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        resolver: MergeResolverCallback,
    ): TreeRecord = engine.mergeWithResolver(base, left, right, resolver)

    suspend fun mergeWithPolicy(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        policy: MergePolicyRegistry,
    ): TreeRecord = engine.mergeWithPolicy(base, left, right, policy)

    suspend fun crdtMerge(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        config: CrdtConfigRecord,
    ): TreeRecord = engine.crdtMerge(base, left, right, config)

    suspend fun crdtMergeWithResolver(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        deletePolicy: CrdtDeletePolicyKind,
        resolver: CrdtResolverCallback,
    ): TreeRecord = engine.crdtMergeWithResolver(base, left, right, deletePolicy, resolver)

    suspend fun mergeExplain(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        resolver: String?,
    ): MergeExplanationRecord = engine.mergeExplain(base, left, right, resolver)

    suspend fun mergeExplainWithResolver(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        resolver: MergeResolverCallback,
    ): MergeExplanationRecord = engine.mergeExplainWithResolver(base, left, right, resolver)

    suspend fun mergeExplainWithPolicy(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        policy: MergePolicyRegistry,
    ): MergeExplanationRecord = engine.mergeExplainWithPolicy(base, left, right, policy)

    suspend fun mergeRange(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        start: ByteArray,
        end: ByteArray?,
        resolver: String?,
    ): TreeRecord = engine.mergeRange(base, left, right, start, end, resolver)

    suspend fun mergeRangeWithResolver(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        start: ByteArray,
        end: ByteArray?,
        resolver: MergeResolverCallback,
    ): TreeRecord = engine.mergeRangeWithResolver(base, left, right, start, end, resolver)

    suspend fun mergeRangeWithPolicy(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        start: ByteArray,
        end: ByteArray?,
        policy: MergePolicyRegistry,
    ): TreeRecord = engine.mergeRangeWithPolicy(base, left, right, start, end, policy)

    suspend fun mergePrefix(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        prefix: ByteArray,
        resolver: String?,
    ): TreeRecord = engine.mergePrefix(base, left, right, prefix, resolver)

    suspend fun mergePrefixWithResolver(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        prefix: ByteArray,
        resolver: MergeResolverCallback,
    ): TreeRecord = engine.mergePrefixWithResolver(base, left, right, prefix, resolver)

    suspend fun mergePrefixWithPolicy(
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        prefix: ByteArray,
        policy: MergePolicyRegistry,
    ): TreeRecord = engine.mergePrefixWithPolicy(base, left, right, prefix, policy)

    suspend fun loadNamedRoot(name: ByteArray): TreeRecord? = engine.loadNamedRoot(name)

    suspend fun loadNamedRoots(names: List<ByteArray>): NamedRootSelectionRecord = engine.loadNamedRoots(names)

    suspend fun loadRetainedNamedRoots(retention: NamedRootRetentionRecord): NamedRootSelectionRecord =
        engine.loadRetainedNamedRoots(retention)

    suspend fun listNamedRoots(): List<NamedRootRecord> = engine.listNamedRoots()

    suspend fun listNamedRootManifests(): List<NamedRootManifestRecord> = engine.listNamedRootManifests()

    suspend fun publishNamedRoot(name: ByteArray, tree: TreeRecord) {
        engine.publishNamedRoot(name, tree)
    }

    suspend fun publishNamedRootAtMillis(name: ByteArray, tree: TreeRecord, timestampMillis: ULong) {
        engine.publishNamedRootAtMillis(name, tree, timestampMillis)
    }

    suspend fun deleteNamedRoot(name: ByteArray) {
        engine.deleteNamedRoot(name)
    }

    suspend fun compareAndSwapNamedRoot(
        name: ByteArray,
        expected: TreeRecord?,
        replacement: TreeRecord?,
    ): NamedRootUpdateRecord = engine.compareAndSwapNamedRoot(name, expected, replacement)

    suspend fun compareAndSwapNamedRootAtMillis(
        name: ByteArray,
        expected: TreeRecord?,
        replacement: TreeRecord?,
        timestampMillis: ULong,
    ): NamedRootUpdateRecord = engine.compareAndSwapNamedRootAtMillis(name, expected, replacement, timestampMillis)

    suspend fun collectStatsJson(tree: TreeRecord): JsonDocumentRecord = engine.collectStatsJson(tree)

    suspend fun collectStats(tree: TreeRecord): TreeStatsRecord = engine.collectStats(tree)

    suspend fun statsDiffJson(before: TreeRecord, after: TreeRecord): JsonDocumentRecord =
        engine.statsDiffJson(before, after)

    suspend fun statsDiff(before: TreeRecord, after: TreeRecord): StatsComparisonRecord =
        engine.statsDiff(before, after)

    suspend fun debugTreeJson(tree: TreeRecord): JsonDocumentRecord = engine.debugTreeJson(tree)

    suspend fun debugTree(tree: TreeRecord): TreeDebugViewRecord = engine.debugTree(tree)

    suspend fun debugTreeText(tree: TreeRecord): String = engine.debugTreeText(tree)

    suspend fun debugCompareTreesJson(left: TreeRecord, right: TreeRecord): JsonDocumentRecord =
        engine.debugCompareTreesJson(left, right)

    suspend fun debugCompareTrees(left: TreeRecord, right: TreeRecord): TreeDebugComparisonRecord =
        engine.debugCompareTrees(left, right)

    suspend fun debugCompareTreesText(left: TreeRecord, right: TreeRecord): String =
        engine.debugCompareTreesText(left, right)

    suspend fun cacheStats(): CacheStatsRecord = engine.cacheStats()

    suspend fun clearCache() {
        engine.clearCache()
    }

    suspend fun pinTreeRoot(tree: TreeRecord): ULong = engine.pinTreeRoot(tree)

    suspend fun pinTreePath(tree: TreeRecord, key: ByteArray): ULong = engine.pinTreePath(tree, key)

    suspend fun unpinAllCacheNodes(): ULong = engine.unpinAllCacheNodes()

    suspend fun metrics(): MetricsRecord = engine.metrics()

    suspend fun resetMetrics() {
        engine.resetMetrics()
    }

    suspend fun publishPrefixPathHint(tree: TreeRecord, prefix: ByteArray): Boolean =
        engine.publishPrefixPathHint(tree, prefix)

    suspend fun hydratePrefixPathHint(tree: TreeRecord, prefix: ByteArray): Boolean =
        engine.hydratePrefixPathHint(tree, prefix)

    suspend fun publishChangedSpansHint(
        base: TreeRecord,
        changed: TreeRecord,
        spans: List<ChangedSpanRecord>,
    ): Boolean = engine.publishChangedSpansHint(base, changed, spans)

    suspend fun loadChangedSpansHint(base: TreeRecord, changed: TreeRecord): ChangedSpanHintRecord? =
        engine.loadChangedSpansHint(base, changed)

    suspend fun structuralDiffPage(
        base: TreeRecord,
        other: TreeRecord,
        cursorJson: String?,
        limit: ULong,
    ): StructuralDiffPageRecord = engine.structuralDiffPage(base, other, cursorJson, limit)

    suspend fun structuralDiffPageWithCursor(
        base: TreeRecord,
        other: TreeRecord,
        cursor: StructuralDiffCursorRecord?,
        limit: ULong,
    ): StructuralDiffPageRecord = engine.structuralDiffPageWithCursor(base, other, cursor, limit)

    suspend fun markReachable(roots: List<TreeRecord>): GcReachabilityRecord = engine.markReachable(roots)

    suspend fun markReachableBlobs(roots: List<TreeRecord>): BlobGcReachabilityRecord =
        engine.markReachableBlobs(roots)

    suspend fun listNodeCids(): List<ByteArray> = engine.listNodeCids()

    suspend fun planGc(roots: List<TreeRecord>, candidateCids: List<ByteArray>): GcPlanRecord =
        engine.planGc(roots, candidateCids)

    suspend fun sweepGc(roots: List<TreeRecord>, candidateCids: List<ByteArray>): GcSweepRecord =
        engine.sweepGc(roots, candidateCids)

    suspend fun planStoreGc(roots: List<TreeRecord>): GcPlanRecord = engine.planStoreGc(roots)

    suspend fun sweepStoreGc(roots: List<TreeRecord>): GcSweepRecord = engine.sweepStoreGc(roots)

    suspend fun planStoreGcForRetention(retention: NamedRootRetentionRecord): GcPlanRecord =
        engine.planStoreGcForRetention(retention)

    suspend fun sweepStoreGcForRetention(retention: NamedRootRetentionRecord): GcSweepRecord =
        engine.sweepStoreGcForRetention(retention)

    suspend fun planBlobGc(
        blobStore: AsyncProllyBlobStore,
        roots: List<TreeRecord>,
        candidateBlobs: List<BlobRefRecord>,
    ): BlobGcPlanRecord = engine.planBlobGc(blobStore.store, roots, candidateBlobs)

    suspend fun sweepBlobGc(
        blobStore: AsyncProllyBlobStore,
        roots: List<TreeRecord>,
        candidateBlobs: List<BlobRefRecord>,
    ): BlobGcSweepRecord = engine.sweepBlobGc(blobStore.store, roots, candidateBlobs)

    suspend fun planBlobStoreGc(
        blobStore: AsyncProllyBlobStore,
        roots: List<TreeRecord>,
    ): BlobGcPlanRecord = engine.planBlobStoreGc(blobStore.store, roots)

    suspend fun sweepBlobStoreGc(
        blobStore: AsyncProllyBlobStore,
        roots: List<TreeRecord>,
    ): BlobGcSweepRecord = engine.sweepBlobStoreGc(blobStore.store, roots)

    suspend fun planMissingNodes(tree: TreeRecord, destination: AsyncProllyEngine): MissingNodePlanRecord =
        engine.planMissingNodes(tree, destination.engine)

    suspend fun copyMissingNodes(tree: TreeRecord, destination: AsyncProllyEngine): MissingNodeCopyRecord =
        engine.copyMissingNodes(tree, destination.engine)

    suspend fun exportSnapshot(tree: TreeRecord): SnapshotBundleRecord = engine.exportSnapshot(tree)

    suspend fun importSnapshot(bundle: SnapshotBundleRecord): TreeRecord = engine.importSnapshot(bundle)

    override fun close() {
        engine.close()
    }
}
