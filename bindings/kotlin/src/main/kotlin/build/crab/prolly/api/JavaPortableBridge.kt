package build.crab.prolly.api

import build.crab.prolly.IndexProjectionRecord
import build.crab.prolly.IndexBuildResultRecord
import build.crab.prolly.IndexMatchRecord
import build.crab.prolly.IndexPageRecord
import build.crab.prolly.IndexVerificationRecord
import build.crab.prolly.IndexedMapHealthRecord
import build.crab.prolly.IndexedRetentionRecord
import build.crab.prolly.IndexedSnapshotIdRecord
import build.crab.prolly.IndexedSourceRecord
import build.crab.prolly.IndexedUpdateRecord
import build.crab.prolly.IndexedVersionRecord
import build.crab.prolly.KeyProofRecord
import build.crab.prolly.KeyProofVerificationRecord
import build.crab.prolly.ProximityMembershipProofRecord
import build.crab.prolly.ProximityMembershipVerificationRecord
import build.crab.prolly.ProximityMutationRecord
import build.crab.prolly.ProximityMutationStatsRecord
import build.crab.prolly.ProximityStructuralProofRecord
import build.crab.prolly.ProximityStructuralVerificationRecord
import build.crab.prolly.IndexedMapMetricsRecord
import build.crab.prolly.MapCatalogVerificationRecord
import build.crab.prolly.MapUpdateRecord
import build.crab.prolly.MapVersionRecord
import build.crab.prolly.MutationKind
import build.crab.prolly.MutationRecord
import build.crab.prolly.MultiKeyProofRecord
import build.crab.prolly.MultiKeyProofVerificationRecord
import build.crab.prolly.ProximityVerificationRecord
import build.crab.prolly.TreeStatsRecord
import build.crab.prolly.ProximitySearchResultRecord
import build.crab.prolly.ProximitySearchVerificationRecord
import build.crab.prolly.RangeCursorRecord
import build.crab.prolly.RangePageRecord
import build.crab.prolly.RangePageProofRecord
import build.crab.prolly.RangePageProofVerificationRecord
import build.crab.prolly.RangeProofRecord
import build.crab.prolly.RangeProofVerificationRecord
import build.crab.prolly.ReverseCursorRecord
import build.crab.prolly.ReversePageRecord
import build.crab.prolly.ProvedRangePageRecord
import build.crab.prolly.SecondaryIndexExtractorCallback
import build.crab.prolly.verifyMultiKeyProof as verifyNativeMultiKeyProof
import build.crab.prolly.verifyRangePageProof as verifyNativeRangePageProof
import build.crab.prolly.verifyRangeProof as verifyNativeRangeProof

data class JavaIndexedMutation(
    val kind: String,
    val key: ByteArray,
    val value: ByteArray?,
)

data class JavaMapMutation(
    val kind: String,
    val key: ByteArray,
    val value: ByteArray?,
)

data class JavaMapUpdate(
    val kind: String,
    val previous: ByteArray?,
    val current: MapVersionRecord?,
)

data class JavaVersionPrune(
    val retained: List<ByteArray>,
    val removed: List<ByteArray>,
)

data class JavaGcReachability(
    val liveCids: List<ByteArray>,
    val liveNodes: Long,
    val liveBytes: Long,
    val leafNodes: Long,
    val internalNodes: Long,
)

data class JavaGcPlan(
    val reachability: JavaGcReachability,
    val candidateNodes: Long,
    val reclaimableCids: List<ByteArray>,
    val reclaimableNodes: Long,
    val reclaimableBytes: Long,
    val missingCandidates: Long,
)

data class JavaGcSweep(
    val plan: JavaGcPlan,
    val deletedNodes: Long,
    val deletedBytes: Long,
)

data class JavaIndexedVersion(
    val sourceVersion: ByteArray,
    val catalogVersion: ByteArray?,
    val indexCount: Long,
)

data class JavaIndexBuildResult(
    val sourceVersion: ByteArray,
    val indexVersion: ByteArray,
    val catalogVersion: ByteArray,
    val generation: Long,
    val entries: Long,
    val attempts: Long,
    val activated: Boolean,
)

data class JavaIndexedUpdate(
    val kind: String,
    val previousSourceVersion: ByteArray?,
    val current: JavaIndexedVersion?,
)

data class JavaIndexedSnapshotId(
    val sourceVersion: ByteArray,
    val catalogVersion: ByteArray,
)

data class JavaActiveIndexHealth(
    val name: ByteArray,
    val generation: Long,
    val fingerprint: ByteArray,
    val projection: String,
    val indexMapId: ByteArray,
    val indexVersion: ByteArray,
)

data class JavaIndexedMapHealth(
    val sourceMapId: ByteArray,
    val sourceVersion: ByteArray?,
    val catalogVersion: ByteArray?,
    val activeIndexes: List<JavaActiveIndexHealth>,
    val supportsTransactions: Boolean,
)

data class JavaIndexVerification(
    val name: ByteArray,
    val sourceVersion: ByteArray,
    val expectedIndexVersion: ByteArray,
    val actualIndexVersion: ByteArray,
    val expectedEntries: Long,
    val actualEntries: Long,
    val semanticDifferences: Long,
    val valid: Boolean,
    val canonical: Boolean,
)

data class JavaIndexedMapMetrics(
    val normalizedSourceMutations: Long,
    val recordsExtracted: Long,
    val termsEmitted: Long,
    val projectedBytes: Long,
    val physicalUpserts: Long,
    val physicalDeletes: Long,
    val unchangedEmissionsSkipped: Long,
    val sourceNodesWritten: Long,
    val indexNodesWritten: Long,
    val catalogNodesWritten: Long,
    val retries: Long,
    val buildAttempts: Long,
    val verificationOutcomes: Long,
    val retainedRoots: Long,
)

data class JavaIndexedRetention(
    val retainedSourceVersions: List<ByteArray>,
    val removedSourceVersions: List<ByteArray>,
    val retainedIndexVersions: List<ByteArray>,
    val removedIndexVersions: List<ByteArray>,
    val removedCatalogVersions: List<ByteArray>,
    val removedCheckpointRecords: Long,
    val removedNamedRoots: List<ByteArray>,
)

data class JavaIndexMatch(
    val term: ByteArray,
    val primaryKey: ByteArray,
    val projection: ByteArray?,
)

data class JavaIndexPage(
    val matches: List<JavaIndexMatch>,
    val nextCursor: ByteArray?,
)

data class JavaIndexedSource(
    val term: ByteArray,
    val primaryKey: ByteArray,
    val projection: ByteArray?,
    val sourceValue: ByteArray,
)

private fun IndexedVersionRecord.toJava() = JavaIndexedVersion(
    sourceVersion, catalogVersion, indexCount.toLong(),
)

private fun MapUpdateRecord.toJava() = JavaMapUpdate(
    kind.name.lowercase(), previous, current,
)

private fun build.crab.prolly.GcReachabilityRecord.toJava() = JavaGcReachability(
    liveCids, liveNodes.toLong(), liveBytes.toLong(), leafNodes.toLong(), internalNodes.toLong(),
)

private fun build.crab.prolly.GcPlanRecord.toJava() = JavaGcPlan(
    reachability.toJava(), candidateNodes.toLong(), reclaimableCids, reclaimableNodes.toLong(),
    reclaimableBytes.toLong(), missingCandidates.toLong(),
)

private fun build.crab.prolly.GcSweepRecord.toJava() = JavaGcSweep(
    plan.toJava(), deletedNodes.toLong(), deletedBytes.toLong(),
)

private fun IndexBuildResultRecord.toJava() = JavaIndexBuildResult(
    sourceVersion, indexVersion, catalogVersion, generation.toLong(), entries.toLong(),
    attempts.toLong(), activated,
)

private fun IndexedUpdateRecord.toJava() = JavaIndexedUpdate(
    kind.name.lowercase(), previousSourceVersion, current?.toJava(),
)

private fun IndexedSnapshotIdRecord.toJava() = JavaIndexedSnapshotId(sourceVersion, catalogVersion)

private fun IndexedMapHealthRecord.toJava() = JavaIndexedMapHealth(
    sourceMapId, sourceVersion, catalogVersion,
    activeIndexes.map {
        JavaActiveIndexHealth(
            it.name, it.generation.toLong(), it.fingerprint, it.projection.name.lowercase(),
            it.indexMapId, it.indexVersion,
        )
    },
    supportsTransactions,
)

private fun IndexVerificationRecord.toJava() = JavaIndexVerification(
    name, sourceVersion, expectedIndexVersion, actualIndexVersion, expectedEntries.toLong(),
    actualEntries.toLong(), semanticDifferences.toLong(), valid, canonical,
)

private fun IndexedMapMetricsRecord.toJava() = JavaIndexedMapMetrics(
    normalizedSourceMutations.toLong(), recordsExtracted.toLong(), termsEmitted.toLong(),
    projectedBytes.toLong(), physicalUpserts.toLong(), physicalDeletes.toLong(),
    unchangedEmissionsSkipped.toLong(), sourceNodesWritten.toLong(), indexNodesWritten.toLong(),
    catalogNodesWritten.toLong(), retries.toLong(), buildAttempts.toLong(),
    verificationOutcomes.toLong(), retainedRoots.toLong(),
)

private fun IndexedRetentionRecord.toJava() = JavaIndexedRetention(
    retainedSourceVersions, removedSourceVersions, retainedIndexVersions, removedIndexVersions,
    removedCatalogVersions, removedCheckpointRecords.toLong(), removedNamedRoots,
)

private fun IndexMatchRecord.toJava() = JavaIndexMatch(term, primaryKey, projection)

private fun IndexPageRecord.toJava() = JavaIndexPage(matches.map(IndexMatchRecord::toJava), nextCursor)

private fun IndexedSourceRecord.toJava() = JavaIndexedSource(
    term, primaryKey, projection, sourceValue,
)

data class JavaProximityMutationResult(
    val map: ProximityMap,
    val stats: JavaProximityMutationStats,
)

data class JavaProximityMutationStats(
    val directoryEntriesScanned: Long,
    val directoryNodesRead: Long,
    val directoryNodesRebuilt: Long,
    val directoryNodesWritten: Long,
    val directoryNodesReused: Long,
    val directoryLevelsRebuilt: Long,
    val directoryRightEdgeRebuilt: Boolean,
    val nodesRead: Long,
    val nodesWritten: Long,
    val nodesReused: Long,
    val recordsRebuilt: Long,
    val distanceEvaluations: Long,
    val fullProximityRebuild: Boolean,
)

private fun ProximityMutationStatsRecord.toJava() = JavaProximityMutationStats(
    directoryEntriesScanned.toLong(), directoryNodesRead.toLong(),
    directoryNodesRebuilt.toLong(), directoryNodesWritten.toLong(), directoryNodesReused.toLong(),
    directoryLevelsRebuilt.toLong(), directoryRightEdgeRebuilt, nodesRead.toLong(),
    nodesWritten.toLong(), nodesReused.toLong(), recordsRebuilt.toLong(),
    distanceEvaluations.toLong(), fullProximityRebuild,
)

data class JavaProximityConfig(
    val dimensions: Int,
    val metric: String,
    val logChunkSize: Int,
    val levelHashSeed: Long,
    val minPageBytes: Int,
    val targetPageBytes: Int,
    val maxPageBytes: Int,
    val overflowHashSeed: Long,
    val inlineThresholdBytes: Int,
    val scalarQuantizationGroupSize: Int?,
)

data class JavaProximityVerification(
    val recordCount: Long,
    val proximityNodeCount: Long,
    val externalVectorCount: Long,
    val quantizedNodeCount: Long,
    val scalarQuantizerCount: Long,
    val overflowPageCount: Long,
    val overflowDirectoryCount: Long,
    val maximumLevel: Int,
    val maximumNodeBytes: Long,
    val distanceChecks: Long,
)

data class JavaProximityStructuralVerification(
    val descriptor: ByteArray,
    val objectCount: Long,
    val summary: JavaProximityVerification,
)

private fun ProximityVerificationRecord.toJava() = JavaProximityVerification(
    recordCount.toLong(), proximityNodeCount.toLong(), externalVectorCount.toLong(),
    quantizedNodeCount.toLong(), scalarQuantizerCount.toLong(), overflowPageCount.toLong(),
    overflowDirectoryCount.toLong(), maximumLevel.toInt(), maximumNodeBytes.toLong(),
    distanceChecks.toLong(),
)

object JavaPortableBridge {
    @JvmStatic
    fun mapVersionCreatedAtMillis(version: MapVersionRecord): Long? =
        version.createdAtMillis?.toLong()

    @JvmStatic
    fun applyVersionedAtMillis(
        map: VersionedMap,
        mutations: List<JavaMapMutation>,
        timestampMillis: Long,
    ): MapVersionRecord = map.applyAtMillis(mutations.map { it.toNative() }, timestampMillis.toULong())

    @JvmStatic
    fun applyVersionedIfAtMillis(
        map: VersionedMap,
        expected: ByteArray?,
        mutations: List<JavaMapMutation>,
        timestampMillis: Long,
    ): JavaMapUpdate = map.applyIfAtMillis(
        expected, mutations.map { it.toNative() }, timestampMillis.toULong(),
    ).toJava()

    @JvmStatic
    fun pruneVersions(map: VersionedMap, keepLatest: Long): JavaVersionPrune =
        map.pruneVersions(keepLatest.toULong()).let { JavaVersionPrune(it.retained, it.removed) }

    @JvmStatic
    fun keepForAt(map: VersionedMap, nowMillis: Long, maxAgeMillis: Long): JavaVersionPrune =
        map.keepForAt(nowMillis.toULong(), maxAgeMillis.toULong()).let {
            JavaVersionPrune(it.retained, it.removed)
        }

    @JvmStatic
    fun keepFor(map: VersionedMap, maxAgeMillis: Long): JavaVersionPrune =
        map.keepFor(maxAgeMillis.toULong()).let { JavaVersionPrune(it.retained, it.removed) }

    @JvmStatic
    fun keepVersions(map: VersionedMap, ids: List<ByteArray>): JavaVersionPrune =
        map.keepVersions(ids).let { JavaVersionPrune(it.retained, it.removed) }

    @JvmStatic
    fun versionedPlanGc(map: VersionedMap): JavaGcPlan = map.planGc().toJava()

    @JvmStatic
    fun versionedSweepGc(map: VersionedMap): JavaGcSweep = map.sweepGc().toJava()

    @JvmStatic
    fun versionedRangePage(
        map: VersionedMap,
        cursor: RangeCursorRecord?,
        end: ByteArray?,
        limit: Long,
    ): RangePageRecord = map.rangePage(cursor, end, limit.toULong())

    @JvmStatic
    fun versionedPrefixPage(
        map: VersionedMap,
        prefix: ByteArray,
        cursor: RangeCursorRecord?,
        limit: Long,
    ): RangePageRecord = map.prefixPage(prefix, cursor, limit.toULong())

    @JvmStatic
    fun versionedRangePageAt(
        map: VersionedMap,
        id: ByteArray,
        cursor: RangeCursorRecord?,
        end: ByteArray?,
        limit: Long,
    ): RangePageRecord = map.rangePageAt(id, cursor, end, limit.toULong())

    @JvmStatic
    fun versionedPrefixPageAt(
        map: VersionedMap,
        id: ByteArray,
        prefix: ByteArray,
        cursor: RangeCursorRecord?,
        limit: Long,
    ): RangePageRecord = map.prefixPageAt(id, prefix, cursor, limit.toULong())

    @JvmStatic
    fun mapSnapshotProveRangePage(
        snapshot: MapSnapshot,
        cursor: RangeCursorRecord?,
        end: ByteArray?,
        limit: Long,
    ): ProvedRangePageRecord {
        require(limit >= 0) { "page limit must be non-negative" }
        return snapshot.proveRangePage(cursor, end, limit.toULong())
    }

    @JvmStatic
    fun mapSnapshotRangePage(
        snapshot: MapSnapshot,
        cursor: RangeCursorRecord?,
        end: ByteArray?,
        limit: Long,
    ): RangePageRecord {
        require(limit >= 0) { "page limit must be non-negative" }
        return snapshot.rangePage(cursor, end, limit.toULong())
    }

    @JvmStatic
    fun mapSnapshotPrefixPage(
        snapshot: MapSnapshot,
        prefix: ByteArray,
        cursor: RangeCursorRecord?,
        limit: Long,
    ): RangePageRecord {
        require(limit >= 0) { "page limit must be non-negative" }
        return snapshot.prefixPage(prefix, cursor, limit.toULong())
    }

    @JvmStatic
    fun mapSnapshotReversePage(
        snapshot: MapSnapshot,
        cursor: ReverseCursorRecord?,
        start: ByteArray,
        limit: Long,
    ): ReversePageRecord {
        require(limit >= 0) { "page limit must be non-negative" }
        return snapshot.reversePage(cursor, start, limit.toULong())
    }

    @JvmStatic
    fun mapSnapshotPrefixReversePage(
        snapshot: MapSnapshot,
        prefix: ByteArray,
        cursor: ReverseCursorRecord?,
        limit: Long,
    ): ReversePageRecord {
        require(limit >= 0) { "page limit must be non-negative" }
        return snapshot.prefixReversePage(prefix, cursor, limit.toULong())
    }

    @JvmStatic
    fun memory(): Engine = Engine.memory()

    private fun JavaMapMutation.toNative(): MutationRecord {
        val mutationKind = when (kind) {
            "upsert" -> MutationKind.UPSERT
            "delete" -> MutationKind.DELETE
            else -> throw IllegalArgumentException("unknown map mutation kind: $kind")
        }
        return MutationRecord(mutationKind, key.copyOf(), value?.copyOf())
    }

    @JvmStatic
    fun applyVersioned(map: VersionedMap, mutations: List<JavaMapMutation>): MapVersionRecord =
        map.apply(mutations.map { it.toNative() })

    @JvmStatic
    fun applyVersionedIf(
        map: VersionedMap,
        expected: ByteArray?,
        mutations: List<JavaMapMutation>,
    ): JavaMapUpdate = map.applyIf(expected?.copyOf(), mutations.map { it.toNative() }).toJava()

    @JvmStatic
    fun putVersionedIf(
        map: VersionedMap,
        expected: ByteArray?,
        key: ByteArray,
        value: ByteArray,
    ): JavaMapUpdate = map.putIf(expected?.copyOf(), key.copyOf(), value.copyOf()).toJava()

    @JvmStatic
    fun deleteVersionedIf(
        map: VersionedMap,
        expected: ByteArray?,
        key: ByteArray,
    ): JavaMapUpdate = map.deleteIf(expected?.copyOf(), key.copyOf()).toJava()

    @JvmStatic
    fun register(
        registry: IndexRegistry,
        name: ByteArray,
        generation: Long,
        extractorId: String,
        projection: IndexProjectionRecord,
        extractor: SecondaryIndexExtractorCallback,
    ) = registry.register(name, generation.toULong(), extractorId, projection, extractor)

    @JvmStatic
    fun buildProximity(
        engine: Engine,
        dimensions: Int,
        records: List<ProximityRecord>,
    ): ProximityMap = engine.buildProximity(dimensions.toUInt(), records)

    @JvmStatic
    fun searchExact(
        map: ProximityMap,
        query: List<Float>,
        k: Long,
    ): ProximitySearchResultRecord = map.searchExact(query, k.toULong())

    @JvmStatic
    fun searchExact(
        session: ProximityReadSession,
        query: List<Float>,
        k: Long,
    ): ProximitySearchResultRecord = session.searchExact(query, k.toULong())

    @JvmStatic
    fun proveSearch(
        map: ProximityMap,
        query: List<Float>,
        k: Long,
    ): ProximitySearchProof = map.proveSearchExact(query, k.toULong())

    @JvmStatic
    fun verify(
        proof: ProximitySearchProof,
        expectedDescriptor: ByteArray?,
    ): ProximitySearchVerificationRecord = proof.verify(expectedDescriptor)

    @JvmStatic
    fun openIndexExact(index: SecondaryIndex, term: ByteArray, limit: Int): PackedIndexPage =
        PackedPages.openIndexExact(index.native.fastHandle(), term.copyOf(), limit.toUInt())

    private fun JavaIndexedMutation.toNative(): MutationRecord {
        val mutationKind = when (kind) {
            "upsert" -> MutationKind.UPSERT
            "delete" -> MutationKind.DELETE
            else -> throw IllegalArgumentException("unknown indexed mutation kind: $kind")
        }
        return MutationRecord(mutationKind, key.copyOf(), value?.copyOf())
    }

    @JvmStatic
    fun indexedId(map: IndexedMap): ByteArray = map.id.copyOf()

    @JvmStatic
    fun putIndexed(map: IndexedMap, key: ByteArray, value: ByteArray): JavaIndexedVersion =
        map.put(key.copyOf(), value.copyOf()).toJava()

    @JvmStatic
    fun deleteIndexed(map: IndexedMap, key: ByteArray): JavaIndexedVersion =
        map.delete(key.copyOf()).toJava()

    @JvmStatic
    fun ensureIndex(map: IndexedMap, name: ByteArray): JavaIndexBuildResult =
        map.ensureIndex(name.copyOf()).toJava()

    @JvmStatic
    fun applyIndexed(map: IndexedMap, mutations: List<JavaIndexedMutation>): JavaIndexedVersion =
        map.apply(mutations.map { it.toNative() }).toJava()

    @JvmStatic
    fun applyIndexedIf(
        map: IndexedMap,
        expectedSource: ByteArray?,
        mutations: List<JavaIndexedMutation>,
    ): JavaIndexedUpdate = map.applyIf(
        expectedSource?.copyOf(), mutations.map { it.toNative() },
    ).toJava()

    @JvmStatic
    fun snapshotId(snapshot: IndexedSnapshot): JavaIndexedSnapshotId = snapshot.id.toJava()

    @JvmStatic
    fun snapshotAt(map: IndexedMap, sourceVersion: ByteArray): IndexedSnapshot =
        map.snapshotAt(sourceVersion.copyOf())

    @JvmStatic
    fun snapshotById(map: IndexedMap, id: JavaIndexedSnapshotId): IndexedSnapshot =
        map.snapshotById(IndexedSnapshotIdRecord(id.sourceVersion.copyOf(), id.catalogVersion.copyOf()))

    @JvmStatic
    fun indexedHealth(map: IndexedMap): JavaIndexedMapHealth = map.health().toJava()

    @JvmStatic
    fun indexedMetrics(map: IndexedMap): JavaIndexedMapMetrics = map.metrics().toJava()

    @JvmStatic
    fun verifyIndex(map: IndexedMap, name: ByteArray, sourceVersion: ByteArray): JavaIndexVerification =
        map.verifyIndex(name.copyOf(), sourceVersion.copyOf()).toJava()

    @JvmStatic
    fun verifyAll(map: IndexedMap, sourceVersion: ByteArray): List<JavaIndexVerification> =
        map.verifyAll(sourceVersion.copyOf()).map(IndexVerificationRecord::toJava)

    @JvmStatic
    fun repairIndex(map: IndexedMap, name: ByteArray, sourceVersion: ByteArray): JavaIndexVerification =
        map.repairIndex(name.copyOf(), sourceVersion.copyOf()).toJava()

    @JvmStatic
    fun deactivateIndex(map: IndexedMap, name: ByteArray): JavaIndexedVersion =
        map.deactivateIndex(name.copyOf()).toJava()

    @JvmStatic
    fun importCurrent(map: IndexedMap, bundle: ByteArray, expectedSource: ByteArray?): JavaIndexedVersion =
        map.importCurrent(bundle.copyOf(), expectedSource?.copyOf()).toJava()

    @JvmStatic
    fun indexName(index: SecondaryIndex): ByteArray = index.name.copyOf()

    @JvmStatic
    fun indexExact(index: SecondaryIndex, term: ByteArray): List<JavaIndexMatch> =
        index.exact(term.copyOf()).map(IndexMatchRecord::toJava)

    @JvmStatic
    fun indexPrefix(index: SecondaryIndex, prefix: ByteArray): List<JavaIndexMatch> =
        index.prefix(prefix.copyOf()).map(IndexMatchRecord::toJava)

    @JvmStatic
    fun indexRange(index: SecondaryIndex, start: ByteArray, end: ByteArray?): List<JavaIndexMatch> =
        index.range(start.copyOf(), end?.copyOf()).map(IndexMatchRecord::toJava)

    @JvmStatic
    fun indexRecords(index: SecondaryIndex, term: ByteArray): List<JavaIndexedSource> =
        index.records(term.copyOf()).map(IndexedSourceRecord::toJava)

    @JvmStatic
    fun indexExactPage(index: SecondaryIndex, term: ByteArray, cursor: ByteArray?, limit: Long): JavaIndexPage =
        index.exactPage(term.copyOf(), cursor?.copyOf(), limit.toULong()).toJava()

    @JvmStatic
    fun indexExactReversePage(index: SecondaryIndex, term: ByteArray, cursor: ByteArray?, limit: Long): JavaIndexPage =
        index.exactReversePage(term.copyOf(), cursor?.copyOf(), limit.toULong()).toJava()

    @JvmStatic
    fun indexPrefixPage(index: SecondaryIndex, prefix: ByteArray, cursor: ByteArray?, limit: Long): JavaIndexPage =
        index.prefixPage(prefix.copyOf(), cursor?.copyOf(), limit.toULong()).toJava()

    @JvmStatic
    fun indexPrefixReversePage(index: SecondaryIndex, prefix: ByteArray, cursor: ByteArray?, limit: Long): JavaIndexPage =
        index.prefixReversePage(prefix.copyOf(), cursor?.copyOf(), limit.toULong()).toJava()

    @JvmStatic
    fun indexRangePage(
        index: SecondaryIndex,
        start: ByteArray,
        end: ByteArray?,
        cursor: ByteArray?,
        limit: Long,
    ): JavaIndexPage = index.rangePage(
        start.copyOf(), end?.copyOf(), cursor?.copyOf(), limit.toULong(),
    ).toJava()

    @JvmStatic
    fun indexRangeReversePage(
        index: SecondaryIndex,
        start: ByteArray,
        end: ByteArray?,
        cursor: ByteArray?,
        limit: Long,
    ): JavaIndexPage = index.rangeReversePage(
        start.copyOf(), end?.copyOf(), cursor?.copyOf(), limit.toULong(),
    ).toJava()

    @JvmStatic
    fun keepLast(map: VersionedMap, count: Long): JavaVersionPrune =
        map.keepLast(count.toULong()).let { JavaVersionPrune(it.retained, it.removed) }

    @JvmStatic
    fun keepLast(map: IndexedMap, count: Long): JavaIndexedRetention =
        map.keepLast(count.toULong()).toJava()

    @JvmStatic
    fun proveStructure(map: ProximityMap) = map.proveStructure()

    @JvmStatic
    fun count(map: ProximityMap) = map.count.toLong()

    @JvmStatic
    fun config(map: ProximityMap): JavaProximityConfig {
        val value = map.config
        return JavaProximityConfig(
            value.dimensions.toInt(), value.metric.name.lowercase(), value.logChunkSize.toInt(),
            value.levelHashSeed.toLong(), value.minPageBytes.toInt(), value.targetPageBytes.toInt(),
            value.maxPageBytes.toInt(), value.overflowHashSeed.toLong(),
            value.inlineThresholdBytes.toInt(), value.scalarQuantizationGroupSize?.toInt(),
        )
    }

    @JvmStatic
    fun verify(map: ProximityMap) = map.verify().toJava()

    @JvmStatic
    fun mutate(map: ProximityMap, mutations: List<ProximityMutationRecord>): JavaProximityMutationResult {
        val (updated, stats) = map.mutate(mutations)
        return JavaProximityMutationResult(updated, stats.toJava())
    }

    @JvmStatic
    fun rebuild(map: ProximityMap, mutations: List<ProximityMutationRecord>) = map.rebuild(mutations)

    @JvmStatic
    fun verify(proof: KeyProofRecord): KeyProofVerificationRecord = verifyKeyProof(proof)

    @JvmStatic
    fun verifyMultiKey(proof: MultiKeyProofRecord): MultiKeyProofVerificationRecord =
        verifyNativeMultiKeyProof(proof)

    @JvmStatic
    fun verifyRange(proof: RangeProofRecord): RangeProofVerificationRecord =
        verifyNativeRangeProof(proof)

    @JvmStatic
    fun verifyRangePage(proof: RangePageProofRecord): RangePageProofVerificationRecord =
        verifyNativeRangePageProof(proof)

    @JvmStatic
    fun verify(
        proof: ProximityMembershipProofRecord,
        expectedDescriptor: ByteArray?,
    ): ProximityMembershipVerificationRecord =
        verifyProximityMembershipProof(proof, expectedDescriptor)

    @JvmStatic
    fun verifyStructural(
        proof: ProximityStructuralProofRecord,
        expectedDescriptor: ByteArray?,
    ): JavaProximityStructuralVerification {
        val value = verifyProximityStructureProof(proof, expectedDescriptor)
        return JavaProximityStructuralVerification(
            value.descriptor, value.objectCount.toLong(), value.summary.toJava(),
        )
    }

    @JvmStatic fun totalKeyValuePairs(stats: TreeStatsRecord) = stats.totalKeyValuePairs.toLong()
    @JvmStatic fun versionCount(verification: MapCatalogVerificationRecord) = verification.versionCount.toLong()
    @JvmStatic fun buildAttempts(metrics: IndexedMapMetricsRecord) = metrics.buildAttempts.toLong()
    @JvmStatic fun recordCount(verification: ProximityVerificationRecord) = verification.recordCount.toLong()
    @JvmStatic fun replayedEvents(verification: ProximitySearchVerificationRecord) = verification.replayedEvents.toLong()
}
