package build.crab.prolly.api

import build.crab.prolly.IndexProjectionRecord
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
import build.crab.prolly.ProximityVerificationRecord
import build.crab.prolly.TreeStatsRecord
import build.crab.prolly.ProximitySearchResultRecord
import build.crab.prolly.ProximitySearchVerificationRecord
import build.crab.prolly.SecondaryIndexExtractorCallback

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
    fun memory(): Engine = Engine.memory()

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

    @JvmStatic
    fun keepLast(map: VersionedMap, count: Long) = map.keepLast(count.toULong())

    @JvmStatic
    fun keepLast(map: IndexedMap, count: Long) = map.keepLast(count.toULong())

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
