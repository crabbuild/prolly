package build.crab.prolly.api

import build.crab.prolly.IndexProjectionRecord
import build.crab.prolly.KeyProofRecord
import build.crab.prolly.KeyProofVerificationRecord
import build.crab.prolly.ProximityMembershipProofRecord
import build.crab.prolly.ProximityMembershipVerificationRecord
import build.crab.prolly.IndexedMapMetricsRecord
import build.crab.prolly.MapCatalogVerificationRecord
import build.crab.prolly.ProximityVerificationRecord
import build.crab.prolly.TreeStatsRecord
import build.crab.prolly.ProximitySearchResultRecord
import build.crab.prolly.ProximitySearchVerificationRecord
import build.crab.prolly.SecondaryIndexExtractorCallback

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
    fun verify(proof: KeyProofRecord): KeyProofVerificationRecord = verifyKeyProof(proof)

    @JvmStatic
    fun verify(
        proof: ProximityMembershipProofRecord,
        expectedDescriptor: ByteArray?,
    ): ProximityMembershipVerificationRecord =
        verifyProximityMembershipProof(proof, expectedDescriptor)

    @JvmStatic fun totalKeyValuePairs(stats: TreeStatsRecord) = stats.totalKeyValuePairs.toLong()
    @JvmStatic fun versionCount(verification: MapCatalogVerificationRecord) = verification.versionCount.toLong()
    @JvmStatic fun buildAttempts(metrics: IndexedMapMetricsRecord) = metrics.buildAttempts.toLong()
    @JvmStatic fun recordCount(verification: ProximityVerificationRecord) = verification.recordCount.toLong()
    @JvmStatic fun replayedEvents(verification: ProximitySearchVerificationRecord) = verification.replayedEvents.toLong()
}
