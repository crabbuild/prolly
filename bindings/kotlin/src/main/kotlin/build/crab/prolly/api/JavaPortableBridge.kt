package build.crab.prolly.api

import build.crab.prolly.IndexProjectionRecord
import build.crab.prolly.ProximitySearchResultRecord
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
    fun openIndexExact(index: SecondaryIndex, term: ByteArray, limit: Int): PackedIndexPage =
        PackedPages.openIndexExact(index.native.fastHandle(), term.copyOf(), limit.toUInt())
}
