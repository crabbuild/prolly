package build.crab.prolly.api

import build.crab.prolly.BindingIndexedMap
import build.crab.prolly.BindingIndexedSnapshot
import build.crab.prolly.BindingIndexRegistry
import build.crab.prolly.BindingMapComparison
import build.crab.prolly.BindingMapMerge
import build.crab.prolly.BindingMapSubscription
import build.crab.prolly.BindingProximityMap
import build.crab.prolly.BindingProximityReadSession
import build.crab.prolly.BindingProximitySearchProof
import build.crab.prolly.BindingSecondaryIndexSnapshot
import build.crab.prolly.BindingVersionedMap
import build.crab.prolly.BindingVersionedTransaction
import build.crab.prolly.BindingMapSnapshot
import build.crab.prolly.ConfigRecord
import build.crab.prolly.ContentGraphLimitsRecord
import build.crab.prolly.IndexProjectionRecord
import build.crab.prolly.IndexedSnapshotIdRecord
import build.crab.prolly.KeyProofRecord
import build.crab.prolly.MutationRecord
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyReadSession
import build.crab.prolly.ProximityMembershipProofRecord
import build.crab.prolly.ProximityMutationRecord
import build.crab.prolly.ProximityMutationStatsRecord
import build.crab.prolly.ProximityRecordRecord
import build.crab.prolly.ProximitySearchResultRecord
import build.crab.prolly.ProximitySearchRequestRecord
import build.crab.prolly.RangeCursorRecord
import build.crab.prolly.ReverseCursorRecord
import build.crab.prolly.ProximityConfigRecord
import build.crab.prolly.ProximityStructuralProofRecord
import build.crab.prolly.SecondaryIndexExtractorCallback
import build.crab.prolly.SnapshotBundleRecord
import build.crab.prolly.defaultConfig
import build.crab.prolly.defaultContentGraphLimits
import build.crab.prolly.defaultProximityConfig
import build.crab.prolly.exactProximitySearchRequest
import build.crab.prolly.verifyKeyProof as verifyNativeKeyProof
import build.crab.prolly.verifyMultiKeyProof as verifyNativeMultiKeyProof
import build.crab.prolly.verifyRangePageProof as verifyNativeRangePageProof
import build.crab.prolly.verifyRangeProof as verifyNativeRangeProof
import build.crab.prolly.verifyProximityMembershipProof as verifyNativeProximityMembershipProof
import build.crab.prolly.verifyProximityStructureProof as verifyNativeProximityStructureProof
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

data class ProximityRecord(val key: ByteArray, val vector: List<Float>, val value: ByteArray)

class Engine private constructor(internal val native: ProllyEngine) : AutoCloseable {
    companion object {
        fun memory(config: ConfigRecord = defaultConfig()) = Engine(ProllyEngine.memory(config))
    }

    fun versionedMap(id: ByteArray) = VersionedMap(native.versionedMap(id.copyOf()))
    fun beginVersionedTransaction() = VersionedTransaction(native.beginVersionedTransaction())

    fun indexRegistry() = IndexRegistry(BindingIndexRegistry())

    fun indexedMap(id: ByteArray, registry: IndexRegistry) =
        IndexedMap(native.indexedMap(id.copyOf(), registry.native))

    fun buildProximity(
        dimensions: UInt,
        records: List<ProximityRecord>,
        config: ProximityConfigRecord = defaultProximityConfig(dimensions),
        threads: ULong? = null,
    ) = ProximityMap(
        native.buildProximityMap(
            config,
            records.map {
                ProximityRecordRecord(it.key.copyOf(), it.vector.toList(), it.value.copyOf())
            },
            threads,
        ),
    )

    fun loadProximity(descriptor: ByteArray) =
        ProximityMap(native.loadProximityMap(descriptor.copyOf()))

    override fun close() = native.close()
}

class VersionedMap(internal val native: BindingVersionedMap) : AutoCloseable {
    val id: ByteArray get() = native.id()
    fun isInitialized() = native.isInitialized()
    fun initialize() = native.initialize()
    fun head() = native.head()
    fun headId() = native.headId()
    fun version(id: ByteArray) = native.version(id.copyOf())
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun containsKey(key: ByteArray) = native.containsKey(key.copyOf())
    fun getMany(keys: List<ByteArray>) = native.getMany(keys.map(ByteArray::copyOf))
    fun getAt(id: ByteArray, key: ByteArray) = native.getAt(id.copyOf(), key.copyOf())
    fun getManyAt(id: ByteArray, keys: List<ByteArray>) =
        native.getManyAt(id.copyOf(), keys.map(ByteArray::copyOf))
    fun put(key: ByteArray, value: ByteArray) = native.put(key.copyOf(), value.copyOf())
    fun apply(mutations: List<MutationRecord>) = native.apply(mutations.map(::ownedMutation))
    fun applyIf(expected: ByteArray?, mutations: List<MutationRecord>) =
        native.applyIf(expected?.copyOf(), mutations.map(::ownedMutation))
    fun putIf(expected: ByteArray?, key: ByteArray, value: ByteArray) =
        native.putIf(expected?.copyOf(), key.copyOf(), value.copyOf())
    fun deleteIf(expected: ByteArray?, key: ByteArray) =
        native.deleteIf(expected?.copyOf(), key.copyOf())
    fun delete(key: ByteArray) = native.delete(key.copyOf())
    fun snapshot() = native.snapshot()?.let(::MapSnapshot)
    fun snapshotAt(id: ByteArray) = native.snapshotAt(id.copyOf())?.let(::MapSnapshot)
    fun compare(base: ByteArray, target: ByteArray) =
        MapComparison(native.compare(base.copyOf(), target.copyOf()))
    fun compareToHead(base: ByteArray) = MapComparison(native.compareToHead(base.copyOf()))
    fun subscribe() = MapSubscription(native.subscribe())
    fun subscribeFrom(lastSeen: ByteArray? = null) =
        MapSubscription(native.subscribeFrom(lastSeen?.copyOf()))
    fun prepareMerge(base: ByteArray, candidate: ByteArray) =
        MapMerge(native.prepareMerge(base.copyOf(), candidate.copyOf()))
    fun versions() = native.versions()
    fun backup() = native.backup()
    fun restoreBackup(bundle: ByteArray) = native.restoreBackup(bundle.copyOf())
    fun importAsHead(bundle: SnapshotBundleRecord) = native.importAsHead(bundle)
    fun keepLast(count: ULong) = native.keepLast(count)
    fun verifyCatalog() = native.verifyCatalog()
    fun planGc() = native.planGc()
    fun sweepGc() = native.sweepGc()
    suspend fun putAsync(key: ByteArray, value: ByteArray) =
        key.copyOf().let { copiedKey ->
            value.copyOf().let { copiedValue ->
                withContext(Dispatchers.IO) { native.put(copiedKey, copiedValue) }
            }
        }
    override fun close() = native.close()
}

class VersionedTransaction(internal var native: BindingVersionedTransaction?) : AutoCloseable {
    private fun open() = native ?: error("versioned transaction is completed")
    fun head(mapId: ByteArray) = open().head(mapId.copyOf())
    fun get(mapId: ByteArray, key: ByteArray) = open().get(mapId.copyOf(), key.copyOf())
    fun apply(mapId: ByteArray, mutations: List<MutationRecord>) =
        open().apply(mapId.copyOf(), mutations.map(::ownedMutation))
    fun applyIf(mapId: ByteArray, expected: ByteArray?, mutations: List<MutationRecord>) =
        open().applyIf(mapId.copyOf(), expected?.copyOf(), mutations.map(::ownedMutation))
    fun put(mapId: ByteArray, key: ByteArray, value: ByteArray) =
        open().put(mapId.copyOf(), key.copyOf(), value.copyOf())
    fun delete(mapId: ByteArray, key: ByteArray) = open().delete(mapId.copyOf(), key.copyOf())
    fun commit() = open().commit().also { close() }
    fun rollback() = open().rollback().also { close() }
    override fun close() { native?.close(); native = null }
}

class MapComparison(internal val native: BindingMapComparison) : AutoCloseable {
    fun base() = native.base()
    fun target() = native.target()
    fun diff() = native.diff()
    fun diffPage(cursor: RangeCursorRecord? = null, end: ByteArray? = null, limit: ULong = 256uL) =
        native.diffPage(cursor, end?.copyOf(), limit)
    override fun close() = native.close()
}

class MapSubscription(internal val native: BindingMapSubscription) : AutoCloseable {
    fun lastSeen() = native.lastSeen()?.copyOf()
    fun poll() = native.poll()
    override fun close() = native.close()
}

class MapMerge(internal val native: BindingMapMerge) : AutoCloseable {
    fun base() = native.base()
    fun head() = native.head()
    fun candidate() = native.candidate()
    fun merge(resolver: String? = null) = native.merge(resolver)
    fun conflictPage(cursor: RangeCursorRecord? = null, limit: ULong = 256uL) =
        native.conflictPage(cursor, limit)
    fun publish(resolver: String? = null) = native.publish(resolver)
    override fun close() = native.close()
}

private fun ownedMutation(mutation: MutationRecord) = MutationRecord(
    mutation.kind, mutation.key.copyOf(), mutation.value?.copyOf(),
)

private fun ownedRangeCursor(cursor: RangeCursorRecord?) =
    cursor?.let { RangeCursorRecord(it.afterKey?.copyOf()) }

private fun ownedReverseCursor(cursor: ReverseCursorRecord?) =
    cursor?.let { ReverseCursorRecord(it.beforeKey?.copyOf()) }

class MapSnapshot(internal val native: BindingMapSnapshot) : AutoCloseable {
    val id: ByteArray get() = native.id()
    val version get() = native.version()
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun getMany(keys: List<ByteArray>) = native.getMany(keys.map(ByteArray::copyOf))
    fun containsKey(key: ByteArray) = native.containsKey(key.copyOf())
    fun firstEntry() = native.firstEntry()
    fun lastEntry() = native.lastEntry()
    fun lowerBound(key: ByteArray) = native.lowerBound(key.copyOf())
    fun upperBound(key: ByteArray) = native.upperBound(key.copyOf())
    fun range(start: ByteArray = ByteArray(0), end: ByteArray? = null) =
        native.range(start.copyOf(), end?.copyOf())
    fun prefix(prefix: ByteArray) = native.prefix(prefix.copyOf())
    fun rangePage(
        cursor: RangeCursorRecord? = null,
        end: ByteArray? = null,
        limit: ULong = 256uL,
    ) = native.rangePage(ownedRangeCursor(cursor), end?.copyOf(), limit)
    fun prefixPage(
        prefix: ByteArray,
        cursor: RangeCursorRecord? = null,
        limit: ULong = 256uL,
    ) = native.prefixPage(prefix.copyOf(), ownedRangeCursor(cursor), limit)
    fun reversePage(
        cursor: ReverseCursorRecord? = null,
        start: ByteArray = ByteArray(0),
        limit: ULong = 256uL,
    ) = native.reversePage(ownedReverseCursor(cursor), start.copyOf(), limit)
    fun prefixReversePage(
        prefix: ByteArray,
        cursor: ReverseCursorRecord? = null,
        limit: ULong = 256uL,
    ) = native.prefixReversePage(prefix.copyOf(), ownedReverseCursor(cursor), limit)
    fun proveKey(key: ByteArray) = native.proveKey(key.copyOf())
    fun proveKeys(keys: List<ByteArray>) = native.proveKeys(keys.map(ByteArray::copyOf))
    fun proveRange(start: ByteArray = ByteArray(0), end: ByteArray? = null) =
        native.proveRange(start.copyOf(), end?.copyOf())
    fun provePrefix(prefix: ByteArray) = native.provePrefix(prefix.copyOf())
    fun proveRangePage(
        cursor: RangeCursorRecord? = null,
        end: ByteArray? = null,
        limit: ULong = 256uL,
    ) = native.proveRangePage(ownedRangeCursor(cursor), end?.copyOf(), limit)
    fun stats() = native.stats()
    fun export() = native.export()
    fun read() = ReadSession(native.readSession())
    override fun close() = native.close()
}

class ReadSession(internal val native: ProllyReadSession) : AutoCloseable {
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun getMany(keys: List<ByteArray>) = native.getMany(keys.map(ByteArray::copyOf))
    fun scanRangeView(
        start: ByteArray = ByteArray(0),
        end: ByteArray? = null,
        block: (EntryView) -> Boolean,
    ) = PackedPages.scanRangeView(native.fastHandle(), start.copyOf(), end?.copyOf(), block)
    override fun close() = native.close()
}

class IndexRegistry(internal val native: BindingIndexRegistry) : AutoCloseable {
    fun register(
        name: ByteArray,
        generation: ULong,
        extractorId: String,
        projection: IndexProjectionRecord,
        extractor: SecondaryIndexExtractorCallback,
    ) = native.register(name.copyOf(), generation, extractorId, projection, null, extractor)
    override fun close() = native.close()
}

class IndexedMap(internal val native: BindingIndexedMap) : AutoCloseable {
    val id: ByteArray get() = native.id()
    fun ensureIndex(name: ByteArray) = native.ensureIndex(name.copyOf())
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun put(key: ByteArray, value: ByteArray) = native.put(key.copyOf(), value.copyOf())
    fun apply(mutations: List<MutationRecord>) = native.apply(mutations)
    fun applyIf(expectedSource: ByteArray?, mutations: List<MutationRecord>) =
        native.applyIf(expectedSource?.copyOf(), mutations)
    fun delete(key: ByteArray) = native.delete(key.copyOf())
    fun snapshot() = IndexedSnapshot(native.snapshot())
    fun snapshotAt(sourceVersion: ByteArray) = IndexedSnapshot(native.snapshotAt(sourceVersion.copyOf()))
    fun snapshotById(id: IndexedSnapshotIdRecord) = IndexedSnapshot(native.snapshotById(id))
    fun health() = native.health()
    fun metrics() = native.metrics()
    fun verifyIndex(name: ByteArray, sourceVersion: ByteArray) =
        native.verifyIndex(name.copyOf(), sourceVersion.copyOf())
    fun verifyAll(sourceVersion: ByteArray) = native.verifyAll(sourceVersion.copyOf())
    fun repairIndex(name: ByteArray, sourceVersion: ByteArray) =
        native.repairIndex(name.copyOf(), sourceVersion.copyOf())
    fun deactivateIndex(name: ByteArray) = native.deactivateIndex(name.copyOf())
    fun exportCurrent() = native.exportCurrent()
    fun importCurrent(bundle: ByteArray, expectedSource: ByteArray? = null) =
        native.importCurrent(bundle.copyOf(), expectedSource?.copyOf())
    fun keepLast(count: ULong) = native.keepLast(count)
    override fun close() = native.close()
}

class IndexedSnapshot(internal val native: BindingIndexedSnapshot) : AutoCloseable {
    val id get() = native.id()
    fun index(name: ByteArray) = SecondaryIndex(native.index(name.copyOf()))
    override fun close() = native.close()
}

class SecondaryIndex(internal val native: BindingSecondaryIndexSnapshot) : AutoCloseable {
    val name: ByteArray get() = native.name()
    fun exact(term: ByteArray) = native.exact(term.copyOf())
    fun prefix(prefix: ByteArray) = native.prefix(prefix.copyOf())
    fun range(start: ByteArray, end: ByteArray? = null) =
        native.range(start.copyOf(), end?.copyOf())
    fun records(term: ByteArray) = native.records(term.copyOf())
    fun exactPage(term: ByteArray, cursor: ByteArray? = null, limit: ULong = 256uL) =
        native.exactPage(term.copyOf(), cursor?.copyOf(), limit)
    fun exactReversePage(term: ByteArray, cursor: ByteArray? = null, limit: ULong = 256uL) =
        native.exactReversePage(term.copyOf(), cursor?.copyOf(), limit)
    fun prefixPage(prefix: ByteArray, cursor: ByteArray? = null, limit: ULong = 256uL) =
        native.prefixPage(prefix.copyOf(), cursor?.copyOf(), limit)
    fun prefixReversePage(prefix: ByteArray, cursor: ByteArray? = null, limit: ULong = 256uL) =
        native.prefixReversePage(prefix.copyOf(), cursor?.copyOf(), limit)
    fun rangePage(start: ByteArray, end: ByteArray? = null, cursor: ByteArray? = null, limit: ULong = 256uL) =
        native.rangePage(start.copyOf(), end?.copyOf(), cursor?.copyOf(), limit)
    fun rangeReversePage(start: ByteArray, end: ByteArray? = null, cursor: ByteArray? = null, limit: ULong = 256uL) =
        native.rangeReversePage(start.copyOf(), end?.copyOf(), cursor?.copyOf(), limit)
    fun <R> withExactPage(
        term: ByteArray,
        limit: UInt = 256u,
        block: (List<IndexMatchView>) -> R,
    ): R = PackedPages.withIndexExact(native.fastHandle(), term, limit, block)
    override fun close() = native.close()
}

class ProximityMap(internal val native: BindingProximityMap) : AutoCloseable {
    val descriptor: ByteArray get() = native.descriptor()
    val count: ULong get() = native.count()
    val config: ProximityConfigRecord get() = native.config()
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun containsKey(key: ByteArray) = native.containsKey(key.copyOf())
    fun searchExact(query: List<Float>, k: ULong): ProximitySearchResultRecord =
        read().use { it.searchExact(query, k) }
    fun read() = ProximityReadSession(native.readSession())
    fun <R> withSearchView(
        query: List<Float>,
        k: UInt,
        block: (List<NeighborView>) -> R,
    ): R = read().use { it.withSearchView(query, k, block) }
    fun verify() = native.verify()
    fun mutate(mutations: List<ProximityMutationRecord>): Pair<ProximityMap, ProximityMutationStatsRecord> {
        val result = native.mutate(mutations)
        return ProximityMap(result.map) to result.stats
    }
    fun rebuild(mutations: List<ProximityMutationRecord>) = ProximityMap(native.rebuild(mutations))
    fun proveMembership(key: ByteArray) = native.proveMembership(key.copyOf())
    fun proveSearch(
        request: ProximitySearchRequestRecord,
        limits: ContentGraphLimitsRecord = defaultContentGraphLimits(),
    ) = ProximitySearchProof(native.proveSearch(request, limits))
    fun proveSearchExact(
        query: List<Float>,
        k: ULong,
        limits: ContentGraphLimitsRecord = defaultContentGraphLimits(),
    ) = proveSearch(exactProximitySearchRequest(query.toList(), k), limits)
    fun proveStructure(limits: ContentGraphLimitsRecord = defaultContentGraphLimits()) =
        native.proveStructure(limits)
    fun clearCache() = native.clearContentCache()
    override fun close() = native.close()
}

class ProximityReadSession(internal val native: BindingProximityReadSession) : AutoCloseable {
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun containsKey(key: ByteArray) = native.containsKey(key.copyOf())
    fun searchExact(query: List<Float>, k: ULong): ProximitySearchResultRecord =
        native.search(exactProximitySearchRequest(query.toList(), k))
    fun <R> withSearchView(
        query: List<Float>,
        k: UInt,
        block: (List<NeighborView>) -> R,
    ): R = PackedPages.withProximitySearch(native.fastHandle(), query, k, block)
    override fun close() = native.close()
}

class ProximitySearchProof(internal val native: BindingProximitySearchProof) : AutoCloseable {
    val sourceDescriptor: ByteArray get() = native.sourceDescriptor()
    fun verify(
        expectedDescriptor: ByteArray? = null,
        limits: ContentGraphLimitsRecord = defaultContentGraphLimits(),
    ) = native.verify(expectedDescriptor?.copyOf(), limits)
    override fun close() = native.close()
}

fun verifyKeyProof(proof: KeyProofRecord) = verifyNativeKeyProof(proof)

fun verifyMultiKeyProof(proof: build.crab.prolly.MultiKeyProofRecord) =
    verifyNativeMultiKeyProof(proof)

fun verifyRangeProof(proof: build.crab.prolly.RangeProofRecord) =
    verifyNativeRangeProof(proof)

fun verifyRangePageProof(proof: build.crab.prolly.RangePageProofRecord) =
    verifyNativeRangePageProof(proof)

fun verifyProximityMembershipProof(
    proof: ProximityMembershipProofRecord,
    expectedDescriptor: ByteArray? = null,
) = verifyNativeProximityMembershipProof(proof, expectedDescriptor?.copyOf())

fun verifyProximityStructureProof(
    proof: ProximityStructuralProofRecord,
    expectedDescriptor: ByteArray? = null,
    limits: ContentGraphLimitsRecord = defaultContentGraphLimits(),
) = verifyNativeProximityStructureProof(proof, expectedDescriptor?.copyOf(), limits)
