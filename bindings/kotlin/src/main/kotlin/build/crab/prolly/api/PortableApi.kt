package build.crab.prolly.api

import build.crab.prolly.BindingIndexedMap
import build.crab.prolly.BindingAcceleratorCatalog
import build.crab.prolly.BindingCompositeAccelerator
import build.crab.prolly.BindingIndexedSnapshot
import build.crab.prolly.BindingHnswIndex
import build.crab.prolly.BindingIndexRegistry
import build.crab.prolly.BindingMapComparison
import build.crab.prolly.BindingMapMerge
import build.crab.prolly.BindingMapSubscription
import build.crab.prolly.BindingProductQuantizer
import build.crab.prolly.BindingProximityCancellationToken
import build.crab.prolly.BindingProximityMap
import build.crab.prolly.BindingProximityReadSession
import build.crab.prolly.BindingProximitySearchProof
import build.crab.prolly.BindingProximitySearchRuntime
import build.crab.prolly.BindingSecondaryIndexSnapshot
import build.crab.prolly.BindingVersionedMap
import build.crab.prolly.BindingVersionedTransaction
import build.crab.prolly.BindingMapSnapshot
import build.crab.prolly.ConfigRecord
import build.crab.prolly.ContentGraphLimitsRecord
import build.crab.prolly.CompositeAcceleratorConfigRecord
import build.crab.prolly.CompositeBuildLimitsRecord
import build.crab.prolly.CompositeBuildOrRebuildKindRecord
import build.crab.prolly.CompositeBuildOrRebuildOutcomeRecord
import build.crab.prolly.CompositeBuildOutcomeRecord
import build.crab.prolly.CompositeBuildStatsRecord
import build.crab.prolly.CompositeRebuildOptionsRecord
import build.crab.prolly.FullRebuildReasonRecord
import build.crab.prolly.EntryRecord
import build.crab.prolly.HnswBuildLimitsRecord
import build.crab.prolly.HnswBuildStatsRecord
import build.crab.prolly.HnswConfigRecord
import build.crab.prolly.IndexProjectionRecord
import build.crab.prolly.IndexedSnapshotIdRecord
import build.crab.prolly.KeyProofRecord
import build.crab.prolly.MutationRecord
import build.crab.prolly.ParallelConfigRecord
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyReadSession
import build.crab.prolly.ProximityMembershipProofRecord
import build.crab.prolly.ProximityMutationRecord
import build.crab.prolly.ProximityMutationStatsRecord
import build.crab.prolly.ProximityRecordRecord
import build.crab.prolly.ProximityRecordVisitorCallback
import build.crab.prolly.ProximitySearchResultRecord
import build.crab.prolly.ProximitySearchRequestRecord
import build.crab.prolly.ProximitySearchRuntimePolicyRecord
import build.crab.prolly.ProximitySearchRuntimeStatsRecord
import build.crab.prolly.ProductQuantizationBuildLimitsRecord
import build.crab.prolly.ProductQuantizationBuildStatsRecord
import build.crab.prolly.ProductQuantizationConfigRecord
import build.crab.prolly.ProductQuantizationQualityRecord
import build.crab.prolly.RangeCursorRecord
import build.crab.prolly.ReverseCursorRecord
import build.crab.prolly.ProximityConfigRecord
import build.crab.prolly.ProximityStructuralProofRecord
import build.crab.prolly.SecondaryIndexExtractorCallback
import build.crab.prolly.SecondaryIndexLimitsRecord
import build.crab.prolly.SnapshotBundleRecord
import build.crab.prolly.defaultConfig
import build.crab.prolly.defaultCompositeAcceleratorConfig
import build.crab.prolly.defaultCompositeBuildLimits
import build.crab.prolly.defaultCompositeRebuildOptions
import build.crab.prolly.defaultContentGraphLimits
import build.crab.prolly.defaultHnswBuildLimits
import build.crab.prolly.defaultHnswConfig
import build.crab.prolly.defaultPqBuildLimits
import build.crab.prolly.defaultPqConfig
import build.crab.prolly.defaultProximityConfig
import build.crab.prolly.defaultProximitySearchRuntimePolicy
import build.crab.prolly.exactProximitySearchRequest
import build.crab.prolly.verifyKeyProof as verifyNativeKeyProof
import build.crab.prolly.verifyMultiKeyProof as verifyNativeMultiKeyProof
import build.crab.prolly.verifyRangePageProof as verifyNativeRangePageProof
import build.crab.prolly.verifyRangeProof as verifyNativeRangeProof
import build.crab.prolly.verifyProximityMembershipProof as verifyNativeProximityMembershipProof
import build.crab.prolly.verifyProximityStructureProof as verifyNativeProximityStructureProof
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import java.util.concurrent.CompletableFuture

data class ProximityRecord(val key: ByteArray, val vector: List<Float>, val value: ByteArray)
data class HnswBuildResult(val index: HnswIndex, val stats: HnswBuildStatsRecord)
data class ProductQuantizationBuildResult(
    val index: ProductQuantizer,
    val stats: ProductQuantizationBuildStatsRecord,
)
data class CompositeBuildOutcome(
    val accelerator: CompositeAccelerator?,
    val reasons: List<FullRebuildReasonRecord>,
    val stats: CompositeBuildStatsRecord,
)
data class CompositeBuildOrRebuildOutcome(
    val kind: CompositeBuildOrRebuildKindRecord,
    val composite: CompositeAccelerator?,
    val hnsw: HnswIndex?,
    val pq: ProductQuantizer?,
    val reasons: List<FullRebuildReasonRecord>,
    val compositeStats: CompositeBuildStatsRecord,
    val hnswStats: HnswBuildStatsRecord?,
    val pqStats: ProductQuantizationBuildStatsRecord?,
)

private fun CompositeBuildOutcomeRecord.portable() = CompositeBuildOutcome(
    accelerator?.let(::CompositeAccelerator), reasons, stats,
)

private fun CompositeBuildOrRebuildOutcomeRecord.portable() = CompositeBuildOrRebuildOutcome(
    kind,
    composite?.let(::CompositeAccelerator),
    hnsw?.let(::HnswIndex),
    pq?.let(::ProductQuantizer),
    reasons,
    compositeStats,
    hnswStats,
    pqStats,
)

private fun ownedSearchRequest(request: ProximitySearchRequestRecord) = request.copy(
    query = request.query.toList(),
    budget = request.budget.copy(),
    filter = request.filter.copy(
        start = request.filter.start?.copyOf(),
        rangeEnd = request.filter.rangeEnd?.copyOf(),
        prefix = request.filter.prefix?.copyOf(),
        eligibleKeys = request.filter.eligibleKeys.map(ByteArray::copyOf),
    ),
)

private suspend fun <T> cooperativeSearch(
    cancellation: ProximityCancellationToken?,
    block: (ProximityCancellationToken) -> T,
): T {
    val token = cancellation ?: ProximityCancellationToken()
    return suspendCancellableCoroutine { continuation ->
        val future = CompletableFuture.supplyAsync { block(token) }
        continuation.invokeOnCancellation { token.cancel() }
        future.whenComplete { value, error ->
            if (continuation.isActive) {
                continuation.resumeWith(
                    if (error == null) Result.success(value) else Result.failure(error),
                )
            }
            if (cancellation == null) token.close()
        }
    }
}

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

    fun proximitySearchRuntime(
        policy: ProximitySearchRuntimePolicyRecord = defaultProximitySearchRuntimePolicy(),
    ) = ProximitySearchRuntime(native.proximitySearchRuntime(policy))

    override fun close() = native.close()
}

class VersionedMap(internal val native: BindingVersionedMap) : AutoCloseable {
    val id: ByteArray get() = native.id()
    fun isInitialized() = native.isInitialized()
    fun initialize() = native.initialize()
    fun initializeSorted(entries: List<EntryRecord>) = native.initializeSorted(entries.map(::ownedEntry))
    fun head() = native.head()
    fun headId() = native.headId()
    fun version(id: ByteArray) = native.version(id.copyOf())
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun containsKey(key: ByteArray) = native.containsKey(key.copyOf())
    fun getMany(keys: List<ByteArray>) = native.getMany(keys.map(ByteArray::copyOf))
    fun getAt(id: ByteArray, key: ByteArray) = native.getAt(id.copyOf(), key.copyOf())
    fun getManyAt(id: ByteArray, keys: List<ByteArray>) =
        native.getManyAt(id.copyOf(), keys.map(ByteArray::copyOf))
    fun range(start: ByteArray = ByteArray(0), end: ByteArray? = null) =
        native.range(start.copyOf(), end?.copyOf())
    fun prefix(prefix: ByteArray) = native.prefix(prefix.copyOf())
    fun rangeAt(id: ByteArray, start: ByteArray = ByteArray(0), end: ByteArray? = null) =
        native.rangeAt(id.copyOf(), start.copyOf(), end?.copyOf())
    fun prefixAt(id: ByteArray, prefix: ByteArray) = native.prefixAt(id.copyOf(), prefix.copyOf())
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
    fun rangePageAt(
        id: ByteArray,
        cursor: RangeCursorRecord? = null,
        end: ByteArray? = null,
        limit: ULong = 256uL,
    ) = native.rangePageAt(id.copyOf(), ownedRangeCursor(cursor), end?.copyOf(), limit)
    fun prefixPageAt(
        id: ByteArray,
        prefix: ByteArray,
        cursor: RangeCursorRecord? = null,
        limit: ULong = 256uL,
    ) = native.prefixPageAt(id.copyOf(), prefix.copyOf(), ownedRangeCursor(cursor), limit)
    fun diff(base: ByteArray, target: ByteArray) = native.diff(base.copyOf(), target.copyOf())
    fun changesSince(base: ByteArray) = native.changesSince(base.copyOf())
    fun rollbackTo(id: ByteArray) = native.rollbackTo(id.copyOf())
    fun put(key: ByteArray, value: ByteArray) = native.put(key.copyOf(), value.copyOf())
    fun apply(mutations: List<MutationRecord>) = native.apply(mutations.map(::ownedMutation))
    fun append(mutations: List<MutationRecord>) = native.append(mutations.map(::ownedMutation))
    fun parallelApply(mutations: List<MutationRecord>, config: ParallelConfigRecord) =
        native.parallelApply(
            mutations.map(::ownedMutation),
            ParallelConfigRecord(config.maxThreads, config.parallelismThreshold),
        )
    fun rebuildSortedIf(expected: ByteArray?, entries: List<EntryRecord>) =
        native.rebuildSortedIf(expected?.copyOf(), entries.map(::ownedEntry))
    fun rebuildFromEntriesIf(expected: ByteArray?, entries: List<EntryRecord>) =
        native.rebuildFromEntriesIf(expected?.copyOf(), entries.map(::ownedEntry))
    fun rebuildFromIterIf(expected: ByteArray?, entries: List<EntryRecord>) =
        rebuildFromEntriesIf(expected, entries)
    fun applyAtMillis(mutations: List<MutationRecord>, timestampMillis: ULong) =
        native.applyAtMillis(mutations.map(::ownedMutation), timestampMillis)
    fun applyIf(expected: ByteArray?, mutations: List<MutationRecord>) =
        native.applyIf(expected?.copyOf(), mutations.map(::ownedMutation))
    fun applyIfAtMillis(expected: ByteArray?, mutations: List<MutationRecord>, timestampMillis: ULong) =
        native.applyIfAtMillis(expected?.copyOf(), mutations.map(::ownedMutation), timestampMillis)
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
    fun pruneVersions(keepLatest: ULong) = native.pruneVersions(keepLatest)
    fun keepForAt(nowMillis: ULong, maxAgeMillis: ULong) = native.keepForAt(nowMillis, maxAgeMillis)
    fun keepFor(maxAgeMillis: ULong) = native.keepFor(maxAgeMillis)
    fun keepVersions(ids: List<ByteArray>) = native.keepVersions(ids.map(ByteArray::copyOf))
    fun retentionPolicy() = native.retentionPolicy()
    fun verifyCatalog() = native.verifyCatalog()
    fun planGc() = native.planGc()
    fun sweepGc() = native.sweepGc()
    suspend fun putAsync(key: ByteArray, value: ByteArray) =
        key.copyOf().let { copiedKey ->
            value.copyOf().let { copiedValue ->
                withContext(Dispatchers.IO) { native.put(copiedKey, copiedValue) }
            }
        }
    suspend fun initializeAsync() = withContext(Dispatchers.IO) { initialize() }
    suspend fun headAsync() = withContext(Dispatchers.IO) { head() }
    suspend fun versionAsync(id: ByteArray) = id.copyOf().let { owned ->
        withContext(Dispatchers.IO) { version(owned) }
    }
    suspend fun getAsync(key: ByteArray) = key.copyOf().let { owned ->
        withContext(Dispatchers.IO) { get(owned) }
    }
    suspend fun applyAsync(mutations: List<MutationRecord>) = mutations.map(::ownedMutation).let { owned ->
        withContext(Dispatchers.IO) { apply(owned) }
    }
    suspend fun deleteAsync(key: ByteArray) = key.copyOf().let { owned ->
        withContext(Dispatchers.IO) { delete(owned) }
    }
    suspend fun snapshotAsync() = withContext(Dispatchers.IO) { snapshot() }
    suspend fun snapshotAtAsync(id: ByteArray) = id.copyOf().let { owned ->
        withContext(Dispatchers.IO) { snapshotAt(owned) }
    }
    suspend fun subscribeAsync() = withContext(Dispatchers.IO) { subscribe() }
    suspend fun subscribeFromAsync(lastSeen: ByteArray? = null) = lastSeen?.copyOf().let { owned ->
        withContext(Dispatchers.IO) { subscribeFrom(owned) }
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
    suspend fun pollAsync() = withContext(Dispatchers.IO) { poll() }
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

private fun ownedEntry(entry: EntryRecord) = EntryRecord(entry.key.copyOf(), entry.value.copyOf())

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
    suspend fun getAsync(key: ByteArray) = key.copyOf().let { owned ->
        withContext(Dispatchers.IO) { get(owned) }
    }
    suspend fun getManyAsync(keys: List<ByteArray>) = keys.map(ByteArray::copyOf).let { owned ->
        withContext(Dispatchers.IO) { getMany(owned) }
    }
    suspend fun rangeAsync(start: ByteArray = ByteArray(0), end: ByteArray? = null) =
        start.copyOf().let { ownedStart ->
            val ownedEnd = end?.copyOf()
            withContext(Dispatchers.IO) { range(ownedStart, ownedEnd) }
        }
    suspend fun prefixAsync(prefix: ByteArray) = prefix.copyOf().let { owned ->
        withContext(Dispatchers.IO) { prefix(owned) }
    }
    suspend fun rangePageAsync(
        cursor: RangeCursorRecord? = null, end: ByteArray? = null, limit: ULong = 256uL,
    ) = withContext(Dispatchers.IO) { rangePage(ownedRangeCursor(cursor), end?.copyOf(), limit) }
    suspend fun prefixPageAsync(
        prefix: ByteArray, cursor: RangeCursorRecord? = null, limit: ULong = 256uL,
    ) = prefix.copyOf().let { owned ->
        withContext(Dispatchers.IO) { prefixPage(owned, ownedRangeCursor(cursor), limit) }
    }
    suspend fun proveKeyAsync(key: ByteArray) = key.copyOf().let { owned ->
        withContext(Dispatchers.IO) { proveKey(owned) }
    }
    suspend fun proveKeysAsync(keys: List<ByteArray>) = keys.map(ByteArray::copyOf).let { owned ->
        withContext(Dispatchers.IO) { proveKeys(owned) }
    }
    suspend fun proveRangeAsync(start: ByteArray = ByteArray(0), end: ByteArray? = null) =
        start.copyOf().let { ownedStart ->
            val ownedEnd = end?.copyOf()
            withContext(Dispatchers.IO) { proveRange(ownedStart, ownedEnd) }
        }
    suspend fun provePrefixAsync(prefix: ByteArray) = prefix.copyOf().let { owned ->
        withContext(Dispatchers.IO) { provePrefix(owned) }
    }
    suspend fun statsAsync() = withContext(Dispatchers.IO) { stats() }
    override fun close() = native.close()
}

class ReadSession(internal val native: ProllyReadSession) : AutoCloseable {
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun getMany(keys: List<ByteArray>) = native.getMany(keys.map(ByteArray::copyOf))
    suspend fun getAsync(key: ByteArray) = key.copyOf().let { owned ->
        withContext(Dispatchers.IO) { get(owned) }
    }
    suspend fun getManyAsync(keys: List<ByteArray>) = keys.map(ByteArray::copyOf).let { owned ->
        withContext(Dispatchers.IO) { getMany(owned) }
    }
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
    fun replaceIndex(
        name: ByteArray,
        generation: ULong,
        extractorId: String,
        projection: IndexProjectionRecord,
        extractor: SecondaryIndexExtractorCallback,
        limits: SecondaryIndexLimitsRecord? = null,
    ) = native.replaceIndex(name.copyOf(), generation, extractorId, projection, limits, extractor)
    fun deactivateIndex(name: ByteArray) = native.deactivateIndex(name.copyOf())
    fun exportCurrent() = native.exportCurrent()
    fun importCurrent(bundle: ByteArray, expectedSource: ByteArray? = null) =
        native.importCurrent(bundle.copyOf(), expectedSource?.copyOf())
    fun keepLast(count: ULong) = native.keepLast(count)
    fun planGc() = native.planGc()
    suspend fun getAsync(key: ByteArray) = key.copyOf().let { owned ->
        withContext(Dispatchers.IO) { get(owned) }
    }
    suspend fun putAsync(key: ByteArray, value: ByteArray) =
        key.copyOf().let { ownedKey ->
            value.copyOf().let { ownedValue ->
                withContext(Dispatchers.IO) { put(ownedKey, ownedValue) }
            }
        }
    suspend fun applyAsync(mutations: List<MutationRecord>) = mutations.map(::ownedMutation).let { owned ->
        withContext(Dispatchers.IO) { apply(owned) }
    }
    suspend fun deleteAsync(key: ByteArray) = key.copyOf().let { owned ->
        withContext(Dispatchers.IO) { delete(owned) }
    }
    suspend fun ensureIndexAsync(name: ByteArray) = name.copyOf().let { owned ->
        withContext(Dispatchers.IO) { ensureIndex(owned) }
    }
    suspend fun snapshotAsync() = withContext(Dispatchers.IO) { snapshot() }
    suspend fun snapshotAtAsync(sourceVersion: ByteArray) = sourceVersion.copyOf().let { owned ->
        withContext(Dispatchers.IO) { snapshotAt(owned) }
    }
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
    fun buildHnsw(
        config: HnswConfigRecord = defaultHnswConfig(),
        limits: HnswBuildLimitsRecord = defaultHnswBuildLimits(),
    ): HnswBuildResult {
        val result = native.buildHnsw(config, limits)
        return HnswBuildResult(HnswIndex(result.index), result.stats)
    }
    fun loadHnsw(manifest: ByteArray) = HnswIndex(native.loadHnsw(manifest.copyOf()))
    fun buildPq(
        config: ProductQuantizationConfigRecord = defaultPqConfig(),
        workerThreads: ULong = 1uL,
        limits: ProductQuantizationBuildLimitsRecord = defaultPqBuildLimits(),
    ): ProductQuantizationBuildResult {
        val result = native.buildPq(config, workerThreads, limits)
        return ProductQuantizationBuildResult(ProductQuantizer(result.index), result.stats)
    }
    fun loadPq(manifest: ByteArray) = ProductQuantizer(native.loadPq(manifest.copyOf()))
    fun buildCompositeHnsw(
        baseMap: ProximityMap,
        base: HnswIndex,
        config: CompositeAcceleratorConfigRecord = defaultCompositeAcceleratorConfig(),
        limits: CompositeBuildLimitsRecord = defaultCompositeBuildLimits(),
    ) = native.buildCompositeHnsw(baseMap.native, base.native, config, limits).portable()
    fun buildCompositePq(
        baseMap: ProximityMap,
        base: ProductQuantizer,
        config: CompositeAcceleratorConfigRecord = defaultCompositeAcceleratorConfig(),
        limits: CompositeBuildLimitsRecord = defaultCompositeBuildLimits(),
    ) = native.buildCompositePq(baseMap.native, base.native, config, limits).portable()
    fun buildOrRebuildCompositeHnsw(
        baseMap: ProximityMap,
        base: HnswIndex,
        config: CompositeAcceleratorConfigRecord = defaultCompositeAcceleratorConfig(),
        limits: CompositeBuildLimitsRecord = defaultCompositeBuildLimits(),
        rebuild: CompositeRebuildOptionsRecord = defaultCompositeRebuildOptions(),
    ) = native.buildOrRebuildCompositeHnsw(
        baseMap.native, base.native, config, limits, rebuild,
    ).portable()
    fun buildOrRebuildCompositePq(
        baseMap: ProximityMap,
        base: ProductQuantizer,
        config: CompositeAcceleratorConfigRecord = defaultCompositeAcceleratorConfig(),
        limits: CompositeBuildLimitsRecord = defaultCompositeBuildLimits(),
        rebuild: CompositeRebuildOptionsRecord = defaultCompositeRebuildOptions(),
    ) = native.buildOrRebuildCompositePq(
        baseMap.native, base.native, config, limits, rebuild,
    ).portable()
    fun loadComposite(manifest: ByteArray) =
        CompositeAccelerator(native.loadComposite(manifest.copyOf()))
    fun buildAcceleratorCatalog(
        hnsw: HnswIndex? = null,
        pq: ProductQuantizer? = null,
        composite: CompositeAccelerator? = null,
    ) = AcceleratorCatalog(
        native.buildAcceleratorCatalog(hnsw?.native, pq?.native, composite?.native),
    )
    fun loadAcceleratorCatalog(manifest: ByteArray) =
        AcceleratorCatalog(native.loadAcceleratorCatalog(manifest.copyOf()))
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun containsKey(key: ByteArray) = native.containsKey(key.copyOf())
    fun search(request: ProximitySearchRequestRecord): ProximitySearchResultRecord =
        read().use { it.search(request) }
    fun searchWithRuntime(
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime,
    ): ProximitySearchResultRecord =
        native.searchWithRuntime(ownedSearchRequest(request), runtime.native)
    fun searchCancellable(
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime? = null,
        cancellation: ProximityCancellationToken,
    ): ProximitySearchResultRecord = native.searchCancellable(
        ownedSearchRequest(request), runtime?.native, cancellation.native,
    )
    suspend fun searchAsync(
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime? = null,
        cancellation: ProximityCancellationToken? = null,
    ): ProximitySearchResultRecord {
        val owned = ownedSearchRequest(request)
        return cooperativeSearch(cancellation) { token ->
            searchCancellable(owned, runtime, token)
        }
    }
    fun searchExact(query: List<Float>, k: ULong): ProximitySearchResultRecord =
        read().use { it.searchExact(query, k) }
    fun scanRecords(visitor: (ProximityRecordRecord) -> Boolean): ULong =
        native.scanRecords(object : ProximityRecordVisitorCallback {
            override fun visit(record: ProximityRecordRecord) = visitor(record)
        })
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
    ) = ProximitySearchProof(native.proveSearch(ownedSearchRequest(request), limits))
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

class ProximitySearchRuntime(
    internal val native: BindingProximitySearchRuntime,
) : AutoCloseable {
    val policy: ProximitySearchRuntimePolicyRecord get() = native.policy()
    val stats: ProximitySearchRuntimeStatsRecord get() = native.stats()
    fun clear() = native.clear()
    override fun close() = native.close()
}

class ProximityCancellationToken(
    internal val native: BindingProximityCancellationToken = BindingProximityCancellationToken(),
) : AutoCloseable {
    val isCancelled: Boolean get() = native.isCancelled()
    fun cancel() = native.cancel()
    override fun close() = native.close()
}

class HnswIndex(internal val native: BindingHnswIndex) : AutoCloseable {
    val manifest: ByteArray get() = native.manifest().copyOf()
    val sourceDescriptor: ByteArray get() = native.sourceDescriptor().copyOf()
    val config: HnswConfigRecord get() = native.config()
    val isCanonical: Boolean get() = native.isCanonical()
    fun search(map: ProximityMap, request: ProximitySearchRequestRecord): ProximitySearchResultRecord =
        native.search(map.native, ownedSearchRequest(request))
    fun searchWithRuntime(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime,
    ): ProximitySearchResultRecord =
        native.searchWithRuntime(map.native, ownedSearchRequest(request), runtime.native)
    fun searchCancellable(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime? = null,
        cancellation: ProximityCancellationToken,
    ): ProximitySearchResultRecord = native.searchCancellable(
        map.native, ownedSearchRequest(request), runtime?.native, cancellation.native,
    )
    suspend fun searchAsync(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime? = null,
        cancellation: ProximityCancellationToken? = null,
    ): ProximitySearchResultRecord {
        val owned = ownedSearchRequest(request)
        return cooperativeSearch(cancellation) { token ->
            searchCancellable(map, owned, runtime, token)
        }
    }
    fun proveSearch(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        limits: ContentGraphLimitsRecord = defaultContentGraphLimits(),
    ) = ProximitySearchProof(native.proveSearch(map.native, ownedSearchRequest(request), limits))
    override fun close() = native.close()
}

class ProductQuantizer(internal val native: BindingProductQuantizer) : AutoCloseable {
    val manifest: ByteArray get() = native.manifest().copyOf()
    val sourceDescriptor: ByteArray get() = native.sourceDescriptor().copyOf()
    val config: ProductQuantizationConfigRecord get() = native.config()
    val quality: ProductQuantizationQualityRecord get() = native.quality()
    fun search(map: ProximityMap, request: ProximitySearchRequestRecord): ProximitySearchResultRecord =
        native.search(map.native, ownedSearchRequest(request))
    fun searchWithRuntime(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime,
    ): ProximitySearchResultRecord =
        native.searchWithRuntime(map.native, ownedSearchRequest(request), runtime.native)
    fun searchCancellable(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime? = null,
        cancellation: ProximityCancellationToken,
    ): ProximitySearchResultRecord = native.searchCancellable(
        map.native, ownedSearchRequest(request), runtime?.native, cancellation.native,
    )
    suspend fun searchAsync(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime? = null,
        cancellation: ProximityCancellationToken? = null,
    ): ProximitySearchResultRecord {
        val owned = ownedSearchRequest(request)
        return cooperativeSearch(cancellation) { token ->
            searchCancellable(map, owned, runtime, token)
        }
    }
    fun proveSearch(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        limits: ContentGraphLimitsRecord = defaultContentGraphLimits(),
    ) = ProximitySearchProof(native.proveSearch(map.native, ownedSearchRequest(request), limits))
    override fun close() = native.close()
}

class CompositeAccelerator(internal val native: BindingCompositeAccelerator) : AutoCloseable {
    val manifest: ByteArray get() = native.manifest().copyOf()
    val currentSourceDescriptor: ByteArray get() = native.currentSourceDescriptor().copyOf()
    val baseSourceDescriptor: ByteArray get() = native.baseSourceDescriptor().copyOf()
    val baseKind get() = native.baseKind()
    val deltaCount: ULong get() = native.deltaCount()
    val shadowCount: ULong get() = native.shadowCount()
    val config: CompositeAcceleratorConfigRecord get() = native.config()
    val buildStats: CompositeBuildStatsRecord get() = native.buildStats()
    fun search(map: ProximityMap, request: ProximitySearchRequestRecord): ProximitySearchResultRecord =
        native.search(map.native, ownedSearchRequest(request))
    fun searchWithRuntime(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime,
    ): ProximitySearchResultRecord =
        native.searchWithRuntime(map.native, ownedSearchRequest(request), runtime.native)
    fun searchCancellable(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime? = null,
        cancellation: ProximityCancellationToken,
    ): ProximitySearchResultRecord = native.searchCancellable(
        map.native, ownedSearchRequest(request), runtime?.native, cancellation.native,
    )
    suspend fun searchAsync(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime? = null,
        cancellation: ProximityCancellationToken? = null,
    ): ProximitySearchResultRecord {
        val owned = ownedSearchRequest(request)
        return cooperativeSearch(cancellation) { token ->
            searchCancellable(map, owned, runtime, token)
        }
    }
    fun proveSearch(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        limits: ContentGraphLimitsRecord = defaultContentGraphLimits(),
    ) = ProximitySearchProof(native.proveSearch(map.native, ownedSearchRequest(request), limits))
    override fun close() = native.close()
}

class AcceleratorCatalog(internal val native: BindingAcceleratorCatalog) : AutoCloseable {
    val manifest: ByteArray get() = native.manifest().copyOf()
    val sourceDescriptor: ByteArray get() = native.sourceDescriptor().copyOf()
    val entries get() = native.entries()
    fun search(map: ProximityMap, request: ProximitySearchRequestRecord): ProximitySearchResultRecord =
        native.search(map.native, ownedSearchRequest(request))
    fun searchWithRuntime(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime,
    ): ProximitySearchResultRecord =
        native.searchWithRuntime(map.native, ownedSearchRequest(request), runtime.native)
    fun searchCancellable(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime? = null,
        cancellation: ProximityCancellationToken,
    ): ProximitySearchResultRecord = native.searchCancellable(
        map.native, ownedSearchRequest(request), runtime?.native, cancellation.native,
    )
    suspend fun searchAsync(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime? = null,
        cancellation: ProximityCancellationToken? = null,
    ): ProximitySearchResultRecord {
        val owned = ownedSearchRequest(request)
        return cooperativeSearch(cancellation) { token ->
            searchCancellable(map, owned, runtime, token)
        }
    }
    fun proveSearch(
        map: ProximityMap,
        request: ProximitySearchRequestRecord,
        limits: ContentGraphLimitsRecord = defaultContentGraphLimits(),
    ) = ProximitySearchProof(native.proveSearch(map.native, ownedSearchRequest(request), limits))
    override fun close() = native.close()
}

class ProximityReadSession(internal val native: BindingProximityReadSession) : AutoCloseable {
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun containsKey(key: ByteArray) = native.containsKey(key.copyOf())
    fun search(request: ProximitySearchRequestRecord): ProximitySearchResultRecord =
        native.search(ownedSearchRequest(request))
    fun searchWithRuntime(
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime,
    ): ProximitySearchResultRecord =
        native.searchWithRuntime(ownedSearchRequest(request), runtime.native)
    fun searchCancellable(
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime? = null,
        cancellation: ProximityCancellationToken,
    ): ProximitySearchResultRecord = native.searchCancellable(
        ownedSearchRequest(request), runtime?.native, cancellation.native,
    )
    suspend fun searchAsync(
        request: ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime? = null,
        cancellation: ProximityCancellationToken? = null,
    ): ProximitySearchResultRecord {
        val owned = ownedSearchRequest(request)
        return cooperativeSearch(cancellation) { token ->
            searchCancellable(owned, runtime, token)
        }
    }
    fun searchExact(query: List<Float>, k: ULong): ProximitySearchResultRecord =
        search(exactProximitySearchRequest(query.toList(), k))
    fun scanRecords(visitor: (ProximityRecordRecord) -> Boolean): ULong =
        native.scanRecords(object : ProximityRecordVisitorCallback {
            override fun visit(record: ProximityRecordRecord) = visitor(record)
        })
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
