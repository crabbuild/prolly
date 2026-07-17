package build.crab.prolly.api

import build.crab.prolly.BindingIndexedMap
import build.crab.prolly.BindingIndexedSnapshot
import build.crab.prolly.BindingIndexRegistry
import build.crab.prolly.BindingProximityMap
import build.crab.prolly.BindingProximityReadSession
import build.crab.prolly.BindingSecondaryIndexSnapshot
import build.crab.prolly.BindingVersionedMap
import build.crab.prolly.BindingMapSnapshot
import build.crab.prolly.ConfigRecord
import build.crab.prolly.ContentGraphLimitsRecord
import build.crab.prolly.IndexProjectionRecord
import build.crab.prolly.KeyProofRecord
import build.crab.prolly.ProllyEngine
import build.crab.prolly.ProllyReadSession
import build.crab.prolly.ProximityMembershipProofRecord
import build.crab.prolly.ProximityRecordRecord
import build.crab.prolly.ProximitySearchResultRecord
import build.crab.prolly.ProximityConfigRecord
import build.crab.prolly.SecondaryIndexExtractorCallback
import build.crab.prolly.SnapshotBundleRecord
import build.crab.prolly.defaultConfig
import build.crab.prolly.defaultContentGraphLimits
import build.crab.prolly.defaultProximityConfig
import build.crab.prolly.exactProximitySearchRequest
import build.crab.prolly.verifyKeyProof as verifyNativeKeyProof
import build.crab.prolly.verifyProximityMembershipProof as verifyNativeProximityMembershipProof
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

data class ProximityRecord(val key: ByteArray, val vector: List<Float>, val value: ByteArray)

class Engine private constructor(internal val native: ProllyEngine) : AutoCloseable {
    companion object {
        fun memory(config: ConfigRecord = defaultConfig()) = Engine(ProllyEngine.memory(config))
    }

    fun versionedMap(id: ByteArray) = VersionedMap(native.versionedMap(id.copyOf()))

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
    fun initialize() = native.initialize()
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun put(key: ByteArray, value: ByteArray) = native.put(key.copyOf(), value.copyOf())
    fun delete(key: ByteArray) = native.delete(key.copyOf())
    fun snapshot() = native.snapshot()?.let(::MapSnapshot)
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

class MapSnapshot(internal val native: BindingMapSnapshot) : AutoCloseable {
    val id: ByteArray get() = native.id()
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun range(start: ByteArray = ByteArray(0), end: ByteArray? = null) =
        native.range(start.copyOf(), end?.copyOf())
    fun proveKey(key: ByteArray) = native.proveKey(key.copyOf())
    fun proveKeys(keys: List<ByteArray>) = native.proveKeys(keys.map(ByteArray::copyOf))
    fun proveRange(start: ByteArray = ByteArray(0), end: ByteArray? = null) =
        native.proveRange(start.copyOf(), end?.copyOf())
    fun stats() = native.stats()
    fun export() = native.export()
    fun read() = ReadSession(native.readSession())
    override fun close() = native.close()
}

class ReadSession(internal val native: ProllyReadSession) : AutoCloseable {
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun getMany(keys: List<ByteArray>) = native.getMany(keys.map(ByteArray::copyOf))
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
    fun ensureIndex(name: ByteArray) = native.ensureIndex(name.copyOf())
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun put(key: ByteArray, value: ByteArray) = native.put(key.copyOf(), value.copyOf())
    fun delete(key: ByteArray) = native.delete(key.copyOf())
    fun snapshot() = IndexedSnapshot(native.snapshot())
    fun health() = native.health()
    fun metrics() = native.metrics()
    fun verifyIndex(name: ByteArray, sourceVersion: ByteArray) =
        native.verifyIndex(name.copyOf(), sourceVersion.copyOf())
    fun verifyAll(sourceVersion: ByteArray) = native.verifyAll(sourceVersion.copyOf())
    fun repairIndex(name: ByteArray, sourceVersion: ByteArray) =
        native.repairIndex(name.copyOf(), sourceVersion.copyOf())
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
    fun exact(term: ByteArray) = native.exact(term.copyOf())
    fun prefix(prefix: ByteArray) = native.prefix(prefix.copyOf())
    fun range(start: ByteArray, end: ByteArray? = null) =
        native.range(start.copyOf(), end?.copyOf())
    fun records(term: ByteArray) = native.records(term.copyOf())
    fun <R> withExactPage(
        term: ByteArray,
        limit: UInt = 256u,
        block: (List<IndexMatchView>) -> R,
    ): R = PackedPages.withIndexExact(native.fastHandle(), term, limit, block)
    override fun close() = native.close()
}

class ProximityMap(internal val native: BindingProximityMap) : AutoCloseable {
    val descriptor: ByteArray get() = native.descriptor()
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun searchExact(query: List<Float>, k: ULong): ProximitySearchResultRecord =
        native.search(exactProximitySearchRequest(query.toList(), k))
    fun read() = ProximityReadSession(native.readSession())
    fun <R> withSearchView(
        query: List<Float>,
        k: UInt,
        block: (List<NeighborView>) -> R,
    ): R = PackedPages.withProximitySearch(native.fastHandle(), query, k, block)
    fun verify() = native.verify()
    fun proveMembership(key: ByteArray) = native.proveMembership(key.copyOf())
    fun proveStructure(limits: ContentGraphLimitsRecord = defaultContentGraphLimits()) =
        native.proveStructure(limits)
    fun clearCache() = native.clearContentCache()
    override fun close() = native.close()
}

class ProximityReadSession(internal val native: BindingProximityReadSession) : AutoCloseable {
    fun get(key: ByteArray) = native.get(key.copyOf())
    fun containsKey(key: ByteArray) = native.containsKey(key.copyOf())
    override fun close() = native.close()
}

fun verifyKeyProof(proof: KeyProofRecord) = verifyNativeKeyProof(proof)

fun verifyProximityMembershipProof(
    proof: ProximityMembershipProofRecord,
    expectedDescriptor: ByteArray? = null,
) = verifyNativeProximityMembershipProof(proof, expectedDescriptor?.copyOf())
