import Foundation
import Prolly
import prollyFFI

public struct ProximityRecord: Sendable {
    public let key: Data
    public let vector: [Float]
    public let value: Data

    public init(key: Data, vector: [Float], value: Data) {
        self.key = key
        self.vector = vector
        self.value = value
    }
}

public final class Engine: @unchecked Sendable {
    let native: ProllyEngine
    private var closed = false

    private init(native: ProllyEngine) {
        self.native = native
    }

    public static func memory(config: ConfigRecord = defaultConfig()) throws -> Engine {
        Engine(native: try ProllyEngine.memory(config: config))
    }

    public static func withMemory<R>(
        config: ConfigRecord = defaultConfig(),
        _ body: (Engine) throws -> R
    ) throws -> R {
        let engine = try memory(config: config)
        defer { engine.close() }
        return try body(engine)
    }

    public static func withMemory<R>(
        config: ConfigRecord = defaultConfig(),
        _ body: (Engine) async throws -> R
    ) async throws -> R {
        let engine = try memory(config: config)
        defer { engine.close() }
        return try await body(engine)
    }

    public func close() {
        closed = true
    }

    public func versionedMap(_ id: Data) throws -> VersionedMap {
        try checkOpen()
        return VersionedMap(native: try native.versionedMap(id: Data(id)))
    }

    public func indexRegistry() throws -> IndexRegistry {
        try checkOpen()
        return IndexRegistry(native: BindingIndexRegistry())
    }

    public func indexedMap(_ id: Data, registry: IndexRegistry) throws -> IndexedMap {
        try checkOpen()
        return IndexedMap(native: try native.indexedMap(id: Data(id), registry: registry.native))
    }

    public func buildProximity(
        dimensions: UInt32,
        records: [ProximityRecord],
        config: ProximityConfigRecord? = nil,
        threads: UInt64? = nil
    ) throws -> ProximityMap {
        try checkOpen()
        let nativeRecords = records.map {
            ProximityRecordRecord(key: Data($0.key), vector: $0.vector, value: Data($0.value))
        }
        return ProximityMap(native: try native.buildProximityMap(
            config: config ?? defaultProximityConfig(dimensions: dimensions),
            records: nativeRecords,
            threads: threads
        ))
    }

    public func loadProximity(_ descriptor: Data) throws -> ProximityMap {
        try checkOpen()
        return ProximityMap(native: try native.loadProximityMap(descriptor: Data(descriptor)))
    }

    private func checkOpen() throws {
        if closed { throw PortableAPIError.closed("Engine") }
    }
}

public enum PortableAPIError: Error, Equatable {
    case closed(String)
    case packedPage(String)
}

public final class VersionedMap: @unchecked Sendable {
    let native: BindingVersionedMap
    private var closed = false

    init(native: BindingVersionedMap) { self.native = native }

    public func close() { closed = true }
    public var id: Data { native.id() }
    public func isInitialized() throws -> Bool { try open { try native.isInitialized() } }
    public func initialize() throws -> MapVersionRecord { try open { try native.initialize() } }
    public func head() throws -> MapVersionRecord? { try open { try native.head() } }
    public func headID() throws -> Data? { try open { try native.headId() } }
    public func version(_ id: Data) throws -> MapVersionRecord? {
        try open { try native.version(id: Data(id)) }
    }
    public func versions() throws -> [MapVersionRecord] { try open { try native.versions() } }
    public func get(_ key: Data) throws -> Data? { try open { try native.get(key: Data(key)) } }
    public func contains(_ key: Data) throws -> Bool {
        try open { try native.containsKey(key: Data(key)) }
    }
    public func getMany(_ keys: [Data]) throws -> [Data?] {
        try open { try native.getMany(keys: keys.map { Data($0) }) }
    }
    public func get(at id: Data, key: Data) throws -> Data? {
        try open { try native.getAt(id: Data(id), key: Data(key)) }
    }
    public func getMany(at id: Data, keys: [Data]) throws -> [Data?] {
        try open { try native.getManyAt(id: Data(id), keys: keys.map { Data($0) }) }
    }
    public func put(_ key: Data, value: Data) throws -> MapVersionRecord {
        try open { try native.put(key: Data(key), value: Data(value)) }
    }
    public func apply(_ mutations: [MutationRecord]) throws -> MapVersionRecord {
        try open { try native.apply(mutations: mutations.map(ownedMutation)) }
    }
    public func applyIf(expected: Data?, mutations: [MutationRecord]) throws -> MapUpdateRecord {
        try open {
            try native.applyIf(expected: expected.map { Data($0) }, mutations: mutations.map(ownedMutation))
        }
    }
    public func putIf(expected: Data?, key: Data, value: Data) throws -> MapUpdateRecord {
        try open {
            try native.putIf(expected: expected.map { Data($0) }, key: Data(key), value: Data(value))
        }
    }
    public func deleteIf(expected: Data?, key: Data) throws -> MapUpdateRecord {
        try open { try native.deleteIf(expected: expected.map { Data($0) }, key: Data(key)) }
    }
    public func delete(_ key: Data) throws -> MapVersionRecord {
        try open { try native.delete(key: Data(key)) }
    }
    public func snapshot() throws -> MapSnapshot? {
        try open { try native.snapshot().map(MapSnapshot.init(native:)) }
    }
    public func snapshot(at id: Data) throws -> MapSnapshot? {
        try open { try native.snapshotAt(id: Data(id)).map(MapSnapshot.init(native:)) }
    }
    public func backup() throws -> Data { try open { try native.backup() } }
    public func restoreBackup(_ bundle: Data) throws -> MapVersionRecord {
        try open { try native.restoreBackup(bytes: Data(bundle)) }
    }
    public func keepLast(_ count: UInt64) throws -> VersionPruneRecord {
        try open { try native.keepLast(count: count) }
    }
    public func verifyCatalog() throws -> MapCatalogVerificationRecord {
        try open { try native.verifyCatalog() }
    }
    public func planGC() throws -> GcPlanRecord { try open { try native.planGc() } }
    public func sweepGC() throws -> GcSweepRecord { try open { try native.sweepGc() } }

    public func putAsync(_ key: Data, value: Data) -> Task<MapVersionRecord, Error> {
        let copiedKey = Data(key)
        let copiedValue = Data(value)
        return Task {
            try Task.checkCancellation()
            return try self.put(copiedKey, value: copiedValue)
        }
    }

    private func open<R>(_ body: () throws -> R) throws -> R {
        if closed { throw PortableAPIError.closed("VersionedMap") }
        return try body()
    }
}

private func ownedMutation(_ mutation: MutationRecord) -> MutationRecord {
    MutationRecord(
        kind: mutation.kind,
        key: Data(mutation.key),
        value: mutation.value.map { Data($0) }
    )
}

public final class MapSnapshot: @unchecked Sendable {
    let native: BindingMapSnapshot
    private var closed = false
    init(native: BindingMapSnapshot) { self.native = native }
    public func close() { closed = true }
    public var id: Data { native.id() }
    public var version: MapVersionRecord { native.version() }
    public func get(_ key: Data) throws -> Data? { try open { try native.get(key: Data(key)) } }
    public func range(from start: Data = Data(), to end: Data? = nil) throws -> [EntryRecord] {
        try open { try native.range(start: Data(start), rangeEnd: end.map { Data($0) }) }
    }
    public func proveKey(_ key: Data) throws -> KeyProofRecord {
        try open { try native.proveKey(key: Data(key)) }
    }
    public func proveKeys(_ keys: [Data]) throws -> MultiKeyProofRecord {
        try open { try native.proveKeys(keys: keys.map { Data($0) }) }
    }
    public func proveRange(from start: Data = Data(), to end: Data? = nil) throws -> RangeProofRecord {
        try open { try native.proveRange(start: Data(start), rangeEnd: end.map { Data($0) }) }
    }
    public func stats() throws -> TreeStatsRecord { try open { try native.stats() } }
    public func export() throws -> SnapshotBundleRecord { try open { try native.export() } }
    public func read() throws -> ReadSession { try open { ReadSession(native: try native.readSession()) } }

    private func open<R>(_ body: () throws -> R) throws -> R {
        if closed { throw PortableAPIError.closed("MapSnapshot") }
        return try body()
    }
}

public final class ReadSession: @unchecked Sendable {
    let native: ProllyReadSession
    private var closed = false
    init(native: ProllyReadSession) { self.native = native }
    public func close() { closed = true }
    public func get(_ key: Data) throws -> Data? { try open { try native.get(key: Data(key)) } }
    public func getMany(_ keys: [Data]) throws -> [Data?] {
        try open { try native.getMany(keys: keys.map { Data($0) }) }
    }
    private func open<R>(_ body: () throws -> R) throws -> R {
        if closed { throw PortableAPIError.closed("ReadSession") }
        return try body()
    }
}

private final class ExtractorAdapter: SecondaryIndexExtractorCallback, @unchecked Sendable {
    let body: @Sendable (Data, Data) throws -> [IndexEntryRecord]
    init(body: @escaping @Sendable (Data, Data) throws -> [IndexEntryRecord]) { self.body = body }
    func extract(primaryKey: Data, sourceValue: Data) throws -> [IndexEntryRecord] {
        try body(primaryKey, sourceValue)
    }
}

public final class IndexRegistry: @unchecked Sendable {
    let native: BindingIndexRegistry
    private var extractors: [ExtractorAdapter] = []
    init(native: BindingIndexRegistry) { self.native = native }

    public func register(
        name: Data,
        generation: UInt64,
        extractorID: String,
        projection: IndexProjectionRecord,
        limits: SecondaryIndexLimitsRecord? = nil,
        extractor: @escaping @Sendable (Data, Data) throws -> [IndexEntryRecord]
    ) throws {
        let adapter = ExtractorAdapter(body: extractor)
        extractors.append(adapter)
        try native.register(
            name: Data(name), generation: generation, extractorId: extractorID,
            projection: projection, limits: limits, extractor: adapter
        )
    }
}

public final class IndexedMap: @unchecked Sendable {
    let native: BindingIndexedMap
    init(native: BindingIndexedMap) { self.native = native }
    public var id: Data { native.id() }
    public func ensureIndex(_ name: Data) throws -> IndexBuildResultRecord { try native.ensureIndex(name: Data(name)) }
    public func get(_ key: Data) throws -> Data? { try native.get(key: Data(key)) }
    public func put(_ key: Data, value: Data) throws -> IndexedVersionRecord {
        try native.put(key: Data(key), value: Data(value))
    }
    public func apply(_ mutations: [MutationRecord]) throws -> IndexedVersionRecord {
        try native.apply(mutations: mutations)
    }
    public func applyIf(expectedSource: Data?, mutations: [MutationRecord]) throws -> IndexedUpdateRecord {
        try native.applyIf(expectedSource: expectedSource.map { Data($0) }, mutations: mutations)
    }
    public func delete(_ key: Data) throws -> IndexedVersionRecord { try native.delete(key: Data(key)) }
    public func snapshot() throws -> IndexedSnapshot { IndexedSnapshot(native: try native.snapshot()) }
    public func snapshot(at sourceVersion: Data) throws -> IndexedSnapshot {
        IndexedSnapshot(native: try native.snapshotAt(sourceVersion: Data(sourceVersion)))
    }
    public func snapshot(id: IndexedSnapshotIdRecord) throws -> IndexedSnapshot {
        IndexedSnapshot(native: try native.snapshotById(snapshotId: id))
    }
    public func health() throws -> IndexedMapHealthRecord { try native.health() }
    public func metrics() throws -> IndexedMapMetricsRecord { try native.metrics() }
    public func verifyIndex(_ name: Data, sourceVersion: Data) throws -> IndexVerificationRecord {
        try native.verifyIndex(name: Data(name), sourceVersion: Data(sourceVersion))
    }
    public func verifyAll(sourceVersion: Data) throws -> [IndexVerificationRecord] {
        try native.verifyAll(sourceVersion: Data(sourceVersion))
    }
    public func repairIndex(_ name: Data, sourceVersion: Data) throws -> IndexVerificationRecord {
        try native.repairIndex(name: Data(name), sourceVersion: Data(sourceVersion))
    }
    public func deactivateIndex(_ name: Data) throws -> IndexedVersionRecord {
        try native.deactivateIndex(name: Data(name))
    }
    public func exportCurrent() throws -> Data { try native.exportCurrent() }
    public func importCurrent(_ bundle: Data, expectedSource: Data? = nil) throws -> IndexedVersionRecord {
        try native.importCurrent(bundle: Data(bundle), expectedSource: expectedSource.map { Data($0) })
    }
    public func keepLast(_ count: UInt64) throws -> IndexedRetentionRecord {
        try native.keepLast(count: count)
    }
}

public final class IndexedSnapshot: @unchecked Sendable {
    let native: BindingIndexedSnapshot
    init(native: BindingIndexedSnapshot) { self.native = native }
    public var id: IndexedSnapshotIdRecord { native.id() }
    public func index(_ name: Data) throws -> SecondaryIndex {
        SecondaryIndex(native: try native.index(name: Data(name)))
    }
}

public final class SecondaryIndex: @unchecked Sendable {
    let native: BindingSecondaryIndexSnapshot
    init(native: BindingSecondaryIndexSnapshot) { self.native = native }
    public var name: Data { native.name() }
    public func exact(_ term: Data) throws -> [IndexMatchRecord] { try native.exact(term: Data(term)) }
    public func prefix(_ prefix: Data) throws -> [IndexMatchRecord] { try native.prefix(prefix: Data(prefix)) }
    public func range(from start: Data, to end: Data? = nil) throws -> [IndexMatchRecord] {
        try native.range(start: Data(start), rangeEnd: end.map { Data($0) })
    }
    public func records(_ term: Data) throws -> [IndexedSourceRecord] { try native.records(term: Data(term)) }
    public func exactPage(_ term: Data, cursor: Data? = nil, limit: UInt64 = 256) throws -> IndexPageRecord {
        try native.exactPage(term: Data(term), cursor: cursor.map { Data($0) }, limit: limit)
    }
    public func exactReversePage(_ term: Data, cursor: Data? = nil, limit: UInt64 = 256) throws -> IndexPageRecord {
        try native.exactReversePage(term: Data(term), cursor: cursor.map { Data($0) }, limit: limit)
    }
    public func prefixPage(_ prefix: Data, cursor: Data? = nil, limit: UInt64 = 256) throws -> IndexPageRecord {
        try native.prefixPage(prefix: Data(prefix), cursor: cursor.map { Data($0) }, limit: limit)
    }
    public func prefixReversePage(_ prefix: Data, cursor: Data? = nil, limit: UInt64 = 256) throws -> IndexPageRecord {
        try native.prefixReversePage(prefix: Data(prefix), cursor: cursor.map { Data($0) }, limit: limit)
    }
    public func rangePage(from start: Data, to end: Data? = nil, cursor: Data? = nil, limit: UInt64 = 256) throws -> IndexPageRecord {
        try native.rangePage(start: Data(start), rangeEnd: end.map { Data($0) }, cursor: cursor.map { Data($0) }, limit: limit)
    }
    public func rangeReversePage(from start: Data, to end: Data? = nil, cursor: Data? = nil, limit: UInt64 = 256) throws -> IndexPageRecord {
        try native.rangeReversePage(start: Data(start), rangeEnd: end.map { Data($0) }, cursor: cursor.map { Data($0) }, limit: limit)
    }
}

private final class PageScope {
    var alive = true
}

public struct ScopedBytes: RandomAccessCollection {
    public typealias Index = Int
    public typealias Element = UInt8

    private let page: UnsafeRawBufferPointer
    private let offset: Int
    private let countValue: Int
    private let scope: PageScope

    fileprivate init(page: UnsafeRawBufferPointer, offset: Int, count: Int, scope: PageScope) {
        self.page = page
        self.offset = offset
        self.countValue = count
        self.scope = scope
    }

    public var startIndex: Int { 0 }
    public var endIndex: Int { countValue }
    public subscript(position: Int) -> UInt8 {
        precondition(scope.alive, "packed page view escaped its callback scope")
        precondition(position >= 0 && position < countValue)
        return page[offset + position]
    }
}

public struct NeighborView {
    public let key: ScopedBytes
    public let distance: Double
    public let rank: UInt32
    public let value: ScopedBytes?
    public let proof: ScopedBytes?
}

public final class ProximityMap: @unchecked Sendable {
    let native: BindingProximityMap
    init(native: BindingProximityMap) { self.native = native }

    public var descriptor: Data { native.descriptor() }
    public var count: UInt64 { get throws { try native.count() } }
    public var config: ProximityConfigRecord { get throws { try native.config() } }
    public func get(_ key: Data) throws -> ExactProximityRecordRecord? { try native.get(key: Data(key)) }
    public func contains(_ key: Data) throws -> Bool { try native.containsKey(key: Data(key)) }
    public func searchExact(_ query: [Float], k: UInt64) throws -> ProximitySearchResultRecord {
        let session = try read()
        defer { session.close() }
        return try session.searchExact(query, k: k)
    }
    public func read() throws -> ProximityReadSession { ProximityReadSession(native: try native.readSession()) }
    public func verify() throws -> ProximityVerificationRecord { try native.verify() }
    public func mutate(
        _ mutations: [ProximityMutationRecord]
    ) throws -> (map: ProximityMap, stats: ProximityMutationStatsRecord) {
        let result = try native.mutate(mutations: mutations)
        return (ProximityMap(native: result.map), result.stats)
    }
    public func rebuild(_ mutations: [ProximityMutationRecord]) throws -> ProximityMap {
        ProximityMap(native: try native.rebuild(mutations: mutations))
    }
    public func proveMembership(_ key: Data) throws -> ProximityMembershipProofRecord {
        try native.proveMembership(key: Data(key))
    }
    public func proveSearch(
        _ request: ProximitySearchRequestRecord,
        limits: ContentGraphLimitsRecord = defaultContentGraphLimits()
    ) throws -> ProximitySearchProof {
        ProximitySearchProof(native: try native.proveSearch(request: request, limits: limits))
    }
    public func proveSearchExact(
        _ query: [Float],
        k: UInt64,
        limits: ContentGraphLimitsRecord = defaultContentGraphLimits()
    ) throws -> ProximitySearchProof {
        try proveSearch(exactProximitySearchRequest(query: query, k: k), limits: limits)
    }
    public func proveStructure(
        limits: ContentGraphLimitsRecord = defaultContentGraphLimits()
    ) throws -> ProximityStructuralProofRecord {
        try native.proveStructure(limits: limits)
    }
    public func clearCache() throws { try native.clearContentCache() }

    public func withSearchView<R>(
        query: [Float],
        k: UInt32,
        _ body: ([NeighborView]) throws -> R
    ) throws -> R {
        let session = try read()
        defer { session.close() }
        return try session.withSearchView(query: query, k: k, body)
    }
}

private func withProximitySearchView<R>(
        handle: UInt64,
        query: [Float],
        k: UInt32,
        _ body: ([NeighborView]) throws -> R
    ) throws -> R {
        guard !query.isEmpty, k > 0 else { throw PortableAPIError.packedPage("query and k must be non-empty") }
        let result = query.withUnsafeBufferPointer { values in
            prolly_fast_proximity_search(handle, values.baseAddress, values.count, k, 64 * 1024 * 1024)
        }
        guard result.status == 0, let pointer = result.data_ptr else {
            throw PortableAPIError.packedPage("native search failed with status \(result.status)")
        }
        defer { prolly_fast_page_release(result.lease_handle) }

        let page = UnsafeRawBufferPointer(start: UnsafeRawPointer(pointer), count: Int(result.data_len))
        guard page.count >= 28,
              page[0] == 0x50, page[1] == 0x52, page[2] == 0x50, page[3] == 0x47,
              readUInt16(page, 4) == 2, readUInt16(page, 6) == 7 else {
            throw PortableAPIError.packedPage("invalid proximity page header")
        }
        let count = Int(readUInt32(page, 12))
        let tableBytes = Int(readUInt32(page, 16))
        let arenaBytes = Int(readUInt64(page, 20))
        guard tableBytes == count * 40, 28 + tableBytes + arenaBytes == page.count else {
            throw PortableAPIError.packedPage("invalid proximity page bounds")
        }

        let scope = PageScope()
        defer { scope.alive = false }
        let arenaStart = 28 + tableBytes
        var neighbors: [NeighborView] = []
        neighbors.reserveCapacity(count)
        for index in 0..<count {
            let base = 28 + index * 40
            let flags = readUInt32(page, base)
            let keyOffset = Int(readUInt32(page, base + 4))
            let keyLength = Int(readUInt32(page, base + 8))
            let valueOffset = Int(readUInt32(page, base + 24))
            let valueLength = Int(readUInt32(page, base + 28))
            let proofOffset = Int(readUInt32(page, base + 32))
            let proofLength = Int(readUInt32(page, base + 36))
            guard keyOffset + keyLength <= arenaBytes,
                  valueOffset + valueLength <= arenaBytes,
                  proofOffset + proofLength <= arenaBytes else {
                throw PortableAPIError.packedPage("neighbor field exceeds arena")
            }
            neighbors.append(NeighborView(
                key: ScopedBytes(page: page, offset: arenaStart + keyOffset, count: keyLength, scope: scope),
                distance: Double(bitPattern: readUInt64(page, base + 12)),
                rank: readUInt32(page, base + 20),
                value: flags & 1 == 0 ? nil : ScopedBytes(page: page, offset: arenaStart + valueOffset, count: valueLength, scope: scope),
                proof: flags & 2 == 0 ? nil : ScopedBytes(page: page, offset: arenaStart + proofOffset, count: proofLength, scope: scope)
            ))
        }
        return try body(neighbors)
}

public final class ProximityReadSession: @unchecked Sendable {
    let native: BindingProximityReadSession
    private var closed = false
    init(native: BindingProximityReadSession) { self.native = native }
    public func get(_ key: Data) throws -> ExactProximityRecordRecord? { try native.get(key: Data(key)) }
    public func contains(_ key: Data) throws -> Bool { try native.containsKey(key: Data(key)) }
    public func searchExact(_ query: [Float], k: UInt64) throws -> ProximitySearchResultRecord {
        guard !closed else { throw PortableAPIError.closed("proximity read session") }
        return try native.search(request: exactProximitySearchRequest(query: query, k: k))
    }
    public func withSearchView<R>(
        query: [Float],
        k: UInt32,
        _ body: ([NeighborView]) throws -> R
    ) throws -> R {
        guard !closed else { throw PortableAPIError.closed("proximity read session") }
        return try withProximitySearchView(handle: native.fastHandle(), query: query, k: k, body)
    }
    public func close() { closed = true }
}

public final class ProximitySearchProof: @unchecked Sendable {
    let native: BindingProximitySearchProof
    private var closed = false
    init(native: BindingProximitySearchProof) { self.native = native }
    public var sourceDescriptor: Data {
        precondition(!closed, "proximity search proof is closed")
        return native.sourceDescriptor()
    }
    public func verify(
        expectedDescriptor: Data? = nil,
        limits: ContentGraphLimitsRecord = defaultContentGraphLimits()
    ) throws -> ProximitySearchVerificationRecord {
        guard !closed else { throw PortableAPIError.closed("proximity search proof") }
        return try native.verify(
            expectedDescriptor: expectedDescriptor.map { Data($0) },
            limits: limits
        )
    }
    public func close() { closed = true }
}

public enum Proofs {
    public static func verify(_ proof: KeyProofRecord) throws -> KeyProofVerificationRecord {
        try verifyKeyProof(proof: proof)
    }

    public static func verify(
        _ proof: ProximityMembershipProofRecord,
        expectedDescriptor: Data? = nil
    ) throws -> ProximityMembershipVerificationRecord {
        try verifyProximityMembershipProof(
            proof: proof,
            expectedDescriptor: expectedDescriptor.map { Data($0) }
        )
    }

    public static func verify(
        _ proof: ProximityStructuralProofRecord,
        expectedDescriptor: Data? = nil,
        limits: ContentGraphLimitsRecord = defaultContentGraphLimits()
    ) throws -> ProximityStructuralVerificationRecord {
        try verifyProximityStructureProof(
            proof: proof,
            expectedDescriptor: expectedDescriptor.map { Data($0) },
            limits: limits
        )
    }
}

private func readUInt16(_ bytes: UnsafeRawBufferPointer, _ offset: Int) -> UInt16 {
    UInt16(littleEndian: bytes.loadUnaligned(fromByteOffset: offset, as: UInt16.self))
}

private func readUInt32(_ bytes: UnsafeRawBufferPointer, _ offset: Int) -> UInt32 {
    UInt32(littleEndian: bytes.loadUnaligned(fromByteOffset: offset, as: UInt32.self))
}

private func readUInt64(_ bytes: UnsafeRawBufferPointer, _ offset: Int) -> UInt64 {
    UInt64(littleEndian: bytes.loadUnaligned(fromByteOffset: offset, as: UInt64.self))
}
