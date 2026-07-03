import Foundation
import Prolly

struct FixtureError: Error, CustomStringConvertible {
    let description: String
}

func fail(_ message: String) throws -> Never {
    throw FixtureError(description: message)
}

func expect(_ condition: @autoclosure () -> Bool, _ message: String) throws {
    if !condition() {
        try fail(message)
    }
}

func expectThrows(_ message: String, _ operation: () throws -> Void) throws {
    do {
        try operation()
    } catch {
        return
    }
    try fail(message)
}

func hexData(_ hex: String) throws -> Data {
    if hex.count % 2 != 0 {
        try fail("invalid odd-length hex string")
    }
    var data = Data()
    var index = hex.startIndex
    while index < hex.endIndex {
        let next = hex.index(index, offsetBy: 2)
        guard let byte = UInt8(hex[index..<next], radix: 16) else {
            try fail("invalid hex byte")
        }
        data.append(byte)
        index = next
    }
    return data
}

func hexString(_ data: Data?) -> String? {
    data?.map { String(format: "%02x", $0) }.joined()
}

func dict(_ value: Any, _ context: String) throws -> [String: Any] {
    guard let result = value as? [String: Any] else {
        try fail("expected object for \(context)")
    }
    return result
}

func array(_ value: Any, _ context: String) throws -> [Any] {
    guard let result = value as? [Any] else {
        try fail("expected array for \(context)")
    }
    return result
}

func string(_ value: Any?, _ context: String) throws -> String {
    guard let result = value as? String else {
        try fail("expected string for \(context)")
    }
    return result
}

func optionalString(_ value: Any?) throws -> String? {
    if value == nil || value is NSNull {
        return nil
    }
    return try string(value, "optional string")
}

func uint64(_ value: Any?, _ context: String) throws -> UInt64 {
    if let number = value as? NSNumber {
        return number.uint64Value
    }
    if let string = value as? String, let number = UInt64(string) {
        return number
    }
    try fail("expected unsigned number for \(context)")
}

func int64(_ value: Any?, _ context: String) throws -> Int64 {
    if let number = value as? NSNumber {
        return number.int64Value
    }
    if let string = value as? String, let number = Int64(string) {
        return number
    }
    try fail("expected signed number for \(context)")
}

func bool(_ value: Any?, _ context: String) throws -> Bool {
    guard let result = value as? Bool else {
        try fail("expected bool for \(context)")
    }
    return result
}

func optionalUInt64(_ value: Any?) -> UInt64? {
    guard let number = value as? NSNumber else {
        return nil
    }
    return number.uint64Value
}

func optionalHexData(_ value: Any?) throws -> Data? {
    guard let hex = try optionalString(value) else {
        return nil
    }
    return try hexData(hex)
}

func fixtureURL() throws -> URL {
    let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
    let candidates = [
        cwd.appendingPathComponent("crates/prolly/conformance/prolly-fixtures.v1.json"),
        cwd.appendingPathComponent("../../conformance/prolly-fixtures.v1.json"),
        cwd.appendingPathComponent("../../../conformance/prolly-fixtures.v1.json"),
    ]
    for candidate in candidates where FileManager.default.fileExists(atPath: candidate.path) {
        return candidate
    }
    try fail("could not locate prolly-fixtures.v1.json")
}

func encodingRecord(_ raw: [String: Any]) throws -> EncodingRecord {
    let kind = try string(raw["kind"], "encoding.kind")
    let swiftKind: EncodingKind
    switch kind {
    case "raw":
        swiftKind = .raw
    case "cbor":
        swiftKind = .cbor
    case "json":
        swiftKind = .json
    case "custom":
        swiftKind = .custom
    default:
        try fail("unknown encoding kind \(kind)")
    }
    return EncodingRecord(kind: swiftKind, customName: try optionalString(raw["custom_name"]))
}

func configRecord(_ raw: [String: Any]) throws -> ConfigRecord {
    ConfigRecord(
        minChunkSize: try uint64(raw["min_chunk_size"], "min_chunk_size"),
        maxChunkSize: try uint64(raw["max_chunk_size"], "max_chunk_size"),
        chunkingFactor: UInt32(try uint64(raw["chunking_factor"], "chunking_factor")),
        hashSeed: try uint64(raw["hash_seed"], "hash_seed"),
        encoding: try encodingRecord(try dict(raw["encoding"] as Any, "encoding")),
        nodeCacheMaxNodes: optionalUInt64(raw["node_cache_max_nodes"]),
        nodeCacheMaxBytes: optionalUInt64(raw["node_cache_max_bytes"])
    )
}

func entryRecord(_ raw: [String: Any]) throws -> EntryRecord {
    EntryRecord(
        key: try hexData(try string(raw["key"], "entry.key")),
        value: try hexData(try string(raw["value"], "entry.value"))
    )
}

final class FixtureHostStore: HostStoreCallback, @unchecked Sendable {
    private var nodes: [Data: Data]

    init(store: [[String: Any]]) throws {
        var loaded: [Data: Data] = [:]
        for item in store {
            loaded[try hexData(try string(item["cid"], "store.cid"))] =
                try hexData(try string(item["bytes"], "store.bytes"))
        }
        nodes = loaded
    }

    func get(key: Data) -> HostStoreBytesResultRecord {
        HostStoreBytesResultRecord(value: nodes[key], error: nil)
    }

    func put(key: Data, value: Data) -> HostStoreUnitResultRecord {
        nodes[key] = value
        return HostStoreUnitResultRecord(error: nil)
    }

    func delete(key: Data) -> HostStoreUnitResultRecord {
        nodes.removeValue(forKey: key)
        return HostStoreUnitResultRecord(error: nil)
    }

    func batch(ops: [MutationRecord]) -> HostStoreUnitResultRecord {
        for op in ops {
            switch op.kind {
            case .upsert:
                nodes[op.key] = op.value ?? Data()
            case .delete:
                nodes.removeValue(forKey: op.key)
            }
        }
        return HostStoreUnitResultRecord(error: nil)
    }

    func batchGetOrdered(keys: [Data]) -> HostStoreBatchGetResultRecord {
        HostStoreBatchGetResultRecord(values: keys.map { nodes[$0] }, error: nil)
    }

    func prefersBatchReads() -> HostStoreBoolResultRecord {
        HostStoreBoolResultRecord(value: true, error: nil)
    }

    func supportsHints() -> HostStoreBoolResultRecord {
        HostStoreBoolResultRecord(value: false, error: nil)
    }

    func getHint(namespace: Data, key: Data) -> HostStoreBytesResultRecord {
        HostStoreBytesResultRecord(value: nil, error: nil)
    }

    func putHint(namespace: Data, key: Data, value: Data) -> HostStoreUnitResultRecord {
        HostStoreUnitResultRecord(error: nil)
    }

    func listNodeCids() -> HostStoreListBytesResultRecord {
        HostStoreListBytesResultRecord(values: nodes.keys.sorted { $0.lexicographicallyPrecedes($1) }, error: nil)
    }

    func getRoot(name: Data) -> HostStoreRootResultRecord {
        HostStoreRootResultRecord(value: nil, error: nil)
    }

    func putRoot(name: Data, manifest: RootManifestRecord) -> HostStoreUnitResultRecord {
        HostStoreUnitResultRecord(error: nil)
    }

    func deleteRoot(name: Data) -> HostStoreUnitResultRecord {
        HostStoreUnitResultRecord(error: nil)
    }

    func compareAndSwapRoot(name: Data, expected: RootManifestRecord?, replacement: RootManifestRecord?) -> HostStoreRootCasResultRecord {
        HostStoreRootCasResultRecord(applied: false, current: nil, error: nil)
    }

    func listRoots() -> HostStoreListRootsResultRecord {
        HostStoreListRootsResultRecord(values: [], error: nil)
    }
}

let fixtureData = try Data(contentsOf: fixtureURL())
let loaded = try JSONSerialization.jsonObject(with: fixtureData)
let root = try dict(loaded, "fixtures root")

for raw in try array(root["node_fixtures"] as Any, "node_fixtures") {
    let fixture = try dict(raw, "node fixture")
    let bytes = try hexData(try string(fixture["bytes"], "node.bytes"))
    let expectedCid = try hexData(try string(fixture["cid"], "node.cid"))
    let node = try nodeFromBytes(bytes: bytes)
    let encodedNode = try nodeToBytes(node: node)
    let actualNodeCid = try nodeCid(node: node)
    try expect(encodedNode == bytes, "node bytes did not round trip")
    try expect(actualNodeCid == expectedCid, "node CID mismatch")
    try expect(cidFromBytes(bytes: bytes) == expectedCid, "cid_from_bytes mismatch")
}

for raw in try array(root["boundary_fixtures"] as Any, "boundary_fixtures") {
    let fixture = try dict(raw, "boundary fixture")
    let config = try configRecord(try dict(fixture["config"] as Any, "boundary config"))
    let actual = try isBoundaryConfig(
        config: config,
        count: try uint64(fixture["count"], "boundary.count"),
        key: try hexData(try string(fixture["key"], "boundary.key")),
        value: try hexData(try string(fixture["value"], "boundary.value"))
    )
    let expected = try bool(fixture["is_boundary"], "boundary.is_boundary")
    try expect(actual == expected, "boundary mismatch")
}

let keys = try dict(root["key_fixtures"] as Any, "key_fixtures")
for raw in try array(keys["prefix_end"] as Any, "prefix_end") {
    let fixture = try dict(raw, "prefix fixture")
    let prefix = try hexData(try string(fixture["prefix"], "prefix"))
    let expected = try optionalHexData(fixture["end"])
    try expect(prefixEnd(prefix: prefix) == expected, "prefix_end mismatch")
    let bounds = prefixRange(prefix: prefix)
    try expect(bounds.start == prefix, "prefix_range start mismatch")
    try expect(bounds.end == expected, "prefix_range end mismatch")
}
for raw in try array(keys["numeric"] as Any, "numeric") {
    let fixture = try dict(raw, "numeric fixture")
    let kind = try string(fixture["kind"], "numeric.kind")
    let expected = try hexData(try string(fixture["encoded"], "numeric.encoded"))
    switch kind {
    case "u64", "timestamp_millis":
        let value = try uint64(fixture["value"], "numeric.value")
        try expect((kind == "u64" ? u64Key(value: value) : timestampMillisKey(value: value)) == expected, "numeric \(kind) mismatch")
    case "u128":
        let actual = try u128Key(value: string(fixture["value"], "numeric.value"))
        try expect(actual == expected, "numeric u128 mismatch")
    case "i64":
        let value = try int64(fixture["value"], "numeric.value")
        try expect(i64Key(value: value) == expected, "numeric i64 mismatch")
    case "i128":
        let actual = try i128Key(value: string(fixture["value"], "numeric.value"))
        try expect(actual == expected, "numeric i128 mismatch")
    default:
        break
    }
}
for raw in try array(keys["segments"] as Any, "segments") {
    let fixture = try dict(raw, "segment fixture")
    var encoded = Data()
    var segmentBytes: [Data] = []
    for segmentHex in try array(fixture["segments"] as Any, "segments") {
        let segment = try hexData(try string(segmentHex, "segment"))
        segmentBytes.append(segment)
        encoded.append(encodeSegment(segment: segment))
    }
    let expected = try hexData(try string(fixture["encoded"], "segments.encoded"))
    try expect(encoded == expected, "segment encoding mismatch")
    try expect(keyFromSegments(segments: segmentBytes) == expected, "key_from_segments mismatch")
    let prefix = keyFromSegments(segments: Array(segmentBytes.prefix(1)))
    try expect(
        keyFromPrefixedSegments(prefix: prefix, segments: Array(segmentBytes.dropFirst())) == expected,
        "key_from_prefixed_segments mismatch"
    )
    let decoded = try decodeSegments(key: expected).map(hexString)
    let expectedDecoded = try array(fixture["decoded"] as Any, "segments.decoded").map { try string($0, "decoded segment") }
    try expect(decoded == expectedDecoded, "segment decoding mismatch")
}
try expect(
    changedSpanFromKey(key: Data("k".utf8)).end == Data([0x6b, 0x00]),
    "changed span exact-key end mismatch"
)
try expect(
    changedSpanForPrefix(prefix: Data("k".utf8)).end == Data("l".utf8),
    "changed span prefix end mismatch"
)
try expect(
    changedSpan(start: Data("k".utf8), end: Data("l".utf8)).end == Data("l".utf8),
    "changed span range end mismatch"
)

for raw in try array(root["tree_fixtures"] as Any, "tree_fixtures") {
    let fixture = try dict(raw, "tree fixture")
    let config = try configRecord(try dict(fixture["config"] as Any, "tree config"))
    let engine = try ProllyEngine.memory(config: config)
    let entries = try array(fixture["entries"] as Any, "tree entries").map { try entryRecord(try dict($0, "tree entry")) }
    let tree = try engine.buildFromSortedEntries(entries: entries)
    let expectedRoot = try hexData(try string(fixture["root"], "tree.root"))
    try expect(tree.root == expectedRoot, "tree root mismatch")
    let lookups = try array(fixture["lookups"] as Any, "lookups")
    var proofKey: Data?
    var proofValue: Data?
    for lookupRaw in lookups {
        let lookup = try dict(lookupRaw, "lookup")
        let key = try hexData(try string(lookup["key"], "lookup.key"))
        let actual = try engine.get(tree: tree, key: key)
        let expected = try optionalHexData(lookup["value"])
        try expect(actual == expected, "lookup mismatch")
        if proofKey == nil, let expected {
            proofKey = key
            proofValue = expected
        }
    }
    if let proofKey, let proofValue {
        let proof = try engine.proveKey(tree: tree, key: proofKey)
        let verified = try verifyKeyProof(proof: proof)
        try expect(verified.valid, "key proof should be valid")
        try expect(verified.exists, "key proof should prove presence")
        try expect(!verified.absence, "present key proof should not be absence")
        try expect(verified.value == proofValue, "key proof value mismatch")

        let decodedProof = try keyProofFromNodeBytes(
            root: proof.root,
            key: proof.key,
            pathNodeBytes: try keyProofPathNodeBytes(proof: proof)
        )
        let decodedVerification = try verifyKeyProof(proof: decodedProof)
        try expect(decodedVerification.value == proofValue, "decoded key proof mismatch")
        let decodedProofFromBytes = try keyProofFromBytes(bytes: try keyProofToBytes(proof: proof))
        let decodedBundleVerification = try verifyKeyProof(proof: decodedProofFromBytes)
        try expect(decodedBundleVerification.value == proofValue, "bundled key proof mismatch")

        let rangeProof = try engine.proveRange(tree: tree, start: proofKey, end: nil)
        let rangeVerified = try verifyRangeProof(proof: rangeProof)
        try expect(rangeVerified.valid, "range proof should be valid")
        try expect(rangeVerified.entries.first?.key == proofKey, "range proof first key mismatch")
        let decodedRangeProof = try rangeProofFromNodeBytes(
            root: rangeProof.root,
            start: rangeProof.start,
            end: rangeProof.end,
            pathNodeBytes: try rangeProofPathNodeBytes(proof: rangeProof)
        )
        let decodedRangeVerification = try verifyRangeProof(proof: decodedRangeProof)
        try expect(decodedRangeVerification.entries.first?.key == proofKey, "decoded range proof mismatch")
        let decodedRangeProofFromBytes = try rangeProofFromBytes(bytes: try rangeProofToBytes(proof: rangeProof))
        let decodedRangeBundleVerification = try verifyRangeProof(proof: decodedRangeProofFromBytes)
        try expect(decodedRangeBundleVerification.entries.first?.key == proofKey, "bundled range proof mismatch")

        let prefixProof = try engine.provePrefix(tree: tree, prefix: Data(proofKey.prefix(1)))
        let prefixVerified = try verifyRangeProof(proof: prefixProof)
        try expect(prefixVerified.valid, "prefix proof should be valid")
        try expect(prefixVerified.entries.contains(where: { $0.key == proofKey }), "prefix proof should include proof key")

        let absentProof = try engine.proveKey(tree: tree, key: Data("definitely-missing".utf8))
        let absent = try verifyKeyProof(proof: absentProof)
        try expect(absent.valid, "absence proof should be valid")
        try expect(!absent.exists, "absence proof should not prove presence")
        try expect(absent.absence, "absence proof should prove absence")

        if var tamperedRoot = proof.root {
            tamperedRoot[tamperedRoot.startIndex] ^= 0x01
            let tampered = KeyProofRecord(root: tamperedRoot, key: proof.key, path: proof.path)
            let tamperedVerification = try verifyKeyProof(proof: tampered)
            try expect(!tamperedVerification.valid, "tampered key proof should be invalid")
        }
    }
    for rangeRaw in try array(fixture["ranges"] as Any, "ranges") {
        let range = try dict(rangeRaw, "range")
        let actual = try engine.range(
            tree: tree,
            start: try hexData(try string(range["start"], "range.start")),
            end: try optionalHexData(range["end"])
        )
        let expected = try array(range["entries"] as Any, "range.entries").map { try entryRecord(try dict($0, "range entry")) }
        try expect(actual == expected, "range mismatch")
    }
}

for raw in try array(root["diff_fixtures"] as Any, "diff_fixtures") {
    let fixture = try dict(raw, "diff fixture")
    let config = try configRecord(try dict(fixture["config"] as Any, "diff config"))
    let store = try FixtureHostStore(store: try array(fixture["store"] as Any, "diff store").map { try dict($0, "store item") })
    let engine = try ProllyEngine.customStore(callback: store, config: config)
    let base = TreeRecord(root: try hexData(try string(fixture["base_root"], "base_root")), config: config)
    let other = TreeRecord(root: try hexData(try string(fixture["other_root"], "other_root")), config: config)
    let actual = try engine.diff(base: base, other: other)
    let expected = try array(fixture["diffs"] as Any, "diffs")
    try expect(actual.count == expected.count, "diff count mismatch")
    for (index, expectedRaw) in expected.enumerated() {
        let expectedDiff = try dict(expectedRaw, "diff")
        let expectedKey = try string(expectedDiff["key"], "diff.key")
        let expectedValue = try optionalString(expectedDiff["value"])
        let expectedOld = try optionalString(expectedDiff["old"])
        let expectedNew = try optionalString(expectedDiff["new"])
        try expect(hexString(actual[index].key) == expectedKey, "diff key mismatch")
        try expect(hexString(actual[index].value) == expectedValue, "diff value mismatch")
        try expect(hexString(actual[index].oldValue) == expectedOld, "diff old mismatch")
        try expect(hexString(actual[index].newValue) == expectedNew, "diff new mismatch")
    }
}

for raw in try array(root["value_fixtures"] as Any, "value_fixtures") {
    let fixture = try dict(raw, "value fixture")
    let schema = try string(fixture["schema_name"], "schema_name")
    let version = try uint64(fixture["version"], "version")
    let record = try VersionedValueRecord(
        schema: schema,
        version: version,
        encoding: encodingRecord(try dict(fixture["encoding"] as Any, "value encoding")),
        payload: hexData(try string(fixture["payload"], "payload"))
    )
    let bytes = try hexData(try string(fixture["bytes"], "value bytes"))
    let encoded = try versionedValueToBytes(record: record)
    let decoded = try versionedValueFromBytes(bytes: bytes)
    try expect(encoded == bytes, "versioned value bytes mismatch")
    try expect(decoded == record, "versioned value decode mismatch")
    let recordMatches = try versionedValueMatchesSchema(record: record, schema: schema, version: version)
    let recordMismatches = try versionedValueMatchesSchema(record: record, schema: schema, version: version + 1)
    try expect(recordMatches, "versioned value schema mismatch")
    try expect(!recordMismatches, "versioned value unexpected schema match")
    try versionedValueRequireSchema(record: record, schema: schema, version: version)
    try expectThrows("versioned value record schema guard did not fail") {
        try versionedValueRequireSchema(record: record, schema: schema, version: version + 1)
    }
    let bytesMatch = try versionedValueBytesMatchesSchema(bytes: bytes, schema: schema, version: version)
    let bytesMismatch = try versionedValueBytesMatchesSchema(bytes: bytes, schema: schema, version: version + 1)
    try expect(bytesMatch, "versioned value bytes schema mismatch")
    try expect(!bytesMismatch, "versioned value bytes unexpected schema match")
    try versionedValueBytesRequireSchema(bytes: bytes, schema: schema, version: version)
    try expectThrows("versioned value bytes schema guard did not fail") {
        try versionedValueBytesRequireSchema(bytes: bytes, schema: schema, version: version + 1)
    }
}

for raw in try array(root["blob_fixtures"] as Any, "blob_fixtures") {
    let fixture = try dict(raw, "blob fixture")
    let kind = try string(fixture["kind"], "blob.kind")
    let record: ValueRefRecord
    if kind == "inline" {
        record = ValueRefRecord(kind: .inline, value: try hexData(try string(fixture["value"], "blob.value")), blob: nil)
    } else {
        record = ValueRefRecord(
            kind: .blob,
            value: nil,
            blob: BlobRefRecord(
                cid: try hexData(try string(fixture["cid"], "blob.cid")),
                len: try uint64(fixture["len"], "blob.len")
            )
        )
    }
    let bytes = try hexData(try string(fixture["bytes"], "blob.bytes"))
    let encoded = try valueRefToBytes(record: record)
    let decoded = try valueRefFromBytes(bytes: bytes)
    try expect(encoded == bytes, "value ref bytes mismatch")
    try expect(decoded == record, "value ref decode mismatch")
    let stored = try valueRefFromStoredBytes(bytes: bytes)
    try expect(stored == record, "stored value ref decode mismatch")
    try expect(valueRefInlineRequiresEscape(value: bytes), "value ref envelope should require inline escaping")
}
let rawValueRef = try valueRefFromStoredBytes(bytes: Data("plain".utf8))
try expect(rawValueRef.kind == .inline, "raw stored value ref kind mismatch")
try expect(rawValueRef.value == Data("plain".utf8), "raw stored value ref payload mismatch")
try expect(!valueRefInlineRequiresEscape(value: Data("plain".utf8)), "plain inline value should not require escaping")

let fixtureBlobStore = ProllyBlobStore.memory()
let fixtureInitialBlobCount = try fixtureBlobStore.blobCount()
try expect(fixtureInitialBlobCount == 0, "fixture blob store should start empty")
let fixtureDirectBlob = Data("direct".utf8)
let fixtureDirectBlobRef = try fixtureBlobStore.putBlob(bytes: fixtureDirectBlob)
try blobRefValidateBytes(reference: fixtureDirectBlobRef, bytes: fixtureDirectBlob)
try expectThrows("blob ref validation did not fail") {
    try blobRefValidateBytes(reference: fixtureDirectBlobRef, bytes: Data("wrong".utf8))
}
let fixtureLoadedBlob = try fixtureBlobStore.getBlob(reference: fixtureDirectBlobRef)
try expect(fixtureLoadedBlob == fixtureDirectBlob, "direct blob mismatch")
try fixtureBlobStore.deleteBlob(reference: fixtureDirectBlobRef)
let fixtureFinalBlobCount = try fixtureBlobStore.blobCount()
try expect(fixtureFinalBlobCount == 0, "fixture blob store should be empty after delete")

for raw in try array(root["manifest_fixtures"] as Any, "manifest_fixtures") {
    let fixture = try dict(raw, "manifest fixture")
    let config = try configRecord(try dict(fixture["config"] as Any, "manifest config"))
    let record = RootManifestRecord(
        tree: TreeRecord(root: try hexData(try string(fixture["root"], "manifest.root")), config: config),
        createdAtMillis: optionalUInt64(fixture["created_at_millis"]),
        updatedAtMillis: optionalUInt64(fixture["updated_at_millis"])
    )
    let bytes = try hexData(try string(fixture["bytes"], "manifest.bytes"))
    let encoded = try rootManifestToBytes(record: record)
    let decoded = try rootManifestFromBytes(bytes: bytes)
    try expect(encoded == bytes, "root manifest bytes mismatch")
    try expect(decoded == record, "root manifest decode mismatch")
}

let parityEngine = try ProllyEngine.memory(config: defaultConfig())
let emptyTree = parityEngine.create()
let builtTree = try parityEngine.buildFromSortedEntries(entries: [
    EntryRecord(key: Data("a".utf8), value: Data("1".utf8)),
    EntryRecord(key: Data("b".utf8), value: Data("2".utf8)),
    EntryRecord(key: Data("c".utf8), value: Data("3".utf8)),
])
let parityStats = try parityEngine.collectStats(tree: builtTree)
try expect(parityStats.totalKeyValuePairs == 3, "typed stats total key/value count mismatch")
try expect(parityStats.nodesPerLevel.contains { $0.level == 0 && $0.value > 0 }, "typed stats level count mismatch")
let parityStatsDiff = try parityEngine.statsDiff(before: emptyTree, after: builtTree)
try expect(parityStatsDiff.before.totalKeyValuePairs == 0, "typed stats diff before count mismatch")
try expect(parityStatsDiff.after.totalKeyValuePairs == 3, "typed stats diff after count mismatch")
try expect(parityStatsDiff.absolute.totalKeyValuePairsDiff == 3, "typed stats diff absolute count mismatch")
let parityDebugTree = try parityEngine.debugTree(tree: builtTree)
try expect(!parityDebugTree.levels.isEmpty, "typed debug tree levels missing")
try expect(parityDebugTree.levels.contains { !$0.nodes.isEmpty }, "typed debug tree nodes missing")
let parityDebugComparison = try parityEngine.debugCompareTrees(left: emptyTree, right: builtTree)
try expect(parityDebugComparison.leftOnlyNodes == 0, "typed debug comparison left-only count mismatch")
try expect(parityDebugComparison.rightOnlyNodes > 0, "typed debug comparison right-only count mismatch")
try expect(
    parityDebugComparison.levels.contains { level in level.nodes.contains { $0.status == .rightOnly } },
    "typed debug comparison right-only node missing"
)
let snapshotBundle = try parityEngine.exportSnapshot(tree: builtTree)
try expect(snapshotBundle.formatVersion == 1, "snapshot bundle format version mismatch")
try expect(!snapshotBundle.nodes.isEmpty, "snapshot bundle nodes missing")
let snapshotBundleBytes = try snapshotBundleToBytes(record: snapshotBundle)
let bundleDigest = try snapshotBundleDigest(record: snapshotBundle)
try expect(bundleDigest == cidFromBytes(bytes: snapshotBundleBytes), "snapshot bundle digest mismatch")
let byteSnapshotBundleDigest = try snapshotBundleDigestBytes(bytes: snapshotBundleBytes)
try expect(byteSnapshotBundleDigest == bundleDigest, "snapshot bundle byte digest mismatch")
let snapshotSummary = try snapshotBundleSummary(record: snapshotBundle)
try expect(snapshotSummary.formatVersion == 1, "snapshot bundle summary format mismatch")
try expect(snapshotSummary.nodeCount == UInt64(snapshotBundle.nodes.count), "snapshot bundle summary node count mismatch")
try expect(snapshotSummary.byteCount > 0, "snapshot bundle summary bytes missing")
let byteSnapshotSummary = try snapshotBundleSummaryFromBytes(bytes: snapshotBundleBytes)
try expect(byteSnapshotSummary == snapshotSummary, "snapshot bundle byte summary mismatch")
let snapshotVerification = try verifySnapshotBundle(record: snapshotBundle)
try expect(snapshotVerification.valid, "snapshot bundle verification failed")
try expect(snapshotVerification.summary == snapshotSummary, "snapshot bundle verification summary mismatch")
try expect(snapshotVerification.missingCids.isEmpty, "snapshot bundle verification reported missing CIDs")
try expect(snapshotVerification.extraCids.isEmpty, "snapshot bundle verification reported extra CIDs")
let byteSnapshotVerification = try verifySnapshotBundleBytes(bytes: snapshotBundleBytes)
try expect(byteSnapshotVerification == snapshotVerification, "snapshot bundle byte verification mismatch")
let incompleteSnapshotBundle = SnapshotBundleRecord(
    formatVersion: snapshotBundle.formatVersion,
    tree: snapshotBundle.tree,
    nodes: Array(snapshotBundle.nodes.dropLast())
)
let incompleteSnapshotVerification = try verifySnapshotBundle(record: incompleteSnapshotBundle)
try expect(!incompleteSnapshotVerification.valid, "incomplete snapshot bundle should be invalid")
try expect(
    !incompleteSnapshotVerification.missingCids.isEmpty,
    "incomplete snapshot bundle should report missing CIDs"
)
let decodedSnapshotBundle = try snapshotBundleFromBytes(bytes: snapshotBundleBytes)
let snapshotDestination = try ProllyEngine.memory(config: defaultConfig())
let importedSnapshotTree = try snapshotDestination.importSnapshot(bundle: decodedSnapshotBundle)
let importedSnapshotValue = try snapshotDestination.get(tree: importedSnapshotTree, key: Data("b".utf8))
try expect(
    importedSnapshotValue == Data("2".utf8),
    "snapshot import value mismatch"
)
try parityEngine.publishNamedRootAtMillis(name: Data("main".utf8), tree: builtTree, timestampMillis: 42)
let loadedNamedRoot = try parityEngine.loadNamedRoot(name: Data("main".utf8))
try expect(loadedNamedRoot != nil, "named root should load")
let namedRootManifests = try parityEngine.listNamedRootManifests()
try expect(namedRootManifests.count == 1, "named root manifest count mismatch")
try expect(namedRootManifests[0].name == Data("main".utf8), "named root manifest name mismatch")
try expect(namedRootManifests[0].manifest.tree.root == builtTree.root, "named root manifest tree mismatch")
try expect(namedRootManifests[0].manifest.createdAtMillis == 42, "named root manifest created timestamp mismatch")
try expect(namedRootManifests[0].manifest.updatedAtMillis == 42, "named root manifest updated timestamp mismatch")
let retainAllRoots = retainAllNamedRoots()
try expect(retainAllRoots.kind == .all, "retain-all policy mismatch")
let retainExactRoots = retainExactNamedRoots(names: [Data("main".utf8), Data("missing".utf8)])
try expect(retainExactRoots.kind == .exact, "retain-exact policy mismatch")
try expect(retainExactRoots.names.count == 2, "retain-exact names mismatch")
let retainPrefixRoots = retainNamedRootPrefix(prefix: Data("ma".utf8))
try expect(retainPrefixRoots.kind == .prefix, "retain-prefix policy mismatch")
try expect(retainPrefixRoots.prefix == Data("ma".utf8), "retain-prefix bytes mismatch")
let retainNewestRoots = retainNewestNamedRoots(prefix: Data("checkpoint/".utf8), count: 2)
try expect(retainNewestRoots.kind == .newestByName, "retain-newest policy mismatch")
try expect(retainNewestRoots.prefix == Data("checkpoint/".utf8), "retain-newest prefix mismatch")
try expect(retainNewestRoots.count == 2, "retain-newest count mismatch")
let retainUpdatedRoots = retainNamedRootsUpdatedSince(
    prefix: Data("checkpoint/".utf8),
    minUpdatedAtMillis: 42
)
try expect(retainUpdatedRoots.kind == .updatedSince, "retain-updated policy mismatch")
try expect(retainUpdatedRoots.prefix == Data("checkpoint/".utf8), "retain-updated prefix mismatch")
try expect(retainUpdatedRoots.minUpdatedAtMillis == 42, "retain-updated timestamp mismatch")
let retainedRoots = try parityEngine.loadRetainedNamedRoots(retention: retainAllRoots)
try expect(retainedRoots.roots.count == 1, "retained roots mismatch")
let retainedGcPlan = try parityEngine.planStoreGcForRetention(retention: retainAllRoots)
try expect(retainedGcPlan.reachability.liveNodes == 1, "retained GC plan live nodes mismatch")
let retainedGcSweep = try parityEngine.sweepStoreGcForRetention(retention: retainAllRoots)
try expect(retainedGcSweep.plan.reachability.liveNodes == 1, "retained GC sweep live nodes mismatch")
try expect(resolutionValue(value: Data("v".utf8)).kind == .value, "resolution value helper mismatch")
try expect(resolutionDelete().kind == .delete, "resolution delete helper mismatch")
try expect(resolutionUnresolved().kind == .unresolved, "resolution unresolved helper mismatch")
let updateConflict = ConflictRecord(
    key: Data("k".utf8),
    base: Data("base".utf8),
    left: Data("left".utf8),
    right: Data("right".utf8)
)
try expect(resolvePreferLeft(conflict: updateConflict).value == Data("left".utf8), "prefer-left helper mismatch")
try expect(resolveDeleteWins(conflict: updateConflict).kind == .unresolved, "delete-wins update/update helper mismatch")
let deleteConflict = ConflictRecord(
    key: Data("k".utf8),
    base: Data("base".utf8),
    left: nil,
    right: Data("right".utf8)
)
try expect(resolveDeleteWins(conflict: deleteConflict).kind == .delete, "delete-wins helper mismatch")
try expect(resolveUpdateWins(conflict: deleteConflict).value == Data("right".utf8), "update-wins helper mismatch")
try expect(crdtResolutionValue(value: Data("v".utf8)).kind == .value, "CRDT resolution value helper mismatch")
try expect(crdtResolutionDelete().kind == .delete, "CRDT resolution delete helper mismatch")
let parityKeyProof = try parityEngine.proveKey(tree: builtTree, key: Data("a".utf8))
let parityKeyBundle = try keyProofToBytes(proof: parityKeyProof)
let parityKeySummary = try inspectProofBundle(bytes: parityKeyBundle)
try expect(parityKeySummary.kind == "key", "proof bundle summary key kind mismatch")
try expect(parityKeySummary.root == builtTree.root, "proof bundle summary key root mismatch")
try expect(parityKeySummary.keyCount == 1, "proof bundle summary key count mismatch")
try expect(parityKeySummary.pathNodeCount == UInt64(parityKeyProof.path.count), "proof bundle summary path count mismatch")
let parityKeyBundleVerified = try verifyProofBundle(bytes: parityKeyBundle)
try expect(parityKeyBundleVerified.valid, "proof bundle key verification should be valid")
try expect(parityKeyBundleVerified.summary.kind == "key", "proof bundle key verification kind mismatch")
try expect(parityKeyBundleVerified.existsCount == 1, "proof bundle key verification exists count mismatch")
try expect(parityKeyBundleVerified.absenceCount == 0, "proof bundle key verification absence count mismatch")
let multiProof = try parityEngine.proveKeys(
    tree: builtTree,
    keys: [Data("a".utf8), Data("missing".utf8), Data("b".utf8)]
)
let multiVerified = try verifyMultiKeyProof(proof: multiProof)
try expect(multiVerified.valid, "multi-key proof should be valid")
try expect(multiVerified.results.count == 3, "multi-key proof result count mismatch")
try expect(multiVerified.results[0].value == Data("1".utf8), "multi-key proof first value mismatch")
try expect(multiVerified.results[1].absence, "multi-key proof absence mismatch")
try expect(multiVerified.results[2].value == Data("2".utf8), "multi-key proof third value mismatch")
let decodedMultiProof = try multiKeyProofFromNodeBytes(
    root: multiProof.root,
    keys: multiProof.keys,
    pathNodeBytes: try multiKeyProofPathNodeBytes(proof: multiProof)
)
let decodedMultiVerified = try verifyMultiKeyProof(proof: decodedMultiProof)
try expect(decodedMultiVerified.results[2].value == Data("2".utf8), "decoded multi-key proof mismatch")
let decodedMultiProofFromBytes = try multiKeyProofFromBytes(bytes: try multiKeyProofToBytes(proof: multiProof))
let decodedMultiBundleVerified = try verifyMultiKeyProof(proof: decodedMultiProofFromBytes)
try expect(decodedMultiBundleVerified.results[2].value == Data("2".utf8), "bundled multi-key proof mismatch")
let parityRangeProof = try parityEngine.proveRange(tree: builtTree, start: Data("a".utf8), end: Data("c".utf8))
let parityRangeVerified = try verifyRangeProof(proof: parityRangeProof)
try expect(parityRangeVerified.valid, "parity range proof should be valid")
try expect(parityRangeVerified.entries.count == 2, "parity range proof count mismatch")
try expect(parityRangeVerified.entries[1].value == Data("2".utf8), "parity range proof value mismatch")
let parityRangeDecoded = try rangeProofFromBytes(bytes: try rangeProofToBytes(proof: parityRangeProof))
let parityRangeDecodedVerification = try verifyRangeProof(proof: parityRangeDecoded)
try expect(parityRangeDecodedVerification.entries[1].value == Data("2".utf8), "parity bundled range proof mismatch")
let parityPrefixProof = try parityEngine.provePrefix(tree: builtTree, prefix: Data("a".utf8))
let parityPrefixVerified = try verifyRangeProof(proof: parityPrefixProof)
try expect(parityPrefixVerified.valid, "parity prefix proof should be valid")
try expect(parityPrefixVerified.entries.count == 1, "parity prefix proof count mismatch")
try expect(parityPrefixVerified.entries[0].value == Data("1".utf8), "parity prefix proof value mismatch")
try expect(rangeCursorStart().afterKey == nil, "range cursor start mismatch")
let parityAfterACursor = rangeCursorAfterKey(key: Data("a".utf8))
try expect(parityAfterACursor.afterKey == Data("a".utf8), "range cursor after-key mismatch")
let parityProvedPage = try parityEngine.proveRangePage(
    tree: builtTree,
    cursor: parityAfterACursor,
    end: nil,
    limit: 1
)
let parityPageVerified = try verifyRangePageProof(proof: parityProvedPage.proof)
try expect(parityPageVerified.valid, "parity range page proof should be valid")
try expect(parityPageVerified.entries.count == 1, "parity range page proof count mismatch")
try expect(parityPageVerified.entries[0].key == Data("b".utf8), "parity range page proof key mismatch")
let parityPageDecoded = try rangePageProofFromNodeBytes(
    root: parityProvedPage.proof.root,
    after: parityProvedPage.proof.after,
    end: parityProvedPage.proof.end,
    pathNodeBytes: try rangePageProofPathNodeBytes(proof: parityProvedPage.proof)
)
let parityPageDecodedVerification = try verifyRangePageProof(proof: parityPageDecoded)
try expect(parityPageDecodedVerification.entries[0].key == Data("b".utf8), "decoded range page proof mismatch")
let parityPageDecodedFromBytes = try rangePageProofFromBytes(bytes: try rangePageProofToBytes(proof: parityProvedPage.proof))
let parityPageDecodedFromBytesVerification = try verifyRangePageProof(proof: parityPageDecodedFromBytes)
try expect(parityPageDecodedFromBytesVerification.entries[0].key == Data("b".utf8), "bundled range page proof mismatch")
let parityChangedForCursor = try parityEngine.batch(tree: builtTree, mutations: [
    MutationRecord(kind: .upsert, key: Data("b".utf8), value: Data("22".utf8)),
    MutationRecord(kind: .upsert, key: Data("c".utf8), value: Data("33".utf8)),
])
let parityResumedDiffs = try parityEngine.diffFromCursor(
    base: builtTree,
    other: parityChangedForCursor,
    cursor: parityAfterACursor,
    end: Data("c".utf8)
)
try expect(parityResumedDiffs.count == 1, "diff_from_cursor count mismatch")
try expect(parityResumedDiffs[0].kind == .changed, "diff_from_cursor kind mismatch")
try expect(parityResumedDiffs[0].key == Data("b".utf8), "diff_from_cursor key mismatch")
var parityDiffOther = try parityEngine.delete(tree: builtTree, key: Data("a".utf8))
parityDiffOther = try parityEngine.put(tree: parityDiffOther, key: Data("b".utf8), value: Data("22".utf8))
parityDiffOther = try parityEngine.put(tree: parityDiffOther, key: Data("d".utf8), value: Data("4".utf8))
let parityProvedDiffPage = try parityEngine.proveDiffPage(
    base: builtTree,
    other: parityDiffOther,
    cursor: nil,
    end: nil,
    limit: 1
)
try expect(parityProvedDiffPage.page.diffs.count == 1, "parity diff page count mismatch")
try expect(parityProvedDiffPage.page.diffs[0].kind == .removed, "parity diff page kind mismatch")
try expect(parityProvedDiffPage.page.diffs[0].key == Data("a".utf8), "parity diff page key mismatch")
try expect(parityProvedDiffPage.page.diffs[0].value == Data("1".utf8), "parity diff page value mismatch")
try expect(parityProvedDiffPage.page.nextCursor?.afterKey == Data("a".utf8), "parity diff page cursor mismatch")
try expect(parityProvedDiffPage.proof.lookaheadBase?.key == Data("b".utf8), "parity diff page base lookahead mismatch")
try expect(parityProvedDiffPage.proof.lookaheadOther?.key == Data("b".utf8), "parity diff page other lookahead mismatch")
let parityDiffPageVerified = try verifyDiffPageProof(proof: parityProvedDiffPage.proof)
try expect(parityDiffPageVerified.valid, "parity diff page proof should be valid")
try expect(parityDiffPageVerified.baseValid, "parity diff page base proof should be valid")
try expect(parityDiffPageVerified.otherValid, "parity diff page other proof should be valid")
try expect(parityDiffPageVerified.lookaheadValid, "parity diff page lookahead should be valid")
try expect(parityDiffPageVerified.limit == 1, "parity diff page proof limit mismatch")
try expect(parityDiffPageVerified.diffs.count == 1, "parity diff page proof diff count mismatch")
try expect(parityDiffPageVerified.diffs[0].kind == .removed, "parity diff page proof kind mismatch")
try expect(parityDiffPageVerified.diffs[0].key == Data("a".utf8), "parity diff page proof key mismatch")
try expect(parityDiffPageVerified.diffs[0].value == Data("1".utf8), "parity diff page proof value mismatch")
try expect(parityDiffPageVerified.nextCursor?.afterKey == Data("a".utf8), "parity diff page proof cursor mismatch")
let parityDiffPageProofBytes = try diffPageProofToBytes(proof: parityProvedDiffPage.proof)
let parityDiffPageProofBytesAgain = try diffPageProofToBytes(proof: parityProvedDiffPage.proof)
try expect(parityDiffPageProofBytes == parityDiffPageProofBytesAgain, "diff page proof bytes should be deterministic")
let parityDiffPageSummary = try inspectProofBundle(bytes: parityDiffPageProofBytes)
try expect(parityDiffPageSummary.kind == "diff_page", "proof bundle summary diff kind mismatch")
try expect(parityDiffPageSummary.root == builtTree.root, "proof bundle summary diff base root mismatch")
try expect(parityDiffPageSummary.otherRoot == parityDiffOther.root, "proof bundle summary diff other root mismatch")
try expect(parityDiffPageSummary.limit == 1, "proof bundle summary diff limit mismatch")
try expect(parityDiffPageSummary.hasLookahead, "proof bundle summary diff lookahead mismatch")
let parityDiffPageBundleVerified = try verifyProofBundle(bytes: parityDiffPageProofBytes)
try expect(parityDiffPageBundleVerified.valid, "proof bundle diff verification should be valid")
try expect(parityDiffPageBundleVerified.summary.kind == "diff_page", "proof bundle diff verification kind mismatch")
try expect(parityDiffPageBundleVerified.diffCount == 1, "proof bundle diff verification count mismatch")
try expect(parityDiffPageBundleVerified.nextCursor?.afterKey == Data("a".utf8), "proof bundle diff verification cursor mismatch")
let parityDecodedDiffPageProof = try diffPageProofFromBytes(bytes: parityDiffPageProofBytes)
let parityDecodedDiffPageVerification = try verifyDiffPageProof(proof: parityDecodedDiffPageProof)
try expect(parityDecodedDiffPageVerification.valid, "bundled diff page proof should be valid")
try expect(parityDecodedDiffPageVerification.diffs[0].key == Data("a".utf8), "bundled diff page proof mismatch")
let paritySignedEnvelope = try signProofBundleHmacSha256(
    proofBundle: parityKeyBundle,
    keyId: Data("swift-key".utf8),
    secret: Data("shared secret".utf8),
    context: Data("tenant=t1".utf8),
    issuedAtMillis: 1_700_000_000_000,
    expiresAtMillis: 1_700_000_100_000,
    nonce: Data("nonce-1".utf8)
)
let paritySignedEnvelopeBytes = try authenticatedProofEnvelopeToBytes(envelope: paritySignedEnvelope)
let paritySignedEnvelopeBytesAgain = try authenticatedProofEnvelopeToBytes(envelope: paritySignedEnvelope)
try expect(
    paritySignedEnvelopeBytes == paritySignedEnvelopeBytesAgain,
    "authenticated proof envelope bytes should be deterministic"
)
let parityDecodedEnvelope = try authenticatedProofEnvelopeFromBytes(bytes: paritySignedEnvelopeBytes)
let parityEnvelopeVerified = verifyAuthenticatedProofEnvelope(
    envelope: parityDecodedEnvelope,
    secret: Data("shared secret".utf8),
    nowMillis: 1_700_000_050_000
)
try expect(parityEnvelopeVerified.valid, "authenticated proof envelope should verify")
try expect(parityEnvelopeVerified.signatureValid, "authenticated proof envelope signature should verify")
try expect(parityEnvelopeVerified.keyId == Data("swift-key".utf8), "authenticated proof envelope key id mismatch")
try expect(parityEnvelopeVerified.context == Data("tenant=t1".utf8), "authenticated proof envelope context mismatch")
let paritySignedProof = try keyProofFromBytes(bytes: parityEnvelopeVerified.proofBundle)
let paritySignedProofVerified = try verifyKeyProof(proof: paritySignedProof)
try expect(paritySignedProofVerified.value == Data("1".utf8), "authenticated proof envelope payload mismatch")
let parityAuthenticatedBundle = try verifyAuthenticatedProofBundle(
    envelopeBytes: paritySignedEnvelopeBytes,
    secret: Data("shared secret".utf8),
    nowMillis: 1_700_000_050_000
)
try expect(parityAuthenticatedBundle.valid, "authenticated proof bundle should verify")
try expect(parityAuthenticatedBundle.envelope.valid, "authenticated proof bundle envelope should verify")
try expect(parityAuthenticatedBundle.proofError == nil, "authenticated proof bundle should not have proof error")
try expect(parityAuthenticatedBundle.proof?.existsCount == 1, "authenticated proof bundle proof count mismatch")
let parityWrongEnvelope = verifyAuthenticatedProofEnvelope(
    envelope: parityDecodedEnvelope,
    secret: Data("wrong secret".utf8),
    nowMillis: 1_700_000_050_000
)
try expect(!parityWrongEnvelope.valid, "authenticated proof envelope should reject wrong secret")
let parityWrongAuthenticatedBundle = try verifyAuthenticatedProofBundle(
    envelopeBytes: paritySignedEnvelopeBytes,
    secret: Data("wrong secret".utf8),
    nowMillis: 1_700_000_050_000
)
try expect(!parityWrongAuthenticatedBundle.valid, "authenticated proof bundle should reject wrong secret")
try expect(!parityWrongAuthenticatedBundle.envelope.valid, "authenticated proof bundle envelope should reject wrong secret")
try expect(parityWrongAuthenticatedBundle.proof == nil, "wrong-secret authenticated proof bundle should not verify proof")
try expect(defaultConfig().encoding.kind == .raw, "default config encoding mismatch")
try expect(encodingRaw().kind == .raw, "raw encoding helper mismatch")
try expect(encodingCbor().kind == .cbor, "cbor encoding helper mismatch")
try expect(encodingJson().kind == .json, "json encoding helper mismatch")
let customEncoding = encodingCustom(name: "postcard")
try expect(customEncoding.kind == .custom, "custom encoding helper kind mismatch")
try expect(customEncoding.customName == "postcard", "custom encoding helper name mismatch")
let constructedConfig = try treeConfig(
    minChunkSize: 2,
    maxChunkSize: 64,
    chunkingFactor: 32,
    hashSeed: 7,
    encoding: customEncoding,
    nodeCacheMaxNodes: 16,
    nodeCacheMaxBytes: 4096
)
try expect(constructedConfig.encoding == customEncoding, "tree config helper encoding mismatch")
try expect(constructedConfig.nodeCacheMaxNodes == 16, "tree config helper cache node mismatch")
try expectThrows("custom encoding without a name should fail") {
    _ = try treeConfig(
        minChunkSize: 2,
        maxChunkSize: 64,
        chunkingFactor: 32,
        hashSeed: 7,
        encoding: EncodingRecord(kind: .custom, customName: nil),
        nodeCacheMaxNodes: nil,
        nodeCacheMaxBytes: nil
    )
}
let constructedLargeValueConfig = try largeValueConfig(inlineThreshold: 8)
try expect(constructedLargeValueConfig.inlineThreshold == 8, "large value config helper mismatch")
let constructedParallelConfig = try parallelConfig(maxThreads: 2, parallelismThreshold: 24)
try expect(constructedParallelConfig.maxThreads == 2, "parallel config helper mismatch")
try expect(parallelConfigSequential().maxThreads == 1, "sequential parallel config helper mismatch")
try expect(upsertMutation(key: Data("probe".utf8), value: Data("value".utf8)).kind == .upsert, "upsert mutation helper mismatch")
try expect(deleteMutation(key: Data("probe".utf8)).kind == .delete, "delete mutation helper mismatch")
let batchStats = try parityEngine.batchWithStats(tree: emptyTree, mutations: [
    upsertMutation(key: Data("b".utf8), value: Data("2".utf8)),
    upsertMutation(key: Data("a".utf8), value: Data("1".utf8)),
    upsertMutation(key: Data("a".utf8), value: Data("11".utf8)),
])
let batchStatsValue = try parityEngine.get(tree: batchStats.tree, key: Data("a".utf8))
try expect(batchStatsValue == Data("11".utf8), "batch_with_stats value mismatch")
try expect(batchStats.stats.inputMutations == 3, "batch input mutation count mismatch")
try expect(batchStats.stats.effectiveMutations == 2, "batch effective mutation count mismatch")
try expect(!batchStats.stats.preprocessInputSorted, "batch sorted flag mismatch")
let firstBoundaryEntry = try parityEngine.firstEntry(tree: batchStats.tree)
let lastBoundaryEntry = try parityEngine.lastEntry(tree: batchStats.tree)
let lowerBoundaryEntry = try parityEngine.lowerBound(tree: batchStats.tree, key: Data("aa".utf8))
let upperBoundaryEntry = try parityEngine.upperBound(tree: batchStats.tree, key: Data("b".utf8))
let prefixEntries = try parityEngine.prefix(tree: batchStats.tree, prefix: Data("a".utf8))
let prefixPage = try parityEngine.prefixPage(tree: batchStats.tree, prefix: Data("a".utf8), cursor: nil, limit: 1)
let prefixReversePage = try parityEngine.prefixReversePage(tree: batchStats.tree, prefix: Data("a".utf8), cursor: nil, limit: 1)
try expect(firstBoundaryEntry?.key == Data("a".utf8), "first entry key mismatch")
try expect(firstBoundaryEntry?.value == Data("11".utf8), "first entry value mismatch")
try expect(lastBoundaryEntry?.key == Data("b".utf8), "last entry key mismatch")
try expect(lowerBoundaryEntry?.key == Data("b".utf8), "lower bound key mismatch")
try expect(upperBoundaryEntry == nil, "upper bound should miss")
try expect(prefixEntries.map(\.value) == [Data("11".utf8)], "prefix entries mismatch")
try expect(prefixPage.entries.map(\.value) == [Data("11".utf8)], "prefix page entries mismatch")
try expect(prefixPage.nextCursor != nil, "prefix page cursor missing")
try expect(prefixReversePage.entries.map(\.value) == [Data("11".utf8)], "prefix reverse page entries mismatch")
try expect(prefixReversePage.nextCursor == nil, "prefix reverse page cursor mismatch")

let cursorWindow = try parityEngine.cursorWindow(
    tree: batchStats.tree,
    key: Data("aa".utf8),
    end: nil,
    limit: 1
)
try expect(cursorWindow.positionKey == Data("a".utf8), "cursor window position key mismatch")
try expect(cursorWindow.positionValue == Data("11".utf8), "cursor window position value mismatch")
try expect(!cursorWindow.found, "cursor window should report inexact seek")
try expect(cursorWindow.entries.count == 1, "cursor window entry count mismatch")
try expect(cursorWindow.entries[0].key == Data("b".utf8), "cursor window entry key mismatch")
try expect(cursorWindow.nextCursor?.afterKey == Data("b".utf8), "cursor window next cursor mismatch")

let exactCursorProbe = try parityEngine.cursorWindow(
    tree: batchStats.tree,
    key: Data("a".utf8),
    end: nil,
    limit: 0
)
try expect(exactCursorProbe.found, "cursor window exact probe mismatch")
try expect(exactCursorProbe.positionKey == Data("a".utf8), "cursor window exact position mismatch")
try expect(exactCursorProbe.entries.isEmpty, "cursor window exact probe should not emit entries")
try expect(exactCursorProbe.nextCursor == nil, "cursor window exact probe should not emit cursor")

let defaultParallel = defaultParallelConfig()
try expect(defaultParallel.parallelismThreshold == 100, "default parallel threshold mismatch")
let parallelTree = try parityEngine.parallelBatch(tree: emptyTree, mutations: [
    MutationRecord(kind: .upsert, key: Data("p".utf8), value: Data("parallel".utf8)),
    MutationRecord(kind: .upsert, key: Data("q".utf8), value: Data("swift".utf8)),
], config: try parallelConfig(maxThreads: 1, parallelismThreshold: 1))
let parallelValue = try parityEngine.get(tree: parallelTree, key: Data("q".utf8))
try expect(parallelValue == Data("swift".utf8), "parallel_batch value mismatch")
let parallelStats = try parityEngine.parallelBatchWithStats(tree: emptyTree, mutations: [
    MutationRecord(kind: .upsert, key: Data("r".utf8), value: Data("route".utf8)),
    MutationRecord(kind: .upsert, key: Data("s".utf8), value: Data("stats".utf8)),
], config: try parallelConfig(maxThreads: 1, parallelismThreshold: 1))
let parallelStatsValue = try parityEngine.get(tree: parallelStats.tree, key: Data("s".utf8))
try expect(parallelStatsValue == Data("stats".utf8), "parallel_batch_with_stats value mismatch")
try expect(parallelStats.stats.inputMutations == 2, "parallel batch input mutation count mismatch")
try expect(parallelStats.stats.effectiveMutations == 2, "parallel batch effective mutation count mismatch")
try expect(parallelStats.stats.writtenNodes > 0, "parallel batch written node count missing")

let appendedStats = try parityEngine.appendBatchWithStats(tree: builtTree, mutations: [
    MutationRecord(kind: .upsert, key: Data("d".utf8), value: Data("4".utf8)),
    MutationRecord(kind: .upsert, key: Data("e".utf8), value: Data("5".utf8)),
    MutationRecord(kind: .upsert, key: Data("d".utf8), value: Data("44".utf8)),
])
let appendedStatsValue = try parityEngine.get(tree: appendedStats.tree, key: Data("d".utf8))
try expect(appendedStatsValue == Data("44".utf8), "append_batch_with_stats value mismatch")
try expect(appendedStats.stats.inputMutations == 3, "append input mutation count mismatch")
try expect(appendedStats.stats.effectiveMutations == 2, "append effective mutation count mismatch")
try expect(!appendedStats.stats.preprocessInputSorted, "append sorted flag mismatch")
try expect(appendedStats.stats.usedAppendFastPath, "append fast-path flag mismatch")
try expect(appendedStats.stats.writtenNodes > 0, "append written nodes missing")

print("Swift fixture_check scenario passed")
