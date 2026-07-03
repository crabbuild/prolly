import Foundation
import Prolly

func bytes(_ value: String) -> Data {
    Data(value.utf8)
}

func text(_ value: Data?) -> String? {
    value.map { String(decoding: $0, as: UTF8.self) }
}

final class JoinResolver: MergeResolverCallback, @unchecked Sendable {
    func resolve(conflict: ConflictRecord) -> ResolutionRecord {
        if let left = conflict.left, let right = conflict.right {
            var joined = left
            joined.append(bytes("|"))
            joined.append(right)
            return resolutionValue(value: joined)
        }
        if let left = conflict.left {
            return resolutionValue(value: left)
        }
        if let right = conflict.right {
            return resolutionValue(value: right)
        }
        return resolutionDelete()
    }
}

let engine = try ProllyEngine.memory(config: defaultConfig())
let empty = engine.create()
let base = try engine.batch(
    tree: empty,
    mutations: [
        MutationRecord(kind: .upsert, key: bytes("doc:title"), value: bytes("Draft")),
        MutationRecord(kind: .upsert, key: bytes("doc:body"), value: bytes("Base")),
    ]
)
let left = try engine.batch(
    tree: base,
    mutations: [
        MutationRecord(kind: .upsert, key: bytes("doc:title"), value: bytes("Left")),
        MutationRecord(kind: .upsert, key: bytes("doc:body"), value: bytes("Left body")),
    ]
)
let right = try engine.batch(
    tree: base,
    mutations: [
        MutationRecord(kind: .upsert, key: bytes("doc:title"), value: bytes("Right")),
        MutationRecord(kind: .upsert, key: bytes("doc:body"), value: bytes("Right body")),
    ]
)

let diffs = try engine.diff(base: base, other: right)
precondition(diffs.count == 2)

let snapshotBundle = try engine.exportSnapshot(tree: right)
precondition(snapshotBundle.formatVersion == 1)
precondition(!snapshotBundle.nodes.isEmpty)
let snapshotBundleBytes = try snapshotBundleToBytes(record: snapshotBundle)
let bundleDigest = try snapshotBundleDigest(record: snapshotBundle)
precondition(bundleDigest == cidFromBytes(bytes: snapshotBundleBytes))
let byteSnapshotBundleDigest = try snapshotBundleDigestBytes(bytes: snapshotBundleBytes)
precondition(byteSnapshotBundleDigest == bundleDigest)
let snapshotSummary = try snapshotBundleSummary(record: snapshotBundle)
precondition(snapshotSummary.formatVersion == 1)
precondition(snapshotSummary.nodeCount == UInt64(snapshotBundle.nodes.count))
precondition(snapshotSummary.byteCount > 0)
let byteSnapshotSummary = try snapshotBundleSummaryFromBytes(bytes: snapshotBundleBytes)
precondition(byteSnapshotSummary == snapshotSummary)
let snapshotVerification = try verifySnapshotBundle(record: snapshotBundle)
precondition(snapshotVerification.valid)
precondition(snapshotVerification.summary == snapshotSummary)
precondition(snapshotVerification.missingCids.isEmpty)
precondition(snapshotVerification.extraCids.isEmpty)
let byteSnapshotVerification = try verifySnapshotBundleBytes(bytes: snapshotBundleBytes)
precondition(byteSnapshotVerification == snapshotVerification)
let incompleteSnapshotBundle = SnapshotBundleRecord(
    formatVersion: snapshotBundle.formatVersion,
    tree: snapshotBundle.tree,
    nodes: Array(snapshotBundle.nodes.dropLast())
)
let incompleteSnapshotVerification = try verifySnapshotBundle(record: incompleteSnapshotBundle)
precondition(!incompleteSnapshotVerification.valid)
precondition(!incompleteSnapshotVerification.missingCids.isEmpty)
let decodedSnapshotBundle = try snapshotBundleFromBytes(bytes: snapshotBundleBytes)
let snapshotDestination = try ProllyEngine.memory(config: defaultConfig())
let importedSnapshotTree = try snapshotDestination.importSnapshot(bundle: decodedSnapshotBundle)
let importedSnapshotTitle = try snapshotDestination.get(tree: importedSnapshotTree, key: bytes("doc:title"))
precondition(text(importedSnapshotTitle) == "Right")

let structuralCursorPage = try engine.structuralDiffPage(base: empty, other: right, cursorJson: nil, limit: 0)
precondition(structuralCursorPage.nextCursor != nil)
precondition(structuralCursorPage.nextCursorJson != nil)
let resumedStructuralPage = try engine.structuralDiffPageWithCursor(
    base: empty,
    other: right,
    cursor: structuralCursorPage.nextCursor,
    limit: 1
)
precondition(resumedStructuralPage.diffs.count == 1)

let preferRight = try engine.merge(base: base, left: left, right: right, resolver: "prefer_right")
let preferRightTitle = try engine.get(tree: preferRight, key: bytes("doc:title"))
precondition(text(preferRightTitle) == "Right")

let joined = try engine.mergeWithResolver(base: base, left: left, right: right, resolver: JoinResolver())
let joinedTitle = try engine.get(tree: joined, key: bytes("doc:title"))
precondition(text(joinedTitle) == "Left|Right")

let rangeMerged = try engine.mergeRange(
    base: base,
    left: left,
    right: right,
    start: bytes("doc:body"),
    end: bytes("doc:body0"),
    resolver: "prefer_left"
)
let rangeMergedBody = try engine.get(tree: rangeMerged, key: bytes("doc:body"))
let rangeMergedTitle = try engine.get(tree: rangeMerged, key: bytes("doc:title"))
precondition(text(rangeMergedBody) == "Left body")
precondition(text(rangeMergedTitle) == "Left")

let prefixMerged = try engine.mergePrefix(base: base, left: left, right: right, prefix: bytes("doc:"), resolver: "prefer_right")
let prefixMergedBody = try engine.get(tree: prefixMerged, key: bytes("doc:body"))
precondition(text(prefixMergedBody) == "Right body")

let explanation = try engine.mergeExplain(base: base, left: left, right: right, resolver: "prefer_right")
precondition(explanation.result != nil)
precondition(explanation.traceJson.contains("events"))
precondition(explanation.trace.events.contains { event in
    event.kind == .resolverCalled && event.resolution == .value
})

print("Swift diff_merge scenario passed")
