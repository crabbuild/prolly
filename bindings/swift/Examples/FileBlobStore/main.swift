import Foundation
import Prolly

func bytes(_ value: String) -> Data {
    Data(value.utf8)
}

func text(_ value: Data?) -> String? {
    value.map { String(decoding: $0, as: UTF8.self) }
}

let baseDir = URL(fileURLWithPath: NSTemporaryDirectory())
    .appendingPathComponent("prolly-swift-\(UUID().uuidString)")
try FileManager.default.createDirectory(at: baseDir, withIntermediateDirectories: true)
defer {
    try? FileManager.default.removeItem(at: baseDir)
}

let nodePath = baseDir.appendingPathComponent("nodes").path
let blobPath = baseDir.appendingPathComponent("blobs").path

let engine = try ProllyEngine.file(path: nodePath, config: defaultConfig())
let blobStore = try ProllyBlobStore.file(path: blobPath)
let config = LargeValueConfigRecord(inlineThreshold: 8)

let empty = engine.create()
let largePayload = bytes("this profile document is intentionally larger than the inline threshold")
let tree = try engine.putLargeValue(
    blobStore: blobStore,
    tree: empty,
    key: bytes("profile:ada"),
    value: largePayload,
    config: config
)

let loadedLargePayload = try engine.getLargeValue(blobStore: blobStore, tree: tree, key: bytes("profile:ada"))
precondition(loadedLargePayload == largePayload)

let valueRef = try engine.getValueRef(tree: tree, key: bytes("profile:ada"))
precondition(valueRef?.kind == .blob)
let blobCount = try blobStore.blobCount()
precondition(blobCount == 1)

let reopened = try ProllyEngine.file(path: nodePath, config: defaultConfig())
let reopenedPayload = try reopened.getLargeValue(blobStore: blobStore, tree: tree, key: bytes("profile:ada"))
precondition(reopenedPayload == largePayload)

let gcPlan = try reopened.planBlobStoreGc(blobStore: blobStore, roots: [tree])
precondition(gcPlan.reclaimableBlobs.isEmpty)

print("Swift file_blob_store scenario passed")
