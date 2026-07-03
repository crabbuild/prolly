import Foundation
import Prolly

func bytes(_ value: String) -> Data {
    Data(value.utf8)
}

func text(_ value: Data?) -> String? {
    value.map { String(decoding: $0, as: UTF8.self) }
}

func upsert(_ key: String, _ value: Data) -> MutationRecord {
    MutationRecord(kind: .upsert, key: bytes(key), value: value)
}

func delete(_ key: String) -> MutationRecord {
    MutationRecord(kind: .delete, key: bytes(key), value: nil)
}

extension Data {
    func hex() -> String {
        map { String(format: "%02x", $0) }.joined()
    }
}

let engine = try ProllyEngine.memory(config: defaultConfig())
let blobStore = ProllyBlobStore.memory()
let textKey = bytes("doc-index/corpus/text/parser-v1/doc-1/chunk-0001")
let metadataKey = bytes("doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000")
var tree = try engine.putLargeValue(
    blobStore: blobStore,
    tree: engine.create(),
    key: textKey,
    value: bytes(String(repeating: "CrabDB stores large chunk text outside prolly leaves.", count: 8)),
    config: LargeValueConfigRecord(inlineThreshold: 32)
)
tree = try engine.put(tree: tree, key: metadataKey, value: bytes("doc-1|chunk-0001|0|384|vector-0001"))

let metadata = try engine.range(tree: tree, start: bytes("doc-index/corpus/parser/"), end: bytes("doc-index/corpus/parser0"))
let loadedText = try engine.getLargeValue(blobStore: blobStore, tree: tree, key: textKey)
precondition(metadata.count == 1)
precondition(text(loadedText)?.hasPrefix("CrabDB stores") == true)

print("document_chunk_index: metadata and blob-backed chunk text are linked")
