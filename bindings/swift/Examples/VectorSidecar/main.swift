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
let sidecar = ["vec-1": [0.9, 0.1], "vec-2": [0.8, 0.2], "vec-stale": [1.0, 0.0]]
let tree = try engine.batch(
    tree: engine.create(),
    mutations: [
        upsert("vector-sidecar/corpus/docs/chunk/doc-1/0001", bytes("vec-1|doc-1|parser-v1")),
        upsert("vector-sidecar/corpus/docs/chunk/doc-2/0001", bytes("vec-2|doc-2|parser-v1")),
    ]
)
let allowed = Set(try engine.range(tree: tree, start: bytes("vector-sidecar/corpus/docs/chunk/"), end: bytes("vector-sidecar/corpus/docs/chunk0")).map {
    text($0.value)!.split(separator: "|", maxSplits: 1).first.map(String.init)!
})
let hits = sidecar.keys.sorted().filter { allowed.contains($0) }
precondition(hits == ["vec-1", "vec-2"])

print("vector_sidecar: filtered sidecar hits to \(hits.count) snapshot vectors")
