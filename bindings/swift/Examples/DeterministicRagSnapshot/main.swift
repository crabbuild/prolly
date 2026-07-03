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
let indexRoot = bytes("rag/corpus/docs/root/index/current")
let indexV1 = try engine.batch(
    tree: engine.create(),
    mutations: [
        upsert("rag/corpus/docs/chunk/doc-1/0001", bytes("vector:v1|CrabDB stores deterministic roots")),
        upsert("rag/corpus/docs/chunk/doc-2/0001", bytes("vector:v2|Prolly trees diff by key")),
    ]
)
try engine.publishNamedRoot(name: indexRoot, tree: indexV1)
let answers = try engine.put(
    tree: engine.create(),
    key: bytes("rag/answer/q1"),
    value: bytes("query:q1|snapshot:\(indexV1.root?.hex() ?? "")|citation:doc-1/0001")
)
try engine.publishNamedRoot(name: bytes("rag/corpus/docs/root/answers"), tree: answers)
let indexV2 = try engine.put(tree: indexV1, key: bytes("rag/corpus/docs/chunk/doc-3/0001"), value: bytes("vector:v3|New content"))
try engine.publishNamedRoot(name: indexRoot, tree: indexV2)

let replayRows = try engine.range(tree: indexV1, start: bytes("rag/corpus/docs/chunk/"), end: bytes("rag/corpus/docs/chunk0")).count
let currentRows = try engine.range(tree: try engine.loadNamedRoot(name: indexRoot)!, start: bytes("rag/corpus/docs/chunk/"), end: bytes("rag/corpus/docs/chunk0")).count
precondition(replayRows == 2 && currentRows == 3)

print("deterministic_rag_snapshot: replay kept original index root")
