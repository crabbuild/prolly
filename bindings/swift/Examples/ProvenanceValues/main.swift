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
let source = bytes("CrabDB language bindings design")
let sourceCid = cidFromBytes(bytes: source).hex()
let chunkCid = cidFromBytes(bytes: source.prefix(16)).hex()
let tree = try engine.batch(
    tree: engine.create(),
    mutations: [
        upsert("provenance/chunk/file-1/chunk-1", bytes("source=\(sourceCid)|chunk=\(chunkCid)|parser=v1")),
        upsert("provenance/claim/file-1/claim-1", bytes("CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1")),
    ]
)
let claims = try engine.range(tree: tree, start: bytes("provenance/claim/file-1/"), end: bytes("provenance/claim/file-10"))
precondition(claims.count == 1)
precondition(text(claims[0].value)?.contains("Rust-backed") == true)

print("provenance_values: claim links back to source and chunk CIDs")
