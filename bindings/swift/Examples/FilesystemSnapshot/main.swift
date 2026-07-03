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
var tree = engine.create()
for (path, contents) in ["README.md": "# Demo\n", "src/lib.rs": "pub fn answer() -> u8 { 42 }\n"] {
    tree = try engine.putLargeValue(blobStore: blobStore, tree: tree, key: bytes("path/\(path)"), value: bytes(contents), config: LargeValueConfigRecord(inlineThreshold: 4))
}
try engine.publishNamedRoot(name: bytes("refs/heads/main"), tree: tree)
let loaded = try engine.loadNamedRoot(name: bytes("refs/heads/main"))!
let readme = try engine.getLargeValue(blobStore: blobStore, tree: loaded, key: bytes("path/README.md"))
precondition(text(readme) == "# Demo\n")

print("filesystem_snapshot: published branch with blob-backed file contents")
