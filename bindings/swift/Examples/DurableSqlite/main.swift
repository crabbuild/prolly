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

let dir = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent("prolly-swift-\(UUID().uuidString)")
try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
defer { try? FileManager.default.removeItem(at: dir) }

let engine = try ProllyEngine.sqlite(path: dir.appendingPathComponent("app.prolly.sqlite").path, config: defaultConfig())
let tree = try engine.batch(
    tree: engine.create(),
    mutations: [
        upsert("user/1", bytes("Ada")),
        upsert("user/2", bytes("Grace")),
    ]
)
try engine.publishNamedRoot(name: bytes("users/main"), tree: tree)
let loaded = try engine.loadNamedRoot(name: bytes("users/main"))!
precondition(loaded.root == tree.root)
let loadedUser = try engine.get(tree: loaded, key: bytes("user/1"))
precondition(text(loadedUser) == "Ada")

print("durable_sqlite: named root survived through SQLite store API")
