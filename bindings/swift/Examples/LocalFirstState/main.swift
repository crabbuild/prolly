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
let main = bytes("app/demo/root/main")
let base = try engine.batch(
    tree: engine.create(),
    mutations: [
        upsert("entity/user/001", bytes("Ada")),
        upsert("index/user/name/Ada/001", Data()),
    ]
)
try engine.publishNamedRoot(name: main, tree: base)

let device = try engine.batch(
    tree: base,
    mutations: [
        upsert("entity/task/900", bytes("offline draft")),
        upsert("index/task/status/open/900", Data()),
    ]
)
let canonical = try engine.put(tree: base, key: bytes("entity/user/002"), value: bytes("Grace"))
try engine.publishNamedRoot(name: main, tree: canonical)

let current = try engine.loadNamedRoot(name: main)!
let merged = try engine.merge(base: base, left: current, right: device, resolver: "prefer_right")
let update = try engine.compareAndSwapNamedRoot(name: main, expected: current, replacement: merged)

precondition(update.applied)
let mergedUser = try engine.get(tree: merged, key: bytes("entity/user/002"))
let mergedTask = try engine.get(tree: merged, key: bytes("entity/task/900"))
precondition(text(mergedUser) == "Grace")
precondition(text(mergedTask) == "offline draft")

print("local_first_state: merged offline branch into main")
