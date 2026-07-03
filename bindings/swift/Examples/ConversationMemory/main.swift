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
let main = bytes("conversation/c42/root/main")
let attemptName = bytes("conversation/c42/attempt/extractor/a1")
let base = try engine.put(tree: engine.create(), key: bytes("conversation/c42/memory/m001"), value: bytes("user|likes terse summaries|0.91"))
try engine.publishNamedRoot(name: main, tree: base)
let attempt = try engine.put(tree: base, key: bytes("conversation/c42/memory/m002"), value: bytes("user|uses Swift|0.87"))
try engine.publishNamedRoot(name: attemptName, tree: attempt)
let canonical = try engine.put(tree: base, key: bytes("conversation/c42/memory/m003"), value: bytes("user|prefers local-first apps|0.82"))
try engine.publishNamedRoot(name: main, tree: canonical)

let merged = try engine.merge(base: base, left: try engine.loadNamedRoot(name: main)!, right: try engine.loadNamedRoot(name: attemptName)!, resolver: "prefer_right")
let update = try engine.compareAndSwapNamedRoot(name: main, expected: canonical, replacement: merged)
let rows = try engine.range(tree: merged, start: bytes("conversation/c42/memory/"), end: bytes("conversation/c42/memory0"))

precondition(update.applied)
precondition(rows.count == 3)

print("conversation_memory: accepted extractor attempt into canonical memory")
