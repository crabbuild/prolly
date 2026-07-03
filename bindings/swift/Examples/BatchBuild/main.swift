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
let entries = (1...64).reversed().map { index in
    EntryRecord(key: bytes(String(format: "event/%04d", index)), value: bytes("payload-\(index)"))
}
let tree = try engine.buildFromEntries(entries: entries)
let rows = try engine.range(tree: tree, start: bytes("event/"), end: bytes("event0"))
let stats = try engine.collectStatsJson(tree: tree).json

precondition(rows.count == 64)
precondition(rows.first?.key == bytes("event/0001"))
precondition(stats.contains("num_nodes"))

print("batch_build: imported \(rows.count) events")
