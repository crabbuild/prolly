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
let events = try engine.batch(
    tree: engine.create(),
    mutations: (1...6).map { index in upsert(String(format: "event/%04d", index), bytes("raw-event-\(index)")) }
)
try engine.publishNamedRoot(name: bytes("compaction/run/r7/root/events/0001"), tree: events)
let compacted = try engine.batch(
    tree: events,
    mutations: [
        delete("event/0001"),
        delete("event/0002"),
        delete("event/0003"),
        delete("event/0004"),
        upsert("event/0004-summary", bytes("summary of events 1..4")),
    ]
)
try engine.publishNamedRoot(name: bytes("compaction/run/r7/root/events/current"), tree: compacted)

let plan = try engine.planStoreGc(roots: [events, compacted])
let remaining = try engine.range(tree: compacted, start: bytes("event/"), end: bytes("event0"))

precondition(remaining.count == 3)
precondition(plan.reclaimableNodes >= 0)

print("background_compaction: compacted log to \(remaining.count) records")
