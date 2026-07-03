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
let root = bytes("agent-log/run-7/root/events/current")
let tree = try engine.batch(
    tree: engine.create(),
    mutations: [
        upsert("agent-log/run-7/event/1783036805000/0001", bytes("user|Summarize the plan")),
        upsert("agent-log/run-7/event/1783036805000/0002", bytes("tool-call|search-docs")),
        upsert("agent-log/run-7/event/1783036806000/0003", bytes("assistant|Plan ready")),
    ]
)
try engine.publishNamedRoot(name: root, tree: tree)
let page = try engine.rangePage(tree: try engine.loadNamedRoot(name: root)!, cursor: nil, end: nil, limit: 2)

precondition(page.entries.count == 2)
precondition(page.nextCursor != nil)

print("agent_event_log: first page has \(page.entries.count) events")
