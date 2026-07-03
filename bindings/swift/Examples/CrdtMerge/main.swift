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
let baseValue = timestampedValueToBytes(record: TimestampedValueRecord(value: bytes("base"), timestamp: 1))
let leftValue = timestampedValueToBytes(record: TimestampedValueRecord(value: bytes("left"), timestamp: 2))
let rightValue = timestampedValueToBytes(record: TimestampedValueRecord(value: bytes("right"), timestamp: 3))

let base = try engine.put(tree: engine.create(), key: bytes("counter/global"), value: baseValue)
let left = try engine.put(tree: base, key: bytes("counter/global"), value: leftValue)
let right = try engine.put(tree: base, key: bytes("counter/global"), value: rightValue)
let merged = try engine.crdtMerge(base: base, left: left, right: right, config: crdtConfigLww(deletePolicy: .updateWins))
let decoded = try timestampedValueFromBytes(bytes: try engine.get(tree: merged, key: bytes("counter/global"))!)
let mergedSet = multiValueSetMerge(left: [bytes("candidate-b")], right: [bytes("candidate-a"), bytes("candidate-b")])

precondition(text(decoded.value) == "right")
precondition(decoded.timestamp == 3)
precondition(text(mergedSet[0]) == "candidate-a")

print("crdt_merge: last-writer-wins and multi-value helpers passed")
