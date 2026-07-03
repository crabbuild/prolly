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
let base = try engine.put(tree: engine.create(), key: bytes("settings/theme"), value: bytes("light"))
let leftDelete = try engine.delete(tree: base, key: bytes("settings/theme"))
let rightUpdate = try engine.put(tree: base, key: bytes("settings/theme"), value: bytes("dark"))

let updateWins = try engine.merge(base: base, left: leftDelete, right: rightUpdate, resolver: "update_wins")
let deleteWins = try engine.merge(base: base, left: leftDelete, right: rightUpdate, resolver: "delete_wins")

let updateWinsTheme = try engine.get(tree: updateWins, key: bytes("settings/theme"))
let deleteWinsTheme = try engine.get(tree: deleteWins, key: bytes("settings/theme"))
precondition(text(updateWinsTheme) == "dark")
precondition(deleteWinsTheme == nil)

print("resolver: demonstrated update-wins and delete-wins policies")
