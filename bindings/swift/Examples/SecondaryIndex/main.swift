import Foundation
import Prolly

struct User {
    let id: String
    let email: String
    let name: String
}

func bytes(_ value: String) -> Data {
    Data(value.utf8)
}

func text(_ value: Data?) -> String? {
    value.map { String(decoding: $0, as: UTF8.self) }
}

func primaryKey(_ id: String) -> Data {
    bytes("user:\(id)")
}

func emailIndexKey(_ email: String) -> Data {
    bytes("email:\(email)")
}

func encodeUser(_ user: User) -> Data {
    bytes("\(user.id)|\(user.email)|\(user.name)")
}

func applyUsers(engine: ProllyEngine, primary: TreeRecord, emailIndex: TreeRecord, users: [User]) throws -> (TreeRecord, TreeRecord) {
    var primaryMutations: [MutationRecord] = []
    var indexMutations: [MutationRecord] = []
    for user in users {
        primaryMutations.append(MutationRecord(kind: .upsert, key: primaryKey(user.id), value: encodeUser(user)))
        indexMutations.append(MutationRecord(kind: .upsert, key: emailIndexKey(user.email), value: bytes(user.id)))
    }
    return (
        try engine.batch(tree: primary, mutations: primaryMutations),
        try engine.batch(tree: emailIndex, mutations: indexMutations)
    )
}

func rebuildEmailIndex(engine: ProllyEngine, primary: TreeRecord) throws -> TreeRecord {
    let entries = try engine.range(tree: primary, start: bytes("user:"), end: bytes("user;"))
    let indexEntries = entries.map { entry -> EntryRecord in
        let fields = String(decoding: entry.value, as: UTF8.self).split(separator: "|", omittingEmptySubsequences: false)
        precondition(fields.count == 3)
        return EntryRecord(key: emailIndexKey(String(fields[1])), value: bytes(String(fields[0])))
    }
    return try engine.buildFromEntries(entries: indexEntries)
}

let engine = try ProllyEngine.memory(config: defaultConfig())
let empty = engine.create()
let users = [
    User(id: "1", email: "ada@example.com", name: "Ada"),
    User(id: "2", email: "grace@example.com", name: "Grace"),
    User(id: "3", email: "linus@example.com", name: "Linus"),
]

let (primary, emailIndex) = try applyUsers(engine: engine, primary: empty, emailIndex: empty, users: users)
let userId = try engine.get(tree: emailIndex, key: emailIndexKey("grace@example.com"))
precondition(text(userId) == "2")
let loadedUser = try engine.get(tree: primary, key: primaryKey("2"))
precondition(text(loadedUser)?.contains("Grace") == true)

let rebuilt = try rebuildEmailIndex(engine: engine, primary: primary)
precondition(rebuilt.root == emailIndex.root)

let changed = try engine.put(
    tree: primary,
    key: primaryKey("2"),
    value: encodeUser(User(id: "2", email: "amazing.grace@example.com", name: "Grace"))
)
let changedIndex = try engine.batch(
    tree: emailIndex,
    mutations: [
        MutationRecord(kind: .delete, key: emailIndexKey("grace@example.com"), value: nil),
        MutationRecord(kind: .upsert, key: emailIndexKey("amazing.grace@example.com"), value: bytes("2")),
    ]
)
let rebuiltChangedIndex = try rebuildEmailIndex(engine: engine, primary: changed)
precondition(changedIndex.root == rebuiltChangedIndex.root)

print("Swift secondary_index scenario passed")
