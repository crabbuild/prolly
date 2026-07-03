import Foundation
import Prolly

func bytes(_ value: String) -> Data {
    Data(value.utf8)
}

func text(_ value: Data?) -> String? {
    value.map { String(decoding: $0, as: UTF8.self) }
}

let engine = try ProllyEngine.memory(config: defaultConfig())
let empty = engine.create()

var tree = empty
tree = try engine.put(tree: tree, key: bytes("acct:1"), value: bytes("Ada"))
tree = try engine.put(tree: tree, key: bytes("acct:2"), value: bytes("Grace"))
tree = try engine.put(tree: tree, key: bytes("acct:3"), value: bytes("Linus"))

let loadedAccount = try engine.get(tree: tree, key: bytes("acct:2"))
let loadedMany = try engine.getMany(tree: tree, keys: [bytes("acct:3"), bytes("missing")])
precondition(text(loadedAccount) == "Grace")
precondition(loadedMany.map(text) == ["Linus", nil])

let range = try engine.range(tree: tree, start: bytes("acct:"), end: bytes("acct;"))
precondition(range.map { text($0.value) } == ["Ada", "Grace", "Linus"])

let firstPage = try engine.rangePage(tree: tree, cursor: nil, end: nil, limit: 2)
precondition(firstPage.entries.count == 2)
precondition(firstPage.nextCursor != nil)

let batched = try engine.batch(
    tree: empty,
    mutations: [
        MutationRecord(kind: .upsert, key: bytes("acct:1"), value: bytes("old")),
        MutationRecord(kind: .upsert, key: bytes("acct:1"), value: bytes("Ada")),
        MutationRecord(kind: .delete, key: bytes("missing"), value: nil),
    ]
)
let loadedBatched = try engine.get(tree: batched, key: bytes("acct:1"))
precondition(text(loadedBatched) == "Ada")

try engine.publishNamedRoot(name: bytes("main"), tree: tree)
let loadedRoot = try engine.loadNamedRoot(name: bytes("main"))
precondition(loadedRoot?.root == tree.root)

let stats = try engine.collectStatsJson(tree: tree)
precondition(stats.json.contains("num_nodes"))

print("Swift basic_map scenario passed")
