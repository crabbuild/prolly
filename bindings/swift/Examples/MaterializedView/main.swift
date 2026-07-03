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

struct Order {
    let tenant: String
    let id: String
    let status: String
    let cents: Int
}

func orderKey(_ order: Order) -> Data {
    bytes("orders/source/tenant/\(order.tenant)/order/\(order.id)")
}
func encodeOrder(_ order: Order) -> Data {
    bytes("\(order.tenant)|\(order.id)|\(order.status)|\(order.cents)")
}
func decodeOrder(_ value: Data) -> Order {
    let parts = text(value)!.split(separator: "|", maxSplits: 3).map(String.init)
    return Order(tenant: parts[0], id: parts[1], status: parts[2], cents: Int(parts[3])!)
}
func viewKey(_ tenant: String, _ status: String) -> Data {
    bytes("orders/view/by-status/tenant/\(tenant)/status/\(status)")
}
func buildRevenueView(_ engine: ProllyEngine, _ source: TreeRecord) throws -> TreeRecord {
    var totals: [String: Int] = [:]
    for entry in try engine.range(tree: source, start: bytes("orders/source/"), end: bytes("orders/source0")) {
        let order = decodeOrder(entry.value)
        totals["\(order.tenant)|\(order.status)", default: 0] += order.cents
    }
    let mutations = totals.keys.sorted().map { key -> MutationRecord in
        let parts = key.split(separator: "|", maxSplits: 1).map(String.init)
        return MutationRecord(kind: .upsert, key: viewKey(parts[0], parts[1]), value: bytes(String(totals[key]!)))
    }
    return try engine.batch(tree: engine.create(), mutations: mutations)
}

let engine = try ProllyEngine.memory(config: defaultConfig())
let o1 = Order(tenant: "acme", id: "o1", status: "paid", cents: 1200)
let o2 = Order(tenant: "acme", id: "o2", status: "open", cents: 500)
let sourceV1 = try engine.batch(
    tree: engine.create(),
    mutations: [
        MutationRecord(kind: .upsert, key: orderKey(o1), value: encodeOrder(o1)),
        MutationRecord(kind: .upsert, key: orderKey(o2), value: encodeOrder(o2)),
    ]
)
let paidO2 = Order(tenant: "acme", id: "o2", status: "paid", cents: 500)
let sourceV2 = try engine.put(tree: sourceV1, key: orderKey(paidO2), value: encodeOrder(paidO2))
let viewV2 = try buildRevenueView(engine, sourceV2)

let paidRevenue = try engine.get(tree: viewV2, key: viewKey("acme", "paid"))
let openRevenue = try engine.get(tree: viewV2, key: viewKey("acme", "open"))
precondition(text(paidRevenue) == "1700")
precondition(openRevenue == nil)

print("materialized_view: folded \((try engine.diff(base: sourceV1, other: sourceV2)).count) source diff")
