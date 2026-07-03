import Foundation
import Prolly

public func bytes(_ value: String) -> Data {
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

public enum Cookbook {
    public static func batchBuild() throws {
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
    }

    public static func localFirstState() throws {
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
    }

    public static func resolver() throws {
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
    }

    public static func crdtMerge() throws {
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
    }

    public static func conversationMemory() throws {
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
    }

    public static func agentEventLog() throws {
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
    }

    public static func backgroundCompaction() throws {
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
    }

    public static func deterministicRagSnapshot() throws {
        let engine = try ProllyEngine.memory(config: defaultConfig())
        let indexRoot = bytes("rag/corpus/docs/root/index/current")
        let indexV1 = try engine.batch(
            tree: engine.create(),
            mutations: [
                upsert("rag/corpus/docs/chunk/doc-1/0001", bytes("vector:v1|CrabDB stores deterministic roots")),
                upsert("rag/corpus/docs/chunk/doc-2/0001", bytes("vector:v2|Prolly trees diff by key")),
            ]
        )
        try engine.publishNamedRoot(name: indexRoot, tree: indexV1)
        let answers = try engine.put(
            tree: engine.create(),
            key: bytes("rag/answer/q1"),
            value: bytes("query:q1|snapshot:\(indexV1.root?.hex() ?? "")|citation:doc-1/0001")
        )
        try engine.publishNamedRoot(name: bytes("rag/corpus/docs/root/answers"), tree: answers)
        let indexV2 = try engine.put(tree: indexV1, key: bytes("rag/corpus/docs/chunk/doc-3/0001"), value: bytes("vector:v3|New content"))
        try engine.publishNamedRoot(name: indexRoot, tree: indexV2)

        let replayRows = try engine.range(tree: indexV1, start: bytes("rag/corpus/docs/chunk/"), end: bytes("rag/corpus/docs/chunk0")).count
        let currentRows = try engine.range(tree: try engine.loadNamedRoot(name: indexRoot)!, start: bytes("rag/corpus/docs/chunk/"), end: bytes("rag/corpus/docs/chunk0")).count
        precondition(replayRows == 2 && currentRows == 3)

        print("deterministic_rag_snapshot: replay kept original index root")
    }

    public static func documentChunkIndex() throws {
        let engine = try ProllyEngine.memory(config: defaultConfig())
        let blobStore = ProllyBlobStore.memory()
        let textKey = bytes("doc-index/corpus/text/parser-v1/doc-1/chunk-0001")
        let metadataKey = bytes("doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000")
        var tree = try engine.putLargeValue(
            blobStore: blobStore,
            tree: engine.create(),
            key: textKey,
            value: bytes(String(repeating: "CrabDB stores large chunk text outside prolly leaves.", count: 8)),
            config: LargeValueConfigRecord(inlineThreshold: 32)
        )
        tree = try engine.put(tree: tree, key: metadataKey, value: bytes("doc-1|chunk-0001|0|384|vector-0001"))

        let metadata = try engine.range(tree: tree, start: bytes("doc-index/corpus/parser/"), end: bytes("doc-index/corpus/parser0"))
        let loadedText = try engine.getLargeValue(blobStore: blobStore, tree: tree, key: textKey)
        precondition(metadata.count == 1)
        precondition(text(loadedText)?.hasPrefix("CrabDB stores") == true)

        print("document_chunk_index: metadata and blob-backed chunk text are linked")
    }

    public static func vectorSidecar() throws {
        let engine = try ProllyEngine.memory(config: defaultConfig())
        let sidecar = ["vec-1": [0.9, 0.1], "vec-2": [0.8, 0.2], "vec-stale": [1.0, 0.0]]
        let tree = try engine.batch(
            tree: engine.create(),
            mutations: [
                upsert("vector-sidecar/corpus/docs/chunk/doc-1/0001", bytes("vec-1|doc-1|parser-v1")),
                upsert("vector-sidecar/corpus/docs/chunk/doc-2/0001", bytes("vec-2|doc-2|parser-v1")),
            ]
        )
        let allowed = Set(try engine.range(tree: tree, start: bytes("vector-sidecar/corpus/docs/chunk/"), end: bytes("vector-sidecar/corpus/docs/chunk0")).map {
            text($0.value)!.split(separator: "|", maxSplits: 1).first.map(String.init)!
        })
        let hits = sidecar.keys.sorted().filter { allowed.contains($0) }
        precondition(hits == ["vec-1", "vec-2"])

        print("vector_sidecar: filtered sidecar hits to \(hits.count) snapshot vectors")
    }

    public static func provenanceValues() throws {
        let engine = try ProllyEngine.memory(config: defaultConfig())
        let source = bytes("CrabDB language bindings design")
        let sourceCid = cidFromBytes(bytes: source).hex()
        let chunkCid = cidFromBytes(bytes: source.prefix(16)).hex()
        let tree = try engine.batch(
            tree: engine.create(),
            mutations: [
                upsert("provenance/chunk/file-1/chunk-1", bytes("source=\(sourceCid)|chunk=\(chunkCid)|parser=v1")),
                upsert("provenance/claim/file-1/claim-1", bytes("CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1")),
            ]
        )
        let claims = try engine.range(tree: tree, start: bytes("provenance/claim/file-1/"), end: bytes("provenance/claim/file-10"))
        precondition(claims.count == 1)
        precondition(text(claims[0].value)?.contains("Rust-backed") == true)

        print("provenance_values: claim links back to source and chunk CIDs")
    }

    public static func materializedView() throws {
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
    }

    public static func filesystemSnapshot() throws {
        let engine = try ProllyEngine.memory(config: defaultConfig())
        let blobStore = ProllyBlobStore.memory()
        var tree = engine.create()
        for (path, contents) in ["README.md": "# Demo\n", "src/lib.rs": "pub fn answer() -> u8 { 42 }\n"] {
            tree = try engine.putLargeValue(blobStore: blobStore, tree: tree, key: bytes("path/\(path)"), value: bytes(contents), config: LargeValueConfigRecord(inlineThreshold: 4))
        }
        try engine.publishNamedRoot(name: bytes("refs/heads/main"), tree: tree)
        let loaded = try engine.loadNamedRoot(name: bytes("refs/heads/main"))!
        let readme = try engine.getLargeValue(blobStore: blobStore, tree: loaded, key: bytes("path/README.md"))
        precondition(text(readme) == "# Demo\n")

        print("filesystem_snapshot: published branch with blob-backed file contents")
    }

    public static func durableSqlite() throws {
        let dir = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent("prolly-swift-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }

        let engine = try ProllyEngine.sqlite(path: dir.appendingPathComponent("app.prolly.sqlite").path, config: defaultConfig())
        let tree = try engine.batch(
            tree: engine.create(),
            mutations: [
                upsert("user/1", bytes("Ada")),
                upsert("user/2", bytes("Grace")),
            ]
        )
        try engine.publishNamedRoot(name: bytes("users/main"), tree: tree)
        let loaded = try engine.loadNamedRoot(name: bytes("users/main"))!
        precondition(loaded.root == tree.root)
        let loadedUser = try engine.get(tree: loaded, key: bytes("user/1"))
        precondition(text(loadedUser) == "Ada")

        print("durable_sqlite: named root survived through SQLite store API")
    }

    public static func runAll() throws {
        try batchBuild()
        try localFirstState()
        try resolver()
        try crdtMerge()
        try conversationMemory()
        try agentEventLog()
        try backgroundCompaction()
        try deterministicRagSnapshot()
        try documentChunkIndex()
        try vectorSidecar()
        try provenanceValues()
        try materializedView()
        try filesystemSnapshot()
        try durableSqlite()
    }
}
