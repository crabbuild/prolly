import Foundation
import MySQLNIO
import NIOCore
import Prolly

private enum MySQLStoreFailure: Error {
    case invalidArgument
    case internalFailure
}

public actor MySQLRemoteStore: ForeignRemoteStore {
    public static let schema = [
        "CREATE TABLE IF NOT EXISTS prolly_nodes (cid VARBINARY(32) PRIMARY KEY, node LONGBLOB NOT NULL)",
        "CREATE TABLE IF NOT EXISTS prolly_hints (namespace VARBINARY(255) NOT NULL, `key` VARBINARY(255) NOT NULL, value LONGBLOB NOT NULL, PRIMARY KEY(namespace, `key`))",
        "CREATE TABLE IF NOT EXISTS prolly_roots (name VARBINARY(255) PRIMARY KEY, manifest LONGBLOB NOT NULL)",
    ]

    private let connection: MySQLConnection
    private var closed = false

    public init(borrowing connection: MySQLConnection) { self.connection = connection }

    public func initializeSchema() async throws {
        try ensureOpen()
        for statement in Self.schema { _ = try await connection.simpleQuery(statement).get() }
    }

    public func close() { closed = true }

    public func descriptor() async -> StoreDescriptorResultRecord {
        StoreDescriptorResultRecord(
            value: StoreDescriptorRecord(
                protocolMajor: 1, adapterName: "mysql-v1", provider: "mysql", schemaVersion: 1,
                capabilities: StoreCapabilitiesRecord(
                    nativeBatchReads: false, atomicBatchWrites: true, nodeScan: true,
                    hints: true, atomicNodesAndHint: true, rootScan: true,
                    rootCompareAndSwap: true, transactions: true, readParallelism: 1
                ),
                limits: StoreLimitsRecord(
                    maxBatchReadItems: nil, maxBatchWriteItems: nil,
                    maxTransactionOperations: nil, maxNodeBytes: nil
                )
            ), error: nil
        )
    }

    public func getNode(cid: Data) async -> OptionalBytesResultRecord {
        await optionalResult { try key(cid, 32); return try await queryOptional("SELECT node FROM prolly_nodes WHERE cid=?", [blob(cid)], "node") }
    }

    public func putNode(cid: Data, value: Data) async -> UnitResultRecord {
        await unitResult { try key(cid, 32); try await execute("INSERT INTO prolly_nodes VALUES (?, ?) AS new ON DUPLICATE KEY UPDATE node=new.node", [blob(cid), blob(value)]) }
    }

    public func deleteNode(cid: Data) async -> UnitResultRecord {
        await unitResult { try key(cid, 32); try await execute("DELETE FROM prolly_nodes WHERE cid=?", [blob(cid)]) }
    }

    public func batchNodes(ops: [NodeMutationRecord]) async -> UnitResultRecord {
        await unitResult {
            try await transaction([]) {
                for item in ops { try key(item.key, 32); try await writeNode(item) }
            }
        }
    }

    public func batchGetNodesOrdered(cids: [Data]) async -> OptionalBytesListResultRecord {
        do {
            var values: [OptionalBytesRecord] = []
            for cid in cids {
                try key(cid, 32)
                values.append(optional(try await queryOptional("SELECT node FROM prolly_nodes WHERE cid=?", [blob(cid)], "node")))
            }
            return OptionalBytesListResultRecord(values: values, error: nil)
        } catch { return OptionalBytesListResultRecord(values: [], error: storeError(error)) }
    }

    public func listNodeCids() async -> BytesListResultRecord {
        do { return BytesListResultRecord(values: try await queryData("SELECT cid FROM prolly_nodes ORDER BY cid", [], "cid"), error: nil) }
        catch { return BytesListResultRecord(values: [], error: storeError(error)) }
    }

    public func getHint(namespace: Data, key hintKey: Data) async -> OptionalBytesResultRecord {
        await optionalResult {
            try key(namespace, 255); try key(hintKey, 255)
            return try await queryOptional("SELECT value FROM prolly_hints WHERE namespace=? AND `key`=?", [blob(namespace), blob(hintKey)], "value")
        }
    }

    public func putHint(namespace: Data, key hintKey: Data, value: Data) async -> UnitResultRecord {
        await unitResult {
            try key(namespace, 255); try key(hintKey, 255)
            try await execute("INSERT INTO prolly_hints VALUES (?, ?, ?) AS new ON DUPLICATE KEY UPDATE value=new.value", [blob(namespace), blob(hintKey), blob(value)])
        }
    }

    public func batchPutNodesWithHint(nodes: [NodeEntryRecord], namespace: Data, key hintKey: Data, value: Data) async -> UnitResultRecord {
        await unitResult {
            try key(namespace, 255); try key(hintKey, 255)
            try await transaction([]) {
                for node in nodes {
                    try key(node.key, 32)
                    try await execute("INSERT INTO prolly_nodes VALUES (?, ?) AS new ON DUPLICATE KEY UPDATE node=new.node", [blob(node.key), blob(node.value)])
                }
                try await execute("INSERT INTO prolly_hints VALUES (?, ?, ?) AS new ON DUPLICATE KEY UPDATE value=new.value", [blob(namespace), blob(hintKey), blob(value)])
            }
        }
    }

    public func getRootManifest(name: Data) async -> OptionalBytesResultRecord {
        await optionalResult { try key(name, 255); return try await queryOptional("SELECT manifest FROM prolly_roots WHERE name=?", [blob(name)], "manifest") }
    }

    public func putRootManifest(name: Data, manifest: Data) async -> UnitResultRecord {
        await unitResult { try key(name, 255); try await execute("INSERT INTO prolly_roots VALUES (?, ?) AS new ON DUPLICATE KEY UPDATE manifest=new.manifest", [blob(name), blob(manifest)]) }
    }

    public func deleteRootManifest(name: Data) async -> UnitResultRecord {
        await unitResult { try key(name, 255); try await execute("DELETE FROM prolly_roots WHERE name=?", [blob(name)]) }
    }

    public func compareAndSwapRootManifest(name: Data, expected: OptionalBytesRecord, new replacement: OptionalBytesRecord) async -> RootCasResultRecord {
        do {
            try key(name, 255)
            return try await transaction([name]) {
                let current = try await queryOptional("SELECT manifest FROM prolly_roots WHERE name=? FOR UPDATE", [blob(name)], "manifest")
                guard matches(current, expected) else { return RootCasResultRecord(applied: false, current: optional(current), error: nil) }
                try await writeRoot(name, replacement)
                return RootCasResultRecord(applied: true, current: replacement, error: nil)
            }
        } catch { return RootCasResultRecord(applied: false, current: optional(nil), error: storeError(error)) }
    }

    public func listRootManifests() async -> NamedBytesListResultRecord {
        do {
            try ensureOpen()
            let rows = try await connection.query("SELECT name,manifest FROM prolly_roots ORDER BY name").get()
            let values = try rows.map { row in
                guard let name = data(row.column("name")), let manifest = data(row.column("manifest")) else { throw MySQLStoreFailure.internalFailure }
                return NamedBytesRecord(name: name, value: manifest)
            }
            return NamedBytesListResultRecord(values: values, error: nil)
        } catch { return NamedBytesListResultRecord(values: [], error: storeError(error)) }
    }

    public func commitTransaction(nodes: [NodeMutationRecord], conditions: [RootConditionRecord], roots: [RootWriteRecord]) async -> TransactionResultRecord {
        do {
            for condition in conditions { try key(condition.name, 255) }
            let names = Array(Set(conditions.map(\.name))).sorted(by: { $0.lexicographicallyPrecedes($1) })
            return try await transaction(names) {
                for condition in conditions {
                    let current = try await queryOptional("SELECT manifest FROM prolly_roots WHERE name=? FOR UPDATE", [blob(condition.name)], "manifest")
                    if !matches(current, condition.expected) {
                        return TransactionResultRecord(
                            applied: false,
                            conflict: StoreTransactionConflictRecord(name: condition.name, expected: condition.expected, current: optional(current)),
                            error: nil
                        )
                    }
                }
                for node in nodes { try key(node.key, 32); try await writeNode(node) }
                for root in roots { try key(root.name, 255); try await writeRoot(root.name, root.replacement) }
                return TransactionResultRecord(applied: true, conflict: nil, error: nil)
            }
        } catch { return TransactionResultRecord(applied: false, conflict: nil, error: storeError(error)) }
    }

    private func execute(_ sql: String, _ binds: [MySQLData] = []) async throws {
        try ensureOpen(); _ = try await connection.query(sql, binds).get()
    }

    private func queryOptional(_ sql: String, _ binds: [MySQLData], _ column: String) async throws -> Data? {
        try ensureOpen(); return try await connection.query(sql, binds).get().first.flatMap { data($0.column(column)) }
    }

    private func queryData(_ sql: String, _ binds: [MySQLData], _ column: String) async throws -> [Data] {
        try ensureOpen()
        return try await connection.query(sql, binds).get().compactMap { data($0.column(column)) }
    }

    private func transaction<T>(_ lockNames: [Data], _ body: () async throws -> T) async throws -> T {
        try ensureOpen()
        var acquired: [Data] = []
        do {
            for name in lockNames {
                let rows = try await connection.query("SELECT GET_LOCK(CONCAT('prolly:', HEX(?)), 10) AS acquired", [blob(name)]).get()
                guard rows.first?.column("acquired")?.int == 1 else { throw MySQLStoreFailure.internalFailure }
                acquired.append(name)
            }
            _ = try await connection.simpleQuery("BEGIN").get()
            do {
                let result = try await body()
                _ = try await connection.simpleQuery("COMMIT").get()
                try await release(acquired)
                return result
            } catch {
                _ = try? await connection.simpleQuery("ROLLBACK").get()
                try? await release(acquired)
                throw error
            }
        } catch {
            try? await release(acquired)
            throw error
        }
    }

    private func release(_ names: [Data]) async throws {
        for name in names.reversed() {
            _ = try await connection.query("SELECT RELEASE_LOCK(CONCAT('prolly:', HEX(?)))", [blob(name)]).get()
        }
    }

    private func writeNode(_ item: NodeMutationRecord) async throws {
        if item.value.present {
            try await execute("INSERT INTO prolly_nodes VALUES (?, ?) AS new ON DUPLICATE KEY UPDATE node=new.node", [blob(item.key), blob(item.value.value)])
        } else { try await execute("DELETE FROM prolly_nodes WHERE cid=?", [blob(item.key)]) }
    }

    private func writeRoot(_ name: Data, _ replacement: OptionalBytesRecord) async throws {
        if replacement.present {
            try await execute("INSERT INTO prolly_roots VALUES (?, ?) AS new ON DUPLICATE KEY UPDATE manifest=new.manifest", [blob(name), blob(replacement.value)])
        } else { try await execute("DELETE FROM prolly_roots WHERE name=?", [blob(name)]) }
    }

    private func ensureOpen() throws { if closed { throw MySQLStoreFailure.internalFailure } }
    private func key(_ value: Data, _ maximum: Int) throws { if value.count > maximum { throw MySQLStoreFailure.invalidArgument } }
    private func matches(_ current: Data?, _ expected: OptionalBytesRecord) -> Bool { expected.present ? current == expected.value : current == nil }
    private func optional(_ value: Data?) -> OptionalBytesRecord { OptionalBytesRecord(present: value != nil, value: value ?? Data()) }

    private func blob(_ value: Data) -> MySQLData {
        var buffer = ByteBufferAllocator().buffer(capacity: value.count)
        buffer.writeBytes(value)
        return MySQLData(type: .blob, buffer: buffer)
    }

    private func data(_ value: MySQLData?) -> Data? {
        guard var buffer = value?.buffer, let bytes = buffer.readBytes(length: buffer.readableBytes) else { return nil }
        return Data(bytes)
    }

    private func optionalResult(_ body: () async throws -> Data?) async -> OptionalBytesResultRecord {
        do { return OptionalBytesResultRecord(value: optional(try await body()), error: nil) }
        catch { return OptionalBytesResultRecord(value: optional(nil), error: storeError(error)) }
    }

    private func unitResult(_ body: () async throws -> Void) async -> UnitResultRecord {
        do { try await body(); return UnitResultRecord(error: nil) }
        catch { return UnitResultRecord(error: storeError(error)) }
    }

    private func storeError(_ error: Error) -> StoreErrorRecord {
        let code: String
        switch error {
        case MySQLStoreFailure.invalidArgument: code = "invalid_argument"
        default: code = "internal"
        }
        return StoreErrorRecord(code: code, message: "MySQL provider operation failed", retryable: false, providerCode: nil)
    }
}
