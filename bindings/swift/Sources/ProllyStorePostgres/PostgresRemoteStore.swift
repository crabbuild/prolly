import Foundation
import Logging
import PostgresNIO
import Prolly

private struct PostgresStoreFailure: Error {}

public actor PostgresRemoteStore: ForeignRemoteStore {
    public static let schema = """
    CREATE TABLE IF NOT EXISTS prolly_nodes (cid bytea PRIMARY KEY, node bytea NOT NULL);
    CREATE TABLE IF NOT EXISTS prolly_hints (namespace bytea NOT NULL, key bytea NOT NULL, value bytea NOT NULL, PRIMARY KEY(namespace, key));
    CREATE TABLE IF NOT EXISTS prolly_roots (name bytea PRIMARY KEY, manifest bytea NOT NULL);
    """

    private let client: PostgresClient
    private let logger = Logger(label: "build.crab.prolly.store.postgres")
    private var closed = false

    public init(borrowing client: PostgresClient) {
        self.client = client
    }

    public func initializeSchema() async throws {
        try ensureOpen()
        for statement in Self.schema.split(separator: ";") where !statement.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            _ = try await client.query(PostgresQuery(unsafeSQL: String(statement)))
        }
    }

    public func close() { closed = true }

    public func descriptor() async -> StoreDescriptorResultRecord {
        let capabilities = StoreCapabilitiesRecord(
            nativeBatchReads: false, atomicBatchWrites: true, nodeScan: true,
            hints: true, atomicNodesAndHint: true, rootScan: true,
            rootCompareAndSwap: true, transactions: true, readParallelism: 16
        )
        let limits = StoreLimitsRecord(
            maxBatchReadItems: nil, maxBatchWriteItems: nil,
            maxTransactionOperations: nil, maxNodeBytes: nil
        )
        return StoreDescriptorResultRecord(
            value: StoreDescriptorRecord(
                protocolMajor: 2, adapterName: "postgres-v1", provider: "postgresql",
                schemaVersion: 1, capabilities: capabilities, limits: limits
            ), error: nil
        )
    }

    public func getNode(cid: Data) async -> OptionalBytesResultRecord {
        await optionalResult { try await queryOptional("SELECT node FROM prolly_nodes WHERE cid = \(cid)") }
    }

    public func putNode(cid: Data, value: Data) async -> UnitResultRecord {
        await unitResult { try await execute("INSERT INTO prolly_nodes VALUES (\(cid), \(value)) ON CONFLICT(cid) DO UPDATE SET node=excluded.node") }
    }

    public func deleteNode(cid: Data) async -> UnitResultRecord {
        await unitResult { try await execute("DELETE FROM prolly_nodes WHERE cid = \(cid)") }
    }

    public func batchNodes(ops: [NodeMutationRecord]) async -> UnitResultRecord {
        await unitResult {
            try await transaction { connection in
                for item in ops { try await self.writeNode(connection, item) }
            }
        }
    }

    public func batchGetNodesOrdered(cids: [Data]) async -> OptionalBytesListResultRecord {
        do {
            let values = try await cids.asyncMap { optional(try await queryOptional("SELECT node FROM prolly_nodes WHERE cid = \($0)")) }
            return OptionalBytesListResultRecord(values: values, error: nil)
        } catch { return OptionalBytesListResultRecord(values: [], error: storeError()) }
    }

    public func listNodeCids() async -> BytesListResultRecord {
        do { return BytesListResultRecord(values: try await queryData("SELECT cid FROM prolly_nodes ORDER BY cid"), error: nil) }
        catch { return BytesListResultRecord(values: [], error: storeError()) }
    }

    public func getHint(namespace: Data, key: Data) async -> OptionalBytesResultRecord {
        await optionalResult { try await queryOptional("SELECT value FROM prolly_hints WHERE namespace = \(namespace) AND key = \(key)") }
    }

    public func putHint(namespace: Data, key: Data, value: Data) async -> UnitResultRecord {
        await unitResult { try await execute("INSERT INTO prolly_hints VALUES (\(namespace), \(key), \(value)) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value") }
    }

    public func batchPutNodesWithHint(nodes: [NodeEntryRecord], namespace: Data, key: Data, value: Data) async -> UnitResultRecord {
        await unitResult {
            try await transaction { connection in
                for node in nodes {
                    _ = try await connection.query("INSERT INTO prolly_nodes VALUES (\(node.key), \(node.value)) ON CONFLICT(cid) DO UPDATE SET node=excluded.node", logger: self.logger)
                }
                _ = try await connection.query("INSERT INTO prolly_hints VALUES (\(namespace), \(key), \(value)) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value", logger: self.logger)
            }
        }
    }

    public func getRootManifest(name: Data) async -> OptionalBytesResultRecord {
        await optionalResult { try await queryOptional("SELECT manifest FROM prolly_roots WHERE name = \(name)") }
    }

    public func putRootManifest(name: Data, manifest: Data) async -> UnitResultRecord {
        await unitResult { try await execute("INSERT INTO prolly_roots VALUES (\(name), \(manifest)) ON CONFLICT(name) DO UPDATE SET manifest=excluded.manifest") }
    }

    public func deleteRootManifest(name: Data) async -> UnitResultRecord {
        await unitResult { try await execute("DELETE FROM prolly_roots WHERE name = \(name)") }
    }

    public func compareAndSwapRootManifest(name: Data, expected: OptionalBytesRecord, new replacement: OptionalBytesRecord) async -> RootCasResultRecord {
        do {
            return try await transaction { connection in
                _ = try await connection.query("SELECT pg_advisory_xact_lock(hashtextextended(encode(\(name)::bytea, 'hex'), 0))", logger: self.logger)
                let current = try await self.queryOptional(connection, "SELECT manifest FROM prolly_roots WHERE name = \(name) FOR UPDATE")
                guard self.matches(current, expected) else {
                    return RootCasResultRecord(applied: false, current: self.optional(current), error: nil)
                }
                try await self.writeRoot(connection, name, replacement)
                return RootCasResultRecord(applied: true, current: replacement, error: nil)
            }
        } catch { return RootCasResultRecord(applied: false, current: optional(nil), error: storeError()) }
    }

    public func listRootManifests() async -> NamedBytesListResultRecord {
        do {
            try ensureOpen()
            let rows = try await client.query("SELECT name, manifest FROM prolly_roots ORDER BY name")
            var values: [NamedBytesRecord] = []
            for try await value in rows.decode((Data, Data).self) {
                values.append(NamedBytesRecord(name: value.0, value: value.1))
            }
            return NamedBytesListResultRecord(values: values, error: nil)
        } catch { return NamedBytesListResultRecord(values: [], error: storeError()) }
    }

    public func commitTransaction(nodes: [NodeMutationRecord], conditions: [RootConditionRecord], roots: [RootWriteRecord]) async -> TransactionResultRecord {
        do {
            return try await transaction { connection in
                for name in Set(conditions.map(\.name)).sorted(by: { $0.lexicographicallyPrecedes($1) }) {
                    _ = try await connection.query("SELECT pg_advisory_xact_lock(hashtextextended(encode(\(name)::bytea, 'hex'), 0))", logger: self.logger)
                }
                for condition in conditions {
                    let current = try await self.queryOptional(connection, "SELECT manifest FROM prolly_roots WHERE name = \(condition.name) FOR UPDATE")
                    if !self.matches(current, condition.expected) {
                        return TransactionResultRecord(
                            applied: false,
                            conflict: StoreTransactionConflictRecord(name: condition.name, expected: condition.expected, current: self.optional(current)),
                            error: nil
                        )
                    }
                }
                for node in nodes { try await self.writeNode(connection, node) }
                for root in roots { try await self.writeRoot(connection, root.name, root.replacement) }
                return TransactionResultRecord(applied: true, conflict: nil, error: nil)
            }
        } catch { return TransactionResultRecord(applied: false, conflict: nil, error: storeError()) }
    }

    private func execute(_ query: PostgresQuery) async throws {
        try ensureOpen()
        _ = try await client.query(query)
    }

    private func queryOptional(_ query: PostgresQuery) async throws -> Data? {
        try ensureOpen()
        let rows = try await client.query(query)
        for try await value in rows.decode(Data.self) { return value }
        return nil
    }

    private func queryOptional(_ connection: PostgresConnection, _ query: PostgresQuery) async throws -> Data? {
        let rows = try await connection.query(query, logger: logger)
        for try await value in rows.decode(Data.self) { return value }
        return nil
    }

    private func queryData(_ query: PostgresQuery) async throws -> [Data] {
        try ensureOpen()
        let rows = try await client.query(query)
        var values: [Data] = []
        for try await value in rows.decode(Data.self) { values.append(value) }
        return values
    }

    private func transaction<T>(_ body: @escaping (PostgresConnection) async throws -> T) async throws -> T {
        try ensureOpen()
        return try await client.withTransaction(logger: logger, body)
    }

    private func writeNode(_ connection: PostgresConnection, _ item: NodeMutationRecord) async throws {
        if item.value.present {
            _ = try await connection.query("INSERT INTO prolly_nodes VALUES (\(item.key), \(item.value.value)) ON CONFLICT(cid) DO UPDATE SET node=excluded.node", logger: logger)
        } else {
            _ = try await connection.query("DELETE FROM prolly_nodes WHERE cid = \(item.key)", logger: logger)
        }
    }

    private func writeRoot(_ connection: PostgresConnection, _ name: Data, _ replacement: OptionalBytesRecord) async throws {
        if replacement.present {
            _ = try await connection.query("INSERT INTO prolly_roots VALUES (\(name), \(replacement.value)) ON CONFLICT(name) DO UPDATE SET manifest=excluded.manifest", logger: logger)
        } else {
            _ = try await connection.query("DELETE FROM prolly_roots WHERE name = \(name)", logger: logger)
        }
    }

    private func ensureOpen() throws { if closed { throw PostgresStoreFailure() } }
    private func matches(_ current: Data?, _ expected: OptionalBytesRecord) -> Bool { expected.present ? current == expected.value : current == nil }
    private func optional(_ value: Data?) -> OptionalBytesRecord { OptionalBytesRecord(present: value != nil, value: value ?? Data()) }

    private func optionalResult(_ body: () async throws -> Data?) async -> OptionalBytesResultRecord {
        do { return OptionalBytesResultRecord(value: optional(try await body()), error: nil) }
        catch { return OptionalBytesResultRecord(value: optional(nil), error: storeError()) }
    }

    private func unitResult(_ body: () async throws -> Void) async -> UnitResultRecord {
        do { try await body(); return UnitResultRecord(error: nil) }
        catch { return UnitResultRecord(error: storeError()) }
    }

    private func storeError() -> StoreErrorRecord {
        StoreErrorRecord(code: "internal", message: "PostgreSQL provider operation failed", retryable: false, providerCode: nil)
    }
}

private extension Array {
    func asyncMap<T>(_ transform: (Element) async throws -> T) async rethrows -> [T] {
        var values: [T] = []
        values.reserveCapacity(count)
        for value in self { values.append(try await transform(value)) }
        return values
    }
}
