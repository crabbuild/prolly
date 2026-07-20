import CSQLite
import Foundation
import Prolly

private let sqliteTransient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

private struct SQLiteStoreFailure: Error {}

public actor SQLiteRemoteStore: ForeignRemoteStore {
    public static let schema = """
    CREATE TABLE IF NOT EXISTS prolly_nodes (cid BLOB PRIMARY KEY NOT NULL, node BLOB NOT NULL) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS prolly_hints (namespace BLOB NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY (namespace, key)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS prolly_roots (name BLOB PRIMARY KEY NOT NULL, manifest BLOB NOT NULL) WITHOUT ROWID;
    """

    private let database: OpaquePointer
    private var closed = false

    public init(borrowing database: OpaquePointer) {
        self.database = database
    }

    public func initializeSchema() throws {
        try executeScript(Self.schema)
    }

    public func close() {
        closed = true
    }

    public func descriptor() async -> StoreDescriptorResultRecord {
        let capabilities = StoreCapabilitiesRecord(
            nativeBatchReads: true, atomicBatchWrites: true, nodeScan: true,
            hints: true, atomicNodesAndHint: true, rootScan: true,
            rootCompareAndSwap: true, transactions: true, readParallelism: 16
        )
        let limits = StoreLimitsRecord(
            maxBatchReadItems: nil, maxBatchWriteItems: nil,
            maxTransactionOperations: nil, maxNodeBytes: nil
        )
        return StoreDescriptorResultRecord(
            value: StoreDescriptorRecord(
                protocolMajor: 2, adapterName: "sqlite-v1", provider: "sqlite",
                schemaVersion: 1, capabilities: capabilities, limits: limits
            ),
            error: nil
        )
    }

    public func getNode(cid: Data) async -> OptionalBytesResultRecord {
        optionalResult { try queryOptional("SELECT node FROM prolly_nodes WHERE cid = ?", [cid]) }
    }

    public func putNode(cid: Data, value: Data) async -> UnitResultRecord {
        unitResult { try execute("INSERT INTO prolly_nodes VALUES (?, ?) ON CONFLICT(cid) DO UPDATE SET node=excluded.node", [cid, value]) }
    }

    public func deleteNode(cid: Data) async -> UnitResultRecord {
        unitResult { try execute("DELETE FROM prolly_nodes WHERE cid = ?", [cid]) }
    }

    public func batchNodes(ops: [NodeMutationRecord]) async -> UnitResultRecord {
        unitResult {
            try transaction {
                for item in ops {
                    if item.value.present {
                        try execute("INSERT INTO prolly_nodes VALUES (?, ?) ON CONFLICT(cid) DO UPDATE SET node=excluded.node", [item.key, item.value.value])
                    } else {
                        try execute("DELETE FROM prolly_nodes WHERE cid = ?", [item.key])
                    }
                }
            }
        }
    }

    public func batchGetNodesOrdered(cids: [Data]) async -> OptionalBytesListResultRecord {
        do {
            return OptionalBytesListResultRecord(
                values: try cids.map { optional(try queryOptional("SELECT node FROM prolly_nodes WHERE cid = ?", [$0])) },
                error: nil
            )
        } catch {
            return OptionalBytesListResultRecord(values: [], error: storeError())
        }
    }

    public func listNodeCids() async -> BytesListResultRecord {
        do { return BytesListResultRecord(values: try queryPairs("SELECT cid, cid FROM prolly_nodes ORDER BY cid").map(\.0), error: nil) }
        catch { return BytesListResultRecord(values: [], error: storeError()) }
    }

    public func getHint(namespace: Data, key: Data) async -> OptionalBytesResultRecord {
        optionalResult { try queryOptional("SELECT value FROM prolly_hints WHERE namespace = ? AND key = ?", [namespace, key]) }
    }

    public func putHint(namespace: Data, key: Data, value: Data) async -> UnitResultRecord {
        unitResult { try execute("INSERT INTO prolly_hints VALUES (?, ?, ?) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value", [namespace, key, value]) }
    }

    public func batchPutNodesWithHint(nodes: [NodeEntryRecord], namespace: Data, key: Data, value: Data) async -> UnitResultRecord {
        unitResult {
            try transaction {
                for node in nodes {
                    try execute("INSERT INTO prolly_nodes VALUES (?, ?) ON CONFLICT(cid) DO UPDATE SET node=excluded.node", [node.key, node.value])
                }
                try execute("INSERT INTO prolly_hints VALUES (?, ?, ?) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value", [namespace, key, value])
            }
        }
    }

    public func getRootManifest(name: Data) async -> OptionalBytesResultRecord {
        optionalResult { try queryOptional("SELECT manifest FROM prolly_roots WHERE name = ?", [name]) }
    }

    public func putRootManifest(name: Data, manifest: Data) async -> UnitResultRecord {
        unitResult { try execute("INSERT INTO prolly_roots VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET manifest=excluded.manifest", [name, manifest]) }
    }

    public func deleteRootManifest(name: Data) async -> UnitResultRecord {
        unitResult { try execute("DELETE FROM prolly_roots WHERE name = ?", [name]) }
    }

    public func compareAndSwapRootManifest(name: Data, expected: OptionalBytesRecord, new replacement: OptionalBytesRecord) async -> RootCasResultRecord {
        do {
            var applied = false
            var current: Data?
            try transaction {
                current = try queryOptional("SELECT manifest FROM prolly_roots WHERE name = ?", [name])
                if matches(current, expected) {
                    try writeRoot(name, replacement)
                    applied = true
                }
            }
            return RootCasResultRecord(applied: applied, current: optional(current), error: nil)
        } catch {
            return RootCasResultRecord(applied: false, current: optional(nil), error: storeError())
        }
    }

    public func listRootManifests() async -> NamedBytesListResultRecord {
        do {
            let values = try queryPairs("SELECT name, manifest FROM prolly_roots ORDER BY name")
                .map { NamedBytesRecord(name: $0.0, value: $0.1) }
            return NamedBytesListResultRecord(values: values, error: nil)
        } catch {
            return NamedBytesListResultRecord(values: [], error: storeError())
        }
    }

    public func commitTransaction(nodes: [NodeMutationRecord], conditions: [RootConditionRecord], roots: [RootWriteRecord]) async -> TransactionResultRecord {
        do {
            var conflict: StoreTransactionConflictRecord?
            try transaction {
                for condition in conditions {
                    let current = try queryOptional("SELECT manifest FROM prolly_roots WHERE name = ?", [condition.name])
                    if !matches(current, condition.expected) {
                        conflict = StoreTransactionConflictRecord(name: condition.name, expected: condition.expected, current: optional(current))
                        return
                    }
                }
                for item in nodes {
                    if item.value.present {
                        try execute("INSERT INTO prolly_nodes VALUES (?, ?) ON CONFLICT(cid) DO UPDATE SET node=excluded.node", [item.key, item.value.value])
                    } else {
                        try execute("DELETE FROM prolly_nodes WHERE cid = ?", [item.key])
                    }
                }
                for root in roots { try writeRoot(root.name, root.replacement) }
            }
            return TransactionResultRecord(applied: conflict == nil, conflict: conflict, error: nil)
        } catch {
            return TransactionResultRecord(applied: false, conflict: nil, error: storeError())
        }
    }

    private func ensureOpen() throws {
        if closed { throw SQLiteStoreFailure() }
    }

    private func executeScript(_ sql: String) throws {
        try ensureOpen()
        guard sqlite3_exec(database, sql, nil, nil, nil) == SQLITE_OK else { throw SQLiteStoreFailure() }
    }

    private func execute(_ sql: String, _ values: [Data]) throws {
        let statement = try prepare(sql)
        defer { sqlite3_finalize(statement) }
        try bind(values, to: statement)
        guard sqlite3_step(statement) == SQLITE_DONE else { throw SQLiteStoreFailure() }
    }

    private func queryOptional(_ sql: String, _ values: [Data]) throws -> Data? {
        let statement = try prepare(sql)
        defer { sqlite3_finalize(statement) }
        try bind(values, to: statement)
        let result = sqlite3_step(statement)
        if result == SQLITE_DONE { return nil }
        guard result == SQLITE_ROW else { throw SQLiteStoreFailure() }
        return columnData(statement, 0)
    }

    private func queryPairs(_ sql: String) throws -> [(Data, Data)] {
        let statement = try prepare(sql)
        defer { sqlite3_finalize(statement) }
        var values: [(Data, Data)] = []
        while true {
            let result = sqlite3_step(statement)
            if result == SQLITE_DONE { return values }
            guard result == SQLITE_ROW else { throw SQLiteStoreFailure() }
            values.append((columnData(statement, 0), columnData(statement, 1)))
        }
    }

    private func prepare(_ sql: String) throws -> OpaquePointer {
        try ensureOpen()
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(database, sql, -1, &statement, nil) == SQLITE_OK, let statement else { throw SQLiteStoreFailure() }
        return statement
    }

    private func bind(_ values: [Data], to statement: OpaquePointer) throws {
        for (offset, value) in values.enumerated() {
            let result = value.withUnsafeBytes { bytes in
                sqlite3_bind_blob(statement, Int32(offset + 1), bytes.baseAddress, Int32(bytes.count), sqliteTransient)
            }
            guard result == SQLITE_OK else { throw SQLiteStoreFailure() }
        }
    }

    private func columnData(_ statement: OpaquePointer, _ column: Int32) -> Data {
        let count = Int(sqlite3_column_bytes(statement, column))
        guard count > 0, let pointer = sqlite3_column_blob(statement, column) else { return Data() }
        return Data(bytes: pointer, count: count)
    }

    private func transaction(_ body: () throws -> Void) throws {
        try executeScript("BEGIN IMMEDIATE")
        do {
            try body()
            try executeScript("COMMIT")
        } catch {
            try? executeScript("ROLLBACK")
            throw error
        }
    }

    private func writeRoot(_ name: Data, _ replacement: OptionalBytesRecord) throws {
        if replacement.present {
            try execute("INSERT INTO prolly_roots VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET manifest=excluded.manifest", [name, replacement.value])
        } else {
            try execute("DELETE FROM prolly_roots WHERE name = ?", [name])
        }
    }

    private func matches(_ current: Data?, _ expected: OptionalBytesRecord) -> Bool {
        expected.present ? current == expected.value : current == nil
    }

    private func optional(_ value: Data?) -> OptionalBytesRecord {
        OptionalBytesRecord(present: value != nil, value: value ?? Data())
    }

    private func optionalResult(_ body: () throws -> Data?) -> OptionalBytesResultRecord {
        do { return OptionalBytesResultRecord(value: optional(try body()), error: nil) }
        catch { return OptionalBytesResultRecord(value: optional(nil), error: storeError()) }
    }

    private func unitResult(_ body: () throws -> Void) -> UnitResultRecord {
        do { try body(); return UnitResultRecord(error: nil) }
        catch { return UnitResultRecord(error: storeError()) }
    }

    private func storeError() -> StoreErrorRecord {
        StoreErrorRecord(code: "internal", message: "SQLite provider operation failed", retryable: false, providerCode: nil)
    }
}
