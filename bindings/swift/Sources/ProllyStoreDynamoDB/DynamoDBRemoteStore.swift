import Foundation
import Prolly
import SotoCore
import SotoDynamoDB

private enum DynamoStoreFailure: Error { case invalidArgument, invalidData, resourceExhausted, closed }

public actor DynamoDBRemoteStore: ForeignRemoteStore {
    private let dynamo: DynamoDB
    private let table: String
    private let prefix: Data
    private var closed = false

    public init(borrowing dynamo: DynamoDB, tableName: String, keyPrefix: Data = Data("prolly:".utf8)) {
        self.dynamo = dynamo
        self.table = tableName
        self.prefix = keyPrefix
    }

    public func close() { closed = true }

    public func initializeTable() async throws {
        try ensureOpen()
        do {
            let output = try await dynamo.describeTable(.init(tableName: table))
            try validateTable(output.table)
            return
        } catch let error as DynamoDBErrorType where error == .resourceNotFoundException {}
        do {
            _ = try await dynamo.createTable(.init(
                attributeDefinitions: [.init(attributeName: "pk", attributeType: .b)],
                billingMode: .payPerRequest,
                keySchema: [.init(attributeName: "pk", keyType: .hash)], tableName: table
            ))
        } catch let error as DynamoDBErrorType where error == .resourceInUseException {}
        for _ in 0..<100 {
            do {
                let output = try await dynamo.describeTable(.init(tableName: table))
                if output.table?.tableStatus == .active { try validateTable(output.table); return }
            } catch let error as DynamoDBErrorType where error == .resourceNotFoundException {}
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        throw DynamoStoreFailure.resourceExhausted
    }

    public func descriptor() async -> StoreDescriptorResultRecord {
        StoreDescriptorResultRecord(value: StoreDescriptorRecord(
            protocolMajor: 2, adapterName: "dynamodb-v1", provider: "dynamodb", schemaVersion: 1,
            capabilities: StoreCapabilitiesRecord(
                nativeBatchReads: true, atomicBatchWrites: false, nodeScan: true, hints: true,
                atomicNodesAndHint: false, rootScan: true, rootCompareAndSwap: true,
                transactions: true, readParallelism: 1
            ),
            limits: StoreLimitsRecord(
                maxBatchReadItems: 100, maxBatchWriteItems: 25,
                maxTransactionOperations: 100, maxNodeBytes: nil
            )
        ), error: nil)
    }

    public func getNode(cid: Data) async -> OptionalBytesResultRecord { await optionalResult { try await get(family("node:", cid)) } }
    public func putNode(cid: Data, value: Data) async -> UnitResultRecord { await unitResult { try await put(family("node:", cid), value) } }
    public func deleteNode(cid: Data) async -> UnitResultRecord { await unitResult { try await delete(family("node:", cid)) } }

    public func batchNodes(ops: [NodeMutationRecord]) async -> UnitResultRecord {
        await unitResult {
            let requests = ops.map { operation -> DynamoDB.WriteRequest in
                let key = family("node:", operation.key)
                return operation.value.present
                    ? .init(putRequest: .init(item: item(key, operation.value.value)))
                    : .init(deleteRequest: .init(key: keyItem(key)))
            }
            try await batchWrite(requests)
        }
    }

    public func batchGetNodesOrdered(cids: [Data]) async -> OptionalBytesListResultRecord {
        do {
            let storageKeys = cids.map { family("node:", $0) }
            var unique: [Data] = []; var seen = Set<Data>()
            for key in storageKeys where seen.insert(key).inserted { unique.append(key) }
            var found: [Data: Data] = [:]
            for start in stride(from: 0, to: unique.count, by: 100) {
                var pending = Array(unique[start..<min(start + 100, unique.count)]).map(keyItem)
                for attempt in 0..<8 {
                    if pending.isEmpty { break }
                    let request = DynamoDB.KeysAndAttributes(
                        consistentRead: true, expressionAttributeNames: ["#pk": "pk", "#value": "value"],
                        keys: pending, projectionExpression: "#pk, #value"
                    )
                    let output = try await dynamo.batchGetItem(.init(requestItems: [table: request]))
                    for entry in output.responses?[table] ?? [] { found[try binary(entry, "pk")] = try binary(entry, "value") }
                    pending = output.unprocessedKeys?[table]?.keys ?? []
                    if !pending.isEmpty {
                        if attempt == 7 { throw DynamoStoreFailure.resourceExhausted }
                        try await Task.sleep(nanoseconds: UInt64(10_000_000 * (1 << min(attempt, 6))))
                    }
                }
            }
            return OptionalBytesListResultRecord(values: storageKeys.map { optional(found[$0]) }, error: nil)
        } catch { return OptionalBytesListResultRecord(values: [], error: storeError(error)) }
    }

    public func listNodeCids() async -> BytesListResultRecord {
        do {
            let head = family("node:", Data())
            let values = try await scanKeys(head).compactMap { key -> Data? in
                let suffix = Data(key.dropFirst(head.count)); return suffix.count == 32 ? suffix : nil
            }.sorted(by: { $0.lexicographicallyPrecedes($1) })
            return BytesListResultRecord(values: values, error: nil)
        } catch { return BytesListResultRecord(values: [], error: storeError(error)) }
    }

    public func getHint(namespace: Data, key: Data) async -> OptionalBytesResultRecord { await optionalResult { try await get(hintKey(namespace, key)) } }
    public func putHint(namespace: Data, key: Data, value: Data) async -> UnitResultRecord { await unitResult { try await put(hintKey(namespace, key), value) } }
    public func batchPutNodesWithHint(nodes: [NodeEntryRecord], namespace: Data, key: Data, value: Data) async -> UnitResultRecord {
        let mutations = nodes.map { NodeMutationRecord(key: $0.key, value: OptionalBytesRecord(present: true, value: $0.value)) }
        let result = await batchNodes(ops: mutations)
        return result.error == nil ? await putHint(namespace: namespace, key: key, value: value) : result
    }

    public func getRootManifest(name: Data) async -> OptionalBytesResultRecord { await optionalResult { try await get(family("root:", name)) } }
    public func putRootManifest(name: Data, manifest: Data) async -> UnitResultRecord { await unitResult { try await put(family("root:", name), manifest) } }
    public func deleteRootManifest(name: Data) async -> UnitResultRecord { await unitResult { try await delete(family("root:", name)) } }

    public func compareAndSwapRootManifest(name: Data, expected: OptionalBytesRecord, new replacement: OptionalBytesRecord) async -> RootCasResultRecord {
        do {
            try validate(expected); try validate(replacement); try ensureOpen()
            let key = family("root:", name); let condition = condition(expected)
            do {
                if replacement.present {
                    _ = try await dynamo.putItem(DynamoDB.PutItemInput(
                        conditionExpression: condition.expression, expressionAttributeNames: condition.names,
                        expressionAttributeValues: condition.values, item: item(key, replacement.value), tableName: table
                    ))
                } else {
                    _ = try await dynamo.deleteItem(.init(
                        conditionExpression: condition.expression, expressionAttributeNames: condition.names,
                        expressionAttributeValues: condition.values, key: keyItem(key), tableName: table
                    ))
                }
                return RootCasResultRecord(applied: true, current: replacement, error: nil)
            } catch let error as DynamoDBErrorType where error == .conditionalCheckFailedException {
                return RootCasResultRecord(applied: false, current: optional(try await get(key)), error: nil)
            }
        } catch { return RootCasResultRecord(applied: false, current: optional(nil), error: storeError(error)) }
    }

    public func listRootManifests() async -> NamedBytesListResultRecord {
        do {
            let head = family("root:", Data()); let keys = try await scanKeys(head).sorted(by: { $0.lexicographicallyPrecedes($1) })
            var values: [NamedBytesRecord] = []
            for key in keys { if let value = try await get(key) { values.append(.init(name: Data(key.dropFirst(head.count)), value: value)) } }
            return NamedBytesListResultRecord(values: values, error: nil)
        } catch { return NamedBytesListResultRecord(values: [], error: storeError(error)) }
    }

    public func commitTransaction(nodes: [NodeMutationRecord], conditions: [RootConditionRecord], roots: [RootWriteRecord]) async -> TransactionResultRecord {
        do {
            let written = Set(roots.map(\.name)); let count = nodes.count + roots.count + conditions.filter { !written.contains($0.name) }.count
            guard count <= 100 else { throw DynamoStoreFailure.resourceExhausted }
            for value in conditions { try validate(value.expected) }; for value in roots { try validate(value.replacement) }
            var byName: [Data: RootConditionRecord] = [:]
            for condition in conditions { byName[condition.name] = condition }
            var items: [DynamoDB.TransactWriteItem] = []
            for check in conditions where !written.contains(check.name) {
                let value = condition(check.expected)
                items.append(.conditionCheck(.init(
                    conditionExpression: value.expression, expressionAttributeNames: value.names,
                    expressionAttributeValues: value.values, key: keyItem(family("root:", check.name)), tableName: table
                )))
            }
            for root in roots {
                let value = byName[root.name].map { condition($0.expected) }
                if root.replacement.present {
                    items.append(.put(.init(
                        conditionExpression: value?.expression, expressionAttributeNames: value?.names,
                        expressionAttributeValues: value?.values, item: item(family("root:", root.name), root.replacement.value), tableName: table
                    )))
                } else {
                    items.append(.delete(.init(
                        conditionExpression: value?.expression, expressionAttributeNames: value?.names,
                        expressionAttributeValues: value?.values, key: keyItem(family("root:", root.name)), tableName: table
                    )))
                }
            }
            for node in nodes {
                let key = family("node:", node.key)
                items.append(node.value.present
                    ? .put(.init(item: item(key, node.value.value), tableName: table))
                    : .delete(.init(key: keyItem(key), tableName: table)))
            }
            if items.isEmpty { return TransactionResultRecord(applied: true, conflict: nil, error: nil) }
            do {
                _ = try await dynamo.transactWriteItems(.init(transactItems: items))
                return TransactionResultRecord(applied: true, conflict: nil, error: nil)
            } catch let error as DynamoDBErrorType where error == .transactionCanceledException {
                for check in conditions {
                    let current = try await get(family("root:", check.name))
                    if !matches(current, check.expected) {
                        return TransactionResultRecord(applied: false, conflict: .init(name: check.name, expected: check.expected, current: optional(current)), error: nil)
                    }
                }
                throw error
            }
        } catch { return TransactionResultRecord(applied: false, conflict: nil, error: storeError(error)) }
    }

    private func get(_ key: Data) async throws -> Data? {
        try ensureOpen()
        let output = try await dynamo.getItem(.init(
            consistentRead: true, expressionAttributeNames: ["#value": "value"], key: keyItem(key),
            projectionExpression: "#value", tableName: table
        ))
        guard let item = output.item, !item.isEmpty else { return nil }; return try binary(item, "value")
    }
    private func put(_ key: Data, _ value: Data) async throws { try ensureOpen(); _ = try await dynamo.putItem(DynamoDB.PutItemInput(item: item(key, value), tableName: table)) }
    private func delete(_ key: Data) async throws { try ensureOpen(); _ = try await dynamo.deleteItem(.init(key: keyItem(key), tableName: table)) }

    private func batchWrite(_ requests: [DynamoDB.WriteRequest]) async throws {
        try ensureOpen()
        for start in stride(from: 0, to: requests.count, by: 25) {
            var pending = Array(requests[start..<min(start + 25, requests.count)])
            for attempt in 0..<8 {
                if pending.isEmpty { break }
                let output = try await dynamo.batchWriteItem(.init(requestItems: [table: pending]))
                pending = output.unprocessedItems?[table] ?? []
                if !pending.isEmpty {
                    if attempt == 7 { throw DynamoStoreFailure.resourceExhausted }
                    try await Task.sleep(nanoseconds: UInt64(10_000_000 * (1 << min(attempt, 6))))
                }
            }
        }
    }

    private func scanKeys(_ head: Data) async throws -> [Data] {
        try ensureOpen(); var start: [String: DynamoDB.AttributeValue]?; var keys: [Data] = []
        repeat {
            let output = try await dynamo.scan(.init(
                consistentRead: true, exclusiveStartKey: start,
                expressionAttributeNames: ["#pk": "pk"], expressionAttributeValues: [":prefix": attribute(head)],
                filterExpression: "begins_with(#pk, :prefix)", projectionExpression: "#pk", tableName: table
            ))
            for entry in output.items ?? [] { keys.append(try binary(entry, "pk")) }
            start = output.lastEvaluatedKey
        } while !(start?.isEmpty ?? true)
        return keys
    }

    private func family(_ name: String, _ suffix: Data) -> Data { var value = prefix; value.append(contentsOf: name.utf8); value.append(suffix); return value }
    private func hintKey(_ namespace: Data, _ key: Data) -> Data { var value = family("hint:", Data()); var size = UInt64(namespace.count).bigEndian; withUnsafeBytes(of: &size) { value.append(contentsOf: $0) }; value.append(namespace); value.append(key); return value }
    private func attribute(_ value: Data) -> DynamoDB.AttributeValue { .b(.data(value)) }
    private func keyItem(_ key: Data) -> [String: DynamoDB.AttributeValue] { ["pk": attribute(key)] }
    private func item(_ key: Data, _ value: Data) -> [String: DynamoDB.AttributeValue] { ["pk": attribute(key), "value": attribute(value)] }
    private func binary(_ item: [String: DynamoDB.AttributeValue], _ name: String) throws -> Data { guard case .b(let value)? = item[name], let bytes = value.decoded() else { throw DynamoStoreFailure.invalidData }; return Data(bytes) }
    private func condition(_ value: OptionalBytesRecord) -> (expression: String, names: [String: String], values: [String: DynamoDB.AttributeValue]?) { value.present ? ("#value = :expected", ["#value": "value"], [":expected": attribute(value.value)]) : ("attribute_not_exists(#pk)", ["#pk": "pk"], nil) }
    private func ensureOpen() throws { if closed { throw DynamoStoreFailure.closed }; if table.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty { throw DynamoStoreFailure.invalidArgument } }
    private func validate(_ value: OptionalBytesRecord) throws { if !value.present && !value.value.isEmpty { throw DynamoStoreFailure.invalidArgument } }
    private func validateTable(_ value: DynamoDB.TableDescription?) throws { guard value?.keySchema?.count == 1, value?.keySchema?.first?.attributeName == "pk", value?.keySchema?.first?.keyType == .hash, value?.attributeDefinitions?.contains(where: { $0.attributeName == "pk" && $0.attributeType == .b }) == true else { throw DynamoStoreFailure.invalidArgument } }
    private func optional(_ value: Data?) -> OptionalBytesRecord { .init(present: value != nil, value: value ?? Data()) }
    private func matches(_ current: Data?, _ expected: OptionalBytesRecord) -> Bool { expected.present ? current == expected.value : current == nil }
    private func optionalResult(_ body: () async throws -> Data?) async -> OptionalBytesResultRecord { do { return .init(value: optional(try await body()), error: nil) } catch { return .init(value: optional(nil), error: storeError(error)) } }
    private func unitResult(_ body: () async throws -> Void) async -> UnitResultRecord { do { try await body(); return .init(error: nil) } catch { return .init(error: storeError(error)) } }
    private func storeError(_ error: Error) -> StoreErrorRecord { let code: String; switch error { case DynamoStoreFailure.invalidArgument: code = "invalid_argument"; case DynamoStoreFailure.invalidData: code = "invalid_data"; case DynamoStoreFailure.resourceExhausted: code = "resource_exhausted"; case DynamoStoreFailure.closed: code = "closed"; default: code = "internal" }; return .init(code: code, message: "DynamoDB provider operation failed", retryable: false, providerCode: String(describing: error)) }
}
