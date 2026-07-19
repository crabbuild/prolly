import Foundation
import Prolly
import RediStack

private enum RedisStoreFailure: Error {
    case invalidArgument
    case invalidData
    case closed
}

public actor RedisRemoteStore: ForeignRemoteStore {
    private static let casScript = """
    local current = redis.call('GET', KEYS[1])
    local expected_present = ARGV[1] == '1'
    if expected_present then
      if current == false or current ~= ARGV[2] then
        return {0, current == false and 0 or 1, current or ''}
      end
    elseif current ~= false then
      return {0, 1, current}
    end
    if ARGV[3] == '1' then
      redis.call('SET', KEYS[1], ARGV[4])
      return {1, 1, ARGV[4]}
    end
    redis.call('DEL', KEYS[1])
    return {1, 0, ''}
    """

    private static let mutateScript = """
    for index = 1, #KEYS do
      local offset = (index - 1) * 2
      if ARGV[offset + 1] == '1' then
        redis.call('SET', KEYS[index], ARGV[offset + 2])
      else
        redis.call('DEL', KEYS[index])
      end
    end
    return 1
    """

    private static let transactionScript = """
    local condition_count = tonumber(ARGV[1])
    local node_count = tonumber(ARGV[2])
    local root_count = tonumber(ARGV[3])
    local argument = 4
    for index = 1, condition_count do
      local current = redis.call('GET', KEYS[index])
      local expected_present = ARGV[argument] == '1'
      local matches = (expected_present and current ~= false and current == ARGV[argument + 1])
        or (not expected_present and current == false)
      if not matches then
        return {0, index, current == false and 0 or 1, current or ''}
      end
      argument = argument + 2
    end
    local key_index = condition_count + 1
    for _ = 1, node_count do
      if ARGV[argument] == '1' then redis.call('SET', KEYS[key_index], ARGV[argument + 1])
      else redis.call('DEL', KEYS[key_index]) end
      argument = argument + 2
      key_index = key_index + 1
    end
    for _ = 1, root_count do
      if ARGV[argument] == '1' then redis.call('SET', KEYS[key_index], ARGV[argument + 1])
      else redis.call('DEL', KEYS[key_index]) end
      argument = argument + 2
      key_index = key_index + 1
    end
    return {1}
    """

    private let client: RedisClient
    private let prefix: Data
    private var closed = false

    public init(borrowing client: RedisClient, keyPrefix: Data = Data("prolly:".utf8)) {
        self.client = client
        self.prefix = keyPrefix
    }

    public func close() { closed = true }

    public func clearNamespace() async throws {
        try ensureOpen()
        guard !prefix.isEmpty else { throw RedisStoreFailure.invalidArgument }
        let keys = try await scan(prefix)
        for index in stride(from: 0, to: keys.count, by: 256) {
            let end = min(index + 256, keys.count)
            _ = try await command("DEL", Array(keys[index..<end]))
        }
    }

    public func descriptor() async -> StoreDescriptorResultRecord {
        StoreDescriptorResultRecord(
            value: StoreDescriptorRecord(
                protocolMajor: 2, adapterName: "redis-v1", provider: "redis", schemaVersion: 1,
                capabilities: StoreCapabilitiesRecord(
                    nativeBatchReads: true, atomicBatchWrites: true, nodeScan: true,
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
        await optionalResult { try await get(family("node:", cid)) }
    }

    public func putNode(cid: Data, value: Data) async -> UnitResultRecord {
        await unitResult { _ = try await command("SET", [family("node:", cid), value]) }
    }

    public func deleteNode(cid: Data) async -> UnitResultRecord {
        await unitResult { _ = try await command("DEL", [family("node:", cid)]) }
    }

    public func batchNodes(ops: [NodeMutationRecord]) async -> UnitResultRecord {
        await unitResult {
            var keys: [Data] = []
            var arguments: [Data] = []
            for item in ops {
                keys.append(family("node:", item.key))
                arguments.append(flag(item.value.present))
                arguments.append(item.value.present ? item.value.value : Data())
            }
            try await mutate(keys, arguments)
        }
    }

    public func batchGetNodesOrdered(cids: [Data]) async -> OptionalBytesListResultRecord {
        do {
            if cids.isEmpty { return OptionalBytesListResultRecord(values: [], error: nil) }
            let response = try await command("MGET", cids.map { family("node:", $0) })
            guard let values = response.array, values.count == cids.count else { throw RedisStoreFailure.invalidData }
            return OptionalBytesListResultRecord(values: values.map(optional), error: nil)
        } catch { return OptionalBytesListResultRecord(values: [], error: storeError(error)) }
    }

    public func listNodeCids() async -> BytesListResultRecord {
        do {
            let familyPrefix = family("node:", Data())
            let keys = try await scan(familyPrefix)
            let values: [Data] = keys.compactMap { key in
                let suffix = Data(key.dropFirst(familyPrefix.count))
                return suffix.count == 32 ? suffix : nil
            }.sorted(by: { $0.lexicographicallyPrecedes($1) })
            return BytesListResultRecord(values: values, error: nil)
        } catch { return BytesListResultRecord(values: [], error: storeError(error)) }
    }

    public func getHint(namespace: Data, key: Data) async -> OptionalBytesResultRecord {
        await optionalResult { try await get(hintKey(namespace, key)) }
    }

    public func putHint(namespace: Data, key: Data, value: Data) async -> UnitResultRecord {
        await unitResult { _ = try await command("SET", [hintKey(namespace, key), value]) }
    }

    public func batchPutNodesWithHint(nodes: [NodeEntryRecord], namespace: Data, key: Data, value: Data) async -> UnitResultRecord {
        await unitResult {
            var keys = nodes.map { family("node:", $0.key) }
            var arguments = nodes.flatMap { [flag(true), $0.value] }
            keys.append(hintKey(namespace, key))
            arguments.append(contentsOf: [flag(true), value])
            try await mutate(keys, arguments)
        }
    }

    public func getRootManifest(name: Data) async -> OptionalBytesResultRecord {
        await optionalResult { try await get(family("root:", name)) }
    }

    public func putRootManifest(name: Data, manifest: Data) async -> UnitResultRecord {
        await unitResult { _ = try await command("SET", [family("root:", name), manifest]) }
    }

    public func deleteRootManifest(name: Data) async -> UnitResultRecord {
        await unitResult { _ = try await command("DEL", [family("root:", name)]) }
    }

    public func compareAndSwapRootManifest(name: Data, expected: OptionalBytesRecord, new replacement: OptionalBytesRecord) async -> RootCasResultRecord {
        do {
            try validate(expected); try validate(replacement)
            let response = try await eval(Self.casScript, keys: [family("root:", name)], arguments: [
                flag(expected.present), expected.value, flag(replacement.present), replacement.value,
            ])
            guard let values = response.array, values.count >= 3,
                  let applied = values[0].int, let present = values[1].int else {
                throw RedisStoreFailure.invalidData
            }
            return RootCasResultRecord(applied: applied == 1, current: try optional(present, values[2]), error: nil)
        } catch { return RootCasResultRecord(applied: false, current: optional(nil), error: storeError(error)) }
    }

    public func listRootManifests() async -> NamedBytesListResultRecord {
        do {
            let familyPrefix = family("root:", Data())
            let keys = try await scan(familyPrefix).sorted(by: { $0.lexicographicallyPrecedes($1) })
            if keys.isEmpty { return NamedBytesListResultRecord(values: [], error: nil) }
            let response = try await command("MGET", keys)
            guard let manifests = response.array, manifests.count == keys.count else { throw RedisStoreFailure.invalidData }
            var values: [NamedBytesRecord] = []
            for (key, manifest) in zip(keys, manifests) {
                if let data = manifest.data {
                    values.append(NamedBytesRecord(name: Data(key.dropFirst(familyPrefix.count)), value: data))
                }
            }
            return NamedBytesListResultRecord(values: values, error: nil)
        } catch { return NamedBytesListResultRecord(values: [], error: storeError(error)) }
    }

    public func commitTransaction(nodes: [NodeMutationRecord], conditions: [RootConditionRecord], roots: [RootWriteRecord]) async -> TransactionResultRecord {
        do {
            for condition in conditions { try validate(condition.expected) }
            for root in roots { try validate(root.replacement) }
            var keys = conditions.map { family("root:", $0.name) }
            keys.append(contentsOf: nodes.map { family("node:", $0.key) })
            keys.append(contentsOf: roots.map { family("root:", $0.name) })
            var arguments = [ascii(conditions.count), ascii(nodes.count), ascii(roots.count)]
            for condition in conditions { arguments.append(contentsOf: [flag(condition.expected.present), condition.expected.value]) }
            for node in nodes { arguments.append(contentsOf: [flag(node.value.present), node.value.value]) }
            for root in roots { arguments.append(contentsOf: [flag(root.replacement.present), root.replacement.value]) }
            let response = try await eval(Self.transactionScript, keys: keys, arguments: arguments)
            guard let values = response.array, let applied = values.first?.int else { throw RedisStoreFailure.invalidData }
            if applied == 1 { return TransactionResultRecord(applied: true, conflict: nil, error: nil) }
            guard values.count >= 4, let rawIndex = values[1].int, let present = values[2].int else { throw RedisStoreFailure.invalidData }
            let index = rawIndex - 1
            guard conditions.indices.contains(index) else { throw RedisStoreFailure.invalidData }
            let condition = conditions[index]
            let conflict = StoreTransactionConflictRecord(
                name: condition.name, expected: condition.expected,
                current: try optional(present, values[3])
            )
            return TransactionResultRecord(applied: false, conflict: conflict, error: nil)
        } catch { return TransactionResultRecord(applied: false, conflict: nil, error: storeError(error)) }
    }

    private func get(_ key: Data) async throws -> Data? {
        let response = try await command("GET", [key])
        return response.isNull ? nil : response.data
    }

    private func mutate(_ keys: [Data], _ arguments: [Data]) async throws {
        if !keys.isEmpty { _ = try await eval(Self.mutateScript, keys: keys, arguments: arguments) }
    }

    private func eval(_ script: String, keys: [Data], arguments: [Data]) async throws -> RESPValue {
        try await command("EVAL", [Data(script.utf8), ascii(keys.count)] + keys + arguments)
    }

    private func command(_ name: String, _ arguments: [Data]) async throws -> RESPValue {
        try ensureOpen()
        return try await client.send(command: name, with: arguments.map { RESPValue(from: $0) }).get()
    }

    private func scan(_ familyPrefix: Data) async throws -> [Data] {
        var cursor = 0
        var keys: [Data] = []
        repeat {
            let response = try await command("SCAN", [ascii(cursor), Data("COUNT".utf8), ascii(1024)])
            guard let values = response.array, values.count == 2,
                  let cursorText = values[0].string, let next = Int(cursorText),
                  let page = values[1].array else { throw RedisStoreFailure.invalidData }
            cursor = next
            for value in page {
                guard let key = value.data else { throw RedisStoreFailure.invalidData }
                if key.starts(with: familyPrefix) { keys.append(key) }
            }
        } while cursor != 0
        return keys
    }

    private func family(_ name: String, _ suffix: Data) -> Data {
        var value = prefix
        value.append(contentsOf: name.utf8)
        value.append(suffix)
        return value
    }

    private func hintKey(_ namespace: Data, _ key: Data) -> Data {
        var value = family("hint:", Data())
        var length = UInt64(namespace.count).bigEndian
        withUnsafeBytes(of: &length) { value.append(contentsOf: $0) }
        value.append(namespace)
        value.append(key)
        return value
    }

    private func ensureOpen() throws { if closed { throw RedisStoreFailure.closed } }
    private func validate(_ value: OptionalBytesRecord) throws {
        if !value.present && !value.value.isEmpty { throw RedisStoreFailure.invalidArgument }
    }
    private func flag(_ value: Bool) -> Data { Data(value ? "1".utf8 : "0".utf8) }
    private func ascii(_ value: Int) -> Data { Data(String(value).utf8) }
    private func optional(_ data: Data?) -> OptionalBytesRecord { OptionalBytesRecord(present: data != nil, value: data ?? Data()) }
    private func optional(_ value: RESPValue) -> OptionalBytesRecord { optional(value.isNull ? nil : value.data) }
    private func optional(_ present: Int, _ value: RESPValue) throws -> OptionalBytesRecord {
        if present == 0 { return optional(nil) }
        guard present == 1, let data = value.data else { throw RedisStoreFailure.invalidData }
        return optional(data)
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
        case RedisStoreFailure.invalidArgument: code = "invalid_argument"
        case RedisStoreFailure.invalidData: code = "invalid_data"
        case RedisStoreFailure.closed: code = "closed"
        default: code = "internal"
        }
        return StoreErrorRecord(code: code, message: "Redis provider operation failed", retryable: false, providerCode: nil)
    }
}
