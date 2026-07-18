import Foundation
import Prolly
import ProllyStoreDynamoDB
import SotoCore
import SotoDynamoDB

@main
struct StoreDynamoDBCheck {
    static func main() async throws {
        guard let endpoint = ProcessInfo.processInfo.environment["PROLLY_DYNAMODB_ENDPOINT"] else { fatalError("PROLLY_DYNAMODB_ENDPOINT is required") }
        let client = AWSClient(credentialProvider: .static(accessKeyId: "local", secretAccessKey: "local"))
        do {
            let dynamo = DynamoDB(client: client, region: .uswest2, endpoint: endpoint)
            let table = "prolly_swift_\(UUID().uuidString.replacingOccurrences(of: "-", with: ""))"
            let prefix = Data("prolly:test:swift:".utf8)
            let store = DynamoDBRemoteStore(borrowing: dynamo, tableName: table, keyPrefix: prefix)
            try await store.initializeTable()
            let probe = await store.putNode(cid: Data("probe".utf8), value: Data("value".utf8))
            if let error = probe.error { fatalError("Swift direct put failed: \(error.code) \(error.providerCode ?? "")") }
            let engine = try await AsyncProllyEngine(store: store, config: defaultConfig())
            let tree = try await engine.put(tree: engine.create(), key: Data("key".utf8), value: Data("value".utf8))
            guard try await engine.get(tree: tree, key: Data("key".utf8)) == Data("value".utf8) else { fatalError("Swift DynamoDB round trip failed") }
            let missing = OptionalBytesRecord(present: false, value: Data())
            var winners = 0
            for index in 0..<32 {
                let result = await store.compareAndSwapRootManifest(name: Data("main".utf8), expected: missing, new: .init(present: true, value: Data([UInt8(index)])))
                if result.applied { winners += 1 }
            }
            guard winners == 1 else { fatalError("Swift DynamoDB CAS failed") }
            let transaction = await store.commitTransaction(
                nodes: [.init(key: Data("rollback".utf8), value: .init(present: true, value: Data("bad".utf8)))],
                conditions: [.init(name: Data("main".utf8), expected: missing)], roots: []
            )
            guard !transaction.applied, !(await store.getNode(cid: Data("rollback".utf8))).value.present else { fatalError("Swift DynamoDB rollback failed") }
            await store.close()
            _ = try await dynamo.describeTable(.init(tableName: table))
            _ = try? await dynamo.deleteTable(.init(tableName: table))
        } catch {
            try? await client.shutdown()
            throw error
        }
        try await client.shutdown()
        print("Swift DynamoDB remote store passed")
    }
}
