import Foundation
import MySQLNIO
import NIOPosix
import Prolly
import ProllyStoreMySQL

@main
struct StoreMySQLCheck {
    static func main() async throws {
        guard let raw = ProcessInfo.processInfo.environment["PROLLY_MYSQL_URL"],
              let url = URLComponents(string: raw), let host = url.host,
              let user = url.user, let database = url.path.split(separator: "/").last.map(String.init)
        else { fatalError("PROLLY_MYSQL_URL is required") }

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let connection = try await MySQLConnection.connect(
            to: .makeAddressResolvingHost(host, port: url.port ?? 3306),
            username: user, database: database, password: url.password,
            tlsConfiguration: nil, on: group.next()
        ).get()
        do {
            let store = MySQLRemoteStore(borrowing: connection)
            try await store.initializeSchema()
            for table in ["prolly_nodes", "prolly_hints", "prolly_roots"] {
                _ = try await connection.simpleQuery("TRUNCATE \(table)").get()
            }
            let engine = try await AsyncProllyEngine(store: store, config: defaultConfig())
            let tree = try await engine.put(tree: engine.create(), key: Data("key".utf8), value: Data("value".utf8))
            guard try await engine.get(tree: tree, key: Data("key".utf8)) == Data("value".utf8) else {
                fatalError("Swift MySQL round trip failed")
            }
            let missing = OptionalBytesRecord(present: false, value: Data())
            var winners = 0
            for index in 0..<32 {
                let result = await store.compareAndSwapRootManifest(
                    name: Data("main".utf8), expected: missing,
                    new: OptionalBytesRecord(present: true, value: Data([UInt8(index)]))
                )
                if result.applied { winners += 1 }
            }
            guard winners == 1 else { fatalError("Swift MySQL CAS failed") }

            let transaction = await store.commitTransaction(
                nodes: [NodeMutationRecord(
                    key: Data(repeating: 120, count: 32),
                    value: OptionalBytesRecord(present: true, value: Data("must-not-write".utf8))
                )],
                conditions: [RootConditionRecord(name: Data("main".utf8), expected: missing)],
                roots: []
            )
            guard !transaction.applied else { fatalError("Swift MySQL rollback failed") }
            let rolledBack = await store.getNode(cid: Data(repeating: 120, count: 32))
            guard !rolledBack.value.present else { fatalError("Swift MySQL rollback wrote a node") }

            let invalid = await store.putNode(cid: Data(repeating: 120, count: 33), value: Data())
            guard invalid.error?.code == "invalid_argument" else { fatalError("Swift MySQL CID limit failed") }
            await store.close()
            _ = try await connection.simpleQuery("SELECT 1").get()
            print("Swift MySQL remote store passed")
        }
        _ = try await connection.close().get()
        try await group.shutdownGracefully()
    }
}
