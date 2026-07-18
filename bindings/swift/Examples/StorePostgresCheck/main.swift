import Foundation
import PostgresNIO
import Prolly
import ProllyStorePostgres

@main
struct StorePostgresCheck {
    static func main() async throws {
        guard let url = ProcessInfo.processInfo.environment["PROLLY_POSTGRES_URL"],
              let components = URLComponents(string: url),
              let host = components.host,
              let user = components.user,
              let database = components.path.split(separator: "/").last.map(String.init)
        else { fatalError("PROLLY_POSTGRES_URL is required") }

        let client = PostgresClient(configuration: .init(
            host: host, port: components.port ?? 5432, username: user,
            password: components.password, database: database, tls: .disable
        ))
        await withTaskGroup(of: Void.self) { group in
            group.addTask { await client.run() }
            do {
                let store = PostgresRemoteStore(borrowing: client)
                try await store.initializeSchema()
                try await client.query("TRUNCATE prolly_nodes, prolly_hints, prolly_roots")
                let engine = try await AsyncProllyEngine(store: store, config: defaultConfig())
                let tree = try await engine.put(tree: engine.create(), key: Data("key".utf8), value: Data("value".utf8))
                guard try await engine.get(tree: tree, key: Data("key".utf8)) == Data("value".utf8) else {
                    fatalError("Swift PostgreSQL round trip failed")
                }

                let missing = OptionalBytesRecord(present: false, value: Data())
                let winners = await withTaskGroup(of: RootCasResultRecord.self) { contenders in
                    for index in 0..<32 {
                        contenders.addTask {
                            await store.compareAndSwapRootManifest(
                                name: Data("main".utf8), expected: missing,
                                new: OptionalBytesRecord(present: true, value: withUnsafeBytes(of: UInt16(index).bigEndian) { Data($0) })
                            )
                        }
                    }
                    var values: [RootCasResultRecord] = []
                    for await value in contenders { values.append(value) }
                    return values
                }
                guard winners.filter(\.applied).count == 1 else { fatalError("Swift PostgreSQL CAS contention failed") }

                let transaction = await store.commitTransaction(
                    nodes: [NodeMutationRecord(
                        key: Data(repeating: 120, count: 32),
                        value: OptionalBytesRecord(present: true, value: Data("must-not-write".utf8))
                    )],
                    conditions: [RootConditionRecord(name: Data("main".utf8), expected: missing)],
                    roots: []
                )
                guard !transaction.applied else { fatalError("Swift PostgreSQL rollback failed") }
                await store.close()
                _ = try await client.query("SELECT 1")
                print("Swift PostgreSQL remote store passed")
            } catch {
                fatalError("Swift PostgreSQL provider failed: \(String(reflecting: error))")
            }
            group.cancelAll()
        }
    }
}
