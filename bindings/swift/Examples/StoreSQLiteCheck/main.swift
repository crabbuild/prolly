import CSQLite
import Foundation
import Prolly
import ProllyStoreSQLite

@main
struct StoreSQLiteCheck {
    static func main() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("prolly-swift-\(UUID().uuidString).sqlite3")
        defer { try? FileManager.default.removeItem(at: path) }

        var database: OpaquePointer?
        guard sqlite3_open(path.path, &database) == SQLITE_OK, let database else {
            fatalError("could not open SQLite")
        }
        defer { sqlite3_close(database) }

        let store = SQLiteRemoteStore(borrowing: database)
        try await store.initializeSchema()
        let engine = try await AsyncProllyEngine(store: store, config: defaultConfig())
        let tree = try await engine.put(tree: engine.create(), key: Data("key".utf8), value: Data("value".utf8))
        let value = try await engine.get(tree: tree, key: Data("key".utf8))
        guard value == Data("value".utf8) else { fatalError("Swift SQLite round trip failed") }
        await store.close()
        guard sqlite3_exec(database, "SELECT 1", nil, nil, nil) == SQLITE_OK else {
            fatalError("adapter closed caller-owned SQLite handle")
        }
        print("Swift SQLite remote store passed")
    }
}
