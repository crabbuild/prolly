import Foundation
import NIOPosix
import Prolly
import ProllyStoreRedis
import RediStack

@main
struct StoreRedisCheck {
    static func main() async throws {
        guard let url = ProcessInfo.processInfo.environment["PROLLY_REDIS_URL"] else {
            fatalError("PROLLY_REDIS_URL is required")
        }
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let connection = try await RedisConnection.make(
            configuration: try .init(url: url), boundEventLoop: group.next()
        ).get()
        let prefix = Data("prolly:test:swift:\(ProcessInfo.processInfo.processIdentifier):".utf8)
        let store = RedisRemoteStore(borrowing: connection, keyPrefix: prefix)
        try await store.clearNamespace()

        let engine = try await AsyncProllyEngine(store: store, config: defaultConfig())
        let tree = try await engine.put(tree: engine.create(), key: Data("key".utf8), value: Data("value".utf8))
        guard try await engine.get(tree: tree, key: Data("key".utf8)) == Data("value".utf8) else {
            fatalError("Swift Redis round trip failed")
        }

        var cid = Data()
        for _ in 0..<8 { cid.append(contentsOf: [0, 127, 128, 255]) }
        await assertNoError(store.putNode(cid: cid, value: Data("node".utf8)))
        var rawNodeKey = prefix
        rawNodeKey.append(contentsOf: "node:".utf8)
        rawNodeKey.append(cid)
        let rawNode = try await connection.send(command: "GET", with: [RESPValue(from: rawNodeKey)]).get()
        guard rawNode.data == Data("node".utf8) else { fatalError("Swift Redis binary layout failed") }

        let ordered = await store.batchGetNodesOrdered(cids: [cid, Data("missing".utf8), cid])
        guard ordered.values.map(\.present) == [true, false, true] else {
            fatalError("Swift Redis ordered batch read failed")
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
        guard winners == 1 else { fatalError("Swift Redis CAS failed") }

        let transaction = await store.commitTransaction(
            nodes: [NodeMutationRecord(
                key: Data("rollback".utf8),
                value: OptionalBytesRecord(present: true, value: Data("must-not-write".utf8))
            )],
            conditions: [RootConditionRecord(name: Data("main".utf8), expected: missing)],
            roots: [RootWriteRecord(
                name: Data("other".utf8),
                replacement: OptionalBytesRecord(present: true, value: Data("must-not-publish".utf8))
            )]
        )
        guard !transaction.applied else { fatalError("Swift Redis rollback failed") }
        guard !(await store.getNode(cid: Data("rollback".utf8))).value.present else {
            fatalError("Swift Redis rollback wrote a node")
        }

        try await store.clearNamespace()
        await store.close()
        guard try await connection.send(command: "PING").get().string == "PONG" else {
            fatalError("Swift Redis adapter closed borrowed connection")
        }
        _ = try await connection.close().get()
        try await group.shutdownGracefully()
        print("Swift Redis remote store passed")
    }

    private static func assertNoError(_ result: UnitResultRecord) {
        if let error = result.error { fatalError("Swift Redis operation failed: \(error.code)") }
    }
}
