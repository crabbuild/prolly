import Foundation
import Prolly
import ProllyAPI
import XCTest

final class PortableParityTests: XCTestCase {
    func testVersionedAndProximityMapsUsePortableAPI() throws {
        try Engine.withMemory { engine in
            let versioned = try engine.versionedMap(Data("users".utf8))
            _ = try versioned.initialize()
            _ = try versioned.put(Data("u1".utf8), value: Data("Ada".utf8))
            XCTAssertEqual(try versioned.get(Data("u1".utf8)), Data("Ada".utf8))

            let proximity = try engine.buildProximity(
                dimensions: 2,
                records: [ProximityRecord(key: Data("a".utf8), vector: [0, 0], value: Data("alpha".utf8))]
            )
            XCTAssertEqual(try proximity.searchExact([0.1, 0.1], k: 1).neighbors.first?.key, Data("a".utf8))
            let key = try proximity.withSearchView(query: [0.1, 0.1], k: 1) { neighbors in
                Data(neighbors[0].key)
            }
            XCTAssertEqual(key, Data("a".utf8))
        }
    }

    func testAsyncWriteCopiesMutableInputBeforeTaskRuns() async throws {
        try await Engine.withMemory { engine in
            let versioned = try engine.versionedMap(Data("async".utf8))
            _ = try versioned.initialize()
            var key = Data("k".utf8)
            let task = versioned.putAsync(key, value: Data("v".utf8))
            key[0] = Character("x").asciiValue!
            _ = try await task.value
            XCTAssertEqual(try versioned.get(Data("k".utf8)), Data("v".utf8))
        }
    }
}
