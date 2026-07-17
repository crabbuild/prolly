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

    func testProofsSessionsAndMaintenanceAreApplicationFacing() throws {
        try Engine.withMemory { engine in
            let versioned = try engine.versionedMap(Data("proofs".utf8))
            _ = try versioned.initialize()
            _ = try versioned.put(Data("k".utf8), value: Data("v".utf8))
            let snapshot = try XCTUnwrap(versioned.snapshot())
            let verified = try Proofs.verify(snapshot.proveKey(Data("k".utf8)))
            XCTAssertTrue(verified.valid)
            XCTAssertEqual(verified.value, Data("v".utf8))
            XCTAssertEqual(try snapshot.stats().totalKeyValuePairs, 1)
            XCTAssertFalse(try snapshot.export().nodes.isEmpty)
            let session = try snapshot.read()
            XCTAssertEqual(try session.get(Data("k".utf8)), Data("v".utf8))
            session.close()
            XCTAssertGreaterThanOrEqual(try versioned.verifyCatalog().versionCount, 2)
            XCTAssertFalse(try versioned.backup().isEmpty)
            XCTAssertFalse(try versioned.planGC().reachability.liveCids.isEmpty)

            let registry = try engine.indexRegistry()
            try registry.register(
                name: Data("by_value".utf8), generation: 1, extractorID: "value-v1",
                projection: .all
            ) { _, value in [IndexEntryRecord(term: Data(value), projection: nil)] }
            let indexed = try engine.indexedMap(Data("indexed-maintenance".utf8), registry: registry)
            let version = try indexed.put(Data("k".utf8), value: Data("term".utf8))
            _ = try indexed.ensureIndex(Data("by_value".utf8))
            XCTAssertTrue(try indexed.verifyIndex(Data("by_value".utf8), sourceVersion: version.sourceVersion).valid)
            XCTAssertGreaterThanOrEqual(try indexed.metrics().buildAttempts, 1)
            XCTAssertFalse(try indexed.exportCurrent().isEmpty)
            XCTAssertFalse(try indexed.keepLast(1).retainedSourceVersions.isEmpty)

            let proximity = try engine.buildProximity(
                dimensions: 2,
                records: [ProximityRecord(key: Data("p".utf8), vector: [0, 0], value: Data("payload".utf8))]
            )
            let membership = try Proofs.verify(
                proximity.proveMembership(Data("p".utf8)),
                expectedDescriptor: proximity.descriptor
            )
            XCTAssertEqual(membership.record?.value, Data("payload".utf8))
            XCTAssertEqual(try proximity.verify().recordCount, 1)
            XCTAssertEqual(try proximity.count, 1)
            XCTAssertTrue(try proximity.contains(Data("p".utf8)))
            XCTAssertEqual(try proximity.config.dimensions, 2)
            let structure = try Proofs.verify(
                proximity.proveStructure(), expectedDescriptor: proximity.descriptor
            )
            XCTAssertEqual(structure.summary.recordCount, 1)
            let mutation = try proximity.mutate([
                ProximityMutationRecord(
                    key: Data("q".utf8), vector: [1, 1], value: Data("second".utf8)
                )
            ])
            XCTAssertEqual(try mutation.map.count, 2)
            XCTAssertGreaterThanOrEqual(mutation.stats.recordsRebuilt, 1)
            let retained = try proximity.read()
            XCTAssertEqual(
                try retained.searchExact([0, 0], k: 1).neighbors.first?.key,
                Data("p".utf8)
            )
            XCTAssertEqual(
                try retained.withSearchView(query: [0, 0], k: 1) { rows in Data(rows[0].key) },
                Data("p".utf8)
            )
            retained.close()
            let searchProof = try proximity.proveSearchExact([0, 0], k: 1)
            let verifiedSearch = try searchProof.verify(expectedDescriptor: proximity.descriptor)
            XCTAssertEqual(verifiedSearch.result.neighbors.first?.key, Data("p".utf8))
            XCTAssertGreaterThan(verifiedSearch.replayedEvents, 0)
            searchProof.close()
        }
    }
}
