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

    func testVersionedSnapshotLifecycle() throws {
        try Engine.withMemory { engine in
            let versioned = try engine.versionedMap(Data("versioned-lifecycle".utf8))
            XCTAssertEqual(versioned.id, Data("versioned-lifecycle".utf8))
            XCTAssertFalse(try versioned.isInitialized())
            let initial = try versioned.initialize()
            XCTAssertTrue(try versioned.isInitialized())
            XCTAssertEqual(try versioned.headID(), initial.id)
            let first = try versioned.put(Data("k".utf8), value: Data("v1".utf8))
            _ = try versioned.put(Data("k".utf8), value: Data("v2".utf8))
            XCTAssertEqual(try versioned.head()?.id, try versioned.headID())
            XCTAssertEqual(try versioned.version(first.id)?.id, first.id)
            XCTAssertGreaterThanOrEqual(try versioned.versions().count, 3)
            let historical = try XCTUnwrap(versioned.snapshot(at: first.id))
            XCTAssertEqual(historical.id, first.id)
            XCTAssertEqual(historical.version.id, first.id)
            XCTAssertEqual(try historical.get(Data("k".utf8)), Data("v1".utf8))
        }
    }

    func testVersionedSnapshotsExposeOrderedNavigationAndBoundedPages() throws {
        try Engine.withMemory { engine in
            let versioned = try engine.versionedMap(Data("versioned-ordered".utf8))
            _ = try versioned.initialize()
            _ = try versioned.apply([
                MutationRecord(kind: .upsert, key: Data("a".utf8), value: Data("one".utf8)),
                MutationRecord(kind: .upsert, key: Data("ab".utf8), value: Data("two".utf8)),
                MutationRecord(kind: .upsert, key: Data("b".utf8), value: Data("three".utf8)),
                MutationRecord(kind: .upsert, key: Data("c".utf8), value: Data("four".utf8)),
            ])
            let snapshot = try XCTUnwrap(versioned.snapshot())
            XCTAssertTrue(try snapshot.contains(Data("ab".utf8)))
            XCTAssertEqual(try snapshot.getMany([Data("a".utf8), Data("missing".utf8)])[0], Data("one".utf8))
            XCTAssertEqual(try snapshot.firstEntry()?.key, Data("a".utf8))
            XCTAssertEqual(try snapshot.lastEntry()?.key, Data("c".utf8))
            XCTAssertEqual(try snapshot.lowerBound(Data("aa".utf8))?.key, Data("ab".utf8))
            XCTAssertEqual(try snapshot.upperBound(Data("ab".utf8))?.key, Data("b".utf8))
            XCTAssertEqual(try snapshot.prefix(Data("a".utf8)).map(\.key), [Data("a".utf8), Data("ab".utf8)])
            XCTAssertEqual(try snapshot.range(from: Data("ab".utf8), to: Data("c".utf8)).map(\.key), [Data("ab".utf8), Data("b".utf8)])
            let prefixPage = try snapshot.prefixPage(Data("a".utf8), limit: 1)
            XCTAssertEqual(prefixPage.entries.map(\.key), [Data("a".utf8)])
            XCTAssertNotNil(prefixPage.nextCursor)
            let first = try snapshot.rangePage(to: Data("c".utf8), limit: 2)
            XCTAssertEqual(first.entries.map(\.key), [Data("a".utf8), Data("ab".utf8)])
            let second = try snapshot.rangePage(cursor: first.nextCursor, to: Data("c".utf8), limit: 2)
            XCTAssertEqual(second.entries.map(\.key), [Data("b".utf8)])
            XCTAssertEqual(try snapshot.reversePage(from: Data("a".utf8), limit: 2).entries.map(\.key), [Data("c".utf8), Data("b".utf8)])
            XCTAssertEqual(try snapshot.prefixReversePage(Data("a".utf8), limit: 2).entries.map(\.key), [Data("ab".utf8), Data("a".utf8)])
        }
    }

    func testVersionedBatchCASAndPinnedPointReads() throws {
        try Engine.withMemory { engine in
            let versioned = try engine.versionedMap(Data("versioned-cas".utf8))
            _ = try versioned.initialize()
            let first = try versioned.apply([
                MutationRecord(kind: .upsert, key: Data("a".utf8), value: Data("one".utf8)),
                MutationRecord(kind: .upsert, key: Data("b".utf8), value: Data("two".utf8)),
            ])
            XCTAssertTrue(try versioned.contains(Data("a".utf8)))
            XCTAssertEqual(try versioned.getMany([Data("a".utf8), Data("missing".utf8)])[0], Data("one".utf8))
            XCTAssertNil(try versioned.getMany([Data("missing".utf8)])[0])
            let applied = try versioned.putIf(
                expected: first.id, key: Data("a".utf8), value: Data("updated".utf8)
            )
            XCTAssertEqual(applied.kind, .applied)
            XCTAssertEqual(
                try versioned.deleteIf(expected: first.id, key: Data("b".utf8)).kind,
                .conflict
            )
            let historical = try versioned.getMany(
                at: first.id, keys: [Data("a".utf8), Data("b".utf8)]
            )
            XCTAssertEqual(historical[0], Data("one".utf8))
            XCTAssertEqual(historical[1], Data("two".utf8))
            XCTAssertEqual(try versioned.get(at: first.id, key: Data("a".utf8)), Data("one".utf8))
            let batch = try versioned.applyIf(
                expected: try XCTUnwrap(applied.current).id,
                mutations: [MutationRecord(kind: .delete, key: Data("b".utf8), value: nil)]
            )
            XCTAssertEqual(batch.kind, .applied)
        }
    }

    func testVersionedBackupRestoreAndRetention() throws {
        try Engine.withMemory { sourceEngine in
            try Engine.withMemory { targetEngine in
                let source = try sourceEngine.versionedMap(Data("versioned-backup".utf8))
                _ = try source.initialize()
                _ = try source.put(Data("k".utf8), value: Data("v1".utf8))
                _ = try source.put(Data("k".utf8), value: Data("v2".utf8))
                let target = try targetEngine.versionedMap(Data("versioned-backup".utf8))
                let restored = try target.restoreBackup(source.backup())
                XCTAssertEqual(restored.id, try source.headID())
                XCTAssertEqual(try target.get(Data("k".utf8)), Data("v2".utf8))
                let pruned = try source.keepLast(1)
                XCTAssertFalse(pruned.retained.isEmpty)
                XCTAssertFalse(pruned.removed.isEmpty)
            }
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
            XCTAssertEqual(indexed.id, Data("indexed-maintenance".utf8))
            let applied = try indexed.apply([
                MutationRecord(kind: .upsert, key: Data("k2".utf8), value: Data("term".utf8))
            ])
            let conditional = try indexed.applyIf(
                expectedSource: applied.sourceVersion,
                mutations: [MutationRecord(kind: .upsert, key: Data("k3".utf8), value: Data("other".utf8))]
            )
            XCTAssertNotNil(conditional.current)
            let historicalIndex = try indexed.snapshot(at: applied.sourceVersion).index(Data("by_value".utf8))
            XCTAssertEqual(try historicalIndex.exactPage(Data("term".utf8)).matches.count, 2)
            XCTAssertEqual(historicalIndex.name, Data("by_value".utf8))
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
