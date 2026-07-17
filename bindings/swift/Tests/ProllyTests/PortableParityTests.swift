import Foundation
import Prolly
import ProllyAPI
import XCTest

final class PortableParityTests: XCTestCase {
    func testVersionedLargeValuesAndBlobGCAreApplicationFacing() throws {
        try Engine.withMemory { engine in
            let blobs = BlobStore.memory()
            defer { blobs.close() }
            let versioned = try engine.versionedMap(Data("large-values".utf8))
            _ = try versioned.initialize()
            XCTAssertFalse(versioned.headName().isEmpty)
            XCTAssertFalse(versioned.versionsPrefix().isEmpty)
            let config = LargeValueConfigRecord(inlineThreshold: 1)
            let first = try versioned.putLargeValue(
                blobs, key: Data("document".utf8), value: Data("large-value".utf8), config: config
            )
            XCTAssertEqual(
                try versioned.getLargeValue(blobs, key: Data("document".utf8)),
                Data("large-value".utf8)
            )
            let updated = try versioned.putLargeValueIf(
                blobs, expected: first.id, key: Data("document".utf8),
                value: Data("new-large-value".utf8), config: config
            )
            XCTAssertEqual(updated.kind, .applied)
            XCTAssertGreaterThanOrEqual(try versioned.planBlobGC(blobs).reachability.liveBlobCount, 1)
            XCTAssertGreaterThanOrEqual(try versioned.sweepBlobGC(blobs).plan.reachability.liveBlobCount, 1)
        }
    }

    func testRetainedSearchRuntimeReusesValidatedContent() throws {
        try Engine.withMemory { engine in
            let proximity = try engine.buildProximity(
                dimensions: 2,
                records: (0..<16).map { index in
                    ProximityRecord(
                        key: Data(String(format: "vector-%02d", index).utf8),
                        vector: [Float(index), 0],
                        value: Data(String(format: "value-%02d", index).utf8)
                    )
                }
            )
            let request = exactProximitySearchRequest(query: [0, 0], k: 3)
            let runtime = try engine.proximitySearchRuntime()
            defer { runtime.close() }

            let cold = try proximity.search(request, runtime: runtime)
            let warm = try proximity.search(request, runtime: runtime)
            XCTAssertGreaterThan(cold.stats.physicalBytesRead, 0)
            XCTAssertEqual(warm.stats.physicalBytesRead, 0)
            XCTAssertGreaterThan(try runtime.stats().physicalReads, 0)
            XCTAssertEqual(try runtime.policy(), defaultProximitySearchRuntimePolicy())

            try runtime.clear()
            XCTAssertGreaterThan(
                try proximity.search(request, runtime: runtime).stats.physicalBytesRead, 0
            )
        }
    }

    func testCompositeAndCatalogLifecycleIsPortableAndBounded() throws {
        try Engine.withMemory { engine in
            let base = try engine.buildProximity(
                dimensions: 2,
                records: (0..<16).map { index in
                    ProximityRecord(
                        key: Data(String(format: "vector-%02d", index).utf8),
                        vector: [Float(index), 0],
                        value: Data(String(format: "value-%02d", index).utf8)
                    )
                }
            )
            let hnsw = try base.buildHnsw().index
            let current = try base.mutate([
                ProximityMutationRecord(
                    key: Data("vector-00".utf8), vector: [0.25, 0], value: Data("updated".utf8)
                )
            ]).map
            let built = try current.buildCompositeHnsw(baseMap: base, base: hnsw)
            XCTAssertEqual(built.stats.vectorUpdatedRecords, 1)
            XCTAssertTrue(built.reasons.isEmpty)
            let composite = try XCTUnwrap(built.accelerator)
            XCTAssertEqual(composite.baseKind, .hnsw)
            XCTAssertEqual(composite.currentSourceDescriptor, current.descriptor)
            XCTAssertEqual(composite.baseSourceDescriptor, base.descriptor)
            XCTAssertEqual(composite.deltaCount, 1)
            XCTAssertEqual(composite.shadowCount, 1)
            var request = exactProximitySearchRequest(query: [0, 0], k: 3)
            request.policy = .fixedBudget
            request.backend = .composite
            XCTAssertEqual(try composite.search(current, request: request).backend, .composite)
            let proof = try composite.proveSearch(current, request: request)
            XCTAssertEqual(
                try proof.verify(expectedDescriptor: current.descriptor).result.backend,
                .composite
            )
            proof.close()
            let manifest = composite.manifest
            let catalog = try current.buildAcceleratorCatalog(composite: composite)
            XCTAssertEqual(catalog.sourceDescriptor, current.descriptor)
            XCTAssertEqual(catalog.entries.first?.kind, .composite)
            XCTAssertEqual(try catalog.search(current, request: request).backend, .composite)
            let loadedCatalog = try current.loadAcceleratorCatalog(catalog.manifest)
            XCTAssertEqual(loadedCatalog.manifest, catalog.manifest)
            loadedCatalog.close()
            catalog.close()
            composite.close()
            let loaded = try current.loadComposite(manifest)
            XCTAssertEqual(loaded.manifest, manifest)
            loaded.close()
            var forced = defaultCompositeAcceleratorConfig()
            forced.maxDeltaRecords = 0
            let rebuilt = try current.buildOrRebuildCompositeHnsw(
                baseMap: base, base: hnsw, config: forced
            )
            XCTAssertEqual(rebuilt.kind, .hnswRebuilt)
            rebuilt.hnsw?.close()
            hnsw.close()
        }
    }

    func testProductQuantizerLifecycleIsPortableAndBounded() throws {
        try Engine.withMemory { engine in
            let proximity = try engine.buildProximity(
                dimensions: 4,
                records: (0..<16).map { index in
                    ProximityRecord(
                        key: Data(String(format: "vector-%02d", index).utf8),
                        vector: [Float(index), Float(index % 3), 0, 1],
                        value: Data(String(format: "value-%02d", index).utf8)
                    )
                }
            )
            let config = ProductQuantizationConfigRecord(
                subquantizers: 2,
                centroidsPerSubquantizer: 4,
                trainingIterations: 2,
                rerankMultiplier: 4,
                seed: .max,
                maxTrainingVectors: 16
            )
            let built = try proximity.buildPq(config: config, workerThreads: 2)
            XCTAssertEqual(built.stats.encodedVectors, 16)
            var request = exactProximitySearchRequest(query: [0, 0, 0, 1], k: 3)
            request.policy = .fixedBudget
            request.backend = .productQuantized
            let index = built.index
            XCTAssertEqual(index.config, config)
            XCTAssertEqual(index.sourceDescriptor, proximity.descriptor)
            XCTAssertGreaterThanOrEqual(index.quality.meanSquaredError, 0)
            let result = try index.search(proximity, request: request)
            XCTAssertEqual(result.backend, .productQuantized)
            XCTAssertEqual(result.neighbors.first?.key, Data("vector-00".utf8))
            let manifest = index.manifest
            let proof = try index.proveSearch(proximity, request: request)
            XCTAssertEqual(
                try proof.verify(expectedDescriptor: proximity.descriptor).result.backend,
                .productQuantized
            )
            proof.close()
            index.close()
            let loaded = try proximity.loadPq(manifest)
            XCTAssertEqual(loaded.manifest, manifest)
            loaded.close()
        }
    }

    func testHnswAcceleratorLifecycleIsPortable() throws {
        try Engine.withMemory { engine in
            let proximity = try engine.buildProximity(
                dimensions: 2,
                records: (0..<16).map { index in
                    ProximityRecord(
                        key: Data(String(format: "vector-%02d", index).utf8),
                        vector: [Float(index), 0],
                        value: Data(String(format: "value-%02d", index).utf8)
                    )
                }
            )
            let built = try proximity.buildHnsw()
            XCTAssertEqual(built.stats.records, 16)
            var request = exactProximitySearchRequest(query: [0, 0], k: 3)
            request.policy = .fixedBudget
            request.backend = .hnsw
            let index = built.index
            XCTAssertTrue(index.isCanonical)
            XCTAssertEqual(index.sourceDescriptor, proximity.descriptor)
            let result = try index.search(proximity, request: request)
            XCTAssertEqual(result.backend, .hnsw)
            XCTAssertEqual(result.neighbors.first?.key, Data("vector-00".utf8))
            let cancellation = ProximityCancellationToken()
            cancellation.cancel()
            let cancelled = try index.searchCancellable(
                proximity, request: request, cancellation: cancellation
            )
            XCTAssertEqual(cancelled.completion, .cancelled)
            XCTAssertTrue(cancelled.neighbors.isEmpty)
            cancellation.close()
            let manifest = index.manifest
            let proof = try index.proveSearch(proximity, request: request)
            XCTAssertEqual(try proof.verify(expectedDescriptor: proximity.descriptor).result.backend, .hnsw)
            proof.close()
            index.close()
            let loaded = try proximity.loadHnsw(manifest)
            XCTAssertEqual(loaded.manifest, manifest)
            loaded.close()
        }
    }

    func testProximityRichSearchRequestIsSharedByMapSessionAndProof() throws {
        try Engine.withMemory { engine in
            let proximity = try engine.buildProximity(
                dimensions: 2,
                records: [
                    ProximityRecord(key: Data("a".utf8), vector: [0, 0], value: Data("alpha".utf8)),
                    ProximityRecord(key: Data("ab".utf8), vector: [1, 0], value: Data("alphabet".utf8)),
                    ProximityRecord(key: Data("b".utf8), vector: [0.1, 0], value: Data("beta".utf8)),
                ]
            )
            let request = ProximitySearchRequestRecord(
                query: [0, 0],
                k: 3,
                policy: .fixedBudget,
                adaptiveQuality: nil,
                budget: SearchBudgetRecord(
                    maxNodes: 1_000,
                    maxCommittedBytes: 1_000_000,
                    maxDistanceEvaluations: 1_000,
                    maxFrontierEntries: 1_000
                ),
                filter: ProximityFilterRecord(
                    kind: .prefix,
                    start: nil,
                    rangeEnd: nil,
                    prefix: Data("a".utf8),
                    eligibleKeys: []
                ),
                kernel: .scalarDeterministic,
                backend: .auto,
                hnswEfSearch: nil,
                pqRerankMultiplier: nil
            )

            let result = try proximity.search(request)
            XCTAssertEqual(result.neighbors.map(\.key), [Data("a".utf8), Data("ab".utf8)])
            XCTAssertGreaterThan(result.stats.distanceEvaluations, 0)
            XCTAssertGreaterThan(result.planFormatVersion, 0)
            var scanned: [Data] = []
            XCTAssertEqual(try proximity.scanRecords { record in
                scanned.append(record.key)
                return scanned.count < 2
            }, 2)
            XCTAssertEqual(scanned, [Data("a".utf8), Data("ab".utf8)])
            let session = try proximity.read()
            XCTAssertEqual(
                try session.search(request).neighbors.map(\.key),
                [Data("a".utf8), Data("ab".utf8)]
            )
            var retained: [Data] = []
            XCTAssertEqual(try session.scanRecords { record in
                retained.append(record.key)
                return true
            }, 3)
            XCTAssertEqual(retained, [Data("a".utf8), Data("ab".utf8), Data("b".utf8)])
            session.close()
            let proof = try proximity.proveSearch(request)
            XCTAssertEqual(
                try proof.verify(expectedDescriptor: proximity.descriptor).result.neighbors.map(\.key),
                [Data("a".utf8), Data("ab".utf8)]
            )
            proof.close()
        }
    }

    func testVersionedBulkPublicationUsesNativePerformancePaths() throws {
        try Engine.withMemory { engine in
            let map = try engine.versionedMap(Data("bulk-publication".utf8))
            let initialized = try map.initializeSorted([
                EntryRecord(key: Data("a".utf8), value: Data("one".utf8)),
                EntryRecord(key: Data("b".utf8), value: Data("two".utf8)),
            ])
            XCTAssertEqual(initialized.kind, .applied)
            _ = try map.append([MutationRecord(kind: .upsert, key: Data("c".utf8), value: Data("three".utf8))])
            let parallel = try map.parallelApply(
                [
                    MutationRecord(kind: .upsert, key: Data("b".utf8), value: Data("updated".utf8)),
                    MutationRecord(kind: .upsert, key: Data("d".utf8), value: Data("four".utf8)),
                ],
                config: ParallelConfigRecord(maxThreads: 1, parallelismThreshold: 1)
            )
            XCTAssertEqual(parallel.stats.inputMutations, 2)
            let rebuilt = try map.rebuildSortedIf(parallel.version.id, entries: [
                EntryRecord(key: Data("x".utf8), value: Data("nine".utf8)),
                EntryRecord(key: Data("y".utf8), value: Data("ten".utf8)),
            ])
            XCTAssertEqual(rebuilt.kind, .applied)
            let iterRebuilt = try map.rebuildFromEntriesIf(rebuilt.current!.id, entries: [
                EntryRecord(key: Data("q".utf8), value: Data("queue".utf8)),
                EntryRecord(key: Data("p".utf8), value: Data("priority".utf8)),
            ])
            XCTAssertEqual(iterRebuilt.kind, .applied)
            XCTAssertEqual(try map.get(Data("p".utf8)), Data("priority".utf8))
        }
    }

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
            let subscription = try await versioned.subscribeAsync().value
            var key = Data("k".utf8)
            let task = versioned.putAsync(key, value: Data("v".utf8))
            key[0] = Character("x").asciiValue!
            let updated = try await task.value
            XCTAssertEqual(try versioned.get(Data("k".utf8)), Data("v".utf8))
            XCTAssertEqual(try await versioned.headAsync().value?.id, updated.id)
            let snapshot = try XCTUnwrap(try await versioned.snapshotAtAsync(updated.id).value)
            XCTAssertEqual(try await snapshot.getAsync(Data("k".utf8)).value, Data("v".utf8))
            var bundle = try await snapshot.exportAsync().value
            let imported = try engine.versionedMap(Data("async-import".utf8))
            let pendingImport = imported.importAsHeadAsync(bundle)
            bundle.nodes[0].bytes[0] = 0
            _ = try await pendingImport.value
            XCTAssertEqual(try imported.get(Data("k".utf8)), Data("v".utf8))
            let session = try snapshot.read()
            XCTAssertEqual(try await session.getAsync(Data("k".utf8)).value, Data("v".utf8))
            session.close()
            XCTAssertNotNil(try await subscription.pollAsync().value)
            snapshot.close()
            subscription.close()
        }
    }

    func testProximityTaskUsesNativeCooperativeCancellation() async throws {
        try await Engine.withMemory { engine in
            let proximity = try engine.buildProximity(
                dimensions: 2,
                records: (0..<256).map { index in
                    ProximityRecord(
                        key: Data(String(format: "vector-%04d", index).utf8),
                        vector: [Float(index), Float(index % 7)],
                        value: Data(String(index).utf8)
                    )
                }
            )
            let runtime = try engine.proximitySearchRuntime()
            defer { runtime.close() }
            let cancellation = ProximityCancellationToken()
            defer { cancellation.close() }
            cancellation.cancel()

            let result = try await proximity.searchAsync(
                exactProximitySearchRequest(query: [0, 0], k: 10),
                runtime: runtime,
                cancellation: cancellation
            ).value
            XCTAssertEqual(result.completion, .cancelled)
            XCTAssertTrue(result.neighbors.isEmpty)
            let session = try proximity.read()
            defer { session.close() }
            let sessionResult = try await session.searchAsync(
                exactProximitySearchRequest(query: [0, 0], k: 10),
                runtime: runtime,
                cancellation: cancellation
            ).value
            XCTAssertEqual(sessionResult.completion, .cancelled)
            XCTAssertTrue(sessionResult.neighbors.isEmpty)
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
                let bundle = try XCTUnwrap(source.snapshot()).export()
                let importedMap = try targetEngine.versionedMap(Data("versioned-import".utf8))
                XCTAssertTrue(try importedMap.importAsHead(bundle).isHead)
                XCTAssertEqual(try importedMap.get(Data("k".utf8)), Data("v2".utf8))
                let timestampedMap = try targetEngine.versionedMap(Data("versioned-import-at".utf8))
                let timestamped = try timestampedMap.importAsHead(
                    bundle, timestampMillis: 12_345
                )
                XCTAssertEqual(timestamped.createdAtMillis, 12_345)
                XCTAssertEqual(try timestampedMap.get(Data("k".utf8)), Data("v2".utf8))
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
            _ = try versioned.put(Data("ka".utf8), value: Data("v2".utf8))
            let snapshot = try XCTUnwrap(versioned.snapshot())
            let verified = try Proofs.verify(snapshot.proveKey(Data("k".utf8)))
            XCTAssertTrue(verified.valid)
            XCTAssertEqual(verified.value, Data("v".utf8))
            let multi = try Proofs.verify(snapshot.proveKeys([Data("k".utf8), Data("missing".utf8)]))
            XCTAssertEqual(multi.results.map(\.exists), [true, false])
            let ranged = try Proofs.verify(snapshot.proveRange(from: Data("k".utf8), to: Data("l".utf8)))
            XCTAssertEqual(ranged.entries.map(\.key), [Data("k".utf8), Data("ka".utf8)])
            let prefixed = try Proofs.verify(snapshot.provePrefix(Data("k".utf8)))
            XCTAssertEqual(prefixed.entries.map(\.key), [Data("k".utf8), Data("ka".utf8)])
            let provedPage = try snapshot.proveRangePage(to: Data("l".utf8), limit: 1)
            XCTAssertTrue(try Proofs.verify(provedPage.proof).valid)
            XCTAssertEqual(provedPage.page.entries.map(\.key), [Data("k".utf8)])
            XCTAssertEqual(try snapshot.stats().totalKeyValuePairs, 2)
            XCTAssertFalse(try snapshot.export().nodes.isEmpty)
            let session = try snapshot.read()
            XCTAssertEqual(try session.get(Data("k".utf8)), Data("v".utf8))
            var seen: [String] = []
            let scan = try session.scanRangeView(
                from: Data("k".utf8), to: Data("l".utf8)
            ) { entry in
                seen.append("\(String(decoding: entry.key, as: UTF8.self))=\(String(decoding: entry.value, as: UTF8.self))")
                return true
            }
            XCTAssertEqual(scan.visited, 2)
            XCTAssertFalse(scan.stopped)
            XCTAssertEqual(seen, ["k=v", "ka=v2"])
            let stopped = try session.scanRangeView(
                from: Data("k".utf8), to: Data("l".utf8)
            ) { _ in false }
            XCTAssertEqual(stopped, ReadScanOutcome(visited: 1, stopped: true))
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
            let oldSnapshotID = try indexed.snapshot().id
            var tooSmall = ProllyAPI.defaultSecondaryIndexLimits()
            tooSmall.maxTermBytes = 3
            XCTAssertThrowsError(try indexed.replaceIndex(
                Data("by_value".utf8), generation: 2, extractorID: "value-too-small-v2",
                projection: .all, limits: tooSmall
            ) { _, value in [IndexEntryRecord(term: Data(value), projection: nil)] })
            XCTAssertEqual(try indexed.health().activeIndexes.first?.generation, 1)
            let replacement = try indexed.replaceIndex(
                Data("by_value".utf8), generation: 2, extractorID: "value-v2", projection: .all
            ) { _, value in [IndexEntryRecord(term: Data(value), projection: nil)] }
            XCTAssertEqual(replacement.generation, 2)
            XCTAssertEqual(try indexed.health().activeIndexes.first?.generation, 2)
            let oldIndex = try indexed.snapshot(id: oldSnapshotID).index(Data("by_value".utf8))
            XCTAssertEqual(try oldIndex.exact(Data("term".utf8)).count, 1)
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
    func testVersionedComparisonsPinVersionsAndPageDiffs() throws {
        try Engine.withMemory { engine in
            let map = try engine.versionedMap(Data("comparison".utf8))
            let base = try map.initialize()
            let target = try map.put(Data("k".utf8), value: Data("v".utf8))
            let comparison = try map.compare(base: base.id, target: target.id)
            XCTAssertEqual(comparison.base.id, base.id)
            XCTAssertEqual(comparison.target.id, target.id)
            XCTAssertEqual(try comparison.diff().map(\.key), [Data("k".utf8)])
            XCTAssertEqual(try comparison.diffPage(limit: 1).diffs.map(\.key), [Data("k".utf8)])
            comparison.close()
        }
    }
    func testVersionedHistoryNavigationDiffAndRollbackStayNative() throws {
        try Engine.withMemory { engine in
            let map = try engine.versionedMap(Data("history-navigation".utf8))
            _ = try map.initialize()
            _ = try map.put(Data("a".utf8), value: Data("one".utf8))
            _ = try map.put(Data("ab".utf8), value: Data("two".utf8))
            let base = try map.put(Data("b".utf8), value: Data("three".utf8))
            let target = try map.put(Data("a".utf8), value: Data("updated".utf8))

            XCTAssertEqual(try map.range(from: Data("a".utf8), to: Data("c".utf8)).map(\.key), [Data("a".utf8), Data("ab".utf8), Data("b".utf8)])
            XCTAssertEqual(try map.prefix(Data("a".utf8)).map(\.key), [Data("a".utf8), Data("ab".utf8)])
            XCTAssertEqual(try map.range(at: base.id, from: Data("a".utf8), to: Data("b".utf8)).first?.value, Data("one".utf8))
            XCTAssertEqual(try map.prefix(at: base.id, Data("a".utf8)).map(\.key), [Data("a".utf8), Data("ab".utf8)])
            XCTAssertEqual(try map.rangePage(limit: 2).entries.map(\.key), [Data("a".utf8), Data("ab".utf8)])
            XCTAssertEqual(try map.prefixPage(Data("a".utf8), limit: 1).entries.map(\.key), [Data("a".utf8)])
            let historicalPage = try map.prefixPage(at: base.id, Data("a".utf8), limit: 1)
            XCTAssertEqual(historicalPage.entries.map(\.key), [Data("a".utf8)])
            XCTAssertNotNil(historicalPage.nextCursor)
            XCTAssertEqual(try map.diff(base: base.id, target: target.id).map(\.key), [Data("a".utf8)])
            XCTAssertEqual(try map.changes(since: base.id).map(\.key), [Data("a".utf8)])

            let rolledBack = try map.rollback(to: base.id)
            XCTAssertEqual(try map.headID(), rolledBack.id)
            XCTAssertEqual(try map.get(Data("a".utf8)), Data("one".utf8))
            XCTAssertTrue(try map.changes(since: base.id).isEmpty)
        }
    }
    func testVersionedTimestampedWritesExposeCompleteMaintenanceAndRetentionRecords() throws {
        try Engine.withMemory { engine in
            let map = try engine.versionedMap(Data("maintenance-complete".utf8))
            let first = try map.applyAtMillis([
                MutationRecord(kind: .upsert, key: Data("k".utf8), value: Data("one".utf8))
            ], timestampMillis: 1_000)
            let second = try XCTUnwrap(map.applyIfAtMillis(
                expected: first.id,
                mutations: [MutationRecord(kind: .upsert, key: Data("k".utf8), value: Data("two".utf8))],
                timestampMillis: 2_000
            ).current)
            let third = try map.applyAtMillis([
                MutationRecord(kind: .upsert, key: Data("k".utf8), value: Data("three".utf8))
            ], timestampMillis: 3_000)

            XCTAssertEqual(first.createdAtMillis, 1_000)
            XCTAssertEqual(second.createdAtMillis, 2_000)
            XCTAssertEqual(map.retentionPolicy().kind, .prefix)
            let verification = try map.verifyCatalog()
            XCTAssertEqual(verification.head, third.id)
            XCTAssertEqual(verification.versionCount, 3)
            let plan = try map.planGC()
            XCTAssertGreaterThan(plan.reachability.liveNodes, 0)
            XCTAssertGreaterThanOrEqual(plan.candidateNodes, plan.reclaimableNodes)

            let aged = try map.keepForAt(nowMillis: 3_000, maxAgeMillis: 1_500)
            XCTAssertTrue(aged.retained.contains(second.id))
            XCTAssertTrue(aged.removed.contains(first.id))
            XCTAssertTrue(try map.keepVersions([second.id]).retained.contains(third.id))
            let pruned = try map.pruneVersions(0)
            XCTAssertEqual(pruned.retained, [third.id])
            XCTAssertTrue(pruned.removed.contains(second.id))
            XCTAssertFalse(try map.keepFor(maxAgeMillis: 10_000).retained.isEmpty)
            XCTAssertGreaterThanOrEqual(try map.sweepGC().deletedNodes, 0)
        }
    }
    func testVersionedSubscriptionResumesAndPollsOwnedDiffs() throws {
        try Engine.withMemory { engine in
            let map = try engine.versionedMap(Data("subscription".utf8))
            let initial = try map.initialize()
            let subscription = try map.subscribe()
            XCTAssertEqual(try subscription.lastSeen, initial.id)
            XCTAssertNil(try subscription.poll())
            let current = try map.put(Data("k".utf8), value: Data("v".utf8))
            let event = try XCTUnwrap(subscription.poll())
            XCTAssertEqual(event.previous, initial.id)
            XCTAssertEqual(event.current.id, current.id)
            XCTAssertEqual(event.diffs.map(\.key), [Data("k".utf8)])
            XCTAssertEqual(try subscription.lastSeen, current.id)
            subscription.close()
        }
    }
    func testMultiMapTransactionsAreAtomicAndReadStagedValues() throws {
        try Engine.withMemory { engine in
            let tx = try engine.beginVersionedTransaction()
            _ = try tx.put(mapID: Data("a".utf8), key: Data("k".utf8), value: Data("one".utf8))
            _ = try tx.put(mapID: Data("b".utf8), key: Data("k".utf8), value: Data("two".utf8))
            XCTAssertEqual(try tx.get(mapID: Data("a".utf8), key: Data("k".utf8)), Data("one".utf8))
            let committed = try tx.commit()
            XCTAssertTrue(committed.applied)
            XCTAssertEqual(committed.versions.count, 2)
            XCTAssertEqual(try engine.versionedMap(Data("a".utf8)).get(Data("k".utf8)), Data("one".utf8))
            XCTAssertEqual(try engine.versionedMap(Data("b".utf8)).get(Data("k".utf8)), Data("two".utf8))
            let rolledBack = try engine.beginVersionedTransaction()
            _ = try rolledBack.put(mapID: Data("a".utf8), key: Data("discard".utf8), value: Data("x".utf8))
            try rolledBack.rollback()
            XCTAssertNil(try engine.versionedMap(Data("a".utf8)).get(Data("discard".utf8)))
        }
    }
    func testPinnedMergesPageConflictsAndCASPublish() throws {
        try Engine.withMemory { engine in
            let map = try engine.versionedMap(Data("merge".utf8))
            let base = try map.initialize()
            let candidate = try map.put(Data("k".utf8), value: Data("candidate".utf8))
            _ = try map.put(Data("k".utf8), value: Data("head".utf8))
            let merge = try map.prepareMerge(base: base.id, candidate: candidate.id)
            XCTAssertEqual(merge.base.id, base.id)
            XCTAssertEqual(merge.candidate.id, candidate.id)
            XCTAssertEqual(try merge.conflictPage(limit: 1).conflicts.map(\.key), [Data("k".utf8)])
            XCTAssertEqual(try merge.publish(resolver: "prefer_right").current?.id, candidate.id)
            XCTAssertEqual(try map.get(Data("k".utf8)), Data("candidate".utf8))
            merge.close()
        }
    }
}
