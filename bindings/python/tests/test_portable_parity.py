import unittest

from prolly import (
    CatalogAcceleratorKindRecord,
    BlobStore,
    CompositeBaseKindRecord,
    CompositeBuildOrRebuildKindRecord,
    Engine,
    EntryRecord,
    IndexProjection,
    MutationKind,
    MutationRecord,
    ParallelConfigRecord,
    LargeValueConfigRecord,
    ProximityFilterKind,
    ProximityFilterRecord,
    ProximityRecord,
    ProductQuantizationConfigRecord,
    ProximityMutationRecord,
    ProximityCancellationToken,
    ProximitySearchRequestRecord,
    QueryKernelRecord,
    SearchBackendRecord,
    SearchBudgetRecord,
    SearchCompletionRecord,
    SearchPolicyKind,
    exact_proximity_search_request,
    verify_key_proof,
    verify_multi_key_proof,
    verify_range_page_proof,
    verify_range_proof,
    verify_proximity_membership_proof,
    verify_proximity_structure_proof,
    default_composite_accelerator_config,
    default_secondary_index_limits,
)


class PortableParityTests(unittest.TestCase):
    def test_versioned_large_values_and_blob_gc_are_application_facing(self):
        with Engine.memory() as engine, BlobStore.memory() as blobs:
            versioned = engine.versioned_map(b"large-values")
            versioned.initialize()
            self.assertTrue(versioned.head_name())
            self.assertTrue(versioned.versions_prefix())
            first = versioned.put_large_value(
                blobs, b"document", b"large-value", LargeValueConfigRecord(inline_threshold=1)
            )
            self.assertEqual(versioned.get_large_value(blobs, b"document"), b"large-value")
            updated = versioned.put_large_value_if(
                blobs, first.id, b"document", b"new-large-value",
                LargeValueConfigRecord(inline_threshold=1),
            )
            self.assertEqual(updated.kind.name, "APPLIED")
            self.assertGreaterEqual(versioned.plan_blob_gc(blobs).reachability.live_blob_count, 1)
            self.assertGreaterEqual(versioned.sweep_blob_gc(blobs).plan.reachability.live_blob_count, 1)

    def test_retained_search_runtime_reuses_validated_content(self):
        with Engine.memory() as engine:
            proximity = engine.build_proximity(
                dimensions=2,
                records=[
                    ProximityRecord(
                        f"vector-{index:02}".encode(),
                        [float(index), 0.0],
                        f"value-{index:02}".encode(),
                    )
                    for index in range(16)
                ],
            )
            index = proximity.build_hnsw().index
            request = exact_proximity_search_request([0.0, 0.0], 3)
            request.policy = SearchPolicyKind.FIXED_BUDGET
            request.backend = SearchBackendRecord.HNSW
            with engine.proximity_search_runtime() as runtime:
                cold = index.search_with_runtime(proximity, request, runtime)
                self.assertGreater(cold.stats.physical_bytes_read, 0)
                cold_stats = runtime.stats
                self.assertGreater(cold_stats.physical_reads, 0)
                warm = index.search_with_runtime(proximity, request, runtime)
                self.assertEqual(warm.neighbors, cold.neighbors)
                self.assertEqual(warm.stats.physical_bytes_read, 0)
                self.assertEqual(runtime.stats, cold_stats)
                runtime.clear()
                self.assertGreater(
                    index.search_with_runtime(proximity, request, runtime)
                    .stats.physical_bytes_read,
                    0,
                )

    def test_composite_and_catalog_lifecycle_is_portable_and_bounded(self):
        with Engine.memory() as engine:
            base = engine.build_proximity(
                dimensions=2,
                records=[
                    ProximityRecord(
                        f"vector-{index:02}".encode(),
                        [float(index), 0.0],
                        f"value-{index:02}".encode(),
                    )
                    for index in range(16)
                ],
            )
            hnsw = base.build_hnsw().index
            current, _ = base.mutate(
                [
                    ProximityMutationRecord(
                        key=b"vector-00",
                        vector=[0.25, 0.0],
                        value=b"updated",
                    )
                ]
            )
            outcome = current.build_composite_hnsw(base, hnsw)
            self.assertEqual(outcome.stats.vector_updated_records, 1)
            self.assertEqual(outcome.reasons, [])
            composite = outcome.accelerator
            self.assertIsNotNone(composite)
            request = exact_proximity_search_request([0.0, 0.0], 3)
            request.policy = SearchPolicyKind.FIXED_BUDGET
            request.backend = SearchBackendRecord.COMPOSITE
            runtime = engine.proximity_search_runtime()
            with composite:
                self.assertEqual(composite.base_kind, CompositeBaseKindRecord.HNSW)
                self.assertEqual(composite.current_source_descriptor, current.descriptor)
                self.assertEqual(composite.base_source_descriptor, base.descriptor)
                self.assertEqual(composite.delta_count, 1)
                self.assertEqual(composite.shadow_count, 1)
                self.assertEqual(
                    composite.search(current, request).backend,
                    SearchBackendRecord.COMPOSITE,
                )
                self.assertEqual(
                    composite.search_with_runtime(current, request, runtime).backend,
                    SearchBackendRecord.COMPOSITE,
                )
                with composite.prove_search(current, request) as proof:
                    self.assertEqual(
                        proof.verify(current.descriptor).result.backend,
                        SearchBackendRecord.COMPOSITE,
                    )
                manifest = composite.manifest
                catalog = current.build_accelerator_catalog(composite=composite)
                self.assertEqual(catalog.source_descriptor, current.descriptor)
                self.assertEqual(
                    catalog.entries[0].kind, CatalogAcceleratorKindRecord.COMPOSITE
                )
                self.assertEqual(
                    catalog.search(current, request).backend,
                    SearchBackendRecord.COMPOSITE,
                )
                self.assertEqual(
                    catalog.search_with_runtime(current, request, runtime).backend,
                    SearchBackendRecord.COMPOSITE,
                )
                catalog_manifest = catalog.manifest
                catalog.close()
            runtime.close()

            with current.load_composite(manifest) as loaded:
                self.assertEqual(loaded.manifest, manifest)
            with current.load_accelerator_catalog(catalog_manifest) as loaded:
                self.assertEqual(loaded.manifest, catalog_manifest)

            forced = default_composite_accelerator_config()
            forced.max_delta_records = 0
            rebuilt = current.build_or_rebuild_composite_hnsw(
                base, hnsw, config=forced
            )
            self.assertEqual(
                rebuilt.kind, CompositeBuildOrRebuildKindRecord.HNSW_REBUILT
            )
            self.assertIsNotNone(rebuilt.hnsw)
            rebuilt.hnsw.close()
            hnsw.close()
            current.close()
            base.close()

    def test_product_quantizer_lifecycle_is_portable_and_bounded(self):
        with Engine.memory() as engine:
            proximity = engine.build_proximity(
                dimensions=4,
                records=[
                    ProximityRecord(
                        f"vector-{index:02}".encode(),
                        [float(index), float(index % 3), 0.0, 1.0],
                        f"value-{index:02}".encode(),
                    )
                    for index in range(16)
                ],
            )
            config = ProductQuantizationConfigRecord(
                subquantizers=2,
                centroids_per_subquantizer=4,
                training_iterations=2,
                rerank_multiplier=4,
                seed=(1 << 64) - 1,
                max_training_vectors=16,
            )
            built = proximity.build_pq(config=config, worker_threads=2)
            self.assertEqual(built.stats.encoded_vectors, 16)
            request = exact_proximity_search_request([0.0, 0.0, 0.0, 1.0], 3)
            request.policy = SearchPolicyKind.FIXED_BUDGET
            request.backend = SearchBackendRecord.PRODUCT_QUANTIZED

            with built.index as index:
                self.assertEqual(index.config, config)
                self.assertEqual(index.source_descriptor, proximity.descriptor)
                self.assertTrue(index.quality.mean_squared_error >= 0.0)
                result = index.search(proximity, request)
                self.assertEqual(result.backend, SearchBackendRecord.PRODUCT_QUANTIZED)
                self.assertEqual(result.neighbors[0].key, b"vector-00")
                with engine.proximity_search_runtime() as runtime:
                    self.assertEqual(
                        index.search_with_runtime(proximity, request, runtime).backend,
                        SearchBackendRecord.PRODUCT_QUANTIZED,
                    )
                manifest = index.manifest
                with index.prove_search(proximity, request) as proof:
                    self.assertEqual(
                        proof.verify(proximity.descriptor).result.backend,
                        SearchBackendRecord.PRODUCT_QUANTIZED,
                    )

            with proximity.load_pq(manifest) as loaded:
                self.assertEqual(loaded.manifest, manifest)

    def test_hnsw_accelerator_lifecycle_is_portable(self):
        with Engine.memory() as engine:
            proximity = engine.build_proximity(
                dimensions=2,
                records=[
                    ProximityRecord(
                        f"vector-{index:02}".encode(),
                        [float(index), 0.0],
                        f"value-{index:02}".encode(),
                    )
                    for index in range(16)
                ],
            )
            built = proximity.build_hnsw()
            self.assertEqual(built.stats.records, 16)
            request = exact_proximity_search_request([0.0, 0.0], 3)
            request.policy = SearchPolicyKind.FIXED_BUDGET
            request.backend = SearchBackendRecord.HNSW

            with built.index as index:
                self.assertTrue(index.is_canonical)
                self.assertEqual(index.source_descriptor, proximity.descriptor)
                result = index.search(proximity, request)
                self.assertEqual(result.backend, SearchBackendRecord.HNSW)
                self.assertEqual(result.neighbors[0].key, b"vector-00")
                with ProximityCancellationToken() as cancellation:
                    cancellation.cancel()
                    cancelled = index.search_cancellable(
                        proximity, request, cancellation=cancellation
                    )
                    self.assertEqual(
                        cancelled.completion, SearchCompletionRecord.CANCELLED
                    )
                    self.assertEqual(cancelled.neighbors, [])
                manifest = index.manifest
                with index.prove_search(proximity, request) as proof:
                    self.assertEqual(
                        proof.verify(proximity.descriptor).result.backend,
                        SearchBackendRecord.HNSW,
                    )

            with proximity.load_hnsw(manifest) as loaded:
                self.assertEqual(loaded.manifest, manifest)

    def test_proximity_rich_search_request_is_shared_by_map_session_and_proof(self):
        with Engine.memory() as engine:
            proximity = engine.build_proximity(
                dimensions=2,
                records=[
                    ProximityRecord(b"a", [0.0, 0.0], b"alpha"),
                    ProximityRecord(b"ab", [1.0, 0.0], b"alphabet"),
                    ProximityRecord(b"b", [0.1, 0.0], b"beta"),
                ],
            )
            request = ProximitySearchRequestRecord(
                query=[0.0, 0.0],
                k=3,
                policy=SearchPolicyKind.FIXED_BUDGET,
                adaptive_quality=None,
                budget=SearchBudgetRecord(
                    max_nodes=1_000,
                    max_committed_bytes=1_000_000,
                    max_distance_evaluations=1_000,
                    max_frontier_entries=1_000,
                ),
                filter=ProximityFilterRecord(
                    kind=ProximityFilterKind.PREFIX,
                    start=None,
                    range_end=None,
                    prefix=b"a",
                    eligible_keys=[],
                ),
                kernel=QueryKernelRecord.SCALAR_DETERMINISTIC,
                backend=SearchBackendRecord.AUTO,
                hnsw_ef_search=None,
                pq_rerank_multiplier=None,
            )

            result = proximity.search(request)
            self.assertEqual([neighbor.key for neighbor in result.neighbors], [b"a", b"ab"])
            self.assertGreater(result.stats.distance_evaluations, 0)
            self.assertGreater(result.plan_format_version, 0)
            scanned = []
            self.assertEqual(
                proximity.scan_records(
                    lambda record: scanned.append(record.key) is None and len(scanned) < 2
                ),
                2,
            )
            self.assertEqual(scanned, [b"a", b"ab"])
            with engine.proximity_search_runtime() as runtime, proximity.read() as session:
                self.assertEqual(
                    [
                        neighbor.key
                        for neighbor in proximity.search_with_runtime(
                            request, runtime
                        ).neighbors
                    ],
                    [b"a", b"ab"],
                )
                self.assertEqual(
                    [neighbor.key for neighbor in session.search(request).neighbors],
                    [b"a", b"ab"],
                )
                self.assertEqual(
                    [
                        neighbor.key
                        for neighbor in session.search_with_runtime(
                            request, runtime
                        ).neighbors
                    ],
                    [b"a", b"ab"],
                )
                retained = []
                self.assertEqual(
                    session.scan_records(
                        lambda record: retained.append(record.key) is None
                    ),
                    3,
                )
                self.assertEqual(retained, [b"a", b"ab", b"b"])
            with proximity.prove_search(request) as proof:
                verified = proof.verify(proximity.descriptor)
                self.assertEqual(
                    [neighbor.key for neighbor in verified.result.neighbors],
                    [b"a", b"ab"],
                )

    def test_versioned_bulk_publication_uses_native_performance_paths(self):
        with Engine.memory() as engine:
            versioned = engine.versioned_map(b"bulk-publication")
            initialized = versioned.initialize_sorted([
                EntryRecord(key=b"a", value=b"one"),
                EntryRecord(key=b"b", value=b"two"),
            ])
            appended = versioned.append([
                MutationRecord(kind=MutationKind.UPSERT, key=b"c", value=b"three")
            ])
            self.assertEqual(versioned.get(b"c"), b"three")
            parallel = versioned.parallel_apply([
                MutationRecord(kind=MutationKind.UPSERT, key=b"b", value=b"updated"),
                MutationRecord(kind=MutationKind.UPSERT, key=b"d", value=b"four"),
            ], ParallelConfigRecord(max_threads=1, parallelism_threshold=1))
            self.assertEqual(parallel.stats.input_mutations, 2)
            rebuilt = versioned.rebuild_sorted_if(parallel.version.id, [
                EntryRecord(key=b"x", value=b"nine"),
                EntryRecord(key=b"y", value=b"ten"),
            ])
            self.assertEqual(rebuilt.kind.name.lower(), "applied")
            current = rebuilt.current.id
            iter_rebuilt = versioned.rebuild_from_entries_if(current, [
                EntryRecord(key=b"q", value=b"queue"),
                EntryRecord(key=b"p", value=b"priority"),
            ])
            self.assertIsNotNone(iter_rebuilt.current)
            self.assertEqual(versioned.get(b"p"), b"priority")
            self.assertIsNotNone(initialized.current)
            self.assertIsNotNone(appended.id)

    def test_versioned_and_proximity_hard_cutover(self):
        with Engine.memory() as engine:
            versioned = engine.versioned_map(b"users")
            versioned.initialize()
            versioned.put(b"u1", b"Ada")
            self.assertEqual(versioned.get(b"u1"), b"Ada")

            registry = engine.index_registry()
            registry.register(
                b"by_team",
                1,
                "team-v1",
                IndexProjection.ALL,
                lambda _key, value: [(value, None)],
            )
            indexed = engine.indexed_map(b"members", registry)
            indexed.put(b"u1", b"red")
            indexed.ensure_index(b"by_team")
            records = indexed.snapshot().index(b"by_team").records(b"red")
            self.assertEqual([record.primary_key for record in records], [b"u1"])
            self.assertEqual(indexed.id, b"members")
            applied = indexed.apply([
                MutationRecord(kind=MutationKind.UPSERT, key=b"u2", value=b"red")
            ])
            conditional = indexed.apply_if(applied.source_version, [
                MutationRecord(kind=MutationKind.UPSERT, key=b"u3", value=b"blue")
            ])
            self.assertIsNotNone(conditional.current)
            historical = indexed.snapshot_at(applied.source_version)
            self.assertEqual(len(historical.index(b"by_team").exact_page(b"red").matches), 2)
            current = indexed.snapshot()
            self.assertEqual(indexed.snapshot_by_id(current.id).id, current.id)
            secondary = current.index(b"by_team")
            self.assertEqual(secondary.name, b"by_team")
            self.assertEqual(len(secondary.prefix_reverse_page(b"r").matches), 2)

            proximity = engine.build_proximity(
                dimensions=2,
                records=[ProximityRecord(b"a", [0.0, 0.0], b"alpha")],
            )
            self.assertEqual(proximity.search_exact([0.1, 0.1], 1).neighbors[0].key, b"a")
            escaped = None

            def visit(rows):
                nonlocal escaped
                escaped = rows[0].key
                return bytes(rows[0].key)

            key = proximity.search_view([0.1, 0.1], 1, visit)
            self.assertEqual(key, b"a")
            with self.assertRaises(RuntimeError):
                bytes(escaped)

            with proximity.read() as session:
                self.assertTrue(session.contains(b"a"))

    def test_async_wrapper_copies_inputs_before_scheduling(self):
        async def run():
            with Engine.memory() as engine:
                versioned = engine.versioned_map(b"async")
                versioned.initialize()
                key = bytearray(b"k")
                pending = versioned.put_async(key, b"v")
                key[:] = b"x"
                await pending
                self.assertEqual(versioned.get(b"k"), b"v")
                with versioned.snapshot() as snapshot:
                    bundle = await snapshot.export_async()
                imported = engine.versioned_map(b"async-import")
                pending_import = imported.import_as_head_async(bundle)
                bundle.nodes[0].bytes = b"mutated-after-handoff"
                await pending_import
                self.assertEqual(imported.get(b"k"), b"v")

        import asyncio

        asyncio.run(run())

    def test_application_versioned_async_surface(self):
        async def run():
            with Engine.memory() as engine:
                versioned = engine.versioned_map(b"async-surface")
                initial = await versioned.initialize_async()
                subscription = await versioned.subscribe_async()
                updated = await versioned.put_async(b"k", b"v")
                self.assertEqual((await versioned.head_async()).id, updated.id)
                self.assertEqual(await versioned.get_async(b"k"), b"v")
                snapshot = await versioned.snapshot_at_async(updated.id)
                self.assertEqual(await snapshot.get_async(b"k"), b"v")
                with snapshot.read() as session:
                    self.assertEqual(await session.get_async(b"k"), b"v")
                self.assertTrue(verify_key_proof(await snapshot.prove_key_async(b"k")).valid)
                event = await subscription.poll_async()
                self.assertEqual(event.previous, initial.id)
                snapshot.close()
                subscription.close()

        import asyncio

        asyncio.run(run())

    def test_proximity_async_wrapper_uses_native_cooperative_cancellation(self):
        async def run():
            with Engine.memory() as engine:
                proximity = engine.build_proximity(
                    dimensions=2,
                    records=[
                        ProximityRecord(
                            f"vector-{index:04}".encode(),
                            [float(index), float(index % 7)],
                            index.to_bytes(8, "little"),
                        )
                        for index in range(256)
                    ],
                )
                request = exact_proximity_search_request([0.0, 0.0], 10)
                with engine.proximity_search_runtime() as runtime:
                    with ProximityCancellationToken() as cancellation:
                        cancellation.cancel()
                        result = await proximity.search_async(
                            request,
                            runtime=runtime,
                            cancellation=cancellation,
                        )
                        with proximity.read() as session:
                            session_result = await session.search_async(
                                request,
                                runtime=runtime,
                                cancellation=cancellation,
                            )
                self.assertEqual(result.completion, SearchCompletionRecord.CANCELLED)
                self.assertEqual(result.neighbors, [])
                self.assertEqual(
                    session_result.completion, SearchCompletionRecord.CANCELLED
                )
                self.assertEqual(session_result.neighbors, [])

        import asyncio

        asyncio.run(run())

    def test_versioned_snapshot_lifecycle(self):
        with Engine.memory() as engine:
            versioned = engine.versioned_map(b"versioned-lifecycle")
            self.assertEqual(versioned.id, b"versioned-lifecycle")
            self.assertFalse(versioned.is_initialized())
            initial = versioned.initialize()
            self.assertTrue(versioned.is_initialized())
            self.assertEqual(versioned.head_id(), initial.id)
            first = versioned.put(b"k", b"v1")
            versioned.put(b"k", b"v2")
            self.assertEqual(versioned.head().id, versioned.head_id())
            self.assertEqual(versioned.version(first.id).id, first.id)
            self.assertGreaterEqual(len(versioned.versions()), 3)
            with versioned.snapshot_at(first.id) as historical:
                self.assertEqual(historical.id, first.id)
                self.assertEqual(historical.version.id, first.id)
                self.assertEqual(historical.get(b"k"), b"v1")

    def test_versioned_snapshot_exposes_ordered_navigation_and_bounded_pages(self):
        with Engine.memory() as engine:
            versioned = engine.versioned_map(b"versioned-ordered")
            versioned.initialize()
            versioned.apply([
                MutationRecord(kind=MutationKind.UPSERT, key=b"a", value=b"one"),
                MutationRecord(kind=MutationKind.UPSERT, key=b"ab", value=b"two"),
                MutationRecord(kind=MutationKind.UPSERT, key=b"b", value=b"three"),
                MutationRecord(kind=MutationKind.UPSERT, key=b"c", value=b"four"),
            ])
            with versioned.snapshot() as snapshot:
                self.assertTrue(snapshot.contains(b"ab"))
                self.assertEqual(snapshot.get_many([b"a", b"missing"]), [b"one", None])
                self.assertEqual(snapshot.first_entry().key, b"a")
                self.assertEqual(snapshot.last_entry().key, b"c")
                self.assertEqual(snapshot.lower_bound(b"aa").key, b"ab")
                self.assertEqual(snapshot.upper_bound(b"ab").key, b"b")
                self.assertEqual([entry.key for entry in snapshot.prefix(b"a")], [b"a", b"ab"])
                self.assertEqual([entry.key for entry in snapshot.range(b"ab", b"c")], [b"ab", b"b"])

                prefix_page = snapshot.prefix_page(b"a", None, 1)
                self.assertEqual([entry.key for entry in prefix_page.entries], [b"a"])
                self.assertIsNotNone(prefix_page.next_cursor)

                first = snapshot.range_page(None, b"c", 2)
                self.assertEqual([entry.key for entry in first.entries], [b"a", b"ab"])
                self.assertIsNotNone(first.next_cursor)
                second = snapshot.range_page(first.next_cursor, b"c", 2)
                self.assertEqual([entry.key for entry in second.entries], [b"b"])

                reverse = snapshot.reverse_page(None, b"a", 2)
                self.assertEqual([entry.key for entry in reverse.entries], [b"c", b"b"])
                prefixed = snapshot.prefix_reverse_page(b"a", None, 2)
                self.assertEqual([entry.key for entry in prefixed.entries], [b"ab", b"a"])

    def test_versioned_batch_cas_and_pinned_point_reads(self):
        with Engine.memory() as engine:
            versioned = engine.versioned_map(b"versioned-cas")
            versioned.initialize()
            first = versioned.apply([
                MutationRecord(kind=MutationKind.UPSERT, key=b"a", value=b"one"),
                MutationRecord(kind=MutationKind.UPSERT, key=b"b", value=b"two"),
            ])
            self.assertTrue(versioned.contains(b"a"))
            self.assertEqual(versioned.get_many([b"a", b"missing"]), [b"one", None])
            applied = versioned.put_if(first.id, b"a", b"updated")
            self.assertEqual(applied.kind.name, "APPLIED")
            self.assertEqual(versioned.delete_if(first.id, b"b").kind.name, "CONFLICT")
            self.assertEqual(versioned.get_many_at(first.id, [b"a", b"b"]), [b"one", b"two"])
            self.assertEqual(versioned.get_at(first.id, b"a"), b"one")
            result = versioned.apply_if(applied.current.id, [
                MutationRecord(kind=MutationKind.DELETE, key=b"b", value=None)
            ])
            self.assertEqual(result.kind.name, "APPLIED")

    def test_versioned_backup_restore_and_retention(self):
        with Engine.memory() as source_engine, Engine.memory() as target_engine:
            source = source_engine.versioned_map(b"versioned-backup")
            source.initialize()
            source.put(b"k", b"v1")
            source.put(b"k", b"v2")
            target = target_engine.versioned_map(b"versioned-backup")
            restored = target.restore_backup(source.backup())
            self.assertEqual(restored.id, source.head_id())
            self.assertEqual(target.get(b"k"), b"v2")
            with source.snapshot() as snapshot:
                bundle = snapshot.export()
            imported_map = target_engine.versioned_map(b"versioned-import")
            imported = imported_map.import_as_head(bundle)
            self.assertTrue(imported.is_head)
            self.assertEqual(imported_map.get(b"k"), b"v2")
            timestamped_map = target_engine.versioned_map(b"versioned-import-at")
            timestamped = timestamped_map.import_as_head_at_millis(bundle, 12_345)
            self.assertEqual(timestamped.created_at_millis, 12_345)
            self.assertEqual(timestamped_map.get(b"k"), b"v2")
            pruned = source.keep_last(1)
            self.assertTrue(pruned.retained)
            self.assertTrue(pruned.removed)

    def test_proofs_sessions_and_maintenance_are_application_facing(self):
        with Engine.memory() as engine:
            versioned = engine.versioned_map(b"proofs")
            versioned.initialize()
            versioned.put(b"k", b"v")
            versioned.put(b"ka", b"v2")
            with versioned.snapshot() as snapshot:
                verified = verify_key_proof(snapshot.prove_key(b"k"))
                self.assertTrue(verified.valid)
                self.assertEqual(verified.value, b"v")
                multi = verify_multi_key_proof(snapshot.prove_keys([b"k", b"missing"]))
                self.assertTrue(multi.valid)
                self.assertEqual([result.exists for result in multi.results], [True, False])
                ranged = verify_range_proof(snapshot.prove_range(b"k", b"l"))
                self.assertEqual([entry.key for entry in ranged.entries], [b"k", b"ka"])
                prefixed = verify_range_proof(snapshot.prove_prefix(b"k"))
                self.assertEqual([entry.key for entry in prefixed.entries], [b"k", b"ka"])
                proved_page = snapshot.prove_range_page(None, b"l", 1)
                page = verify_range_page_proof(proved_page.proof)
                self.assertTrue(page.valid)
                self.assertEqual([entry.key for entry in proved_page.page.entries], [b"k"])
                self.assertGreaterEqual(snapshot.stats().total_key_value_pairs, 1)
                self.assertGreater(len(snapshot.export().nodes), 0)
                with snapshot.read() as session:
                    self.assertEqual(session.get(b"k"), b"v")
                    escaped = []
                    seen = []

                    def visit_entry(view):
                        escaped.append(view.key)
                        seen.append((bytes(view.key), bytes(view.value)))
                        return True

                    outcome = session.scan_range_view(b"k", b"l", visit_entry)
                    self.assertEqual(outcome.visited, 2)
                    self.assertFalse(outcome.stopped)
                    self.assertEqual(seen, [(b"k", b"v"), (b"ka", b"v2")])
                    with self.assertRaises(RuntimeError):
                        bytes(escaped[0])
                    stopped = session.scan_range_view(b"k", b"l", lambda _entry: False)
                    self.assertEqual(stopped.visited, 1)
                    self.assertTrue(stopped.stopped)
            self.assertGreaterEqual(versioned.verify_catalog().version_count, 2)
            self.assertGreater(len(versioned.backup()), 0)
            self.assertGreaterEqual(len(versioned.plan_gc().reachability.live_cids), 1)

            registry = engine.index_registry()
            registry.register(
                b"by_value", 1, "value-v1", IndexProjection.ALL,
                lambda _key, value: [(value, None)],
            )
            indexed = engine.indexed_map(b"indexed-maintenance", registry)
            version = indexed.put(b"k", b"term")
            indexed.ensure_index(b"by_value")
            old_snapshot_id = indexed.snapshot().id
            too_small = default_secondary_index_limits()
            too_small.max_term_bytes = 3
            with self.assertRaises(Exception):
                indexed.replace_index(
                    b"by_value", 2, "value-too-small-v2", IndexProjection.ALL,
                    lambda _key, value: [(value, None)], too_small,
                )
            self.assertEqual(indexed.health().active_indexes[0].generation, 1)
            replacement = indexed.replace_index(
                b"by_value", 2, "value-v2", IndexProjection.ALL,
                lambda _key, value: [(value, None)],
            )
            self.assertEqual(replacement.generation, 2)
            self.assertEqual(indexed.health().active_indexes[0].generation, 2)
            self.assertEqual(
                len(indexed.snapshot_by_id(old_snapshot_id).index(b"by_value").exact(b"term")),
                1,
            )
            self.assertTrue(indexed.verify_index(b"by_value", version.source_version).valid)
            self.assertGreaterEqual(indexed.metrics().build_attempts, 1)
            self.assertGreater(len(indexed.export_current()), 0)
            self.assertGreaterEqual(len(indexed.keep_last(1).retained_source_versions), 1)

            proximity = engine.build_proximity(
                dimensions=2,
                records=[ProximityRecord(b"p", [0.0, 0.0], b"payload")],
            )
            proof = proximity.prove_membership(b"p")
            verified_membership = verify_proximity_membership_proof(
                proof, proximity.descriptor
            )
            self.assertEqual(verified_membership.record.value, b"payload")
            self.assertEqual(proximity.verify().record_count, 1)
            self.assertEqual(proximity.count, 1)
            self.assertTrue(proximity.contains(b"p"))
            self.assertEqual(proximity.config.dimensions, 2)
            structure = verify_proximity_structure_proof(
                proximity.prove_structure(), proximity.descriptor
            )
            self.assertEqual(structure.summary.record_count, 1)
            mutated, stats = proximity.mutate([
                ProximityMutationRecord(key=b"q", vector=[1.0, 1.0], value=b"second")
            ])
            self.assertEqual(mutated.count, 2)
            self.assertGreaterEqual(stats.records_rebuilt, 1)
            with proximity.read() as retained:
                self.assertEqual(
                    retained.search_exact([0.0, 0.0], 1).neighbors[0].key, b"p"
                )
                self.assertEqual(
                    retained.search_view(
                        [0.0, 0.0], 1, lambda rows: bytes(rows[0].key)
                    ),
                    b"p",
                )
            with proximity.prove_search_exact([0.0, 0.0], 1) as search_proof:
                verified_search = search_proof.verify(proximity.descriptor)
                self.assertEqual(verified_search.result.neighbors[0].key, b"p")
                self.assertGreater(verified_search.replayed_events, 0)

    def test_versioned_comparison_pins_versions_and_pages_diffs(self):
        with Engine.memory() as engine:
            versioned = engine.versioned_map(b"comparison")
            base = versioned.initialize()
            target = versioned.put(b"k", b"v")
            with versioned.compare(base.id, target.id) as comparison:
                self.assertEqual(comparison.base.id, base.id)
                self.assertEqual(comparison.target.id, target.id)
                self.assertEqual([diff.key for diff in comparison.diff()], [b"k"])
                self.assertEqual([diff.key for diff in comparison.diff_page(limit=1).diffs], [b"k"])

    def test_versioned_history_navigation_diff_and_rollback(self):
        with Engine.memory() as engine:
            versioned = engine.versioned_map(b"history-navigation")
            versioned.initialize()
            versioned.put(b"a", b"one")
            versioned.put(b"ab", b"two")
            base = versioned.put(b"b", b"three")
            target = versioned.put(b"a", b"updated")

            self.assertEqual([entry.key for entry in versioned.range(b"a", b"c")], [b"a", b"ab", b"b"])
            self.assertEqual([entry.key for entry in versioned.prefix(b"a")], [b"a", b"ab"])
            self.assertEqual(versioned.range_at(base.id, b"a", b"b")[0].value, b"one")
            self.assertEqual([entry.key for entry in versioned.prefix_at(base.id, b"a")], [b"a", b"ab"])
            self.assertEqual([entry.key for entry in versioned.range_page(limit=2).entries], [b"a", b"ab"])
            self.assertEqual([entry.key for entry in versioned.prefix_page(b"a", limit=1).entries], [b"a"])
            historical_page = versioned.prefix_page_at(base.id, b"a", limit=1)
            self.assertEqual([entry.key for entry in historical_page.entries], [b"a"])
            self.assertIsNotNone(historical_page.next_cursor)
            self.assertEqual([diff.key for diff in versioned.diff(base.id, target.id)], [b"a"])
            self.assertEqual([diff.key for diff in versioned.changes_since(base.id)], [b"a"])

            rolled_back = versioned.rollback_to(base.id)
            self.assertEqual(versioned.head_id(), rolled_back.id)
            self.assertEqual(versioned.get(b"a"), b"one")
            self.assertEqual(versioned.changes_since(base.id), [])

    def test_versioned_timestamped_writes_and_complete_maintenance_records(self):
        with Engine.memory() as engine:
            versioned = engine.versioned_map(b"maintenance-complete")
            first = versioned.apply_at_millis([
                MutationRecord(kind=MutationKind.UPSERT, key=b"k", value=b"one")
            ], 1_000)
            second_update = versioned.apply_if_at_millis(first.id, [
                MutationRecord(kind=MutationKind.UPSERT, key=b"k", value=b"two")
            ], 2_000)
            second = second_update.current
            third = versioned.apply_at_millis([
                MutationRecord(kind=MutationKind.UPSERT, key=b"k", value=b"three")
            ], 3_000)

            self.assertEqual(first.created_at_millis, 1_000)
            self.assertEqual(second.created_at_millis, 2_000)
            self.assertEqual(versioned.retention_policy().kind.name, "PREFIX")
            verification = versioned.verify_catalog()
            self.assertEqual(verification.head, third.id)
            self.assertEqual(verification.version_count, 3)
            plan = versioned.plan_gc()
            self.assertGreater(plan.reachability.live_nodes, 0)
            self.assertGreaterEqual(plan.candidate_nodes, plan.reclaimable_nodes)

            aged = versioned.keep_for_at(3_000, 1_500)
            self.assertIn(second.id, aged.retained)
            self.assertIn(first.id, aged.removed)
            explicit = versioned.keep_versions([second.id])
            self.assertIn(third.id, explicit.retained)
            pruned = versioned.prune_versions(0)
            self.assertEqual(pruned.retained, [third.id])
            self.assertIn(second.id, pruned.removed)
            self.assertIn(third.id, versioned.keep_for(10_000).retained)
            sweep = versioned.sweep_gc()
            self.assertGreaterEqual(sweep.deleted_nodes, 0)

    def test_versioned_subscription_resumes_and_polls_owned_diffs(self):
        with Engine.memory() as engine:
            versioned = engine.versioned_map(b"subscription")
            initial = versioned.initialize()
            with versioned.subscribe() as subscription:
                self.assertEqual(subscription.last_seen, initial.id)
                self.assertIsNone(subscription.poll())
                current = versioned.put(b"k", b"v")
                event = subscription.poll()
                self.assertEqual(event.previous, initial.id)
                self.assertEqual(event.current.id, current.id)
                self.assertEqual([diff.key for diff in event.diffs], [b"k"])
                self.assertEqual(subscription.last_seen, current.id)

    def test_multi_map_transaction_is_atomic_and_reads_staged_values(self):
        with Engine.memory() as engine:
            tx = engine.begin_versioned_transaction()
            tx.put(b"a", b"k", b"one")
            tx.put(b"b", b"k", b"two")
            self.assertEqual(tx.get(b"a", b"k"), b"one")
            committed = tx.commit()
            self.assertTrue(committed.applied)
            self.assertEqual(len(committed.versions), 2)
            self.assertEqual(engine.versioned_map(b"a").get(b"k"), b"one")
            self.assertEqual(engine.versioned_map(b"b").get(b"k"), b"two")
            rolled_back = engine.begin_versioned_transaction()
            rolled_back.put(b"a", b"discard", b"x")
            rolled_back.rollback()
            self.assertIsNone(engine.versioned_map(b"a").get(b"discard"))

    def test_pinned_merge_pages_conflicts_and_cas_publishes(self):
        with Engine.memory() as engine:
            versioned = engine.versioned_map(b"merge")
            base = versioned.initialize()
            candidate = versioned.put(b"k", b"candidate")
            versioned.put(b"k", b"head")
            with versioned.prepare_merge(base.id, candidate.id) as merge:
                self.assertEqual(merge.base.id, base.id)
                self.assertEqual(merge.candidate.id, candidate.id)
                self.assertEqual([row.key for row in merge.conflict_page(limit=1).conflicts], [b"k"])
                published = merge.publish("prefer_right")
                self.assertEqual(published.current.id, candidate.id)
            self.assertEqual(versioned.get(b"k"), b"candidate")


if __name__ == "__main__":
    unittest.main()
