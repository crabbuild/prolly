import unittest

from prolly import (
    Engine,
    IndexProjection,
    MutationKind,
    MutationRecord,
    ProximityRecord,
    ProximityMutationRecord,
    verify_key_proof,
    verify_multi_key_proof,
    verify_range_page_proof,
    verify_range_proof,
    verify_proximity_membership_proof,
    verify_proximity_structure_proof,
)


class PortableParityTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
