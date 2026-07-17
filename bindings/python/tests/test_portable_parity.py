import unittest

from prolly import (
    Engine,
    IndexProjection,
    ProximityRecord,
    verify_key_proof,
    verify_proximity_membership_proof,
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

    def test_proofs_sessions_and_maintenance_are_application_facing(self):
        with Engine.memory() as engine:
            versioned = engine.versioned_map(b"proofs")
            versioned.initialize()
            versioned.put(b"k", b"v")
            with versioned.snapshot() as snapshot:
                verified = verify_key_proof(snapshot.prove_key(b"k"))
                self.assertTrue(verified.valid)
                self.assertEqual(verified.value, b"v")
                self.assertGreaterEqual(snapshot.stats().total_key_value_pairs, 1)
                self.assertGreater(len(snapshot.export().nodes), 0)
                with snapshot.read() as session:
                    self.assertEqual(session.get(b"k"), b"v")
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


if __name__ == "__main__":
    unittest.main()
