import unittest

from prolly import Engine, IndexProjection, ProximityRecord


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


if __name__ == "__main__":
    unittest.main()
