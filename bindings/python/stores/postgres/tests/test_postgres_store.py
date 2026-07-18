import asyncio
import importlib
import os
import unittest

from prolly.remote_store import missing_bytes, present_bytes
from prolly.uniffi import prolly as ffi

try:
    PostgresStore = importlib.import_module("prolly_store_postgres").PostgresStore
except (ImportError, AttributeError):
    PostgresStore = None


@unittest.skipUnless(os.environ.get("PROLLY_POSTGRES_URL"), "PROLLY_POSTGRES_URL is not set")
class PostgresStoreTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(PostgresStore, "PostgreSQL adapter is not implemented")
        from psycopg_pool import AsyncConnectionPool

        self.pool = AsyncConnectionPool(os.environ["PROLLY_POSTGRES_URL"], min_size=1, max_size=40, open=False)
        await self.pool.open()
        self.store = PostgresStore(self.pool)
        await self.store.initialize_schema()
        async with self.pool.connection() as connection:
            await connection.execute("TRUNCATE prolly_nodes, prolly_hints, prolly_roots")

    async def asyncTearDown(self):
        if hasattr(self, "store"):
            await self.store.close()
        if hasattr(self, "pool"):
            await self.pool.close()

    async def test_protocol_layout_contention_and_rollback(self):
        cid = b"c" * 32
        await self.store.put_node(cid, b"")
        ordered = await self.store.batch_get_nodes_ordered([cid, b"m" * 32, cid])
        self.assertEqual([item.present for item in ordered.values], [True, False, True])

        async with self.pool.connection() as connection:
            row = await (await connection.execute(
                "SELECT node FROM prolly_nodes WHERE cid = %s", (cid,)
            )).fetchone()
            self.assertEqual(bytes(row[0]), b"")

        contenders = await asyncio.gather(*[
            self.store.compare_and_swap_root_manifest(
                b"main", missing_bytes(), present_bytes(index.to_bytes(2, "big"))
            )
            for index in range(32)
        ])
        self.assertEqual(sum(result.applied for result in contenders), 1)

        transaction = await self.store.commit_transaction(
            [ffi.NodeMutationRecord(key=b"x" * 32, value=present_bytes(b"must-not-write"))],
            [ffi.RootConditionRecord(name=b"main", expected=missing_bytes())],
            [ffi.RootWriteRecord(name=b"other", replacement=present_bytes(b"must-not-publish"))],
        )
        self.assertFalse(transaction.applied)
        self.assertFalse((await self.store.get_node(b"x" * 32)).value.present)

    async def test_rust_engine_cancellation_and_borrowed_pool(self):
        engine = await ffi.open_remote_prolly_engine(self.store, ffi.default_config())
        tree = await engine.put(engine.create(), b"key", b"value")
        self.assertEqual(await engine.get(tree, b"key"), b"value")

        async def blocked_query():
            async with self.pool.connection() as connection:
                await connection.execute("SELECT pg_sleep(10)")

        task = asyncio.create_task(blocked_query())
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        await self.store.close()
        async with self.pool.connection() as connection:
            row = await (await connection.execute("SELECT 1")).fetchone()
            self.assertEqual(row[0], 1)


if __name__ == "__main__":
    unittest.main()
