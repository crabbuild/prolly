import asyncio
import importlib
import os
import unittest
from urllib.parse import urlparse

from prolly.remote_store import missing_bytes, present_bytes
from prolly.uniffi import prolly as ffi

try:
    MysqlStore = importlib.import_module("prolly_store_mysql").MysqlStore
except (ImportError, AttributeError):
    MysqlStore = None


@unittest.skipUnless(os.environ.get("PROLLY_MYSQL_URL"), "PROLLY_MYSQL_URL is not set")
class MysqlStoreTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(MysqlStore, "MySQL adapter is not implemented")
        import aiomysql

        url = urlparse(os.environ["PROLLY_MYSQL_URL"])
        self.pool = await aiomysql.create_pool(
            host=url.hostname, port=url.port or 3306, user=url.username,
            password=url.password, db=url.path.lstrip("/"), minsize=1,
            maxsize=40, autocommit=True,
        )
        self.store = MysqlStore(self.pool)
        await self.store.initialize_schema()
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                for table in ("prolly_nodes", "prolly_hints", "prolly_roots"):
                    await cursor.execute(f"TRUNCATE {table}")

    async def asyncTearDown(self):
        if hasattr(self, "store"):
            await self.store.close()
        if hasattr(self, "pool"):
            self.pool.close()
            await self.pool.wait_closed()

    async def test_protocol_layout_contention_and_rollback(self):
        cid = b"c" * 32
        await self.store.put_node(cid, b"")
        ordered = await self.store.batch_get_nodes_ordered([cid, b"m" * 32, cid])
        self.assertEqual([item.present for item in ordered.values], [True, False, True])

        contenders = await asyncio.gather(*[
            self.store.compare_and_swap_root_manifest(
                b"main", missing_bytes(), present_bytes(index.to_bytes(2, "big"))
            ) for index in range(32)
        ])
        self.assertEqual(sum(result.applied for result in contenders), 1)

        result = await self.store.commit_transaction(
            [ffi.NodeMutationRecord(key=b"x" * 32, value=present_bytes(b"must-not-write"))],
            [ffi.RootConditionRecord(name=b"main", expected=missing_bytes())],
            [],
        )
        self.assertFalse(result.applied)
        self.assertFalse((await self.store.get_node(b"x" * 32)).value.present)

        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.columns WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='prolly_nodes' AND COLUMN_NAME='cid'")
                self.assertEqual(await cursor.fetchone(), ("varbinary", 32))

    async def test_rust_engine_cancellation_limits_and_borrowed_pool(self):
        engine = await ffi.open_remote_prolly_engine(self.store, ffi.default_config())
        tree = await engine.put(engine.create(), b"key", b"value")
        self.assertEqual(await engine.get(tree, b"key"), b"value")

        too_long = await self.store.put_node(b"x" * 33, b"value")
        self.assertEqual(too_long.error.code, "invalid_argument")

        task = asyncio.create_task(self.store.get_node(b"key"))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        await self.store.close()
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT 1")
                self.assertEqual((await cursor.fetchone())[0], 1)


if __name__ == "__main__":
    unittest.main()
