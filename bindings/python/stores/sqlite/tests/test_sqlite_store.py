import asyncio
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from prolly.remote_store import missing_bytes, present_bytes
from prolly.uniffi import prolly as ffi
from prolly_store_sqlite import SqliteStore


class SqliteStoreTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "prolly.sqlite3"
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="prolly-sqlite")
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.store = SqliteStore(self.connection, self.executor)
        await self.store.initialize_schema()

    async def asyncTearDown(self):
        await self.store.close()
        self.connection.close()
        self.executor.shutdown(wait=True)
        self.temporary.cleanup()

    async def test_conformance_layout_contention_and_rollback(self):
        cid = b"c" * 32
        await self.store.put_node(cid, b"")
        result = await self.store.batch_get_nodes_ordered([cid, b"m" * 32, cid])
        self.assertEqual([value.present for value in result.values], [True, False, True])
        self.assertEqual(result.values[0].value, b"")

        await self.store.put_hint(b"namespace", b"key", b"hint")
        await self.store.batch_put_nodes_with_hint(
            [ffi.NodeEntryRecord(key=b"n" * 32, value=b"node")], b"namespace", b"batch", b"value"
        )
        self.assertEqual((await self.store.get_hint(b"namespace", b"batch")).value.value, b"value")

        name = b"main"
        contenders = await asyncio.gather(*[
            self.store.compare_and_swap_root_manifest(name, missing_bytes(), present_bytes(bytes([index])))
            for index in range(32)
        ])
        self.assertEqual(sum(result.applied for result in contenders), 1)
        current = (await self.store.get_root_manifest(name)).value

        transaction = await self.store.commit_transaction(
            [ffi.NodeMutationRecord(key=b"x" * 32, value=present_bytes(b"must-not-write"))],
            [ffi.RootConditionRecord(name=name, expected=present_bytes(b"wrong"))],
            [ffi.RootWriteRecord(name=name, replacement=present_bytes(b"must-not-publish"))],
        )
        self.assertFalse(transaction.applied)
        self.assertEqual((await self.store.get_root_manifest(name)).value, current)
        self.assertFalse((await self.store.get_node(b"x" * 32)).value.present)

        row = self.connection.execute("SELECT node FROM prolly_nodes WHERE cid = ?", (cid,)).fetchone()
        self.assertEqual(bytes(row[0]), b"")
        tables = {row[0] for row in self.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue({"prolly_nodes", "prolly_hints", "prolly_roots"}.issubset(tables))

    async def test_rust_engine_durability_cancellation_and_borrowed_ownership(self):
        engine = await ffi.open_remote_prolly_engine(self.store, ffi.default_config())
        tree = await engine.put(engine.create(), b"key", b"value")
        self.assertEqual(await engine.get(tree, b"key"), b"value")

        self.connection.create_function("prolly_wait", 0, lambda: __import__("time").sleep(0.2))
        task = asyncio.create_task(self.store._run(lambda: self.connection.execute("SELECT prolly_wait()").fetchone()))
        await asyncio.sleep(0.01)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        await asyncio.sleep(0.25)
        await self.store.close()
        self.assertEqual(self.connection.execute("SELECT 1").fetchone()[0], 1)

        reopened = sqlite3.connect(self.path)
        try:
            self.assertGreater(reopened.execute("SELECT count(*) FROM prolly_nodes").fetchone()[0], 0)
        finally:
            reopened.close()


if __name__ == "__main__":
    unittest.main()
