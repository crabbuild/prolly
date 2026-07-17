import asyncio
import importlib
import os
import unittest

from prolly.remote_store import missing_bytes, present_bytes
from prolly.uniffi import prolly as ffi

try:
    RedisStore = importlib.import_module("prolly_store_redis").RedisStore
except (ImportError, AttributeError):
    RedisStore = None


@unittest.skipUnless(os.environ.get("PROLLY_REDIS_URL"), "PROLLY_REDIS_URL is not set")
class RedisStoreTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(RedisStore, "Redis adapter is not implemented")
        import redis.asyncio as redis

        self.client = redis.from_url(os.environ["PROLLY_REDIS_URL"], decode_responses=False)
        self.prefix = f"prolly:test:python:{os.getpid()}:".encode()
        self.store = RedisStore(self.client, key_prefix=self.prefix)
        await self.store.clear_namespace()

    async def asyncTearDown(self):
        if hasattr(self, "client"):
            keys = [bytes(key) async for key in self.client.scan_iter() if bytes(key).startswith(self.prefix)]
            if keys:
                await self.client.delete(*keys)
        if hasattr(self, "store"):
            await self.store.close()
        if hasattr(self, "client"):
            await self.client.aclose()

    async def test_binary_layout_contention_and_rollback(self):
        cid = bytes([0, 127, 128, 255]) * 8
        namespace = bytes([0, 255, 1])
        hint_key = bytes([128, 0])
        await self.store.put_node(cid, b"node")
        await self.store.put_hint(namespace, hint_key, b"hint")
        self.assertEqual(await self.client.get(self.prefix + b"node:" + cid), b"node")
        encoded_hint = self.prefix + b"hint:" + len(namespace).to_bytes(8, "big") + namespace + hint_key
        self.assertEqual(await self.client.get(encoded_hint), b"hint")

        ordered = await self.store.batch_get_nodes_ordered([cid, b"missing", cid])
        self.assertEqual([item.present for item in ordered.values], [True, False, True])
        contenders = await asyncio.gather(*[
            self.store.compare_and_swap_root_manifest(
                b"main", missing_bytes(), present_bytes(index.to_bytes(2, "big"))
            ) for index in range(32)
        ])
        self.assertEqual(sum(result.applied for result in contenders), 1)

        transaction = await self.store.commit_transaction(
            [ffi.NodeMutationRecord(key=b"rollback", value=present_bytes(b"must-not-write"))],
            [ffi.RootConditionRecord(name=b"main", expected=missing_bytes())],
            [ffi.RootWriteRecord(name=b"other", replacement=present_bytes(b"must-not-publish"))],
        )
        self.assertFalse(transaction.applied)
        self.assertFalse((await self.store.get_node(b"rollback")).value.present)
        self.assertFalse((await self.store.get_root_manifest(b"other")).value.present)

    async def test_rust_engine_cancellation_and_borrowed_client(self):
        engine = await ffi.open_remote_prolly_engine(self.store, ffi.default_config())
        tree = await engine.put(engine.create(), b"key", b"value")
        self.assertEqual(await engine.get(tree, b"key"), b"value")

        task = asyncio.create_task(self.store.get_node(b"key"))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        await self.store.close()
        self.assertTrue(await self.client.ping())


if __name__ == "__main__":
    unittest.main()
