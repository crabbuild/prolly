import asyncio
import importlib
import os
import time
import unittest

from prolly.remote_store import missing_bytes, present_bytes
from prolly.uniffi import prolly as ffi

try:
    DynamoDbStore = importlib.import_module("prolly_store_dynamodb").DynamoDbStore
except (ImportError, AttributeError):
    DynamoDbStore = None


@unittest.skipUnless(os.environ.get("PROLLY_DYNAMODB_ENDPOINT"), "PROLLY_DYNAMODB_ENDPOINT is not set")
class DynamoDbStoreTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(DynamoDbStore, "DynamoDB adapter is not implemented")
        import aioboto3

        self.context = aioboto3.Session().client(
            "dynamodb", endpoint_url=os.environ["PROLLY_DYNAMODB_ENDPOINT"],
            region_name="us-west-2", aws_access_key_id="local", aws_secret_access_key="local",
        )
        self.client = await self.context.__aenter__()
        self.table = f"prolly_python_{os.getpid()}_{time.time_ns()}"
        self.prefix = b"prolly:test:python:"
        self.store = DynamoDbStore(self.client, table_name=self.table, key_prefix=self.prefix)
        await self.store.initialize_table()

    async def asyncTearDown(self):
        if hasattr(self, "store"):
            await self.store.close()
        if hasattr(self, "client"):
            try:
                await self.client.delete_table(TableName=self.table)
            except Exception:
                pass
        if hasattr(self, "context"):
            await self.context.__aexit__(None, None, None)

    async def test_layout_limits_contention_and_rollback(self):
        descriptor = await self.store.descriptor()
        self.assertEqual(descriptor.value.limits.max_batch_read_items, 100)
        self.assertEqual(descriptor.value.limits.max_batch_write_items, 25)
        self.assertEqual(descriptor.value.limits.max_transaction_operations, 100)

        cid = bytes([0, 127, 128, 255]) * 8
        await self.store.put_node(cid, b"node")
        raw = await self.client.get_item(TableName=self.table, Key={"pk": {"B": self.prefix + b"node:" + cid}})
        self.assertEqual(bytes(raw["Item"]["value"]["B"]), b"node")
        ordered = await self.store.batch_get_nodes_ordered([cid, b"missing", cid])
        self.assertEqual([value.present for value in ordered.values], [True, False, True])

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

    async def test_rust_engine_cancellation_and_borrowed_client(self):
        engine = await ffi.open_remote_prolly_engine(self.store, ffi.default_config())
        tree = await engine.put(engine.create(), b"key", b"value")
        self.assertEqual(await engine.get(tree, b"key"), b"value")
        task = asyncio.create_task(self.store.get_node(b"key"))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await self.store.close()
        self.assertIn("Table", await self.client.describe_table(TableName=self.table))


if __name__ == "__main__":
    unittest.main()
