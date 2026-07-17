import asyncio
import base64
import copy
import importlib
import os
import unittest
import uuid

from prolly.remote_store import missing_bytes, present_bytes
from prolly.uniffi import prolly as ffi

try:
    CosmosDbStore = importlib.import_module("prolly_store_cosmosdb").CosmosDbStore
except (ImportError, AttributeError):
    CosmosDbStore = None


class FakeCosmosError(Exception):
    def __init__(self, status_code, message="fake Cosmos failure"):
        super().__init__(message)
        self.status_code = status_code


class FakeContainer:
    def __init__(self):
        self.items = {}
        self.etag = 0
        self.partition_paths = ["/kind"]
        self.batch_partitions = []
        self.matched_etags = []
        self.calls = 0
        self.closed = False
        self.fail_upsert = None
        self.block_upsert = False

    async def read(self):
        self.calls += 1
        return {"partitionKey": {"paths": self.partition_paths}}

    async def read_item(self, item, partition_key):
        self.calls += 1
        value = self.items.get((partition_key, item))
        if value is None:
            raise FakeCosmosError(404)
        return copy.deepcopy(value)

    async def create_item(self, body):
        self.calls += 1
        key = (body["kind"], body["id"])
        if key in self.items:
            raise FakeCosmosError(409)
        self.items[key] = self._record(body)
        return copy.deepcopy(self.items[key])

    async def upsert_item(self, body):
        self.calls += 1
        if self.fail_upsert is not None:
            raise self.fail_upsert
        if self.block_upsert:
            await asyncio.Future()
        key = (body["kind"], body["id"])
        self.items[key] = self._record(body)
        return copy.deepcopy(self.items[key])

    async def replace_item(self, item, body, *, etag=None, match_condition=None):
        self.calls += 1
        key = (body["kind"], item)
        current = self.items.get(key)
        if current is None:
            raise FakeCosmosError(404)
        if etag is not None:
            self.matched_etags.append(etag)
            if current["_etag"] != etag:
                raise FakeCosmosError(412)
        self.items[key] = self._record(body)
        return copy.deepcopy(self.items[key])

    async def delete_item(self, item, partition_key, *, etag=None, match_condition=None):
        self.calls += 1
        key = (partition_key, item)
        current = self.items.get(key)
        if current is None:
            raise FakeCosmosError(404)
        if etag is not None:
            self.matched_etags.append(etag)
            if current["_etag"] != etag:
                raise FakeCosmosError(412)
        del self.items[key]

    def query_items(self, *, query, parameters, partition_key):
        self.calls += 1
        family = next(value["value"] for value in parameters if value["name"] == "@family")

        async def values():
            for (partition, _), document in list(self.items.items()):
                if partition == partition_key and document["family"] == family:
                    yield copy.deepcopy(document)

        return values()

    async def execute_item_batch(self, batch_operations, partition_key):
        self.calls += 1
        self.batch_partitions.append(partition_key)
        working = copy.deepcopy(self.items)
        for operation in batch_operations:
            name, arguments, *rest = operation
            options = rest[0] if rest else {}
            try:
                self._apply_batch(working, partition_key, name, arguments, options)
            except FakeCosmosError:
                raise
        self.items = working
        return [{"statusCode": 200} for _ in batch_operations]

    def close(self):
        self.closed = True

    def document(self, partition, item):
        return copy.deepcopy(self.items[(partition, item)])

    def _record(self, body):
        self.etag += 1
        return {**copy.deepcopy(body), "_etag": str(self.etag)}

    def _apply_batch(self, target, partition, name, arguments, options):
        if name in ("create", "upsert"):
            body = arguments[0]
            key = (partition, body["id"])
            if name == "create" and key in target:
                raise FakeCosmosError(409)
            target[key] = self._record(body)
            return
        item = arguments[0]
        key = (partition, item)
        current = target.get(key)
        if current is None:
            raise FakeCosmosError(404)
        etag = options.get("if_match_etag")
        if etag is not None:
            self.matched_etags.append(etag)
            if current["_etag"] != etag:
                raise FakeCosmosError(412)
        if name == "replace":
            target[key] = self._record(arguments[1])
        elif name == "delete":
            del target[key]
        else:
            raise FakeCosmosError(400)


class CosmosDbStoreTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(CosmosDbStore, "Cosmos DB adapter is not implemented")
        self.container = FakeContainer()
        self.partition = "tenant-a"
        self.prefix = b"prolly:test:python:"
        self.store = CosmosDbStore(
            self.container, partition_key=self.partition, key_prefix=self.prefix
        )

    async def test_layout_etags_transactions_and_rust_engine(self):
        await self.store.validate_container()
        descriptor = await self.store.descriptor()
        self.assertFalse(descriptor.value.capabilities.native_batch_reads)
        self.assertEqual(descriptor.value.limits.max_transaction_operations, 100)

        cid = bytes([0, 127, 128, 255]) * 8
        logical_key = self.prefix + b"node:" + cid
        document_id = "k" + logical_key.hex()
        await self.store.put_node(cid, b"value")
        raw = self.container.document(self.partition, document_id)
        self.assertEqual(
            {key: raw[key] for key in ("id", "kind", "family", "key", "value")},
            {
                "id": document_id,
                "kind": self.partition,
                "family": "node",
                "key": logical_key.hex(),
                "value": base64.b64encode(b"value").decode("ascii"),
            },
        )
        ordered = await self.store.batch_get_nodes_ordered([cid, b"missing", cid])
        self.assertEqual([item.present for item in ordered.values], [True, False, True])
        self.assertIn(cid, (await self.store.list_node_cids()).values)

        contenders = await asyncio.gather(*[
            self.store.compare_and_swap_root_manifest(
                b"main", missing_bytes(), present_bytes(index.to_bytes(2, "big"))
            ) for index in range(32)
        ])
        self.assertEqual(sum(result.applied for result in contenders), 1)
        current = await self.store.get_root_manifest(b"main")
        updated = await self.store.compare_and_swap_root_manifest(
            b"main", current.value, present_bytes(b"etag-update")
        )
        self.assertTrue(updated.applied)
        committed = await self.store.commit_transaction(
            [ffi.NodeMutationRecord(key=b"batch-node", value=present_bytes(b"batch-value"))],
            [ffi.RootConditionRecord(name=b"main", expected=present_bytes(b"etag-update"))],
            [ffi.RootWriteRecord(name=b"main", replacement=present_bytes(b"transaction-root"))],
        )
        self.assertTrue(committed.applied)
        self.assertTrue((await self.store.get_node(b"batch-node")).value.present)
        transaction = await self.store.commit_transaction(
            [ffi.NodeMutationRecord(key=b"rollback", value=present_bytes(b"must-not-write"))],
            [ffi.RootConditionRecord(name=b"main", expected=missing_bytes())],
            [ffi.RootWriteRecord(name=b"other", replacement=present_bytes(b"must-not-publish"))],
        )
        self.assertFalse(transaction.applied)
        self.assertFalse((await self.store.get_node(b"rollback")).value.present)
        self.assertFalse((await self.store.get_root_manifest(b"other")).value.present)

        engine = await ffi.open_remote_prolly_engine(self.store, ffi.default_config())
        tree = await engine.put(engine.create(), b"key", b"engine-value")
        self.assertEqual(await engine.get(tree, b"key"), b"engine-value")
        self.assertTrue(self.container.matched_etags)
        self.assertTrue(all(value == self.partition for value in self.container.batch_partitions))

        await self.store.close()
        self.assertFalse(self.container.closed)
        self.assertIn("partitionKey", await self.container.read())

    async def test_limit_error_redaction_cancellation_and_partition_validation(self):
        calls = self.container.calls
        nodes = [
            ffi.NodeMutationRecord(key=index.to_bytes(2, "big"), value=present_bytes(b"value"))
            for index in range(101)
        ]
        result = await self.store.commit_transaction(nodes, [], [])
        self.assertEqual(result.error.code, "resource_exhausted")
        self.assertEqual(self.container.calls, calls)

        secret = "cosmos-account-secret"
        self.container.fail_upsert = FakeCosmosError(503, secret)
        failure = await self.store.put_node(b"cid", b"value")
        self.assertEqual(failure.error.code, "unavailable")
        self.assertNotIn(secret, failure.error.message)
        self.container.fail_upsert = None

        self.container.block_upsert = True
        task = asyncio.create_task(self.store.put_node(b"cancel", b"value"))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.container.block_upsert = False

        other = FakeContainer()
        other.partition_paths = ["/wrong"]
        with self.assertRaises(Exception):
            await CosmosDbStore(other).validate_container()


@unittest.skipUnless(
    all(os.environ.get(name) for name in (
        "PROLLY_COSMOS_ENDPOINT", "PROLLY_COSMOS_KEY", "PROLLY_COSMOS_DATABASE",
    )),
    "Cosmos DB live credentials are not set",
)
class CosmosDbLiveTest(unittest.IsolatedAsyncioTestCase):
    async def test_cloud_round_trip(self):
        from azure.cosmos import PartitionKey
        from azure.cosmos.aio import CosmosClient

        async with CosmosClient(
            os.environ["PROLLY_COSMOS_ENDPOINT"],
            credential=os.environ["PROLLY_COSMOS_KEY"],
        ) as client:
            database = client.get_database_client(os.environ["PROLLY_COSMOS_DATABASE"])
            container = await database.create_container(
                id=f"prolly-python-{uuid.uuid4().hex}",
                partition_key=PartitionKey(path="/kind"),
            )
            store = CosmosDbStore(container, partition_key=f"python-{uuid.uuid4().hex}")
            try:
                await store.validate_container()
                engine = await ffi.open_remote_prolly_engine(store, ffi.default_config())
                tree = await engine.put(engine.create(), b"key", b"value")
                self.assertEqual(await engine.get(tree, b"key"), b"value")
            finally:
                await store.close()
                await database.delete_container(container)


if __name__ == "__main__":
    unittest.main()
