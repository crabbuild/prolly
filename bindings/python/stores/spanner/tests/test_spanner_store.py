import asyncio
import copy
import importlib
import os
import time
import unittest

from prolly.remote_store import missing_bytes, present_bytes
from prolly.uniffi import prolly as ffi

try:
    module = importlib.import_module("prolly_store_spanner")
    SpannerStore, SPANNER_DDL = module.SpannerStore, module.SPANNER_DDL
except (ImportError, AttributeError):
    SpannerStore, SPANNER_DDL = None, None


class FakeTransaction:
    def __init__(self, state):
        self.state = state
        self.mutations = []

    def get_root(self, name):
        return self.state["roots"].get(bytes(name))

    def buffer(self, mutations):
        self.mutations.extend(copy.deepcopy(mutations))


class FakeClient:
    def __init__(self):
        self.state = {"nodes": {}, "hints": {}, "roots": {}}
        self.lock = asyncio.Lock()
        self.last_mutations = []
        self.failure = None
        self.block_apply = False
        self.closed = False

    async def get_node(self, key):
        return self.state["nodes"].get(bytes(key))

    async def get_hint(self, namespace, key):
        return self.state["hints"].get((bytes(namespace), bytes(key)))

    async def get_root(self, name):
        return self.state["roots"].get(bytes(name))

    async def list_node_cids(self):
        return list(self.state["nodes"])

    async def list_roots(self):
        return list(self.state["roots"].items())

    async def apply(self, mutations):
        if self.failure is not None:
            error, self.failure = self.failure, None
            raise error
        if self.block_apply:
            await asyncio.Future()
        async with self.lock:
            next_state = copy.deepcopy(self.state)
            self._apply(next_state, mutations)
            self.state = next_state
            self.last_mutations = copy.deepcopy(mutations)

    async def read_write(self, callback):
        async with self.lock:
            next_state = copy.deepcopy(self.state)
            transaction = FakeTransaction(next_state)
            result = callback(transaction)
            self._apply(next_state, transaction.mutations)
            self.state = next_state
            return result

    def close(self):
        self.closed = True

    @staticmethod
    def _apply(state, mutations):
        for mutation in mutations:
            kind = mutation[0]
            if kind == "upsert_node":
                state["nodes"][mutation[1]] = mutation[2]
            elif kind == "delete_node":
                state["nodes"].pop(mutation[1], None)
            elif kind == "upsert_hint":
                state["hints"][(mutation[1], mutation[2])] = mutation[3]
            elif kind == "upsert_root":
                state["roots"][mutation[1]] = mutation[2]
            elif kind == "delete_root":
                state["roots"].pop(mutation[1], None)


class FakeGrpcError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.grpc_status_code = code


class SpannerStoreTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(SpannerStore, "Spanner adapter is not implemented")
        self.client = FakeClient()
        self.store = SpannerStore.from_client(self.client)

    async def test_ddl_atomic_batches_transactions_and_rust_engine(self):
        self.assertEqual(SPANNER_DDL, (
            "CREATE TABLE ProllyNodes (\n  Cid BYTES(32) NOT NULL,\n  Node BYTES(MAX) NOT NULL\n) PRIMARY KEY (Cid)",
            "CREATE TABLE ProllyHints (\n  Namespace BYTES(MAX) NOT NULL,\n  HintKey BYTES(MAX) NOT NULL,\n  Value BYTES(MAX) NOT NULL\n) PRIMARY KEY (Namespace, HintKey)",
            "CREATE TABLE ProllyRoots (\n  Name BYTES(MAX) NOT NULL,\n  Manifest BYTES(MAX) NOT NULL\n) PRIMARY KEY (Name)",
        ))
        descriptor = await self.store.descriptor()
        self.assertTrue(descriptor.value.capabilities.atomic_batch_writes)
        self.assertTrue(descriptor.value.capabilities.atomic_nodes_and_hint)

        cid = bytes([0, 127, 128, 255]) * 8
        await self.store.put_node(cid, b"node")
        self.assertEqual(self.client.state["nodes"][cid], b"node")
        nodes = [ffi.NodeEntryRecord(key=bytes([index]) * 32, value=b"value") for index in range(4)]
        await self.store.batch_put_nodes_with_hint(nodes, b"namespace", b"key", b"hint")
        self.assertEqual(len(self.client.last_mutations), 5)

        contenders = await asyncio.gather(*[
            self.store.compare_and_swap_root_manifest(
                b"main", missing_bytes(), present_bytes(index.to_bytes(2, "big"))
            ) for index in range(32)
        ])
        self.assertEqual(sum(result.applied for result in contenders), 1)
        conflict = await self.store.commit_transaction(
            [ffi.NodeMutationRecord(key=b"rollback", value=present_bytes(b"bad"))],
            [ffi.RootConditionRecord(name=b"main", expected=missing_bytes())],
            [ffi.RootWriteRecord(name=b"other", replacement=present_bytes(b"bad"))],
        )
        self.assertFalse(conflict.applied)
        self.assertFalse((await self.store.get_node(b"rollback")).value.present)

        engine = await ffi.open_remote_prolly_engine(self.store, ffi.default_config())
        tree = await engine.put(engine.create(), b"key", b"value")
        self.assertEqual(await engine.get(tree, b"key"), b"value")
        await self.store.close()
        self.assertFalse(self.client.closed)

    async def test_error_redaction_and_cancellation(self):
        secret = "spanner-service-account-secret"
        self.client.failure = FakeGrpcError(14, secret)
        result = await self.store.put_node(b"cid", b"value")
        self.assertEqual(result.error.code, "unavailable")
        self.assertTrue(result.error.retryable)
        self.assertNotIn(secret, result.error.message)

        self.client.block_apply = True
        task = asyncio.create_task(self.store.put_node(b"cancel", b"value"))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task


@unittest.skipUnless(os.environ.get("SPANNER_EMULATOR_HOST"), "Spanner emulator is not set")
class SpannerEmulatorTest(unittest.IsolatedAsyncioTestCase):
    async def test_emulator_round_trip(self):
        from google.cloud import spanner

        project = "prolly-test"
        client = spanner.Client(project=project)
        instance = client.instance(
            "prolly-test",
            configuration_name=f"projects/{project}/instanceConfigs/emulator-config",
            node_count=1,
        )
        if not await asyncio.to_thread(instance.exists):
            operation = await asyncio.to_thread(instance.create)
            await asyncio.to_thread(operation.result, timeout=120)
        database = instance.database(
            f"prolly_py_{time.time_ns() % 10**12}", ddl_statements=list(SPANNER_DDL)
        )
        operation = await asyncio.to_thread(database.create)
        await asyncio.to_thread(operation.result, timeout=120)
        store = SpannerStore(database)
        try:
            engine = await ffi.open_remote_prolly_engine(store, ffi.default_config())
            tree = await engine.put(engine.create(), b"key", b"value")
            self.assertEqual(await engine.get(tree, b"key"), b"value")
            await store.close()
            self.assertTrue(await asyncio.to_thread(database.exists))
        finally:
            await asyncio.to_thread(database.drop)


if __name__ == "__main__":
    unittest.main()
