import asyncio
import unittest

from prolly.uniffi import prolly as ffi
from prolly.remote_store import GENERAL, POINT_UPSERT, normalize_publication_origin_code


def missing():
    return ffi.OptionalBytesRecord(present=False, value=b"")


def optional(value):
    return missing() if value is None else ffi.OptionalBytesRecord(present=True, value=value)


class MemoryRemoteStore(ffi.ForeignRemoteStore):
    def __init__(self):
        self.nodes = {}
        self.hints = {}
        self.roots = {}
        self.publications = []
        self.lock = asyncio.Lock()

    async def descriptor(self):
        capabilities = ffi.StoreCapabilitiesRecord(
            native_batch_reads=True,
            atomic_batch_writes=True,
            node_scan=True,
            hints=True,
            atomic_nodes_and_hint=True,
            root_scan=True,
            root_compare_and_swap=True,
            transactions=True,
            read_parallelism=4,
        )
        limits = ffi.StoreLimitsRecord(
            max_batch_read_items=None,
            max_batch_write_items=None,
            max_transaction_operations=None,
            max_node_bytes=None,
        )
        descriptor = ffi.StoreDescriptorRecord(
            protocol_major=2,
            adapter_name="python-test-memory",
            provider="memory",
            schema_version=1,
            capabilities=capabilities,
            limits=limits,
        )
        return ffi.StoreDescriptorResultRecord(value=descriptor, error=None)

    async def get_node(self, cid):
        return ffi.OptionalBytesResultRecord(value=optional(self.nodes.get(cid)), error=None)

    async def put_node(self, cid, value):
        self.nodes[bytes(cid)] = bytes(value)
        return ffi.UnitResultRecord(error=None)

    async def delete_node(self, cid):
        self.nodes.pop(cid, None)
        return ffi.UnitResultRecord(error=None)

    async def batch_nodes(self, ops):
        for operation in ops:
            if operation.value.present:
                self.nodes[bytes(operation.key)] = bytes(operation.value.value)
            else:
                self.nodes.pop(operation.key, None)
        return ffi.UnitResultRecord(error=None)

    async def publish_nodes(self, publication):
        self.publications.append(publication)
        if publication.hint is not None:
            return await self.batch_put_nodes_with_hint(
                publication.nodes,
                publication.hint.namespace,
                publication.hint.key,
                publication.hint.value,
            )
        operations = [
            ffi.NodeMutationRecord(key=node.key, value=optional(node.value))
            for node in publication.nodes
        ]
        return await self.batch_nodes(operations)

    async def batch_get_nodes_ordered(self, cids):
        values = [optional(self.nodes.get(cid)) for cid in cids]
        return ffi.OptionalBytesListResultRecord(values=values, error=None)

    async def list_node_cids(self):
        return ffi.BytesListResultRecord(values=sorted(self.nodes), error=None)

    async def get_hint(self, namespace, key):
        return ffi.OptionalBytesResultRecord(value=optional(self.hints.get((namespace, key))), error=None)

    async def put_hint(self, namespace, key, value):
        self.hints[(bytes(namespace), bytes(key))] = bytes(value)
        return ffi.UnitResultRecord(error=None)

    async def batch_put_nodes_with_hint(self, nodes, namespace, key, value):
        async with self.lock:
            for node in nodes:
                self.nodes[bytes(node.key)] = bytes(node.value)
            self.hints[(bytes(namespace), bytes(key))] = bytes(value)
        return ffi.UnitResultRecord(error=None)

    async def get_root_manifest(self, name):
        return ffi.OptionalBytesResultRecord(value=optional(self.roots.get(name)), error=None)

    async def put_root_manifest(self, name, manifest):
        self.roots[bytes(name)] = bytes(manifest)
        return ffi.UnitResultRecord(error=None)

    async def delete_root_manifest(self, name):
        self.roots.pop(name, None)
        return ffi.UnitResultRecord(error=None)

    async def compare_and_swap_root_manifest(self, name, expected, new):
        async with self.lock:
            current = self.roots.get(name)
            matches = (not expected.present and current is None) or (
                expected.present and current == expected.value
            )
            if not matches:
                return ffi.RootCasResultRecord(applied=False, current=optional(current), error=None)
            if new.present:
                self.roots[bytes(name)] = bytes(new.value)
            else:
                self.roots.pop(name, None)
            return ffi.RootCasResultRecord(applied=True, current=optional(current), error=None)

    async def list_root_manifests(self):
        values = [ffi.NamedBytesRecord(name=name, value=value) for name, value in sorted(self.roots.items())]
        return ffi.NamedBytesListResultRecord(values=values, error=None)

    async def commit_transaction(self, nodes, conditions, roots):
        async with self.lock:
            for condition in conditions:
                current = self.roots.get(condition.name)
                matches = (not condition.expected.present and current is None) or (
                    condition.expected.present and current == condition.expected.value
                )
                if not matches:
                    conflict = ffi.StoreTransactionConflictRecord(
                        name=condition.name,
                        expected=condition.expected,
                        current=optional(current),
                    )
                    return ffi.TransactionResultRecord(applied=False, conflict=conflict, error=None)
            await self.batch_nodes(nodes)
            for root in roots:
                if root.replacement.present:
                    self.roots[bytes(root.name)] = bytes(root.replacement.value)
                else:
                    self.roots.pop(root.name, None)
            return ffi.TransactionResultRecord(applied=True, conflict=None, error=None)


class RemoteStoreBridgeTest(unittest.IsolatedAsyncioTestCase):
    async def test_publication_record_preserves_context_and_unknown_uses_general(self):
        store = MemoryRemoteStore()
        expected_bytes = b"published-node"
        publication = ffi.NodePublicationRecord(
            nodes=[ffi.NodeEntryRecord(key=b"cid", value=expected_bytes)],
            hint=ffi.NodePublicationHintRecord(
                namespace=b"rightmost",
                key=b"key",
                value=b"cid",
            ),
            origin=ffi.PublicationOriginRecord(code=POINT_UPSERT),
        )

        result = await store.publish_nodes(publication)

        self.assertIsNone(result.error)
        captured = store.publications.pop()
        self.assertEqual(captured.origin.code, POINT_UPSERT)
        self.assertEqual(captured.nodes[0].value, expected_bytes)
        self.assertEqual(captured.hint.namespace, b"rightmost")
        self.assertEqual(normalize_publication_origin_code(0xFFFFFFFF), GENERAL)

    async def test_rust_engine_uses_python_async_store(self):
        store = MemoryRemoteStore()
        engine = await ffi.open_remote_prolly_engine(store, ffi.default_config())
        tree = engine.create()
        updated = await engine.put(tree, b"key", b"value")
        self.assertEqual(await engine.get(updated, b"key"), b"value")
        self.assertGreater(len(store.nodes), 0)


if __name__ == "__main__":
    unittest.main()
