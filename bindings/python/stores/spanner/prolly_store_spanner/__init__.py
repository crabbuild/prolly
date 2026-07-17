"""Google Cloud Spanner adapter for the version-1 Prolly store protocol."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Callable, Sequence
from typing import Any, TypeVar

from prolly.remote_store import RemoteStoreAdapter, StoreFailure, descriptor, optional_bytes
from prolly.uniffi import prolly as ffi

T = TypeVar("T")
Mutation = tuple[Any, ...]

SPANNER_DDL = (
    "CREATE TABLE ProllyNodes (\n  Cid BYTES(32) NOT NULL,\n  Node BYTES(MAX) NOT NULL\n) PRIMARY KEY (Cid)",
    "CREATE TABLE ProllyHints (\n  Namespace BYTES(MAX) NOT NULL,\n  HintKey BYTES(MAX) NOT NULL,\n  Value BYTES(MAX) NOT NULL\n) PRIMARY KEY (Namespace, HintKey)",
    "CREATE TABLE ProllyRoots (\n  Name BYTES(MAX) NOT NULL,\n  Manifest BYTES(MAX) NOT NULL\n) PRIMARY KEY (Name)",
)


class SpannerStore(RemoteStoreAdapter):
    """Borrow a caller-owned Cloud Spanner ``Database``."""

    def __init__(
        self,
        database: Any,
        *,
        read_parallelism: int = 16,
        _client: Any | None = None,
    ):
        if database is None and _client is None:
            raise ValueError("Cloud Spanner database is required")
        super().__init__(descriptor(
            "spanner", adapter_name="spanner-v1", native_batch_reads=False,
            atomic_batch_writes=True, atomic_nodes_and_hint=True,
            read_parallelism=read_parallelism,
        ))
        self._client = _client if _client is not None else _SpannerSdkClient(database)
        self._closed = False

    @classmethod
    def from_client(cls, client: Any, *, read_parallelism: int = 16) -> "SpannerStore":
        """Construct from the narrow client contract used by conformance tests."""
        if client is None:
            raise ValueError("Cloud Spanner client is required")
        return cls(None, read_parallelism=read_parallelism, _client=client)

    async def close(self) -> None:
        self._closed = True

    async def _get_node(self, cid: bytes) -> bytes | None:
        self._ensure_open()
        return await self._client.get_node(cid)

    async def _put_node(self, cid: bytes, value: bytes) -> None:
        await self._apply([("upsert_node", cid, value)])

    async def _delete_node(self, cid: bytes) -> None:
        await self._apply([("delete_node", cid)])

    async def _batch_nodes(self, operations) -> None:
        mutations = []
        for item in operations:
            mutations.append(
                ("upsert_node", bytes(item.key), bytes(item.value.value))
                if item.value.present else ("delete_node", bytes(item.key))
            )
        await self._apply(mutations)

    async def _batch_get_nodes_ordered(self, cids: tuple[bytes, ...]) -> list[bytes | None]:
        self._ensure_open()
        return list(await asyncio.gather(*(self._client.get_node(cid) for cid in cids)))

    async def _list_node_cids(self) -> list[bytes]:
        self._ensure_open()
        return sorted(cid for cid in await self._client.list_node_cids() if len(cid) == 32)

    async def _get_hint(self, namespace: bytes, key: bytes) -> bytes | None:
        self._ensure_open()
        return await self._client.get_hint(namespace, key)

    async def _put_hint(self, namespace: bytes, key: bytes, value: bytes) -> None:
        await self._apply([("upsert_hint", namespace, key, value)])

    async def _batch_put_nodes_with_hint(self, nodes, namespace: bytes, key: bytes, value: bytes) -> None:
        mutations = [("upsert_node", bytes(node.key), bytes(node.value)) for node in nodes]
        mutations.append(("upsert_hint", namespace, key, value))
        await self._apply(mutations)

    async def _get_root_manifest(self, name: bytes) -> bytes | None:
        self._ensure_open()
        return await self._client.get_root(name)

    async def _put_root_manifest(self, name: bytes, manifest: bytes) -> None:
        await self._apply([("upsert_root", name, manifest)])

    async def _delete_root_manifest(self, name: bytes) -> None:
        await self._apply([("delete_root", name)])

    async def _compare_and_swap_root_manifest(
        self, name: bytes, expected: bytes | None, replacement: bytes | None
    ) -> tuple[bool, bytes | None]:
        self._ensure_open()

        def apply(transaction: Any) -> tuple[bool, bytes | None]:
            current = transaction.get_root(name)
            if current != expected:
                return False, current
            transaction.buffer([_root_mutation(name, replacement)])
            return True, replacement

        return await self._client.read_write(apply)

    async def _list_root_manifests(self) -> list[tuple[bytes, bytes]]:
        self._ensure_open()
        return sorted(await self._client.list_roots(), key=lambda item: item[0])

    async def _commit_transaction(self, nodes, conditions, roots):
        self._ensure_open()

        def apply(transaction: Any):
            for condition in conditions:
                name = bytes(condition.name)
                expected = self._optional_value(condition.expected)
                current = transaction.get_root(name)
                if current != expected:
                    return ffi.StoreTransactionConflictRecord(
                        name=name, expected=condition.expected, current=optional_bytes(current)
                    )
            mutations: list[Mutation] = []
            for item in nodes:
                mutations.append(
                    ("upsert_node", bytes(item.key), bytes(item.value.value))
                    if item.value.present else ("delete_node", bytes(item.key))
                )
            for root in roots:
                mutations.append(_root_mutation(bytes(root.name), self._optional_value(root.replacement)))
            transaction.buffer(mutations)
            return None

        return await self._client.read_write(apply)

    async def _apply(self, mutations: Sequence[Mutation]) -> None:
        self._ensure_open()
        await self._client.apply(list(mutations))

    def _ensure_open(self) -> None:
        if self._closed:
            raise StoreFailure("closed", "Cloud Spanner store is closed")

    @staticmethod
    def _error(error: BaseException) -> ffi.StoreErrorRecord:
        if isinstance(error, asyncio.CancelledError):
            raise error
        if isinstance(error, StoreFailure):
            return RemoteStoreAdapter._error(error)
        code = _grpc_code(error)
        if code == 1:
            raise asyncio.CancelledError() from error
        if code == 8:
            failure = StoreFailure("resource_exhausted", "Cloud Spanner resource limit reached", True, "grpc:8")
        elif code in (4, 10, 14):
            failure = StoreFailure("unavailable", "Cloud Spanner operation is temporarily unavailable", True, f"grpc:{code}")
        else:
            failure = StoreFailure(
                "internal", "Cloud Spanner provider operation failed", False,
                None if code is None else f"grpc:{code}",
            )
        return RemoteStoreAdapter._error(failure)


class _SpannerSdkClient:
    def __init__(self, database: Any):
        self._database = database

    async def get_node(self, key: bytes) -> bytes | None:
        return await asyncio.to_thread(self._read, "ProllyNodes", ("Node",), (key,))

    async def get_hint(self, namespace: bytes, key: bytes) -> bytes | None:
        return await asyncio.to_thread(self._read, "ProllyHints", ("Value",), (namespace, key))

    async def get_root(self, name: bytes) -> bytes | None:
        return await asyncio.to_thread(self._read, "ProllyRoots", ("Manifest",), (name,))

    async def list_node_cids(self) -> list[bytes]:
        return await asyncio.to_thread(self._query_bytes, "SELECT Cid FROM ProllyNodes ORDER BY Cid")

    async def list_roots(self) -> list[tuple[bytes, bytes]]:
        def call() -> list[tuple[bytes, bytes]]:
            with self._database.snapshot() as snapshot:
                return [(bytes(row[0]), bytes(row[1])) for row in snapshot.execute_sql(
                    "SELECT Name, Manifest FROM ProllyRoots ORDER BY Name"
                )]
        return await asyncio.to_thread(call)

    async def apply(self, mutations: Sequence[Mutation]) -> None:
        def call() -> None:
            with self._database.batch() as batch:
                _buffer_sdk(batch, mutations)
        await asyncio.to_thread(call)

    async def read_write(self, callback: Callable[[Any], T]) -> T:
        def call() -> T:
            return self._database.run_in_transaction(lambda transaction: callback(_SpannerSdkTransaction(transaction)))
        return await asyncio.to_thread(call)

    def _read(self, table: str, columns: tuple[str, ...], key: tuple[bytes, ...]) -> bytes | None:
        with self._database.snapshot() as snapshot:
            return _read_sdk(snapshot, table, columns, key)

    def _query_bytes(self, sql: str) -> list[bytes]:
        with self._database.snapshot() as snapshot:
            return [bytes(row[0]) for row in snapshot.execute_sql(sql)]


class _SpannerSdkTransaction:
    def __init__(self, transaction: Any):
        self._transaction = transaction

    def get_root(self, name: bytes) -> bytes | None:
        return _read_sdk(self._transaction, "ProllyRoots", ("Manifest",), (name,))

    def buffer(self, mutations: Sequence[Mutation]) -> None:
        _buffer_sdk(self._transaction, mutations)


def _read_sdk(reader: Any, table: str, columns: tuple[str, ...], key: tuple[bytes, ...]) -> bytes | None:
    from google.cloud.spanner_v1.keyset import KeySet

    rows = list(reader.read(table, columns, KeySet(keys=[[_sdk_bytes(value) for value in key]])))
    return None if not rows else bytes(rows[0][0])


def _buffer_sdk(target: Any, mutations: Sequence[Mutation]) -> None:
    from google.cloud.spanner_v1.keyset import KeySet

    for mutation in mutations:
        kind = mutation[0]
        if kind == "upsert_node":
            target.insert_or_update(
                "ProllyNodes", ("Cid", "Node"),
                [(_sdk_bytes(mutation[1]), _sdk_bytes(mutation[2]))],
            )
        elif kind == "delete_node":
            target.delete("ProllyNodes", KeySet(keys=[[_sdk_bytes(mutation[1])]]))
        elif kind == "upsert_hint":
            target.insert_or_update(
                "ProllyHints", ("Namespace", "HintKey", "Value"),
                [tuple(_sdk_bytes(value) for value in mutation[1:4])],
            )
        elif kind == "upsert_root":
            target.insert_or_update(
                "ProllyRoots", ("Name", "Manifest"),
                [(_sdk_bytes(mutation[1]), _sdk_bytes(mutation[2]))],
            )
        elif kind == "delete_root":
            target.delete("ProllyRoots", KeySet(keys=[[_sdk_bytes(mutation[1])]]))
        else:
            raise StoreFailure("invalid_argument", "unknown Cloud Spanner mutation")


def _root_mutation(name: bytes, replacement: bytes | None) -> Mutation:
    return ("delete_root", name) if replacement is None else ("upsert_root", name, replacement)


def _sdk_bytes(value: bytes) -> bytes:
    """Encode BYTES values as required by the Python Spanner protobuf facade."""
    return base64.b64encode(value)


def _grpc_code(error: BaseException) -> int | None:
    value = getattr(error, "grpc_status_code", None)
    if value is None:
        code_method = getattr(error, "code", None)
        value = code_method() if callable(code_method) else code_method
    if isinstance(value, int):
        return value
    enum_value = getattr(value, "value", None)
    if isinstance(enum_value, tuple) and enum_value and isinstance(enum_value[0], int):
        return enum_value[0]
    if isinstance(enum_value, int):
        return enum_value
    return None


__all__ = ["SPANNER_DDL", "SpannerStore"]
