"""Shared Python facade for the version-1 asynchronous store protocol."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass

from .uniffi import prolly as ffi


@dataclass(frozen=True)
class StoreFailure(Exception):
    code: str
    message: str
    retryable: bool = False
    provider_code: str | None = None


def missing_bytes() -> ffi.OptionalBytesRecord:
    return ffi.OptionalBytesRecord(present=False, value=b"")


def present_bytes(value: bytes) -> ffi.OptionalBytesRecord:
    return ffi.OptionalBytesRecord(present=True, value=bytes(value))


def optional_bytes(value: bytes | None) -> ffi.OptionalBytesRecord:
    return missing_bytes() if value is None else present_bytes(value)


def descriptor(
    provider: str,
    *,
    adapter_name: str,
    native_batch_reads: bool,
    atomic_batch_writes: bool,
    atomic_nodes_and_hint: bool,
    read_parallelism: int = 16,
    max_batch_read_items: int | None = None,
    max_batch_write_items: int | None = None,
    max_transaction_operations: int | None = None,
    max_node_bytes: int | None = None,
) -> ffi.StoreDescriptorRecord:
    return ffi.StoreDescriptorRecord(
        protocol_major=1,
        adapter_name=adapter_name,
        provider=provider,
        schema_version=1,
        capabilities=ffi.StoreCapabilitiesRecord(
            native_batch_reads=native_batch_reads,
            atomic_batch_writes=atomic_batch_writes,
            node_scan=True,
            hints=True,
            atomic_nodes_and_hint=atomic_nodes_and_hint,
            root_scan=True,
            root_compare_and_swap=True,
            transactions=True,
            read_parallelism=read_parallelism,
        ),
        limits=ffi.StoreLimitsRecord(
            max_batch_read_items=max_batch_read_items,
            max_batch_write_items=max_batch_write_items,
            max_transaction_operations=max_transaction_operations,
            max_node_bytes=max_node_bytes,
        ),
    )


class RemoteStoreAdapter(ffi.ForeignRemoteStore, ABC):
    """Converts idiomatic Python values and exceptions to UniFFI result records."""

    def __init__(self, store_descriptor: ffi.StoreDescriptorRecord):
        self._store_descriptor = store_descriptor

    async def descriptor(self):
        return ffi.StoreDescriptorResultRecord(value=self._store_descriptor, error=None)

    async def get_node(self, cid):
        try:
            return ffi.OptionalBytesResultRecord(value=optional_bytes(await self._get_node(bytes(cid))), error=None)
        except BaseException as error:
            return ffi.OptionalBytesResultRecord(value=missing_bytes(), error=self._error(error))

    async def put_node(self, cid, value):
        return await self._unit(self._put_node(bytes(cid), bytes(value)))

    async def delete_node(self, cid):
        return await self._unit(self._delete_node(bytes(cid)))

    async def batch_nodes(self, operations):
        return await self._unit(self._batch_nodes(tuple(operations)))

    async def batch_get_nodes_ordered(self, cids):
        try:
            values = await self._batch_get_nodes_ordered(tuple(bytes(cid) for cid in cids))
            return ffi.OptionalBytesListResultRecord(values=[optional_bytes(value) for value in values], error=None)
        except BaseException as error:
            return ffi.OptionalBytesListResultRecord(values=[], error=self._error(error))

    async def list_node_cids(self):
        try:
            return ffi.BytesListResultRecord(values=[bytes(value) for value in await self._list_node_cids()], error=None)
        except BaseException as error:
            return ffi.BytesListResultRecord(values=[], error=self._error(error))

    async def get_hint(self, namespace, key):
        try:
            value = await self._get_hint(bytes(namespace), bytes(key))
            return ffi.OptionalBytesResultRecord(value=optional_bytes(value), error=None)
        except BaseException as error:
            return ffi.OptionalBytesResultRecord(value=missing_bytes(), error=self._error(error))

    async def put_hint(self, namespace, key, value):
        return await self._unit(self._put_hint(bytes(namespace), bytes(key), bytes(value)))

    async def batch_put_nodes_with_hint(self, nodes, namespace, key, value):
        return await self._unit(
            self._batch_put_nodes_with_hint(tuple(nodes), bytes(namespace), bytes(key), bytes(value))
        )

    async def get_root_manifest(self, name):
        try:
            value = await self._get_root_manifest(bytes(name))
            return ffi.OptionalBytesResultRecord(value=optional_bytes(value), error=None)
        except BaseException as error:
            return ffi.OptionalBytesResultRecord(value=missing_bytes(), error=self._error(error))

    async def put_root_manifest(self, name, manifest):
        return await self._unit(self._put_root_manifest(bytes(name), bytes(manifest)))

    async def delete_root_manifest(self, name):
        return await self._unit(self._delete_root_manifest(bytes(name)))

    async def compare_and_swap_root_manifest(self, name, expected, new):
        try:
            applied, current = await self._compare_and_swap_root_manifest(
                bytes(name), self._optional_value(expected), self._optional_value(new)
            )
            return ffi.RootCasResultRecord(applied=applied, current=optional_bytes(current), error=None)
        except BaseException as error:
            return ffi.RootCasResultRecord(applied=False, current=missing_bytes(), error=self._error(error))

    async def list_root_manifests(self):
        try:
            values = [ffi.NamedBytesRecord(name=bytes(name), value=bytes(value)) for name, value in await self._list_root_manifests()]
            return ffi.NamedBytesListResultRecord(values=values, error=None)
        except BaseException as error:
            return ffi.NamedBytesListResultRecord(values=[], error=self._error(error))

    async def commit_transaction(self, nodes, conditions, roots):
        try:
            conflict = await self._commit_transaction(tuple(nodes), tuple(conditions), tuple(roots))
            return ffi.TransactionResultRecord(applied=conflict is None, conflict=conflict, error=None)
        except BaseException as error:
            return ffi.TransactionResultRecord(applied=False, conflict=None, error=self._error(error))

    async def _unit(self, awaitable):
        try:
            await awaitable
            return ffi.UnitResultRecord(error=None)
        except BaseException as error:
            return ffi.UnitResultRecord(error=self._error(error))

    @staticmethod
    def _optional_value(value: ffi.OptionalBytesRecord) -> bytes | None:
        if not value.present:
            if value.value:
                raise StoreFailure("invalid_argument", "absent optional bytes must have an empty value")
            return None
        return bytes(value.value)

    @staticmethod
    def _error(error: BaseException) -> ffi.StoreErrorRecord:
        if isinstance(error, asyncio.CancelledError):
            raise error
        if isinstance(error, StoreFailure):
            failure = error
        elif isinstance(error, TimeoutError):
            failure = StoreFailure("unavailable", "store operation timed out", True)
        else:
            failure = StoreFailure("internal", "store provider operation failed")
        return ffi.StoreErrorRecord(
            code=failure.code,
            message=failure.message,
            retryable=failure.retryable,
            provider_code=failure.provider_code,
        )

    @abstractmethod
    async def _get_node(self, cid: bytes) -> bytes | None: ...
    @abstractmethod
    async def _put_node(self, cid: bytes, value: bytes) -> None: ...
    @abstractmethod
    async def _delete_node(self, cid: bytes) -> None: ...
    @abstractmethod
    async def _batch_nodes(self, operations) -> None: ...
    @abstractmethod
    async def _batch_get_nodes_ordered(self, cids: tuple[bytes, ...]) -> list[bytes | None]: ...
    @abstractmethod
    async def _list_node_cids(self) -> list[bytes]: ...
    @abstractmethod
    async def _get_hint(self, namespace: bytes, key: bytes) -> bytes | None: ...
    @abstractmethod
    async def _put_hint(self, namespace: bytes, key: bytes, value: bytes) -> None: ...
    @abstractmethod
    async def _batch_put_nodes_with_hint(self, nodes, namespace: bytes, key: bytes, value: bytes) -> None: ...
    @abstractmethod
    async def _get_root_manifest(self, name: bytes) -> bytes | None: ...
    @abstractmethod
    async def _put_root_manifest(self, name: bytes, manifest: bytes) -> None: ...
    @abstractmethod
    async def _delete_root_manifest(self, name: bytes) -> None: ...
    @abstractmethod
    async def _compare_and_swap_root_manifest(self, name: bytes, expected: bytes | None, replacement: bytes | None) -> tuple[bool, bytes | None]: ...
    @abstractmethod
    async def _list_root_manifests(self) -> list[tuple[bytes, bytes]]: ...
    @abstractmethod
    async def _commit_transaction(self, nodes, conditions, roots) -> ffi.StoreTransactionConflictRecord | None: ...


__all__ = [
    "RemoteStoreAdapter",
    "StoreFailure",
    "descriptor",
    "missing_bytes",
    "optional_bytes",
    "present_bytes",
]
