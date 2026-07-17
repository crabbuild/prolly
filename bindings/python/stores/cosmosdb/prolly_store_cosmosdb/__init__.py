"""Async Azure Cosmos DB adapter for the version-1 Prolly store protocol."""

from __future__ import annotations

import asyncio
import base64
import re
from typing import Any

from azure.core import MatchConditions

from prolly.remote_store import RemoteStoreAdapter, StoreFailure, descriptor, optional_bytes
from prolly.uniffi import prolly as ffi

TRANSACTION_LIMIT = 100
FAMILIES = ("node", "root", "hint")
HEX_PATTERN = re.compile(r"(?:[0-9a-f]{2})*\Z")


class CosmosDbStore(RemoteStoreAdapter):
    """Borrow a caller-owned ``azure.cosmos.aio.ContainerProxy``."""

    def __init__(
        self, container: Any, *, partition_key: str = "prolly",
        key_prefix: bytes = b"prolly:", read_parallelism: int = 16,
    ):
        if container is None:
            raise ValueError("Cosmos DB container is required")
        super().__init__(descriptor(
            "cosmosdb", adapter_name="cosmosdb-v1", native_batch_reads=False,
            atomic_batch_writes=False, atomic_nodes_and_hint=False,
            read_parallelism=read_parallelism,
            max_transaction_operations=TRANSACTION_LIMIT,
        ))
        self._container = container
        self._partition = partition_key.strip() or "prolly"
        self._prefix = bytes(key_prefix)
        self._closed = False

    async def validate_container(self) -> None:
        self._ensure_open()
        properties = await self._container.read()
        paths = properties.get("partitionKey", {}).get("paths")
        if paths != ["/kind"]:
            raise StoreFailure(
                "invalid_argument", "Cosmos DB container partition key must be /kind"
            )

    async def close(self) -> None:
        self._closed = True

    async def clear_namespace(self) -> None:
        self._ensure_open()
        if not self._prefix:
            raise StoreFailure("invalid_argument", "refusing to clear an empty Cosmos DB key prefix")
        for family in FAMILIES:
            for document in await self._query_family(family):
                key = self._decode_key(document)
                if key.startswith(self._prefix):
                    await self._delete(key, ignore_missing=True)

    async def _get_node(self, cid: bytes) -> bytes | None:
        return await self._get(self._family(b"node:", cid))

    async def _put_node(self, cid: bytes, value: bytes) -> None:
        await self._upsert("node", self._family(b"node:", cid), value)

    async def _delete_node(self, cid: bytes) -> None:
        await self._delete(self._family(b"node:", cid), ignore_missing=True)

    async def _batch_nodes(self, operations) -> None:
        for operation in operations:
            if operation.value.present:
                await self._put_node(bytes(operation.key), bytes(operation.value.value))
            else:
                await self._delete_node(bytes(operation.key))

    async def _batch_get_nodes_ordered(self, cids: tuple[bytes, ...]) -> list[bytes | None]:
        return [await self._get_node(cid) for cid in cids]

    async def _list_node_cids(self) -> list[bytes]:
        prefix = self._family(b"node:", b"")
        return sorted(
            key[len(prefix):]
            for document in await self._query_family("node")
            if (key := self._decode_key(document)).startswith(prefix)
            and len(key) == len(prefix) + 32
        )

    async def _get_hint(self, namespace: bytes, key: bytes) -> bytes | None:
        return await self._get(self._hint(namespace, key))

    async def _put_hint(self, namespace: bytes, key: bytes, value: bytes) -> None:
        await self._upsert("hint", self._hint(namespace, key), value)

    async def _batch_put_nodes_with_hint(self, nodes, namespace: bytes, key: bytes, value: bytes) -> None:
        for node in nodes:
            await self._put_node(bytes(node.key), bytes(node.value))
        await self._put_hint(namespace, key, value)

    async def _get_root_manifest(self, name: bytes) -> bytes | None:
        return await self._get(self._family(b"root:", name))

    async def _put_root_manifest(self, name: bytes, manifest: bytes) -> None:
        await self._upsert("root", self._family(b"root:", name), manifest)

    async def _delete_root_manifest(self, name: bytes) -> None:
        await self._delete(self._family(b"root:", name), ignore_missing=True)

    async def _compare_and_swap_root_manifest(self, name, expected, replacement):
        self._ensure_open()
        key = self._family(b"root:", name)
        item_id = self._document_id(key)
        if expected is None:
            if replacement is None:
                current = await self._get_raw(key)
                return current is None, current
            try:
                await self._container.create_item(self._document("root", key, replacement))
                return True, replacement
            except Exception as error:
                if self._status(error) != 409:
                    raise
                return False, await self._get_raw(key)

        try:
            current = await self._read_document(key)
        except Exception as error:
            if self._status(error) == 404:
                return False, None
            raise
        current_value = self._decode_value(current)
        if current_value != expected:
            return False, current_value
        try:
            if replacement is None:
                await self._container.delete_item(
                    item_id, self._partition, etag=current["_etag"],
                    match_condition=MatchConditions.IfNotModified,
                )
            else:
                await self._container.replace_item(
                    item_id, self._document("root", key, replacement),
                    etag=current["_etag"], match_condition=MatchConditions.IfNotModified,
                )
            return True, replacement
        except Exception as error:
            if self._status(error) not in (404, 412):
                raise
            return False, await self._get_raw(key)

    async def _list_root_manifests(self) -> list[tuple[bytes, bytes]]:
        prefix = self._family(b"root:", b"")
        values = []
        for document in await self._query_family("root"):
            key = self._decode_key(document)
            if key.startswith(prefix):
                values.append((key[len(prefix):], self._decode_value(document)))
        return sorted(values)

    async def _commit_transaction(self, nodes, conditions, roots):
        initial_count = len(nodes) + len(roots)
        if initial_count > TRANSACTION_LIMIT:
            raise self._limit(initial_count)
        self._ensure_open()
        written = {bytes(root.name) for root in roots}
        by_name = {bytes(condition.name): condition for condition in conditions}
        operations = []

        for condition in conditions:
            if bytes(condition.name) not in written:
                additions, conflict = await self._condition_operations(condition)
                if conflict is not None:
                    return conflict
                operations.extend(additions)
        for root in roots:
            additions, conflict = await self._root_operations(
                root, by_name.get(bytes(root.name))
            )
            if conflict is not None:
                return conflict
            operations.extend(additions)
        for node in nodes:
            key = self._family(b"node:", bytes(node.key))
            if node.value.present:
                operations.append(("upsert", (self._document("node", key, bytes(node.value.value)),)))
            else:
                try:
                    current = await self._read_document(key)
                    operations.append((
                        "delete", (self._document_id(key),),
                        {"if_match_etag": current["_etag"]},
                    ))
                except Exception as error:
                    if self._status(error) != 404:
                        raise
        if len(operations) > TRANSACTION_LIMIT:
            raise self._limit(len(operations))
        if not operations:
            return None
        try:
            await self._container.execute_item_batch(operations, self._partition)
            return None
        except Exception:
            for condition in conditions:
                current = await self._get_raw(self._family(b"root:", bytes(condition.name)))
                expected = self._optional_value(condition.expected)
                if current != expected:
                    return ffi.StoreTransactionConflictRecord(
                        name=bytes(condition.name), expected=condition.expected,
                        current=optional_bytes(current),
                    )
            raise

    async def _condition_operations(self, condition):
        name = bytes(condition.name)
        key = self._family(b"root:", name)
        expected = self._optional_value(condition.expected)
        try:
            current = await self._read_document(key)
            value = self._decode_value(current)
            if expected is None or value != expected:
                return [], self._conflict(condition, value)
            return [(
                "replace", (self._document_id(key), self._plain_document(current)),
                {"if_match_etag": current["_etag"]},
            )], None
        except Exception as error:
            if self._status(error) != 404:
                raise
            if expected is not None:
                return [], self._conflict(condition, None)
            placeholder = self._document("root", key, b"")
            return [
                ("create", (placeholder,)),
                ("delete", (placeholder["id"],)),
            ], None

    async def _root_operations(self, root, condition):
        name = bytes(root.name)
        key = self._family(b"root:", name)
        item_id = self._document_id(key)
        replacement = self._optional_value(root.replacement)
        if condition is None:
            if replacement is not None:
                return [("upsert", (self._document("root", key, replacement),))], None
            try:
                current = await self._read_document(key)
                return [(
                    "delete", (item_id,), {"if_match_etag": current["_etag"]},
                )], None
            except Exception as error:
                if self._status(error) == 404:
                    return [], None
                raise

        expected = self._optional_value(condition.expected)
        try:
            current = await self._read_document(key)
            value = self._decode_value(current)
            if expected is None or value != expected:
                return [], self._conflict(condition, value)
            options = {"if_match_etag": current["_etag"]}
            if replacement is not None:
                return [(
                    "replace", (item_id, self._document("root", key, replacement)), options,
                )], None
            return [("delete", (item_id,), options)], None
        except Exception as error:
            if self._status(error) != 404:
                raise
            if expected is not None:
                return [], self._conflict(condition, None)
            if replacement is not None:
                return [("create", (self._document("root", key, replacement),))], None
            placeholder = self._document("root", key, b"")
            return [
                ("create", (placeholder,)),
                ("delete", (item_id,)),
            ], None

    async def _get(self, key: bytes) -> bytes | None:
        self._ensure_open()
        return await self._get_raw(key)

    async def _get_raw(self, key: bytes) -> bytes | None:
        try:
            return self._decode_value(await self._read_document(key))
        except Exception as error:
            if self._status(error) == 404:
                return None
            raise

    async def _read_document(self, key: bytes) -> dict:
        self._ensure_open()
        item_id = self._document_id(key)
        document = await self._container.read_item(item_id, self._partition)
        if document.get("id") != item_id or document.get("kind") != self._partition:
            raise StoreFailure("invalid_data", "Cosmos DB document identity does not match requested key")
        if not isinstance(document.get("_etag"), str):
            raise StoreFailure("invalid_data", "Cosmos DB document is missing its ETag")
        return dict(document)

    async def _upsert(self, family: str, key: bytes, value: bytes) -> None:
        self._ensure_open()
        await self._container.upsert_item(self._document(family, key, value))

    async def _delete(self, key: bytes, *, ignore_missing: bool) -> None:
        self._ensure_open()
        try:
            await self._container.delete_item(self._document_id(key), self._partition)
        except Exception as error:
            if not ignore_missing or self._status(error) != 404:
                raise

    async def _query_family(self, family: str) -> list[dict]:
        self._ensure_open()
        query = self._container.query_items(
            query="SELECT * FROM c WHERE c.kind = @kind AND c.family = @family",
            parameters=[
                {"name": "@kind", "value": self._partition},
                {"name": "@family", "value": family},
            ],
            partition_key=self._partition,
        )
        return [
            dict(document) async for document in query
            if document.get("kind") == self._partition and document.get("family") == family
        ]

    def _ensure_open(self) -> None:
        if self._closed:
            raise StoreFailure("closed", "Cosmos DB store is closed")

    def _family(self, family: bytes, suffix: bytes) -> bytes:
        return self._prefix + family + bytes(suffix)

    def _hint(self, namespace: bytes, key: bytes) -> bytes:
        return self._prefix + b"hint:" + len(namespace).to_bytes(8, "big") + namespace + key

    def _document(self, family: str, key: bytes, value: bytes) -> dict:
        return {
            "id": self._document_id(key), "kind": self._partition, "family": family,
            "key": key.hex(), "value": base64.b64encode(value).decode("ascii"),
        }

    @staticmethod
    def _plain_document(document: dict) -> dict:
        return {
            key: value for key, value in document.items()
            if not key.startswith("_")
        }

    @staticmethod
    def _document_id(key: bytes) -> str:
        return "k" + key.hex()

    @staticmethod
    def _decode_key(document: dict) -> bytes:
        value = document.get("key")
        if not isinstance(value, str) or HEX_PATTERN.fullmatch(value) is None:
            raise StoreFailure("invalid_data", "Cosmos DB document key is not valid lowercase hex")
        return bytes.fromhex(value)

    @staticmethod
    def _decode_value(document: dict) -> bytes:
        value = document.get("value")
        if not isinstance(value, str):
            raise StoreFailure("invalid_data", "Cosmos DB document value is not valid base64")
        try:
            return base64.b64decode(value, validate=True)
        except (ValueError, TypeError):
            raise StoreFailure("invalid_data", "Cosmos DB document value is not valid base64") from None

    @staticmethod
    def _conflict(condition, current: bytes | None):
        return ffi.StoreTransactionConflictRecord(
            name=bytes(condition.name), expected=condition.expected,
            current=optional_bytes(current),
        )

    @staticmethod
    def _limit(count: int) -> StoreFailure:
        return StoreFailure(
            "resource_exhausted",
            f"Cosmos DB transaction has {count} operations, exceeding the {TRANSACTION_LIMIT} operation limit",
        )

    @staticmethod
    def _status(error: BaseException) -> int | None:
        value = getattr(error, "status_code", None)
        if isinstance(value, int):
            return value
        value = getattr(error, "status", None)
        return value if isinstance(value, int) else None

    @staticmethod
    def _error(error: BaseException) -> ffi.StoreErrorRecord:
        if isinstance(error, asyncio.CancelledError):
            raise error
        if isinstance(error, StoreFailure):
            return RemoteStoreAdapter._error(error)
        status = CosmosDbStore._status(error)
        retryable = status == 408 or status == 429 or (status is not None and status >= 500)
        return ffi.StoreErrorRecord(
            code="unavailable" if retryable else "internal",
            message="Cosmos DB provider operation failed", retryable=retryable,
            provider_code=None if status is None else f"cosmos:{status}",
        )


__all__ = ["CosmosDbStore"]
