"""aiomysql adapter for the version-1 asynchronous Prolly store protocol."""

from __future__ import annotations

import warnings
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from prolly.remote_store import RemoteStoreAdapter, StoreFailure, descriptor, optional_bytes
from prolly.uniffi import prolly as ffi

T = TypeVar("T")

CREATE_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS prolly_nodes (cid VARBINARY(32) PRIMARY KEY, node LONGBLOB NOT NULL)",
    "CREATE TABLE IF NOT EXISTS prolly_hints (namespace VARBINARY(255) NOT NULL, `key` VARBINARY(255) NOT NULL, value LONGBLOB NOT NULL, PRIMARY KEY(namespace, `key`))",
    "CREATE TABLE IF NOT EXISTS prolly_roots (name VARBINARY(255) PRIMARY KEY, manifest LONGBLOB NOT NULL)",
)
UPSERT_NODE = "INSERT INTO prolly_nodes VALUES (%s, %s) AS new ON DUPLICATE KEY UPDATE node=new.node"
UPSERT_HINT = "INSERT INTO prolly_hints VALUES (%s, %s, %s) AS new ON DUPLICATE KEY UPDATE value=new.value"
UPSERT_ROOT = "INSERT INTO prolly_roots VALUES (%s, %s) AS new ON DUPLICATE KEY UPDATE manifest=new.manifest"


class MysqlStore(RemoteStoreAdapter):
    """Borrow a caller-owned ``aiomysql.Pool``."""

    def __init__(self, pool: Any, *, adapter_name: str = "mysql-v1", read_parallelism: int = 16):
        if pool is None:
            raise ValueError("MySQL pool is required")
        super().__init__(descriptor(
            "mysql", adapter_name=adapter_name, native_batch_reads=True,
            atomic_batch_writes=True, atomic_nodes_and_hint=True,
            read_parallelism=read_parallelism,
        ))
        self._pool = pool
        self._closed = False

    async def initialize_schema(self) -> None:
        await self._ensure_open()
        async with self._pool.acquire() as connection:
            async with connection.cursor() as cursor:
                for statement in CREATE_SCHEMA:
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", message="Table '.*' already exists")
                        await cursor.execute(statement)

    async def close(self) -> None:
        self._closed = True

    async def _get_node(self, cid: bytes) -> bytes | None:
        self._key(cid, 32, "node CID")
        return await self._query_optional("SELECT node FROM prolly_nodes WHERE cid=%s", (cid,))

    async def _put_node(self, cid: bytes, value: bytes) -> None:
        self._key(cid, 32, "node CID")
        await self._execute(UPSERT_NODE, (cid, value))

    async def _delete_node(self, cid: bytes) -> None:
        self._key(cid, 32, "node CID")
        await self._execute("DELETE FROM prolly_nodes WHERE cid=%s", (cid,))

    async def _batch_nodes(self, operations) -> None:
        async def apply(connection):
            for item in operations:
                key = self._key(bytes(item.key), 32, "node CID")
                if item.value.present:
                    await self._execute_with(connection, UPSERT_NODE, (key, bytes(item.value.value)))
                else:
                    await self._execute_with(connection, "DELETE FROM prolly_nodes WHERE cid=%s", (key,))
        await self._transaction(apply)

    async def _batch_get_nodes_ordered(self, cids: tuple[bytes, ...]) -> list[bytes | None]:
        keys = tuple(self._key(cid, 32, "node CID") for cid in cids)
        if not keys:
            return []
        placeholders = ",".join(["%s"] * len(keys))
        rows = await self._query_all(f"SELECT cid,node FROM prolly_nodes WHERE cid IN ({placeholders})", keys)
        values = {bytes(cid): bytes(node) for cid, node in rows}
        return [values.get(key) for key in keys]

    async def _list_node_cids(self) -> list[bytes]:
        return [bytes(row[0]) for row in await self._query_all("SELECT cid FROM prolly_nodes ORDER BY cid")]

    async def _get_hint(self, namespace: bytes, key: bytes) -> bytes | None:
        namespace = self._key(namespace, 255, "hint namespace")
        key = self._key(key, 255, "hint key")
        return await self._query_optional("SELECT value FROM prolly_hints WHERE namespace=%s AND `key`=%s", (namespace, key))

    async def _put_hint(self, namespace: bytes, key: bytes, value: bytes) -> None:
        await self._execute(UPSERT_HINT, (self._key(namespace, 255, "hint namespace"), self._key(key, 255, "hint key"), value))

    async def _batch_put_nodes_with_hint(self, nodes, namespace: bytes, key: bytes, value: bytes) -> None:
        namespace = self._key(namespace, 255, "hint namespace")
        key = self._key(key, 255, "hint key")
        async def apply(connection):
            for node in nodes:
                await self._execute_with(connection, UPSERT_NODE, (self._key(bytes(node.key), 32, "node CID"), bytes(node.value)))
            await self._execute_with(connection, UPSERT_HINT, (namespace, key, value))
        await self._transaction(apply)

    async def _get_root_manifest(self, name: bytes) -> bytes | None:
        name = self._key(name, 255, "root name")
        return await self._query_optional("SELECT manifest FROM prolly_roots WHERE name=%s", (name,))

    async def _put_root_manifest(self, name: bytes, manifest: bytes) -> None:
        await self._execute(UPSERT_ROOT, (self._key(name, 255, "root name"), manifest))

    async def _delete_root_manifest(self, name: bytes) -> None:
        await self._execute("DELETE FROM prolly_roots WHERE name=%s", (self._key(name, 255, "root name"),))

    async def _compare_and_swap_root_manifest(self, name, expected, replacement):
        name = self._key(name, 255, "root name")
        async def apply(connection):
            current = await self._query_optional_with(connection, "SELECT manifest FROM prolly_roots WHERE name=%s FOR UPDATE", (name,))
            if current != expected:
                return False, current
            await self._write_root(connection, name, replacement)
            return True, replacement
        return await self._transaction(apply, (name,))

    async def _list_root_manifests(self) -> list[tuple[bytes, bytes]]:
        return [(bytes(name), bytes(value)) for name, value in await self._query_all("SELECT name,manifest FROM prolly_roots ORDER BY name")]

    async def _commit_transaction(self, nodes, conditions, roots):
        names = tuple(sorted({self._key(bytes(condition.name), 255, "root name") for condition in conditions}))
        async def apply(connection):
            for condition in conditions:
                name = bytes(condition.name)
                current = await self._query_optional_with(connection, "SELECT manifest FROM prolly_roots WHERE name=%s FOR UPDATE", (name,))
                if current != self._optional_value(condition.expected):
                    return ffi.StoreTransactionConflictRecord(
                        name=name, expected=condition.expected, current=optional_bytes(current)
                    )
            for item in nodes:
                key = self._key(bytes(item.key), 32, "node CID")
                if item.value.present:
                    await self._execute_with(connection, UPSERT_NODE, (key, bytes(item.value.value)))
                else:
                    await self._execute_with(connection, "DELETE FROM prolly_nodes WHERE cid=%s", (key,))
            for root in roots:
                await self._write_root(connection, self._key(bytes(root.name), 255, "root name"), self._optional_value(root.replacement))
            return None
        return await self._transaction(apply, names)

    async def _execute(self, sql: str, values: tuple[Any, ...]) -> None:
        await self._ensure_open()
        async with self._pool.acquire() as connection:
            await self._execute_with(connection, sql, values)

    async def _query_optional(self, sql: str, values: tuple[Any, ...]) -> bytes | None:
        await self._ensure_open()
        async with self._pool.acquire() as connection:
            return await self._query_optional_with(connection, sql, values)

    async def _query_all(self, sql: str, values: tuple[Any, ...] = ()):
        await self._ensure_open()
        async with self._pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(sql, values)
                return await cursor.fetchall()

    async def _transaction(self, call: Callable[[Any], Awaitable[T]], lock_names: tuple[bytes, ...] = ()) -> T:
        await self._ensure_open()
        async with self._pool.acquire() as connection:
            acquired: list[bytes] = []
            try:
                for name in lock_names:
                    async with connection.cursor() as cursor:
                        await cursor.execute("SELECT GET_LOCK(CONCAT('prolly:', HEX(%s)), 10)", (name,))
                        if (await cursor.fetchone())[0] != 1:
                            raise StoreFailure("unavailable", "MySQL root lock timed out", True)
                    acquired.append(name)
                await connection.begin()
                try:
                    result = await call(connection)
                    await connection.commit()
                    return result
                except BaseException:
                    await connection.rollback()
                    raise
            finally:
                for name in reversed(acquired):
                    async with connection.cursor() as cursor:
                        await cursor.execute("SELECT RELEASE_LOCK(CONCAT('prolly:', HEX(%s)))", (name,))

    async def _ensure_open(self) -> None:
        if self._closed:
            raise StoreFailure("closed", "MySQL store is closed")

    @staticmethod
    def _key(value: bytes, maximum: int, label: str) -> bytes:
        value = bytes(value)
        if len(value) > maximum:
            raise StoreFailure("invalid_argument", f"{label} exceeds {maximum} bytes")
        return value

    @staticmethod
    async def _execute_with(connection, sql: str, values: tuple[Any, ...]) -> None:
        async with connection.cursor() as cursor:
            await cursor.execute(sql, values)

    @staticmethod
    async def _query_optional_with(connection, sql: str, values: tuple[Any, ...]) -> bytes | None:
        async with connection.cursor() as cursor:
            await cursor.execute(sql, values)
            row = await cursor.fetchone()
            return None if row is None else bytes(row[0])

    @staticmethod
    async def _write_root(connection, name: bytes, replacement: bytes | None) -> None:
        if replacement is None:
            await MysqlStore._execute_with(connection, "DELETE FROM prolly_roots WHERE name=%s", (name,))
        else:
            await MysqlStore._execute_with(connection, UPSERT_ROOT, (name, replacement))


__all__ = ["CREATE_SCHEMA", "MysqlStore"]
