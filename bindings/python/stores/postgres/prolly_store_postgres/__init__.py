"""Psycopg 3 adapter for the version-1 asynchronous Prolly store protocol."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from prolly.remote_store import RemoteStoreAdapter, StoreFailure, descriptor
from prolly.uniffi import prolly as ffi

T = TypeVar("T")

CREATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS prolly_nodes (
  cid bytea PRIMARY KEY,
  node bytea NOT NULL
);
CREATE TABLE IF NOT EXISTS prolly_hints (
  namespace bytea NOT NULL,
  key bytea NOT NULL,
  value bytea NOT NULL,
  PRIMARY KEY(namespace, key)
);
CREATE TABLE IF NOT EXISTS prolly_roots (
  name bytea PRIMARY KEY,
  manifest bytea NOT NULL
);
"""

UPSERT_NODE = """INSERT INTO prolly_nodes (cid, node) VALUES (%s, %s)
ON CONFLICT(cid) DO UPDATE SET node = excluded.node"""
UPSERT_HINT = """INSERT INTO prolly_hints (namespace, key, value) VALUES (%s, %s, %s)
ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value"""
UPSERT_ROOT = """INSERT INTO prolly_roots (name, manifest) VALUES (%s, %s)
ON CONFLICT(name) DO UPDATE SET manifest = excluded.manifest"""
LOCK_ROOT = "SELECT pg_advisory_xact_lock(hashtextextended(encode(%s::bytea, 'hex'), 0))"


class PostgresStore(RemoteStoreAdapter):
    """Borrow an open ``psycopg_pool.AsyncConnectionPool``."""

    def __init__(self, pool: Any, *, adapter_name: str = "postgres-v1", read_parallelism: int = 16):
        if pool is None:
            raise ValueError("PostgreSQL async connection pool is required")
        super().__init__(descriptor(
            "postgresql", adapter_name=adapter_name, native_batch_reads=True,
            atomic_batch_writes=True, atomic_nodes_and_hint=True,
            read_parallelism=read_parallelism,
        ))
        self._pool = pool
        self._closed = False

    async def initialize_schema(self) -> None:
        await self._ensure_open()
        async with self._pool.connection() as connection:
            await connection.execute(CREATE_SCHEMA)

    async def close(self) -> None:
        self._closed = True

    async def _get_node(self, cid: bytes) -> bytes | None:
        return await self._query_optional("SELECT node FROM prolly_nodes WHERE cid = %s", (cid,))

    async def _put_node(self, cid: bytes, value: bytes) -> None:
        await self._execute(UPSERT_NODE, (cid, value))

    async def _delete_node(self, cid: bytes) -> None:
        await self._execute("DELETE FROM prolly_nodes WHERE cid = %s", (cid,))

    async def _batch_nodes(self, operations) -> None:
        async def apply(connection):
            for item in operations:
                if item.value.present:
                    await connection.execute(UPSERT_NODE, (bytes(item.key), bytes(item.value.value)))
                else:
                    await connection.execute("DELETE FROM prolly_nodes WHERE cid = %s", (bytes(item.key),))
        await self._transaction(apply)

    async def _batch_get_nodes_ordered(self, cids: tuple[bytes, ...]) -> list[bytes | None]:
        if not cids:
            return []
        await self._ensure_open()
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                "SELECT cid, node FROM prolly_nodes WHERE cid = ANY(%s::bytea[])",
                (list(cids),),
            )
            values = {bytes(cid): bytes(node) for cid, node in await cursor.fetchall()}
        return [values.get(cid) for cid in cids]

    async def _list_node_cids(self) -> list[bytes]:
        return [bytes(row[0]) for row in await self._query_all("SELECT cid FROM prolly_nodes ORDER BY cid")]

    async def _get_hint(self, namespace: bytes, key: bytes) -> bytes | None:
        return await self._query_optional(
            "SELECT value FROM prolly_hints WHERE namespace = %s AND key = %s", (namespace, key)
        )

    async def _put_hint(self, namespace: bytes, key: bytes, value: bytes) -> None:
        await self._execute(UPSERT_HINT, (namespace, key, value))

    async def _batch_put_nodes_with_hint(self, nodes, namespace: bytes, key: bytes, value: bytes) -> None:
        async def apply(connection):
            for node in nodes:
                await connection.execute(UPSERT_NODE, (bytes(node.key), bytes(node.value)))
            await connection.execute(UPSERT_HINT, (namespace, key, value))
        await self._transaction(apply)

    async def _get_root_manifest(self, name: bytes) -> bytes | None:
        return await self._query_optional("SELECT manifest FROM prolly_roots WHERE name = %s", (name,))

    async def _put_root_manifest(self, name: bytes, manifest: bytes) -> None:
        await self._execute(UPSERT_ROOT, (name, manifest))

    async def _delete_root_manifest(self, name: bytes) -> None:
        await self._execute("DELETE FROM prolly_roots WHERE name = %s", (name,))

    async def _compare_and_swap_root_manifest(
        self, name: bytes, expected: bytes | None, replacement: bytes | None
    ) -> tuple[bool, bytes | None]:
        async def apply(connection):
            await connection.execute(LOCK_ROOT, (name,))
            current = await self._query_optional_with(connection, "SELECT manifest FROM prolly_roots WHERE name = %s FOR UPDATE", (name,))
            if current != expected:
                return False, current
            await self._write_root(connection, name, replacement)
            return True, replacement
        return await self._transaction(apply)

    async def _list_root_manifests(self) -> list[tuple[bytes, bytes]]:
        return [
            (bytes(name), bytes(manifest))
            for name, manifest in await self._query_all("SELECT name, manifest FROM prolly_roots ORDER BY name")
        ]

    async def _commit_transaction(self, nodes, conditions, roots):
        async def apply(connection):
            for name in sorted({bytes(condition.name) for condition in conditions}):
                await connection.execute(LOCK_ROOT, (name,))
            for condition in conditions:
                name = bytes(condition.name)
                expected = self._optional_value(condition.expected)
                current = await self._query_optional_with(
                    connection, "SELECT manifest FROM prolly_roots WHERE name = %s FOR UPDATE", (name,)
                )
                if current != expected:
                    from prolly.remote_store import optional_bytes
                    return ffi.StoreTransactionConflictRecord(
                        name=name, expected=condition.expected, current=optional_bytes(current)
                    )
            for item in nodes:
                if item.value.present:
                    await connection.execute(UPSERT_NODE, (bytes(item.key), bytes(item.value.value)))
                else:
                    await connection.execute("DELETE FROM prolly_nodes WHERE cid = %s", (bytes(item.key),))
            for root in roots:
                await self._write_root(connection, bytes(root.name), self._optional_value(root.replacement))
            return None
        return await self._transaction(apply)

    async def _execute(self, sql: str, values: tuple[Any, ...]) -> None:
        await self._ensure_open()
        async with self._pool.connection() as connection:
            await connection.execute(sql, values)

    async def _query_optional(self, sql: str, values: tuple[Any, ...]) -> bytes | None:
        await self._ensure_open()
        async with self._pool.connection() as connection:
            return await self._query_optional_with(connection, sql, values)

    async def _query_all(self, sql: str):
        await self._ensure_open()
        async with self._pool.connection() as connection:
            return await (await connection.execute(sql)).fetchall()

    async def _transaction(self, call: Callable[[Any], Awaitable[T]]) -> T:
        await self._ensure_open()
        async with self._pool.connection() as connection:
            async with connection.transaction():
                return await call(connection)

    async def _ensure_open(self) -> None:
        if self._closed:
            raise StoreFailure("closed", "PostgreSQL store is closed")

    @staticmethod
    async def _query_optional_with(connection, sql: str, values: tuple[Any, ...]) -> bytes | None:
        row = await (await connection.execute(sql, values)).fetchone()
        return None if row is None else bytes(row[0])

    @staticmethod
    async def _write_root(connection, name: bytes, replacement: bytes | None) -> None:
        if replacement is None:
            await connection.execute("DELETE FROM prolly_roots WHERE name = %s", (name,))
        else:
            await connection.execute(UPSERT_ROOT, (name, replacement))


__all__ = ["CREATE_SCHEMA", "PostgresStore"]
