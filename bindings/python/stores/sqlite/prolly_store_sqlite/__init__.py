"""SQLite adapter for the Prolly async store protocol."""

from __future__ import annotations

import asyncio
import sqlite3
from concurrent.futures import Executor
from functools import partial

from prolly.remote_store import RemoteStoreAdapter, descriptor, optional_bytes
from prolly.uniffi import prolly as ffi

CREATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS prolly_nodes (cid BLOB PRIMARY KEY NOT NULL, node BLOB NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS prolly_hints (namespace BLOB NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY (namespace, key)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS prolly_roots (name BLOB PRIMARY KEY NOT NULL, manifest BLOB NOT NULL) WITHOUT ROWID;
"""


class SqliteStore(RemoteStoreAdapter):
    """Async adapter over a caller-owned, cross-thread SQLite connection."""

    def __init__(self, connection: sqlite3.Connection, executor: Executor | None = None):
        if not isinstance(connection, sqlite3.Connection):
            raise TypeError("connection must be sqlite3.Connection")
        super().__init__(descriptor(
            "sqlite",
            adapter_name="sqlite-v1",
            native_batch_reads=True,
            atomic_batch_writes=True,
            atomic_nodes_and_hint=True,
        ))
        self._connection = connection
        self._executor = executor
        self._lock = asyncio.Lock()
        self._closed = False

    async def initialize_schema(self) -> None:
        await self._run(self._connection.executescript, CREATE_SCHEMA)

    async def close(self) -> None:
        self._closed = True

    async def _run(self, function, *args):
        if self._closed:
            raise RuntimeError("SQLite store is closed")
        async with self._lock:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(self._executor, partial(function, *args))

    async def _get_node(self, cid):
        return await self._query_optional("SELECT node FROM prolly_nodes WHERE cid = ?", (cid,))

    async def _put_node(self, cid, value):
        await self._write("INSERT INTO prolly_nodes VALUES (?, ?) ON CONFLICT(cid) DO UPDATE SET node=excluded.node", (cid, value))

    async def _delete_node(self, cid):
        await self._write("DELETE FROM prolly_nodes WHERE cid = ?", (cid,))

    async def _batch_nodes(self, operations):
        def operation():
            def body():
                for item in operations:
                    if item.value.present:
                        self._connection.execute("INSERT INTO prolly_nodes VALUES (?, ?) ON CONFLICT(cid) DO UPDATE SET node=excluded.node", (item.key, item.value.value))
                    else:
                        self._connection.execute("DELETE FROM prolly_nodes WHERE cid = ?", (item.key,))
            self._transaction(body)
        await self._run(operation)

    async def _batch_get_nodes_ordered(self, cids):
        def operation():
            statement = "SELECT node FROM prolly_nodes WHERE cid = ?"
            return [self._row_value(self._connection.execute(statement, (cid,)).fetchone()) for cid in cids]
        return await self._run(operation)

    async def _list_node_cids(self):
        return await self._run(lambda: [bytes(row[0]) for row in self._connection.execute("SELECT cid FROM prolly_nodes ORDER BY cid")])

    async def _get_hint(self, namespace, key):
        return await self._query_optional("SELECT value FROM prolly_hints WHERE namespace = ? AND key = ?", (namespace, key))

    async def _put_hint(self, namespace, key, value):
        await self._write("INSERT INTO prolly_hints VALUES (?, ?, ?) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value", (namespace, key, value))

    async def _batch_put_nodes_with_hint(self, nodes, namespace, key, value):
        def operation():
            def body():
                for node in nodes:
                    self._connection.execute("INSERT INTO prolly_nodes VALUES (?, ?) ON CONFLICT(cid) DO UPDATE SET node=excluded.node", (node.key, node.value))
                self._connection.execute("INSERT INTO prolly_hints VALUES (?, ?, ?) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value", (namespace, key, value))
            self._transaction(body)
        await self._run(operation)

    async def _get_root_manifest(self, name):
        return await self._query_optional("SELECT manifest FROM prolly_roots WHERE name = ?", (name,))

    async def _put_root_manifest(self, name, manifest):
        await self._write("INSERT INTO prolly_roots VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET manifest=excluded.manifest", (name, manifest))

    async def _delete_root_manifest(self, name):
        await self._write("DELETE FROM prolly_roots WHERE name = ?", (name,))

    async def _compare_and_swap_root_manifest(self, name, expected, replacement):
        def operation():
            result = None
            def body():
                nonlocal result
                current = self._row_value(self._connection.execute("SELECT manifest FROM prolly_roots WHERE name = ?", (name,)).fetchone())
                if current != expected:
                    result = (False, current)
                    return
                self._write_root(name, replacement)
                result = (True, replacement)
            self._transaction(body)
            return result
        return await self._run(operation)

    async def _list_root_manifests(self):
        return await self._run(lambda: [(bytes(row[0]), bytes(row[1])) for row in self._connection.execute("SELECT name, manifest FROM prolly_roots ORDER BY name")])

    async def _commit_transaction(self, nodes, conditions, roots):
        def operation():
            conflict = None
            def body():
                nonlocal conflict
                for condition in conditions:
                    current = self._row_value(self._connection.execute("SELECT manifest FROM prolly_roots WHERE name = ?", (condition.name,)).fetchone())
                    expected = condition.expected.value if condition.expected.present else None
                    if current != expected:
                        conflict = ffi.StoreTransactionConflictRecord(name=condition.name, expected=condition.expected, current=optional_bytes(current))
                        return
                for item in nodes:
                    if item.value.present:
                        self._connection.execute("INSERT INTO prolly_nodes VALUES (?, ?) ON CONFLICT(cid) DO UPDATE SET node=excluded.node", (item.key, item.value.value))
                    else:
                        self._connection.execute("DELETE FROM prolly_nodes WHERE cid = ?", (item.key,))
                for root in roots:
                    self._write_root(root.name, root.replacement.value if root.replacement.present else None)
            self._transaction(body, should_rollback=lambda: conflict is not None)
            return conflict
        return await self._run(operation)

    async def _query_optional(self, sql, parameters):
        return await self._run(lambda: self._row_value(self._connection.execute(sql, parameters).fetchone()))

    async def _write(self, sql, parameters):
        def operation():
            with self._connection:
                self._connection.execute(sql, parameters)
        await self._run(operation)

    def _write_root(self, name, replacement):
        if replacement is None:
            self._connection.execute("DELETE FROM prolly_roots WHERE name = ?", (name,))
        else:
            self._connection.execute("INSERT INTO prolly_roots VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET manifest=excluded.manifest", (name, replacement))

    def _transaction(self, body, should_rollback=lambda: False):
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            body()
            if should_rollback():
                self._connection.rollback()
            else:
                self._connection.commit()
        except BaseException:
            self._connection.rollback()
            raise

    @staticmethod
    def _row_value(row):
        return None if row is None else bytes(row[0])


__all__ = ["CREATE_SCHEMA", "SqliteStore"]
