"""redis-py asyncio adapter for the version-1 Prolly store protocol."""

from __future__ import annotations

from typing import Any

from prolly.remote_store import RemoteStoreAdapter, StoreFailure, descriptor, optional_bytes
from prolly.uniffi import prolly as ffi

CAS_SCRIPT = """
local current = redis.call('GET', KEYS[1])
local expected_present = ARGV[1] == '1'
if expected_present then
  if current == false or current ~= ARGV[2] then
    return {0, current == false and 0 or 1, current or ''}
  end
elseif current ~= false then
  return {0, 1, current}
end
if ARGV[3] == '1' then
  redis.call('SET', KEYS[1], ARGV[4])
  return {1, 1, ARGV[4]}
end
redis.call('DEL', KEYS[1])
return {1, 0, ''}
"""

MUTATE_SCRIPT = """
for index = 1, #KEYS do
  local offset = (index - 1) * 2
  if ARGV[offset + 1] == '1' then
    redis.call('SET', KEYS[index], ARGV[offset + 2])
  else
    redis.call('DEL', KEYS[index])
  end
end
return 1
"""

TRANSACTION_SCRIPT = """
local condition_count = tonumber(ARGV[1])
local node_count = tonumber(ARGV[2])
local root_count = tonumber(ARGV[3])
local argument = 4
for index = 1, condition_count do
  local current = redis.call('GET', KEYS[index])
  local expected_present = ARGV[argument] == '1'
  local matches = (expected_present and current ~= false and current == ARGV[argument + 1])
    or (not expected_present and current == false)
  if not matches then
    return {0, index, current == false and 0 or 1, current or ''}
  end
  argument = argument + 2
end
local key_index = condition_count + 1
for _ = 1, node_count do
  if ARGV[argument] == '1' then redis.call('SET', KEYS[key_index], ARGV[argument + 1])
  else redis.call('DEL', KEYS[key_index]) end
  argument = argument + 2
  key_index = key_index + 1
end
for _ = 1, root_count do
  if ARGV[argument] == '1' then redis.call('SET', KEYS[key_index], ARGV[argument + 1])
  else redis.call('DEL', KEYS[key_index]) end
  argument = argument + 2
  key_index = key_index + 1
end
return {1}
"""


class RedisStore(RemoteStoreAdapter):
    """Borrow a caller-owned ``redis.asyncio.Redis`` client."""

    def __init__(
        self, client: Any, *, key_prefix: bytes = b"prolly:",
        adapter_name: str = "redis-v1", read_parallelism: int = 16,
    ):
        if client is None:
            raise ValueError("Redis client is required")
        super().__init__(descriptor(
            "redis", adapter_name=adapter_name, native_batch_reads=True,
            atomic_batch_writes=True, atomic_nodes_and_hint=True,
            read_parallelism=read_parallelism,
        ))
        self._client = client
        self._prefix = bytes(key_prefix)
        self._closed = False

    async def close(self) -> None:
        self._closed = True

    async def clear_namespace(self) -> None:
        self._ensure_open()
        if not self._prefix:
            raise StoreFailure("invalid_argument", "refusing to clear an empty Redis key prefix")
        keys = await self._scan(self._prefix)
        for index in range(0, len(keys), 256):
            await self._client.delete(*keys[index:index + 256])

    async def _get_node(self, cid: bytes) -> bytes | None:
        return await self._get(self._family(b"node:", cid))

    async def _put_node(self, cid: bytes, value: bytes) -> None:
        await self._set(self._family(b"node:", cid), value)

    async def _delete_node(self, cid: bytes) -> None:
        await self._delete(self._family(b"node:", cid))

    async def _batch_nodes(self, operations) -> None:
        keys = [self._family(b"node:", bytes(item.key)) for item in operations]
        arguments: list[bytes] = []
        for item in operations:
            arguments.extend((b"1", bytes(item.value.value)) if item.value.present else (b"0", b""))
        await self._mutate(keys, arguments)

    async def _batch_get_nodes_ordered(self, cids: tuple[bytes, ...]) -> list[bytes | None]:
        self._ensure_open()
        if not cids:
            return []
        values = await self._client.mget([self._family(b"node:", cid) for cid in cids])
        return [None if value is None else bytes(value) for value in values]

    async def _list_node_cids(self) -> list[bytes]:
        family = self._prefix + b"node:"
        return sorted(key[len(family):] for key in await self._scan(family) if len(key) - len(family) == 32)

    async def _get_hint(self, namespace: bytes, key: bytes) -> bytes | None:
        return await self._get(self._hint_key(namespace, key))

    async def _put_hint(self, namespace: bytes, key: bytes, value: bytes) -> None:
        await self._set(self._hint_key(namespace, key), value)

    async def _batch_put_nodes_with_hint(self, nodes, namespace: bytes, key: bytes, value: bytes) -> None:
        keys = [self._family(b"node:", bytes(node.key)) for node in nodes]
        arguments = [part for node in nodes for part in (b"1", bytes(node.value))]
        keys.append(self._hint_key(namespace, key))
        arguments.extend((b"1", value))
        await self._mutate(keys, arguments)

    async def _get_root_manifest(self, name: bytes) -> bytes | None:
        return await self._get(self._family(b"root:", name))

    async def _put_root_manifest(self, name: bytes, manifest: bytes) -> None:
        await self._set(self._family(b"root:", name), manifest)

    async def _delete_root_manifest(self, name: bytes) -> None:
        await self._delete(self._family(b"root:", name))

    async def _compare_and_swap_root_manifest(self, name, expected, replacement):
        self._ensure_open()
        response = await self._client.eval(
            CAS_SCRIPT, 1, self._family(b"root:", name),
            self._flag(expected is not None), expected or b"",
            self._flag(replacement is not None), replacement or b"",
        )
        self._array(response, 3, "CAS")
        return int(response[0]) == 1, self._optional_parts(response[1], response[2])

    async def _list_root_manifests(self) -> list[tuple[bytes, bytes]]:
        self._ensure_open()
        family = self._prefix + b"root:"
        keys = sorted(await self._scan(family))
        if not keys:
            return []
        values = await self._client.mget(keys)
        return [
            (key[len(family):], bytes(value))
            for key, value in zip(keys, values, strict=True) if value is not None
        ]

    async def _commit_transaction(self, nodes, conditions, roots):
        self._ensure_open()
        keys = [self._family(b"root:", bytes(item.name)) for item in conditions]
        keys.extend(self._family(b"node:", bytes(item.key)) for item in nodes)
        keys.extend(self._family(b"root:", bytes(item.name)) for item in roots)
        arguments: list[Any] = [len(conditions), len(nodes), len(roots)]
        for condition in conditions:
            expected = self._optional_value(condition.expected)
            arguments.extend((self._flag(expected is not None), expected or b""))
        for node in nodes:
            arguments.extend((self._flag(node.value.present), bytes(node.value.value) if node.value.present else b""))
        for root in roots:
            replacement = self._optional_value(root.replacement)
            arguments.extend((self._flag(replacement is not None), replacement or b""))
        response = await self._client.eval(TRANSACTION_SCRIPT, len(keys), *keys, *arguments)
        if not isinstance(response, (list, tuple)) or not response:
            raise StoreFailure("invalid_data", "Redis returned an invalid transaction response")
        if int(response[0]) == 1:
            return None
        self._array(response, 4, "transaction conflict")
        index = int(response[1]) - 1
        if index < 0 or index >= len(conditions):
            raise StoreFailure("invalid_data", "Redis returned an invalid conflict index")
        condition = conditions[index]
        return ffi.StoreTransactionConflictRecord(
            name=bytes(condition.name), expected=condition.expected,
            current=optional_bytes(self._optional_parts(response[2], response[3])),
        )

    async def _get(self, key: bytes) -> bytes | None:
        self._ensure_open()
        value = await self._client.get(key)
        return None if value is None else bytes(value)

    async def _set(self, key: bytes, value: bytes) -> None:
        self._ensure_open()
        await self._client.set(key, value)

    async def _delete(self, key: bytes) -> None:
        self._ensure_open()
        await self._client.delete(key)

    async def _mutate(self, keys: list[bytes], arguments: list[bytes]) -> None:
        self._ensure_open()
        if keys:
            await self._client.eval(MUTATE_SCRIPT, len(keys), *keys, *arguments)

    async def _scan(self, family: bytes) -> list[bytes]:
        cursor: int | bytes = 0
        keys: list[bytes] = []
        while True:
            cursor, page = await self._client.scan(cursor=cursor, count=1024)
            keys.extend(raw for item in page if (raw := bytes(item)).startswith(family))
            if int(cursor) == 0:
                return keys

    def _ensure_open(self) -> None:
        if self._closed:
            raise StoreFailure("closed", "Redis store is closed")

    def _family(self, family: bytes, suffix: bytes) -> bytes:
        return self._prefix + family + bytes(suffix)

    def _hint_key(self, namespace: bytes, key: bytes) -> bytes:
        return self._prefix + b"hint:" + len(namespace).to_bytes(8, "big") + namespace + key

    @staticmethod
    def _flag(value: bool) -> bytes:
        return b"1" if value else b"0"

    @staticmethod
    def _array(value: Any, length: int, label: str) -> None:
        if not isinstance(value, (list, tuple)) or len(value) < length:
            raise StoreFailure("invalid_data", f"Redis returned an invalid {label} response")

    @staticmethod
    def _optional_parts(present: Any, value: Any) -> bytes | None:
        return bytes(value) if int(present) == 1 else None


__all__ = ["CAS_SCRIPT", "MUTATE_SCRIPT", "RedisStore", "TRANSACTION_SCRIPT"]
