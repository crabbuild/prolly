"""aioboto3 DynamoDB adapter for the version-1 Prolly store protocol."""

from __future__ import annotations

import asyncio
from typing import Any

from prolly.remote_store import RemoteStoreAdapter, StoreFailure, descriptor, optional_bytes
from prolly.uniffi import prolly as ffi

BATCH_GET_LIMIT = 100
BATCH_WRITE_LIMIT = 25
TRANSACTION_LIMIT = 100
RETRY_LIMIT = 8


class DynamoDbStore(RemoteStoreAdapter):
    """Borrow a caller-owned low-level aioboto3 DynamoDB client."""

    def __init__(self, client: Any, *, table_name: str, key_prefix: bytes = b"prolly:", read_parallelism: int = 16):
        if client is None:
            raise ValueError("DynamoDB client is required")
        if not table_name.strip():
            raise ValueError("DynamoDB table name is required")
        super().__init__(descriptor(
            "dynamodb", adapter_name="dynamodb-v1", native_batch_reads=True,
            atomic_batch_writes=False, atomic_nodes_and_hint=False,
            read_parallelism=read_parallelism, max_batch_read_items=BATCH_GET_LIMIT,
            max_batch_write_items=BATCH_WRITE_LIMIT,
            max_transaction_operations=TRANSACTION_LIMIT,
        ))
        self._client = client
        self._table = table_name
        self._prefix = bytes(key_prefix)
        self._closed = False

    async def initialize_table(self) -> None:
        self._ensure_open()
        try:
            output = await self._client.describe_table(TableName=self._table)
            self._validate_table(output.get("Table"))
            return
        except Exception as error:
            if self._error_code(error) != "ResourceNotFoundException":
                raise
        try:
            await self._client.create_table(
                TableName=self._table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "B"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
        except Exception as error:
            if self._error_code(error) != "ResourceInUseException":
                raise
        for _ in range(100):
            try:
                output = await self._client.describe_table(TableName=self._table)
                if output.get("Table", {}).get("TableStatus") == "ACTIVE":
                    self._validate_table(output["Table"])
                    return
            except Exception as error:
                if self._error_code(error) != "ResourceNotFoundException":
                    raise
            await asyncio.sleep(0.05)
        raise StoreFailure("unavailable", "DynamoDB table did not become active", True)

    async def close(self) -> None:
        self._closed = True

    async def clear_namespace(self) -> None:
        if not self._prefix:
            raise StoreFailure("invalid_argument", "refusing to clear an empty DynamoDB key prefix")
        await self._batch_write([{"DeleteRequest": {"Key": self._key(key)}} for key in await self._scan_keys(self._prefix)])

    async def _get_node(self, cid: bytes) -> bytes | None:
        return await self._get(self._family(b"node:", cid))

    async def _put_node(self, cid: bytes, value: bytes) -> None:
        await self._put(self._family(b"node:", cid), value)

    async def _delete_node(self, cid: bytes) -> None:
        await self._delete(self._family(b"node:", cid))

    async def _batch_nodes(self, operations) -> None:
        requests = []
        for item in operations:
            key = self._family(b"node:", bytes(item.key))
            requests.append(
                {"PutRequest": {"Item": self._item(key, bytes(item.value.value))}}
                if item.value.present else {"DeleteRequest": {"Key": self._key(key)}}
            )
        await self._batch_write(requests)

    async def _batch_get_nodes_ordered(self, cids: tuple[bytes, ...]) -> list[bytes | None]:
        storage_keys = [self._family(b"node:", cid) for cid in cids]
        unique = list(dict.fromkeys(storage_keys))
        values: dict[bytes, bytes] = {}
        for start in range(0, len(unique), BATCH_GET_LIMIT):
            pending = [self._key(key) for key in unique[start:start + BATCH_GET_LIMIT]]
            for attempt in range(RETRY_LIMIT):
                if not pending:
                    break
                output = await self._client.batch_get_item(RequestItems={self._table: {
                    "Keys": pending, "ConsistentRead": True,
                    "ProjectionExpression": "#pk, #value",
                    "ExpressionAttributeNames": {"#pk": "pk", "#value": "value"},
                }})
                for item in output.get("Responses", {}).get(self._table, []):
                    values[self._binary(item, "pk")] = self._binary(item, "value")
                pending = output.get("UnprocessedKeys", {}).get(self._table, {}).get("Keys", [])
                if pending:
                    if attempt + 1 == RETRY_LIMIT:
                        raise StoreFailure("resource_exhausted", "DynamoDB batch get left keys unprocessed", True)
                    await asyncio.sleep(0.01 * 2 ** min(attempt, 6))
        return [values.get(key) for key in storage_keys]

    async def _list_node_cids(self) -> list[bytes]:
        prefix = self._family(b"node:", b"")
        return sorted(key[len(prefix):] for key in await self._scan_keys(prefix) if len(key) - len(prefix) == 32)

    async def _get_hint(self, namespace: bytes, key: bytes) -> bytes | None:
        return await self._get(self._hint(namespace, key))

    async def _put_hint(self, namespace: bytes, key: bytes, value: bytes) -> None:
        await self._put(self._hint(namespace, key), value)

    async def _batch_put_nodes_with_hint(self, nodes, namespace: bytes, key: bytes, value: bytes) -> None:
        await self._batch_nodes(tuple(
            ffi.NodeMutationRecord(key=bytes(node.key), value=optional_bytes(bytes(node.value))) for node in nodes
        ))
        await self._put_hint(namespace, key, value)

    async def _get_root_manifest(self, name: bytes) -> bytes | None:
        return await self._get(self._family(b"root:", name))

    async def _put_root_manifest(self, name: bytes, manifest: bytes) -> None:
        await self._put(self._family(b"root:", name), manifest)

    async def _delete_root_manifest(self, name: bytes) -> None:
        await self._delete(self._family(b"root:", name))

    async def _compare_and_swap_root_manifest(self, name, expected, replacement):
        key = self._family(b"root:", name)
        condition = self._condition(expected)
        try:
            if replacement is None:
                await self._client.delete_item(TableName=self._table, Key=self._key(key), **condition)
            else:
                await self._client.put_item(TableName=self._table, Item=self._item(key, replacement), **condition)
            return True, replacement
        except Exception as error:
            if self._error_code(error) != "ConditionalCheckFailedException":
                raise
            return False, await self._get(key)

    async def _list_root_manifests(self) -> list[tuple[bytes, bytes]]:
        prefix = self._family(b"root:", b"")
        result = []
        for key in sorted(await self._scan_keys(prefix)):
            value = await self._get(key)
            if value is not None:
                result.append((key[len(prefix):], value))
        return result

    async def _commit_transaction(self, nodes, conditions, roots):
        root_names = {bytes(root.name) for root in roots}
        count = len(nodes) + len(roots) + sum(bytes(condition.name) not in root_names for condition in conditions)
        if count > TRANSACTION_LIMIT:
            raise StoreFailure("resource_exhausted", f"DynamoDB transaction exceeds {TRANSACTION_LIMIT} operations")
        condition_by_name = {bytes(item.name): item for item in conditions}
        items = []
        for condition in conditions:
            name = bytes(condition.name)
            if name not in root_names:
                items.append({"ConditionCheck": {
                    "TableName": self._table, "Key": self._key(self._family(b"root:", name)),
                    **self._condition(self._optional_value(condition.expected)),
                }})
        for root in roots:
            name = bytes(root.name)
            replacement = self._optional_value(root.replacement)
            conditional = self._condition(self._optional_value(condition_by_name[name].expected)) if name in condition_by_name else {}
            operation = (
                {"Put": {"TableName": self._table, "Item": self._item(self._family(b"root:", name), replacement), **conditional}}
                if replacement is not None else
                {"Delete": {"TableName": self._table, "Key": self._key(self._family(b"root:", name)), **conditional}}
            )
            items.append(operation)
        for node in nodes:
            key = self._family(b"node:", bytes(node.key))
            items.append(
                {"Put": {"TableName": self._table, "Item": self._item(key, bytes(node.value.value))}}
                if node.value.present else {"Delete": {"TableName": self._table, "Key": self._key(key)}}
            )
        if not items:
            return None
        try:
            await self._client.transact_write_items(TransactItems=items)
            return None
        except Exception as error:
            if self._error_code(error) != "TransactionCanceledException":
                raise
            for condition in conditions:
                current = await self._get(self._family(b"root:", bytes(condition.name)))
                if current != self._optional_value(condition.expected):
                    return ffi.StoreTransactionConflictRecord(
                        name=bytes(condition.name), expected=condition.expected, current=optional_bytes(current)
                    )
            raise

    async def _get(self, key: bytes) -> bytes | None:
        self._ensure_open()
        output = await self._client.get_item(
            TableName=self._table, Key=self._key(key), ConsistentRead=True,
            ProjectionExpression="#value", ExpressionAttributeNames={"#value": "value"},
        )
        return None if not output.get("Item") else self._binary(output["Item"], "value")

    async def _put(self, key: bytes, value: bytes) -> None:
        self._ensure_open()
        await self._client.put_item(TableName=self._table, Item=self._item(key, value))

    async def _delete(self, key: bytes) -> None:
        self._ensure_open()
        await self._client.delete_item(TableName=self._table, Key=self._key(key))

    async def _batch_write(self, requests: list[dict]) -> None:
        self._ensure_open()
        for start in range(0, len(requests), BATCH_WRITE_LIMIT):
            pending = requests[start:start + BATCH_WRITE_LIMIT]
            for attempt in range(RETRY_LIMIT):
                if not pending:
                    break
                output = await self._client.batch_write_item(RequestItems={self._table: pending})
                pending = output.get("UnprocessedItems", {}).get(self._table, [])
                if pending:
                    if attempt + 1 == RETRY_LIMIT:
                        raise StoreFailure("resource_exhausted", "DynamoDB batch write left requests unprocessed", True)
                    await asyncio.sleep(0.01 * 2 ** min(attempt, 6))

    async def _scan_keys(self, prefix: bytes) -> list[bytes]:
        self._ensure_open()
        keys = []
        start = None
        while True:
            request = {
                "TableName": self._table, "ConsistentRead": True,
                "ProjectionExpression": "#pk", "FilterExpression": "begins_with(#pk, :prefix)",
                "ExpressionAttributeNames": {"#pk": "pk"},
                "ExpressionAttributeValues": {":prefix": {"B": prefix}},
            }
            if start:
                request["ExclusiveStartKey"] = start
            output = await self._client.scan(**request)
            keys.extend(self._binary(item, "pk") for item in output.get("Items", []))
            start = output.get("LastEvaluatedKey")
            if not start:
                return keys

    def _ensure_open(self) -> None:
        if self._closed:
            raise StoreFailure("closed", "DynamoDB store is closed")

    def _family(self, family: bytes, suffix: bytes) -> bytes:
        return self._prefix + family + bytes(suffix)

    def _hint(self, namespace: bytes, key: bytes) -> bytes:
        return self._prefix + b"hint:" + len(namespace).to_bytes(8, "big") + namespace + key

    @staticmethod
    def _key(key: bytes) -> dict:
        return {"pk": {"B": key}}

    @classmethod
    def _item(cls, key: bytes, value: bytes) -> dict:
        return {**cls._key(key), "value": {"B": value}}

    @staticmethod
    def _binary(item: dict, name: str) -> bytes:
        try:
            return bytes(item[name]["B"])
        except (KeyError, TypeError):
            raise StoreFailure("invalid_data", f"DynamoDB item has invalid {name} attribute") from None

    @staticmethod
    def _condition(expected: bytes | None) -> dict:
        if expected is None:
            return {"ConditionExpression": "attribute_not_exists(#pk)", "ExpressionAttributeNames": {"#pk": "pk"}}
        return {
            "ConditionExpression": "#value = :expected",
            "ExpressionAttributeNames": {"#value": "value"},
            "ExpressionAttributeValues": {":expected": {"B": expected}},
        }

    @staticmethod
    def _error_code(error: Exception) -> str | None:
        return getattr(error, "response", {}).get("Error", {}).get("Code")

    @staticmethod
    def _validate_table(table: dict | None) -> None:
        if not table or table.get("KeySchema") != [{"AttributeName": "pk", "KeyType": "HASH"}] or not any(
            item.get("AttributeName") == "pk" and item.get("AttributeType") == "B"
            for item in table.get("AttributeDefinitions", [])
        ):
            raise StoreFailure("invalid_argument", "DynamoDB table must use one binary HASH key named pk")


__all__ = ["DynamoDbStore"]
