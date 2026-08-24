# `@crabbuild/prolly-store-dynamodb`

DynamoDB implementation of the shared async store protocol using the official AWS SDK. It borrows a configured `DynamoDBClient`; `close()` drains adapter operations but never destroys the client.

```ts
const store = new DynamoDbStore(client, { tableName: "prolly", keyPrefix: Buffer.from("prolly:") });
await store.initializeTable();
```

The primary table uses one binary HASH key named `pk` and a binary `value`
attribute. Named roots use the canonical companion table `${tableName}-roots`,
with binary HASH `pk` and RANGE `sk` keys; pass `rootTableName` to use an
explicitly provisioned name. `initializeTable()` creates and validates both
tables so the layout is interoperable with the Rust adapter.

Batch reads and writes are chunked to DynamoDB limits and retry unprocessed
items. Logical batch writes and node-plus-hint publication are intentionally
advertised as non-atomic. Strict commits use `TransactWriteItems` across the
primary and root tables and reject more than 100 physical transaction
operations before calling the SDK.
