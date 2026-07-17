# `@trail/prolly-store-dynamodb`

DynamoDB implementation of the shared async store protocol using the official AWS SDK. It borrows a configured `DynamoDBClient`; `close()` drains adapter operations but never destroys the client.

```ts
const store = new DynamoDbStore(client, { tableName: "prolly", keyPrefix: Buffer.from("prolly:") });
await store.initializeTable();
```

The table uses one binary HASH key named `pk` and a binary `value` attribute. Batch reads and writes are chunked to DynamoDB limits and retry unprocessed items. Logical batch writes and node-plus-hint publication are intentionally advertised as non-atomic. Strict commits use `TransactWriteItems` and reject more than 100 physical transaction operations before calling the SDK.
