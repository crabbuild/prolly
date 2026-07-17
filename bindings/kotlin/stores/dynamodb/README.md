# Kotlin DynamoDB store

This module implements the shared async store protocol over a caller-owned AWS SDK `DynamoDbAsyncClient`.

```kotlin
val store = DynamoDbStore(client, DynamoDbStoreOptions(tableName = "prolly"))
store.initializeTable()
```

The single-table schema uses a binary HASH key named `pk` and binary `value` payloads. Reads and writes are chunked to AWS limits with bounded unprocessed-item retries. Logical batch writes and node-plus-hint publication are non-atomic; strict commits use `TransactWriteItems` with a locally enforced 100-operation limit. Closing the adapter never closes the AWS client.
