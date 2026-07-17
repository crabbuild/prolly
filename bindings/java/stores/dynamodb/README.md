# Java DynamoDB store

Java factory for the shared AWS SDK asynchronous DynamoDB adapter:

```java
var store = DynamoDbStores.from(client, "prolly", "prolly:".getBytes());
DynamoDbStores.initializeTable(store).join();
```

The adapter borrows the client and never closes it. It uses the Rust-compatible binary `pk`/`value` table, chunks batch requests to DynamoDB limits, and uses conditional writes and transactions for root CAS and strict commits.
