# DynamoDB store for Go

This module implements the shared asynchronous store protocol with AWS SDK for
Go v2. It uses the same schema-version-1 layout as the Rust adapter: one binary
`pk` HASH key, a binary `value`, and `prolly:node:`, `prolly:root:`, and
`prolly:hint:` key families.

`BatchGetItem` requests are split at 100 keys, non-atomic `BatchWriteItem`
requests at 25 writes, and strict transactions are rejected above 100 items.
Unprocessed batches are retried with bounded, context-aware backoff. The
combined nodes-and-hint operation is deliberately not advertised as atomic.

Run the live conformance suite against DynamoDB Local with:

```sh
PROLLY_DYNAMODB_ENDPOINT=http://127.0.0.1:8000 go test -race ./...
```
