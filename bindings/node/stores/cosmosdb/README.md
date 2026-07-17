# `@trail/prolly-store-cosmosdb`

Cosmos DB implementation of the shared async store protocol using the official `@azure/cosmos` SDK. Construct it with a caller-owned `Container`; the adapter never closes the surrounding client.

The container must use `/kind` as its partition key. Call `validateContainer()` during startup. Every adapter instance uses one `kind` value so strict commits can execute as one transactional batch. Documents match the Rust/Go layout: `id`, `kind`, `family`, hex `key`, and base64 `value`. Root CAS uses create-if-absent and ETag `IfMatch` operations. Transactions above 100 physical operations are rejected locally.
