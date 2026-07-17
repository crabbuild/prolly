# Kotlin Cosmos DB store

Cosmos DB implementation of the shared async store protocol using the official
Azure Cosmos DB SDK. Pass a caller-owned `CosmosAsyncContainer` configured with
the `/kind` partition key; closing the store never closes the container or its
client.

Call `validateContainer()` before serving requests to verify the partition-key
contract. All documents use one configured logical partition so ETag-based root
CAS and transactional batches remain atomic.
