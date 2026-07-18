# Python Cosmos DB store

This package adapts a caller-owned asynchronous Azure Cosmos DB
`ContainerProxy` to the shared store protocol. The container must use `/kind`
as its only partition-key path. All documents for an adapter instance share one
partition, allowing conditional root updates and protocol transactions to use
Cosmos DB transactional batches. Closing the adapter does not close the
container or its owning client.
