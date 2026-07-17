# Java Cosmos DB store

Java entry points for the shared Kotlin Cosmos DB adapter. Create a store with
`CosmosDbStores.from(container, partitionKey, keyPrefix)`, then call
`CosmosDbStores.validateContainer(store)` before serving requests. The supplied
Azure `CosmosAsyncContainer` remains caller-owned.
