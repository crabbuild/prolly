# prolly-java-store-postgres

Java entry points for the shared Kotlin/JDBC PostgreSQL store.

```java
var store = PostgresStores.from(dataSource, boundedExecutor);
PostgresStores.initializeSchema(store).join();
```

`PostgresStores.from` returns the Kotlin `PostgresStore`, which directly
implements the shared JVM `RemoteStore` interface. Initialization returns a
cancellation-propagating `CompletableFuture`. The data source and executor remain
owned by the caller and are never closed by the adapter.
