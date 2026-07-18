# prolly-java-store-mysql

Java entry points for the shared Kotlin/JDBC MySQL adapter.

```java
var store = MysqlStores.from(dataSource, boundedExecutor);
MysqlStores.initializeSchema(store).join();
```

The returned `MysqlStore` implements the shared JVM `RemoteStore` interface.
Schema initialization is a cancellation-propagating `CompletableFuture`. The
data source and executor are caller-owned and remain open after `store.close()`.
