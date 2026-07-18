# Java SQLite remote store

`build.crab:prolly-java-store-sqlite` exposes the shared Kotlin/JDBC adapter to
Java without duplicating provider logic.

```java
var executor = Executors.newFixedThreadPool(16);
var store = SqliteStores.from(dataSource, executor);
SqliteStores.initializeSchema(store).join();
var engine = RemoteProlly.open(store, executor).join();
```

The application owns the `DataSource` and `Executor`. Closing the store or the
engine never closes either injected object. Schema and transaction behavior is
identical to the Kotlin and Rust SQLite implementations.
