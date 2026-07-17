# Kotlin SQLite remote store

`build.crab:prolly-kotlin-store-sqlite` implements store protocol version 1
with an application-owned JDBC `DataSource`.

```kotlin
val dataSource = SQLiteDataSource().apply { url = "jdbc:sqlite:prolly.db" }
val store = SqliteStore(dataSource)
store.initializeSchema()
val engine = RemoteProlly.open(store)
```

Schema initialization is explicit and idempotent. The adapter uses the exact
three `WITHOUT ROWID` tables used by the Rust SQLite store. JDBC calls run with
`runInterruptible` on `Dispatchers.IO.limitedParallelism(16)` by default; pass
an application dispatcher when a different bound is required. Immediate
transactions protect batch writes, node-plus-hint writes, root CAS, and strict
commits.

Closing the adapter cancels its Java initialization scope. It never closes the
injected data source or dispatcher.
