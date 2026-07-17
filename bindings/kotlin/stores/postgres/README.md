# prolly-kotlin-store-postgres

Kotlin/JDBC PostgreSQL implementation of the Prolly version 1 asynchronous store
protocol. Construct `PostgresStore` with a caller-owned `DataSource` and,
optionally, a bounded `CoroutineDispatcher` or `Executor`, then call
`initializeSchema()`.

Every JDBC operation runs with `runInterruptible` on the configured dispatcher.
Closing the adapter cancels its Java initialization scope but does not close the
data source, dispatcher, or executor. Applications retain ownership of all three.

The adapter uses the exact three-table `BYTEA` schema from
`prolly-store-postgres`. Ordered batch reads issue one `ANY(bytea[])` query and
restore duplicates and missing positions in memory. Atomic operations use JDBC
transactions; root CAS and strict commits combine `SELECT ... FOR UPDATE` with a
per-root transaction advisory lock so absent-root races have exactly one winner.
