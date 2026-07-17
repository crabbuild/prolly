# prolly-kotlin-store-mysql

Kotlin/JDBC MySQL implementation of the Prolly version 1 asynchronous store
protocol. Inject a caller-owned `DataSource` plus an optional bounded
`CoroutineDispatcher` or `Executor`, then call `initializeSchema()`.

JDBC work runs with `runInterruptible`. The adapter never closes the data source,
dispatcher, or executor. It validates `VARBINARY(32)` CIDs and 255-byte hint/root
keys before obtaining a connection, preserves unsigned byte ordering, and uses a
single query for ordered batch reads.

All compound writes use InnoDB transactions. Deadlock and lock-timeout victims
are retried as complete transactions, preserving atomicity for CAS, strict
commits, batches, and node-plus-hint writes.
