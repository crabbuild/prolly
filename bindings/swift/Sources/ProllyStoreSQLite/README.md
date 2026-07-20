# Swift SQLite store

`ProllyStoreSQLite` borrows an open SQLite handle, implements async store
protocol major 2 as an actor, and never closes the caller-owned database. Apply
`initializeSchema()` explicitly before opening the remote engine.
