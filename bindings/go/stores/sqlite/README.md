# Go SQLite store

This module implements the shared asynchronous Prolly remote-store protocol on
top of `database/sql` and `modernc.org/sqlite`.

```go
store, err := sqlite.Open("prolly.db", sqlite.Options{})
if err != nil {
    return err
}
defer store.Close()

if err := store.InitializeSchema(ctx); err != nil {
    return err
}
engine, err := prolly.NewAsyncEngine(ctx, store, nil)
```

The adapter uses the same `prolly_nodes`, `prolly_hints`, and `prolly_roots`
tables and opaque BLOB encodings as `prolly-store-sqlite` in Rust. Schema
version 1 supports ordered batch reads, atomic writes, scans, hints, root CAS,
and strict transactions.
