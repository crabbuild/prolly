# Go PostgreSQL store

The PostgreSQL adapter uses `pgx/v5` and the same `BYTEA` schema as the Rust
`prolly-store-postgres` crate. Construct it from an existing `*pgxpool.Pool`,
run `InitializeSchema`, and pass it to `prolly.NewAsyncEngine`.

Root compare-and-swap and strict transactions lock `prolly_roots` inside one
database transaction. Nodes, hints, and named-root manifests remain opaque
bytes compatible with Rust.
