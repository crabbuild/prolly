# Runnable integration examples

Run these commands from `integrations/prolly-gluesql`.

| Example | Command | Demonstrates |
| --- | --- | --- |
| [`basic_sql.rs`](basic_sql.rs) | `cargo run --example basic_sql` | In-memory setup, schema, foreign keys, transactions, rollback, a covering index, a custom function, and typed GlueSQL payloads. |
| [`concurrent_writers.rs`](concurrent_writers.rs) | `cargo run --example concurrent_writers` | A shared custom Prolly backend, two GlueSQL connections, optimistic transactions, and a serialization conflict that preserves the winning commit. |
| [`commit_graph.rs`](commit_graph.rs) | `cargo run --example commit_graph` | Git-like commits, authors and metadata, named refs, ancestry and merge-base queries, explicit merge parents, and branches created from refs, branches, commits, or version IDs. |
| [`versions_and_branches.rs`](versions_and_branches.rs) | `cargo run --example versions_and_branches` | Opaque versions, typed diffs, durable branches, branch isolation, historical checkout, and CAS reset. |
| [`merge_clean.rs`](merge_clean.rs) | `cargo run --example merge_clean` | An explicit-base three-way merge, structural combination of disjoint changes, rebuilt indexes, and the returned logical diff. |
| [`merge_conflicts.rs`](merge_conflicts.rs) | `cargo run --example merge_conflicts` | Decoded row, schema, function, unique-column, and foreign-key conflicts without changing the target branch. |
| [`redb_durable.rs`](redb_durable.rs) | `cargo run --features redb --example redb_durable` | A self-cleaning pure-Rust redb store, shared GlueSQL connections, durable versions and branches, and reopening both branch heads. |
| [`sqlite_durable.rs`](sqlite_durable.rs) | `cargo run --features sqlite --example sqlite_durable` | A self-cleaning temporary SQLite store, durable versions and branches, and reopening both branch heads. |

Every example is self-contained: it creates its own database state, validates
the result with assertions, and prints the important typed output. No external
database server is required.
