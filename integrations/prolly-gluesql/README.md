# prolly-gluesql

`prolly-gluesql` is a transactional, versioned
[GlueSQL](https://github.com/gluesql/gluesql) storage engine backed by
[`prolly-map`](https://crates.io/crates/prolly-map). It integrates through
GlueSQL's public storage traits and does not patch or fork GlueSQL.

A complete logical SQL database—catalog, rows, sequences, metadata, custom
functions, and covering secondary indexes—is one immutable Prolly tree.
Successful transactions atomically advance a named branch root. This gives
readers stable snapshots, structural sharing between versions, inexpensive
logical diffs, and optimistic conflict detection for concurrent writers.

## Capabilities

- GlueSQL schema, row, mutation, transaction, alter-table, metadata, custom
  function, index, and planner traits.
- Schemaful and schemaless GlueSQL rows.
- Persistent covering secondary indexes with bounded Prolly range scans.
- Explicit transactions and GlueSQL autocommit with atomic catalog-and-data
  publication.
- Named branches, immutable version handles, historical reads, logical diffs,
  and compare-and-swap resets.
- In-memory storage by default and durable SQLite storage behind the `sqlite`
  feature.
- An optional `prolly-sql` command-line client behind the `cli` feature.
- A versioned record envelope that detects incompatible or corrupt data.

## Library use

Add the crate and an async runtime to your application:

```toml
[dependencies]
prolly-gluesql = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The in-memory engine is useful for tests and ephemeral databases:

```rust
use prolly_gluesql::{Glue, ProllyStorage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = ProllyStorage::in_memory()?;
    let mut db = Glue::new(storage);

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
        .await?;
    db.execute("INSERT INTO users VALUES (1, 'Ada');").await?;
    let payloads = db.execute("SELECT * FROM users;").await?;
    println!("{payloads:#?}");
    Ok(())
}
```

Enable `sqlite` for a process-durable database:

```toml
prolly-gluesql = { version = "0.1", features = ["sqlite"] }
```

```rust,no_run
use prolly_gluesql::{Glue, SqliteProllyStorage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = SqliteProllyStorage::open_sqlite("database.prolly.sqlite")?;
    let mut db = Glue::new(storage);
    let payloads = db.execute("SELECT * FROM users;").await?;
    println!("{payloads:#?}");
    Ok(())
}
```

Any custom backend implementing both `prolly::Store` and
`prolly::ManifestStore` can be passed to `ProllyStorage::new`.

## Versions and branches

`head` returns an immutable handle to the selected branch state. A branch is a
durable retention root; creating one also keeps all nodes reachable from that
version alive during Prolly garbage collection.

```rust,ignore
let before = db.storage.head()?.expect("the branch has been published");
db.execute("UPDATE users SET name = 'Ada Lovelace' WHERE id = 1;")
    .await?;
let after = db.storage.head()?.unwrap();

let changes = db.storage.diff(&before, &after)?;
db.storage.create_branch("experiment")?;
db.storage.checkout_branch("experiment")?;

// Historical checkout opens an explicit transaction pinned to `before`.
db.storage.checkout(&before)?;
let old_rows = db.execute("SELECT * FROM users;").await?;
db.execute("ROLLBACK;").await?;

// Move the selected branch only if no concurrent writer has advanced it.
db.storage.reset(&before)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`diff` returns decoded SQL changes grouped into `schemas`, `rows`, and
`functions`. Row entries contain the table name, primary key, and complete
before/after `DataRow` values. Physical Prolly keys and values, covering-index
entries, sequences, and metadata bookkeeping are deliberately not exposed.

```rust,ignore
use prolly_gluesql::RowChange;

for change in changes.rows {
    match change {
        RowChange::Added { table, key, row } => { /* ... */ }
        RowChange::Removed { table, key, row } => { /* ... */ }
        RowChange::Modified { table, key, before, after } => { /* ... */ }
    }
}
```

`DatabaseVersion` itself is a lightweight tree handle, not a durable pin. Keep
important states reachable through a branch before running store garbage
collection. The adapter intentionally exposes storage-level versions and does
not invent SQL syntax for branch operations.

## Transaction and concurrency model

At `BEGIN`, the connection resolves the selected branch once. Mutations build
a private immutable candidate tree, so reads in that transaction see their own
writes while other connections continue to see the published tree. `COMMIT`
uses an atomic compare-and-swap from the original root to the candidate root.
If another writer won first, commit returns a serialization conflict and does
not overwrite that writer. Applications may retry the complete transaction.

Catalog changes, data changes, index maintenance, metadata, and custom
functions share the same root and therefore commit atomically. Rolled-back and
losing candidate nodes remain unreachable and can be reclaimed by the
underlying store's garbage collector.

Most reads resolve the branch head for each GlueSQL statement. Custom functions
are the exception because GlueSQL 0.19 returns them by borrowed reference;
they are cached per connection. Call `storage.refresh()` after another
connection creates or drops a function. Opening a connection, switching a
branch, and resetting a branch refresh the cache automatically.

## Command-line client

Build or run the CLI with the `cli` feature:

```sh
cargo run --features cli --bin prolly-sql -- \
  --database app.sqlite execute \
  "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);"

cargo run --features cli --bin prolly-sql -- \
  --database app.sqlite head

cargo run --features cli --bin prolly-sql -- \
  --database app.sqlite branch experiment

cargo run --features cli --bin prolly-sql -- \
  --database app.sqlite --branch experiment shell
```

The shell supports `.head`, `.branch NAME`, `.help`, and `.quit`. SQL payloads
are emitted as JSON.

## Storage format and compatibility

Keys live under a private, versioned namespace. Identifiers use length-prefixed
segments, while primary keys and indexed expressions use order-preserving byte
encodings.
Every persisted value starts with the `PGSQ` magic bytes and a wire-format
version before its serialized payload. Unknown versions fail explicitly rather
than being silently misread.

The current crate release is pre-1.0. Its wire format is explicit, but backward
compatibility is not promised until a stable release. Back up a durable store
before upgrading between pre-1.0 releases.

## Scope

SQL parsing, execution semantics, and supported SQL syntax come from GlueSQL
0.19. This adapter provides versioned storage rather than a Git-like commit
graph: it has branch heads and immutable roots, but no author metadata,
reflogs, semantic SQL merge, or automatic history retention. Those can be
layered above `DatabaseVersion`, Prolly diffs, and named roots when an
application needs them.

The adapter currently targets synchronous Prolly stores. Remote
`AsyncStore`/`AsyncManifestStore` backends would require a separate async
adapter because GlueSQL's present storage traits and Prolly's async backends
have different ownership and execution models.

## Verification

The integration runs GlueSQL's published storage conformance suite plus tests
for rollback, branch isolation, historical reads, reset, SQLite reopen,
secondary-index durability, custom-function durability, and concurrent-writer
conflicts:

```sh
cargo test --all-features --all-targets
cargo clippy --all-features --all-targets -- -D warnings
```
