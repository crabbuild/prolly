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
  typed three-way merges, and compare-and-swap resets.
- Durable Git-like commits, authors, extensible metadata, named refs, ancestry,
  merge-base discovery, and retained commit snapshots.
- In-memory storage by default, durable pure-Rust redb storage behind the
  `redb` feature, and durable SQLite storage behind the `sqlite` feature.
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

Enable `redb` for a process-durable, pure-Rust single-file database:

```toml
prolly-gluesql = { version = "0.1", features = ["redb"] }
```

```rust,no_run
use prolly_gluesql::{Glue, RedbProllyStorage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = RedbProllyStorage::open_redb("database.prolly.redb")?;
    let mut db = Glue::new(storage);
    let payloads = db.execute("SELECT * FROM users;").await?;
    println!("{payloads:#?}");
    Ok(())
}
```

Redb permits one writable database handle per file. For multiple GlueSQL
connections, open one `prolly_store_redb::RedbStore`, wrap it in `Arc`, and
pass clones to `RedbProllyStorage::new`. This preserves independent SQL
transaction state while sharing redb's database handle. See the
[`redb_durable`](examples/redb_durable.rs) example for the complete pattern.
Applications using this shared-handle pattern should also add
`prolly-store-redb = "0.5"` as a direct dependency.

Any custom backend implementing both `prolly::Store` and
`prolly::ManifestStore` can be passed to `ProllyStorage::new`.

## Runnable examples

The [`examples`](examples/README.md) directory contains complete programs. Run
them from this crate directory:

```sh
cargo run --example basic_sql
cargo run --example concurrent_writers
cargo run --example commit_graph
cargo run --example versions_and_branches
cargo run --example merge_clean
cargo run --example merge_conflicts
cargo run --features redb --example redb_durable
cargo run --features sqlite --example sqlite_durable
```

Each example creates its own state, checks its expected results, and needs no
external service or input. They cover SQL execution, transactions, indexes,
custom functions, shared custom stores, optimistic writer conflicts, typed
diffs, historical reads, commit graphs, flexible branch sources, branch
isolation, clean merges, every typed conflict category, constraint conflicts,
CAS publication, and durable SQLite reopen behavior. The redb example
additionally demonstrates sharing one backend between connections and
reopening durable branch heads.

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

`Version` is an opaque, lightweight state handle with a printable `VersionId`;
it does not expose the underlying Prolly tree and is not itself a durable pin.
Keep important states reachable through a branch or graph commit before running
store garbage collection. The adapter intentionally does not invent SQL syntax
for branch operations.

## Commit graph and refs

SQL `COMMIT` atomically publishes database state. A graph commit is an explicit
history checkpoint created afterward with `storage.commit`; this separation
lets applications choose which transactional states deserve messages,
authorship, review metadata, tags, or long-term retention.

```rust,ignore
use prolly_gluesql::{CommitActor, CommitOptions, DatabaseRef};

let base = db.storage.commit_with(
    CommitOptions::new("initialize users")
        .author(CommitActor::named("Ada"))
        .metadata(b"request-id", b"req-42"),
)?;

db.storage.create_branch_from(
    "feature",
    &DatabaseRef::Commit(base.id.clone()),
)?;

// Generic refs support tags, checkpoints, or application-defined namespaces.
db.storage.create_ref("refs/tags/v1", &base.id)?;
db.storage.create_branch_from(
    "release-fix",
    &DatabaseRef::Ref("refs/tags/v1".to_owned()),
)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`create_branch_from` accepts `DatabaseRef::Branch`, `Ref`, `Commit`, or
`Version`. `CommitId` and `VersionId` implement `FromStr`, so persisted IDs can
be parsed and used without exposing trees or raw storage records. Creating a
branch from a commit or commit-backed ref also initializes its history parent.

Commits contain a retained `Version`, ordered parent IDs, optional author and
committer identities, a message, timestamp, generation, and byte-oriented
metadata. Explicit parents support merge commits:

```rust,ignore
let merged = db.storage.commit_with(
    CommitOptions::new("merge feature")
        .parents([main.id.clone(), feature.id.clone()]),
)?;

let log = db.storage.log(&merged.id, 100)?;
let common = db.storage.merge_base(&main.id, &feature.id)?;
assert!(db.storage.is_ancestor(&main.id, &merged.id)?);
let changes = db.storage.diff_commits(&main.id, &merged.id)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Named refs use compare-and-swap through `compare_and_swap_ref`; failed updates
return `RefUpdate::Conflict` with the current typed ref. Commit records and refs
live in a private Prolly metadata tree, while each commit publishes a private
retention root for its SQL snapshot. Graph records therefore never appear in
SQL diffs, and durable backends can reopen old commits after branch heads move.

## Merging branches

`merge` performs a typed three-way merge. The selected branch is the current
side, `incoming` is the version being merged, and `base` is their common
ancestor. The merge primitive remains explicit and also works without history;
applications using graph commits can obtain `base` with `merge_base`.

```rust,ignore
use prolly_gluesql::{MergeConflict, MergeResult};

let base = db.storage.head()?.unwrap();
db.storage.create_branch("feature")?;

// Make changes on main and feature, then retain the feature head.
db.storage.checkout_branch("feature")?;
db.execute("UPDATE users SET name = 'Feature' WHERE id = 1;")
    .await?;
let incoming = db.storage.head()?.unwrap();
db.storage.checkout_branch("main")?;

match db.storage.merge(&base, &incoming).await? {
    MergeResult::Applied { version, changes } => {
        println!("published {} logical changes at {:?}", changes.len(), version.id());
    }
    MergeResult::Conflicted { conflicts } => {
        for conflict in conflicts {
            match conflict {
                MergeConflict::Row { table, key, base, current, incoming } => {
                    // All values are decoded GlueSQL records; no raw Prolly bytes leak.
                }
                MergeConflict::Schema { .. }
                | MergeConflict::Function { .. }
                | MergeConflict::Constraint { .. } => { /* present for resolution */ }
            }
        }
    }
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

Clean changes are combined with Prolly's structural prefix merge. The adapter
then validates row shape, types, nullability, primary keys, unique columns, and
foreign keys. Covering indexes, generated-key sequences, and metadata are
rebuilt internally before the selected branch is moved with compare-and-swap.
Conflicts and validation failures leave the branch unchanged; a concurrent
head update returns a serialization conflict.

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
Every persisted SQL value starts with the `PGSQ` magic bytes and a wire-format
version; commit graph records use the separate `PGHG` envelope. Unknown
versions fail explicitly rather than being silently misread.

The current crate release is pre-1.0. Its wire format is explicit, but backward
compatibility is not promised until a stable release. Back up a durable store
before upgrading between pre-1.0 releases.

## Scope

SQL parsing, execution semantics, and supported SQL syntax come from GlueSQL
0.19. The history layer provides commits, refs, ancestry, merge bases, and
retention, but intentionally leaves reflogs, signatures, authorization,
automatic merge policies, and remote synchronization to applications. Its byte
metadata fields provide an extension point for those concerns without changing
the core graph format. Every commit currently retains its SQL snapshot; a
future pruning API will be needed for applications that want bounded history.

The adapter currently targets synchronous Prolly stores. Remote
`AsyncStore`/`AsyncManifestStore` backends would require a separate async
adapter because GlueSQL's present storage traits and Prolly's async backends
have different ownership and execution models.

## Verification

The integration runs GlueSQL's published storage conformance suite plus tests
for rollback, branch isolation, historical reads, reset, typed merge conflicts,
commit graph traversal, merge-base discovery, flexible branch creation, graph
durability, merge constraint validation, derived-state rebuilding, redb and
SQLite reopen, secondary-index durability, custom-function durability, durable
branch isolation, and concurrent-writer conflicts:

```sh
cargo test --all-features --all-targets
cargo clippy --all-features --all-targets -- -D warnings
```
