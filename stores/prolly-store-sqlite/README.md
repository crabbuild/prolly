# prolly-store-sqlite

SQLite storage adapter for [`prolly-map`](https://crates.io/crates/prolly-map).
It stores content-addressed nodes, traversal hints, and named roots in one
portable SQLite database.

## When to use it

Use this adapter for desktop software, CLIs, local services, durable tests, and
single-host applications that value a self-contained database file and SQLite
tooling. Choose RocksDB for an embedded key-value engine tuned for sustained
write workloads. Choose a remote adapter when multiple hosts must share the
same store.

## Installation

```toml
[dependencies]
prolly-map = "0.4"
prolly-store-sqlite = "0.3.0"
```

The crate enables `rusqlite`'s bundled SQLite build, so a system SQLite library
is not required.

## Quick start

```rust
use prolly::{Config, Prolly};
use prolly_store_sqlite::SqliteStore;

let store = SqliteStore::open_in_memory()?;
let prolly = Prolly::new(store, Config::default());

let tree = prolly.put(
    &prolly.create(),
    b"project/name".to_vec(),
    b"CrabDB".to_vec(),
)?;
prolly.publish_named_root(b"main", &tree)?;

let loaded = prolly.load_named_root(b"main")?.expect("main root");
assert_eq!(
    prolly.get(&loaded, b"project/name")?,
    Some(b"CrabDB".to_vec())
);
# Ok::<(), Box<dyn std::error::Error>>(())
```

Use `SqliteStore::open("./data/app.prolly.sqlite")` for persistence. Reopen the
same file in a later process and load a published named root to recover the
tree handle.

## Schema and configuration

`open`, `open_with_config`, and `open_in_memory` create these tables if needed:

- `prolly_nodes(cid, node)` stores nodes by 32-byte CID.
- `prolly_hints(namespace, key, value)` stores optional traversal hints.
- `prolly_roots(name, manifest)` stores named root manifests.

`SqliteStoreConfig` controls the busy timeout, WAL journaling, and SQLite's
`synchronous=NORMAL` setting. Defaults are a 5-second busy timeout, WAL for
file-backed databases, and `synchronous=NORMAL`.

```rust,no_run
use prolly_store_sqlite::{SqliteStore, SqliteStoreConfig};

let store = SqliteStore::open_with_config(
    "./data/app.prolly.sqlite",
    SqliteStoreConfig {
        busy_timeout_ms: 10_000,
        enable_wal: true,
        synchronous_normal: false,
    },
)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Use a separate database file for physical tenant or environment isolation.
Named-root prefixes such as `tenant/42/main` organize roots logically, while
content-addressed nodes may be shared by multiple roots in the file.

## Opening existing databases safely

On Unix, `SqliteStore::open_existing` opens an existing database without
creating the file or running schema DDL. The caller is responsible for
validating that the required tables exist.

On Unix, `open_existing_verified` exposes the device, inode, and length of
SQLite's actual open main-database file descriptor to a verifier before any
pragma or SQL statement runs. It is useful when a caller must prove that SQLite
opened an expected file. Both existing-only entry points fail closed on
non-Unix platforms.

## Transactions and named roots

The adapter implements `Store`, `ManifestStore`, scanning, and
`TransactionalStore`. SQLite transactions atomically validate named-root
preconditions and apply node and root writes. A stale precondition returns a
conflict without committing staged changes.

`SqliteStore` owns one connection behind a mutex, so calls through one store are
thread-safe and serialized. WAL and the busy timeout help when multiple SQLite
connections contend for a file; application-level concurrency should still be
measured under the intended workload.

Deleting a named root does not immediately delete unreachable nodes. Plan
retention and garbage collection before pruning content-addressed data.

## Durable semantic RAG example

[`examples/semantic_rag.rs`](https://github.com/crabbuild/prolly/blob/main/stores/prolly-store-sqlite/examples/semantic_rag.rs)
builds a native `ProximityMap` over six support-documentation chunks, publishes
its descriptor as the SQLite named root `rag/corpus/main`, and performs exact
cosine retrieval. Run it twice against the same database to see initial
construction followed by a process-independent reopen:

```bash
rm -f ./target/semantic-rag.sqlite*

cargo run --manifest-path stores/prolly-store-sqlite/Cargo.toml \
  --example semantic_rag -- \
  ./target/semantic-rag.sqlite password-reset

cargo run --manifest-path stores/prolly-store-sqlite/Cargo.toml \
  --example semantic_rag -- \
  ./target/semantic-rag.sqlite lost-2fa
```

The example prints three ranked chunks with stable keys and source citations,
then renders an offline `<context>` block ready to pass to an LLM. It does not
call an embedding or generation service.

The checked-in fixture uses synthetic, precomputed, normalized vectors with
1,536 dimensions. Replace `embedding_for_query` and corpus ingestion with the
embedding provider used by your application; keep the model identifier and
dimensions in named-root metadata so incompatible indexes fail closed on
reopen.

## Testing

The default suite is credential-free and exercises in-memory and file-backed
storage, reopen behavior, transactions, scanning, and indexed maps:

```bash
cargo test --manifest-path stores/prolly-store-sqlite/Cargo.toml
```

See the [`prolly-map` API documentation](https://docs.rs/prolly-map) for map,
transaction, diff, merge, and proximity-map operations available with this
store.
