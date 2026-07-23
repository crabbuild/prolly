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
prolly-map = "0.5.1"
prolly-store-sqlite = "0.3.1"
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

- `prolly_nodes(cid, encoding, node)` stores nodes by 32-byte CID, using raw or
  LZ4-encoded payloads.
- `prolly_hints(namespace, key, value)` stores optional traversal hints.
- `prolly_roots(name, manifest)` stores named root manifests.

`SqliteStoreConfig` controls the busy timeout, WAL journaling, SQLite's
`synchronous=NORMAL` setting, database page size, page cache, WAL checkpoint
interval, the decoded-node read cache, the minimum node size considered for
compression, the maximum memory-mapped read window, the read-connection pool,
adaptive multi-key reads, background checkpoints, and optional group commit.
Defaults are a 5-second busy timeout, WAL for file-backed databases,
`synchronous=NORMAL`, 64 KiB database pages, a 64 MiB page cache, a
128 MiB sharded decoded-node cache, a 256 MiB mapping, four thread-affine read
connections, and a dedicated passive checkpoint worker. The worker begins
checkpointing after 64 MiB of WAL allocation and caps retained WAL
allocation at 256 MiB. Nodes of at least 8 KiB are compressed when LZ4 makes
them smaller; typical compact nodes remain raw to avoid spending more CPU than
the write saves. Group commit remains disabled unless
`group_commit_delay_micros` is set.

```rust,no_run
use prolly_store_sqlite::{SqliteStore, SqliteStoreConfig};

let store = SqliteStore::open_with_config(
    "./data/app.prolly.sqlite",
    SqliteStoreConfig {
        busy_timeout_ms: 10_000,
        enable_wal: true,
        synchronous_normal: false,
        node_compression_min_bytes: 8 * 1024,
        reader_connections: 4,
        checkpoint_wal_bytes: 64 * 1024 * 1024,
        ..SqliteStoreConfig::default()
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

File-backed stores use one serialized writer and a pool of read-only
connections. By default, the first reading thread reuses the writer connection
to preserve SQLite's single-thread page and statement cache fast path. Other
calling threads remain affine to one reader, allowing independent threads to
read in parallel with WAL publication. Set `primary_reader_uses_writer` to
`false` when every application thread should use the read-only pool. In-memory
and caller-supplied connections retain the single-connection behavior.

Set `group_commit_delay_micros` to a small non-zero window when many threads
publish independent immutable-node batches. Concurrent publications collected
inside that window share one durable SQLite transaction; every caller returns
only after that transaction commits. Root compare-and-swap transactions remain
strictly serialized and are never merged implicitly.

## Operations and maintenance

`metrics()` reports decoded-cache activity, publication and transaction counts,
compression savings, checkpoint activity, and SQLite pager hit, miss, write,
and spill counters. `storage_stats()` reports page, freelist, database, and WAL
frame sizes.

Use `checkpoint` for an explicit passive, full, restart, or truncate checkpoint.
The default background worker keeps checkpoint synchronization out of the
writer's commit path. `quick_check` provides a fast structural health check,
`backup_to` creates a consistent online backup, `optimize` runs bounded SQLite
planner maintenance, and `compact` runs an explicit blocking `VACUUM` after the
application has quiesced traffic.

Tree reachability and garbage collection are provided by `prolly-map`:
`plan_store_gc_for_retention` performs the dry run and
`sweep_store_gc_for_retention` deletes unreachable nodes through one SQLite
batch transaction. Inspect `storage_stats().free_bytes` after sweeping and run
`compact` during an appropriate maintenance window when filesystem reclamation
is needed.

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
