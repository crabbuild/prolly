# prolly-store-redb

Pure-Rust [`redb`](https://crates.io/crates/redb) storage adapter for
[`prolly-map`](https://crates.io/crates/prolly-map). It provides a persistent,
synchronous, single-file backend with atomic write batches, named-root
compare-and-swap, deterministic scans, advisory hints, and strict transactions.
Large nodes are transparently LZ4-compressed, immutable publications are ordered
for redb's B-tree, and decoded nodes support retained shared reads.

## Requirements and installation

This adapter uses redb 4.1 and requires Rust 1.89 or newer. The higher toolchain
requirement is isolated to this crate; `prolly-map` itself continues to support
its lower minimum Rust version.

```toml
[dependencies]
prolly-map = "0.5.1"
prolly-store-redb = "0.3"
```

Redb is implemented in Rust and does not require a C or C++ toolchain.

## Quick start

```rust,no_run
use prolly::{Config, Prolly};
use prolly_store_redb::RedbStore;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = RedbStore::open("./data/app.prolly.redb")?;
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
    Ok(())
}
```

Dropping the map closes the database. Reopen the same file to load published
named roots in a later process.

## Configuration

`RedbStoreConfig` controls redb's page cache and transaction durability. The
defaults are a 1 GiB cache and `Durability::Immediate`, which guarantees that a
successful commit is persistent when it returns. Existing `RedbStoreConfig`
struct literals remain compatible with adapter 0.3.1.

```rust,no_run
use prolly_store_redb::{Durability, RedbStore, RedbStoreConfig};

let store = RedbStore::open_with_config(
    "./data/app.prolly.redb",
    RedbStoreConfig {
        cache_size_bytes: 128 * 1024 * 1024,
        durability: Durability::Immediate,
    },
)?;
drop(store);
Ok::<(), Box<dyn std::error::Error>>(())
```

`Durability::None` can improve throughput when loss of recent commits after a
crash is acceptable. It does not weaken transaction atomicity or consistency,
but such commits are not guaranteed to reach disk until a later immediate
commit.

Use `RedbStoreOptions` when tuning the decoded-node read cache separately from
redb's encoded page cache:

```rust,no_run
use prolly_store_redb::{Durability, RedbStore, RedbStoreConfig, RedbStoreOptions};

let store = RedbStore::open_with_options(
    "./data/app.prolly.redb",
    RedbStoreOptions {
        database: RedbStoreConfig {
            cache_size_bytes: 64 * 1024 * 1024,
            durability: Durability::Immediate,
        },
        node_read_cache_size_bytes: 128 * 1024 * 1024,
        compress_nodes: true,
    },
)?;
drop(store);
Ok::<(), Box<dyn std::error::Error>>(())
```

Set `node_read_cache_size_bytes` to zero to disable decoded-node retention.
The page-cache and node-cache limits are separate memory budgets. Compression
is enabled by default; update-heavy workloads can set `compress_nodes` to
`false` when write latency matters more than file size.

## Storage model

The adapter creates four typed tables inside one redb file:

- `prolly_nodes_v2` stores content-addressed nodes with an explicit raw or LZ4
  encoding. Nodes smaller than 8 KiB and nodes that do not compress smaller are
  stored raw.
- `prolly_nodes` is the legacy raw-node table retained for transparent reads of
  databases created by adapter 0.3.0. New and updated nodes move to the v2 table.
- `prolly_roots` stores encoded named-root manifests.
- `prolly_hints` stores advisory values by `(namespace, key)`.

Separate tables keep garbage-collection scans isolated from mutable metadata.
Back up the complete `.redb` file; named-root prefixes provide logical
organization, not physical tenant isolation.

## Transactions and concurrency

The adapter implements `Store`, `ManifestStore`, `ManifestStoreScan`,
`NodeStoreScan`, and `TransactionalStore`. Batches and node-plus-hint
publication commit atomically. Root compare-and-swap reads, validates, and
updates a manifest in one redb write transaction.

Strict transactions validate every named-root precondition before applying any
node or root write, then commit all changes together. A stale condition aborts
the transaction without applying staged writes. Redb supports concurrent
snapshot readers and serializes writers; reuse one `RedbStore` or an
`Arc<RedbStore>` across threads. Platforms with file locking reject a second
writable database handle for the same file.

The adapter persists hints but does not advertise a preference for rightmost-
path hints. That preference is workload-dependent and should be enabled only
after measurement.

## Compaction

Deleting or replacing data makes pages reusable but does not necessarily shrink
the file immediately. Call `RedbStore::compact(&mut self)` during an explicit
maintenance window to let redb relocate live pages and truncate reclaimable
space. Compaction preserves nodes, roots, and hints, requires exclusive mutable
access to the store, and may perform substantial I/O.

## Testing

Run the adapter's conformance, transaction, persistence, and concurrency tests:

```bash
cargo test --manifest-path stores/prolly-store-redb/Cargo.toml
```

See the [`prolly-map` API documentation](https://docs.rs/prolly-map) for map,
transaction, diff, merge, indexing, and garbage-collection APIs.
