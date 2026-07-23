# prolly-store-rocksdb

RocksDB storage adapter for [`prolly-map`](https://crates.io/crates/prolly-map).
It provides a persistent, synchronous, embedded store with atomic write batches
and configurable compression and block caching.

## When to use it

Use this adapter for native applications that need an embedded store optimized
for sustained key-value workloads and do not need a separate database service.
Choose SQLite when a single portable database file and SQL tooling are more
important. Choose a remote adapter when multiple hosts must share the store.

## Installation

```toml
[dependencies]
prolly-map = "0.5.1"
prolly-store-rocksdb = "0.3.0"
```

The `rocksdb` dependency builds native code. Development and CI environments
need a working C/C++ toolchain and the platform build tools required by
`librocksdb-sys`.

## Quick start

```rust,no_run
use prolly::{Config, Prolly};
use prolly_store_rocksdb::RocksDBStore;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = RocksDBStore::open("./data/app.prolly.rocksdb")?;
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

Dropping the map closes RocksDB. Reopen the same path to load a published named
root in a later process.

## Configuration

`RocksDBConfig` controls database creation, compression, block-cache size, and
statistics collection:

```rust,no_run
use prolly_store_rocksdb::{CompressionType, RocksDBConfig, RocksDBStore};

let store = RocksDBStore::open_with_config(
    "./data/app.prolly.rocksdb",
    RocksDBConfig {
        compression: CompressionType::Zstd,
        cache_size: 128 * 1024 * 1024,
        enable_statistics: true,
        ..RocksDBConfig::default()
    },
)?;
Ok::<(), Box<dyn std::error::Error>>(())
```

The default configuration creates missing databases, uses LZ4 compression, and
allocates a 64 MiB block cache. Other compression choices are `None`, `Snappy`,
`Zlib`, `Lz4hc`, and `Zstd`.

## Storage model

Content-addressed nodes live in RocksDB's default column family. Named root
manifests live in the `prolly_roots` column family. Use a separate database path
for physical tenant or environment isolation; named-root prefixes only provide
logical organization inside a database.

## Transactions and named roots

The adapter implements `Store`, `ManifestStore`, scanning, and
`TransactionalStore`. A commit validates named-root preconditions while holding
the manifest lock, then writes node and root changes in one RocksDB `WriteBatch`.
A stale precondition returns a conflict without applying the batch.

Named roots are durable handles to immutable tree versions. Removing a root
does not immediately remove unreachable content-addressed nodes, so garbage
collection requires an application-level retention decision.

## Operational notes

- Reuse a store instance; RocksDB controls exclusive access to its database path.
- `RocksDBStore` is `Send + Sync` and supports atomic batch operations.
- Tune the block cache and compression for the workload and available memory.
- Back up or checkpoint the full RocksDB directory, including every column family.

## Testing

The integration suite creates isolated temporary databases and exercises the
store, scan, named-root, transaction, and indexed-map contracts:

```bash
cargo test --manifest-path stores/prolly-store-rocksdb/Cargo.toml
```

See the [`prolly-map` API documentation](https://docs.rs/prolly-map) for map,
transaction, diff, and merge operations available with this store.
