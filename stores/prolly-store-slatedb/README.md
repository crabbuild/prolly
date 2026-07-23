# prolly-store-slatedb

SlateDB storage adapter for [`prolly-map`](https://crates.io/crates/prolly-map).
It accepts a SlateDB-compatible object store while exposing the synchronous
`Store` contract used by `Prolly`.

## When to use it

Use this adapter when Prolly data should live in an object store and SlateDB's
LSM architecture matches the deployment. It is useful for durable object-store
backends, local object-store emulators, and tests that use SlateDB's in-memory
implementation. Choose SQLite or RocksDB for a simpler local embedded database,
or a remote async adapter when the application already operates a database
service.

## Installation

The quick-start example constructs an object store directly, so add SlateDB as
a direct dependency as well as the adapter:

```toml
[dependencies]
prolly-map = "0.5.1"
prolly-store-slatedb = "0.3.0"
slatedb = "0.14"
```

## Quick start

```rust,no_run
use std::sync::Arc;

use prolly::{Config, Prolly};
use prolly_store_slatedb::SlateDbStore;
use slatedb::object_store::{memory::InMemory, ObjectStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = SlateDbStore::open("my-app", object_store)?;
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

The in-memory object store is ephemeral. For persistence, pass an
`Arc<dyn ObjectStore>` configured for the filesystem or cloud object store used
by the application. The SlateDB path is the database namespace within that
object store.

## Configuration

`SlateDbStoreConfig` exposes:

- `settings`: SlateDB engine settings.
- `flush_after_write`: flush to object storage after every adapter write;
  defaults to `true`.
- `close_on_drop`: close SlateDB when the adapter is dropped; defaults to
  `true`.
- `read_parallelism`: maximum concurrent reads in a batch; defaults to `64` and
  is clamped to at least one.

Call `SlateDbStore::flush` explicitly when `flush_after_write` is disabled and
the application reaches a durability boundary.

## Storage model

The adapter stores three key families in the selected SlateDB path:

- `node:` for content-addressed Prolly nodes.
- `hint:` for optional traversal hints.
- `root:` for named root manifests.

Use distinct SlateDB paths for physical tenants or environments. Named-root
prefixes are logical names and do not separate the underlying node set.

## Transactions and named roots

The adapter implements `Store`, `ManifestStore`, scanning, and
`TransactionalStore`. Within one `SlateDbStore` instance, it validates
named-root preconditions and applies the associated node and root changes in one
SlateDB write batch. A stale root precondition returns a conflict without
applying that batch.

Deleting a named root does not immediately delete unreachable nodes. Coordinate
retention and garbage collection at the application layer.

## Runtime and durability notes

- SlateDB is async-first. This adapter owns a private multi-threaded Tokio
  runtime to expose a synchronous API.
- Do not call the synchronous store directly from a Tokio worker when blocking
  would starve the application. Use a blocking thread or `spawn_blocking`.
- The default flush-after-write behavior favors durability over write throughput.
  Benchmark before disabling it, and add explicit flush boundaries.
- Reuse both the object-store client and the adapter instead of rebuilding them
  for individual operations.
- Coordinate a single writer for each SlateDB path; the adapter's manifest lock
  protects one adapter instance, not independent processes.

## Testing

The integration suite uses SlateDB's in-memory object store and does not require
cloud credentials:

```bash
cargo test --manifest-path stores/prolly-store-slatedb/Cargo.toml
```

See the [`prolly-map` API documentation](https://docs.rs/prolly-map) for map,
transaction, diff, and merge operations available with this store.
