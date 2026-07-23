# prolly-store-pglite

PGlite sidecar storage adapter for
[`prolly-map`](https://crates.io/crates/prolly-map). It gives synchronous Rust
applications a persistent Prolly store backed by PGlite's PostgreSQL-compatible
WebAssembly engine.

The adapter starts a Node.js child process, loads `@electric-sql/pglite`, and
exchanges JSON Lines requests over standard input and output. The child process
and database are closed when the store is dropped.

## When to use it

Use this adapter when a desktop tool, local service, or test environment already
ships Node.js and benefits from PGlite persistence without running a PostgreSQL
server. Choose the native SQLite or RocksDB adapters when a Node.js sidecar is
undesirable. Choose the PostgreSQL adapter when multiple processes or hosts must
share the same database server.

## Installation

Add the Rust crates:

```toml
[dependencies]
prolly-map = "0.5.1"
prolly-store-pglite = "0.3.0"
```

Install PGlite where Node.js can resolve it:

```bash
npm install @electric-sql/pglite
```

By default, the adapter runs `node` in the application's current directory. Set
`PROLLY_PGLITE_NODE` to use another Node.js executable and
`PROLLY_PGLITE_NODE_CWD` to use a directory containing a different
`node_modules` tree.

## Quick start

```rust,no_run
use prolly::{Config, Prolly};
use prolly_store_pglite::PgliteStore;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = PgliteStore::open("./data/app.pglite")?;
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

Use `PgliteStore::open_in_memory()` for an ephemeral store.

## Configuration

`PgliteStoreConfig` exposes the three sidecar settings:

- `data_dir` is passed to PGlite. Use `memory://` for an in-memory database or
  a filesystem path for persistence.
- `node_command` selects the Node.js executable.
- `node_working_dir` controls Node module resolution.

```rust,no_run
use std::path::PathBuf;
use prolly_store_pglite::{PgliteStore, PgliteStoreConfig};

let store = PgliteStore::open_with_config(PgliteStoreConfig {
    data_dir: "./data/app.pglite".into(),
    node_command: PathBuf::from("node"),
    node_working_dir: Some(PathBuf::from("./pglite-runtime")),
})?;
Ok::<(), Box<dyn std::error::Error>>(())
```

## Storage model

The sidecar creates three tables in the PGlite database:

- `prolly_nodes` stores content-addressed nodes by 32-byte CID.
- `prolly_hints` stores optional traversal hints by namespace and key.
- `prolly_roots` stores named root manifests.

Use a separate PGlite data directory for physical isolation. Within one store,
slash-separated named roots such as `tenant/42/main` are useful logical names,
but nodes remain content-addressed and may be shared by multiple roots.

## Transactions and named roots

The adapter implements `Store`, `ManifestStore`, scanning, and
`TransactionalStore`. Node writes, root preconditions, and named-root updates
are committed inside one PGlite transaction. A stale root precondition returns a
transaction conflict without applying the staged writes.

Deleting a named root does not delete nodes that are no longer reachable. Plan
retention and garbage collection at the application layer.

## Operational notes

- Calls are synchronous and serialized through the store's sidecar connection.
- Each `PgliteStore` owns one Node.js child process. Reuse the store instead of
  opening one for every operation.
- Sidecar startup errors include captured Node.js stderr, which commonly reveals
  a missing `@electric-sql/pglite` installation or an incorrect working directory.
- `open_in_memory` is suitable for tests, but its contents disappear when the
  store is dropped.

## Testing

The normal test suite compiles without starting Node.js. To run the PGlite
conformance and persistence tests, install `@electric-sql/pglite` and opt in:

```bash
PROLLY_PGLITE_TEST=1 \
  cargo test --manifest-path stores/prolly-store-pglite/Cargo.toml
```

See the [`prolly-map` API documentation](https://docs.rs/prolly-map) for map,
transaction, diff, and merge operations available with this store.
