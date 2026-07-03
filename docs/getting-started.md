# Getting Started

`prolly-map` is a content-addressed, immutable ordered map for Rust. It stores
byte keys and byte values, returns a new `Tree` for each mutation, and reuses
unchanged nodes across snapshots.

The package name and Rust crate name are intentionally different:

- Cargo package: `prolly-map`
- Rust library crate: `prolly`

That means users depend on `prolly-map`, then import `prolly`.

## Install

From crates.io, once published:

```toml
[dependencies]
prolly-map = "0.1"
```

From this workspace during development:

```toml
[dependencies]
prolly-map = { path = "crates/prolly" }
```

Then in Rust:

```rust
use prolly::{Config, MemStore, Prolly};
```

Optional feature flags:

```toml
[dependencies]
prolly-map = { version = "0.1", features = ["async-store", "tokio", "sqlite"] }
```

Feature guide:

- `async-store`: enables `AsyncStore`, `AsyncProlly`, async range and diff
  iterators, and sync-to-async store adapters.
- `tokio`: enables Tokio blocking adapters for sync stores and blob stores.
- `sqlite`: enables the SQLite node store.
- `rocksdb`: enables the RocksDB node store.
- `slatedb`: enables the SlateDB backend.
- `pglite`: enables the PGlite backend.

The default feature set is intentionally small.

## Mental Model

A `Tree` is a small persistent handle:

```rust
pub struct Tree {
    pub root: Option<Cid>,
    pub config: Config,
}
```

The tree handle does not contain all data. Nodes are stored in a pluggable
`Store`. A mutation writes new content-addressed nodes and returns a new
`Tree`; the previous `Tree` remains valid while its nodes are retained.

```text
Tree handle -> root CID -> internal node -> leaf nodes -> key/value pairs
```

Because node IDs are hashes of deterministic node bytes, identical subtrees
have identical CIDs. Diff and merge use that property to skip whole unchanged
subtrees.

## First Map

```rust
use prolly::{Config, MemStore, Prolly};

fn main() -> Result<(), prolly::Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());

    let tree = prolly.create();
    let tree = prolly.put(&tree, b"user:001".to_vec(), b"Ada".to_vec())?;
    let tree = prolly.put(&tree, b"user:002".to_vec(), b"Grace".to_vec())?;

    assert_eq!(prolly.get(&tree, b"user:001")?, Some(b"Ada".to_vec()));

    let tree = prolly.delete(&tree, b"user:002")?;
    assert_eq!(prolly.get(&tree, b"user:002")?, None);

    Ok(())
}
```

Important details:

- Keys and values are `Vec<u8>` at the storage boundary.
- Reads use borrowed byte slices such as `b"user:001"`.
- Mutations are persistent and return a new `Tree`.
- Deletes remove the key from the logical map.
- Empty byte values, `b""`, are real values and are distinct from deletion.

## Range Scans

Keys are ordered by raw byte lexicographic order. Range scans are efficient
when related keys share prefixes.

```rust
use prolly::{Config, MemStore, Prolly, prefix_range};

let prolly = Prolly::new(MemStore::new(), Config::default());
let tree = prolly.create();
let tree = prolly.put(&tree, b"user:001".to_vec(), b"Ada".to_vec())?;
let tree = prolly.put(&tree, b"user:002".to_vec(), b"Grace".to_vec())?;
let tree = prolly.put(&tree, b"team:eng".to_vec(), b"Engineering".to_vec())?;

let (start, end) = prefix_range(b"user:");
let users = prolly
    .range(&tree, &start, end.as_deref())?
    .collect::<Result<Vec<_>, _>>()?;

assert_eq!(users.len(), 2);
```

For API pagination, use range pages and cursors rather than holding a long
iterator across requests.

## Batch Mutations

For many changes, prefer `batch` over repeated single-key writes.

```rust
use prolly::{Config, MemStore, Mutation, Prolly};

let prolly = Prolly::new(MemStore::new(), Config::default());
let tree = prolly.create();

let tree = prolly.batch(
    &tree,
    vec![
        Mutation::Upsert {
            key: b"user:001".to_vec(),
            val: b"Ada".to_vec(),
        },
        Mutation::Upsert {
            key: b"user:002".to_vec(),
            val: b"Grace".to_vec(),
        },
        Mutation::Delete {
            key: b"user:old".to_vec(),
        },
    ],
)?;
```

Batching lets the engine group edits by affected leaves, reuse unchanged
subtrees, and write new nodes together.

## Bulk Loading

For initial imports, use `BatchBuilder` or `SortedBatchBuilder`.

```rust
use prolly::{BatchBuilder, Config, MemStore, Prolly};
use std::sync::Arc;

let store = Arc::new(MemStore::new());
let config = Config::default();

let mut builder = BatchBuilder::new(store.clone(), config.clone());
for i in 0..10_000 {
    builder.add(
        format!("doc:{i:08}").into_bytes(),
        format!("document {i}").into_bytes(),
    );
}

let tree = builder.build()?;
let prolly = Prolly::new(store, config);

assert!(prolly.get(&tree, b"doc:00000042")?.is_some());
```

Use `SortedBatchBuilder` when entries are already sorted by key. That avoids
extra sorting work and is a good fit for database exports, log compaction, and
index rebuilds.

## Diff

Diff compares two immutable snapshots.

```rust
use prolly::{Config, Diff, MemStore, Prolly};

let prolly = Prolly::new(MemStore::new(), Config::default());
let base = prolly.create();
let left = prolly.put(&base, b"a".to_vec(), b"1".to_vec())?;

let diffs = prolly.diff(&base, &left)?;
assert!(matches!(diffs.as_slice(), [Diff::Added { .. }]));
```

The engine prunes identical CIDs, so a diff over large trees can skip whole
unchanged subtrees.

## Merge

Three-way merge uses `base`, `left`, and `right`.

```rust
use prolly::{Config, MemStore, Prolly};

let prolly = Prolly::new(MemStore::new(), Config::default());

let base = prolly.create();
let base = prolly.put(&base, b"name".to_vec(), b"Ada".to_vec())?;

let left = prolly.put(&base, b"city".to_vec(), b"London".to_vec())?;
let right = prolly.put(&base, b"language".to_vec(), b"Rust".to_vec())?;

let merged = prolly.merge(&base, &left, &right, None)?;

assert_eq!(prolly.get(&merged, b"name")?, Some(b"Ada".to_vec()));
assert_eq!(prolly.get(&merged, b"city")?, Some(b"London".to_vec()));
assert_eq!(prolly.get(&merged, b"language")?, Some(b"Rust".to_vec()));
```

When both sides change the same key incompatibly, pass a resolver.

```rust
use prolly::{resolver, Config, MemStore, Prolly};

let prolly = Prolly::new(MemStore::new(), Config::default());
let base = prolly.create();
let base = prolly.put(&base, b"setting:theme".to_vec(), b"system".to_vec())?;

let left = prolly.delete(&base, b"setting:theme")?;
let right = prolly.put(&base, b"setting:theme".to_vec(), b"dark".to_vec())?;

let merged = prolly.merge(
    &base,
    &left,
    &right,
    Some(Box::new(resolver::update_wins)),
)?;

assert_eq!(prolly.get(&merged, b"setting:theme")?, Some(b"dark".to_vec()));
```

See [Guides](guides.md#merge-resolvers) for resolver patterns.

## Named Roots

Named roots are durable names for immutable snapshots. They are useful for
branch heads, application checkpoints, published indexes, and materialized
views.

```rust
use prolly::{Config, MemStore, Prolly};
use std::sync::Arc;

let store = Arc::new(MemStore::new());
let prolly = Prolly::new(store, Config::default());

let tree = prolly.create();
let tree = prolly.put(&tree, b"name".to_vec(), b"CrabDB".to_vec())?;

let update = prolly.compare_and_swap_named_root(b"main", None, Some(&tree))?;
assert!(update.is_applied());

let loaded = prolly.load_named_root(b"main")?.unwrap();
assert_eq!(prolly.get(&loaded, b"name")?, Some(b"CrabDB".to_vec()));
```

Use compare-and-swap when multiple processes or agents may publish to the same
root name.

## Async Start

The async API is enabled with the `async-store` feature. It does not require
Tokio by itself.

```toml
[dependencies]
prolly-map = { version = "0.1", features = ["async-store"] }
```

Use `SyncStoreAsAsync` for simple migration from an existing sync store:

```rust
use prolly::{AsyncProlly, Config, MemStore, SyncStoreAsAsync};

async fn run() -> Result<(), prolly::Error> {
    let store = SyncStoreAsAsync::new(MemStore::new());
    let prolly = AsyncProlly::new(store, Config::default());

    let tree = prolly.create();
    let tree = prolly.put(&tree, b"k".to_vec(), b"v".to_vec()).await?;
    assert_eq!(prolly.get(&tree, b"k").await?, Some(b"v".to_vec()));

    Ok(())
}
```

Use `tokio` when you want blocking sync stores to run on Tokio's blocking pool:

```toml
[dependencies]
prolly-map = { version = "0.1", features = ["tokio"] }
```

## Run Examples

From the workspace root:

```sh
cargo run -p prolly-map --example basic_map
cargo run -p prolly-map --example resolver
cargo run -p prolly-map --example conversation_memory
cargo run -p prolly-map --example deterministic_rag_snapshot
```

Useful examples:

- `basic_map.rs`: basic put, get, delete, and range.
- `batch_build.rs`: bulk loading and stats.
- `diff_merge.rs`: structural diff and three-way merge.
- `resolver.rs`: delete-aware merge resolvers.
- `crdt_merge.rs`: conflict-free merge strategies.
- `conversation_memory.rs`: agent memory branches and CAS publish.
- `deterministic_rag_snapshot.rs`: reproducible RAG metadata snapshots.
- `secondary_index.rs`: derived indexes from diffs.
- `materialized_view.rs`: source/view root management.
- `vector_sidecar.rs`: vector DB sidecar keys.
- `file_blob_store.rs`: large value offload.
- `background_compaction.rs`: compaction and GC workflow.
