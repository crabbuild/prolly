# Guides

This page collects practical guidance for building applications on top of
`prolly-map`.

## Choose Keys Deliberately

All ordering is byte lexicographic. Good key design is the difference between a
pleasant storage layer and accidental full scans.

Use stable prefixes for logical collections:

```text
user:<tenant>:<user-id>
memory:<workspace>:<conversation>:<timestamp>:<event-id>
doc:<workspace>:<doc-id>:chunk:<chunk-id>
index:title:<workspace>:<normalized-title>:<doc-id>
```

Guidelines:

- Put the highest-cardinality range selector near the front when you commonly
  scan by it.
- Use fixed-width numeric encodings when byte ordering must match numeric
  ordering.
- Keep values out of keys unless you need the key to be an index entry.
- Add a unique suffix when multiple records can share the same logical sort
  fields.
- Treat key schema changes as migrations. Old snapshots retain old keys.

Helpers:

```rust
use prolly::{KeyBuilder, prefix_range};

let key = KeyBuilder::new()
    .push_str("memory")
    .push_str("workspace-1")
    .push_timestamp_millis(1_700_000_000_000)
    .push_u64(42)
    .finish();

let (start, end) = prefix_range(b"memory:workspace-1:");
```

Use `prefix_range` rather than hand-writing end keys when the prefix may end in
`0xff`.

## Encode Values

The tree stores bytes. You choose the application encoding.

Common patterns:

- JSON for readable debugging and broad language support.
- CBOR for compact binary values with serde support.
- A custom codec for compatibility-sensitive formats.
- A versioned envelope for values that will evolve.

```rust
use prolly::{decode_json, encode_json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct MemoryRecord {
    source: String,
    content: String,
}

let record = MemoryRecord {
    source: "conversation/c42".to_string(),
    content: "User prefers durable local-first state.".to_string(),
};

let bytes = encode_json(&record)?;
let decoded: MemoryRecord = decode_json(&bytes)?;
assert_eq!(record, decoded);
```

For long-lived data, prefer a versioned envelope:

```rust
use prolly::VersionedValue;

let value = VersionedValue::raw("memory-record", 1, b"payload".to_vec());
```

Versioned values make future Rust, Python, and other language ports easier
because old snapshots can be decoded by schema version.

## Range Scans and Pagination

Use `range` for local iteration:

```rust
let rows = prolly
    .range(&tree, b"user:", Some(b"user;"))?
    .collect::<Result<Vec<_>, _>>()?;
```

Use paged APIs or cursors for:

- HTTP endpoints;
- background jobs that may be paused;
- browser storage;
- remote stores;
- very large ranges.

The core rule is: do not hold an in-memory iterator as an application cursor.
Persist the cursor state or the last key you returned.

## Use Named Roots for Application State

A prolly tree snapshot is immutable. Application state usually needs a stable
name that advances over time.

Examples:

- `main`: canonical application state.
- `workspace:<id>:memory`: published memory tree.
- `workspace:<id>:index:rag`: published RAG metadata index.
- `agent:<run-id>:attempt:<n>`: branch for an agent attempt.
- `view:<name>`: materialized view root.

Publish with compare-and-swap:

```rust
let current = prolly.load_named_root(b"main")?;
let next = match &current {
    Some(tree) => prolly.put(tree, b"k".to_vec(), b"v".to_vec())?,
    None => prolly.put(&prolly.create(), b"k".to_vec(), b"v".to_vec())?,
};

let update = prolly.compare_and_swap_named_root(
    b"main",
    current.as_ref(),
    Some(&next),
)?;

if !update.is_applied() {
    // Another writer won. Reload, merge or retry, then publish again.
}
```

Keep older named roots when they are part of your retention policy. Garbage
collection should consider every root that may still be loaded.

## Merge Resolvers

Merge conflicts preserve absence explicitly:

```rust
pub struct Conflict {
    pub key: Vec<u8>,
    pub base: Option<Vec<u8>>,
    pub left: Option<Vec<u8>>,
    pub right: Option<Vec<u8>>,
}
```

`None` means the key is absent on that side. An empty value is
`Some(Vec::new())`, not a delete.

Resolver return values:

```rust
pub enum Resolution {
    Value(Vec<u8>),
    Delete,
    Unresolved,
}
```

Built-in standard resolvers:

- `resolver::prefer_left`
- `resolver::prefer_right`
- `resolver::delete_wins`
- `resolver::update_wins`

Delete-wins:

```rust
use prolly::resolver;

let merged = prolly.merge(
    &base,
    &left,
    &right,
    Some(Box::new(resolver::delete_wins)),
)?;
```

Update-wins:

```rust
use prolly::resolver;

let merged = prolly.merge(
    &base,
    &left,
    &right,
    Some(Box::new(resolver::update_wins)),
)?;
```

Custom value merge:

```rust
use prolly::{Resolution, Resolver};

let append_notes: Resolver = Box::new(|conflict| {
    match (&conflict.left, &conflict.right) {
        (Some(left), Some(right)) => {
            let mut merged = left.clone();
            merged.extend_from_slice(b"\n");
            merged.extend_from_slice(right);
            Resolution::value(merged)
        }
        _ => Resolution::unresolved(),
    }
});
```

Leave unresolved:

```rust
use prolly::{Resolution, Resolver};

let manual_review: Resolver = Box::new(|_conflict| Resolution::unresolved());
```

Resolver guidance:

- Make resolvers deterministic and side-effect free.
- Decode values according to your application schema, not by ad hoc string
  concatenation, once records become structured.
- Return `Resolution::Delete` only when deletion is a valid outcome for the
  key family.
- Return `Resolution::Unresolved` when the application needs a human or domain
  workflow to decide.

## CRDT-Style Merges

CRDT merge strategies are designed to be conflict-free. Built-in strategies do
not return `Error::Conflict`.

Available strategies:

- Last-writer-wins using `TimestampedValue` or a custom timestamp extractor.
- Multi-value, which preserves concurrent values.
- Custom, which returns `CrdtResolution::Value` or `CrdtResolution::Delete`.

Delete policy is used by built-in strategies:

```rust
use prolly::{CrdtConfig, DeletePolicy};

let config = CrdtConfig::lww().with_delete_policy(DeletePolicy::DeleteWins);
```

Custom CRDT resolver:

```rust
use prolly::{CrdtConfig, CrdtResolution};

let config = CrdtConfig::custom(|conflict| {
    if conflict.left.is_none() || conflict.right.is_none() {
        CrdtResolution::delete()
    } else {
        CrdtResolution::value(conflict.right.clone().unwrap())
    }
});
```

Use standard merge resolvers when conflicts are allowed and meaningful. Use
CRDT-style merge when your application requires merge to always produce a tree.

## Async Storage

The sync `Store` trait is still the simplest fit for in-memory, SQLite, and
RocksDB-like backends. Use async storage when reads or writes may wait on:

- object stores such as S3 or R2;
- network peer sync;
- remote cache services;
- browser storage;
- background agents sharing an async runtime;
- high-latency stores that benefit from concurrent batched reads.

Enable it:

```toml
prolly-map = { version = "0.1", features = ["async-store"] }
```

If you have a sync store:

```rust
use prolly::{AsyncProlly, Config, MemStore, SyncStoreAsAsync};

async fn build_manager() {
    let store = SyncStoreAsAsync::new(MemStore::new());
    let prolly = AsyncProlly::new(store, Config::default());
}
```

If you use Tokio and a blocking sync backend, prefer the Tokio adapter:

```toml
prolly-map = { version = "0.1", features = ["tokio"] }
```

Async is not automatically faster. It improves throughput when work is I/O
bound and can be overlapped. For CPU-bound in-memory workloads, the sync API is
usually simpler and may be faster.

## Large Values and Blob Stores

Small values can live inline in leaf nodes. Large values can be offloaded to a
`BlobStore`, with the tree storing a `ValueRef`.

Use offload when:

- values are much larger than keys;
- many snapshots refer to the same blob;
- you want separate retention for blobs;
- values should live in object storage while nodes stay in a faster index
  store.

Keep blob references content-addressed where possible. That makes dedupe and
cross-store sync easier.

## Garbage Collection

Nodes are immutable, so old nodes remain in the store until you sweep them.
Garbage collection is a two-step workflow:

1. Select all roots you want to retain.
2. Plan and sweep unreachable nodes or blobs.

Rules:

- Include named roots, in-flight branches, checkpoints, and any pinned debug
  roots in the retention set.
- Do not sweep nodes that may still be referenced by a branch another process
  can publish.
- Keep blob GC aligned with node GC if leaf values contain blob references.

GC should be conservative in multi-writer systems. It is better to keep old
nodes longer than to delete a snapshot another process still needs.

## Store Synchronization

Because nodes are content addressed, stores can sync by CID.

Typical workflow:

1. Compare the desired tree root against the destination store.
2. Plan missing CIDs.
3. Copy missing nodes.
4. Publish or record the received root.

This is useful for:

- local cache warmup;
- peer-to-peer sync;
- uploading snapshots to object storage;
- replicating branch heads between devices;
- sending compact context snapshots to background agents.

## Inspection and Debugging

Use tree stats to understand shape:

```rust
let stats = prolly.collect_stats(&tree)?;
println!("{stats:#?}");
```

Use debug views when validating tree structure, fanout, and changed nodes.

The crate also includes a CLI binary:

```sh
cargo run -p prolly-map --bin prolly-inspect -- --help
```

Use it for local inspection while tuning chunking, testing stores, or debugging
unexpected merges.

## Configuration

Defaults are appropriate for getting started:

```rust
use prolly::{Config, Encoding};

let config = Config::builder()
    .min_chunk_size(4)
    .max_chunk_size(1024)
    .chunking_factor(128)
    .hash_seed(42)
    .encoding(Encoding::Raw)
    .node_cache_max_nodes(50_000)
    .node_cache_max_bytes(256 * 1024 * 1024)
    .build();
```

Tuning guidance:

- Larger chunk sizes reduce tree height and store calls, but increase rewrite
  size for small edits.
- Smaller chunk sizes improve fine-grained sharing, but increase metadata.
- Keep `hash_seed` stable for a dataset unless you intentionally want a new
  tree shape.
- Bound caches in long-running services.
- Use benchmarks before changing defaults for production workloads.
