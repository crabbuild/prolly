# Prolly Trees

A high-performance, content-addressable, probabilistically-balanced search tree implementation in Rust.

## Overview

Prolly trees (Probabilistic B-Trees) are persistent, immutable data structures that combine the efficiency of B+ trees with deterministic merging capabilities. They use content-defined chunking to achieve structural sharing and enable efficient version control operations like diff and merge.

## Key Features

- **Immutable & Persistent**: All operations return new trees, enabling safe concurrent access and version history
- **Content-Addressed**: Nodes are identified by their SHA-256 hash (CID), enabling deduplication and structural sharing
- **Deterministic**: Same content always produces the same tree structure, making merges predictable
- **Efficient Diffs**: Structural sharing enables fast comparisons between versions
- **Pluggable Storage**: Generic over storage backend - use in-memory, RocksDB, or custom implementations
- **Batch Operations**: Optimized bulk mutations with atomic writes
- **Range Queries**: Efficient iteration over key ranges in lexicographic order
- **CRDT Support**: Conflict-free replicated data type semantics for distributed systems
- **Parallel Processing**: Optional parallel batch operations for large trees

See [`performance.md`](../../docs/performance.md) for the paper-derived optimization plan, implemented hardening, and benchmark coverage.

## Quick Start

```rust
use prolly::{Prolly, MemStore, Config};

// Create a store and tree manager
let store = MemStore::new();
let prolly = Prolly::new(store, Config::default());

// Create an empty tree
let tree = prolly.create();

// Insert key-value pairs (returns a new tree - immutable)
let tree = prolly.put(&tree, b"key".to_vec(), b"value".to_vec()).unwrap();

// Retrieve values
let value = prolly.get(&tree, b"key").unwrap();
assert_eq!(value, Some(b"value".to_vec()));

// Delete keys
let tree = prolly.delete(&tree, b"key").unwrap();
assert!(prolly.get(&tree, b"key").unwrap().is_none());
```

## Core Concepts

### Content-Defined Chunking

Prolly trees use probabilistic boundary detection to determine where nodes should split. This is based on hashing key-value pairs and checking if the hash meets a threshold condition:

- **Min Chunk Size**: Minimum entries before considering boundaries (default: 4)
- **Max Chunk Size**: Maximum entries before forcing a split (default: 1,048,576)
- **Chunking Factor**: Controls average node size (default: 128, ~0.78% boundary probability)
- **Hash Seed**: Seed for boundary detection (default: 0)

The boundary detection uses xxHash64 for fast, deterministic chunking. A boundary is created when:
1. Node size < min_chunk_size: Never split
2. Node size >= max_chunk_size: Always split
3. Otherwise: Hash-based probabilistic boundary (hash & 0xFFFFFFFF) <= (u32::MAX / chunking_factor)

### Tree Structure

```
Root (Internal Node)
├─ [key1] → Child1 (Internal/Leaf)
├─ [key2] → Child2 (Internal/Leaf)
└─ [key3] → Child3 (Internal/Leaf)

Leaf Node:
keys: [k1, k2, k3, ...]
vals: [v1, v2, v3, ...]  // Raw bytes

Internal Node:
keys: [k1, k2, k3, ...]
vals: [cid1, cid2, cid3, ...]  // CIDs of child nodes
```

### Immutability

All operations return new trees rather than modifying existing ones:

```rust
let tree1 = prolly.create();
let tree2 = prolly.put(&tree1, b"key".to_vec(), b"value".to_vec()).unwrap();

// tree1 is still empty
assert!(tree1.is_empty());

// tree2 has the new key
assert!(!tree2.is_empty());
```

## API Reference

### Basic Operations

#### `create() -> Tree`
Create a new empty tree.

#### `get(&tree, key) -> Result<Option<Vec<u8>>, Error>`
Retrieve a value by key. Returns `None` if key doesn't exist.

#### `put(&tree, key, val) -> Result<Tree, Error>`
Insert or update a key-value pair. Returns a new tree.

**Idempotent**: Inserting the same key-value pair returns the original tree (same root CID).

#### `delete(&tree, key) -> Result<Tree, Error>`
Delete a key from the tree. Returns a new tree.

**Idempotent**: Deleting a non-existent key returns the original tree.

### Range Queries

#### `range(&tree, start, end) -> Result<RangeIter, Error>`
Iterate over key-value pairs in lexicographic order.

- `start`: Inclusive start key (use `&[]` for beginning)
- `end`: Exclusive end key (use `None` for end)

```rust
// Iterate over all keys
for result in prolly.range(&tree, &[], None).unwrap() {
    let (key, val) = result.unwrap();
    println!("{:?} -> {:?}", key, val);
}

// Iterate over range [b, d)
for result in prolly.range(&tree, b"b", Some(b"d")).unwrap() {
    let (key, val) = result.unwrap();
    println!("{:?} -> {:?}", key, val);
}
```

#### `prefix(&tree, prefix) -> Result<RangeIter, Error>`
Iterate over every key that starts with `prefix`, using the same byte-prefix
bounds as `prefix_range(prefix)`.

#### `prefix_page(&tree, prefix, cursor, limit) -> Result<RangePage, Error>`
Read a bounded page over keys that start with `prefix`. A start cursor begins
at the prefix start; returned cursors resume strictly after the last emitted
key.

#### `first_entry(&tree)` / `last_entry(&tree)`
Return the first or last key-value pair in lexicographic order, or `None` for
an empty tree.

#### `lower_bound(&tree, key)` / `upper_bound(&tree, key)`
Return the first entry whose key is greater than or equal to `key`, or strictly
greater than `key`, respectively.

### Batch Operations

#### `batch(&tree, mutations) -> Result<Tree, Error>`
Apply multiple mutations atomically with optimized performance.

```rust
use prolly::Mutation;

let mutations = vec![
    Mutation::Upsert { key: b"a".to_vec(), val: b"1".to_vec() },
    Mutation::Upsert { key: b"b".to_vec(), val: b"2".to_vec() },
    Mutation::Delete { key: b"c".to_vec() },
];

let new_tree = prolly.batch(&tree, mutations).unwrap();
```

**Behavior**:
- Mutations are sorted by key for efficient processing
- Duplicate keys use last-write-wins semantics
- All nodes are written atomically via `Store::batch`
- More efficient than individual `put`/`delete` operations

### Diff and Merge

#### `diff(&base, &other) -> Result<Vec<Diff>, Error>`
Compute differences between two trees.

```rust
let diffs = prolly.diff(&base, &other).unwrap();

for diff in diffs {
    match diff {
        Diff::Added { key, val } => println!("Added: {:?} -> {:?}", key, val),
        Diff::Removed { key, val } => println!("Removed: {:?} -> {:?}", key, val),
        Diff::Changed { key, old, new } => {
            println!("Changed: {:?} from {:?} to {:?}", key, old, new)
        }
    }
}
```

**Short-circuit**: If both trees have the same root CID, returns empty vector immediately.

#### `structural_diff_page(&base, &other, cursor, limit) -> Result<StructuralDiffPage, Error>`
Read a bounded page from the structural diff traversal. The returned
`StructuralDiffCursor` records the remaining CID frontier and pending leaf
diffs, so background jobs can checkpoint large diffs without restarting from a
key cursor.

```rust
let mut cursor = None;
loop {
    let page = prolly
        .structural_diff_page(&base, &other, cursor.as_ref(), 64)
        .unwrap();
    for diff in page.diffs {
        println!("{:?}", diff);
    }
    match page.next_cursor {
        Some(next) => cursor = Some(next),
        None => break,
    }
}
```

#### `stream_conflicts(&base, &left, &right) -> Result<Iterator<Item = Result<Conflict, Error>>, Error>`
Stream only conflicts for a standard three-way merge. The iterator walks the
same structural diff path as `stream_diff`, skips non-conflicting right-side
changes, and yields delete-aware `Conflict` values where absent sides are
preserved as `None`.

```rust
let conflicts = prolly
    .stream_conflicts(&base, &left, &right)
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
```

`AsyncProlly::stream_conflicts` exposes the same conflict stream under the
`async-store` feature with async `next`, `collect`, and stream adapters.

#### `merge(&base, &left, &right, resolver) -> Result<Tree, Error>`
Three-way merge using `base` as the common ancestor.

```rust
// Merge without conflicts
let merged = prolly.merge(&base, &left, &right, None).unwrap();

// Merge with conflict resolver (prefer left)
let resolver: Resolver = Box::new(|conflict| {
    conflict
        .left
        .clone()
        .map_or_else(Resolution::delete, Resolution::value)
});
let merged = prolly.merge(&base, &left, &right, Some(resolver)).unwrap();
```

**Conflict Handling**:
- Conflict occurs when both branches modify the same key differently
- If no resolver provided or resolver returns `Resolution::Unresolved`, returns `Error::Conflict`
- Resolver receives `Conflict` with `key`, `base`, `left`, and `right`; each side is `Option<Vec<u8>>`
- Resolver returns `Resolution::Value`, `Resolution::Delete`, or `Resolution::Unresolved`

#### `merge_range(&base, &left, &right, start, end, resolver) -> Result<Tree, Error>`
Three-way merge scoped to right-side changes in the half-open key range
`[start, end)`. Keys outside the range remain exactly as they are in `left`.
Use `merge_prefix(&base, &left, &right, prefix, resolver)` for prefix-partitioned
data such as one document, tenant, workspace, or index shard.

```rust
let merged = prolly
    .merge_prefix(&base, &left, &right, b"documents/123/", None)
    .unwrap();
```

#### `merge_explain(&base, &left, &right, resolver) -> MergeExplanation`
Diagnostics-oriented merge. The returned explanation contains both the merge
result and a typed `MergeTrace`, so callers can inspect fast paths, structural
reuse, rewritten nodes, resolver calls, and fallback reasons even when the merge
result is `Error::Conflict`. Diff/batch fallbacks also emit `DiffTraversal`
counters for compared nodes, reused subtrees, collected fallback paths, and
emitted diffs.

```rust
let explanation = prolly.merge_explain(&base, &left, &right, None);
for event in &explanation.trace.events {
    println!("{event:?}");
}
```

`AsyncProlly::merge_explain` exposes the same `MergeExplanation` shape under
the `async-store` feature.

### Advanced Features

#### `cursor(&tree, key) -> Result<Cursor, Error>`
Create a cursor for efficient tree navigation.

```rust
let cursor = prolly.cursor(&tree, b"key").unwrap();
if cursor.is_valid() {
    println!("Key: {:?}", cursor.get_key());
}
```

#### `cursor_window(&tree, key, end, limit) -> Result<CursorWindow, Error>`
Seek with the cursor and read a bounded forward page in one call. The result
reports the cursor landing entry, whether `key` was an exact match, up to
`limit` entries starting at the first key greater than or equal to `key`, and a
range cursor that resumes after the last emitted entry.

Use `limit == 0` for an exact/inexact seek probe without reading page entries.

#### `diff_cursor(&base, &other) -> Result<DiffCursor, Error>`
Stream differences without collecting all diffs upfront (memory-efficient for large trees).

```rust
for diff in prolly.diff_cursor(&base, &other).unwrap() {
    println!("{:?}", diff);
}
```

#### `crdt_merge(&base, &left, &right, config) -> Result<Tree, Error>`
Merge using CRDT semantics for automatic conflict resolution.

```rust
use prolly::{CrdtConfig, MergeStrategy};

let config = CrdtConfig::default();
let merged = prolly.crdt_merge(&base, &left, &right, &config).unwrap();
```

**Strategies**:
- **LastWriterWins (LWW)**: Value with higher timestamp wins
- **MultiValue (MV)**: Preserve all concurrent values as a set
- **Custom**: User-provided merge function

#### `parallel_batch(&tree, mutations, config) -> Result<Tree, Error>`
Apply batch mutations through the tunable high-throughput batch path.
Use `parallel_batch_with_stats` when you need the same route/write telemetry
returned by `batch_with_stats`.

```rust
use prolly::ParallelConfig;

let config = ParallelConfig::default();
let new_tree = prolly.parallel_batch(&tree, mutations, &config).unwrap();
let result = prolly.parallel_batch_with_stats(&tree, mutations, &config).unwrap();
```

#### `collect_stats(&tree) -> Result<TreeStats, Error>`
Gather comprehensive statistics about tree structure and efficiency.

```rust
let stats = prolly.collect_stats(&tree).unwrap();
println!("Total entries: {}", stats.total_entries);
println!("Tree height: {}", stats.height);
println!("Average node size: {:.2}", stats.avg_node_size);
```

## Configuration

```rust
use prolly::{Config, Encoding};

let config = Config::builder()
    .min_chunk_size(4)           // Minimum entries before considering boundaries
    .max_chunk_size(1024)        // Maximum entries before forcing split
    .chunking_factor(128)        // Higher = larger nodes (default: 128)
    .hash_seed(42)               // Seed for boundary detection
    .encoding(Encoding::Raw)     // Value encoding (Raw, Cbor, Json, Custom)
    .node_cache_max_nodes(50_000) // Optional decoded-node cache cap
    .node_cache_max_bytes(256 * 1024 * 1024) // Optional serialized-byte cap
    .build();

let prolly = Prolly::new(store, config);
```

### Tuning Parameters

**Chunking Factor**:
- Lower values (e.g., 4) → More boundaries → Smaller nodes → More I/O
- Higher values (e.g., 1024) → Fewer boundaries → Larger nodes → Less I/O
- Default (128) provides good balance (~0.78% boundary probability)

**Min/Max Chunk Size**:
- `min_chunk_size`: Prevents excessive splitting for small nodes
- `max_chunk_size`: Prevents unbounded growth (security consideration)

**Hash Seed**:
- Different seeds produce different tree structures for same data
- Useful for testing or creating multiple independent trees

## Storage Backends

### In-Memory Store

```rust
use prolly::MemStore;

let store = MemStore::new();
let prolly = Prolly::new(store, Config::default());
```

### RocksDB Store

```rust
use prolly::RocksDBStore;

let store = RocksDBStore::open("./data").unwrap();
let prolly = Prolly::new(store, Config::default());
```

### Custom Store

Implement the `Store` trait:

```rust
use prolly::Store;

pub trait Store: Clone {
    type Error: std::error::Error + Send + Sync + 'static;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;
    fn batch(&self, ops: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), Self::Error>;
}
```

## Module Organization

The implementation is organized into focused modules:

- **`mod.rs`**: Main `Prolly<S>` API and orchestration
- **`tree.rs`**: Tree structure definition
- **`node.rs`**: Node structure and builder
- **`cid.rs`**: Content identifier (SHA-256 hash)
- **`config.rs`**: Configuration and builder
- **`error.rs`**: Error types and mutation/diff definitions
- **`encoding.rs`**: Encoding types and constants
- **`boundary.rs`**: Boundary detection for chunking
- **`batch.rs`**: Batch mutation operations
- **`diff.rs`**: Tree diff and merge operations
- **`range.rs`**: Range iteration
- **`rebalance.rs`**: Tree rebalancing logic
- **`cursor.rs`**: Cursor-based navigation
- **`crdt.rs`**: CRDT merge semantics
- **`parallel.rs`**: Parallel batch processing
- **`streaming.rs`**: Streaming diff operations
- **`builder.rs`**: Parallel tree construction
- **`utils.rs`**: Shared utility functions
- **`store/`**: Storage backend implementations

## Performance Characteristics

### Time Complexity

- **Get**: O(log n) - Binary search at each level
- **Put**: O(log n) - Path traversal + rebalancing
- **Delete**: O(log n) - Path traversal + rebalancing
- **Range**: O(log n + k) - Initial seek + k results
- **Diff**: O(n + m) - Linear scan of both trees
- **Merge**: O(n + m) - Diff + batch insert
- **Batch**: O(n log n + m) - Sort + grouped operations

### Space Complexity

- **Tree**: O(n) - All key-value pairs stored
- **Node**: O(k) - Average k entries per node
- **Structural Sharing**: Unchanged subtrees share nodes between versions

### Optimization Tips

1. **Use batch operations** for bulk modifications (10-100x faster than individual operations)
2. **Tune chunking factor** based on your workload (larger for fewer, larger nodes)
3. **Use streaming diff** for large trees to avoid memory overhead
4. **Enable parallel batch** for very large mutation sets (>10,000 entries)
5. **Choose appropriate storage backend** (RocksDB for persistence, MemStore for testing)

## Thread Safety

The `Prolly<S>` struct is `Send` and `Sync` when the underlying store is. The immutable nature of trees means multiple threads can safely read from the same tree simultaneously.

```rust
use std::sync::Arc;

let store = Arc::new(MemStore::new());
let prolly = Prolly::new(store.clone(), Config::default());

// Safe to share across threads
let tree = Arc::new(prolly.create());
```

## Examples

### Version Control System

```rust
// Create initial version
let v1 = prolly.create();
let v1 = prolly.put(&v1, b"file.txt".to_vec(), b"content v1".to_vec()).unwrap();

// Create branch
let v2 = prolly.put(&v1, b"file.txt".to_vec(), b"content v2".to_vec()).unwrap();

// Compute diff
let diffs = prolly.diff(&v1, &v2).unwrap();
```

### Key-Value Database

```rust
// Batch insert
let mutations: Vec<_> = (0..1000)
    .map(|i| Mutation::Upsert {
        key: format!("key{:04}", i).into_bytes(),
        val: format!("val{:04}", i).into_bytes(),
    })
    .collect();

let tree = prolly.batch(&tree, mutations).unwrap();

// Range query
for result in prolly.range(&tree, b"key0100", Some(b"key0200")).unwrap() {
    let (key, val) = result.unwrap();
    println!("{:?} -> {:?}", key, val);
}
```

### Distributed Merge

```rust
// Node A makes changes
let tree_a = prolly.put(&base, b"key1".to_vec(), b"value_a".to_vec()).unwrap();

// Node B makes changes
let tree_b = prolly.put(&base, b"key2".to_vec(), b"value_b".to_vec()).unwrap();

// Merge with CRDT semantics
let config = CrdtConfig::default();
let merged = prolly.crdt_merge(&base, &tree_a, &tree_b, &config).unwrap();
```

## Testing

The implementation includes comprehensive test coverage:

- Unit tests for each module
- Property-based tests using proptest
- Integration tests for complex scenarios
- Benchmarks for performance validation

Run tests:
```bash
cargo test --lib prolly
```

Run benchmarks:
```bash
cargo bench --bench prolly_operations
```

## References

- [Peer to Peer Ordered Search Indexes](https://0fps.net/2020/12/19/peer-to-peer-ordered-search-indexes/)
- [Dolt: How Dolt Stores Table Data](https://www.dolthub.com/blog/2020-04-01-how-dolt-stores-table-data/)
- [Efficient Diff on Prolly Trees](https://www.dolthub.com/blog/2020-06-16-efficient-diff-on-prolly-trees/)
- [Noms: Prolly Trees](https://github.com/attic-labs/noms/blob/master/doc/intro.md#prolly-trees-probabilistic-b-trees)
- [Merkle Search Trees: Efficient State-Based CRDTs](https://hal.inria.fr/hal-02303490)
- 
