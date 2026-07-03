# Implementation Notes

This document helps contributors and advanced users understand where behavior
lives in the Rust crate.

## Module Map

Public API is re-exported from `src/lib.rs`.

Core modules:

- `prolly/mod.rs`: `Prolly`, `AsyncProlly`, high-level operations, metrics,
  named-root helpers, large-value helpers, batch entry points.
- `prolly/tree.rs`: `Tree` handle.
- `prolly/cid.rs`: `Cid`.
- `prolly/node.rs`: node representation and builders.
- `prolly/boundary.rs`: content-defined boundary checks.
- `prolly/config.rs`: tree configuration.
- `prolly/encoding.rs`: encoding selectors and defaults.
- `prolly/store/`: memory, file, SQLite, RocksDB, SlateDB, PGlite, sync/async
  adapters.
- `prolly/batch.rs`: batch mutation planning and application.
- `prolly/builder.rs`: bulk builders.
- `prolly/range.rs`: range iterators and cursors.
- `prolly/diff.rs`: diff, structural diff, merge, merge explanation, async
  diff iterators.
- `prolly/error.rs`: public errors, conflict and resolution types.
- `prolly/crdt.rs`: conflict-free merge strategies.
- `prolly/policy.rs`: merge policy registry.
- `prolly/manifest.rs`: root manifests and named roots.
- `prolly/blob.rs`: large value references and blob stores.
- `prolly/gc.rs`: reachability and sweep plans.
- `prolly/sync.rs`: missing-node planning and copy helpers.
- `prolly/stats.rs`: tree statistics.
- `prolly/debug.rs`: debug tree views.
- `src/bin/prolly-inspect.rs`: inspection CLI.

## Public Re-Exports

Users should import from `prolly`, not deep internal modules:

```rust
use prolly::{Config, MemStore, Prolly, Tree};
```

The crate root re-exports the stable public surface. Internal module paths may
change during early releases.

## Read Path

Point lookup:

1. If the tree root is `None`, return `None`.
2. Load the root node by CID.
3. Search the node for the key or child span.
4. Descend until a leaf.
5. Return the value if the key is present.

Implementation details:

- decoded nodes can be cached per manager;
- serialized bytes can be counted against cache budgets;
- `get_many` can use ordered batch reads when the store prefers them;
- async reads can overlap fetches when `read_parallelism` is greater than one.

Correctness must not depend on caches or hints.

## Write Path

Single-key `put` and `delete`:

1. Traverse to the affected leaf.
2. Apply the key mutation.
3. Rebuild affected nodes.
4. Rebalance according to chunking config.
5. Serialize new nodes.
6. Store them by CID.
7. Return the new `Tree`.

Unchanged sibling subtrees keep their CIDs.

If a delete removes the last key, the result can be an empty tree with
`root == None`.

## Batch Mutation Path

Batch mutation exists because repeated single-key edits can rewrite overlapping
paths many times.

The batch implementation can:

- sort and coalesce mutations;
- group edits by leaf;
- route edits through the existing tree;
- rewrite only affected leaf groups;
- rebuild affected ancestors;
- use append-heavy fast paths;
- persist rightmost-path hints where supported.

Batch writes are especially important for:

- imports;
- index maintenance;
- materialized view updates;
- event log compaction;
- merge fallback paths that need delete resolutions.

## Bulk Builders

`BatchBuilder` accepts unsorted entries and builds a tree in bulk.

`SortedBatchBuilder` assumes entries are already sorted.

Bulk builders are preferable when creating an initial tree from many entries
because they avoid repeatedly traversing and rebalancing from an empty root.

## Diff Implementation

Diff is structural when possible.

Fast path:

- equal roots produce no diff;
- equal child CIDs are skipped;
- disjoint key spans avoid unnecessary descent.

Fallback:

- traverse leaves and compare ordered key/value streams.

The implementation exposes both eager diff results and streaming/cursor-based
interfaces for large trees.

## Merge Implementation

Merge is built on three-way comparison of `base`, `left`, and `right`.

The engine tries to reuse existing CIDs when a subtree can be selected without
rewriting. When conflicts require value resolutions, the merge can apply the
resolved value directly where safe. When a resolver returns `Delete` in a
structural path, the implementation falls back to the diff/batch path so
rebalancing remains correct.

Conflict shape:

```rust
pub struct Conflict {
    pub key: Vec<u8>,
    pub base: Option<Vec<u8>>,
    pub left: Option<Vec<u8>>,
    pub right: Option<Vec<u8>>,
}
```

This is shared by standard merge and CRDT custom merge.

`merge_explain` returns a trace that helps diagnose fast paths, reuse, fallback
reasons, and resolution kinds.

## Async Implementation

`AsyncProlly` mirrors the sync manager for async stores.

Important design choices:

- `async-store` is optional.
- Tokio is optional.
- The base `AsyncStore` trait is single-thread friendly.
- Tokio adapters are available behind the `tokio` feature.
- Sync stores can be adapted into async APIs.
- Blocking sync stores can be moved to Tokio's blocking pool when Tokio is
  enabled.

The async implementation covers reads, writes, range scans, diff, merge, CRDT
merge, stats, batch mutation, large value helpers, and cache pinning.

## Store Implementations

Current store families include:

- `MemStore`: in-memory development and tests.
- `FileNodeStore`: file-backed node storage.
- `SqliteStore`: durable embedded store behind `sqlite`.
- `RocksDBStore`: RocksDB backend behind `rocksdb`.
- `SlateDbStore`: SlateDB backend behind `slatedb`.
- `PgliteStore`: PGlite backend behind `pglite`.

Store conformance tests should check:

- missing reads;
- present reads;
- overwrite;
- delete;
- batch upsert/delete;
- ordered batch reads;
- node CID scan if supported;
- manifest behavior if supported;
- hint behavior if supported.

## Manifests

Named root helpers live on `Prolly` and use manifest traits underneath.

The implementation separates immutable tree nodes from mutable root names. CAS
updates are the main concurrency primitive. Retention policies decide how long
old roots remain available.

## Large Value Offload

Large value helpers encode either inline bytes or blob references. The tree
stores the reference. Blob bytes live in a `BlobStore`.

The implementation includes memory and file blob stores plus async adapters.

When adding a new blob backend, verify:

- idempotent writes;
- missing blob reads;
- delete behavior;
- scan behavior if used for GC;
- async behavior if the backend is remote or browser-based.

## Metrics, Stats, and Debug Views

The manager records metrics for cache and store behavior. Stats inspect tree
shape, fill factor, levels, and serialized size. Debug views expose structured
node information for tests, CLI inspection, and development.

These features are diagnostic. They must not change tree semantics.

## CLI Inspection

`prolly-inspect` is a local debugging tool. Use it to inspect store contents,
tree shape, and node data while developing examples, tuning configs, or
debugging backend behavior.

Run:

```sh
cargo run -p prolly-map --bin prolly-inspect -- --help
```

## Adding a Feature

Before adding a feature, decide which layer owns it:

- key/value schema: application code;
- ordered map semantics: tree manager;
- node persistence: store backend;
- branch head: manifest layer;
- large payload: blob store;
- conflict policy: resolver or CRDT config;
- sync: missing-node planner and copy helper;
- observability: stats, metrics, debug, CLI.

Checklist:

- preserve byte-key ordering;
- avoid changing CIDs unless the feature intentionally changes encoding or
  shape;
- keep caches and hints optional;
- add sync and async coverage when the feature touches store calls;
- add conformance tests for backend-facing behavior;
- update docs and examples for user-visible APIs.

## Test and Release Gates

Useful local gates:

```sh
cargo fmt --check
cargo test -p prolly-map
cargo test -p prolly-map --features async-store
cargo test -p prolly-map --features tokio
cargo test -p prolly-map --features sqlite
cargo clippy -p prolly-map --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc -p prolly-map --no-deps --features async-store
```

Before publishing, also run packaging checks and example builds.

