# Prolly Maintainer Design Notes

This document is for contributors changing the internals of the `prolly` crate.
The README is the user-facing guide; this file records the invariants that keep
the crate useful as versioned-map infrastructure.

## Design Goals

- Keep `Tree` values immutable and cheap to copy.
- Make equal content produce equal CIDs and roots.
- Preserve sorted byte-key ordering for lookup, range scan, diff, and merge.
- Rewrite only affected paths or subtrees on mutation.
- Let stores be simple byte stores while keeping batching, hints, and async
  concurrency available as optional optimizations.
- Keep cache entries, store hints, and fast paths correctness-optional.

## Public API Boundary

The crate root is the supported public API. The implementation module stays
private so callers use imports such as `prolly::Prolly`, `prolly::Store`,
`prolly::AsyncStore`, `prolly::BatchBuilder`, and `prolly::resolver`.

Prefer adding high-level APIs at the crate root over exposing route planning,
node rebuild, cache, or serialization helpers. When an internal helper becomes
public, add examples and conformance tests before treating it as supported.

## Tree Shape

`Tree` is a durable handle:

- `root: Option<Cid>` points at the content-addressed root node.
- `config: Config` records chunking and encoding settings used to build the
  tree.
- `root == None` is the empty tree.

Nodes live in a `Store`. Store keys are CID bytes and store values are
serialized node bytes. A `Cid` is the SHA-256 digest of deterministic node
bytes, so changing serialization, node content, or chunking config can change
new CIDs and roots.

Node invariants:

- `keys` and `vals` are parallel vectors with the same length.
- `keys` are sorted lexicographically as raw bytes.
- Leaves have `leaf == true`, `level == 0`, and `vals` contain user value
  bytes.
- Internal nodes have `leaf == false`, `level > 0`, and `vals` contain child
  CID bytes.
- Each internal key is the first key reachable in the matching child.
- The root may be a leaf or an internal node.

Empty byte values are valid user values. Absence is represented by missing
keys, not by an empty value.

## Content-Defined Boundaries

Prolly trees use content-defined chunking instead of fixed B-tree split points.
For the same ordered entries and config, boundary placement is deterministic.

Boundary rules:

1. Do not split while the current chunk is below `min_chunk_size`.
2. Force a split when the current chunk reaches `max_chunk_size`.
3. Otherwise hash `key || value` with xxHash64 and compare against the
   configured `chunking_factor`.

The practical result is stable structural sharing. A small edit usually rewrites
one leaf and its ancestor path while untouched subtrees keep the same CIDs.

When changing boundary logic, update serialization fixtures and benchmark tree
shape reports. Even correct changes can alter root identities.

## Write Path

Single-key mutation follows a path-copying model:

1. Load the root-to-leaf path.
2. Clone and update the target leaf.
3. Rebalance, split, merge, or propagate parent changes.
4. Collect new nodes.
5. Flush serialized node bytes to the store.
6. Return a new `Tree` with the new root CID.

Batch mutation sorts and deduplicates mutations by key with last-write-wins
semantics, groups work by target leaf, and flushes new nodes in a batch when the
store supports it.

Rightmost-path hints can accelerate append-heavy writes, but they are never part
of correctness. If a hint is absent, stale, or ignored by the store, the engine
must fall back to the normal tree walk.

## Read, Cache, and Metrics

All reads must be correct with an empty cache. A cache miss loads node bytes from
the store, decodes the node, and may populate the bounded node cache. Cache
limits and eviction policy affect memory and speed only.

Root/path pinning is also a cache hint. Pinned entries may temporarily exceed
configured cache limits for hot snapshots or lookup paths, but unpinning must
immediately restore normal eviction. A pinned miss is still just a store read
followed by a cache insert; no algorithm may require a node to be pinned for
correctness.

Metrics are observational. They should be useful for tuning and regression
tests, but no algorithm should depend on a metric counter for correctness.

## Store Contract

`Store` implementations must preserve bytes exactly. The prolly engine assumes
that `get(cid)` returns the same serialized node bytes that were written for
that CID.

Required behavior:

- `get` returns `Ok(Some(bytes))` for present keys and `Ok(None)` for missing
  keys.
- `put` inserts or replaces bytes exactly.
- `delete` is idempotent.
- `batch` applies all operations, atomically when the backend can support it.
- Store errors are surfaced through `Error::Store`.

Optimized behavior:

- `batch_get_ordered` must return one result per requested key in the same
  order as the input, including duplicate keys.
- `batch_get_ordered_unique` may assume the caller already deduplicated keys.
- `prefers_batch_reads` should return `true` only when multi-get, request
  coalescing, or parallel reads are actually more efficient than point reads.
- `batch_put` and `batch_put_with_hint` should preserve all node writes even if
  hint persistence fails or is unsupported.
- Hints are performance data. Stores may ignore them.

The sync `Store` trait is `Send + Sync` for native embedded backends.

## Manifest Store Contract

`ManifestStore` is the mutable naming layer for immutable tree handles. It is
separate from `Store` because content-addressed nodes and named roots have
different consistency requirements.

Manifest invariants:

- Names are opaque byte strings owned by the application.
- Values are versioned `RootManifest` payloads containing `root: Option<Cid>`
  and `config: Config`.
- `RootManifest` also carries optional `created_at_millis` and
  `updated_at_millis` metadata. Older manifest bytes without these fields decode
  with both timestamps unset.
- `get_root` returns `None` when a name is absent.
- `put_root` unconditionally replaces the named root.
- `delete_root` is idempotent.
- `compare_and_swap_root` must apply the update only when the current manifest
  equals the expected manifest.
- `expected == None` means the name must be absent.
- `new == None` deletes the name after a successful compare.
- `ManifestStoreScan::list_roots` must return named manifests sorted by raw name
  bytes and must not include content nodes, hints, or other metadata.

Backends with transactions should implement compare-and-swap atomically. Backends
without atomic root updates should avoid implementing `ManifestStore` until they
can clearly document their concurrency semantics.

Built-in storage layout:

- `MemStore` keeps manifests in a separate in-memory map.
- `SqliteStore` keeps manifests in the `prolly_roots` table.
- `RocksDBStore` keeps manifests in the `prolly_roots` column family and guards
  compare-and-swap with the store instance's manifest lock.
- `PgliteStore` keeps manifests in the `prolly_roots` table and performs
  compare-and-swap inside the sidecar's SQL transaction.
- `SlateDbStore` keeps manifests under a dedicated `root:` key prefix and
  guards root mutations with the store instance's manifest lock.

`Prolly` exposes tree-oriented helpers for stores that implement
`ManifestStore`: `load_named_root`, `publish_named_root`, `delete_named_root`,
and `compare_and_swap_named_root`. These helpers convert between `Tree` and
`RootManifest` at the API boundary so application code does not need to manage
manifest encoding directly.

Manager-level publish and compare-and-swap helpers stamp manifest timestamps by
default. The `*_at_millis` variants accept explicit Unix-millisecond timestamps
for deterministic imports and tests. Tree-level compare-and-swap first compares
the current tree handle, then CASes against the exact current manifest so
metadata fields do not make normal tree CAS unusable.

Stores that implement `ManifestStoreScan` also support `list_named_roots`,
`load_retained_named_roots`, and retention policies over all roots, exact names,
prefixes, lexicographically newest names, and roots updated since a timestamp.
`NewestByName` is intentionally name-based; `UpdatedSince` requires
`updated_at_millis` metadata and skips roots without timestamps.

## Large Value Offload Contract

Large value offloading is a value-layer convention, not a node-format change.
Normal `put` and `get` continue to store and return raw leaf bytes. The
`put_large_value` helper applies a `LargeValueConfig` threshold:

- Values at or below `inline_threshold` are stored as raw leaf bytes, unless
  they start with the value-reference magic prefix.
- Raw values starting with the value-reference magic prefix are escaped as
  `ValueRef::Inline` so `get_large_value` can round-trip them safely.
- Values larger than `inline_threshold` are written to a `BlobStore` and stored
  in the tree as `ValueRef::Blob { cid, len }`.
- `get_large_value` resolves `ValueRef::Blob` through the provided blob store
  and validates the blob length and content CID before returning bytes.
- `BlobStore` is content-addressed by convention. `MemBlobStore` deduplicates
  identical blobs by CID and is intended for tests and lightweight use.
- `FileBlobStore` is the built-in durable local backend. It stores blobs under
  `blobs/sha256/aa/bb/<cid-hex>`, publishes completed writes by rename, validates
  content IDs on read, and implements `BlobStoreScan` for backend-listed GC
  candidates.

This keeps tree nodes small for large documents, embeddings, media, and agent
artifacts while preserving diff locality: tree diffs compare compact value
references, not full payload bytes.

Blob garbage collection is intentionally separate from node GC because
applications may share blobs across maps or external object stores. The blob GC
contract mirrors node GC:

- `mark_reachable_blobs` scans retained trees for reachable `ValueRef::Blob`
  envelopes and deduplicates by blob CID.
- `plan_blob_gc` accepts caller-supplied `BlobRef` candidates, keeps reachable
  candidates, validates present unreachable blob bytes against their reference,
  and reports missing candidates separately.
- `sweep_blob_gc` deletes exactly the reclaimable candidates from the plan.

Object-store integrations can supply candidates from bucket listings or a blob
index, while embedded applications can keep a local list of known blob
references. GC never assumes the blob namespace is private to a single tree.
The `AsyncBlobStore` surface follows the same contract for async/object-store
implementations and remains behind the runtime-neutral `async-store` feature.
Backends that can list their blob namespace implement `BlobStoreScan`, enabling
`plan_blob_store_gc` and `sweep_blob_store_gc`.

## Garbage Collection Contract

Generic garbage collection is explicit-candidate based because `Store` does not
require listing all stored node CIDs. This keeps custom stores small while still
giving applications a safe mark, dry-run, and sweep path when they can provide a
candidate set from a backend index, object-store listing, or previous
reachability scan.

Backends that can scan their node namespace implement `NodeStoreScan`.
`MemStore`, `SqliteStore`, `RocksDBStore`, `PgliteStore`, and `SlateDbStore`
implement it for store-wide `plan_store_gc` and `sweep_store_gc`.

GC invariants:

- Retained roots must be complete. The caller is responsible for loading every
  branch, checkpoint, workspace, and sync cursor that should survive.
- `mark_reachable` walks retained tree roots breadth-first, deduplicates shared
  subtrees, and returns live CIDs sorted by CID bytes.
- `plan_gc` must not mutate the store. It reports only caller-supplied
  candidates that are both unreachable and present in the store.
- Missing candidates are counted separately from reclaimable nodes and bytes.
- `sweep_gc` deletes exactly the reclaimable candidates from its plan with a
  store batch and clears the manager cache afterward.
- `NodeStoreScan` implementations must list only content-addressed node CIDs,
  excluding hints, root manifests, and other metadata.
- Store-wide helpers call `NodeStoreScan::list_node_cids` and then feed those
  candidates into the generic planner.
- `plan_store_gc_for_retention` and `sweep_store_gc_for_retention` first select
  retained roots from manifests and then call the store-wide GC helpers.
- Exact-name retention reports missing names. The GC convenience helpers fail
  with `Error::MissingNamedRoots` when exact names are absent rather than
  silently sweeping as if those roots did not exist.
- Time-window retention uses `NamedRootRetention::UpdatedSince`, which selects
  retained roots from manifest `updated_at_millis` metadata before calling the
  generic GC planner. Duration-style helpers construct the same policy from an
  explicit `now_millis`, keeping clock choice in the caller's control for
  tests, replay, and distributed sync.

## Async Store Contract

`AsyncStore` mirrors `Store` behind the `async-store` feature. It is for
object stores, remote peers, browser storage, network caches, and background
agents that need to overlap I/O.

The base `AsyncStore` trait intentionally does not require `Send` or `Sync` so
single-threaded browser or WASM stores can implement it. Native managers and
adapters can add stronger bounds where needed.

Async-specific expectations:

- Ordered batch reads preserve input order exactly.
- `read_parallelism` is a bound, not a target; keep it conservative.
- Stores with native multi-get should override `batch_get_ordered`.
- Blocking embedded stores should use a runtime-specific adapter such as
  `TokioBlockingStore` instead of blocking async runtime worker threads.
- The core async API stays runtime-neutral. Tokio support remains opt-in.

## Diff and Merge

Diff and merge are structural first:

- Equal root CIDs mean equal trees and can return immediately.
- Equal child CIDs let the engine skip entire subtrees.
- Matching child spans recurse structurally.
- Divergent boundaries fall back to ordered local collection and comparison for
  the affected span.

Standard merge uses delete-aware conflicts:

```rust
pub struct Conflict {
    pub key: Vec<u8>,
    pub base: Option<Vec<u8>>,
    pub left: Option<Vec<u8>>,
    pub right: Option<Vec<u8>>,
}
```

`None` means the key is absent on that side. This distinction is critical:
`Some(Vec::new())` is an empty byte value, not deletion.

Resolvers return:

- `Resolution::Value(bytes)` to upsert a value.
- `Resolution::Delete` to delete the key.
- `Resolution::Unresolved` to return `Error::Conflict`.

Structural merge may reuse unchanged subtrees and may resolve value-only
conflicts directly. Delete resolutions can fall back to the diff/batch merge
path so rebalancing remains correct.

CRDT merge uses the same `Conflict` shape, but custom CRDT functions return
`CrdtResolution` and cannot leave conflicts unresolved. Built-in CRDT strategies
must remain conflict-free.

Resolver callbacks should be deterministic and side-effect-free. Fast paths and
fallback paths may evaluate equivalent conflicts through different control flow.

## Encoding Compatibility

Node bytes are persisted data. The compact format with the `CRAB` magic header
is deterministic and fixture-tested. Legacy CBOR node bytes are still readable.

When changing encoding:

- Add fixture tests for the new bytes.
- Keep old fixtures readable or document a migration path.
- Update compatibility notes in the README.
- Assume new CIDs and roots may differ after re-encoding.

## Extension Checklist

Before merging a store, encoding, merge, or tree-shape change:

- Add or update conformance tests for every affected store trait behavior.
- Verify ordered batch reads with missing and duplicate keys.
- Verify empty byte values remain distinct from deletion.
- Run rustdoc with warnings denied.
- Compile examples and benchmark targets that exercise public APIs.
- Add README or rustdoc examples for new public behavior.
- Update this file when the invariant model changes.
