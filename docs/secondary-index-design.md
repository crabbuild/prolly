# IndexedMap Secondary Indexes

`IndexedMap` is the strict synchronous secondary-index layer for a
`VersionedMap`. It is available in the Rust core API and supports non-unique,
sparse, multi-valued indexes with `KeysOnly`, `Include`, and `All` projections.

The source map remains authoritative. Each active index is a hidden immutable
prolly tree, and a versioned catalog associates one exact source version with
the exact index versions derived from it.

## Consistency model

Source changes, index changes, catalog checkpoints, immutable version roots,
and mutable heads commit in one `TransactionalStore` transaction. A successful
write exposes all of them; a conflict or extractor error exposes none.

Reads begin by pinning one catalog version. The resulting `IndexedSnapshot`
then opens only the source and index versions named by that catalog. Concurrent
writes cannot produce a torn source/index read.

Once an index is active, head-changing raw `VersionedMap` operations return
`Error::IndexesRequireIndexedMap`. This includes ordinary writes, rebuilds,
imports, restores, rollbacks, merge publication, multi-map publication, and raw
version pruning. Read-only access remains available. Operations without an
index-aware implementation return `IndexOperationUnsupported`.

Strict indexing requires a backend with real `TransactionalStore` support.

## Runtime definitions

Definitions use deterministic Rust callbacks. An extractor receives the
primary key and exact stored source bytes and emits zero or more terms:

```rust
use prolly::{SecondaryIndex, SecondaryIndexRegistry};

let by_tag = SecondaryIndex::non_unique(
    "by-tag",
    1,
    "app.users.by-tag/v1",
    |_primary_key, value| {
        Ok(value
            .split(|byte| *byte == b',')
            .filter(|tag| !tag.is_empty())
            .map(ToOwned::to_owned)
            .collect())
    },
)?;

let registry = SecondaryIndexRegistry::new().register(by_tag)?;
# Ok::<(), prolly::Error>(())
```

Zero terms make an index sparse. Multiple terms support fields such as tags.
Exact duplicate emissions are deduplicated. Conflicting projected values for
one physical `(term, primary_key)` entry are rejected.

The extractor ID and generation are persisted as semantic identity. Callback
code is not serialized. Every process opening an active index must register an
exact matching definition.

Extractors must be deterministic, side-effect free, and retry safe. Optimistic
write and build conflicts may execute them more than once. Panics are not an
error protocol; return `SecondaryIndexError` for invalid records.

## Projection modes

| Mode | Physical value | Read behavior | Cost |
| --- | --- | --- | --- |
| `KeysOnly` | Empty | Returns primary keys; `records` batch-loads source | Lowest write/storage amplification |
| `Include` | Extractor-supplied bytes | `projected` is index-only | Application-controlled covering data |
| `All` | Exact source value bytes | `projected` returns full stored record | Highest amplification; bounded by limits |

`All` copies the raw source-tree bytes. It does not resolve or duplicate an
external blob payload. Projection-only changes rewrite `Include` or `All`
entries even when their terms stay unchanged. `KeysOnly` skips physical index
writes when old and new emissions are identical.

## Creating indexes after population

Opening a coordinator does not require an empty source:

```rust,ignore
let users = engine.indexed_map(b"users", registry)?;
users.ensure_index(b"by-tag")?;
```

`ensure_index` pins the source, shadow-builds a sorted hidden tree, and then
atomically activates it only if the source/catalog/control selection is still
current. A conflict discards the unpublished root selection and retries from a
fresh snapshot up to `max_build_retries`. Content-addressed nodes written by a
failed build are unreferenced and safe for later GC.

The operation is idempotent for an already-active exact generation. Use
`replace_index` for a new generation.

## Writes and queries

Use `put`, `delete`, `apply`, `apply_if`, or `edit` on `IndexedMap`. Mutation
batches are normalized by primary key, old source values are batch-read once,
and only changed physical emissions are applied.

```rust,ignore
users.edit(|edit| {
    edit.put(b"user-1", encoded_user);
    edit.delete(b"user-2");
})?;

let snapshot = users.snapshot()?;
let by_tag = snapshot.index(b"by-tag")?;
let matches = by_tag.exact(b"database")?;
let keys = by_tag.primary_keys(b"database")?;
let records = by_tag.records(b"database")?;
let projected = by_tag.projected(b"database")?;
```

Indexes support exact terms, arbitrary-byte term prefixes, half-open term
ranges, and forward/reverse pages. A serialized cursor is bound to source,
catalog, index version, definition fingerprint, direction, and logical bounds.
Using it with another snapshot or query returns
`IndexCursorVersionMismatch`.

`records` performs one ordered `get_many` against the pinned source and treats
a missing primary key as checkpoint corruption. `projected` never reads the
source.

Historical reads use `snapshot_at(source_version)` with current generations or
`snapshot_by_id(IndexedSnapshotId)` for an exact source/catalog pair. They never
substitute a different index version or scan the source as a fallback.

## Lifecycle

- `health` performs bounded structural validation at open time.
- `verify_index` and `verify_all` rebuild from retained source data, compare
  every logical entry, and report expected/actual roots and entry counts
  without changing named roots. `is_valid` means semantic equality;
  `is_canonical` additionally means structural identity with the sorted rebuild.
- `repair_index` publishes a deterministic rebuilt root and corrected
  checkpoint atomically. Queries never repair implicitly.
- `replace_index` requires a greater generation and different fingerprint. It
  shadow-builds while the old generation remains readable, then atomically
  swaps catalog/control selection and marks the old descriptor retired.
- `deactivate_index` removes only the active selection. Historical records and
  roots remain until retention. Deactivating the final index removes the write
  fence.

All extraction, projection, build, verification, temporary-memory, retry, and
bundle work is bounded by `SecondaryIndexLimits`. Exceeding a bound returns
`IndexResourceLimitExceeded` before publication.

## Retention, GC, and transfer

`keep_last(n)` always keeps the current source, even for `n == 0`. Every kept
source checkpoint transitively retains its exact index version, including a
retired generation still referenced by retained history. Checkpoint records
and unreferenced immutable root names are removed in one transaction.

Root pruning does not delete content-addressed nodes. `plan_indexed_gc` plans
from every remaining named root in the shared store. GC must be operationally
serialized with readers or use an external live-root lease; cache pinning alone
is not a GC lease.

`export_current` creates one deterministic, verified bundle with the current
source, catalog, control, descriptors, checkpoints, index trees, and a globally
deduplicated CID-sorted node set. `import_current` validates the whole bundle
and runtime definitions before atomically staging nodes and publishing roots,
conditioned on the expected destination source version.

## DynamoDB GSI/LSI analogy

The analogy is conceptual; the Rust API deliberately does not expose GSI or
LSI types.

- A DynamoDB global secondary index can use a different partition key and has
  independent distributed capacity/placement. A prolly secondary index is
  similar in allowing an arbitrary derived term, but it is local to the same
  transactional store and is strictly synchronous.
- A DynamoDB local secondary index shares the table partition key and changes
  the sort key, with creation and item-collection constraints. A prolly index
  can model a composite term such as `(tenant, status)`, but it has no special
  shared-partition primitive and can be added after population with
  `ensure_index`.

Therefore an index over a global attribute is GSI-like, while an index whose
term begins with a tenant/partition component is LSI-like. Both use the same
`SecondaryIndex` and `IndexedMap` implementation.

## Deliberate v1 exclusions

V1 excludes unique indexes, asynchronous/deferred maintenance, derive macros,
FFI bindings, online guaranteed-progress builds, independent index dropping,
definition upgrades in place, indexed merge/rollback publication, and
policy-rich historical backup/restore. Unsupported raw paths fail closed.

See [the complete design specification](superpowers/specs/2026-07-13-indexed-map-secondary-index-design.md)
for persisted layouts, algorithms, and acceptance criteria, and run
`cargo run --example secondary_index` for an end-to-end example.
