# Versioned Map Guide

`VersionedMap` is the application-facing lifecycle layer built on top of a
prolly tree. It combines immutable trees, content-addressed nodes, durable named
roots, and strict transactions into one handle that applications can use as a
normal ordered key/value map with history.

This guide explains the complete `VersionedMap` API, its consistency model,
how it relates to the lower-level `Prolly` engine, and how to use its advanced
features safely in production.

## Contents

- [Introduction](#introduction)
- [The correct mental model](#the-correct-mental-model)
- [What `VersionedMap` manages](#what-versionedmap-manages)
- [Store capabilities](#store-capabilities)
- [Opening and initializing a map](#opening-and-initializing-a-map)
- [Versions and version identity](#versions-and-version-identity)
- [Basic reads](#basic-reads)
- [Atomic writes](#atomic-writes)
- [Optimistic concurrency and conditional writes](#optimistic-concurrency-and-conditional-writes)
- [Pinned snapshots](#pinned-snapshots)
- [Historical reads and comparison](#historical-reads-and-comparison)
- [Proofs and verification](#proofs-and-verification)
- [Three-way merge](#three-way-merge)
- [Rollback](#rollback)
- [Portability and synchronization](#portability-and-synchronization)
- [Large values and blob offload](#large-values-and-blob-offload)
- [Retention and version pruning](#retention-and-version-pruning)
- [Integrity verification and garbage collection](#integrity-verification-and-garbage-collection)
- [Cache pinning and hot-prefix hints](#cache-pinning-and-hot-prefix-hints)
- [Ingestion and rebuild workflows](#ingestion-and-rebuild-workflows)
- [Atomic multi-map transactions](#atomic-multi-map-transactions)
- [Typed maps and schema validation](#typed-maps-and-schema-validation)
- [Schema migration](#schema-migration)
- [Change subscriptions](#change-subscriptions)
- [Async versioned maps](#async-versioned-maps)
- [Error handling](#error-handling)
- [Consistency and durability guarantees](#consistency-and-durability-guarantees)
- [Performance guidance](#performance-guidance)
- [Production checklist](#production-checklist)
- [Choosing a lower-level or higher-level API](#when-to-use-a-lower-level-or-higher-level-api)
- [Related documentation](#related-documentation)

## Introduction

The core `Prolly` API is intentionally explicit: an update receives a `Tree`
and returns another immutable `Tree`. That is ideal when an application wants
to manage branches, roots, or repository semantics itself. Most applications,
however, want a simpler abstraction:

- open a named map;
- read its current contents;
- atomically write one or more keys;
- retain immutable historical versions;
- compare, prove, export, merge, or roll back those versions;
- avoid manually coordinating nodes, tree handles, manifests, and transactions.

`VersionedMap` provides exactly that layer.

Add the package to the application. The package is named `prolly-map`, while
the Rust library import is named `prolly`:

```toml
[dependencies]
prolly-map = "0.2"
```

Here is a complete introductory example:

```rust
use prolly::{Config, MemStore, Prolly};

fn main() -> Result<(), prolly::Error> {
    let engine = Prolly::new(MemStore::new(), Config::default());
    let users = engine.versioned_map(b"users");

    // One atomic edit creates the first immutable version and advances head.
    let first = users.edit(|edit| {
        edit.put(b"user/1/name", b"Ada");
        edit.put(b"user/1/status", b"active");
        edit.put(b"user/2/name", b"Grace");
    })?;

    // Convenience updates automatically use a strict transaction and retry
    // optimistic transaction conflicts.
    let second = users.put(b"user/1/name", b"Ada Lovelace")?;

    assert_eq!(
        users.get(b"user/1/name")?,
        Some(b"Ada Lovelace".to_vec())
    );

    // Historical versions remain readable until retention removes them.
    assert_eq!(
        users.get_at(&first.id, b"user/1/name")?,
        Some(b"Ada".to_vec())
    );

    // Versions are content-derived tree states, so they can be compared.
    let changes = users.diff(&first.id, &second.id)?;
    assert_eq!(changes.len(), 1);

    // Rollback moves head to an existing immutable version. It does not erase
    // the newer version or manufacture a duplicate history entry.
    users.rollback_to(&first.id)?;
    assert_eq!(users.get(b"user/1/name")?, Some(b"Ada".to_vec()));

    Ok(())
}
```

The important point is that `users` is not a mutable tree. It is a durable
handle that resolves a mutable **head** to an immutable tree version. Every
successful content change publishes a new immutable version and atomically
moves the head.

## The Correct Mental Model

There are four related concepts:

| Concept | Meaning |
|---|---|
| `Prolly<S>` | The low-level engine operating on immutable `Tree` handles in store `S`. |
| `Tree` | One immutable ordered byte-key/byte-value snapshot. |
| `VersionedMap<'a, S>` | A named lifecycle facade with a current head and immutable version catalog. |
| `MapSnapshot<'a, S>` | A request-scoped view pinned to exactly one immutable map version. |
| `IndexedMap<'a, S>` | A strict coordinator for one source `VersionedMap` and its derived secondary indexes. |

A `VersionedMap` is not automatically a database secondary index. It can hold
an authoritative collection, a secondary index, a materialized view, or any
other ordered key/value state. “Versioned map” describes lifecycle and history;
“database index” describes an application's data model.

When the engine must maintain derived indexes synchronously and prevent writes
from bypassing maintenance, use `IndexedMap` rather than manually coordinating
separate `VersionedMap` heads. See [IndexedMap Secondary Indexes](secondary-index-design.md).

For example:

- `users`: authoritative map from `user_id` to user records;
- `users_by_email`: derived database index from normalized email to `user_id`;
- `active_user_count`: materialized view containing an aggregate.

All three may be `VersionedMap`s. Use a multi-map transaction to keep them
consistent, as described later in this guide.

## What `VersionedMap` Manages

For each application map ID, the facade manages:

1. A mutable named root for the current head.
2. An immutable named root for every cataloged version.
3. Strict transactional publication of new nodes, version root, and head.
4. Content-derived `MapVersionId` values.
5. Retention, verification, backup, synchronization, and maintenance helpers.

Application IDs are encoded before being placed below the reserved
`maps/versioned/` namespace. Arbitrary application bytes therefore cannot
escape the namespace or collide through path-like separators.

You can inspect the generated names when integrating operational tooling:

```rust
use prolly::{Config, MemStore, Prolly};

let engine = Prolly::new(MemStore::new(), Config::default());
let map = engine.versioned_map(b"orders/us-west");

assert_eq!(map.id(), b"orders/us-west");
println!("head root: {:?}", map.head_name());
println!("version prefix: {:?}", map.versions_prefix());
```

Applications should treat these root names as diagnostic information. Normal
reads and writes should use the `VersionedMap` methods.

## Store Capabilities

The API uses Rust trait bounds to expose only operations supported by the
backing store:

| Capability | Required trait |
|---|---|
| Tree node reads | `Store` |
| Head/version root reads | `ManifestStore` |
| Atomic writes | `TransactionalStore` |
| Listing versions and retention | `ManifestStoreScan` |
| Store node GC | `NodeStoreScan` |
| Large values | `BlobStore` |
| Blob GC | `BlobStoreScan` |

`MemStore` and `FileNodeStore` support the common managed-map path. Other
backends must implement the relevant traits for the operations they expose.

## Opening and Initializing a Map

Opening a handle does not create durable data:

```rust
use prolly::{Config, MemStore, Prolly};

let engine = Prolly::new(MemStore::new(), Config::default());
let settings = engine.versioned_map(b"settings");

assert!(!settings.is_initialized()?);
assert!(settings.head()?.is_none());

let empty_version = settings.initialize()?;
assert!(settings.is_initialized()?);
assert_eq!(settings.head_id()?, Some(empty_version.id));

```

`initialize()` publishes an empty tree if the map is absent. If the map already
has a head, it returns that head without creating a new version.

### Durable local example

`MemStore` is useful for tests and transient state. A durable store allows the
same map ID and version catalog to be reopened after process restart:

```rust
use prolly::{Config, FileNodeStore, Prolly};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "./application.prolly";

    {
        let engine = Prolly::new(FileNodeStore::open(path)?, Config::default());
        let settings = engine.versioned_map(b"settings");
        settings.put(b"theme", b"dark")?;
    }

    {
        let engine = Prolly::new(FileNodeStore::open(path)?, Config::default());
        let settings = engine.versioned_map(b"settings");
        assert_eq!(settings.get(b"theme")?, Some(b"dark".to_vec()));
        assert_eq!(settings.versions()?.len(), 1);
    }

    Ok(())
}
```

Use an application-controlled directory and lifecycle policy in production.
Do not have independent processes open a backend concurrently unless that
backend explicitly documents safe multi-process transactions.

## Versions and Version Identity

`MapVersion` contains:

- `id`: a stable `MapVersionId` derived from the complete tree manifest;
- `tree`: the immutable `Tree` handle;
- `created_at_millis`: catalog publication time when available;
- `is_head`: whether this catalog entry was head when loaded.

Version IDs identify **content states**, not write events. Repeating an
idempotent write does not create a distinct logical version. Rolling back to a
previous state reuses that state's ID.

Consequences:

- version IDs are suitable for optimistic concurrency tokens;
- equal map contents under the same tree configuration produce equal IDs;
- IDs do not encode author, message, parents, branch, or commit ancestry;
- a full Git-like repository belongs in a higher-level VCS layer.

List cataloged versions newest first:

```rust
let versions = settings.versions()?;
for version in versions {
    println!(
        "{} created={:?} head={}",
        version.id, version.created_at_millis, version.is_head
    );
}

```

## Basic Reads

Current-head convenience reads resolve the head once for each operation:

```rust
let value = settings.get(b"theme")?;
let exists = settings.contains_key(b"theme")?;

let values = settings.get_many(&[
    b"theme".as_slice(),
    b"locale".as_slice(),
    b"missing".as_slice(),
])?;

assert_eq!(exists, value.is_some());
assert_eq!(values.len(), 3);

```

`get_many` preserves input order, duplicate positions, and missing values. It
also allows a backend to use efficient batched node reads.

Range and prefix methods return lazy iterators:

```rust
let rows = settings
    .range(b"feature/", Some(b"feature0"))?
    .collect::<Result<Vec<_>, _>>()?;

let flags = settings
    .prefix(b"feature/")?
    .collect::<Result<Vec<_>, _>>()?;

assert_eq!(rows, flags);

```

Ranges are half-open: `[start, end)`. Prefix scans compute the correct byte
upper bound automatically.

## Atomic Writes

### Single-key writes

`put` and `delete` are convenience methods:

```rust
let written = settings.put(b"theme", b"dark")?;
assert_eq!(settings.get(b"theme")?, Some(b"dark".to_vec()));

let deleted = settings.delete(b"theme")?;
assert_ne!(written.id, deleted.id);
assert_eq!(settings.get(b"theme")?, None);

```

They automatically retry optimistic transaction conflicts up to the managed
map retry limit. Independent concurrent updates are therefore normally merged
by retrying the logical mutation batch against the latest head.

### Multi-key edits

Use `edit` to publish related mutations as one version:

```rust
let version = settings.edit(|edit| {
    edit.put(b"theme", b"dark");
    edit.put(b"locale", b"en-CA");
    edit.delete(b"deprecated-option");
})?;

println!("published {}", version.id);

```

The closure only collects mutations. Publication occurs after it returns. If
the transaction fails, none of the new head, immutable version root, or node
writes become visible.

Use `apply(Vec<Mutation>)` when mutations are already represented in the engine
type:

```rust
use prolly::Mutation;

let version = settings.apply(vec![
    Mutation::Upsert {
        key: b"theme".to_vec(),
        val: b"light".to_vec(),
    },
    Mutation::Delete {
        key: b"old-key".to_vec(),
    },
])?;

println!("published {}", version.id);

```

## Optimistic Concurrency and Conditional Writes

Automatic retry is useful when independent logical mutations can be replayed
on a newer head. Some application operations must instead reject a stale
caller. Use `put_if`, `delete_if`, `edit_if`, or `apply_if` for compare-and-swap
semantics.

```rust
use prolly::VersionedMapUpdate;

let expected = settings.head_id()?;

// Another request could update the map here.
let update = settings.put_if(expected.as_ref(), b"theme", b"solarized")?;

match update {
    VersionedMapUpdate::Applied { previous, current } => {
        println!("advanced {:?} -> {}", previous, current.id);
    }
    VersionedMapUpdate::Unchanged { current } => {
        println!("the requested content was already present: {:?}", current);
    }
    VersionedMapUpdate::Conflict { current } => {
        println!("stale caller; current head is {:?}", current);
    }
}

```

Passing `None` as the expected version means “the map must be uninitialized.”
This is useful for create-if-absent workflows.

`VersionedMapUpdate` also provides:

- `current()` to borrow the resulting or observed current version;
- `is_applied()` to test successful advancement;
- `is_conflict()` to test stale-head rejection.

## Pinned Snapshots

Calling several convenience reads independently permits head to advance between
those calls. For request-level consistency, pin a `MapSnapshot` once:

```rust
let snapshot = settings
    .snapshot()?
    .expect("settings must be initialized");

let version_id = snapshot.id().clone();
let theme = snapshot.get(b"theme")?;
let locale = snapshot.get(b"locale")?;

// Even if another writer advances settings here, both values above and every
// later read through snapshot use version_id.
println!("read {:?} and {:?} at {}", theme, locale, version_id);

```

Pin a historical version with `snapshot_at(&version_id)`.

### Snapshot point and boundary queries

Snapshots expose:

- `get`, `contains_key`, and `get_many`;
- `first_entry` and `last_entry`;
- `lower_bound`, returning the first key greater than or equal to a key;
- `upper_bound`, returning the first key strictly greater than a key;
- `cursor_window`, returning seek position plus a bounded forward page.

```rust
let first = snapshot.first_entry()?;
let last = snapshot.last_entry()?;
let lower = snapshot.lower_bound(b"feature/b")?;
let upper = snapshot.upper_bound(b"feature/b")?;
let window = snapshot.cursor_window(b"feature/b", None, 25)?;

println!("first={first:?} last={last:?}");
println!("lower={lower:?} upper={upper:?}");
println!("seek found={} entries={}", window.found, window.entries.len());

```

### Forward pagination

`RangeCursor` resumes strictly after its stored key:

```rust
use prolly::RangeCursor;

let mut cursor = RangeCursor::start();
loop {
    let page = snapshot.prefix_page(b"feature/", &cursor, 100)?;
    for (key, value) in &page.entries {
        println!("{:?} = {:?}", key, value);
    }

    match page.next_cursor {
        Some(next) => cursor = next,
        None => break,
    }
}

```

Because the snapshot is immutable, a cursor sequence cannot skip or duplicate
entries due to concurrent writes. Persist both the cursor and version ID if a
scan must resume across process or request boundaries.

### Reverse pagination and lazy reverse scans

```rust
use prolly::ReverseCursor;

let page = snapshot.prefix_reverse_page(
    b"feature/",
    &ReverseCursor::end(),
    100,
)?;

for (key, value) in page.entries {
    println!("descending: {:?} = {:?}", key, value);
}

// The lazy iterator fetches bounded reverse pages internally.
for entry in snapshot.prefix_reverse_scan(b"feature/", 128) {
    let (key, value) = entry?;
    println!("descending: {:?} = {:?}", key, value);
}

```

`reverse_scan(start, page_size)` scans `[start, +inf)` in descending order.
`prefix_reverse_scan(prefix, page_size)` limits the scan to one prefix.

### Streaming large ranges

`range`, `prefix`, `stream_range`, and `stream_prefix` are lazy. They do not
materialize the full result unless the caller collects it.

```rust
let mut processed = 0usize;
for entry in snapshot.stream_prefix(b"event/")? {
    let (key, value) = entry?;
    process_record(&key, &value);
    processed += 1;
}
println!("processed {processed} records");

```

### Statistics and debug views

```rust
let stats = snapshot.stats()?;
println!("entries: {}", stats.total_key_value_pairs);
println!("serialized bytes: {}", stats.total_tree_size_bytes);

let debug = snapshot.debug_view()?;
println!("tree levels: {}", debug.levels.len());

```

Statistics are suitable for monitoring growth and tuning. Debug views expose
tree structure and should primarily be used for diagnostics, inspection tools,
and tests rather than application behavior.

## Historical Reads and Comparison

For a single historical operation, use `get_at`, `get_many_at`, `range_at`,
`prefix_at`, `range_page_at`, or `prefix_page_at`.

For several repeatable operations, pin snapshots or create a `MapComparison`:

```rust
let old = settings.versions()?.last().unwrap().id.clone();
let current = settings.head_id()?.unwrap();
let comparison = settings.compare(&old, &current)?;

let diffs = comparison.diff()?;
let growth = comparison.stats()?;
let structure = comparison.debug_view()?;

println!("logical changes: {}", diffs.len());
println!("stats comparison: {growth:?}");
println!("structural comparison: {structure:?}");

```

`compare_to_head(&base)` pins a historical base and the current head in one
comparison handle.

### Streaming diff

```rust
for diff in comparison.stream_diff()? {
    match diff? {
        prolly::Diff::Added { key, val } => {
            println!("added {:?} = {:?}", key, val);
        }
        prolly::Diff::Removed { key, val } => {
            println!("removed {:?}, old {:?}", key, val);
        }
        prolly::Diff::Changed { key, old, new } => {
            println!("changed {:?}: {:?} -> {:?}", key, old, new);
        }
    }
}

```

The engine skips structurally equal subtrees. Streaming avoids collecting every
logical change before processing begins.

### Resumable and structural diff pages

Use `diff_page` when a durable key cursor is sufficient:

```rust
use prolly::RangeCursor;

let page = comparison.diff_page(&RangeCursor::start(), None, 500)?;
println!("changes in page: {}", page.diffs.len());

```

Use `structural_diff_page` when work should resume from the exact CID traversal
frontier. Its cursor preserves structural traversal state and is tied to the
specific base and target roots.

### Changed-span hints

Changed-span hints are correctness-optional acceleration metadata. A producer
can publish normalized key spans for an indexer or synchronization job, and a
consumer can load them later:

```rust
use prolly::ChangedSpan;

comparison.publish_changed_spans([
    ChangedSpan::new(b"user/100".to_vec(), Some(b"user/200".to_vec())),
])?;

if let Some(hint) = comparison.changed_spans()? {
    println!("hinted spans: {}", hint.spans.len());
}

```

Hints must never replace proof, diff, or tree traversal for correctness. Missing
or malformed hints are ignored by the engine.

## Proofs and Verification

Prolly tree roots authenticate their reachable node graph. A snapshot can
produce self-contained proofs that a verifier checks without opening the
original store.

### Key proofs

```rust
let proof = snapshot.prove_key(b"theme")?;
let verification = proof.verify();

assert!(verification.valid);
println!("verified value: {:?}", verification.value);

let multi = snapshot.prove_keys(&[
    b"theme".as_slice(),
    b"missing".as_slice(),
])?;
assert!(multi.verify().all_valid());

```

Absence proofs are supported. A valid proof can show that a requested key does
not exist at the authenticated root.

### Complete range and prefix proofs

```rust
let range_proof = snapshot.prove_range(b"feature/", Some(b"feature0"))?;
let range_verification = range_proof.verify();
assert!(range_verification.valid);

let prefix_proof = snapshot.prove_prefix(b"feature/")?;
assert!(prefix_proof.verify().valid);

```

These proofs authenticate completeness, not only the returned entries. The
verifier checks that no matching entry was omitted from the requested range.

### Cursor-page and diff-page proofs

```rust
use prolly::RangeCursor;

let proved_page = snapshot.prove_range_page(
    &RangeCursor::start(),
    None,
    100,
)?;
assert!(proved_page.proof.verify().valid);

let proved_diff = comparison.prove_diff_page(
    &RangeCursor::start(),
    None,
    100,
)?;
assert!(proved_diff.proof.verify().valid);

```

### Authenticated proof envelopes

A Merkle proof authenticates data against a root, but a recipient may also need
to authenticate who issued that root and bind it to a domain, validity window,
or nonce. `ProofAuthentication` builds an HMAC-SHA256 envelope:

```rust
use prolly::{
    verify_authenticated_proof_bundle,
    ProofAuthentication,
};

let proof = snapshot.prove_key(b"theme")?;
let envelope = snapshot.authenticate_proof_bundle(
    proof.to_bundle_bytes()?,
    b"shared-secret",
    ProofAuthentication::new(b"key-2026-07")
        .with_context(b"settings-api/v1")
        .with_validity(Some(1_700_000_000_000), Some(1_700_000_060_000))
        .with_nonce(b"request-42"),
)?;

let verified = verify_authenticated_proof_bundle(
    &envelope.to_bytes()?,
    b"shared-secret",
    Some(1_700_000_030_000),
)?;
assert!(verified.valid);

```

Use separate secrets and contexts for separate trust domains. A nonce only
prevents replay when the application records or otherwise validates nonce use.

## Three-Way Merge

`prepare_merge(base, candidate)` pins three immutable versions:

- `base`: the common application-selected ancestor;
- `head`: the current map head at preparation time;
- `candidate`: the version whose changes should be incorporated.

The map catalog is linear and does not calculate ancestry or a merge base. The
application must select `base` from its own workflow metadata.

```rust
let merge = settings.prepare_merge(&base_id, &candidate_id)?;

println!("base: {}", merge.base().id);
println!("pinned head: {}", merge.head().id);
println!("candidate: {}", merge.candidate().id);

let conflicts = merge
    .stream_conflicts()?
    .collect::<Result<Vec<_>, _>>()?;
println!("conflicts: {}", conflicts.len());

let update = merge.publish(None)?;
if update.is_conflict() {
    // Head changed after prepare_merge. Re-read state and prepare again.
}

```

`publish` performs a three-way merge and moves head only if the pinned head is
still current. The compare-and-swap prevents a merge prepared against stale
state from overwriting a concurrent writer.

### Merge policy registry

Use a `MergePolicyRegistry` when different keyspaces need different conflict
rules:

```rust
use prolly::{MergePolicyRegistry, Resolution};

let policies = MergePolicyRegistry::with_default(|conflict| {
    conflict
        .right
        .clone()
        .map(Resolution::value)
        .unwrap_or_else(Resolution::delete)
});

let update = merge.publish_with_policy(&policies)?;
assert!(!update.is_conflict());

```

The registry also supports exact-key, prefix, and pattern rules. Conflict
callbacks should be deterministic and side-effect free.

### CRDT merge

`crdt_merge` and `publish_crdt` use `CrdtConfig` for conflict-free strategies
such as last-writer-wins, multi-value preservation, delete policies, and custom
resolution:

```rust
use prolly::CrdtConfig;

let config = CrdtConfig::default();
let merged_tree = merge.crdt_merge(&config)?;
println!("candidate merged root: {:?}", merged_tree.root);

let update = merge.publish_crdt(&config)?;
assert!(!update.is_conflict());

```

CRDT strategy avoids unresolved logical conflicts; CAS can still report a
publication conflict if head moved after the merge was prepared.

## Rollback

Rollback moves head to an existing cataloged version:

```rust
let target = settings.versions()?.last().unwrap().id.clone();
let restored = settings.rollback_to(&target)?;
assert_eq!(restored.id, target);

```

Rollback does not:

- delete versions newer than the target;
- create a duplicate version for the same tree state;
- record parentage, a revert commit, author, or reason.

If audit semantics require a recorded revert event, store that metadata in an
application log or use a repository layer.

## Portability and Synchronization

### Export and import one version

Every pinned snapshot can be exported as a self-contained `SnapshotBundle`:

```rust
let bundle = snapshot.export()?;
let verification = bundle.verify()?;
assert!(verification.valid);

let destination_engine = Prolly::new(MemStore::new(), Config::default());
let destination = destination_engine.versioned_map(b"settings-copy");
let imported = destination.import_as_head(&bundle)?;

assert_eq!(imported.id, *snapshot.id());

```

Import verifies that the bundle is complete and uses the same tree
configuration as the destination engine. It imports nodes, catalogs the
version, and atomically makes it head.

### Missing-node planning and copying

For content-addressed synchronization, avoid sending nodes already present at
the destination:

```rust
let destination_store = MemStore::new();

let plan = snapshot.plan_missing_nodes(&destination_store)?;
println!("missing nodes: {}", plan.missing_nodes);

let copied = snapshot.copy_missing_nodes(&destination_store)?;
println!("copied nodes: {}", copied.copied_nodes);

```

Node copying alone does not publish a destination head. Use `push_to` when the
destination is another managed map:

```rust
let destination_engine = Prolly::new(MemStore::new(), Config::default());
let destination = destination_engine.versioned_map(b"settings-replica");

let pushed = snapshot.push_to(&destination)?;
assert_eq!(pushed.id, *snapshot.id());

```

### Full catalog backup and restore

`backup()` captures every cataloged version, original timestamps, and the head
selection:

```rust
let backup = settings.backup()?;
backup.verify()?;

let bytes = backup.to_bytes()?;
let decoded = prolly::VersionedMapBackup::from_bytes(&bytes)?;

let restore_engine = Prolly::new(MemStore::new(), Config::default());
let restore_map = restore_engine.versioned_map(b"settings");
let restored_head = restore_map.restore_backup(&decoded)?;

assert_eq!(restored_head.id, decoded.head);

```

Restore requires:

- a fully verified backup;
- the same application map ID;
- a compatible tree configuration;
- an uninitialized destination map.

The encoded backup format is versioned deterministic CBOR. Verification checks
bundle completeness, unique version IDs, tree/ID agreement, and head presence.

## Large Values and Blob Offload

Prolly tree leaf values are byte vectors. Large payloads can instead be stored
in a content-addressed blob store while the tree stores a compact `ValueRef`.

```rust
use prolly::{LargeValueConfig, MemBlobStore};

let blobs = MemBlobStore::new();
let policy = LargeValueConfig::new(4 * 1024);

let version = settings.put_large_value(
    &blobs,
    b"document/1/body",
    vec![b'x'; 64 * 1024],
    policy,
)?;

let body = settings.get_large_value(&blobs, b"document/1/body")?;
assert_eq!(body.unwrap().len(), 64 * 1024);
println!("published {}", version.id);

```

Values at or below the inline threshold remain inline. Larger values are
deduplicated in the blob store by content and represented by a blob reference.

On a snapshot, `get_value_ref` inspects whether a value is inline or blob-backed
without resolving the blob. `get_large_value` resolves either representation.

Conditional blob-aware publication is available through
`put_large_value_if`.

## Retention and Version Pruning

Catalog roots keep historical tree nodes reachable. Bound catalog growth with
one of the retention helpers:

```rust
use std::time::Duration;

// Keep the newest ten versions, plus head even if head is an older rollback.
let result = settings.keep_last(10)?;
println!("removed {} versions", result.removed_count());

// Keep versions from the last seven days, plus head.
let result = settings.keep_for(Duration::from_secs(7 * 24 * 60 * 60))?;

// Keep an explicit set, plus head. Unknown IDs are rejected.
if let Some(head) = settings.head_id()? {
    let result = settings.keep_versions([&head])?;
    assert!(result.retained.contains(&head));
}

```

`prune_versions(n)` is an alias for keeping the newest `n` versions plus head.
Pruning removes immutable **root names**, not node bytes. This separation makes
pruning fast and transactional. Run GC afterward to reclaim content no longer
reachable from any retained named root.

The current head is always protected, including after rollback to an older
version.

## Integrity Verification and Garbage Collection

### Catalog verification

```rust
let report = settings.verify_catalog()?;
println!("head: {}", report.head);
println!("versions: {}", report.version_count);
println!("reachable nodes: {}", report.reachable_nodes);
println!("reachable bytes: {}", report.reachable_bytes);

```

`verify_catalog` checks head membership, version IDs, manifests, and every
reachable node. Missing or corrupt content produces an error rather than a
partial success report.

### Node GC

```rust
let plan = settings.plan_gc()?;
println!("reclaimable nodes: {}", plan.reclaimable_nodes);

// Run only after inspecting or accepting the plan.
let sweep = settings.sweep_gc()?;
println!("deleted nodes: {}", sweep.deleted_nodes);

```

Although invoked through one map, node GC must use a store-wide safety
boundary. Content-addressed nodes can be shared between versions and maps.
`plan_gc` and `sweep_gc` therefore retain **every remaining named root** in the
store. They will not treat nodes used by another managed map, branch, tag, or
custom named root as garbage.

Do not call a store-wide sweep using only one map's `retention_policy()` when
other roots share that node store. The prefix policy is useful for selecting or
inspecting the map catalog; it is not by itself a safe global GC root set.

### Blob GC

```rust
let plan = settings.plan_blob_gc(&blobs)?;
println!("reclaimable blobs: {}", plan.reclaimable_blob_count);

let sweep = settings.sweep_blob_gc(&blobs)?;
println!("deleted blobs: {}", sweep.deleted_blobs);

```

Blob GC likewise retains blob references reachable from every remaining named
root because multiple maps may share one blob store.

## Cache Pinning and Hot-Prefix Hints

Long-running requests or latency-sensitive keyspaces can warm and pin relevant
paths:

```rust
let pinned_nodes = snapshot.pin_root()?;
println!("pinned root nodes: {pinned_nodes}");

let pinned_path = snapshot.pin_path(b"user/42")?;
println!("pinned path nodes: {pinned_path}");

snapshot.publish_prefix_hint(b"user/")?;
snapshot.hydrate_prefix_hint(b"user/")?;

```

Cache hints affect performance, not correctness. Pinned nodes may temporarily
exceed configured cache limits. Use bounded, measured pinning rather than
pinning every historical tree.

## Ingestion and Rebuild Workflows

### Sorted bulk initialization

When creating a large map from sorted input, avoid repeated point updates:

```rust
let engine = Prolly::new(MemStore::new(), Config::default());
let catalog = engine.versioned_map(b"catalog");

let update = catalog.initialize_sorted([
    (b"item/0001".to_vec(), b"keyboard".to_vec()),
    (b"item/0002".to_vec(), b"monitor".to_vec()),
    (b"item/0003".to_vec(), b"mouse".to_vec()),
])?;
assert!(update.is_applied());

```

Input must be in byte-lexicographic order. An out-of-order key returns
`Error::UnsortedInput` rather than creating an invalid tree.

### Append-optimized edits

For monotonically increasing keys such as event IDs or timestamps:

```rust
use prolly::Mutation;

let version = catalog.append(vec![
    Mutation::Upsert {
        key: b"item/0004".to_vec(),
        val: b"speaker".to_vec(),
    },
    Mutation::Upsert {
        key: b"item/0005".to_vec(),
        val: b"webcam".to_vec(),
    },
])?;
println!("append version: {}", version.id);

```

The engine uses its right-edge fast path when the mutation shape permits it and
falls back safely when it does not.

### Parallel batch updates

```rust
use prolly::{Mutation, ParallelConfig};

let result = catalog.parallel_apply(
    vec![
        Mutation::Upsert {
            key: b"item/0100".to_vec(),
            val: b"desk".to_vec(),
        },
        Mutation::Delete {
            key: b"item/0001".to_vec(),
        },
    ],
    &ParallelConfig::default(),
)?;

println!("version: {}", result.version.id);
println!("batch stats: {:?}", result.stats);

```

### Build first, then atomically replace

Rebuild helpers construct the complete candidate tree before moving head:

```rust
let expected = catalog.head_id()?;

let update = catalog.rebuild_sorted_if(
    expected.as_ref(),
    [
        (b"item/a".to_vec(), b"new-a".to_vec()),
        (b"item/b".to_vec(), b"new-b".to_vec()),
    ],
)?;

if update.is_conflict() {
    // The old map remains current. Decide whether to rebuild against the new
    // head or discard the completed candidate.
}

```

`rebuild_from_iter_if` accepts unsorted iterator input and uses the general
batch builder. Both methods CAS-publish only after the candidate tree is fully
built, so readers never observe a half-built replacement.

## Atomic Multi-Map Transactions

Real database indexes must change atomically with their source data. Use
`Prolly::versioned_maps_transaction` to update any number of managed maps in one
strict backend transaction:

```rust
let engine = Prolly::new(MemStore::new(), Config::default());

engine.versioned_maps_transaction(|maps| {
    // Authoritative record.
    maps.put(b"users", b"user/1/name", b"Ada")?;
    maps.put(b"users", b"user/1/email", b"ada@example.com")?;

    // Derived database index: normalized email -> user ID.
    maps.put(
        b"users_by_email",
        b"ada@example.com",
        b"user/1",
    )?;

    // Materialized view.
    maps.put(
        b"user_counts",
        b"total",
        1_u64.to_be_bytes(),
    )?;

    Ok(())
})?;

let users = engine.versioned_map(b"users");
let by_email = engine.versioned_map(b"users_by_email");

assert_eq!(users.get(b"user/1/name")?, Some(b"Ada".to_vec()));
assert_eq!(
    by_email.get(b"ada@example.com")?,
    Some(b"user/1".to_vec())
);

```

The transaction validates and commits all original heads, new tree nodes,
immutable version roots, and head movements together. If the closure returns an
error or the transaction conflicts, none of the participating maps advance.

Within the closure, `VersionedMapsTransaction` supports `head`, `get`, `apply`,
`apply_if`, `put`, `delete`, and `edit`. Reads observe writes already staged by
the same transaction.

This is the recommended path for strict secondary indexes. Diff-driven
asynchronous indexers remain useful when eventual consistency is acceptable or
the derived engine cannot participate in the same transaction.

## Typed Maps and Schema Validation

The core map stores byte keys and values. `TypedVersionedMap<K, V, KC, VC>`
keeps application encoding policy near the map.

`StringKeyCodec` maps Rust strings to UTF-8 bytes. `BytesKeyCodec` is the
identity codec for `Vec<u8>`. Applications can implement `KeyCodec<K>` for
order-preserving numeric, tuple, or domain-specific keys.

```rust
use prolly::{
    Config, MemStore, Prolly, StringKeyCodec, VersionedJsonCodec,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct User {
    display_name: String,
    active: bool,
}

let engine = Prolly::new(MemStore::new(), Config::default());
let raw = engine.versioned_map(b"typed-users");
let users = raw.typed::<String, User, _, _>(
    StringKeyCodec,
    VersionedJsonCodec::new("app.User", 2),
);

users.put(
    &"user/1".to_string(),
    &User {
        display_name: "Ada".to_string(),
        active: true,
    },
)?;

let user = users.get(&"user/1".to_string())?.unwrap();
assert_eq!(user.display_name, "Ada");

let all = users.entries()?;
assert_eq!(all.len(), 1);

```

`VersionedJsonCodec` and `VersionedCborCodec` wrap each value with schema name,
schema version, encoding, and payload. Decode rejects a different schema or
version before deserializing the application type. Plain `JsonCodec` and
`CborCodec` are available when an envelope is unnecessary.

The typed facade deliberately exposes `raw()` for advanced snapshot, proof,
merge, backup, and maintenance operations.

### Custom key codecs

A `KeyCodec` must preserve the ordering expected by the application:

```rust
use prolly::{Error, KeyCodec};

#[derive(Clone, Copy)]
struct U64KeyCodec;

impl KeyCodec<u64> for U64KeyCodec {
    fn encode_key(&self, key: &u64) -> Result<Vec<u8>, Error> {
        Ok(key.to_be_bytes().to_vec())
    }

    fn decode_key(&self, bytes: &[u8]) -> Result<u64, Error> {
        let array: [u8; 8] = bytes.try_into().map_err(|_| {
            Error::Deserialize("u64 key must contain eight bytes".to_string())
        })?;
        Ok(u64::from_be_bytes(array))
    }
}
```

Big-endian unsigned integers preserve numeric order under byte-lexicographic
comparison. Signed integers and compound keys require an order-preserving
encoding rather than a naive platform representation.

## Schema Migration

`migrate_from` reads one explicitly pinned source version with an old codec,
transforms every value, encodes it with the new codec, and CAS-publishes the
result only if the source version is still head.

```rust
use prolly::{StringKeyCodec, VersionedJsonCodec};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct UserV1 {
    display_name: String,
}

#[derive(Serialize, Deserialize)]
struct UserV2 {
    display_name: String,
    active: bool,
}

let source_codec = VersionedJsonCodec::new("app.User", 1);
let source = raw.typed::<String, UserV1, _, _>(
    StringKeyCodec,
    source_codec.clone(),
);
let source_head = source.raw().head_id()?.unwrap();

let target = raw.typed::<String, UserV2, _, _>(
    StringKeyCodec,
    VersionedJsonCodec::new("app.User", 2),
);

let result = target.migrate_from::<UserV1, _>(
    &source_head,
    &source_codec,
    |old| {
        Ok(UserV2 {
            display_name: old.display_name,
            active: true,
        })
    },
)?;

println!("scanned: {}", result.scanned_values);
println!("rewritten: {}", result.rewritten_values);

if result.update.is_conflict() {
    // A writer changed head while migration was running. No partial migrated
    // head was published; restart or reconcile explicitly.
}

```

The migration is whole-map and atomic at publication. For very large datasets,
build a new map or external checkpointed pipeline, validate it, and switch
consumers with an application-level pointer or coordinated transaction.

## Change Subscriptions

Subscriptions are resumable, explicit polling handles. They do not create a
background thread and do not require a runtime.

### Observe future changes

```rust
let mut subscription = settings.subscribe()?;

// No change since subscription began.
assert!(subscription.poll()?.is_none());

settings.put(b"theme", b"dark")?;

if let Some(event) = subscription.poll()? {
    println!("previous: {:?}", event.previous);
    println!("current: {}", event.current.id);
    println!("diff count: {}", event.diffs.len());
}

```

`subscribe()` begins at the current head and emits only later transitions.
`subscribe_from(Some(version_id))` resumes from a persisted checkpoint. Passing
`None` compares the first observed head with an empty tree.

Polling compares the last observed version directly with current head. It may
coalesce several intermediate head movements into one logical diff. Use the
version catalog or an application event log when every transition must be
processed individually.

If the resume version was pruned, polling returns an error instead of silently
calculating a different change set.

## Async Versioned Maps

Async storage is available without a Cargo feature for remote, browser, and
async-native stores:

```toml
[dependencies]
prolly-map = "0.3"
```

The Rust import name remains `prolly`.

The following example adapts a synchronous memory store for demonstration:

```rust
use std::sync::Arc;
use prolly::{AsyncProlly, Config, MemStore, RangeCursor, SyncStoreAsAsync};

let store = Arc::new(MemStore::new());
let engine = AsyncProlly::new(
    SyncStoreAsAsync::new(store),
    Config::default(),
);
let users = engine.versioned_map(b"users");

let first = users.put(b"user/1", b"Ada").await?;
users.edit(|edit| {
    edit.put(b"user/2", b"Grace");
    edit.put(b"user/3", b"Margaret");
}).await?;

let historical = users.snapshot_at(&first.id).await?.unwrap();
assert_eq!(historical.get(b"user/2").await?, None);

let snapshot = users.snapshot().await?.unwrap();
let page = snapshot
    .prefix_page(b"user/", &RangeCursor::start(), 100)
    .await?;
assert_eq!(page.entries.len(), 3);

let proof = snapshot.prove_key(b"user/1").await?;
assert!(proof.verify().valid);
```

`AsyncVersionedMap` supports current and historical heads, pinned snapshots,
point reads, range/prefix streams, cursor pages, statistics, key/multi-key/range
proofs, atomic mutation batches, convenience put/delete/edit, and change
subscriptions.

Async subscription polling mirrors the synchronous API:

```rust
let mut subscription = users.subscribe().await?;
if let Some(event) = subscription.poll().await? {
    println!("new async head: {}", event.current.id);
}
```

## Error Handling

All managed-map methods return the crate's `Error` type. Important failure
categories include:

- store or manifest I/O errors;
- transaction conflicts after retry exhaustion;
- explicit `VersionedMapUpdate::Conflict` from conditional operations;
- unknown, pruned, or mismatched version IDs;
- invalid or incomplete snapshot/backup data;
- tree configuration mismatch during import or restore;
- schema/version mismatch during typed decode;
- missing blob content;
- corrupt or missing reachable nodes;
- unsorted input supplied to a sorted builder.

Treat conditional conflicts differently from infrastructure failures:

```rust
let expected = settings.head_id()?;
let update = settings.put_if(expected.as_ref(), b"theme", b"dark")?;

if update.is_conflict() {
    // Normal application concurrency outcome: return HTTP 409, show a merge
    // UI, reload, or retry only if the business operation is replay-safe.
} else {
    // Applied or already unchanged.
}

```

Do not blindly retry a stale conditional operation. The caller selected an
expected version precisely so it could decide how to reconcile newer state.

## Consistency and Durability Guarantees

For a store implementing the required strict transaction traits, a successful
content-changing managed-map publication makes the following visible together:

1. All new content-addressed tree nodes.
2. The immutable version root.
3. The new mutable head root.

Readers therefore observe either the previous head or the complete new head,
never a partially published tree.

Additional guarantees:

- old tree versions are immutable;
- head reads are atomic at the manifest level;
- pinned snapshots remain on one version while head moves;
- conditional publication validates expected head;
- merge and rebuild publication use CAS;
- backup/import verification validates content completeness and identity.

Durability after process or machine failure depends on the selected store's
own persistence guarantees. `MemStore` is process-local. Use a durable backend
for durable application state.

## Performance Guidance

Choose the narrowest API that matches the workload:

| Workload | Recommended API |
|---|---|
| One independent write | `put` or `delete` |
| Related writes in one map | `edit` or `apply` |
| Caller-supplied concurrency token | `put_if`, `edit_if`, or `apply_if` |
| Consistent request reads | `snapshot` |
| Long scan | `stream_range` or `stream_prefix` |
| User-facing pages | pinned `range_page` or `prefix_page` |
| New sorted dataset | `initialize_sorted` |
| Monotonic event ingestion | `append` |
| Large mutation set | `parallel_apply` |
| Offline full replacement | `rebuild_sorted_if` or `rebuild_from_iter_if` |
| Source plus strict derived indexes | `versioned_maps_transaction` |
| Eventual derived index | subscription/diff worker with a persisted version ID |
| Large payloads | blob-aware put/get |

Avoid repeatedly calling current-head range helpers during one logical page
sequence. Pin a snapshot so concurrency cannot change the dataset between
pages.

Avoid retaining unlimited versions by accident. Monitor catalog count and
reachable bytes, establish a retention policy, verify, then run store-safe GC.

## Production Checklist

Before shipping a `VersionedMap` integration:

1. Define byte-order-preserving key formats and prefix boundaries.
2. Decide whether values are raw, JSON/CBOR, schema-versioned, or blob-backed.
3. Choose which maps are authoritative and which are derived database indexes.
4. Use multi-map transactions for derived state requiring strict consistency.
5. Use conditional writes for business operations that must detect stale data.
6. Pin snapshots for multi-read requests, scans, proofs, and pagination.
7. Persist both version ID and cursor for resumable work.
8. Set version retention intentionally and always protect required audit roots.
9. Run `verify_catalog` before destructive maintenance or after restore.
10. Use map-triggered GC helpers so all named roots in a shared store survive.
11. Back up and verify the complete catalog, not only the current head, when
    historical recovery matters.
12. Test store reopen, transaction conflicts, missing blobs, pruned resume
    versions, migration conflicts, and corrupted import data.
13. Record application audit metadata separately if authorship, messages,
    parentage, or every transition matters.

## When to Use a Lower-Level or Higher-Level API

Use raw `Prolly` and `Tree` handles when:

- the application deliberately manages many temporary branches;
- roots are ephemeral or managed in another transactional system;
- custom publication semantics are required;
- the application is implementing another storage abstraction.

Use `VersionedMap` when:

- one named map has a current authoritative head;
- linear version history and rollback are sufficient;
- application code should not manually coordinate manifests and nodes;
- proofs, synchronization, retention, typed values, or managed maintenance are
  desired from one handle.

Use a repository/VCS layer when:

- commits need parents and ancestry;
- branches, tags, reflogs, author, message, and signatures are domain concepts;
- merge-base calculation or fast-forward policy is required;
- history represents events rather than unique content states.

`VersionedMap` intentionally remains a focused bridge between the immutable
tree engine and common application state management. That boundary keeps the
simple path small while still exposing the engine's structural sharing, diff,
merge, proof, sync, ingestion, and maintenance capabilities when applications
need them.

## Related Documentation

- [Getting Started](getting-started.md) introduces the immutable tree API and
  package setup.
- [Guides](guides.md) covers key construction, values, stores, roots, sync, and
  general operational recipes.
- [Versioned Secondary Indexes](secondary-index-design.md) goes deeper on strict
  source/index coordination, historical index reads, and index lifecycle.
- [Async Store](async-store.md) explains async-native stores, adapters, and
  remote/browser integration.
- [Architecture](architecture.md) shows how trees, nodes, stores, manifests,
  diff, merge, and synchronization fit together.
- [Wire Format](wire-format.md) documents deterministic node and manifest
  encoding contracts.
- [prolly-vcs Design](prolly-vcs-design.md) describes the proposed higher-level
  repository model for commits, ancestry, branches, tags, and reflogs.
