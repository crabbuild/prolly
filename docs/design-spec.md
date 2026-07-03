# Design Spec

This document describes the behavioral contract for `prolly-map`. It is the
starting point for compatibility tests and future language ports.

## Scope

The spec covers:

- byte-key ordering;
- immutable tree snapshots;
- node content addressing;
- content-defined tree shape;
- store behavior;
- diff and merge semantics;
- named roots;
- large value references;
- garbage collection;
- async storage expectations;
- cross-language compatibility.

The Rust implementation is currently authoritative. Future implementations in
Python or other languages should conform to this spec and to conformance
fixtures produced from the Rust crate.

## Key Ordering

Keys are byte strings.

Ordering is lexicographic by unsigned byte value:

```text
b"a" < b"aa" < b"b"
b"\x00" < b"\x01" < b"\xff"
```

No Unicode normalization, locale collation, numeric parsing, or string
case-folding is applied by the tree. Applications that need those behaviors
must encode keys accordingly.

Duplicate keys are not allowed inside a logical tree. Later mutations for the
same key replace or delete the previous value.

## Values and Deletion

Values are byte strings.

Deletion is represented by absence from the logical map, not by a sentinel
value. Empty byte values are valid values:

```text
Some(Vec::new()) != None
```

Merge conflicts must preserve this distinction.

## Tree Snapshot

A `Tree` contains:

- `root: Option<Cid>`;
- `config: Config`.

`root == None` means the tree is empty.

Mutations do not modify an existing tree in place. They return a new `Tree`
that points to a new root, or `None` if the result is empty.

Implementations must not mutate previously published nodes.

## Node Content Addressing

A node CID is:

```text
SHA-256(serialized_node_bytes)
```

The serialized node bytes must be deterministic for the same node content.

The CID does not include:

- store key prefixes;
- named root names;
- cache hints;
- local write time;
- backend metadata.

If two implementations serialize the same logical node differently, their CIDs
will differ. Cross-language portability therefore requires shared encoding
fixtures before non-Rust implementations are considered compatible.

## Tree Shape

Tree shape is content-defined. Implementations use configuration values:

- `min_chunk_size`;
- `max_chunk_size`;
- `chunking_factor`;
- `hash_seed`;
- `encoding`;
- node cache limits, which do not affect logical content.

The boundary function must be deterministic. Given the same sorted key/value
content and shape-affecting config, the implementation should produce the same
logical tree shape and root CID.

Node cache settings must not affect CIDs.

## Store Contract

`Store` is a byte key-value interface used by the Rust synchronous manager.

Required behavior:

- `get(key)` returns `Ok(Some(bytes))` for present keys.
- `get(key)` returns `Ok(None)` for absent keys.
- `put(key, value)` inserts or replaces bytes.
- `delete(key)` removes a key and succeeds if the key is already absent.
- `batch(ops)` applies a sequence of upserts and deletes.

Atomicity:

- Stores should make `batch` atomic when the backend supports it.
- If a backend cannot provide atomic batches, it must document that behavior.

Content-addressed nodes:

- Node bytes are stored under CID bytes.
- Writing the same CID multiple times must be safe.
- Stores may deduplicate identical writes.

Optional behavior:

- batch reads;
- ordered batch reads;
- unique ordered batch reads;
- batch puts;
- performance hints;
- node CID scans for GC.

Performance hints are not part of tree correctness. If hints are missing or
stale, the manager must have a correct fallback path.

## Async Store Contract

`AsyncStore` mirrors the sync store contract with async methods.

The base trait does not require Tokio. It is designed for:

- object stores;
- remote peers;
- browser/WASM storage;
- network caches;
- background workers.

Async stores should override `batch_get_ordered` when they have a native
multi-get API. Stores with only async point reads can use `read_parallelism` to
overlap requests.

Async APIs must preserve the same logical behavior as sync APIs.

## Named Root Contract

Named roots map byte names to tree snapshots.

Required behavior:

- loading a missing root returns `None`;
- publishing a root records its tree handle and metadata;
- deleting a root removes the name, not the nodes;
- compare-and-swap updates apply only if the expected root matches.

Named roots are the recommended way to store application heads. Root names are
application-defined byte strings.

CAS is the concurrency primitive. If CAS fails, callers should reload and
retry, merge, or surface a conflict to the application.

## Diff Contract

Diff compares two snapshots and returns logical changes:

- added key/value;
- removed key/value;
- modified key from old value to new value.

Implementations may use structural pruning by comparing CIDs. They must still
produce the same logical diff as a full ordered map comparison.

Range diff restricts comparison to the provided key range.

Resumable diff must not skip or duplicate changes when resumed from a cursor
generated by the same implementation and tree pair.

## Standard Merge Contract

Standard three-way merge uses `base`, `left`, and `right`.

Automatic cases:

- same result on both sides: keep that result;
- only left changed: keep left;
- only right changed: keep right;
- disjoint key changes: apply both.

Conflict cases include:

- both sides changed the same base value differently;
- both sides added different values for an absent key;
- one side deleted while the other updated.

Conflicts are represented as:

```rust
pub struct Conflict {
    pub key: Vec<u8>,
    pub base: Option<Vec<u8>>,
    pub left: Option<Vec<u8>>,
    pub right: Option<Vec<u8>>,
}
```

`None` means absent on that side.

Resolvers return:

```rust
pub enum Resolution {
    Value(Vec<u8>),
    Delete,
    Unresolved,
}
```

Effects:

- `Value(bytes)` upserts the key with `bytes`;
- `Delete` removes the key;
- `Unresolved` returns `Error::Conflict`.

Resolvers should be deterministic and side-effect free. Structural merge may
evaluate equivalent conflict logic through different paths, and fallback paths
should produce the same result.

## CRDT Merge Contract

CRDT-style merge uses the same conflict shape but must not return unresolved
conflicts.

Custom CRDT functions return:

```rust
pub enum CrdtResolution {
    Value(Vec<u8>),
    Delete,
}
```

Built-in strategies:

- last-writer-wins;
- multi-value.

Built-in delete/update conflicts are governed by `DeletePolicy`:

- `DeleteWins`;
- `UpdateWins`.

Built-in CRDT strategies must not return `Error::Conflict`.

## Large Value Contract

Large value offload stores either inline bytes or a reference:

```text
ValueRef::Inline(bytes)
ValueRef::Blob(BlobRef)
```

A blob reference should contain enough information to retrieve and validate the
payload. Blob bytes may live in a different store from tree nodes.

The tree remains the source of truth for which blobs are reachable from a
snapshot. Blob GC must trace retained tree roots and sweep only unreachable
blob references.

## GC Contract

GC is reachability based.

Inputs:

- retained tree roots;
- candidate node CIDs or blob refs.

Outputs:

- reachable set;
- sweep set.

GC must never require mutation of retained nodes. Sweeping is a store-level
delete of unreachable content.

Multi-writer applications must coordinate GC with root retention. A root that
can still be published, loaded, or referenced by another process must be part
of the retention set.

## Error Contract

Errors should distinguish:

- store failures;
- encoding or decoding failures;
- missing required nodes;
- merge conflicts;
- invalid input;
- backend-specific failures.

Application-level resolver failures should generally map to
`Resolution::Unresolved` for standard merge or a domain-specific value for
CRDT merge.

## Compatibility Levels

Compatibility has several levels:

1. API compatibility: code using public Rust APIs continues to compile.
2. Logical compatibility: operations return the same ordered map content.
3. Structural compatibility: roots and CIDs match for the same content.
4. Wire compatibility: persisted stores can be read by another version or
   language.

For `0.1`, the project should prioritize clear APIs and logical correctness.
Before declaring cross-language compatibility, the project needs fixtures for
node bytes, CIDs, roots, diffs, merges, manifests, and blob refs.

## Cross-Language Requirements

A non-Rust implementation should not claim compatibility until it can:

- sort byte keys exactly like Rust;
- encode nodes byte-for-byte for fixture cases;
- compute the same CIDs;
- build the same roots for fixture datasets;
- read and write compatible manifests;
- preserve delete/absence semantics in conflicts;
- run merge fixture tests;
- round-trip value envelopes used by common codecs;
- pass store conformance tests for missing, present, batch, and delete cases.

The language port may expose idiomatic APIs, but the storage contract must stay
byte-compatible where compatibility is promised.

