# Object-Store VCS Design

This document designs the `prolly-map` library changes needed to support a
production Git-like version-control system whose durable substrate is an object
store. It focuses on the library-level infrastructure: object-store-backed
nodes, blobs, manifests, distributed ref updates, sync, and garbage collection.

The broader CrabDB product design can build on these primitives. This document
does not replace `docs/design/distributed-prolly-vcs.md`; it narrows the problem
to the reusable `prolly-map` storage and concurrency contracts.

## Status

Proposed.

## Problem Statement

`prolly-map` already gives applications immutable ordered maps, content-addressed
nodes, structural diff, three-way merge, named roots, large-value blobs, and
GC. That is enough for local-first and embedded uses.

For distributed version control over S3, R2, GCS, Azure Blob Storage, or a local
object-store-compatible filesystem, two additional guarantees are required:

- immutable objects must be stored and fetched efficiently from object storage;
- mutable branch heads must move with a real distributed compare-and-swap (CAS).

The current `SlateDbStore` can run on an object store, but its named-root CAS is
guarded by an in-process mutex. That is correct for one process. It is not, by
itself, a distributed branch-ref lock for independent writers.

The design goal is to make this distinction explicit:

```text
immutable object plane:
  nodes, blobs, commit objects, tag objects

mutable ref plane:
  refs/heads/main, refs/tags/v1.0.0, remote-tracking refs
```

Immutable data can be uploaded idempotently. Mutable refs require conditional
updates, leases, a single-writer coordinator, or an append-only linearization
protocol.

## Goals

- Add a first-class object-store backend for prolly nodes and large values.
- Add an explicit distributed ref store abstraction with CAS tokens.
- Preserve the current simple `Store` and `ManifestStore` APIs for embedded
  local use.
- Make backend consistency capabilities visible to callers.
- Support Git-like commit, branch, tag, fetch, push, clone, merge, fsck, and GC
  in downstream applications.
- Keep immutable object writes idempotent and verifiable by content hash.
- Avoid unsafe "last writer wins" ref updates in multi-writer object-store
  deployments.
- Keep `SlateDbStore` useful as a local cache or single-writer structured store,
  while not depending on it for distributed branch locking.
- Keep object-store support feature-gated so users who only need memory, SQLite,
  RocksDB, or file stores do not inherit unnecessary dependencies.

## Non-Goals

- Reimplementing all of Git in the `prolly-map` crate.
- Making object storage behave like a POSIX filesystem.
- Guaranteeing distributed CAS on object-store implementations that cannot
  provide conditional updates.
- Requiring SlateDB for the direct object-store backend.
- Making every object-store backend support the same consistency level.
- Changing prolly node identity, tree shape, diff semantics, or merge semantics.

## Existing Building Blocks

The current library already has:

- `Tree`: immutable snapshot handle.
- `Cid`: SHA-256 content ID for deterministic node bytes.
- `Store`: synchronous content-addressed node store.
- `AsyncStore`: async node-store contract.
- `BlobStore` and `AsyncBlobStore`: large value offload.
- `ManifestStore`: named root storage for embedded/local stores.
- `NodeStoreScan` and `BlobStoreScan`: candidate listing for GC.
- `Prolly::diff`, `range_diff`, `merge`, `merge_prefix`, and CRDT merge.
- `plan_missing_nodes` and `copy_missing_nodes`.
- `SlateDbStore`: object-store-backed SlateDB adapter with local named-root CAS.

The missing piece is a distributed ref contract that carries object-store
version metadata through the API.

## High-Level Architecture

```text
Application / VCS layer
  commit graph, branch policy, merge policy, auth, checkout

Prolly engine
  Tree, diff, merge, range, GC, missing-node sync

Object-store node/blob backend
  immutable nodes and blobs under content-addressed paths

Distributed ref backend
  conditional updates for refs/heads/* and refs/tags/*

Object store
  S3, R2, GCS, Azure, local filesystem, memory test backend
```

The application should publish a commit or snapshot in this order:

1. Write blobs.
2. Write prolly nodes.
3. Write commit/tag objects.
4. Load the current ref and its update token.
5. Conditional-write the new ref.
6. If the ref update conflicts, reload, merge or rebase, then retry.

## Dependency Strategy

Add `object_store` as an optional dependency.

```toml
[features]
object-store = ["async-store", "dep:object_store"]

[dependencies]
object_store = { workspace = true, optional = true }
```

The workspace already resolves `object_store` transitively through SlateDB.
Making it a direct optional dependency gives `prolly-map` access to:

- `ObjectStore`;
- `PutMode::Create`;
- `PutMode::Update(UpdateVersion)`;
- `PutResult`;
- `ObjectMeta` with `e_tag` and `version`;
- list APIs for GC candidate discovery.

Important MSRV decision:

- The workspace currently declares Rust 1.81.
- Newer `object_store` releases may require newer Rust.
- The project must either align the direct dependency with the resolved SlateDB
  compatible version and verify MSRV, or deliberately raise MSRV when enabling
  this backend.

This should be recorded in the implementation PR. Do not let the lockfile
accidentally decide the public MSRV.

## Storage Layout

Use a deterministic prefix layout. A single bucket may host many repositories,
so every backend instance should take a repository prefix.

```text
<repo-prefix>/
  format/v1.json

  nodes/sha256/ab/cd/<cid-hex>
  blobs/sha256/ab/cd/<cid-hex>
  commits/sha256/ab/cd/<commit-id-hex>
  tags/sha256/ab/cd/<tag-id-hex>

  refs/heads/main
  refs/heads/feature-x
  refs/tags/v1.0.0

  reflogs/heads/main/<sequence-or-time>-<writer-id>
  uploads/<upload-id>/manifest
  gc/runs/<run-id>/...
  hints/<namespace>/<encoded-key>
```

For raw prolly nodes, the store key exposed to `AsyncStore` remains CID bytes.
The object backend maps those bytes to the path:

```text
nodes/sha256/ab/cd/<hex>
```

For blobs, `BlobRef.cid` maps to:

```text
blobs/sha256/ab/cd/<hex>
```

Refs are mutable and must not be stored in the same namespace as immutable
objects.

## Object Formats

### Node Object

```text
path:  nodes/sha256/ab/cd/<cid-hex>
bytes: canonical serialized Node bytes
id:    sha256(bytes)
```

Read path must validate:

```text
sha256(bytes) == requested cid
```

If validation fails, return `Error::CidMismatch` or a backend error mapped to
that semantic.

### Blob Object

```text
path:  blobs/sha256/ab/cd/<cid-hex>
bytes: raw payload bytes
id:    sha256(bytes)
```

Blob reads must validate the content ID and length against `BlobRef`.

### Ref Object

MVP ref payload:

```rust
struct RefValueV1 {
    version: u16,
    kind: RefKind,
    target: RefTarget,
    updated_at_millis: u64,
    writer_id: Vec<u8>,
    message: Option<String>,
}

enum RefKind {
    Branch,
    Tag,
    RemoteTracking,
    Checkpoint,
}

enum RefTarget {
    RootManifest(RootManifest),
    Commit(Vec<u8>),
    Tag(Vec<u8>),
}
```

`RootManifest` is enough for generic prolly snapshots. A VCS application should
prefer `Commit` targets so refs move through an immutable commit graph.

Serialization must be deterministic. Use an internal versioned wire format and
include `version` and `kind`.

## Capability Model

Expose backend capabilities. Ref safety depends on them.

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStoreCapabilities {
    pub conditional_create: bool,
    pub conditional_update: bool,
    pub versioned_get: bool,
    pub list_consistency: ListConsistency,
    pub delete_consistency: DeleteConsistency,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListConsistency {
    Strong,
    Eventual,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefConsistency {
    DistributedCas,
    CreateOnly,
    AppendOnlyLinearized,
    SingleWriterOnly,
}
```

The object-store crate API can express conditional writes, but individual
backend implementations may return `NotImplemented` or fail preconditions.
Capabilities should be measured by an explicit probe or declared by backend
configuration, not guessed silently.

## New Public Abstractions

### Async Manifest Store

The current `ManifestStore` is synchronous. Object stores are async. Add an
async equivalent for ordinary named roots.

```rust
#[allow(async_fn_in_trait)]
pub trait AsyncManifestStore {
    type Error: std::error::Error + 'static;

    async fn get_root(
        &self,
        name: &[u8],
    ) -> Result<Option<RootManifest>, Self::Error>;

    async fn put_root(
        &self,
        name: &[u8],
        manifest: &RootManifest,
    ) -> Result<(), Self::Error>;

    async fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error>;

    async fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error>;
}
```

This mirrors the embedded API. It is useful, but not enough for robust VCS refs
because it hides the object-store update token.

### Distributed Ref Store

Add a stronger API for multi-writer refs.

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefToken {
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LoadedRef<T> {
    pub value: T,
    pub token: RefToken,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RefUpdate<T> {
    Applied { current: LoadedRef<T> },
    Conflict { current: Option<LoadedRef<T>> },
    Unsupported { reason: String },
}

#[allow(async_fn_in_trait)]
pub trait RefStore {
    type Error: std::error::Error + 'static;

    async fn capabilities(&self) -> Result<RefConsistency, Self::Error>;

    async fn load_ref(
        &self,
        name: &[u8],
    ) -> Result<Option<LoadedRef<RefValue>>, Self::Error>;

    async fn create_ref(
        &self,
        name: &[u8],
        value: &RefValue,
    ) -> Result<RefUpdate<RefValue>, Self::Error>;

    async fn compare_and_swap_ref(
        &self,
        name: &[u8],
        expected: Option<&RefToken>,
        new: &RefValue,
    ) -> Result<RefUpdate<RefValue>, Self::Error>;

    async fn delete_ref(
        &self,
        name: &[u8],
        expected: &RefToken,
    ) -> Result<RefUpdate<RefValue>, Self::Error>;
}
```

Why keep `RefStore` separate from `ManifestStore`:

- `ManifestStore` is a named-root convenience abstraction.
- `RefStore` models distributed branch movement.
- `RefStore` carries an update token that callers can keep across application
  validation steps.
- VCS refs may target commits or tags, not only root manifests.

## Object Store Backend Types

Feature-gated exports:

```rust
#[cfg(feature = "object-store")]
pub struct ObjectNodeStore {
    object_store: Arc<dyn object_store::ObjectStore>,
    prefix: ObjectPathPrefix,
    read_parallelism: usize,
    write_parallelism: usize,
    verify_reads: bool,
}

#[cfg(feature = "object-store")]
pub struct ObjectBlobStore { ... }

#[cfg(feature = "object-store")]
pub struct ObjectRefStore { ... }
```

`ObjectNodeStore` implements:

- `AsyncStore`;
- async node candidate listing for GC;
- optional hint storage if useful.

`ObjectBlobStore` implements:

- `AsyncBlobStore`;
- blob candidate listing for GC.

`ObjectRefStore` implements:

- `AsyncManifestStore`, for simple root manifests;
- `RefStore`, for distributed VCS refs.

## Node Write Semantics

Immutable node write algorithm:

```text
input: cid, node_bytes

1. verify sha256(node_bytes) == cid
2. path = nodes/sha256/ab/cd/<cid>
3. put with PutMode::Create if supported
4. if AlreadyExists:
     optionally read and verify existing bytes
     treat as success if bytes match
5. if Create is unsupported:
     put overwrite is acceptable only because key is content-derived
```

Overwriting a content-addressed node is safe only if the bytes match the CID.
The backend should verify before writing and after reading.

For performance, verification policy can be configurable:

```rust
pub enum VerificationPolicy {
    Always,
    OnRead,
    DebugOnly,
}
```

The default for remote object stores should be `OnRead`.

## Ref CAS Semantics

### Create Ref

```text
object_store.put_opts(
    refs/heads/main,
    encoded_ref,
    PutMode::Create
)
```

Results:

- success -> `Applied`
- `AlreadyExists` -> load current and return `Conflict`
- `NotImplemented` -> `Unsupported`

### Update Ref

```text
loaded = get refs/heads/main
token = { e_tag: loaded.meta.e_tag, version: loaded.meta.version }

object_store.put_opts(
    refs/heads/main,
    encoded_new_ref,
    PutMode::Update(UpdateVersion {
        e_tag: token.e_tag,
        version: token.version,
    })
)
```

Results:

- success -> `Applied`
- `Precondition` -> load current and return `Conflict`
- `NotImplemented` -> `Unsupported`

### Delete Ref

The object-store crate does not make conditional delete the core portable ref
primitive in the same way as conditional put. For MVP, avoid distributed delete
or implement delete as a conditional update to a tombstone payload:

```rust
RefValue::Deleted {
    deleted_at_millis,
    previous_target,
}
```

Physical deletion can be a GC operation after a grace period.

## Publish Protocol

For a branch update:

```text
base_ref = load_ref("refs/heads/main")
base_commit = base_ref.target

new_tree = apply workspace changes to base tree
write all blobs reachable from new_tree
write all prolly nodes reachable from new_tree
write commit object pointing to new_tree and base_commit

new_ref = RefValue::Branch { target: Commit(new_commit) }
compare_and_swap_ref("refs/heads/main", base_ref.token, new_ref)
```

If CAS conflicts:

```text
current = conflict.current
merge_base = find_lca(base_commit, current.target)
merged_tree = prolly.merge(merge_base.tree, current.tree, new_tree, resolver)
write merged objects
write merge commit with parents [current.target, new_commit]
retry CAS with current.token
```

This is exactly the Git push race shape: immutable objects first, branch head
last.

## Commit Object Layer

`prolly-map` does not need to own a full VCS commit type, but it should provide
enough primitives for downstream crates. A companion crate or optional module
can define:

```rust
struct VersionCommit {
    version: u16,
    tree: RootManifest,
    parents: Vec<CommitId>,
    author: Identity,
    committer: Identity,
    message: String,
    metadata: BTreeMap<String, Vec<u8>>,
}

struct CommitId {
    hash_alg: HashAlg,
    bytes: [u8; 32],
}
```

Commit ID:

```text
sha256("prolly.commit.v1\0" || canonical_commit_bytes)
```

Do not use object-store e-tags as commit IDs. E-tags are backend metadata.

## Append-Only Ref Log Fallback

When conditional update is unavailable, there are two honest options:

- declare the backend `SingleWriterOnly`;
- use an append-only ref-update log with deterministic linearization.

Append-only layout:

```text
ref-updates/heads/main/<logical-time>-<writer-id>-<nonce>
```

Record:

```rust
struct RefUpdateRecord {
    ref_name: Vec<u8>,
    expected_target: Option<RefTarget>,
    new_target: RefTarget,
    parents: Vec<RefTarget>,
    writer_id: Vec<u8>,
    timestamp_millis: u64,
    nonce: [u8; 16],
    signature: Option<Vec<u8>>,
}
```

Linearization rule:

1. List all records for the ref.
2. Sort by `(timestamp_millis, writer_id, nonce)`.
3. Apply a record only if its `expected_target` equals the current derived head.
4. Rejected records remain audit data and can be retried as new merge commits.

This provides eventual convergence, not immediate CAS. It is suitable for
offline collaboration or append-only audit, but not a drop-in replacement for
strong branch-head CAS.

MVP should implement direct conditional CAS first.

## SlateDB Integration

SlateDB remains valuable, but its role should be explicit.

Recommended roles:

- local structured cache over object-store data;
- single-writer embedded object-store-backed KV;
- fast prefix scans for local indexes;
- optional acceleration layer for manifests when one writer owns the DB.

Do not advertise `SlateDbStore::compare_and_swap_root` as distributed-safe
unless SlateDB's configured deployment mode provides the required writer and
transaction guarantees.

Possible integration path:

```text
ObjectNodeStore       direct object-store source of truth
SlateDbStore          optional local cache/index
ObjectRefStore        source-of-truth distributed refs
```

## Sync and Clone

Clone protocol:

1. Load selected remote refs.
2. Traverse commit graph from those refs.
3. For each commit tree, plan missing nodes against the local node store.
4. Copy missing nodes and blobs.
5. Publish local remote-tracking refs.

Fetch protocol:

1. Load remote refs.
2. Compare against local remote-tracking refs.
3. Copy missing commit objects, tree nodes, and blobs.
4. Move `refs/remotes/<remote>/<branch>` locally.

Push protocol:

1. Compute closure from local commit to remote ref's known commit.
2. Upload missing blobs, nodes, commits, tags.
3. Conditional-update remote ref.
4. On conflict, fetch and merge/rebase.

`plan_missing_nodes` already covers tree nodes. The object-store backend should
add similar helpers for blobs and commit objects.

## Garbage Collection

Object-store GC must be conservative.

Retained roots:

- branch refs;
- tag refs;
- remote-tracking refs;
- checkpoint refs;
- recent reflog entries;
- active upload manifests;
- active GC protected sets;
- application pins.

GC phases:

1. Snapshot retained refs.
2. Mark reachable commits, tags, prolly tree nodes, and blobs.
3. List candidate objects by namespace.
4. Exclude objects newer than a grace period.
5. Produce a dry-run plan.
6. Sweep only reclaimable candidates.
7. Write a GC report.

Object-store listing may be eventually consistent. Therefore:

- never delete objects younger than a configured grace period;
- never GC while an upload manifest is active unless it has expired;
- make sweep idempotent;
- verify object kind by path and optional header before deletion;
- preserve reflog windows long enough to recover from mistaken ref movement.

## Reflogs and Recovery

Every successful ref update should write a reflog entry:

```text
reflogs/heads/main/<sequence-or-time>-<writer-id>
```

Payload:

```rust
struct ReflogEntry {
    ref_name: Vec<u8>,
    old_target: Option<RefTarget>,
    new_target: RefTarget,
    writer_id: Vec<u8>,
    message: String,
    timestamp_millis: u64,
    ref_update_token: Option<RefToken>,
}
```

Reflogs are not the primary CAS mechanism. They are recovery and audit data.

## Security and Integrity

Integrity checks:

- verify node CID on read;
- verify blob CID and length on read;
- verify commit ID from canonical bytes;
- reject ref payloads that point to missing required objects during strict fsck;
- treat e-tags and object versions as concurrency metadata, not identity.

Optional security extensions:

- signed commits;
- signed refs;
- repository format file with allowed hash algorithms;
- server-side encryption configuration in backend setup;
- object-store IAM policies that separate read, write, delete, and ref-update
  privileges.

## Error Model

Add object-store-specific errors and map them to semantic variants:

```rust
enum ObjectStoreBackendError {
    NotFound,
    AlreadyExists,
    PreconditionFailed,
    ConditionalUpdateUnsupported,
    CorruptObject,
    CidMismatch,
    ListUnsupported,
    Store(Box<dyn Error + Send + Sync>),
}
```

At the public `prolly::Error` boundary, preserve enough structure for callers to
distinguish:

- storage outage;
- CAS conflict;
- backend missing conditional update support;
- corrupt content;
- missing object;
- permission denied.

Do not collapse CAS conflict into a generic store error.

## Testing Strategy

Unit tests:

- path encoding and sharding;
- node write idempotence;
- blob write idempotence;
- CID validation on read;
- manifest/ref encode/decode;
- create-ref conflict;
- update-ref success;
- update-ref stale-token conflict;
- unsupported conditional update path;
- delete tombstone behavior.

Concurrency tests:

- N writers increment one ref through CAS retry loop;
- competing branch updates produce exactly one successful direct update;
- losing writers can reload and create merge commits;
- reflog entries are written for successful updates.

Object-store conformance tests:

- in-memory store;
- local filesystem store;
- S3-compatible test service if available in CI;
- backend that intentionally returns `NotImplemented` for update;
- backend that returns corrupted bytes for validation tests.

Integration tests:

- build filesystem snapshot;
- publish branch;
- clone into empty local store;
- change two branches;
- merge;
- push with CAS;
- run dry-run GC;
- sweep unreachable old objects after grace period.

## Performance Requirements

Reads:

- batch node reads through `AsyncStore::batch_get_ordered`;
- bound in-flight reads;
- cache hot internal nodes;
- optionally prefetch child frontiers during broad diff and stats scans.

Writes:

- parallelize blob uploads;
- parallelize node uploads;
- deduplicate objects before upload;
- use create-only where supported;
- avoid per-file ref updates; publish one ref after object closure upload.

GC:

- page object-store listings;
- checkpoint mark/sweep progress;
- expose dry-run counts and bytes before deletion.

## Rollout Plan

### Phase 1: Design and Capability Boundaries

- Add this design doc.
- Add `object-store` feature proposal to roadmap.
- Decide MSRV and object_store crate version.
- Define `AsyncManifestStore`, `RefStore`, `RefToken`, `RefValue`, and
  capabilities behind feature flags.

### Phase 2: Direct Object Node and Blob Stores

- Implement `ObjectNodeStore`.
- Implement `ObjectBlobStore`.
- Add node/blob validation.
- Add candidate listing for GC.
- Add examples for object-store-backed snapshots.

### Phase 3: Distributed Ref Store

- Implement `ObjectRefStore`.
- Use `PutMode::Create` for new refs.
- Use `PutMode::Update(UpdateVersion)` for ref CAS.
- Map `AlreadyExists`, `Precondition`, and `NotImplemented` to semantic update
  results.
- Add concurrent writer tests.

### Phase 4: VCS Helper Layer

- Add optional commit/tag object helpers, either in `prolly-map` or a companion
  crate.
- Add publish/fetch/push examples.
- Add reflog writes.
- Add fsck commands or library APIs.

### Phase 5: GC and Operational Hardening

- Add retention policy helpers for refs, tags, reflogs, pins, and upload
  manifests.
- Add object-store GC dry-run and sweep.
- Add recovery docs.
- Add metrics and tracing hooks.

## Open Questions

- Should commit/tag object helpers live in `prolly-map`, `crabdb`, or a new
  `prolly-vcs` crate?
- Should `AsyncManifestStore` be stabilized before `RefStore`, or should
  distributed refs be the first object-store manifest API?
- Which object-store backend set must be CI-tested before calling this
  production-ready?
- Should delete-ref be a tombstone-only operation for all distributed backends?
- Should branch refs target `RootManifest` directly for simple apps, or should
  VCS examples always target commit objects?
- Should `ObjectNodeStore` provide a sync wrapper, or should object-store users
  use `AsyncProlly` only?

## Recommended Decision

Implement direct `object_store` support behind an optional feature, but keep it
async-first and split into two source-of-truth layers:

```text
ObjectNodeStore / ObjectBlobStore
  immutable content-addressed data

ObjectRefStore
  distributed CAS for mutable refs
```

Do not rely on `SlateDbStore`'s in-process manifest mutex for distributed branch
movement. Keep SlateDB as a useful local/single-writer KV backend and optional
cache. For production multi-writer version control, require `ObjectRefStore`
with conditional update support, or explicitly select an append-only or
single-writer consistency mode.
