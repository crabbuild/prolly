# prolly-vcs Design

This document designs a proposed `prolly-vcs` crate: a business-neutral
repository layer for building Git-like version-control systems on top of
`prolly-map`.

`prolly-map` gives applications immutable ordered map snapshots, named roots,
diff, merge, missing-node sync, blob offload, and GC. That is the right
foundation, but application builders still need a higher-level crate that
organizes those primitives into commits, refs, reflogs, history traversal,
patches, merge workflows, remote sync plans, and retention policy.

## Status

Proposed.

This design is a companion to
[`object-store-vcs-design.md`](object-store-vcs-design.md). That document
focuses on object-store backends, distributed ref CAS, and publish protocol.
This document focuses on the neutral high-level crate API and implementation
plan.

## Package Name and Placement

Use a separate crate:

```text
package: prolly-vcs
crate:   prolly_vcs
```

Recommended workspace placement:

```text
crates/prolly/vcs/
```

Add it to the root workspace:

```toml
[workspace]
members = [
    ".",
    "vcs",
    # stores and bindings...
]
```

This follows the existing `stores/...` and `bindings/...` layout. Use
`crates/prolly-vcs/` only if this repository later becomes a nested multi-crate
workspace.

## Goals

- Provide a `Repository` facade for Git-like repository workflows.
- Keep the crate neutral to business domains such as files, documents,
  agent memory, RAG indexes, databases, and event logs.
- Make the safe VCS publish sequence hard to misuse:
  write immutable data first, then CAS refs last.
- Add commit objects above raw prolly `Tree` handles.
- Add ref APIs with compare-and-swap and reflog entries.
- Add history traversal, ancestry checks, and merge-base computation.
- Add durable patch/change-set primitives that can be applied, inverted,
  filtered, and composed.
- Add merge orchestration that distinguishes fast-forward, clean merge, and
  conflicts.
- Add sync planning hooks that copy missing data before moving remote refs.
- Add repository-level retention and GC planning.
- Abstract repository metadata, commits, refs, reflogs, patches, tags, remotes,
  uploads, and indexes as backend-neutral key/value records.
- Reuse `prolly-map` storage, named roots, diff, merge, blob, and GC APIs.

## Non-Goals

- Do not move commit semantics into `prolly-map`.
- Do not make `prolly-vcs` understand file paths, Git modes, documents, rows,
  embeddings, or any specific application value format.
- Do not require users to adopt Git object formats.
- Do not require object storage for local repository use.
- Do not replace backend-specific distributed CAS contracts.
- Do not hide `prolly-map`; advanced users should still be able to drop down
  to map APIs.
- Do not guarantee semantic merge correctness for arbitrary values. The crate
  can orchestrate merge; applications own value semantics and conflict policy.

## Core Mental Model

```text
Tree
  Immutable ordered key/value snapshot from prolly-map.

Commit
  Application-neutral history object. Points at one Tree and zero or more
  parent commits.

Ref
  Mutable named pointer, usually to a Commit. Updated with CAS.

Reflog
  Append-only audit trail for ref movements.

Patch
  Durable ordered key/value changes between two Trees or commits.

Repository
  Facade that coordinates maps, commit objects, refs, reflogs, patches,
  merges, sync, and GC.
```

Users model business state into ordered key/value trees:

```text
filesystem app: path/<path> -> FileEntry
document app:   doc/<id>/block/<id> -> Block
memory app:     conversation/<id>/event/<ts> -> Event
database app:   table/<name>/row/<pk> -> Row
index app:      index/<name>/<term>/<id> -> Posting
```

`prolly-vcs` treats all keys and values as bytes. Business codecs live in the
application or a domain-specific crate.

## Crate Layering

```text
Application domain
  key schema, value codec, UI, policy, authorization, checkout/materialization

prolly-vcs
  Repository, commits, refs, reflogs, patches, merge orchestration, sync plans

prolly-map
  Tree, Prolly, Store, named roots, diff, merge, range, blobs, GC

Storage backend
  memory, file, SQLite, RocksDB, SlateDB, object store, browser storage
```

## Public API Organization

Suggested module layout:

```text
prolly-vcs/src/
  lib.rs
  repository.rs
  commit.rs
  refs.rs
  reflog.rs
  patch.rs
  merge.rs
  graph.rs
  sync.rs
  gc.rs
  codec.rs
  store.rs
  error.rs
  testing.rs
```

Public re-exports should make the common path concise:

```rust
use prolly_vcs::{
    Actor, CommitId, CommitOptions, RefName, Repository, RepositoryConfig,
};
```

## Repository Facade

The primary type should be `Repository`, not `VersionRepo`.

Inside a crate named `prolly-vcs`, `Repository` is clear and concise:

```rust
pub struct Repository<M, R> {
    map: prolly::Prolly<M>,
    storage: R,
    config: RepositoryConfig,
}
```

Where:

- `M` stores prolly nodes and named roots through `prolly-map`.
- `R` stores repository metadata, commits, refs, reflogs, patch objects, tags,
  remotes, uploads, and indexes through the `KvStore` abstraction
  described below. `R` must either implement `KvStore` directly or
  expose one through `RepositoryStorage`.

The initial implementation should be synchronous. Add `AsyncRepository` after
the sync API has stabilized, using the same shape as `Prolly` and
`AsyncProlly`.

### Constructors

```rust
impl<M, R> Repository<M, R> {
    pub fn open(map: prolly::Prolly<M>, storage: R) -> Result<Self>;
    pub fn open_with_config(
        map: prolly::Prolly<M>,
        storage: R,
        config: RepositoryConfig,
    ) -> Result<Self>;

    pub fn map(&self) -> &prolly::Prolly<M>;
    pub fn storage(&self) -> &R;
    pub fn config(&self) -> &RepositoryConfig;
}
```

### Minimal User Flow

```rust
let repo = Repository::open(prolly, repository_store)?;

repo.init("refs/heads/main")?;

let report = repo
    .commit_to_ref("refs/heads/main")
    .message("update settings")
    .author(Actor::name("alice"))
    .mutate(|tree, map| {
        map.put(tree, b"settings/theme".to_vec(), b"dark".to_vec())
    })
    .run()?;
```

Internally this performs:

```text
load current ref
load current commit tree
apply mutation -> new Tree
write commit object
CAS ref from old commit to new commit
append reflog
```

This is the most important ergonomics goal: users should not accidentally
assume a `put` moved a named root or branch head.

## Object Model

### Object Identifiers

Use a neutral content ID type:

```rust
pub struct ObjectId {
    algorithm: HashAlgorithm,
    bytes: [u8; 32],
}

pub type CommitId = ObjectId;
pub type TagId = ObjectId;
pub type PatchId = ObjectId;
```

Object kinds identify the logical object family before decoding bytes:

```rust
pub enum ObjectKind {
    Commit,
    Tag,
    Patch,
    Reflog,
    Custom(Vec<u8>),
}
```

The default hash algorithm should be SHA-256. IDs should hash canonical object
bytes, not database row IDs or insertion timestamps.

Display format can be stable and compact:

```text
vcs_<hex-sha256>
```

or, if the crate prefers unprefixed IDs:

```text
sha256:<hex>
```

The prefixed form is friendlier for mixed systems. The canonical bytes should
not include the display prefix.

### Tree References

A commit must store the complete prolly tree handle, not only the root CID,
because the tree config is needed to interpret nodes.

```rust
pub struct TreeRef {
    pub root: Option<prolly::Cid>,
    pub config: prolly::Config,
}
```

Conversions:

```rust
impl From<&prolly::Tree> for TreeRef;
impl TryFrom<TreeRef> for prolly::Tree;
```

### Commit Object

```rust
pub struct Commit {
    pub id: CommitId,
    pub tree: TreeRef,
    pub parents: Vec<CommitId>,
    pub author: Option<Actor>,
    pub committer: Option<Actor>,
    pub message: Option<String>,
    pub created_at_millis: u64,
    pub metadata: Metadata,
}
```

Rules:

- `parents` order is significant.
- Parent `0` is the mainline parent for merge and revert workflows.
- A commit with no parents is a root commit.
- The commit ID is the hash of canonical commit bytes excluding `id`.
- Metadata is byte-keyed and byte-valued to stay business-neutral.
- Human fields such as `message` and `author` are optional, not required.

### Actor

```rust
pub struct Actor {
    pub name: Option<String>,
    pub email: Option<String>,
    pub id: Option<String>,
    pub metadata: Metadata,
}
```

Applications can map this to users, devices, agents, service accounts, or
anonymous writers.

### Metadata

```rust
pub type Metadata = BTreeMap<Vec<u8>, Vec<u8>>;
```

Do not use `serde_json::Value` as the core metadata type. JSON is convenient,
but byte metadata keeps the storage contract language-neutral and avoids
locking repository objects to one textual representation.

Optional helpers can encode typed metadata:

```rust
metadata.insert_json(b"review", &review)?;
metadata.get_json::<Review>(b"review")?;
```

### Tag Object

Tags are optional but useful for release-like names and immutable labels:

```rust
pub struct Tag {
    pub id: TagId,
    pub target: RefTarget,
    pub name: Vec<u8>,
    pub tagger: Option<Actor>,
    pub message: Option<String>,
    pub created_at_millis: u64,
    pub metadata: Metadata,
}
```

Lightweight tags can be refs directly. Annotated tags use `Tag` objects.

## Ref Model

### Ref Names

Use byte names internally:

```rust
pub struct RefName(Vec<u8>);
```

Provide validation helpers:

```rust
RefName::branch("main")              // refs/heads/main
RefName::tag("v1.0.0")               // refs/tags/v1.0.0
RefName::remote("origin", "main")    // refs/remotes/origin/main
RefName::checkpoint("run-1/0001")    // refs/checkpoints/run-1/0001
```

Recommended namespaces:

```text
refs/heads/<branch>
refs/tags/<tag>
refs/checkpoints/<scope>/<sequence>
refs/remotes/<remote>/<branch>
refs/worktrees/<id>/head
refs/sync/<peer>/cursor
```

Validation should reject:

- empty names;
- names containing `..`;
- leading slash;
- trailing slash;
- NUL bytes;
- backslash;
- components named `.` or `..`;
- lockfile-like suffixes if the chosen backend needs them.

### Ref Targets

The main path should be commit refs:

```rust
pub enum RefTarget {
    Commit(CommitId),
    Tag(TagId),
    Tree(TreeRef),
}
```

`Commit` should be the recommended target. `Tree` is useful for lightweight
users, migration, temporary worktrees, or apps that truly do not need history.

### Ref Records

```rust
pub struct RefRecord {
    pub name: RefName,
    pub target: RefTarget,
    pub generation: u64,
    pub updated_at_millis: u64,
    pub metadata: Metadata,
}
```

Refs should be updated with CAS:

```rust
pub enum RefUpdate {
    Applied { record: RefRecord },
    Conflict { current: Option<RefRecord> },
}
```

### Ref Store

`RefStore` is a typed store over `KvStore`. It is not a separate
backend abstraction and must not be implemented against a different storage
model.

```rust
pub trait RefStoreApi {
    type Error;

    fn get_ref(&self, name: &RefName) -> Result<Option<RefRecord>, Self::Error>;
    fn list_refs(&self, prefix: &[u8]) -> Result<Vec<RefRecord>, Self::Error>;
    fn compare_and_swap_ref(
        &self,
        name: &RefName,
        expected: Option<&RefRecord>,
        replacement: Option<&RefRecord>,
    ) -> Result<RefUpdate, Self::Error>;
}
```

Implementations map refs into the general repository KV layout:

```text
partition_key = refs
sort_key      = <ref-name>
value         = encoded RefRecord
version       = backend CAS token
```

For distributed stores, ref updates still require a real conditional write as
described in [`object-store-vcs-design.md`](object-store-vcs-design.md). The
conditional write happens through the `KvStore` adapter.

### Relationship to prolly Named Roots

`prolly-map` named roots store `name -> Tree`.

`prolly-vcs` refs store `name -> Commit`.

A named root is a mutable pointer to one immutable tree handle, not a commit
and not a live view. `put`, `delete`, `batch`, and builder mutations return new
`Tree` values; they do not forward an existing named root. The named root moves
only when the application explicitly calls `publish_named_root` or
`compare_and_swap_named_root`.

In `prolly-vcs`, a branch head moves through a ref CAS instead. The recommended
flow is: derive a new `Tree`, write a `Commit` object that points at that tree,
then compare-and-swap the ref from the old commit to the new commit.

The VCS crate should not implement refs as an unrelated storage mechanism.
Refs are serialized `RefRecord` values in the repository KV store. A ref may
target a `Commit`, `Tag`, or lightweight `Tree`, but the ref record itself
still lives under the shared `refs` partition.

Do not overload a prolly named root with hidden commit semantics. If a ref
points to a commit, the ref target should say so explicitly.

## Reflog

Every successful ref update should be able to append a reflog entry:

```rust
pub struct ReflogEntry {
    pub ref_name: RefName,
    pub old_target: Option<RefTarget>,
    pub new_target: Option<RefTarget>,
    pub actor: Option<Actor>,
    pub reason: Option<String>,
    pub timestamp_millis: u64,
    pub metadata: Metadata,
}
```

Reflog writes should happen after the ref CAS succeeds. If reflog append fails,
the ref has already moved. The API must report this as a partial success:

```rust
pub struct RefUpdateReport {
    pub update: RefUpdate,
    pub reflog: ReflogStatus,
}

pub enum ReflogStatus {
    Written,
    Skipped,
    Failed { message: String },
}
```

Do not roll back a successful ref CAS only because the reflog failed; many
backends cannot do that atomically.

## Commit and Ref APIs

### Low-Level APIs

```rust
impl Repository<M, O> {
    pub fn create_commit(&self, input: CommitInput) -> Result<Commit>;
    pub fn get_commit(&self, id: &CommitId) -> Result<Option<Commit>>;
    pub fn require_commit(&self, id: &CommitId) -> Result<Commit>;

    pub fn resolve_ref(&self, name: impl IntoRefName) -> Result<Option<RefRecord>>;
    pub fn create_ref(
        &self,
        name: impl IntoRefName,
        target: RefTarget,
        options: RefCreateOptions,
    ) -> Result<RefUpdateReport>;
    pub fn compare_and_swap_ref(
        &self,
        name: impl IntoRefName,
        expected: Option<&RefRecord>,
        replacement: Option<RefRecord>,
        options: RefUpdateOptions,
    ) -> Result<RefUpdateReport>;
}
```

### High-Level APIs

```rust
impl Repository<M, O> {
    pub fn init(&self, default_branch: impl AsRef<str>) -> Result<InitReport>;

    pub fn commit_to_ref(
        &self,
        name: impl IntoRefName,
    ) -> CommitBuilder<M, O>;

    pub fn branch(
        &self,
        name: impl AsRef<str>,
    ) -> BranchBuilder<M, O>;

    pub fn tag(
        &self,
        name: impl AsRef<str>,
    ) -> TagBuilder<M, O>;

    pub fn reset_ref(
        &self,
        name: impl IntoRefName,
        target: RefTarget,
        options: ResetOptions,
    ) -> Result<RefUpdateReport>;
}
```

### Commit Builder

```rust
repo.commit_to_ref("refs/heads/main")
    .message("update search index")
    .author(actor)
    .metadata(b"source", b"batch-import-42")
    .mutate(|tree, map| {
        let tree = map.put(tree, key, value)?;
        Ok(tree)
    })
    .run()?;
```

Builder options:

```rust
pub struct CommitOptions {
    pub message: Option<String>,
    pub author: Option<Actor>,
    pub committer: Option<Actor>,
    pub timestamp_millis: Option<u64>,
    pub metadata: Metadata,
    pub allow_empty: bool,
    pub conflict_policy: RefConflictPolicy,
}
```

`RefConflictPolicy`:

```rust
pub enum RefConflictPolicy {
    Fail,
    RetryApply { max_attempts: usize },
    RetryMerge { max_attempts: usize, strategy: MergeStrategy },
}
```

The first milestone should implement `Fail`. Retry policies can follow once
merge and patch APIs are stable.

## Commit Graph

Graph APIs should be independent of business values:

```rust
impl Repository<M, O> {
    pub fn parents(&self, commit: &CommitId) -> Result<Vec<CommitId>>;
    pub fn ancestors(&self, commit: &CommitId) -> Result<AncestorIter>;
    pub fn is_ancestor(&self, base: &CommitId, head: &CommitId) -> Result<bool>;
    pub fn merge_base(
        &self,
        left: &CommitId,
        right: &CommitId,
    ) -> Result<Option<CommitId>>;
    pub fn log(&self, start: RefOrCommit) -> Result<CommitLogIter>;
}
```

Implementation notes:

- Start with breadth-first or generation-aware traversal.
- Store optional generation numbers in commits for faster ancestry checks.
- Add a commit graph index later if repository sizes require it.
- Avoid requiring topological indexes in the object format from day one.

## Patch and Change-Set API

Raw `diff` is a stream of key/value changes between two `Tree` handles.
Applications need durable change sets for review, cherry-pick, revert, sync,
partial apply, and UI previews.

### Patch Object

```rust
pub struct Patch {
    pub id: Option<PatchId>,
    pub base: Option<TreeRef>,
    pub target: Option<TreeRef>,
    pub changes: Vec<PatchChange>,
    pub metadata: Metadata,
}

pub enum PatchChange {
    Put {
        key: Vec<u8>,
        old: Option<Vec<u8>>,
        new: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
        old: Vec<u8>,
    },
}
```

Rules:

- Changes are sorted by key.
- `old` values are optional for compact patches, but required for inversion and
  safer application.
- Large patches should support streaming and paging.

### Patch APIs

```rust
impl Repository<M, O> {
    pub fn diff_trees(&self, base: &Tree, target: &Tree) -> Result<Patch>;
    pub fn diff_commits(&self, base: &CommitId, target: &CommitId) -> Result<Patch>;
    pub fn apply_patch(&self, tree: &Tree, patch: &Patch) -> Result<PatchApplyReport>;
    pub fn invert_patch(&self, patch: &Patch) -> Result<Patch>;
    pub fn compose_patches(&self, patches: &[Patch]) -> Result<Patch>;
    pub fn filter_patch_by_prefix(&self, patch: &Patch, prefix: &[u8]) -> Patch;
}
```

`PatchApplyReport`:

```rust
pub struct PatchApplyReport {
    pub tree: Tree,
    pub applied: usize,
    pub conflicts: Vec<PatchConflict>,
}
```

Patch apply must not move refs. It only returns a new tree.

## Merge API

The high-level merge API should classify outcomes before mutating refs:

```rust
pub enum MergePreview {
    AlreadyUpToDate {
        target: CommitId,
    },
    FastForward {
        base: CommitId,
        target: CommitId,
        source: CommitId,
    },
    CleanMerge {
        base: CommitId,
        target: CommitId,
        source: CommitId,
        tree: Tree,
        summary: MergeSummary,
    },
    Conflicted {
        base: CommitId,
        target: CommitId,
        source: CommitId,
        conflicts: Vec<MergeConflict>,
    },
}
```

Builder:

```rust
let preview = repo
    .merge("refs/heads/feature")
    .into("refs/heads/main")
    .strategy(MergeStrategy::default())
    .preview()?;

repo.finish_merge(preview)
    .message("merge feature")
    .author(actor)
    .run()?;
```

Rules:

- Fast-forward should only move the target ref after an ancestry check.
- Clean merge creates a new merge commit with parents `[target, source]`.
- Conflicted merge does not move the target ref.
- Conflict values remain raw bytes; applications render domain-specific
  explanations.
- Store the selected merge base in the merge report for reproducibility.

## Reset, Revert, and Cherry-Pick

These are repository operations built from commits, patches, and ref CAS.

### Reset

Move a ref to a target if the expected ref still matches:

```rust
repo.reset_ref("refs/heads/main", RefTarget::Commit(target), options)?;
```

Reset does not create a commit. It must write a reflog entry.

### Revert

Create a new commit that applies the inverse of a selected commit's patch:

```rust
repo.revert(commit_id)
    .onto("refs/heads/main")
    .message("revert bad change")
    .run()?;
```

For merge commits, require a mainline parent:

```rust
repo.revert(merge_commit).mainline_parent(0).run()?;
```

### Cherry-Pick

Apply `diff(parent, picked)` onto another head:

```rust
repo.cherry_pick(commit_id)
    .onto("refs/heads/main")
    .run()?;
```

Cherry-pick can conflict if the patch's expected old values do not match the
target tree.

## Status and Checkout

`prolly-vcs` should not know how to scan a filesystem or materialize documents.
It can provide generic comparison and planning APIs:

```rust
repo.status_tree("refs/heads/main", &candidate_tree)?;
repo.checkout_plan(commit_or_ref)?;
```

`CheckoutPlan` is an ordered list of key/value puts and deletes. Applications
translate it into filesystem writes, document updates, cache updates, or
database mutations.

Filesystem-specific checkout should live in a separate crate or application:

```text
prolly-vcs-files
```

or in the downstream product.

## Remote Sync

Remote sync should be layered, not hidden inside basic ref APIs.

### Fetch

```rust
let plan = repo.plan_fetch(remote, wanted_refs)?;
repo.execute_fetch(plan)?;
```

Fetch sequence:

1. Read remote refs.
2. Determine missing commits, tags, tree nodes, and blobs.
3. Copy missing immutable objects.
4. Verify copied object hashes.
5. Update local remote-tracking refs.

### Push

```rust
let plan = repo.plan_push("refs/heads/main", remote, "refs/heads/main")?;
repo.execute_push(plan)?;
```

Push sequence:

1. Load local ref and remote ref.
2. Check fast-forward or configured push policy.
3. Copy missing commits, tags, tree nodes, and blobs to remote.
4. Verify remote closure.
5. CAS remote ref from expected target to new target.
6. Record local push metadata and optional reflog.

Push must never move a remote ref before the referenced closure is present.

## Backend-Neutral Repository Storage Model

The VCS layer should not invent a different persistence model for each backend.
Represent every repository-level item as a key/value record with:

```text
repository_id
partition_key
sort_key
value
version
metadata
```

This is a hard layering rule for `prolly-vcs`:

```text
KvStore
  general repository records with repository_id + partition_key + sort_key

RepositoryStore, CommitStore, ObjectStore, RefStore, ReflogStore,
RemoteStore, UploadStore, LeaseStore, IndexStore
  typed stores implemented by KvStore

Repository
  workflow facade over the typed stores
```

`RepositoryStore`, `CommitStore`, `ObjectStore`, `RefStore`, `ReflogStore`,
`RemoteStore`, `UploadStore`, `LeaseStore`, `IndexStore`, and any future
higher-level store must be implemented by the same general `KvStore`.
Higher-level stores must not define their own independent backend contracts. If
a feature needs persistence, it must first map to `RepositoryKey` and
`RepositoryRecord`. Backend-specific crates implement only the general KV
adapter; the typed stores are shared library code.

This maps cleanly to:

- DynamoDB, Cosmos DB, FoundationDB-style tuple spaces, and similar stores with
  partition/sort-key data models;
- SQL tables with `(repository_id, partition_key, sort_key)` primary keys;
- ordered embedded stores such as RocksDB;
- file stores with encoded directory and file names;
- object stores with deterministic object paths;
- memory stores backed by `BTreeMap`;
- service-backed repository stores.

The VCS API can then expose typed stores for repositories, commits, refs,
objects, reflogs, patches, tags, remotes, and upload manifests without making
each store define its own backend adapter.

### Logical Record Key

```rust
pub struct RepositoryId(Vec<u8>);

pub struct RepositoryKey {
    pub repository_id: RepositoryId,
    pub partition_key: Vec<u8>,
    pub sort_key: Vec<u8>,
}
```

Terminology:

- `repository_id`: isolates one logical repository from another.
- `partition_key`: groups records with the same access pattern.
- `sort_key`: the main key inside a partition. It orders records and
  identifies one record.

Some backends call this field a main key, row key, range key, clustering key,
or sort key. `prolly-vcs` should use `sort_key` in Rust APIs and document it
as the partition-local main key.

Backends that do not have native partitions can concatenate the three segments
with an escaped binary encoding. Backends that do have native partitions should
store them separately.

### Storage Record

```rust
pub struct RepositoryRecord {
    pub key: RepositoryKey,
    pub kind: RecordKind,
    pub value: Vec<u8>,
    pub version: Option<RecordVersion>,
    pub updated_at_millis: Option<u64>,
    pub metadata: RecordMetadata,
}

pub struct RecordVersion(Vec<u8>);
pub type RecordMetadata = BTreeMap<Vec<u8>, Vec<u8>>;
```

`version` is an opaque backend CAS token:

- SQLite can use an integer generation encoded as bytes.
- Object stores can use ETag or object version IDs.
- DynamoDB-style stores can use a numeric version attribute.
- Memory stores can use a monotonically increasing counter.
- Backends with no compare-and-swap support must report that limitation in
  capabilities and cannot safely serve multi-writer refs.

### Record Kinds and Key Layout

Use explicit record kinds for validation and inspection:

```rust
pub enum RecordKind {
    RepositoryMeta,
    Commit,
    Tag,
    Patch,
    Ref,
    Reflog,
    Remote,
    Upload,
    Lease,
    Index,
    Custom(Vec<u8>),
}
```

Recommended logical layout:

| Record | Partition key | Sort key | Notes |
| --- | --- | --- | --- |
| Repository metadata | `repo` | `meta` | Format version, default branch, repository config |
| Commit object | `objects/commit` | `sha256/<commit-id>` | Immutable, put-if-absent |
| Tag object | `objects/tag` | `sha256/<tag-id>` | Immutable, put-if-absent |
| Patch object | `objects/patch` | `sha256/<patch-id>` | Immutable or content-addressed patch bytes |
| Ref | `refs` | `<ref-name>` | Mutable, CAS required |
| Reflog | `reflog/<ref-name>` | `<millis>/<generation>/<writer>` | Append-only ordered log |
| Commit child index | `index/commit-children/<parent-id>` | `<child-id>` | Optional graph acceleration |
| Commit time index | `index/commit-time` | `<millis>/<commit-id>` | Optional log acceleration |
| Remote ref | `remote/<remote>/refs` | `<ref-name>` | Remote-tracking state |
| Upload manifest | `uploads/<upload-id>` | `manifest` | Protects in-flight publish data |
| Upload item | `uploads/<upload-id>` | `object/<kind>/<id>` | Optional upload progress |
| Lease | `leases` | `<lease-id>` | Optional writer or GC lease |
| GC run | `gc/<run-id>` | `plan` | Optional dry-run and audit records |

This data model is intentionally not Git-specific. Git-like names such as
`refs/heads/main` are conventions inside sort keys, not hard-coded storage
tables.

### Access Patterns

The layout must support these operations efficiently:

- load repository metadata;
- put immutable commit/tag/patch objects if absent;
- get immutable objects by ID;
- load one ref by name;
- compare-and-swap one ref;
- list refs by prefix;
- append reflog entries for one ref;
- scan reflog entries by ref and time;
- walk commit parents by loading commit objects;
- optionally find children through an index partition;
- list remote-tracking refs for one remote;
- retain upload manifests during GC;
- list candidate records for repository-level fsck and GC.

The minimal backend does not need every index. It must support the core access
patterns for commits, refs, and reflogs.

### KvStore Trait

All repository storage must be implemented through one lower-level trait:

```rust
pub trait KvStore {
    type Error;

    fn capabilities(&self) -> KvStoreCapabilities;

    fn get(
        &self,
        key: &RepositoryKey,
        consistency: ReadConsistency,
    ) -> Result<Option<RepositoryRecord>, Self::Error>;

    fn put(
        &self,
        record: RepositoryRecord,
        mode: PutMode,
    ) -> Result<RepositoryRecord, Self::Error>;

    fn delete(
        &self,
        key: &RepositoryKey,
        expected: WriteExpectation,
    ) -> Result<DeleteResult, Self::Error>;

    fn query(
        &self,
        partition: &RepositoryPartition,
        range: SortKeyRange,
        limit: Option<usize>,
    ) -> Result<QueryPage, Self::Error>;

    fn batch(
        &self,
        writes: &[RepositoryWrite],
        mode: BatchMode,
    ) -> Result<BatchResult, Self::Error>;
}
```

Important supporting types:

```rust
pub enum PutMode {
    Any,
    Create,
    Replace { expected: RecordVersion },
    CompareAndSwap {
        expected: Option<RecordVersion>,
    },
}

pub enum WriteExpectation {
    Any,
    MustExist { version: Option<RecordVersion> },
    MustNotExist,
}

pub enum ReadConsistency {
    Default,
    Strong,
}
```

`PutMode::Create` is used for immutable objects. `CompareAndSwap` is used for
refs and mutable metadata. `Any` should be avoided for refs.

### Capabilities

Backends must advertise operational guarantees:

```rust
pub struct KvStoreCapabilities {
    pub strong_reads: bool,
    pub conditional_writes: bool,
    pub ordered_partition_scan: bool,
    pub atomic_batch: bool,
    pub cross_partition_transaction: bool,
    pub server_timestamps: bool,
    pub ttl: bool,
    pub watch: bool,
}
```

Behavioral rules:

- A backend with `conditional_writes = false` must not be used for multi-writer
  refs without an external coordinator.
- A backend with `ordered_partition_scan = false` can still store commits and
  refs, but reflog pagination, prefix ref listing, and GC candidate scanning may
  need secondary indexes or service-side APIs.
- A backend with `strong_reads = false` must document stale-read behavior and
  use write tokens from successful writes when possible.
- `atomic_batch` is an optimization, not a semantic requirement for the core
  publish path.

### Typed Stores Over KvStore

The user-facing repository code should not call raw partitions everywhere.
Instead, provide focused typed stores that share the same backing `KvStore`:

```rust
pub struct RepositoryStore<S> { kv: S }
pub struct CommitStore<S> { kv: S }
pub struct ObjectStore<S> { kv: S }
pub struct RefStore<S> { kv: S }
pub struct ReflogStore<S> { kv: S }
pub struct RemoteStore<S> { kv: S }
pub struct UploadStore<S> { kv: S }
pub struct LeaseStore<S> { kv: S }
pub struct IndexStore<S> { kv: S }
```

Each typed store is pure mapping code from a typed API to `RepositoryKey`,
`RepositoryRecord`, and `KvStore` operations. Typed stores must not perform
backend-specific I/O directly or own backend clients.

Backend adapters implement only `KvStore`. `RepositoryStore`, `CommitStore`,
`ObjectStore`, `RefStore`, and the other typed stores are reusable library
wrappers over that adapter, not backend extension points.

Repository metadata store:

```rust
pub trait RepositoryStoreApi {
    type Error;

    fn load_metadata(&self) -> Result<Option<RepositoryMetadata>, Self::Error>;
    fn update_metadata(
        &self,
        expected: Option<RecordVersion>,
        metadata: RepositoryMetadata,
    ) -> Result<RepositoryMetadata, Self::Error>;
}
```

Commit store:

```rust
pub trait CommitStoreApi {
    type Error;

    fn get_commit(&self, id: &CommitId) -> Result<Option<Commit>, Self::Error>;
    fn put_commit(&self, commit: &Commit) -> Result<(), Self::Error>;
    fn has_commit(&self, id: &CommitId) -> Result<bool, Self::Error>;
}
```

Object store:

```rust
pub trait ObjectStoreApi {
    type Error;

    fn get_object(
        &self,
        kind: ObjectKind,
        id: &ObjectId,
    ) -> Result<Option<Vec<u8>>, Self::Error>;

    fn put_object(
        &self,
        kind: ObjectKind,
        bytes: &[u8],
    ) -> Result<ObjectId, Self::Error>;

    fn has_object(
        &self,
        kind: ObjectKind,
        id: &ObjectId,
    ) -> Result<bool, Self::Error>;
}
```

The ref API is implemented by `RefStore<S>` over the same general `KvStore`.
For example, `compare_and_swap_ref` loads
`partition_key = refs`, `sort_key = <ref-name>`, checks the expected
`RecordVersion`, writes a new serialized `RefRecord`, and returns either
`Applied` or `Conflict`.

### Backend Adapter Mapping

Memory adapter:

```text
BTreeMap<(repository_id, partition_key, sort_key), RepositoryRecord>
```

File adapter:

```text
<root>/<repo>/<escaped-partition>/<escaped-sort-key>.record
```

SQLite adapter:

```sql
CREATE TABLE repository_records (
  repository_id BLOB NOT NULL,
  partition_key BLOB NOT NULL,
  sort_key BLOB NOT NULL,
  kind TEXT NOT NULL,
  value BLOB NOT NULL,
  version INTEGER NOT NULL,
  updated_at_millis INTEGER,
  metadata BLOB,
  PRIMARY KEY (repository_id, partition_key, sort_key)
);
```

Object-store adapter:

```text
<prefix>/<repo>/<escaped-partition>/<escaped-sort-key>
```

Object-store refs require conditional writes using ETag/version IDs or an
external coordinator. If the object store cannot provide a safe conditional
write, it can still store immutable objects, but not multi-writer branch refs.

DynamoDB-style adapter:

```text
PK = <repo>#<partition_key>
SK = <sort_key>
```

Postgres adapter:

Use the same primary key as SQLite, with `SELECT ... FOR UPDATE` or optimistic
version checks for local transactional CAS.

### RepositoryStorage Facade

`RepositoryStorage` is optional convenience, not a second storage model. If it
exists, it must expose typed stores over one `KvStore`:

```rust
pub trait RepositoryStorage {
    type Kv: KvStore;
    type Error;

    fn kv(&self) -> &Self::Kv;
    fn repository(&self) -> RepositoryStore<&Self::Kv>;
    fn commits(&self) -> CommitStore<&Self::Kv>;
    fn objects(&self) -> ObjectStore<&Self::Kv>;
    fn refs(&self) -> RefStore<&Self::Kv>;
    fn reflogs(&self) -> ReflogStore<&Self::Kv>;
    fn remotes(&self) -> RemoteStore<&Self::Kv>;
    fn uploads(&self) -> UploadStore<&Self::Kv>;
    fn leases(&self) -> LeaseStore<&Self::Kv>;
    fn indexes(&self) -> IndexStore<&Self::Kv>;
}
```

The first implementation can skip this facade and let `Repository` construct
stores directly from `KvStore`. The non-negotiable design constraint is
that all repository records and all higher-level store APIs share one neutral
key/value substrate.

## Codecs and Wire Format

Use canonical CBOR or another deterministic binary format for VCS objects.
The object bytes should include:

- version;
- kind;
- hash algorithm;
- fixed field order;
- required fields;
- optional extension metadata.

Example wire envelope:

```rust
struct ObjectEnvelope {
    version: u16,
    kind: ObjectKind,
    hash_alg: HashAlgorithm,
    payload: Vec<u8>,
}
```

Rules:

- Unknown required fields fail decoding.
- Unknown optional metadata can be preserved only if the codec supports stable
  round-tripping.
- Timestamps are data fields; object-store insertion time is not part of the
  object ID.
- Maps must be serialized in key order.
- String fields should be UTF-8. Byte fields should remain bytes.

## Consistency and Transaction Boundaries

The repository publish sequence is intentionally not one global transaction:

```text
write tree nodes
write blobs
write commit object
CAS ref
append reflog
```

Required guarantees:

- Immutable object writes are idempotent.
- Object reads verify content hashes.
- Ref CAS is the only step that makes a commit reachable from a head.
- A failed CAS leaves uploaded immutable objects unreachable but harmless.
- GC must not delete upload-in-progress objects that may still be published.
- Commit, tag, and patch records use `PutMode::Create` or equivalent
  put-if-absent behavior.
- Ref records use `PutMode::CompareAndSwap`; unsafe overwrite mode is not a
  valid branch update.
- Reflog records use append-only keys and should not replace existing records.
- Repository metadata may use CAS when schema version or default branch changes.

For local transactional backends, `Repository` may optimize by using a database
transaction. The public semantics should still match the sequence above.

Backend adapter tests must prove these semantics through the public
`KvStore` contract, not through backend-specific implementation
details.

## GC and Retention

Repository-level GC should gather retention roots from VCS concepts:

- branch refs;
- tags;
- remote-tracking refs;
- checkpoint refs;
- worktree refs;
- sync cursor refs;
- reflog entries within retention windows;
- active upload manifests;
- active leases;
- user-provided exact commits or trees.

API:

```rust
pub struct RetentionPolicy {
    pub keep_branches: bool,
    pub keep_tags: bool,
    pub keep_remote_tracking_refs: bool,
    pub keep_checkpoints: CheckpointRetention,
    pub keep_reflog_for_millis: Option<u64>,
    pub extra_refs: Vec<RefName>,
    pub extra_commits: Vec<CommitId>,
}

repo.gc_plan(policy)?;
repo.gc_sweep(policy)?;
```

GC should mark:

1. Ref targets.
2. Tag targets.
3. Commit ancestors selected by retention.
4. Trees referenced by commits.
5. Prolly nodes reachable from those trees.
6. Blobs referenced from values when the blob store supports reachability.
7. Patch/reflog objects retained by policy.

## Error Model

The error enum should preserve operational meaning:

```rust
pub enum Error {
    RefNotFound(RefName),
    CommitNotFound(CommitId),
    ObjectNotFound(ObjectKind, ObjectId),
    RefConflict { name: RefName },
    NotFastForward { name: RefName },
    MergeConflict,
    InvalidRefName(Vec<u8>),
    InvalidObject(String),
    MissingClosure { missing: Vec<ObjectRef> },
    MissingStoreCapability {
        capability: &'static str,
        operation: &'static str,
    },
    Prolly(prolly::Error),
    Store(String),
}
```

Do not collapse ref conflicts, missing objects, and invalid object bytes into a
generic storage error. Application builders need to handle them differently.

## Feature Flags

Suggested features:

```toml
[features]
default = []
serde = ["dep:serde"]
json = ["serde", "dep:serde_json"]
file-store = []
sqlite = ["dep:rusqlite"]
async = ["prolly-map/async-store"]
object-store = ["async", "dep:object_store"]
test-utils = []
```

Keep the initial crate light. Do not make object-store, SQLite, or async
dependencies mandatory.

## Relationship to Git

`prolly-vcs` should be Git-like, not Git-specific.

Similarities:

- commits point to immutable snapshots;
- refs point to commits;
- branch updates use compare-and-swap semantics;
- fast-forward requires ancestry;
- merge commits have multiple parents;
- reflogs explain ref movement;
- fetch/push copy immutable objects before moving refs.

Differences:

- snapshots are prolly trees, not Git trees;
- values are arbitrary bytes, not Git blobs;
- paths are not built in;
- commit IDs are not Git OIDs;
- remotes may be any backend with object and ref capabilities;
- application metadata is byte-keyed and domain-defined.

Git import/export can be a separate adapter:

```text
prolly-vcs-git
```

or live in a downstream product.

## Business-Neutral Extension Points

Applications should plug in semantics without forking the repository core:

```rust
pub trait ValueMergePolicy {
    fn resolve(
        &self,
        key: &[u8],
        base: Option<&[u8]>,
        left: Option<&[u8]>,
        right: Option<&[u8]>,
    ) -> Result<MergeResolution>;
}

pub trait Materializer {
    fn apply_checkout_plan(&self, plan: CheckoutPlan) -> Result<()>;
}

pub trait CommitMetadataPolicy {
    fn validate(&self, commit: &CommitInput) -> Result<()>;
}
```

Examples:

- A filesystem app validates path normalization and file modes.
- A document app validates block IDs and schema versions.
- A database app validates table names and primary keys.
- An agent-memory app validates event ordering and provenance metadata.

## Implementation Plan

### Phase 0: Design and Workspace Skeleton

Status: proposed.

Deliverables:

- Add `vcs/Cargo.toml`.
- Add `vcs/src/lib.rs`.
- Add workspace member.
- Add this design to documentation maps.
- Add empty modules for repository, commit, refs, reflog, patch, merge, graph,
  sync, gc, codec, store, and error.

Exit criteria:

- `cargo check -p prolly-vcs` passes.
- Crate docs explain the relationship to `prolly-map`.

### Phase 1: Repository Storage Substrate

Deliverables:

- `RepositoryId`, `RepositoryKey`, `RepositoryRecord`, `RecordVersion`, and
  `RecordKind`.
- `KvStore`, `PutMode`, `WriteExpectation`, `ReadConsistency`, and
  `KvStoreCapabilities`.
- `MemKvStore` backed by a `BTreeMap`.
- Typed stores for repository metadata, commits, objects, refs, reflogs,
  remotes, and uploads over the KV store.
- Lease and index stores over the same KV store.
- Key layout helpers for commits, refs, reflogs, remotes, uploads, leases, and
  indexes.

Exit criteria:

- Unit tests prove deterministic key encoding.
- Unit tests prove put-if-absent for immutable objects.
- Unit tests prove compare-and-swap conflicts on mutable ref records.
- Unit tests prove ordered partition scans for refs and reflogs.
- Unit tests prove `RepositoryStore`, `CommitStore`, `ObjectStore`, `RefStore`,
  `ReflogStore`, remote, upload, lease, and index stores all write through the
  same `KvStore`.
- Capability reports distinguish memory, file, SQL, and object-store adapter
  requirements.

### Phase 2: Local Commit and Ref Core

Deliverables:

- `Commit`, `CommitInput`, `CommitId`, `TreeRef`, `Actor`, `Metadata`.
- `RefName`, `RefTarget`, `RefRecord`, `RefUpdate`.
- `ObjectStore`, `RefStore`, and `ReflogStore` typed stores backed by
  `KvStore`.
- `Repository::open`.
- `Repository::create_commit`.
- `Repository::get_commit`.
- `Repository::resolve_ref`.
- `Repository::compare_and_swap_ref`.
- `Repository::commit_to_ref(...).mutate(...).run()`.

Exit criteria:

- Unit tests prove commit ID determinism.
- Unit tests prove CAS conflict behavior.
- Unit tests prove `commit_to_ref` writes commit before moving ref.
- Unit tests prove a failed CAS leaves the old ref unchanged.

### Phase 3: Reflog and Basic Branching

Deliverables:

- `ReflogEntry`.
- Reflog write status.
- `Repository::branch`.
- `Repository::tag`.
- `Repository::reset_ref`.
- `Repository::log_ref`.

Exit criteria:

- Ref updates append reflogs.
- Reflog failure is reported as partial success.
- Branch creation refuses to overwrite by default.
- Reset writes reflog and does not create a commit.

### Phase 4: Commit Graph

Deliverables:

- Ancestor iterator.
- `is_ancestor`.
- `merge_base`.
- Topological log traversal.
- Optional generation metadata.

Exit criteria:

- Tests cover linear history, branches, criss-cross-like histories, root
  commits, and missing parents.
- Fast-forward checks use graph ancestry, not tree equality.

### Phase 5: Patch API

Deliverables:

- `Patch`, `PatchChange`, `PatchConflict`.
- `diff_trees`.
- `diff_commits`.
- `apply_patch`.
- `invert_patch`.
- `compose_patches`.
- Prefix filtering.

Exit criteria:

- Applying `diff(a, b)` to `a` yields `b`.
- Inverting that patch and applying to `b` yields `a`.
- Conflicting old-value checks are reported without partial ref movement.
- Large patch streaming design is documented even if not implemented.

### Phase 6: Merge Orchestration

Deliverables:

- `MergePreview`.
- Merge builder.
- Fast-forward merge.
- Clean three-way merge commit.
- Conflict result without ref mutation.

Exit criteria:

- Already-up-to-date, fast-forward, clean merge, and conflict paths are tested.
- Merge commits use parent order `[target, source]`.
- The selected merge base is recorded in reports.

### Phase 7: Repository GC

Deliverables:

- `RetentionPolicy`.
- Ref/reflog/checkpoint retention collection.
- Commit closure marking.
- Integration with `prolly-map` node and blob GC planning.

Exit criteria:

- GC plan keeps branch heads, tags, selected reflog commits, and checkpoints.
- GC plan excludes unreachable commits outside policy.
- Sweep refuses to run if required exact retention roots are missing.

### Phase 8: Sync Planning

Deliverables:

- `Remote` facade backed by `KvStore` typed stores for objects, refs,
  reflogs, upload manifests, and remote-tracking refs.
- `plan_fetch`.
- `execute_fetch`.
- `plan_push`.
- `execute_push`.
- Closure verification.

Exit criteria:

- Push copies missing immutable objects before CAS.
- Push handles remote ref conflicts.
- Fetch updates remote-tracking refs after verifying copied objects.
- Tests use memory remotes first.

### Phase 9: Durable Stores and Async

Deliverables:

- File-backed `KvStore` adapter.
- SQLite-backed `KvStore` adapter.
- `AsyncRepository`.
- Object-store-backed `KvStore` adapter.
- DynamoDB/Cosmos-style partition and sort-key adapter guidance.

Exit criteria:

- Crash/reopen tests for file or SQLite store.
- Async API mirrors sync semantics.
- Object-store distributed CAS behavior is explicit in capability reports.

## Testing Strategy

Test levels:

- Unit tests for IDs, codecs, ref validation, and graph algorithms.
- Property tests for patch apply/invert/compose.
- Store conformance tests for `KvStore` adapters and typed stores.
- Repository scenario tests for init, commit, branch, merge, reset, revert,
  cherry-pick, fetch, push, and GC.
- Crash-style tests for durable stores where possible.
- Cross-language fixture tests once bindings exist.

Core invariants to test:

- Mutating a `Tree` never moves a ref.
- A ref moves only through CAS.
- A commit ID is deterministic.
- Ref conflict leaves the current ref unchanged.
- Commit objects are written before refs point at them.
- Reflog failure does not hide ref update success.
- Fast-forward requires ancestry.
- Merge conflict does not move target refs.
- Push never updates remote ref before closure is present.
- GC never deletes objects reachable from retained refs.

## Documentation Plan

Documentation should include:

- `prolly-vcs` crate README with quick start.
- API examples for committing to refs without raw named-root handling.
- Guide: modeling business domains as ordered key/value trees.
- Guide: branch, tag, reset, merge, revert, and cherry-pick.
- Guide: repository GC and retention policy.
- Guide: remote sync and object-store consistency.
- Cookbook examples:
  - file snapshots;
  - document block repository;
  - agent memory repository;
  - secondary index repository;
  - remote push/fetch with memory stores.

## Open Questions

- Should the first implementation expose `KvStore` directly, or hide
  it behind `RepositoryStorage` from day one?
- Which backend adapters should be shipped with `prolly-vcs` itself versus
  separate crates?
- Should `RefTarget::Tree` be public in `0.1`, or kept internal until a user
  proves the lightweight mode is necessary?
- Should commit IDs include the tree config in every case, or normalize tree
  config through repository configuration?
- Should patches store old values by default, or offer compact and safe modes?
- Should reflogs be mandatory for all ref stores, or optional by capability?
- Should async support be a separate crate (`prolly-vcs-async`) or a feature?
- Should Git import/export live in `prolly-vcs-git` or remain downstream?

## Recommended First PR

The first PR should not try to implement all VCS operations. It should establish
the crate and prove the core safety path:

1. Create `crates/prolly/vcs`.
2. Add `KvStore`, `RepositoryKey`, `RepositoryRecord`, and
   `MemKvStore`.
3. Add repository/commit/object/ref/reflog/remote/upload/lease/index stores over
   the KV store.
4. Add `Repository`.
5. Add `Commit`, `RefRecord`, and `RefTarget`.
6. Implement `create_commit`, `resolve_ref`, `compare_and_swap_ref`, and
   `commit_to_ref`.
7. Add tests for deterministic storage keys, deterministic commit IDs, and CAS
   conflict behavior.
8. Add docs showing that users mutate trees through the builder and the ref
   moves only after a successful CAS.

That gives application builders a small but useful local VCS substrate while
leaving patches, merge orchestration, sync, and durable stores for follow-up
PRs.
