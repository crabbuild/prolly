# Native-versioned S3 client architecture

> Status: accepted architecture; experimental implementation
>
> Decision owner: Prolly S3 maintainers
>
> Scope: a new repository profile in which Prolly is the authoritative,
> exclusive writer and each logical object version is one native S3 object
> version at the original key.

This document defines how Prolly supplies logical history for whole objects in
a versioned Amazon S3 bucket. It specifies the authority model, persisted
identities, write and recovery protocols, request budgets, operational limits,
and the evidence required before production use.

This design replaces repository-level content chunks for the new profile. It
does not change the persisted layout of existing repositories.

![Native-versioned S3 architecture](diagram/native-versioned-s3-architecture.svg)

Related material:

- [Versioned S3 workspace overview](README.md)
- [Existing SlateDB node-store proposal](SLATEDB-NODE-STORE-DESIGN.md)
- [Operations runbook](OPERATIONS.md)
- [Qualification record](QUALIFICATION.md)

## Current delivery status

The implementation exercises the core architecture, but the profile is not
production-qualified. The repository currently supports:

- persisted profile negotiation and exact native `VersionId` bindings;
- whole-object put, copy, delete, current reads, and historical reads;
- one packed Prolly-node publication per bucket commit;
- four-call warm writes at 1, 8, and 32 queued callers;
- five-call two-object commit sessions and three-call merge or restore;
- writer fencing, explicit takeover, idempotent replay, lost-put response
  reconciliation, lost-copy and lost-delete reconciliation, checkpoints,
  exact-version garbage collection, and fsck;
- native multipart create, streamed part upload, part copy, listing,
  restart-safe completion, and abort at the `N + 5` request budget;
- bounded disk spooling for generic whole-object uploads, multipart parts, and
  cross-bucket object transfer;
- logical clone, fetch, push, and repair with destination-issued `VersionId`
  rebinding and destination-local commit IDs;
- independent lease renewal for writable AWS-shaped clients, with fail-closed
  admission after an ambiguous or conflicting renewal.

The four-call whole-object path, `N + 5` multipart path, and logical
clone/incremental-push path pass against the pinned RustFS image. RustFS
beta.10 does not return per-part SHA-256 values from `ListParts`, rejects the
AWS full-object checksum mode at multipart creation, and can briefly omit a
new upload from `ListMultipartUploads`; the client therefore persists the
original signed upload handle, accepts caller-carried completion checksums and
part sizes after restart, and treats provider listings as discovery rather
than completion authority.

Automatic checkpoint scheduling, the migration/bootstrap command surface,
the bounded resumable-transfer batch API, broader crash drills, and the AWS
scale gates remain release work. None of the local evidence qualifies the
profile for production or million-object scale.

## Decision

Add a persisted repository profile named `native-versioned-v1` with these
properties:

- The physical bucket has native versioning enabled.
- A live object is stored once, as a whole object at its original S3 key.
- The object version records the exact provider-issued S3 `VersionId`.
- Prolly trees remain the authority for current state, version history,
  branches, commit sessions, merge, and restore.
- Exactly one fenced writer service owns the repository. Concurrent callers
  queue inside that service; independent writer processes are unsupported.
- Readers always resolve a logical object version through Prolly and issue
  `GetObject(key, version_id)`. A raw unversioned `GetObject(key)` is
  diagnostic only.
- New Prolly nodes created by one bucket commit are packed into one immutable
  S3 object.
- One small immutable bucket-commit object and one conditional branch-ref
  update complete publication.

The steady-state foreground budget for a conflict-free 64 KiB put is four S3
calls:

1. `PutObject` at the original key;
2. put one immutable Prolly node pack;
3. put one immutable bucket commit;
4. compare-and-exchange the branch ref.

This four-call claim applies to a warm, long-lived writer. Provider retries,
writer-lease renewal, cold reopen, checkpoint creation, GC, and multipart part
uploads are measured separately and are never hidden inside that number.

## Why this profile

The current profile stores a small object as a content chunk, a content-index
tree, and a content manifest before updating three logical-bucket trees and
publishing the commit. That gives the repository its own content-addressed
data plane, but it duplicates capabilities already supplied by a versioned S3
bucket and amplifies requests for ordinary files.

The new profile keeps the part Prolly is good at: an ordered, immutable history
of logical metadata. Native S3 keeps the bytes and supplies physical versions.
The two identities remain distinct:

- `ObjectVersionId` is a logical repository identity.
- S3 `VersionId` identifies the physical bytes or physical delete marker.
- `StorageToken` protects a mutable repository record such as a branch ref.

## Goals

- At most four foreground S3 calls for a warm, conflict-free single-object
  put, copy, or delete.
- One payload copy at the original key; no repository content chunks,
  content-index tree, or content manifest.
- Exact current and historical reads even when the physical bucket's current
  version belongs to another branch.
- Preserve logical branches, atomic commit sessions, merge, restore,
  idempotency, conditional writes, and delete-marker behavior.
- Make every visible bucket commit recoverable by a new process using only
  durable bucket state.
- Reclaim unpublished native versions and node packs without risking retained
  history.
- Qualify AWS latency, request cost, throttling, hot-branch behavior, cold
  recovery, one million live keys, and ten million retained versions.

## Non-goals

- Supporting unmanaged writers to the same object-key namespace.
- Treating raw, unversioned S3 reads or listings as canonical.
- Supporting several independent writer processes.
- Repository-level chunking, deduplication, or partial-file mutation.
- Preserving provider `VersionId` values when cloning to another bucket.
- Transparently converting an existing repository in place.
- Claiming that a large multipart upload takes fewer than its native S3
  create, part, and completion calls.
- Supporting S3 directory buckets, which do not provide the required native
  version history.

## Authority and invariants

The implementation must fail closed when any invariant is false.

1. **Prolly is authoritative.** A native S3 version is visible only after a
   reachable bucket commit names it.
2. **Native versioning is mandatory.** Repository create and writable open
   require `Enabled`, not `Unversioned` or `Suspended`.
3. **Every physical mutation returns an identity.** A successful put, copy,
   multipart completion, or delete must yield a non-empty S3 `VersionId`.
4. **Canonical reads are exact.** Live reads use `GetObject` with the recorded
   `VersionId`; GC deletes with `DeleteObject(key, version_id)`.
5. **One fenced writer publishes.** A bucket commit and its branch ref carry
   the current writer-fence generation. Credential isolation and exclusive
   process ownership prevent overlapping writers; branch CAS rejects a stale
   base state.
6. **Publication is ordered.** Payload version, node pack, and bucket commit
   are durable before the branch ref changes.
7. **The branch ref is the visibility point.** Objects written before a failed
   ref update are unreachable orphans, not partially committed state.
8. **Lifecycle never removes retained closure.** Noncurrent-version expiry and
   current-version expiry are forbidden for managed keys. Prolly GC is the
   retention authority.
9. **Repository metadata is isolated.** The prefix
   `.prolly/native-versioned/v1/` is reserved and rejected as a user key.
10. **Caches do not affect results.** Node-locator indexes and local caches are
    advisory indexes and must be rebuildable from retained bucket commits and
    node-pack tables of contents.

## Persisted profile and compatibility

Repository initialization records the profile in a create-once format marker:

```rust,ignore
pub enum RepositoryStorageProfile {
    DistributedContentAddressedV1,
    NativeVersionedV1,
}
```

The marker also records minimum reader and writer capabilities. A client must
decode and validate it before reading any profile-specific object-version
record. Older clients and clients built without native-version support reject
the repository before performing writes.

Existing `RepositoryFormatV1` repositories retain their current layout and
semantics. `native-versioned-v1` is created under a fresh repository prefix;
there is no mixed-content mode and no in-place marker rewrite.

## Logical and physical data model

The logical body is hashed independently from the provider binding. This
allows a clone to preserve logical object identities when the destination S3
bucket assigns different native `VersionId` values.

```rust,ignore
pub struct ObjectVersionV2 {
    pub id: ObjectVersionId,
    pub body: LogicalObjectVersionBodyV2,
    pub binding: NativeObjectBindingV1,
}

pub struct LogicalObjectVersionBodyV2 {
    pub order: ObjectVersionOrder,
    pub created_at_millis: u64,
    pub kind: LogicalObjectVersionKindV2,
}

pub enum LogicalObjectVersionKindV2 {
    Live {
        size: u64,
        logical_etag: String,
        headers: ObjectHeaders,
        checksums: Checksums,
        user_metadata: BTreeMap<String, String>,
        tags: BTreeMap<String, String>,
    },
    DeleteMarker,
}

pub enum NativeObjectBindingV1 {
    Live {
        version_id: String,
        provider_etag: String,
        checksum_sha256: [u8; 32],
    },
    DeleteMarker {
        version_id: String,
    },
}
```

`ObjectVersionId` is derived from repository ID, logical key, operation ID,
and canonical logical body. The native binding is authenticated by the bucket
commit but excluded from that logical ID derivation. The Prolly versions tree
stores the complete `ObjectVersionV2`, including the binding.

The live binding deliberately does not store another physical key. In this
profile the physical key is the logical UTF-8 key, byte for byte. This removes
aliasing and prevents a record from redirecting a read outside the managed
namespace.

The response ETag remains the logical S3-shaped ETag. The provider ETag is
diagnostic and must not be interpreted as MD5, especially for multipart or
encrypted objects.

## Physical layout

User data stays outside the repository prefix:

```text
photos/2026/launch.jpg                         # native S3 versions of the file
reports/q3.parquet                             # native S3 versions of the file

.prolly/native-versioned/v1/
├── format.cbor                                # create-once profile marker
├── writers/lease.cbor                         # mutable fenced writer lease
├── refs/heads/<encoded-branch>.cbor            # mutable branch ref
├── refs/tags/<encoded-tag>.cbor                # mutable tag ref
├── commits/sha256/aa/bb/<commit-id>.cbor       # immutable bucket commits
├── node-packs/sha256/aa/bb/<pack-id>.pack      # immutable Prolly nodes
├── node-index/checkpoints/<generation>.cbor    # advisory index checkpoint
├── pins/<encoded-name>.cbor                    # retention pins
├── gc/...                                      # resumable mark/sweep state
└── maintenance/...                             # fsck and migration state
```

The same-bucket layout intentionally reserves one key prefix. Deployments that
must expose every possible S3 key, including the reserved prefix, need a future
separate control-bucket profile.

## Packed Prolly node store

One logical mutation may create nodes in the objects, versions, and operations
trees. The repository buffers those immutable nodes in a commit-scoped batch,
deduplicates them by CID, and writes one pack.

```rust,ignore
pub struct NodePackV1 {
    pub format_digest: TreeFormatDigest,
    pub entries: Vec<NodePackEntryV1>, // sorted by CID
    pub attachments: Vec<NodePackAttachmentV1>,
    pub payload: Vec<u8>,              // concatenated canonical node bytes
}

pub struct NodePackEntryV1 {
    pub cid: Cid,
    pub offset: u64,
    pub len: u32,
    pub sha256: [u8; 32],
}

pub struct NodePackRefV1 {
    pub id: NodePackId,
    pub object_len: u64,
    pub node_count: u32,
}

pub struct NodePackAttachmentV1 {
    pub kind: NodePackAttachmentKindV1,
    pub digest: [u8; 32],
    pub offset: u64,
    pub len: u32,
}

pub enum NodePackAttachmentKindV1 {
    BucketDelta,
}
```

The pack ID hashes the complete canonical pack. Reads use ranged GETs after
locating the CID in its table of contents. Every returned node is verified
against its CID.

Each bucket commit records the pack containing nodes introduced by that
commit. The union of reachable commit records is therefore a canonical way to
rebuild `CID -> (pack, offset, length)`. A periodically written node-index
checkpoint accelerates open and lookup, but it is an advisory index: deleting
it can hurt latency, never correctness.

The write path must expose a commit-scoped `NodePublicationBatch`; allowing the
three state trees to flush independently would break the one-pack call budget.

### Checkpoint and reopen policy

- Create a checkpoint after a configurable commit or byte threshold.
- Publish checkpoints in the background and account for their S3 calls in a
  separate metric.
- On writable open, load the newest valid checkpoint and replay the bounded
  bucket-commit tail.
- If all checkpoints are missing or corrupt, rebuild from reachable commits
  and pack tables of contents.
- Do not admit writes until the writer can locate the complete base-state
  closure.
- The AWS release gate requires writable reopen in at most 30 seconds at the
  qualified scale with the newest checkpoint present. Full rebuild is a
  separately measured repair operation.

## Bucket commit and branch ref

The v2 bucket commit embeds data that currently requires separate delta and
normal-path reflog objects:

```rust,ignore
pub struct BucketCommitV2 {
    pub state: BucketStateV1,
    pub parents: Vec<CommitId>,
    pub generation: CommitGeneration,
    pub changes: BucketChangeSummaryV2,
    pub node_pack: Option<NodePackRefV1>,
    pub writer_fence_generation: u64,
    pub author: String,
    pub message: Option<String>,
    pub created_at_millis: u64,
    pub metadata: BTreeMap<String, Vec<u8>>,
}

pub enum BucketChangeSummaryV2 {
    Inline(BucketDeltaV1),
    Packed {
        digest: [u8; 32],
        len: u32,
    },
}

pub struct RefValueV2 {
    pub target: CommitId,
    pub previous_target: Option<CommitId>,
    pub generation: RefGeneration,
    pub operation: OperationId,
    pub writer_id: String,
    pub writer_fence_generation: u64,
    pub updated_at_millis: u64,
    pub tombstone: bool,
}
```

Normal history comes from commit parents and embedded changes. Administrative
ref moves that do not create a bucket commit may use a separate immutable
audit record; their extra call is not charged to ordinary writes.

Large commit-session deltas are encoded inside the node pack and referenced by
the bucket commit rather than emitted as another S3 object. The commit object
has a strict maximum size.

## Exclusive writer and fencing

The writer service acquires `.prolly/native-versioned/v1/writers/lease.cbor`
with compare-and-exchange. The record contains repository ID, writer ID,
monotonic generation, random fencing token, expiry, and last-renewed time.

Rules:

- Acquisition, renewal, and handoff are conditional writes.
- A takeover increments the generation.
- Every bucket commit and branch ref carries that generation.
- Publication verifies the locally cached monotonic lease deadline immediately
  before branch-ref CAS; lease renewal runs independently of the operation.
- Losing renewal stops admission of new mutations and fails closed before the
  ref update.
- A branch-ref conflict in this profile is a fencing or invariant failure. The
  writer does not enter the distributed logical retry loop.
- Multiple application callers may be concurrent, but one in-process commit
  sequencer orders branch publications. Work may be prepared concurrently.
- Automatic takeover based only on lease expiry is forbidden. A coordinator or
  operator must first prove the old process has stopped or lost its write
  credentials. The new writer then CAS-updates each branch ref to the same
  target with the new fence generation before admitting mutations.

Lease renewal is amortized service traffic. Its calls and failures are exposed
separately from logical-operation call counts.

S3 cannot atomically make a branch-ref update conditional on a separate writer
lease object. The lease is therefore cooperative coordination, not a hard
cross-object fence. The hard boundary is that only the exclusive writer
service holds the write role. Deployments that cannot guarantee credential
isolation and non-overlapping handoff do not qualify for this profile.

IAM and bucket policy provide the outer fence. Only the writer role can put or
delete managed object keys and repository metadata. Reader roles receive exact
version reads and the minimum metadata reads needed by the client. No other
principal may mutate managed keys.

## Protocols

### Put object

1. Admit the operation through the fenced writer and serialize its branch
   publication slot.
2. Load the cached branch ref and evaluate logical preconditions against that
   exact base bucket commit.
3. Below the qualified single-put threshold, stream the whole body to
   `PutObject(logical_key)`. Add the repository ID, operation ID,
   writer-fence generation, and logical checksum as reserved object metadata.
   Require a non-empty returned `VersionId`. Use native multipart above that
   threshold.
4. Build the logical object version with the returned native binding and update
   the objects, versions, and operations Prolly trees in one node batch.
5. Put the immutable node pack.
6. Put the immutable bucket commit.
7. Revalidate the writer lease and compare-and-exchange the branch ref.
8. Return the logical object version and bucket commit IDs.

The input stream is not buffered merely to implement repository chunking.
Checksums are computed while streaming; SDK/provider checksum trailers are
used where available. If a caller requests validation that cannot be computed
in one pass, the API requires a replayable body or rejects that combination
before upload.

### Get object

1. Pin a bucket commit from the requested branch or explicit snapshot.
2. Resolve the logical key and optional `ObjectVersionId` through the Prolly
   trees.
3. For a live record, call `GetObject(key, version_id)` with the exact native
   binding. Preserve range and conditional-read behavior.
4. For a current delete marker, return `NoSuchKey`; for an explicitly selected
   logical delete marker, return delete-marker metadata.

A warm read requires one S3 data call. A cold read may additionally load a
branch ref, bucket commit, checkpoint, and one or more node-pack ranges. Those
metadata calls are reported as cold-read amplification.

### Delete object

Call `DeleteObject(key)` so the versioned bucket creates a native delete
marker. Require its returned `VersionId`, create the logical delete marker,
then publish node pack, bucket commit, and branch ref. A warm delete therefore
uses four calls.

### Copy object

Use native `CopyObject` with the exact source `VersionId` to create a
destination version at the destination key, capture its new `VersionId`, and
publish the logical mutation. A metadata-only logical copy optimization is not
used because the original-key bucket must contain an independent destination
object.

### Multipart upload

Multipart remains native S3 multipart:

- create and upload parts directly against the original key;
- do not put parts or a chunk index in Prolly;
- on successful native completion, require the completed object's
  `VersionId`;
- then publish one node pack, one bucket commit, and one branch-ref CAS;
- aborting multipart creates no logical object version.

For `N` parts, a complete upload requires at least `N + 5` foreground calls:
create, `N` uploads, complete, node pack, bucket commit, and branch ref.
Only the post-upload completion phase has a four-call target.

### Atomic commit session

Upload each staged object as one native version, then apply every mutation to
one Prolly state and publish one node pack, one bucket commit, and one branch
ref. A two-object put session therefore targets five calls, not eight. If
publication fails, neither uploaded version is logically visible.

### Merge and restore

Merge and restore normally select bindings that already exist. They write no
payload and target three calls: node pack, bucket commit, and branch ref. If a
policy requires copying bytes to a new original-key version, each copy is an
additional explicit data call.

### Clone, fetch, push, and repair

Cross-bucket transfer operates on logical history, not physical repository
paths. Copying commit objects and node packs byte for byte is invalid because
their native bindings name source-bucket `VersionId` values.

A transfer follows this protocol:

1. Pin every selected source ref and compute its retained commit closure.
2. Traverse commits in parent-before-child order and assign each source
   commit a destination commit ID.
3. For each previously unseen live logical object version, read the exact
   source `(key, VersionId)`, stream it to the destination's original key,
   verify its logical checksum, and capture the destination `VersionId`.
4. Recreate retained delete markers in the same per-key logical order and
   capture their destination `VersionId` values.
5. Rebuild each logical version with its destination binding. Preserve its
   `ObjectVersionId` because binding data is excluded from logical identity.
6. Rebuild Prolly state, node packs, and bucket commits with mapped parent IDs.
   Commit IDs change because each commit authenticates destination bindings.
7. Verify the complete destination closure before updating destination refs.
8. Move each destination ref through its local writer fence and branch CAS.

Each completed destination commit is also an immutable transfer checkpoint.
The transfer implementation derives a binding-independent logical commit
fingerprint, scans both referenced and unreferenced destination commits, and
uses matching fingerprints as the durable source-to-destination commit map.
Payload requests carry the source operation ID and checksum, so a response-lost
payload can be reconciled at its exact key. This self-journaling layout avoids
adding a mutable journal write to every copied version or commit. A future
bounded batch API may add a small mutable cursor for discovery progress, but
that cursor is advisory and does not replace immutable commit checkpoints.

A repair uses the same replay protocol. If fsck finds a missing destination
binding, repair deliberately forces fresh bindings, rebuilds the selected
history, verifies it, and moves the destination branch through local CAS. It
never publishes a commit whose binding still points at the source bucket.

## Idempotency and outcome reconciliation

The operation ID remains the durable idempotency handle.

- The operation record is written into the operations Prolly tree in the same
  node pack as the mutation.
- A lost branch-ref response is reconciled by reading the branch ref and its
  selected bucket commit, then checking the operation record.
- A retried request with the same operation ID and input digest returns the
  prior logical result without uploading again.
- Reuse with a different input digest fails with `IdempotencyConflict`.

A payload request whose response is lost is more difficult because the caller
may not know its `VersionId`. Each native object upload carries the operation
ID and checksum in user metadata. Reconciliation performs a bounded
`ListObjectVersions` for that exact key and verifies candidate metadata and
checksum before continuing. More than one matching candidate is a fail-closed
repair case; the client never guesses.

## Failure behavior

| Failure point | Visible state | Recovery |
| --- | --- | --- |
| Before native payload succeeds | Old branch state | Retry normally. |
| Payload succeeds, response lost | Old branch state; possible orphan version | Reconcile exact key by operation metadata and checksum. |
| After payload, before node pack | Old branch state; orphan native version | GC after grace period. |
| After node pack, before bucket commit | Old branch state; orphan version and pack | GC after grace period. |
| After bucket commit, before ref CAS | Old branch state; unreachable proposed closure | GC after grace period. |
| Branch-ref CAS conflicts | Old branch state for this operation | Fence writer, stop publication, require operator/reopen. |
| Ref CAS succeeds, response lost | New state is visible | Reconcile operation from selected commit and return success. |
| Writer lease expires before CAS | Old branch state | Do not publish; revoke/stop the old writer before a credential-isolated handoff. |
| Retained native version is missing | Repository closure is incomplete | Fail read/fsck; restore from qualified backup. |
| External unindexed version appears | No logical effect | Alert and quarantine; never auto-adopt. |

## Garbage collection and retention

GC traces every branch ref, tag, retention pin, in-progress protected
operation, and policy-retained bucket commit. It marks:

- bucket commits and their node packs;
- every exact `(logical key, native VersionId)` reachable from retained logical
  object versions;
- native delete-marker versions when retained;
- required format, ref, pin, and maintenance records.

Sweep rules:

- Apply a grace period greater than the maximum writer-lease duration and
  maximum operation-reconciliation window.
- Delete native payloads and delete markers only with exact `VersionId`.
- Never use key-only deletion during GC.
- Treat Object Lock or legal-hold rejection as retained and report it.
- Recompute reachability from canonical roots after a crash; checkpoints may
  resume enumeration but never replace the mark proof.
- Reclaim an unpublished native version only after proving it is absent from
  all retained object-version bindings.

Bucket lifecycle may transition retained noncurrent versions to a storage
class the client supports, but it may not expire them. Abort-incomplete-
multipart rules are allowed because uncompleted parts are not repository
history.

## Security and operational requirements

- Enable native versioning before repository creation and continuously verify
  it remains enabled.
- Deny direct `PutObject`, `DeleteObject`, multipart completion, and metadata-
  prefix mutation except from the fenced writer role.
- Keep the writer role inside the exclusive service. If application code can
  access the same raw AWS credentials, bucket policy cannot distinguish a
  wrapper call from a bypass call and authority is only a coding convention.
- Permit readers to use `GetObjectVersion`; grant version listing only to
  components that need history, reconciliation, fsck, or GC.
- Include the necessary SSE-KMS permissions for both original objects and
  repository metadata.
- Deny or continuously audit lifecycle rules that expire managed current or
  noncurrent versions.
- Emit writer generation, branch generation, operation ID, logical version ID,
  native `VersionId`, and bucket commit ID in structured traces.
- Alarm on missing `VersionId`, branch-ref conflict, unindexed native version,
  versioning suspension, lifecycle drift, checkpoint lag, GC backlog, and
  closure corruption.
- Back up both native object versions and repository metadata. A metadata-only
  backup cannot restore exact reads.

## Request budgets

Foreground budgets exclude provider retries and are enforced independently
from cold-open and background-maintenance budgets.

| Logical operation | Warm foreground S3 calls | Notes |
| --- | ---: | --- |
| 64 KiB put | 4 | Payload + pack + commit + branch CAS. |
| Delete | 4 | Native delete marker + three metadata calls. |
| Copy | 4 | Native copy + three metadata calls. |
| Two-object commit session | 5 | Two payloads + one shared publication. |
| Warm current/historical get | 1 | Exact native-version GET. |
| Merge or restore | 3 | Reuses existing native bindings. |
| Multipart, `N` parts | `N + 5` | Native create/parts/complete + publication. |
| Push, `N` copied object versions in one commit | `N + 3` destination writes | `N` destination payload writes + pack + commit + ref. Source exact GETs and history discovery are reported separately; end-to-end transfer is at least `2N + 3`. |

The implementation records SDK calls by category: payload, node pack, commit,
ref, cold metadata, provider retry, reconciliation, writer lease, checkpoint,
GC, and repair. A release result must never report only the foreground number.

## Production qualification

RustFS is the deterministic development and crash-test provider. AWS is the
release authority for latency, cost, throttling, versioning semantics, and
scale.

### Correctness gates

- Native versioning enabled, suspended, and accidentally disabled cases.
- Exact current and historical reads across put, copy, delete, restore, merge,
  and multiple branches.
- Native multipart completion and abort.
- Crash injection after every protocol step in the failure table.
- Lost responses for payload upload and branch-ref CAS.
- Writer-lease expiry, takeover, and stale-writer branch publication.
- Credential-isolated handoff and rejection of automatic overlapping takeover.
- GC with unpublished versions, node packs, retained history, pins, Object
  Lock, and lifecycle drift.
- Checkpoint deletion/corruption followed by deterministic index rebuild.
- Fsck detects every missing or corrupt native version, bucket commit, pack,
  and node.

### Request and load gates

- Warm 64 KiB put: maximum four foreground calls at 1, 8, and 32 concurrent
  application callers.
- Concurrency uses one writer queue; logical branch-CAS retries must remain
  zero. Report queue time separately from service time.
- At least 1,000 AWS samples each for put, current get, and historical get.
- At least 250 AWS samples each for two-object commit and multipart completion.
- Measure p50, p95, p99, error rate, provider retry rate, throttle rate, bytes,
  and request cost at expected key counts and traffic levels.
- Run hot-branch tests with sustained caller concurrency and realistic writer
  lease renewal.
- No hidden checkpoint or GC call may be charged to an unreported category.

### Scale and recovery gates

- At least one million live logical keys.
- At least ten million retained logical object versions.
- Writable reopen within 30 seconds using the newest valid checkpoint.
- Bounded-memory index rebuild after local-cache loss.
- Cold point-read latency reported with all branch, commit, checkpoint, pack,
  and payload calls.
- GC mark and sweep complete within an operator-defined window without
  unbounded memory or list calls.
- Clone/restore proves every logical checksum after destination `VersionId`
  rebinding.

Million-object production readiness is not claimed until these AWS gates pass.

## Migration and bootstrap

Migration creates a new `native-versioned-v1` repository. It never rewrites an
existing format marker.

### From the content-addressed profile

1. Stop old writers and pin the cutover bucket commit.
2. Create a fresh native-versioned repository and acquire its writer fence.
3. Materialize each retained logical live version as one native S3 object
   version at the original key, preserving logical checksum and metadata.
4. Recreate delete markers and bucket commits in deterministic order.
5. Preserve logical object-version IDs where the logical body is unchanged;
   record newly assigned native `VersionId` bindings.
6. Verify logical listings, exact reads, checksums, branch refs, and retained
   history on both sides.
7. Switch all readers and the exclusive writer together.

### From an existing ordinary versioned bucket

Bootstrap requires an exclusive maintenance window. Inventory every key and
native version, reject the reserved metadata prefix, and build Prolly history
from exact version records. S3 does not provide a reliable total order across
different keys, so bootstrap must either:

- import one current-state bucket commit plus per-key historical versions; or
- use an external authoritative event log to reconstruct cross-key commits.

The importer must not invent atomic relationships from `LastModified` ties.

Native `VersionId` values cannot be preserved when the target is a different
physical bucket. Clone copies bytes, verifies logical checksums, records the
new bindings, and necessarily creates new bucket commit IDs.

## API impact

The AWS-shaped client surface remains recognizable. Configuration adds:

- explicit `native-versioned-v1` selection at create time;
- writer identity and lease settings for writable clients;
- read-only mode for readers;
- reserved-prefix validation;
- separate metrics for warm foreground, cold metadata, provider retry, and
  background traffic.

### Relationship to the SlateDB proposal

This profile is an alternative to the SlateDB proposal's original payload
layout, not an implicit upgrade to it. The earlier proposal retains repository
content chunks and changes how Prolly nodes are stored. `native-versioned-v1`
removes repository content chunks and introduces its own commit-scoped node
pack. SlateDB may later accelerate the advisory CID locator, but it must remain
rebuildable and must not become another foreground publication call.

Raw provider access is intentionally outside the correctness contract. The
client documentation must describe direct reads as diagnostics and direct
writes as repository corruption risk.

An uploaded native version becomes the physical bucket's raw current version
before its branch ref is published. Consequently, raw readers can observe an
uncommitted or branch-inappropriate version. Atomic visibility and history are
guaranteed only through the wrapper's exact-version reads.

## Implementation plan

Each phase must land with codec fixtures, upgrade rejection tests, and request
accounting before the next phase depends on it.

1. **Persist the profile.** Add the format descriptor, capability negotiation,
   create/open validation, and reserved-key guard.
2. **Add the v2 object model.** Separate logical object identity from native
   binding and add canonical codec fixtures.
3. **Add the native data plane.** Implement exact-version put, get, delete,
   copy, multipart completion, checksums, and lost-response reconciliation.
4. **Add commit-scoped node packing.** Buffer publications across all state
   trees, implement pack range reads, and verify CIDs.
5. **Add advisory checkpoints.** Implement bounded reopen, deterministic
   rebuild, corruption fallback, and checkpoint metrics.
6. **Collapse publication.** Embed changes in `BucketCommitV2`, remove ordinary
   per-write lease/delta/reflog objects, and publish with one ref CAS.
7. **Fence the exclusive writer.** Add acquisition, renewal, takeover, queueing,
   fail-closed conflicts, and IAM examples.
8. **Complete repository features.** Adapt commit sessions, branches, merge,
   restore, clone, push, and operation lookup to native bindings.
9. **Add GC, fsck, and repair.** Trace exact native versions and packs, test
   every crash boundary, and produce operator reports.
10. **Add migration and qualification.** Build import/export tools, RustFS
    verification, AWS load/cost/throttle gates, and million-key recovery tests.

## Rejected alternatives

### Keep repository chunks for small files

This preserves deduplication but retains the content-index and manifest writes
that dominate ordinary-file amplification. It does not match the thin-wrapper
goal.

### Store only the native key, not `VersionId`

This makes reads depend on the physical bucket's current version. Branches,
history, crash isolation, and concurrent prepared work become incorrect.

### Let applications write S3 directly and index afterward

There is a window in which raw state and Prolly state disagree, and the indexer
cannot reliably recover multi-object atomic intent or lost delete events.
Exclusive authoritative writes are required.

### Put every Prolly node in a separate S3 object

This stores one object per node, but a single logical mutation rewrites several
root-to-leaf paths and cannot meet the four-call target.

### Treat the node-locator checkpoint as authority

A missing or stale checkpoint would make a retained bucket commit unreadable.
The authoritative locator chain therefore comes from bucket commits and their
immutable node-pack references.

## Open implementation questions

These do not change the architectural decision but must be resolved before the
format is marked stable:

- Exact maximum node-pack and bucket-commit sizes.
- Checkpoint thresholds that satisfy the 30-second reopen gate at qualified
  scale.
- Whether checksum trailers cover every supported replayable and streaming
  body shape without local spooling.
- The bounded search window and operator workflow for ambiguous lost-payload
  reconciliation.
- Whether deployments needing the reserved key prefix require a separate
  control-bucket profile in the first release.
- The initial storage-class matrix for historical exact-version reads.
