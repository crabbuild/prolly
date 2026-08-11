# Protocol state machines

## Repository initialization

States: `Absent → Intent → Closure → Format → MainRef → Ready`.

1. Qualify the provider and choose canonical limits/tree format.
2. Derive RepositoryId from the initialization OperationId.
3. Create `InitializationIntentV1` immutably.
4. Create empty roots, optional node pack, initial commit, and reflog closure.
5. Create `format/v1.cbor` immutably.
6. Create the default branch ref conditionally.
7. Re-read format/ref and validate the complete closure.

Recovery resumes from the first absent object. Any existing object must decode
and equal the proposed canonical value. A conflict is never overwritten.

## Exclusive writer lease

States: `Absent | Expired → Held(g) → Renewed(g) → Released/Expired`.

A writer acquires with compare-and-swap, setting generation to previous+1 (or
1), a fresh nonzero 32-byte fencing token, and expiry after update time. Only
the holder with the current storage token may renew or release. Every provider
mutation and published ref carries the held generation. Before publication the
writer reloads the lease and verifies writer ID, generation, token, and expiry.
A stale writer MUST stop and return `RefConflict`/`OutcomeUnknown`; it may not
publish under a newer lease.

## Put, delete, and copy

States: `Prepared → PhysicalWritten → Versioned → ClosureStored → Published`.

1. Load branch ref/commit and evaluate logical preconditions.
2. Canonicalize the input and reserve the OperationId.
3. Write/copy/delete the S3 object, obtaining an exact VersionId.
4. Construct and validate `ObjectVersionV1`; its logical ID excludes binding.
5. Update object/version/operation Prolly trees and store node closure.
6. Store `BucketCommitV1` immutably.
7. Create reflog and compare-and-swap `RefValueV1` from the loaded token.
8. Re-read the ref when the conditional outcome is uncertain.

Failure before step 7 leaves unreachable physical data and is safe for GC.
Failure/conflict at step 7 does not make the commit visible. The operation may
be retried: the implementation reconciles the OperationId and physical version
before performing another provider mutation.

## Idempotency

An operation record stores `domain_hash(operation-input/v1, canonical input
parts)` and its canonical result. Same ID + same digest returns the stored
receipt with `idempotent_replay=true`. Same ID + different digest returns
`IdempotencyConflict`. An absent logical record after an uncertain S3 response
requires provider reconciliation using operation metadata before rewriting.

## Batch commit

States: `Open → PhysicalPrepared → Committed | Expired/Aborted`.

The durable `PhysicalBatchV1` fixes branch, base commit, operation, message, and
expiry. Mutations have unique logical keys. Provider writes happen first and
produce `PhysicalPreparedMutationV1` bindings. One commit applies all prepared
mutations and one ref CAS publishes them atomically. A changed base yields
`BatchConflict`; physical results remain unreachable and collectible.

## Multipart

States: `Created → Parts → CompletedPhysical → Published | Aborted`.

The original `PhysicalMultipartSessionV1` is required for completion; a session
discovered by listing is read-only and cannot publish because it lacks the
original operation/fence authority. Complete validates ordered parts and whole
checksums, obtains the exact VersionId, then follows Put from `Versioned`.
Abort is idempotent. `NoSuchUpload` after an uncertain complete triggers exact
version/operation reconciliation rather than a new upload.

## Branches, tags, merge, restore

Branch/tag names use [paths.md](paths.md) validation. Ref generation starts at
1 and increments exactly once per successful CAS. Commits are immutable and
parent generation is strictly less than child generation; a normal commit has
one parent, initialization has none, and merge has two or more distinct
parents. Tags identify immutable commits but their tag records are CAS-managed.
Restore creates new logical versions; it never rewrites old commits.

## Clone, fetch, and push

Logical history is replayed in topological/generation order. The destination
must be empty for clone unless the operation explicitly targets an existing
compatible repository. Live payloads are copied or streamed from the exact
source VersionId; delete markers are recreated. Logical ObjectVersionIds are
preserved only after payload/checksum verification, while destination provider
bindings contain new VersionIds. Ref publication occurs after all destination
closure exists.

## Node packs and index checkpoints

A pack object is `PLYPACK1 || u32be(header_length) || canonical_toc || payload`.
Entries are strictly CID-sorted, nonempty, nonoverlapping within payload, and
have `CID == SHA256(node bytes) == entry.sha256`. Attachments are bounded and
digest-verified. Checkpoint entries are strictly CID-sorted and absolute
offsets point past the 12-byte fixed header and canonical TOC.

## Garbage collection

States: `MarkRunning → Planned → SweepRunning ↔ Paused → Completed | Aborted`.

Mark snapshots every live branch, tag, nonexpired retention pin, and the cutoff
time into `GcFenceV1`; it traces the complete commit/node/physical-version graph.
Candidates must be older than the grace cutoff and absent from reachability.
Sweep reloads roots/fence before each destructive window, rechecks candidate
reachability, and deletes only the exact VersionId recorded in the plan. It
persists the next index and counters after each bounded window. Missing exact
versions count as already missing. Changed authority that invalidates the fence
pauses or aborts; it never broadens deletion.

## Crash matrix

| Crash point | Observable result | Recovery |
|---|---|---|
| before physical write | none | retry same operation |
| after physical write | unreachable version | reconcile, then continue or GC |
| after nodes/commit | unreachable closure | retry ref CAS or GC |
| during ref CAS | unknown | reload ref and operation record |
| after ref CAS | committed | replay returns receipt |
| during multipart | incomplete upload | resume from listed verified parts |
| during GC mark | no deletion | recompute from canonical roots |
| during GC sweep | exact subset deleted | resume persisted next index |
