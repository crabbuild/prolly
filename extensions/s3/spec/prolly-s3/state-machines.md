# Prolly S3 state machines

## Repository initialization

1. Validate bucket versioning and provider capabilities.
2. Store a signed provider attestation.
3. Create the repository format and initialization records.
4. Create the initial commit, publication event, branch ref, and empty advisory
   index heads.
5. Complete only after reopening and validating the authoritative branch.

Initialization is idempotent only for identical repository identity and format.

## Branch publication

1. Acquire or validate authority for the selected branch.
2. Read the branch ref and operation-index window.
3. If the operation ID is already applied with identical identity, return the
   recorded receipt.
4. Upload immutable payload candidates and tree nodes.
5. Store the immutable commit and publication event.
6. Compare-and-swap the branch ref from the observed physical version.
7. If the response is ambiguous, reconcile by operation ID and ref/event
   identity before fencing.
8. If another commit won, return a ref conflict and leave candidates
   unreachable.
9. Advance advisory indexes independently.

Only step 6 makes the commit visible.

## Authority renewal and takeover

- A writable client periodically renews every held branch lease.
- Renewal preserves scope and generation and conditionally replaces the
  observed physical version.
- A lost response is reconciled by reading the exact current authority record.
- An unequal successor fences the local branch.
- Takeover requires a declared previous writer/generation and installs a higher
  generation after the operational isolation barrier.
- Stale writers validate authority before payload upload.
- Independent branches have independent leases and publication lanes.

## Durable commit session

1. Snapshot branch head and allocate batch and operation identities.
2. Upload each whole-file payload while staging its logical mutation.
3. Periodically write canonical checkpoints.
4. Resume from the latest valid checkpoint after restart.
5. Build the new Prolly roots in a single batched tree update.
6. Publish once through the branch publication state machine.
7. Mark success or abort; clean expired mutable checkpoints in bounded pages.

Payloads are never chunked. Ephemeral sessions omit remote checkpoints.

## Journal and operation indexes

- Open a stable publication-journal cursor from the current branch ref.
- Consume at most the configured event budget.
- Apply events oldest-to-newest to immutable index trees/segments.
- Conditionally advance one branch-local head.
- Ref movement does not invalidate the already-open immutable traversal.
- Exceeding the permitted unindexed tail fails closed.
- Rebuild uses a persisted stable cursor and bounded chunks; it never silently
  scans the namespace during a foreground lookup.

## Structural merge

1. Snapshot source and target heads and create a durable job.
2. Discover all best bases with bounded persisted graph frontier work.
3. Require explicit base selection for ambiguous criss-cross history.
4. Structurally diff base/target/source and persist changes and conflicts.
5. Build output trees in bounded pages.
6. Revalidate the target branch.
7. Publish one two-parent commit through its ref CAS.
8. Reconcile ambiguous publication by operation ID.
9. Clean only job-scoped mutable administration state in bounded pages.

A moving target is a ref conflict, never an implicit rebase.

## Read

- Current read: ref → publication → commit → root → tree path → payload.
- Historical read: explicit commit → root → tree path → payload.
- Verify all immutable content IDs.
- Treat cache or advisory-index failure as miss/rebuild, never new truth.
