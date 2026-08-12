# Protocol v2 authority state machines

## Branch-shard lease

Each branch is initially its own authority shard. This makes one branch-ref
CAS the complete takeover barrier and avoids provider listing in the authority
path.

States:

`Absent → Active(g) → Renewed(g) → BarrierPending(g+1) → Active(g+1)`

- `Absent → Active(1)` uses create-if-absent.
- Only the current unexpired active permit may renew through its storage-token
  compare-and-swap.
- Expiry never permits automatic reacquisition. It requires explicit takeover.
- Ambiguous or conflicting renewal invalidates only that shard's local permit.
- Different branch lease objects can be acquired and renewed independently.

## Takeover barrier

Takeover separates lease acquisition from publication authority because S3
cannot make a branch-ref CAS conditional on a separate lease object.

1. Compare-and-swap the expected lease to `BarrierPending(g+1)`.
2. Compare-and-swap the branch ref without changing its target, stamping it
   with the new authority generation and changing its storage token.
3. Compare-and-swap the lease to `Active(g+1)`.

Step 1 is resumable by the same next writer and expected generation. A pending
permit cannot mutate payloads or publish commits. Failure before step 2 leaves
the old ref authoritative. Failure after step 2 leaves the new ref fence in
place; retrying step 3 activates the same permit. A stale writer with a cached
ref loses its CAS, while a stale writer that reloads the ref rejects its
different authority stamp.

## Authority stamps

`AuthorityStampV2` contains the full scope, generation, writer ID, and digest
of the opaque fencing token. The same stamp must appear in the physical
mutation metadata, multipart handle, commit, and branch ref for one
publication. Repository ID plus operation ID alone is not a v2 idempotency
identity; the authority scope is also required.

## Global maintenance

Repository-wide maintenance may not infer exclusivity from one branch permit.
Until the v2 maintenance gate, stable snapshot generations, and dirty-root
journal are implemented, GC and other destructive whole-repository operations
must fail closed for a sharded repository.

## Durable commit session

`Absent → Open(0) → Open(n) → Published`

`Open(n) → Aborted(n+1) → Expired/Cleaned`

- Session creation persists checkpoint zero with the stable batch ID,
  operation ID, base commit, authority stamp, and expiry.
- Staging uploads whole payloads to immutable content-addressed keys. A
  checkpoint contains only sorted logical mutations and verified bindings.
- Every checkpoint is a create-once object at a monotonically increasing
  sequence; an ambiguous create is reconciled by exact canonical bytes.
- Resume lists only the batch checkpoint prefix and selects the greatest
  canonical sequence. A process may adopt the session into a newer authority
  epoch only when its writer ID is unchanged and the branch still points to
  the original base commit.
- Publication revalidates authority and base, applies both Prolly trees in
  batch, and reconciles the operation ID before treating an ambiguous ref CAS
  as fencing evidence.
- A successfully published session needs no additional mutable completion
  record: replaying its final checkpoint resolves through the operation index.
- Bounded cleanup pages physical checkpoint versions and exact-deletes only
  records whose embedded expiry is in the past.

## Journal-index rebuild

`Discovering → Applying → Complete → Cleaned`

- The job opens one immutable branch-journal snapshot and records its ref
  generation and target.
- Each discovery step reads at most the requested event limit and stores one
  immutable content-addressed chunk. A chunk points to the previously written,
  newer chunk, so the final oldest chunk is also the start of chronological
  replay.
- The constant-size cursor is canonical and process-independent. It contains
  only the job/snapshot identity, one journal cursor or chunk ID, fresh Prolly
  roots, counters, and the index-head baseline.
- Each apply step consumes one chunk oldest-to-newest and updates fresh node
  and commit-graph roots. It never materializes the complete journal or commit
  set in memory.
- After node/graph application completes, the operation-index rebuild consumes
  the same oldest-to-newest chunk chain into bounded LSM segment levels and
  conditionally publishes its replacement head.
- Final head CAS is allowed only if the durable index still matches the
  baseline captured at job start. A moving branch does not invalidate the
  immutable snapshot; ordinary incremental catch-up consumes later events.
- Successful and abandoned jobs exact-delete chunk objects in bounded pages.
  Cleanup starts only after every consumer of the shared chunks is complete.
