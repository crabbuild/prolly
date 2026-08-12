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

## Ref lifecycle and sharded catalog

Authoritative branch states are:

`Absent -> Live(g) -> Live(g+1) -> Tombstone(g+1) -> Live(g+2)`

Tags follow the same generation progression. Branch moves first publish their
immutable `PublicationEventV2`; tag transitions retain an inline reflog. Every
transition then emits a common immutable `RefCatalogEventV2` and attempts one
CAS against only the selected catalog shard.

- A catalog event links to the previous event in its shard, not to an event in
  another shard.
- Concurrent writers to different shards never contend on a mutable catalog
  object. Same-shard CAS losers reload the head, retain the winner's update,
  apply their event to the new tree, and retry.
- An update with a lower generation is ignored. Equal generation with
  different state is corruption/conflict, while an exact repeat is idempotent.
- A catalog failure cannot authorize or roll back a ref transition. Ordinary
  branch index maintenance retries it from the authoritative branch head.
- Branch and tag creation/deletion wait for their catalog update so lifecycle
  APIs provide read-after-write enumeration. The bounded repair scanner closes
  gaps after crashes and is the only path that lists the ref namespace.
- Tombstoned entries remain in the derived tree but are omitted from listings.
  Native GC may reclaim unreachable event/tree objects only after accounting
  for every live shard head and in-progress repair/administration root.

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

## Explicit v1-to-v2 branch migration

`Pinned -> Copying -> Publishing -> Complete -> Cleaned`

- Start snapshots one authoritative v1 branch head, creates a non-expiring v1
  retention pin, and opens a constant-size parent-before-child closure cursor.
- Copying transforms at most the caller's commit budget. V1 live versions are
  streamed through a verified disk spool into native immutable payload keys;
  delete markers remain logical-only. A stable derivation maps each v1 object
  version to one v2 identity. Versions already present through another merge
  parent reuse their immutable binding without another payload transfer.
- Imported commits preserve parent topology, generation, author, message,
  timestamp, and metadata. They carry the target `v1-migration` system
  authority stamp and remain unreachable until publication.
- Every completed commit adds a source-to-destination mapping to the source
  closure tree and adds its node pack plus binary-lifting ancestry entry to
  target-side imported index roots. Both roots are in the returned cursor.
- On restart, those imported roots serve ancestor-packed node lookups before
  any user ref exists. A page is therefore resumable without rebuilding whole
  historical snapshots or depending on process-local node locations.
- Publishing creates the destination branch at the mapped source head and
  atomically replaces its bootstrap index with the complete imported closure.
  Only then is the source retention pin tombstoned and completion returned.
- Replaying a completed cursor verifies the destination ref and exact durable
  index roots. Cleanup exact-deletes source traversal nodes in bounded pages.
- Abort unregisters the transient target index root, tombstones the source pin,
  and exact-deletes traversal nodes in bounded pages. It never publishes a
  partial branch; unreachable immutable imports are left for native-v2 GC.
- V1 and v2 prefixes are physically separate. The protocol never dual-writes
  mutations or changes an existing repository's format marker.

## Resumable structural merge

`DiscoveringBases -> CollectingBases -> Planning -> BuildingVersions -> BuildingObjects -> ReadyToPublish`

Optional stops are `CollectingBases -> AwaitingBase -> Planning` for several
best bases and `Planning -> Conflicted` for the fail-on-conflict policy.

- Start snapshots the target and source commit IDs and first advances both
  branch-local node/graph indexes. A first-parent ancestor is found through
  binary-lifting pointers; other histories seed a generation-priority,
  bidirectional paint-down frontier in the job plan.
- A common commit becomes a candidate. Painting its ancestors stale removes
  non-best common ancestors. Several surviving candidates are all exposed and
  require an explicit caller selection.
- Planning structurally diffs base-to-target and base-to-source. Equal CID
  subtrees are pruned. The cursor retains at most one pending record from each
  stream, and one advance batches its selected changes/conflicts into one new
  durable plan root.
- Version construction structurally unions the immutable parent version trees.
  Unequal values at the same version-tree key are corruption. Object
  construction applies only selected changes and creates deterministic logical
  delete markers for source-selected deletions.
- A large merge delta is an immutable Prolly root and exact count. The final
  commit has parents `[observed-target, observed-source]` and generation one
  greater than the newest parent.
- Publication locks only the target branch, reconciles the operation ID first,
  and then requires the target ref still to equal the observed target. A moved
  target is a conflict; the implementation never rebases the plan.
- Replaying publication with the same cursor returns the prior merge receipt.
  An ambiguous, unreconciled publication fences only the target branch.
- The caller exact-deletes merge-plan nodes in bounded pages after success or
  abandonment. Output nodes referenced by a commit are never job-cleanup
  candidates.
