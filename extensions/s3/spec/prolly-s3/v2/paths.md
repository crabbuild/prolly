# Protocol v2 authority namespace

Protocol v2 adds explicit authority scopes without changing or reinterpreting
any protocol v1 record. `P` is the configured repository prefix.

| Object | Key |
|---|---|
| branch authority lease | `P/authority/v2/branches/N/lease.cbor` |
| system authority lease | `P/authority/v2/system/N/lease.cbor` |
| maintenance gate | `P/authority/v2/maintenance/gate.cbor` |
| branch ref | `P/refs/v2/heads/N` |
| authority-stamped commit object | `P/commits/v2/sha256/H0/H1/H` |
| publication event | `P/publications/v2/sha256/H0/H1/H` |
| immutable payload | `P/payloads/v2/R/sha256/H0/H1/H` |
| commit-session checkpoint | `P/staging/v2/R/B/checkpoints/Q.cbor` |
| operation-index head | `P/operation-index/v2/heads/N.cbor` |
| operation-index segment | `P/operation-index/v2/segments/N/sha256/H0/H1/H` |
| journal-derived index head | `P/journal-index/v2/heads/N.cbor` |
| journal-derived node tree | `P/journal-index/v2/node-tree/nodes/sha256/H0/H1/H` |
| journal-derived graph tree | `P/journal-index/v2/graph-tree/nodes/sha256/H0/H1/H` |
| journal-index rebuild chunk | `P/administration/v2/index-rebuild/N/E/chunks/sha256/H0/H1/H` |
| resumable commit-closure state | `P/administration/v2/closure/E/tree/nodes/sha256/H0/H1/H` |
| physical transfer mapping | `P/administration/v2/transfer-mappings/sha256/H0/H1/H` |
| GC coordinator | `P/gc/v2/coordinator.cbor` |
| GC dirty root | `P/gc/v2/dirty-roots/E/S/H` |

`N` is lowercase hex of the UTF-8 branch or system-namespace name. The
canonical lease embeds its repository ID and full authority scope, so moving a
lease object to another path or repository is corruption.

`H` is lowercase hex of a protocol-v2 commit ID. `H0` and `H1` are its first
and second byte pairs. V2 commits and refs use distinct content-ID domains and
wire records (`CommitIdV2`, `BucketCommitV2`, `RefValueV2`, and
`ReflogEntryV2`); no v1 reader can mistake them for v1 state.

Every branch publication requires exact equality between the active permit's
authority stamp, the commit authority stamp, and the resulting ref authority
stamp. The ref CAS is reconciled by exact canonical bytes after a lost or
ambiguous response. A different value at the path is a real ref conflict.

Before the ref CAS, the writer stores a content-addressed
`PublicationEventV2`. The ref points at that event, and the event points at the
previous event for the same branch. Consumers open the mutable ref once and
then page newest-to-oldest through an immutable snapshot without listing a
namespace. A losing CAS may leave an unreachable event, but it cannot enter the
journal and is safe for GC to reclaim.

`R` is lowercase hex of the repository ID. A live `ObjectVersionV2` carries
the full derived payload path and optional provider VersionId; a delete marker
carries no physical binding. Repeated writes to one logical user key therefore
create different immutable payload keys instead of accumulating provider
versions at that user key. Identical content reuses the same immutable object.
An original-key object, when materialized for external compatibility, is a
rebuildable projection and is never part of repository closure.

`B` is a protocol-v2 batch ID and `Q` is a fixed-width checkpoint sequence.
Commit-session checkpoints are immutable, canonical snapshots containing the
batch operation ID, base commit, authority stamp, expiry, and sorted staged
mutations. Put mutations retain only verified immutable payload bindings, not
object bodies. Resume lists only the selected batch prefix and adopts the
checkpoint only when the writer ID and base branch head still match. Expired
checkpoint versions are removed through bounded, cursor-based exact deletion;
there is no hot mutable staging head.

The operation-ID index is advisory and branch-local. Its bounded mutable head
checkpoints one publication event and references immutable, sorted LSM-style
segments. A configured generation and age window defines the idempotency
contract; an entry outside either boundary is not replayable. Lookup checks the
bounded journal tail after the checkpoint and then the compact segment levels.
Index heads must be initialized with branch creation. If lag exceeds the
configured tail bound, normal advancement fails closed and requires the
resumable rebuild path instead of scanning unbounded history in one call.

`JournalDerivedIndexesV2` consumes the same stable branch journal to maintain
the node locator and commit graph. One mutable branch-local checkpoint CAS
publishes both immutable Prolly roots, so readers never observe the indexes at
different journal positions. Events are applied oldest-to-newest, allowing
binary-lifting ancestry links to reuse parents indexed in the same batch.
Steady-state advancement performs no commit- or ref-namespace listing and is
proportional only to the unindexed journal tail. The head also records the
derived current target, providing exact per-branch ref freshness; global branch
enumeration remains a separate resumable administrative concern.

Foreground object reads compare the selected ref target with the durable index
target and never replay publication events. A locally published target is
immediately readable from its registered commit pack. A cold process reports
`MissingClosure` with retry advice until branch-local background maintenance
advances the durable node and graph roots. Index lag and the last maintenance
error are exposed as branch health rather than hidden inside read latency.

Whole-history administration first pages refs, tags, and pins, incrementally
attaches their targets to a `CommitClosureCursor`, and advances under explicit
step and output budgets. The constant-size cursor names immutable Prolly state
holding the traversal stack and visited set. Commits are emitted
parent-before-child so clone and repair can checkpoint mappings without
buffering the DAG. Finished or abandoned jobs exact-delete `E` in bounded
cleanup calls; the state is not a GC candidate while its externally persisted
cursor may still be live.

Physical clone, fetch, push, and repair consume that traversal in pages of at
most 256 commits. Each completed source commit records an immutable
source-to-destination mapping. A later transfer resolves the mapping with one
point GET and validates that the destination commit still exists; it never
lists or fingerprints the destination commit namespace. If GC has removed the
mapped commit, the stale mapping is exact-deleted and safely rebuilt. Mapping
nodes inside the active traversal are committed only after all destination
side effects for that page succeed, so retrying an interrupted page is
idempotent.

`E` is the hex GC epoch operation ID and `S` is a fixed-width, monotonically
increasing dirty-root sequence. While an epoch is active, every branch, tag,
or retention-pin CAS first persists a `GcDirtyRootV2` event containing both
the pre-publication and post-publication roots (when present). GC records a
stable sequence watermark under a brief publication barrier, releases the
barrier, and marks those ordered events concurrently with writers. Sweep takes
the barrier only for one bounded deletion batch. Completed epochs deactivate
the coordinator before deleting their journal in restartable batches.

Provider qualification for v2 must attest either an unlimited per-key version
count or a finite count at least two greater than the configured mutable-control
retention bound. An unknown limit is not qualified for production writes.

Protocol v1's `P/writers/lease.cbor` remains repository-exclusive and retains
its original scalar-generation meaning. A v1 writer must never read or write
v2 authority records. A repository advertises v2 before v2 refs, commits,
multipart handles, physical metadata, or GC checkpoints may carry an
`AuthorityStampV2`.
