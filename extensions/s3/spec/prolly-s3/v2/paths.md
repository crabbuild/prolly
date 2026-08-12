# Protocol v2 authority namespace

Protocol v2 adds explicit authority scopes without changing or reinterpreting
any protocol v1 record. `P` is the configured repository prefix.

| Object | Key |
|---|---|
| repository format marker | `P/format/v2.cbor` |
| branch authority lease | `P/authority/v2/branches/N/lease.cbor` |
| system authority lease | `P/authority/v2/system/N/lease.cbor` |
| maintenance gate | `P/authority/v2/maintenance/gate.cbor` |
| branch ref | `P/refs/v2/heads/N` |
| tag ref | `P/refs/v2/tags/N` |
| authority-stamped commit object | `P/commits/v2/sha256/H0/H1/H` |
| publication event | `P/publications/v2/sha256/H0/H1/H` |
| ref-catalog lifecycle event | `P/ref-events/v2/sha256/H0/H1/H` |
| ref-catalog shard head | `P/ref-catalog/v2/shards/SS/head.cbor` |
| ref-catalog node tree | `P/ref-catalog/v2/tree/nodes/sha256/H0/H1/H` |
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
| resumable merge plan node | `P/administration/v2/merge/E/plan/nodes/sha256/H0/H1/H` |
| durable administrative output node | `P/nodes/sha256/H0/H1/H` |
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

Tag refs use the same canonical name encoding and mutable-control retention as
branch refs. They are fenced by the `tags` system-authority scope. A tag
create, recreation, or delete validates that authority before its exact CAS;
deletion publishes a tombstone rather than removing the mutable key.

Ref enumeration is derived from 16 independently published catalog shards.
Each branch or tag transition stores one immutable lifecycle event and updates
the shard selected by hashing its kind and name. The event links to the prior
event for that shard, while the mutable shard head points to a Prolly root
containing the latest generation for every ref. Tombstones remain in that tree
so delayed events cannot resurrect deleted refs. Readers page shard-by-shard
using a canonical cursor and issue point GETs only; they never list the ref
namespace. The catalog is advisory: callers resolve an entry through its
authoritative ref before mutation. A crash between ref publication and catalog
publication is repaired by the explicit bounded ref scanner; namespace scans
are not part of normal reads or writes.

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
The operation-index rebuild reuses the journal-index rebuild's immutable
linked chunks after node/graph application completes. Chunk cleanup therefore
waits for both replacement heads to publish.

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

An explicit v1-to-v2 migration reuses this closure namespace. Keys under the
closure root retain source-to-native commit mappings alongside traversal work.
Target imported node and commit-graph roots use the normal journal-index node
namespaces, but are not installed as a branch head until the full pinned source
closure is durable. Imported commits use the `v1-migration` system-authority
scope, giving shared source commits stable target identities across separately
named branch migrations performed in the same authority epoch.

Physical clone, fetch, push, and repair consume that traversal in pages of at
most 256 commits. Each completed source commit records an immutable
source-to-destination mapping. A later transfer resolves the mapping with one
point GET and validates that the destination commit still exists; it never
lists or fingerprints the destination commit namespace. If GC has removed the
mapped commit, the stale mapping is exact-deleted and safely rebuilt. Mapping
nodes inside the active traversal are committed only after all destination
side effects for that page succeed, so retrying an interrupted page is
idempotent.

A native-v2 merge cursor names one `E`-scoped plan root. The plan tree stores
the bidirectional graph frontier, seen flags, best-base candidates/results,
selected object changes, conflicts, and a sealed copy of the cursor state.
Cursor tokens never contain those unbounded collections. Each successful page
creates a new immutable plan root; successful and abandoned jobs exact-delete
only their merge-plan prefix through bounded cleanup.

Partially built object, version, and external-delta trees use deterministic
`P/nodes/sha256/...` point-addressed nodes. These nodes are outside the
job-cleanup prefix because a published commit may reference them. Packed-node
readers fall back to the exact CID path after the journal-derived locator
misses; they must not list the node namespace. A merge commit with a non-empty
external delta carries an empty inline change vector, the delta root manifest,
and its exact logical change count.

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
