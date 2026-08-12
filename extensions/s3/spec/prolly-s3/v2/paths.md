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

Provider qualification for v2 must attest either an unlimited per-key version
count or a finite count at least two greater than the configured mutable-control
retention bound. An unknown limit is not qualified for production writes.

Protocol v1's `P/writers/lease.cbor` remains repository-exclusive and retains
its original scalar-generation meaning. A v1 writer must never read or write
v2 authority records. A repository advertises v2 before v2 refs, commits,
multipart handles, physical metadata, or GC checkpoints may carry an
`AuthorityStampV2`.
