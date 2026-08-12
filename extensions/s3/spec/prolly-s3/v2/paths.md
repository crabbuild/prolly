# Protocol v2 authority namespace

Protocol v2 adds explicit authority scopes without changing or reinterpreting
any protocol v1 record. `P` is the configured repository prefix.

| Object | Key |
|---|---|
| branch authority lease | `P/authority/v2/branches/N/lease.cbor` |
| system authority lease | `P/authority/v2/system/N/lease.cbor` |
| maintenance gate | `P/authority/v2/maintenance/gate.cbor` |

`N` is lowercase hex of the UTF-8 branch or system-namespace name. The
canonical lease embeds its repository ID and full authority scope, so moving a
lease object to another path or repository is corruption.

Protocol v1's `P/writers/lease.cbor` remains repository-exclusive and retains
its original scalar-generation meaning. A v1 writer must never read or write
v2 authority records. A repository advertises v2 before v2 refs, commits,
multipart handles, physical metadata, or GC checkpoints may carry an
`AuthorityStampV2`.
