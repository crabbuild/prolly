# Prolly S3 durable paths

`P` is the configured repository prefix. Path names are part of the sole
repository format.

| Record | Path template |
|---|---|
| repository format | `P/format/repository.cbor` |
| initialization state | `P/format/initialization.cbor` |
| authority lease | `P/authority/S/lease.cbor` |
| branch ref | `P/refs/heads/N` |
| tag ref | `P/refs/tags/N` |
| immutable payload | `P/payloads/R/sha256/H0/H1/H` |
| immutable commit | `P/commits/sha256/H0/H1/H` |
| publication event | `P/publications/sha256/H0/H1/H` |
| commit-session state | `P/staging/R/B/...` |
| operation-index head | `P/operation-index/heads/N.cbor` |
| operation-index segment | `P/operation-index/segments/N/sha256/H0/H1/H` |
| journal-index head | `P/journal-index/heads/N.cbor` |
| journal node tree | `P/journal-index/node-tree/...` |
| journal graph tree | `P/journal-index/graph-tree/...` |
| ref-catalog shard head | `P/ref-catalog/shards/SS/head.cbor` |
| ref-catalog tree | `P/ref-catalog/tree/...` |
| index-rebuild chunk | `P/administration/index-rebuild/N/E/chunks/sha256/...` |
| merge plan | `P/administration/merge/E/plan/...` |
| commit-closure work tree | `P/administration/closure/E/tree/nodes/sha256/...` |
| fsck cursor | `P/administration/fsck/E/cursor.cbor` |
| fsck distinct-payload work tree | `P/administration/fsck/E/payloads/nodes/sha256/...` |
| GC coordinator | `P/gc/coordinator.cbor` |
| GC epoch cursor | `P/gc/epochs/E/cursor.cbor` |
| GC reachability work tree | `P/administration/gc/E/tree/nodes/sha256/...` |
| publication admission ticket | `P/gc/publications/R/E/H` |

`R` is the repository identity. `N` is the canonical encoded ref name. `S` is
an encoded authority scope. `B` is a commit-session ID. `E` is an administration
job identity. `H` is lowercase SHA-256 hex; `H0` and `H1` are its first two
byte pairs. `SS` is a two-digit ref-catalog shard.

## Rules

- The repository prefix is reserved exclusively for Prolly S3.
- Immutable paths are create-once. Existing unequal bytes are corruption.
- Mutable records use exact physical-version compare-and-swap.
- Branch refs point to immutable publication events; the event points to the
  commit and previous event.
- Payload paths derive from repository identity and content hash. Identical
  bytes in one repository may reuse a complete payload object. A payload path
  never stores multiple logical bodies or a chunk of one logical body.
- Delete markers do not have payload paths.
- Ordinary reads and index maintenance must not discover nodes by namespace
  listing.
- Mutable control records retain a bounded number of provider versions.
- Every fsck page advances a CAS-protected checkpoint generation. Only the
  durable generation may advance. Completed jobs exact-delete payload and
  closure work trees before deleting older and then current checkpoint
  versions in bounded cleanup pages.
- Branch/tag publication tickets are immutable, instance-scoped, and removed
  by exact version after the ref CAS resolves. GC may remove expired tickets.
- There are no alternative versioned path families and no format migration
  namespace.
