# Prolly S3 Protocol v1

Status: **normative and frozen**

This directory is the language-neutral contract for repositories created by
the Prolly S3 client. A conforming implementation can read and write
the same repository from Rust, Java, Go, TypeScript, or another language
without sharing implementation code.

The key words MUST, MUST NOT, REQUIRED, SHOULD, SHOULD NOT, and MAY are to be
interpreted as described by RFC 2119 and RFC 8174.

## Version registry

Every protocol owned by this profile starts at and defaults to `1`:

| Component | Version | Default |
|---|---:|---:|
| repository format | 1 | 1 |
| capability profile | 1 | 1 |
| reader protocol | 1 | 1 |
| writer protocol | 1 | 1 |
| canonical CBOR profile | 1 | 1 |
| hashing and identifiers | 1 | 1 |
| physical path layout | 1 | 1 |
| state machines | 1 | 1 |
| error taxonomy | 1 | 1 |
| conformance suite | 1 | 1 |
| semantic API model | 1 | 1 |

`ListObjectsV2` is an AWS API operation name and is not a version of this
protocol. The embedded Prolly node format has its own independently versioned
magic; a new S3 protocol version must not relabel existing Prolly bytes.
Likewise, `$version: "2"` in `api.smithy` selects the Smithy IDL 2 grammar; the
modeled service and every Prolly S3 default remain version 1.

## Normative documents

- [encoding.md](encoding.md) defines the deterministic CBOR wire profile.
- [schema.cddl](schema.cddl) defines persisted record shapes.
- [hashing.md](hashing.md) defines digests and printable identifiers.
- [paths.md](paths.md) defines the S3 object namespace.
- [s3-substrate.md](s3-substrate.md) defines required S3 behavior.
- [state-machines.md](state-machines.md) defines observable operations,
  publication, recovery, leases, multipart, and GC.
- [errors.md](errors.md) defines portable errors and retry behavior.
- [compatibility.md](compatibility.md) defines evolution rules.
- [api.smithy](api.smithy) is the language-binding-neutral semantic API.
- [protocol.json](protocol.json) is the machine-readable registry.
- [conformance](conformance/) contains executable vectors and release gates.

Where prose and CDDL disagree for byte representation, CDDL wins. Where a
record shape and an operation invariant disagree, the stricter requirement
wins. `protocol.json` is a registry, not an alternative schema.

Records are not self-describing. The physical path, Prolly tree, or API
operation selects the exact CDDL production using the dispatch tables in
`protocol.json`; implementations MUST NOT guess a record type from its map
shape.

## Authority and publication

The current branch ref is the only authority for branch visibility. An object
version or commit envelope may exist physically without being visible.
Visibility changes only when the branch ref compare-and-swap succeeds.

Implementations MUST preserve these invariants:

1. Immutable objects are verified before use and never overwritten with
   different bytes.
2. A commit is published only after all objects in its closure exist.
3. Ref generation increases by exactly one and the ref update is conditional
   on the previously observed storage token.
4. A logical live version resolves to exactly one provider VersionId and a
   delete marker resolves to exactly one provider delete-marker VersionId.
5. Retrying an operation ID with the same canonical input returns the original
   result; using it with different input fails with `IdempotencyConflict`.
6. Unknown record fields, enum variants, noncanonical CBOR, invalid IDs, and
   unsupported versions are rejected before state is changed.

## Conformance levels

- **Reader**: validates format, CBOR, identifiers, tree closure, exact physical
  VersionIds, and can read/list/history/diff a v1 repository.
- **Writer**: reader conformance plus qualification, fencing, idempotency,
  immutable publication, and conditional ref updates.
- **Full**: writer conformance plus multipart recovery, clone/sync, retention,
  fsck/repair, and mark/sweep GC.

An implementation MUST publish its level and pass every applicable vector and
scenario. It MUST NOT claim partial support for a level.
