# Physical S3 namespace

`P` is the configured repository prefix. Its v1 default is `.prolly/v1`.
It MUST satisfy the physical object-path rules: UTF-8, 1–1024 bytes, no leading
or trailing `/`, no empty segment, and no segment equal to `..`.

Hex components below are lowercase. `H` is the 64-character lowercase hex form
of a raw 32-byte digest; `H0` and `H1` are its first and second byte pairs.
`N` is lowercase hex of the UTF-8 branch/tag/pin name. `U` is the 32-character
lowercase UUID hex form. `G20` is a zero-padded 20-digit decimal generation.

| Object | Key |
|---|---|
| repository format | `P/format/v1.cbor` |
| initialization intent | `P/format/initialization.cbor` |
| writer lease | `P/writers/lease.cbor` |
| branch ref | `P/refs/heads/N` |
| tag ref | `P/refs/tags/N` |
| tag reflog entry | `P/reflogs/tags/N/H` |
| commit envelope | `P/commits/sha256/H0/H1/H` |
| direct Prolly node | `P/nodes/sha256/H0/H1/H` |
| node-index checkpoint | `P/node-index/checkpoints/G20-H.cbor` |
| current node-index pointer | `P/node-index/latest.cbor` |
| retention pin | `P/retention/pins/N` |
| GC plan | `P/gc/plans/pgc1_….cbor` |
| GC run | `P/gc/runs/pgc1_….cbor` |
| GC mark run | `P/gc/mark-runs/U.cbor` |
| provider attestation | `P/providers/ppf1_….cbor` |
| qualification probe | `P/probes/U/…` |

Branch, tag, and pin names MUST be 1–255 UTF-8 bytes and pass the canonical ref
validator: not `HEAD`; no control or space; no `~ ^ : ? * [ \\`; no leading or
trailing slash, `//`, `..`, `@{`, trailing `.`, component `.`/`..`, or component
ending `.lock`.

User payload objects live at their logical S3 key, outside `P`. A logical key
equal to `P` or beginning `P/` is reserved and MUST be rejected. The native S3
VersionId returned by the provider is stored in `NativeObjectBindingV1` and is
never synthesized from a logical ID.

## Creation and mutability

- Format, commit envelopes, node objects, reflogs, GC plans, and provider
  attestations are immutable and use create-if-absent semantics.
- Branch refs, tags, leases, retention pins, the current node-index pointer, GC
  runs, and mark runs are mutable only through storage-token compare-and-swap.
- An existing immutable key with identical SHA-256 is idempotent success; the
  same key with different bytes is corruption.
- Listing is discovery only. It never establishes branch authority.
