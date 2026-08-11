# Required S3 substrate

V1 is defined over a bucket whose S3 Versioning state is `Enabled`.
`Suspended` and unversioned buckets are not conforming.

## Required operations

An object-plane adapter MUST provide these semantics, whether implemented with
AWS S3, a compatible service, or an emulator:

| Protocol primitive | S3 behavior |
|---|---|
| get | GET by key, optional inclusive byte range, optional exact VersionId |
| head | HEAD current key and return ETag, VersionId, length, timestamp |
| immutable put | PUT plus create-if-absent; verify existing content on conflict |
| mutable load | GET current key plus storage token |
| compare exchange | conditional PUT using the observed current ETag/token |
| list | paginated lexicographic listing; optionally include all versions and delete markers |
| exact delete | permanently delete the named physical VersionId |
| physical put | PUT user object; return provider VersionId, ETag, and checksums |
| physical delete | DELETE without VersionId; return the created delete-marker VersionId |
| physical copy | copy an exact source VersionId; return destination VersionId |
| multipart | create, upload/copy parts, list, complete, abort; completion returns VersionId |

The adapter MUST preserve opaque VersionId and ETag strings exactly. It MUST
not assume ETag is MD5. For every returned body it computes SHA-256 locally or
validates an equivalent trusted checksum and exposes the canonical digest.

## Qualification profile 1

Before creating a repository, the client MUST prove and attest:

- conditional create and conditional update work;
- GET-after-PUT and list-after-PUT/delete satisfy the profile's strong
  visibility requirement;
- ranged GET, pagination, version listing, and exact-version delete work;
- bucket versioning is `Enabled`;
- no conflicting lifecycle rule can delete repository closure;
- no default object-lock retention prevents required exact deletes;
- reported object and single-PUT limits are nonzero.

The signed `ProviderAttestationV1` binds endpoint and bucket fingerprints,
capabilities, probe-suite version 1, SDK version, validity interval, and signer.
Opening a writer requires an unexpired, signature-verified attestation whose ID
matches its canonical body. Readers MAY be configured to accept an expired
attestation for disaster recovery but MUST report degraded qualification.

## Conditional requests

HTTP status codes are adapter details; the portable result is semantic:

- a failed create-if-absent is followed by a read and digest comparison;
- a failed compare-and-swap returns `Conflict` with the current object if it is
  readable, otherwise `Conflict(null)`;
- timeouts after a possibly accepted mutation are `OutcomeUnknown`, never a
  blind `Timeout`; reconciliation is required before retrying;
- provider request IDs and physical codes are retained as diagnostic metadata but
  do not replace the stable error code.

## Multipart

Parts are numbered 1 through 10,000 and completion order is strictly
increasing. An implementation MUST validate the total size and whole-object
MD5/SHA-256 from its spool or verified parts. Multipart upload IDs are opaque.
A completed upload is not logically visible until its object version and commit
are published through the branch-ref state machine.
