# Hashes and identifiers

All digests use SHA-256. Content IDs are computed with this exact framing:

```text
domain_hash(domain, parts...) = SHA256(
    u32be(byte_length(domain)) || domain ||
    for each part: u64be(byte_length(part)) || part
)
```

Lengths are unsigned big-endian integers. `domain` is the literal ASCII byte
sequence listed below. There is no terminator, salt, or implicit encoding.

| Value | Domain |
|---|---|
| RepositoryId | `prolly-s3/repository/v1` |
| TreeFormatDigest | `prolly-s3/tree-format/v1` |
| ProviderProfileId | `prolly-s3/provider-profile/v1` |
| ObjectVersionId | `prolly-s3/object-version/v1` |
| CommitId | `prolly-s3/commit/v1` |
| ReflogEntryId | `prolly-s3/reflog/v1` |
| NodePackId | `prolly-s3/node-pack/v1` |
| NodeIndexCheckpointId | `prolly-s3/node-index-checkpoint/v1` |
| GcPlanId | `prolly-s3/gc-plan/v1` |
| operation input digest | `prolly-s3/operation-input/v1` |

The ordered parts for composite IDs are:

- RepositoryId: operation UUID bytes.
- ObjectVersionId: repository digest, logical key bytes, operation UUID bytes,
  canonical `LogicalObjectVersionBodyV1` bytes. Provider binding is excluded.
- CommitId: canonical `BucketCommitV1` bytes. The physical commit envelope and
  provider-specific node location are excluded.
- All other one-body content IDs: canonical body or object bytes as their sole
  part.

Printable 32-byte IDs use lowercase, unpadded RFC 4648 base32 after the prefix:

| ID | Prefix |
|---|---|
| RepositoryId | `pr1_` |
| CommitId | `pbc1_` |
| ObjectVersionId | `pov1_` |
| ReflogEntryId | `prl1_` |
| TreeFormatDigest | `ptf1_` |
| ProviderProfileId | `ppf1_` |
| GcPlanId | `pgc1_` |
| NodePackId | `pnp1_` |
| NodeIndexCheckpointId | `nic1_` |

Operation and batch IDs are canonical lowercase UUID simple form (32 hex
digits) prefixed with `op1_` and `pb1_`. Parsers MUST reject uppercase,
padding, alternate UUID punctuation, wrong length, or a mismatched prefix.
