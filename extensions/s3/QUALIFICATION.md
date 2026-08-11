# Native-versioned S3 qualification

Status: local protocol qualification passes; production AWS qualification is
incomplete.

## Enforced locally

Core tests verify:

- exact physical `VersionId` binding and historical reads;
- four-call warm whole-object writes;
- four calls per write with 1, 8, and 32 queued callers;
- five-call two-object atomic publication;
- three-call merge and restore;
- `N + 5` provider-native multipart publication;
- idempotent replay and lost put/copy/delete response reconciliation;
- exclusive writer takeover fencing;
- clone, fetch, push, repair, and provider-ID rebinding;
- exact-version GC and corrupt-checkpoint recovery.

RustFS integration tests verify a 64 KiB whole-object write at four S3 calls,
historical content after overwrite, and a two-part multipart write at seven
calls.

Run them with:

```bash
cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-core --test native_versioned_profile

PROLLY_S3_RUSTFS=1 \
  cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --test rustfs_repository -- --nocapture
```

## Not yet qualified

The following are release blockers for a production claim:

| Gate | Required evidence |
|---|---|
| AWS behavior | General-purpose versioned buckets in every target region |
| Request cost | Measured traffic mix using current AWS request prices |
| Latency | p50/p95/p99 for writes, reads, batches, multipart, merge, restore |
| Throttling | Sustained and burst load including transport retry attempts |
| Hot branch | Queue latency, timeout policy, and lease renewal under peak load |
| Scale | 1M live keys and 10M retained versions with reopen, list, diff, fsck, GC |
| Failure matrix | Process/network loss before and after every physical step |
| Operations | Backup/restore, key rotation, takeover, GC, and lifecycle audits |
| Resource bounds | Atomic-session memory and multipart-session recovery policy |

RustFS results must not be presented as AWS latency, durability, availability,
cost, or million-object evidence.
