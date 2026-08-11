# Prolly S3 qualification

Status: local protocol qualification passes; production AWS qualification is
incomplete.

## Enforced locally

Core tests verify:

- exact physical `VersionId` binding and historical reads;
- three-call warm whole-object writes;
- three calls per write with 1, 8, and 32 concurrent callers;
- four-call two-object atomic publication and multi-delete;
- two-call merge and restore;
- `N + 4` provider-native multipart publication;
- bounded parallel payload preparation and concurrent OperationId singleflight;
- bounded commit, branch, and node-pack caches;
- two-GET node-index checkpoint open without a metadata listing;
- idempotent replay and lost put/copy/delete response reconciliation;
- exclusive writer takeover fencing;
- clone, fetch, push, repair, and provider-ID rebinding;
- exact-version GC and corrupt-checkpoint recovery.
- raw cross-bucket restore rejection plus portable archive clone/rebind,
  read-only `fsck`, explicit writer takeover, post-restore write, and cleanup.

RustFS integration tests verify a 64 KiB whole-object write at three S3 calls,
one-call warm current and historical reads, historical content after overwrite,
a two-part multipart write at six calls, and 32 concurrent writes at exactly 96
calls. Two local runs on 2026-08-11 completed the 32-write tier in 862–878 ms
(p99 778–805 ms, 36.44–37.09 writes/s). This is a reproducible local baseline,
not an AWS SLO.

Run them with:

```bash
cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-core --test prolly_s3_profile

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
| Resource bounds | Exercise the configured atomic-session bound and multipart restart contract |

RustFS results must not be presented as AWS latency, durability, availability,
cost, or million-object evidence.

## Fail-closed AWS performance gate

The ignored-by-default `aws_performance_qualification` test runs 64 KiB writes
against one hot branch at concurrency 1, 8, and 32. It verifies three SDK calls
per completed write, reports p50/p95/p99, throughput, publication wait, and
queue depth, and fails thresholds supplied by the operator:

```bash
PROLLY_S3_AWS_PERF=1 \
PROLLY_AWS_REGION=us-west-2 \
PROLLY_AWS_BUCKET_VERSIONED=my-qualified-bucket \
PROLLY_AWS_PERF_WRITES_PER_TIER=256 \
PROLLY_AWS_PERF_MAX_P99_MS=2000 \
PROLLY_AWS_PERF_MIN_WRITES_PER_SECOND=10 \
cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --test aws_performance_qualification \
  -- --ignored --nocapture
```

Thresholds are deliberately required rather than baked into the library. Run
the gate in every target region with production-equivalent encryption,
networking, object size distribution, retry configuration, key cardinality,
and request rate. SDK call counts do not include Smithy HTTP retries; capture
wire-attempt metrics and CloudWatch throttling alongside this test.
