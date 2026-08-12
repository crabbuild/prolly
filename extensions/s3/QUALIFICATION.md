# Prolly S3 qualification

Status: local protocol qualification passes; production AWS qualification is
incomplete.

## Enforced locally

Core tests verify:

- exact physical `VersionId` binding and historical reads;
- four-call warm whole-object writes, including stale-writer authority validation;
- four calls per write with 1, 8, and 32 concurrent callers;
- five-call two-object atomic publication and multi-delete;
- three-call merge and restore;
- `N + 6` provider-native multipart publication;
- bounded parallel payload preparation and concurrent OperationId singleflight;
- bounded commit, branch, and node-pack caches;
- persistent verified-node cache reopen and durable corruption invalidation;
- two-GET node-index checkpoint open without a metadata listing;
- bounded legacy node-location fallback after in-memory eviction;
- corrupt v2 node/ref/graph indexes failing open and rebuilding from authority;
- idempotent replay and lost put/copy/delete response reconciliation;
- applied-then-conflicted CAS reconciliation by operation ID for puts and
  atomic batches;
- branch-scoped writer takeover fencing and separate-process independent branch writers;
- clone, fetch, push, repair, and provider-ID rebinding;
- exact-version GC and corrupt-checkpoint recovery.
- concurrent partitioned GC with ordered dirty-root catch-up, restart recovery,
  bounded sweep fencing, and journal cleanup;
- branch-local node and commit-graph indexing from bounded publication-journal
  tails with zero commit/ref namespace scans and fail-closed late startup;
- root/internal-only cache prewarm with no object enumeration, plus verified
  cache reuse across a fresh repository process;
- constant-size, restartable, parent-first commit-closure traversal with
  bounded work/output, paged pins/reflogs, and bounded exact cleanup;
- raw cross-bucket restore rejection plus portable archive clone/rebind,
  read-only `fsck`, explicit writer takeover, post-restore write, and cleanup.
- native-v2 merge with a durable generation-priority merge-base frontier,
  structural three-way diff, paged changes/conflicts, restart after every
  two-record page, cursor-tamper rejection, source deletion, target-ref
  movement, CAS reconciliation, and bounded plan cleanup;

RustFS integration tests verify a 64 KiB whole-object write at four S3 calls,
one-call warm current and historical reads, historical content after overwrite,
a two-part multipart write at eight calls, and 32 concurrent writes at exactly 128
calls. Four local runs on 2026-08-11 completed the 32-write tier in 862–1,211 ms
(p99 778–1,113 ms, 26.40–37.09 writes/s). This is a reproducible local range,
not an AWS SLO.

Run them with:

```bash
cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-core --test prolly_s3_profile

PROLLY_S3_RUSTFS=1 \
  cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --test rustfs_repository -- --nocapture
```

Run the native-v2 merge scale gates separately. The first builds a 10K-object
snapshot, changes one key per branch, and requires structural pruning to keep
logical merge work below 32 records. The second builds 4,096 first-parent
commits and requires skip-pointer ancestry detection without entering the
general frontier.

```bash
cargo test --release --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-core --test merge_v2 \
  native_v2_sparse_merge_prunes_unchanged_10k_snapshot \
  -- --ignored --exact --nocapture

cargo test --release --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-core --test merge_v2 \
  native_v2_merge_base_skips_deep_first_parent_history \
  -- --ignored --exact --nocapture
```

On 2026-08-12, both release gates passed locally. The 10K sparse test body
completed in 1.60 s after compilation. The 4,096-commit history gate completed
in 200.88 s; almost all of that time constructs the sequential history, while
the final assertion requires zero general-frontier visits.

The RustFS merge regression drops and reopens `ClientV2` between every bounded
page, round-trips the canonical cursor, publishes through the real conditional
S3 adapter, and verifies both selected object bodies:

```bash
PROLLY_S3_RUSTFS=1 \
  cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --test rustfs_repository \
  rustfs_native_v2_merge_resumes_and_publishes_structural_plan \
  -- --exact --nocapture
```

On 2026-08-12, the RustFS merge regression completed in 2.30 s. These timings
are development-machine regression evidence, not AWS latency SLOs.

Run the reproducible 10K concurrent-commit regression gate separately. It is
ignored by default because it is intentionally sustained and writes a unique
development repository prefix:

```bash
PROLLY_S3_RUSTFS=1 \
PROLLY_S3_RUSTFS_10K=1 \
PROLLY_RUSTFS_10K_CONCURRENCY=32 \
PROLLY_RUSTFS_10K_OBJECT_BYTES=65536 \
cargo test --release --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --test rustfs_repository \
  rustfs_10k_concurrent_commits_are_reconciled_and_complete \
  -- --ignored --exact --nocapture
```

The gate runs cumulative 1K, 5K, and 10K tiers against one hot branch. It
reports throughput and p50/p95/p99 latency, bounds SDK calls per write, checks
the final ref generation, pages through exactly 10K logical objects, and
requires an empty publication queue.

On 2026-08-11, the pinned local RustFS gate passed all 10K commits in 469.81 s.
The final 5K tier sustained 19.63 writes/s with p50/p95/p99 of
1,462/2,760/3,285 ms and 3.005 SDK calls/write. The additional 24 calls were
amortized branch-ref version compaction. RustFS beta.10 hardcodes a 10,000
physical-version limit per object; automatic compaction at generation 5,000
retained 100 ref versions while preserving the complete logical commit DAG.

This gate uses 10K distinct user keys and does not qualify protocol v1 for 10K
revisions of one user key. Frozen v1 history binds the original key and exact
provider VersionId, so a provider with a finite or unknown per-key limit is not
qualified for unbounded hot-key history. Protocol v2 qualification instead
requires the immutable-payload profile: repeated logical revisions must spread
across one-version content-addressed keys, every mutable control family must
stay within its configured bound, and the provider must attest sufficient
per-key headroom.

Run the batched-ingest and persisted-cache gate with the Foyer feature:

```bash
PROLLY_S3_RUSTFS=1 \
PROLLY_S3_RUSTFS_BATCH_10K=1 \
cargo test --release --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --features foyer-cache --test rustfs_repository \
  rustfs_10k_batched_ingest_has_bounded_bytes_and_persisted_cache \
  -- --ignored --exact --nocapture
```

On 2026-08-11, the pre-sharded-authority build ingested 100 commits containing
10K × 64 KiB files in 78.89 s (126.76 files/s) at 1.020 calls/file. The current
gate expects 1.030 calls/file after adding one authority GET per commit and
must be rerun before quoting new latency. Upload amplification was 1.083×, down
from the pre-fix 2.70×; all node packs totaled 49.73 MiB and the largest was
965.65 KiB. After a graceful Foyer close/reopen, listing all 10K files took
285 ms, one commit GET, and zero node-range GETs, down from 421+ S3 calls in the
uncached baseline.

## Not yet qualified

The following are release blockers for a production claim:

| Gate | Required evidence |
|---|---|
| AWS behavior | General-purpose versioned buckets in every target region |
| Request cost | Measured traffic mix using current AWS request prices |
| Latency | p50/p95/p99 for writes, reads, batches, multipart, merge, restore |
| Throttling | Sustained and burst load including transport retry attempts |
| Hot branch | Queue latency, timeout policy, and authority renewal under peak load |
| Scale | 1M live keys and 10M retained versions with reopen, list, diff, fsck, GC |
| Transfer scale | Interrupted clone/push/repair at 10M commits with bounded RSS, zero commit-namespace LISTs, and stale-mapping rebuild |
| Administrative scale | Restart deep fsck in every phase at 1M keys/10M versions; assert bounded RSS, cursor size, and provider requests per page |
| Failure matrix | Process/network loss before and after every physical step |
| Operations | Backup/restore, key rotation, takeover, GC, and lifecycle audits |
| Resource bounds | Exercise the configured atomic-session bound and multipart restart contract |

RustFS results must not be presented as AWS latency, durability, availability,
cost, or million-object evidence.

## Fail-closed AWS performance gate

The ignored-by-default `aws_performance_qualification` test runs 64 KiB writes
against one hot branch at concurrency 1, 8, and 32. It verifies four SDK calls
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
