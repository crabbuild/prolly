# Prolly S3 qualification

Qualification has three layers: deterministic core tests, local RustFS
integration, and provider-specific load tests.

## Local checks

```bash
cargo fmt --manifest-path extensions/s3/Cargo.toml --all -- --check
cargo clippy --manifest-path extensions/s3/Cargo.toml \
  --workspace --all-targets --all-features -- -D warnings
cargo test --manifest-path extensions/s3/Cargo.toml --workspace
python3 extensions/s3/spec/prolly-s3/conformance/verify.py
extensions/s3/scripts/check_clean_downstream.sh
```

Core tests cover canonical encoding, immutable payloads, operation
reconciliation, authority renewal/takeover, branch-scoped publication,
structural merge, bounded indexes, ref catalogs, commit-DAG transfer, fsck,
repair, retention pins, and concurrent immutable GC.

## RustFS

```bash
docker compose -f extensions/s3/docker-compose.rustfs.yml up -d
PROLLY_S3_RUSTFS=1 \
  cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --test rustfs_repository -- --nocapture
```

The suite verifies:

- bucket versioning and provider attestation;
- whole-file immutable payload history;
- writable reopen and authority renewal;
- takeover fencing before payload upload;
- operation-ID reconciliation;
- durable commit-session resume;
- branch, tag, merge, and historical reads;
- independent branch publication.

The required pull-request workflow starts the pinned RustFS image and runs this
suite with `PROLLY_S3_RUSTFS=1`; provider tests must not silently self-skip.

Run all documented scenarios:

```bash
extensions/s3/scripts/run_rustfs_examples.sh
```

## Scale gates

Run ignored scale tests explicitly and record:

- 1K and 10K files per batch;
- 10K commits at configured concurrency;
- hot-branch and independent-branch workloads;
- cold, prewarmed, and persistent-cache reads;
- sparse diff and merge at 10K+ keys;
- restart during staging, merge, index rebuild, and publication response loss.
- provider-native multipart create/part/complete/abort behavior while retaining
  exactly one physical S3 object per logical large object;
- multipart crash/restart reconciliation, including proof that completed native
  parts are not uploaded again;
- cross-process publication fencing throughout a complete GC epoch;
- payload-pack utilization before and after bounded repack pages.

Results must include p50/p95/p99 latency, S3 calls by operation, bytes
transferred, CAS retries, cache hit ratio, index lag, wall time, provider
configuration, commit hash, and exact command.

Run the exact 10K concurrent-commit gate with:

```bash
PROLLY_S3_RUSTFS=1 PROLLY_S3_10K_CONCURRENCY=32 \
  cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --test rustfs_repository \
  rustfs_10k_concurrent_commit_regression_gate -- --ignored --nocapture
```

Run the 20K point-read, full-list, branch, sparse-diff, and merge gate with:

```bash
PROLLY_S3_RUSTFS=1 \
  cargo test --release --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --test rustfs_repository \
  rustfs_20k_branch_diff_merge_meets_amplification_slos \
  -- --ignored --exact --nocapture
```

The release gate requires warm read p99 below 100 ms, listing above 10K
entries/s, branch creation below 500 ms and 512 KiB downloaded, each 100-key
sparse diff below 500 ms and 1 MiB, and merge planning below one second and
2 MiB. It also verifies merge publication and merged object content.

## AWS qualification

Run the ignored AWS tests only against an isolated versioned bucket:

```bash
PROLLY_S3_AWS_QUALIFICATION=1 \
PROLLY_S3_AWS_BUCKET=your-isolated-bucket \
PROLLY_S3_AWS_REGION=us-west-2 \
  cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --test aws_qualification -- --nocapture
```

Also run `aws_performance_qualification` with workload-specific request bounds:

```bash
PROLLY_S3_AWS_PERF=1 \
PROLLY_S3_AWS_BUCKET=your-isolated-bucket \
PROLLY_S3_AWS_REGION=us-west-2 \
PROLLY_S3_AWS_PERF_WRITES_PER_TIER=1000 \
PROLLY_S3_AWS_PERF_MAX_P99_MS=500 \
PROLLY_S3_AWS_PERF_MIN_WRITES_PER_SECOND=10 \
PROLLY_S3_AWS_PERF_MAX_CALLS_PER_WRITE=10 \
  cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --test aws_performance_qualification \
  aws_hot_branch_performance_release_gate -- --ignored --exact --nocapture
```

Do not promote on RustFS results alone. AWS qualification must cover:

- regional latency and cross-AZ behavior;
- request cost and data transfer;
- throttling and retry policy;
- provider per-key version retention;
- expected object count and traffic;
- hot-branch CAS contention;
- cache-loss cold start;
- lifecycle and replication interactions.

The signed provider capability profile records whether replication
configuration was readable and whether replication was enabled. Replication is
reported rather than rejected because it does not mutate source objects, but
its destination lifecycle, replica ownership, KMS grants, latency, and request
cost must be included in the deployment runbook.

## Promotion criteria

Promote only when:

- all local and RustFS checks pass from a clean checkout;
- downstream core and client builds pass their declared MSRVs;
- a 10K regression is reproducible;
- write amplification stays within the configured workload budget;
- no test relies on bucket listing for normal node lookup;
- operation response loss reconciles before fencing;
- authority renewal survives the intended deployment duration;
- GC grace periods, durable cross-process admission fencing, crash/resume, and
  retention policies are tested in the deployment runbook. External writer
  quiescence is not a correctness requirement for clients using the repository
  protocol; bypass writers remain unsupported.

Million- or billion-object claims require measurements at representative
cardinality and traffic. Passing a 10K test is a regression gate, not proof of
unbounded production capacity.

The staged benchmark defaults to 10K, 20K, 50K, 100K, 500K, and 1M objects and
now records streaming ingest, point reads, full listing, branch creation,
sparse diff, and merge for every stage:

```bash
PROLLY_RUSTFS_PERF_STAGES=100000,500000,1000000 \
PROLLY_RUSTFS_PERF_WRITE_CONCURRENCY=32 \
  cargo run --release --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --example rustfs_small_files_benchmark
```
