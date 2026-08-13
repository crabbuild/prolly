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
structural merge, bounded indexes, and ref catalogs.

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

The runnable example provides a small end-to-end smoke test:

```bash
cargo run --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --example rustfs_versioned_bucket
```

## Scale gates

Run ignored scale tests explicitly and record:

- 1K and 10K files per batch;
- 10K commits at configured concurrency;
- hot-branch and independent-branch workloads;
- cold, prewarmed, and persistent-cache reads;
- sparse diff and merge at 10K+ keys;
- restart during staging, merge, index rebuild, and publication response loss.

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

## AWS qualification

Run the ignored AWS tests only against an isolated versioned bucket:

```bash
PROLLY_S3_AWS_QUALIFICATION=1 \
PROLLY_S3_AWS_BUCKET=your-isolated-bucket \
PROLLY_S3_AWS_REGION=us-west-2 \
  cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --test aws_qualification -- --ignored --nocapture
```

Also run `aws_performance_qualification` with workload-specific request bounds.
Do not promote on RustFS results alone. AWS qualification must cover:

- regional latency and cross-AZ behavior;
- request cost and data transfer;
- throttling and retry policy;
- provider per-key version retention;
- expected object count and traffic;
- hot-branch CAS contention;
- cache-loss cold start;
- lifecycle and replication interactions.

## Promotion criteria

Promote only when:

- all local and RustFS checks pass from a clean checkout;
- downstream core and client builds pass their declared MSRVs;
- a 10K regression is reproducible;
- write amplification stays within the configured workload budget;
- no test relies on bucket listing for normal node lookup;
- operation response loss reconciles before fencing;
- authority renewal survives the intended deployment duration;
- gaps such as absent GC are accepted in the capacity plan.

Million- or billion-object claims require measurements at representative
cardinality and traffic. Passing a 10K test is a regression gate, not proof of
unbounded production capacity.
