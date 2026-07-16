# Dolt Go vs Rust Prolly Benchmark

This benchmark compares build, write, point-read, and full-range-scan performance
for the Dolt Go prolly tree and this Rust implementation. It uses native runners
in separate processes so neither runtime affects the other's measurements.

## Run

Run the complete requested matrix:

```sh
scripts/run_prolly_comparison.sh
```

Run a quick parity and smoke comparison:

```sh
BENCH_SIZES="10000" BENCH_RUNS=1 BENCH_LARGE_RUNS=1 \
  scripts/run_prolly_comparison.sh
```

Results are written under `performance-results/dolt-rust/`. Set `BENCH_OUT` to
retain multiple named runs. The runner stops on any failed process. The
summarizer also rejects mismatched workload digests, operation counts, or result
cardinalities between implementations.

## Method

- One worker: `RAYON_NUM_THREADS=1` and `GOMAXPROCS=1`.
- Fresh datasets: 10K, 50K, 1M, 5M, and 10M records.
- UTF-8 keys are fixed-width and lexicographically sortable.
- Values are deterministic pseudo-random payloads from 1 through 100 bytes.
- Each fresh build or mutation workload is submitted as one bulk write. This
  avoids retaining thousands of obsolete content-addressed snapshots in the
  in-memory stores at the 1M–10M tiers.
- Fresh append, random, and clustered workloads contain the same final records
  but differ in arrival order.
- Mutation workloads write 30% of the base size. Random and clustered mutations
  contain equal numbers of new inserts and updates; append mutations are all new
  keys above the current maximum.
- Point reads are validated and capped at 100,000 operations per scenario.
- Range scans traverse the complete resulting tree and validate strict key order
  and cardinality in an untimed pass. The timed pass includes logical tuple
  traversal/decoding and a cheap byte count in both implementations.
- Reports record the Rust source hash, Go runner hash, and SHA-256 digest of
  each copied executable. A dirty implementation is never represented by the
  last commit SHA alone.
- Logical fixture construction and Dolt tuple encoding happen outside timed write
  regions. Sorting, mutation buffering, chunking, hashing, encoding tree nodes,
  and in-memory node-store writes remain timed.

The comparison intentionally uses each implementation's default encoding and
chunking configuration. It measures the products presented to callers, not a
forced common wire format.

## Verified final zero-copy run (2026-07-16)

The complete final matrix reran every requested size three times with identical
binary hashes. See the
[detailed performance report](prolly-rust-go-performance-report-2026-07-16.md)
for the full 90-row matrix, repetition stability, peak RSS, provenance, and
limitations.

Across the 90 scenario-operation medians, Rust won 88 and Dolt Go won 2. Rust
won all point-read (30/30) and full-range-scan (30/30) groups. Point-read
speedups range from 1.68x to 5.75x with a 3.16x geometric mean; all 90 paired
point-read repetitions exceed 1.5x. Full-scan speedups range from 3.41x to
6.77x with a 4.98x geometric mean.

The remaining exceptions are explicit. Dolt Go wins random 30%-mutation writes
at 10K and 50K, and Rust's peak process memory is higher for random mutation at
1M, 5M, and 10M. Seven fresh large-tree point-read medians remain below the
universal 2x target, although all remain above 1.5x.
