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

## Verified zero-copy run (2026-07-16)

The final runs used identical binary hashes across every requested size:

- 10K, 50K, and 1M (three repetitions):
  `performance-results/zero-copy-10k-50k-1m-final-provenance/`
- 5M and 10M (one complete repetition; exact ratios are provisional):
  `performance-results/zero-copy-5m-10m-final-provenance/`

Across all 90 scenarios Rust won 82 and Dolt Go won 8. Rust won every range
scan (30/30), with measured scan speedups from 2.97x through 6.50x. Point reads
split 23/30 for Rust and 7/30 for Dolt:

- 10K: Rust won all point workloads, 1.42x–2.18x;
- 50K: Rust won all point workloads, 1.07x–1.89x;
- 1M: locality-heavy mutation append/clustered reached 1.59x–1.95x, while
  fresh/random cases were near parity or slower;
- 5M and 10M: Rust reached 1.58x–2.15x for mutation append/clustered and
  1.11x–1.16x for mutation random, while Dolt won every fresh point workload
  by 1.16x–1.28x.

These results do not establish the requested 1.5x–2x point-read target for a
large random working set. They show that callback-scoped result borrowing and
session-local routing solve allocation/locality overhead, while decoded
`Vec<Vec<u8>>` nodes and their cache footprint dominate at 1M–10M. The next
point-read optimization must therefore evaluate the packed `ReadNode` phase in
the zero-copy architecture design rather than further enlarging session caches.
