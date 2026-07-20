# PostgreSQL Prolly 1M Baseline

This directory is the validated baseline for revision
`d6f33dc7351ba93f6117913a11cfb85e7d751696` plus the benchmark-harness changes
captured in `harness-source-diff.patch`. PostgreSQL 16.14 ran in Docker Desktop
on an Apple M2 Max with 12 logical CPUs and 32 GiB RAM.

## Baseline contract

- 1,000,000 fixed-width base records.
- 300,000 total mutations (30% of the base).
- Append inserts 300,000 suffix keys; random and clustered update existing keys.
- Merge uses two disjoint 150,000-key branches. Random branch membership is
  interleaved so both branches span the full keyspace.
- Point get, multi-get, and bounded scan use 10,000 sampled keys/entries.
- Three rotated repetitions, except one-shot build and full scan.
- Single client, serial cells, default Prolly configuration, and natural
  PostgreSQL/OS cache state.

The strict run contains 71 validated raw rows and 25 aggregated workload
groups. `raw-results.csv` is the source of truth; `summary.csv` and `report.md`
are derived outputs.

## Headline medians

| Operation | Append | Clustered | Random |
|---|---:|---:|---:|
| Single put | 6.81 ms | 9.63 ms | 9.13 ms |
| 300k batch | 1.122 s / 267,298 keys/s | 2.838 s / 105,692 keys/s | 9.085 s / 33,020 keys/s |
| 300k diff | 1.616 s | 0.955 s | 3.717 s |
| 300k merge | 1.463 s | 13.8 ms | 48.250 s |
| 10k cold point gets | 29.235 s / 342 gets/s | 30.064 s / 333 gets/s | 28.362 s / 353 gets/s |
| 10k warm point gets | 4.37 ms / 2.29M gets/s | 4.35 ms / 2.30M gets/s | 12.72 ms / 786k gets/s |
| 10k multi-get | 62.8 ms | 55.5 ms | 3.506 s |
| 10k bounded scan | 58.7 ms | 53.1 ms | Not applicable |

The one-shot 1M build took 3.425 s (291,941 records/s); the one-shot full scan
took 5.168 s (193,516 records/s).

## Primary optimization signals

1. Random access amplifies tree and SQL work. Random batch uses 15,318
   PostgreSQL statements and touches 7,659 read plus 7,659 written nodes,
   versus 2,299 statements and 4/2,295 nodes for append.
2. Random multi-get is 56–63 times slower than localized multi-get and reads
   4,498 nodes instead of 74–86. Coalescing node fetches and improving
   traversal locality are high-value profiling targets.
3. Cold point get performs exactly four PostgreSQL node reads per key (40,000
   calls for 10,000 gets) and is roughly 65,000 times slower than warm-cache
   append/clustered get. Round-trip and decoded-node cache behavior dominate.
4. Interleaved random merge is the largest write-side hotspot: 48.25 s median,
   30,112 PostgreSQL statements, 22,584 nodes read, and 7,528 written. The
   clustered merge demonstrates the benefit of structural sharing for
   range-separated branches.
5. PostgreSQL statement execution is only about 2–12% of end-to-end wall time
   in the representative cells. This does not mean PostgreSQL is free: network
   round trips, SQLx/runtime work, encoding, hashing, traversal, and validation
   outside the database executor are excluded from `pg_stat_statements` time.

Random merge had the widest measured spread (47.75–65.84 s), while its median
remained close to the minimum. Use at least three repetitions and compare
medians for future optimization work; use five or more when a change is near
the observed variance band.

## Files

- `report.md`: primary human-readable aggregate table.
- `summary.csv`: machine-readable medians and min/max values.
- `raw-results.csv`: all per-cell counters and timings.
- `run-manifest.txt`: frozen workload and seed.
- `machine.txt`, `postgres.txt`, `preflight.txt`: environment provenance.
- `dependencies.txt`, `binary.sha256`, `harness-source-diff.patch`: executable
  provenance.
- `build.log`, `run.log`: build and execution logs.
- `smoke/`: 1,000-record full-operation validation run.
- `preliminary-range-split-random-merge/`: retained audit run with the rejected
  range-separated random-merge definition. Do not use it as the baseline.

To reconstruct the measured dirty-tree harness on the recorded revision, run
`git apply --unidiff-zero performance-results/postgres/baseline/harness-source-diff.patch`.

## Reproduce the 1M baseline

Run from the repository root with an idle host:

```bash
BENCH_SIZES=1000000 \
BENCH_RUNS=3 \
BENCH_CHANGES=300000 \
BENCH_READ_SAMPLES=10000 \
BENCH_MIN_FREE_GB=3 \
scripts/run_postgres_scale_benchmark.sh \
  --profile full \
  --output performance-results/postgres/baseline-rerun
```

## Expand to 5M and 10M

The following preserve the 30% mutation and 10,000-read-sample contract:

```bash
BENCH_SIZES=5000000 BENCH_RUNS=3 BENCH_CHANGES=1500000 \
BENCH_READ_SAMPLES=10000 BENCH_MIN_FREE_GB=10 \
scripts/run_postgres_scale_benchmark.sh --profile full \
  --output performance-results/postgres/5m-baseline

BENCH_SIZES=10000000 BENCH_RUNS=3 BENCH_CHANGES=3000000 \
BENCH_READ_SAMPLES=10000 BENCH_MIN_FREE_GB=20 \
scripts/run_postgres_scale_benchmark.sh --profile full \
  --output performance-results/postgres/10m-baseline
```

For cross-scale comparisons, keep PostgreSQL/Docker settings, key/value widths,
seed, read sample count, repetition count, and operation ordering unchanged.
