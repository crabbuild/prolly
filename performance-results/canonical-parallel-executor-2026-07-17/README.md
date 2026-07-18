# Canonical Parallel Executor Performance Evidence

This directory contains the retained release-mode measurements for the 2026-07-17 canonical parallel mutation executor. Exploratory measurements with time drift, fixed A/B ordering, obsolete telemetry, or superseded preflight logic were discarded.

## Files

- `ab-results.csv`: 20 order-balanced old/new process pairs for value-only and mixed 60/20/20 mutations.
- `scaling-results.csv`: 30 balanced samples for widths 1, 12, and automatic on the two primary 1M/100k workloads.
- `caller-results.csv`: 8 balanced samples for widths 1, 4, 12, and automatic with 2, 4, and 8 callers across all seven workloads.
- `workload-results.csv`: 6 balanced width-1/automatic samples across all seven 1M/100k workloads.
- `machine.txt`: host, toolchain, seed, and source-revision metadata.
- `report.md`: interpretation, regression gates, and limitations.

## Commands

```bash
PROLLY_WORKLOADS=value-only,mixed-60-20-20 \
PROLLY_WORKERS=1,12,0 \
PROLLY_BENCH_RUNS=30 \
cargo bench --bench prolly_bench -- parallel-scaling 1000000 100000

PROLLY_WORKERS=1,4,12,0 \
PROLLY_CALLERS=2,4,8 \
PROLLY_BENCH_RUNS=5 \
cargo bench --bench prolly_bench -- parallel-callers 100000 10000

PROLLY_WORKERS=1,0 \
PROLLY_BENCH_RUNS=6 \
cargo bench --bench prolly_bench -- parallel-scaling 1000000 100000
```

`PROLLY_BENCH_RUNS` is rounded up to a complete rotation of configured widths. Each width therefore occupies every measurement position equally.

The A/B runner compiled revision `fd8f200` and the candidate as separate release binaries. Each process built the 1M-entry base outside the timed interval, warmed the selected mutation once, then timed one 100k-mutation call. Odd pairs ran old then new; even pairs ran new then old. Both binaries used automatic scheduling. Using explicit width one would not be a valid old/new comparison because the old implementation ignored that setting while the candidate intentionally honors it.

All benchmark modes compare every timed root with width one and a fresh canonical rebuild before accepting the row.
