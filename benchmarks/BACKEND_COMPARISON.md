# Reproduce the PostgreSQL and DynamoDB Local comparison

The backend comparison runs byte-identical public Prolly operations against fresh local PostgreSQL and DynamoDB Local containers. It validates complete logical outcomes after timing and refuses incomplete, dirty, resumed, or mismatched evidence.

Run the default 1M-record comparison:

```bash
./scripts/run_backend_comparison.sh
```

The driver requires one excluded warm-up and at least seven measured repetitions. It alternates backend order, removes service volumes between invocations, and runs the exact release binaries copied into the result directory.

Configure a 10M-record run:

```bash
BENCH_RECORDS=10000000 \
BENCH_VALUE_BYTES=27 \
BENCH_CHANGES=10000 \
BENCH_SAMPLES=10000 \
BENCH_CONCURRENCY=32 \
BENCH_RUNS=7 \
./scripts/run_backend_comparison.sh
```

`BENCH_CHANGES` must be even and cannot exceed the record count. `BENCH_SAMPLES` cannot exceed the record count.

Each result directory contains:

- `manifest.txt`: clean source, binary, workload, command, and image identity
- `raw-results.csv`: all measured rows in the common evidence schema
- `comparison.csv`: validated descriptive statistics and confidence intervals
- `report.md`: latency, throughput, dispersion, and supported winner claims
- `measurement-commands.txt`: exact service and runner commands
- `bin/`: the release binaries used by the run
- `invocations/` and `warmup/`: per-process evidence

The report declares a winner only when the paired bootstrap 95% confidence interval excludes parity and the median latency effect exceeds 5%.

DynamoDB Local supports repeatable adapter regression testing. It does not predict Amazon DynamoDB network latency, throttling, partitions, capacity, or cost. PostgreSQL also runs locally in Docker, so the comparison measures local adapter implementations rather than production infrastructure.

## Recorded 10M comparison

The audited seven-repetition 10M result is in
[`performance-results/backend-comparison-10m-hardened-2026-07-26`](../performance-results/backend-comparison-10m-hardened-2026-07-26/report.md).
Its manifest records the exact clean source tree, release binaries, workload configuration, commands, and pinned container images used for the run.

## Verify broader byte-for-byte behavior

Run the separate deterministic cross-backend correctness harness:

```bash
./scripts/run_backend_correctness.sh
```

That harness covers shuffled builds, duplicate mutations, deletes, inserts, disjoint merges, conflicting merges, reopened snapshots, stored content identifiers, and serialized node bytes.
