# Reproduce backend performance comparisons

## MySQL vs PostgreSQL

Run the controlled local comparison with pinned MySQL 8 and PostgreSQL 16
containers:

```bash
./scripts/run_mysql_postgres_comparison.sh
```

The command runs byte-identical public Prolly build, batch, ordered query,
concurrent query, diff, and merge workloads. It also runs a shared adapter
service suite covering bounded batch puts, ordered batch gets, concurrent point
reads, and contended root compare-and-swap. Request-level p50/p95/p99/p99.9/max
latency, throughput, conflicts, pool size, and adapter batch size are recorded.

Sweep client and pool saturation cells:

```bash
BENCH_CLIENTS=1,8,32 \
BENCH_POOL_SIZES=4,16 \
./scripts/run_mysql_postgres_service_matrix.sh
```

Tune one comparison with `BENCH_POOL_SIZE`,
`BENCH_ADAPTER_BATCH_ITEMS`, `BENCH_CONCURRENCY`, `BENCH_RECORDS`,
`BENCH_CHANGES`, and `BENCH_SAMPLES`. Every publishable cell uses one excluded
warmup, at least seven alternating measured repetitions, fresh local volumes,
and release binaries copied into the result directory.

External managed services are supported only through disposable isolated
benchmark databases. The destructive acknowledgement and explicit server
identities are mandatory:

```bash
BENCH_MODE=external \
BENCH_EXTERNAL_RESET_ACK=I_UNDERSTAND_BENCHMARK_DATA_WILL_BE_DELETED \
BENCH_EXTERNAL_POSTGRES_IDENTITY='provider/postgres/version/config' \
BENCH_EXTERNAL_MYSQL_IDENTITY='provider/mysql/version/config' \
PROLLY_BACKEND_POSTGRES_URL='postgres://…' \
PROLLY_BACKEND_MYSQL_URL='mysql://…' \
./scripts/run_mysql_postgres_comparison.sh
```

Recorded commands redact URL credentials. External evidence is labeled and
cannot be mixed with controlled local evidence.

The SQL result directory adds:

- `raw-service-results.csv`: validated adapter/service samples
- `service-comparison.csv`: paired service statistics
- `service-report.md`: batch, concurrent-read, and root-contention comparison

## PostgreSQL vs DynamoDB Local

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
