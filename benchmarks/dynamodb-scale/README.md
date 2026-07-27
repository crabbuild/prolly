# DynamoDB Local scale benchmark

This Rust harness measures the DynamoDB adapter and end-to-end Prolly tree
operations against DynamoDB Local. It covers raw batch reads and writes, named
root scans, compare-and-swap conflicts, concurrent point reads, tree creation,
batched mutation, query, diff, and merge.

`BENCH_CHANGES` is the total mutation count for batch, diff, and merge. Merge
splits that count evenly across two disjoint branches so its workload matches
the PostgreSQL scale harness.

DynamoDB Local makes regression testing repeatable. Its throughput and latency
do not predict AWS DynamoDB performance.

## Quick start

From the repository root:

```bash
./scripts/run_dynamodb_scale_benchmark.sh --profile smoke
./scripts/run_dynamodb_scale_benchmark.sh --profile full
```

The runner starts DynamoDB Local with Docker, builds an optimized Rust binary,
captures machine and workload metadata, writes every completed measurement
durably to `raw-results.csv`, and creates `summary.csv` and `report.md`.

Set `BENCH_CLEANUP=1` to stop and remove the benchmark container after a run.
Use `PROLLY_BENCH_SKIP_DOCKER=1` with `--endpoint` to target an already running
DynamoDB Local instance.

## Configuring scale and concurrency

Every important workload dimension is configurable with environment variables:

```bash
BENCH_RECORDS=10000000 \
BENCH_SAMPLES=100000 \
BENCH_CHANGES=100000 \
BENCH_CONCURRENCY=64 \
BENCH_CONCURRENT_OPERATIONS=100000 \
BENCH_BATCH_GET_PARALLELISM=16 \
BENCH_BATCH_WRITE_PARALLELISM=16 \
./scripts/run_dynamodb_scale_benchmark.sh \
  --profile full \
  --output performance-results/dynamodb-local-10m
```

Other controls are `BENCH_VALUE_BYTES`, `BENCH_RAW_ITEMS`, `BENCH_ROOTS`,
`BENCH_CONFLICTS`, `BENCH_RUNS`, `BENCH_READ_PARALLELISM`, and
`BENCH_SCAN_PARALLELISM`.

Namespace deletion can dominate the wall-clock time after a multi-million-record
run while remaining outside measured timings. Set `BENCH_NAMESPACE_CLEANUP=0`
when using a disposable DynamoDB Local container, then use `BENCH_CLEANUP=1` or
remove the container after collecting results.

For a before/after report, point `--baseline` at an earlier result directory:

```bash
./scripts/run_dynamodb_scale_benchmark.sh \
  --profile full \
  --baseline performance-results/dynamodb-local-baseline
```

The result directory can be reused after an interruption. Completed
operation/repetition pairs are validated and skipped.
