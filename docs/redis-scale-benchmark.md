# Redis prolly scale benchmark

This benchmark establishes a reproducible Redis baseline for the prolly tree's build, point put, batch mutation, cold and warm point get, batched query, bounded and full scan, diff, and three-way merge operations.

## Baseline contract

- The default full profile builds 1,000,000 deterministic records, applies 300,000 changes (30%), performs 10,000 reads or bounded-scan rows, and runs three independent repetitions.
- Append, random, and clustered key patterns are covered. Full scan runs once per repetition because its key pattern does not change the workload.
- Keys are 24 bytes and values are 100 bytes. The random seed, change semantics, and matrix are frozen in `run-manifest.txt`.
- Each repetition builds one source Redis namespace. Every measured cell receives a server-side `COPY` clone under a unique namespace. Clone, cleanup, branch setup, validation, publication, persistence checks, and statistics are outside the timed interval.

## Strong durability

The runner starts the pinned `redis:7.4.2-bookworm` image with AOF enabled and `appendfsync always`. RDB snapshots and automatic AOF rewrites are disabled, and `no-appendfsync-on-rewrite` remains `no`. The benchmark binary checks these settings itself and refuses to run if they differ. After deleting each completed cell namespace, the harness explicitly completes and validates `BGREWRITEAOF` outside the timed interval. This prevents clone/delete history from growing without bound while keeping maintenance deterministic and away from measured operations.

This is the strongest normal Redis AOF acknowledgement mode, but a Docker Desktop Linux VM and host storage may still contain volatile layers. Results measure the configured Redis durability path and are not a proof of power-loss durability for the physical machine.

## Run it

From the repository root:

```bash
scripts/run_redis_scale_benchmark.sh --profile smoke \
  --output performance-results/redis/baseline/smoke

scripts/run_redis_scale_benchmark.sh --profile full \
  --output performance-results/redis/baseline
```

The runner chooses an unused localhost port, verifies Redis readiness, builds a release binary, records source and binary checksums, captures Docker/image/configuration/system provenance, and removes its container and volume after the run. Set `REDIS_BENCH_KEEP_CONTAINER=1` or `REDIS_BENCH_KEEP_FIXTURES=1` only for debugging.

The full output belongs under `performance-results/redis/baseline`. `raw-results.csv` contains one validated row per cell and repetition, `fixture-results.csv` contains build measurements, `summary.csv` contains medians, and `report.md` is the human-readable baseline.

The runner supports `REDIS_BENCH_SIZES`, `REDIS_BENCH_RUNS`, `REDIS_BENCH_CHANGES`, `REDIS_BENCH_READ_SAMPLES`, `REDIS_BENCH_OPERATIONS`, `REDIS_BENCH_PATTERNS`, `REDIS_BENCH_TOKIO_WORKERS`, and `REDIS_BENCH_MIN_FREE_GB` for scaling and focused profiling.
