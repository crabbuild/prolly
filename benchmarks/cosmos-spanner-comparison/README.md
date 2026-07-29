# Cosmos DB / Spanner adapter comparison

This Rust benchmark exercises the `RemoteStoreBackend` operations used by
prolly: batch node publication, ordered batch reads, and named-root
compare-and-swap. It is intended for repeatable adapter regression detection,
not for claiming cloud-service capacity from emulator timings.

Run both credential-free Docker emulators, conformance tests, contention tests,
and the comparison:

```bash
scripts/run-cosmos-spanner-comparison.sh
```

Results are written beneath `performance-results/cosmos-spanner-*`. Configure
the workload with:

```bash
export PROLLY_CLOUD_BENCH_ITEMS=1000
export PROLLY_CLOUD_BENCH_VALUE_BYTES=1024
export PROLLY_CLOUD_BENCH_CAS_ITERATIONS=100
export PROLLY_CLOUD_BENCH_CONCURRENCY=16
```

To use managed services, supply all four `PROLLY_STORE_COSMOS_*` values and
`PROLLY_STORE_SPANNER_DATABASE`; normal application-default Spanner
authentication is used when `SPANNER_EMULATOR_HOST` is absent. Use isolated
test resources because the Spanner conformance suite clears the three adapter
tables.

Emulators verify correctness, batching, request counts, contention behavior,
and relative regressions on one machine. They do not reproduce production RU
accounting, multi-region latency, autoscaling, replication, IAM, or Spanner
query planning. Repeat candidate configurations against isolated managed
resources before production sizing.
