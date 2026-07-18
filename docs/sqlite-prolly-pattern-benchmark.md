# SQLite-backed prolly key-pattern benchmark

This benchmark measures the synchronous `Prolly<SqliteStore>` path with fixed
24-byte keys and fixed 100-byte values. It compares append, deterministic
random, and clustered workloads at 10K, 50K, 100K, 500K, and 1M base records.

Run the complete release matrix:

```sh
scripts/run_sqlite_prolly_pattern_benchmark.sh
```

Run a fast real-SQLite validation:

```sh
scripts/run_sqlite_prolly_pattern_benchmark.sh \
  --profile smoke \
  --output performance-results/sqlite-prolly-patterns-smoke
```

The full matrix has 15 fixtures and 225 raw rows: three repetitions of five
sizes and 15 workload cells per fixture. Each fixture measures a sorted base
build. Each workload gets an isolated clone and a fresh manager.

The 15 cells are six mutations (individual put and transactional batch for
three patterns), six point-read cells (cold and warm manager for three
patterns), and three bounded range scans. Mutation and read samples are one
percent of base cardinality, clamped to 100 through 10,000 operations.

Outputs include `raw-results.csv`, `fixture-results.csv`, `summary.csv`,
`report.md`, `machine.txt`, `dependencies.txt`, `driver-provenance.txt`,
`run-manifest.txt`, and `run-status.txt`. Successful rows are validated before
they are flushed. Mutation results are published, closed, reopened, and
validated again.

SQLite uses the adapter defaults: WAL, `synchronous=NORMAL`, a 5,000 ms busy
timeout, and `temp_store=MEMORY`. A cold manager has no decoded prolly nodes in
its in-process cache; a warm manager receives one untimed pass. The operating
system filesystem cache is not dropped, so neither label means physically cold
storage.
