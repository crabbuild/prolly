# SQLite 1M / 30% Prolly Baseline Design

## Goal

Rename the existing SQLite prolly-pattern benchmark to `sqlite-scale`, harden it into a repeatable scale harness, and establish a PostgreSQL-comparable baseline in `performance-results/sqlite/baseline`.

## Workload contract

- Base tree: exactly 1,000,000 sorted 24-byte keys with 100-byte values.
- Repetitions: three independent fixture builds and three isolated measurements per workload cell.
- Mutation workload: 300,000 logical changes (30% of the base size).
- Read workload: 10,000 sampled keys or rows, independent of mutation cardinality.
- Patterns: append, deterministic random, and centered clustered keys.
- Operations: build, single-key put, batch mutation, cold point get, warm point get, batched query (`get_many`), bounded scan, full scan, diff, and three-way merge.
- Merge: 300,000 total changes split evenly across disjoint left and right branches. Random branch keys are interleaved across the keyspace.

Append mutations add new keys. Random and clustered mutations update existing keys. A single append put therefore grows the tree by one; an append batch grows it by 300,000. Merge validates the combined non-conflicting branch result.

## Isolation and measurement

Each repetition builds and closes a source SQLite fixture. Every workload cell runs against a filesystem clone so writes, cache state, and published roots cannot contaminate later cells. SQLite uses WAL and `synchronous=NORMAL`, matching the existing harness. Manager cache state is explicit; OS filesystem cache remains uncontrolled and is disclosed in the report.

Timing encloses only the named prolly operation and required iteration of lazy results. Fixture cloning, setup mutations for diff/merge, correctness checks, stats collection, publishing, reopening, and reporting occur outside the timed interval.

The harness records elapsed time, per-operation throughput, sampled latency percentiles where meaningful, prolly node/cache/byte metrics, tree shape, SQLite database/WAL/SHM sizes, expected and observed counts, and validation status.

## Harness migration

- Move `benchmarks/sqlite-prolly-patterns` to `benchmarks/sqlite-scale`.
- Rename the Rust package, library, and binary to SQLite scale terminology.
- Rename the runner to `scripts/run_sqlite_scale_benchmark.sh`.
- Update active documentation and references; retain dated historical result directories unchanged.
- Default baseline output to `performance-results/sqlite/baseline`.
- Preserve resumability with a strict manifest that rejects incompatible prior rows.

## Correctness and failure handling

Every build is reopened and sampled. Write results are checked for exact cardinality and changed values, then published and reopened. Reads verify every returned value. Scans verify ordering, bounds, and count. Diff verifies every expected key/generation pair. Merge verifies disjoint branch construction and every merged change. Failed cells are recorded as invalid and halt the run; completed validated cells remain resumable.

Disk-space checks and a completion status prevent starting an unsafe scale run or mistaking partial output for a baseline. Provenance captures the revision, dirty state, dependency graph, machine details, runner command, and source diff.

## Verification

Unit tests cover CLI defaults and overrides, the complete matrix, cardinality semantics, deterministic patterns, random merge interleaving, manifest compatibility, percentile/report behavior, and result validation. Integration tests run every operation on a small real SQLite fixture, including reopen persistence. The release binary must pass a smoke run before the 1M baseline begins.
