# Turso 1M / 30% prolly baseline design

## Goal

Create a reproducible, local-only Turso baseline that matches the hardened SQLite scale workload closely enough for direct engine-level comparison and future 5M/10M expansion.

## Contract

- Harness: `benchmarks/turso-scale`, using `AsyncProlly<TursoStore>` and a native local Turso database with cloud sync disabled.
- Baseline output: `performance-results/turso/baseline`.
- Base: 1,000,000 sorted 24-byte keys and deterministic 100-byte values, built with `AsyncSortedBatchBuilder` and `Config::default()`.
- Repetitions: three independent build fixtures.
- Mutation set: 300,000 keys (30%) for batch, diff, and merge; merge splits the total equally across disjoint branches.
- Read set: 10,000 keys/rows.
- Patterns: append, deterministic random, and clustered.
- Operations: build, single put, batch, cold point get, warm point get, multi-get query, bounded scan, full scan, diff, and merge.
- Full scan runs once per fixture because pattern does not change its semantics. This yields 25 cells per repetition, 75 raw rows total.

## Measurement boundaries

Each workload uses an isolated clone of a closed base fixture. Timed intervals contain only the named prolly operation and complete iterator consumption. Fixture cloning, diff/merge branch setup, validation, statistics, named-root publication, and reopen checks stay outside timing. Manager cache state is explicit; OS filesystem cache remains uncontrolled and documented.

## Correctness and provenance

Every result validates cardinality and returned or changed content. Mutating results are published and reopened. The runner records machine information, dependency features, exact Git state, checksums, logs, and a source archive; it refuses cloud-sync features and incompatible resume manifests.
