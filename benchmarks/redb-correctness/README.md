# redb correctness harness

This executable verifies the redb adapter against the in-memory store and
directly audits the persisted redb tables. It covers randomized mutations and
reopens, canonical roots, point and range reads, node/CID invariants, LZ4
encoding, legacy-table migration, garbage collection, compaction, diff paging,
three-way merge and conflict resolution, version history, compare-and-swap,
multi-map rollback, concurrent writers, and redb's single-writer locking
characteristic.

Run the complete harness with Rust 1.89 or newer:

```bash
cargo +1.89.0 run --release \
  --manifest-path benchmarks/redb-correctness/Cargo.toml
```

The harness creates uniquely named databases under the operating system's
temporary directory and removes them after each scenario. A successful run
ends with a `CORRECT` record. Intermediate `STORAGE`, `COMPACTION`,
`LEGACY_ENGINE`, `DIFF_MERGE`, and `VERSION_HISTORY` records summarize the
characteristics that were verified.
