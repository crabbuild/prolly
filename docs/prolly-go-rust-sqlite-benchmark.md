# Dolt Go vs Rust SQLite prolly benchmark

This harness compares the Dolt Go and Rust prolly trees when both persist their
content-addressed nodes in SQLite. It uses each implementation's native tree
encoding and chunking policy while enforcing the same logical records,
mutations, operation counts, and result cardinalities.

Run a real-SQLite smoke comparison:

```sh
BENCH_PROFILE=smoke scripts/run_dolt_sqlite_comparison.sh
```

Run the default 1,000,000-record, three-repetition matrix:

```sh
BENCH_PROFILE=full scripts/run_dolt_sqlite_comparison.sh
```

Set `DOLT_REV=<commit>` to reproduce an exact Dolt revision. Set `BENCH_OUT` to
a new directory to retain multiple runs. The driver refuses to overwrite any
existing output directory.

## Operations and workloads

The comparison mirrors `benchmarks/sqlite-scale`: fixture build, put, batch,
cold and warm point get, query, bounded scan, full scan, diff, and three-way
merge. Pattern-sensitive cells run append, deterministic random, and centered
clustered inputs. Full scan runs once per fixture.

Keys are fixed-width 24-byte strings. Values are deterministic 100-byte
payloads. Automatic change count is 30% of the base, merge changes are split
evenly across disjoint branches, and the default read sample is 10,000.

Each language builds one closed, checkpointed base fixture for every size and
repetition. Each measured cell receives a filesystem clone and a fresh store.
The driver alternates which language runs first, never overlaps processes, and
sets `GOMAXPROCS=1` and `RAYON_NUM_THREADS=1`.

SQLite uses WAL, `synchronous=NORMAL`, a 5,000 ms busy timeout, and
`temp_store=MEMORY`. Fixture construction, cloning, validation, root
publication, reopen checks, and statistics are outside measured operation
intervals. Tree node persistence is inside build and mutation intervals.

## Correctness and output

Every row validates exact point values, ordered scans, diff keys, merged branch
values, cardinality, and—after mutations—close/reopen persistence. The
summarizer rejects missing or duplicate rows, failed processes, incomplete
repetitions, and cross-language mismatches in the logical contract.

The output contains:

- `raw-results.jsonl` and `process-manifest.csv`;
- paired `results.csv` and median `summary.csv`;
- `report.md`;
- exact Dolt and Rust revisions, source and executable hashes;
- toolchain, host, SQLite, workload, and process peak-RSS metadata; and
- per-process JSON, stderr, and `/usr/bin/time` output.

## Interpretation limits

This is an end-to-end product-path comparison, not a common-format
microbenchmark. The persisted tree formats, node sizes, runtime allocators, and
cache implementations differ.

Rust exposes a native map-level `get_many` operation. Dolt currently has no
equivalent logical multi-get API, so its query cell performs repeated native
`Map.Get` calls. Reports label both strategies explicitly; query ratios should
be read as available-product-API comparisons rather than identical batching
primitives.
