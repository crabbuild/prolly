# SQLite prolly scale benchmark

The harness in `benchmarks/sqlite-scale` measures synchronous
`Prolly<SqliteStore>` operations with fixed 24-byte keys and 100-byte values.
Its default full profile establishes the SQLite baseline at 1,000,000 base
records, 300,000 mutations, 10,000 read samples, and three repetitions.

Run the complete release baseline:

```sh
scripts/run_sqlite_scale_benchmark.sh
```

Run a fast real-SQLite validation:

```sh
scripts/run_sqlite_scale_benchmark.sh \
  --profile smoke \
  --output performance-results/sqlite/baseline/smoke
```

The operation set is build, single-key put, batch, cold and warm point get,
batched query (`get_many`), bounded scan, full scan, diff, and three-way merge.
Append, deterministic random, and centered clustered patterns apply to every
pattern-sensitive operation. Full scan runs once per fixture because its input
does not vary by key pattern.

Append changes add right-edge keys. Random and clustered changes update
existing keys. Merge interprets the change count as the total across two equal,
disjoint branches; random branch assignments are interleaved across the sorted
keyspace. Mutation cardinality is independent from read/scan sample count.

Every workload receives a filesystem clone of a closed base fixture and a
fresh manager. Diff and merge branch setup, fixture cloning, validation, stats,
publication, and reopen checks are outside the timed interval. Lazy scans are
fully consumed inside it. Cold point gets clear the manager cache before each
lookup, while warm gets receive an untimed warmup pass.

Results are written under `performance-results/sqlite/baseline`. The output
contains raw and summarized CSV, a Markdown report, fixture measurements,
status and manifest files, machine and dependency information, the release
binary checksum, build/run logs, and the exact compressed tracked source patch
used for a dirty run. A checksummed harness source archive also preserves
renamed or other untracked harness files that Git patches cannot represent.
Validated cells are resumable only when the existing manifest matches the
requested workload exactly.

SQLite uses WAL, `synchronous=NORMAL`, a 5,000 ms busy timeout, and
`temp_store=MEMORY`. The manager cache is controlled, but the operating-system
filesystem cache is not. Results characterize one local synchronous connection;
they do not predict concurrent writers, remote filesystems, or raw SQLite SQL
performance.
