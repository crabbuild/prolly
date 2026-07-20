# Turso prolly scale benchmark

The harness in `benchmarks/turso-scale` measures local-only asynchronous
`AsyncProlly<TursoStore>` operations with fixed 24-byte keys and 100-byte
values. Its default full profile establishes the Turso baseline at 1,000,000
base records, 300,000 mutations, 10,000 read samples, and three repetitions.

Run the complete release baseline:

```sh
scripts/run_turso_scale_benchmark.sh
```

Run a fast native-Turso validation:

```sh
scripts/run_turso_scale_benchmark.sh \
  --profile smoke \
  --output performance-results/turso/baseline/smoke
```

Generate the SQLite-versus-Turso comparison from compatible scale baselines:

```sh
python3 scripts/compare_sqlite_turso_scale.py \
  --sqlite-dir performance-results/sqlite/baseline \
  --turso-dir performance-results/turso/baseline \
  --output performance-results/turso/baseline/sqlite-comparison.md
```

The comparator rejects mismatched workload cells or manifests and reports both
the Turso latency delta and the SQLite/Turso speedup direction. It also carries
the recorded revision and dirty-worktree limitations into the generated report.

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

Results are written under `performance-results/turso/baseline`. The output
contains raw and summarized CSV, a Markdown report, fixture measurements,
status and manifest files, machine and dependency information, the release
binary checksum, build/run logs, and the exact compressed tracked source patch
used for a dirty run. A checksummed harness source archive also preserves
renamed or other untracked harness files that Git patches cannot represent.
Validated cells are resumable only when the existing manifest matches the
requested workload exactly.

This is native local Turso with `turso-cloud-sync` disabled. The default runtime
uses four Tokio worker threads, and async scheduling/store overhead is included.
The manager cache is controlled, but the operating-system filesystem cache is
not. Results do not predict Turso Cloud synchronization, concurrent writers,
remote filesystems, or raw SQL performance.
