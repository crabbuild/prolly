# PostgreSQL 1M / 30% Prolly Baseline Design

## Goal

Produce a reproducible PostgreSQL 16 baseline for the current Rust
`AsyncProlly<RemoteProllyStore<PostgresBackend>>` implementation at exactly
1,000,000 initial records. Measure build, single write, batch mutation, point
get, multi-get, bounded and full scan, diff, and three-way merge for append,
random, and clustered key distributions. Store durable raw data, provenance,
validation evidence, and a readable report in
`performance-results/postgres/baseline`.

## Selected Approach

Extend the existing `benchmarks/postgres-scale` harness instead of adding a
raw-SQL benchmark or a second Prolly benchmark crate. The existing harness
already exercises the public async Prolly API, restores a PostgreSQL snapshot
before every independent cell, captures Prolly and PostgreSQL counters, and
strictly validates its CSV matrix. Extending it preserves comparability with
the 2026-07-18 scale run while keeping the 30% write workload unambiguous.

## Workload Contract

- Build exactly 1,000,000 sorted fixed-width key/value records in one public
  `batch()` call against an empty tree.
- Use the existing deterministic seed `0x6a09e667f3bcc909`.
- Treat 30% mutation as 300,000 logical changes:
  - append inserts IDs `[1_000_000, 1_300_000)`;
  - random updates 300,000 deterministic unique existing IDs;
  - clustered updates one centered contiguous 300,000-key interval.
- For merge, use two disjoint branches with 150,000 changes each, so the merged
  result contains 300,000 total branch changes. Append branches use adjacent
  suffix ranges; clustered branches split one centered interval; random keys
  alternate between branches after deterministic selection so both branches
  remain distributed across the full base keyspace.
- Keep read sampling independent of mutation size. Point get, multi-get, and
  bounded scan use 10,000 keys/entries per cell. Full scan consumes all
  1,000,000 entries.
- Point get includes cold-manager and warm-manager modes. “Cold” controls only
  the decoded Prolly node cache; PostgreSQL and host filesystem caches remain
  natural and are documented.
- Run cells serially with one client. Build and full scan have one observation;
  other cells have three independent repetitions with rotated pattern order.

## Isolation and Timing

Use the existing dedicated Docker Compose project with `postgres:16-alpine`,
`pg_stat_statements`, and `track_io_timing=on`. Build a validated base fixture,
snapshot the three production Prolly tables, and restore that snapshot before
every independent cell. Input generation, snapshot restore, `ANALYZE`, metric
reset, and correctness validation are outside timed regions.

Wall time spans only the public Prolly operation. PostgreSQL statement metrics,
Prolly nodes/bytes/cache counters, tree shape, WAL, blocks, and physical sizes
are captured separately. Every row is flushed durably after validation so a
long run can resume.

## Harness Changes

Add an independent `read_samples` configuration field and CLI/runner option.
`CellSpec` carries both `changes` and `read_samples`; write, diff, and merge use
the former, while get/query/bounded scan use the latter. Add a merge-total mode
to the workload contract by passing 150,000 changes per branch for a configured
300,000 total. Record both values and the merge interpretation in the manifest
and report so later 5M/10M runs remain comparable.

The raw result schema remains compatible because each row already records its
actual `logical_operations` and `observed_items`; the manifest freezes the
configured mutation and read cardinalities. Resume is permitted only inside
the exact output directory and configuration.

## Validation

- Unit tests cover parsing/defaults for read samples, matrix propagation, the
  30% calculation, and 15%+15% merge branch cardinality.
- Existing deterministic-pattern, CSV, and strict summarizer tests continue to
  pass.
- A Docker-backed 1,000-record smoke run uses 300 changes, 100 read samples,
  and 150 changes per merge branch, exercising every operation and pattern.
- The full run must produce the exact required matrix, all rows must have
  `validated=true`, and the strict summarizer must complete without partial
  mode.

## Outputs

`performance-results/postgres/baseline` contains:

- `raw-results.csv` and `summary.csv`;
- `report.md` with the primary tables and interpretation limits;
- `run-manifest.txt`, `machine.txt`, `postgres.txt`, and `dependencies.txt`;
- `build.log`, `run.log`, and the release-binary SHA-256;
- a concise baseline README describing rerun and future 5M/10M commands.

## Interpretation Limits

Results are machine- and revision-specific, use small fixed-width values,
single-client execution, Docker Desktop PostgreSQL defaults, and natural
database/OS cache state. They establish a stable regression baseline, not a
maximum production throughput claim. The report must call out any host load,
container restart, resumed cell, or validation anomaly observed during the run.
