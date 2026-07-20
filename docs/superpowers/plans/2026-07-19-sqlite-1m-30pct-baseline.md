# SQLite 1M / 30% Prolly Baseline Implementation Plan

> Execute with the `superpowers:executing-plans` workflow and verify each stage before proceeding.

**Goal:** Migrate the existing SQLite harness to `sqlite-scale`, add the full PostgreSQL-compatible prolly operation matrix, and produce a validated 1M baseline.

**Architecture:** Reuse the existing synchronous `Prolly<SqliteStore>` fixture-clone harness. Separate mutation cardinality from read cardinality, dispatch operation-specific setup and timing, and retain resumable CSV/report output with a stricter schema and manifest.

**Tech stack:** Rust, `prolly`, `prolly-store-sqlite`, CSV/Serde, shell provenance runner.

---

### Task 1: Establish the migration and workload contract

1. Run the current SQLite harness test suite as the pre-change baseline.
2. Move `benchmarks/sqlite-prolly-patterns` to `benchmarks/sqlite-scale` and rename package/library/binary references.
3. Rename the runner and update active documentation paths.
4. Verify no live code or docs still target the old harness name (historical results excluded).

### Task 2: Add scale configuration and matrix tests

1. Add failing tests for the 1M full profile, 30% automatic changes, 10k reads, filters, and disk minimum.
2. Add failing tests for build plus all operation/pattern/cache combinations.
3. Add failing tests for single-put versus batch cardinality and even merge changes.
4. Implement the configuration and matrix until tests pass.

### Task 3: Harden deterministic workload generation

1. Add failing tests for append, random, clustered, lookup/range, and disjoint merge IDs.
2. Require random merge branches to be interleaved across the keyspace.
3. Implement deterministic generators and pass unit tests.

### Task 4: Implement and validate all operations

1. Add integration tests that exercise every operation on a small SQLite fixture.
2. Implement single put, batch, cold/warm get, query, bounded/full scan, diff, and merge.
3. Keep setup and validation outside timed sections.
4. Validate cardinality, values, ordering, diff contents, merge contents, publication, and reopen persistence.
5. Run the crate test suite.

### Task 5: Upgrade measurement, reports, and resumability

1. Add failing tests for schema/manifest rejection, summary completeness, and invalid rows.
2. Add build to raw results and expose logical operations versus observed items.
3. Record engine/tree/SQLite sizes and provenance consistently across operations.
4. Generate a concise Markdown baseline report with methodology and limitations.
5. Run formatting, Clippy, and all harness tests.

### Task 6: Verify smoke profile

1. Build the release binary.
2. Run the complete smoke matrix into `performance-results/sqlite/baseline/smoke`.
3. Confirm status is complete, every row is validated, expected row counts match, and reports parse.
4. Fix any defect with a failing regression test before rerunning smoke.

### Task 7: Run and package the 1M baseline

1. Record revision, dirty patch, machine, dependency, disk, and runner provenance.
2. Run 1,000,000 records, 300,000 changes, 10,000 read samples, three repetitions into `performance-results/sqlite/baseline`.
3. Monitor progress and resume only against an identical manifest.
4. Verify completion status, row counts, validation flags, summaries, disk artifacts, and report claims.
5. Review the scoped diff and provide exact result locations and headline measurements.
