# MySQL adapter performance and PostgreSQL comparison design

## Status

Approved in conversation on 2026-07-28. This document defines the implementation
contract for hardening the Rust MySQL adapter and producing reproducible
MySQL/PostgreSQL performance evidence.

## Context

`prolly-store-mysql` implements the complete remote-store contract, but its
batch paths currently execute one SQL statement per node. Ordered batch reads,
node batches, node publications with hints, and transactional node writes all
scale linearly in database round trips. The PostgreSQL adapter already uses
bounded set-based reads and writes, so a direct comparison of the current
adapters would mostly measure avoidable MySQL statement amplification.

The repository also has a hardened PostgreSQL/DynamoDB Local comparison
framework and a PostgreSQL service-scale harness. The new work must reuse those
contracts and safeguards instead of creating two independent benchmark
implementations that can drift.

## Goals

1. Remove per-node SQL round trips from MySQL batch paths.
2. Preserve the remote-store contract, stored node format, public data tables,
   and atomic publication semantics.
3. Make concurrent creation and update of named roots safe for absent and
   existing roots.
4. Run byte-identical end-to-end Prolly workloads against MySQL and PostgreSQL.
5. Measure adapter and service scalability across batch sizes, client counts,
   pool sizes, and root-contention levels.
6. Make controlled local Docker runs easy to reproduce.
7. Support externally supplied managed-service URLs without presenting those
   runs as controlled local evidence.
8. Emit fail-closed raw evidence, statistical comparisons, and a readable
   report with complete provenance.

## Non-goals

- Optimizing or benchmarking the Node, JVM, Python, Ruby, or Go MySQL adapters
- Claiming that one local Docker result predicts every managed MySQL or
  PostgreSQL deployment
- Changing public Prolly semantics or its content-addressed wire format
- Hiding input generation, validation, diagnostics, or database-statistics
  collection inside timed regions
- Adding automatic retries that conceal database errors or contention
- Replacing the existing PostgreSQL/DynamoDB Local comparison mode

## Architecture

The implementation extends the existing trustworthy comparison framework.
`prolly-store-mysql` remains the production adapter. The shared backend workload
contract remains the only source of compared workload bytes and expected
outcomes. The backend-comparison crate gains MySQL support while retaining its
PostgreSQL and DynamoDB Local runners. A dedicated SQL comparison entry point
orchestrates MySQL and PostgreSQL without changing the existing
PostgreSQL/DynamoDB command.

A shared SQL service suite exercises both adapters through the same public
Prolly and remote-store interfaces. Backend-specific code is restricted to
connection construction, schema reset, database diagnostics, and adapter
selection. Workload generation, operation selection, latency collection,
validation, evidence schemas, regression gates, and report generation are
shared.

Controlled evidence uses pinned MySQL 8 and PostgreSQL 16 container references,
fresh service volumes, release binaries copied into the result directory, and
alternating backend order. Remote mode accepts externally supplied URLs and
uses the same workload and evidence schema, but records the environment class
as `external` and cannot be mixed with controlled Docker evidence.

## MySQL adapter hardening

### Additive configuration

Add `MySqlBackendOptions` with a nonzero `max_batch_items`. The default is 1,000
items. Existing `new` and `connect` constructors keep their signatures and use
the default. Add `new_with_options`, `connect_with_options`, and `options`.
Callers continue to control connection-pool size with `sqlx::MySqlPoolOptions`
and `MySqlBackend::new`; adapter batch size and pool size remain independent.

All SQL builders must also respect MySQL's prepared-statement parameter limit.
The configured item count is an upper bound, and helpers derive the safe chunk
size from the number of parameters required per item.

### Set-based node writes

`batch_put_nodes`, upserts in `batch_nodes`, node publication with a hint, and
transactional node writes use bounded multi-row `INSERT ... ON DUPLICATE KEY
UPDATE` statements. Duplicate keys are collapsed before SQL generation with
the last requested operation winning.

Deletes use bounded `DELETE ... WHERE cid IN (...)` statements. A mixed node
batch first reduces to one final operation per CID, then executes deletes and
upserts inside one transaction. Statement order cannot affect the final
logical result because a CID appears in only one reduced set.

An empty node batch or node-publication batch returns without opening a
transaction when it has no accompanying hint or root work. A publication with
an empty node set and a hint still updates the hint atomically.

### Ordered batch reads

`batch_get_nodes_ordered` fetches each bounded set with one
`SELECT cid, node ... WHERE cid IN (...)` query. The adapter reconstructs the
result from returned CIDs so output order matches input order, duplicate
requests produce duplicate output slots, and absent CIDs produce `None`.

Duplicate requested CIDs may be collapsed within a SQL chunk because the
client-side reconstruction preserves their observable multiplicity. Empty
input returns an empty vector without acquiring a connection.

### Root locking and transactions

`initialize_schema` adds an internal table:

```sql
CREATE TABLE IF NOT EXISTS prolly_root_locks (
  name VARBINARY(255) PRIMARY KEY
);
```

Existing `prolly_nodes`, `prolly_hints`, and `prolly_roots` rows require no
migration. The new table contains lock identities only; it is not part of the
public data model or content-addressed representation.

Before a transaction checks or changes roots, it:

1. Collects and lexicographically sorts the distinct root names.
2. Inserts missing lock identities with bounded `INSERT IGNORE`.
3. Acquires the corresponding rows with `SELECT ... FOR UPDATE` in sorted
   order.
4. Reads current manifests with bounded set-based queries.
5. Validates all root conditions.
6. Applies reduced node and root writes.
7. Commits once.

The lock row makes an absent root lockable. Sorted acquisition gives competing
multi-root transactions a common lock order. `put_root_manifest`,
`delete_root_manifest`, compare-and-swap, and `commit_transaction` all use the
same locking protocol so callers cannot bypass it through another public
method.

Root writes are reduced by name with last-write-wins semantics and use bounded
set-based upserts and deletes. A condition mismatch rolls back without
publishing node or root changes and returns the current conflicting manifest.

### Error behavior

Every public batch or multi-root transaction remains atomic across all SQL
chunks. Any SQL failure rolls the transaction back. The adapter does not retry
deadlocks, timeouts, connection failures, or rejected statements. These errors
remain visible to the caller, while deterministic locking removes the expected
cross-root lock-order deadlock.

## Benchmark workload contract

### End-to-end suite

Both adapters run the same deterministic workload bytes for:

- `build`: publish the complete base map
- `batch`: apply the configured update, insert, and delete set
- `query`: return an ordered multi-key request
- `concurrent_query`: execute the same point-read set with bounded concurrency
- `diff`: create and completely consume the logical diff
- `merge`: execute a complete deterministic three-way merge

The existing workload contract owns fixed-width keys, values, mutations, query
order, merge branches, expected maps, expected diffs, canonical roots, and
workload/outcome digests. Adding MySQL does not create a MySQL-specific
generator.

### Service and adapter suite

The shared SQL service suite sweeps:

- tree record counts and value sizes
- adapter batch sizes
- logical batch sizes
- client counts
- SQLx pool sizes
- tenant and named-root counts
- independent-root and hot-root traffic shares
- warmup and measurement durations
- operation mixes and bounded compare-and-swap retries

The measured operation classes are batch writes, ordered batch reads, point
reads, versioned commits, diffs, and merges. Each request uses the same cache
policy on both backends. Pool acquisition time is included in end-to-end
latency because pool saturation is part of service scalability.

Service rows record attempted and successful throughput, p50, p95, p99,
p99.9, and maximum latency, compare-and-swap attempts, conflicts, exhausted
retries, timeouts, SQL errors, validation errors, and worker panics. They also
record Prolly node/cache/store counters, database statement and I/O diagnostics
when available, and physical database/table/index sizes.

Database diagnostics are descriptive. Missing optional diagnostics in external
mode are recorded as unavailable and cannot change semantic validation.

## Timing and validation

Input generation, service reset, fixture construction, result validation,
diagnostic collection, and report generation are outside timed regions. Each
end-to-end timer encloses one complete public Prolly operation. Each service
sample measures one logical request, including connection-pool wait.

Validation runs after timing:

- Builds and batches match expected counts, ordered content digests, and roots.
- Ordered reads preserve keys, values, missing entries, and duplicates.
- Concurrent reads return every requested result exactly once.
- Diffs match complete ordered change records.
- Merges match the expected conflict count, complete map, digest, and root.
- Successful publications reopen to the same observable state.

Validation failure invalidates the row. Timings from invalid rows remain
diagnostic and cannot enter a winner calculation.

## Controlled local orchestration

A publishable local run:

1. Rejects an existing output path.
2. Requires a committed `HEAD` and clean tracked worktree.
3. Validates workload dimensions and requires at least seven repetitions.
4. Builds release binaries and records the dependency lockfile.
5. Copies and hashes the exact runners and summarizer into the result directory.
6. Pulls pinned MySQL and PostgreSQL images and records resolved image IDs.
7. Starts each service with a fresh volume and waits for health.
8. Runs one excluded warmup per backend.
9. Alternates backend order across measured repetitions.
10. Captures exact commands and machine, Docker, service, source, binary, and
    configuration identity.
11. Stops and removes service volumes after each isolated invocation.
12. Writes a failure marker on interruption or error.
13. Generates a report only after the raw evidence passes all checks.

The result directory is immutable for the invocation. The comparison driver
does not resume it or append measurements from another environment.

## External service mode

External mode accepts operator-supplied MySQL and PostgreSQL URLs and does not
start or remove those services. Each URL must point to a disposable, isolated
benchmark database because the production adapters use fixed table names.
External execution requires an explicit destructive-reset acknowledgement
before it clears those benchmark tables; otherwise it fails before connecting.
It never discovers or selects a database to reset from server metadata.

The manifest records `environment_class=external`, redacts credentials from
displayed URLs and commands, and captures the server versions and accessible
configuration. External evidence can be compared only with evidence from the
same run and environment class. The report states that infrastructure,
distance, load, and configuration are operator-controlled.

## Evidence and statistics

The common evidence schema gains MySQL as a backend value without changing the
meaning of existing PostgreSQL/DynamoDB rows. The summarizer accepts an explicit
two-backend comparison pair. The existing comparison command continues to
default to PostgreSQL/DynamoDB Local; the SQL command explicitly selects
MySQL/PostgreSQL.

The summarizer rejects:

- missing, duplicate, or unexpected rows
- fewer than seven measured repetitions
- mixed source, binary, workload, schema, environment, or service identities
- workload or outcome digest mismatches
- logical result or canonical-root mismatches
- invalid timing arithmetic or non-finite statistics
- failed validation
- dirty, resumed, incomplete, or stale manifests
- controlled local evidence without pinned resolved images

For every operation and backend, the report includes repetition count, median
latency, median throughput derived from latency, minimum, maximum, median
absolute deviation, coefficient of variation, and the deterministic paired
bootstrap 95% confidence interval for the latency ratio.

A backend is called faster only when the paired interval excludes parity and
the median latency difference exceeds 5%. Other results are `inconclusive`.
Service-scale reports emphasize saturation curves and tail latency; they do not
collapse a client/pool matrix into one universal winner.

## Testing strategy

Development follows red-green-refactor cycles.

### MySQL adapter tests

- option defaults and explicit batch limits
- empty-batch fast paths
- ordered reads across chunk boundaries
- duplicate and missing ordered reads
- last-write-wins duplicate node mutations
- bounded multi-row insert and delete statement counts
- atomic rollback when a later chunk fails
- atomic node publication with a hint
- reduced transactional node and root writes
- compare-and-swap on absent and existing roots
- concurrent absent-root creation with exactly one valid winner
- contended existing-root commits with valid serial outcomes
- multi-root transactions with reversed caller order and no torn publication
- complete remote-backend conformance

Docker-backed statement-count tests verify that publishing five unique nodes
with a two-item limit issues exactly three node-insert statements. A forced
failure in the third chunk must leave none of the earlier chunks committed.

### Harness tests

- MySQL parses and emits the common evidence schema
- explicit comparison-pair parsing preserves the old default
- complete MySQL/PostgreSQL fixtures summarize successfully
- incomplete, duplicate, mismatched, invalid, or mixed-environment fixtures fail
- percentile, dispersion, bootstrap, and winner calculations remain pinned
- fake-service orchestration alternates order and excludes warmups
- local and external manifests are labeled and validated differently
- credential-bearing URLs are redacted from recorded commands and reports
- runner failure prevents summarization and writes a failure marker

### End-to-end verification

A controlled Docker smoke run executes both suites on a small deterministic
dataset. The raw end-to-end rows must have matching workload digests, outcome
digests, counts, and roots. Every service cell must have the expected operation
and tenant-class rows with no validation errors or worker panics.

After smoke verification, a larger local run produces the initial checked-in or
published MySQL/PostgreSQL comparison report. Its claims remain scoped to the
captured machine, Docker allocation, adapter versions, database configuration,
and workload.

## Documentation and entry points

The repository documents:

- one command for the controlled MySQL/PostgreSQL comparison
- one command or flag set for the service-scale matrix
- configuration profiles for smoke and default runs
- external URL mode and its evidence limitations
- adapter batch-size and SQLx pool-size tuning
- result-directory contents and report interpretation
- how to rerun the exact workload and regenerate a report

The MySQL adapter README explains set-based batching, the internal root-lock
table, atomicity, defaults, and how to construct a custom pool and batch size.

## Acceptance criteria

Implementation is complete when:

1. The Rust MySQL adapter replaces per-node batch SQL with bounded set-based
   operations.
2. Existing constructors and stored node, hint, and root data remain compatible.
3. Ordered batch reads preserve order, duplicates, and missing slots.
4. Multi-chunk batches and transactions roll back atomically on failure.
5. Concurrent absent and existing root updates satisfy the allowed serial
   outcomes without torn publication.
6. The existing remote-store conformance suite passes for MySQL.
7. The comparison framework supports MySQL/PostgreSQL without breaking the
   PostgreSQL/DynamoDB Local mode.
8. Both end-to-end and service/adapter suites use shared workload and evidence
   code across MySQL and PostgreSQL.
9. Controlled local runs require pinned images, clean source, fresh volumes,
   excluded warmups, alternating order, and at least seven repetitions.
10. External runs are credential-redacted and cannot be confused with
    controlled local evidence.
11. Fail-closed analysis rejects incomplete or incomparable results.
12. Unit, integration, conformance, orchestration, formatting, and strict
    compilation checks pass.
13. A Docker smoke comparison passes with matching logical outcomes.
14. A larger local run produces a provenance-complete MySQL/PostgreSQL report
    with appropriately scoped conclusions.

## Implementation constraints

- Rust 1.81 remains the minimum supported compiler.
- No production dependency or public Prolly API is added solely for benchmarks.
- Adapter changes use `sqlx` and do not require database-specific native client
  libraries.
- Randomness is deterministic, seedable, and recorded.
- Timed code does not include validation or diagnostic collection.
- Benchmark output defaults outside tracked source unless intentionally
  promoted.
- Existing untracked and unrelated worktree content is preserved.
