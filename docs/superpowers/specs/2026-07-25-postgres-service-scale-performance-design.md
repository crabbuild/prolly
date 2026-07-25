# Improve PostgreSQL service and large-tree performance

## Goal

Make the Rust PostgreSQL adapter and its performance harness suitable for
evaluating Prolly as the storage engine for a large, multi-tenant version
control service.

The primary benchmark measures concurrent service throughput, tail latency,
connection-pool saturation, and root contention. The existing serial
large-tree suite remains available and becomes configurable for users who want
to optimize single-client tree operations. The implementation also removes the
adapter's known per-node SQL round trips and table-wide root lock without
changing its schema or existing public entry points.

The recorded results describe the measured environment. They are regression
evidence, not universal production-capacity claims.

## Scope

This work covers:

- the Rust `prolly-store-postgres` adapter;
- the existing `benchmarks/postgres-scale` crate and repository runner;
- a checked-in TOML workload specification;
- concurrent version-control workloads and the existing large-tree workloads;
- raw results, summaries, provenance, correctness validation, resume support,
  and configurable regression gates; and
- focused unit, integration, and Docker-backed smoke tests.

This work does not:

- optimize or port the language-specific PostgreSQL adapters;
- introduce a PostgreSQL schema migration;
- add distributed load generators;
- claim performance for an unmeasured deployment topology; or
- change Prolly's content-addressing, tree format, or merge semantics.

The optimized SQL and locking contract will be documented so other language
adapters can adopt it later.

## Selected architecture

Keep `benchmarks/postgres-scale` as one non-published Rust executable with two
independently selectable suites:

1. `service` runs concurrent version-control operations across independent and
   contended named roots. It is the default suite and the primary performance
   regression gate.
2. `scale` runs the current serial build, mutation, read, scan, diff, and merge
   matrix. It accepts configurable record counts, value sizes, change counts,
   samples, repetitions, operations, and access patterns.

Both suites share deterministic fixture generation, PostgreSQL setup and
restoration, configuration resolution, measurement primitives, validation,
provenance capture, durable output, and reporting. They use separate raw row
types and result files so a concurrent duration-based sample is never
aggregated with a serial operation sample.

The repository runner continues to own the dedicated PostgreSQL Docker Compose
project. The executable also accepts an external PostgreSQL URL. The runner
records which topology was used.

## Configuration

The canonical workload specification is
`benchmarks/postgres-scale/workloads/default.toml`. It has a versioned schema
and contains both suites. The checked-in default enables `service` and disables
`scale`.

The service section includes:

- base record count and deterministic value size;
- client concurrency levels and PostgreSQL pool sizes;
- tenant count, hot-root count, and hot-root traffic share;
- warmup and measurement durations;
- operation weights;
- point-read, multi-read, commit, diff, and merge cardinalities;
- retained version count;
- compare-and-swap (CAS) retry limit;
- adapter SQL batch size; and
- random seed.

The full default service matrix uses client concurrency levels `1, 8, 32, 64`
and pool sizes `8, 32`. It runs their Cartesian product. It uses 64 tenants,
one hot root, and a 20% hot-root traffic share. The initial operation mix is:

- 45% point reads;
- 15% multi-key reads;
- 25% commits;
- 10% diffs; and
- 5% merges.

The initial base has 1,000,000 records with 256-byte values. Multi-key reads use
32 keys, commits change 16 keys, the warmup is 15 seconds, and measurement is
60 seconds. These are workload defaults, not hard-coded limits.

The scale section preserves the existing 1,000,000 and 10,000,000 record
defaults and current fixed-width value size for baseline comparability. It can
be enabled alone for large-tree work.

The executable accepts `--config`, `--suite`, `--output`, `--url`, and
well-defined overrides for common service and scale fields. Existing scale CLI
flags remain accepted. Every override is reflected in the resolved
configuration.

Configuration validation rejects:

- unknown schema versions or fields;
- empty matrices or zero-valued counts and durations;
- operation weights that do not total 100;
- a hot-root share outside `0.0..=1.0`;
- more hot roots than tenant roots;
- merge cardinalities that cannot be split into disjoint branches;
- a zero adapter batch size;
- invalid percentile or regression budgets; and
- configurations whose estimated fixture size exceeds a configured disk guard.

Each run stores the original TOML, canonical resolved TOML, and a SHA-256
configuration hash. Resume is allowed only when the schema version,
configuration hash, source revision, dirty state, and database-layout version
match.

## Service workload

### Fixture and roots

Build one deterministic base tree through the public async Prolly API. All
tenant roots initially reference that immutable tree, allowing realistic
cross-tenant content sharing. Each tenant has a main root and prepared left and
right branch roots. The harness retains a bounded version history for reads,
diffs, and merges.

Before every measured concurrency/pool cell, restore the same validated
PostgreSQL snapshot and reconstruct the in-memory version catalog. This makes
cells independent and comparable. Fixture creation, snapshot restoration,
`ANALYZE`, statistics reset, trace construction, warmup, and final validation
are outside the measurement interval.

### Trace and scheduling

Precompute a deterministic logical-operation trace from the configured seed.
The trace fixes operation type, tenant class, root, keys, and mutation values.
Workers claim trace entries by sequence number. Thread scheduling and
transaction interleaving remain naturally nondeterministic and are identified
as such in the report.

Each cell is a closed-loop load test. Every client starts its next operation
after the previous operation completes. This produces a saturation curve across
the configured concurrency levels without pretending to model an external
arrival process. Latency starts before connection acquisition, so pool wait is
included. At the measurement deadline, workers finish their current operation
and stop.

Warmup uses the same operation mix but does not contribute samples or mutate
the measured fixture. The harness restores the cell snapshot between warmup
and measurement.

### Tenant and contention selection

The trace sends `hot_root_share` of operations to the configured hot-root set.
All remaining operations are distributed uniformly across independent tenant
roots. The report separates hot-root and independent-root measurements.

This design measures:

- ordinary multi-tenant scale;
- connection-pool queuing;
- deliberate contention on version-control heads;
- CAS conflicts and retry amplification; and
- whether unrelated roots make progress independently.

### Operations

Point read opens the selected current head and reads one deterministic key.
Multi-read opens the head and calls the public ordered multi-key lookup API.
Hits, misses, values, and ordering are checked.

Commit opens the selected head, applies the configured deterministic mutations,
persists the new tree, and compare-and-swap publishes the head. On conflict, it
reopens the current head and retries up to the configured limit. The logical
operation latency includes all attempts. The result records attempts,
conflicts, retries, and whether the commit eventually succeeded. Nodes written
by a losing content-addressed attempt are valid but unreachable and are counted
as write amplification.

Diff chooses two retained versions from one tenant, executes the public diff
API, consumes all returned differences, and verifies their count and sampled
contents.

Merge selects a retained base and prepared left and right descendants for one
tenant, executes the public three-way merge, and CAS publishes the configured
target root. Branch changes are deterministic and disjoint unless an explicit
conflict profile is selected. The default workload has no semantic merge
conflicts; root publication conflicts remain possible and are measured.

### Service measurements

For every suite, concurrency, pool-size, tenant class, and operation tuple,
record:

- logical operations attempted and completed;
- successful operations per second and attempted operations per second;
- p50, p95, p99, p99.9, and maximum end-to-end latency;
- sample count and histogram range;
- CAS attempts, conflicts, retries, and exhausted retries;
- semantic merge conflicts;
- timeouts, SQL errors, validation errors, and worker panics;
- Prolly node, byte, batch, and cache counters;
- PostgreSQL statement, block, temporary I/O, write-ahead log (WAL), commit, and rollback
  counters; and
- database and table/index size before and after the cell.

Percentiles come from a high dynamic range (HDR) histogram with a range wide
enough for the configured operation timeout. A percentile cannot be used as a
regression gate unless the cell meets its configured minimum sample count. No
percentile is described as statistically stable solely because the harness can
calculate it.

## Scale workload

The scale suite retains the existing operation definitions and isolation:

- initial sorted build;
- append, random, and clustered point and batch mutations;
- cold-manager and warm-manager point reads;
- ordered and random multi-key reads;
- bounded and full scans;
- structural diffs; and
- three-way merges.

Users can configure record and value sizes, mutation counts, read samples,
patterns, operations, and repetitions in TOML or through compatible CLI
overrides. Input generation remains deterministic and outside timed regions.
Each cell restores the same base snapshot and validates exact counts, ordering,
checksums, and sampled values.

Service and scale fixtures use the same value generator, which produces exactly
the requested deterministic byte length. The existing scale defaults preserve
the old 27-byte values to keep historical results interpretable.

## PostgreSQL adapter hardening

### Options and compatibility

Add `PostgresBackendOptions` with `max_batch_items`, defaulting to 1,024.
`PostgresBackend::connect` and `PostgresBackend::new` retain their current
signatures and defaults. New constructors accept explicit options. Callers can
still provide and own a configured `PgPool`.

The table names, columns, keys, and stored bytes do not change.

### Set-based node reads

Implement ordered batch reads with a byte-array parameter and
`UNNEST ... WITH ORDINALITY`, left joining the requested content identifiers
(CIDs) to
`prolly_nodes`. Each SQL chunk returns one row per input position, preserving
missing entries, duplicate keys, and input order.

Empty input returns immediately. Inputs larger than `max_batch_items` are split
into ordered chunks and concatenated without changing the observable result.

### Set-based node writes

Implement node upserts with paired byte-array parameters and `UNNEST`. Each
chunk executes one set-based `INSERT ... ON CONFLICT DO UPDATE`, preserving the
current overwrite semantics.

Mixed node batches first reduce repeated CIDs to their final operation in
memory. Inside one transaction, each chunk performs set-based deletes and
set-based upserts. This preserves the final state produced by executing the
original operations in order.

Node publication with hints reuses the bulk node path and writes the hint in
the same transaction. Strict transaction commits reuse the same bulk helpers.
An error in any chunk rolls back the complete public operation.

### Root locking and transactions

Replace `LOCK TABLE prolly_roots IN SHARE ROW EXCLUSIVE MODE` with
transaction-scoped PostgreSQL advisory locks. Acquire each lock with
`pg_advisory_xact_lock(hashtextextended('prolly-root-v1:' || encode($1, 'hex'), 0))`.
Hash collisions can serialize unrelated roots but cannot violate correctness.

Every root mutation path participates in the protocol:

- compare-and-swap root publication;
- unconditional root put and delete; and
- strict multi-root transactions.

Multi-root operations sort and deduplicate root names before acquiring locks.
This prevents lock-order deadlocks. After locking, strict transactions read all
root conditions set-wise, compare exact optional manifests, and apply root
writes set-wise. A conflict rolls back node and root writes and returns the
existing public conflict result.

This protocol serializes writers to one named root while allowing unrelated
roots to progress concurrently. It requires no new table and is documented for
future non-Rust adapter parity.

## Output and reporting

A run directory contains:

- `workload.toml` and `resolved-workload.toml`;
- `run-manifest.txt` with the configuration hash;
- `service-raw.csv` and `service-summary.csv`;
- `scale-raw.csv` and `scale-summary.csv` when scale is enabled;
- `report.md`;
- `machine.txt`, `postgres.txt`, and `dependencies.txt`;
- `build.log`, `run.log`, and the release-binary SHA-256; and
- a durable failure record when a run stops early.

Legacy scale-only invocations also emit `raw-results.csv` and `summary.csv` as
copies of the scale files. Existing scripts and historical comparison tools
continue to work.

Raw rows are flushed after each validated cell. A partially measured
duration-based cell is never marked complete and is rerun in full on resume.
Reports identify resumed runs and preserve raw min/max values rather than
hiding variance.

The report leads with the service saturation matrix, showing throughput, p99,
conflicts, errors, and PostgreSQL calls at each concurrency/pool pair. It then
shows per-operation tail latency, hot-root contention, adapter round trips and
write amplification, followed by the serial scale tables when enabled.

The report states:

- host, container, network, and PostgreSQL topology;
- PostgreSQL and SQLx versions and relevant settings;
- pool and adapter batch sizes;
- fixture and value sizes;
- warmup, duration, clients, tenants, and operation mix;
- controlled and uncontrolled cache state;
- sample-count limitations; and
- any environment or resume anomaly.

## Regression gates

An optional baseline path enables strict comparison. Cells match on suite,
operation, tenant class, concurrency, pool size, fixture dimensions, and all
workload fields that affect behavior.

Configurable budgets include:

- maximum successful-throughput loss;
- maximum p99 latency increase;
- maximum CAS conflict and exhausted-retry rates;
- maximum unexpected error rate;
- maximum PostgreSQL statements per logical operation; and
- minimum samples for percentile gates.

Correctness errors, worker panics, malformed rows, non-finite metrics,
incomplete required cells, duplicate cells, and configuration mismatches always
fail. Missing baseline cells fail in strict mode.

Machine, PostgreSQL, Docker, and material setting mismatches fail by default.
An explicit exploratory override permits comparison but marks the report
non-gating. Revision differences are expected in a regression comparison and
are recorded rather than rejected.

The checked-in smoke configuration uses correctness and SQL-round-trip gates.
Machine-specific throughput and latency budgets belong in a recorded baseline,
not in source as universal constants.

## Failure semantics

The harness applies an operation timeout to every service request. SQL errors,
timeouts, join failures, malformed configuration, disk-guard failures,
validation failures, and incompatible resume data stop the run after flushing
diagnostics and validated rows. Expected root CAS conflicts and configured
semantic merge conflicts are measurements, not harness errors.

A worker panic is converted into a durable cell failure and a nonzero process
exit. Other workers are cancelled and joined. The harness does not continue
with a partially trustworthy cell.

Database restoration and statistics collection failures identify the affected
cell and stop before timing another cell.

## Test strategy

Unit tests cover:

- TOML defaults, explicit values, CLI overrides, and invalid configurations;
- canonical configuration serialization and hash stability;
- deterministic operation traces and exact operation weights;
- tenant and hot-root selection;
- deterministic values at requested lengths;
- service matrix enumeration and unique resume keys;
- HDR histogram summaries and minimum-sample rules;
- raw CSV escaping and strict aggregation;
- regression matching and every budget boundary; and
- failure and resume classification.

Docker-backed adapter integration tests cover:

- empty, missing, duplicate, ordered, and multi-chunk batch reads;
- duplicate and multi-chunk upserts;
- mixed repeated upsert/delete ordering;
- atomic node publication with hints;
- rollback after a forced chunk failure;
- strict transaction rollback on root conflict;
- missing-root and existing-root CAS;
- exactly one winner for concurrent same-root CAS;
- independent-root progress while another root lock is held; and
- deadlock-free multi-root transactions with reversed input order.

The benchmark smoke test:

- uses a small deterministic fixture;
- exercises every service and scale operation;
- includes independent and hot roots;
- runs at more clients than pool connections;
- validates all persisted raw and summary rows;
- resumes without repeating completed cells; and
- compares the result with a compatible smoke baseline.

## Performance verification

Capture adapter-focused before-and-after measurements for:

- ordered and random node batches;
- bulk node publication;
- independent-root CAS throughput; and
- same-root CAS conflict behavior.

Then run the service concurrency sweep and a representative scale subset.
Acceptance requires:

- all correctness and integration tests passing;
- fewer PostgreSQL calls for multi-node read and write batches;
- independent root writers no longer blocked by a table-wide lock;
- no unexpected SQL, timeout, panic, or validation errors;
- atomic conflict and rollback behavior unchanged; and
- no violation of the configured environment-specific throughput or
  tail-latency budgets.

All reported improvements include the command, resolved configuration,
revision, environment metadata, raw results, and comparison method.
