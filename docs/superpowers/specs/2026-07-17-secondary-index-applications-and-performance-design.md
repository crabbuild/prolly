# Secondary Index Applications and Performance Design

**Date:** 2026-07-17

## Objective

Make the Rust `IndexedMap` secondary-index API easier to apply in a real
multi-tenant SaaS system and establish a repeatable, evidence-based view of its
latency, throughput, amplification, configured resource limits, and practical
capacity on a specific machine.

The deliverable consists of production-shaped runnable examples, a configurable
benchmark harness, deterministic boundary tests, raw benchmark data, and a
checked-in performance report. It documents and exercises shipped behavior; it
does not add secondary-index semantics or change persisted formats.

## Selected approach

Keep the existing `examples/secondary_index.rs` as the compact byte-oriented
lifecycle tour. Add two focused serde/JSON application examples and evolve the
existing one-shot benchmark into an isolated repeated-sample harness. A small
runner and standard-library-only summarizer will capture reproducible results
without adding a statistics dependency to the crate.

This is preferred to expanding the existing example because a single file would
mix too many application concepts with lifecycle operations. It is preferred to
adopting Criterion because this work emphasizes end-to-end transactional builds,
writes, queries, amplification, and bounded failure behavior rather than only
hot-loop microbenchmarks.

## Application examples

### Multi-tenant user directory

The user-directory example uses a typed serde model with a stable user ID as the
primary key and fields for tenant, email, account status, display name, tags, and
creation time. It demonstrates:

- a basic non-unique status index;
- sparse, multi-valued tag extraction;
- a canonical tenant/status composite term;
- an email-domain term derived from stored data;
- an ordered creation-time term;
- an `Include` projection for an index-only directory summary;
- exact, range, record-join, projected, and paginated reads;
- creating indexes after source population; and
- atomic indexed updates followed by semantic verification.

### Multi-tenant task queue

The task-queue example uses a typed task model with tenant, state, priority,
assignee, due time, title, and labels. It demonstrates:

- a canonical tenant/state/priority queue term;
- a sparse assignee index;
- multi-valued label extraction;
- an ordered due-time range index;
- a compact `Include` projection for a dashboard row;
- moving a task between queue terms through one indexed update;
- tenant-scoped pagination and snapshot-stable reads; and
- the difference between index-only projections and source-record joins.

Both examples return malformed JSON as `SecondaryIndexError`. Extractors are
deterministic, side-effect free, retry safe, and panic free. Composite terms use
the crate's canonical segment encoding. Numerically ordered fields use
order-preserving big-endian bytes rather than formatted decimal strings.

## Benchmark architecture

The benchmark remains a Cargo bench target with a custom harness. It accepts a
profile and scenario filters through environment variables. The `report`
profile defaults `PROLLY_INDEX_BENCH_BUDGET_SECS` to 600 and stops scheduling
optional scenarios after 540 seconds, reserving the final minute for output and
summary generation:

- `smoke` runs small fixtures and few samples for local correctness checks;
- `report` targets roughly ten minutes after compilation and emits the complete
  supported matrix; and
- focused modes run one scenario family while developing or investigating.

Each measured scenario creates a fresh in-memory engine and fixture. Fixture
generation, warmup, validation, and output formatting remain outside the timed
region. A scenario warms up before recording repeated `Instant` samples. Every
result is validated before emission; incorrect cardinality, an unexpected
error, or partial publication fails the benchmark.

Raw output is stable CSV. Each row identifies the scenario, scale, relevant
shape parameters, iteration, elapsed time, work count, throughput, and
verification state. A standard-library-only summarizer groups samples and
reports count, minimum, p50, p95, p99, maximum, mean, and operations per second.

## Performance matrix

The report profile measures repeated samples at 1,000, 10,000, and 100,000
records. If the elapsed-time budget permits, it performs one lighter
1,000,000-record capacity probe. The 1,000,000-record probe is optional; the
smaller scales and all configured-limit probes are required. The runner stops
adding optional probes before the budget rather than discarding completed valid
results.

### Build and activation

- source-map population;
- `KeysOnly` index activation;
- `Include` index activation; and
- `All` index activation.

Projection modes are measured independently so their costs are attributable.

### Writes

- plain and indexed single-record writes;
- batches of 10, 100, and 1,000 records;
- term-changing updates; and
- projection-only updates where `KeysOnly` emissions remain stable but
  `Include` and `All` values change.

The report includes indexed/plain latency and throughput ratios.

### Reads

- exact queries with low, medium, and high match cardinality;
- tenant-scoped canonical composite queries;
- ordered range scans;
- one-page forward reads;
- `records` queries that batch-join the source snapshot; and
- `projected` queries that remain index-only.

### Shape and practical capacity

- 1, 4, 8, 16, and 32 active indexes;
- fan-out of 1, 8, 64, and 256 terms per source record; and
- the largest completed scale in the report profile.

The report describes empirical capacity only as “largest tested successfully on
this machine.” It must not claim a universal maximum or extrapolate beyond
observed data.

### Amplification

The harness records source, index, and catalog nodes written from
`IndexedMapMetricsSnapshot`, projected bytes, physical upserts/deletes, bundle
node count, and bundle byte count. These values make write and storage
amplification visible even when process memory measurements are unavailable or
platform specific.

## Configured resource-limit probes

Configured hard limits and empirical capacity are reported separately. Fast,
deterministic probes use deliberately small `SecondaryIndexLimits` values to
exercise the exact accepted boundary and the first rejected value for:

- term bytes;
- one projection's bytes;
- terms per record;
- projected bytes per record;
- derived mutations per transaction;
- projected bytes per transaction;
- active index count;
- temporary build-sort bytes;
- verification entry count;
- bundle node count; and
- bundle byte count.

`All` projection source-value limits are probed separately from application
projection limits. Each rejection must match `IndexResourceLimitExceeded` with
the expected resource, limit, and actual fields. Publication-sensitive probes
take a snapshot before the rejected operation and confirm that the source,
catalog, and index selection remain unchanged afterward.

The report also lists the crate's default configured values, read from the same
Rust definition used by the benchmark rather than duplicated in a script.

## Runner, provenance, and report

A repository script builds the release benchmark once, records provenance, runs
the selected profile, preserves raw CSV, and invokes the summarizer. The output
directory is `performance-results/secondary-index-YYYY-MM-DD/` and contains:

- a manifest with UTC timestamp, OS, architecture, CPU, memory, Rust and Cargo
  versions, git revision, dirty-state marker, profile, sample counts, scales,
  and exact commands;
- raw per-sample CSV;
- summarized CSV suitable for further analysis; and
- a Markdown report with methodology, default limits, environment, result
  tables, observed capacity, caveats, and reproduction commands.

Compilation time is reported separately and does not consume the benchmark's
approximately ten-minute measurement budget. The runner preserves valid partial
results and labels omitted optional capacity probes if the time budget is
reached.

## Documentation and discovery

The README and secondary-index documentation link to both new examples, explain
when each application pattern is useful, and show smoke and report reproduction
commands. The performance documentation distinguishes latency distributions,
throughput, logical amplification counters, configured safety limits, and
machine-specific tested capacity.

## Error handling

- Example encoding and decoding errors propagate through typed `Error` or
  `SecondaryIndexError` values.
- The harness fails a scenario on an unexpected error or invalid result rather
  than emitting a successful row.
- Expected limit rejections are data only when the complete error payload and
  unchanged publication state match the probe definition.
- The runner retains raw data on a later scenario failure and records the failed
  command in the manifest.
- Report generation rejects missing required CSV columns or unverified rows.

## Testing and verification

Automated tests cover statistics percentile selection, CSV parsing and grouping,
limit-boundary acceptance/rejection, and atomic non-publication after rejected
writes or builds. Application examples are assertion backed and must run to
completion.

Acceptance requires fresh successful runs of:

- both new application examples;
- focused benchmark/report tests and the existing secondary-index integration
  suite;
- Rust formatting and Clippy for touched Rust targets;
- the smoke benchmark profile; and
- the full report profile, completing within the intended budget or stopping
  cleanly at the budget with valid, explicitly partial output.

The checked-in report must be produced by the shipped runner, include raw data
and provenance, and avoid claims unsupported by those measurements.

## Scope boundaries

This work does not add unique indexes, asynchronous maintenance, full-text
search, fuzzy matching, distributed index placement, new persistence formats,
hard CI latency thresholds, or performance claims for stores other than the
measured in-memory configuration. It does not modify the unrelated language
binding work already present in the working tree.
