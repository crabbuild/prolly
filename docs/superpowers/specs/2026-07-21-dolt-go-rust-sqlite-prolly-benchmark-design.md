# Dolt Go vs Rust SQLite Prolly Benchmark Design

**Status:** Approved for implementation planning

**Date:** 2026-07-21

## Objective

Build a reproducible performance and correctness comparison between the Dolt
Go prolly tree and this repository's Rust prolly tree when both use SQLite as
their persistent content-addressed backend. The comparison mirrors the complete
operation matrix in `benchmarks/sqlite-scale` and retains that harness's
logical workload contract, fixture isolation, validation, and reporting rules.

This work adds benchmark infrastructure only. It does not add SQLite to Dolt as
a production backend, modify Dolt's prolly implementation, or force either
implementation to use the other's tree encoding or chunking policy.

## Selected Architecture

The checked-in Go benchmark at `benchmarks/dolt-prolly-sqlite-compare/` will
contain a benchmark-scoped SQLite implementation of Dolt's
`chunks.ChunkStore`. The comparison driver will copy that source into a pinned
Dolt checkout and build it inside Dolt's Go module. Dolt's standard
`tree.NewNodeStore` will wrap the SQLite chunk store, so the measured path uses
Dolt's real node serialization, hashing, caching, chunking, mutation, diff, and
merge code.

The SQLite adapter stores:

- content-addressed chunks in a table keyed by the complete Dolt chunk hash;
- chunk bytes as non-null blobs; and
- benchmark root metadata in a separate table so a map can be closed, cloned,
  reopened, and reconstructed from its root hash.

`Put` follows the `chunks.ChunkStore` contract by making pending chunks visible
to the current store before persistence. `Commit` validates referenced chunks
and atomically writes pending chunks and compare-and-swap root metadata. A
successful commit clears the pending set; a failed comparison or transaction
leaves the previously committed root visible. Duplicate content is idempotent.

The adapter uses one synchronous SQLite connection configured with WAL,
`synchronous=NORMAL`, a 5,000 ms busy timeout, and `temp_store=MEMORY`, matching
the Rust scale harness. Unsupported garbage-collection and ghost-chunk
operations return Dolt's unsupported-operation error rather than silently
pretending to succeed.

## Reproducible Dolt Source

The driver will use the same pinned-checkout pattern as the existing native
Dolt comparison:

1. fetch `https://github.com/dolthub/dolt.git` into a configured cache;
2. resolve `origin/main` once, unless `DOLT_REV` supplies an exact commit;
3. detach the checkout at the resolved SHA;
4. copy the checked-in SQLite comparison command into Dolt's Go module;
5. run its tests and build a trimmed release binary; and
6. record the Dolt SHA, runner source hash, dependency versions, and binary
   SHA-256.

The benchmark will use the SQLite driver already compatible with Dolt's module
and record the exact resolved driver version. A manually patched `dolt/`
checkout is not an input to the run.

## Shared Logical Workload

Both implementations retain the `sqlite-scale-v2` logical contract:

- keys are fixed-width, 24-byte ordered strings of the form
  `key-{id:020}`;
- values are deterministic 100-byte payloads containing the record identifier
  and generation;
- the random seed is `0x6a09e667f3bcc909`;
- default full runs use 1,000,000 base records, three repetitions, 10,000 read
  samples, and a 30% change count;
- patterns are append, deterministic random, and centered clustered; and
- merge interprets the change count as the total split evenly across two
  disjoint branches.

The workload generators in Go reproduce the Rust model's selected IDs, range
bounds, merge branch assignment, expected cardinality, and even-change
validation. Contract tests use golden vectors so drift in either generator is
detected before performance measurement.

## Fixture Isolation and Data Flow

For every size and repetition, each implementation builds its own SQLite base
fixture using its native persisted tree representation. The base is validated,
its root is published, the store is closed, and WAL is checkpointed. Every
operation cell receives a filesystem clone of that closed base fixture and a
fresh store and node manager.

Fixture construction, tuple preparation, fixture cloning, branch setup,
validation, statistics collection, publication, checkpointing, and reopen
checks are outside timed intervals. Lazy iterators are fully consumed inside
the timed interval. Each implementation runs in a separate process with one
language worker (`GOMAXPROCS=1` and `RAYON_NUM_THREADS=1`). Rust-first and
Go-first order alternates across cells and repetitions; measurements never
overlap.

After a mutating operation, the runner validates logical content and
cardinality, publishes the result root, closes the store, reopens the cloned
database, reconstructs the map, and validates the persisted result again.

## Measured Operations

The comparison covers the complete Rust SQLite scale matrix.

### Build

The fixture build uses each implementation's native sorted bulk construction.
Logical key/value generation and Dolt tuple encoding occur before the timer;
sorting, tree construction, chunking, hashing, node encoding, and SQLite chunk
writes occur inside it.

### Put and batch

Go uses `Map.Mutate`, native mutations, and `Flush`. A put changes one key. A
batch applies the same append, random-update, or clustered-update IDs and
generation values as Rust.

### Cold and warm point reads

Go uses `Map.Get`. Cold-manager reads purge the Dolt node cache before each
lookup. Warm-manager reads perform one untimed warmup pass and retain the
resulting cache state for the timed pass. The operating-system filesystem cache
is uncontrolled in both implementations and is disclosed as a limitation.

### Multi-key query

Rust uses its native map-level `get_many`. Dolt exposes no corresponding
logical map-level multi-get API, so the Go query cell performs repeated native
`Map.Get` calls over the identical key set. Reports explicitly identify the Go
query strategy as repeated point lookups and do not describe it as native bulk
query performance.

### Bounded and full scans

Go uses `IterKeyRange` for bounded scans and `IterAll` for full scans. Both are
fully consumed and validate exact order, keys, values, and row count inside the
timed traversal.

### Diff

Branch construction occurs outside the timer. Go times `prolly.DiffMaps`,
consumes all emitted diffs, and validates the exact changed-key and old/new
value sets.

### Merge

Left and right branches are constructed outside the timer from the shared
disjoint ID assignment. Go times `prolly.MergeMaps`, then validates every
branch value and the complete result cardinality. Appendix insert merges and
random or clustered update merges use the same branch generations as Rust.

## Measurements and Output Contract

Every successful cell records:

- implementation, exact revision, contract version, size, repetition,
  operation, pattern, and cache state;
- logical operation count, observed item count, elapsed nanoseconds,
  nanoseconds per operation, operations per second, and point-read percentiles
  where applicable;
- process peak resident set size;
- chunk reads and writes plus payload bytes read and written;
- final logical cardinality and validation status; and
- SQLite database, WAL, shared-memory, and combined fixture sizes.

Implementation-specific tree and cache statistics may be retained in raw
output. Only metrics with equivalent definitions participate in direct
cross-language comparisons. Missing implementation-specific metrics are
represented as unavailable, not as zero.

A common summarizer rejects duplicate or missing cells, failed validation,
different matrix definitions, different logical operation counts, different
expected or observed cardinalities, and incomplete repetition groups. It emits
raw normalized CSV, per-group medians and dispersion, winner and ratio, peak
RSS and SQLite-size comparisons, a Markdown report, and a limitations section.

## Correctness Gates

A cell may emit `validated=true` only after checking all applicable invariants:

- generated keys and values have the required bytes and deterministic content;
- selected IDs are unique, in range, and match the requested pattern;
- merge branches are disjoint and contain exactly the requested total changes;
- point and query reads return the exact expected value;
- scans return exact ordered content and cardinality;
- diffs contain the exact changed keys and values;
- merges contain both branches' expected values;
- the final map count matches the contract; and
- published results survive close and reopen with the same root hash and
  logical content.

A failed process, malformed row, missing peak-RSS measurement, or failed parity
check remains a reported failure. No row is inferred, interpolated, or replaced
by an estimate.

## Error Handling and Filesystem Safety

Errors identify the implementation, operation, pattern, size, repetition, and
fixture path. Raw stdout, stderr, process timing, and failed rows remain in the
result directory, and the overall driver exits nonzero.

The driver refuses to overwrite an existing completed comparison. Fixture
cleanup is restricted to validated generated paths beneath the selected output
directory. It rejects symlinks and paths outside the generated fixture or cell
roots. The user's existing checkout, untracked `dolt/` directory, and unrelated
performance results are never mutated or deleted.

## Testing Strategy

Development follows test-first red-green-refactor cycles.

### SQLite chunk-store tests

- put, get, has, get-many, has-many, and missing chunks;
- pending and committed visibility;
- content deduplication and hash preservation;
- referenced-chunk validation;
- successful and stale compare-and-swap commits;
- transaction rollback on persistence failure;
- root and chunk persistence across reopen;
- context cancellation and closed-store behavior; and
- explicit errors for unsupported operations.

### Workload and CLI tests

- golden key/value bytes and sizes;
- random, append, clustered, range, mutation, and merge ID vectors;
- expected cardinalities and logical operation counts;
- invalid sizes, zero counts, odd merge counts, unknown operations, and unknown
  patterns; and
- stable CSV schema and unavailable-metric encoding.

### Integration and driver tests

- complete small matrix against real SQLite;
- fixture clone, checkpoint, close, reopen, and root reconstruction;
- validation failures for intentionally wrong values or cardinalities;
- duplicate, missing, mismatched, and unvalidated summarizer inputs;
- safe refusal to overwrite output or clean unsafe paths; and
- a one-repetition cross-language smoke run with exact matrix parity.

Final verification includes Go formatting and tests inside the pinned Dolt
module, Rust SQLite scale tests, summarizer and shell-driver tests, release
builds, and the cross-language smoke matrix.

## Alternatives Considered

### Benchmark-scoped SQLite `chunks.ChunkStore` — selected

This preserves Dolt's standard node-store and prolly-tree product path while
making the adapter and pinned revision reproducible from this repository. It
also provides the persistence semantics needed for fixture cloning and reopen
validation without creating a maintained Dolt fork.

### Direct SQLite `tree.NodeStore` — rejected

A direct node store would duplicate or bypass Dolt's standard serialization,
caching, value-store, and chunk-reference behavior. It would be smaller but a
less representative Dolt measurement.

### Logical key/value rows in SQLite — rejected

Storing benchmark records directly in SQLite would bypass the prolly tree. It
would compare SQLite table access rather than the two prolly implementations.

## Acceptance Criteria

Implementation is complete when:

1. The benchmark-scoped SQLite store satisfies the Dolt `chunks.ChunkStore`
   behavior needed by the real prolly node store and passes persistence tests.
2. Go golden workload tests match the Rust `sqlite-scale-v2` logical contract.
3. The Go runner covers build, put, batch, cold/warm get, query, bounded/full
   scan, diff, and merge for append, random, and clustered patterns where
   applicable.
4. Every measured cell validates content, cardinality, root publication, and
   reopen behavior before success.
5. The driver pins and records Dolt, tests and builds both runners, alternates
   process order, captures peak RSS and provenance, and refuses incomplete
   results.
6. A real-SQLite smoke matrix completes with equal matrix definitions, logical
   operation counts, expected cardinalities, and validated outcomes.
7. The report clearly distinguishes persisted formats and the Dolt repeated-get
   query strategy, and compares only metrics with equivalent definitions.
8. Formatting, unit tests, integration tests, script tests, release builds, and
   the cross-language smoke run all pass without modifying either production
   prolly implementation.
