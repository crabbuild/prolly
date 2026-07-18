# Current Dolt Go vs Rust Prolly Benchmark Design

**Status:** Approved for implementation

**Date:** 2026-07-17

## Objective

Re-run the native Dolt Go versus Rust prolly comparison after the Rust
canonical-streaming and zero-copy improvements. The authoritative comparison
uses Dolt's current `main` at the start of the run, resolved once to an exact
commit SHA. It preserves the July 16 workload contract so the new matrix can be
compared directly with the checked-in zero-copy baseline.

This work changes benchmark and reporting infrastructure only. It does not
modify either prolly-tree implementation.

## Compared Products

The benchmark measures each implementation through its normal public product
path and default persisted tree format:

- Rust uses the release-built `prolly_compare` binary, `MemStore`, the default
  `Config`, canonical bulk and append mutation APIs, `ReadSession::get_with`,
  and callback-scoped `scan_range`.
- Dolt Go uses a release-built native command inside a clean Dolt checkout,
  Dolt's in-memory `TestStorage`/`NodeStore`, default tuple encoding and prolly
  chunking, `MutableMap` publication, callback point reads, and native ordered
  iteration.

The comparison does not force a common wire format or chunking policy. It is an
end-to-end product-path comparison under one logical workload, not a language
microbenchmark.

## Reproducible Dolt Source

The repository will contain the complete Dolt benchmark command source instead
of depending on an untracked patched checkout. The driver will:

1. fetch `https://github.com/dolthub/dolt.git` into a temporary or configured
   cache;
2. resolve `origin/main` once, unless `DOLT_REV` explicitly supplies a commit;
3. detach the checkout at that SHA;
4. copy the checked-in command into `go/cmd/prolly-compare`;
5. test and build the command inside Dolt's Go module; and
6. record the resolved SHA, runner source hash, and final binary SHA-256.

Resolving current `main` once prevents different scenarios in the same matrix
from using different Dolt revisions. A recorded SHA can reproduce the exact run
later through `DOLT_REV`.

## Workload Contract

### Dataset sizes

Every phase and workload runs at these base record counts:

- 10,000
- 50,000
- 1,000,000
- 5,000,000
- 10,000,000

All sizes receive three complete process-isolated repetitions.

### Logical records

- Keys are fixed-width, zero-padded UTF-8 strings derived from an integer
  position. Lexicographic and numeric order are identical.
- Values are deterministic pseudo-random payloads from 1 through 100 bytes.
- The shared generator, permutation, seeds, generation numbers, and FNV-1a
  operation digest remain byte-for-byte compatible with the July 16 runners.
- Logical fixture and Dolt tuple construction occur outside timed write
  regions.

### Fresh phase

Each fresh scenario contains the same final records and differs only in arrival
order:

- `append`: ascending keys;
- `random`: one deterministic uniform permutation; and
- `clustered`: deterministic permutation of 1,000-key clusters while retaining
  ascending order inside each cluster.

All records are submitted through one native bulk write. Sorting, mutation
buffering, chunking, hashing, node encoding, and in-memory node-store writes are
timed.

### Mutation phase

Each mutation scenario first creates an ascending base tree outside the timed
mutation interval. The measured write count is exactly 30% of the base size:

- `append`: all mutations insert keys above the current maximum;
- `random`: alternating deterministic random updates and inserts, with an exact
  50/50 split; and
- `clustered`: alternating updates and inserts inside one centered contiguous
  region, with an exact 50/50 split.

Rust uses its public append API for append mutation and its canonical batch API
for random and clustered mutation. Dolt uses the corresponding native mutable
map write and publication path.

## Measured Operations

Every scenario process emits three normalized rows:

1. `write`: fresh bulk build or 30% mutation publication;
2. `point_read`: at most 100,000 deterministic existing-key reads after one
   untimed warm validation pass; and
3. `range_scan`: one complete ordered traversal of the resulting tree.

The timed Rust read path is callback-scoped zero-copy through one retained
`ReadSession`. The timed Dolt read path uses its callback/tuple access without
copying values into an additional benchmark-owned result. Range scans perform a
cheap byte count so traversal cannot be optimized away.

## Isolation and Ordering

- Rust and Go runners execute in different processes.
- `RAYON_NUM_THREADS=1` and `GOMAXPROCS=1` enforce one worker.
- Binaries are release-built once before measurement and copied into the result
  directory.
- Scenario order alternates Rust-first and Go-first by size, phase, workload,
  and repetition.
- Scenarios run sequentially; no Rust and Go measurement overlaps.
- The first execution of each runner is an untimed smoke/parity scenario so
  compilation, dynamic loading, and one-time initialization are not charged to
  a matrix row.

## Correctness Gates

A process must validate its result before emitting `validated=true`:

- final cardinality equals the contract;
- every measured point-read target exists with the exact expected value;
- full traversal is strictly ordered and has the expected cardinality;
- random and clustered mutation positions are unique and preserve the required
  insert/update mix; and
- workload digest matches the shared logical operation stream.

The summarizer rejects the complete result set if a paired Rust/Go row differs
in workload digest, operation count, result count, or validation state. A
failed, timed-out, or missing process remains a reported failure and is never
replaced by an estimate.

## Measurements and Provenance

Normalized output records:

- elapsed nanoseconds;
- nanoseconds per logical operation;
- operations per second;
- process peak resident set size;
- record count, phase, workload, operation, repetition, and implementation;
- logical workload digest and final result count; and
- validation status.

The run manifest records:

- exact Rust commit plus source-tree content hash;
- exact Dolt commit plus checked-in runner content hash;
- SHA-256 of both copied executables;
- Rust and Go toolchain versions;
- CPU, memory, operating system, and timestamp;
- sizes, repetitions, thread limits, mutation ratio, and point-read cap; and
- per-process stdout, stderr, timing output, exit status, and peak RSS.

## Reporting

The summarizer produces:

- raw normalized CSV;
- a median summary for every size/phase/workload/operation group;
- winner and speedup without rounding before winner selection;
- observed minimum/maximum and coefficient of variation;
- peak-RSS medians and maxima by scenario;
- operation-level win counts; and
- an explicit limitations section.

A second report compares the new Rust medians with
`performance-results/zero-copy-final-rerun-2026-07-16/summary.csv`. Historical
deltas are emitted only when size, phase, workload, operation, and workload
contract version match. The current Dolt-vs-Rust result remains separate from
the historical Rust-before/Rust-after view.

The default output directory is
`performance-results/dolt-current-rust-canonical-2026-07-17/`. Copied binaries
and large raw timing artifacts remain local unless explicitly selected for
commit. Normalized results, provenance, and generated reports are suitable for
version control.

## Approaches Considered

### Checked-in native runners with a pinned Dolt checkout — selected

This preserves native product paths, process isolation, and exact workload
parity while making the Dolt command reproducible. It also retains direct
comparability with the July 16 matrix.

### Manually patched external Dolt checkout — rejected

The previous driver depended on `dolt/go/cmd/prolly-compare/main.go` existing in
an untracked checkout. The source cannot be audited or reproduced from this
repository, and a clean workspace cannot run the comparison.

### One orchestrator linked to both engines — rejected

Linking through FFI or running both runtimes in one process would add adapter
overhead, share allocator/cache state, and make peak memory attribution
ambiguous. It would no longer measure either native product path cleanly.

## Acceptance Criteria

Implementation is complete when:

1. Rust and Dolt workload contract tests share the same golden digest and
   mutation fixtures.
2. A 10,000-record smoke matrix completes with exact paired parity and generated
   reports.
3. The complete 10K, 50K, 1M, 5M, and 10M matrix runs three times per scenario
   against one recorded Dolt current-main SHA.
4. All 180 scenario processes succeed, producing 540 validated operation rows
   and 270 exact Rust/Go pairs.
5. Peak RSS and timing provenance are retained for every process.
6. The current comparison and July 16 historical delta reports disclose every
   regression and do not infer or interpolate missing data.
7. Benchmark-only changes pass formatting, tests, static checks, and shell/Python
   validation without changing production tree behavior.
