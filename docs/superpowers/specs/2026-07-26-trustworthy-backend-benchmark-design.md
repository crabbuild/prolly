# How to produce trustworthy PostgreSQL and DynamoDB benchmark results

## Status

Approved in conversation on 2026-07-26. This document records the exact contract for implementation review before code changes begin.

## Content plan

- **Content type**: Conceptual design specification
- **Audience**: Prolly maintainers and reviewers of backend performance claims
- **Goal**: Define a reproducible benchmark that can support PostgreSQL and DynamoDB Local performance claims at 10 million records
- **Plan**: Specify workload identity, timed regions, validation, provenance, statistics, tests, and rollout
- **Open questions**: None

## Why the current 10M result must be replaced

The current comparison cannot support a trustworthy winner claim. Its raw elapsed-time arithmetic is reproducible, but the two adapters do not run an equivalent experiment.

The audit found these defects:

- The report labels elapsed milliseconds as operations per second
- DynamoDB Local includes post-operation statistics collection in some timed regions while PostgreSQL excludes it
- The adapters generate different value bytes and select keys differently
- The merge workloads assign changes to branches differently
- Query order differs between adapters
- Build has one observation, so the report cannot quantify build variance
- The saved PostgreSQL output combines invocations from a 14-minute interval
- The manifest records a dirty source tree and does not prove a fresh output directory
- DynamoDB Local uses an unpinned `latest` image
- Validation checks differ between adapters

The implementation must remove these causes before publishing another 10M comparison. The old result directory must be removed or marked as superseded so readers cannot treat it as current evidence.

## Goals

1. Run byte-identical logical workloads against PostgreSQL and DynamoDB Local.
2. Time equivalent public Prolly operations and exclude setup, validation, and diagnostics.
3. Validate complete logical outcomes after every measured operation.
4. Record enough provenance to reproduce the source, binaries, services, workload, and analysis.
5. Quantify variance with repeated measurements and deterministic confidence intervals.
6. Reject comparisons that contain mismatched, incomplete, dirty, resumed, or stale data.
7. Run a clean 10M comparison and publish corrected units, statistics, and limitations.

## Non-goals

- Claiming that DynamoDB Local predicts Amazon DynamoDB latency, cost, throttling, or network behavior
- Comparing production service configurations
- Hiding adapter-specific setup costs inside operation timings
- Changing public Prolly semantics to improve a benchmark
- Treating a performance match as a correctness proof beyond the validated workload

## One shared workload contract

A new Rust library crate under `benchmarks/` owns workload generation for both adapters. Both benchmark crates use it through a path dependency. Adapter code cannot implement a private generator for a compared operation.

The workload specification contains:

- Contract version
- Record count
- Value size
- Mutation count
- Query sample count
- Concurrency
- Seed
- Operation name
- Repetition number

The library deterministically derives:

- Fixed-width keys
- Base values
- Updated values
- Build entries
- Batch mutations
- Sequential and concurrent query identifiers
- Diff mutations
- Left and right merge branches
- Expected final key/value maps
- Expected diff records
- Workload and outcome digests

Keys, values, mutation order, query order, branch assignment, and conflict resolution must match byte for byte. Merge branches use deterministic interleaving across the selected identifiers rather than backend-specific partitions.

### Contract versioning and golden vectors

The contract version changes when any input derivation, ordering rule, value encoding, digest, or merge policy changes. Golden-vector tests pin representative keys, values, selections, branches, and digests for fixed small specifications.

Each raw row records `contract_version`, `workload_digest`, and `outcome_digest`. The summarizer rejects a comparison when either digest differs between backends or repetitions.

## Equivalent timed regions

Each timer encloses one public Prolly operation. Input generation, store reset, fixture construction, result validation, diagnostics, and statistics collection remain outside the timer.

The timed operations are:

- `build`: publish the complete base mutation set with the public batch operation
- `batch`: apply the configured mutation set to the base root
- `query`: execute the ordered multi-key request
- `concurrent_query`: execute the same point-read set with configured bounded concurrency
- `diff`: create and completely consume the logical diff
- `merge`: execute the complete three-way merge and materialize its root

The harness records elapsed nanoseconds and a logical operation count. Throughput is derived as `logical_operations / elapsed_seconds`. Reports keep total latency and throughput in separate, correctly labeled columns.

A shared measurement helper returns the operation result and timing evidence before validation runs. Tests verify the call order so future validation or diagnostics cannot enter the measured region accidentally.

## Complete correctness validation

Every operation validates complete results after timing:

- `build`: root content count and ordered content digest match the expected base map
- `batch`: root content count and ordered content digest match the expected mutated map
- `query`: every returned key and value matches the requested order and expected bytes
- `concurrent_query`: every requested key appears once with the expected value
- `diff`: ordered change type, key, old value, new value, count, and digest match the expected diff
- `merge`: conflict count, root count, every merged key/value pair, unaffected records, and ordered content digest match the expected map

The two backends must produce the same logical content digest and canonical Prolly root for each equivalent outcome. A mismatch stops the run and prevents summary generation.

Adapter-specific store statistics may be collected after validation for diagnostics. They cannot determine correctness and cannot affect elapsed time.

## Fresh and attributable runs

The comparison entry point refuses to run unless:

- `HEAD` is a commit
- The tracked worktree is clean
- The output directory does not exist
- Required container images use pinned references
- The requested repetitions and workload dimensions are valid

The entry point creates a unique run identifier and never resumes a comparison directory. A failed run remains available for diagnosis but cannot be summarized as publishable evidence.

The manifest records:

- Git commit and tree hash
- Contract version and configuration digest
- Rust toolchain and lockfile hash
- Release binary paths and SHA-256 hashes
- Operating system, architecture, CPU, memory, and Docker resource limits
- PostgreSQL and DynamoDB Local versions
- Requested image references and resolved image identifiers or digests
- Exact commands, environment settings, start time, end time, and exit status
- Run identifier and result-schema version

Standalone exploratory runners may support resume behavior, but their output cannot enter the publishable comparison.

## Repetition and execution policy

Every operation, including build, uses one excluded warm-up and at least seven measured repetitions. Each repetition starts from a fresh logical database so content-addressed upserts cannot turn a build into a no-op.

The orchestrator alternates backend order between repetitions to reduce thermal and background-load bias. Each backend receives the same repetition-specific workload. The service starts from a declared clean state, and the runner verifies that state before loading fixtures.

The publishable sequence is:

1. Run contract, unit, and summarizer tests
2. Run a Docker-backed smoke comparison at 10,000 records
3. Commit and push the complete harness implementation
4. Run the 10M benchmark from that clean implementation commit
5. Audit the raw matrix and regenerate the report from raw rows
6. Commit the immutable result directory separately

This sequence ensures that result provenance names the implementation commit rather than the later results commit.

## Statistical analysis

The report includes these fields for each backend and operation:

- Number of measured repetitions
- Median latency
- Median throughput derived from median latency
- Minimum and maximum latency
- Median absolute deviation
- Coefficient of variation
- Deterministic bootstrap 95% confidence interval for the backend latency ratio

The bootstrap uses a fixed documented seed and paired repetition identities. The report declares a backend faster only when:

1. The confidence interval excludes parity
2. The median effect exceeds 5%

All other outcomes are `inconclusive`. High variance remains visible even when the winner rule passes. Build receives the same statistical treatment as every other operation.

## Result schema and fail-closed analysis

Raw results use one versioned schema across both adapters. Each row includes:

- Run, backend, repetition, and operation identity
- Complete workload dimensions
- Source, binary, service, and contract identity
- Elapsed nanoseconds and logical operation count
- Root, count, workload digest, outcome digest, and validation status
- Timed-scope version

The summarizer verifies:

- Exactly one expected row per backend, repetition, and operation
- At least seven measured repetitions
- No duplicate or unexpected rows
- Matching workload dimensions and digests
- Matching logical outcomes and roots
- One source revision and binary hash per backend
- One contract and result-schema version
- Successful validation for every row
- A fresh-run manifest with no resume marker

Any failure produces a non-zero exit and no publishable Markdown table.

## Harness architecture

The implementation has four parts:

1. `backend-workload-contract`: deterministic workload and oracle library
2. PostgreSQL and DynamoDB Local runners: adapter setup, public operation calls, and complete validation
3. Comparison orchestrator: clean-state checks, service lifecycle, alternating repetitions, and provenance
4. Rust summarizer: schema validation, statistical analysis, and report generation

The comparison orchestrator owns the cross-backend experiment. Backend runners emit evidence but cannot declare a winner.

## Test strategy

Implementation follows red, green, and refactor cycles.

### Workload contract tests

- Pin golden keys, values, identifier selections, mutation order, branches, and digests
- Prove repeated generation is byte-identical
- Prove different contract inputs change the workload digest
- Check expected batch, diff, and merge maps against a `BTreeMap` oracle

### Runner tests

- Prove validation runs after measurement
- Reject wrong query values, missing results, duplicate results, diff mismatches, and merge mismatches
- Check that each adapter emits the common schema and contract evidence
- Run small adapter integration cases through Docker-backed stores

### Summarizer tests

- Accept a complete matching fixture
- Reject missing, duplicate, mismatched, invalid, dirty, resumed, and mixed-provenance fixtures
- Pin median, median absolute deviation, coefficient of variation, bootstrap interval, and winner calculations
- Prove elapsed milliseconds cannot appear under an operations-per-second heading

### End-to-end tests

- Run both adapters at 10,000 records
- Compare every workload and outcome digest
- Recompute the report from saved raw rows and require byte-identical output
- Verify the result directory contains all referenced binaries, hashes, manifests, and commands

## Rollout and pull request updates

The work lands in three reviewable commits:

1. This design contract
2. Harness implementation, tests, and corrected report generation
3. Clean 10M raw results and generated report

The pull request description must:

- Replace the incorrect operations-per-second table
- Link this contract
- State that DynamoDB measurements use DynamoDB Local
- Show latency and throughput with correct units
- Report repetition counts, dispersion, and confidence intervals
- Document the exact reproduction command
- Mark the previous comparison as superseded

No performance winner claim remains in the pull request unless the hardened analysis produces one.
