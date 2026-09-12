# TurboQuant Qualification

This document is the release evidence ledger for Prolly's native,
independently implemented `TurboQuant_mse` routing accelerator. It separates
forced-backend availability from `Auto` eligibility. Missing evidence is a
closed gate, never an inferred pass.

## Current disposition

- Forced backend: implemented; release remains gated by the unchecked items
  below.
- `Auto`: disabled. The planner deliberately treats TurboQuant as unavailable
  during automatic selection.
- QJL/product residual mode: out of scope and not implemented.
- Turbovec compatibility or dependency: none.

The implementation follows the algorithmic structure described in
[TurboQuant: Online Vector Quantization with Near-optimal Distortion Rate](https://arxiv.org/abs/2504.19874),
but uses a deterministic structured orthogonal transform instead of the
paper's dense Gaussian-QR/Haar rotation. Consequently, Prolly makes no claim
to the paper's exact dense-rotation theorem. No Turbovec source code, API, or
wire format is used.
The detailed independent-implementation and release-owner record is in
[`proximity-turboquant-provenance.md`](proximity-turboquant-provenance.md).

The development-only dense Gaussian-QR oracle and all-dimension comparison are
documented in
[`proximity-turboquant-dense-reference.md`](proximity-turboquant-dense-reference.md).
That evidence checks the structured-transform engineering deviation; it does
not replace the production-scale recall and performance matrix below.

## Checked-in correctness evidence

| Requirement | Evidence | Status |
| --- | --- | --- |
| Frozen PRNG, transform, codebook, packing, and wire bytes | `conformance/proximity-fixtures.json`, `scripts/generate_turboquant_codebooks.py`, TurboQuant unit tests, `tests/proximity_wire.rs` | Implemented |
| Source-bound canonical build and validation | `src/prolly/proximity/accelerator/turboquant.rs`, sync/async trusted-open root-count unit tests, a CID-valid partial-sidecar regression proving direct and composite source-count binding, and adaptive pre-encoding temporary-budget tests that preserve the canonical manifest while accounting for transform scratch, retained input/output, the live code-tree hierarchy, pending publications, and transient raw/encoded node overlap; unbounded builds cap code-tree publication batches at 16 nodes | Implemented |
| Authoritative exact reranking and bounded candidate admission | `src/prolly/proximity/accelerator/quantized.rs`, TurboQuant/PQ search tests | Implemented |
| Scalar/SIMD bit identity | `src/prolly/proximity/distance/simd.rs`, TurboQuant unit tests | Implemented |
| Sync/async, catalog, composite, proof, content graph, and GC integration | focused proximity accelerator, async, proof, and content-graph tests, including manifest-context validation of every TurboQuant tree node, child count, and packed leaf value; `turboquant_async_cancellation_covers_every_store_read_boundary` for full-scan/direct-lookup/final-rerank cancellation and zero-I/O pre-cancellation; explicit deadline completion; and typed rejection of old proof versions before closure work | Implemented |
| Maintained portable binding lifecycle and cookbook | UniFFI plus Python, Go, Node, Kotlin, Java, Ruby, Swift, and WASM facades/tests; every language lifecycle asserts TurboQuant build/search statistics and pre-cancelled search, then executes direct TurboQuant catalog dispatch and TurboQuant-based composite construction/search; each language's `bindings/*/COOKBOOK.md` includes a forced TurboQuant RAG sidecar lifecycle | Implemented |
| Cross-target fixture execution on x86_64, aarch64, and browser WASM | [`proximity-turboquant-cross-target.md`](proximity-turboquant-cross-target.md), Linux CI, local aarch64 suite, browser-WASM canonical CID test | Implemented |
| Dense paper-reference distortion and recall comparison | [`proximity-turboquant-dense-reference.md`](proximity-turboquant-dense-reference.md), retained raw JSON, reproducible generator | Implemented |
| Complete adversarial fault injection and tamper rejection | [`proximity-turboquant-fault-injection.md`](proximity-turboquant-fault-injection.md), ordinal cold-read/publication tests, and explicit source CID, dimensions, metric, count, seed, transform ID, codebook ID, bit width, quality, zero-count, code-root, and fingerprint mutation coverage | Implemented |
| Bounded fuzz smoke | TurboQuant unit and proof tests cover manifest decoding, packed-bit validation/unpacking, transform derivation through the maximum supported dimension, all-metric code scoring, and randomized proof-transcript replay mutations | Implemented |

## Benchmark protocol

The ordinary proximity harness contains `turboquant_build`,
`turboquant_search_scalar`, `turboquant_search_simd`, and
`turboquant_recall` rows beside equivalent PQ rows. A bounded smoke run is:

```sh
PROLLY_PROXIMITY_BENCH_RECORDS=1000 \
PROLLY_PROXIMITY_BENCH_DIMENSIONS=128,200,768,1536,3072 \
PROLLY_PROXIMITY_BENCH_THREADS=1,2,4 \
PROLLY_PROXIMITY_BENCH_SEARCH_REPEATS=30 \
cargo bench --all-features --bench prolly_proximity_bench
```

For the large qualification matrix, set
`PROLLY_PROXIMITY_BENCH_QUANTIZERS_ONLY=1`. This profile builds the
authoritative source once, then runs only TurboQuant and the equal-shortlist
PQ baseline. It omits exact/adaptive source searches, mutation, proof,
copy/GC, HNSW, composite, and generic async rows so those unrelated costs do
not make every matrix cell repeat them. `PROLLY_PROXIMITY_BENCH_THREADS`
applies to both quantizer builds in this profile and in the complete profile.
Each requested worker count emits a build row. The harness also asserts that
all requested worker counts produce the same manifest CID and logical build
statistics before it emits search evidence.

```sh
PROLLY_PROXIMITY_BENCH_RECORDS=100000 \
PROLLY_PROXIMITY_BENCH_DIMENSIONS=768,1536 \
PROLLY_PROXIMITY_BENCH_THREADS=1,2,4 \
PROLLY_PROXIMITY_BENCH_SEARCH_REPEATS=30 \
PROLLY_PROXIMITY_BENCH_QUANTIZERS_ONLY=1 \
cargo bench --all-features --bench prolly_proximity_bench
```

The scale-only and quantizers-only profiles are mutually exclusive. Worker
counts must be positive and unique; invalid lists fail before data generation
so retained CSVs cannot contain ambiguous duplicate cells.

The default store is `memory`. Durable-local cells use the repository
`FileNodeStore` by selecting `PROLLY_PROXIMITY_BENCH_STORE=file` and providing
an existing parent directory. The harness creates a uniquely named child for
the run, reports its canonical path, isolates each build in a separate store,
and removes the owned run directory on ordinary exit. An interrupted process
may leave that uniquely named child for diagnosis and manual cleanup.

```sh
mkdir -p /var/tmp/prolly-turboquant-bench
PROLLY_PROXIMITY_BENCH_STORE=file \
PROLLY_PROXIMITY_BENCH_STORE_PATH=/var/tmp/prolly-turboquant-bench \
PROLLY_PROXIMITY_BENCH_RECORDS=100000 \
PROLLY_PROXIMITY_BENCH_DIMENSIONS=768,1536 \
PROLLY_PROXIMITY_BENCH_THREADS=1,2,4 \
PROLLY_PROXIMITY_BENCH_QUANTIZERS_ONLY=1 \
cargo bench --all-features --bench prolly_proximity_bench
```

Search rows report the sample median; `_p95` and `_p99` companion rows report
nearest-rank tail latency. The main quantized-search row records quantized and
exact distance evaluations in `metric_a` and `metric_b`; `_p95` records logical
and physical bytes read; `_p99` records candidate-handle and retained-byte
peaks. `_work` rows retain frontier peak and the completion discriminator (`0`
exact, `1` approximate-policy-satisfied, `2` budget-exhausted, `3` cancelled,
`4` deadline-exceeded). `_io` records logical nodes consumed and actual store
read operations, while `_rerank` records authoritative candidates reranked and
committed logical bytes. The output preamble's `schema_version=2` identifies
these counter meanings.
`turboquant_build_resources` reports peak owned scratch bytes and butterfly
operations, while
`source_closure_bytes` reports the authoritative map closure and
`turboquant_sidecar_bytes` and `pq_sidecar_bytes` report only derived
accelerator bytes beside encoded payload counts. Referenced source objects are
excluded from the sidecar totals even though they are part of each typed
closure. The corresponding `*_manifest_code_bytes` rows split manifest bytes
from code-tree bytes. The harness emits the git revision, compiler, target
architecture/OS, store, seed, cache mode, and repetition count before the CSV
rows.

The scalar and automatic TurboQuant scan kernels precompute the finite
query-by-codebook product table once per search. Candidate scoring then decodes
packed codes, reconstructs the exact positive half from the codebook's frozen
bit-symmetric negative half, and performs the same coordinate-ordered scalar
reduction without repeating a floating-point multiply for every candidate
component. This halves the scalar/automatic query table to
`dimensions * 2^(bit_width - 1) * 8` bytes (1 MiB at the maximum supported
shape). The
explicit SIMD kernel remains an independent conformance path and reconstructs
only one bounded coordinate chunk at a time. Its query preparation therefore
does not allocate the scalar product table, avoiding
`dimensions * 2^(bit_width - 1) * 8` unused bytes per query (1 MiB at the maximum
16,384-dimension four-bit configuration). Tests require all three kernels to
return bit-identical scores, plans, neighbors, and logical statistics.

Production qualification must retain raw outputs under
`performance-results/proximity-turboquant/` and cover the full matrix in the
approved design: 1K/10K/100K/1M records; dimensions
128/200/768/1536/3072; every metric; `k` 1/10/100; all and
10%/1%/0.1% eligibility; bit widths 2/3/4; rerank multipliers 4/8/16 and
exhaustive where feasible; warm/cold memory, durable local, async/batched, and
WASM smoke environments. Each retained run must identify the revision,
compiler, target, host, store, seed, and repetition count.

One harness invocation selects a single matrix configuration with
`PROLLY_PROXIMITY_BENCH_METRIC` (`l2`, `cosine`, or `inner_product`),
`PROLLY_PROXIMITY_BENCH_K`, `PROLLY_PROXIMITY_BENCH_ELIGIBILITY_PPM`
(`1000000`, `100000`, `10000`, or `1000`),
`PROLLY_PROXIMITY_BENCH_TURBOQUANT_BITS`, and
`PROLLY_PROXIMITY_BENCH_RERANK_MULTIPLIER`. Set
`PROLLY_PROXIMITY_BENCH_RESET_SEARCH_CACHE=1` for cold-cache rows. Invalid
metric text, malformed numeric values, duplicate/non-positive worker counts,
and unsupported TurboQuant configurations fail instead of being silently
normalized.

Set `PROLLY_PROXIMITY_BENCH_ASYNC_QUANTIZERS=1` with the `async-store` feature
to add async build, publication, search, percentile/work, and
environment-specific recall rows for TurboQuant and PQ. The async builders
construct canonical sidecars, publish their complete catalog closures in
bounded batches, and assert that each accelerator manifest and its logical
statistics match the synchronous build for every requested worker count.
TurboQuant reads the immutable async directory in key order, encodes at most
128 source vectors at a time, and feeds a memory-bounded async sorted builder;
it does not materialize or restage the corpus. Async verification likewise
streams the source and code trees in lockstep. PQ retains its established
staging path. `*_build_async_publication` rows record published object and byte
counts.

For the normal co-resident ancestor/current case, TurboQuant composite
construction also stays on the native async path. It structurally diffs the
immutable directories, streams ordered delta and shadow entries into bounded
async sorted builders, and publishes the canonical composite manifest last.
It does not collect or rebuild either complete source corpus, and its manifest
and logical build statistics must match synchronous construction exactly. A
cross-store base whose authenticated closure is absent from the destination
retains the established compatibility staging path so the operation still
copies a complete closure rather than publishing dangling references.
Searches execute the native async/batched engine through `SyncStoreAsAsync`,
preserving the selected memory or file store. Warm samples share one
authenticated `SearchIo` runtime after an untimed warmup. Cold samples create a
new runtime/namespace and reload the descriptor and sidecar metadata inside
each timed sample; their physical byte count therefore includes source,
manifest, and code-tree reads.

Cold-cache quantizer samples clear the authoritative map caches and use a new
`SearchIo` runtime/namespace for each timed query, so descriptor, manifest,
source, and code-tree physical reads are measured rather than inherited from a
prior sample. Warm rows perform one untimed warmup through the same instrumented
runtime used by every measured sample. The output preamble records the machine
hostname and the frozen TurboQuant seed in addition to revision, compiler,
target, store, and cache mode.

### Qualification runner

`scripts/run_turboquant_qualification.py` is the authoritative native-matrix
orchestrator. The full profile deterministically enumerates 45,360 cells: all
required count, dimension, metric, requested-`k`, eligibility, bit-width, and
fixed rerank combinations across warm/cold memory, warm/cold file, and
warm/cold async/batched environments, plus exhaustive reranking through 10K
records. When eligibility contains fewer records than the requested `k`, the
harness records both `k` and `effective_k`; recall is computed at the largest
mathematically possible result count.

Each cell has an immutable ID and isolated raw CSV, stderr log, completion
record, and SHA-256 digest. A cell is complete only after the runner validates
the exact revision and schema, every required operation and worker row, finite
counters, warm/cold physical-I/O behavior, and sync/async logical parity.
The benchmark itself additionally asserts identical neighbors (including exact
distances and order), committed plans, completion states, and every logical
statistic across scalar/SIMD/automatic kernels and the async engine before a
cell can emit successful evidence.
Resume revalidates both the content digest and the entire row contract before
skipping a cell. Mixed revisions, schemas, shard definitions, repetitions, or
worker lists fail closed. Hash sharding is stable and disjoint:

```sh
python3 scripts/run_turboquant_qualification.py \
  --profile full \
  --output performance-results/proximity-turboquant/full-REV/shard-00-of-64 \
  --shard-index 0 \
  --shard-count 64

# Restart the exact same shard after interruption.
python3 scripts/run_turboquant_qualification.py \
  --profile full \
  --output performance-results/proximity-turboquant/full-REV/shard-00-of-64 \
  --shard-index 0 \
  --shard-count 64 \
  --resume
```

On a qualification host that cannot safely execute the 1M tier, pass an
explicit record ceiling of at least 100K. For example,
`--max-cell-records 100000` executes every 1K/10K/100K cell and records each
1M cell as a versioned `ProximityResourceLimitExceeded` disposition with the
exact resource, limit, actual count, and preflight phase. This mirrors the
production builder's typed `max_records` failure and is accepted only for 1M
cells; it never creates a synthetic benchmark CSV or counts as performance or
recall evidence. The ceiling is part of the immutable shard contract, resume
revalidates every disposition, and the summarizer emits the complete list in
`scalability-failures.json`. Omitting the option requires every 1M cell to run.

Shard zero also rebuilds the browser WASM package and requires the package test
suite to report zero failures and zero skips, including the TurboQuant lifecycle
and Rust-wire-fixture tests. This is the required WASM smoke gate; native
latency is not presented as browser performance. Production runs reject a
tracked-dirty worktree. `--allow-dirty` exists only for the six-cell smoke
profile and its output is never publishable qualification evidence.

After every shard completes, the strict summarizer must receive the complete
shard set. It revalidates manifests, raw/state digests, every cell, WASM logs,
and disjoint full-matrix coverage before emitting `summary.csv`, `gates.json`,
and `report.md`. It refuses incomplete, duplicate, mixed-provenance, or extra
cells. The report evaluates forced-backend matrix gates separately from the
stricter `Auto` gates and explicitly leaves legal, global binding inventory,
and final supported-host release commands unevaluated:

```sh
python3 scripts/summarize_turboquant_qualification.py \
  --input performance-results/proximity-turboquant/full-REV/shard-* \
  --output performance-results/proximity-turboquant/full-REV/summary
```

## Release gates

Forced-backend GA still requires:

- recall@10 at least 0.95 for the default four-bit configuration on every
  checked dataset/metric row, and no row more than 0.01 below PQ at an equal
  shortlist;
- the complete correctness, resource, binding, and release command suites;
- retained cross-target and benchmark evidence; and
- a release-owner legal/provenance disposition. CC BY 4.0 attribution is not a
  patent grant, so this repository cannot infer that disposition from the
  paper's publication.

`Auto` additionally requires default four-bit TurboQuant to build faster than
default PQ at 100K × 768 and 100K × 1536, have warm p95 search no worse than
1.25 times PQ at equal recall of at least 0.95, and show a qualifying speed,
size, or recall advantage for every enabled benchmark family. Until retained
evidence satisfies every comparative rule, `Auto` remains disabled.

## Release-command audit

The local release-command audit on 2026-09-12 records each gate separately:

| Command or suite | Result |
| --- | --- |
| `cargo fmt --all -- --check` and `git diff --check` | Passed |
| warnings-denied all-target/all-feature Clippy | Passed |
| Rust 1.89 all-target/all-feature check | Passed; repository-wide unfulfilled-lint-expectation warnings remain non-fatal under that compiler |
| all-feature tests | Passed: 546 library tests plus every integration suite and 74 doc tests; one unrelated extended splice stress test remains explicitly ignored |
| strict-provenance Miri scorer checks | Passed for the every-code signed-zero/extreme-weight symmetry oracle and scalar/SIMD bit-identity test |
| all-feature doctests | Passed: 74/74 |
| all-feature benchmark compilation | Passed |
| browser-WASM build, typecheck, and package tests | Passed: 37/37, including canonical TurboQuant wire parity |
| maintained binding TurboQuant lifecycle | Python, Go, Node/TypeScript, Kotlin, Java, Ruby, Swift, and browser WASM lifecycle tests exercise pre-cancelled search, reported TurboQuant statistics, direct TurboQuant catalog dispatch, and TurboQuant-based composite search; Ruby passes 23/23 with 264 assertions, and the full 18-module Kotlin/Java reactor passes locally |
| maintained binding cookbooks | Python, Go, Node/TypeScript, Kotlin, Java, Ruby, Swift, and browser WASM document build, forced search, verification, and manifest reopen |
| generated classification audit | Regenerated from current-head async-feature rustdoc: all 73 TurboQuant rows are `implemented` and `release_complete`; the normal 3,418-operation inventory check passes, including the four Rust-owned validated-read store defaults |
| release binding inventory | Repository gate remains open: 2,998 pre-existing public Rust entries are classified incomplete; none is a TurboQuant entry |

The binding inventory distinction is intentional. TurboQuant's production
cells are implemented, but the approved release command requires the global
repository inventory to pass, so forced-backend GA cannot claim this gate yet.

## Local smoke evidence

The implementation PR records a development-machine 64-record × 8-dimension
smoke run. It demonstrates executable benchmark coverage and exact reranking,
not production capacity or `Auto` qualification. Production evidence must not
replace or average away a failing dataset/metric row.

A controlled 10K-record × 768-dimension warm-search diagnostic measured the
default four-bit scalar median at 9.016 ms before bounds-check elimination and
8.498/8.506 ms in two compact symmetric-table runs. Relative to the unchanged
PQ control in each run, the TurboQuant/PQ median ratio improved from 6.177× to
5.905×/5.904×. Recall, candidate/rerank counts, and logical/physical work were
unchanged. This is implementation diagnostic evidence, not a retained matrix
cell or an `Auto` qualification result.

The retained
[`proximity-turboquant-1k-qualification.md`](proximity-turboquant-1k-qualification.md)
report covers the next bounded tier across every supported dimension and
metric. Its default-path recall rows pass, but its comparative timing and size
results reinforce the decision to keep `Auto` disabled. The report explicitly
does not close the remaining production-matrix gates.

The retained
[`proximity-turboquant-10k-qualification.md`](proximity-turboquant-10k-qualification.md)
report adds one real schema-v2 10K × 768 squared-L2 default-path cell. It
demonstrates 1.0 TurboQuant recall and materially faster construction than PQ,
but its warm p95 search is 2.63× PQ and its sidecar is 22.79× larger. It is
therefore evidence for the forced path and against enabling `Auto`, not a
substitute for the remaining 45,359 production cells.
