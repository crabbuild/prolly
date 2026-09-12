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
| Source-bound canonical build and validation | `src/prolly/proximity/accelerator/turboquant.rs`, `tests/proximity_turboquant.rs` | Implemented |
| Authoritative exact reranking and bounded candidate admission | `src/prolly/proximity/accelerator/quantized.rs`, TurboQuant/PQ search tests | Implemented |
| Scalar/SIMD bit identity | `src/prolly/proximity/distance/simd.rs`, TurboQuant unit tests | Implemented |
| Sync/async, catalog, composite, proof, content graph, and GC integration | focused proximity accelerator, async, proof, and content-graph tests | Implemented |
| Maintained portable binding lifecycle | UniFFI plus Python, Go, Node, Kotlin, Java, Ruby, Swift, and WASM facades/tests | Implemented |
| Cross-target fixture execution on x86_64, aarch64, and browser WASM | [`proximity-turboquant-cross-target.md`](proximity-turboquant-cross-target.md), Linux CI, local aarch64 suite, browser-WASM canonical CID test | Implemented |
| Dense paper-reference distortion and recall comparison | [`proximity-turboquant-dense-reference.md`](proximity-turboquant-dense-reference.md), retained raw JSON, reproducible generator | Implemented |
| Complete adversarial fault injection at every read/write boundary | [`proximity-turboquant-fault-injection.md`](proximity-turboquant-fault-injection.md), ordinal cold-read/publication tests | Implemented |

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

Search rows report the sample median; `_p95` and `_p99` companion rows report
nearest-rank tail latency. Their counters retain logical/physical bytes read
and candidate handle/byte peaks. `_work` rows retain frontier peak and the
completion discriminator (`0` exact, `1` approximate-policy-satisfied, `2`
budget-exhausted, `3` cancelled, `4` deadline-exceeded).
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

Cold-cache quantizer samples clear the authoritative map caches and use a new
`SearchIo` runtime/namespace for each timed query, so descriptor, manifest,
source, and code-tree physical reads are measured rather than inherited from a
prior sample. Warm rows perform one untimed warmup through the same instrumented
runtime used by every measured sample. The output preamble records the machine
hostname and the frozen TurboQuant seed in addition to revision, compiler,
target, store, and cache mode.

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
| all-feature tests | Passed: 536 library tests plus every integration suite; one unrelated extended splice stress test remains explicitly ignored |
| all-feature doctests | Passed: 74/74 |
| all-feature benchmark compilation | Passed |
| browser-WASM build, typecheck, and package tests | Passed: 37/37, including canonical TurboQuant wire parity |
| release binding inventory | Repository gate remains open: 2,998 pre-existing public Rust entries are classified incomplete; none is a TurboQuant entry |

The binding inventory distinction is intentional. TurboQuant's production
cells are implemented, but the approved release command requires the global
repository inventory to pass, so forced-backend GA cannot claim this gate yet.

## Local smoke evidence

The implementation PR records a development-machine 64-record × 8-dimension
smoke run. It demonstrates executable benchmark coverage and exact reranking,
not production capacity or `Auto` qualification. Production evidence must not
replace or average away a failing dataset/metric row.

The retained
[`proximity-turboquant-1k-qualification.md`](proximity-turboquant-1k-qualification.md)
report covers the next bounded tier across every supported dimension and
metric. Its default-path recall rows pass, but its comparative timing and size
results reinforce the decision to keep `Auto` disabled. The report explicitly
does not close the remaining production-matrix gates.
