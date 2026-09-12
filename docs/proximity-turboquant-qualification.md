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

## Checked-in correctness evidence

| Requirement | Evidence | Status |
| --- | --- | --- |
| Frozen PRNG, transform, codebook, packing, and wire bytes | `conformance/proximity-fixtures.json`, `scripts/generate_turboquant_codebooks.py`, TurboQuant unit tests, `tests/proximity_wire.rs` | Implemented |
| Source-bound canonical build and validation | `src/prolly/proximity/accelerator/turboquant.rs`, `tests/proximity_turboquant.rs` | Implemented |
| Authoritative exact reranking and bounded candidate admission | `src/prolly/proximity/accelerator/quantized.rs`, TurboQuant/PQ search tests | Implemented |
| Scalar/SIMD bit identity | `src/prolly/proximity/distance/simd.rs`, TurboQuant unit tests | Implemented |
| Sync/async, catalog, composite, proof, content graph, and GC integration | focused proximity accelerator, async, proof, and content-graph tests | Implemented |
| Maintained portable binding lifecycle | UniFFI plus Python, Go, Node, Kotlin, Java, Ruby, Swift, and WASM facades/tests | Implemented |
| Cross-target fixture execution on x86_64, aarch64, and browser WASM | CI or retained target logs | Pending |
| Dense paper-reference distortion comparison | retained development report | Pending |
| Complete adversarial fault injection at every read/write boundary | deterministic fault-injection report | Pending |

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
metric text falls back to L2, while unsupported TurboQuant numeric
configurations fail during the benchmark build instead of being silently
normalized.

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

## Local smoke evidence

The implementation PR records a development-machine 64-record × 8-dimension
smoke run. It demonstrates executable benchmark coverage and exact reranking,
not production capacity or `Auto` qualification. Production evidence must not
replace or average away a failing dataset/metric row.
