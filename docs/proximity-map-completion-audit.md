# Proximity Map Completion Audit

This audit maps every approved goal to implementation, adversarial tests,
and a benchmark row. The release is a hard format cutoff: proximity v1 is
legacy rejection input only; ordered CRAB bytes remain unchanged.

## Fourteen-goal evidence matrix

| # | Goal | Implementation evidence | Test evidence | Benchmark row |
| ---: | --- | --- | --- | --- |
| 1 | Canonical localized exact-directory mutation | `src/prolly/splice.rs`, `src/prolly/proximity/mutation.rs`, `src/prolly/proximity/map.rs` | `tests/splice.rs`, `tests/proximity_mutation.rs` | `localized_mutation` (`nodes_written`, `nodes_reused`) |
| 2 | Deterministic global best-first search | `proximity/search/engine.rs`, `map.rs` | `tests/proximity_search.rs` | `search_exact_scalar`, `search_adaptive_sq8` |
| 3 | Conservative compositional bounds | `proximity/distance/canonical.rs`, `storage/node.rs`, `storage/overflow.rs` | `tests/proximity_metrics.rs`, `tests/proximity_overflow.rs`, verifier corruption tests | exact-search and proof rows |
| 4 | Range/prefix/eligible/secondary filters | `proximity/search/filter.rs` | `tests/proximity_search.rs`, `tests/proximity_proofs.rs` | prefix filter is applied to all search rows |
| 5 | Adaptive policies and explicit budgets | `proximity/search/policy.rs`, `search/mod.rs` | `tests/proximity_search.rs` | `search_adaptive_sq8` |
| 6 | Byte-identical parallel construction | `proximity/build/parallel.rs` | `tests/proximity_parallel.rs` | `build` for thread counts 1/2/4 |
| 7 | Ordered async execution/backpressure | `proximity/search/async.rs` | `tests/proximity_async.rs` | `async_search` under `--all-features` |
| 8 | Scalar-equivalent query-only SIMD | `proximity/distance/simd.rs` | `tests/proximity_simd.rs` | `search_exact_scalar` versus `search_exact_simd` |
| 9 | L2/cosine/inner-product canonical math | `proximity/distance/{canonical,scalar}.rs` | `tests/proximity_metrics.rs` | harness metric can be changed in `config`; test matrix is authoritative for bits |
| 10 | Local SQ8 and deterministic offline PQ | `proximity/storage/quantized.rs`, `accelerator/{sq8,pq}.rs` | `tests/proximity_quantization.rs` | `search_adaptive_sq8`, `pq_build`, `pq_search` |
| 11 | Typed traversal, sync, manifests, GC, proofs | `content_graph/`, `proximity/proof/` | `tests/proximity_content_graph.rs`, `tests/proximity_proofs.rs` | `content_graph_copy`, `content_graph_gc_plan`, `search_proof_*` |
| 12 | Overflow hierarchies and external vectors | `proximity/storage/{overflow,vector}.rs` | `tests/proximity_overflow.rs` | all rows use bounded overflow and externalize vectors above 4 KiB |
| 13 | Validated source-bound HNSW | `proximity/accelerator/hnsw/` | `tests/proximity_hnsw.rs`, `tests/proximity_proofs.rs` | `hnsw_build`, `hnsw_search` |
| 14 | Native source-bound TurboQuant-MSE routing | `proximity/accelerator/{turboquant,quantized,async}.rs`, `proximity/distance/simd.rs`, `proximity/search/runtime.rs`; the TurboQuant algorithm module statically forbids unsafe code | `tests/proximity_turboquant.rs`, including bounded native async direct/composite construction, canonical repeated-equal-error quality persistence, and cancellation at every full-scan/direct-lookup/rerank store-read boundary; `fuzz/fuzz_targets/` contains arbitrary-decode and bounded lifecycle/corruption targets; runtime tests prove that only cache-authenticated reads bypass duplicate CID hashing and corrupt fallbacks remain unverified; every-code signed-zero/extreme-weight product-table identity and scalar/SIMD identity pass strict-provenance Miri; `tests/proximity_wire.rs` and async/proof/content/composite suites complete the focused coverage | `turboquant_build`, `turboquant_search_scalar`, `turboquant_search_simd`, `turboquant_recall` |

The TurboQuant binding deliverable is also mapped directly: Python, Go,
Node/TypeScript, Kotlin, Java, Ruby, Swift, and browser WASM each expose and
test build/load/verify/forced-search behavior, pre-cancelled search, and
reported build/search statistics. Every lifecycle also attaches the native
TurboQuant sidecar directly to an accelerator catalog, dispatches a catalog
search through it, and constructs and searches a mutation-aware composite
whose base kind is TurboQuant. Each corresponding
`bindings/*/COOKBOOK.md` now includes a deterministic RAG-sidecar lifecycle.
The regenerated `bindings/api/classification-audit.json` contains 73
TurboQuant rows, all `implemented` and `release_complete`.
The repository-wide inventory and production qualification gates remain
tracked separately in
[`proximity-turboquant-qualification.md`](proximity-turboquant-qualification.md).

## Wire and migration evidence

- `conformance/proximity-fixtures.json` freezes exact bytes and CIDs.
- `tests/proximity_wire.rs` covers malformed objects and explicit v1 rejection.
- Ordered `CRAB` codecs were not revised by the proximity cutoff.
- Migration is rebuild-only from logical key/vector/value records; see
  [`proximity-map.md`](proximity-map.md#migration-and-compatibility).

## Benchmark protocol

`benches/prolly_proximity_bench.rs` emits CSV:

```text
operation,dimensions,threads,micros,metric_a,metric_b
```

For repeated accelerator searches, the base row's `micros` value is the
sample median and `_p95`/`_p99` companion rows contain nearest-rank tail
latency. Companion counters expose logical/physical read bytes and candidate
retention peaks. Quantized `_io` rows expose logical nodes and actual store read
operations; `_rerank` rows expose authoritative rerank counts and committed
logical bytes. Dedicated resource and sidecar rows record owned build bytes,
transform work, authoritative source-closure bytes, derived accelerator bytes,
and encoded payload counts. Accelerator sidecar totals exclude
content-addressed source objects referenced by the manifest; companion rows
split manifest bytes from code-tree bytes.

Default dimensions are 8, 128, 768, and 1536; build rows use 1, 2, and 4
workers. The harness also records mutation locality, exact/adaptive/SQ8,
scalar/SIMD, sync/async, TurboQuant/PQ/HNSW, content copy/GC, and proof
generation/replay.
Counters in `metric_a`/`metric_b` are operation-specific and printed beside
wall time so regressions can be attributed to logical work rather than timing
noise.

The strict matrix entry point is
`scripts/run_turboquant_qualification.py`. Its `full` profile contains 45,360
deterministic cells and supports stable hash sharding and exact-contract resume.
It validates schema/revision metadata, row completeness, worker determinism,
warm/cold physical I/O, and sync/async logical parity before writing an atomic
completion record. The harness asserts neighbor, exact-distance, plan,
completion, and complete logical-statistic parity across scalar/SIMD/automatic
and async execution. Shard zero also records an unskipped browser-WASM build
and test smoke gate. `scripts/summarize_turboquant_qualification.py` accepts
only the complete, disjoint shard set and emits consolidated rows plus separate
forced-backend and `Auto` gate results.

The first ten corrected schema-v3 cells are retained in
[`proximity-turboquant-v3-qualification.md`](proximity-turboquant-v3-qualification.md).
They cover 10K and 100K × 768 at the default four-bit, 8× setting for all
three metrics, plus matching 16× cosine and inner-product diagnostics.
Squared-L2 recall is 1.00 after correcting reconstructed-vector norm scoring.
Correct cosine reconstruction normalization raises 10K recall to 0.40 but
still fails the floor, as do the 16× product-oriented rows. This partial
evidence leaves 45,350 matrix cells and all final release gates open.

Smoke command:

```sh
PROLLY_PROXIMITY_BENCH_RECORDS=64 \
PROLLY_PROXIMITY_BENCH_DIMENSIONS=8 \
PROLLY_PROXIMITY_BENCH_THREADS=1,2 \
cargo bench --all-features --bench prolly_proximity_bench
```

Recorded smoke result on 2026-07-14 (Apple Silicon development machine, release
profile, in-memory store, 64 records × 8 dimensions):

| Row | µs | Logical evidence |
| --- | ---: | --- |
| build / 1 worker | 1197.791 | 63 evaluations, 4 objects written |
| build / 2 workers | 894.375 | identical 63 evaluations, 4 objects written |
| exact scalar / SIMD | 385.875 / 62.875 | 2 nodes, 64 evaluations, recall@10 1.0 |
| adaptive SQ8 | 61.541 | 2 nodes, 65 total exact+quantized evaluations |
| localized mutation | 816.500 | 3 nodes written, 1 reused |
| graph copy / GC plan | 202.500 / 170.000 | 8 objects, 10,175 bytes; 0 reclaimable |
| proof generate / verify | 265.459 / 693.958 | 72 replay events, 8 authenticated objects |
| PQ build / search | 779.584 / 123.125 | 64 encoded vectors, 64 reranked |
| HNSW build / search | 4718.958 / 268.084 | 1,046 directed edges, 33 graph nodes read |
| async search | 152.250 | sync-identical 2 nodes, 64 evaluations |

These smoke values validate coverage and counter identity, not production
capacity. The 1/2-worker rows demonstrate identical logical work; timing is
reported only to make future regressions measurable.

Production characterization command:

```sh
PROLLY_PROXIMITY_BENCH_RECORDS=10000 \
PROLLY_PROXIMITY_BENCH_DIMENSIONS=8,128,768,1536 \
PROLLY_PROXIMITY_BENCH_THREADS=1,2,4 \
cargo bench --all-features --bench prolly_proximity_bench
```

Results are hardware-, compiler-, and store-specific. Persist CSV output with
the deployment's machine description rather than treating one development
machine as a service-level guarantee.

## Release verification commands

The completion gate is:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo +1.89.0 check --all-targets --all-features
cargo test --all-features --no-fail-fast
cargo test --doc --all-features
cargo bench --all-features --no-run
cargo +nightly fuzz run proximity_turboquant_decode -- -runs=2048 -max_len=4096 -timeout=5
cargo +nightly fuzz run proximity_turboquant_lifecycle -- -runs=256 -max_len=1024 -timeout=5
git diff --check
```

The focused evidence suites are:

```sh
cargo test --test proximity_api --test proximity_metrics --test proximity_wire
cargo test --test splice --test proximity_overflow --test proximity_mutation
cargo test --test proximity_search --test proximity_parallel --test proximity_simd
cargo test --all-features --test proximity_async
cargo test --test proximity_quantization --test proximity_hnsw
cargo test --test proximity_turboquant --test proximity_wire
cargo test --test proximity_content_graph --test proximity_proofs
```

TurboQuant is explicit-only until its separate recall and comparative value
gates pass. See
[`proximity-turboquant-qualification.md`](proximity-turboquant-qualification.md)
for pending evidence; this audit does not treat a local smoke timing as a GA or
`Auto` qualification result.

## Commit trail

The program is split into reviewable commits from `cd08ce3` (hard-cut API)
through `b2ca128` (descriptor-bound proofs), with canonical splice, storage,
localized mutation, search, parallel/async/SIMD execution, accelerators, and
typed graph integration committed independently between them.
