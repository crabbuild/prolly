# Proximity Map Completion Audit

This audit maps every approved goal to implementation, adversarial tests,
and a benchmark row. The release is a hard format cutoff: proximity v1 is
legacy rejection input only; ordered CRAB bytes remain unchanged.

## Thirteen-goal evidence matrix

| # | Goal | Implementation evidence | Test evidence | Benchmark row |
| ---: | --- | --- | --- | --- |
| 1 | Canonical localized exact-directory mutation | `src/prolly/canonical_splice.rs`, `proximity/mutation/`, `map.rs` | `tests/canonical_splice.rs`, `tests/proximity_mutation.rs` | `localized_mutation` (`nodes_written`, `nodes_reused`) |
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

## Wire and migration evidence

- `conformance/proximity-fixtures.json` freezes exact bytes and CIDs.
- `tests/proximity_wire.rs` covers malformed objects and explicit v1 rejection.
- Ordered `CRAB` codecs were not revised by the proximity cutoff.
- Migration is rebuild-only from logical key/vector/value records; see
  [`proximity-map.md`](proximity-map.md#migration-and-compatibility).

## Benchmark protocol

`benches/proximity_bench.rs` emits CSV:

```text
operation,dimensions,threads,micros,metric_a,metric_b
```

Default dimensions are 8, 128, 768, and 1536; build rows use 1, 2, and 4
workers. The harness also records mutation locality, exact/adaptive/SQ8,
scalar/SIMD, sync/async, PQ/HNSW, content copy/GC, and proof generation/replay.
Counters in `metric_a`/`metric_b` are operation-specific and printed beside
wall time so regressions can be attributed to logical work rather than timing
noise.

Smoke command:

```sh
PROLLY_PROXIMITY_BENCH_RECORDS=64 \
PROLLY_PROXIMITY_BENCH_DIMENSIONS=8 \
PROLLY_PROXIMITY_BENCH_THREADS=1,2 \
cargo bench --all-features --bench proximity_bench
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
cargo bench --all-features --bench proximity_bench
```

Results are hardware-, compiler-, and store-specific. Persist CSV output with
the deployment's machine description rather than treating one development
machine as a service-level guarantee.

## Release verification commands

The completion gate is:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo +1.81.0 check --all-targets --all-features
cargo test --all-features --no-fail-fast
cargo test --doc --all-features
cargo bench --all-features --no-run
git diff --check
```

The focused evidence suites are:

```sh
cargo test --test proximity_api --test proximity_metrics --test proximity_wire
cargo test --test canonical_splice --test proximity_overflow --test proximity_mutation
cargo test --test proximity_search --test proximity_parallel --test proximity_simd
cargo test --all-features --test proximity_async
cargo test --test proximity_quantization --test proximity_hnsw
cargo test --test proximity_content_graph --test proximity_proofs
```

## Commit trail

The program is split into reviewable commits from `cd08ce3` (hard-cut API)
through `b2ca128` (descriptor-bound proofs), with canonical splice, storage,
localized mutation, search, parallel/async/SIMD execution, accelerators, and
typed graph integration committed independently between them.
