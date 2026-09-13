# TurboQuant Schema-v4 Partial Qualification Evidence

This report retains eleven real cells from the corrected TurboQuant
production matrix. Ten cover 10K and 100K records, 768 dimensions, the
default four-bit encoding, every supported metric at 8× reranking, and
matching 16× cosine and inner-product diagnostics. The eleventh exhaustively
reranks the 10K cosine fixture. These cells are partial evidence, not a
complete production qualification, and `Auto` remains disabled.

## Environment and contract

- Date: 2026-09-12
- Implementation revision:
  `729c2ffac8835db9c99d7706746c321067abfa1d`
- Qualification schema: `prolly-turboquant-qualification-v4`
- Benchmark schema: 4
- Dataset: `linear-mod-2000003-v2`
- Recall oracle: authoritative exact scalar ProximityMap search over persisted
  canonical vectors, with the same filter, effective `k`, metric, and
  `(exact_score, key)` order as reranking
- Compiler: Rust 1.97.0, LLVM 22.1.6
- Target: `aarch64-apple-darwin`
- Machine: `Haipings-Mac-Studio.local`
- Store/cache: in-memory, warm bounded search runtime
- Query: `k=10`, all records eligible, seed 0
- Build workers: 1, 2, and 4
- Search samples: 30 per implementation

Each retained directory contains an immutable manifest, raw CSV, stderr log,
completion state, and content digest. Exact `--resume` invocations revalidated
all eleven directories without rerunning their benchmarks. The focused
directories use sparse shard contracts and do not form a complete shard set;
the strict summarizer therefore cannot mistake them for the 45,360-cell
production matrix.

## Default recall and search results

| Records | Metric | TQ recall@10 | PQ recall@10 | TQ median | PQ median | TQ warm p95 | PQ warm p95 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 10K | Squared L2 | 1.00 | 1.00 | 9,708.208 µs | 1,465.208 µs | 10,524.875 µs | 1,688.750 µs |
| 10K | Cosine | 0.00 | 1.00 | 9,621.000 µs | 1,476.583 µs | 10,089.584 µs | 3,484.167 µs |
| 10K | Inner product | 1.00 | 0.00 | 10,132.541 µs | 1,395.791 µs | 12,858.416 µs | 1,605.208 µs |
| 100K | Squared L2 | 1.00 | 0.00 | 127,451.292 µs | 13,391.416 µs | 139,941.542 µs | 14,667.166 µs |
| 100K | Cosine | 0.00 | 0.00 | 120,933.000 µs | 14,795.041 µs | 124,382.667 µs | 16,910.375 µs |
| 100K | Inner product | 0.00 | 0.00 | 122,724.667 µs | 12,751.167 µs | 123,636.375 µs | 13,039.750 µs |

TurboQuant/PQ median ratios are 6.63×/6.52×/7.26× at 10K and
9.52×/8.17×/9.63× at 100K for L2/cosine/inner product. Corresponding p95
ratios are 6.23×/2.90×/8.01× and 9.54×/7.36×/9.48×. The unusually high 10K
PQ cosine p95 is a retained observation, not a portable performance claim.

The TurboQuant sidecar is 3,975,264 bytes at 10K and 39,757,465 bytes at
100K, respectively 22.79× and 30.43× the matching PQ sidecar. The 100K
working set exceeds the default 32 MiB TurboQuant runtime-cache partition, so
physical reads after warmup are expected bounded-cache behavior.

## Oracle and collision diagnosis

Exhaustive 10K cosine reranking returns recall 1.00 for both TurboQuant and PQ,
with exactly 10,000 authoritative reranks. This validates the schema-4 oracle
and exact reranking path. Its TurboQuant/PQ p95 ratio is 1.51×, but exhaustive
work is an oracle, not an acceptable approximate-search default.

The ignored qualification regression
`qualification_cosine_fixture_exposes_default_shortlist_code_ties` derives the
same dataset through production preparation, rotation, codebook, and packing.
It proves that the authoritative exact top-10 keys are indices 3313 through
3331 at odd intervals, all ten have the query's exact packed code, and 222
lower-key records have that same packed code. Because approximate candidates
are totally ordered by `(approximate_score, key)`, all 222 lower keys outrank
the exact top 10 inside that tie. The frozen 80-candidate default shortlist
therefore has a deterministic recall upper bound of zero for this fixture,
independent of any scoring-formula change that preserves the packed code and
ordering contract.

The regression is reproducible with:

```sh
cargo test --release --all-features --lib \
  qualification_cosine_fixture_exposes_default_shortlist_code_ties \
  -- --ignored
```

## Rerank-window diagnostics

| Records | Metric | TQ recall, 8× → 16× | PQ recall, 8× → 16× | TQ/PQ 16× p95 |
| ---: | --- | ---: | ---: | ---: |
| 10K | Cosine | 0.00 → 0.00 | 1.00 → 1.00 | 6.06× |
| 10K | Inner product | 1.00 → 1.00 | 0.00 → 1.00 | 5.05× |
| 100K | Cosine | 0.00 → 1.00 | 0.00 → 0.00 | 6.90× |
| 100K | Inner product | 0.00 → 1.00 | 0.00 → 0.00 | 5.11× |

The approved 16× window corrects both sampled 100K product-like rows and does
not correct the deterministic 10K cosine collision. Increasing the frozen
default from 8× to 16× is therefore not a general remedy.

## Build results

| Records | Metric | TQ/PQ build time, 1 worker | 2 workers | 4 workers |
| ---: | --- | ---: | ---: | ---: |
| 10K | Squared L2 | 36.85% | 35.89% | 31.05% |
| 10K | Cosine | 31.13% | 28.42% | 22.33% |
| 10K | Inner product | 28.72% | 36.15% | 32.50% |
| 100K | Squared L2 | 33.62% | 33.88% | 27.84% |
| 100K | Cosine | 34.49% | 35.49% | 29.10% |
| 100K | Inner product | 47.72% | 40.29% | 47.38% |

Every sampled TurboQuant build is faster than its paired PQ build and every
worker count produces an identical manifest and canonical logical statistics
within a cell. These development-host observations do not replace the pinned
100K × 768/1536 production-host gate.

## Gate disposition

- Squared L2 passes the sampled forced-backend recall floor at both scales.
- Inner product passes at 10K with the default window and at 100K with 16×,
  but fails the 100K default row.
- Cosine fails both default rows. The 10K failure is unavoidable under the
  frozen packed-code, key-tie, and 8× shortlist contract.
- Every sampled default row fails the `Auto` p95 ceiling, and both sampled
  scales fail the comparative-size gate.
- Eleven current-schema cells are characterized at one implementation
  revision. All 45,360 cells still require one final frozen-revision run; the
  pinned-host, global binding-inventory, and legal/patent gates also remain
  open.

TurboQuant remains available only through explicit backend selection. `Auto`
and a forced-backend GA claim remain closed. Closing the 10K cosine gate
requires an approved contract change: a larger bounded shortlist, additional
query-comparable routing information with a new wire/objective identifier, or
a replacement deterministic fixture whose neighbor separations are suitable
for the frozen four-bit MSE objective. The measured gate must not be weakened
or averaged away.
