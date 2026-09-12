# TurboQuant Schema-v3 Partial Qualification Evidence

This report retains ten real cells from the corrected TurboQuant production
matrix: the default four-bit, 8× rerank path at 10K and 100K records, 768
dimensions, and every supported metric, plus matching 16× cosine and
inner-product diagnostics. These cells use the unique,
architecture-stable schema-v3 dataset. They are partial evidence, not a
complete production qualification, and they keep `Auto` selection disabled.

## Environment and contract

- Date: 2026-09-12
- Revisions: default 8× squared-L2 cells at
  `f08e7dfa604bcc4bfd1c406d4c5e4dfcc8550d09`, default 8× cosine and
  inner-product cells at `43a6c55564df2750e77298542a490dec9baa6ff9`, and
  16× diagnostics at
  `8966204c11d3bf73173ea2f1354002344582d5b2`
- Qualification schema: `prolly-turboquant-qualification-v3`
- Benchmark schema: 3
- Dataset: `linear-mod-2000003-v2`
- Compiler: Rust 1.97.0, LLVM 22.1.6
- Target: `aarch64-apple-darwin`
- Machine: `Haipings-Mac-Studio.local`
- Store/cache: in-memory, warm bounded search runtime
- Records/dimensions/metrics: 10,000 and 100,000 × 768; squared L2,
  cosine, and inner product
- Query: `k=10`, all records eligible, four-bit codes, rerank multiplier 8
- Build workers: 1, 2, and 4
- Search samples: 30 per implementation

The ten retained directories contain the immutable manifest, raw CSV, stderr
log, completion state, and content digest for one stable full-matrix shard.
An exact `--resume` invocation revalidated every directory without rerunning
its benchmark.

## Recall and search results

| Records | Metric | TQ recall@10 | PQ recall@10 | TQ median | PQ median | TQ warm p95 | PQ warm p95 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 10K | Squared L2 | 1.00 | 1.00 | 9,078.458 µs | 1,362.500 µs | 9,467.416 µs | 1,555.000 µs |
| 10K | Cosine | 0.00 | 0.50 | 8,839.292 µs | 1,418.292 µs | 9,126.667 µs | 1,648.667 µs |
| 10K | Inner product | 0.00 | 0.00 | 8,633.792 µs | 1,382.125 µs | 9,063.250 µs | 1,517.000 µs |
| 100K | Squared L2 | 1.00 | 0.00 | 112,362.791 µs | 21,696.166 µs | 116,111.541 µs | 40,152.750 µs |
| 100K | Cosine | 0.00 | 0.00 | 117,354.375 µs | 13,170.208 µs | 121,304.333 µs | 14,027.917 µs |
| 100K | Inner product | 0.00 | 0.00 | 120,492.667 µs | 12,583.125 µs | 130,040.000 µs | 13,219.042 µs |

The squared-L2 rows validate the corrected distance calculation. A quantized
Lloyd–Max reconstruction is not exactly unit length, so its squared norm must
be derived from the packed codes instead of being replaced by one. With that
correction, TurboQuant L2 recall moved from 0.00 in the pre-fix v3 diagnostic
to 1.00 at both retained scales. Cosine source-scale invariance is separately
enforced by regression test, but its fixed 80-candidate shortlist still misses
the exact neighbors on this dataset. Inner product has the same shortlist
quality failure.

TurboQuant search is 6.66×/6.23×/6.25× PQ at the median and
6.09×/5.54×/5.97× at p95 for 10K L2/cosine/inner product. At 100K it is
5.18×/8.91×/9.58× at the median and 2.89×/8.65×/9.84× at p95.
Absolute and paired ratios are individual development-host observations; the
logical counters and gate disposition are deterministic.

The corrected L2 scorer initially decoded every packed code twice: once for
the query dot product and again for the reconstruction norm. The retained
current scorer precomputes the finite centroid-square table once per query and
accumulates both values in the same coordinate-ordered packed-code traversal.
Relative to the retained two-pass baseline, this reduces TurboQuant L2 median
search time by 44.52% at 10K and 41.63% at 100K, and p95 by 43.90% and 45.70%,
without changing score bits, recall, sidecar bytes, candidates, I/O,
completion, rerank work, or any logical statistic.

The TurboQuant sidecar is 3,975,264 bytes at 10K and 39,757,465 bytes at
100K, respectively 22.79× and 30.43× the matching PQ sidecar. The 100K
working set exceeds the default 32 MiB TurboQuant runtime-cache partition, so
physical reads after warmup are valid bounded-cache behavior.

## Rerank-window diagnostic

Doubling the approved rerank multiplier from 8× to 16× does not provide a
general quality correction:

| Records | Metric | TQ recall, 8× → 16× | PQ recall, 8× → 16× | TQ/PQ 16× p95 |
| ---: | --- | ---: | ---: | ---: |
| 10K | Cosine | 0.00 → 0.00 | 0.50 → 0.50 | 3.88× |
| 10K | Inner product | 0.00 → 0.00 | 0.00 → 1.00 | 7.43× |
| 100K | Cosine | 0.00 → 0.50 | 0.00 → 0.00 | 11.44× |
| 100K | Inner product | 0.00 → 0.00 | 0.00 → 0.00 | 10.61× |

The approved 160-candidate window still fails every TurboQuant recall floor
in this diagnostic and increases rerank work. This rules out changing the
frozen default from 8× to 16× as a sufficient cosine/inner-product remedy.

## Build results

| Records | Metric | TQ/PQ build time, 1 worker | 2 workers | 4 workers |
| ---: | --- | ---: | ---: | ---: |
| 10K | Squared L2 | 30.91% | 25.32% | 23.14% |
| 10K | Cosine | 31.11% | 31.90% | 45.59% |
| 10K | Inner product | 31.01% | 24.96% | 24.31% |
| 100K | Squared L2 | 31.94% | 36.71% | 31.68% |
| 100K | Cosine | 38.24% | 36.86% | 31.32% |
| 100K | Inner product | 33.60% | 35.78% | 40.90% |

Every sampled build is faster than its paired PQ build. All worker counts
produce identical manifests and canonical logical statistics within a cell.
These development-host observations do not replace the required pinned-host
100K × 768/1536 build gate.

## Gate disposition

- The two squared-L2 cells pass the forced-backend recall floor: recall is at
  least 0.95 and does not trail equal-shortlist PQ by more than 0.01.
- All four cosine and inner-product cells fail the 0.95 recall floor. A row
  cannot be rescued by averaging it with a passing metric or scale.
- All six cells fail `Auto`: the L2 rows exceed the 1.25× PQ warm-p95 ceiling,
  while cosine and inner product fail both recall and latency requirements.
- Both sampled scales fail the comparative-size gate; the TurboQuant sidecar
  is larger than PQ rather than at least 25% smaller.
- Ten of 45,360 schema-v3 matrix cells are retained. The other 45,350 cells,
  complete-shard summarization, pinned production-host rerun, global binding
  inventory, and legal/patent disposition remain open.

TurboQuant therefore remains available only through explicit backend
selection. `Auto` and a forced-backend GA claim remain closed.
