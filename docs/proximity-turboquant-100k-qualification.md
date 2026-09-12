# TurboQuant 100K Qualification Evidence

This report retains nine real, schema-v2 100K-record qualification cells for
the native TurboQuant accelerator. The frozen four-bit, 8× default is covered
at both design-mandated build dimensions across every metric. Three additional
768-dimensional cells isolate the approved 16× rerank setting. These are the
first retained cells at the design's 100K build-gate scale. They expose
forced-backend recall failures and do not qualify either forced-backend GA or
`Auto` selection.

> **Superseded qualification dataset:** A subsequent audit found that the v2
> benchmark generator repeated complete vectors every 20,003 records and used
> architecture-width `usize` wrapping. At 100K, each vector therefore appears
> about five times and Recall@10 changes in coarse duplicate groups. These
> rows remain reproducible performance and rank-depth diagnostics, but they do
> not count toward the corrected, architecture-stable schema-v3 production
> matrix.

## Environment and contract

- Date: 2026-09-12
- Benchmark revisions: 768-dimensional 8× default at
  `430a744df2832f5a1d5bc927e33e1f9c4cfe0d1a`; 768-dimensional 16× and
  1536-dimensional 8× cells at
  `d5a24d63fd14a8fae454b16aee67050bdf0db8fc`
- Compiler: Rust 1.97.0, LLVM 22.1.6
- Target: `aarch64-apple-darwin`
- Machine: `Haipings-Mac-Studio.local`
- Store/cache: in-memory, warm bounded search runtime
- Records/dimensions/metrics: 100,000 × 768/1536, squared L2, cosine, and inner
  product
- Query: `k=10`, all records eligible, four-bit codes, rerank multiplier 8;
  the 768-dimensional diagnostic also covers multiplier 16
- Build workers: 1, 2, and 4
- Search samples: 30 per implementation
- Qualification contracts: schema v2 full-matrix shards, each with exactly one
  selected cell from the frozen 45,360-cell matrix. The 768-dimensional 8×
  shards are `472514`, `550539`, and `626636`; the matching 16× shards are
  `222470`, `198811`, and `729950`; and the 1536-dimensional 8× shards are
  `2322889/5000000`, `488292/1000000`, and `655396/1000000`, in L2, cosine,
  and inner-product order. Unqualified indices use a shard count of 1,000,000.

Each resumable shard—including its contract, raw CSV, exact digest, compile
record, stderr log, and terminal status—is retained under the corresponding
`performance-results/proximity-turboquant/qualification-100k-<configuration>-warm-sync-shard/`
directory. The benchmarks produced no stderr. Exact `--resume` runs accepted
all nine retained states without rerunning a cell.

## Recall and search results

| Metric | TQ recall@10 | PQ recall@10 | TQ median | PQ median | TQ warm p95 | PQ warm p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Squared L2 | 0.00 | 0.10 | 149,835.334 µs | 13,000.125 µs | 156,191.667 µs | 19,097.333 µs |
| Cosine | 0.50 | 0.00 | 150,906.250 µs | 13,304.042 µs | 166,749.333 µs | 16,702.000 µs |
| Inner product | 0.00 | 0.20 | 147,554.958 µs | 13,248.708 µs | 153,081.417 µs | 16,369.083 µs |

TurboQuant is 11.53×/11.34×/11.14× slower at the median and
8.18×/9.98×/9.35× slower at p95 for L2/cosine/inner product, respectively.
Every search scanned 100,000 codes and reranked 80 authoritative candidates.

The 39,757,465-byte TurboQuant sidecar is 30.43× the 1,306,641-byte PQ
sidecar. It exceeds the default 32 MiB TurboQuant runtime-cache partition, so
the shared, warmed runtime legitimately performs physical reads as its
authenticated working set is evicted. This finding exposed and corrected a
runner defect that had incorrectly equated `warm` with an unbounded,
fully-resident cache. Cold cells continue to require physical I/O and every
counter must be finite and nonnegative.

The 1536-dimensional default cells produced:

| Metric | TQ recall@10 | PQ recall@10 | TQ median | PQ median | TQ warm p95 | PQ warm p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Squared L2 | 0.00 | 0.00 | 288,257.083 µs | 12,924.709 µs | 322,335.584 µs | 13,725.125 µs |
| Cosine | 0.00 | 0.00 | 286,003.000 µs | 12,836.000 µs | 307,674.667 µs | 13,390.209 µs |
| Inner product | 0.00 | 0.20 | 264,753.708 µs | 13,038.833 µs | 276,385.750 µs | 13,512.833 µs |

TurboQuant is 22.30×/22.28×/20.31× slower at the median and
23.49×/22.98×/20.45× slower at p95 for 1536-dimensional L2/cosine/inner
product, respectively. The 78,157,465-byte TurboQuant sidecar is 57.65× the
1,355,801-byte PQ sidecar. Every search reranked 80 authoritative candidates.

## Rerank-window diagnostic

At 768 dimensions, increasing the approved rerank multiplier from the frozen
8× default to 16× changes the deterministic result as follows:

| Metric | TQ recall 8× → 16× | PQ recall 8× → 16× | TQ 16× p95 | PQ 16× p95 |
| --- | ---: | ---: | ---: | ---: |
| Squared L2 | 0.00 → 1.00 | 0.10 → 0.20 | 164,063.375 µs | 13,821.584 µs |
| Cosine | 0.50 → 0.50 | 0.00 → 0.10 | 168,200.167 µs | 13,116.750 µs |
| Inner product | 0.00 → 0.00 | 0.20 → 0.20 | 164,023.208 µs | 14,303.750 µs |

The larger 160-candidate window restores L2 recall but does not improve
TurboQuant cosine or inner-product recall. It therefore rules out a universal
shortlist-only correction within the approved fixed settings. The 16× p95 is
11.87×/12.82×/11.47× PQ for L2/cosine/inner product.

## Build results

| Metric | Workers | TurboQuant | PQ | TQ as percentage of PQ |
| --- | ---: | ---: | ---: | ---: |
| Squared L2 | 1 / 2 / 4 | 1.339 / 0.884 / 0.652 s | 7.100 / 4.393 / 3.727 s | 18.86% / 20.13% / 17.50% |
| Cosine | 1 / 2 / 4 | 1.997 / 2.671 / 1.168 s | 3.531 / 2.617 / 2.182 s | 56.55% / 102.07% / 53.50% |
| Inner product | 1 / 2 / 4 | 1.369 / 0.898 / 0.679 s | 6.866 / 4.325 / 2.836 s | 19.93% / 20.75% / 23.95% |

All worker counts produced the same accelerator manifest and canonical
logical build statistics within each cell. TurboQuant built faster in eight
of nine development-host worker/metric observations. The isolated two-worker
cosine result was 2.07% slower than PQ and should be treated as host timing
evidence, not as a determinism failure.

For the 1536-dimensional default cells, build results were:

| Metric | Workers | TurboQuant | PQ | TQ as percentage of PQ |
| --- | ---: | ---: | ---: | ---: |
| Squared L2 | 1 / 2 / 4 | 2.810 / 2.395 / 1.831 s | 8.287 / 5.334 / 4.534 s | 33.90% / 44.90% / 40.39% |
| Cosine | 1 / 2 / 4 | 2.821 / 1.906 / 1.337 s | 7.604 / 5.364 / 4.717 s | 37.10% / 35.53% / 28.35% |
| Inner product | 1 / 2 / 4 | 2.564 / 1.633 / 1.194 s | 7.425 / 5.286 / 4.717 s | 34.53% / 30.90% / 25.31% |

TurboQuant built faster than PQ for every 1536-dimensional worker/metric
observation. As required, timing does not alter the byte-identical worker
results.

## Disposition

- The required four-bit forced-backend recall floor is 0.95 for every checked
  dataset/metric row. All six default cells fail that floor; 768-dimensional
  L2 and inner product plus 1536-dimensional inner product also trail
  equal-shortlist PQ by more than the allowed 0.01.
- The warm-p95 `Auto` ceiling is 1.25× PQ at recall of at least 0.95. These
  cells fail both prerequisites.
- The sidecar does not meet the comparative size criterion, while the build
  advantage alone cannot override recall and search-latency failures.
- Forced-backend GA qualification and `Auto` therefore remain disabled. The
  result is retained as a regression target for candidate-quality work.
- The other record counts, `k`, eligibility, bit-width, rerank, store/cache,
  async, cross-target, and pinned-production-host configurations remain open.

These timings are a single development-machine observation, not a production
capacity claim. The recall failures are deterministic output-quality evidence
and may not be averaged away by successful smaller cells.
