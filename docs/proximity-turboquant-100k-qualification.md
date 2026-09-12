# TurboQuant 100K Qualification Evidence

This report retains three real, schema-v2 100K-record qualification cells for
the native TurboQuant accelerator: one for every supported metric. These are
the first retained cells at the design's 100K build-gate scale. They expose a
forced-backend recall failure and do not qualify either forced-backend GA or
`Auto` selection.

## Environment and contract

- Date: 2026-09-12
- Benchmark revision: `430a744df2832f5a1d5bc927e33e1f9c4cfe0d1a`
- Compiler: Rust 1.97.0, LLVM 22.1.6
- Target: `aarch64-apple-darwin`
- Machine: `Haipings-Mac-Studio.local`
- Store/cache: in-memory, warm bounded search runtime
- Records/dimensions/metrics: 100,000 × 768, squared L2, cosine, and inner
  product
- Query: `k=10`, all records eligible, four-bit codes, rerank multiplier 8
- Build workers: 1, 2, and 4
- Search samples: 30 per implementation
- Qualification contracts: schema v2 full-matrix shards `472514/1000000`
  (L2), `550539/1000000` (cosine), and `626636/1000000` (inner product), each
  with exactly one selected cell from the frozen 45,360-cell matrix

Each resumable shard—including its contract, raw CSV, exact digest, compile
record, stderr log, and terminal status—is retained under the corresponding
`performance-results/proximity-turboquant/qualification-100k-d768-<metric>-k10-all-b4-r8-warm-sync-shard/`
directory. The benchmarks produced no stderr. Exact `--resume` runs accepted
all three retained states without rerunning a cell.

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

## Disposition

- The required four-bit forced-backend recall floor is 0.95 for every checked
  dataset/metric row. All three cells fail that floor; L2 and inner product
  also trail equal-shortlist PQ by more than the allowed 0.01.
- The warm-p95 `Auto` ceiling is 1.25× PQ at recall of at least 0.95. These
  cells fail both prerequisites.
- The sidecar does not meet the comparative size criterion, while the build
  advantage alone cannot override recall and search-latency failures.
- Forced-backend GA qualification and `Auto` therefore remain disabled. The
  result is retained as a regression target for candidate-quality work.
- The other record counts, 1536-dimension build-gate cells, `k`, eligibility,
  bit-width, rerank, store/cache, async, cross-target, and pinned-production-
  host configurations remain open.

These timings are a single development-machine observation, not a production
capacity claim. The recall failures are deterministic output-quality evidence
and may not be averaged away by successful smaller cells.
