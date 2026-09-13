# TurboQuant 10K Qualification Evidence

This report retains three real, schema-v2 10K-record qualification cells for
the native TurboQuant accelerator: one for every supported metric. They
advance scale evidence beyond the complete 1K bounded tier, but they are not
the complete production matrix and do not qualify `Auto` selection.

## Environment and contract

- Date: 2026-09-12
- Benchmark revisions: L2 at
  `92be88dc3933259f4d75dffce91645a7ce67afb9`; cosine and inner product at
  `915bbbaa55f8f6ab8fde12a68ad57fb924ebce2c`
- Compiler: Rust 1.97.0, LLVM 22.1.6
- Target: `aarch64-apple-darwin`
- Machine: `Haipings-Mac-Studio.local`
- Store/cache: in-memory, warm search cache
- Records/dimensions/metrics: 10,000 × 768, squared L2, cosine, and inner
  product
- Query: `k=10`, all records eligible, four-bit codes, rerank multiplier 8
- Build workers: 1, 2, and 4
- Search samples: 30 per implementation
- Qualification contracts: schema v2 full-matrix shards `131629/1000000`
  (L2), `79516/1000000` (cosine), and `513486/1000000` (inner product), each
  with exactly one selected cell from the frozen 45,360-cell matrix

Each resumable shard—including its contract, raw CSV, exact digest, compile
record, and terminal status—is retained under the corresponding
`performance-results/proximity-turboquant/qualification-10k-d768-<metric>-k10-all-b4-r8-warm-sync-shard/`
directory. The benchmarks produced no stderr. Exact `--resume` runs accepted
all three retained states without rerunning a cell.

## Results

| Measure | TurboQuant | PQ | Interpretation |
| --- | ---: | ---: | --- |
| Recall@10 | 1.00 | 0.60 | TurboQuant clears the 0.95 forced-backend recall floor for this cell |
| Median search | 22,489.166 µs | 8,236.750 µs | TurboQuant is 2.73× slower |
| Warm p95 search | 23,566.625 µs | 8,966.542 µs | TurboQuant is 2.63× slower |
| Derived sidecar | 3,975,264 bytes | 174,439 bytes | TurboQuant is 22.79× larger |
| One-worker build | 134,577.167 µs | 425,243.459 µs | TurboQuant uses 31.65% of PQ build time |
| Two-worker build | 71,899.125 µs | 287,599.334 µs | TurboQuant uses 25.00% of PQ build time |
| Four-worker build | 54,231.250 µs | 219,576.375 µs | TurboQuant uses 24.70% of PQ build time |

TurboQuant reranked 80 authoritative candidates and reported mean squared
reconstruction error `0.00942432544887716`. All worker counts produced the
same manifest and canonical logical build statistics; the runner rejected any
missing or duplicated operation row and confirmed zero warm-cache physical
reads.

The matching cosine and inner-product cells produced:

| Metric | TQ recall | PQ recall | TQ median | PQ median | TQ warm p95 | PQ warm p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cosine | 1.00 | 0.80 | 9,594.500 µs | 1,643.000 µs | 10,214.666 µs | 2,233.750 µs |
| Inner product | 1.00 | 0.40 | 9,982.750 µs | 1,533.084 µs | 10,693.750 µs | 1,968.167 µs |

Both additional metrics retained the same 3,975,264-byte TurboQuant sidecar
and 174,439-byte PQ sidecar. TurboQuant was 5.84×/6.51× slower at the median
and 4.57×/5.43× slower at p95 for cosine/inner product, respectively. Its
one-, two-, and four-worker build times were 30.71%/24.13%/19.86% of PQ for
cosine and 30.34%/25.22%/23.92% for inner product. Each cell reranked 80
authoritative candidates, produced identical canonical manifests and logical
build statistics across worker counts, and recorded zero warm-cache physical
reads.

## Disposition

- These individual forced-backend recall cells pass: TurboQuant recall is at
  least 0.95 and is not below the equal-shortlist PQ result for every metric.
- The cells demonstrate a substantial training-free build-time advantage, but
  the design's build gate is specifically 100K × 768/1536 on a pinned
  production host, so this observation does not satisfy that gate.
- The warm-p95 and comparative-value gates do not pass in these cells. Higher
  recall does not offset 2.63× to 5.43× p95 latency under the approved 1.25×
  ceiling, and the sidecar is larger rather than at least 25% smaller.
- `Auto` therefore remains disabled. The remaining 10K configurations and the
  full 100K/1M, metric, `k`, eligibility, bit-width, rerank, store/cache,
  async, cross-target, and production-host matrix remain open.

These timings are a single development-machine observation, not a production
capacity claim.
