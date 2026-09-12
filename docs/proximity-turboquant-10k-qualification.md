# TurboQuant 10K Qualification Evidence

This report retains one real, schema-v2 10K-record qualification cell for the
native TurboQuant accelerator. It advances scale evidence beyond the complete
1K bounded tier, but it is not the complete production matrix and does not
qualify `Auto` selection.

## Environment and contract

- Date: 2026-09-12
- Benchmark revision: `92be88dc3933259f4d75dffce91645a7ce67afb9`
- Compiler: Rust 1.97.0, LLVM 22.1.6
- Target: `aarch64-apple-darwin`
- Machine: `Haipings-Mac-Studio.local`
- Store/cache: in-memory, warm search cache
- Records/dimensions/metric: 10,000 × 768, squared L2
- Query: `k=10`, all records eligible, four-bit codes, rerank multiplier 8
- Build workers: 1, 2, and 4
- Search samples: 30 per implementation
- Qualification contract: schema v2 full-matrix shard `131629/1000000`, with
  exactly one selected cell from the frozen 45,360-cell matrix

The resumable shard—including its contract, raw CSV, exact digest, compile
record, and terminal status—is retained under
`performance-results/proximity-turboquant/qualification-10k-d768-l2-k10-all-b4-r8-warm-sync-shard/`.
The benchmark produced no stderr. An exact `--resume` run accepted the retained
state without rerunning the cell.

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

## Disposition

- This individual forced-backend recall cell passes: TurboQuant recall is at
  least 0.95 and is not below the equal-shortlist PQ result.
- The cell demonstrates a substantial training-free build-time advantage, but
  the design's build gate is specifically 100K × 768/1536 on a pinned
  production host, so this observation does not satisfy that gate.
- The warm-p95 and comparative-value gates do not pass in this cell. Higher
  recall does not offset 2.63× latency under the approved 1.25× ceiling, and
  the sidecar is larger rather than at least 25% smaller.
- `Auto` therefore remains disabled. The remaining 10K configurations and the
  full 100K/1M, metric, `k`, eligibility, bit-width, rerank, store/cache,
  async, cross-target, and production-host matrix remain open.

These timings are a single development-machine observation, not a production
capacity claim.
