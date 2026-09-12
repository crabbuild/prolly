# TurboQuant 1K Qualification Evidence

This report retains the bounded 1K-record qualification tier for the native
TurboQuant accelerator. It is reproducible evidence for the default
four-bit, rerank-eight, unfiltered `k=10` path. It is not the production GA
matrix and does not qualify `Auto` selection.

## Environment and inputs

- Date: 2026-09-12
- Benchmark revision: `d73477244539e21139003d4ca38a5c1f64528f9c`
- Compiler: Rust 1.97.0, LLVM 22.1.6
- Target: `aarch64-apple-darwin`
- Store/cache: in-memory, warm search cache
- Records: 1,000 deterministic synthetic records
- Dimensions: 128, 200, 768, 1536, and 3072
- Metrics: squared L2, cosine, and inner product
- Search samples: 30 per implementation and dimension
- Query: `k=10`, all records eligible, four-bit TurboQuant, rerank multiplier 8

Raw benchmark output is retained in:

- `performance-results/proximity-turboquant/qualification-1k-l2-k10-all-b4-r8-arm64.csv`
- `performance-results/proximity-turboquant/qualification-1k-cosine-k10-all-b4-r8-arm64.csv`
- `performance-results/proximity-turboquant/qualification-1k-inner-product-k10-all-b4-r8-arm64.csv`

## Results

Every one of the 15 TurboQuant metric/dimension rows produced recall@10 of
`1.0`; the equal-shortlist PQ comparison also produced `1.0`. This bounded
tier therefore clears both recall checks: recall is at least `0.95`, and no
TurboQuant row is more than `0.01` below PQ.

The storage numbers below exclude the authoritative proximity-map closure and
count only derived manifest and code-tree objects. Search ratios compare the
30-sample TurboQuant scalar median with the PQ median; the range is across the
three metrics.

| Dimensions | TurboQuant sidecar bytes | PQ sidecar bytes | TQ/PQ median search ratio | TQ recall range | PQ recall range |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 128 | 76,699 | 20,914 | 2.52–2.57× | 1.0–1.0 | 1.0–1.0 |
| 200 | 112,699 | 25,522 | 2.94–3.16× | 1.0–1.0 | 1.0–1.0 |
| 768 | 397,699 | 61,874 | 6.78–7.01× | 1.0–1.0 | 1.0–1.0 |
| 1536 | 781,699 | 111,034 | 8.78–9.25× | 1.0–1.0 | 1.0–1.0 |
| 3072 | 1,549,699 | 209,338 | 9.53–11.78× | 1.0–1.0 | 1.0–1.0 |

The exact rerank path explains the perfect result on this small deterministic
fixture: TurboQuant admits and reranks 80 candidates for each search. This is
useful correctness evidence, but it is not evidence that the same recall will
hold at production scale or under selective filters.

## Disposition

- Bounded 1K default-path recall criterion: passed for all 15 rows.
- Production forced-backend qualification: still open. The retained matrix
  must also cover 10K/100K/1M scale, all requested `k`, eligibility, bit-width,
  rerank, cache/store, async/batched, and WASM configurations.
- `Auto` qualification: failed to establish an advantage. On this tier,
  TurboQuant search and sidecar size are both worse than PQ. The production
  100K build and warm-p95 gates have not been run. `Auto` therefore remains
  disabled.

These values are a single development-machine observation, not a capacity
claim. Timing comparisons require retained production-host runs before a
release decision.
