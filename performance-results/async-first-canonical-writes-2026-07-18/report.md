# Async-First Canonical Writes — Interim Evidence

Date: 2026-07-18  
Commit measured: `1d2e0e5`  
Machine: Apple M2 Max, 32 GiB, rustc 1.97.0

## Proven

- `AsyncProlly<S>` is now a type alias for `ProllyEngine<S>`; async mutation
  methods are engine methods, not facade forwarding methods.
- The sync `Prolly` put/delete/batch/append/parallel entry points execute the
  canonical writer with their owned engine as the write context.
- Sync canonical-root, invariant, and write-stat suites pass unchanged.
- Native async append remains canonical across the tested policies/layouts.
- Native async key-only entry-count value updates use routed/coalesced leaf
  rewrites and one atomic batch publication. The 128-entry sparse regression
  originally observed at least 76 node reads; the localized gate requires
  fewer than 16 and passes.
- Native async structural mutations and value-sensitive chunking still use the
  verified full-tree fallback. This is correct but remains a performance gap;
  this report is therefore interim, not the phase completion report.

## Sync Mutation Sentinel

Command:

```text
PROLLY_BENCH_ONLY=batch-regressions \
PROLLY_BENCH_SCALE=10000 \
cargo bench --bench prolly_bench
```

| Workload | Total | Iterations | Items/run | Median/item | p95/item |
|---|---:|---:|---:|---:|---:|
| mixed canonical batch | 57.898 ms | 20 | 1,000 | 2,808 ns | 3,700 ns |
| parallel canonical batch | 43.735 ms | 20 | 1,000 | 2,152 ns | 2,418 ns |

The engine-context cutover retains the batched key-stable rewrite gate; the
`scattered_value_updates_use_batched_canonical_rewrite` write-stat regression
passes with strict Clippy and all 488 library tests.
