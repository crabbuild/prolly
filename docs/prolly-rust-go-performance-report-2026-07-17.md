# Current Dolt Go vs Rust Prolly Performance Report

Benchmark date: 2026-07-17. Status: complete and validated.

## Executive result

This run compared Rust prolly revision
`a1e31dc403e5+src.24beb50ee9ec` with current Dolt Go revision
`9e80d3aa2cf8+runner.6bf12dc88bef`. Every scenario was a separate,
single-worker, in-memory process. All five requested base sizes—10K, 50K, 1M,
5M, and 10M—ran three complete repetitions.

Rust won 88 of 90 scenario-operation medians:

| Operation | Rust wins | Dolt Go wins | Median Go/Rust | Geometric mean | Observed Go/Rust range |
|---|---:|---:|---:|---:|---:|
| Write | 28/30 | 2/30 | 2.32x | 2.51x | 0.81x-8.81x |
| Point read | 30/30 | 0/30 | 2.59x | 2.76x | 1.71x-4.48x |
| Full range scan | 30/30 | 0/30 | 5.21x | 5.51x | 3.80x-11.23x |

The two losses are narrow, small-tree random-mutation writes:

| Base size | Rust ns/op | Dolt Go ns/op | Dolt advantage | Rust CV | Dolt CV |
|---:|---:|---:|---:|---:|---:|
| 10K | 1,774.7 | 1,430.5 | 1.24x | 0.4% | 0.4% |
| 50K | 1,741.1 | 1,557.4 | 1.12x | 0.9% | 8.7% |

These are not correctness failures: both runners produced the same operation
digest, operation count, final cardinality, exact point-read values, and strict
scan order. The 10K loss is particularly credible because both implementations
had low variance. It remains a fixed-cost/allocation optimization target.

## Scale behavior

The median ratio across the six phase/workload combinations at each size shows
that Rust's write and scan advantage generally increases with scale:

| Base size | Write Go/Rust | Point-read Go/Rust | Scan Go/Rust |
|---:|---:|---:|---:|
| 10K | 2.09x | 2.57x | 4.10x |
| 50K | 2.29x | 2.60x | 4.71x |
| 1M | 2.90x | 2.58x | 5.21x |
| 5M | 3.57x | 2.05x | 6.66x |
| 10M | 3.61x | 3.04x | 7.02x |

At 10M, Rust wins all 18 medians. The strongest write result is fresh/random:
845.2 ns/op for Rust versus 7,447.3 ns/op for Dolt, an 8.81x advantage. The
weakest 10M write result is clustered mutation: 510.9 versus 768.6 ns/op, still
1.50x in Rust's favor.

Fresh 10M point reads favor Rust by 1.72x-1.79x. Mutation append and clustered
reads favor Rust by 4.29x and 4.48x. The 10M mutation-random read and scan
medians show 4.44x and 11.23x advantages, but both implementations had very
high CV in those two groups, so their exact ratios are not stable enough for a
fine-grained claim.

## Impact relative to the July 16 Rust baseline

The current Rust runner retained the `prolly-compare-v1` logical contract. The
historical comparison first revalidated the six 10K golden digests in the July
16 raw results, then matched all 90 size/phase/workload/operation groups.

- 80 of 90 current Rust medians improved.
- The median improvement across all 90 groups was 11.0%.
- Writes improved in 27/30 groups, with a median 8.5% gain.
- Point reads improved in 25/30 groups, with a median 10.2% gain.
- Scans improved in 28/30 groups, with a median 12.3% gain.
- Every 10K, 50K, 1M, and 5M group improved. At 10M, 8 improved and 10
  regressed; the median 10M change was -0.16%.

The strongest measured gains were 42.9% for 1M fresh/random point reads, 39.5%
for 1M fresh/clustered scans, 33.5% for 50K random-mutation point reads, and
30.0% for 50K random-mutation writes.

The regressions must remain visible:

| Scenario | Operation | July 16 ns/op | Current ns/op | Regression |
|---|---|---:|---:|---:|
| 10M mutation/append | Range scan | 6.142 | 6.875 | 11.9% |
| 10M mutation/append | Write | 299.1 | 333.3 | 11.5% |
| 10M fresh/clustered | Point read | 968.6 | 1,053.0 | 8.7% |
| 10M fresh/random | Write | 784.1 | 845.2 | 7.8% |
| 10M fresh/random | Point read | 967.8 | 1,042.0 | 7.7% |
| 10M fresh/clustered | Write | 399.6 | 414.9 | 3.8% |

Four additional 10M regressions were 2.8% or smaller. These before/after
figures are same-machine process measurements, not causal proof that one source
change produced the delta. Several 10M read/scan groups are noisy, while the
append and fresh-random write regressions have current CV around 9%; both need a
longer CPU-pinned confirmation run before optimization decisions.

## Peak process memory

Peak RSS includes deterministic fixture ownership, Dolt tuple construction,
tree state, mutation staging, runtime state, and measured operations. It is not
tree-only memory. Median is across all 18 processes at a size (two phases,
three workloads, and three repetitions).

| Base size | Rust median RSS | Dolt median RSS | Rust/Dolt | Rust max | Dolt max |
|---:|---:|---:|---:|---:|---:|
| 10K | 7.0 MiB | 28.6 MiB | 0.25x | 12.2 MiB | 29.1 MiB |
| 50K | 25.2 MiB | 62.7 MiB | 0.40x | 52.6 MiB | 71.7 MiB |
| 1M | 297.4 MiB | 405.3 MiB | 0.73x | 731.0 MiB | 1,236.2 MiB |
| 5M | 1.41 GiB | 1.86 GiB | 0.75x | 3.61 GiB | 7.20 GiB |
| 10M | 2.81 GiB | 3.69 GiB | 0.76x | 7.78 GiB | 18.22 GiB |

Current Rust used less median RSS at every size. The demanding random-mutation
path still deserves attention: Rust's median was 0.71 GiB at 1M, 3.61 GiB at
5M, and 7.21 GiB at 10M. Current Dolt was higher at 0.84, 5.99, and 17.94 GiB,
respectively. Dolt fresh/random also reached 17.28 GiB median at 10M. Because
fixture and tuple ownership are included, these results identify end-to-end
memory pressure; allocation profiling is required before attributing it to the
tree alone.

## Variance and confidence

Three repetitions are sufficient to expose gross instability and compute a
median, observed range, and coefficient of variation, but not a narrow
confidence interval. Six groups exceeded 10% CV in at least one implementation:

- 10K fresh/random point read (Rust 10.1%);
- 10M mutation/append point read (Dolt 14.4%);
- 10M mutation/append scan (Dolt 22.9%);
- 10M mutation/clustered scan (Rust 16.5%);
- 10M mutation/random point read (Rust 74.1%, Dolt 42.2%); and
- 10M mutation/random scan (Rust 60.7%, Dolt 31.4%).

The two Dolt write wins have low enough variance to treat them as real targets.
Most large write comparisons also have acceptable CV, although 10M Rust append,
clustered, and random fresh/mutation writes are around 8%-9% in several groups.

## Correctness and completeness

The final output contains exactly:

- 180 successful scenario processes;
- 540 validated operation rows;
- 270 exact Rust/Go operation-run pairs;
- 90 scenario-operation median groups and 90 matched historical deltas;
- three repetitions at every size, including 5M and 10M; and
- peak RSS for every process.

The harness rejects nonzero exits, malformed CSV, missing or duplicate
repetitions, incomplete scenarios, unvalidated rows, and pair mismatches in
contract version, digest, operation count, or result cardinality. No failed row
was estimated, interpolated, or replaced.

## Environment and provenance

- Host: Apple M2 Max, 32 GiB RAM, macOS Darwin 25.5.0, arm64.
- Rust: `rustc 1.97.0`; commit
  `a1e31dc403e5237d4db0f0c3a8fc9c480a139ef4`; source SHA-256
  `24beb50ee9ece0cfa17b7fe38b372c912cc6b3e7519312cefda1e0d42ee97f30`;
  binary SHA-256
  `1156f735e5e4eca428cfd4a7b4d50691c14a355d4296392a65271971fa532564`.
- Dolt: Go `1.26.0`; commit
  `9e80d3aa2cf8ff473765f5603af32304d72c67b5`; runner SHA-256
  `6bf12dc88bef4bc40a9a47360681dcfc8f84581f73e440bb38f99eddbd9d7d8b`;
  binary SHA-256
  `3500ead7856bd10a77152f09ad9b73ac9abb995de12f38c05519f95bf206d8b1`.
- Worker limits: `RAYON_NUM_THREADS=1`, `GOMAXPROCS=1`.
- Timed storage: each product's native in-memory store and default persisted
  encoding/chunking policy.

Reproduction command:

```sh
DOLT_REV=9e80d3aa2cf8ff473765f5603af32304d72c67b5 \
BENCH_OUT=performance-results/dolt-current-rust-canonical-2026-07-17 \
BENCH_SIZES='10000 50000 1000000 5000000 10000000' \
BENCH_RUNS=3 BENCH_LARGE_RUNS=3 \
  scripts/run_prolly_comparison.sh
```

Evidence:

- [normalized results](../performance-results/dolt-current-rust-canonical-2026-07-17/results.csv)
- [median/variance/RSS summary](../performance-results/dolt-current-rust-canonical-2026-07-17/summary.csv)
- [generated current report](../performance-results/dolt-current-rust-canonical-2026-07-17/report.md)
- [historical delta](../performance-results/dolt-current-rust-canonical-2026-07-17/historical-delta.csv)
- [provenance manifest](../performance-results/dolt-current-rust-canonical-2026-07-17/manifest.txt)

## What to harden next

1. **Remove small random-mutation fixed overhead.** Profile 10K and 50K
   canonical batch setup, sorting, routing, and allocation counts. Accept a
   change only if it eliminates the two Dolt write wins without regressing the
   scale-efficient 1M-10M path.
2. **Gate the 10M regressions.** Repeat the six largest regressions with at
   least five CPU-pinned, thermally stable runs. Treat append-write and
   fresh-random-write as the clearest candidates because read/scan variance is
   much higher.
3. **Reduce random-mutation memory.** Rust is now below current Dolt, but 7.2
   GiB median at 10M is still expensive. Profile simultaneous ownership of the
   base fixture, mutation vector, sorting scratch, old/new nodes, and read
   accelerators; shorten lifetimes or stream stages only after allocation data
   identifies the peak.
4. **Preserve the cross-scale gate.** Every candidate optimization should rerun
   10K, 50K, 1M, 5M, and 10M append/random/clustered writes plus reads/scans.
   Reject changes that move cost into another workload or increase RSS without
   a justified latency gain.
5. **Add complementary evidence.** Measure cold-cache reads, selective ranges,
   mixed read/write sequences, value-size buckets, allocation counts, and a
   Linux CPU-pinned run. Keep the current digest/cardinality gates unchanged.

## Conclusion

For this exact native, warm, single-worker, in-memory contract, Rust is the
faster prolly implementation overall: 88/90 wins, all read and scan wins, and a
performance advantage that generally grows with scale. The implementation is
not flawless: Dolt still wins two small random-mutation writes, 10M shows ten
historical regressions, and the largest random-mutation process remains memory
heavy. The correct next move is targeted profiling and cross-scale regression
gating—not a broad algorithm rewrite based on aggregate wins.
