# Rust vs Dolt Go Prolly Tree Performance Report

Benchmark date: 2026-07-16. Status: complete and validated.

## Executive result

This run compared the Rust prolly tree at `b087770d79b5+src.b85a0408b624`
with Dolt Go at `6b2372c7d4de+runner.0eeaa05ba605`. Every scenario used
single-worker, process-isolated, in-memory storage and three repetitions at
10K, 50K, 1M, 5M, and 10M base records.

Rust won 88 of 90 median comparisons:

| Operation | Rust wins | Dolt Go wins | Geometric-mean Rust speedup | Observed median range |
|---|---:|---:|---:|---:|
| Write | 28/30 | 2/30 | 4.26x | 0.83x-52.14x |
| Point read | 30/30 | 0/30 | 3.16x | 1.68x-5.75x |
| Full range scan | 30/30 | 0/30 | 4.98x | 3.41x-6.77x |

The requested 1.5x-2x point-read objective is partly exceeded and partly at
its lower bound:

- All 30 point-read scenario medians exceed 1.5x; 23 of 30 reach at least 2x.
- All 90 paired point-read repetitions favor Rust by at least 1.56x.
- The weakest median is 1.68x for the 10M fresh-random tree.
- The seven medians below 2x are the 1M fresh-random case and all fresh
  workloads at 5M and 10M. These remain the primary read-performance target.
- Mutation append and clustered point reads are especially strong at scale:
  4.97x-5.75x at 5M-10M.

The result is not uniformly favorable. Dolt Go wins the 30%-mutation random
write at 10K by 1.21x and at 50K by 1.10x. The same workload is effectively
tied at 1M (Rust 1.02x faster), then favors Rust at 5M (1.77x) and 10M
(1.99x). Rust also uses more peak process memory for random mutation at 1M,
5M, and 10M, despite using less memory for the other large workload patterns.

## Read-performance interpretation

Point reads now clear the 1.5x floor across every measured workload and every
individual repetition. The final implementation, which includes the packed
read path and zero-copy callback surface, has a real measured advantage,
including for a large random working set. The narrower margin on fresh 5M-10M
trees is consistent with a larger routing/search working set and less useful
locality, but the benchmark does not isolate those causes; profiling must
confirm them.

Full scans are the clearest Rust win. Rust remains near 5-7 ns per returned
entry while Dolt Go rises from about 21 ns at 10K to about 40-42 ns at
5M-10M. The speedup therefore increases with scale and reaches 6.61x-6.77x in
all 10M scenarios.

These figures are warm, in-memory measurements. They establish the performance
of the tree and decoding paths after an untimed point-read warm pass; they do
not establish cold-cache, storage-I/O, or multi-worker behavior.

## Complete median matrix

`Go/Rust` is Dolt Go ns/op divided by Rust ns/op. Values above 1.0 favor Rust.
Each value is the median of three process-isolated runs.

### Write

| Size | Phase | Workload | Rust ns/op | Dolt Go ns/op | Go/Rust | Winner |
|---:|---|---|---:|---:|---:|---|
| 10K | fresh | append | 298.4 | 755.7 | 2.53x | Rust |
| 10K | fresh | random | 507.9 | 1094.4 | 2.15x | Rust |
| 10K | fresh | clustered | 347.1 | 764.3 | 2.20x | Rust |
| 10K | mutation | append | 328.4 | 1168.5 | 3.56x | Rust |
| 10K | mutation | random | 1996.9 | 1653.9 | 0.83x | Dolt Go |
| 10K | mutation | clustered | 611.2 | 859.5 | 1.41x | Rust |
| 50K | fresh | append | 293.9 | 891.4 | 3.03x | Rust |
| 50K | fresh | random | 466.7 | 1484.3 | 3.18x | Rust |
| 50K | fresh | clustered | 342.3 | 956.8 | 2.80x | Rust |
| 50K | mutation | append | 309.3 | 1146.6 | 3.71x | Rust |
| 50K | mutation | random | 2488.7 | 2258.1 | 0.91x | Dolt Go |
| 50K | mutation | clustered | 525.5 | 942.9 | 1.79x | Rust |
| 1M | fresh | append | 295.3 | 2038.7 | 6.90x | Rust |
| 1M | fresh | random | 600.3 | 2439.4 | 4.06x | Rust |
| 1M | fresh | clustered | 393.9 | 1641.3 | 4.17x | Rust |
| 1M | mutation | append | 305.2 | 1629.3 | 5.34x | Rust |
| 1M | mutation | random | 2497.1 | 2550.1 | 1.02x | Rust |
| 1M | mutation | clustered | 538.8 | 1275.3 | 2.37x | Rust |
| 5M | fresh | append | 285.7 | 7048.0 | 24.67x | Rust |
| 5M | fresh | random | 778.5 | 5460.1 | 7.01x | Rust |
| 5M | fresh | clustered | 403.4 | 4034.7 | 10.00x | Rust |
| 5M | mutation | append | 301.2 | 2797.3 | 9.29x | Rust |
| 5M | mutation | random | 2388.9 | 4221.7 | 1.77x | Rust |
| 5M | mutation | clustered | 532.5 | 2679.5 | 5.03x | Rust |
| 10M | fresh | append | 284.3 | 14822.6 | 52.14x | Rust |
| 10M | fresh | random | 784.1 | 9576.1 | 12.21x | Rust |
| 10M | fresh | clustered | 399.6 | 7943.8 | 19.88x | Rust |
| 10M | mutation | append | 299.1 | 5199.2 | 17.38x | Rust |
| 10M | mutation | random | 3097.0 | 6150.5 | 1.99x | Rust |
| 10M | mutation | clustered | 513.3 | 5151.1 | 10.04x | Rust |

The very large Dolt write gaps at 5M-10M are measured results, but they should
be interpreted as end-to-end product-path results, not as a language-only
comparison. Timed work includes each implementation's sorting, mutation
buffering, chunking, hashing, node encoding, and in-memory store writes under
its default tree format and chunking policy.

### Point read

| Size | Phase | Workload | Rust ns/op | Dolt Go ns/op | Go/Rust | Winner |
|---:|---|---|---:|---:|---:|---|
| 10K | fresh | append | 152.4 | 666.9 | 4.38x | Rust |
| 10K | fresh | random | 153.7 | 567.4 | 3.69x | Rust |
| 10K | fresh | clustered | 169.6 | 651.2 | 3.84x | Rust |
| 10K | mutation | append | 110.9 | 475.2 | 4.29x | Rust |
| 10K | mutation | random | 156.8 | 522.2 | 3.33x | Rust |
| 10K | mutation | clustered | 104.8 | 447.0 | 4.26x | Rust |
| 50K | fresh | append | 190.2 | 616.1 | 3.24x | Rust |
| 50K | fresh | random | 185.9 | 774.0 | 4.16x | Rust |
| 50K | fresh | clustered | 181.0 | 592.5 | 3.27x | Rust |
| 50K | mutation | append | 112.9 | 493.6 | 4.37x | Rust |
| 50K | mutation | random | 253.7 | 719.7 | 2.84x | Rust |
| 50K | mutation | clustered | 102.7 | 485.3 | 4.73x | Rust |
| 1M | fresh | append | 553.9 | 1442.3 | 2.60x | Rust |
| 1M | fresh | random | 705.6 | 1408.5 | 2.00x | Rust |
| 1M | fresh | clustered | 573.7 | 1413.2 | 2.46x | Rust |
| 1M | mutation | append | 179.2 | 647.2 | 3.61x | Rust |
| 1M | mutation | random | 507.8 | 1436.7 | 2.83x | Rust |
| 1M | mutation | clustered | 134.1 | 636.3 | 4.75x | Rust |
| 5M | fresh | append | 890.1 | 1714.7 | 1.93x | Rust |
| 5M | fresh | random | 894.0 | 1527.9 | 1.71x | Rust |
| 5M | fresh | clustered | 884.5 | 1723.3 | 1.95x | Rust |
| 5M | mutation | append | 135.2 | 672.1 | 4.97x | Rust |
| 5M | mutation | random | 931.2 | 2133.8 | 2.29x | Rust |
| 5M | mutation | clustered | 123.4 | 666.2 | 5.40x | Rust |
| 10M | fresh | append | 972.3 | 1793.7 | 1.84x | Rust |
| 10M | fresh | random | 967.8 | 1626.4 | 1.68x | Rust |
| 10M | fresh | clustered | 968.6 | 1809.4 | 1.87x | Rust |
| 10M | mutation | append | 135.3 | 704.9 | 5.21x | Rust |
| 10M | mutation | random | 1112.2 | 2533.1 | 2.28x | Rust |
| 10M | mutation | clustered | 119.7 | 687.6 | 5.75x | Rust |

The 1M fresh-random ratio is 1.996x before display rounding, so it is counted
below the strict 2.0x threshold.

The critical 10M fresh-random point-read medians are 967.8 ns for Rust and
1626.4 ns for Dolt Go. Across its three repetitions, Rust measured
949.6-980.0 ns and Dolt Go measured 1483.0-2440.2 ns. The median ratio is
1.68x; the weakest paired repetition is still 1.56x.

### Full range scan

| Size | Phase | Workload | Rust ns/op | Dolt Go ns/op | Go/Rust | Winner |
|---:|---|---|---:|---:|---:|---|
| 10K | fresh | append | 5.158 | 21.792 | 4.22x | Rust |
| 10K | fresh | random | 6.396 | 21.829 | 3.41x | Rust |
| 10K | fresh | clustered | 5.217 | 21.067 | 4.04x | Rust |
| 10K | mutation | append | 5.904 | 20.734 | 3.51x | Rust |
| 10K | mutation | random | 6.127 | 22.583 | 3.69x | Rust |
| 10K | mutation | clustered | 6.112 | 21.957 | 3.59x | Rust |
| 50K | fresh | append | 4.979 | 21.508 | 4.32x | Rust |
| 50K | fresh | random | 4.814 | 21.839 | 4.54x | Rust |
| 50K | fresh | clustered | 4.829 | 21.390 | 4.43x | Rust |
| 50K | mutation | append | 5.201 | 22.124 | 4.25x | Rust |
| 50K | mutation | random | 4.798 | 24.304 | 5.07x | Rust |
| 50K | mutation | clustered | 5.328 | 22.319 | 4.19x | Rust |
| 1M | fresh | append | 5.388 | 26.834 | 4.98x | Rust |
| 1M | fresh | random | 5.481 | 26.068 | 4.76x | Rust |
| 1M | fresh | clustered | 7.488 | 26.408 | 3.53x | Rust |
| 1M | mutation | append | 6.998 | 27.118 | 3.88x | Rust |
| 1M | mutation | random | 5.441 | 27.074 | 4.98x | Rust |
| 1M | mutation | clustered | 6.946 | 27.297 | 3.93x | Rust |
| 5M | fresh | append | 5.883 | 38.383 | 6.52x | Rust |
| 5M | fresh | random | 5.836 | 38.514 | 6.60x | Rust |
| 5M | fresh | clustered | 5.902 | 39.148 | 6.63x | Rust |
| 5M | mutation | append | 6.500 | 40.843 | 6.28x | Rust |
| 5M | mutation | random | 6.059 | 40.755 | 6.73x | Rust |
| 5M | mutation | clustered | 7.051 | 40.067 | 5.68x | Rust |
| 10M | fresh | append | 5.959 | 40.194 | 6.75x | Rust |
| 10M | fresh | random | 6.025 | 40.209 | 6.67x | Rust |
| 10M | fresh | clustered | 6.024 | 40.795 | 6.77x | Rust |
| 10M | mutation | append | 6.142 | 40.616 | 6.61x | Rust |
| 10M | mutation | random | 6.115 | 41.270 | 6.75x | Rust |
| 10M | mutation | clustered | 6.275 | 41.525 | 6.62x | Rust |

## Peak process memory

Peak RSS covers the entire scenario process: deterministic fixture ownership,
tree construction, mutation staging, runtime, read structures, and the measured
operations. It is not memory attributable to a single operation. Each median
below combines the three workloads and three repetitions for the given
size/phase; `max` is the worst of those nine processes.

| Size | Phase | Rust median GiB | Rust max GiB | Dolt Go median GiB | Dolt Go max GiB |
|---:|---|---:|---:|---:|---:|
| 10K | fresh | 0.007 | 0.007 | 0.027 | 0.030 |
| 10K | mutation | 0.009 | 0.013 | 0.028 | 0.034 |
| 50K | fresh | 0.020 | 0.021 | 0.056 | 0.067 |
| 50K | mutation | 0.032 | 0.055 | 0.059 | 0.067 |
| 1M | fresh | 0.267 | 0.268 | 0.588 | 0.613 |
| 1M | mutation | 0.360 | 0.855 | 0.594 | 0.672 |
| 5M | fresh | 1.281 | 1.282 | 3.191 | 3.239 |
| 5M | mutation | 1.763 | 4.119 | 3.471 | 3.531 |
| 10M | fresh | 2.545 | 2.546 | 5.706 | 6.221 |
| 10M | mutation | 3.547 | 6.521 | 5.701 | 6.612 |

Rust has the lower scenario-median RSS for 27 of 30 size/phase/workload
combinations. All three exceptions are random mutation:

| Base size | Rust median GiB | Dolt Go median GiB | Rust overhead |
|---:|---:|---:|---:|
| 1M | 0.852 | 0.591 | 44% |
| 5M | 4.021 | 3.524 | 14% |
| 10M | 6.369 | 6.161 | 3% |

The worst individual process was 6.521 GiB for Rust 10M mutation-random and
6.612 GiB for Dolt Go 10M mutation-append. The comparable worst-case footprint
does not erase Rust's random-mutation memory issue: it identifies a concrete
path where mutation buffers, base-tree state, and read-side structures need
allocation-lifetime and representation profiling.

## Repetition stability

With only three repetitions, this run supports medians and observed ranges, not
narrow confidence intervals. Across all 90 groups, median coefficient of
variation was 2.65% for Rust and 2.98% for Dolt Go. Eight Rust groups and 13
Dolt Go groups exceeded 10% CV. The worst CVs came from isolated 10K scan
outliers, where very short elapsed times amplify scheduler noise; the median
prevents those single outliers from determining the comparison.

For point reads specifically, median CV was 3.22% for Rust and 2.50% for Dolt
Go. Three Rust point groups and five Dolt Go point groups exceeded 10% CV. Most
importantly, every paired point-read repetition still favored Rust by at least
1.56x, so the 1.5x floor is not an artifact of one median.

## Correctness and parity checks

The completed output contains:

- 180 successful scenario processes: 5 sizes x 3 repetitions x 2 phases x 3
  workloads x 2 implementations;
- 540 validated operation rows: write, point read, and range scan per process;
- 270 exact Rust/Go operation-run pairs;
- 90 scenario-operation median groups, all with three Rust and three Go runs;
- zero non-zero process exits and zero unvalidated result rows.

For every paired operation-run, the harness verified identical operation count,
workload digest, and result count. The runners additionally verified point-read
values and full-scan ordering/cardinality. A mismatch terminates the matrix
instead of producing a performance row.

## Workload contract

- Keys are fixed-width, zero-padded UTF-8 strings, preserving lexical and
  numeric order.
- Values are deterministic pseudo-random byte payloads from 1 through 100
  bytes.
- Fresh workloads are ascending append, a deterministic uniform permutation,
  and permuted 1,000-key clusters. They contain identical final records.
- Mutation workloads write 30% of the base size. Random and clustered
  mutation contain 50% inserts and 50% updates; append mutation contains only
  new keys above the existing maximum.
- A fresh build or mutation is submitted as one bulk write. Logical fixture
  construction and Dolt tuple construction are excluded from write timing.
- Point reads use at most 100,000 existing deterministic targets and one
  untimed warm pass before the measured pass.
- Range scans traverse the complete resulting tree. The timed pass includes
  logical tuple traversal/decoding and a cheap byte count in both runners.
- `RAYON_NUM_THREADS=1` and `GOMAXPROCS=1`; scenarios run sequentially in
  separate processes, with implementation order alternated by scenario/run.
- Both implementations use product-default encoding and chunking. Dolt uses
  its tuple representation; Rust uses its byte-entry representation. This is a
  caller-visible product comparison, not a common-wire-format microbenchmark.

## Environment and provenance

- Host: Apple M2 Max, 32 GiB RAM, macOS Darwin 25.5.0, arm64.
- Rust: `rustc 1.97.0`, binary SHA-256
  `b29a676979f061b72202bcb623774a00af023cbf1560ba53afd54a5f050ed76b`.
- Go: `go1.26.0`, binary SHA-256
  `fcdaf1c346cd41b0309b888a359568152786249e4112f065e00a563582ab4b45`.
- Rust revision: `b087770d79b5+src.b85a0408b624`.
- Dolt Go revision: `6b2372c7d4de+runner.0eeaa05ba605`.
- Command:

  ```sh
  BENCH_OUT=performance-results/zero-copy-final-rerun-2026-07-16 \
  BENCH_RUNS=3 BENCH_LARGE_RUNS=3 \
    scripts/run_prolly_comparison.sh
  ```

The normalized [three-run measurements](../performance-results/zero-copy-final-rerun-2026-07-16/results.csv),
[median summary](../performance-results/zero-copy-final-rerun-2026-07-16/summary.csv),
and [machine/provenance record](../performance-results/zero-copy-final-rerun-2026-07-16/machine.txt)
are checked in with this report. The complete 48 MiB result directory is also
retained locally at `performance-results/zero-copy-final-rerun-2026-07-16/`;
it includes copied binaries and all 180 per-process CSV/stderr/time artifacts.

## What to optimize next

1. **Harden large fresh-random point reads.** The 5M and 10M fresh medians are
   1.71x and 1.68x, respectively: above the floor, but below the universal 2x
   goal. Profile cache misses, key-comparison bytes, internal-node routing, and
   read-accelerator footprint. Candidate changes should be accepted only when
   they improve the 5M/10M random cases without regressing clustered or scan
   behavior.
2. **Reduce random-mutation peak memory.** Rust exceeds Dolt Go by 44%, 14%,
   and 3% at 1M, 5M, and 10M. Measure simultaneous ownership of mutation
   sorting buffers, old/new nodes, and read acceleration data; then shorten
   lifetimes, stream merges, or compact/lazily build auxiliary structures.
3. **Fix small random-mutation write overhead.** Dolt wins at 10K and 50K,
   while the crossover occurs near 1M. This shape suggests fixed setup and
   allocation costs. Profile those tiers separately rather than changing the
   scale-efficient bulk path based only on aggregate write wins.
4. **Expand the evidence before broader claims.** Add cold-cache point reads,
   bounded/selective scans, mixed read/write sequences, value-size buckets,
   allocation counts, and Linux CPU-pinned runs. Keep the present matrix as the
   regression baseline and require digest/cardinality parity for every new
   variant.

## Conclusion

For this exact single-worker, warm, in-memory contract, Rust is decisively
faster for reads: 30/30 point-read wins and 30/30 full-scan wins. It meets the
1.5x point-read floor in every scenario and every repetition, but does not yet
reach 2x for seven fresh large-tree medians. Rust also wins most writes and
scales much better on large bulk construction, while retaining two small
random-mutation write losses and a measurable random-mutation memory problem.
Those exceptions are the next honest optimization targets.
