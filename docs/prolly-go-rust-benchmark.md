# Dolt Go vs Rust Prolly Benchmark

This benchmark compares build, write, point-read, and full-range-scan performance
for the Dolt Go prolly tree and this Rust implementation. It uses native runners
in separate processes so neither runtime affects the other's measurements.

## Run

Run the complete requested matrix:

```sh
scripts/run_prolly_comparison.sh
```

The driver fetches `https://github.com/dolthub/dolt.git`, resolves
`origin/main` once, detaches the benchmark checkout at that exact commit, copies
the checked-in command from `benchmarks/dolt-prolly-compare/`, tests it inside
Dolt's Go module, and builds both release binaries. No manually patched `dolt/`
checkout is required.

Reproduce a recorded Dolt revision exactly:

```sh
DOLT_REV=9e80d3aa2cf8ff473765f5603af32304d72c67b5 \
  scripts/run_prolly_comparison.sh
```

Run a quick parity and smoke comparison:

```sh
BENCH_SIZES="10000" BENCH_RUNS=1 BENCH_LARGE_RUNS=1 \
  scripts/run_prolly_comparison.sh
```

Results are written under `performance-results/dolt-rust/`. Set `BENCH_OUT` to
retain multiple named runs. The current default is
`performance-results/dolt-current-rust-canonical-2026-07-17/`. The runner stops
on any failed process or malformed output. The summarizer rejects missing or
duplicate repetitions, incomplete scenario matrices, unvalidated rows, and
mismatched contract versions, workload digests, operation counts, or result
cardinalities between implementations.

## Method

- One worker: `RAYON_NUM_THREADS=1` and `GOMAXPROCS=1`.
- Fresh datasets: 10K, 50K, 1M, 5M, and 10M records.
- UTF-8 keys are fixed-width and lexicographically sortable.
- Values are deterministic pseudo-random payloads from 1 through 100 bytes.
- Each fresh build or mutation workload is submitted as one bulk write. This
  avoids retaining thousands of obsolete content-addressed snapshots in the
  in-memory stores at the 1M–10M tiers.
- Fresh append, random, and clustered workloads contain the same final records
  but differ in arrival order.
- Mutation workloads write 30% of the base size. Random and clustered mutations
  contain equal numbers of new inserts and updates; append mutations are all new
  keys above the current maximum.
- Point reads are validated and capped at 100,000 operations per scenario.
- Range scans traverse the complete resulting tree and validate strict key order
  and cardinality in an untimed pass. The timed pass includes logical tuple
  traversal/decoding and a cheap byte count in both implementations.
- Reports record the Rust source hash, Go runner hash, and SHA-256 digest of
  each copied executable. A dirty implementation is never represented by the
  last commit SHA alone.
- `/usr/bin/time` records peak process RSS for every scenario. RSS covers the
  entire process, including untimed fixture and tuple construction; it is not
  memory attributable only to the measured operation.
- Logical fixture construction and Dolt tuple encoding happen outside timed write
  regions. Sorting, mutation buffering, chunking, hashing, encoding tree nodes,
  and in-memory node-store writes remain timed.

The comparison intentionally uses each implementation's default encoding and
chunking configuration. It measures the products presented to callers, not a
forced common wire format.

## Version-operation benchmark

Run the native common comparison and the separate Rust lifecycle measurements:

```sh
scripts/run_prolly_version_comparison.sh
```

The default matrix uses 10K, 50K, 1M, 5M, and 10M base records; 0%, 1%, and
30% change densities; append, random, and clustered locality; three process-
isolated repetitions; in-memory stores; and one worker. The common runners time
full and range diff, native patch generation/application, and no-op, disjoint,
convergent, and conflicting three-way merge as applicable. The Rust-only runner
separately measures publication, head and snapshot resolution, historical point
reads and range scans, version listing, rollback, and retention pruning.

To reproduce the verified run exactly:

```sh
BENCH_OUT=performance-results/prolly-version-2026-07-18-final \
BENCH_SIZES="10000 50000 1000000 5000000 10000000" \
BENCH_RUNS=3 \
DOLT_REV=6b2372c7d4ded1a54f55c6204304dbb72a33835c \
  scripts/run_prolly_version_comparison.sh
```

The driver refuses to overwrite an existing output directory. Use a new
`BENCH_OUT` path for a rerun. The workload contract is
`prolly-version-compare-v2`; it deterministically generates values shorter than
100 bytes and uses a 40% update, 30% insert, and 30% delete mix for non-append
changes. Complete scans validate ordered logical content after timing.

### Optimized v2 verification (2026-07-18)

The [optimized v2 report](../performance-results/prolly-version-optimization-v2-1m-final/report.md)
contains a three-repetition, process-isolated verification of the complete 1M
matrix. Its [common measurements](../performance-results/prolly-version-optimization-v2-1m-final/results-common.csv),
[lifecycle measurements](../performance-results/prolly-version-optimization-v2-1m-final/results-lifecycle.csv),
and [reproducibility audit](../performance-results/prolly-version-optimization-v2-1m-final/reproducibility.csv)
are machine-readable. Every expected matrix cell was present, all validation
flags passed, workload and logical-result identities matched between languages,
and two very short-duration groups changed winner direction across repetitions.

Rust won 36 of the 47 common-operation groups. The v2 patch path replaces
per-key logical patches with a verified content-addressed target-root envelope
for same-store version operations. At 1M records and 30% random changes, patch
generation fell from 47.96 ms in v1 to 0.67 us and patch application from
772.43 ms to 6.54 us. Against Dolt Go v2 those medians are 19.87x and 6.27x
faster, respectively. Dense conflicting merge fell from 732.71 ms to 372.19 ms,
a 1.96x Rust-internal improvement, by reconciling sorted change streams without
one left-tree lookup per right-side change.

The target is not universal yet. In the same 1M/30%-random scenario, Rust is
2.98x faster for full diff and 2.93x faster for range diff, but Dolt Go remains
1.39x faster for disjoint merge and 1.27x faster for conflicting merge. The
next merge optimization should reuse structural subtrees through the complete
three-way merge path rather than materializing both logical diff streams.

Because sub-microsecond patch and identity operations approach timer
granularity, interpret their exact ratios cautiously. The larger diff and merge
measurements are the more stable comparison. The report includes every CV,
including high-variance short-duration groups.

The exact copied binaries were also repeated three times at 10M records with
30% random changes. Rust is 3.09x faster for full diff, 2.43x for range diff,
21.45x for patch generation, and 44.11x for patch application. Dolt Go remains
1.63x faster for disjoint merge and 1.58x faster for conflicting merge. The
[10M scale summary](../performance-results/prolly-version-optimization-v2-1m-final/scale-10m-30-random.csv)
records medians, CVs, parity, and the native patch-count distinction.

### Verified historical v1 run (2026-07-18)

The [version-operation report](../performance-results/prolly-version-2026-07-18-final/report.md)
contains the complete 10M table and the Rust lifecycle table. The
[common raw measurements](../performance-results/prolly-version-2026-07-18-final/results-common.csv),
[lifecycle raw measurements](../performance-results/prolly-version-2026-07-18-final/results-lifecycle.csv),
and [reproducibility audit](../performance-results/prolly-version-2026-07-18-final/reproducibility.csv)
are machine-readable.

The run produced 1,410 validated common rows, 195 validated lifecycle rows, and
345 successful scenario processes. Every expected matrix cell and all three
repetitions were present. Workload digests, logical result digests, operation
counts, cardinalities, and logical conflict counts matched across Rust and Go;
they were also invariant across repetitions. The copied executable hashes match
the recorded manifest.

Across all 235 common-operation medians, Rust won 101 and Dolt Go won 134. The
operation split is more useful than the aggregate: Rust won 34/35 full-diff and
29/35 range-diff groups, plus every convergent and no-op merge group. Dolt Go
won every conflicting merge, disjoint merge, and patch-apply group. At 10M,
Rust won all seven full-diff shapes and five of seven range-diff shapes; Dolt Go
won all six conflicting and disjoint merge shapes and all seven patch-apply
shapes. These results identify different optimization strengths rather than one
universal winner.

Patch timings in this historical v1 run need a representation caveat. Rust v1
materialized logical point edits, while Dolt could emit structural subtree patches. The benchmark
validates identical logical effects but does not claim equivalent native patch
item counts. Very short identity/convergent operations are also near timer
granularity. Fourteen winner groups flipped direction across repetitions, the
median coefficient of variation was 4.91%, and high-CV rows are disclosed in
the report instead of being hidden.

The measured Rust snapshot was
`677bc05461ac3ea3da03db9317a78061a07f1b54` with source hash
`4839a3a46636e273597b90d9f9b2f96f214dbb78e222adc896bd9a29f6df370a`.
Dolt was pinned at `6b2372c7d4ded1a54f55c6204304dbb72a33835c` with Go runner
hash `652d11a2d7f29cdcf03a5b717e7198764de3a5f9890fb9b8e73addab3dd5bee5`.
Machine and toolchain details, plus all copied binary hashes, are recorded in
the [run manifest](../performance-results/prolly-version-2026-07-18-final/manifest.txt).

## Verified current-main run (2026-07-17)

The complete current-main matrix ran all five sizes three times: 180 successful
scenario processes, 540 validated operation rows, 270 exact Rust/Go pairs, and
no estimated results. See the
[current performance report](prolly-rust-go-performance-report-2026-07-17.md),
[normalized measurements](../performance-results/dolt-current-rust-canonical-2026-07-17/results.csv),
and [median summary](../performance-results/dolt-current-rust-canonical-2026-07-17/summary.csv).

Rust won 88 of 90 medians against Dolt current main: 28/30 writes, 30/30 point
reads, and 30/30 scans. Median Go/Rust ratios were 2.32x, 2.59x, and 5.21x,
respectively. Dolt retained the 10K and 50K random-mutation write wins. Compared
with the July 16 Rust baseline under the same logical contract, 80/90 current
Rust medians improved, with an 11.0% median improvement; all ten measured
regressions occurred at 10M and are disclosed in the report.

## Verified final zero-copy run (2026-07-16)

The complete final matrix reran every requested size three times with identical
binary hashes. See the
[detailed performance report](prolly-rust-go-performance-report-2026-07-16.md)
for the full 90-row matrix, repetition stability, peak RSS, provenance, and
limitations.

For the 10M behavior and the measured Rust read-path mechanisms in one visual,
see the [10M one-page report](prolly-10m-one-page-report.md).

Across the 90 scenario-operation medians, Rust won 88 and Dolt Go won 2. Rust
won all point-read (30/30) and full-range-scan (30/30) groups. Point-read
speedups range from 1.68x to 5.75x with a 3.16x geometric mean; all 90 paired
point-read repetitions exceed 1.5x. Full-scan speedups range from 3.41x to
6.77x with a 4.98x geometric mean.

The remaining exceptions are explicit. Dolt Go wins random 30%-mutation writes
at 10K and 50K, and Rust's peak process memory is higher for random mutation at
1M, 5M, and 10M. Seven fresh large-tree point-read medians remain below the
universal 2x target, although all remain above 1.5x.
