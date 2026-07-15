# Final range-delete performance verification

## Scope and method

This is the final five-run, alternating-order verification of current revision
`d3d6d4cb67aad611189164a984c98483c4d6c41e` against original revision
`fa7c219afc7e1ee5769dd85e5223ea5dde9e3074`. The current source was clean at
build time. Both revisions used the same copied benchmark sources, whose
combined SHA-256 was
`ab97654ff20f9b5e59b2cc5dc3fa6800ce4193a16f51f345637f4b807f2cab49`.
The original library source was not patched; the benchmark runner uses its
compatibility adapter only to express the same fixed-width half-open interval
as point deletes on the original revision.

The full machine, compiler, database, binary, fixture-cloning, profile, and
order metadata are in [machine.txt](machine.txt). The generated aggregate,
including five-run medians, complete ranges, structural metrics, and method
limits, is in [report.md](report.md). Per-process evidence is retained in
`raw/`.

## Data-quality audit

- `run-manifest.csv` has exactly 80 distinct process tuples: two revisions,
  two profiles, two sizes, five repetitions, and both the fixture build and
  range-delete workload. All 80 exited zero and have `validation=ok`.
- `raw-results.csv` has the matching 80 distinct tuples. All have
  `validated=true` and `status=ok`.
- Every clustered delete performed 10,000 removals and left 990,000 entries at
  1M input records or 9,990,000 at 10M. The paired build rows contain the
  complete original cardinality.
- The runner applied `wal_checkpoint(TRUNCATE)` and `integrity_check` to each
  build fixture before accepting the workload row.

## Final clustered range-delete result

Lower latency is better. Values are five-run medians; ranges are the complete
observed samples, not confidence intervals.

| Profile | Records | Original median (range) | Final median (range) | Delta |
|---|---:|---:|---:|---:|
| WAL+FULL | 1,000,000 | 80.020 ms (74.705–87.581) | 26.434 ms (21.361–138.270) | -67.0% |
| WAL+NORMAL | 1,000,000 | 81.107 ms (73.305–85.727) | 24.409 ms (23.096–28.266) | -69.9% |
| WAL+FULL | 10,000,000 | 125.527 ms (115.640–129.097) | 56.348 ms (50.819–63.949) | -55.1% |
| WAL+NORMAL | 10,000,000 | 124.758 ms (120.645–128.988) | 54.749 ms (53.468–226.513) | -56.1% |

All four latency groups are material gains according to the report's stated
rule. The one 1M FULL final outlier and the one 10M NORMAL final outlier are
retained in the ranges; neither changes the median-based classification.
The paired sorted-stream-build setup rows are noise-sensitive in every group
(-2.4% through +0.8%), so this capture does not establish a build regression.

## I/O and residual regressions

The deletion reduces read work substantially: at 1M, nodes read fall from
39 to 9 and bytes read from 459,956 to 95,136; at 10M, nodes read fall from
37 to 10 and bytes read from 432,979 to 127,828. At 1M it also reduces writes
from four nodes / 46,083 bytes to three nodes / 23,911 bytes.

There remains one real, material resource regression. At 10M in both
durability profiles, written bytes increase from 19,251 to 52,518 (+172.8%),
while node writes remain four. Fixture size rises by about 0.4%, and the
current tree is correspondingly 0.4% larger. The aggregate therefore marks
both 10M delete rows as `I/O regression`; this is not offset or reclassified
because latency improves. Peak RSS has no material regression.

This exact 10M written-byte pair (19,251 to 52,518) is unchanged from the
earlier durable range-delete evaluation, so the final correctness/statistics
repair did not remove or worsen that measured write-I/O cost.

## Comparison with prior evidence

The prior durable capture benchmarked pre-repair revision `e150542`; Task 7
then performed only a one-run 1M smoke while the canonical-root/statistics
repair was present. These are independent captures, so their absolute times
are not an isolated attribution of a source change. The final paired run is
the controlling comparison with the original revision.

| Profile / records | Prior durable delta | Final delta | Interpretation |
|---|---:|---:|---|
| FULL / 1M | -66.6% | -67.0% | Same practical gain; final original and current medians are both higher. |
| NORMAL / 1M | -67.8% | -69.9% | Gain persists and is slightly larger in this capture. |
| FULL / 10M | -61.6% | -55.1% | Gain persists but is 6.5 percentage points smaller; final current median is 28.0% higher than the prior current median, and the final original median is 9.4% higher. |
| NORMAL / 10M | -55.8% | -56.1% | Same practical gain; final original and current medians are both about 28% higher. |

Against the Task 7 single-run smoke, final 1M FULL is -67.0% versus -70.2%
and final 1M NORMAL is -69.9% versus -65.0%. The final five-run capture is
therefore more reliable than that smoke. In particular, it confirms the
targeted benefit without claiming that the independent time shifts prove a
repair-caused latency regression.

## Decision

The final head has no material clustered-delete latency, memory, or fixture
size regression versus the original revision, and every correctness and
storage validation row succeeds. It does retain the explicit 10M written-byte
regression described above. Any integration decision should preserve that
qualification rather than characterizing the result as regression-free across
all measured resources.
