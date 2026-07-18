# Canonical Parallel Mutation Executor Report

## Outcome

The hard cutover passes the protected median and p95 regression gates on this 12-core Apple M2 Max and delivers a large gain for the workload that exposes proven independent leaf work. The executor does not manufacture parallelism for structural workloads: they stay on the canonical sequential path unless separated mutation islands prove safe.

Correctness evidence is stronger than the timing evidence: every retained row matched width one and a fresh canonical build, and all worker widths produced the same root for a workload. Width changes did not change nodes or bytes written. The key-stable route is restricted to key-only, entry-count hashing; key+value, byte-measured, rolling, and Weibull policies use canonical streaming.

## Old/New A/B

Twenty process pairs per workload were order-balanced to control thermal and background-load drift. Times are milliseconds; median is the mean of the two central samples, p95 is nearest-rank sample 19, and p99 is the maximum of 20.

| Workload | Old median | New median | Delta | Old p95 | New p95 | Delta |
|---|---:|---:|---:|---:|---:|---:|
| Value-only | 238.205 | 237.592 | -0.3% | 251.611 | 249.381 | -0.9% |
| Mixed 60/20/20 | 331.063 | 326.201 | -1.5% | 345.154 | 339.364 | -1.7% |

The value-only p99 moved from 251.798 ms to 253.026 ms (+0.5%). Mixed p99 moved from 391.099 ms to 413.757 ms (+5.8%) because each side contained a single outlier. With only 20 samples, those maxima are inconclusive; neither is presented as a stable tail change. The paired mean deltas were +0.47% for value-only and -0.97% for mixed, also inside the noise band.

## Single-call scaling

Thirty balanced samples used a 1M-entry base and 100k mutations.

| Workload | Width | Effective width | Tasks | Median ms | p95 ms | p99 ms | Median vs width 1 | p95 vs width 1 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Value-only | 1 | 1 | 0 | 299.299 | 303.345 | 305.490 | baseline | baseline |
| Value-only | 12 | 12 | 12 | 226.890 | 237.066 | 237.874 | -24.2% | -21.8% |
| Value-only | automatic | 12 | 12 | 226.118 | 229.346 | 231.346 | -24.5% | -24.4% |
| Mixed 60/20/20 | 1 | 1 | 0 | 332.399 | 337.637 | 351.716 | baseline | baseline |
| Mixed 60/20/20 | automatic | 1 | 0 | 333.746 | 335.685 | 336.368 | +0.4% | -0.6% |

Value-only reads and writes were identical at every width: 14,912 nodes read and 14,912 written, with 27,820,240 bytes read and 27,220,240 bytes written. Automatic scheduling split ordered route hydration into 36 batch-get calls and used 12 leaf partitions; width one issued no batch gets. The root was `3ef215964636db4b2c61d4d3073a3213d7503e5ecc21a3e193ba51d78db93c3a` for every sample.

Mixed execution did no parallel work at any width. The +0.4% median difference therefore has no causal parallel mechanism and is inside the 2% protected noise band. The root and all store-work counters were identical.

The six-sample all-workload sweep also showed no median regression for append, random, clustered, insert-only, delete-only, or mixed. Automatic scheduling reported effective width one and zero tasks for each. These short cells validate route selection and roots, not stable p95/p99 conclusions.

## Concurrent callers

The concurrent matrix used a 100k-entry base, 10k mutations per caller, and eight balanced observations per width. Value-only results show the adaptive inner-width policy:

| Callers | Width 1 median | Auto median | Delta | Width 1 p95 | Auto p95 | Delta | Auto effective width |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 29.052 ms | 24.062 ms | -17.2% | 30.904 ms | 25.429 ms | -17.7% | 12 |
| 4 | 30.840 ms | 26.930 ms | -12.7% | 33.464 ms | 29.081 ms | -13.1% | 12 |
| 8 | 34.142 ms | 34.126 ms | -0.1% | 38.303 ms | 38.900 ms | +1.6% | 1 |

At eight callers, automatic and explicit wider configurations all collapsed to effective width one and zero inner tasks. The median remained flat. The eight-observation p95/p99 values are maxima or near-maxima and are too small a sample for a tail claim.

Pure insert batches also stayed at width one: the representative route preflight rejected the missing key before full key-stable hydration. Across all structural caller cells, median differences around width one were generally inside ±1.6%; isolated short-sample tail differences are retained in `caller-results.csv` and treated as noise.

## Correctness and hardening findings

- `ParallelConfig` now controls the canonical writer instead of being ignored.
- Width one performs no parallel route or leaf work; automatic and explicit widths are capped by the shared Rayon pool.
- Active large callers disable inner parallelism when fewer than three pool threads per write would remain.
- Dense structural spans and expensive guarded layouts are rejected before replay. One failed CID resynchronization proof immediately discards speculation and falls back; there is no iterative O(n log n) replay.
- A representative key-stability preflight uses the same owned-node cache as full route hydration, avoiding a second store traversal.
- Batched route safety now reads the actual shared route path rather than an empty legacy ancestor vector.
- Rightmost leaves no longer bypass the value-stability requirement. Only key-only entry-count hashing can use the direct value executor.
- `parallel_width` reports width actually used. A policy that admits width 12 but performs no independent work reports width one and zero tasks.
- Explicitly bounded work now creates exactly the admitted partition count with balanced contiguous ranges; the prior ceiling-based splitter could under-partition near one-item-per-worker inputs.
- The binding hard cutover was completed after the benchmark run: Go decoders now consume the four executor telemetry fields, public Go/Node/JVM facades expose them, and the remaining Node entry-count boundary helper was removed. Cross-language parity tests cover width-one telemetry so a shortened wire decoder fails immediately.

## Memory and limitations

Peak RSS is a process-lifetime high-water mark on macOS, not a per-cell allocation measurement. The largest observed marks were 1,178,632,192 bytes in the 30-sample scaling process, 585,728,000 bytes in the caller process, and 1,057,931,264 bytes in the all-workload process. Because fixture construction, fresh-root validation, prior cells, and allocator retention contribute to those values, they cannot support a claim that one worker width uses more or less memory.

This report validates `MemStore` on one 12-core machine. The harness supports larger bases and mutation counts, but 10M/1M, synthetic high-latency storage, persistent storage, CPU utilization, and allocator-level byte counts were not captured in this run. Those are follow-up characterization targets, not grounds to weaken the correctness or protected-latency gates.

## Decision

Enable the canonical executor for automatic scheduling. The proven key-stable route has a large many-core gain, protected old/new medians and p95 do not regress, saturated callers avoid inner contention, and unsupported structural/value-sensitive cases remain on the canonical path. Do not claim universal parallel speedup; the measured benefit is deliberately limited to work whose independence and boundary stability are proven.
