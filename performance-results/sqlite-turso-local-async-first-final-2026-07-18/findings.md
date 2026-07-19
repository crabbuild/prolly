# Async-first SQLite/Turso findings

The full local-only matrix completed with 432 validated measurement cells, 36
validated fixtures, three repetitions, and no skipped or failed cells. The raw
report is [`report.md`](report.md); exact per-operation percentiles are in
[`summary.csv`](summary.csv).

## Outcome

The architecture transition removed the O(N) async point-update rebuild. Turso
point latency remains essentially flat from 50K through 2M records:

| Pattern | Turso p50 at 50K | Turso p50 at 2M | p50 scaling | p95 scaling |
| --- | ---: | ---: | ---: | ---: |
| append | 1.511 ms | 1.331 ms | 0.881x | 0.823x |
| random | 2.056 ms | 2.475 ms | 1.204x | 0.898x |
| clustered | 1.880 ms | 2.318 ms | 1.233x | 0.973x |

This passes the design gate of at most 3x p50 and 4x p95 growth between 50K
and 2M. It also confirms that point-update work no longer scales with total
record count.

Before the cutover, local 50K random and clustered point updates were about
140x and 169x slower than SQLite, and partial 500K measurements were about
727x and 831x slower. The final ratios are:

| Records | Pattern | Final Turso/SQLite total latency | Improvement versus old ratio |
| ---: | --- | ---: | ---: |
| 50K | random | 4.896x | about 28.6x |
| 50K | clustered | 6.444x | about 26.2x |
| 500K | random | 3.167x | about 229.6x |
| 500K | clustered | 4.095x | about 202.9x |

The required 10x throughput-improvement gate is therefore exceeded for both
50K random and clustered point updates.

At 2M records, Turso point throughput is 648 ops/s append, 373 ops/s random,
and 386 ops/s clustered. Relative to SQLite, total latency is 2.426x, 3.098x,
and 3.180x respectively. Turso p50 is 4.86x to 5.32x SQLite at that size.

Batching is the preferred high-throughput write interface. At 2M records,
Turso is 6.45x faster than SQLite for append batches, 2.76x faster for
clustered batches, and approximately equal for random batches. Turso also wins
the 2M append diff and merge cells by about 2.07x.

## Remaining performance boundary

Native Turso async still has a larger per-operation local-file constant than
native SQLite sync. The 10K clustered point p50 is 10.89x SQLite, narrowly
missing the 10x target for that single small cell, although its total-latency
ratio is 9.09x. Every random/clustered point p50 from 50K through 2M is within
10x, and the ratio improves with scale.

The worst small-cell ratio is 10K random batch at 26.13x. Its absolute work is
small and SQLite's denominator is unusually low; it does not persist at scale:
Turso random batch is 1.04x faster than SQLite at 2M. Further local tuning
should target Turso statement/transaction fixed costs, not weaken CID/format
validation or reintroduce a second mutation algorithm.

## Measurement limits

- All databases were local files; Turso Cloud sync was disabled and no
  credentials, `push()`, or `pull()` were used.
- SQLite used WAL and `synchronous=NORMAL`; Turso used native 0.7 local
  defaults. The harness does not claim identical durability semantics.
- Managers began cold, while the operating-system page cache was uncontrolled.
- The revision is exact. The run is marked dirty only because earlier untracked
  benchmark artifacts were present; tracked source was clean at `7d26dde`.
