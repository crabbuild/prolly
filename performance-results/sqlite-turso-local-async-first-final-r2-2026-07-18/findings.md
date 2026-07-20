# Async-first SQLite/Turso findings

The exact-final-revision local-only matrix completed with 432 validated
measurement cells, 36 validated fixtures, three repetitions, and no skipped or
failed cells. The generated comparison is [`report.md`](report.md); exact
per-operation percentiles are in [`summary.csv`](summary.csv).

## Outcome

The architecture transition removed the O(N) async point-update rebuild. Turso
point latency remains nearly flat from 50K through 2M records:

| Pattern | Turso p50 at 50K | Turso p50 at 2M | p50 scaling | p95 scaling |
| --- | ---: | ---: | ---: | ---: |
| append | 1.348 ms | 1.343 ms | 0.996x | 1.077x |
| random | 1.919 ms | 2.473 ms | 1.289x | 1.470x |
| clustered | 1.727 ms | 2.292 ms | 1.327x | 1.430x |

This passes the design gate of at most 3x p50 and 4x p95 growth between 50K
and 2M. It also confirms that point-update work no longer scales with total
record count.

Before the cutover, local 50K random and clustered point updates were about
140x and 169x slower than SQLite, and partial 500K measurements were about
727x and 831x slower. The exact final ratios are:

| Records | Pattern | Final Turso/SQLite total latency | Improvement versus old ratio |
| ---: | --- | ---: | ---: |
| 50K | random | 5.600x | about 25.0x |
| 50K | clustered | 4.997x | about 33.8x |
| 500K | random | 4.194x | about 173.3x |
| 500K | clustered | 4.322x | about 192.3x |

The required 10x throughput-improvement gate is therefore exceeded for both
50K random and clustered point updates.

At 2M records, Turso point throughput is 671 ops/s append, 373 ops/s random,
and 405 ops/s clustered. Relative to SQLite, total latency is 2.447x, 3.213x,
and 3.033x respectively. Turso p50 is 4.98x to 5.29x SQLite at that size.

Batching is the preferred high-throughput write interface. At 2M records,
Turso is 4.17x faster than SQLite for append batches and 3.10x faster for
clustered batches; random batch is 1.20x slower. Turso also wins 2M append diff
by 1.97x and append merge by 2.78x. Clustered merge is approximately equal.

## Remaining performance boundary

Native Turso async still has a larger per-operation local-file constant than
native SQLite sync. The 10K clustered point p50 is 11.17x SQLite, missing the
10x target for that single small cell, although its total-latency ratio is
9.56x. Every random/clustered point p50 from 50K through 2M is within 10x, and
the ratio improves with scale.

The worst small-cell total ratio is 10K random batch at 12.88x. It does not
persist at scale: the ratio is 1.20x at 2M. Further local tuning should target
Turso statement and transaction fixed costs, not weaken CID/format validation
or reintroduce a second mutation algorithm.

## Measurement limits

- All databases were local files; Turso Cloud sync was disabled and no
  credentials, `push()`, or `pull()` were used.
- SQLite used WAL and `synchronous=NORMAL`; Turso used native 0.7 local
  defaults. The harness does not claim identical durability semantics.
- Managers began cold, while the operating-system page cache was uncontrolled.
- The exact measured revision is `57e8ecfa4ba6348d9ecbdff055d42a84b6ebabf5`.
  The generated manifest says dirty only because prior untracked benchmark
  artifacts were present; tracked source was clean.
