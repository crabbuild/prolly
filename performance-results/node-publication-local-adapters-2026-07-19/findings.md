# Universal node-publication performance evidence

This local-only revision comparison evaluates the universal node-publication
contract at baseline `a2f4e7a3066d2259783c8d815b45c0461c77b06b` and engine candidate
`81357948363fdb51dbc201ae5ba1a7377bb297e0`. Turso Cloud synchronization was
disabled throughout.

## Result

- The in-memory foundation gate passed all 21 groups across 840 raw revision
  rows and 20 alternating pairs. Each foundation invocation used 300 samples.
- The focused SQLite-sync/Turso-async gate passed all 24 API/pattern groups
  across 960 raw revision rows and 20 alternating pairs. Each recorded row
  aggregates 31 measurement samples.
- The final all-store gate passed all 108 groups across 5,490 raw rows. It
  covers memory, file, SQLite, RocksDB, SlateDB, and Turso adapters; synchronous
  and asynchronous paths use the same publication contract.
- PGlite was not measured because `@electric-sql/pglite` was unavailable. The
  limitation is recorded in `stores-final/environment-limitations.csv` and is
  not counted as a pass.

For the intended Turso point-publication fast path, the focused paired medians
were:

| Pattern | Latency | Throughput | p50 | p95 |
| --- | ---: | ---: | ---: | ---: |
| append | -64.90% | +184.94% | -67.40% | -65.25% |
| clustered | -69.93% | +232.61% | -74.23% | -65.46% |
| random | -57.13% | +133.27% | -62.33% | -49.29% |

The worst focused non-target paired median latency change was +4.07%, and the
worst non-target p95 change was +4.82%.

## Confirmation policy

`screen/` is a detection pass, not the authoritative gate. It measured every
all-store group with five alternating revision pairs and three samples per
revision/pair. Every one of its 25 flagged groups was selected before any
confirmation result was inspected and replaced in full by a 20-pair,
three-sample confirmation. Confirmations never stopped early. The other 83
groups retain their screen measurements. `stores-final/confirmation-sources.csv`
records every replacement, and `stores-final/` is the authoritative composed
result.

Repeated samples are reduced by median within each revision/pair before paired
changes are evaluated. The broad local-adapter gate requires both the existing
percentage threshold and an absolute regression above 5 us for median latency
or 10 us for p95 latency. These resolution floors do not apply to the focused
SQLite/Turso or foundation gates. The summarizer rejects duplicate rows,
missing roles, mismatched sample identifiers, inconsistent sample counts,
revision/root inconsistencies within a pair, and fixture validation failures.

## Scale-run boundary

A new follow-up matrix spanning 10K through 2M records was stopped at the
requester's direction after 504 data rows. That partial output is not committed,
is not summarized as a completed result, and is not used by any gate here. The
previously completed 10K-through-2M async-first matrix remains available in
`../sqlite-turso-local-async-first-final-r2-2026-07-18/`; it is earlier scale
evidence, not a claim that this exact follow-up candidate completed another full
scale rerun.
