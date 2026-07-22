# Rust memory vs SQLite balanced publication comparison

Six repetitions per cell at 1,000,000 records and 150,000 changes. Each
specific memory/SQLite cell ran three times in each execution order. All 144
rows passed value, count, root, and reopen validation. Values below are median
total latency in milliseconds.

| API | Pattern | Memory | SQLite | SQLite / memory |
|---|---|---:|---:|---:|
| batch | append | 50.668 | 53.581 | 1.057x |
| batch | random | 214.288 | 235.424 | 1.099x |
| batch | clustered | 61.484 | 68.562 | 1.115x |
| build | append | 347.248 | 367.543 | 1.058x |
| build | random | 716.130 | 717.901 | 1.002x |
| build | clustered | 439.046 | 443.053 | 1.009x |
| diff | append | 17.911 | 18.060 | 1.008x |
| diff | random | 106.464 | 106.057 | 0.996x |
| diff | clustered | 23.447 | 22.678 | 0.967x |
| merge | append | 85.839 | 85.373 | 0.995x |
| merge | random | 775.888 | 786.911 | 1.014x |
| merge | clustered | 11.123 | 11.365 | 1.022x |

The sum of cell medians is 2,849.536 ms for memory and 2,916.510 ms for
SQLite. SQLite is 1.0235x slower overall. Read/diff and merge behavior is at
parity, but durable row/index publication leaves a strict 2.35% aggregate gap.
WAL, `synchronous=NORMAL`, atomic publication, and reopen durability remain
enabled.

## Follow-up optimization audit (2026-07-22)

A second complete balanced-six matrix tested pooled parallel compression with
a 256-byte threshold. All 144 rows validated, but its median sums were
2,846.389 ms for memory and 2,913.366 ms for SQLite, or **1.02353x**. This is
statistically identical to the result above, so the added parallel allocation
and compression machinery was removed. Raw data:
`prolly-memory-sqlite-parity-parallel-compression-balanced6-20260722.csv`.

Balanced screens also rejected the following changes because they regressed
the SQLite ratio: a read/write-locked node cache, a `WITHOUT ROWID` payload
table, 8 KiB SQLite lookaside slots, two-parameter raw-node statements, tuple
parameter binding, inline 32-byte cache keys, and an append-only one-row
publication-segment prototype. Profiling attributes the stable remaining gap
to committed WAL payload and primary-index publication, rather than point-read
or decompression overhead. Removing that cost would require weakening the
durability comparison or changing the SQLite storage contract; neither was
accepted.
