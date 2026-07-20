# SQLite versus Turso prolly scale baseline

Lower latency and higher SQLite/Turso speedup are better for Turso.

## Outcome

Turso is faster in 21 of 25 cells; SQLite is faster in 4; 0 are tied. The equally weighted geometric-mean SQLite/Turso latency ratio is **1.681x**.

## Fixture build and storage

| Metric | SQLite | Turso | Turso delta | SQLite/Turso ratio |
|---|---:|---:|---:|---:|
| Median build | 1236.357 ms | 996.425 ms | -19.4% | 1.241x |
| Median database size | 113.11 MiB | 105.69 MiB | -6.6% | 1.070x |
| Validated fixtures | 3 | 3 | — | — |

## Geometric mean by operation

| Operation | SQLite/Turso latency ratio | Winner |
|---|---:|---|
| batch | 1.785x | Turso |
| diff | 0.978x | SQLite |
| full_scan | 0.506x | SQLite |
| get_cold | 1.079x | Turso |
| get_warm | 1.044x | Turso |
| merge | 0.808x | SQLite |
| put | 4.143x | Turso |
| query | 3.626x | Turso |
| scan | 3.987x | Turso |

## Per-cell comparison

`Turso delta` is `(Turso ns/op ÷ SQLite ns/op) − 1`; negative values favor Turso. `Speedup` is `SQLite ns/op ÷ Turso ns/op`; values above 1 favor Turso.

| Records | Operation | Pattern | Cache | SQLite ns/op | Turso ns/op | Turso delta | Speedup |
|---:|---|---|---|---:|---:|---:|---:|
| 1000000 | batch | append | n/a | 3918.7 | 1286.0 | -67.2% | 3.047x |
| 1000000 | batch | clustered | n/a | 4754.6 | 2682.8 | -43.6% | 1.772x |
| 1000000 | batch | random | n/a | 8060.0 | 7648.9 | -5.1% | 1.054x |
| 1000000 | diff | append | n/a | 562.1 | 447.1 | -20.5% | 1.257x |
| 1000000 | diff | clustered | n/a | 1004.0 | 1106.5 | +10.2% | 0.907x |
| 1000000 | diff | random | n/a | 3070.4 | 3739.7 | +21.8% | 0.821x |
| 1000000 | full_scan | append | n/a | 1659.3 | 3282.2 | +97.8% | 0.506x |
| 1000000 | get_cold | append | cold-manager | 173244.3 | 166624.8 | -3.8% | 1.040x |
| 1000000 | get_cold | clustered | cold-manager | 179463.0 | 176628.3 | -1.6% | 1.016x |
| 1000000 | get_cold | random | cold-manager | 248650.1 | 209291.1 | -15.8% | 1.188x |
| 1000000 | get_warm | append | warm-manager | 248.5 | 245.9 | -1.1% | 1.011x |
| 1000000 | get_warm | clustered | warm-manager | 260.8 | 243.1 | -6.8% | 1.073x |
| 1000000 | get_warm | random | warm-manager | 726.8 | 691.9 | -4.8% | 1.050x |
| 1000000 | merge | append | n/a | 858.0 | 741.4 | -13.6% | 1.157x |
| 1000000 | merge | clustered | n/a | 8.0 | 18.6 | +131.2% | 0.433x |
| 1000000 | merge | random | n/a | 119286.3 | 113340.7 | -5.0% | 1.052x |
| 1000000 | put | append | n/a | 19920042.0 | 4412833.0 | -77.8% | 4.514x |
| 1000000 | put | clustered | n/a | 25918125.0 | 7331000.0 | -71.7% | 3.535x |
| 1000000 | put | random | n/a | 27686500.0 | 6212458.0 | -77.6% | 4.457x |
| 1000000 | query | append | n/a | 12392.0 | 2242.8 | -81.9% | 5.525x |
| 1000000 | query | clustered | n/a | 12001.6 | 2510.9 | -79.1% | 4.780x |
| 1000000 | query | random | n/a | 126205.5 | 69907.8 | -44.6% | 1.805x |
| 1000000 | scan | append | n/a | 16140.9 | 4083.2 | -74.7% | 3.953x |
| 1000000 | scan | clustered | n/a | 11352.2 | 3110.8 | -72.6% | 3.649x |
| 1000000 | scan | random | n/a | 13113.9 | 2984.7 | -77.2% | 4.394x |

## Comparability and limits

- SQLite revision: `aca96b17fd44f9557181e571c64a854c7f495c07` (`dirty=true`); Turso revision: `6b2a603f73997e20d1669c3f270917c2a3c1b0b7` (`dirty=true`).
- Both runs use the same 1M/30%/10K/three-repetition workload contract, deterministic data, manager-cache rules, M2 Max host, and Rust 1.97 toolchain.
- This is not a strict causal A/B because the recorded revisions differ and both source trees were dirty. Treat it as an indicative backend baseline, not proof that the store adapter alone caused every delta.
- SQLite is synchronous with WAL and `synchronous=NORMAL`; Turso uses local-only async execution with four Tokio workers and no cloud synchronization.
- OS filesystem cache state is uncontrolled. Medians summarize three independent fixtures and do not provide confidence intervals.
