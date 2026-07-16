# Rust Prolly Go Binding vs Native Go Prolly Performance

This comparison measures the API seen by a Go application, not direct Rust calls. The Rust implementation runs as an optimized native library behind cgo/UniFFI; the native implementation is the Go prolly tree used by Dolt. All scenarios are process-isolated, single-worker, and in-memory. Lower nanoseconds per operation is better.

Rust-through-Go-binding wins: 25; native Go wins: 65; ties: 0.

| Operation | Rust Go binding wins | Native Go wins | Ties |
|---|---:|---:|---:|
| write | 25 | 5 | 0 |
| point_read | 0 | 30 | 0 |
| range_scan | 0 | 30 | 0 |

Measured repetitions: 3–3 per scenario.

## Aggregate signal

The ratios below are medians of the per-scenario binding/native-Go latency ratios; below 1.0 means the binding is faster, above 1.0 means native Go is faster.

| Operation | All sizes median ratio | 10M ratio range |
|---|---:|---:|
| write | 0.50x | 0.04–0.54x |
| point_read | 5.34x | 2.86–5.25x |
| range_scan | 7.24x | 4.72–4.93x |

## What is included at the binding boundary

- Write timing includes encoding the complete Go mutation slice into UniFFI bytes, one cgo call, Rust tree work, and decoding the returned tree handle.
- Point reads call the public `Engine.Get` once per key, including cgo crossing plus owned result-buffer decode/free.
- Range scans call the public bounded-memory `Engine.ScanRange`, which fetches 1,024-entry pages and delivers owned Go `Entry` values.
- Fixture/key/value construction is outside write timing; one untimed point-read warm pass precedes measurement.
- Highest scenario median peak RSS: Rust Go binding 10.22 GiB; native Go 6.84 GiB.

| Records | Phase | Workload | Operation | Runs | Rust Go binding ns/op | Native Go ns/op | Winner | Speedup |
|---:|---|---|---|---:|---:|---:|---|---:|
| 10000 | fresh | append | point_read | 3 | 5757.958 | 495.525 | native-go | 11.620x |
| 10000 | fresh | append | range_scan | 3 | 184.221 | 21.529 | native-go | 8.557x |
| 10000 | fresh | append | write | 3 | 906.775 | 654.362 | native-go | 1.386x |
| 10000 | fresh | clustered | point_read | 3 | 5677.800 | 503.988 | native-go | 11.266x |
| 10000 | fresh | clustered | range_scan | 3 | 169.533 | 21.125 | native-go | 8.025x |
| 10000 | fresh | clustered | write | 3 | 540.862 | 788.312 | rust-go-binding | 1.458x |
| 10000 | fresh | random | point_read | 3 | 6981.238 | 546.812 | native-go | 12.767x |
| 10000 | fresh | random | range_scan | 3 | 174.283 | 23.863 | native-go | 7.303x |
| 10000 | fresh | random | write | 3 | 841.171 | 934.121 | rust-go-binding | 1.111x |
| 10000 | mutation | append | point_read | 3 | 3756.349 | 429.032 | native-go | 8.755x |
| 10000 | mutation | append | range_scan | 3 | 184.481 | 20.458 | native-go | 9.018x |
| 10000 | mutation | append | write | 3 | 564.986 | 1088.403 | rust-go-binding | 1.926x |
| 10000 | mutation | clustered | point_read | 3 | 3708.189 | 505.853 | native-go | 7.331x |
| 10000 | mutation | clustered | range_scan | 3 | 187.286 | 20.453 | native-go | 9.157x |
| 10000 | mutation | clustered | write | 3 | 859.500 | 781.569 | native-go | 1.100x |
| 10000 | mutation | random | point_read | 3 | 6134.484 | 536.529 | native-go | 11.434x |
| 10000 | mutation | random | range_scan | 3 | 196.228 | 20.978 | native-go | 9.354x |
| 10000 | mutation | random | write | 3 | 2291.792 | 1914.903 | native-go | 1.197x |
| 50000 | fresh | append | point_read | 3 | 6515.156 | 599.722 | native-go | 10.864x |
| 50000 | fresh | append | range_scan | 3 | 198.419 | 20.615 | native-go | 9.625x |
| 50000 | fresh | append | write | 3 | 581.342 | 871.857 | rust-go-binding | 1.500x |
| 50000 | fresh | clustered | point_read | 3 | 7497.852 | 597.970 | native-go | 12.539x |
| 50000 | fresh | clustered | range_scan | 3 | 188.499 | 20.781 | native-go | 9.071x |
| 50000 | fresh | clustered | write | 3 | 643.583 | 1018.122 | rust-go-binding | 1.582x |
| 50000 | fresh | random | point_read | 3 | 5912.048 | 643.403 | native-go | 9.189x |
| 50000 | fresh | random | range_scan | 3 | 188.895 | 21.731 | native-go | 8.692x |
| 50000 | fresh | random | write | 3 | 860.425 | 1232.973 | rust-go-binding | 1.433x |
| 50000 | mutation | append | point_read | 3 | 3745.493 | 542.892 | native-go | 6.899x |
| 50000 | mutation | append | range_scan | 3 | 184.780 | 22.920 | native-go | 8.062x |
| 50000 | mutation | append | write | 3 | 552.267 | 1149.808 | rust-go-binding | 2.082x |
| 50000 | mutation | clustered | point_read | 3 | 3814.747 | 483.879 | native-go | 7.884x |
| 50000 | mutation | clustered | range_scan | 3 | 191.021 | 21.419 | native-go | 8.918x |
| 50000 | mutation | clustered | write | 3 | 769.281 | 880.878 | rust-go-binding | 1.145x |
| 50000 | mutation | random | point_read | 3 | 5941.617 | 688.150 | native-go | 8.634x |
| 50000 | mutation | random | range_scan | 3 | 185.483 | 22.726 | native-go | 8.162x |
| 50000 | mutation | random | write | 3 | 2416.639 | 1885.558 | native-go | 1.282x |
| 1000000 | fresh | append | point_read | 3 | 6306.962 | 1419.550 | native-go | 4.443x |
| 1000000 | fresh | append | range_scan | 3 | 192.066 | 25.832 | native-go | 7.435x |
| 1000000 | fresh | append | write | 3 | 564.118 | 1985.419 | rust-go-binding | 3.520x |
| 1000000 | fresh | clustered | point_read | 3 | 6452.960 | 1414.483 | native-go | 4.562x |
| 1000000 | fresh | clustered | range_scan | 3 | 193.090 | 26.383 | native-go | 7.319x |
| 1000000 | fresh | clustered | write | 3 | 651.924 | 1673.054 | rust-go-binding | 2.566x |
| 1000000 | fresh | random | point_read | 3 | 6254.062 | 1417.366 | native-go | 4.412x |
| 1000000 | fresh | random | range_scan | 3 | 192.597 | 26.810 | native-go | 7.184x |
| 1000000 | fresh | random | write | 3 | 980.300 | 2517.612 | rust-go-binding | 2.568x |
| 1000000 | mutation | append | point_read | 3 | 3675.120 | 640.103 | native-go | 5.741x |
| 1000000 | mutation | append | range_scan | 3 | 192.485 | 26.379 | native-go | 7.297x |
| 1000000 | mutation | append | write | 3 | 680.568 | 1671.144 | rust-go-binding | 2.456x |
| 1000000 | mutation | clustered | point_read | 3 | 3575.040 | 642.557 | native-go | 5.564x |
| 1000000 | mutation | clustered | range_scan | 3 | 190.051 | 27.502 | native-go | 6.910x |
| 1000000 | mutation | clustered | write | 3 | 875.085 | 1260.801 | rust-go-binding | 1.441x |
| 1000000 | mutation | random | point_read | 3 | 6402.450 | 1421.568 | native-go | 4.504x |
| 1000000 | mutation | random | range_scan | 3 | 187.980 | 27.291 | native-go | 6.888x |
| 1000000 | mutation | random | write | 3 | 2760.387 | 2566.332 | native-go | 1.076x |
| 5000000 | fresh | append | point_read | 3 | 6675.889 | 1519.160 | native-go | 4.394x |
| 5000000 | fresh | append | range_scan | 3 | 193.587 | 38.241 | native-go | 5.062x |
| 5000000 | fresh | append | write | 3 | 558.537 | 7107.855 | rust-go-binding | 12.726x |
| 5000000 | fresh | clustered | point_read | 3 | 6567.358 | 1696.545 | native-go | 3.871x |
| 5000000 | fresh | clustered | range_scan | 3 | 193.914 | 38.918 | native-go | 4.983x |
| 5000000 | fresh | clustered | write | 3 | 679.989 | 3689.514 | rust-go-binding | 5.426x |
| 5000000 | fresh | random | point_read | 3 | 6603.120 | 1701.083 | native-go | 3.882x |
| 5000000 | fresh | random | range_scan | 3 | 202.499 | 38.559 | native-go | 5.252x |
| 5000000 | fresh | random | write | 3 | 1053.436 | 5164.674 | rust-go-binding | 4.903x |
| 5000000 | mutation | append | point_read | 3 | 3640.715 | 670.766 | native-go | 5.428x |
| 5000000 | mutation | append | range_scan | 3 | 194.422 | 43.292 | native-go | 4.491x |
| 5000000 | mutation | append | write | 3 | 587.654 | 3025.909 | rust-go-binding | 5.149x |
| 5000000 | mutation | clustered | point_read | 3 | 3399.447 | 658.731 | native-go | 5.161x |
| 5000000 | mutation | clustered | range_scan | 3 | 187.905 | 39.389 | native-go | 4.770x |
| 5000000 | mutation | clustered | write | 3 | 781.726 | 2565.200 | rust-go-binding | 3.281x |
| 5000000 | mutation | random | point_read | 3 | 6746.767 | 2127.721 | native-go | 3.171x |
| 5000000 | mutation | random | range_scan | 3 | 192.014 | 40.373 | native-go | 4.756x |
| 5000000 | mutation | random | write | 3 | 3113.507 | 4185.815 | rust-go-binding | 1.344x |
| 10000000 | fresh | append | point_read | 3 | 6566.550 | 1768.055 | native-go | 3.714x |
| 10000000 | fresh | append | range_scan | 3 | 193.117 | 39.133 | native-go | 4.935x |
| 10000000 | fresh | append | write | 3 | 534.377 | 14164.496 | rust-go-binding | 26.507x |
| 10000000 | fresh | clustered | point_read | 3 | 6510.945 | 1513.880 | native-go | 4.301x |
| 10000000 | fresh | clustered | range_scan | 3 | 192.828 | 39.621 | native-go | 4.867x |
| 10000000 | fresh | clustered | write | 3 | 648.179 | 7629.967 | rust-go-binding | 11.771x |
| 10000000 | fresh | random | point_read | 3 | 6537.536 | 1485.150 | native-go | 4.402x |
| 10000000 | fresh | random | range_scan | 3 | 192.385 | 39.344 | native-go | 4.890x |
| 10000000 | fresh | random | write | 3 | 1013.454 | 9224.789 | rust-go-binding | 9.102x |
| 10000000 | mutation | append | point_read | 3 | 3593.099 | 683.807 | native-go | 5.255x |
| 10000000 | mutation | append | range_scan | 3 | 194.882 | 40.583 | native-go | 4.802x |
| 10000000 | mutation | append | write | 3 | 549.310 | 4682.912 | rust-go-binding | 8.525x |
| 10000000 | mutation | clustered | point_read | 3 | 3363.734 | 694.129 | native-go | 4.846x |
| 10000000 | mutation | clustered | range_scan | 3 | 189.574 | 40.190 | native-go | 4.717x |
| 10000000 | mutation | clustered | write | 3 | 767.170 | 4035.539 | rust-go-binding | 5.260x |
| 10000000 | mutation | random | point_read | 3 | 6810.516 | 2383.378 | native-go | 2.858x |
| 10000000 | mutation | random | range_scan | 3 | 189.762 | 39.024 | native-go | 4.863x |
| 10000000 | mutation | random | write | 3 | 2997.503 | 5505.532 | rust-go-binding | 1.837x |

## Workload and validation contract

- Dataset sizes: 10K, 50K, 1M, 5M, and 10M base records.
- Keys: fixed-width, zero-padded UTF-8 strings; values: deterministic pseudo-random 1–100 byte payloads.
- Fresh workloads: ascending append order, uniform deterministic permutation, and permuted 1,000-key clusters.
- Mutation workloads: 30% of base size; random and clustered use 50% inserts and 50% updates.
- Point reads use at most 100,000 existing keys; scans traverse the complete resulting tree.
- Paired runs must match operation count, workload digest, result cardinality, point values, and scan ordering.
- Implementations use their product-default encoding and chunking; this is not a common-wire-format microbenchmark.

## Interpretation limits

The result isolates neither cgo nor UniFFI serialization. Those costs are intentionally included because they are paid by a Go caller. It does not cover disk I/O, cold cache, multiple workers, deployment packaging, partial/selective range scans, or concurrent readers. Medians describe these measured runs; they are not confidence intervals.

The macOS host reported 6,399.25 MiB of machine-wide swap in use during the 10M tier, although every benchmark process's `/usr/bin/time` record reported zero swaps. The exact 10M medians are honest measurements of this host state, not an idle-host claim; publication-grade numbers should be repeated on a dedicated idle machine.

## Engineering implications

These are code-path hypotheses, not contributions isolated by this benchmark:

1. Add a retained Go read-session handle so point reads do not repeatedly encode/decode the tree record or reacquire root state.
2. Add a specialized single-call `get_into` ABI that borrows Go input only for the cgo call and writes into caller-owned storage, avoiding RustBuffer allocate/copy/free crossings.
3. Make `GetMany` the throughput-oriented Go API and benchmark batch sizes separately; it amortizes the native boundary without changing single-key semantics.
4. Return packed scan pages with offset tables and scope-bound entry views, then let Go decode lazily without allocating two byte slices per row.
5. Encode writes into a packed key/value arena so the binding does not retain a Go slice-of-slices and a second complete UniFFI mutation buffer simultaneously.

Each fast path needs byte-for-byte parity tests, malformed-offset rejection, explicit lifetime rules, race/close tests, and separate allocation/cgo-call counters before it replaces the compatibility API.

Raw process output is retained in `raw/`; `results.csv` contains normalized measurements, and `machine.txt` identifies the exact release library loaded.
