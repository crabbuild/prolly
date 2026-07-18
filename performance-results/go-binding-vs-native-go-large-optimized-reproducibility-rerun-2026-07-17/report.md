# Rust Prolly Go Binding vs Native Go Prolly Performance

This comparison measures the API seen by a Go application, not direct Rust calls. The Rust implementation runs as an optimized native library behind cgo/UniFFI; the native implementation is the Go prolly tree implementation. All scenarios are process-isolated, single-worker, and in-memory. Lower nanoseconds per operation is better.

Rust-through-Go-binding wins: 36; native Go wins: 0; ties: 0.

| Operation | Rust Go binding wins | Native Go wins | Ties |
|---|---:|---:|---:|
| write | 12 | 0 | 0 |
| point_read | 12 | 0 | 0 |
| range_scan | 12 | 0 | 0 |

Measured repetitions: 3–3 per scenario.

## Aggregate signal

The ratios below are medians of the per-scenario binding/native-Go latency ratios; below 1.0 means the binding is faster, above 1.0 means native Go is faster.

| Operation | All sizes median ratio | 10M ratio range |
|---|---:|---:|
| write | 0.18x | 0.04–0.53x |
| point_read | 0.59x | 0.45–0.77x |
| range_scan | 0.95x | 0.91–1.00x |

## Scale-by-scale result

Binding/native-Go ratios below 1.0 favor the Rust binding. Each row contains six median workload cells.

| Records | Operation | Binding median wins | Binding/native ratio range |
|---:|---|---:|---:|
| 5,000,000 | write | 6/6 | 0.079–0.631x |
| 5,000,000 | point_read | 6/6 | 0.488–0.633x |
| 5,000,000 | range_scan | 6/6 | 0.904–0.964x |
| 10,000,000 | write | 6/6 | 0.037–0.526x |
| 10,000,000 | point_read | 6/6 | 0.451–0.767x |
| 10,000,000 | range_scan | 6/6 | 0.907–0.996x |

At 10M, 4/6 point-read cells meet or exceed a 1.5x binding advantage. The universal 1.5–2x point-read goal is therefore not yet met. Full-scan gains are smaller still and do not meet that target.

## Repetition stability

Across all paired repetitions, the Rust binding won 104/108 operation runs; native Go won 4/108.

- 5,000,000: binding 53/54; native Go 1/54.
- 10,000,000: binding 51/54; native Go 3/54.
- 4/36 scenario cells changed per-run winner.
- Largest paired-ratio span: 5,000,000 mutation random range_scan, 0.905–1.318x; the per-run winner changed.

## What is included at the binding boundary

- Write timing includes encoding the complete Go mutation slice into UniFFI bytes, one cgo call, Rust tree work, and decoding the returned tree handle.
- Binding point-read path: ReadSession.GetView with callback-scoped retained leaf lease.
- Binding scan path: ReadSession.ScanRangeView with retained 4096-entry packed pages.
- Fixture/key/value construction is outside write timing; one untimed point-read warm pass precedes measurement.
- Highest scenario median peak RSS: Rust Go binding 9.51 GiB; native Go 6.94 GiB.

### Peak RSS by workload

| Records | Phase | Workload | Rust Go binding | Native Go | Binding delta |
|---:|---|---|---:|---:|---:|
| 5,000,000 | fresh | append | 3.56 GiB | 3.22 GiB | +10.6% |
| 5,000,000 | fresh | clustered | 3.56 GiB | 3.21 GiB | +10.7% |
| 5,000,000 | fresh | random | 3.61 GiB | 3.15 GiB | +14.7% |
| 5,000,000 | mutation | append | 3.80 GiB | 3.45 GiB | +10.2% |
| 5,000,000 | mutation | clustered | 4.06 GiB | 3.44 GiB | +17.8% |
| 5,000,000 | mutation | random | 6.16 GiB | 3.54 GiB | +73.9% |
| 10,000,000 | fresh | append | 7.06 GiB | 6.31 GiB | +11.8% |
| 10,000,000 | fresh | clustered | 7.05 GiB | 6.35 GiB | +11.0% |
| 10,000,000 | fresh | random | 7.05 GiB | 6.21 GiB | +13.6% |
| 10,000,000 | mutation | append | 7.59 GiB | 6.81 GiB | +11.5% |
| 10,000,000 | mutation | clustered | 8.09 GiB | 6.81 GiB | +18.7% |
| 10,000,000 | mutation | random | 9.51 GiB | 6.94 GiB | +37.0% |

| Records | Phase | Workload | Operation | Runs | Rust Go binding ns/op | Native Go ns/op | Winner | Speedup |
|---:|---|---|---|---:|---:|---:|---|---:|
| 5000000 | fresh | append | point_read | 3 | 1083.535 | 1711.859 | rust-go-binding | 1.580x |
| 5000000 | fresh | append | range_scan | 3 | 37.118 | 38.514 | rust-go-binding | 1.038x |
| 5000000 | fresh | append | write | 3 | 543.946 | 6862.092 | rust-go-binding | 12.615x |
| 5000000 | fresh | clustered | point_read | 3 | 1084.941 | 1755.932 | rust-go-binding | 1.618x |
| 5000000 | fresh | clustered | range_scan | 3 | 37.477 | 38.954 | rust-go-binding | 1.039x |
| 5000000 | fresh | clustered | write | 3 | 688.878 | 3949.681 | rust-go-binding | 5.733x |
| 5000000 | fresh | random | point_read | 3 | 1101.378 | 1761.761 | rust-go-binding | 1.600x |
| 5000000 | fresh | random | range_scan | 3 | 34.704 | 38.407 | rust-go-binding | 1.107x |
| 5000000 | fresh | random | write | 3 | 1057.872 | 5274.734 | rust-go-binding | 4.986x |
| 5000000 | mutation | append | point_read | 3 | 345.995 | 686.786 | rust-go-binding | 1.985x |
| 5000000 | mutation | append | range_scan | 3 | 37.811 | 39.260 | rust-go-binding | 1.038x |
| 5000000 | mutation | append | write | 3 | 557.849 | 3000.751 | rust-go-binding | 5.379x |
| 5000000 | mutation | clustered | point_read | 3 | 323.642 | 662.810 | rust-go-binding | 2.048x |
| 5000000 | mutation | clustered | range_scan | 3 | 37.086 | 39.168 | rust-go-binding | 1.056x |
| 5000000 | mutation | clustered | write | 3 | 777.789 | 2478.010 | rust-go-binding | 3.186x |
| 5000000 | mutation | random | point_read | 3 | 1238.854 | 2198.142 | rust-go-binding | 1.774x |
| 5000000 | mutation | random | range_scan | 3 | 38.443 | 40.997 | rust-go-binding | 1.066x |
| 5000000 | mutation | random | write | 3 | 2617.818 | 4150.425 | rust-go-binding | 1.585x |
| 10000000 | fresh | append | point_read | 3 | 1171.697 | 1840.136 | rust-go-binding | 1.570x |
| 10000000 | fresh | append | range_scan | 3 | 38.112 | 39.697 | rust-go-binding | 1.042x |
| 10000000 | fresh | append | write | 3 | 552.892 | 14754.259 | rust-go-binding | 26.686x |
| 10000000 | fresh | clustered | point_read | 3 | 1170.971 | 1755.596 | rust-go-binding | 1.499x |
| 10000000 | fresh | clustered | range_scan | 3 | 38.345 | 40.477 | rust-go-binding | 1.056x |
| 10000000 | fresh | clustered | write | 3 | 660.143 | 7387.386 | rust-go-binding | 11.191x |
| 10000000 | fresh | random | point_read | 3 | 1180.665 | 1539.180 | rust-go-binding | 1.304x |
| 10000000 | fresh | random | range_scan | 3 | 38.670 | 39.108 | rust-go-binding | 1.011x |
| 10000000 | fresh | random | write | 3 | 1041.671 | 9351.987 | rust-go-binding | 8.978x |
| 10000000 | mutation | append | point_read | 3 | 334.762 | 680.338 | rust-go-binding | 2.032x |
| 10000000 | mutation | append | range_scan | 3 | 38.646 | 42.632 | rust-go-binding | 1.103x |
| 10000000 | mutation | append | write | 3 | 576.383 | 4686.648 | rust-go-binding | 8.131x |
| 10000000 | mutation | clustered | point_read | 3 | 314.557 | 696.723 | rust-go-binding | 2.215x |
| 10000000 | mutation | clustered | range_scan | 3 | 38.554 | 40.723 | rust-go-binding | 1.056x |
| 10000000 | mutation | clustered | write | 3 | 800.438 | 4430.743 | rust-go-binding | 5.535x |
| 10000000 | mutation | random | point_read | 3 | 1311.138 | 2373.456 | rust-go-binding | 1.810x |
| 10000000 | mutation | random | range_scan | 3 | 39.117 | 39.260 | rust-go-binding | 1.004x |
| 10000000 | mutation | random | write | 3 | 2967.447 | 5644.548 | rust-go-binding | 1.902x |

## Workload and validation contract

- Dataset sizes measured in this report: 5000000 10000000 base records.
- Keys: fixed-width, zero-padded UTF-8 strings; values: deterministic pseudo-random 1–100 byte payloads.
- Fresh workloads: ascending append order, uniform deterministic permutation, and permuted 1,000-key clusters.
- Mutation workloads: 30% of base size; random and clustered use 50% inserts and 50% updates.
- Point reads use at most 100,000 existing keys; scans traverse the complete resulting tree.
- Paired runs must match operation count, workload digest, result cardinality, point values, and scan ordering.
- Implementations use their product-default encoding and chunking; this is not a common-wire-format microbenchmark.

## Interpretation limits

The result isolates neither cgo nor UniFFI serialization. Those costs are intentionally included because they are paid by a Go caller. It does not cover disk I/O, cold cache, multiple workers, deployment packaging, partial/selective range scans, or concurrent readers. Medians describe these measured runs; they are not confidence intervals.

Host swap before the run: vm.swapusage: total = 25088.00M  used = 23422.69M  free = 1665.31M  (encrypted). Host swap after the run: vm.swapusage: total = 22016.00M  used = 20619.75M  free = 1396.25M  (encrypted). Machine-wide swap changed during the measured tiers, so exact medians may include memory-pressure noise and should be repeated on an idle dedicated host before publication.

## Implemented mechanisms and remaining work

The benchmark measures these mechanisms together; it does not isolate each contribution:

1. Point reads reuse a root-bound native session; owned reads use caller-provided output and view reads retain the immutable packed leaf for callback scope.
2. Multi-get crosses cgo once with a packed key arena and returns one validated packed result page in caller order.
3. Full scans seek once, retain the native traversal stack, and return validated 4,096-record pages; `ScanRangeView` allocates no Go key/value slices per row.
4. Opaque registry handles reject stale IDs; page/value release and scan close are idempotent.
5. Remaining work is to profile the scan gap, add packed retained diff/conflict pages, benchmark multi-get widths, add transport counters, and reduce write-side mutation/RSS copies.

Owned compatibility APIs remain unchanged. View APIs are opt-in and callback-scoped; callers must copy any bytes retained after the callback.

Raw process output is retained in `raw/`; `results.csv` contains normalized measurements, and `machine.txt` identifies the exact release library loaded.
