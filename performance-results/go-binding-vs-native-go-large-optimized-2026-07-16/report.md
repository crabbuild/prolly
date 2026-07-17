# Rust Prolly Go Binding vs Native Go Prolly Performance

This comparison measures the API seen by a Go application, not direct Rust calls. The Rust implementation runs as an optimized native library behind cgo/UniFFI; the native implementation is the Go prolly tree implementation. All scenarios are process-isolated, single-worker, and in-memory. Lower nanoseconds per operation is better.

Rust-through-Go-binding wins: 35; native Go wins: 1; ties: 0.

| Operation | Rust Go binding wins | Native Go wins | Ties |
|---|---:|---:|---:|
| write | 12 | 0 | 0 |
| point_read | 12 | 0 | 0 |
| range_scan | 11 | 1 | 0 |

Measured repetitions: 3–3 per scenario.

## Aggregate signal

The ratios below are medians of the per-scenario binding/native-Go latency ratios; below 1.0 means the binding is faster, above 1.0 means native Go is faster.

| Operation | All sizes median ratio | 10M ratio range |
|---|---:|---:|
| write | 0.18x | 0.04–0.59x |
| point_read | 0.61x | 0.47–0.74x |
| range_scan | 0.97x | 0.87–0.97x |

## Scale-by-scale result

Binding/native-Go ratios below 1.0 favor the Rust binding. Each row contains six median workload cells.

| Records | Operation | Binding median wins | Binding/native ratio range |
|---:|---|---:|---:|
| 5,000,000 | write | 6/6 | 0.089–0.678x |
| 5,000,000 | point_read | 6/6 | 0.492–0.646x |
| 5,000,000 | range_scan | 5/6 | 0.871–1.004x |
| 10,000,000 | write | 6/6 | 0.038–0.587x |
| 10,000,000 | point_read | 6/6 | 0.467–0.736x |
| 10,000,000 | range_scan | 6/6 | 0.866–0.972x |

At 10M, 4/6 point-read cells meet or exceed a 1.5x binding advantage. The universal 1.5–2x point-read goal is therefore not yet met. Full-scan gains are smaller still and do not meet that target.

## Repetition stability

Across all paired repetitions, the Rust binding won 103/108 operation runs; native Go won 5/108.

- 5,000,000: binding 49/54; native Go 5/54.
- 10,000,000: binding 54/54; native Go 0/54.
- 4/36 scenario cells changed per-run winner.
- Largest paired-ratio span: 10,000,000 mutation random point_read, 0.535–0.892x; the Rust binding won all repetitions.

## What is included at the binding boundary

- Write timing includes encoding the complete Go mutation slice into UniFFI bytes, one cgo call, Rust tree work, and decoding the returned tree handle.
- Binding point-read path: ReadSession.GetView with callback-scoped retained leaf lease.
- Binding scan path: ReadSession.ScanRangeView with retained 4096-entry packed pages.
- Fixture/key/value construction is outside write timing; one untimed point-read warm pass precedes measurement.
- Highest scenario median peak RSS: Rust Go binding 8.66 GiB; native Go 6.98 GiB.

### Peak RSS by workload

| Records | Phase | Workload | Rust Go binding | Native Go | Binding delta |
|---:|---|---|---:|---:|---:|
| 5,000,000 | fresh | append | 3.23 GiB | 3.20 GiB | +0.8% |
| 5,000,000 | fresh | clustered | 3.40 GiB | 3.22 GiB | +5.5% |
| 5,000,000 | fresh | random | 3.59 GiB | 3.15 GiB | +14.0% |
| 5,000,000 | mutation | append | 3.80 GiB | 3.46 GiB | +9.9% |
| 5,000,000 | mutation | clustered | 3.94 GiB | 3.45 GiB | +14.2% |
| 5,000,000 | mutation | random | 5.71 GiB | 3.54 GiB | +61.2% |
| 10,000,000 | fresh | append | 7.05 GiB | 6.34 GiB | +11.2% |
| 10,000,000 | fresh | clustered | 7.05 GiB | 6.34 GiB | +11.3% |
| 10,000,000 | fresh | random | 7.06 GiB | 6.19 GiB | +14.0% |
| 10,000,000 | mutation | append | 7.46 GiB | 6.79 GiB | +9.9% |
| 10,000,000 | mutation | clustered | 8.09 GiB | 6.81 GiB | +18.8% |
| 10,000,000 | mutation | random | 8.66 GiB | 6.98 GiB | +24.0% |

| Records | Phase | Workload | Operation | Runs | Rust Go binding ns/op | Native Go ns/op | Winner | Speedup |
|---:|---|---|---|---:|---:|---:|---|---:|
| 5000000 | fresh | append | point_read | 3 | 1041.965 | 1647.662 | rust-go-binding | 1.581x |
| 5000000 | fresh | append | range_scan | 3 | 38.108 | 37.973 | native-go | 1.004x |
| 5000000 | fresh | append | write | 3 | 562.572 | 6331.907 | rust-go-binding | 11.255x |
| 5000000 | fresh | clustered | point_read | 3 | 1007.162 | 1567.249 | rust-go-binding | 1.556x |
| 5000000 | fresh | clustered | range_scan | 3 | 38.042 | 38.299 | rust-go-binding | 1.007x |
| 5000000 | fresh | clustered | write | 3 | 676.543 | 3724.167 | rust-go-binding | 5.505x |
| 5000000 | fresh | random | point_read | 3 | 1047.356 | 1622.433 | rust-go-binding | 1.549x |
| 5000000 | fresh | random | range_scan | 3 | 37.502 | 38.296 | rust-go-binding | 1.021x |
| 5000000 | fresh | random | write | 3 | 1043.417 | 4894.948 | rust-go-binding | 4.691x |
| 5000000 | mutation | append | point_read | 3 | 338.250 | 668.230 | rust-go-binding | 1.976x |
| 5000000 | mutation | append | range_scan | 3 | 36.861 | 42.315 | rust-go-binding | 1.148x |
| 5000000 | mutation | append | write | 3 | 560.395 | 2995.425 | rust-go-binding | 5.345x |
| 5000000 | mutation | clustered | point_read | 3 | 327.053 | 665.160 | rust-go-binding | 2.034x |
| 5000000 | mutation | clustered | range_scan | 3 | 39.058 | 39.354 | rust-go-binding | 1.008x |
| 5000000 | mutation | clustered | write | 3 | 788.486 | 2460.253 | rust-go-binding | 3.120x |
| 5000000 | mutation | random | point_read | 3 | 1152.975 | 1975.750 | rust-go-binding | 1.714x |
| 5000000 | mutation | random | range_scan | 3 | 38.790 | 39.529 | rust-go-binding | 1.019x |
| 5000000 | mutation | random | write | 3 | 2757.314 | 4066.211 | rust-go-binding | 1.475x |
| 10000000 | fresh | append | point_read | 3 | 1261.084 | 1793.198 | rust-go-binding | 1.422x |
| 10000000 | fresh | append | range_scan | 3 | 35.093 | 40.531 | rust-go-binding | 1.155x |
| 10000000 | fresh | append | write | 3 | 558.220 | 14664.306 | rust-go-binding | 26.270x |
| 10000000 | fresh | clustered | point_read | 3 | 1177.458 | 1806.724 | rust-go-binding | 1.534x |
| 10000000 | fresh | clustered | range_scan | 3 | 37.900 | 40.202 | rust-go-binding | 1.061x |
| 10000000 | fresh | clustered | write | 3 | 679.333 | 7436.281 | rust-go-binding | 10.946x |
| 10000000 | fresh | random | point_read | 3 | 1234.807 | 1678.708 | rust-go-binding | 1.359x |
| 10000000 | fresh | random | range_scan | 3 | 39.275 | 40.484 | rust-go-binding | 1.031x |
| 10000000 | fresh | random | write | 3 | 1075.276 | 9732.225 | rust-go-binding | 9.051x |
| 10000000 | mutation | append | point_read | 3 | 353.160 | 689.776 | rust-go-binding | 1.953x |
| 10000000 | mutation | append | range_scan | 3 | 39.234 | 41.105 | rust-go-binding | 1.048x |
| 10000000 | mutation | append | write | 3 | 608.787 | 4806.352 | rust-go-binding | 7.895x |
| 10000000 | mutation | clustered | point_read | 3 | 331.420 | 710.105 | rust-go-binding | 2.143x |
| 10000000 | mutation | clustered | range_scan | 3 | 38.028 | 42.225 | rust-go-binding | 1.110x |
| 10000000 | mutation | clustered | write | 3 | 787.675 | 4371.715 | rust-go-binding | 5.550x |
| 10000000 | mutation | random | point_read | 3 | 1459.738 | 2678.214 | rust-go-binding | 1.835x |
| 10000000 | mutation | random | range_scan | 3 | 39.002 | 40.121 | rust-go-binding | 1.029x |
| 10000000 | mutation | random | write | 3 | 3473.031 | 5915.948 | rust-go-binding | 1.703x |

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

Host swap before the run: vm.swapusage: total = 6912.00M  used = 5639.19M  free = 1272.81M  (encrypted). Host swap after the run: vm.swapusage: total = 7936.00M  used = 7259.81M  free = 676.19M  (encrypted). Machine-wide swap changed during the measured tiers, so exact medians may include memory-pressure noise and should be repeated on an idle dedicated host before publication.

## Implemented mechanisms and remaining work

The benchmark measures these mechanisms together; it does not isolate each contribution:

1. Point reads reuse a root-bound native session; owned reads use caller-provided output and view reads retain the immutable packed leaf for callback scope.
2. Multi-get crosses cgo once with a packed key arena and returns one validated packed result page in caller order.
3. Full scans seek once, retain the native traversal stack, and return validated 4,096-record pages; `ScanRangeView` allocates no Go key/value slices per row.
4. Opaque registry handles reject stale IDs; page/value release and scan close are idempotent.
5. Remaining work is to profile the scan gap, add packed retained diff/conflict pages, benchmark multi-get widths, add transport counters, and reduce write-side mutation/RSS copies.

Owned compatibility APIs remain unchanged. View APIs are opt-in and callback-scoped; callers must copy any bytes retained after the callback.

Raw process output is retained in `raw/`; `results.csv` contains normalized measurements, and `machine.txt` identifies the exact release library loaded.
