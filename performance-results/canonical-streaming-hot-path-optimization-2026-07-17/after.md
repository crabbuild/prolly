# Canonical Streaming Hot-Path Optimization: After Measurements

Captured on 2026-07-17 after commits `a706ab8`, `c41d4c4`, and `36dbedc` on
Apple M2 Max / Darwin arm64 / Rust 1.97.0. Cargo used the optimized `bench`
profile and `MemStore`.

## Commands

```bash
for run_id in 1 2 3 4 5; do
  PROLLY_BENCH_ONLY=boundary-hot-path cargo bench --bench prolly_bench --quiet
done
for run_id in 1 2 3 4 5; do
  PROLLY_BENCH_ONLY=chunking-cutover PROLLY_BENCH_SCALE=100000 \
    PROLLY_BENCH_ITERATIONS=100 cargo bench --bench prolly_bench --quiet
done
for run_id in 1 2 3; do
  SCALE_VERSION=hotpath-$run_id SCALE_RECORDS=1000000 \
    cargo bench --bench scale_workloads --quiet
done
/usr/bin/time -l env SCALE_VERSION=hotpath-rss SCALE_RECORDS=1000000 \
  cargo bench --bench scale_workloads --quiet
```

An exact baseline worktree at `d5d039e` was also run three times in the same
session. The machine was not idle during the scale runs: 24 unrelated `rustc`
processes and macOS media-analysis/security services were active, with load
averages reaching 13.04. The scale rows are retained for transparency, but the
focused results are the authoritative latency gate.

## Five-Process Median Summary

Values are ns/entry or ns/item. Each row is the median of the five process
medians; p95 is the median of the five process-local p95 values.

| Workload | Median | p95 |
| --- | ---: | ---: |
| Boundary entry-count key hash | 12 | 13 |
| Boundary logical-byte Weibull | 46 | 48 |
| Boundary logical-byte rolling hash | 90 | 91 |
| Sorted build | 197 | 205 |
| Unsorted build | 101 | 114 |
| Append 1 | 18,917 | 24,167 |
| Append 64 | 564 | 624 |
| Append 4,096 | 192 | 218 |
| Middle value update | 35,291 | 40,209 |
| Middle insert | 209,750 | 232,833 |
| Middle delete | 34,583 | 38,708 |

## Boundary Raw Samples

```csv
process,name,median_ns,p95_ns,p99_ns
1,boundary_entry_count_key_hash,12,13,13
1,boundary_logical_bytes_key_weibull,46,48,48
1,boundary_logical_bytes_rolling_hash,90,91,91
2,boundary_entry_count_key_hash,12,14,14
2,boundary_logical_bytes_key_weibull,46,48,48
2,boundary_logical_bytes_rolling_hash,91,93,93
3,boundary_entry_count_key_hash,12,13,13
3,boundary_logical_bytes_key_weibull,46,48,48
3,boundary_logical_bytes_rolling_hash,91,121,121
4,boundary_entry_count_key_hash,13,14,14
4,boundary_logical_bytes_key_weibull,48,49,49
4,boundary_logical_bytes_rolling_hash,90,91,91
5,boundary_entry_count_key_hash,13,13,13
5,boundary_logical_bytes_key_weibull,46,46,46
5,boundary_logical_bytes_rolling_hash,90,91,91
```

## Cutover Raw Samples

```csv
process,name,median_ns,p95_ns,p99_ns
1,cutover_sorted_build,197,204,218
1,cutover_unsorted_build,100,111,130
1,cutover_append_1,18791,21000,24458
1,cutover_append_64,564,644,699
1,cutover_append_4096,189,218,236
1,cutover_middle_update,35209,40458,44542
1,cutover_middle_insert,206791,232833,269750
1,cutover_middle_delete,34500,38875,52917
2,cutover_sorted_build,197,207,222
2,cutover_unsorted_build,100,110,128
2,cutover_append_1,19000,24375,24750
2,cutover_append_64,565,637,696
2,cutover_append_4096,189,219,235
2,cutover_middle_update,35291,40209,47125
2,cutover_middle_insert,209750,228792,259750
2,cutover_middle_delete,34625,38542,44459
3,cutover_sorted_build,197,212,226
3,cutover_unsorted_build,106,131,214
3,cutover_append_1,18917,25166,28041
3,cutover_append_64,566,624,656
3,cutover_append_4096,194,220,231
3,cutover_middle_update,35417,39833,44042
3,cutover_middle_insert,208833,225291,277875
3,cutover_middle_delete,34542,38708,48042
4,cutover_sorted_build,197,205,218
4,cutover_unsorted_build,101,114,125
4,cutover_append_1,19542,23250,34125
4,cutover_append_64,562,577,643
4,cutover_append_4096,192,217,234
4,cutover_middle_update,35250,37750,40292
4,cutover_middle_insert,210334,248500,267084
4,cutover_middle_delete,34583,43750,52875
5,cutover_sorted_build,197,204,214
5,cutover_unsorted_build,101,114,140
5,cutover_append_1,18834,24167,32292
5,cutover_append_64,562,590,648
5,cutover_append_4096,192,215,226
5,cutover_middle_update,35334,41958,67584
5,cutover_middle_insert,212500,246750,266917
5,cutover_middle_delete,34583,37791,40333
```

## One-Million-Record Samples

Every `validated` field was true. All samples retained the exact same tree
shape (7,719 nodes, 7,644 leaves, 75 internal nodes, height 3), serialized tree
bytes (33,662,829), and workload I/O counters as the baseline.

```csv
version,workload,ns_per_op,nodes_read,nodes_written,bytes_read,bytes_written
hotpath-1,base_build,471.137,0,0,0,0
hotpath-1,random_reads,4852.490,0,0,0,0
hotpath-1,clustered_reads,552.121,0,0,0,0
hotpath-1,append_mutations,194.371,4,77,13526,349867
hotpath-1,random_mutations,17029.950,4440,4439,27218059,27216306
hotpath-1,clustered_mutations,368.146,76,74,385202,373843
hotpath-1,append_diff,192.650,81,0,363393,0
hotpath-1,random_diff,22888.263,8878,0,54432612,0
hotpath-1,clustered_diff,400.454,148,0,747686,0
hotpath-2,base_build,461.813,0,0,0,0
hotpath-2,random_reads,4851.491,0,0,0,0
hotpath-2,clustered_reads,494.263,0,0,0,0
hotpath-2,append_mutations,196.083,4,77,13526,349867
hotpath-2,random_mutations,17244.713,4440,4439,27218059,27216306
hotpath-2,clustered_mutations,424.058,76,74,385202,373843
hotpath-2,append_diff,197.042,81,0,363393,0
hotpath-2,random_diff,23214.108,8878,0,54432612,0
hotpath-2,clustered_diff,402.279,148,0,747686,0
hotpath-3,base_build,465.452,0,0,0,0
hotpath-3,random_reads,4834.205,0,0,0,0
hotpath-3,clustered_reads,495.359,0,0,0,0
hotpath-3,append_mutations,202.754,4,77,13526,349867
hotpath-3,random_mutations,17046.583,4440,4439,27218059,27216306
hotpath-3,clustered_mutations,492.817,76,74,385202,373843
hotpath-3,append_diff,207.721,81,0,363393,0
hotpath-3,random_diff,23234.000,8878,0,54432612,0
hotpath-3,clustered_diff,411.712,148,0,747686,0
```

The paired current process used 611,450,880 bytes peak RSS. The exact baseline
worktree used 629,866,496 bytes in the same session, a 2.9% reduction.

Same-session three-process medians are included only to expose the noisy
controls, not as the focused acceptance gate:

| Workload | Exact baseline ns/op | Current ns/op | Delta |
| --- | ---: | ---: | ---: |
| Base build | 474.866 | 465.452 | -2.0% |
| Random reads | 4,859.141 | 4,851.491 | -0.2% |
| Clustered reads | 499.928 | 495.359 | -0.9% |
| Append mutations | 196.671 | 196.083 | -0.3% |
| Random mutations | 17,329.279 | 17,046.583 | -1.6% |
| Clustered mutations | 365.562 | 424.058 | +16.0% noisy/inconclusive |
| Append diff | 217.942 | 197.042 | -9.6% |
| Random diff | 22,968.750 | 23,214.108 | +1.1% noisy/inconclusive |
| Clustered diff | 415.742 | 402.279 | -3.2% |

To isolate the apparent clustered-mutation outlier, commit `c41d4c4`
(immediately before the point-route edit) and current `36dbedc` were each run
three more times after the compiler burst subsided. Current ran second, so any
thermal drift was conservative:

| Workload | Before point route ns/op | Current ns/op | Delta |
| --- | ---: | ---: | ---: |
| Base build control | 461.900 | 466.419 | +1.0% |
| Random-read control | 4,844.113 | 4,863.689 | +0.4% |
| Clustered-read control | 497.455 | 497.924 | +0.1% |
| Append mutations | 196.242 | 196.050 | -0.1% |
| Random mutations | 17,370.033 | 17,122.008 | -1.4% |
| Clustered mutations | 383.850 | 363.875 | **-5.2%** |
| Append diff | 212.117 | 207.892 | -2.0% |
| Random diff | 23,282.188 | 22,996.283 | -1.2% |
| Clustered diff | 414.567 | 411.288 | -0.8% |

```csv
version,workload,ns_per_op
hotpath-4,base_build,466.419
hotpath-4,random_reads,4863.689
hotpath-4,clustered_reads,497.924
hotpath-4,append_mutations,190.071
hotpath-4,random_mutations,17094.683
hotpath-4,clustered_mutations,357.204
hotpath-4,append_diff,245.483
hotpath-4,random_diff,22996.283
hotpath-4,clustered_diff,411.317
hotpath-5,base_build,459.523
hotpath-5,random_reads,4851.243
hotpath-5,clustered_reads,493.727
hotpath-5,append_mutations,196.050
hotpath-5,random_mutations,17122.008
hotpath-5,clustered_mutations,375.854
hotpath-5,append_diff,207.892
hotpath-5,random_diff,22985.862
hotpath-5,clustered_diff,411.288
hotpath-6,base_build,466.999
hotpath-6,random_reads,4886.599
hotpath-6,clustered_reads,499.059
hotpath-6,append_mutations,212.879
hotpath-6,random_mutations,17290.487
hotpath-6,clustered_mutations,363.875
hotpath-6,append_diff,204.329
hotpath-6,random_diff,23211.821
hotpath-6,clustered_diff,408.692
```
