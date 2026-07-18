# Canonical Streaming Hot-Path Optimization Baseline

Captured on 2026-07-17 before production hot-path edits, after measurement
commit `ec4c504`, on Apple M2 Max / Darwin arm64 / Rust 1.97.0. Cargo used the
optimized `bench` profile and `MemStore`.

## Commands

```bash
for run_id in 1 2 3 4 5; do
  PROLLY_BENCH_ONLY=boundary-hot-path cargo bench --bench prolly_bench --quiet
done
for run_id in 1 2 3 4 5; do
  PROLLY_BENCH_ONLY=chunking-cutover PROLLY_BENCH_SCALE=100000 \
    PROLLY_BENCH_ITERATIONS=100 cargo bench --bench prolly_bench --quiet
done
```

## Five-Process Median Summary

Values are ns/entry or ns/item. Each row is the median of the five process
medians; p95 is the median of the five process-local p95 values.

| Workload | Median | p95 |
| --- | ---: | ---: |
| Boundary entry-count key hash | 13 | 13 |
| Boundary logical-byte Weibull | 48 | 49 |
| Boundary logical-byte rolling hash | 236 | 273 |
| Sorted build | 199 | 205 |
| Unsorted build | 106 | 122 |
| Append 1 | 19,708 | 23,709 |
| Append 64 | 589 | 684 |
| Append 4,096 | 197 | 225 |
| Middle value update | 36,833 | 40,584 |
| Middle insert | 214,834 | 239,458 |
| Middle delete | 36,166 | 42,167 |

## Boundary Raw Samples

```csv
process,name,median_ns,p95_ns,p99_ns
1,boundary_entry_count_key_hash,12,13,13
1,boundary_logical_bytes_key_weibull,47,47,47
1,boundary_logical_bytes_rolling_hash,294,392,392
2,boundary_entry_count_key_hash,13,14,14
2,boundary_logical_bytes_key_weibull,50,52,52
2,boundary_logical_bytes_rolling_hash,237,240,240
3,boundary_entry_count_key_hash,13,13,13
3,boundary_logical_bytes_key_weibull,49,60,60
3,boundary_logical_bytes_rolling_hash,235,276,276
4,boundary_entry_count_key_hash,13,13,13
4,boundary_logical_bytes_key_weibull,48,49,49
4,boundary_logical_bytes_rolling_hash,236,273,273
5,boundary_entry_count_key_hash,13,14,14
5,boundary_logical_bytes_key_weibull,48,49,49
5,boundary_logical_bytes_rolling_hash,233,235,235
```

## Cutover Raw Samples

```csv
process,name,median_ns,p95_ns,p99_ns
1,cutover_sorted_build,200,208,216
1,cutover_unsorted_build,106,122,137
1,cutover_append_1,19625,23709,26000
1,cutover_append_64,589,753,986
1,cutover_append_4096,199,225,429
1,cutover_middle_update,36834,40584,47125
1,cutover_middle_insert,221417,547208,640083
1,cutover_middle_delete,37084,40292,55875
2,cutover_sorted_build,198,205,227
2,cutover_unsorted_build,106,114,129
2,cutover_append_1,19750,21250,24750
2,cutover_append_64,592,684,1139
2,cutover_append_4096,196,225,433
2,cutover_middle_update,36833,40291,47541
2,cutover_middle_insert,212958,243167,281917
2,cutover_middle_delete,35958,43417,66167
3,cutover_sorted_build,198,204,218
3,cutover_unsorted_build,105,126,136
3,cutover_append_1,19708,24416,51833
3,cutover_append_64,568,595,637
3,cutover_append_4096,197,227,385
3,cutover_middle_update,35667,41583,48708
3,cutover_middle_insert,212583,236709,274166
3,cutover_middle_delete,36166,42167,46750
4,cutover_sorted_build,199,205,217
4,cutover_unsorted_build,111,124,156
4,cutover_append_1,20709,79709,139542
4,cutover_append_64,589,698,839
4,cutover_append_4096,199,220,252
4,cutover_middle_update,36833,39084,40958
4,cutover_middle_insert,216250,234583,254583
4,cutover_middle_delete,36166,43666,87167
5,cutover_sorted_build,199,217,229
5,cutover_unsorted_build,105,117,132
5,cutover_append_1,19708,22042,24959
5,cutover_append_64,591,652,694
5,cutover_append_4096,196,216,227
5,cutover_middle_update,37042,43833,64042
5,cutover_middle_insert,214834,239458,253583
5,cutover_middle_delete,36333,40416,42459
```
