# Canonical Streaming Hard Cutover: After Measurements

Captured on 2026-07-17 against the implementation finalized in `fbafdf1`.

## Environment

- CPU: Apple M2 Max
- Memory: 34,359,738,368 bytes (32 GiB)
- OS: Darwin 25.5.0, arm64
- Rust: `rustc 1.97.0 (2d8144b78 2026-07-07)`
- Cargo: `cargo 1.97.0 (c980f4866 2026-06-30)`
- Profile: Cargo `bench`/optimized
- Store: in-memory `MemStore`
- Policy: entry-count, key-only, prefix-compressed unless noted

## Commands

The baseline selectors, scale, policy, and machine were unchanged. The latency
harness was extended symmetrically on baseline commit `c919ff8` and the cutover
to retain timed samples and emit nearest-rank median/p95/p99.

```bash
PROLLY_BENCH_ONLY=chunking-cutover PROLLY_BENCH_SCALE=100000 \
PROLLY_BENCH_ITERATIONS=100 cargo bench --bench prolly_bench

SCALE_VERSION=optimized-N SCALE_RECORDS=1000000 \
cargo bench --bench scale_workloads

/usr/bin/time -l env SCALE_VERSION=final-rss SCALE_RECORDS=1000000 \
cargo bench --bench scale_workloads
```

Build medians additionally use five independent processes with 20 timed
samples each, because sub-microsecond operation tails and process scheduling
otherwise contaminated the build controls.

## Latency Raw Summary

The mutation rows below are the paired 100-sample runs. Values are ns/item.

```csv
version,name,median_ns,p95_ns,p99_ns
before,cutover_append_1,21875,27208,28916
after,cutover_append_1,18792,23125,24250
before,cutover_append_64,619,627,729
after,cutover_append_64,563,593,647
before,cutover_append_4096,195,198,201
after,cutover_append_4096,188,197,202
before,cutover_middle_update,35125,36125,43167
after,cutover_middle_update,35250,36500,39125
before,cutover_middle_insert,207792,220417,226708
after,cutover_middle_insert,204625,216583,223667
before,cutover_middle_delete,34542,34875,39000
after,cutover_middle_delete,34542,37791,39792
```

Five-process build medians:

```csv
version,name,median_ns
before,cutover_sorted_build,192
after,cutover_sorted_build,186
before,cutover_unsorted_build,101
after,cutover_unsorted_build,102
```

## One-million-record Raw Samples

All `validated` fields were true. Tree shape and serialized bytes were
identical to baseline: 7,719 nodes, 7,644 leaves, 75 internal nodes, height 3,
and 33,662,829 tree bytes.

```csv
version,workload,ns_per_op,nodes_read,nodes_written,bytes_read,bytes_written
optimized-1,base_build,473.459,0,0,0,0
optimized-1,random_reads,4399.451,0,0,0,0
optimized-1,clustered_reads,509.146,0,0,0,0
optimized-1,append_mutations,215.004,4,77,13526,349867
optimized-1,random_mutations,18504.521,4440,4439,27218059,27216306
optimized-1,clustered_mutations,367.038,76,74,385202,373843
optimized-1,append_diff,208.358,81,0,363393,0
optimized-1,random_diff,22243.933,8878,0,54432612,0
optimized-1,clustered_diff,381.500,148,0,747686,0
optimized-2,base_build,442.411,0,0,0,0
optimized-2,random_reads,4480.556,0,0,0,0
optimized-2,clustered_reads,488.577,0,0,0,0
optimized-2,append_mutations,207.113,4,77,13526,349867
optimized-2,random_mutations,17654.517,4440,4439,27218059,27216306
optimized-2,clustered_mutations,347.446,76,74,385202,373843
optimized-2,append_diff,196.396,81,0,363393,0
optimized-2,random_diff,22205.904,8878,0,54432612,0
optimized-2,clustered_diff,407.887,148,0,747686,0
optimized-3,base_build,441.423,0,0,0,0
optimized-3,random_reads,4421.126,0,0,0,0
optimized-3,clustered_reads,481.877,0,0,0,0
optimized-3,append_mutations,201.254,4,77,13526,349867
optimized-3,random_mutations,17406.854,4440,4439,27218059,27216306
optimized-3,clustered_mutations,366.188,76,74,385202,373843
optimized-3,append_diff,202.283,81,0,363393,0
optimized-3,random_diff,22390.362,8878,0,54432612,0
optimized-3,clustered_diff,392.479,148,0,747686,0
optimized-4,base_build,445.832,0,0,0,0
optimized-4,random_reads,4397.098,0,0,0,0
optimized-4,clustered_reads,482.930,0,0,0,0
optimized-4,append_mutations,196.242,4,77,13526,349867
optimized-4,random_mutations,17913.537,4440,4439,27218059,27216306
optimized-4,clustered_mutations,378.254,76,74,385202,373843
optimized-4,append_diff,195.004,81,0,363393,0
optimized-4,random_diff,22198.046,8878,0,54432612,0
optimized-4,clustered_diff,388.413,148,0,747686,0
optimized-5,base_build,446.423,0,0,0,0
optimized-5,random_reads,4442.623,0,0,0,0
optimized-5,clustered_reads,484.112,0,0,0,0
optimized-5,append_mutations,201.012,4,77,13526,349867
optimized-5,random_mutations,17934.963,4440,4439,27218059,27216306
optimized-5,clustered_mutations,354.971,76,74,385202,373843
optimized-5,append_diff,195.725,81,0,363393,0
optimized-5,random_diff,22188.429,8878,0,54432612,0
optimized-5,clustered_diff,386.488,148,0,747686,0
```

Peak RSS was 641,335,296 bytes (611.63 MiB), versus 655,556,608 bytes
(625.19 MiB) before.

## Chunk Distributions

The deterministic corpus contains 250,000 records of 44 logical bytes.

```csv
policy,chunks,mean,median,p90,p99,max,forced,forced_ppm
rolling,645,17045,13772,33044,54824,65560,1,1550
weibull,704,15601,14960,25212,33880,44924,0,0
```
