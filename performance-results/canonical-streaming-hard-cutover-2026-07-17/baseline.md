# Canonical Streaming Hard Cutover Baseline

Captured before production changes on 2026-07-17 UTC.

## Environment

- Commit: `26e0c73fcc157836e5eee5f4abb0f831d10154d3`
- Branch: `codex/canonical-streaming-cutover`
- CPU: Apple M2 Max
- Memory: 34,359,738,368 bytes (32 GiB)
- OS: Darwin 25.5.0, arm64
- Rust: `rustc 1.97.0 (2d8144b78 2026-07-07)`
- Cargo: `cargo 1.97.0 (c980f4866 2026-06-30)`
- Profile: Cargo `bench`/optimized
- Store: in-memory `MemStore`
- Default benchmark policy: entry-count, key-only, prefix-compressed

## Known Correctness Baseline

The pre-cutover focused suite passed 21 tests. The two new regression tests fail
as intended:

- `direct_batch_writer_stats_matches_canonical_root_for_every_policy`: direct
  stats writer returns a different root from canonical bulk construction.
- `rolling_logical_bytes_tracks_target_distribution`: observed mean 62,824
  logical bytes versus the 16,384-byte target.

## Latency/Throughput Command

```bash
PROLLY_BENCH_ONLY=chunking-cutover \
PROLLY_BENCH_SCALE=100000 \
PROLLY_BENCH_ITERATIONS=20 \
cargo bench --bench prolly_bench
```

The harness performs one untimed warmup and reports the aggregate of 20 timed
iterations.

```csv
name,total_ms,iterations,items,ns_per_item
cutover_sorted_build,407.830,20,100000,203
cutover_unsorted_build,210.028,20,100000,105
cutover_append_1,0.506,20,1,25306
cutover_append_64,0.863,20,64,674
cutover_append_4096,16.878,20,4096,206
cutover_middle_update,0.744,20,1,37210
cutover_middle_insert,4.375,20,1,218747
cutover_middle_delete,0.725,20,1,36237
```

## One-million-record Scale Commands

Five independent process samples used:

```bash
SCALE_VERSION=before SCALE_RECORDS=1000000 cargo bench --bench scale_workloads
SCALE_VERSION=before-rss SCALE_RECORDS=1000000 cargo bench --bench scale_workloads
SCALE_VERSION=before-2 SCALE_RECORDS=1000000 cargo bench --bench scale_workloads
SCALE_VERSION=before-3 SCALE_RECORDS=1000000 cargo bench --bench scale_workloads
SCALE_VERSION=before-4 SCALE_RECORDS=1000000 cargo bench --bench scale_workloads
```

The second sample was wrapped in `/usr/bin/time -l`; peak RSS was 655,556,608
bytes (625.19 MiB).

```csv
version,records,workload,operations,total_ns,ns_per_op,validated,nodes_read,nodes_written,bytes_read,bytes_written,num_nodes,num_leaves,num_internal,height,tree_bytes
before,1000000,base_build,1000000,470716709,470.717,true,0,0,0,0,7719,7644,75,3,33662829
before,1000000,random_reads,1000000,4555880750,4555.881,true,0,0,0,0,7719,7644,75,3,33662829
before,1000000,clustered_reads,1000000,484669083,484.669,true,0,0,0,0,7719,7644,75,3,33662829
before,1000000,append_mutations,10000,2296792,229.679,true,4,77,13526,349867,7792,7717,75,3,33999170
before,1000000,random_mutations,10000,175175834,17517.583,true,4440,4439,27218059,27216306,7719,7644,75,3,33662829
before,1000000,clustered_mutations,10000,3577375,357.738,true,76,74,385202,373843,7719,7644,75,3,33662829
before,1000000,append_diff,10000,2138083,213.808,true,81,0,363393,0,7792,7717,75,3,33999170
before,1000000,random_diff,10000,230246292,23024.629,true,8878,0,54432612,0,7719,7644,75,3,33662829
before,1000000,clustered_diff,10000,3945375,394.538,true,148,0,747686,0,7719,7644,75,3,33662829
before-rss,1000000,base_build,1000000,447528417,447.528,true,0,0,0,0,7719,7644,75,3,33662829
before-rss,1000000,random_reads,1000000,4417910208,4417.910,true,0,0,0,0,7719,7644,75,3,33662829
before-rss,1000000,clustered_reads,1000000,487512542,487.513,true,0,0,0,0,7719,7644,75,3,33662829
before-rss,1000000,append_mutations,10000,2076041,207.604,true,4,77,13526,349867,7792,7717,75,3,33999170
before-rss,1000000,random_mutations,10000,176405292,17640.529,true,4440,4439,27218059,27216306,7719,7644,75,3,33662829
before-rss,1000000,clustered_mutations,10000,3562833,356.283,true,76,74,385202,373843,7719,7644,75,3,33662829
before-rss,1000000,append_diff,10000,1944459,194.446,true,81,0,363393,0,7792,7717,75,3,33999170
before-rss,1000000,random_diff,10000,224682084,22468.208,true,8878,0,54432612,0,7719,7644,75,3,33662829
before-rss,1000000,clustered_diff,10000,4002542,400.254,true,148,0,747686,0,7719,7644,75,3,33662829
before-2,1000000,base_build,1000000,442367708,442.368,true,0,0,0,0,7719,7644,75,3,33662829
before-2,1000000,random_reads,1000000,4612307459,4612.307,true,0,0,0,0,7719,7644,75,3,33662829
before-2,1000000,clustered_reads,1000000,508021625,508.022,true,0,0,0,0,7719,7644,75,3,33662829
before-2,1000000,append_mutations,10000,2199084,219.908,true,4,77,13526,349867,7792,7717,75,3,33999170
before-2,1000000,random_mutations,10000,248288042,24828.804,true,4440,4439,27218059,27216306,7719,7644,75,3,33662829
before-2,1000000,clustered_mutations,10000,3570084,357.008,true,76,74,385202,373843,7719,7644,75,3,33662829
before-2,1000000,append_diff,10000,2046000,204.600,true,81,0,363393,0,7792,7717,75,3,33999170
before-2,1000000,random_diff,10000,234890875,23489.088,true,8878,0,54432612,0,7719,7644,75,3,33662829
before-2,1000000,clustered_diff,10000,3952250,395.225,true,148,0,747686,0,7719,7644,75,3,33662829
before-3,1000000,base_build,1000000,449662792,449.663,true,0,0,0,0,7719,7644,75,3,33662829
before-3,1000000,random_reads,1000000,5026060000,5026.060,true,0,0,0,0,7719,7644,75,3,33662829
before-3,1000000,clustered_reads,1000000,595250583,595.251,true,0,0,0,0,7719,7644,75,3,33662829
before-3,1000000,append_mutations,10000,2483917,248.392,true,4,77,13526,349867,7792,7717,75,3,33999170
before-3,1000000,random_mutations,10000,194116792,19411.679,true,4440,4439,27218059,27216306,7719,7644,75,3,33662829
before-3,1000000,clustered_mutations,10000,4306875,430.688,true,76,74,385202,373843,7719,7644,75,3,33662829
before-3,1000000,append_diff,10000,2376375,237.637,true,81,0,363393,0,7792,7717,75,3,33999170
before-3,1000000,random_diff,10000,259479917,25947.992,true,8878,0,54432612,0,7719,7644,75,3,33662829
before-3,1000000,clustered_diff,10000,4465958,446.596,true,148,0,747686,0,7719,7644,75,3,33662829
before-4,1000000,base_build,1000000,531946667,531.947,true,0,0,0,0,7719,7644,75,3,33662829
before-4,1000000,random_reads,1000000,4458338708,4458.339,true,0,0,0,0,7719,7644,75,3,33662829
before-4,1000000,clustered_reads,1000000,502695625,502.696,true,0,0,0,0,7719,7644,75,3,33662829
before-4,1000000,append_mutations,10000,2110667,211.067,true,4,77,13526,349867,7792,7717,75,3,33999170
before-4,1000000,random_mutations,10000,195259416,19525.942,true,4440,4439,27218059,27216306,7719,7644,75,3,33662829
before-4,1000000,clustered_mutations,10000,4955167,495.517,true,76,74,385202,373843,7719,7644,75,3,33662829
before-4,1000000,append_diff,10000,2281209,228.121,true,81,0,363393,0,7792,7717,75,3,33999170
before-4,1000000,random_diff,10000,266394542,26639.454,true,8878,0,54432612,0,7719,7644,75,3,33662829
before-4,1000000,clustered_diff,10000,4517292,451.729,true,148,0,747686,0,7719,7644,75,3,33662829
```

## Median Scale Baseline

| Workload | Median ns/op |
|---|---:|
| Base sorted build | 449.663 |
| Random read | 4,555.881 |
| Clustered read | 502.696 |
| Append mutation | 219.908 |
| Random mutation | 19,411.679 |
| Clustered mutation | 357.738 |

The after-cutover report must repeat these exact commands and report both raw
samples and percentage deltas. Correctness failures invalidate performance
comparisons.
