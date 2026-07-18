# Go Binding vs Native Go: Reproducibility Verification

## Verdict

The performance conclusion reproduced, but the absolute timings are not
lab-grade stable on this shared workstation.

- Correctness and workload identity: **reproduced exactly**.
- Write winner: **12/12 median cells agree** between the original and rerun.
- Point-read winner: **12/12 median cells agree**.
- Full-scan winner: **11/12 median cells agree**.
- Overall: **35/36 median winners agree**.
- The sole winner change is a near-tie: the 5M fresh-append scan moved from
  native Go faster by 1.004x to the Rust Go binding faster by 1.038x.
- The 10M point-read target is unchanged: **4/6 cells reach at least 1.5x** in
  both runs. A universal 1.5–2x point-read claim is still not supported.

The binding/native-Go latency ratio changed by 3.34% at the median, 9.95% at
the 90th percentile, and 10.88% at the maximum across the 36 cells. That is
good directional reproducibility for this noisy host, but it is not a basis
for narrow confidence intervals.

## Reproduction contract

Both runs used the same product artifacts, benchmark parameters, logical
inputs, and result validation:

```text
host: Apple M2 Max, 32 GiB, Darwin arm64
storage: in-memory
workers: 1
sizes: 5,000,000 and 10,000,000 base records
repetitions: 3 per implementation/scenario
phases: fresh and 30% mutation
orders: append, deterministic random, clustered (1,000-key clusters)
mutation mix: 50% inserts / 50% updates for random and clustered
point reads: up to 100,000 existing keys
scan: complete resulting tree
binding point API: ReadSession.GetView
binding scan API: ReadSession.ScanRangeView, 4,096-entry retained pages
```

Artifact identity was verified byte-for-byte:

```text
Rust Go benchmark executable:
5560175cdb659a3042ebd523329d3cff9526ca4dbc390e3af1494e391294937b

Native Go benchmark executable:
fcdaf1c346cd41b0309b888a359568152786249e4112f065e00a563582ab4b45

Loaded Rust dylib:
391fb51824d6f715bbf3b1213b1746340d1c0f89b69b31480ec1fafaad9d144c
```

The top-level repository revision differs because the first aggregate report
was committed after its run. The embedded binding source revision, native Go
runner revision, benchmark executable hashes, and dynamically loaded Rust
library hash are identical. The hashes, rather than the report-only commit,
identify the measured code.

## Integrity gate

The completed rerun passed all structural and correctness checks:

| Check | Result |
|---|---:|
| Process executions | 72/72 present |
| Process exit status | 72/72 zero |
| Normalized operation rows | 216/216 present |
| Validated rows | 216/216 true |
| Unique operation/run keys | 216/216 |
| Repetitions per implementation/cell | 3/3 |
| Paired logical result groups | 108/108 |
| Rust/native digest, count, and operation parity | 108/108 |
| Original/rerun digest, count, operation, and validation parity | 216/216 |

Validation covers workload digests, operation counts, result cardinality,
point values, and scan ordering. It does not merely compare elapsed time.

## Cross-run performance stability

The table reports the absolute percentage change in the binding/native-Go
median latency ratio. Lower is more reproducible.

| Operation | Winner agreement | Median change | P90 change | Maximum change |
|---|---:|---:|---:|---:|
| Write | 12/12 | 2.54% | 10.10% | 10.78% |
| Point read | 12/12 | 3.21% | 4.24% | 9.46% |
| Full scan | 11/12 | 4.52% | 10.28% | 10.88% |
| **All cells** | **35/36** | **3.34%** | **9.95%** | **10.88%** |

Absolute median latency shifted by 2.71% at the median for the Rust binding
and 2.70% for native Go. Their P90 shifts were 7.59% and 8.48%, respectively.
This supports comparing broad performance margins; it does not support
treating sub-5% differences as durable wins.

Across paired individual repetitions, the binding won 103/108 operations in
the original run and 104/108 in the rerun.

## Speedups: original to rerun

Values are native-Go latency divided by Rust-binding latency. Values above
1.0 favor the Rust binding; values below 1.0 favor native Go.

| Records | Phase | Order | Write | Point read | Full scan |
|---:|---|---|---:|---:|---:|
| 5M | Fresh | Append | 11.255 -> 12.615x | 1.581 -> 1.580x | 0.996 -> 1.038x |
| 5M | Fresh | Random | 4.691 -> 4.986x | 1.549 -> 1.600x | 1.021 -> 1.107x |
| 5M | Fresh | Clustered | 5.505 -> 5.733x | 1.556 -> 1.618x | 1.007 -> 1.039x |
| 5M | Mutation | Append | 5.345 -> 5.379x | 1.976 -> 1.985x | 1.148 -> 1.038x |
| 5M | Mutation | Random | 1.475 -> 1.585x | 1.714 -> 1.774x | 1.019 -> 1.066x |
| 5M | Mutation | Clustered | 3.120 -> 3.186x | 2.034 -> 2.048x | 1.008 -> 1.056x |
| 10M | Fresh | Append | 26.270 -> 26.686x | 1.422 -> 1.570x | 1.155 -> 1.042x |
| 10M | Fresh | Random | 9.051 -> 8.978x | 1.359 -> 1.304x | 1.031 -> 1.011x |
| 10M | Fresh | Clustered | 10.946 -> 11.191x | 1.534 -> 1.499x | 1.061 -> 1.056x |
| 10M | Mutation | Append | 7.895 -> 8.131x | 1.953 -> 2.032x | 1.048 -> 1.103x |
| 10M | Mutation | Random | 1.703 -> 1.902x | 1.835 -> 1.810x | 1.029 -> 1.004x |
| 10M | Mutation | Clustered | 5.550 -> 5.535x | 2.143 -> 2.215x | 1.110 -> 1.056x |

The rerun's observed ranges are:

| Records | Write | Point read | Full scan |
|---:|---:|---:|---:|
| 5M | 1.585–12.615x | 1.580–2.048x | 1.038–1.107x |
| 10M | 1.902–26.686x | 1.304–2.215x | 1.004–1.103x |

## Memory and host-noise disclosure

The Rust binding used more median peak RSS in all 12 scenarios in both runs.
The rerun's highest median peak RSS was 9.51 GiB for the binding versus 6.94
GiB for native Go. Binding RSS changed more between runs: median 1.20%, P90
9.63%, maximum 10.16%; native Go's corresponding shifts were 0.26%, 0.45%,
and 0.55%.

The original run began with 5.51 GiB swap used and ended with 7.09 GiB. The
rerun began with 22.87 GiB swap used and ended with 20.14 GiB. This machine-wide
state is a material limitation even though each benchmark process was isolated
and Rust/native execution was alternated.

During verification, an earlier attempt was completely excluded after another
benchmark overlapped it. When an external diagnostic or build appeared during
the retained attempt, the active runner was stopped, affected process rows were
discarded, and those exact process keys were rerun through the harness's
hash-checked resume mode. No known overlapping benchmark/build process row is
present in the final 72-process manifest. Normal desktop, browser, and VM load
was still present, so this is an honest shared-workstation result—not an idle
dedicated-host result.

## Exact rerun command

From the repository root:

```bash
BENCH_OUT=performance-results/go-binding-vs-native-go-large-optimized-reproducibility-rerun-2026-07-17 \
BENCH_SIZES='5000000 10000000' \
BENCH_RUNS=3 \
BENCH_LARGE_RUNS=3 \
BENCH_RESUME=1 \
PROLLY_COMPARE_POINT_API=view \
PROLLY_COMPARE_SCAN_API=view \
./scripts/run_go_binding_comparison.sh
```

For a publication-quality follow-up, run the same immutable binaries on an
idle dedicated host, disable unrelated VM/browser workloads, record thermal and
CPU-frequency state, use at least 10 repetitions, randomize pair order, and
report bootstrap confidence intervals. The current two-run evidence is strong
enough for the broad conclusion that the Rust Go binding is materially faster
for writes and point reads in most tested cells. Scan differences are small and
should be described as near parity to modest binding wins.
