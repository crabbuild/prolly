# Performance evaluation findings

## Decision

Do not merge this branch under a strict no-regression performance gate.

The enhanced implementation has large, repeatable gains in reads, random
updates, sparse diffs, delete diffs, and conflict-resolved merges. It also has
a confirmed large regression in clustered batch deletion and smaller
regressions in some build, append-merge, clustered-update, and
clustered-conflict-merge cases.

## Measurement scope

- Baseline revision: `fa7c219afc7e1ee5769dd85e5223ea5dde9e3074`
- Enhanced revision: `bf6fca50730e4dfc15c84e5ff328b9d2cec9a8af`
- Clean tracked worktree at matrix start: yes
- Shared harness hash: `6d89b3c8856bd157084a31de80654a68a7eded4f17e2a997e045b4dd1ded7268`
- Sizes: 1K, 10K, 50K, 100K, 1M, and 10M records
- Profiles: SQLite WAL with `synchronous=FULL` and `synchronous=NORMAL`
- Repetitions: five, with profile and revision order alternated
- Workloads: 24 covering build, cold/warm reads, append/random/clustered
  mutations, sparse and deletion diffs, disjoint merges, and conflict-resolved
  merges
- Measured processes: 2,880
- Failed or invalid processes: 0
- Aggregate comparisons: 288
- Material latency gains: 120
- Material latency regressions: 12
- Noise-sensitive latency comparisons: 156

Every process validated workload semantics and SQLite integrity. The runner
used APFS clonefile copies of immutable fixtures so fixture preparation was not
included in operation latency.

## Headline 10M results

All values below are five-run medians. Deltas are enhanced versus baseline.

| Workload | FULL | NORMAL | Assessment |
|---|---:|---:|---|
| Clustered cold reads | 1.003 s -> 549.800 ms (-45.2%) | 912.561 ms -> 541.476 ms (-40.7%) | Material gain |
| Right-edge warm reads | 888.048 ms -> 438.289 ms (-50.6%) | 875.366 ms -> 426.869 ms (-51.2%) | Material gain |
| Append batch upserts | 141.078 ms -> 118.851 ms (-15.8%) | 117.003 ms -> 111.154 ms (-5.0%) | Better median, noisy |
| Random batch updates | 7.263 s -> 6.188 s (-14.8%) | 6.873 s -> 5.609 s (-18.4%) | Better median, noisy |
| Clustered batch updates | 211.127 ms -> 211.638 ms (+0.2%) | 177.066 ms -> 199.932 ms (+12.9%) | NORMAL regression |
| Random batch deletes | 10.906 s -> 8.202 s (-24.8%) | 6.903 s -> 7.740 s (+12.1%) | Profile-dependent and noisy |
| Clustered batch deletes | 105.900 ms -> 389.257 ms (+267.6%) | 102.107 ms -> 340.643 ms (+233.6%) | Confirmed major regression |
| Random sparse diff | 1.753 s -> 1.246 s (-28.9%) | 1.710 s -> 1.157 s (-32.3%) | Material gain |
| Clustered delete diff | 311.853 ms -> 4.625 ms (-98.5%) | 303.812 ms -> 4.907 ms (-98.4%) | Material gain |
| Random delete diff | 3.059 s -> 1.647 s (-46.2%) | 2.462 s -> 1.677 s (-31.9%) | Material gain |
| Random conflict-resolved merge | 1.652 s -> 978.619 ms (-40.7%) | 1.655 s -> 962.904 ms (-41.8%) | Material gain |
| Clustered disjoint sparse merge | 10.777 ms -> 9.414 ms (-12.7%) | 10.079 ms -> 7.044 ms (-30.1%) | Better; NORMAL is material |
| Clustered conflict-resolved merge | 19.046 ms -> 14.638 ms (-23.1%) | 19.505 ms -> 24.344 ms (+24.8%) | Profile-dependent |
| Sorted stream build | 9.586 s -> 10.903 s (+13.7%) | 8.916 s -> 9.136 s (+2.5%) | FULL regression; NORMAL noisy |
| Shuffled batch build | 10.719 s -> 14.321 s (+33.6%) | 11.165 s -> 10.613 s (-4.9%) | Highly variable/profile-dependent |

## Delete-diff regression repair

Before the traversal repair, a shifted child boundary made eager diff hydrate
nearly the entire 10M tree:

- Clustered delete diff read 36,635 nodes and about 416 MB, taking 6.1-6.4 s.
- Random delete diff read 47,372 nodes and about 562 MB, taking 3.7-3.9 s.

After local boundary resynchronization and descent through divergent internal
frontiers:

- Clustered delete diff reads 48 nodes and about 521 KB, taking 4.6-4.9 ms.
- Random delete diff reads 21,545 nodes and about 294 MB, taking 1.65-1.68 s.

This removes the prior catastrophic latency, memory, and I/O regression.

## Remaining regression diagnosis

The largest unresolved issue is canonical clustered deletion. At 10M records,
the baseline reads 37 nodes while the enhanced path reads 177 nodes. The
canonical writer enumerates the internal leaf frontier and replays predecessor
context to re-establish deterministic boundaries. That preserves canonical
convergence but turns a localized clustered deletion into substantially more
tree traversal. Fixing this cleanly requires hierarchical canonical splicing
or equivalent subtree-level resynchronization; simply removing predecessor
context would weaken the format's canonical guarantees.

Random deletion is also more I/O intensive: at 10M it reads 21,391 nodes versus
8,559 and writes 10,741 versus 8,559. Its latency is profile-dependent, peak
RSS is 8.7-9.9% higher, and the post-operation SQLite fixture is 4.8% larger.

The enhanced layout generally reduces peak RSS by about 7-8% at 10M for
clustered and append workloads and keeps the main fixture-size change near
+0.4%. Small trees have proportionally larger fixed-format overhead, reaching
+13.6% at 1K. Random-delete fixtures are the material size exception at +4.8%.

## Verification status

- Rust formatting: passed
- Rust clippy, all targets/features with warnings denied: passed
- Rust tests, all features: passed; no failures
- SQLite store and deterministic workload tests: 18 passed
- UniFFI Rust facade: 26 passed
- Python: 16 passed
- Go: passed, including compilation of all examples
- Node native/TypeScript: 19 passed
- Browser WASM: Rust target build passed; 3 JavaScript tests passed
- Kotlin/JVM: 15 passed
- Java/JVM: 15 passed
- Swift fixture scenario: passed
- Ruby: not verified on this machine. The test dependency `ffi` 1.16.3 cannot
  compile because the installed Apple command-line SDK lacks the header needed
  by the installed Ruby headers. The failure occurs while installing the test
  dependency, before binding code or tests execute.

## Artifacts

- `report.md`: complete human-readable 288-row report
- `results.csv`: one aggregate row per profile, size, and workload
- `raw-results.csv`: all 2,880 validated measurement rows
- `run-manifest.csv`: exit and validation status plus raw artifact paths
- `machine.txt`: revisions, hashes, toolchain, machine, profiles, sizes, and
  workload provenance
- `raw/`: stdout, stderr, and `/usr/bin/time -l` output for every process
