# Canonical range-delete verification and merge gate

## Scope

This evaluation compares current revision `e1505427bc3feaf4aa018c68d7507f5aa48371c2` with the original revision `fa7c219afc7e1ee5769dd85e5223ea5dde9e3074`. The runner recorded the common benchmark-harness SHA-256 as `ab97654ff20f9b5e59b2cc5dc3fa6800ce4193a16f51f345637f4b807f2cab49`; it builds both revisions from the same copied harness.

The complete machine, compiler, SQLite, binary-hash, fixture-cloning, profile, and run-order metadata are in [machine.txt](machine.txt). The generated aggregate is [report.md](report.md), with per-process stdout, stderr, and timing in `raw/`.

## Source verification

All of the following completed with exit status zero against `e150542` before the benchmark run:

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `git diff --check`
- `cargo test --all-features`

The all-features suite completed 936 tests without failures, with 3 explicitly ignored tests. Its doctest phase reported 97 passed and 2 of those ignored tests. The separately rerun binding/native async command also completed successfully; detailed binding evidence is in [bindings-verification.md](bindings-verification.md).

## Raw-data audit

- `run-manifest.csv` contains 80 process rows: 2 revisions × 2 SQLite profiles × 2 record counts × 5 runs × (setup build plus clustered delete).
- All 80 rows have `exit_status=0` and `validation=ok`, and all 80 manifest tuples are unique.
- `raw-results.csv` contains the matching 80 rows. Every row has `validated=true` and `status=ok`.
- The aggregate contains eight five-run groups: the requested clustered-delete workload plus the runner's paired `sorted_stream_build` fixture setup for each profile and size.
- Clustered deletion has 10,000 operations in every measured row. It leaves 990,000 entries at 1M records and 9,990,000 at 10M records. Build rows contain 1,000,000 or 10,000,000 operations and the full input entry count.
- The runner also performed `PRAGMA wal_checkpoint(TRUNCATE)` and `PRAGMA integrity_check` after each build fixture before accepting workload rows.

## Clustered range-delete performance

Lower time is better; each value is the median of five alternating-order runs. Full ranges and classifications are retained in [report.md](report.md).

| SQLite profile | Records | Original median | Current median | Delta | Classification |
|---|---:|---:|---:|---:|---|
| WAL+FULL | 1,000,000 | 66.856 ms | 22.361 ms | -66.6% | material gain |
| WAL+NORMAL | 1,000,000 | 66.817 ms | 21.535 ms | -67.8% | material gain |
| WAL+FULL | 10,000,000 | 114.713 ms | 44.029 ms | -61.6% | noise-sensitive (broad ranges) |
| WAL+NORMAL | 10,000,000 | 97.504 ms | 43.067 ms | -55.8% | material gain |

No clustered-delete group has a material latency regression. The optimized path reduces median nodes read from 39 to 9 at 1M records and from 37 to 10 at 10M records, with substantially lower read bytes in both profiles.

## Residual observations

- At 10M, current median bytes written increase from 19,251 to 52,518 for both FULL and NORMAL. The report therefore marks a Prolly I/O regression, even though total median latency improves by more than 55%, fixture growth is only about 0.4%, and node writes remain four in both revisions. This is a real monitoring concern, not a hidden success criterion.
- The 10M WAL+FULL current and original ranges overlap broadly because of isolated slow samples, so the report classifies that large median gain as `noise-sensitive`; it is not counted as a material gain or loss.
- The binding matrix has one external host limitation: Ruby dependency resolution cannot locate a locally installed `ffi` gem satisfying `>= 1.15, < 1.17`. Both Ruby commands fail before test or cookbook code loads; they are not counted as passing. Details and the successful non-Ruby matrix are in [bindings-verification.md](bindings-verification.md).

## Merge-gate decision

**Recommendation: merge-ready by the stated range-delete gate, with the external Ruby host prerequisite recorded. Do not merge `main` as part of this task.**

The correctness rows all validate, source checks and the full Rust suite pass, every runnable binding check passes, and neither 1M nor 10M exhibits a material latency regression against the original revision. The Ruby issue is an explicitly external dependency-resolution blocker rather than a binding-test result. The 10M write-I/O increase should be monitored in follow-up benchmarking, but it does not meet this task's latency-blocking condition.
