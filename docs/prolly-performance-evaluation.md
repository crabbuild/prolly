# Repeatable Rust/Go prolly performance evaluation

Use `scripts/run_prolly_evaluation.sh` for the canonical Dolt Go versus Rust comparison. One invocation builds and pins both implementations, runs one native Go baseline, runs bounded and unbounded Rust cache profiles, validates logical identity, captures peak RSS, and produces one combined report.

## Quick contract check

```sh
BENCH_PROFILE=smoke \
BENCH_OUT=performance-results/prolly-evaluation-smoke \
DOLT_REV=<full-dolt-commit-sha> \
scripts/run_prolly_evaluation.sh
```

The smoke profile uses 10,000 records, one repetition, a 1% random version workload, and no lifecycle matrix. It still runs all fresh and mutation tree localities.

## Canonical matrix

```sh
BENCH_PROFILE=canonical \
BENCH_OUT=performance-results/prolly-evaluation-$(date -u +%Y%m%dT%H%M%SZ) \
DOLT_REV=<full-dolt-commit-sha> \
scripts/run_prolly_evaluation.sh
```

Canonical defaults are:

- sizes: 10K, 50K, 1M, 5M, and 10M;
- repetitions: three process-isolated samples;
- tree phases: fresh build and 30% mutation;
- localities: append, random, and clustered;
- version densities: 0%, 1%, and 30%;
- Rust cache profiles: bounded and unbounded;
- one worker and in-memory storage;
- Rust lifecycle measurements enabled.

Always set `DOLT_REV` for a publishable rerun. If it is omitted, the driver resolves `origin/main` once and records the resulting commit, but a later invocation may resolve a newer revision.

## Resume an interrupted run

```sh
BENCH_PROFILE=canonical \
BENCH_OUT=performance-results/prolly-evaluation-20260720T200000Z \
BENCH_RESUME=1 \
scripts/run_prolly_evaluation.sh
```

Resume uses the immutable binaries captured under `bin/`; it does not fetch Dolt or rebuild by default. Every completed sample is revalidated against its durable state file before reuse. A changed configuration, source hash, framework hash, binary hash, revision, command, raw CSV, stderr, or timing/RSS artifact stops the run.

Set `BENCH_REBUILD_ON_RESUME=1` only when intentionally verifying that a rebuild is byte-identical to the captured provenance. It cannot be used to continue a run after changing code or configuration.

The output-directory lock prevents concurrent writers. A dead process lock is removed only during an explicit resume.

## Useful configuration

| Variable | Default | Purpose |
|---|---|---|
| `BENCH_PROFILE` | `canonical` | Select `smoke` or `canonical` defaults. |
| `BENCH_OUT` | Timestamped result directory | Immutable artifact destination. |
| `BENCH_RESUME` | `0` | Reuse validated completed samples when set to `1`. |
| `BENCH_SIZES` | Profile-specific | Space-separated record counts. |
| `BENCH_RUNS` | Profile-specific | Repetitions per process-isolated scenario. |
| `BENCH_DENSITIES` | Profile-specific | Version change densities from `0 1 30`. |
| `BENCH_LOCALITIES` | Profile-specific | Any ordered subset of `append random clustered`. |
| `BENCH_RUST_CACHE_PROFILES` | `bounded unbounded` | Rust cache profiles; canonical publication should retain both. |
| `BENCH_LIFECYCLE` | Profile-specific | Enable the separate Rust lifecycle matrix with `1`. |
| `BENCH_SKIP_SMOKE` | `0` | Skip pre-matrix parity smoke only during framework debugging. |
| `DOLT_REV` | Resolved `origin/main` | Exact Dolt commit to benchmark. |
| `DOLT_CACHE` | `target/dolt-benchmark` | Reusable Dolt checkout. |

## Artifacts

Each result directory contains:

- `manifest.txt`: immutable configuration fingerprint, source and binary hashes, revisions, matrix, and cache policies;
- `machine.txt`: host, OS, CPU, memory, Rust, and Go details;
- `bin/`: exact Rust and Go executables used by the run;
- `tree/`, `version/`, and `lifecycle/`: raw CSV, stderr, process timing, durable sample state, normalized results, summaries, and cache-effect CSVs;
- `report.md`: bounded and unbounded Rust results against the same Go samples;
- `WARNINGS`: present when lifecycle result digests differ between Rust cache profiles;
- `COMPLETE`: written only after strict matrix validation and report generation.

`COMPLETE` can coexist with `WARNINGS`: it means the configured matrix is intact and fully processed, while `WARNINGS` identifies lifecycle rows that still require correctness review before publication. Starting a resume removes `COMPLETE` until every saved sample has been revalidated and the report has been regenerated.

Raw filenames identify implementation, cache profile, scenario, and repetition. Go samples use the `native` cache profile. Rust samples use `bounded` or `unbounded`.

## Interpretation rules

The report labels a result `winner_flip` when winner direction changes across repetitions, `narrow` when medians differ by at most 5%, and `high_variance` when either implementation's coefficient of variation exceeds 25%. Do not advertise those rows as strong winner claims.

Peak RSS covers the complete scenario process, including untimed fixture construction and validation. Unbounded Rust means the decoded-node cache has no node or byte cap; normal engine safety limits and the host's physical-memory limit still apply.

The lifecycle workload contract compares deterministic logical inputs, operation counts, cardinalities, and validation flags across cache profiles. Version-ID-derived result digests are reported separately. A `DIVERGED` lifecycle result identity and the `WARNINGS` marker mean the logical workload completed but the profiles produced different version identifiers; treat those rows as a correctness investigation, not publishable performance evidence.
