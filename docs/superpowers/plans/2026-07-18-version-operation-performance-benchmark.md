# Version-Operation Performance Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. This plan intentionally uses implementation-first validation rather than TDD, per the user's explicit instruction.

**Goal:** Build and run a reproducible native Rust versus Dolt Go benchmark for diff, range diff, patch, and merge operations, plus a separate Rust `VersionedMap` lifecycle benchmark.

**Architecture:** Two optimized native common-operation runners independently generate the same deterministic base and branch contents and emit a shared CSV contract. A separate Rust lifecycle runner measures managed version operations. A shell harness pins sources and binaries, runs scenario-level process isolation, validates cross-language parity, and feeds a Python summarizer.

**Tech Stack:** Rust 2024 edition and the repository's `prolly` crate, Go 1.26 and Dolt `store/prolly/tree`, POSIX shell, Python 3 standard library, CSV and Markdown artifacts.

## Global Constraints

- Implement directly without a red-green TDD cycle.
- Work only in `/Users/haipingfu/CrabDB-worktrees/prolly-version-performance-benchmark` on `codex/version-performance-benchmark`.
- Do not modify the conflicted primary worktree.
- Use in-memory stores, one worker, optimized binaries, and fresh processes per scenario/repetition.
- Use sizes 10K, 50K, 1M, 5M, and 10M with three repetitions at every size.
- Use 0%, 1%, and 30% densities; omit duplicate locality variants at 0%.
- Use deterministic values shorter than 100 bytes and versioned integer-only workload generation.
- Fail closed on every parity, digest, count, conflict, patch, merge, lifecycle, or matrix validation error.

---

### Task 1: Rust common workload and operation runner

**Files:**
- Create: `src/bin/prolly_version_support/mod.rs`
- Create: `src/bin/prolly_version_compare.rs`

**Interfaces:**
- Produces deterministic base/branch generators, edit summaries, ordered FNV digests, and CSV helpers.
- Produces CLI `prolly_version_compare --records N --density {0|1|30} --locality {none|append|random|clustered}`.
- Emits rows for `full_diff`, `range_diff`, `patch_generate`, `patch_apply`, `merge_disjoint`, `merge_convergent`, and `merge_conflict` when applicable.

- [ ] Implement fixed-width keys, SplitMix64 values, edit allocation, branch relationships, range bounds, expected `BTreeMap` contents, and FNV folding.
- [ ] Build native `Prolly<MemStore>` trees outside timing and time complete diff, range-diff, patch, and merge operations.
- [ ] Validate complete ordered output, counts, conflicts, and digests after timing; emit the shared CSV schema only for validated results.
- [ ] Run `cargo fmt -- src/bin/prolly_version_compare.rs src/bin/prolly_version_support/mod.rs` and `cargo build --release --bin prolly_version_compare`.
- [ ] Run 10K 0%-none and 1%-random focused scenarios and require the defined validated operation rows.

### Task 2: Native Dolt Go common runner

**Files:**
- Create: `benchmarks/dolt-prolly-version-compare/main.go`
- Create: `benchmarks/dolt-prolly-version-compare/workload.go`

**Interfaces:**
- Consumes the exact CLI and contract constants from Task 1.
- Produces the same CSV header, metadata, logical digests, counts, conflicts, and operation sequence as Rust.
- Uses native Dolt ordered-tree diff, key-range diff, patch generator/application, and three-way merge APIs.

- [ ] Port SplitMix64, fixed-width keys, edit allocation, branches, range bounds, and FNV folding with identical integer behavior.
- [ ] Construct Dolt in-memory maps outside timing and fully consume native diff and patch streams.
- [ ] Apply pre-generated patches, run merge relationships with prefer-left collision handling, and validate complete logical output.
- [ ] Return non-zero on mismatch rather than emitting an unvalidated timing.
- [ ] Copy into a pinned Dolt checkout, run `gofmt`, build `./cmd/prolly-version-compare`, and match the Rust 10K scenario contract.

### Task 3: Rust `VersionedMap` lifecycle runner

**Files:**
- Create: `src/bin/prolly_version_lifecycle.rs`
- Reuse: `src/bin/prolly_version_support/mod.rs`

**Interfaces:**
- Produces CLI `prolly_version_lifecycle --records N --scenario {publish|read|rollback|prune}` with density/locality arguments for publication.
- Emits `version_publish`, `head_resolve`, `snapshot_resolve`, `historical_point_read`, `historical_range_scan`, `version_list`, `rollback`, and `retention_prune` rows.

- [ ] Build exactly 100 versions, including the base, with 99 deterministic small deltas outside timing.
- [ ] Time one 1% or 30% version publication and validate its content/version identity.
- [ ] Time repeated head/snapshot resolution, 100K historical reads, historical scans, and full catalog listing.
- [ ] Time rollback between retained versions and pruning that must retain 11 and remove 89 versions.
- [ ] Format, release-build, and run 10K `read` and `prune` smoke scenarios with every row validated.

### Task 4: Orchestration and parity validation

**Files:**
- Create: `scripts/run_prolly_version_comparison.sh`

**Interfaces:**
- Consumes `BENCH_OUT`, `BENCH_SIZES`, `BENCH_RUNS`, `DOLT_REV`, and `DOLT_CACHE`.
- Produces provenance, manifest, common/lifecycle results, copied binaries, raw outputs, and smoke artifacts.

- [ ] Follow the existing comparison harness to pin a detached Dolt revision and copy the checked-in Go runner into it.
- [ ] Build all three optimized binaries and record Git/source/binary hashes, compiler versions, host data, seeds, and policies.
- [ ] Validate headers, operation sets, metadata, finite timings, and `validated=true` before accepting output.
- [ ] Compare Rust/Go workload digests, unit counts, result counts, conflict counts, and result digests before appending paired rows.
- [ ] Run 10K zero-density and sparse-random smoke scenarios, then the complete requested common and lifecycle matrices.
- [ ] Run `sh -n scripts/run_prolly_version_comparison.sh` and a one-size/one-repetition smoke output.

### Task 5: Summarizer and reproducibility audit

**Files:**
- Create: `scripts/summarize_prolly_version_comparison.py`

**Interfaces:**
- Consumes common/lifecycle results, manifest, and provenance files.
- Produces `summary-common.csv`, `summary-lifecycle.csv`, `reproducibility.csv`, and `report.md`.

- [ ] Reject missing repetitions, duplicates, mismatched revisions, invalid rows, incomplete implementation pairs, or inconsistent workload/result identity.
- [ ] Compute median latency, normalized throughput, Rust/Go speedup, winner, population standard deviation, and coefficient of variation.
- [ ] Render separate common-comparison and Rust-lifecycle report sections, provenance, winner totals, tables, and variation warnings above 10% CV.
- [ ] Run `python3 -m py_compile scripts/summarize_prolly_version_comparison.py` and summarize smoke results.
- [ ] Confirm a deliberately corrupted copied smoke result causes a non-zero validation exit.

### Task 6: Full verification and publication artifacts

**Files:**
- Modify: `docs/prolly-go-rust-benchmark.md`
- Generate: `performance-results/prolly-version-2026-07-18/`

**Interfaces:**
- Consumes the complete harness.
- Produces the verified full report and documented reproduction command.

- [ ] Run focused Rust formatting/build, Go formatting/build, shell syntax, and Python compilation checks.
- [ ] Run the full five-size, three-repetition matrix with `BENCH_OUT=performance-results/prolly-version-2026-07-18`.
- [ ] Independently recompute binary hashes, expected row/process counts, parity identities, repetitions, winner consistency, and median/p95/max CV.
- [ ] Document branch, commits, hashes, command, methodology, result path, caveats, and verified conclusions.
- [ ] Commit only benchmark source, scripts, plan/spec, and benchmark documentation; do not commit large generated raw results unless existing repository policy requires it.
