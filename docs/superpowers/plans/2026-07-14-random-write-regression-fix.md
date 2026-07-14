# Random Write Regression Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the scattered existing-key value-update regression while preserving canonical roots, configurable chunking, configurable node layouts, and the resynchronizing path for structural edits.

**Architecture:** Keep the canonical mutation dispatcher as the correctness authority. For sufficiently large value-only batches on stores that support ordered batch reads, route mutations to leaves in breadth-first batches, verify every key already exists and every replacement cannot enlarge its leaf encoding, then reuse the coalesced parallel leaf/ancestor rewrite. Any delete, insertion, potentially growing value, unsupported layout, small batch, or non-batched store remains on the existing canonical path.

**Tech Stack:** Rust, Rayon, existing `Store::batch_get_ordered`, the existing batch-route/coalesced-rebuild implementation, Cargo integration tests, and the scale benchmark harness.

## Global Constraints

- Do not add implementation comments or identifiers referring to the external reference implementation.
- Do not weaken canonical-root convergence for inserts, deletes, mutation order, chunking policy, or node layout.
- Do not merge to main until the full benchmark report and correctness/binding verification pass.
- Report every remaining regression honestly; no regression-free claim without fresh measurements.

---

### Task 1: Add a failing route-selection regression test

**Files:**
- Modify: `tests/canonical_write_stats.rs`

**Interfaces:**
- Consumes: `Prolly::canonical_batch_with_stats` and `CanonicalWriteStats`.
- Produces: a test requiring `used_batched_value_update_path: bool` and canonical-root equality for 1,000 scattered, same-width value updates.

- [ ] **Step 1: Write the failing test**

Add a test that builds 100,000 sorted records, updates 1,000 deterministic scattered existing keys with same-width values, and asserts the new route flag plus equality with a full sorted rebuild.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test canonical_write_stats scattered_value_updates_use_batched_canonical_rewrite -- --exact`

Expected: compilation fails because `CanonicalWriteStats::used_batched_value_update_path` does not exist.

### Task 2: Add the guarded key-stable batched rewrite

**Files:**
- Modify: `src/prolly/canonical.rs`
- Modify: `src/prolly/batch.rs`

**Interfaces:**
- Consumes: normalized sorted mutations, `group_mutations_by_leaf_with_paths_batched`, `apply_groups_coalesced`, and `BatchWriteCollector`.
- Produces: `batch::try_apply_batched_value_updates(...) -> Result<Option<KeyStableBatchResult>, Error>` and `CanonicalWriteStats::used_batched_value_update_path`.

- [ ] **Step 1: Add the internal result and eligibility validator**

The validator must reject fewer than 256 mutations, non-batched stores, missing keys, deletes, values longer than the value they replace, and mutation groups that do not map one-to-one onto existing leaf keys.

- [ ] **Step 2: Apply validated groups exactly once**

Use the existing coalesced rewrite and collector, return the root, affected/changed leaf counts, entries processed, and written node/byte counts, and flush atomically through the existing collector helper.

- [ ] **Step 3: Dispatch from the canonical key-stable path**

Attempt the guarded batched rewrite before the serial recursive rewrite. Populate canonical stats from the internal result and manager metric delta; otherwise continue through the existing serial or resynchronizing paths.

- [ ] **Step 4: Run the focused test to verify it passes**

Run: `cargo test --test canonical_write_stats scattered_value_updates_use_batched_canonical_rewrite -- --exact`

Expected: PASS.

### Task 3: Lock down fallbacks and format equivalence

**Files:**
- Modify: `tests/canonical_write_stats.rs`
- Modify: `tests/builder_policy_equivalence.rs`

**Interfaces:**
- Consumes: public batch APIs and all built-in node layouts/chunking policies.
- Produces: regression coverage proving structural mutations and potentially growing values bypass the optimization while value-only roots match full rebuilds.

- [ ] **Step 1: Add failing fallback/equivalence tests**

Cover a missing-key upsert, a delete, a longer replacement value, prefix-compressed/plain/offset-table layouts, and entry-count key-only policies. Assert canonical-root equality in every case and the route flag only for eligible batches.

- [ ] **Step 2: Run the focused tests**

Run: `cargo test --test canonical_write_stats --test builder_policy_equivalence`

Expected: PASS after the guarded implementation.

### Task 4: Prove the performance fix

**Files:**
- Modify: `performance-results/scale-2026-07-14/report.md`
- Modify: `performance-results/scale-2026-07-14/results.csv`
- Modify: `performance-results/scale-2026-07-14/run-manifest.csv`
- Modify: `performance-results/scale-2026-07-14/raw/*`

**Interfaces:**
- Consumes: `scripts/run_scale_report.sh` and original revision `fa7c219`.
- Produces: fresh three-run medians for all six sizes and nine workloads.

- [ ] **Step 1: Rerun the 100K differential loop**

Run the shared harness at 100K for three alternating repetitions. Require random mutations to be no slower than the original outside the report's ±3% noise band.

- [ ] **Step 2: Rerun the full matrix**

Run 1K, 10K, 50K, 100K, 1M, and 10M with three repetitions for append, random, and clustered read/mutation/diff workloads.

- [ ] **Step 3: Regenerate and audit the report**

Require every raw CSV to validate, every process to exit zero, and generated `results.csv`/`report.md` to reproduce byte-for-byte.

### Task 5: Full correctness and binding verification

**Files:**
- Modify only if verification exposes a regression.

**Interfaces:**
- Consumes: repository test, lint, documentation, formatting, conformance, and binding commands.
- Produces: fresh zero-exit evidence before merge review.

- [ ] **Step 1: Run Rust verification**

Run `cargo test --all-features`, `cargo clippy --all-features --all-targets -- -D warnings`, `RUSTDOCFLAGS='-D warnings' cargo doc --all-features --no-deps`, and `cargo fmt --all -- --check`.

- [ ] **Step 2: Run language-binding verification**

Run the repository's binding verification commands for Go, Java, Kotlin, Node, Python, Ruby, Swift, UniFFI, and WASM as documented in `bindings/VERIFICATION.md`.

- [ ] **Step 3: Remove diagnostic artifacts**

Delete the temporary profiling bench, its Cargo entry, all `[DEBUG-profile-random-write]` output code, temporary worktrees/targets, and root `Cargo.lock` if it was generated only by this investigation.

- [ ] **Step 4: Commit only verified production, tests, and updated evidence**

Commit with a message stating that serial canonical routing caused the scattered-write regression and that the fix restores guarded batched value-update rewriting.
