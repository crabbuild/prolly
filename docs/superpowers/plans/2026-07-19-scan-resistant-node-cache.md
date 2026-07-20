# Scan-Resistant Node Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve bounded-cache point-read locality across large range scans without increasing default memory limits.

**Architecture:** Add internal admit and observe-only cache modes to the canonical async engine. Cold scans admit nodes; scans opened over a warm read cache reuse hits without admitting one-pass misses.

**Tech Stack:** Rust, the runtime-neutral `ProllyEngine`, sync ready adapter, existing `MemStore` integration tests.

## Global Constraints

- Correctness and CID validation are non-negotiable.
- Do not change public APIs, storage traits, node encoding, or persisted tree identity.
- Keep the existing 16,384-node and 256 MiB default limits.
- Implement the behavior once in the canonical async engine; sync uses the same path.

---

### Task 1: Specify scan-resistant cache behavior

**Files:**
- Modify: `tests/basic_ops.rs`
- Modify: `tests/async_store.rs`

**Interfaces:**
- Consumes: `Prolly::read`, `AsyncProlly::read`, `scan_range`, cache metrics.
- Produces: regression tests proving a full scan does not evict a primed point path.

- [ ] **Step 1: Write the failing sync regression test**

Build a small-chunk tree, clear the cache, prime its leftmost key through a
read session, scan the full tree through another session, and assert a new
session reads the primed key with zero cache misses.

- [ ] **Step 2: Run the sync test and verify RED**

Run: `cargo test --test basic_ops scan_preserves_hot_point_path_in_bounded_cache -- --exact`

Expected: FAIL because scan misses are admitted and evict the primed path.

- [ ] **Step 3: Write and run the equivalent async regression test**

Run: `cargo test --features async-store --test async_store async_scan_preserves_hot_point_path_in_bounded_cache -- --exact`

Expected: FAIL for the same canonical cache behavior.

### Task 2: Add cache access modes to the canonical engine

**Files:**
- Modify: `src/prolly/engine/mod.rs`
- Modify: `src/prolly/read.rs`

**Interfaces:**
- Produces: private `ReadCacheMode::{Admit, ObserveOnly}`, cached read-node
  accounting, and an adaptive loader used by cursor advancement.

- [ ] **Step 1: Add the minimal loader policy**

Normal bounded and unbounded hits keep the existing non-mutating fast path.
Observe-only hits also call `peek_read`. Observe-only misses still call
`get_shared` and `validation::decode_read` but skip bounded-cache insertion.

- [ ] **Step 2: Route cursor advancement through observe-only loading**

Record whether a session opened over decoded read nodes. Change forward and
reverse async cursor advancement, including owned range sessions, to use the
observe-only loader only for warm sessions. Keep cold scans and initial seek on
normal loading.

- [ ] **Step 3: Run the focused tests and verify GREEN**

Run:

```text
cargo test --test basic_ops scan_preserves_hot_point_path_in_bounded_cache -- --exact
cargo test --features async-store --test async_store async_scan_preserves_hot_point_path_in_bounded_cache -- --exact
```

Expected: both PASS.

### Task 3: Verify correctness and performance

**Files:**
- Modify only if a regression is found in the files above.

**Interfaces:**
- Consumes: completed cache behavior.
- Produces: test and benchmark evidence suitable for PR #24.

- [ ] **Step 1: Run cache and read regression suites**

Run: `cargo test --test basic_ops --test async_store --test execution_config`

Expected: all tests PASS.

- [ ] **Step 2: Run formatting, lint, and full tests**

Run:

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

Expected: all commands exit successfully.

- [ ] **Step 3: Rerun the matched five-million-record benchmark**

Run: `target/release/prolly_compare --records 5000000 --phase fresh --workload random`

Expected: output validates all operations; write throughput remains within
normal run variance, and range scan performs no cache-admission evictions after
the seek path.

- [ ] **Step 4: Review, commit, and push**

Inspect the complete diff, commit the cache enhancement, and push
`codex/async-first-prolly-engine` to update PR #24.
