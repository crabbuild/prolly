# Canonical Streaming Hot-Path Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Subagent execution is prohibited by the workspace instructions.

**Goal:** Reduce rolling-detector, cascade, and point-mutation CPU/allocation cost without changing any persisted output or regressing protected workloads.

**Architecture:** Preserve the current canonical algorithms and optimize only their transient state. Add lazy memoization and a circular rolling window, reuse hierarchy cascade scratch, and classify point-update routing before allocating batched-adapter representations. Measure and commit each optimization independently.

**Tech Stack:** Rust 1.81+ library code, Cargo release benchmarks, existing deterministic canonical-root and store-failure tests.

## Global Constraints

- Do not change persisted format, boundary placement, node bytes, CIDs, or public APIs.
- Add no unsafe code and no runtime dependency.
- The user explicitly waived mandatory TDD sequencing; regression tests remain required.
- Preserve unrelated untracked files, including `performance-results/dolt-current-rust-canonical-2026-07-17/`.
- Revert or redesign any individual optimization that repeatedly violates its performance gate.

---

### Task 1: Establish Permanent Focused Measurements

**Files:**
- Modify: `benches/prolly_bench.rs`
- Create: `performance-results/canonical-streaming-hot-path-optimization-2026-07-17/baseline.md`

**Interfaces:**
- Produces: `PROLLY_BENCH_ONLY=boundary-hot-path` release benchmark output for all built-in detectors.
- Produces: the existing `chunking-cutover` output for protected build and mutation workloads.

- [ ] **Step 1: Add a boundary-only benchmark selector**

Add a `boundary-hot-path` selector that drives 24-byte deterministic entries through `BoundaryDetector` for entry-count key hash, logical-byte Weibull, and logical-byte rolling policies. Use `black_box`, report ns/entry through the existing measurement output, and keep the workload allocation-free after detector construction.

- [ ] **Step 2: Capture five-process baselines**

Run:

```bash
for run_id in 1 2 3 4 5; do
  PROLLY_BENCH_ONLY=boundary-hot-path cargo bench --bench prolly_bench --quiet
done
for run_id in 1 2 3 4 5; do
  PROLLY_BENCH_ONLY=chunking-cutover PROLLY_BENCH_SCALE=100000 \
    PROLLY_BENCH_ITERATIONS=100 cargo bench --bench prolly_bench --quiet
done
```

Record the raw output and environment in `baseline.md` before production edits.

- [ ] **Step 3: Verify benchmark compilation**

Run:

```bash
cargo check --bench prolly_bench
```

Expected: exit 0 with no warnings.

- [ ] **Step 4: Commit the measurement seam**

```bash
git add benches/prolly_bench.rs performance-results/canonical-streaming-hot-path-optimization-2026-07-17/baseline.md
git commit -m "bench: capture canonical streaming hot-path baseline"
```

### Task 2: Optimize Rolling Hash State

**Files:**
- Modify: `src/prolly/boundary.rs`
- Test: `src/prolly/boundary.rs`

**Interfaces:**
- Produces: private `ByteHashCache` with `fn get(&mut self, byte: u8) -> u64`.
- Produces: private `RollingWindow` with `fn push(&mut self, byte: u8) -> Option<u8>` and `fn clear(&mut self)`.
- Preserves: `BoundaryDetector::observe` and `BoundaryDetector::reset` signatures.

- [ ] **Step 1: Add differential regression coverage**

Add tests that compare cached byte hashes with the existing reference `byte_hash` for all 256 byte values under seeds `0`, `1`, and `u64::MAX`; compare ring-window outgoing bytes with a `VecDeque` reference through multiple wraps; and compare complete rolling boundary sequences across reset and deterministic randomized streams.

- [ ] **Step 2: Implement lazy byte-hash memoization**

Store `[u64; 256]` plus a 256-bit initialized bitmap only for rolling policies. On a cache miss, compute the existing `byte_hash(seed, byte)` exactly once; hits return the stored value. Keep `byte_hash` as the reference primitive.

- [ ] **Step 3: Implement the circular window**

Allocate `Vec<u8>` with the configured window length once. Track `len` and `next`; return the replaced byte only after the window is full. Reset indices without reallocating or zeroing unused capacity.

- [ ] **Step 4: Route rolling updates through both structures**

Update incoming and outgoing terms in `roll_byte` with `ByteHashCache::get`. Preserve the current rotation and XOR equation exactly.

- [ ] **Step 5: Verify correctness and performance**

Run:

```bash
cargo test --all-features prolly::boundary
cargo test --test chunking_policies
cargo test --test canonical_roots
for run_id in 1 2 3 4 5; do
  PROLLY_BENCH_ONLY=boundary-hot-path cargo bench --bench prolly_bench --quiet
done
```

Required: identical golden vectors and roots; rolling median improves by at least 40%; entry-count and Weibull medians do not regress by more than 2%.

- [ ] **Step 6: Commit**

```bash
git add src/prolly/boundary.rs
git commit -m "perf: cache canonical rolling hash state"
```

### Task 3: Reuse Cascade Scratch

**Files:**
- Modify: `src/prolly/builder/streaming.rs`
- Test: `src/prolly/builder/streaming.rs`

**Interfaces:**
- Adds: private `cascade_scratch: Vec<(u8, NodeSummary)>` on `HierarchicalEmitter`.
- Preserves: emitted-node order and every public builder interface.

- [ ] **Step 1: Add scratch reuse coverage**

Add a test-only capacity/pointer observation method. Drive enough leaf entries to cascade through at least two parent levels, then assert subsequent cascades reuse the warmed scratch allocation and produce the same root as `BatchBuilder`.

- [ ] **Step 2: Reuse the hierarchy-owned stack**

Replace `let mut queue = vec![...]` with `std::mem::take(&mut self.cascade_scratch)`. Clear and seed the local vector, execute propagation inside a closure returning `Result`, then clear and restore the vector before returning that result. This restores scratch on both success and error while preserving LIFO ordering.

- [ ] **Step 3: Verify protected build and append workloads**

Run:

```bash
cargo test --all-features prolly::builder
cargo test --test canonical_roots
for run_id in 1 2 3 4 5; do
  PROLLY_BENCH_ONLY=chunking-cutover PROLLY_BENCH_SCALE=100000 \
    PROLLY_BENCH_ITERATIONS=100 cargo bench --bench prolly_bench --quiet
done
```

Required: identical roots/tree bytes; sorted, unsorted, and append medians regress no more than 2%, p95 no more than 3%.

- [ ] **Step 4: Commit**

```bash
git add src/prolly/builder/streaming.rs
git commit -m "perf: reuse canonical cascade scratch"
```

### Task 4: Remove Sub-Threshold Point-Update Adapter Work

**Files:**
- Modify: `src/prolly/batch.rs`
- Modify: `src/prolly/write.rs`
- Test: `src/prolly/batch.rs`
- Test: `tests/write_stats.rs`

**Interfaces:**
- Produces: `pub(crate) fn should_try_batched_value_updates<S: Store>(prolly: &Prolly<S>, tree: &Tree, mutation_count: usize) -> bool`.
- Preserves: the existing 256-mutation threshold and `KeyStableBatchAttempt` behavior.

- [ ] **Step 1: Add route-classification coverage**

Test that 1 and 255 eligible updates return false, 256 eligible updates on a batch-read-preferring store and nonempty tree return true, and empty trees or stores without preferred batch reads return false.

- [ ] **Step 2: Centralize eligibility**

Move the existing threshold/store/root checks into `should_try_batched_value_updates` and call it from `try_apply_batched_value_updates`.

- [ ] **Step 3: Classify before adapter allocation**

In `try_direct_value_updates`, call the eligibility function before converting normalized `(Vec<u8>, Option<Vec<u8>>)` entries into `Mutation::Upsert`. Sub-threshold updates proceed directly to `rewrite_value_update_subtree`; eligible large batches retain the current adapter and fallback semantics.

- [ ] **Step 4: Measure before considering further point edits**

Run the focused five-process cutover benchmark. Do not alter insert/delete logic unless counters or profiles identify a separate redundant allocation and the same correctness proof can be retained.

- [ ] **Step 5: Verify point correctness and gates**

Run:

```bash
cargo test --test write_stats
cargo test --test canonical_roots
cargo test --test canonical_range_delete
for run_id in 1 2 3 4 5; do
  PROLLY_BENCH_ONLY=chunking-cutover PROLLY_BENCH_SCALE=100000 \
    PROLLY_BENCH_ITERATIONS=100 cargo bench --bench prolly_bench --quiet
done
```

Required: middle update/insert/delete each neutral or faster within the 2% median envelope; append and build gates remain satisfied.

- [ ] **Step 6: Commit**

```bash
git add src/prolly/batch.rs src/prolly/write.rs tests/write_stats.rs
git commit -m "perf: bypass small-update batch adapters"
```

### Task 5: Full Verification and Results

**Files:**
- Create: `performance-results/canonical-streaming-hot-path-optimization-2026-07-17/after.md`
- Create: `performance-results/canonical-streaming-hot-path-optimization-2026-07-17/report.md`

**Interfaces:**
- Produces: raw after samples and an honest before/after decision report.

- [ ] **Step 1: Run full static and correctness gates**

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --all-targets
cargo test --doc --all-features
python3 scripts/binding_api_inventory.py check
```

- [ ] **Step 2: Repeat focused and one-million-record performance gates**

Run five independent focused processes and at least three validated
`SCALE_RECORDS=1000000 cargo bench --bench scale_workloads` processes. Record
nodes/bytes read and written, shape, serialized bytes, and peak RSS.

- [ ] **Step 3: Publish the result without averaging away negatives**

Document every protected workload independently. If any repeated gate fails,
remove the responsible optimization and rerun the complete gate.

- [ ] **Step 4: Commit results**

```bash
git add performance-results/canonical-streaming-hot-path-optimization-2026-07-17
git commit -m "docs: publish canonical hot-path optimization results"
```
