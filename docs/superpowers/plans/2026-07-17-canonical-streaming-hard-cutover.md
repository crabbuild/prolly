# Canonical Streaming Hard Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace every legacy prolly-tree boundary path with one deterministic policy-aware streaming engine, correct rolling and Weibull chunk distributions, enforce exact node byte caps, and make sorted construction hierarchy-streaming without regressing critical latency.

**Architecture:** `BoundaryDetector` owns all canonical probability and hash decisions. A new exact `EncodedNodeSizer` and reusable `LevelEmitter` assemble bounded nodes for bulk, sorted, mutation, and async paths. Public stats and parallel APIs become views over canonical execution rather than alternate writers, and the legacy rebalancer/boundary APIs are deleted.

**Tech Stack:** Rust 2021, xxHash64, Rayon, existing `Store`/`AsyncStore` abstractions, custom harness-free Cargo benchmarks, deterministic integer fixed-point arithmetic.

## Global Constraints

- Correctness is non-negotiable: every public write surface must match a fresh canonical bulk root.
- Performance is the ultimate goal after correctness; release-mode baseline and after measurements are mandatory.
- This is an alpha hard cutover. Do not add migration or backwards-compatibility branches.
- Keep `entry_count_key_hash()` as `Config::default()`.
- Preserve all four user-selectable built-in chunking mechanisms.
- Remove, rather than deprecate, legacy helpers that substitute entry-count key/value hashing.
- Do not stage, overwrite, or reformat unrelated existing worktree changes.
- Use test-first red-green-refactor for each behavior change.
- Do not change node CID hashing, storage backends, or blob offloading.

---

## File Structure

- Modify `src/prolly/boundary.rs`: sole hash/probability boundary engine and deterministic threshold arithmetic.
- Modify `src/prolly/format.rs`: hard-cutover policy validation and canonical format semantics.
- Create `src/prolly/builder/size.rs`: exact incremental serialized-node sizing.
- Create `src/prolly/builder/streaming.rs`: reusable active-node and hierarchical level emitters.
- Modify `src/prolly/builder.rs`: integrate exact sizing, hierarchical sorted construction, and shared serial/parallel assembly.
- Modify `src/prolly/write.rs`: consume shared emitters for canonical mutation/append/resynchronization.
- Modify `src/prolly/batch.rs`: make stats APIs adapt canonical write stats and delete alternate mutation/rebalance execution.
- Modify `src/prolly/parallel.rs`: retain `ParallelConfig` routing but remove direct node-rebalance APIs.
- Modify `src/prolly/mod.rs`: migrate sync and async production paths to canonical primitives and remove embedded legacy split helpers.
- Delete `src/prolly/rebalance.rs`: remove the incompatible rebalancer after all production callers are cut over.
- Modify `src/prolly/traits.rs` and `src/lib.rs`: remove obsolete rebalancer and boundary exports.
- Modify binding facade/generated files under `bindings/`: remove the deleted boundary probe from every language surface.
- Modify `src/bin/prolly-conformance.rs`, `tests/conformance_fixtures.rs`, and `conformance/prolly-fixtures.v1.json`: replace the stateless legacy probe with streamed canonical vectors.
- Modify `tests/canonical_roots.rs`, `tests/chunking_policies.rs`, `tests/builder_policy_equivalence.rs`, `tests/async_store.rs`, `tests/invariants.rs`, and `tests/write_stats.rs`: regression and equivalence coverage.
- Modify `benches/prolly_bench.rs` and `benches/scale_workloads.rs`: reproducible before/after latency, throughput, chunk distribution, and memory evidence.
- Create `performance-results/canonical-streaming-hard-cutover-2026-07-17/report.md`: measured conclusions and acceptance-gate table.

---

### Task 1: Capture Baseline and Reproduce the Correctness Failures

**Files:**
- Modify: `tests/canonical_roots.rs`
- Modify: `tests/chunking_policies.rs`
- Modify: `benches/prolly_bench.rs`
- Modify: `benches/scale_workloads.rs`
- Create: `performance-results/canonical-streaming-hard-cutover-2026-07-17/baseline.md`

**Interfaces:**
- Consumes: current `BatchWriter::apply_batch_with_stats`, `BoundaryDetector`, and existing benchmark helpers.
- Produces: failing canonicality/distribution tests and reproducible baseline commands used unchanged in Task 11.

- [ ] **Step 1: Add a direct stats-writer canonicality regression test**

Add a helper that builds 5,000 records, inserts `key-002500a` through direct `BatchWriter::apply_batch_with_stats`, rebuilds the final set with `BatchBuilder`, and compares roots for both the default and rolling policies:

```rust
#[test]
fn direct_batch_writer_stats_matches_canonical_root_for_every_policy() {
    for policy in [
        chunking::entry_count_key_hash(),
        chunking::entry_count_key_value_hash(),
        chunking::logical_bytes_key_weibull(),
        chunking::logical_bytes_rolling_hash(),
    ] {
        let config = Config::builder().chunking(policy).build();
        let store = Arc::new(MemStore::new());
        let base = build_numbered_tree(store.clone(), config.clone(), 5_000);
        let manager = Prolly::new(store, config.clone());
        let actual = BatchWriter::new()
            .apply_batch_with_stats(
                &manager,
                &base,
                vec![Mutation::Upsert {
                    key: b"key-002500a".to_vec(),
                    val: vec![b'y'; 32],
                }],
            )
            .unwrap()
            .tree;
        let expected = rebuild_with_middle_insert(config, 5_000);
        assert_eq!(actual.root, expected.root, "policy diverged");
    }
}
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test --test canonical_roots direct_batch_writer_stats_matches_canonical_root_for_every_policy -- --exact
```

Expected: FAIL because at least the default policy produces a different root.

- [ ] **Step 3: Add a rolling distribution regression test**

Drive 250,000 deterministic 44-byte logical records through `logical_bytes_rolling_hash()`. Record each completed chunk's logical bytes and assert:

```rust
assert!(mean.abs_diff(spec.target) <= spec.target / 10, "mean={mean}");
assert!(forced_max_chunks * 100 < chunk_sizes.len(), "forced={forced_max_chunks}");
```

- [ ] **Step 4: Run the rolling test and verify RED**

Run:

```bash
cargo test --test chunking_policies rolling_logical_bytes_tracks_target_distribution -- --exact
```

Expected: FAIL with a mean near 63 KiB instead of 16 KiB.

- [ ] **Step 5: Add stable benchmark selectors without changing measured code**

Add `PROLLY_BENCH_ONLY=chunking-cutover` to emit separate CSV rows for sorted build, unsorted build, append-1, append-64, append-4096, middle value update, middle insert, and middle delete. Extend `scale_workloads` with `SCALE_POLICY` and ensure `SortedBatchBuilder::new` remains outside the timed loop setup only when the measured workload requires it.

- [ ] **Step 6: Capture release-mode baseline**

Run at least 20 samples per latency workload and five samples for 1-million-record scale/RSS:

```bash
PROLLY_BENCH_ONLY=chunking-cutover PROLLY_BENCH_SCALE=100000 PROLLY_BENCH_ITERATIONS=20 cargo bench --bench prolly_bench
SCALE_VERSION=before SCALE_RECORDS=1000000 cargo bench --bench scale_workloads
/usr/bin/time -l env SCALE_VERSION=before SCALE_RECORDS=1000000 cargo bench --bench scale_workloads
```

Record command, commit, CPU, Rust version, raw rows, and peak RSS in `baseline.md`.

- [ ] **Step 7: Commit only tests, benchmark harness, and baseline evidence**

```bash
git add tests/canonical_roots.rs tests/chunking_policies.rs benches/prolly_bench.rs benches/scale_workloads.rs performance-results/canonical-streaming-hard-cutover-2026-07-17/baseline.md
git commit -m "test: capture canonical chunking cutover baseline"
```

---

### Task 2: Cut Stats and Parallel APIs Over to the Canonical Writer

**Files:**
- Modify: `src/prolly/batch.rs`
- Modify: `src/prolly/parallel.rs`
- Modify: `tests/canonical_roots.rs`
- Modify: `tests/write_stats.rs`

**Interfaces:**
- Consumes: `write::apply(prolly, tree, mutations) -> Result<(Tree, WriteStats), Error>`.
- Produces: `BatchApplyStats::from_write_stats` and root-identical stats-bearing APIs.

- [ ] **Step 1: Add a stats parity test**

For the same base/mutations, assert `BatchWriter::apply_batch`, `BatchWriter::apply_batch_with_stats().tree`, `Prolly::batch`, `Prolly::batch_with_stats().tree`, and `Prolly::parallel_batch` all return one root for every built-in policy.

- [ ] **Step 2: Run the parity test and verify RED**

```bash
cargo test --test canonical_roots every_stats_and_parallel_writer_returns_the_canonical_root -- --exact
```

Expected: FAIL on direct `BatchWriter::apply_batch_with_stats`.

- [ ] **Step 3: Reuse one canonical stats adapter**

Keep `batch::apply_with_stats` as the only store-neutral adapter. It computes
`input_sorted`, invokes `write::apply` exactly once, and maps every
`BatchApplyStats` field explicitly:

```rust
BatchApplyStats {
    input_mutations: write_stats.input_mutations as usize,
    effective_mutations: write_stats.effective_mutations as usize,
    preprocess_input_sorted: input_sorted,
    affected_leaves: write_stats.resync_distance_nodes as usize,
    changed_leaves: write_stats.nodes_written as usize,
    sparse_leaf_applies: 0,
    written_nodes: write_stats.nodes_written as usize,
    written_bytes: write_stats.bytes_written as usize,
    used_append_fast_path: false,
    used_batched_route: write_stats.used_batched_value_update_path,
    used_coalesced_rebuild: true,
    used_deferred_rebalancing: false,
    used_bottom_up_rebuild: false,
    cache_written_nodes: false,
}
```

Do not add a second adapter or a public `Default`-based partial conversion.

- [ ] **Step 4: Replace the direct stats writer body**

`BatchWriter::apply_batch_with_stats` must delegate to `batch::apply_with_stats`. It must not branch into append, grouped rebalance, deferred, or bottom-up legacy writers.

- [ ] **Step 5: Keep parallel configuration limited to safe runtime tuning**

`parallel_batch_with_stats` may pass bounded read parallelism into a canonical execution context, but it must not select a different tree algorithm. Until that context exists, delegate directly to canonical stats execution and report the config as advisory.

- [ ] **Step 6: Run focused and full batch tests**

```bash
cargo test --test canonical_roots --test write_stats --test batch_stats --test batch_behavior
cargo test --lib prolly::batch
```

Expected: all pass, including the RED tests from Task 1.

- [ ] **Step 7: Commit**

```bash
git add src/prolly/batch.rs src/prolly/parallel.rs tests/canonical_roots.rs tests/write_stats.rs
git commit -m "fix: route stats writers through canonical mutations"
```

---

### Task 3: Make Rolling and Weibull Thresholds Deterministic and Measure-Aware

**Files:**
- Modify: `src/prolly/boundary.rs`
- Modify: `src/prolly/format.rs`
- Modify: `tests/chunking_policies.rs`
- Modify: `tests/tree_format.rs`

**Interfaces:**
- Consumes: `ChunkingSpec`, previous/current chunk measure, and a deterministic hash sample.
- Produces: `deterministic_exponential_threshold(delta: u128, scale: u128) -> u64` and integer-only Weibull/rolling decisions.

- [ ] **Step 1: Add threshold golden tests before implementation**

Add monotonicity, boundary, and golden-vector tests:

```rust
assert_eq!(deterministic_exponential_threshold(0, 12_288), 0);
assert!(deterministic_exponential_threshold(44, 12_288)
    < deterministic_exponential_threshold(4_096, 12_288));
assert!(deterministic_exponential_threshold(12_288, 12_288)
    > u64::MAX / 2);
assert_eq!(deterministic_exponential_threshold(u128::MAX, 1), u64::MAX);
```

Store exact expected threshold constants after independently calculating them in the test fixture; do not derive expected values with the implementation helper.

- [ ] **Step 2: Run and verify RED**

```bash
cargo test --test chunking_policies deterministic_threshold_golden_vectors -- --exact
```

Expected: compilation failure because the helper does not exist.

- [ ] **Step 3: Implement fixed-point exponential threshold arithmetic**

Use Q62 so `1.0` and intermediate products fit `u128`. Range-reduce until `x <= 1/16`, evaluate the alternating series through term 8, then square back:

```rust
const Q62: u128 = 1u128 << 62;

fn deterministic_exponential_threshold(delta: u128, scale: u128) -> u64 {
    if delta == 0 { return 0; }
    if scale == 0 || delta >= scale.saturating_mul(64) { return u64::MAX; }
    let mut x = scaled_ratio_q62(delta, scale);
    let mut squarings = 0;
    while x > Q62 / 16 {
        x = (x + 1) / 2;
        squarings += 1;
    }
    let mut survival = exp_neg_series_q62(x);
    for _ in 0..squarings {
        survival = survival.saturating_mul(survival) / Q62;
    }
    (((Q62 - survival) * u128::from(u64::MAX)) / Q62) as u64
}
```

`scaled_ratio_q62` must normalize `u128` numerator/denominator before multiplication, and tests must cover normalization.

- [ ] **Step 4: Apply the threshold to rolling measure deltas**

Compute eligible delta after `min` and use `scale = target.saturating_sub(min).max(1)`. Compare `rolling_hash <= threshold`. Preserve forced `max` and hard-cap decisions before probabilistic logic.

- [ ] **Step 5: Replace floating Weibull math**

Support deterministic integer shapes 1 and 2 in the hard-cutover format:

```rust
let hazard_delta = match shape {
    1 => u128::from(current - previous),
    2 => u128::from(current).pow(2) - u128::from(previous).pow(2),
    _ => return Err(Error::InvalidFormat("Weibull shape must be 1 or 2".into())),
};
let hazard_scale = u128::from(target).pow(shape);
sample <= deterministic_exponential_threshold(hazard_delta, hazard_scale)
```

Update `ChunkingSpec::validate` to accept only shapes 1 and 2. Remove all `f64`, `powf`, and platform `exp` calls from canonical boundary code.

- [ ] **Step 6: Run distribution and policy tests**

```bash
cargo test --test chunking_policies --test tree_format
```

Expected: rolling mean within 10% of target, forced maximum below 1%, deterministic vectors pass.

- [ ] **Step 7: Commit**

```bash
git add src/prolly/boundary.rs src/prolly/format.rs tests/chunking_policies.rs tests/tree_format.rs
git commit -m "fix: make canonical chunk probabilities deterministic"
```

---

### Task 4: Introduce Exact Incremental Node Sizing

**Files:**
- Create: `src/prolly/builder/size.rs`
- Modify: `src/prolly/builder.rs`
- Modify: `src/prolly/node.rs`
- Modify: `tests/node_layouts.rs`
- Modify: `tests/write_stats.rs`

**Interfaces:**
- Consumes: `TreeFormat`, leaf/internal flag, level, ordered key/value, optional child count.
- Produces: `EncodedNodeSizer::{new, size_after, push, reset}` with byte-for-byte equality to `Node::encoded_len()`.

- [ ] **Step 1: Add exact-prefix sizing tests**

For `PrefixCompressed`, `Plain`, and `OffsetTable`, append at least 300 leaf entries and 300 internal entries. After every append assert:

```rust
assert_eq!(sizer.size(), node.encoded_len() as u64, "layout={layout:?} index={index}");
```

Include count transitions 127→128 and 16,383→16,384, prefix changes, value-length varint changes, offset changes, and child-count varint changes.

- [ ] **Step 2: Run and verify RED**

```bash
cargo test --test node_layouts exact_incremental_sizer_matches_serialized_nodes -- --exact
```

Expected: compilation failure because `EncodedNodeSizer` does not exist.

- [ ] **Step 3: Implement `EncodedNodeSizer`**

The type must maintain header bytes, count-varint width, entry bytes, offset payload bytes, trailing payload-length varint, previous key, and current total:

```rust
pub(crate) struct EncodedNodeSizer {
    format: TreeFormat,
    leaf: bool,
    level: u8,
    count: u64,
    size: u64,
    payload_len: u64,
    previous_key: Vec<u8>,
}

pub(crate) struct EncodedEntry<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub child_count: Option<u64>,
}
```

`size_after` must be non-mutating. `push` applies exactly the same delta. `reset` returns to the exact empty-node encoded length.

- [ ] **Step 4: Replace estimated hard-cap accounting in builders and emitters**

Remove `node_encoding_overhead` and the hard-cap use of `entry_encoded_len`. Estimates may remain only for `Vec::reserve`. Before append, call `size_after`; on overflow flush and retry; on empty overflow return `EntryTooLarge`.

- [ ] **Step 5: Add release-mode hard-cap postcondition tests**

Exercise all layouts at caps around header, varint, prefix, and internal child-count transitions. Traverse every stored node and assert `bytes.len() <= hard_max_node_bytes`.

- [ ] **Step 6: Run focused tests**

```bash
cargo test --test node_layouts --test write_stats --test chunking_policies
```

- [ ] **Step 7: Commit**

```bash
git add src/prolly/builder/size.rs src/prolly/builder.rs src/prolly/node.rs tests/node_layouts.rs tests/write_stats.rs
git commit -m "fix: enforce exact serialized node byte caps"
```

---

### Task 5: Build a Reusable Canonical Level Emitter

**Files:**
- Create: `src/prolly/builder/streaming.rs`
- Modify: `src/prolly/builder.rs`
- Modify: `src/prolly/write.rs`
- Modify: `tests/builder_policy_equivalence.rs`
- Modify: `tests/invariants.rs`

**Interfaces:**
- Consumes: `Config`, level, `EncodedEntry`, and exact size state.
- Produces: `LevelEmitter::push`, `LevelEmitter::finish`, `EmittedNode`, and canonical `NodeSummary` values shared by builders and writers.

- [ ] **Step 1: Add emitter equivalence tests**

For each policy/layout, feed the same ordered leaf entries to the existing serial chunk-range builder and the wished-for `LevelEmitter`. Assert identical node bytes, CIDs, first keys, counts, and boundary indexes.

- [ ] **Step 2: Run and verify RED**

```bash
cargo test --test builder_policy_equivalence level_emitter_matches_serial_chunk_ranges -- --exact
```

Expected: compilation failure because `LevelEmitter` does not exist.

- [ ] **Step 3: Implement focused emitter types**

```rust
pub(crate) struct LevelEmitter {
    config: Config,
    level: u8,
    node: Node,
    detector: BoundaryDetector,
    sizer: EncodedNodeSizer,
}

pub(crate) struct EmittedNode {
    pub summary: NodeSummary,
    pub node: Node,
    pub bytes: Vec<u8>,
}

impl LevelEmitter {
    pub(crate) fn push_leaf(&mut self, key: Vec<u8>, value: Vec<u8>)
        -> Result<Option<EmittedNode>, Error>;
    pub(crate) fn push_child(&mut self, child: NodeSummary)
        -> Result<Option<EmittedNode>, Error>;
    pub(crate) fn finish(&mut self) -> Result<Option<EmittedNode>, Error>;
}
```

The emitter owns all reset and hard-cap behavior. Callers may not invoke `BoundaryDetector::reset` independently.

- [ ] **Step 4: Replace `LeafEmitter` internals in `write.rs`**

Keep the mutation-facing `LeafEmitter` facade and delegate every append/flush decision to `LevelEmitter`. Preserve emitted-node caching and `is_aligned_with` CID logic.

- [ ] **Step 5: Add degenerate and exact-boundary tests**

Cover empty, singleton, boundary on final item, hard-cap pre-split, two-child internal minimum, and a pattern that fires on every item. Assert no one-child non-root internal nodes.

- [ ] **Step 6: Run tests**

```bash
cargo test --test builder_policy_equivalence --test invariants --test canonical_roots --test write_stats
```

- [ ] **Step 7: Commit**

```bash
git add src/prolly/builder/streaming.rs src/prolly/builder.rs src/prolly/write.rs tests/builder_policy_equivalence.rs tests/invariants.rs
git commit -m "refactor: centralize canonical level emission"
```

---

### Task 6: Make Sorted Construction Hierarchy-Streaming and Fallible

**Files:**
- Modify: `src/prolly/builder/streaming.rs`
- Modify: `src/prolly/builder.rs`
- Modify: all `SortedBatchBuilder::new` call sites reported by `rg`
- Modify: `tests/builder_policy_equivalence.rs`
- Modify: `benches/scale_workloads.rs`

**Interfaces:**
- Consumes: `LevelEmitter` and bounded persistence batches.
- Produces: fallible `SortedBatchBuilder::new(store, config) -> Result<Self, Error>` and `HierarchicalEmitter` with height-bounded active metadata.

- [ ] **Step 1: Add final-root and bounded-state tests**

Expose only under `cfg(test)`:

```rust
pub(crate) fn active_level_count(&self) -> usize;
pub(crate) fn retained_summary_count(&self) -> usize;
```

Build 200,000 sorted entries and assert retained summaries are bounded by a constant multiple of active levels and persistence batch size, not total leaves.

- [ ] **Step 2: Run and verify RED**

```bash
cargo test --test builder_policy_equivalence sorted_builder_retains_height_bounded_state -- --exact
```

Expected: FAIL because the current builder retains all leaf summaries.

- [ ] **Step 3: Implement `HierarchicalEmitter`**

```rust
pub(crate) struct HierarchicalEmitter {
    config: Config,
    levels: Vec<LevelEmitter>,
    pending_nodes: Vec<BuiltNode>,
}

impl HierarchicalEmitter {
    pub(crate) fn push_leaf(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), Error>;
    fn propagate(&mut self, emitted: EmittedNode) -> Result<(), Error>;
    pub(crate) fn finish(self) -> Result<(Option<Cid>, Vec<BuiltNode>), Error>;
}
```

`propagate` iteratively pushes summaries upward. `finish` flushes trailing nodes bottom-up while collapsing a single remaining node directly to the root. It must not manufacture a one-child parent.

- [ ] **Step 4: Change `SortedBatchBuilder::new` to return `Result`**

Validate `config.format` and construct level zero without `expect`. Update every Rust call site to use `?`, `.unwrap()`, or `.expect(...)` according to its existing error contract.

- [ ] **Step 5: Remove `leaf_nodes: Vec<NodeSummary>`**

Persist sealed node bytes in batches of 256 while immediately propagating summaries. `build()` finalizes the hierarchy, flushes the final batch, and returns the root.

- [ ] **Step 6: Run all builder consumers**

```bash
cargo test --test builder_policy_equivalence --test secondary_index --test proximity_composite --test proximity_hnsw --test versioned_map
cargo check --all-targets --all-features
```

- [ ] **Step 7: Commit**

Stage `src/prolly/builder*`, all updated call sites, tests, and benches explicitly, then:

```bash
git commit -m "perf: stream sorted builds through active levels"
```

---

### Task 7: Reuse Canonical Emitters in Batch and Append Assembly

**Files:**
- Modify: `src/prolly/builder.rs`
- Modify: `src/prolly/write.rs`
- Modify: `tests/builder_policy_equivalence.rs`
- Modify: `tests/canonical_roots.rs`
- Modify: `tests/write_stats.rs`

**Interfaces:**
- Consumes: `LevelEmitter` and `HierarchicalEmitter`.
- Produces: one serial assembly implementation plus optional parallel independent-hash precomputation.

- [ ] **Step 1: Add serial/parallel/root matrix tests**

Generate deterministic variable-size records and compare `BatchBuilder`, `SortedBatchBuilder`, sequential puts, append batches, and parallel batches across all policies/layouts.

- [ ] **Step 2: Run the new matrix before refactoring**

Expected: PASS for ordinary public paths. This is a characterization test; temporarily inject one altered boundary candidate in a local test-only hook and verify the matrix fails, then remove the hook before implementation.

- [ ] **Step 3: Refactor serial upper-level assembly**

Replace `build_level_serial` boundary loops with `LevelEmitter::push_child`. Preserve deferred node collection so mutation publication remains one `batch_put`.

- [ ] **Step 4: Preserve safe parallelism only for independent hashing**

Keep Rayon precomputation when `BoundaryDetector::supports_independent_hashing()` is true. Feed precomputed candidates through the same min/max/exact-byte state machine; do not maintain a second range algorithm.

- [ ] **Step 5: Re-run focused performance smoke tests**

```bash
PROLLY_BENCH_ONLY=batch-builder PROLLY_BENCH_SCALE=100000 PROLLY_BENCH_ITERATIONS=5 cargo bench --bench prolly_bench
PROLLY_BENCH_ONLY=append-chain PROLLY_BENCH_SCALE=100000 PROLLY_BENCH_ITERATIONS=5 cargo bench --bench prolly_bench
```

Investigate any regression above 5% before continuing; do not defer obvious allocation or hashing regressions.

- [ ] **Step 6: Run correctness tests**

```bash
cargo test --test builder_policy_equivalence --test canonical_roots --test write_stats
```

- [ ] **Step 7: Commit**

```bash
git add src/prolly/builder.rs src/prolly/write.rs tests/builder_policy_equivalence.rs tests/canonical_roots.rs tests/write_stats.rs
git commit -m "refactor: share canonical emitters across write paths"
```

---

### Task 8: Cut Async Writes Over to Canonical Chunk Assembly

**Files:**
- Modify: `src/prolly/mod.rs`
- Modify: `tests/async_store.rs`
- Modify: `src/prolly/builder/streaming.rs`

**Interfaces:**
- Consumes: pure `LevelEmitter` node assembly and async collector persistence.
- Produces: async bulk, append, batch, and range-delete roots identical to sync roots for all policies/layouts.

- [ ] **Step 1: Add async policy/API root matrix tests and a source guard**

For each policy/layout, build the same base and apply append, middle insert, value update, delete, mixed batch, and range delete through sync and async managers. Assert roots and ordered entries match.

Also add a precise source guard that scans `src/prolly/mod.rs` and rejects calls
matching `boundary::is_boundary(` or bare `is_boundary(` inside async write
assembly. This guard deliberately fails on the current async append/split
helpers even if a small behavioral corpus happens to produce equal roots.

- [ ] **Step 2: Run and verify RED on the legacy async source guard**

```bash
cargo test --features async-store --test async_store async_every_policy_and_layout_matches_sync_canonical_roots -- --exact
```

Expected: FAIL because the current async append/split helpers call the legacy
stateless boundary function. The behavioral matrix must cross at least three
leaf boundaries and one internal boundary for every policy, but is allowed to
pass before the refactor as characterization coverage.

- [ ] **Step 3: Replace async `split_node_chunks` and parent boundary loops**

Use `LevelEmitter` to assemble leaf/internal nodes synchronously in memory, then add emitted bytes to `AsyncWriteCollector`. Remove calls to `boundary::is_boundary` from async append and split helpers.

- [ ] **Step 4: Preserve async I/O behavior**

Keep ordered batch reads, cached rightmost-path reuse, and one `AsyncStore::batch_put` publication. Pure assembly must not introduce point writes or blocking store calls.

- [ ] **Step 5: Run async behavioral and metric tests**

```bash
cargo test --features async-store --test async_store
cargo test --all-features --test canonical_roots --test builder_policy_equivalence
```

- [ ] **Step 6: Commit**

```bash
git add src/prolly/mod.rs src/prolly/builder/streaming.rs tests/async_store.rs
git commit -m "fix: use canonical chunk assembly for async writes"
```

---

### Task 9: Remove Legacy Boundary and Rebalance Escape Hatches

**Files:**
- Modify: `src/prolly/boundary.rs`
- Delete: `src/prolly/rebalance.rs`
- Modify: `src/prolly/batch.rs`
- Modify: `src/prolly/parallel.rs`
- Modify: `src/prolly/traits.rs`
- Modify: `src/prolly/mod.rs`
- Modify: `src/lib.rs`
- Modify: `src/bin/prolly-conformance.rs`
- Modify: `tests/conformance_fixtures.rs`
- Modify: `conformance/prolly-fixtures.v1.json`
- Modify: handwritten and generated binding files containing `is_boundary_config`

**Interfaces:**
- Consumes: canonical writers and detector now used by every production path.
- Produces: no compiled or exported legacy boundary/rebalance implementation.

- [ ] **Step 1: Add a source-surface guard test**

Extend the API inventory/check tooling or add a repository test that rejects these symbols in Rust production and binding source:

```text
is_boundary
is_boundary_config
is_hash_boundary_config
ParallelRebalancer
DefaultParallelRebalancer
rebalance::rebalance
```

Allow ordinary English uses of “boundary” and “rebalance”; match identifiers precisely.

- [ ] **Step 2: Run and verify RED**

Run the inventory test and confirm it lists the existing Rust and binding exports.

- [ ] **Step 3: Remove legacy functions and exports**

Delete the raw key/value boundary implementation and its unit tests. Remove public exports from `src/lib.rs`. Remove `pub mod rebalance`, the direct node rebalancer trait/types, and documentation examples that advertise them. Keep `ParallelConfig` and `Prolly::parallel_batch*` because they route canonical execution.

- [ ] **Step 4: Delete the unused legacy mutation closure**

After `BatchWriter::apply_batch_with_stats` is canonical, use compiler errors and `rg` to delete private grouped/deferred/bottom-up rebalance code that has no remaining canonical consumer. Preserve `preprocess_mutations`, stats DTOs, batched route hydration used by `write`, and public configuration fields until their removal is separately proven source-dead.

- [ ] **Step 5: Replace conformance fixtures with streamed vectors**

Represent each chunking fixture as a policy, level, ordered list of entries, encoded entry sizes, and expected cut indexes. Drive a real `BoundaryDetector`; do not emulate count by calling a stateless probe.

- [ ] **Step 6: Remove binding APIs and regenerate checked-in bindings**

Remove handwritten UniFFI, WASM, napi, and Python wrapper functions. Follow each `PROVENANCE.md` and `bindings/VERIFICATION.md` command to regenerate Python, Kotlin, Ruby, and Swift sources. Update Node declarations and binding API inventory JSON using the repository inventory generator. Do not hand-edit generated files when a provenance command exists.

- [ ] **Step 7: Prove no legacy identifiers remain**

```bash
rg -n 'is_boundary_config|is_hash_boundary_config|\bis_boundary\(|ParallelRebalancer|DefaultParallelRebalancer|rebalance::rebalance' src tests bindings benches examples README.md
```

Expected: no production/API matches; only historical design documents may remain.

- [ ] **Step 8: Compile the complete surface**

```bash
cargo check --workspace --all-targets --all-features
cargo test --test conformance_fixtures
```

- [ ] **Step 9: Commit**

Stage the explicit removal/regeneration set and commit:

```bash
git commit -m "refactor!: remove legacy prolly rebalancing paths"
```

---

### Task 10: Exhaustive Correctness and Failure Validation

**Files:**
- Modify: `tests/canonical_roots.rs`
- Modify: `tests/builder_policy_equivalence.rs`
- Modify: `tests/invariants.rs`
- Modify: `tests/store_conformance.rs`
- Modify: `tests/chunking_policies.rs`

**Interfaces:**
- Consumes: completed hard-cutover implementation.
- Produces: deterministic randomized root matrix and failure-publication proof.

- [ ] **Step 1: Add deterministic randomized histories**

For seeds 0 through 31, generate variable-size sorted records plus inserts,
deletes, shrinking/growing updates, duplicate mutations, and append suffixes.
Compare this explicit matrix:

- sync construction: `BatchBuilder`, `SortedBatchBuilder`, and sequential `put`;
- sync mutation: `Prolly::batch`, `batch_with_stats`, `batch_with_write_stats`,
  `parallel_batch`, `parallel_batch_with_stats`, direct
  `BatchWriter::apply_batch`, direct `BatchWriter::apply_batch_with_stats`, and
  right-edge append;
- async mutation under `async-store`: `put`, `delete`, `batch`, and
  `delete_range`;
- merge only for conflict-free disjoint histories, compared with direct final
  construction;
- delete/reinsert recovery, compared with a fresh bulk root at every recovery
  point.

- [ ] **Step 2: Add failing-store publication tests**

Create a store that fails each `batch_put` ordinal in turn. Assert every failed build/mutation returns `Error::Store` and never returns a root. Then run without failure and traverse every reachable CID.

- [ ] **Step 3: Add structural invariant traversal**

For every resulting tree assert sorted unique keys, equal key/value lengths, exact format, exact hard cap, correct child counts, level monotonicity, no one-child non-root internals, and root count equal to range count.

- [ ] **Step 4: Run focused matrices**

```bash
cargo test --test canonical_roots --test builder_policy_equivalence --test invariants --test store_conformance --test chunking_policies
cargo test --features async-store --test async_store
```

- [ ] **Step 5: Run the complete workspace suite**

```bash
cargo test --workspace --all-features
cargo test --workspace --all-targets --all-features --no-run
```

Expected: zero failures and no new warnings in touched modules.

- [ ] **Step 6: Commit**

```bash
git add tests/canonical_roots.rs tests/builder_policy_equivalence.rs tests/invariants.rs tests/store_conformance.rs tests/chunking_policies.rs tests/async_store.rs
git commit -m "test: enforce canonical roots across every writer"
```

---

### Task 11: Measure, Profile, and Optimize the Cutover

**Files:**
- Modify: implementation files only when a measured regression has an identified cause and a failing performance/invariant test.
- Create: `performance-results/canonical-streaming-hard-cutover-2026-07-17/after.md`
- Create: `performance-results/canonical-streaming-hard-cutover-2026-07-17/report.md`

**Interfaces:**
- Consumes: identical Task 1 benchmark commands and baseline environment.
- Produces: median/p95/p99, throughput, RSS, I/O amplification, distribution comparison, and gate decision.

- [ ] **Step 1: Repeat the baseline commands unchanged**

Run the same scale, iteration count, policy corpus, machine, build profile, and environment. Store raw rows rather than only summaries.

- [ ] **Step 2: Calculate acceptance metrics**

Report before/after percentage for:

```text
sorted build throughput and peak RSS
unsorted parallel build throughput
append-1/64/4096 median, p95, p99
middle update/insert/delete median, p95, p99
entries and nodes read/written
store get/batch-get/put/batch-put calls
rolling and Weibull chunk distributions
```

- [ ] **Step 3: Gate on correctness before optimization**

If any canonical or invariant test fails, stop performance work and fix correctness through a new RED test. Do not benchmark known-invalid code.

- [ ] **Step 4: Profile regressions above the spec thresholds**

Use release symbols and the platform profiler. Check repeated hashing, `Vec` growth, key cloning, exact-sizer recomputation, detector resets, small persistence batches, and unnecessary level creation. Make only evidence-backed changes.

- [ ] **Step 5: Re-run correctness after every optimization**

At minimum:

```bash
cargo test --test canonical_roots --test builder_policy_equivalence --test invariants --test chunking_policies --test write_stats
```

- [ ] **Step 6: Publish the report**

`report.md` must include machine/commit provenance, raw artifact links, every acceptance gate as PASS/FAIL, any neutral or negative result, and an honest answer to whether latency improved. Do not average away tail regressions.

- [ ] **Step 7: Commit measured optimization and report artifacts**

Stage only intentional source and compact report/CSV artifacts; do not commit profiler dumps or build products.

```bash
git commit -m "perf: validate canonical streaming hard cutover"
```

---

### Task 12: Final Documentation and Verification

**Files:**
- Modify: `README.md`
- Modify: relevant files under `docs/`
- Modify: `CHANGELOG.md` if present
- Modify: binding documentation affected by API removal

**Interfaces:**
- Consumes: final APIs and measured results.
- Produces: accurate chunking selection guidance and hard-cutover release notes.

- [ ] **Step 1: Document policy selection**

Explain default key-only entry-count stability, key/value sensitivity, logical-byte Weibull behavior, corrected rolling behavior, hard byte caps, and when to choose each policy.

- [ ] **Step 2: Document breaking removals**

State that stateless boundary probes and direct node rebalancers were removed because they could not represent persisted policy semantics. Point users to `BoundaryDetector` for streamed inspection and `Prolly::*batch*` for writes.

- [ ] **Step 3: Verify documentation examples**

```bash
cargo test --doc
cargo test --workspace --all-features
cargo check --workspace --all-targets --all-features
git diff --check
```

- [ ] **Step 4: Inspect the final diff and worktree scope**

Confirm no unrelated pre-existing modifications are staged or included. Confirm deleted generated APIs are consistently absent across supported bindings.

- [ ] **Step 5: Commit documentation**

```bash
git add README.md docs CHANGELOG.md bindings/*/README.md
git commit -m "docs: describe canonical chunking policy cutover"
```

- [ ] **Step 6: Request final code review**

Use `superpowers:requesting-code-review`, address findings through `superpowers:receiving-code-review`, and rerun the full verification gate before declaring completion.

---

## Plan Self-Review Checklist

- Every approved design requirement maps to at least one task.
- Correctness failures are captured before production edits.
- Baseline is captured before behavior changes and repeated unchanged afterward.
- Sync, async, stats, parallel, merge/range publication, bindings, and conformance surfaces are covered.
- Exact sizing and deterministic probability arithmetic are independently tested.
- Legacy code is removed only after replacement paths pass root-equivalence tests.
- Performance claims require release-mode median and tail measurements.
- No task authorizes migration or backwards-compatibility work.
