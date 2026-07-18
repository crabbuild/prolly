# Canonical Parallel Mutation Executor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `parallel_batch` into a configurable, deterministic many-core writer while preserving byte-identical canonical roots and the existing low-latency sequential fast paths.

**Architecture:** `ParallelConfig` produces one internal `ExecutionPolicy` used by the canonical writer, batched route planner, key-stable leaf executor, and structural mutation-island executor. Parallel stages operate on indexed work in bounded waves, gather results in key order, and never decide chunk boundaries; dense structural work is rejected before replay and any failed proof immediately invokes the existing canonical resynchronizing fallback.

**Tech Stack:** Rust 2021, Rayon indexed parallel iterators, existing `Store`, `BoundaryDetector`, `LevelEmitter`, `BatchWriteCollector`, canonical-root tests, and the repository’s release benchmark harness.

**Implementation outcome:** Delivered through Tasks 1-7 and measured on a 12-core Apple M2 Max. Retained commands, raw results, regression gates, and limitations are in [`performance-results/canonical-parallel-executor-2026-07-17`](../../../performance-results/canonical-parallel-executor-2026-07-17/README.md). Automatic value-only execution improved 24.5% at median and 24.4% at p95 versus true width one; protected old/new medians and p95 did not regress. Dense structural workloads report width one and zero tasks.

## Global Constraints

- Correctness is the release gate; performance changes may not select a different chunking algorithm.
- Worker count and completion order must not change serialized node bytes, CIDs, reachable node sets, or final roots.
- Use the shared Rayon pool; never construct a thread pool per mutation call.
- Bound structural-island waves to `4 * effective_width`; use exactly the configured partitions for explicitly bounded leaf work and indexed Rayon splitting at full shared-pool width.
- `ParallelConfig::max_threads`, `parallelism_threshold`, and `sequential()` must have observable effects.
- Remove inert batch-writer tuning choices instead of retaining compatibility branches.
- Preserve the current canonical sequential path for small, append-fast-path, point-mutation, and non-independent workloads.
- Do not use TDD, per the user’s explicit instruction; add and run correctness tests immediately after each implementation slice.
- Make no migration or backwards-compatibility accommodation; the repository is alpha-stage.
- Do not claim a performance improvement without repeated release-mode measurements on the same host.

---

### Task 1: Canonical Execution Policy

**Files:**
- Modify: `src/prolly/parallel.rs`
- Test: `src/prolly/parallel.rs`

**Interfaces:**
- Consumes: public `ParallelConfig { max_threads, parallelism_threshold }`.
- Produces: `ExecutionPolicy::from_config`, `ExecutionPolicy::automatic`, `ExecutionPolicy::sequential`, `ExecutionPolicy::enabled`, `ExecutionPolicy::width`, `ExecutionPolicy::read_width`, `ExecutionPolicy::wave_size`, `ExecutionPolicy::limit_to`, `ExecutionPolicy::ranges`, and the active-write concurrency guard.

- [ ] **Step 1: Implement the internal policy and indexed range partitioning**

Add the following scheduling-only type below `ParallelConfig`:

```rust
use std::ops::Range;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ExecutionPolicy {
    width: usize,
    wave_size: usize,
    enabled: bool,
}

impl ExecutionPolicy {
    pub(crate) fn from_config(
        config: &ParallelConfig,
        effective_mutations: usize,
        independent_work: usize,
    ) -> Self {
        let pool_width = rayon::current_num_threads().max(1);
        let configured = if config.max_threads == 0 {
            pool_width
        } else {
            config.max_threads.min(pool_width).max(1)
        };
        let width = configured.min(independent_work.max(1));
        let enabled = width > 1
            && independent_work > 1
            && effective_mutations >= config.parallelism_threshold;
        let width = if enabled { width } else { 1 };
        Self {
            width,
            wave_size: width.saturating_mul(4).max(1),
            enabled,
        }
    }

    pub(crate) fn automatic(effective_mutations: usize, independent_work: usize) -> Self {
        Self::from_config(
            &ParallelConfig::default(),
            effective_mutations,
            independent_work,
        )
    }

    pub(crate) fn sequential() -> Self {
        Self {
            width: 1,
            wave_size: 1,
            enabled: false,
        }
    }

    pub(crate) fn enabled(self) -> bool {
        self.enabled
    }

    pub(crate) fn width(self) -> usize {
        self.width
    }

    pub(crate) fn wave_size(self) -> usize {
        self.wave_size
    }

    pub(crate) fn limit_to(self, independent_work: usize) -> Self {
        let width = self.width.min(independent_work.max(1));
        let enabled = self.enabled && width > 1 && independent_work > 1;
        let width = if enabled { width } else { 1 };
        Self {
            width,
            wave_size: width.saturating_mul(4).max(1),
            enabled,
        }
    }

    pub(crate) fn ranges(self, len: usize) -> Vec<Range<usize>> {
        if len == 0 {
            return Vec::new();
        }
        let partitions = self.width.min(len).max(1);
        let chunk = len.div_ceil(partitions);
        (0..len)
            .step_by(chunk)
            .map(|start| start..(start + chunk).min(len))
            .collect()
    }
}
```

Measured hardening extends this skeleton with a separate ordered-read width and an RAII active-write counter. Explicit `max_threads` caps both widths; automatic scheduling retains up to 16 ordered-read partitions, and inner execution drops to width one when active callers would leave fewer than three shared-pool threads per write.

- [ ] **Step 2: Add policy tests after the implementation**

Add tests that assert threshold fallback, sequential behavior, width capping, and exact non-overlapping range coverage:

```rust
#[test]
fn execution_policy_honors_threshold_and_width() {
    let sequential = ExecutionPolicy::from_config(&ParallelConfig::new(8, 100), 99, 64);
    assert!(!sequential.enabled());
    assert_eq!(sequential.width(), 1);

    let parallel = ExecutionPolicy::from_config(&ParallelConfig::new(2, 1), 100, 8);
    assert!(parallel.enabled() || rayon::current_num_threads() == 1);
    assert!(parallel.width() <= 2);
}

#[test]
fn execution_policy_ranges_cover_input_once_in_order() {
    let policy = ExecutionPolicy::from_config(&ParallelConfig::new(4, 1), 17, 17);
    let ranges = policy.ranges(17);
    let covered = ranges.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(covered, (0..17).collect::<Vec<_>>());
}
```

- [ ] **Step 3: Verify and commit the policy slice**

Run:

```bash
cargo test --lib execution_policy
cargo fmt --check
```

Expected: all matching tests pass and formatting reports no diff.

Commit:

```bash
git add src/prolly/parallel.rs
git commit -m "feat: add canonical write execution policy"
```

### Task 2: Thread Configuration Through Every Canonical Batch Entry Point

**Files:**
- Modify: `src/prolly/write.rs`
- Modify: `src/prolly/batch.rs`
- Modify: `src/prolly/parallel.rs`
- Modify: `src/prolly/mod.rs`
- Test: `src/prolly/mod.rs`
- Test: `tests/canonical_roots.rs`

**Interfaces:**
- Consumes: `ParallelConfig` and `ExecutionPolicy` from Task 1.
- Produces: `write::apply_configured`, `write::apply_tree_configured`, and `batch::apply_with_stats_configured`; existing unconfigured entry points delegate with automatic policy.

- [ ] **Step 1: Add configured canonical-writer entry points**

Refactor `write::apply_impl` to receive `Option<&ParallelConfig>` without changing mutation semantics:

```rust
pub(crate) fn apply_configured<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<Mutation>,
    config: &super::parallel::ParallelConfig,
) -> Result<(Tree, WriteStats), Error> {
    apply_impl(manager, tree, mutations, true, Some(config))
}

pub(crate) fn apply_tree_configured<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<Mutation>,
    config: &super::parallel::ParallelConfig,
) -> Result<Tree, Error> {
    Ok(apply_impl(manager, tree, mutations, false, Some(config))?.0)
}
```

Make existing `apply` and `apply_tree` call the same function with `None`. After normalization, calculate the policy once and pass it to every eligible fast path:

```rust
let policy = parallel_config.map_or_else(
    || ExecutionPolicy::automatic(mutations.len(), mutations.len()),
    |config| ExecutionPolicy::from_config(config, mutations.len(), mutations.len()),
);
```

- [ ] **Step 2: Add configured stats routing**

Add:

```rust
pub(crate) fn apply_with_stats_configured<S: Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<Mutation>,
    config: &ParallelConfig,
) -> Result<BatchApplyResult, Error> {
    let input_sorted = mutations.windows(2).all(|pair| pair[0].key() <= pair[1].key());
    let (tree, write_stats) = super::write::apply_configured(prolly, tree, mutations, config)?;
    Ok(BatchApplyResult::from_write_stats(tree, write_stats, input_sorted))
}
```

Extract the duplicated `BatchApplyResult` conversion into `from_write_stats` so configured and automatic paths report identical counters.

- [ ] **Step 3: Route public parallel APIs through the configured writer**

Replace both ignored `_config` parameters:

```rust
pub fn parallel_batch(
    &self,
    tree: &Tree,
    mutations: Vec<Mutation>,
    config: &parallel::ParallelConfig,
) -> Result<Tree, Error> {
    write::apply_tree_configured(self, tree, mutations, config)
}
```

and:

```rust
pub(crate) fn parallel_batch_with_stats<S: Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<Mutation>,
    config: &ParallelConfig,
) -> Result<BatchApplyResult, Error> {
    batch::apply_with_stats_configured(prolly, tree, mutations, config)
}
```

- [ ] **Step 4: Extend write telemetry with execution-policy evidence**

Add `u64` counters to `WriteStats`:

```rust
pub parallel_width: u64,
pub parallel_tasks: u64,
pub structural_islands: u64,
pub coalesced_islands: u64,
```

Add the corresponding `usize` counters to `BatchApplyStats` and cast them in `BatchApplyResult::from_write_stats`:

```rust
pub parallel_width: usize,
pub parallel_tasks: usize,
pub structural_islands: usize,
pub coalesced_islands: usize,
```

Initialize `parallel_width` to one and raise it only when an admitted executor actually launches independent work. Leave task/island counts zero until later tasks. Tests must be able to distinguish `sequential()` from an enabled configuration even when roots match, and a rejected route must not claim an unused width.

- [ ] **Step 5: Add routing and root-equivalence tests**

Update the current delegation test to assert:

```rust
assert_eq!(sequential.stats.parallel_width, 1);
assert!(parallel.stats.parallel_width >= 1);
assert_eq!(sequential.tree.root, parallel.tree.root);
```

Extend `parallel_batch_matches_the_canonical_batch_root` to run widths `[1, 2, 4, 8, 0]` and compare every result with `batch` and fresh bulk build.

- [ ] **Step 6: Verify and commit configured routing**

Run:

```bash
cargo test --lib parallel_batch_with_stats
cargo test --test canonical_roots parallel_batch_matches_the_canonical_batch_root
cargo fmt --check
```

Expected: all targeted tests pass, and each configured route produces the canonical root.

Commit:

```bash
git add src/prolly/write.rs src/prolly/batch.rs src/prolly/parallel.rs src/prolly/mod.rs tests/canonical_roots.rs
git commit -m "fix: honor canonical parallel batch configuration"
```

### Task 3: Configured Key-Stable Routing and Leaf Execution

**Files:**
- Modify: `src/prolly/batch.rs`
- Modify: `src/prolly/write.rs`
- Modify: `src/prolly/mod.rs`
- Test: `src/prolly/batch.rs`
- Test: `src/prolly/mod.rs`

**Interfaces:**
- Consumes: `ExecutionPolicy` calculated in `write::apply_impl`.
- Produces: configured `try_apply_batched_value_updates`, bounded ordered routing, and bounded indexed leaf preparation.

- [ ] **Step 1: Remove the fixed routing width**

Delete `BATCHED_VALUE_UPDATE_PARALLELISM`. Change the helper signature to:

```rust
pub(crate) fn try_apply_batched_value_updates<S: Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<Mutation>,
    policy: ExecutionPolicy,
) -> Result<KeyStableBatchAttempt, Error>
```

Pass `policy.read_width()` into `group_mutations_by_leaf_with_paths_batched`. Explicit `max_threads` caps CPU and ordered-read width; automatic scheduling retains up to 16 ordered-read partitions on smaller pools. The eligibility predicate must require effective width four or greater, `policy.enabled()`, a store that prefers batch reads, and the existing safety floor of 256 mutations. `parallelism_threshold` may raise that floor through the already-calculated policy, but it does not force undersized batches into a route whose overhead is known to dominate.

Before the full route, hydrate one representative mutation through the same owned-node cache. A missing or growing value rejects pure insert/growth batches cheaply. The full route still validates every mutation. Admit direct leaf replacement only for key-only entry-count hashing; route-path ancestry must classify rightmost leaves correctly, and rightmost status must never bypass the value-stability requirement for key+value, byte-measured, rolling, or Weibull policies.

- [ ] **Step 2: Process independent leaf groups with bounded partitions**

Change `prepare_leaf_groups_for_coalesced_rebuild` to accept `ExecutionPolicy`. Preserve the sequential loop when disabled. At full shared-pool width, use one indexed Rayon iterator so work stealing balances uneven leaves without allocating hundreds of waves. For an explicit width below the pool, split the ordered groups into exactly `policy.width()` partitions and process each partition sequentially inside one Rayon task.

Rayon indexed collection preserves group and partition order. Do not invoke a nested parallel iterator while a partition is executing.

- [ ] **Step 3: Record actual task counts**

Return the number of scheduled partitions from the batched update result and add it to `WriteStats.parallel_tasks`. Set `parallel_width` to one when fewer than two affected leaves are found even if the requested configuration was wider.

- [ ] **Step 4: Add bounded-read and deterministic-leaf tests**

Use the existing counting store to execute the same large value-update batch with widths one, two, and four. Assert:

```rust
assert_eq!(sequential.tree.root, width_two.tree.root);
assert_eq!(sequential.tree.root, width_four.tree.root);
assert_eq!(sequential.stats.parallel_width, 1);
assert!(width_two.stats.parallel_width <= 2);
assert!(width_four.stats.parallel_width <= 4);
assert!(store.max_batch_get_ordered_len.load(Ordering::Relaxed) <= expected_bound);
```

Also compare reachable serialized node bytes for width one and the widest available configuration.

- [ ] **Step 5: Verify and commit key-stable parallel execution**

Run:

```bash
cargo test --lib batched_value
cargo test --lib parallel_batch_with_stats
cargo test --test canonical_roots
cargo fmt --check
```

Expected: all tests pass with identical roots and reachable bytes across widths.

Commit:

```bash
git add src/prolly/batch.rs src/prolly/write.rs src/prolly/mod.rs
git commit -m "perf: configure parallel key-stable batch execution"
```

### Task 4: Canonical Structural Mutation-Island Planner

**Files:**
- Modify: `src/prolly/write.rs`
- Test: `src/prolly/write.rs`

**Interfaces:**
- Consumes: normalized mutations and ordered old `NodeSummary` leaves.
- Produces: `MutationIsland`, `IslandReplay`, `plan_mutation_islands`, and `replay_mutation_island` with no store writes.

- [ ] **Step 1: Add explicit island data structures**

Add private types:

```rust
#[derive(Clone, Debug)]
struct MutationIsland {
    leaf_range: Range<usize>,
    mutation_range: Range<usize>,
    protected_end: usize,
}

struct IslandReplay {
    island: MutationIsland,
    summaries: Vec<NodeSummary>,
    emitted: Vec<EmittedLeaf>,
    resynced_at: Option<usize>,
    entries_streamed: u64,
    nodes_read: u64,
    bytes_read: u64,
}

impl IslandReplay {
    fn proved_independent(&self) -> bool {
        self.resynced_at
            .map(|index| index < self.island.protected_end)
            .unwrap_or(false)
    }
}
```

- [ ] **Step 2: Implement deterministic island planning**

`plan_mutation_islands` maps each mutation to its predecessor leaf with `partition_point`, groups mutations whose target leaves are adjacent, gives each group two predecessor leaves for exact-cap context, and requires at least one untouched leaf between candidates. Candidates without an untouched separator are coalesced before execution.

Use this signature:

```rust
fn plan_mutation_islands(
    old_leaves: &[NodeSummary],
    mutations: &[(Vec<u8>, Option<Vec<u8>>)],
) -> Vec<MutationIsland>
```

The returned ranges must be ordered, non-empty, mutation-complete, and non-overlapping at their protected boundaries.

- [ ] **Step 3: Extract side-effect-free island replay from the canonical loop**

Move the existing leaf replay/emitter logic into:

```rust
fn replay_mutation_island<S: Store>(
    manager: &Prolly<S>,
    old_leaves: &[NodeSummary],
    mutations: &[(Vec<u8>, Option<Vec<u8>>)],
    island: MutationIsland,
    config: &Config,
    measure_read_bytes: bool,
) -> Result<IslandReplay, Error>
```

It must clone mutation key/value data into its local emitter, stop only after matching an old leaf CID or exhausting its guard, and perform no `batch_put`, cache insertion, or root publication.

- [ ] **Step 4: Add planner and replay tests after extraction**

Cover:

- distant clusters produce two islands;
- adjacent clusters coalesce;
- every normalized mutation appears in exactly one mutation range;
- an unchanged anchor produces `proved_independent() == true`;
- a cascading boundary change reaches the guard and returns false;
- rolling and Weibull policies either prove CID resynchronization or fall back without publishing partial nodes.

- [ ] **Step 5: Verify and commit the island planner**

Run:

```bash
cargo test --lib mutation_island
cargo test --test canonical_roots
cargo fmt --check
```

Expected: planner/replay tests and the full canonical-root integration test pass.

Commit:

```bash
git add src/prolly/write.rs
git commit -m "refactor: extract canonical mutation island replay"
```

### Task 5: Parallel Structural-Island Execution and Deterministic Coalescing

**Files:**
- Modify: `src/prolly/write.rs`
- Modify: `src/prolly/parallel.rs`
- Test: `src/prolly/write.rs`
- Test: `tests/canonical_roots.rs`

**Interfaces:**
- Consumes: `ExecutionPolicy`, `MutationIsland`, and `IslandReplay`.
- Produces: `execute_mutation_islands`, bounded admission, and immediate canonical fallback before the existing frontier assembly.

- [ ] **Step 1: Add ordered bounded execution helper**

Implement:

```rust
fn execute_mutation_islands<S: Store>(
    manager: &Prolly<S>,
    old_leaves: &[NodeSummary],
    mutations: &[(Vec<u8>, Option<Vec<u8>>)],
    islands: Vec<MutationIsland>,
    policy: ExecutionPolicy,
    measure_read_bytes: bool,
) -> Result<Vec<IslandReplay>, Error>
```

When disabled or only one island exists, execute in order. Otherwise, process `policy.wave_size()` islands per wave with indexed partitions, collecting ordered `Result<IslandReplay, Error>` values.

- [ ] **Step 2: Bound admission and fall back immediately on a failed proof**

Before replay, reject dense spans, plans whose guarded leaf coverage exceeds 25%, and plans whose candidates collapse by more than 4:1. After the single bounded wave, scan ordered `Result<IslandReplay, Error>` values left to right and return the first real error. If any replay cannot prove independence, discard every speculative result and execute the existing sequential canonical loop immediately. Do not retry progressively larger regions; empirical testing showed O(n log n) work and pathological tail latency.

Never add emitted bytes from an unproved replay to the publication collector.

- [ ] **Step 3: Merge successful island output in canonical order**

Splice successful `summaries` into the unchanged old-leaf sequence by ordered `leaf_range`. Deduplicate changed leaves by CID, accumulate stats in island order, then invoke the existing fixed-separator or canonical `BatchBuilder::build_from_chunks_serial_deferred` frontier path.

Record:

```rust
stats.parallel_tasks += executed_island_count as u64;
stats.structural_islands += initial_island_count as u64;
stats.coalesced_islands += coalesced_count as u64;
```

- [ ] **Step 4: Add worker-count and fallback matrices**

For widths `[1, 2, 4, 8, 0]`, compare batch, parallel batch, and fresh-build roots for distant clusters, adjacent clusters, inserts, deletes, and mixed mutations under every built-in chunking policy. Assert at least one distant-cluster fixture uses more than one island on a machine with more than one Rayon worker. Assert the adjacent fixture is rejected by admission and remains canonical.

- [ ] **Step 5: Verify and commit structural parallelism**

Run:

```bash
cargo test --lib mutation_island
cargo test --test canonical_roots
cargo test --test store_conformance
cargo fmt --check
```

Expected: all tests pass with identical roots for every worker width and policy.

Commit:

```bash
git add src/prolly/write.rs src/prolly/parallel.rs tests/canonical_roots.rs
git commit -m "perf: execute canonical mutation islands in parallel"
```

### Task 6: Remove Inert Batch Controls and Correct Public Documentation

**Files:**
- Modify: `src/prolly/batch.rs`
- Modify: `src/prolly/parallel.rs`
- Modify: `src/prolly/mod.rs`
- Modify: `src/lib.rs`
- Modify: `src/prolly/README.md`
- Modify: Rust tests and doctests that import `BatchWriterConfig`

**Interfaces:**
- Consumes: the configured canonical writer completed in Tasks 1–5.
- Produces: one truthful execution configuration and no public algorithm switch that silently does nothing.

- [ ] **Step 1: Replace `BatchWriterConfig` with `ParallelConfig`**

Delete `BatchWriterConfig` and its setters. Change `BatchWriter` to:

```rust
pub struct BatchWriter {
    config: ParallelConfig,
}

impl BatchWriter {
    pub fn new() -> Self {
        Self {
            config: ParallelConfig::default(),
        }
    }

    pub fn with_config(config: ParallelConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &ParallelConfig {
        &self.config
    }

    pub fn apply_batch<S: Store>(
        &self,
        prolly: &Prolly<S>,
        tree: &Tree,
        mutations: Vec<Mutation>,
    ) -> Result<Tree, Error> {
        super::write::apply_tree_configured(prolly, tree, mutations, &self.config)
    }

    pub fn apply_batch_with_stats<S: Store>(
        &self,
        prolly: &Prolly<S>,
        tree: &Tree,
        mutations: Vec<Mutation>,
    ) -> Result<BatchApplyResult, Error> {
        apply_with_stats_configured(prolly, tree, mutations, &self.config)
    }
}
```

- [ ] **Step 2: Remove stale exports and algorithm claims**

Remove `BatchWriterConfig` from `src/lib.rs`. Rewrite docs to state that `ParallelConfig` affects scheduling only, the canonical boundary engine is invariant, and width one is the deterministic sequential baseline. Remove claims about selectable optimized merge, bottom-up rebuild, deferred rebalancing, or cache warming.

- [ ] **Step 3: Add a public-surface regression test**

Add a compile/runtime test that constructs `BatchWriter::with_config(ParallelConfig::new(2, 1))`, applies a large batch, and compares its root and stats with `Prolly::parallel_batch_with_stats` using the same config.

- [ ] **Step 4: Verify and commit the hard API cutover**

Run:

```bash
cargo test --all-features
cargo test --doc
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: all tests and doctests pass; Clippy emits no warnings.

Commit:

```bash
git add src/prolly/batch.rs src/prolly/parallel.rs src/prolly/mod.rs src/lib.rs src/prolly/README.md tests
git commit -m "refactor: remove inert batch execution controls"
```

### Task 7: Concurrency and Scaling Benchmark Harness

**Files:**
- Modify: `benches/prolly_bench.rs`
- Create: `performance-results/canonical-parallel-executor-2026-07-17/README.md`
- Create after measurement: `performance-results/canonical-parallel-executor-2026-07-17/results.csv`
- Create after measurement: `performance-results/canonical-parallel-executor-2026-07-17/report.md`

**Interfaces:**
- Consumes: public `ParallelConfig`, `parallel_batch_with_stats`, and canonical root validation.
- Produces: repeatable worker-scaling and concurrent-caller benchmark modes plus raw evidence.

- [ ] **Step 1: Add explicit scaling modes to the existing benchmark**

Add `parallel-scaling` and `parallel-callers` benchmark modes. Parse `PROLLY_WORKERS` as a comma-separated list and default to `1,2,4,8,12,16,0`. For each width, report CSV fields:

```text
workload,base_entries,mutations,workers,effective_workers,callers,run,elapsed_ns,ops_per_sec,p50_ns,p95_ns,p99_ns,peak_rss_bytes,nodes_read,nodes_written,bytes_read,bytes_written,batch_get_calls,batch_put_calls,parallel_tasks,structural_islands,coalesced_islands,root
```

Before timing, build each fixture once. Outside the timed interval, compare the produced root with width one and a fresh canonical build.

- [ ] **Step 2: Add workload generators**

Generate deterministic append, random, clustered, value-only, insert-only, delete-only, and 60/20/20 mixed batches for base sizes 100 thousand, 1 million, and 10 million and mutation sizes 1 thousand, 10 thousand, 100 thousand, and 1 million. Seed every random generator with a constant recorded in output.

- [ ] **Step 3: Add concurrent-caller saturation**

Use `std::thread::scope` and a barrier to launch 2, 4, and 8 callers against the same immutable base and shared store. Each caller uses disjoint deterministic mutation data and validates its root independently. Report total throughput and per-call latency samples; do not add caller thread count to Rayon worker count when labeling results.

- [ ] **Step 4: Capture baseline and candidate measurements**

Run at least five samples per cell in release mode. Round samples up to a complete rotation of configured widths so every width occupies every measurement position equally. Use at least 20 samples for p95/p99 decisions. Record exact commands and host metadata in `README.md`. Minimum commands:

```bash
cargo bench --bench prolly_bench -- parallel-scaling 1000000
cargo bench --bench prolly_bench -- parallel-callers 1000000
```

- [ ] **Step 5: Enforce the regression gates and write the report**

Compare with width one and the pre-change commit. Report medians and p95/p99. Reject or disable an automatic parallel route if protected small-workload median regresses beyond 2% or p95 beyond 5%. Report inconclusive cells as noise. Include CPU count, worker-width saturation, RSS, store-call counts, and island fallback rate.

- [ ] **Step 6: Verify and commit the benchmark evidence**

Run:

```bash
cargo test --all-features
cargo fmt --check
git diff --check
```

Expected: all tests pass and performance artifacts contain raw results plus an honest report.

Commit:

```bash
git add benches/prolly_bench.rs performance-results/canonical-parallel-executor-2026-07-17
git commit -m "bench: validate canonical parallel executor scaling"
```

### Task 8: Final Verification and Review

**Files:**
- Inspect: all files changed since the design commit
- Update if necessary: `performance-results/canonical-parallel-executor-2026-07-17/report.md`

**Interfaces:**
- Consumes: every preceding implementation slice and its commits.
- Produces: verified merge candidate with documented correctness and performance evidence.

- [ ] **Step 1: Run the complete verification suite from a clean status**

Run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo test --doc
git diff --check
```

Expected: every command exits zero.

- [ ] **Step 2: Audit requirements against evidence**

Confirm from tests and raw benchmark data:

- roots and reachable bytes match across widths;
- width one performs no parallel work;
- observed width never exceeds `max_threads`;
- concurrent callers finish without deadlock;
- small-workload regression gates hold;
- large independent workloads scale enough to justify automatic scheduling;
- all remaining public execution knobs affect real behavior.

- [ ] **Step 3: Request focused code review**

Provide the reviewer with the design, this plan, base SHA, head SHA, correctness commands, benchmark report, and explicit questions about canonical island independence, deterministic error selection, oversubscription, and hidden sequential bottlenecks.

- [ ] **Step 4: Resolve every critical or important review finding**

Apply fixes, rerun the complete verification suite, and append any performance-impacting reruns to the report rather than overwriting raw evidence.

- [ ] **Step 5: Commit final review fixes**

If review required changes:

```bash
git add src/prolly/parallel.rs src/prolly/write.rs src/prolly/batch.rs src/prolly/mod.rs src/lib.rs src/prolly/README.md tests/canonical_roots.rs benches/prolly_bench.rs performance-results/canonical-parallel-executor-2026-07-17
git commit -m "fix: address canonical parallel executor review"
```

Do not create an empty commit when no changes are required.
