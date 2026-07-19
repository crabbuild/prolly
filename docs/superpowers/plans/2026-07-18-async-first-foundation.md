# Async-First Prolly Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish one always-available async storage foundation, one centrally validated node-loading boundary, and a canonical `ProllyEngine<S: AsyncStore>` that both native async and ready-only synchronous facades can use without changing persisted node bytes or tree CIDs.

**Architecture:** `ProllyEngine` owns store I/O, validation, cache admission, and operation-local execution context. `AsyncProlly` calls it directly. `Prolly` wraps a `SyncStoreAsAsync` engine and uses a sealed ready-only executor that rejects `Pending`; it never creates a runtime or blocks a worker thread. This phase migrates point and ordered batch reads before mutation algorithms move in later phases.

**Tech Stack:** Rust 2021, stable async functions in traits, `futures-util`, existing `Store`, `AsyncStore`, `Node`, `ReadNode`, and `TreeFormat` types.

## Global Constraints

- Correctness is non-negotiable. Validate requested CID, serialized structure, and expected tree format before any cache admission.
- Preserve canonical node bytes, CID hashing, tree roots, named-root manifests, and externally stored data.
- Source compatibility is not required. Delete obsolete execution paths instead of maintaining duplicate algorithms.
- Performance follows correctness: keep zero-copy shared reads, ordered bounded batch reads, and operation-local metrics.
- A synchronous store adapted by `SyncStoreAsAsync` must return `Poll::Ready` on every poll. Treat `Pending` as an internal invariant violation.
- Never start a Tokio runtime, call `block_on`, park a thread, or spawn work from the sync facade.
- Use red-green-refactor for every behavior change and commit only after focused and full test gates pass.
- Do not import the dirty checkout's untracked Turso or benchmark artifacts during this phase.

## Transition Sequence

1. This plan: always-on async store foundation, validated I/O, ready-only sync adapter, engine shell, point/batch read cutover.
2. `2026-07-18-async-first-canonical-writes.md`: async builders, canonical put/batch/delete/append, removal of async full-tree rebuilds.
3. `2026-07-18-async-first-read-services.md`: ranges, cursors, proofs, diff, merge, cancellation, and deterministic bounded concurrency.
4. `2026-07-18-async-first-domain-cutover-and-performance.md`: named roots, transactions, snapshots, versioned maps, GC/copy, blobs, content graph, indexes, local SQLite/Turso matrix, and legacy deletion.

Create each later plan with exact interfaces and tests immediately before executing that phase. A phase may begin only after the previous phase's full gate passes.

## File Structure

- Modify `Cargo.toml`: make the async foundation unconditional and retain `async-store` as a temporary no-op feature during this phase.
- Modify `src/lib.rs`: expose async storage and facade types in default builds.
- Modify `src/prolly/store/mod.rs`: remove feature gates from `AsyncStore` and `SyncStoreAsAsync`.
- Create `src/prolly/engine/mod.rs`: canonical engine owner and validated point/batch load entry points.
- Create `src/prolly/engine/validation.rs`: CID, format, and structural decoding boundary.
- Create `src/prolly/engine/ready.rs`: sealed ready-only future contract and inline executor.
- Create `src/prolly/engine/execution.rs`: validated runtime limits and operation-local counters.
- Modify `src/prolly/mod.rs`: route synchronous and asynchronous point/batch read helpers through the engine.
- Modify `src/prolly/node.rs`: expose crate-private packed-node format inspection needed by validation.
- Modify `tests/async_store.rs`: malicious-store validation and engine equivalence tests.
- Create `tests/async_foundation_default.rs`: prove async APIs compile and execute with default features.
- Create `tests/ready_sync.rs`: ready-only executor and sync/async facade equivalence tests.

---

### Task 1: Make Stored Bytes Untrusted at Every Node Boundary

**Files:**
- Create: `src/prolly/engine/validation.rs`
- Modify: `src/prolly/engine/mod.rs`
- Modify: `src/prolly/node.rs`
- Modify: `src/prolly/mod.rs`
- Modify: `tests/async_store.rs`
- Modify: `tests/invariants.rs`

**Interfaces:**

```rust
pub(crate) fn validate_cid(expected: &Cid, bytes: &[u8]) -> Result<(), Error>;
pub(crate) fn decode_owned(
    expected_cid: &Cid,
    expected_format: &TreeFormat,
    bytes: &[u8],
) -> Result<Node, Error>;
pub(crate) fn decode_read(
    expected_cid: &Cid,
    expected_format: &TreeFormat,
    bytes: Arc<[u8]>,
) -> Result<ReadNode, Error>;
```

- [x] Add sync and async malicious-store tests that return valid node bytes under the wrong requested CID. Exercise owned `load_arc`, shared `load_read_arc`, and ordered batch loading; assert `Error::CidMismatch` and prove a second request reads the store again rather than hitting poisoned cache state.
- [x] Add tests that return structurally valid bytes encoded with a different `TreeFormat`; assert `Error::FormatMismatch` for owned, shared, and batch paths.
- [x] Run `cargo test --test invariants stored_node -- --nocapture` and `cargo test --features async-store --test async_store stored_node -- --nocapture`; verify RED because owned loads currently accept the wrong CID and manager-format validation is inconsistent.
- [x] Implement `validate_cid`, `decode_owned`, and `decode_read`. Map malformed packed decode to `Error::InvalidNode`, compare format after decode, and perform all checks before returning a value eligible for caching.
- [x] Replace direct `Node::from_bytes`, `Node::from_bytes_with_format`, and `read_node_from_shared` calls in manager load paths with these functions. Cover cache conversion and ordered batch reads as well as point reads.
- [x] Run the focused tests, then `cargo test --lib` and `cargo test --features async-store --lib`.
- [x] Commit with `git commit -m "fix: validate every stored prolly node"`.

---

### Task 2: Make Async Storage an Unconditional Foundation

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/prolly/store/mod.rs`
- Modify: `src/prolly/mod.rs`
- Modify: `src/lib.rs`
- Create: `tests/async_foundation_default.rs`

**Interfaces:**

```toml
[features]
default = []
async-store = []
tokio = ["dep:tokio"]

[dependencies]
futures-util = "0.3"
```

- [x] Add a default-feature integration test implementing `AsyncStore` for a minimal in-memory backend and constructing `AsyncProlly`; do not use `#[cfg(feature = "async-store")]` in the test.
- [x] Run `cargo test --no-default-features --test async_foundation_default`; verify RED because async APIs are not exported.
- [x] Remove `#[cfg(feature = "async-store")]` from `AsyncStore`, `SyncStoreAsAsync`, `AsyncProlly`, required helper imports/modules, and public exports. Keep the Cargo feature as an empty compatibility spelling only for this phase.
- [x] Ensure Tokio-specific adapters remain behind `tokio`; async core code must not depend on Tokio.
- [x] Run `cargo test --no-default-features --test async_foundation_default`, `cargo test --no-default-features --lib`, and `cargo test --all-features --lib`.
- [x] Commit with `git commit -m "refactor: make async storage foundations unconditional"`.

---

### Task 3: Add a Sealed Ready-Only Synchronous Execution Contract

**Files:**
- Create: `src/prolly/engine/ready.rs`
- Modify: `src/prolly/engine/mod.rs`
- Create: `tests/ready_sync.rs`

**Interfaces:**

```rust
pub(crate) mod sealed { pub trait ReadyFuture {} }

pub(crate) trait ReadyFuture: Future + sealed::ReadyFuture {}

pub(crate) fn run_ready<F>(future: F) -> F::Output
where
    F: ReadyFuture;
```

The store adapter supplies named future wrapper types for each operation. Their `Future::poll` calls the synchronous store once and returns `Poll::Ready`; arbitrary user futures cannot implement the sealed marker.

- [ ] Add unit tests for ready success, ready error, nested/reentrant ready execution, exactly-one store call, and a test-only marked future that returns `Pending` and must panic with `"ready-only future returned Pending"`.
- [ ] Add a compile-fail doctest showing that an arbitrary pending future cannot be passed to `run_ready`.
- [ ] Run `cargo test --test ready_sync`; verify RED because the contract does not exist.
- [ ] Implement a no-op `RawWaker`, pin the future locally, poll exactly once, and panic on `Pending`. Do not use `futures::executor`, Tokio, Condvar, thread parking, or spinning.
- [ ] Add ready wrappers to `SyncStoreAsAsync` without changing native `AsyncStore` implementations.
- [ ] Run `cargo test --test ready_sync` and `cargo test --doc`.
- [ ] Commit with `git commit -m "feat: add ready-only sync execution contract"`.

---

### Task 4: Establish Validated Execution Limits and Operation Metrics

**Files:**
- Create: `src/prolly/engine/execution.rs`
- Modify: `src/prolly/config.rs`
- Modify: `src/prolly/error.rs`
- Create: `tests/execution_config.rs`

**Interfaces:**

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionConfig {
    pub read_parallelism: NonZeroUsize,
    pub max_in_flight_bytes: NonZeroUsize,
    pub node_cache_max_nodes: NonZeroUsize,
    pub node_cache_max_bytes: NonZeroUsize,
}

pub(crate) struct OperationContext {
    limits: ExecutionConfig,
    stats: OperationStats,
    cancelled: AtomicBool,
}
```

- [ ] Add tests rejecting zero limits, accepting documented defaults, keeping execution settings out of `TreeFormat`, and returning exact operation stats for cache hit/miss, bytes read/written, nodes decoded/written, and peak in-flight reads.
- [ ] Run `cargo test --test execution_config`; verify RED.
- [ ] Replace optional/unbounded runtime cache defaults with finite validated defaults. Keep persisted format and execution configuration separate in constructors and manifests.
- [ ] Implement saturating counters scoped to one public operation. Existing cumulative observability may aggregate completed operation deltas but must never derive a result by subtracting two global snapshots.
- [ ] Run focused tests plus `cargo test --lib`.
- [ ] Commit with `git commit -m "feat: add bounded async execution context"`.

---

### Task 5: Introduce `ProllyEngine` and Cut Over Point/Batch Reads

**Files:**
- Modify: `src/prolly/engine/mod.rs`
- Modify: `src/prolly/mod.rs`
- Modify: `src/lib.rs`
- Modify: `tests/async_store.rs`
- Modify: `tests/ready_sync.rs`

**Interfaces:**

```rust
pub(crate) struct ProllyEngine<S: AsyncStore> {
    store: S,
    execution: ExecutionConfig,
    cache: NodeCache,
    metrics: ProllyMetrics,
}

impl<S: AsyncStore> ProllyEngine<S> {
    async fn load_owned(&self, tree: &Tree, cid: &Cid) -> Result<Arc<Node>, Error>;
    async fn load_read(&self, tree: &Tree, cid: &Cid) -> Result<Arc<ReadNode>, Error>;
    async fn load_many_read_ordered(
        &self,
        tree: &Tree,
        cids: &[Cid],
        operation: &mut OperationContext,
    ) -> Result<Vec<Arc<ReadNode>>, Error>;
    async fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    async fn get_many(
        &self,
        tree: &Tree,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, Error>;
}
```

- [ ] Add facade equivalence tests over empty, single-level, and multi-level trees. Assert native async and sync-ready facades return identical values and error variants, preserve input key order and duplicates, and issue the same logical node reads after warmup normalization.
- [ ] Add a test constructing a manager with a different creation default than the input tree; reads must use `tree.format` and succeed. Add the inverse wrong-node-format test, which must fail.
- [ ] Run the focused tests; verify RED where manager config currently overrides or rejects the input tree format.
- [ ] Move cache and metrics ownership behind `ProllyEngine`. Implement only validated point and ordered batch loaders plus `get`/`get_many` in this task.
- [ ] Make `AsyncProlly<S>` a direct facade over `ProllyEngine<S>`. Make `Prolly<S>` own `ProllyEngine<SyncStoreAsAsync<S>>` and call only the sealed ready path for migrated operations.
- [ ] Delete the old facade-local point/batch loader implementations after all their callers use the engine. Do not retain fallback reads.
- [ ] Run `cargo test --test async_store --test ready_sync --test invariants`, `cargo test --no-default-features --lib`, and `cargo test --all-features --lib`.
- [ ] Commit with `git commit -m "refactor: route prolly reads through async engine"`.

---

### Task 6: Foundation Completion Gate

**Files:**
- Modify: this plan (check completed boxes)
- Create: `performance-results/async-first-foundation-2026-07-18/report.md`

- [ ] Run `cargo fmt --all -- --check` and `cargo clippy --all-targets --all-features -- -D warnings`.
- [ ] Run `cargo test --no-default-features`, `cargo test --all-features`, and all doctests.
- [ ] Run the existing Miri-compatible node/cache validation subset and record the exact command. If nightly/Miri is unavailable, record that as an environmental limitation rather than claiming the gate passed.
- [ ] Benchmark sync and async-adapted `get`/`get_many` on the same in-memory tree. Record release build, CPU, Rust version, sample count, median, p95, throughput, allocations if available, and peak RSS. This is a regression sentinel, not the final SQLite/Turso comparison.
- [ ] Record preserved root vectors before/after for every built-in `TreeFormat` and confirm byte-for-byte equality.
- [ ] Write `report.md` with commands, raw artifact paths, pass/fail gates, and any measured regression. Do not begin canonical writes while a correctness gate fails.
- [ ] Commit with `git commit -m "test: verify async-first engine foundation"`.

## Phase Exit Criteria

- Default builds expose and test `AsyncStore` and `AsyncProlly` without Tokio.
- Every engine node load validates CID, structure, and input-tree format before cache admission.
- Sync migrated APIs execute only sealed ready futures and contain no runtime or blocking executor.
- `Prolly` and `AsyncProlly` point/batch reads share one engine implementation.
- Existing canonical root fixtures remain byte-identical.
- Default-feature and all-feature test suites, formatting, and Clippy pass.
- The foundation benchmark report contains reproducible evidence and no unexplained critical regression.
