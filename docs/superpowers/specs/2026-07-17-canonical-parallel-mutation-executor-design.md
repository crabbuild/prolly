# Canonical Parallel Mutation Executor Design

## Status

Approved in principle on 2026-07-17 through the canonical-parallel executor proposal. This document fixes the implementation boundary before development. The repository is alpha-stage: migration and source compatibility are out of scope. Correctness is the release gate; sustained throughput and tail latency on many-core hardware are the performance goals.

## Objective

Make `parallel_batch` a real, configurable, many-core execution path without reintroducing an alternate chunking or rebalancing algorithm. Parallel and sequential execution must emit byte-identical nodes and the same root for the same tree, mutations, format, and chunking policy.

The executor must improve large independent workloads while preserving the current low-overhead path for small, append-heavy, or highly contended batches. Multiple callers must share the process Rayon pool instead of creating a thread pool per batch.

## Current Defects

`ParallelConfig` is currently accepted but ignored by both `parallel_batch` and `parallel_batch_with_stats`. `max_threads`, `parallelism_threshold`, and `sequential()` therefore have no effect.

`BatchWriterConfig` exposes several execution switches whose values no longer control canonical writes. Keeping inert performance controls is worse than removing them in an alpha API because callers cannot reason about scheduling or benchmark a selected policy.

The surviving parallel work is narrow and uses fixed constants. Large key-stable value updates can route with a fixed width of 16 and prepare leaf groups through Rayon, and node decoding can run in parallel. General insert/delete and mixed mutation fallback remains predominantly sequential. Existing root-equivalence tests validate output, but existing benchmarks do not establish scaling by worker count.

The removed `ParallelRebalancer` is not a suitable foundation. It ran independent legacy rebalances over caller-supplied nodes and ancestor paths, returned multiple roots, and used entry-count/stateless boundary decisions that were not canonical for every persisted chunking policy.

## Requirements

1. One boundary authority: `BoundaryDetector` and canonical level emitters remain the only components allowed to decide persisted chunk boundaries.
2. Determinism: worker count and task completion order must not change serialized node bytes, CIDs, or the final root.
3. Real configuration: `ParallelConfig::max_threads`, `parallelism_threshold`, and `sequential()` must change execution scheduling observably and testably.
4. No per-call pools: all calls use the shared Rayon pool, preventing thread creation latency and CPU oversubscription when callers execute concurrently.
5. Bounded work: parallel routing, mutation application, and encoding operate in waves bounded by worker width and a small fixed multiplier.
6. Safe fallback: if independence or resynchronization cannot be proven, execution coalesces work and runs the existing canonical path.
7. Atomic publication: no root is returned until all nodes reachable from it have been successfully persisted.
8. No inert public knobs: obsolete `BatchWriterConfig` choices are removed or replaced by the single execution configuration used by the canonical writer.
9. No compatibility branches or legacy writer selection.
10. Performance claims require release-mode worker-scaling evidence, including tail latency and memory.

## API and Configuration

`ParallelConfig` becomes the single per-call execution policy:

```rust
pub struct ParallelConfig {
    /// Maximum independent work partitions for this call. Zero uses the
    /// current Rayon pool width.
    pub max_threads: usize,

    /// Effective mutation count below which the canonical sequential path is
    /// used without parallel planning overhead.
    pub parallelism_threshold: usize,
}
```

The implementation derives an effective width:

```text
configured width = max_threads == 0 ? rayon pool width : max_threads
effective width  = min(configured width, available independent work)
```

`max_threads` limits the number of partitions created by this call. Tasks run on the shared Rayon pool. Parallel stages do not invoke nested parallel iterators, so a call cannot expand beyond its partition width.

`ParallelConfig::sequential()` selects width one and an effectively infinite threshold. Tests must demonstrate that it performs no parallel route-read waves or leaf-task execution.

Ordinary `batch` uses an internal automatic execution policy selected for the store and mutation shape. `parallel_batch` uses the supplied policy. Both call the same canonical writer; configuration changes scheduling only.

The obsolete `BatchWriterConfig` algorithm switches are removed. If `BatchWriter` remains as a convenience object, it stores a `ParallelConfig` and delegates to the canonical configured writer. It must not expose optimized-merge, bottom-up-rebuild, deferred-rebalance, or cache switches unless those switches have distinct canonical implementations and measurements.

## Architecture

### 1. Execution policy and scheduler

A small execution-policy module computes:

- whether the workload crosses `parallelism_threshold`;
- effective worker width;
- bounded wave size, initially `4 * effective_width`;
- whether the store benefits from ordered batched reads;
- which mutation strategy has proved-safe independent work.

The scheduler consumes indexed inputs and returns indexed outputs. Every collection is restored to source-key order before canonical assembly. The scheduler never owns chunking logic.

### 2. Mutation preprocessing

Sorting and last-write-wins deduplication remain deterministic. Large unsorted inputs may use Rayon’s stable indexed parallel sort. Already sorted inputs retain the current linear scan. The effective mutation count, rather than raw input count, controls the threshold.

### 3. Ordered route hydration

The route planner walks each tree level in ordered frontier waves. It calls `load_many_ordered_with_parallelism` with the effective width instead of a fixed constant. Duplicate CIDs are loaded once and results are expanded back to their ordered positions.

For a store that does not prefer batched reads, route planning avoids splitting a single cheap in-memory batch into unnecessary parallel calls. Node decoding may still use the configured CPU width when the frontier is large enough.

### 4. Key-stable leaf execution

When the chunking input and measure prove that value replacement cannot move boundaries, affected leaf groups are independent. Groups are processed in indexed Rayon partitions:

1. merge mutations into the leaf;
2. enforce exact serialized byte limits;
3. encode the leaf once;
4. compute its CID;
5. return an ordered replacement summary.

Parent frontiers are grouped by parent CID. Independent parents at the same level are encoded and hashed in parallel, then gathered in key order. Each parent is constructed once.

### 5. Structural mutation islands

Insert, delete, key-dependent byte-measure, rolling, and Weibull workloads may move boundaries. They cannot be split at arbitrary keys.

The route planner creates candidate mutation islands only at existing canonical leaf boundaries. Islands must be separated by at least one untouched canonical leaf. Each island owns its mutations plus a bounded right-side resynchronization guard.

An island starts a fresh canonical leaf emitter at an existing leaf boundary, replays old entries and mutations in key order, and succeeds only if it emits an old leaf CID before reaching the next island’s protected boundary. A matching old CID proves that the boundary detector has returned to the same reset state. Successful non-overlapping islands are independent.

If an island does not resynchronize, touches its neighbor’s guard, or produces an ambiguous boundary, it is not published. The scheduler deterministically coalesces it with the adjacent island and retries. Repeated collisions eventually produce one region, which is the current canonical sequential resynchronizing write. This makes speculation an optimization rather than a second correctness path.

No speculative node is flushed before island validation. Failed island bytes stay local and are dropped.

### 6. Canonical frontier assembly

Validated leaf replacements are merged with reused summaries in key order. At each internal level, the canonical level emitter alone decides parent boundaries. CPU-heavy encoding and CID generation for already closed node plans may run in parallel, but the ordered emitter state remains authoritative.

For fixed-separator formats, independent changed parents may be rebuilt in parallel. For boundary-sensitive internal levels, ordered emission remains sequential until a later optimization can prove safe resynchronizing islands at that level.

### 7. Persistence and cache behavior

Validated nodes are deduplicated by CID and persisted with one ordered `batch_put` per bounded publication batch. Persistence order does not affect the root but remains deterministic for repeatable store traces.

The existing small-write cache policy remains automatic. Cache warming is not exposed as an unrelated batch algorithm switch. Large writes avoid filling the node cache unless the runtime cache policy explicitly requests it.

## Error Handling

Any routing, decoding, boundary, encoding, hashing, or persistence error cancels the call and returns the original error. Rayon results are collected as ordered `Result` values; the first error in input order is returned for deterministic diagnostics.

A failed speculative island is not an error when it merely cannot prove independence. It is coalesced and retried. A malformed node, format mismatch, or store failure remains an error and is never converted to a fallback.

Content-addressed nodes written before a later store failure may be unreachable, but no successful root is returned. This matches the existing publication contract.

## Performance Protection

Parallel planning is bypassed when any of these conditions holds:

- effective mutations are below `parallelism_threshold`;
- effective width is one;
- the strategy exposes fewer than two independent tasks;
- the workload is already handled by a cheaper append or point-mutation fast path;
- structural islands immediately coalesce into one region.

Work is processed in waves of at most `4 * effective_width` tasks. This prevents mutation-count-proportional task metadata and keeps peak memory bounded while providing enough work for Rayon stealing.

The implementation must not promise universal improvement: scheduling noise and hardware differ. The merge gate is empirical. Against the immediately preceding release baseline on the same host, protected small-workload medians may not regress beyond 2%, protected p95 latency may not regress beyond 5%, and any result inside that band must be reported as noise rather than an improvement. Large independent workloads must show positive scaling before the parallel route is enabled by default.

## Correctness Tests

1. Compare `batch`, `parallel_batch`, and fresh bulk-build roots for widths 1, 2, 4, 8, and automatic.
2. Run the matrix across all built-in chunking policies, fixed and variable values, and supported layouts.
3. Cover sorted and unsorted inputs, duplicate mutations, append, random, clustered, value-only, insert-only, delete-only, and mixed batches.
4. Force mutation-island success, collision/coalescing, no-resync fallback, and store failure.
5. Assert byte-identical reachable node sets, not only root equality.
6. Instrument maximum route-read width and task concurrency; verify `max_threads` and `sequential()`.
7. Run concurrent callers against the same immutable base and shared store, verifying roots and absence of deadlock or oversubscription.
8. Preserve exact encoded-node cap tests and format mismatch behavior.

## Performance Evaluation

All performance conclusions use release builds and repeated samples. Capture raw data, environment, commit IDs, worker counts, and commands.

### Workloads

- Base trees: 100 thousand, 1 million, and 10 million entries.
- Mutation batches: 1 thousand, 10 thousand, 100 thousand, and 1 million effective mutations.
- Shapes: append, random, clustered, value-only, insert-only, delete-only, and 60/20/20 upsert/insert/delete mixed.
- Stores: `MemStore`, batched high-latency synthetic store, and one persistent store representative.
- Callers: one batch and concurrent 2/4/8 caller saturation.
- Worker widths: 1, 2, 4, 8, 12, 16, and automatic, capped by available hardware.

### Measurements

- operations per second and effective mutations per second;
- p50, p95, and p99 wall latency;
- CPU utilization and scaling efficiency;
- peak RSS and allocated bytes when available;
- nodes and bytes read/written;
- point and batched store calls;
- maximum observed task and route-read concurrency;
- speculative island success, coalescing, and fallback rates.

### Acceptance

- Every worker width passes canonical root and reachable-byte equality.
- Width one is behaviorally sequential.
- `max_threads` is respected by instrumentation.
- Small protected workloads remain inside the regression gates.
- At least the 100-thousand random value-update and mixed structural workloads demonstrate measurable improvement on available many-core hardware; otherwise their automatic parallel strategy remains disabled.
- Reports state regressions and statistically inconclusive results explicitly.

## Implementation Boundaries

Expected source changes are limited to:

- `src/prolly/parallel.rs`: execution policy, width calculation, and indexed scheduling helpers;
- `src/prolly/write.rs`: configured canonical entry point and structural-island orchestration;
- `src/prolly/batch.rs`: configured route hydration and parallel key-stable group execution; removal of inert writer switches;
- `src/prolly/mod.rs`: public routing and concurrency instrumentation hooks;
- `src/lib.rs`: truthful exports;
- canonical root and performance tests/benchmarks.

`boundary.rs`, `builder/streaming.rs`, and persisted formats remain authoritative and unchanged unless an implementation exposes a narrowly scoped allocation or encoding optimization that preserves byte identity.

## Rejected Alternatives

### Restore `ParallelRebalancer`

Rejected because it reconstructs roots independently through a legacy boundary model and cannot atomically combine overlapping ancestor changes.

### Parallel full-tree rebuild for every large batch

Rejected because it discards structural sharing, increases I/O and memory, and harms clustered mutations even if CPU utilization looks high.

### Parallelize only encoding and hashing

Useful as a low-risk component, but insufficient as the complete design because routing and independent leaf mutation dominate large random batches.

## Success Definition

The work is complete when parallel configuration is observable and honored, all worker counts produce byte-identical canonical trees, general batches have a safe deterministic parallel route with canonical fallback, inert batch tuning APIs are gone, and the release-mode report demonstrates scaling without crossing the protected latency and memory regression gates.
