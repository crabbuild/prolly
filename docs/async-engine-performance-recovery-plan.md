# Async engine performance recovery implementation plan

**Date:** 2026-07-19

**Status:** Ready for implementation

**Design:** `docs/superpowers/specs/2026-07-19-async-engine-performance-recovery-design.md`

## Outcome

Improve the shared `ProllyEngine<S: AsyncStore>` implementation without adding a production native-sync route. The candidate must beat the pre-async baseline by at least 1.5 times on all five recovery workloads and must preserve the current point and mutation gains.

This plan uses a native measurement-driven workflow:

1. freeze reproducible binaries and fixtures;
2. profile the current implementation;
3. implement one engine-level cost reduction at a time;
4. verify correctness after each slice;
5. retain a slice only after paired benchmarks prove its value;
6. run the complete cross-workload gate before completion.

The plan does not require tests to fail before implementation. It requires direct correctness evidence and before-and-after measurements for every retained change.

## Acceptance contract

### Recovery latency gates

The pre-async revision `0efc8237` is authoritative. Threshold calculations use unrounded baseline values divided by 1.5.

| Workload | Pre-async baseline | Maximum candidate median | Current median | Improvement needed from current |
| --- | ---: | ---: | ---: | ---: |
| Conflict-resolved merge | 0.257 ms | 0.171 ms | 35.26 ms | 206x |
| Sparse merge | 64.1 µs | 42.7 µs | 167.8 µs | 3.93x |
| Range diff | 0.320 ms | 0.213 ms | 0.882 ms | 4.14x |
| Full range scan | 3.39 ms | 2.26 ms | 3.58 ms | 1.58x |
| Append-suffix stream diff | 0.309 ms | 0.206 ms | 0.324 ms | 1.57x |

### Protected workload gates

Compare protected paths with an unmodified current binary built from the implementation starting revision:

- median latency ratio at most 1.03;
- p95 latency ratio at most 1.05;
- no increase in logical node reads or writes;
- identical roots, counts, and result digests.

Protected workloads are:

- incremental insert;
- batch builder;
- owned and borrowed point get;
- point update;
- point delete;
- batch, mixed batch, append batch, and parallel batch mutation.

### Correctness and architecture gates

- Both public facades call the same async engine functions.
- No `SyncStoreAsAsync` type check or ready-only algorithm branch is added.
- Canonical roots and persisted node bytes remain unchanged.
- Every node load keeps content identifier and tree-format validation.
- Conflict resolvers run once per logical conflict in key order.
- Merge publishes no nodes until it knows the final root.
- Cancellation and failure publish nothing.
- Store reads remain bounded and do not load complete unchanged subtrees.
- The full release unit and integration suite passes.

## Files and responsibilities

### New files

- `src/prolly/diff/async_merge.rs`: persistent lineage planning, iterative structural merge frames, leaf resolution, parent assembly, and async merge publication
- `src/prolly/diff/async_range.rs`: compact async range-diff frames and eager/streaming sinks
- `scripts/run_async_engine_performance_gate.sh`: build isolated revisions, alternate run order, capture provenance, and preserve raw samples
- `scripts/summarize_async_engine_performance_gate.py`: calculate paired medians, p95 ratios, exact thresholds, logical-I/O checks, and pass/fail status
- `scripts/tests/test_summarize_async_engine_performance_gate.py`: summarizer fixture coverage

### Modified files

- `src/prolly/diff.rs`: expose shared semantic helpers, delegate production async merge/range diff to focused modules, and keep test-only native oracles isolated
- `src/prolly/engine/mod.rs`: operation-local traversal loading, branch lineage recording, and engine-owned publication helpers
- `src/prolly/engine/execution.rs`: operation diagnostics and bounded traversal scratch policy
- `src/prolly/engine/write.rs`: retain replay for canonical mutation paths, remove merge's dependency on replay, and expose publication support needed by the async merge collector
- `src/prolly/mod.rs`: bounded persistent lineage cache and internal cache accessors; no public facade routing changes
- `src/prolly/batch.rs`: allow the async engine to consume collected canonical node bytes and cache-ready decoded nodes without a second encode/decode pass
- `src/prolly/range.rs`: leaf-run cursor state for owned and borrowed async iteration
- `benches/prolly_bench.rs`: focused recovery mode, protected workload mode, result digests, roots, logical I/O, publication counts, and stable sample output
- `docs/performance.md`: final verified numbers and gate commands

## Phase 0: Isolate the work and freeze evidence

### Actions

1. Record the implementation starting revision, dirty-file digest, compiler, allocator, host, and power state.
2. Leave the unrelated PostgreSQL benchmark changes and untracked reference trees untouched.
3. Build three isolated release binaries outside the parent Cargo workspace:
   - pre-async baseline `0efc8237`;
   - unmodified async starting revision;
   - candidate working tree.
4. Copy each binary to a revision-labeled immutable path and record its SHA-256 hash.
5. Run the 50,000-entry suite seven times in alternating order to refresh baseline and current distributions on the same host.
6. Preserve the existing deterministic seeds, key encoding, values, mutation count, resolver, cache size, and warm-up policy.
7. Record 10,000-entry and 100,000-entry baseline samples for later scale confirmation.

### Exit evidence

- Three reproducible binary manifests exist.
- Baseline and current results reproduce the reported direction and remain within 10 percent of the earlier medians.
- Every sample has a matching root or result digest.

## Phase 1: Build an auditable focused gate

### Benchmark changes

Add `PROLLY_BENCH_ONLY=async-engine-recovery`. It runs these rows in a stable order:

1. `merge_conflict_resolved_mem`;
2. `merge_sparse_mem`;
3. `range_diff_window_mem`;
4. `range_scan_mem`;
5. `stream_diff_append_suffix_mem`;
6. all protected point and mutation rows.

Extend each focused row with:

- fixture scale and logical item count;
- total latency and per-item latency;
- p50, p95, and p99;
- output root or deterministic result digest;
- output count;
- nodes and bytes read;
- nodes and bytes written;
- point, ordered-batch, and publication call counts;
- merge resolver call count;
- replay attempt count while replay still exists.

The gate script must:

- alternate baseline/current/candidate order by pair;
- reject missing or malformed rows;
- reject correctness or logical-I/O mismatches before evaluating latency;
- calculate exact baseline-divided-by-1.5 targets;
- calculate protected candidate-to-current ratios;
- emit `raw-results.csv`, `summary.csv`, `gate.csv`, `manifest.txt`, and `report.md`.

### Validation

- Run the summarizer's fixture tests.
- Run one smoke pair and inspect every output column.
- Confirm the unmodified current binary fails all five recovery gates.
- Confirm the pre-async binary does not accidentally pass the 1.5-times-baseline gates.

### Exit evidence

The harness fails for known-slow binaries, detects root/count/I/O corruption, and produces deterministic summaries.

## Phase 2: Profile merge replay and range traversal

### Merge diagnostics

Instrument `execute_replay` and `try_structural_merge_async` under benchmark diagnostics. Record:

- replay attempts;
- unique and repeated CIDs requested per attempt;
- operation restarts;
- cache locks and cache hits;
- node decodes and re-encodes;
- collector nodes and bytes;
- resolver calls;
- time in hydration, pure merge, publication preparation, and publication.

Use Instruments Time Profiler and Allocations on both merge workloads. Capture the top CPU stacks and allocation sites.

### Diff and range diagnostics

Record:

- frame pushes and pops;
- node-pair loads;
- span endpoint allocations;
- `Diff` allocations;
- `VecDeque` growth;
- leaf transitions;
- `Arc` clones;
- end-bound comparisons;
- owned key and value allocations.

### Decision checkpoint

Use the profile to confirm or revise the ordering of later phases. Keep the architectural constraints unchanged. Do not retain diagnostic branches that add measurable overhead when diagnostics are disabled.

## Phase 3: Add bounded persistent branch lineage

The warm same-engine merge targets are too strict for a full structural walk. Use branch history already known to the engine, without making it part of correctness.

### Data model

Replace the four-record direct lineage cache with a bounded persistent chain keyed by child root:

```rust
struct BranchLineageRecord {
    parent: Option<Cid>,
    child: Cid,
    mutations: Arc<[Mutation]>,
    depth: u16,
}
```

The cache retains enough records for both 100-change benchmark branches. Start with a 512-record and 8 MiB internal cap, then tune only from measurements. Eviction removes acceleration data only.

### Recording

Record normalized, sorted, unique mutations after a successful canonical publication for point, batch, append, and delete origins. Reuse the existing mutation allocation where possible. Do not copy values solely for lineage if the canonical writer can transfer ownership into an `Arc<[Mutation]>` after publication.

Measure the added point-write cost immediately. If median or p95 exceeds the protected gate, reduce lock scope, shard the cache, or store compact single-mutation records before continuing.

### Planning

Add an engine method that walks parent links from a branch root to the requested base root and produces one normalized change stream. It must:

- enforce a maximum hop and mutation count;
- detect eviction, cycles, and disconnected ancestry;
- preserve last-write-wins semantics;
- return `None` on uncertainty;
- never consult storage;
- never affect the fallback result.

### Merge optimizations

For two complete lineage streams:

1. merge the sorted changes with two indices;
2. load base values only for overlapping keys;
3. invoke the resolver once for each true conflict;
4. return `left` when every selected value matches left;
5. return `right` when the merged logical state is exactly right;
6. otherwise apply only the selected right-side delta to left through the canonical async batch writer.

The benchmark's all-right conflict resolver should return the existing right root without publication. Sparse disjoint suffixes should become one small canonical batch.

### Correctness validation

- Compare warm-lineage results with the test-only merge oracle across updates, inserts, deletes, duplicate histories, and resolver choices.
- Clear or evict lineage and confirm the fallback produces the same root.
- Reopen the tree in a new engine and confirm correctness without lineage.
- Confirm cache truncation and memory caps cannot change results.

### Performance checkpoint

Run seven focused pairs for both merge workloads and all protected writes. Retain this phase only if:

- conflict merge is at most 0.171 ms;
- sparse merge is at most 42.7 µs;
- protected medians and p95 values pass;
- resolver and publication counts match expectations.

If either merge target remains unmet, profile lineage normalization, overlap value lookup, resolver allocation, and canonical batch setup before implementing the structural fallback.

## Phase 4: Replace restart-based async structural merge

Lineage cannot help reopened trees, independent replicas, or evicted histories. Replace merge replay with a resumable async kernel that remains efficient without history.

### State machine

Create compact frames:

```rust
enum MergeFrame {
    Visit { base: Cid, left: Cid, right: Cid },
    Assemble { nodes: [Arc<Node>; 3], child_start: usize },
}
```

Use a parallel result stack for `StructuralMergeResult`. The concrete layout may change to reduce cloning, but it must support these transitions:

- CID equality reuse without a load;
- one ordered triple load for a divergent frame;
- leaf resolution without an await;
- child frames in deterministic key order;
- bottom-up internal assembly;
- bounded shape-mismatch fallback;
- final one-shot publication.

### Frontier loading

At each internal level:

- collect divergent base/left/right child triples;
- deduplicate CIDs while retaining result positions;
- take the global node-cache lock once for the complete frontier;
- load misses through `load_many_ordered_for_format`;
- retain loaded `Arc<Node>` values in the operation context;
- release all locks before resolver calls or awaits.

Do not prefetch equal subtrees. Bound the frontier with the existing execution configuration and process additional chunks in logical order.

### Canonical assembly and publication

Reuse `BatchWriteCollector` encoding and CID calculation. Add read-only accessors for encoded entries and retained cache nodes. The async merge kernel must:

- avoid decoding nodes it created;
- deduplicate identical output CIDs;
- validate reused child counts;
- publish with `PublicationOrigin::Merge` once;
- cache published nodes after acknowledgment;
- return no tree when publication fails.

Keep `execute_replay` for canonical mutation code until a separate measured project replaces it. Remove only merge's dependency on it.

### Cold-path validation

- Run merges with node and lineage caches cleared.
- Run on a fresh engine backed by the same `MemStore`.
- Run on an async delayed store that returns `Pending` before each read.
- Compare roots, bytes, explanations, conflicts, and I/O with the oracle.
- Verify cancellation before publication and publication failure behavior.

### Performance checkpoint

Report warm lineage and cold structural numbers separately. The warm contract must retain the strict acceptance targets. The cold path must beat the current async implementation by at least 1.5 times without increasing logical reads.

## Phase 5: Replace eager async range diff with a compact cursor

### Cursor design

Move production async range-diff traversal into `diff/async_range.rs`. Use compact frame variants for compare, added, and removed spans. Each frame owns a span endpoint only when it survives an await.

Process one ordered frontier at a time:

1. discard equal CIDs and out-of-range spans;
2. batch-load divergent node pairs;
3. compare aligned internal spans without collecting entire subtrees;
4. push child frames in reverse order so popping preserves key order;
5. compare leaf slices directly;
6. write into the requested sink.

### Allocation reductions

- Reserve eager result capacity from the changed range when cardinality is known.
- Reuse frame, CID, and node vectors across frontiers.
- Borrow separator keys during pure transitions.
- Clone a key or value only when constructing a returned owned `Diff`.
- Avoid converting between `Node` and `ReadNode` in one operation.
- Hold an operation-local node map to avoid repeated global cache locks.

### Consumers

- `compute_async_range_diff` collects owned results through the cursor.
- Range-limited merge consumes cursor changes directly.
- Existing public result ordering and errors remain unchanged.

### Validation and checkpoint

Verify empty, reversed, narrow, boundary-crossing, added-only, removed-only, shape-mismatch, malformed, and custom-format ranges. Run seven focused pairs and retain the phase only when median range diff is at most 0.213 ms with unchanged logical reads.

## Phase 6: Optimize append-suffix streaming diff

### Pending state

Replace append-only `VecDeque<Diff>` materialization with a pending leaf cursor:

```rust
struct PendingLeafDiff {
    node: Arc<ReadNode>,
    next: usize,
    end: usize,
    kind: DiffFrameKind,
}
```

The iterator materializes one `Diff` when `next()` returns it. It retains compact pending subtree frames instead of flattening an entire appended suffix.

### Fast append proof

Reuse the existing append-only structural proof. When the shared prefix ends inside aligned right-edge leaves, store the first unmatched leaf position and then continue through added right siblings. Do not rescan the shared prefix on each `next()` call.

### Checkpoint compatibility

Translate pending leaf state into the existing stable cursor representation, or extend the internal marker format without changing public serialized cursor behavior. Resume tests must reconstruct the exact remaining stream.

### Validation and checkpoint

Verify empty suffixes, multi-level suffixes, boundary drift, non-append fallback, malformed children, cursor resume, and result order. Retain the phase only when append-suffix stream diff is at most 0.206 ms and sparse/full-rewrite diff rows do not regress.

## Phase 7: Optimize full range scans with leaf runs

### Cursor state

Replace per-entry end checks and `last_location` maintenance with explicit leaf-run fields:

```rust
struct LeafRun {
    node: Arc<ReadNode>,
    next: usize,
    end: usize,
}
```

When entering a leaf, compute `end` once with binary search against the range end. Entry advancement increments `next` only. Structural stack work resumes after the run ends.

### Owned entry extraction

Add one `ReadNode` helper that validates the entry index once and copies key and value into the public owned pair. Avoid separate bounds checks and repeated offset-table decoding for `key()` and `value()`.

Keep borrowed iteration on the same leaf-run state and return slices without allocation. Preserve resume-cursor behavior by storing the last yielded key index rather than cloning an extra node pointer.

### Allocation-floor analysis

Owned `(Vec<u8>, Vec<u8>)` results require two allocations per entry. Profile after the state reduction. If median remains above 2.26 ms:

1. verify mimalloc reuse and allocation hot spots;
2. specialize packed-node entry copying to calculate both lengths once;
3. eliminate temporary key reconstruction buffers;
4. reuse prefix bytes directly when compact encoding permits;
5. confirm the benchmark includes only contract-required ownership work.

Do not change the public return type, omit ownership, or substitute the borrowed benchmark.

### Validation and checkpoint

Verify full, bounded, empty, single-leaf, multi-level, resume, forward, reverse, cache-resistant, and malformed-node cases. Retain the phase only when full range scan is at most 2.26 ms and borrowed range scan does not regress.

## Phase 8: Combined optimization and cleanup

### Cross-component review

- Remove superseded merge replay wrappers and dead async range-diff helpers.
- Keep test-only native oracles clearly marked and unreachable from production.
- Consolidate duplicated frame and sink utilities only when measurements show no regression.
- Remove diagnostic output from default builds or guard it behind existing benchmark diagnostics.
- Run formatting and clippy on touched crates.

### Correctness suite

Run:

```bash
cargo test --release --lib --tests
cargo test --release --test zero_copy_reads
cargo test --release --test range_limited_merge
cargo test --release --test resumable_diff
cargo test --release --test traversal
cargo test --release --test async_foundation_default
```

Because the parent workspace currently has an unrelated `prolly-map` version mismatch, run root-crate verification from an isolated source copy until that packaging issue is fixed. Also compile one native async store confirmation crate separately.

### Performance suite

Run seven alternating pairs at 50,000 entries for baseline, current, and candidate. Then run five pairs at 10,000 and 100,000 entries. Run the complete default in-memory suite to detect displaced costs.

Reject completion if any of these conditions holds:

- one recovery median exceeds its baseline-divided-by-1.5 threshold;
- a protected median exceeds 1.03 times current;
- a protected p95 exceeds 1.05 times current;
- logical reads or writes increase;
- roots, digests, counts, resolver calls, or publication calls differ;
- a correctness test fails;
- only the ready-backed facade improves while a native async store regresses.

## Phase 9: Documentation and delivery

Update `docs/performance.md` with:

- exact revisions and binary hashes;
- host and compiler provenance;
- raw artifact paths;
- baseline, current, and candidate medians and p95 values;
- logical I/O and publication counts;
- warm-lineage and cold-structural merge results;
- native async confirmation results;
- any rejected experiments and their measured reason.

Deliver a concise final report that maps every acceptance requirement to direct evidence. Do not call the goal complete until all five strict recovery gates and every protected-path gate pass.

## Commit and rollback strategy

Use small measured commits in this order:

1. benchmark gate and diagnostics;
2. bounded persistent lineage;
3. iterative async structural merge;
4. compact async range diff;
5. append-aware stream diff;
6. leaf-run range scan;
7. combined cleanup and evidence.

After each commit:

- record its focused before-and-after table;
- run affected correctness suites;
- revert or revise it if its target or protected gates fail;
- do not stack another optimization on an unproven regression.

This sequence makes every performance claim bisectable while preserving the single async-engine architecture.
