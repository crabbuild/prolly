# How will the async engine exceed pre-async performance?

**Date:** 2026-07-19

**Status:** Approved for implementation planning

**Audience:** Prolly maintainers, storage-adapter authors, and performance reviewers

**Goal:** Make the canonical async Prolly engine at least 1.5 times faster than the pre-async native baseline for the five regressed in-memory workloads without regressing protected point and mutation paths

**Content type:** Conceptual design specification

**Content plan:** Freeze the baseline-relative performance contract, define the shared async traversal kernels, preserve correctness and remote-store bounds, and specify implementation and release gates

**Open questions:** None

## TL;DR

Keep `ProllyEngine<S: AsyncStore>` as the only production tree engine. Replace restart-based structural merge replay with a resumable async state machine, make eager and streaming diff share allocation-conscious async traversal primitives, and remove avoidable per-entry work from the async range cursor. Both `AsyncProlly` and the ready-backed `Prolly` facade continue to execute the same engine code.

The performance gate is intentionally stronger than restoring the old native
latency. Each affected workload must be at least 1.5 times faster than its
pre-async baseline. Point reads, point updates, batch mutations, deletes, and
inserts may not lose more than 3 percent median throughput or latency. A result
that meets only the current implementation's latency is a failure.

## Context

The async-first migration preserved one canonical engine and greatly improved
point and mutation paths. It also moved synchronous diff and merge through
async traversal and replay machinery. Seven alternating in-memory benchmark
pairs at 50,000 entries identified five repeatable regressions:

| Workload | Pre-async baseline | Current async engine | Current change |
| --- | ---: | ---: | ---: |
| Conflict-resolved merge | 0.257 ms | 35.26 ms | +14,274% |
| Sparse merge | 64.1 us | 167.8 us | +164% |
| Range diff | 0.320 ms | 0.882 ms | +172% |
| Full range scan | 3.39 ms | 3.58 ms | +6.5% |
| Append-suffix stream diff | 0.309 ms | 0.324 ms | +5.1% |

A direct before-and-after measurement of commit `01d0f151` isolated the
sync-to-async diff and merge cutover. That commit made conflict-resolved merge
about 127 times slower, sparse merge 3.6 times slower, and range diff about 20
percent slower. Later async range-diff changes increased the final range-diff
gap.

The dominant merge cost is architectural. `try_structural_merge_async` calls `execute_replay`, whose synchronous closure aborts when `ReplayStore` discovers a missing node. The engine hydrates the missing frontier and restarts the operation. A merge can rediscover and reprocess earlier levels once per discovered frontier. Correctness is preserved, but central processing unit (CPU) work, allocation, hashing, decoding, and lock traffic grow with the frontier count.

Range and diff regressions are smaller but follow the same pattern: general
async iterator machinery owns and clones traversal state where a pure cursor
transition or borrowed node view is sufficient.

## Decision

Implement iterative async traversal kernels inside `ProllyEngine`. These
kernels retain their state across `.await`, batch reads at structural
frontiers, and materialize owned results only at the public API boundary.

Do not restore the superseded native synchronous algorithms. Do not dispatch
on `SyncStoreAsAsync`, store type, executor type, or whether a future happens
to be ready. The ready-backed facade and native async facade must call the same
functions and produce the same metrics, roots, errors, and publications.

## Performance contract

The frozen pre-async medians are the acceptance baseline. The candidate must
meet or beat these upper latency bounds:

| Workload | Required speedup | Maximum accepted median |
| --- | ---: | ---: |
| Conflict-resolved merge | 1.5x | 0.171 ms |
| Sparse merge | 1.5x | 42.7 us |
| Range diff | 1.5x | 0.213 ms |
| Full range scan | 1.5x | 2.26 ms |
| Append-suffix stream diff | 1.5x | 0.206 ms |

The unrounded gate uses `baseline / 1.5`; the displayed values are summaries,
not alternate thresholds.

Protected hot paths are point get, borrowed point get, point update, point
delete, incremental insert, batch mutation, mixed batch mutation, append batch,
and parallel batch mutation. For each protected workload:

- candidate-to-current median latency must be at most 1.03;
- candidate-to-current p95 latency must be at most 1.05;
- logical node reads and writes may not increase;
- canonical roots and result cardinalities must match.

## Required invariants

- `ProllyEngine<S: AsyncStore>` remains the sole production implementation.
- Sync and async facades execute the same traversal and merge kernels.
- Equal inputs and formats produce byte-identical roots and persisted nodes.
- Every loaded node is checked against its requested content identifier (CID)
  and tree format.
- Cache state, store batching, concurrency, and readiness affect cost only.
- Resolver calls remain ordered and occur exactly once per logical conflict.
- A failed or cancelled operation publishes no partial root or visible tree.
- Merge publishes immutable nodes once, after the complete root is known.
- No lock guard, borrowed node view, or resolver value crosses `.await`.
- Remote reads remain bounded and ordered; in-memory speed cannot come from
  unbounded prefetch or reading complete unchanged subtrees.
- Public APIs, encodings, CIDs, tree formats, and manifests do not change.

## Architecture

### Operation-scoped traversal context

Add a small internal context used by diff, merge, and range traversal. It owns
operation-local loaded-node references, deterministic I/O counters, bounded
frontier scratch space, and reusable key and CID buffers. It delegates all misses
to the existing validated engine loaders.

The context is not another global cache. Its lifetime is one operation, and it
cannot change results. Reusing a node within the operation avoids repeated
global cache locks and validation while keeping global cache policy unchanged.

### Iterative async structural merge

Replace the production merge call to `try_structural_merge_async` and
`execute_replay` with an explicit stack of merge frames. Each frame contains
the base, left, and right CIDs plus the child span needed to rebuild its parent.

The state machine performs these transitions:

1. Reuse immediately when branch CIDs prove equality or one branch equals the
   base.
2. Batch-load the remaining aligned CID triple through validated async I/O.
3. For compatible internal nodes, enqueue divergent children in key order and
   retain a parent continuation frame.
4. For compatible leaves, compare borrowed key/value slices, invoke the
   resolver once for each conflict, and emit the selected entries.
5. For a shape mismatch, switch that bounded span to the existing diff/batch
   fallback without restarting completed spans.
6. Rebuild parents bottom-up through a collector that deduplicates CIDs and
   records canonical bytes.
7. Publish the collector once after the final root is known.

The state machine must reuse the existing canonical node construction,
boundary, and serialization helpers. It does not introduce a second chunker or
writer. Test-only sync merge remains an oracle, not a production route.

### Async range diff cursor

Use one internal range-diff cursor for eager range diff, range-limited merge,
and future resumable range diff. The cursor owns a stack of compact frames and
loads base/other node pairs in ordered batches. Equal CIDs and out-of-range
spans are discarded before decoding descendants.

Leaf comparison writes directly into a caller-provided sink. Eager range diff
pushes owned `Diff` values into its result vector. Merge consumes borrowed
change views before advancing. The cursor does not build an intermediate
collection and then copy it into another collection.

Span endpoints use frame-owned storage only when they must survive an await.
Pure transitions borrow existing node keys. Reusable vectors retain capacity
for the operation.

### Append-aware streaming diff

Keep `AsyncDiffIter` as the public streaming engine, but change its pending
state from a `VecDeque<Diff>` of fully owned suffix results to a compact leaf
cursor when an append-only suffix is proven. Each `next` call materializes only
the diff it returns. Structural pruning, deterministic order, error behavior,
and checkpoint semantics remain unchanged.

### Leaf-run range iteration

Retain `AsyncRangeIter` for both public facades. Split its state into a current
leaf run and an internal traversal stack. Advancing entries within the current
leaf updates indices only; it does not clone an `Arc`, re-check an unchanged
end bound, or reconstruct callback state for every item.

The iterator checks the end bound once when entering a leaf and computes the
exclusive end index with binary search. It resumes structural traversal only
when the leaf run is exhausted. Owned iteration still copies returned keys and
values because that is its public contract. Borrowed visitors remain
allocation-free.

## Data flow

```text
Prolly / AsyncProlly
        |
        v
ProllyEngine operation
        |
        +--> operation traversal context
        |       |
        |       +--> validated ordered node loads
        |       +--> compact resumable frames
        |       +--> operation-local counters/scratch
        |
        +--> canonical collector (merge only)
                |
                +--> one NodePublication after final root
```

There is no branch from this flow to a native synchronous algorithm.

## Failure and cancellation

All traversal state is private to the future or iterator. Dropping a read or
diff future discards only that state. Merge does not call `publish_nodes` until
the state machine has produced and validated the final root. A store error,
invalid node, resolver error, cancellation, or internal invariant failure
therefore returns without publishing the collector.

Ordered frontiers retain logical key order even when reads complete out of
order. The first logical error is selected by frame order, not completion
order. Resolver callbacks are synchronous and receive callback-scoped values;
their borrows end before the next await.

## Verification strategy

Implementation follows measurement-driven engineering slices. Each slice
captures a profile, changes one cost center, verifies correctness, and then
runs its focused performance gate. Tests may be written alongside or after the
implementation; failing-first test order is not required.

1. Add diagnostics that expose replay attempt counts and quantify the current
   merge's repeated work before production changes.
2. Add state-machine equivalence tests against the test-only merge oracle for
   equal branches, disjoint sparse edits, dense conflicts, deletes, shape
   mismatch, custom formats, malformed nodes, and resolver failures.
3. Add store-observation tests for ordered batches, bounded frontiers, exact
   resolver calls, single publication, and cancellation before publication.
4. Add range-diff differential tests across empty, narrow, disjoint, boundary-
   crossing, and shape-misaligned ranges.
5. Add iterator tests for leaf-run transitions, end bounds, resume cursors,
   borrowed visitors, cache modes, and malformed children.
6. Preserve the full unit, integration, conformance, fixture, and root-vector
   suites as release gates.

Performance tests are not unit tests. Add a focused benchmark mode that emits
machine-readable rows for the five recovery workloads and the protected hot
paths. Every row includes roots or result digests, item counts, logical I/O,
publication counts, and latency samples so an incorrect shortcut cannot pass.

## Performance methodology

- Build baseline, current, and candidate with the same release toolchain and
  dependency graph.
- Use the frozen 50,000-entry deterministic fixtures and workload definitions.
- Run at least seven measured pairs, alternating revision order each pair.
- Use a warm-up before every measured sample.
- Compare paired medians and p95 values; preserve every raw sample.
- Reject samples with root, digest, count, or logical-I/O mismatches.
- Record revision, binary hash, dirty digest, host, compiler, allocator, and
  benchmark environment.
- Confirm the five recovery wins at 10,000 and 100,000 entries to reject a
  fixed-overhead or single-scale optimization.
- Run one native asynchronous store confirmation to prove the design does not
  optimize only ready futures.

## Rollout

Land the work as independently measured slices:

1. benchmark and diagnostic gates;
2. iterative async structural merge;
3. async range-diff cursor;
4. append-aware streaming diff;
5. leaf-run range iteration;
6. combined correctness and performance confirmation.

Each slice must keep all earlier gates green. A slice that improves its target
but regresses a protected hot path is revised or reverted before the next
slice.

## Alternatives rejected

### Prefetch then replay

Hydrating a large changed closure before the current synchronous closure would
reduce restart counts, but it can over-read remote stores and still repeats
pure work. It fails the requirement that in-memory gains not come from reading
unchanged subtrees.

### Ready-store specialization

Dispatching on `SyncStoreAsAsync`, a ready capability, or concrete store type
could recover local numbers quickly. It would recreate two production
algorithms and leave native async stores behind, contradicting the async-first
architecture.

### Restore native sync diff and merge

The previous native implementation is useful as a test oracle and historical
baseline. Restoring it as a production route would abandon the approved goal.

## Acceptance criteria

- All five recovery workloads meet the exact baseline-divided-by-1.5 latency
  gates at 50,000 entries in seven alternating pairs.
- The direction of improvement holds at 10,000 and 100,000 entries.
- Every protected hot path stays within the median and p95 no-regression gates.
- Logical node reads and writes do not increase for protected or recovery
  workloads.
- A native asynchronous store confirms the same algorithm and correctness.
- Sync and async differential tests produce identical values, conflicts,
  errors, roots, and persisted bytes.
- Merge invokes each resolver exactly once and publishes at most once.
- Cancellation and every tested failure publish nothing.
- The full release unit and integration suite passes.
- No production native-sync diff, merge, or range algorithm is introduced.
