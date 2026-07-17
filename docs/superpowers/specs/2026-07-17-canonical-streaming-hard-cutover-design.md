# Canonical Streaming Hard Cutover Design

## Status

Approved for implementation on 2026-07-17. This is an alpha-stage hard cutover: persisted-root compatibility, migration, and legacy API behavior are explicitly out of scope.

## Objective

Make one policy-aware streaming engine the only authority for prolly-tree boundaries, then use it for every bulk-build and mutation surface. Correctness is the release gate. Within that constraint, the implementation should minimize mutation work, sorted-build memory, allocation pressure, storage round trips, and high-percentile latency.

## Current Problems

The repository currently has two incompatible boundary models:

- `BoundaryDetector` honors persisted `ChunkingSpec` fields, including measure, boundary input, rule, level salt, and the hard encoded-byte cap.
- The legacy `is_boundary` family always applies entry-count bounds and hashes raw key/value bytes. It silently ignores non-default measures, key-only boundaries, Weibull and rolling rules, level salting, length framing, and the hard byte cap.

This duplication is already a correctness defect. Direct `BatchWriter::apply_batch_with_stats` can produce a root different from a fresh canonical rebuild for both the default policy and the rolling logical-byte policy. The ordinary `Prolly` mutation APIs route through the canonical writer and do not reproduce that divergence.

The rolling preset is also miscalibrated. It compares one rolling-hash sample per entry against a threshold derived from a byte target. In a one-million-record probe, the 16 KiB rolling preset averaged 63,127 bytes per chunk and reached the 64 KiB maximum for the median, p90, and p99 chunks. The Weibull preset averaged 15,214 bytes on the same input.

Finally, `SortedBatchBuilder` streams leaf key/value data but retains every leaf summary until `build()`. Its peak metadata memory is therefore proportional to the number of leaves rather than tree height.

## Goals

1. One authoritative boundary state machine for every persisted chunking policy and every tree level.
2. Canonical root equality across bulk, sorted-streaming, append, individual mutation, batch mutation, stats-bearing, parallel, merge, and range-delete entry points.
3. Preserve user-selectable chunking policies while retaining key-only entry-count chunking as `Config::default()`.
4. Correct rolling-policy units so `target` means the expected chunk measure, not the expected number of entries.
5. Enforce exact serialized-node hard caps on every construction and mutation path.
6. Make sorted construction hierarchical and streaming with memory bounded by active chunks, tree height, and the persistence batch.
7. Preserve or improve default-policy mutation latency and throughput.
8. Measure median and tail latency before and after the cutover.

## Non-Goals

- Preserving existing root CIDs or reproducing roots written by the alpha implementation.
- Providing an on-disk migration path.
- Keeping legacy boundary helper APIs.
- Changing the default away from key-only entry-count chunking.
- Replacing content hashing, node CIDs, storage backends, or value-blob offloading.
- Making the unsorted batch builder fully streaming; it must retain input to sort it.

## Canonical Invariants

The implementation is acceptable only if all of these remain true:

1. A tree root is a deterministic function of the sorted unique key/value set and persisted `TreeFormat`.
2. Every public write path produces the same root as a fresh canonical bulk build.
3. A detector reset occurs exactly after an emitted chunk or an explicit hard-cap pre-split.
4. No non-root internal node contains fewer than two children.
5. A serialized node never exceeds `hard_max_node_bytes`; an individually unencodable entry returns `EntryTooLarge` before publishing a root.
6. Storage errors are returned to the caller. They are never discarded or converted into a root that references missing nodes.
7. User-selected policy fields have identical semantics at leaves and internal levels.
8. Parallelism may change scheduling but never boundaries, node bytes, or root CIDs.

## Selected Architecture

### 1. One boundary engine

`BoundaryDetector` becomes the sole stateful boundary implementation. It consumes:

- the complete persisted `ChunkingSpec`;
- the tree level;
- key and value bytes;
- the exact encoded contribution of the entry;
- the exact current serialized-node size needed for hard-cap enforcement.

It returns a typed decision distinguishing:

- continue the current chunk;
- close after the observed entry;
- reject an entry that cannot fit in an empty node.

The implementation retains the independent-hash probe only as a policy-specific optimization for entry-count `HashThreshold`. That probe must be implemented by the same hash/framing primitive used by the stateful detector.

The following helpers are removed rather than deprecated:

- `is_boundary`
- `is_boundary_config`
- `is_hash_boundary_config`
- their raw key/value hash implementation

Any public conformance check that needs a boundary answer will instantiate and drive `BoundaryDetector`, so it tests real canonical behavior.

### 2. Rolling calibration

Rolling boundaries remain entry-aligned: a node closes only after a complete key/value record. The rolling hash still summarizes the configured boundary input window, but its boundary probability is scaled by the chunk measure consumed by the entry.

For the eligible portion of an entry after `min`, define:

```text
eligible_delta = eligible_current - eligible_previous
scale = max(target - min, 1)
p = 1 - exp(-eligible_delta / scale)
```

The detector compares its deterministic rolling-hash sample with `p`. Crossing `max` or the encoded hard cap still forces a boundary. If `target == min`, the first entry reaching `min` closes the chunk. This gives an exponential rolling policy whose expected measure is approximately `target`, while preserving rolling content sensitivity and entry-aligned nodes.

Canonical probability calculation must not depend on platform libm behavior. The implementation will use a documented deterministic fixed-point threshold routine, with golden vectors covering small deltas, target-sized deltas, saturation, and overflow limits. The existing floating-point Weibull calculation will be moved onto the same deterministic threshold substrate during the cutover so canonical roots do not depend on platform floating-point transcendental functions.

### 3. Exact encoded-size accounting

Hard-cap decisions use the selected node layout's exact serialized length, including headers, persisted format bytes, prefix compression, offsets, child counts, and varints. Conservative estimates may remain for capacity reservation but not correctness.

Each active level tracks the exact encoded size of its current node. Before accepting an entry it computes the size with that entry. If the entry does not fit a non-empty node, the current node is emitted and the entry is retried against an empty node. If it still does not fit, the operation returns `EntryTooLarge`.

After emission, a release-mode postcondition check verifies the serialized byte length is within the cap. A cap violation is returned as an error rather than guarded only by `debug_assert!`.

### 4. Hierarchical streaming builder

`SortedBatchBuilder` is replaced internally by a hierarchy of active level emitters:

```text
sorted key/value entry
        |
        v
level 0 emitter -- sealed leaf summary --> level 1 emitter
                                             |
                                             v
                           sealed internal summary --> level 2 emitter
                                                              ...
```

Each level owns only:

- one active node;
- one `BoundaryDetector`;
- exact encoded-size state;
- enough finalization state to avoid a single-child internal node.

When a node closes, its bytes and summary are generated once. The bytes enter a bounded persistence batch, and its `(first_key, CID, subtree_count)` summary is pushed immediately into the next level. There is no all-leaf summary vector.

Finalization proceeds bottom-up. A level with one unpromoted node and no sibling becomes the root directly. Multiple nodes are propagated until exactly one canonical root remains. The finalization algorithm must produce the same root as the policy-aware batch builder for every built-in policy and layout.

`BatchBuilder` keeps sorting and deduplication, then uses the same level-emitter primitives for node assembly. For the independent entry-count threshold policy it may still precompute hash candidates and leaf ranges in parallel. Parallel and serial assembly must share the exact boundary and encoded-size primitives.

### 5. Mutation hard cutover

Every public mutation surface routes through the canonical `write` pipeline:

- `Prolly::{put, delete, batch, batch_with_stats, batch_with_write_stats, append_batch}`
- `BatchWriter::{apply_batch, apply_batch_with_stats}`
- transaction and write-session wrappers
- parallel mutation wrappers
- merge and range-delete publication paths

Stats are derived from the canonical execution rather than selecting a different algorithm. A method that requests stats must never alter the resulting tree.

The canonical writer retains optimized operations when their preconditions prove root equivalence:

- rightmost-path append;
- key-stable direct value rewrites;
- localized delete windows;
- resynchronization with old leaf CIDs;
- batched route hydration and batched persistence.

Legacy rebalance code that depends on removed boundary helpers is deleted if it has no read-only consumer. Required structural utilities are rewritten to consume canonical emitted summaries rather than reconstructing boundaries independently.

### 6. Error and publication behavior

All node creation and storage operations return `Result`. Node batches may create unreachable content-addressed nodes before finalization, but a root is returned only after every node reachable from it has been persisted successfully. Unreachable nodes after a failed build are safe for later garbage collection.

Invalid `ChunkingSpec` values are rejected by fallible constructors. `SortedBatchBuilder::new` will no longer panic on invalid policy configuration.

## Performance Plan

Performance is evaluated against a baseline captured immediately before production changes. Debug builds are not used for conclusions.

### Workloads

1. Sorted fresh build at 10 thousand, 100 thousand, and 1 million records.
2. Unsorted parallel build at 100 thousand and 1 million records.
3. Single append and append batches of 64 and 4096 records.
4. Single middle value update with unchanged encoded size.
5. Single middle insertion and deletion.
6. Clustered and scattered batches.
7. All four built-in policies with fixed and variable value sizes.
8. In-memory store for CPU/allocation cost and SQLite store for storage latency.

### Measurements

- throughput;
- median, p95, and p99 wall-clock latency;
- entries streamed;
- nodes and bytes read/written;
- store point and batch calls;
- peak resident memory for 1-million-record sorted construction;
- chunk-size mean, median, p90, p99, maximum, and forced-maximum rate.

### Acceptance gates

1. Zero canonical-root divergence across the full policy/layout/API matrix.
2. Rolling mean chunk measure within 10% of `target` on deterministic fixed-size and variable-size corpora.
3. Rolling forced-maximum rate below 1% on non-adversarial corpora.
4. Default-policy append remains proportional to rightmost-path height plus the trailing leaf and appended entries.
5. Default-policy key-stable value updates do not move leaf boundaries.
6. Sorted-build peak metadata memory becomes proportional to tree height and persistence-batch size, not leaf count.
7. No more than 3% median regression in default-policy sorted or unsorted build throughput. Any larger regression blocks the cutover unless it buys a separately approved correctness requirement.
8. No more than 3% regression in default-policy append or value-update p95 latency across at least 20 measured release-mode samples after warm-up.

Performance improvements are expected in rolling-policy builds, large sorted-build memory, allocation-related tail latency, and append behavior compared with the referenced implementation. Small sorted builds may be neutral. Parallel unsorted builds are expected to remain the throughput leader.

## Test Strategy

Implementation follows red-green-refactor cycles.

1. Add regression tests reproducing direct `BatchWriter::apply_batch_with_stats` root divergence for the default and rolling policies.
2. Add policy/layout/API matrix tests comparing every public writer with a fresh bulk root.
3. Add rolling distribution tests that fail with the current near-max behavior.
4. Add hard-cap tests using exact serialized lengths at leaf and internal levels.
5. Add invalid-policy tests proving constructors return errors instead of panicking.
6. Add serial/parallel and batch/sorted root-equivalence tests over deterministic randomized records.
7. Add finalization tests for empty, singleton, exact-boundary, trailing-partial, multi-level, and degenerate internal cases.
8. Add failing-store tests proving no successful root is returned when any reachable-node write fails.
9. Run existing canonical roots, invariants, merge, range-delete, store conformance, and write-stat suites after each cutover stage.
10. Capture release-mode performance baselines before implementation and repeat the identical commands afterward.

## Removal and API Impact

This design intentionally permits breaking source and root changes.

- Legacy public boundary probes are removed from exports.
- Direct callers use `BoundaryDetector` or a new policy-aware stream probe.
- `SortedBatchBuilder::new` becomes fallible if necessary to reject invalid persisted policies.
- Rolling and Weibull roots may change because their canonical probability calculation changes.
- Alpha roots written before the cutover are not guaranteed to be reproducible.

## Rollout Sequence

1. Capture correctness failures and release-mode performance baseline.
2. Cut all stats-bearing public APIs over to canonical writes.
3. Introduce deterministic measure-aware rolling and Weibull thresholds.
4. Replace estimated hard-cap accounting with exact accounting.
5. Remove legacy boundary helpers and migrate remaining callers.
6. Introduce hierarchical level emitters behind `SortedBatchBuilder`.
7. Reuse level emitters in serial bulk assembly and preserve safe parallel precomputation.
8. Delete unreachable rebalance and compatibility code.
9. Run the complete correctness matrix.
10. Repeat performance and latency measurements and publish before/after results.

## Success Definition

The cutover is complete when the repository has one boundary engine, every public write surface produces the canonical bulk root, rolling chunks meet their configured target distribution, exact byte caps hold in release builds, sorted construction uses height-bounded metadata memory, all required tests pass, and the measured default-policy performance gates are satisfied.
