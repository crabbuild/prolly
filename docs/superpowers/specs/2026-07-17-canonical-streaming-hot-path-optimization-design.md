# Canonical Streaming Hot-Path Optimization Design

## Status

Approved for implementation on 2026-07-17. This optimization cycle follows the
canonical streaming hard cutover and may not change persisted format, boundary
placement, node bytes, CIDs, public API behavior, or publication semantics.

## Objective

Recover the remaining rolling-detector and point-mutation CPU/allocation cost
without giving back the cutover's append, batch, memory, or correctness gains.
The cycle covers three related hot paths: rolling hashing, hierarchical cascade
propagation, and direct point update/insert/delete dispatch.

## Selected Approach

Implement three independently measured commits in this order:

1. Optimize the rolling detector while preserving every hash value and reset.
2. Reuse height-bounded cascade scratch instead of allocating per propagation.
3. Remove redundant point-mutation adapters and temporaries only where direct
   route invariants prove canonical equivalence.

Each commit must pass its focused correctness and performance gates before the
next begins. A win in one workload cannot offset a regression in another.

## Canonical Invariants

1. Existing rolling golden vectors and complete boundary sequences are exact.
2. Every built-in policy and node layout produces the same roots and node bytes
   before and after this cycle.
3. Exact serialized hard caps and `EntryTooLarge` behavior remain unchanged.
4. Sync, async, stats, parallel, append, update, insert, delete, merge, and
   range-delete writers remain equivalent to a fresh canonical build.
5. Storage failure never returns a root that references an unpublished node.
6. Optimizations add no unsafe code and no new runtime dependency.

## Rolling Detector

`BoundaryDetector` keeps the persisted rolling algorithm unchanged. Its current
per-byte mixer constructs an `Xxh64` state for every incoming and outgoing
window byte. Replace that repeated work with detector-local memoization of the
same `byte_hash(seed, byte)` results. Cache entries are populated lazily so
small or low-entropy streams do not pay to initialize all 256 values.

Replace `VecDeque<u8>` with a fixed-capacity circular window allocated once for
rolling policies. The state tracks the next replacement slot and current
length. Incoming and outgoing bytes use the cached mixer, so the rolling-hash
equation and emitted boundaries remain byte-for-byte identical.

## Cascade Propagation

`HierarchicalEmitter` currently allocates a temporary propagation vector for
each cascade. Give the hierarchy one reusable scratch vector whose capacity is
bounded by emitted siblings and tree height. Clear and reuse it for each
cascade; never retain entry payloads beyond the call.

The ordering of emitted nodes and parent summaries must remain unchanged. A
test-only observation point will verify that repeated cascades reuse the same
scratch allocation after warm-up.

## Point Mutations

The direct key-stable update path currently converts normalized mutations into
`Mutation` values to call the batched-value adapter even when the batch is
below its 256-mutation threshold, then converts them back on fallback. Classify
the route before materializing that adapter representation. Single and small
updates go directly to subtree rewriting; qualifying large batches keep the
existing batched route.

After that change, measure update, insert, and delete separately. Additional
edits are allowed only when they remove demonstrably redundant root/cache
lookups, node clones, or write-vector allocations while preserving the direct
route's boundary proof. Point insertion and deletion must continue to fall back
whenever their canonical proof is incomplete.

## TDD Strategy

1. Add a failing rolling-mixer test that requires a memoized mixer to match the
   reference `byte_hash` for all byte values and multiple seeds.
2. Add a failing rolling-window differential test against a test-only
   `VecDeque` reference over wraparound, reset, and randomized streams.
3. Add a failing hierarchy scratch-reuse test covering leaf and parent
   cascades.
4. Add a failing point-route classification test proving sub-threshold updates
   bypass the batched adapter while threshold-sized eligible updates retain it.
5. Preserve existing randomized policy/layout/API root-equivalence and failed
   publication tests after every implementation step.

## Performance Gates

Use release builds and identical deterministic fixtures. Capture at least five
independent process samples for focused latency comparisons.

- Rolling detector median: improve by at least 40%; target the current Dolt Go
  rolling-splitter range without changing Rust boundary semantics.
- Append 1/64/4096: no median regression greater than 2% and no p95 regression
  greater than 3%.
- Middle update/insert/delete: each must be neutral or faster within a 2%
  median noise envelope; none may be hidden by aggregate mutation results.
- Sorted and unsorted builds: no median regression greater than 2%.
- One-million-record random and clustered mutation workloads: no median
  regression greater than 2%.
- Nodes and bytes read/written, serialized tree bytes, and tree height: exactly
  unchanged for matched fixtures.
- Peak RSS and measured allocation bytes: no increase beyond 2% noise.

If any gate fails repeatedly, revert or redesign that individual optimization;
do not relax correctness or average the regression against another win.

## Out of Scope

- Changing the default chunking policy, hash algorithm, rolling framing, or
  Weibull shape.
- Root migration or compatibility code.
- Unsafe zero-copy mutation.
- Allocator pooling that retains unbounded node or value capacity.
- Durable-store redesign or unrelated read-path work.
