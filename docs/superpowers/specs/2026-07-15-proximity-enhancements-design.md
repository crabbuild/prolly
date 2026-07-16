# Proximity Enhancements Umbrella Design

## Status

Approved in conversation on 2026-07-15.

This document defines the umbrella architecture for proximity-search
enhancements and the implementation-ready first phase. Later phases establish
architectural direction and compatibility boundaries, but each requires a
focused child design before implementation.

The first phase is a hard cutover for HNSW and PQ accelerator formats. There is
no compatibility requirement for existing accelerator objects or accelerator
search proofs. The authoritative PRXI, PRVR, PRXN, PRXV, and PQS8 formats remain
unchanged.

Component names are deliberately unversioned. `HnswIndex`,
`ProductQuantizer`, `CompositeAccelerator`, and `AcceleratorCatalog` always mean
the current implementation and wire shape. A future structural replacement
updates those components in place unless compatibility becomes an explicit
requirement; it does not introduce parallel `V1`/`V2` public types or product
names. Internal magic bytes and codec discriminators are wire-validation
details, not architecture component names.

## Decision Summary

The engine will use a layered derived-accelerator architecture:

- the ordered PRVR directory and canonical PRXN hierarchy remain authoritative;
- an `AcceleratorSet` supplies validated, source-bound derived sidecars;
- a deterministic `SearchPlanner` chooses one logical execution plan;
- backend executors implement native, eligible-exact, PQ, and HNSW search;
- a shared `SearchRuntime` provides byte-budgeted caching, batching, bounded
  concurrency, prefetching, and request coalescing;
- authoritative full-precision vectors determine returned distances;
- proofs commit to the selected plan and replay it without replanning.

Phase one hard-cuts HNSW over to deterministic bounded construction, adds
selectivity-aware filter planning, bounds PQ construction and search, and adds
the shared runtime. Phase two adds composite base-plus-delta accelerators. Phase
three adds a packed disk-oriented graph. Phase four considers advanced routing
and query composition.

## Context

The current proximity implementation already provides the difficult
authoritative properties:

- immutable content-addressed snapshots;
- an exact ordered directory and canonical proximity hierarchy;
- deterministic scalar and SIMD distance evaluation;
- source-bound SQ8, PQ, and HNSW accelerators;
- exact, approximate, filtered, budgeted, and async search;
- structural, membership, and replayable search proofs;
- typed content walking for replication and garbage collection.

The current accelerator implementation has four material limitations:

1. `src/prolly/proximity/accelerator/hnsw/build.rs` compares each inserted
   vector with every previously inserted vector at each applicable level.
   `ef_construction` is validated and persisted but does not bound build work.
2. HNSW applies eligibility when producing final results rather than maintaining
   an eligible result heap during traversal. Selective filters therefore waste
   work and may underfill the requested result count.
3. PQ construction materializes the complete record/vector/code set. PQ search
   retains and sorts every approximate score before taking its shortlist.
4. The proximity content cache records entry byte weights but evicts only by a
   fixed node count and is not shared across immutable snapshots.

The design borrows execution patterns from zvec—bounded HNSW insertion,
selectivity-aware filtering, streaming quantization, shared memory budgets,
coalesced page loading, immutable block composition, and disk-oriented beam
search—without adopting zvec's mutable collection, WAL, segment IDs, or SQL
engine.

## Goals

1. Replace quadratic HNSW construction with deterministic bounded graph-search
   insertion that uses `ef_construction`.
2. Make filtering an explicit planner and executor concern.
3. Bound accelerator build memory, candidate memory, distance work, and I/O.
4. Preserve authoritative full-precision reranking and deterministic key ties.
5. Make automatic backend selection independent of store type, cache state,
   timing, CPU features, and thread scheduling.
6. Share validated immutable content across snapshots and concurrent searches.
7. Give sync, local durable, async, and object-store execution the same logical
   behavior.
8. Preserve typed content walking, replication, GC, and replayable accelerator
   proofs.
9. Establish stable extension boundaries for delta and packed-disk
   accelerators without prematurely exposing a plugin API.
10. Retain familiar public names while permitting breaking structural API
    changes needed for typed options and richer statistics.

## Non-goals

Phase one does not include:

- backward decoding or migration of superseded HNSW or PQ objects;
- changes to authoritative proximity formats;
- external-memory HNSW construction;
- incremental or background accelerator maintenance;
- a disk-oriented graph accelerator;
- SQ8 or RaBitQ routing inside HNSW;
- sparse vectors, alternate vector element types, or variable dimensions;
- SQL, FTS, joins, or a general cost-based query optimizer;
- multi-query fusion or group/diversity retrieval;
- dynamic third-party accelerator plugins;
- machine-calibrated or latency-adaptive logical planning.

## Alternatives Considered

### Layered derived accelerators

This is the selected design. It keeps authoritative state and accelerator state
separate, introduces a pure planner, and centralizes physical resources. It
preserves Prolly's existing invariants while supporting future accelerator
families.

### zvec-style segmented vector engine

This would add mutable and immutable vector segments, a collection manifest,
WAL, compaction, and a unified query engine. Incremental and disk-backed search
would be natural, but the design would duplicate Prolly's snapshot, CAS,
content-addressing, and lifecycle machinery. Segment-local identity would also
conflict with canonical key identity and proof closure.

### Tactical optimization only

This would replace HNSW construction, bound PQ heaps, and improve filtering and
caching without a planner or shared capability boundary. It is smaller in the
short term, but composite and disk accelerators would force another API and
proof redesign. It is rejected because the additional phase-one abstractions
are small and have immediate testable responsibilities.

## Core Invariants

### Authoritative source

PRXI names the exact ordered PRVR directory and canonical PRXN hierarchy. An
accelerator is always disposable derived state. Opening, exact lookup,
mutation, verification, replication, GC, and native search never require an
accelerator.

### Exact source binding

Every accelerator manifest names one exact PRXI descriptor CID, vector
dimension, metric, record count, configuration fingerprint, and accelerator
root. A handle cannot be inserted into an `AcceleratorSet` for another source.

### Authoritative results

Routing scores may be approximate. Every returned candidate is resolved and
reranked against the current authoritative directory. Equal full-precision
scores are ordered by source key.

### Deterministic planning

Given the same source descriptor, accelerator manifests, request, eligibility
cardinality, and planner policy, the planner returns the same `SearchPlan`.
Store type, cache warmth, observed timing, CPU features, worker count, and task
completion order are not planner inputs.

### Cache-independent logical work

Logical node, byte, distance, frontier, and result budgets count committed work
regardless of cache hits. Cache state changes physical reads and latency only.

### Honest completion

The selected executor returns `Exact`, `ApproximatePolicySatisfied`,
`BudgetExhausted`, `Cancelled`, or `DeadlineExceeded` according to its actual
work. Budget exhaustion does not cause a hidden backend switch.

### Fail closed

Malformed, corrupt, unsupported, or CID-invalid accelerator content returns an
error. Automatic fallback occurs only during deterministic planning for an
absent, stale, incapable, or budget-inadmissible accelerator.

### Replayable selection

An accelerator search proof commits to the selected plan and exact accelerator
closure. Verification executes that plan directly and never reruns automatic
planning.

## Architecture

```text
PRXI descriptor
  |-- ordered PRVR directory -----------------------------+
  `-- canonical PRXN hierarchy                            |
                                                           |
AcceleratorSet                                             |
  |-- optional PQ manifest                                 |
  `-- optional HNSW manifest                               |
           |                                               |
           +--------------------+--------------------------+
                                |
                                v
                     Deterministic SearchPlanner
                                |
                                v
                           SearchPlan
             Native | EligibleExact | PQ | HNSW
                                |
                                v
                       Backend executor
                                |
                                v
                  Authoritative vector reranking
                                |
                                v
                           SearchResult

SearchRuntime supplies physical caching, coalescing, batching,
prefetching, and bounded concurrency to every executor.
```

The planner and logical executors live in the proximity module. Physical
content loading is delegated to the runtime. Accelerator-specific codecs,
builders, and traversal remain in separate modules.

## Public Type and API Direction

The public type names below are normative. Private helper names and module
placement may follow the repository's Rust conventions.

### Accelerator set

```rust,ignore
pub struct AcceleratorSet<S: Store> {
    hnsw: Option<HnswIndex<S>>,
    pq: Option<ProductQuantizer<S>>,
}
```

`AcceleratorSet::try_new` validates that every accelerator names the same
source descriptor, dimension, metric, and count. It rejects duplicate kinds.
Adding a sidecar returns a new validated set or an error; it does not mutate
persisted state.

The set exposes an internal capability view rather than trait objects:

```rust,ignore
struct AcceleratorCapabilities {
    hnsw: Option<HnswCapabilities>,
    pq: Option<PqCapabilities>,
}
```

Each capability record contains the manifest CID, source descriptor, metric,
dimensions, count, configuration fingerprint, and persisted build/quality
metadata defined by that accelerator format.

The capability layer remains crate-private in phase one. A public plugin or
registration API is explicitly out of scope.

### Search request

`SearchRequest` retains its conceptual role and gains structured options:

```rust,ignore
pub struct SearchRequest<'a> {
    pub query: &'a [f32],
    pub k: usize,
    pub policy: SearchPolicy,
    pub budget: SearchBudget,
    pub filter: ProximityFilter<'a>,
    pub kernel: QueryKernel,
    pub options: SearchOptions,
}

pub struct SearchOptions {
    pub backend: SearchBackend,
    pub planner: PlannerPolicy,
    pub hnsw: HnswSearchOptions,
    pub pq: PqSearchOptions,
}

pub struct PlannerPolicy {
    pub allow_exact_for_approximate: bool,
    pub eligible_exact_max_records: usize,
    pub eligible_exact_ratio_ppm: u32,
    pub approximate_preference: ApproximatePreference,
}

pub enum ApproximatePreference {
    HnswFirst,
    ProductQuantizedFirst,
}

pub struct HnswSearchOptions {
    pub ef_search: Option<u32>,
}

pub struct PqSearchOptions {
    pub rerank_multiplier: Option<u16>,
}
```

The default planner permits an exact plan for an approximate request, uses an
eligible-exact cap of 4,096 records and ratio of 10,000 parts per million, and
prefers HNSW. Backend-specific `None` values use the persisted accelerator
configuration. Every effective override is copied into `SearchPlan` and its
summary.

`SearchBackend` retains `Native`, `ProductQuantized`, `Hnsw`, and `Auto`.
Breaking field changes are allowed, but callers retain familiar type and
variant names.

The principal entry points are:

```rust,ignore
map.search(request)

map.search_with(
    &accelerators,
    &search_io,
    request,
)
```

`map.search` retains native execution. `HnswIndex::search` and
`ProductQuantizer::search` remain convenience entry points, but construct a
single-kind `AcceleratorSet` and force the corresponding backend through the
same planner and executor.

`SearchIo<S>` binds one store handle and opaque cache namespace to a shared
`SearchRuntime`. Cloning a binding preserves its namespace; binding an
unrelated store creates a new namespace. Sync and async methods specialize the
same binding for `Store` and `AsyncStore` respectively.

### Search plan

```rust,ignore
enum SearchPlan {
    Native(NativePlan),
    EligibleExact(EligibleExactPlan),
    ProductQuantized(PqPlan),
    Hnsw(HnswPlan),
}
```

Each variant contains every derived execution parameter. Executors do not
reinterpret `PlannerPolicy`.

`SearchResult` adds a serializable `SearchPlanSummary`. Logical and physical
statistics remain separate.

## Eligibility Preparation

`PreparedFilter` becomes a non-copying `PreparedEligibility`:

```rust,ignore
enum PreparedEligibility<'a> {
    All,
    Range {
        start: Option<&'a [u8]>,
        end: Option<&'a [u8]>,
    },
    SortedKeys {
        keys: &'a [Vec<u8>],
        source_directory: Option<&'a Tree>,
    },
}

enum EligibilityCardinality {
    Known(u64),
    Unknown,
}
```

`All` and `SortedKeys` have known cardinality. Range and prefix filters have
known cardinality only when it can be obtained from already-available
structural metadata without a full preparatory scan. Phase one treats unknown
cardinality as broad rather than scanning solely to improve planning.

Sorted keys must be strictly ascending and unique. `SecondaryEligible` retains
its exact source-directory binding. Eligibility preparation borrows caller
storage and does not clone all keys.

The representation provides:

- `contains(key)` for result admission;
- `intersects(minimum, maximum)` for structural pruning;
- ordered iteration for eligible-exact search;
- known or unknown cardinality for planning.

An ordinal bitmap may be added after subtree cardinality and ordinal navigation
are available. Such a bitmap is derived, bound to the exact source directory,
and never enters authoritative proximity formats.

## Deterministic Search Planning

The planner is a pure function:

```rust,ignore
fn plan_search(
    tree: &ProximityTree,
    capabilities: &AcceleratorCapabilities,
    request: &SearchRequest<'_>,
    cardinality: EligibilityCardinality,
) -> Result<SearchPlan, Error>;
```

### Forced backends

- `Native` always selects native execution.
- `ProductQuantized` requires a valid PQ accelerator and a non-exact policy.
- `Hnsw` requires a valid HNSW accelerator and a non-exact policy.
- A forced backend that cannot satisfy the request returns an error.

### Automatic exact planning

An exact request selects:

1. an empty `EligibleExact` plan when known eligibility is zero;
2. `EligibleExact` when a known sorted eligible set is below the deterministic
   threshold;
3. otherwise `Native`.

Approximate accelerators never claim exact completion.

### Automatic approximate planning

An approximate request selects:

1. an empty `EligibleExact` plan when known eligibility is zero;
2. `EligibleExact` when a known sorted eligible set is below the deterministic
   threshold and planner policy permits an exact plan for an approximate
   request;
3. the first budget-admissible approximate accelerator according to
   `PlannerPolicy::approximate_preference`, whose default is `HnswFirst` and
   whose alternate value is `ProductQuantizedFirst`;
4. native execution otherwise.

The eligible-exact threshold is:

```text
ratio_limit = ceil(source_count * eligible_exact_ratio_ppm / 1_000_000)
max(k, min(eligible_exact_max_records, ratio_limit))
```

Planner format versioning makes a default-policy change observable in proofs
and plan summaries.

For HNSW with known eligibility, the default minimum expansion target is:

```text
configured_ef = request.options.hnsw.ef_search.unwrap_or(config.ef_search)
base = max(configured_ef, k * overfetch_multiplier)
target = min(source_count, ceil(base * source_count / eligible_count))
```

The calculation uses checked integer arithmetic. Unknown eligibility uses
`base`. Explicit search budgets cap admissible plans; they do not get silently
increased by the planner.

The HNSW rerank target is:

```text
max(k, k * overfetch_multiplier)
```

clamped to known eligible cardinality and source cardinality. The HNSW plan
contains both the expansion target and rerank target; the executor does not
derive either value again.

## HNSW

### Format cutover

The current HNSW implementation reuses:

- `HnswIndex` and `HnswConfig` public names;
- `HnswManifest` and `HnswPage` content kinds;
- source binding and configuration fingerprints;
- typed content walking, replication, GC, and proof closure;
- canonical and disposable build modes.

The manifest and graph-record codecs accept only the current wire shape.
Superseded bytes fail decoding. There is no compatibility decoder, public
generation suffix, or migration API.

### Configuration and limits

`HnswConfig` includes:

- `max_connections`;
- `ef_construction`;
- default `ef_search`;
- `level_bits`;
- `overfetch_multiplier`;
- deterministic `seed`;
- routing-vector encoding.

Phase one accepts only `RoutingVectorEncoding::FullF32`. The encoding tag is
persisted so a later phase can add SQ8 without changing the public HNSW type.

`HnswBuildLimits` bounds:

- records;
- conservatively accounted owned bytes;
- distance evaluations;
- worker threads;
- encoded graph bytes.

The builder checks limits before growing key, vector, adjacency, candidate, or
encoded buffers. Exceeding a limit returns a resource error before manifest
publication. Phase one does not spill HNSW build state to temporary storage.

### Internal build representation

The builder reads source records in canonical key order and assigns dense
numeric build IDs. Per-node state contains the source key, full vector,
deterministic level, and adjacency arrays. Dense IDs are internal only and
never become source identity.

The level remains a deterministic function of `(key, level_bits, seed)`. Input
iteration order and worker count cannot affect it.

### Bounded insertion

For each source record:

1. If the graph is empty, install the node as entry point.
2. Starting at the current maximum level, greedily descend through levels above
   the new node's level.
3. At every applicable level, perform best-first graph search with a candidate
   heap and a bounded `ef_construction` result heap.
4. Rank candidates by `(distance, key)`.
5. Select neighbors using the deterministic diversified HNSW heuristic: a
   candidate is preferred when it is closer to the new node than to every
   already-selected neighbor. Fill remaining degree slots in ranked order when
   diversification alone does not meet the configured degree.
6. Insert reverse edges and prune affected adjacency lists using the same
   deterministic heuristic.
7. Replace the entry point only when the new level is strictly higher. Equal
   levels retain the existing entry point.

Adjacency lists are stored sorted by source key for encoding. Search order is
always distance then key, not encoded adjacency order.

Distance batches may run in parallel, but input lanes and scalar-order
reductions remain fixed. One, two, or four workers must produce identical
graph bytes, build statistics, and manifest CID.

### Persisted graph record

Each v2 graph value contains:

- format version;
- node level;
- full routing vector and encoding tag;
- one sorted neighbor-key list per level.

The graph remains an ordered prolly tree keyed by authoritative source key.
Embedding the routing vector eliminates an authoritative directory lookup for
every traversal score. Finalists still resolve through the authoritative
directory.

The manifest records source descriptor, dimensions, metric, record count,
configuration, graph root, entry-point key, maximum level, canonical flag, and
configuration fingerprint.

### Search traversal

Upper levels use greedy descent. Level zero uses:

- a min-heap of traversal candidates;
- a bounded max-heap of closest traversal nodes;
- a bounded max-heap of eligible rerank candidates;
- a visited-key set;
- the planned expansion target.

Filtered nodes remain traversable and may enter the traversal heap. Only
eligible nodes enter the rerank heap. Search cannot terminate on an unfiltered
top set while the eligible heap contains fewer than `k` entries.

After the planned minimum expansion target is reached and the eligible rerank
heap is full, standard distance-bound termination may stop traversal. If the
eligible heap is not full, search continues until it fills, the frontier is
exhausted, or a request budget is reached.

The executor loads graph records through `SearchRuntime`, batches frontier
loads when the store supports it, scores embedded routing vectors, and reranks
the planned finalist count from the source directory.

### HNSW validation and statistics

Manifest load validates the root and entry point eagerly. Graph records validate
format, dimensions, levels, degree, sorted unique neighbors, no self-edge, and
neighbor-key bounds lazily when loaded.

`HnswBuildStats` adds peak owned bytes and per-level candidate work.
`ProximitySearchStats` adds graph reads, routing evaluations, eligible
candidates, filter rejections, candidate/result heap peaks, rerank reads, cache
hits, and stop reason.

## Eligible-Exact Execution

`EligibleExact` requires a known sorted key set. It performs one authoritative
lookup per eligible key, scores every present authoritative vector, and
maintains a bounded top-`k` max-heap ordered by `(distance, key)`.

The result is exact for L2 squared, cosine, and inner product. A missing key in
an ordinary eligible list is ignored because the list denotes allowed keys,
not required records. A missing key in a source-bound secondary-derived list
is an index-consistency error. A stale secondary-derived set fails its
source-directory check before execution.

Eligible-exact work participates in node, committed-byte, physical-byte,
distance, cancellation, and deadline accounting. It may return an honest
partial result when a non-exact request supplies a hard budget. An exact
request that exhausts a budget returns `BudgetExhausted`, never `Exact`.

## Product Quantization v2

### Format cutover

PQ moves to a new manifest format because training sampling and build limits
become persisted configuration. Older PQ manifests are rejected with a
rebuild-required error. The `ProductQuantizer` and
`ProductQuantizationConfig` names remain.

### Deterministic sampled training

`ProductQuantizationConfig` includes subquantizers, centroids per
subquantizer, training iterations, rerank multiplier, seed, and
`max_training_vectors`.

The first directory pass selects the `max_training_vectors` smallest tuples:

```text
(hash(seed, key), key)
```

using a bounded max-heap. The selected samples are sorted by key before
training. Selection is independent of source iteration and worker order.
The manifest persists the sampling-hash algorithm ID and version; changing the
hash algorithm is a format change.

Centroid initialization, assignment ordering, empty-cluster recovery, and
floating-point reductions remain deterministic. Parallel workers produce
fixed indexed partials that are reduced in index order.

### Streaming code construction

After training, a second key-ordered directory pass:

1. validates and encodes each vector;
2. updates reconstruction-error aggregates in key order;
3. feeds the code directly to `SortedBatchBuilder`;
4. discards per-record temporary state.

The builder does not retain codes or a second complete vector collection.
`ProductQuantizationBuildLimits` bounds training vectors, training bytes,
temporary code bytes, distance evaluations, encoded output bytes, and worker
threads.

The manifest records sampling policy, actual sample count, codebooks, quality
aggregates, source binding, code root, and configuration fingerprint.

### Bounded search

PQ search builds the query lookup table once and retains at most:

```text
k * rerank_multiplier
```

approximate candidates in a max-heap ordered by worst `(score, key)`.

For a small sorted eligible set, the executor performs direct code lookups by
key. Otherwise it scans the applicable code-tree range and applies eligibility
before scoring. It never retains or sorts every approximate score.

The executor supports all normal `SearchBudget` dimensions. Finalists resolve
and rerank through the authoritative directory. Missing codes, code/source key
mismatches, or malformed code lengths fail closed.

## Shared Search Runtime

### Ownership

```rust,ignore
pub struct SearchRuntime {
    policy: SearchRuntimePolicy,
    caches: SharedContentCaches,
    sync_in_flight: SyncRequestCoalescer,
    async_in_flight: AsyncRequestCoalescer,
}

pub struct SearchIo<S> {
    store: S,
    namespace: StoreCacheNamespace,
    runtime: Arc<SearchRuntime>,
}
```

The runtime is process-local and shareable across proximity maps, immutable
snapshots, sidecars, threads, and async tasks. A `SearchIo` binding is scoped to
one logical store namespace, and all executor reads go through its store.
Unrelated bindings cannot satisfy one another's cache or in-flight loads. The
runtime and bindings are not persisted and are not part of a proof claim.

### Qualified cache keys

Cache entries use:

```text
(StoreCacheNamespace, ContentObjectKind, CID, decoder format version)
```

This preserves typed-object isolation while allowing snapshots that share a
CID in the same logical store to share a decoded object. Store clones share an
opaque namespace token by cloning their `SearchIo` binding. New bindings receive
new tokens; namespace tokens are never supplied by callers or inferred from a
Rust type or memory address.

Phase-one partitions are:

- authoritative proximity nodes and external vectors;
- HNSW graph records and routing vectors;
- PQ tree pages and codebooks.

Each partition has byte and entry limits under one total runtime limit.
Configured partition byte limits must sum to no more than the total. A zero
limit disables that partition.

### Admission and eviction

Entries are weighted by encoded bytes plus retained decoded storage. An object
larger than its partition limit is validated and returned without admission.

Phase one uses generation-LRU eviction. Search-scoped `Arc` handles keep an
entry alive while in use; pinned bytes may temporarily remain resident after
logical eviction until the final handle drops. Metrics distinguish retained,
evictable, and pinned bytes.

Only fully CID-validated, decoded, dimension-validated, and
structure-validated objects enter the cache. All cached content is immutable;
there is no dirty state or writeback.

### Request coalescing

Concurrent loads of one qualified CID share one in-flight operation:

1. The first caller becomes owner and performs the physical read and
   validation.
2. Later callers wait on the same entry.
3. Success is delivered to all live waiters and may populate the cache.
4. Failure is delivered to all waiters but is never cached.
5. The in-flight entry is removed after completion.

Sync execution uses a condition-backed entry. Async execution uses a shared
future. A cancelled or expired waiter leaves without cancelling a load still
needed by other waiters.

### Batching and prefetch

Executors submit ordered, deduplicated CID batches. A store with native batch
support receives one batch. Other async stores use bounded concurrent reads;
sync stores use their available batch operation or sequential fallback.

HNSW batches frontier graph records. PQ batches authoritative finalist reads.
Native async search retains ordered frontier prefetching. Prefetch width and
physical concurrency are runtime policy only and cannot alter logical result
admission or termination.

### Logical and physical accounting

- `committed_bytes` counts validated encoded bytes consumed by execution,
  including cache hits.
- `physical_bytes_read` counts bytes actually fetched from a store.
- `nodes_read` and distance limits count logical work.
- cache hits, misses, coalesced waiters, evictions, and batch sizes are physical
  statistics.

Warm and cold execution must therefore produce the same `SearchPlanSummary`,
neighbors, distances, completion, and logical statistics.

## Failure Semantics

### Explicit selection

Explicit HNSW or PQ selection errors when the accelerator is absent, stale,
incapable, unsupported, malformed, corrupt, or bound to another source.

### Automatic selection

`Auto` may exclude an accelerator during planning when it is absent, stale,
incapable of satisfying the policy, or inadmissible under the explicit budget.
The resulting fallback is recorded in `SearchPlanSummary`.

A CID mismatch, store error, decode error, or invariant violation discovered
after execution starts is returned. It does not trigger native retry.

### Build publication

Accelerator builders write content-addressed descendants before the manifest
and publish the manifest last. Failure may leave unreachable objects, which
normal content GC may reclaim. No partial manifest is returned.

### Cancellation and deadlines

Cancellation and deadlines stop the selected executor at deterministic commit
boundaries and return the appropriate completion state with committed partial
results. They do not switch executors or cancel shared loads required by other
requests.

## Search Proofs

Accelerator search proofs move to a new proof format containing a serialized
`SearchPlanSummary`. Existing accelerator search-proof bytes are rejected.
Membership and structural proximity proofs remain unchanged.

The plan summary commits to:

- planner format version;
- requested and selected backend;
- accelerator manifest CIDs;
- eligibility representation and cardinality state;
- derived HNSW expansion and rerank targets;
- PQ direct-lookup or scan strategy and shortlist size;
- query kernel and logical budgets;
- deterministic fallback decision;
- final completion state.

Proof generation includes the authoritative source closure and exact selected
accelerator closure. Verification installs the objects into an isolated store,
validates the typed closures, and executes the committed plan directly.

Runtime cache state, batching, prefetch, worker count, and physical timing are
excluded from the claim.

The typed content walker understands the current HNSW and PQ codecs while retaining the
existing `HnswManifest`, `HnswPage`, and `ProductQuantization` content kinds.
Unsupported wire shapes fail before closure traversal or replay.

## Later Phase Architecture

Phase two was subsequently authorized by the phase-two child design and is
implemented. Phases three and four remain directional and are not authorized
for implementation by this specification.

### Phase two: composite base-plus-delta accelerators

A `CompositeAcceleratorManifest` bound to the current descriptor references:

- a full base accelerator built for an ancestor descriptor;
- a current-source delta containing inserted and updated vectors;
- a sorted tombstone/shadow set for deleted and updated keys;
- base/current source lineage and configuration fingerprints.

Search executes the committed base plan, excludes shadowed base results, scans
the explicitly bounded full-precision delta, merges by key, and reranks against
the current authoritative directory.

Explicit thresholds on delta cardinality, tombstone ratio, and build work
return `FullRebuildRequired`; `build_or_rebuild` can synchronously construct the
replacement, and callers may schedule that same operation in the background.
Publication changes an accelerator catalog root, never PRXI. Old snapshots
retain their exact closures.

### Phase three: packed disk graph

A new source-bound packed graph manifest references:

- stable key-order numeric ID metadata;
- an ID-to-key table;
- graph pages containing neighbor IDs and compact routing codes;
- a page directory mapping ID ranges to page CIDs;
- codebooks, build metadata, and quality statistics.

Beam search groups frontier IDs by page CID, submits ordered batch reads, uses
compact routing codes for expansion, and reranks finalists from the source
directory. It uses the common cache and search budget.

The logical format depends only on content objects and store APIs. Local stores
may optimize access internally, but the accelerator does not require `mmap`,
`O_DIRECT`, libaio, or a fixed filesystem layout.

### Phase four: routing and query composition

- Add SQ8 routing vectors under the HNSW routing encoding tag.
- Evaluate RaBitQ only after provenance, deterministic encoding,
  cross-platform identity, recall, and build-cost gates pass.
- Add RRF or weighted multi-query fusion in a separate orchestration layer above
  proximity and secondary indexes.
- Add group/diversity retrieval only with explicit expansion budgets and honest
  incomplete-result semantics.
- Treat sparse and alternate element types as separate map or accelerator
  formats.

### Accelerator catalog lifecycle

Phase one receives an explicit runtime `AcceleratorSet`. Phase two adds a
source-bound `AcceleratorCatalog` content object whose entries are sorted by
kind and configuration fingerprint. Catalog publication is independent of
PRXI and uses existing named-root/CAS infrastructure. Planner input is always
one pinned catalog snapshot.

## Testing Strategy

### Unit tests

- HNSW manifest and graph-record round trips;
- rejection of superseded HNSW and PQ bytes;
- configuration fingerprints and source binding;
- deterministic level assignment and entry-point updates;
- candidate-heap, result-heap, and key-tie ordering;
- diversified neighbor selection and reverse-edge pruning;
- `ef_construction` and `ef_search` work bounds;
- planner truth tables for exact, approximate, forced, automatic, known, and
  unknown eligibility;
- non-copying sorted eligibility and source-directory validation;
- deterministic PQ sampling, centroid training, and empty-cluster recovery;
- PQ shortlist bounds and direct eligible-code lookup;
- weighted cache admission, eviction, oversized bypass, and pin accounting;
- sync and async request coalescing success, failure, cancellation, and retry;
- logical versus physical statistics.

### Property tests

- HNSW and PQ manifests are identical across source input permutations;
- one, two, and four build workers produce identical bytes and statistics;
- HNSW degree, level, no-self-edge, sorted-neighbor, and reachability
  invariants hold;
- every persisted graph key exists in the source directory;
- eligible-exact search equals a brute-force oracle for every metric;
- PQ candidate retention never exceeds the planned shortlist;
- warm, cold, sync, and async execution have identical logical results;
- automatic plan selection is independent of runtime configuration.

### Integration and fault tests

- explicit stale or missing sidecars fail;
- automatic absent/stale sidecars fall back according to the recorded plan;
- corruption and store errors during execution do not silently fall back;
- build failures publish no manifest;
- content graph copy, import, export, GC, and cache invalidation include all
  descendants;
- accelerator proof generation, replay, and tamper detection;
- proof replay does not invoke automatic planning;
- cancellation and deadlines preserve committed partial results;
- one uncached CID requested concurrently performs one physical load;
- local durable and async store failures preserve the same logical error
  boundary.

### Conformance fixtures

Commit fixtures for:

- HNSW config, manifest, graph records, root, and build statistics;
- PQ config, deterministic training sample, codebooks, manifest, and root;
- `SearchPlanSummary` for every plan variant;
- accelerator search-proof envelopes;
- explicit unsupported-version errors.

## Performance Evaluation

### Matrix

Measure:

- record counts: 1K, 10K, 100K, and 1M;
- dimensions: 8, 128, 768, and 1,536;
- metrics: L2 squared, cosine, and inner product;
- `k`: 1, 10, and 100;
- eligibility: all, 10%, 1%, and 0.1%;
- cold and warm runtime caches;
- memory, at least one durable local store, and at least one async/batched store
  adapter.

Every row records build time, query latency, recall, completion, distance
evaluations, logical and physical bytes, nodes/pages read, peak owned build
bytes, retained cache bytes, candidate heap peaks, and sidecar size. Raw rows,
repetitions, build metadata, revisions, and machine metadata are retained.

Absolute latency is reported but is not a portable correctness gate. Logical
work, memory bounds, determinism, and recall are merge gates.

A configured resource-limit failure is retained as a measured result rather
than omitted. Default configurations must complete through 100K. The 1M tier
may use explicitly reported larger build limits; an inability to run it is a
documented scalability limit and cannot be presented as a passing row.

### Phase-one acceptance gates

- HNSW and PQ builds are byte-identical across input permutations and worker
  counts.
- HNSW defaults achieve recall@10 of at least 0.95 on every checked-in
  deterministic benchmark fixture.
- PQ defaults achieve recall@10 of at least 0.90 after authoritative reranking.
- HNSW performs at least five times fewer construction distance evaluations
  than the frozen superseded implementation at 10K records.
- Increasing `ef_construction` increases or preserves construction work and
  every tested setting meets the HNSW recall floor.
- On one fixed graph, increasing `ef_search` increases or preserves traversal
  work, explores a candidate superset, and does not reduce recall.
- Selective eligible-exact execution returns exact results and evaluates only
  eligible vectors.
- HNSW fills `k` eligible results whenever at least `k` exist and the request
  budget permits traversal to discover them.
- PQ retained candidates never exceed `k * rerank_multiplier`.
- Builders remain within conservative owned-memory limits or return a resource
  error before manifest publication.
- Warm and cold executions return identical plans, neighbors, distances,
  completion states, and logical statistics.
- Concurrent requests for one uncached CID perform one coalesced physical load.
- Native exact-search behavior, authoritative formats, and non-search proofs
  remain unchanged.
- Accelerator proof replay reproduces the committed plan without replanning.

## Phase-One Implementation Sequence

1. Capture and retain the current HNSW/PQ logical-work and recall baseline.
2. Add v2 configurations, build limits, codecs, errors, and failing conformance
   tests.
3. Replace HNSW construction and search while retaining existing public and
   content-kind names.
4. Add `PreparedEligibility`, `AcceleratorSet`, `SearchPlanner`, `SearchPlan`,
   and unified forced/automatic execution.
5. Replace PQ training, code construction, and search with bounded
   implementations.
6. Add `SearchRuntime`, weighted caches, batching, prefetch integration, and
   sync/async request coalescing.
7. Add plan summaries, v2 accelerator search proofs, and typed content walking.
8. Update documentation, examples, benchmark reports, and maintained bindings.
9. Run formatting, Clippy with warnings denied, the supported Rust toolchain,
   full feature tests, documentation tests, benchmark compilation, binding
   verification, and `git diff --check`.

Later-phase implementation cannot begin until phase one passes the acceptance
gates and measured production-scale results identify the next bottleneck.
