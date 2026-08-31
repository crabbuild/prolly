# Native TurboQuant Proximity Accelerator Design

## Status

Proposed for implementation on 2026-08-30.

This document defines an independent, Prolly-native TurboQuant accelerator for
`ProximityMap`. It is implementation-ready for the initial generally available
(GA) scope and establishes an explicit research gate for the paper's optional
QJL residual correction.

The accelerator is derived, source-bound, immutable, content-addressed, and
disposable. It never replaces the authoritative PRXI descriptor, PRVR ordered
directory, PRXN hierarchy, or full-precision vector reranking.

The design does not integrate, link, copy, or persist Turbovec. The research
basis is the public Google Research paper, not a third-party implementation.

## Decision Summary

The implementation will:

- add `TurboQuantizer` as a new derived accelerator beside HNSW and PQ;
- implement the MSE-oriented TurboQuant algorithm for GA candidate generation;
- normalize each source vector, preserve its deterministic norm, apply a
  frozen structured orthogonal transform, and encode each rotated coordinate
  with a fixed Lloyd-Max scalar codebook;
- store codes in a canonical Prolly tree keyed by the original record key;
- scan quantized codes under the same logical budgets, filtering, cancellation,
  and async rules as PQ;
- resolve every shortlisted key from PRVR and rerank with the authoritative
  full-precision vector before returning it;
- support L2 squared, cosine, and inner product through approximate dot-product
  scoring followed by exact metric-specific reranking;
- preserve sync, async, warm-cache, cold-cache, and cross-language logical
  parity;
- add independent content kinds, catalog entries, plan variants, proof replay,
  content walking, GC, conformance fixtures, bindings, and benchmark rows;
- keep the accelerator out of `Auto` until the release qualification gates in
  this document pass; and
- treat the QJL/product-oriented variant as a separate gated extension because
  replacing its dense Gaussian projection with a structured transform changes
  the paper's unbiasedness proof.

The GA default is four bits per coordinate, a shortlist multiplier of eight,
and a deterministic transform seed of zero. Two- and three-bit codes are
supported when explicitly configured, but do not enter `Auto` until they meet
the same recall gates as the four-bit default.

## Research Basis and Provenance

The design follows the public paper
[TurboQuant: Online Vector Quantization with Near-optimal Distortion Rate](https://arxiv.org/html/2504.19874)
and the
[Google Research overview](https://research.google/blog/turboquant-redefining-ai-efficiency-with-extreme-compression/).

The paper defines two related algorithms:

1. `TurboQuant_mse`: random rotation followed by independent optimal scalar
   quantization of each coordinate.
2. `TurboQuant_prod`: one fewer MSE bit plus a one-bit QJL quantization of the
   reconstruction residual to obtain an unbiased inner-product estimator in
   expectation.

The GA scope implements the first algorithm as a routing accelerator. It uses
a deterministic structured random orthogonal transform rather than the paper's
dense Gaussian-QR rotation. This preserves the algorithm's rotate-then-scalar-
quantize structure and makes CPU, WASM, and store-neutral execution practical,
but it does not claim the paper's exact Haar-rotation theorem. Recall,
distortion, and performance claims therefore come only from Prolly's checked-in
measurements.

No third-party source code may be copied, translated, or treated as the
source-level specification for the production module. The implementation must
derive from this design and the paper. The provenance record must disclose the
earlier Turbovec feasibility review and confirm that no Turbovec code or wire
format entered the implementation. Implementation notes may cite equations,
theorem numbers, and public experimental methodology from the paper. The
source and user-facing documentation must attribute the paper without
suggesting Google endorsement.

The paper is distributed under CC BY 4.0. That license is not a patent grant.
Before a commercial release enables this accelerator by default, the release
owner must record a legal/provenance disposition covering the implementation.
This is a release gate, not a runtime concern.

## Context

The existing proximity engine already provides the difficult system
properties required by a safe quantized accelerator:

- PRVR is the sole authority for keys, vectors, and values;
- accelerator manifests bind an exact PRXI descriptor, dimension, metric,
  record count, configuration, and code root;
- PQ already builds a canonical ordered code tree and performs bounded
  approximate scanning followed by authoritative reranking;
- sync and async executors use deterministic logical work accounting;
- plans are selected without observing store type, timing, CPU features,
  cache warmth, or task completion order;
- replayable proofs commit the selected plan and authenticated closure;
- typed content walking drives replication, copy, import, export, and GC; and
- composite accelerators provide immutable ancestor-base plus current-delta
  operation.

TurboQuant should extend those boundaries instead of introducing a parallel
vector-index lifecycle.

## Goals

1. Provide a training-free, low-bit candidate accelerator for high-dimensional
   proximity search.
2. Preserve exact authoritative final distances, values, and `(score, key)`
   ordering.
3. Produce byte-identical persisted content for the same logical source and
   configuration across input order, worker count, supported architecture, and
   sync/async construction.
4. Support synchronous, async-only, local durable, object-store, and browser
   WASM deployments without a filesystem or native-library dependency.
5. Bound build memory, encoded output, transform work, candidate memory,
   logical reads, and reranking work.
6. Fail closed for corrupt, missing, stale, malformed, or unsupported content.
7. Preserve proof replay, typed closure walking, replication, and GC.
8. Provide maintained-language binding parity before GA.
9. Qualify `Auto` using deterministic structural policy and checked-in
   benchmark evidence, never machine-adaptive planning.
10. Make the implementation small enough to audit by factoring shared
    quantized scan/rerank behavior from PQ.

## Non-goals

The initial GA does not include:

- a Turbovec dependency, compatibility layer, file reader, or file writer;
- mutable slot IDs or incremental in-place sidecar mutation;
- a change to PRXI, PRVR, PRXN, PRXV, PQS8, HNSW, or PQ wire formats;
- replacing exact search or claiming `SearchCompletion::Exact`;
- GPU, CUDA, Metal, BLAS, or platform-specific matrix dependencies;
- a dense `d x d` rotation in the production build or search path;
- entropy coding of centroid indices;
- data-dependent codebook training or calibration;
- automatic latency learning or cache-aware planner decisions;
- variable-dimensional, sparse, or non-`f32` vectors;
- QJL/product-oriented TurboQuant in the initial GA;
- a public accelerator plugin interface; or
- backward decoding of superseded TurboQuant accelerator bytes after a hard
  format cutover.

## Core Invariants

### Authoritative source

The PRXI descriptor and its PRVR directory remain authoritative. A
TurboQuantizer can be deleted without affecting lookup, mutation, verification,
native search, or exact results.

### Exact source binding

The manifest names one exact source descriptor CID, dimension, metric, record
count, configuration fingerprint, and code-tree root. An accelerator cannot be
attached to another source, even when the sources contain equal record counts
or dimensions.

### Authoritative result resolution

Approximate scores are used only to retain a bounded shortlist. Every retained
key is resolved from the current authoritative PRVR directory and scored with
the request's exact deterministic query kernel. Equal exact scores are ordered
by raw byte key.

### Canonical persisted construction

Source traversal order, input order, worker count, scheduling, CPU SIMD
features, cache state, and store implementation cannot alter the manifest,
code values, tree shape, CIDs, quality measurements, or logical build
statistics.

### Deterministic logical execution

For a fixed source, manifest, request, filter, plan, and budget, sync and async
execution return the same plan, neighbors, distances, completion state, and
logical statistics. Physical reads and latency may differ.

### Honest approximation

TurboQuant search returns `ApproximatePolicySatisfied`, `BudgetExhausted`,
`Cancelled`, or `DeadlineExceeded`. It never returns `Exact` and never hides a
fallback after execution begins.

### Fail closed

Missing code entries, extra code entries, invalid code lengths, nonzero padding
bits, invalid norm values, CID mismatches, source mismatches, unsupported
versions, and malformed transforms or codebooks are errors. `Auto` may choose
another backend only before execution when the accelerator is absent, stale,
unsupported, or deterministically inadmissible.

### Replayable selection

A search proof records the exact TurboQuant plan and authenticated closure.
Verification executes that plan directly and does not invoke `Auto` planning.

## Architecture

```text
PRXI descriptor
  |-- authoritative PRVR directory ------------------------------+
  `-- canonical PRXN hierarchy                                   |
                                                                  |
TurboQuant manifest                                               |
  |-- exact source binding                                        |
  |-- frozen transform/codebook configuration                     |
  `-- ordered code tree: source key -> norm + packed codes         |
                           |                                      |
                           v                                      |
                 deterministic quantized scan                     |
                           |                                      |
                           v                                      |
                    bounded candidate heap                        |
                           |                                      |
                           +---------------+----------------------+
                                           |
                                           v
                             authoritative PRVR reranking
                                           |
                                           v
                                  SearchResult `(score,key)`
```

`TurboQuantizer` owns codec, build, validation, and sync search entry points.
The deterministic planner owns selection. A shared internal quantized scan and
rerank layer owns the behavior common to PQ and TurboQuant. `SearchRuntime`
owns physical caching, coalescing, and async scheduling.

## Public API

Public component names remain unversioned, following the proximity accelerator
hard-cutover policy.

```rust,ignore
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurboQuantizationConfig {
    pub bit_width: u8,
    pub rerank_multiplier: u32,
    pub seed: u64,
}

impl Default for TurboQuantizationConfig {
    fn default() -> Self {
        Self {
            bit_width: 4,
            rerank_multiplier: 8,
            seed: 0,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TurboQuantizationBuildLimits {
    pub max_records: Option<usize>,
    pub max_input_bytes: Option<usize>,
    pub max_temporary_bytes: Option<usize>,
    pub max_transform_operations: Option<usize>,
    pub max_encoded_output_bytes: Option<usize>,
    pub max_worker_threads: Option<usize>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TurboQuantizationBuildStats {
    pub encoded_vectors: usize,
    pub zero_vectors: usize,
    pub transformed_components: usize,
    pub butterfly_operations: usize,
    pub input_bytes: usize,
    pub encoded_output_bytes: usize,
    pub peak_temporary_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TurboQuantizationQuality {
    pub mean_squared_error: f64,
    pub maximum_squared_error: f64,
}

pub struct TurboQuantizer<S: Store> { /* private fields */ }
pub struct AsyncTurboQuantizer { /* private fields */ }
```

Required entry points mirror PQ:

```rust,ignore
TurboQuantizer::build
TurboQuantizer::build_with_limits
TurboQuantizer::load
TurboQuantizer::verify
TurboQuantizer::search
TurboQuantizer::manifest_cid
TurboQuantizer::source_descriptor
TurboQuantizer::config
TurboQuantizer::quality

AsyncTurboQuantizer::build
AsyncTurboQuantizer::load
AsyncTurboQuantizer::verify
AsyncTurboQuantizer::search
```

`SearchBackend` gains `TurboQuantized`. `SearchOptions` gains:

```rust,ignore
pub struct TurboQuantSearchOptions {
    pub rerank_multiplier: Option<u16>,
}
```

`ApproximatePreference` gains `TurboQuantizedFirst`. Existing preferences use
these fixed orders when all three sidecars exist:

- `HnswFirst`: HNSW, TurboQuant, PQ;
- `ProductQuantizedFirst`: PQ, TurboQuant, HNSW;
- `TurboQuantizedFirst`: TurboQuant, HNSW, PQ.

Numeric wire and binding discriminators are appended. Existing IDs are never
renumbered.

The initial appended assignments are normative:

| Surface | Existing maximum | TurboQuant value |
| --- | ---: | ---: |
| `ContentObjectKind` | 12 | 13 |
| `CatalogAcceleratorKind` | 3 | 4 |
| `CompositeBaseKind` | 2 | 3 |
| proof request backend | 4 | 5 |
| serialized `SearchPlan` variant | 4 | 5 |
| portable binding `SearchBackend` | 5 | 6 |
| `ApproximatePreference` | 1 | 2 |

Catalog and composite records without TurboQuant entries retain their existing
bytes and version. The appended kind is an additive discriminator. The shared
search-plan/proof schema does change because it commits TurboQuant request
options, so its version is incremented and older search proofs are rejected.

## Supported Configuration

The first format supports:

- bit widths `2`, `3`, and `4`;
- dimensions in `8..=16_384` that are divisible by eight;
- every current `DistanceMetric`;
- every current filter form;
- all current sync and async stores; and
- WASM scalar build and search.

An unsupported dimension rejects an explicitly requested build with a typed
configuration error. It does not restrict the underlying `ProximityMap`.
`Auto` treats the accelerator as unavailable for unsupported dimensions.

An empty source has no accelerator work. Direct build returns a typed invalid-
configuration error, while composite `build_or_rebuild` preserves the existing
`NoAcceleratorRequired` disposition.

Only four-bit indexes with dimensions at least 128 are eligible for the first
`Auto` qualification. Forced two- and three-bit search remains supported and
honestly approximate.

## Algorithm

### Source preparation

Construction visits PRVR records in byte-key order. The persisted vector has
already passed ProximityMap's dimension and finite-value validation.

For each vector `x`:

1. Compute `norm_squared = sum(f64(x_i) * f64(x_i))` in ascending coordinate
   order.
2. Compute `norm` using Prolly's deterministic software square root.
3. For nonzero vectors, form `u_i = f64(x_i) / norm` in ascending coordinate
   order.
4. For zero L2 or inner-product vectors, emit the canonical zero code described
   below. Cosine vectors are never zero because authoritative ingestion already
   rejects them.
5. Apply `StructuredRotation` to `u`.
6. Scale rotated coordinates by deterministic `sqrt(dimensions)` so the fixed
   codebook operates in approximately standard-normal space.
7. Quantize each scaled coordinate using the fixed threshold table.
8. Persist `norm` and the packed centroid indices.

The stored norm is the deterministic `f64` norm, not a platform `sqrt` result
and not an `f32` approximation.

### Structured rotation

The production transform is frozen as `STRUCTURED_ROTATION_ID = 1` and has two
rounds. Each round performs, in this exact order:

1. Derive a global Fisher-Yates permutation from `(seed, dimensions, round)`
   using the frozen `SplitMix64V1` `u64` stream. Map each draw into the current
   Fisher-Yates bound with the high 64 bits of a `u64 * u64` product. This uses
   exactly one draw per position and has fixed work; its negligible mapping
   bias is part of the engineering transform rather than a Haar-randomness
   claim.
2. Permute all coordinates into a separate work buffer.
3. Derive one sign bit per coordinate from a distinct domain-separated stream
   and apply `+1` or `-1`.
4. Partition the vector into blocks whose width is the largest power-of-two
   divisor of `dimensions`; the supported-dimension rule guarantees a minimum
   width of eight.
5. Apply an in-place Walsh-Hadamard butterfly in increasing stage, block, and
   element order.
6. Multiply every component by the frozen deterministic reciprocal square root
   of the block width.

`SplitMix64V1` is fixed as:

```text
state = state + 0x9e3779b97f4a7c15 (wrapping u64)
z = state
z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9 (wrapping u64)
z = (z ^ (z >> 27)) * 0x94d049bb133111eb (wrapping u64)
output = z ^ (z >> 31)
```

Round streams start from:

```text
permutation_state = seed ^ 0x5451_5045_524d_0001
                         ^ (u64(dimensions) << 16) ^ u64(round)
sign_state        = seed ^ 0x5451_5349_474e_0001
                         ^ (u64(dimensions) << 16) ^ u64(round)
```

The permutation starts as `[0, 1, ..., dimensions - 1]`. For `i` descending
from `dimensions - 1` through `1`, it consumes one permutation draw and sets:

```text
j = high_u64(u128(draw) * u128(i + 1))
swap(permutation[i], permutation[j])
```

The sign stream consumes one draw per coordinate in ascending order; output bit
zero selects `+1`, and bit one selects `-1`.

The exact PRNG transition, domain constants, bounded-integer multiply-high
rule, permutation direction, sign-bit ordering, butterfly ordering, and
normalization bits are wire-format behavior. They require conformance fixtures.

The transform plan contains only permutations, signs, block width, and frozen
normalization constants. It is derived from the manifest configuration and is
cached by `(transform_id, dimensions, seed)`. It is not persisted as a mutable
runtime artifact and cannot affect the manifest CID.

Construction uses canonical scalar `f64` operations. An optimized build may
parallelize records, but it must produce the same per-record bytes and select
the same first canonical error as serial construction.

### Lloyd-Max codebooks

`NORMAL_LLOYD_MAX_CODEBOOK_ID = 1` defines exact threshold and reconstruction
tables for two, three, and four bits. Tables are embedded as `f64::from_bits`
constants, never recomputed at runtime.

The table-generation utility is development-only. It records:

- the numerical method and iteration count;
- source precision;
- generated threshold and centroid bit patterns;
- mean squared error against the standard normal density; and
- the paper citation.

The generated table module is checked in. Regeneration must reproduce every
bit or fail. Changing any threshold or centroid is a hard accelerator-format
cutover.

Encoding compares a coordinate with thresholds in ascending order. Equality
selects the lower code. Search uses the corresponding reconstruction centroid.

### Canonical bit packing

Code index `i` occupies bit positions `[i * b, i * b + b)` in a logical
little-endian bit stream. The least significant bit of a code is written first,
and bit zero of each byte is the least significant bit. Codes may cross byte
boundaries. Unused high bits in the final byte must be zero.

The packed length is exactly:

```text
ceil(dimensions * bit_width / 8)
```

The decoder rejects a different length, an index outside the configured
codebook, or nonzero padding bits.

### Canonical zero vector

A zero vector is encoded with `norm == +0.0` and every packed code bit zero.
Negative zero is rejected. A decoder rejects `norm == 0` with nonzero code
bits. Its approximate dot product is exactly positive zero.

### Approximate dot product

The query is prepared through the existing metric-specific input validation.
The transform is applied to the query once per search. Search precomputes
`weighted_query_i = transformed_query_i / sqrt(dimensions)` in coordinate
order. It also computes L2 `query_norm_squared` from the prepared query with the
canonical scalar accumulation.

For one encoded vector:

```text
approx_dot = stored_norm *
             sum_i(transformed_query_i * reconstructed_centroid[code_i]
                   / sqrt(dimensions))
```

Products are generated in coordinate order and reduced in canonical scalar
`f64` order. SIMD kernels may fill independent product lanes, but the final
reduction must remain scalar and ordered, matching the existing deterministic
SIMD strategy.

Metric scores are:

```text
L2Squared   = query_norm_squared + stored_norm^2 - 2 * approx_dot
Cosine      = 1 - clamp(approx_dot, -1, 1)
InnerProduct = -approx_dot
```

Small negative L2 estimates caused by approximation or rounding are clamped to
positive zero for candidate ordering. This clamp affects routing only.

Approximate candidates are totally ordered by `(approximate_score, key)`.

### Exact reranking

The shortlist target is:

```text
min(eligible_count, max(k, k * rerank_multiplier))
```

Every candidate key is resolved from the exact source directory. Missing keys
are corruption. The exact source vector is scored with the request's selected
deterministic query kernel. Candidates are sorted by `(exact_score, key)` and
truncated to `k`.

The returned value always comes from the current authoritative record, so a
value-only update does not require a new quantized code in a composite.

### Persisted quality measurements

For a nonzero vector, reconstruction MSE is measured in rotated unit-vector
space:

```text
sum_i((rotated_unit_i - reconstructed_centroid[code_i]
       / sqrt(dimensions))^2)
```

This is equivalent to source-space reconstruction error only to the numerical
orthogonality of the structured transform, so documentation calls it the
persisted routing reconstruction error. A canonical zero vector contributes
zero. Mean MSE divides the key-ordered sum across all source records by source
count; maximum MSE is the largest record value. Parallel workers return
per-record MSE values, which are reduced in source-key order.

## Persisted Format

### Content kinds

Append, without renumbering existing values:

```text
ContentObjectKind::TurboQuantization = 13
```

TurboQuant manifests use this kind. The referenced code tree uses the frozen
ordered-node codec under TurboQuant decode context, following PQ's current
typed-walker pattern.

### Manifest

The manifest begins with `TQTQ` and format version `1`.

Canonical field order:

| Field | Encoding | Validation |
| --- | --- | --- |
| magic | four bytes `TQTQ` | exact |
| format version | `u8` | current only |
| flags | `u8` | zero |
| source descriptor | CID | must authenticate PRXI |
| dimensions | canonical varint | supported range and multiple of eight |
| metric | existing metric ID | known value |
| count | canonical varint | positive and equal to code-tree count |
| bit width | `u8` | 2, 3, or 4 |
| rerank multiplier | canonical varint | positive and bounded by `u32` |
| seed | little-endian `u64` | any value |
| transform ID | `u8` | `1` |
| codebook ID | `u8` | `1` |
| code root | CID | required and authenticated |
| mean MSE | canonical finite `f64` | nonnegative |
| maximum MSE | canonical finite `f64` | nonnegative and >= mean |
| zero-vector count | canonical varint | <= count |
| config fingerprint | CID | exact recomputation |

The manifest does not persist runtime cache data, worker count, timestamps,
host information, wall time, or platform feature flags.

### Code-tree values

Each ordered code-tree value is:

```text
[8-byte little-endian finite nonnegative f64 norm]
[exact packed centroid-index bytes]
```

There is no slot ID, per-record version, timestamp, checksum, or duplicated
key. The Prolly tree key is the source key. Content addressing supplies object
integrity.

The tree configuration is a frozen `turboquant_code_tree_config()` and must not
inherit future ordered-tree defaults. It initially matches PQ's bounded raw
code-tree shape, but is a separate function and wire contract.

### Configuration fingerprint

The fingerprint hashes only canonical configuration bytes:

```text
bit_width, rerank_multiplier, seed, transform_id, codebook_id
```

Dimension and metric remain explicit manifest fields and exact source-binding
checks; they are not hidden inside the configuration fingerprint.

### Publication

Builders write code-tree descendants first and the manifest last. A failed
build may leave unreachable content but cannot return or publish an incomplete
manifest. Async provider publication uses existing bounded staging and ordered
publication behavior.

### Loading and verification

`load` authenticates the manifest CID, decodes all bounded fields, validates
configuration, and opens the code tree. `verify` additionally walks the entire
code tree and proves:

- code count equals manifest and source count;
- every code key exists exactly once in PRVR;
- every source key has exactly one code;
- norms and packed bytes are canonical;
- the source descriptor, dimension, metric, and count match;
- tree bytes and CIDs authenticate; and
- recomputed quality measurements equal the persisted quality bits.

Normal search does not perform the expensive full-source cardinality audit;
typed catalog loading and closure validation provide the established trusted
open boundary.

## Deterministic Construction

Construction is a deterministic preflight followed by one streaming source
pass:

1. Validate limits and derive the transform plan and codebook.
2. Traverse source records in key order, encode records in bounded batches,
   restore strict key order, stream them into `SortedBatchBuilder`, and collect
   deterministic quality/build statistics.

No training sample or corpus-wide calibration is required.

`input_bytes` counts source vector component bytes (`count * dimensions * 4`),
not keys, application values, tree nodes, or provider protocol overhead.

For block width `B` and two rounds, one nonzero encoded record consumes exactly

```text
dimensions * (2 * (3 + log2(B)) + 1)
```

logical transform operations: one permutation copy, one sign multiplication,
`log2(B)` Hadamard add/subtract operations, and one normalization multiplication
per coordinate per round, plus one standard-normal scaling multiplication.
`butterfly_operations` is the `2 * dimensions * log2(B)` subset.

`max_temporary_bytes` includes the derived transform plan, every worker's two
`dimensions * 8` transform buffers, bounded key/code batch output, ordered
reassembly state, and statistics. The builder computes a conservative checked
upper bound before starting workers and tracks the actual peak at or below it.

Parallel construction assigns contiguous key-ordered batches to workers.
Results are committed only in batch sequence order. If several records fail,
the lexicographically earliest source key's canonical error is returned,
independent of worker completion order.

Build limits are preflighted where exact sizes are known. For a source count
`n`, dimensions `d`, and bit width `b`, encoded value bytes are exactly:

```text
n * (8 + ceil(d * b / 8))
```

Checked arithmetic is mandatory. Overflow is a resource error, never wrapping
allocation arithmetic.

`max_transform_operations` counts the documented logical permutation, sign,
butterfly, and normalization operations. It is independent of SIMD width and
worker count.

## Search Planning and Execution

### Search plan

Add:

```rust,ignore
SearchPlan::TurboQuantized {
    rerank_target: usize,
    direct_lookup: bool,
}
```

The plan summary records the new backend plus these fields. The search-plan
format version increments from `3` to `4`. Existing plan variant IDs are
retained and the new variant is appended.

### Eligibility planning

Planner behavior mirrors PQ:

- a small known sorted eligible-key set may use direct code lookup;
- otherwise the executor scans the code tree and applies the prepared filter;
- eligibility cardinality caps the shortlist target;
- exact policy never selects TurboQuant;
- an explicit TurboQuant backend fails clearly when unavailable or stale; and
- `Auto` considers TurboQuant only after the qualification phase enables it.

### Budgets

One admitted code consumes:

- one logical node/read unit under the existing PQ-compatible accounting;
- the exact encoded value length in committed logical bytes; and
- one `quantized_distance_evaluation`.

The candidate heap never exceeds `rerank_target`. Retained candidate bytes,
frontier peak, reranked candidates, authoritative bytes, and exact distance
evaluations use existing `ProximitySearchStats` fields.

Before reading or admitting a record, the executor checks node, committed-byte,
distance-evaluation, and frontier budgets. It returns the committed partial
result with `BudgetExhausted` when the next unit would exceed a limit.

### Cancellation and deadlines

Sync search observes only logical budgets, as today. Async search checks
cancellation and deadline state before every code-tree operation and every
authoritative rerank lookup. It commits candidates in deterministic source-key
scan order, regardless of physical completion order.

### Runtime caching

`SearchRuntimePolicy` gains `turboquant_max_bytes`. Runtime partition IDs are
appended without changing existing IDs. The runtime caches authenticated
manifest/code-tree content and derived transform plans.

Derived transform plans are weighted by their owned permutation, sign, and
scratch-plan bytes. Cache admission or eviction affects physical work only.
The logical plan and search counters remain unchanged.

## Accelerator Set, Catalog, and Composite Integration

`AcceleratorSet` and `AsyncAcceleratorSet` gain at most one direct
TurboQuantizer. The same-source validation applied to HNSW and PQ applies to
TurboQuant.

Append `CatalogAcceleratorKind::TurboQuantized`. Catalog entries remain sorted
and unique by `(kind, configuration fingerprint, root CID)`. Existing catalog
kind IDs are not renumbered.

Append `CompositeBaseKind::TurboQuantized` and
`CompositeBase::TurboQuantized(TurboQuantizer<S>)`. Composite construction,
shadowing, exact delta scanning, rebuild thresholds, and one-generation depth
do not change.

When a TurboQuant-based composite requires a full rebuild, it rebuilds a
current-source TurboQuantizer using the base configuration and current build
limits. Its nested plan is a complete `SearchPlan::TurboQuantized` plan.

Acceptance requires direct and composite TurboQuant searches to share the same
candidate-scoring implementation; they may not fork separate math.

## Proofs and Content Graph

### Search proofs

The accelerator search-proof format is incremented. Existing backend IDs are
retained; TurboQuant is appended.

A TurboQuant proof authenticates:

- the trusted PRXI descriptor and required authoritative PRVR closure;
- the accelerator manifest;
- the exact code-tree closure used by execution;
- the committed request, filters, budgets, kernel, and TurboQuant options;
- the exact `SearchPlan::TurboQuantized` summary;
- candidate admission and authoritative rerank events; and
- the completion state.

Verification reconstructs the accelerator in an isolated store and replays the
committed plan. It does not call the automatic planner and does not consult an
external codebook, transform implementation, or third-party library beyond the
current frozen Prolly codec.

### Typed walking

The typed content walker recognizes the TurboQuant manifest, validates its
source and code root, and walks the code tree with the correct raw-value
context. Copy, import, export, replication, named-root validation, and GC must
include the complete closure and preserve sharing.

### Corruption behavior

The following proof or graph mutations must fail:

- changing source CID, metric, dimension, count, seed, transform ID, codebook
  ID, bit width, quality bits, code root, or fingerprint;
- removing, adding, duplicating, reordering, or changing a code entry;
- changing a norm, code bit, padding bit, tree child, or CID;
- altering plan fields, shortlist target, filter, budget, or completion; and
- replaying against another trusted source descriptor.

## Error Model

Use existing proximity error families where they are semantically correct:

- `InvalidProximityConfig` for unsupported build configuration;
- `InvalidProximityObject` for malformed authenticated content;
- `UnsupportedProximityVersion` for a format cutover;
- `ProximityResourceLimitExceeded` for deterministic build limits;
- `InvalidProximitySearch` for an unavailable forced backend or incompatible
  request;
- `CidMismatch` and `NotFound` for authenticated storage failures; and
- existing cancellation/deadline completion states for cooperative stops.

Error strings must name `TurboQuant` and the failed field or resource. They
must not expose platform-dependent debug output or raw third-party errors.

## Security and Robustness

- All size arithmetic uses checked operations before allocation.
- Decoders bound dimensions, counts, varints, code lengths, and tree fanout.
- No unsafe code is required for the scalar implementation.
- SIMD enters only after scalar conformance and receives focused memory-safety
  and tail-length tests.
- Untrusted manifests cannot allocate transform plans until dimension and
  configuration bounds pass.
- Seeded permutations use one fixed multiply-high mapping per Fisher-Yates
  position and contain no rejection loop.
- Search never trusts an encoded key or value without tree and manifest
  validation.
- Proof and content-graph traversal retain existing object, depth, byte, and
  fanout limits.
- Fuzz targets cover manifest decoding, bit unpacking, transform derivation,
  code scoring, and proof replay.

## Binding and Documentation Scope

GA includes public parity for Rust, UniFFI, Python, Go, Node/TypeScript,
Kotlin, Java, Ruby, Swift, and browser WASM.

Each maintained binding exposes:

- TurboQuant configuration, build limits, build statistics, and quality;
- synchronous or idiomatic asynchronous build/load/verify operations matching
  that binding's existing PQ surface;
- `TurboQuantized` search backend and search options;
- accelerator-set and catalog attachment;
- TurboQuant composite-base construction where composites are already exposed;
- cancellation and deadline behavior; and
- a deterministic RAG/vector-sidecar cookbook example or extension of the
  existing example.

`bindings/api/parity.json` and `classification-audit.json` must have no planned
or missing TurboQuant production cells at GA. Generated provenance files are
regenerated through the documented binding workflow.

`docs/proximity-map.md`, crate exports, rustdoc examples, binding cookbooks,
benchmark documentation, conformance documentation, and the completion audit
must describe the accelerator and its limitations.

## Testing Strategy

### Algorithm unit tests

- frozen SplitMix64 output and domain separation;
- bounded-integer sampling fixtures;
- permutation bijection and sign-plan fixtures;
- structured rotation golden outputs at dimensions 8, 24, 128, 200, 768,
  1536, and 3072;
- norm preservation within a frozen deterministic tolerance;
- fixed Lloyd-Max threshold and centroid bit patterns;
- threshold equality selects the lower code;
- 2/3/4-bit packing round trips at every alignment and tail length;
- nonzero padding rejection;
- zero-vector canonical encoding;
- scalar approximate score fixtures for all metrics;
- L2 estimate nonnegative clamp; and
- scalar versus SIMD score bit identity.

### Codec and validation tests

- config, fingerprint, manifest, code value, root, and quality round trips;
- unsupported version and unknown flag rejection;
- every malformed field and impossible length rejected before allocation;
- missing, extra, duplicate, or malformed code entries fail verification;
- source, dimension, metric, and count mismatch rejection;
- clean rebuild produces the same manifest CID; and
- manifest publication occurs last.

### Determinism property tests

- source input permutations produce identical bytes and statistics;
- one, two, and four workers produce identical bytes, first errors, quality,
  and statistics;
- sync and async builders produce identical manifests and descendants;
- x86_64 Linux, aarch64 macOS, and wasm32 fixtures encode identically;
- repeated builds after unrelated store/cache activity are identical;
- warm and cold search return identical logical outcomes; and
- scalar, deterministic SIMD, and auto kernels return identical exact final
  distances and order.

### Search correctness tests

- exhaustive shortlist (`rerank_target == eligible_count`) equals the
  brute-force authoritative oracle for all metrics;
- every returned key belongs to the filter;
- result values are current authoritative values;
- ties use byte-key order;
- candidate heap never exceeds plan target;
- direct lookup and full scan agree for the same eligible set;
- budget exhaustion preserves the exact committed prefix of work;
- cancellation/deadline checks preserve deterministic partial results;
- explicit unavailable/stale accelerator fails;
- automatic unavailable/stale accelerator falls back only during planning; and
- corruption after execution starts never falls back.

### Composite tests

- insert, vector update, delete, and value-only update behavior;
- shadowed base keys cannot escape;
- exact delta results merge correctly with approximate base results;
- rebuild disposition preserves TurboQuant configuration;
- one-generation depth is enforced; and
- direct versus composite exhaustive results match the current-source oracle.

### Proof and graph tests

- proof generation/replay for all metrics, filters, budgets, and completion
  states;
- replay never invokes `Auto` planning;
- every listed tamper case fails;
- typed copy into a fresh store reopens and searches identically;
- interrupted copy never publishes a root;
- GC preserves shared live transforms/code trees and reclaims unreachable
  TurboQuant closures; and
- named catalog CAS atomically replaces complete closures.

### Fuzz and adversarial tests

- arbitrary manifest bytes;
- arbitrary code lengths and bit patterns;
- maximum supported dimension and count arithmetic;
- very long keys and selective filters;
- all-equal vectors and all-equal approximate scores;
- extreme finite `f32` magnitudes for L2 and inner product;
- zero/nonzero mixtures;
- cancellation at every logical operation boundary; and
- injected store failures at every manifest, tree, proof, and rerank read.

## Benchmark and Qualification Matrix

Measure record counts `1K`, `10K`, `100K`, and `1M`; dimensions `128`, `200`,
`768`, `1536`, and `3072`; all metrics; `k` values `1`, `10`, and `100`;
eligibility `all`, `10%`, `1%`, and `0.1%`; bit widths `2`, `3`, and `4`; and
rerank multipliers `4`, `8`, `16`, and exhaustive where feasible.

Run warm and cold memory stores, one durable local store, one async/batched
store, and WASM smoke coverage. Every row records:

- source, code-tree, and manifest bytes;
- build wall time and logical transform operations;
- peak owned build bytes;
- query median, p95, and p99 latency;
- recall@1, recall@10, and recall@100 against brute force;
- quantized and exact distance evaluations;
- logical and physical bytes/pages read;
- candidate/frontier peak;
- reranked candidates;
- completion state; and
- build revision, compiler, target, machine, store, seed, and repetitions.

Raw rows are retained under `performance-results/proximity-turboquant/`.
Absolute latency is evidence, not a portable correctness claim.

## GA Acceptance Gates

Every item is mandatory unless explicitly scoped to `Auto` qualification.

### Correctness and determinism

- All algorithm, codec, property, search, composite, proof, graph, fuzz-smoke,
  and binding tests pass.
- Builds are byte-identical across input permutation, 1/2/4 workers, sync and
  async construction, x86_64 Linux, aarch64 macOS, and wasm32 fixture runs.
- Exhaustive reranking equals the brute-force oracle for every metric and
  checked dimension.
- Warm/cold and sync/async searches have identical plans, neighbors, exact
  distances, completion states, and logical statistics.
- No existing authoritative descriptor, ordered node, proximity node, external
  vector, SQ8, PQ, or HNSW fixture changes.
- Existing exact/native search results and logical statistics remain identical.
- Membership and structural proof bytes remain unchanged. Search proofs use the
  documented hard-cut plan/request version and are regenerated for every
  backend, including native search.

### Resource behavior

- Code values use exactly `8 + ceil(dimensions * bit_width / 8)` bytes.
- At dimensions at least 128, four-bit encoded values consume at most 15% of
  raw `f32` vector bytes, excluding shared ordered-tree structure.
- Candidate retention never exceeds `rerank_target`.
- Every build limit either holds or returns before manifest publication.
- Default build limits complete the 100K matrix; the 1M matrix either completes
  within explicitly reported limits or records a typed scalability failure.
- Async cancellation is observed within one code-tree operation or one
  authoritative rerank operation; no unbounded CPU loop exists between checks.

### Recall

- The default four-bit configuration achieves recall@10 >= 0.95 on every
  checked-in deterministic dataset/metric fixture after authoritative rerank.
- It is no more than 0.01 below the current default PQ recall@10 on any matrix
  row with the same shortlist size.
- No aggregate average may hide a failing dataset or metric.
- Two- and three-bit modes publish their measured recall but are not required
  to meet the GA default floor unless proposed for `Auto`.

### Performance and value

- TurboQuant construction performs no corpus training and records exactly one
  encoding transform per nonzero source vector.

The following comparative items are `Auto` qualification gates. They do not
block an otherwise accepted forced-backend GA:

- At 100K records for dimensions 768 and 1536, default four-bit build wall time
  is lower than default PQ build wall time on the same pinned benchmark host.
- At equal recall >= 0.95, warm quantized scan plus rerank p95 is no worse than
  1.25x PQ p95 for dimensions 768 and 1536.
- At least one of the following is true for every `Auto`-eligible benchmark
  family: TurboQuant is faster at equal recall, uses at least 25% fewer sidecar
  bytes at equal recall, or provides at least 0.02 higher recall at no more
  than 1.25x latency.

### Integration and release

- Explicit, automatic, catalog, composite, sync, async-only, proof, copy, and
  GC paths pass.
- The public API inventory has implemented and tested entries for all maintained
  bindings, with documented WASM parity.
- Documentation states that the structured rotation is an engineering variant
  and does not claim the paper's exact dense-rotation theorem.
- Provenance attribution and the required legal/patent disposition are recorded.
- Formatting, Clippy with warnings denied, MSRV 1.89 checks, all-feature tests,
  doc tests, benchmark compilation, binding verification, and
  `git diff --check` pass.

## Auto Qualification Rule

GA availability and `Auto` eligibility are separate switches.

The backend may be released for forced use after all non-`Auto` GA gates pass.
It enters `Auto` only after the comparative performance/value gates also pass
for the default four-bit configuration.

The planner rule is structural and frozen. It may consider:

- accelerator presence and exact source binding;
- supported dimensions and bit width;
- source count;
- eligibility cardinality;
- request policy and explicit budgets; and
- configured approximate preference.

It may not consider observed latency, machine type, CPU features, store type,
cache state, queue depth, or prior query outcomes.

If the benchmark gates do not pass, the backend remains explicit-only. That is
a valid implementation outcome, not permission to weaken correctness or recall
criteria.

## Phased Delivery Plan

No phase starts until the previous phase's acceptance criteria are satisfied
and its evidence is committed or linked from the implementation pull request.

### Phase 0: provenance, baseline, and contract lock

Deliverables:

- approve this design and record the independent-implementation/provenance
  owner;
- record the paper revision and citations;
- record the legal/patent release disposition owner;
- freeze PQ/HNSW/native benchmark baselines and deterministic datasets;
- add failing API/codec/conformance test skeletons;
- assign appended wire IDs without renumbering existing IDs; and
- write an implementation checklist mapping every acceptance gate to a test,
  benchmark row, or release artifact.

Acceptance:

- no unresolved GA-scope or wire-format decision remains;
- baseline commands reproduce on the pinned benchmark host;
- existing conformance fixtures and benchmark CSVs are archived;
- the provenance record discloses prior evaluations and confirms that the
  implementation copies no third-party TurboQuant code or wire format; and
- every new public or persisted field has an owner and test location.

### Phase 1: deterministic math and test oracle

Deliverables:

- canonical norm helpers and transform-plan derivation;
- structured rotation scalar implementation;
- frozen codebook tables and provenance generator;
- 2/3/4-bit pack/unpack implementation;
- approximate metric scoring;
- development-only dense paper-reference implementation for distortion and
  recall comparison; and
- algorithm golden fixtures and property tests.

Acceptance:

- every algorithm unit test listed above passes;
- transform and packing fixtures match on x86_64, aarch64, and wasm32;
- structured rotation norm error stays within the frozen per-dimension bound;
- scalar scoring contains no unsafe code and passes Miri-compatible tests;
- dense reference and structured implementation report comparison data for all
  benchmark dimensions; and
- no persistence or public backend selection is enabled yet.

### Phase 2: canonical persisted sidecar and synchronous search

Deliverables:

- `TurboQuantizationConfig`, limits, stats, quality, and `TurboQuantizer`;
- manifest and code-value codecs;
- bounded serial and parallel construction;
- load and whole-sidecar verification;
- sync code scan, filter/direct lookup, heap retention, and exact reranking;
- source binding in `AcceleratorSet`; and
- focused codec, corruption, determinism, budget, and brute-force tests.

Acceptance:

- manifests and descendants are byte-identical across input order and 1/2/4
  workers;
- build failures never return or publish a manifest;
- exhaustive rerank equals brute force for all metrics;
- all forced-backend filter and budget tests pass;
- full verification detects every missing, extra, or malformed code;
- candidate and build-memory bounds are demonstrated; and
- the new backend remains unavailable to `Auto`.

### Phase 3: planner, async runtime, catalog, and composite

Deliverables:

- `SearchBackend`, options, preference, plan, summary, and planner integration;
- async build/load/verify/search;
- runtime cache partition and transform-plan caching;
- direct async accelerator-set and catalog lifecycle;
- TurboQuant catalog kind;
- TurboQuant composite base, nested plan, and rebuild path; and
- cancellation, deadline, coalescing, async-only store, and composite tests.

Acceptance:

- sync/async and warm/cold logical parity passes;
- cancellation/deadline latency is bounded by one documented logical unit;
- catalog sort/source/config validation passes;
- composite mutation cases pass against the current-source oracle;
- corruption never triggers execution fallback;
- runtime byte caps include derived transform allocations; and
- automatic planning still excludes TurboQuant pending qualification.

### Phase 4: proofs, typed content lifecycle, and adversarial hardening

Deliverables:

- proof-format and plan-format increments with appended discriminators;
- TurboQuant proof generation and replay;
- typed content walking, copy, replication, import/export, GC, and named-root
  validation;
- fuzz targets and deterministic fault injection;
- complete conformance fixtures; and
- completion-audit evidence rows.

Acceptance:

- proof replay never replans and matches direct execution;
- every enumerated tamper mutation fails;
- fresh-store copy/reopen/search is identical;
- GC preserves live shared closure and reclaims unreachable content;
- fault injection at every read/write boundary fails closed;
- fuzz smoke completes with no panic, hang, or unbounded allocation; and
- old accelerator proof bytes fail with the explicit supported-version error.

### Phase 5: SIMD, scale qualification, and `Auto`

Deliverables:

- AVX2/AVX-512 where supported, aarch64 NEON, and WASM SIMD scoring kernels;
- scalar fallback for every target;
- complete benchmark matrix and raw retained results;
- tuned but frozen GA defaults;
- planner `Auto` enablement only if qualification passes; and
- performance regression thresholds in CI where stable logical counters can be
  enforced.

Acceptance:

- every SIMD kernel is bit-identical to scalar for adversarial lengths and
  values;
- all GA correctness, resource, and recall gates pass;
- comparative performance/value gates pass before `Auto` is enabled;
- defaults are frozen before conformance fixture generation;
- `Auto` truth tables are deterministic and fixture-backed; and
- if performance qualification fails, explicit-only delivery is documented and
  `Auto` remains disabled.

### Phase 6: maintained bindings, documentation, and release

Deliverables:

- all maintained language bindings and generated provenance;
- binding parity and platform tests;
- Rust and language cookbook examples;
- proximity-map, benchmark, conformance, migration, and operational docs;
- changelog and release notes;
- provenance/legal release record; and
- final completion-audit matrix.

Acceptance:

- binding API inventory contains no missing TurboQuant production cell;
- every supported binding builds, loads, forces, searches, cancels, and reports
  TurboQuant statistics through its idiomatic API;
- browser WASM matches Rust golden fixtures and brute-force outcomes;
- all repository and binding release commands pass;
- documentation makes approximation, dimension, and `Auto` limits explicit;
- no unrelated worktree content is modified; and
- the completion audit links every goal to implementation, tests, benchmarks,
  and release evidence.

### Phase 7: optional QJL/product-oriented extension

This phase is not required for MSE TurboQuant GA. It requires a focused child
design before implementation.

Entry criteria:

- MSE TurboQuant GA evidence is complete;
- measured inner-product/cosine recall identifies residual-estimator bias as a
  material bottleneck;
- a deterministic portable projection design exists; and
- its relationship to the paper's dense Gaussian QJL guarantee is stated
  precisely.

Minimum acceptance for a product-oriented mode:

- separate objective/projection wire IDs and conformance fixtures;
- deterministic packed residual signs and residual norm;
- empirical estimator mean falls within a preregistered 99% confidence interval
  around exact inner product on deterministic test distributions;
- variance and recall are compared with the dense paper-reference oracle;
- recall improves by at least 0.02 at the same total bit budget and shortlist,
  or reduces shortlist work by at least 25% at the same recall;
- build/search resource limits, proofs, content walking, bindings, and WASM
  parity pass; and
- documentation does not claim the paper's unbiasedness theorem unless the
  implemented projection meets its assumptions.

## Implementation File Map

Expected core changes:

| Area | Files |
| --- | --- |
| exports/API | `src/prolly/proximity/mod.rs`, `src/prolly/mod.rs`, `src/lib.rs` |
| algorithm/codec/build | new `src/prolly/proximity/accelerator/turboquant.rs` or a small `turboquant/` module |
| shared quantized executor | factor from `accelerator/pq.rs` and `search/async.rs` without changing PQ behavior |
| accelerator set/async | `accelerator/mod.rs`, `accelerator/async.rs` |
| catalog/composite | `accelerator/catalog.rs`, `accelerator/composite.rs` |
| planner/options | `search/mod.rs`, `search/planner.rs` |
| sync/async execution | `accelerator/turboquant.rs`, `search/async.rs` |
| runtime/cache | `search/runtime.rs` |
| proofs | `proof/search.rs` and proof envelope helpers |
| content graph | `content_graph/mod.rs` and codec dispatch/walker modules |
| tests | new `tests/proximity_turboquant.rs` plus existing async/proof/content/composite suites |
| conformance | `conformance/proximity-fixtures.json` |
| benchmarks | `benches/prolly_proximity_bench.rs` and benchmark reporting docs |
| bindings | UniFFI facade, Go, Node, WASM, generated Python/Kotlin/Ruby/Swift, Java facade, API parity files |
| documentation | `docs/proximity-map.md`, completion audit, binding cookbooks, changelog |

The shared-executor refactor must be behavior-preserving for PQ and land with
tests before TurboQuant uses it. It may generalize candidate admission and
authoritative reranking, but codec validation and approximate scoring remain
accelerator-specific.

## Review and Commit Strategy

Use small reviewable commits with a passing relevant test suite at each step:

1. design, provenance, baselines, and failing fixtures;
2. codebook, packing, and transform math;
3. manifest/codecs and serial builder;
4. parallel deterministic builder and limits;
5. sync forced search and shared executor refactor;
6. planner/options and accelerator-set integration;
7. async runtime and cancellation;
8. catalog and composite integration;
9. proofs and typed content graph;
10. SIMD kernels and benchmarks;
11. bindings and generated parity;
12. documentation, qualification evidence, and release audit.

Do not combine wire-format creation, planner enablement, SIMD unsafe code, and
binding regeneration in one commit.

## Release Verification

At minimum:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo +1.89.0 check --all-targets --all-features
cargo test --all-features --no-fail-fast
cargo test --doc --all-features
cargo bench --all-features --no-run
cargo check --manifest-path bindings/wasm/Cargo.toml --target wasm32-unknown-unknown
cargo test --manifest-path bindings/uniffi/Cargo.toml --target-dir target
python3 scripts/binding_api_inventory.py check --release
git diff --check
```

The full binding verification matrix in `bindings/VERIFICATION.md` is also a GA
gate. Benchmark qualification uses the pinned commands and retained raw output
defined by the implementation phase, not ad hoc local timings.

## Risks and Mitigations

| Risk | Consequence | Mitigation / gate |
| --- | --- | --- |
| structured rotation differs from paper's dense random rotation | theoretical claim or recall mismatch | explicit documentation, dense test oracle, per-dataset recall gates |
| scalar codebook bias harms inner-product candidate recall | missed candidates before rerank | four-bit default, expanded shortlist, per-metric recall floors, gated QJL phase |
| deterministic math drifts across targets | different CIDs and failed proofs | frozen bit fixtures on x86/aarch64/WASM, scalar canonical construction |
| three-bit packing bugs | silent wrong scores | exhaustive alignment/tail property tests and padding validation |
| SIMD reduction changes bits | target-dependent candidate order | SIMD fills products only; scalar ordered reduction; adversarial parity tests |
| full code scan is too slow at high cardinality | poor latency versus HNSW | explicit benchmark gate, composite support, keep out of `Auto` if it loses |
| proof closure is large | expensive proof generation/replay | existing graph limits, honest size reporting, no proof-size claim |
| transform-plan cache is unaccounted | memory cap violation | weighted dedicated runtime partition and cache-independence tests |
| format/API expansion misses a binding | cross-language divergence | release API inventory and maintained-binding acceptance gate |
| research licensing or patent uncertainty | release risk | independent-implementation provenance and recorded legal disposition before default enablement |

## Definition of Done

The feature is fully delivered only when:

1. every in-scope phase through Phase 6 is accepted;
2. all GA gates have objective linked evidence;
3. the accelerator is canonical, source-bound, bounded, async-capable,
   proof-replayable, content-walkable, GC-safe, and available in maintained
   bindings;
4. exact authoritative behavior and existing formats have not regressed;
5. `Auto` is enabled only if its separate qualification passes;
6. limitations and the structured-transform deviation are documented plainly;
7. provenance and release review are recorded; and
8. no required work is deferred under an untracked “follow-up” label.

Phase 7 is complete only if separately approved and accepted. Its absence does
not make the MSE-oriented GA incomplete, because that scope is an independently
defined algorithm in the paper and this design does not promise an unbiased
product estimator.
