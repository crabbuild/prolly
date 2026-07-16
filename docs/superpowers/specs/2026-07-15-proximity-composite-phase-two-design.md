# Proximity Composite Accelerators: Phase-Two Design

## Status

Implemented and validated on 2026-07-15. This child design defines phase two
of the proximity enhancement umbrella.

## Outcome

Phase two adds immutable base-plus-delta accelerator snapshots. A composite is
bound to one current PRXI descriptor and references one HNSW or PQ base
accelerator built for an ancestor PRXI descriptor. It stores only vector
insertions and vector updates in a delta tree and stores deleted or
vector-updated keys in a shadow tree. Value-only changes do not enter either
tree because authoritative reranking already resolves the current value.

Composite state is derived and disposable. PRXI, PRVR, and PRXN remain
authoritative and unchanged.

The hard-cutover naming rule applies throughout: the components are
`CompositeAccelerator` and `AcceleratorCatalog`, without generation suffixes.
Only their current wire shapes are supported; no legacy reader, compatibility
alias, or migration API is retained.

## Formats

The composite manifest commits to:

- current and base PRXI descriptor CIDs;
- current and base record counts, dimensions, and metric;
- base accelerator kind, manifest CID, and configuration fingerprint;
- optional ordered delta and shadow roots;
- inserted, vector-updated, deleted, delta, and shadow counts;
- deterministic composite policy and its fingerprint;
- exact diff/build statistics. A published composite manifest is inherently
  the accepted disposition; `FullRebuildRequired` is returned before publication.

Delta leaves contain current `StoredRecord` bytes. Shadow leaves contain an
empty value. Both use a frozen ordered-tree configuration. Keys are sorted and
unique. A vector update appears in both trees; an insertion appears only in
delta; a deletion appears only in shadow.

The source-bound accelerator catalog contains entries strictly
sorted and unique by `(kind, configuration fingerprint, root CID)`. Catalogs
are published independently through the existing typed named-root CAS API.

HNSW, PQ, PRXI, PRVR, PRXN, PRXV, and PQS8 formats do not change. Composite
manifests and accelerator catalogs have one current wire shape with no legacy
reader, generation suffix, or migration path.

## Construction

Construction structurally diffs the base and current PRVR directories. Equal
directory subtrees are skipped by CID. Changed records are decoded and
canonical vectors compared; application-value differences alone are ignored.
The builder streams sorted output into delta and shadow trees and never
materializes the authoritative record sets.

Hard limits cover diff entries, retained bytes, output bytes, and distance
work (which is deterministically zero for canonical vector comparison).
Publication writes the manifest last. A failure may leave unreachable
content but cannot publish an incomplete composite.

The deterministic policy defines maximum delta records, shadow records, delta
ratio, and shadow ratio. A composite is deliberately one generation deep: its base
must be a direct HNSW or PQ sidecar, never another composite. Crossing
any threshold produces a typed `FullRebuildRequired` disposition before catalog publication. Convenience
`build_or_rebuild` entry points synchronously construct a full current-source
accelerator using the base configuration. Callers that schedule background
work use the same disposition and publish only after that rebuild completes.

## Planning and Execution

`AcceleratorSet` may contain one current-source composite in addition to direct
current-source HNSW and PQ sidecars. Direct current-source accelerators win
when the configured preference selects their kind. Otherwise the planner may
choose `SearchPlan::Composite` when approximate search is allowed and the
composite is budget-admissible.

The composite plan commits to:

- base backend and its complete child plan;
- exact delta cardinality and scan target;
- shadow cardinality;
- base expansion/rerank inflation derived from the shadow ratio;
- final merge target.

Execution prepares the caller filter once. Base traversal admits a key only
when it passes the caller filter and is absent from the shadow tree. Delta
execution scores eligible full-precision vectors exactly. Branch candidates
are merged by key, resolved from the current authoritative directory, reranked
with the current full-precision vector, ordered by `(distance, key)`, and
truncated to `k`.

Missing shadowed base keys are never read from the current directory. A
non-shadowed base key missing from current PRVR, a delta key missing from
current PRVR, malformed content, CID mismatch, or source/configuration mismatch
is corruption and fails closed. No backend fallback occurs after execution
starts.

The same logical planner and executor rules apply to sync and async-only
stores. Cache warmth, store type, concurrency, and completion order affect only
physical statistics.

## Catalog Lifecycle

An `AcceleratorCatalog` is bound to one current PRXI descriptor. It can contain
direct HNSW, direct PQ, and composite roots. Loading validates every manifest
before producing an `AcceleratorSet`. Named publication uses typed content-root
CAS, so catalog replacement is atomic and independent of PRXI publication.
Old named-root manifests and pinned catalog CIDs retain their exact closures.

## Proofs and Content Graph

Composite search proofs commit to the catalog/composite manifest, both source
descriptors, the base accelerator closure, delta/shadow trees, and the exact
composite plan. Verification installs the complete closure and executes the
committed plan without replanning.

The typed walker recognizes composite and catalog objects. Replication, GC,
copy, and named-root publication traverse current/base descriptors, base
accelerator content, and delta/shadow descendants.

## Acceptance Gates

- Composite manifests and catalogs are byte-identical across diff traversal,
  source insertion order, and worker counts.
- Composite results equal a brute-force current-source oracle when the base
  branch is exhaustively configured; approximate tests meet the base recall
  floor after inserts, updates, and deletes.
- Deleted and vector-updated base entries can never escape the shadow set.
- Value-only updates return the current value without growing delta/shadow.
- Sync, warm/cold runtime, durable local, and async-only execution produce the
  same plan, neighbors, completion, and logical statistics.
- Delta and shadow retained memory and candidate heaps remain within explicit
  limits or fail before manifest publication.
- Threshold crossings deterministically require or perform a full rebuild and
  never publish an over-threshold composite.
- Corruption and missing content fail closed without execution fallback.
- Proof replay reproduces the committed composite plan without replanning.
- Content copy, GC, and named catalog CAS cover the complete composite closure.
- For a delta below the default threshold, composite build work is proportional
  to changed structural spans and composite query work is bounded by base plan
  work plus delta cardinality; checked benchmarks record both.
