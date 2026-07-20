# Async-first ownership inventory

**Revision scope:** async-first architecture cutover, 2026-07-18

This inventory classifies every storage-dependent public API family by its
production owner. It is an architecture audit, not a source-compatibility
promise. Pure data types, codecs, proof verifiers, policies, metrics snapshots,
and error enums are omitted because they perform no storage I/O.

No public facade owns an alternative ordered-tree routing or persistence
algorithm.

| Public API family | Production owner | Sync convention | Async convention | Correctness boundary |
| --- | --- | --- | --- | --- |
| `Prolly`, `AsyncProlly` construction, configuration, caches, metrics | `ProllyEngine` | ready adapter | direct | one cache and metric source per manager |
| point, batch, visitor, shared, rank, select, bounds, first/last reads | `ProllyEngine` read operations | one top-level ready poll | direct await | requested CID, structure, and tree format validated before use |
| ranges, pages, prefixes, reverse traversal, owned read sessions | `ProllyEngine` traversal operations | eager/iterator shape adapter | async iterator/page | bounded frontier; input and cursor order preserved |
| put, delete, batch, append, range delete | `ProllyEngine` canonical write service | one top-level ready poll | direct await | localized canonical replay; validated atomic node publication |
| batch and sorted builders | shared pure canonical emitter plus engine publisher | parallel or sequential CPU policy, ready publication | bounded async publication | emitted nodes validated; all policies preserve roots and bytes |
| diff, range diff, structural pages, conflict streams | `ProllyEngine` diff service | eager/iterator shape adapter | async stream/page | CID pruning and deterministic logical ordering |
| merge, merge explanation, CRDT merge | `ProllyEngine` merge service | ready adapter | direct await | one conflict model and canonical mutation output |
| membership/range/diff proofs and order statistics | `ProllyEngine` proof/stat services | ready adapter | direct await | proof construction uses validated nodes; verification is pure |
| stats, debug views, reachability, missing-node planning/copy | `ProllyEngine` diagnostics/copy services | ready adapter | direct await | bounded validated traversal and deterministic ordering |
| named roots and snapshots | engine manifest/snapshot service over capability adapters | ready manifest adapter | async manifest capability | immutable tree return is distinct from mutable-root publication |
| node and blob GC | engine GC service plus scan capability | ready scan adapter | async scan capability | mark before sweep; caller/store namespace authority remains explicit |
| large values and blobs | engine value service plus `AsyncBlobStore` | ready blob adapter | native async blob calls | blob CID and length validation; cross-store commit is not implied |
| borrowed and owned transactions | async transaction overlay service using an overlay engine | ready transaction capability | direct await | root conditions and node writes commit under the store contract |
| versioned-map heads, reads, writes, comparisons, merge, backup | async-first versioned-map service composing `ProllyEngine` | ready facade | async service | head CAS is separate from immutable version construction |
| secondary indexes | strict synchronous coordinator composing versioned-map and transaction services | ready ordered-tree operations | no public native async coordinator | **Approved domain exception:** coordination is sync-only; it owns no tree algorithm |
| content graph walk/copy/GC | content-graph service with its own codec and validators | native sync capability | no public native async facade | **Approved domain exception:** non-Prolly object family; no duplicate tree algorithm |
| proximity map and accelerators | domain-specific proximity engines plus `ProllyEngine` directory | native sync driver | native async driver | **Approved domain exception:** PRXN/PRXV formats require separate validators; shared pure planner and ranking state define results |
| remote root/node synchronization | copy/transaction services over remote capabilities | ready adapters where available | native async capabilities | immutable-node visibility precedes root update |

## Deliberate public shape differences

- Sync traversal returns borrowing iterators where Rust lifetimes permit it;
  async traversal returns explicitly polled iterators or bounded pages.
- Generic `AsyncStore` futures are not promised `Send`. Concrete native stores
  may prove `Send` and use multi-threaded executors.
- Cancellation does not promise rollback of already published immutable nodes.
- `Prolly::store()` exposes the original store for separate capability domains;
  the ordered-tree facade itself does not use it to select an alternate path.
- The empty `async-store` feature remains a compatibility spelling and does not
  select an implementation.

## Retired production paths

- facade-local sync mutation, append, traversal, proof, diff, and merge bodies;
- async non-append full logical-map reconstruction;
- separate facade caches and metrics;
- direct production sync cursor loaders;
- builder publication that bypassed engine validation;
- sync versioned-map head mutation that bypassed the async-first service.

Some legacy helpers remain under `cfg(test)` as differential oracles. They are
not selectable in production builds.

## Duplication audit rules

Future changes must fail review if they introduce any of the following:

- raw stored Prolly node bytes decoded outside engine validation;
- a storage-backed tree algorithm on `Prolly` or a second async manager;
- a mutation route that collects the complete logical tree merely to update a
  bounded key set;
- cache or hint contents used as proof of correctness;
- a ready sync facade that polls once per store call instead of once per public
  operation;
- concurrency completion order affecting values, roots, ordering, or errors.

The detailed invariants and release gates live in
[`superpowers/specs/2026-07-18-async-first-prolly-engine-design.md`](superpowers/specs/2026-07-18-async-first-prolly-engine-design.md).
