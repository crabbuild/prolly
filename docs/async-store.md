# Async-first storage architecture

Async storage is part of the core architecture. It is not an optional second
tree implementation. `ProllyEngine<S: AsyncStore>` owns the production tree
algorithms for reads, mutation, traversal, diff, merge, proofs, statistics,
builders, reachability, snapshots, and garbage collection.

The two public calling conventions use that same engine:

```text
AsyncProlly<S: AsyncStore> -----------+
                                      +--> ProllyEngine<S> --> AsyncStore
Prolly<S: Store> --> ready adapter ---+
```

`AsyncProlly<S>` is the engine's public async name. `Prolly<S>` wraps a
`SyncStoreAsAsync<Arc<S>>` engine and polls each complete public operation once
with an inline ready-only runner. The sync path does not create a runtime,
spawn a thread, park the caller, or call Tokio. An unexpected `Pending` from a
ready adapter is an internal invariant violation.

The legacy `async-store` Cargo feature is an empty compatibility spelling.
Async traits, adapters, and `AsyncProlly` are available with no feature. The
optional `tokio` feature only adds adapters for intentionally running blocking
stores on Tokio's blocking pool.

## Correctness contract

Storage policy may change cost, never results. In particular:

- every node loaded for a CID is hashed and structurally decoded before it can
  be used or admitted to a cache;
- the input `Tree` supplies its persisted format; a manager's default format is
  only the creation default;
- equal logical content and format produce the same canonical root and node
  bytes through sync and async APIs;
- ordered batch reads return one result per requested key in request order;
- caches and rightmost-path hints are optional and may be cold, absent, or
  stale without changing behavior;
- mutation publishes immutable nodes before returning the new tree, while an
  existing tree remains readable after failed or cancelled work;
- named-root publication and strict transaction commit are separate atomicity
  boundaries from returning a new immutable `Tree`.

Dropping an async future is cancellation, not rollback. Already completed
immutable node writes can remain as unreachable, reclaimable objects. A
transactional store is required when node publication and mutable-root changes
must commit under one atomic capability contract. Large values stored in a
separate blob store can likewise leave reclaimable blobs if later tree
publication fails.

## Store traits and adapters

`AsyncStore` mirrors the node, batch, hint, and preference surface of `Store`.
Its base trait deliberately does not require `Send` or `Sync`, allowing
single-threaded browser and WASM stores. Therefore a generic
`AsyncProlly<S: AsyncStore>` future is not promised to be `Send`; concrete
native stores can provide `Send` futures and may be used with multi-threaded
executors when their types prove that bound.

`SyncStoreAsAsync<S>` calls a synchronous store directly while its future is
polled. It is the internal bridge used by `Prolly<S>` and is also useful for
tests or async code that intentionally accepts caller-thread blocking.

`TokioBlockingStore<S>` is available with the `tokio` feature. It delegates
blocking calls to Tokio's blocking pool and is the appropriate bridge when an
embedded synchronous backend must be used from latency-sensitive Tokio tasks.
The core engine itself has no Tokio dependency.

Equivalent adapters exist for blob, manifest, scan, and transaction
capabilities. Native async adapters should implement their native ordered batch
operations rather than emulate them with sequential point calls.

## Execution and performance

The engine is executor-neutral and never spawns tasks. It overlaps independent
reads by polling bounded child futures and honors engine and backend limits.
Completion order cannot change result ordering or which logical error is
reported first.

Pure deterministic work remains synchronous Rust: codecs, validation,
mutation normalization, boundary detection, canonical emission, conflict
resolution, and proof verification do not become futures merely because the
engine is async. Optional parallel CPU builders must emit byte-identical nodes
to the sequential policy.

Point mutation uses localized canonical replay rather than collecting and
rebuilding the complete logical map. Sparse batches route to affected leaves,
append-heavy batches may use a validated right-edge hint, and every route has a
cold correctness path. Large sorted builders stream bounded levels and publish
validated node batches.

Async ranges and diff streams retain only traversal state and bounded read
frontiers. Eager APIs such as collected diff, collected stats, proof results,
and `get_many` necessarily allocate in proportion to their returned result or
input. The decoded node cache is bounded by configuration, except when the
explicit legacy unbounded setting is selected.

## Visibility levels

There are three distinct completion points:

1. A tree operation returns a `Tree` after all immutable nodes reachable from
   its root are available in the store's declared consistency scope.
2. Publishing a named root makes that tree discoverable under mutable metadata;
   compare-and-swap is required for concurrent publishers.
3. A strict transaction commits its staged nodes and root conditions according
   to `AsyncTransactionalStore` or the ready synchronous equivalent.

Applications must not treat the first level as an automatic branch/head update.

## Higher-level services

`ProllyEngine` is intentionally not a repository-wide god object. Versioned
maps, transactions, named roots, copy/sync, blobs, secondary indexes, content
graphs, and proximity structures keep separate capability and lifetime
boundaries. Storage-backed logic in those domains is implemented by an
async-first service or by a documented domain-specific engine. Their ordered
map work composes `ProllyEngine`; content-graph and proximity node families use
their own CID and format validators.

## Local backend guidance

- Native SQLite's synchronous adapter is the preferred embedded SQLite path.
- Native Turso async is the preferred Turso path. Local tests do not use Turso
  Cloud. The Turso adapter's optional `sync` feature means explicit cloud
  `push()` and `pull()`, not a synchronous Prolly engine.
- A blocking store inside an async application should use an explicit runtime
  adapter rather than block an executor worker accidentally.
- Object stores should keep immutable nodes, mutable manifests, hints, and
  large blobs in separate namespaces and verify content IDs at the trust
  boundary.

The local SQLite/Turso comparison and its reproducible runner are documented in
[`sqlite-turso-local-performance.md`](sqlite-turso-local-performance.md).

## Minimal examples

Native async use requires no Cargo feature:

```rust,ignore
use prolly::{AsyncProlly, AsyncStore, Config};

async fn write<S>(db: &AsyncProlly<S>) -> Result<prolly::Tree, prolly::Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let empty = db.create();
    db.put(&empty, b"key".to_vec(), b"value".to_vec()).await
}
```

The runtime-free sync facade drives the same operation:

```rust
use prolly::{Config, MemStore, Prolly};

let db = Prolly::new(MemStore::new(), Config::default());
let empty = db.create();
let tree = db.put(&empty, b"key".to_vec(), b"value".to_vec()).unwrap();
assert_eq!(db.get(&tree, b"key").unwrap(), Some(b"value".to_vec()));
```

## Verification

Architecture changes are release-blocked on canonical root fixtures,
sync/async differential tests, malformed-store tests, strict Clippy, rustdoc,
no-feature/default/all-feature builds, and `wasm32-unknown-unknown`. Performance
claims are accepted only after those correctness gates pass with CID and format
validation enabled.

The approved detailed design is
[`superpowers/specs/2026-07-18-async-first-prolly-engine-design.md`](superpowers/specs/2026-07-18-async-first-prolly-engine-design.md).
