# Optimize async point publication without changing canonical results

**Date:** 2026-07-19

**Status:** Approved architecture, implementation pending written-spec review

**Content type:** Conceptual design specification

**Audience:** Prolly maintainers, storage-adapter authors, and performance reviewers

**Goal:** Reduce native Turso point-put latency while preserving canonical bytes, atomic publication, durability, and non-point performance

## TL;DR

`AsyncProlly::put` will identify its final immutable-node publication as a point upsert. The async store contract will carry that runtime-only intent through transparent wrappers and `RemoteProllyStore` to the Turso backend. Turso will use a deferred transaction for that narrow path and retain immediate transactions for generic batches, builders, diff preparation, merge preparation, root operations, and strict transaction commits.

The intent cannot change mutation planning, node bytes, content identifiers (CIDs), roots, validation, visibility, or error behavior. A one-mutation call to `AsyncProlly::batch` remains general. Stores that do not recognize the intent delegate to their existing batch methods, so the synchronous SQLite path and every other adapter keep their current behavior.

## Context and measured cause

The async-first engine removed the previous O(N) mutation rebuild and sequential replay discovery. Native Turso point puts still cost more than synchronous SQLite point puts at 10,000 records. Five clean local repetitions at revision `a2f4e7a3` produced these medians:

| Pattern | Turso async total | SQLite sync total | Turso/SQLite latency | Turso p50 per put |
| --- | ---: | ---: | ---: | ---: |
| Append | 87.264 ms | 11.815 ms | 7.39x | 0.847 ms |
| Clustered | 80.110 ms | 8.322 ms | 9.63x | 0.755 ms |
| Random | 102.172 ms | 17.407 ms | 5.87x | 0.952 ms |

Temporary tagged instrumentation measured 100 point puts. Each put published two nodes in one transaction. Connection acquisition consumed 0.6 to 0.7 ms across the complete run. `BEGIN IMMEDIATE` consumed 37 to 42 ms, SQL writes consumed 3 to 4 ms, and commits consumed 5 to 6 ms. Transaction lock acquisition, not connection creation or Prolly node work, dominated the remaining fixed cost.

An adapter-wide deferred-transaction prototype reduced Turso point-put totals by 55.9% to 70.8%:

| Pattern | Immediate transaction | Deferred prototype | Change |
| --- | ---: | ---: | ---: |
| Append | 87.26 ms | 28.48 ms | -67.4% |
| Clustered | 80.11 ms | 23.36 ms | -70.8% |
| Random | 102.17 ms | 45.08 ms | -55.9% |

That prototype also regressed small Turso diff cells. Append diff rose 12.7%, and clustered diff rose 12.1%. Diff fixture preparation reaches the same generic node-publication method as point put, so the backend cannot select the faster transaction safely from entry count alone.

Autocommit improved point puts by about 56% to 73%, but it regressed cold diff cells by about 9% to 14% and removed one explicit atomic publication boundary. Direct one-key reads and SQL caching also produced regressions. Those alternatives are rejected.

## Goals

This change has one performance goal under fixed correctness constraints:

- Preserve one canonical mutation algorithm for sync and async callers
- Reduce local Turso point-put transaction overhead
- Keep generic batch, builder, diff, merge, transaction, and root behavior unchanged
- Preserve byte-identical roots and persisted node bytes
- Preserve CID and tree-format verification at the engine and remote-adapter boundaries
- Preserve atomic node and hint publication when the backend supports it
- Make the optimization explicit, inspectable, and testable
- Keep the intent runtime-only and outside persisted formats
- Retain runtime neutrality and ready-sync behavior
- Reject the change if any correctness check fails or any protected median regresses by more than 5%

## Non-goals

This design does not broaden the transaction or storage contract:

- Change tree encoding, chunking, mutation normalization, or canonical routing
- Infer point intent from batch length, node count, key shape, or hint presence
- Change synchronous `Store`, SQLite configuration, or SQLite transaction policy
- Change Turso durability, journal, cloud synchronization, or root compare-and-swap behavior
- Apply deferred transactions to arbitrary batches or read-before-write operations
- Change strict transaction commit, overlay staging, or conflict semantics
- Add retries, connection pooling, prepared-statement caching, or experimental multiversion concurrency control
- Expose a caller-selectable durability or locking knob on `AsyncProlly`
- Use cloud credentials or network synchronization in the performance tests

## Required invariants

The implementation must satisfy every invariant in this table:

| Invariant | Required behavior |
| --- | --- |
| Canonical identity | Equal base tree, format, and mutation produce equal node bytes and the same root for every publication intent. |
| Content integrity | Every node is decoded and checked against its CID and tree format before publication and cache admission. |
| Intent isolation | Only `AsyncProlly::put` and tree-node publication inherited by `put_large_value` use point intent. |
| Batch stability | `AsyncProlly::batch` uses general intent for one or more mutations. Delete, range delete, builders, diff preparation, and merge preparation also remain general. |
| Atomic publication | A backend that atomically publishes node batches must retain that boundary. Node-plus-hint publication remains one transaction. |
| Visibility | The engine returns a new tree only after every referenced node has been acknowledged by the store. |
| Failure behavior | Publication errors return no new tree. Existing immutable trees remain valid and unchanged. |
| Transaction stability | Overlay writes remain staged. Final strict transaction commits keep their existing transaction behavior. |
| Runtime-only policy | Intent is not encoded into nodes, manifests, hints, benchmark fixtures, or wire formats. |
| Default compatibility | A store that does not override the intent methods executes its existing batch or batch-plus-hint method. |
| Sync stability | `Prolly<S: Store>` and `SyncStoreAsAsync<S>` retain current synchronous store selection and ready completion. |

## Public contract

Add a small runtime policy enum beside `AsyncStore`:

```rust,ignore
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum NodeWriteIntent {
    #[default]
    General,
    PointUpsert,
}
```

`NodeWriteIntent` describes why the engine publishes an immutable node batch. It does not request weaker durability or permit a backend to split an atomic operation. `PointUpsert` means one public point-upsert operation reached final node publication without a read inside the publication transaction.

Add two default methods to `AsyncStore`:

```rust,ignore
async fn batch_put_with_intent(
    &self,
    entries: &[(&[u8], &[u8])],
    intent: NodeWriteIntent,
) -> Result<(), Self::Error> {
    let _ = intent;
    self.batch_put(entries).await
}

async fn batch_put_with_hint_and_intent(
    &self,
    entries: &[(&[u8], &[u8])],
    namespace: &[u8],
    key: &[u8],
    value: &[u8],
    intent: NodeWriteIntent,
) -> Result<(), Self::Error> {
    let _ = intent;
    self.batch_put_with_hint(entries, namespace, key, value).await
}
```

The hinted default delegates to the existing combined method. It must not decompose the call into separate node and hint writes because an override may provide atomicity.

Add matching default methods to `RemoteStoreBackend` named `batch_put_nodes_with_intent` and `batch_put_nodes_with_hint_and_intent`. Their defaults delegate to `batch_put_nodes` and `batch_put_nodes_with_hint`. Re-export `NodeWriteIntent` with `AsyncStore` so downstream adapters can implement the optimization without an internal module dependency.

The enum is non-exhaustive because future engines may identify another safe publication class. Adding a variant must receive its own correctness and performance review. Backends must treat unknown future variants as `General`.

## Engine routing

`ProllyEngine` will retain one canonical write implementation. A private `canonical_batch_with_publication_intent` helper will contain the current `canonical_batch` body and accept `NodeWriteIntent`. The existing `canonical_batch` calls it with `General`.

Add a private `canonical_point_upsert` entry point that accepts exactly one key and value, constructs one `Mutation::Upsert`, and calls the shared helper with `PointUpsert`. `AsyncProlly::put` calls this entry point directly instead of delegating to its public `batch` method. This shape prevents one-element public batches from acquiring point intent and avoids a second canonical mutation algorithm.

`execute_replay` accepts the intent only for final publication. Replay discovery, missing-node hydration, validation, rightmost-path computation, node decoding, metrics, and cache admission remain unchanged. The final calls become:

- `batch_put_with_intent` when no rightmost hint is published
- `batch_put_with_hint_and_intent` when nodes and a rightmost hint are published together

The engine does not inspect backend type or transaction semantics. Empty publication remains a no-op and makes no store call. `put_large_value` inherits point intent for its tree-node publication because it calls `put`; blob storage behavior remains unchanged.

## Adapter and wrapper routing

Transparent wrappers must preserve intent explicitly:

| Layer | Required handling |
| --- | --- |
| `Arc<T>: AsyncStore` | Forward both intent methods to `T`. |
| `SearchIo<S>` | Forward both intent methods to the wrapped store. |
| `RemoteProllyStore<B>` | Verify every node CID first, then forward the unchanged intent to `B`. |
| `Arc<T>: RemoteStoreBackend` | Forward both backend intent methods to `T`. |
| `SyncStoreAsAsync<S>` | Deliberately consume the intent and call the existing synchronous batch methods inline. |
| `TokioBlockingStore<S>` | Deliberately consume the intent and call the existing synchronous batch methods on its blocking worker. |
| Async transaction overlays | Stage nodes through their existing batch path, keep hints disabled, and do not forward intent to the base store before commit. |
| Foreign and other remote backends | Use the default general path unless their native implementation receives a separate reviewed optimization. |

The transaction overlays are semantic boundaries, not transparent wrappers. They currently report that hints are unsupported, so point mutations stage nodes without publishing a hint. A point mutation can later commit with roots, conditions, deletes, and other node writes. Its final `commit_transaction` therefore remains general and keeps Turso's immediate transaction.

Tests must inventory every `AsyncStore` and `RemoteStoreBackend` implementation. A new transparent wrapper cannot rely on the default method because that would erase an inner adapter's optimized intent handling.

## Turso transaction selection

`TursoBackend` will map intent to transaction behavior in one private selector:

| Operation | Intent | Transaction behavior |
| --- | --- | --- |
| `batch_put_nodes_with_intent` | `PointUpsert` | `Deferred` |
| `batch_put_nodes_with_hint_and_intent` | `PointUpsert` | `Deferred` |
| Either intent method | `General` or unknown future variant | `Immediate` |
| Existing `batch_nodes` | Not applicable | `Immediate` |
| Existing `batch_put_nodes` | Not applicable | `Immediate` |
| Existing `batch_put_nodes_with_hint` | Not applicable | `Immediate` |
| Root compare-and-swap and strict commit | Not applicable | `Immediate` |

The point-publication transaction performs no reads. Its first database statement writes immutable nodes, followed by the optional hint write, then commit. Deferred mode changes when Turso acquires the writer lock. It does not change the statements, transaction boundary, commit acknowledgment, or adapter error mapping.

Node writes remain idempotent content-addressed upserts. The hinted path writes every node and the hint in the same transaction. A statement or commit failure returns an error and cannot return a usable new tree. A failed or cancelled operation may leave unreachable immutable nodes under the existing store contract, but it cannot expose a partially updated named root. Cancellation keeps the existing Turso transaction contract; this design does not promise rollback solely because a future is dropped.

Concurrent point publications may contend when their first write acquires the writer lock. This change adds no retry loop and preserves the current busy/error policy. Any future operation that reads within its publication transaction must use `General` until a separate concurrency analysis proves another mode safe.

## Correctness tests

Implementation starts with failing tests for these contracts:

1. A recording async store observes `PointUpsert` for a changed `AsyncProlly::put`.
2. The same store observes `General` for a one-upsert `AsyncProlly::batch`.
3. Point put and one-item batch produce the same root and persisted node bytes as a clean rebuild.
4. Append point put uses the combined hinted intent method when hints are enabled.
5. Random and clustered point puts use the non-hinted intent method when no rightmost hint applies.
6. `Arc`, `SearchIo`, and `RemoteProllyStore` preserve the intent exactly once.
7. `RemoteProllyStore` rejects a CID mismatch before either backend intent method runs.
8. Ready sync and Tokio blocking adapters remain ready or blocking as designed and call their existing synchronous batch methods.
9. Async transaction overlays stage point writes without publishing nodes or hints to the base store; commit retains the general transaction path.
10. Turso's selector maps only `PointUpsert` to deferred mode.
11. Local Turso point publication persists across close and reopen.
12. A forced hint-statement failure rolls back its node-plus-hint transaction and returns no tree.
13. Concurrent local point publications preserve valid trees or return the documented backend error without corruption.

Existing canonical-root, malicious-store, range-delete, transaction, remote conformance, and Turso reopen tests remain mandatory. No test may weaken validation to make the optimized path pass.

## Performance verification

Correctness runs before every performance gate. A root, value, count, publication, or reopen failure invalidates the measurement.

### Focused local gate

Build baseline revision `a2f4e7a3` and the candidate in separate target directories. Run five alternating baseline/candidate process pairs for the frozen 10,000-record, 100-change SQLite-sync and Turso-async matrix. Each pair uses fresh local fixtures on the same volume and covers append, deterministic random, and clustered workloads for put, batch, diff, and merge.

Accept the focused gate only when:

- Turso point-put median total latency improves by at least 40% for every pattern
- Turso point-put p50 and p95 do not regress for any pattern
- No SQLite or Turso batch, diff, or merge median latency regresses by more than 5%
- No SQLite point-put median latency regresses by more than 5%
- Every row validates exact values, record counts, roots, and reopen behavior

Any cell above the 5% threshold receives five more alternating pairs. Evaluate the combined ten pairs. A confirmed regression blocks the change.

### Full local matrix

After the focused gate passes, run the existing local-only comparison at 10K, 50K, 100K, 500K, 1M, and 2M records. Use three fresh alternating baseline/candidate pairs for both adapters, all three patterns, and put, batch, diff, and merge. Keep the existing fixed keys, values, seeds, 1% change count clamped to 100 through 10,000, cold-manager fixture policy, durability settings, and validation schema.

Accept the full matrix only when:

- No protected API/pattern/size median latency regresses by more than 5%
- Turso point-put latency remains better than baseline at every size and pattern
- Point-put node reads and writes remain bounded by tree height
- Point-put publication uses at most one node batch
- All 432 candidate rows validate and no row is skipped
- The report records revision, dirty digest, compiler, hardware, filesystem, dependency features, and effective local database settings

The full run does not enable `turso-cloud-sync`, read credentials, call `push`, or call `pull`. Local results make no claim about network synchronization performance.

### In-memory regression gate

Run the same in-memory foundation and core operation screens used for the async-first cutover. Compare candidate results with the recorded current-head baseline and repeat the alternating measurements when a median crosses 5%. Point get, point update, point delete, owned ranges, generic and mixed batches, append paths, diff, merge, and ready-sync versus adapted-async calls must remain within the existing 5% median and 10% p95 gates.

The new enum dispatch and wrapper forwarding must not add node reads, node writes, publication calls, allocations proportional to tree size, Tokio dependencies, or executor entry to the ready-sync path.

## Verification commands

The implementation plan will select exact focused test filters. Final verification includes these repository-level checks:

```sh
cargo fmt --all -- --check
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --manifest-path stores/prolly-store-turso/Cargo.toml
cargo test --manifest-path stores/prolly-store-turso/Cargo.toml --features turso-cloud-sync
cargo check --manifest-path bindings/wasm/Cargo.toml --target wasm32-unknown-unknown
```

Run the focused and full local comparison through `scripts/run_sqlite_turso_local_comparison.sh` with separate output directories for each revision. Preserve raw rows, summaries, manifests, and machine metadata under `performance-results/`.

## Documentation and API updates

Documentation must distinguish publication scheduling from canonical and durability semantics.

Update the `AsyncStore` rustdoc to define intent as a performance-only publication classification. Update the Turso adapter README to state that point upserts use deferred local transactions while generic batches and mutable-root transactions retain immediate locking. Do not describe deferred mode as weaker durability or as a cloud-sync setting.

Update the local SQLite/Turso report with the exact before/after matrix, transaction policy, limitations, and rejected alternatives. The pull request description must distinguish native SQLite sync from native Turso async and state that every measurement is local-only.

## Implementation inventory

The change crosses only the engine-to-adapter publication path and its tests:

The expected production changes are limited to:

- `src/prolly/store/mod.rs`: define and forward `NodeWriteIntent`
- `src/lib.rs`: re-export the new adapter contract type
- `src/prolly/engine/write.rs`: route final replay publication by intent
- `src/prolly/mod.rs`: send point put through the dedicated engine entry point
- `src/prolly/remote.rs`: validate and forward intent to remote backends
- `src/prolly/proximity/search/runtime.rs`: preserve intent through `SearchIo`
- `src/prolly/transaction.rs`: document and test overlay intent absorption
- `stores/prolly-store-turso/src/lib.rs`: select deferred mode for point publication only
- Core, remote, transaction, and Turso tests: prove routing, identity, atomicity, persistence, and concurrency
- Benchmark results and documentation: record the verified effect and unchanged paths

The implementation plan may narrow this list after test discovery. Expanding transaction semantics, persisted formats, cloud behavior, or sync store contracts requires a new design review.

## Rollout and rollback

Rollout remains contingent on both the focused and full verification gates.

Land the change as one behavior slice after its tests and focused benchmarks pass. Run the full local matrix before marking the pull request ready. Keep no feature flag because the default methods already provide a safe general fallback and the Turso selector is one isolated policy function.

If correctness, atomicity, reopen, or performance gates fail, revert Turso's intent override and retain the default general path. The public intent methods can remain only if another adapter uses them and their conformance tests pass; otherwise revert the complete slice. No partial rollout may infer intent from batch shape.

## Resolved design decisions

The approved architecture resolves every design choice needed for planning:

- Use `NodeWriteIntent`, not `Core`, `CoreStore`, a durability option, or a Turso-specific flag
- Keep the enum runtime-only, additive, and non-exhaustive
- Mark public point upsert at the engine entry point, not inside the adapter
- Keep one canonical mutation implementation
- Preserve intent through transparent wrappers and absorb it at transaction overlays
- Use deferred Turso transactions only for explicit point publication
- Retain immediate Turso transactions for every existing general and mutable-state path
- Require local paired performance evidence and zero correctness exceptions before release

There are no open design questions.
