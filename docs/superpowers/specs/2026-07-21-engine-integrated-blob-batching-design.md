# Engine-Integrated Blob Batching Design

## Summary

Large-value offloading will become an optional value-storage policy owned by the
prolly engine. Once a blob store and `LargeValueConfig` are attached, callers use
the ordinary `put`, `batch`, `get`, and `get_many` APIs. The engine transparently
offloads large upserts, stores deterministic `ValueRef::Blob` envelopes in leaf
nodes, and resolves those envelopes on reads.

Blob storage traits will expose overridable batch reads, writes, and deletes.
The mutation pipeline will normalize a logical batch before blob I/O, publish
all required blobs in one backend batch, and then submit one canonical node
batch. This removes per-value backend round trips while preserving existing tree
formats, content addressing, immutable snapshot semantics, and compatibility for
engines and blob stores that do not opt in.

## Goals

- Configure large-value offloading once on `Prolly` or `ProllyEngine`.
- Make the ordinary point and batch APIs operate on logical application values
  when blob storage is configured.
- Give blob backends explicit native batch-read, batch-write, and batch-delete
  override points with source-compatible point-operation defaults.
- Ensure one logical mutation batch uses at most one blob batch-put and one
  canonical node publication after normalization.
- Deduplicate overwritten mutations and identical large payloads before blob
  publication.
- Batch blob resolution for `get_many` and batch blob deletion during GC.
- Preserve current unconfigured behavior and the persisted tree/value-reference
  formats.
- Preserve runtime-neutral async support, including single-threaded WASM.

## Non-Goals

- Cross-backend atomic transactions between the node store and blob store.
- Immediate deletion of blobs replaced or deleted by a mutation. Older immutable
  trees may still reference them, so reclamation remains the responsibility of
  blob GC.
- Changing chunking, node serialization, CIDs, `ValueRef` encoding, or GC
  reachability formats.
- Requiring every backend to implement a native batch primitive.
- Removing the existing explicit large-value helper APIs in this change.

## Public API Shape

`Prolly::new` and `ProllyEngine::new` continue to construct inline-only engines.
A consuming configuration method attaches an owned blob store and offload policy:

```rust
let prolly = Prolly::new(node_store, Config::default())
    .with_blob_store(blob_store, LargeValueConfig::new(64 * 1024));

let tree = prolly.create();
let tree = prolly.batch(
    &tree,
    vec![
        Mutation::Upsert {
            key: b"small".to_vec(),
            val: b"inline".to_vec(),
        },
        Mutation::Upsert {
            key: b"large".to_vec(),
            val: large_payload.clone(),
        },
    ],
)?;

assert_eq!(prolly.get(&tree, b"large")?, Some(large_payload));
```

The public manager does not gain a blob-store generic parameter. Internally, an
object-safe adapter maps blob-store errors into `Error` and returns locally boxed
futures for reads, writes, and deletes. On native targets its erased handle is
`Send + Sync`, and `.with_blob_store` requires a thread-safe async blob store. On
`wasm32-unknown-unknown`, the handle omits those auto-trait bounds so existing
single-threaded asynchronous stores remain supported. Synchronous `BlobStore`
implementations already satisfy the required native bounds.

The existing behavior remains unchanged when no blob store is configured:

- `put` and `batch` store supplied values directly.
- `get` and `get_many` return stored leaf bytes directly.
- A tree containing a `ValueRef::Blob` envelope can still be inspected without
  a configured blob store; ordinary reads return that stored envelope as they do
  today.

With a configured blob store:

- `put` and `batch` treat upsert values as logical values and apply the configured
  inline threshold automatically.
- `get` and `get_many` return logical values, resolving blob references.
- `get_value_ref` remains the explicit raw-reference inspection API.
- Internal tree algorithms use raw read/write paths and never accidentally
  resolve values that must remain encoded for canonical processing.

The existing `put_large_value` and `get_large_value` helpers remain available for
source compatibility and explicit use with a store other than the configured
one. They share the integrated encoder and resolver, but bypass transparent
logical-value handling after encoding: `put_large_value` submits its encoded
value through the internal raw mutation path, and `get_large_value` starts with
an internal raw read. This prevents a configured engine from escaping or
offloading an already encoded value a second time.

## Blob Store Batch Contracts

`BlobStore` gains ordered batch methods equivalent to the following contracts:

```rust
fn get_blobs_ordered(
    &self,
    references: &[BlobRef],
) -> Result<Vec<Option<Vec<u8>>>, Self::Error>;

fn put_blobs(
    &self,
    values: &[&[u8]],
) -> Result<Vec<BlobRef>, Self::Error>;

fn delete_blobs(
    &self,
    references: &[BlobRef],
) -> Result<(), Self::Error>;
```

`AsyncBlobStore` keeps its existing `get_blobs_ordered` method and gains async
`put_blobs` and `delete_blobs` methods with the same input/output semantics. The
synchronous and asynchronous surfaces use these exact parallel names.

Contract requirements:

- Ordered reads return exactly one slot per requested reference in caller order.
- Batch puts return exactly one reference per input payload in caller order.
- Deleting a missing blob is not an error, matching `delete_blob`.
- Implementations may partially publish content-addressed blobs before returning
  an error. Such blobs are unreachable and safe to collect later.
- The engine validates result cardinality and all CID/length relationships before
  publishing tree nodes or returning resolved values.

Default implementations call existing point methods, keeping existing custom
blob stores source-compatible. Async defaults use bounded concurrency for point
fallbacks; a backend with a native multi-operation request or transaction should
override the corresponding method.

`Arc<T>` and reference forwarding implementations delegate every new method so
backend overrides are not hidden by adapters. `SyncBlobStoreAsAsync` forwards
batch calls to the synchronous store instead of degrading them into async point
calls. `TokioBlockingBlobStore` submits one blocking task per batch, not one task
per blob.

Built-in behavior:

- `MemBlobStore` overrides reads, puts, and deletes to acquire its `RwLock` once
  per batch.
- `FileBlobStore` may retain validated point-operation defaults because each blob
  still requires its own durable temporary-file publication. It must preserve
  existing validation and cleanup behavior.

## Mutation Data Flow

The configured engine processes a logical mutation batch in this order:

1. Normalize mutations using canonical last-write-wins semantics. Sorting and
   duplicate-key elimination happen before blob I/O so overwritten values never
   cause unnecessary blob publication.
2. Partition surviving upserts into raw inline values, escaped inline values,
   and values larger than `LargeValueConfig::inline_threshold`.
3. Compute content references for large values and deduplicate identical payloads
   within the batch. A repeated payload is submitted to the blob store once even
   when several keys reference it.
4. Call the blob store's batch-put once for the unique large payloads.
5. Require the returned reference count to match the submitted payload count and
   validate every returned CID and length against its bytes.
6. Replace large logical values with deterministic `ValueRef::Blob` envelopes;
   preserve deletes and prepared inline values.
7. Submit the prepared mutations to the existing canonical mutation engine in
   one call and publish rewritten nodes through its normal batch path.

Point `put` uses this same pipeline with a one-element logical batch. This keeps
point and batch encoding behavior identical and avoids maintaining a separate
offload implementation.

Blob publication precedes node publication. If blob publication fails, the tree
mutation is not attempted. If node publication fails afterward, already-written
blobs may remain unreachable and are reclaimed through existing blob GC. The
engine never returns a tree that it knows references a missing or invalid blob.

Branch-lineage recording stores the same prepared, encoded mutation sequence sent
to the canonical tree writer. Internal merge and replay paths operate on those
raw stored mutations and must bypass logical-value preparation, preventing blob
references from being escaped or offloaded a second time. Public batch statistics
continue to count the corresponding logical tree mutation, whose key/delete
shape is identical after preparation.

## Read Data Flow

Configured point reads perform a raw tree read, parse the stored value, and
resolve a blob reference through the configured store when necessary. Raw bytes
that are not value-reference envelopes remain ordinary inline values.

Configured `get_many` performs these steps:

1. Use the existing level-by-level tree frontier algorithm to obtain raw stored
   values in caller order.
2. Parse each present stored value without resolving it.
3. Collect blob references, deduplicate repeated references, and retain expansion
   positions.
4. Issue one ordered blob batch-read for unique references.
5. Verify output cardinality, require every referenced blob to exist, and validate
   each payload against its reference.
6. Expand duplicate results and reconstruct logical values in the original key
   order, retaining `None` for missing keys.

A referenced blob missing from storage returns `Error::NotFound(reference.cid)`.
Corrupt bytes or malformed envelopes return the existing validation/deserialization
error family. No partially reconstructed result vector is returned.

`get_value_ref` bypasses logical resolution and reads raw leaf bytes. This is
required for diagnostics, GC tests, migrations, and applications that need to
inspect storage representation explicitly.

## Garbage Collection

Reachability marking and planning are unchanged. Blob sweep passes the complete
reclaimable set to `delete_blobs` once. The default implementation retains current
point-delete semantics, while remote and transactional backends can remove the
set with one native operation.

Ordinary `put`, `batch`, and `delete` never delete displaced blobs eagerly. A blob
may be referenced by another key, another tree, a retained root, or a historical
snapshot unknown to the current mutation. Candidate-driven GC remains the only
safe reclamation boundary.

## Errors and Validation

The engine treats backend batch results as untrusted:

- Wrong result cardinality is an error.
- A returned `BlobRef` whose length or CID does not match its submitted bytes is
  an error before node publication.
- A batch-read payload whose bytes do not match the requested reference is an
  error before values are returned.
- A missing referenced payload is `Error::NotFound`.
- Backend failures are wrapped in `Error::Store` consistently with current point
  helpers.

Batch methods are fail-fast at the API boundary, but no rollback is attempted for
content-addressed blobs already published by a backend. This is safe because blob
keys are derived from immutable content and unreachable results are collectible.

## Performance Expectations

For a normalized mutation batch containing `N` unique offloaded payloads, a
backend that overrides native batch put receives one batch call rather than `N`
point calls. Identical payloads and overwritten same-key mutations do not increase
that call's item count.

For `get_many` resolving `N` unique blob references, a native backend receives one
batch read rather than `N` point reads. `MemBlobStore` performs one lock acquisition
per operation class. `TokioBlockingBlobStore` performs one task handoff per batch.

Performance tests must assert observable call structure rather than elapsed time.
A benchmark will measure integrated batch mutation against repeated explicit
large-value puts for mixed inline/offloaded workloads at multiple batch sizes.
Benchmark output will report operation count, payload bytes, and elapsed time; it
will not impose a platform-sensitive pass/fail ratio.

## Testing Strategy

### Trait compatibility and forwarding

- A minimal blob store implementing only the three point methods compiles and
  behaves correctly through all default batch methods.
- `Arc<T>`, borrowed-store, sync-to-async, and Tokio-blocking adapters preserve
  overridden batch calls.
- Async point fallbacks respect their configured concurrency bound and preserve
  request order.

### Integrated mutation behavior

- An unconfigured engine preserves raw value behavior.
- A configured engine transparently offloads values above the threshold through
  ordinary `put` and resolves them through ordinary `get`.
- One mixed `batch` handles inline upserts, escaped magic-prefix values, large
  upserts, and deletes correctly.
- Duplicate keys are normalized before blob I/O with last-write-wins behavior.
- Identical surviving payloads are written once and referenced by multiple keys.
- An instrumented store observes one blob batch-put and one canonical node batch
  publication for one logical mutation batch.
- Blob failure prevents node publication.
- Invalid batch-put result cardinality or references prevent node publication.
- A subsequent node-store failure may leave blobs present but returns no tree.

### Integrated read behavior

- `get_many` preserves key order across inline values, repeated blob references,
  and missing keys.
- An instrumented store observes one blob batch-get and no blob point reads for
  a multi-blob `get_many` when it overrides the batch method.
- Missing and corrupt blobs return the expected errors.
- `get_value_ref` returns the encoded reference without resolving it.

### GC and built-in stores

- Blob sweep makes one overridden batch-delete call for all reclaimable blobs.
- `MemBlobStore` batch methods preserve deduplication, idempotence, validation,
  and missing-delete behavior.
- `FileBlobStore` retains persistence, validation, listing, and directory cleanup
  behavior through default batch operations.

### Async parity

Every configured sync mutation/read behavior above has an async counterpart.
Tests include a single-threaded executor path so the integration does not assume
Tokio or `Send` futures.

## Benchmark and Documentation

A focused blob-batch benchmark will compare:

- repeated `put_large_value` calls, and
- one configured-engine `batch`

for mixed inline and large payloads at representative small and large batch
sizes. The benchmark uses identical logical inputs and verifies equivalent final
values before reporting timings.

README and rustdoc examples will lead with configuring the blob store once and
using ordinary engine APIs. Documentation will also explain:

- publication ordering and possible unreachable blobs after node failure;
- why map deletes do not eagerly delete blobs;
- how backend authors override native batch methods;
- how unconfigured managers expose stored envelopes;
- when explicit large-value helpers remain useful.

## Compatibility

- Existing `Prolly::new` and `ProllyEngine::new` call sites are unchanged.
- Existing `BlobStore` and `AsyncBlobStore` implementations inherit default batch
  behavior and require no new methods.
- Existing explicit large-value helper signatures remain available.
- Existing persisted trees and blobs remain readable without migration.
- Existing unconfigured read semantics remain unchanged.
- No dependency or Rust-version increase is required.

## Acceptance Criteria

The work is complete when:

1. A blob-configured engine transparently offloads and resolves values through
   ordinary point and batch APIs.
2. A logical mutation batch performs one blob batch-put for unique surviving
   large payloads and one canonical node publication.
3. `get_many` performs one blob batch-read for unique references and preserves
   caller order.
4. GC sweep uses one blob batch-delete override call.
5. Default point fallbacks keep existing blob stores compatible.
6. Sync, async, adapter, failure, validation, deduplication, and built-in-store
   tests pass.
7. The benchmark and documentation describe and measure the integrated path.
8. Tree/value-reference formats and unconfigured behavior remain unchanged.
