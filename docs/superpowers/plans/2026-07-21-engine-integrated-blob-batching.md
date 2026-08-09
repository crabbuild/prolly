# Engine-Integrated Blob Batching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make an optionally blob-configured prolly engine transparently batch-offload and resolve large values through its ordinary point and batch APIs.

**Architecture:** Extend sync and async blob traits with compatible batch operations, then add an engine-owned erased `BlobValueStore` that prepares logical mutations and resolves stored values. Keep canonical tree writers raw and unchanged; public manager entry points cross the logical/raw boundary exactly once before calling them.

**Tech Stack:** Rust 2021, Rust 1.81, runtime-neutral async traits, `futures-util`, existing SHA-256 `Cid`/`BlobRef` addressing, built-in `MemBlobStore` and `FileBlobStore`, custom benchmark harnesses.

## Global Constraints

- Preserve `Prolly::new` and `ProllyEngine::new` signatures and unconfigured behavior.
- Do not add a public blob-store generic parameter to `Prolly` or `ProllyEngine`.
- Do not change node encoding, chunking, CIDs, `ValueRef` wire encoding, or GC reachability formats.
- Keep existing `BlobStore` and `AsyncBlobStore` implementations source-compatible through default batch methods.
- Publish all required blobs before node publication; blob failure aborts the tree mutation, while later node failure may leave GC-reclaimable blobs.
- Never eagerly delete blobs from ordinary mutations because older immutable trees may still reference them.
- Preserve native thread safety and single-threaded `wasm32-unknown-unknown` async support.
- Keep `put_large_value` and `get_large_value` source-compatible and prevent double encoding on configured engines.
- Preserve all unrelated working-tree changes.
- Add no dependencies and do not raise the root crate's Rust 1.81 floor.

---

## File Structure

- `src/prolly/blob.rs`: public blob batch contracts, default fallbacks, adapter forwarding, built-in store overrides, and shared value-reference validation helpers.
- `src/prolly/engine/blob_values.rs`: focused object-safe blob runtime, logical mutation preparation, payload deduplication, and ordered value resolution.
- `src/prolly/engine/mod.rs`: optional engine value-store ownership plus raw/logical read split.
- `src/prolly/mod.rs`: sync/async configuration methods and logical/raw mutation routing at public API boundaries.
- `src/prolly/write.rs`: one reusable stable last-write-wins normalization helper shared by tree writing and blob preparation.
- `tests/blob_batch_integration.rs`: sync engine integration, call-count, validation, compatibility, and failure-order tests.
- `tests/async_store.rs`: async engine parity, bounded fallback, adapter forwarding, and single-threaded executor tests.
- `tests/large_value_offload.rs`: explicit-helper compatibility and batched GC deletion tests.
- `benches/prolly_blob_batch_bench.rs`: repeated explicit point puts versus integrated blob-backed batch benchmark.
- `Cargo.toml`: benchmark target registration only.
- `README.md`: configure-once usage, backend batch overrides, publication ordering, and GC semantics.

### Task 1: Add source-compatible blob batch contracts

**Files:**
- Modify: `src/prolly/blob.rs:181-596`
- Modify: `src/prolly/blob.rs:636-853`
- Test: `src/prolly/blob.rs:1264-end`
- Test: `tests/async_store.rs:330-405`

**Interfaces:**
- Consumes: existing `BlobStore::{get_blob, put_blob, delete_blob}`, `AsyncBlobStore`, `OrderedBlobReadPlan`, and `BlobRef`.
- Produces: `BlobStore::{get_blobs_ordered, put_blobs, delete_blobs}`, `AsyncBlobStore::{put_blobs, delete_blobs, write_parallelism}`, and forwarding overrides on every adapter.

- [ ] **Step 1: Write failing sync batch-contract tests**

Append these tests to `src/prolly/blob.rs`'s existing `tests` module:

```rust
#[test]
fn blob_store_batch_defaults_preserve_order_and_duplicates() {
    let store = MemBlobStore::new();
    let first = BlobStore::put_blob(&store, b"first").unwrap();
    let second = BlobStore::put_blob(&store, b"second").unwrap();
    let missing = BlobRef::from_bytes(b"missing");

    let values = BlobStore::get_blobs_ordered(
        &store,
        &[first.clone(), second.clone(), first.clone(), missing],
    )
    .unwrap();
    assert_eq!(
        values,
        vec![
            Some(b"first".to_vec()),
            Some(b"second".to_vec()),
            Some(b"first".to_vec()),
            None,
        ]
    );
}

#[test]
fn mem_blob_store_batch_put_and_delete_are_content_addressed() {
    let store = MemBlobStore::new();
    let refs = BlobStore::put_blobs(&store, &[b"same", b"other", b"same"]).unwrap();
    assert_eq!(refs[0], refs[2]);
    assert_eq!(store.len().unwrap(), 2);

    BlobStore::delete_blobs(&store, &[refs[0].clone(), refs[1].clone()]).unwrap();
    assert!(store.is_empty().unwrap());
}
```

- [ ] **Step 2: Run the sync tests and verify RED**

Run: `cargo test --lib prolly::blob::tests`

Expected: compilation fails because the three `BlobStore` batch methods do not exist.

- [ ] **Step 3: Add sync trait defaults and `MemBlobStore` overrides**

Add these methods after `delete_blob` in `BlobStore`:

```rust
fn get_blobs_ordered(
    &self,
    references: &[BlobRef],
) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
    let plan = OrderedBlobReadPlan::new(references);
    let mut unique = Vec::with_capacity(plan.unique_refs().len());
    for reference in plan.unique_refs() {
        unique.push(self.get_blob(reference)?);
    }
    Ok(plan.expand_owned(unique))
}

fn put_blobs(&self, values: &[&[u8]]) -> Result<Vec<BlobRef>, Self::Error> {
    values.iter().map(|value| self.put_blob(value)).collect()
}

fn delete_blobs(&self, references: &[BlobRef]) -> Result<(), Self::Error> {
    for reference in references {
        self.delete_blob(reference)?;
    }
    Ok(())
}
```

Forward all three methods in the `Arc<T>` and `&T` implementations with the
same bodies:

```rust
fn get_blobs_ordered(&self, references: &[BlobRef]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
    (**self).get_blobs_ordered(references)
}

fn put_blobs(&self, values: &[&[u8]]) -> Result<Vec<BlobRef>, Self::Error> {
    (**self).put_blobs(values)
}

fn delete_blobs(&self, references: &[BlobRef]) -> Result<(), Self::Error> {
    (**self).delete_blobs(references)
}
```

Override all three in `MemBlobStore` so each method acquires its lock once. The put override must use this exact shape:

```rust
fn put_blobs(&self, values: &[&[u8]]) -> Result<Vec<BlobRef>, Self::Error> {
    let entries = values
        .iter()
        .map(|value| (BlobRef::from_bytes(value), *value))
        .collect::<Vec<_>>();
    let mut data = self
        .data
        .write()
        .map_err(|err| MemBlobStoreError(format!("lock poisoned: {err}")))?;
    for (reference, value) in &entries {
        data.entry(reference.cid.clone())
            .or_insert_with(|| value.to_vec());
    }
    Ok(entries.into_iter().map(|(reference, _)| reference).collect())
}
```

The read override clones values from one read guard in caller order. The delete override removes every requested CID from one write guard and returns `Ok(())` for missing entries.

- [ ] **Step 4: Write failing async fallback and forwarding tests**

Extend `ParallelBlobReadStore` in `tests/async_store.rs` with `put_calls`, `delete_calls`, `in_flight_writes`, `max_in_flight_writes`, and `write_parallelism`. Add:

```rust
#[test]
fn async_blob_batch_defaults_bound_point_writes() {
    let store = ParallelBlobReadStore::new_with_write_parallelism(2);
    let refs = block_on(store.put_blobs(&[b"a", b"b", b"c", b"d"])).unwrap();
    assert_eq!(refs.len(), 4);
    assert_eq!(store.put_calls.load(Ordering::Relaxed), 4);
    assert_eq!(store.max_in_flight_writes.load(Ordering::Relaxed), 2);

    block_on(store.delete_blobs(&refs)).unwrap();
    assert_eq!(store.delete_calls.load(Ordering::Relaxed), 4);
    assert_eq!(store.max_in_flight_writes.load(Ordering::Relaxed), 2);
}
```

Implement point `put_blob` and `delete_blob` in that test store with the same `YieldOnce`/atomic in-flight pattern already used by `get_blob`.

- [ ] **Step 5: Run the async test and verify RED**

Run: `cargo test --test async_store async_blob_batch_defaults_bound_point_writes`

Expected: compilation fails because `AsyncBlobStore::put_blobs`, `delete_blobs`, and `write_parallelism` do not exist.

- [ ] **Step 6: Implement bounded async defaults and adapter forwarding**

Add to `AsyncBlobStore`:

```rust
fn write_parallelism(&self) -> usize {
    1
}

async fn put_blobs(&self, values: &[&[u8]]) -> Result<Vec<BlobRef>, Self::Error> {
    async_put_blobs_with_limit(self, values, self.write_parallelism()).await
}

async fn delete_blobs(&self, references: &[BlobRef]) -> Result<(), Self::Error> {
    async_delete_blobs_with_limit(self, references, self.write_parallelism()).await
}
```

Implement the bounded helpers without cloning payload bytes:

```rust
async fn async_put_blobs_with_limit<S: AsyncBlobStore + ?Sized>(
    store: &S,
    values: &[&[u8]],
    max_in_flight: usize,
) -> Result<Vec<BlobRef>, S::Error> {
    let mut references = vec![None; values.len()];
    let mut pending = stream::iter(values.iter().copied().enumerate())
        .map(|(index, value)| async move { (index, store.put_blob(value).await) })
        .buffer_unordered(max_in_flight.max(1));
    while let Some((index, result)) = pending.next().await {
        references[index] = Some(result?);
    }
    Ok(references
        .into_iter()
        .map(|reference| reference.expect("every indexed blob put completed"))
        .collect())
}

async fn async_delete_blobs_with_limit<S: AsyncBlobStore + ?Sized>(
    store: &S,
    references: &[BlobRef],
    max_in_flight: usize,
) -> Result<(), S::Error> {
    let mut pending = stream::iter(references)
        .map(|reference| store.delete_blob(reference))
        .buffer_unordered(max_in_flight.max(1));
    while let Some(result) = pending.next().await {
        result?;
    }
    Ok(())
}
```

Forward `write_parallelism`, `put_blobs`, and `delete_blobs` in `Arc<T>`. In `SyncBlobStoreAsAsync`, forward all three batch methods directly to the sync store. Under `tokio`, override all three on `TokioBlockingBlobStore`: clone the entire request once, invoke one `spawn_blob_blocking`, build borrowed slices inside that closure, and call the wrapped sync batch method once.

- [ ] **Step 7: Run targeted trait tests and verify GREEN**

Run: `cargo test --lib prolly::blob::tests && cargo test --test async_store async_blob_batch_defaults_bound_point_writes`

Expected: all selected tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/prolly/blob.rs tests/async_store.rs
git commit -m "feat: add blob store batch contracts"
```

### Task 2: Add the engine-owned erased blob value layer

**Files:**
- Create: `src/prolly/engine/blob_values.rs`
- Modify: `src/prolly/engine/mod.rs:1-64`
- Modify: `src/prolly/write.rs:2729-2769`
- Test: `tests/blob_batch_integration.rs`

**Interfaces:**
- Consumes: `AsyncBlobStore`, `BlobRef`, `LargeValueConfig`, `ValueRef`, `Mutation`, and `Error`.
- Produces: `BlobValueStore::{new, prepare_mutations, resolve_one, resolve_many}`, `ProllyEngine::with_blob_store`, `ProllyEngine::prepare_logical_mutations`, and reusable `write::normalize_mutations`.

- [ ] **Step 1: Create the integration test file with a failing configuration test**

Create `tests/blob_batch_integration.rs`:

```rust
use std::sync::Arc;

use prolly::{BlobStore, Config, LargeValueConfig, MemBlobStore, MemStore, Prolly};

#[test]
fn configured_sync_engine_owns_blob_store_without_changing_its_public_type() {
    fn accepts_plain_manager(_: &Prolly<MemStore>) {}

    let blobs = Arc::new(MemBlobStore::new());
    let prolly = Prolly::new(MemStore::new(), Config::default())
        .with_blob_store(blobs.clone(), LargeValueConfig::new(4));
    accepts_plain_manager(&prolly);
    assert!(blobs.is_empty().unwrap());
}
```

- [ ] **Step 2: Run the test and verify RED**

Run: `cargo test --test blob_batch_integration configured_sync_engine_owns_blob_store_without_changing_its_public_type`

Expected: compilation fails because `Prolly::with_blob_store` does not exist.

- [ ] **Step 3: Extract stable last-write-wins normalization**

In `src/prolly/write.rs`, replace private `normalize`'s sorting/deduplication body with a reusable helper:

```rust
pub(crate) fn normalize_mutations(mut mutations: Vec<Mutation>) -> Vec<Mutation> {
    if !mutations
        .windows(2)
        .all(|pair| pair[0].key() <= pair[1].key())
    {
        mutations.sort_by(|left, right| left.key().cmp(right.key()));
    }

    let mut normalized = Vec::with_capacity(mutations.len());
    for mutation in mutations {
        if normalized
            .last()
            .is_some_and(|previous: &Mutation| previous.key() == mutation.key())
        {
            normalized.pop();
        }
        normalized.push(mutation);
    }
    normalized
}

fn normalize(mutations: Vec<Mutation>) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    normalize_mutations(mutations)
        .into_iter()
        .map(mutation_parts)
        .collect()
}
```

This uses Rust's stable slice sort, so equal-key mutations retain input order and the final entry wins.

- [ ] **Step 4: Implement `engine/blob_values.rs`**

Create the module with these concrete units:

```rust
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::prolly::blob::{AsyncBlobStore, BlobRef, LargeValueConfig, ValueRef};
use crate::prolly::error::{Error, Mutation};
use crate::prolly::write::normalize_mutations;

type BlobFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + 'a>>;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
trait ErasedBlobStore: Send + Sync {
    fn get_blobs_ordered<'a>(&'a self, refs: &'a [BlobRef]) -> BlobFuture<'a, Vec<Option<Vec<u8>>>>;
    fn put_blobs<'a>(&'a self, values: &'a [&'a [u8]]) -> BlobFuture<'a, Vec<BlobRef>>;
    fn delete_blobs<'a>(&'a self, refs: &'a [BlobRef]) -> BlobFuture<'a, ()>;
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
trait ErasedBlobStore {
    fn get_blobs_ordered<'a>(&'a self, refs: &'a [BlobRef]) -> BlobFuture<'a, Vec<Option<Vec<u8>>>>;
    fn put_blobs<'a>(&'a self, values: &'a [&'a [u8]]) -> BlobFuture<'a, Vec<BlobRef>>;
    fn delete_blobs<'a>(&'a self, refs: &'a [BlobRef]) -> BlobFuture<'a, ()>;
}

struct ErasedAdapter<B>(B);

pub(crate) struct BlobValueStore {
    store: Arc<dyn ErasedBlobStore>,
    config: LargeValueConfig,
}
```

Add cfg-dependent backend marker traits followed by one adapter implementation:

```rust
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) trait EngineBlobBackend: AsyncBlobStore + Send + Sync
where
    Self::Error: Send + Sync,
{
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl<T> EngineBlobBackend for T
where
    T: AsyncBlobStore + Send + Sync,
    T::Error: Send + Sync,
{
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) trait EngineBlobBackend: AsyncBlobStore
where
    Self::Error: Send + Sync,
{
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T> EngineBlobBackend for T
where
    T: AsyncBlobStore,
    T::Error: Send + Sync,
{
}

impl<B> ErasedBlobStore for ErasedAdapter<B>
where
    B: EngineBlobBackend + 'static,
    B::Error: Send + Sync,
{
    fn get_blobs_ordered<'a>(
        &'a self,
        refs: &'a [BlobRef],
    ) -> BlobFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            self.0
                .get_blobs_ordered(refs)
                .await
                .map_err(|error| Error::Store(Box::new(error)))
        })
    }

    fn put_blobs<'a>(&'a self, values: &'a [&'a [u8]]) -> BlobFuture<'a, Vec<BlobRef>> {
        Box::pin(async move {
            self.0
                .put_blobs(values)
                .await
                .map_err(|error| Error::Store(Box::new(error)))
        })
    }

    fn delete_blobs<'a>(&'a self, refs: &'a [BlobRef]) -> BlobFuture<'a, ()> {
        Box::pin(async move {
            self.0
                .delete_blobs(refs)
                .await
                .map_err(|error| Error::Store(Box::new(error)))
        })
    }
}
```

Implement construction with the marker bound, then implement `prepare_mutations`:

```rust
impl BlobValueStore {
    pub(crate) fn new<B>(store: B, config: LargeValueConfig) -> Self
    where
        B: EngineBlobBackend + 'static,
        B::Error: Send + Sync,
    {
        Self {
            store: Arc::new(ErasedAdapter(store)),
            config,
        }
    }
}
```

Use this body for mutation preparation:

```rust
pub(crate) async fn prepare_mutations(
    &self,
    mutations: Vec<Mutation>,
) -> Result<Vec<Mutation>, Error> {
    enum Prepared {
        Delete(Vec<u8>),
        Inline(Vec<u8>, Vec<u8>),
        Blob(Vec<u8>, usize),
    }

    let mutations = normalize_mutations(mutations);
    let mut unique_indexes = HashMap::<BlobRef, usize>::new();
    let mut unique_values = Vec::<Vec<u8>>::new();
    let mut prepared = Vec::with_capacity(mutations.len());

    for mutation in mutations {
        match mutation {
            Mutation::Delete { key } => prepared.push(Prepared::Delete(key)),
            Mutation::Upsert { key, val } if val.len() > self.config.inline_threshold => {
                let reference = BlobRef::from_bytes(&val);
                let index = *unique_indexes.entry(reference).or_insert_with(|| {
                    let index = unique_values.len();
                    unique_values.push(val);
                    index
                });
                prepared.push(Prepared::Blob(key, index));
            }
            Mutation::Upsert { key, val } => {
                let stored = if ValueRef::inline_requires_escape(&val) {
                    ValueRef::Inline(val).to_bytes()
                } else {
                    val
                };
                prepared.push(Prepared::Inline(key, stored));
            }
        }
    }

    let references = if unique_values.is_empty() {
        Vec::new()
    } else {
        let value_refs = unique_values.iter().map(Vec::as_slice).collect::<Vec<_>>();
        self.store.put_blobs(&value_refs).await?
    };
    if references.len() != unique_values.len() {
        return Err(Error::Deserialize(format!(
            "blob batch put returned {} references for {} values",
            references.len(),
            unique_values.len()
        )));
    }
    for (reference, value) in references.iter().zip(&unique_values) {
        reference.validate_bytes(value)?;
    }

    Ok(prepared
        .into_iter()
        .map(|entry| match entry {
            Prepared::Delete(key) => Mutation::Delete { key },
            Prepared::Inline(key, val) => Mutation::Upsert { key, val },
            Prepared::Blob(key, index) => Mutation::Upsert {
                key,
                val: ValueRef::Blob(references[index].clone()).to_bytes(),
            },
        })
        .collect())
}
```

Add ordered resolution in the same `impl BlobValueStore`:

```rust
pub(crate) async fn resolve_one(&self, stored: Vec<u8>) -> Result<Vec<u8>, Error> {
    match self.resolve_many(vec![Some(stored)]).await?.pop() {
        Some(Some(value)) => Ok(value),
        _ => Err(Error::Deserialize(
            "blob point resolution lost its input slot".to_string(),
        )),
    }
}

pub(crate) async fn resolve_many(
    &self,
    stored_values: Vec<Option<Vec<u8>>>,
) -> Result<Vec<Option<Vec<u8>>>, Error> {
    enum Slot {
        Missing,
        Inline(Vec<u8>),
        Blob(usize),
    }

    let mut unique_indexes = HashMap::<BlobRef, usize>::new();
    let mut unique_refs = Vec::<BlobRef>::new();
    let mut slots = Vec::with_capacity(stored_values.len());
    for stored in stored_values {
        match stored {
            None => slots.push(Slot::Missing),
            Some(stored) => match ValueRef::from_stored_bytes(&stored)? {
                ValueRef::Inline(value) => slots.push(Slot::Inline(value)),
                ValueRef::Blob(reference) => {
                    let index = *unique_indexes.entry(reference.clone()).or_insert_with(|| {
                        let index = unique_refs.len();
                        unique_refs.push(reference);
                        index
                    });
                    slots.push(Slot::Blob(index));
                }
            },
        }
    }

    let loaded = if unique_refs.is_empty() {
        Vec::new()
    } else {
        self.store.get_blobs_ordered(&unique_refs).await?
    };
    if loaded.len() != unique_refs.len() {
        return Err(Error::Deserialize(format!(
            "blob batch read returned {} values for {} references",
            loaded.len(),
            unique_refs.len()
        )));
    }

    let mut resolved = Vec::with_capacity(loaded.len());
    for (reference, value) in unique_refs.iter().zip(loaded) {
        let value = value
            .ok_or_else(|| Error::NotFound(reference.cid.clone()))?;
        reference.validate_bytes(&value)?;
        resolved.push(value);
    }

    Ok(slots
        .into_iter()
        .map(|slot| match slot {
            Slot::Missing => None,
            Slot::Inline(value) => Some(value),
            Slot::Blob(index) => Some(resolved[index].clone()),
        })
        .collect())
}
```

- [ ] **Step 5: Attach `BlobValueStore` to the engine**

In `engine/mod.rs`, add `pub(crate) mod blob_values;`, import `LargeValueConfig`, and add:

```rust
pub(super) blob_values: Option<blob_values::BlobValueStore>,
```

Initialize it to `None` in `with_execution_config`. Add these native and WASM
public overloads, followed by the common preparation method:

```rust
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub fn with_blob_store<B>(mut self, store: B, config: LargeValueConfig) -> Self
where
    B: AsyncBlobStore + Send + Sync + 'static,
    B::Error: Send + Sync,
{
    self.blob_values = Some(blob_values::BlobValueStore::new(store, config));
    self
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub fn with_blob_store<B>(mut self, store: B, config: LargeValueConfig) -> Self
where
    B: AsyncBlobStore + 'static,
    B::Error: Send + Sync,
{
    self.blob_values = Some(blob_values::BlobValueStore::new(store, config));
    self
}

pub(crate) async fn prepare_logical_mutations(
    &self,
    mutations: Vec<Mutation>,
) -> Result<Vec<Mutation>, Error> {
    match &self.blob_values {
        Some(values) => values.prepare_mutations(mutations).await,
        None => Ok(mutations),
    }
}
```

- [ ] **Step 6: Add the sync manager configuration method only**

In `impl<S: Store> Prolly<S>`, add:

```rust
pub fn with_blob_store<B>(mut self, store: B, config: LargeValueConfig) -> Self
where
    B: BlobStore + 'static,
{
    self.engine = self
        .engine
        .with_blob_store(blob::SyncBlobStoreAsAsync::new(store), config);
    self
}
```

Do not route reads or writes yet.

- [ ] **Step 7: Verify normalization and construction compile**

Run: `cargo check --all-targets && cargo test --test blob_batch_integration configured_sync_engine_owns_blob_store_without_changing_its_public_type`

Expected: both commands exit 0; the configured manager has the same public type and owns the store.

- [ ] **Step 8: Commit**

```bash
git add src/prolly/engine/blob_values.rs src/prolly/engine/mod.rs src/prolly/write.rs src/prolly/mod.rs tests/blob_batch_integration.rs
git commit -m "feat: attach blob value storage to prolly engine"
```

### Task 3: Route logical mutations through one blob batch

**Files:**
- Modify: `src/prolly/mod.rs:1724-1792`
- Modify: `src/prolly/mod.rs:4208-4494`
- Modify: `src/prolly/mod.rs:4644-4787`
- Test: `tests/blob_batch_integration.rs`
- Test: `tests/async_store.rs`

**Interfaces:**
- Consumes: `ProllyEngine::prepare_logical_mutations` and existing raw canonical writers.
- Produces: transparent blob preparation for `put`, `batch`, stats/lineage batch variants, parallel batch variants, and async equivalents.

- [ ] **Step 1: Add a recording blob store and failing batch-call test**

Add to `tests/blob_batch_integration.rs` a `RecordingBlobStore` wrapping `MemBlobStore`, with `AtomicUsize` counters for point puts and batch puts plus `Mutex<Vec<usize>>` for submitted batch lengths. Override `put_blobs` to increment once and delegate to `BlobStore::put_blobs(&self.inner, values)`.

Add:

```rust
#[test]
fn configured_batch_normalizes_and_deduplicates_before_one_blob_write() {
    let blobs = Arc::new(RecordingBlobStore::default());
    let prolly = Prolly::new(MemStore::new(), Config::default())
        .with_blob_store(blobs.clone(), LargeValueConfig::new(4));
    let tree = prolly.create();
    let tree = prolly
        .batch(
            &tree,
            vec![
                Mutation::Upsert { key: b"a".to_vec(), val: b"discarded".to_vec() },
                Mutation::Upsert { key: b"b".to_vec(), val: b"shared-large".to_vec() },
                Mutation::Upsert { key: b"a".to_vec(), val: b"shared-large".to_vec() },
                Mutation::Delete { key: b"missing".to_vec() },
            ],
        )
        .unwrap();

    assert_eq!(blobs.batch_put_calls(), 1);
    assert_eq!(blobs.batch_lengths(), vec![1]);
    assert_eq!(blobs.point_put_calls(), 0);
    assert_eq!(
        prolly.get_large_value(blobs.as_ref(), &tree, b"a").unwrap(),
        Some(b"shared-large".to_vec())
    );
    assert_eq!(
        prolly.get_large_value(blobs.as_ref(), &tree, b"b").unwrap(),
        Some(b"shared-large".to_vec())
    );
}
```

- [ ] **Step 2: Run the sync test and verify RED**

Run: `cargo test --test blob_batch_integration configured_batch_normalizes_and_deduplicates_before_one_blob_write`

Expected: the blob batch-put counter is zero because `Prolly::batch` still sends logical values directly to the canonical writer.

- [ ] **Step 3: Add raw and logical sync mutation boundaries**

In `Prolly<S>`, add:

```rust
fn prepare_logical_mutations(&self, mutations: Vec<Mutation>) -> Result<Vec<Mutation>, Error> {
    let ready_store = self.engine.store.clone();
    let future = self.engine.prepare_logical_mutations(mutations);
    engine::ready::run_ready(ready_store.ready(future))
}

fn batch_stored_with_origin(
    &self,
    tree: &Tree,
    mutations: Vec<Mutation>,
    origin: PublicationOrigin,
) -> Result<Tree, Error> {
    let lineage = automatic_branch_lineage(origin, &mutations);
    let branch = self
        .engine
        .canonical_batch_tree_ready(tree, mutations, origin)?;
    if let Some(lineage) = lineage {
        self.engine.record_branch_lineage(tree, &branch, lineage);
    }
    Ok(branch)
}
```

Change `batch_with_origin` to prepare once and call `batch_stored_with_origin`. For `batch_with_lineage`, prepare the cloned logical vector, wrap the prepared vector in `Arc`, send its clone to `canonical_batch_ready`, and record that same prepared `Arc`. For `batch_with_write_stats`, `batch_with_stats_and_origin`, `parallel_batch`, and `parallel_batch_with_stats`, prepare before calling the existing canonical ready/configured methods. Preserve `input_sorted` from the original logical input for stats.

Change sync `put_large_value` to call `batch_stored_with_origin` with its already encoded value rather than `self.put`, preventing a configured engine from encoding twice.

- [ ] **Step 4: Add failing async parity test**

In `tests/async_store.rs`, add an async recording store equivalent and:

```rust
#[test]
fn async_configured_batch_uses_one_blob_batch_put() {
    let blobs = Arc::new(AsyncRecordingBlobStore::default());
    let prolly = AsyncProlly::new(
        SyncStoreAsAsync::new(Arc::new(MemStore::new())),
        Config::default(),
    )
    .with_blob_store(blobs.clone(), LargeValueConfig::new(4));
    let tree = prolly.create();
    let tree = block_on(prolly.batch(
        &tree,
        vec![
            Mutation::Upsert { key: b"a".to_vec(), val: b"same-large".to_vec() },
            Mutation::Upsert { key: b"b".to_vec(), val: b"same-large".to_vec() },
        ],
    ))
    .unwrap();
    assert_eq!(blobs.batch_put_calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        block_on(prolly.get_large_value(blobs.as_ref(), &tree, b"a")).unwrap(),
        Some(b"same-large".to_vec())
    );
}
```

- [ ] **Step 5: Run the async test and verify RED**

Run: `cargo test --test async_store async_configured_batch_uses_one_blob_batch_put`

Expected: behavior fails because async public mutation methods do not prepare logical values.

- [ ] **Step 6: Route async public mutations and preserve raw helpers**

Rename current async `batch_with_origin` body to `batch_stored_with_origin`. Add a logical `batch_with_origin` that awaits `prepare_logical_mutations` once and calls the stored method. Update async `batch_with_lineage` and `batch_with_write_stats` exactly like the sync variants: prepare once, pass prepared mutations to canonical writing, and store prepared lineage.

Change async `put_large_value` to call `batch_stored_with_origin` with the encoded mutation. Keep delete-only and range-delete paths raw because they contain no values.

- [ ] **Step 7: Verify GREEN for sync and async writes**

Run: `cargo test --test blob_batch_integration configured_batch_normalizes_and_deduplicates_before_one_blob_write && cargo test --test async_store async_configured_batch_uses_one_blob_batch_put`

Expected: both tests pass with one unique blob in one batch.

- [ ] **Step 8: Commit**

```bash
git add src/prolly/mod.rs tests/blob_batch_integration.rs tests/async_store.rs
git commit -m "feat: batch logical values through blob storage"
```

### Task 4: Transparently resolve point and multi-key reads

**Files:**
- Modify: `src/prolly/engine/mod.rs:176-291`
- Modify: `src/prolly/mod.rs:1662-1684`
- Modify: `src/prolly/mod.rs:4608-4642`
- Test: `tests/blob_batch_integration.rs`
- Test: `tests/async_store.rs`

**Interfaces:**
- Consumes: `BlobValueStore::{resolve_one, resolve_many}` and current engine tree frontier reads.
- Produces: `ProllyEngine::{get_stored, get_many_stored}` plus transparent public `get` and `get_many`.

- [ ] **Step 1: Add a failing ordered multi-read call-count test**

Extend `RecordingBlobStore` with point-get and batch-get counters and an override that delegates to the inner `MemBlobStore`. Add:

```rust
#[test]
fn configured_get_many_uses_one_ordered_blob_batch() {
    let blobs = Arc::new(RecordingBlobStore::default());
    let prolly = Prolly::new(MemStore::new(), Config::default())
        .with_blob_store(blobs.clone(), LargeValueConfig::new(4));
    let tree = prolly
        .batch(
            &prolly.create(),
            vec![
                Mutation::Upsert { key: b"a".to_vec(), val: b"shared-large".to_vec() },
                Mutation::Upsert { key: b"b".to_vec(), val: b"tiny".to_vec() },
                Mutation::Upsert { key: b"c".to_vec(), val: b"shared-large".to_vec() },
            ],
        )
        .unwrap();
    blobs.reset_read_counts();

    let values = prolly.get_many(&tree, &[b"c", b"missing", b"b", b"a"]).unwrap();
    assert_eq!(
        values,
        vec![
            Some(b"shared-large".to_vec()),
            None,
            Some(b"tiny".to_vec()),
            Some(b"shared-large".to_vec()),
        ]
    );
    assert_eq!(blobs.batch_get_calls(), 1);
    assert_eq!(blobs.last_batch_get_len(), 1);
    assert_eq!(blobs.point_get_calls(), 0);
}
```

- [ ] **Step 2: Run the test and verify RED**

Run: `cargo test --test blob_batch_integration configured_get_many_uses_one_ordered_blob_batch`

Expected: returned large values are stored `PLVB` envelopes and no blob batch read occurs.

- [ ] **Step 3: Split raw and logical engine reads**

Rename the current engine algorithms to:

```rust
pub(crate) async fn get_stored(
    &self,
    tree: &Tree,
    key: &[u8],
) -> Result<Option<Vec<u8>>, Error>

pub(crate) async fn get_many_stored<K: AsRef<[u8]>>(
    &self,
    tree: &Tree,
    keys: &[K],
) -> Result<Vec<Option<Vec<u8>>>, Error>
```

Add public wrappers:

```rust
pub async fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    let stored = self.get_stored(tree, key).await?;
    match (&self.blob_values, stored) {
        (Some(values), Some(stored)) => Ok(Some(values.resolve_one(stored).await?)),
        (_, stored) => Ok(stored),
    }
}

pub async fn get_many<K: AsRef<[u8]>>(
    &self,
    tree: &Tree,
    keys: &[K],
) -> Result<Vec<Option<Vec<u8>>>, Error> {
    let stored = self.get_many_stored(tree, keys).await?;
    match &self.blob_values {
        Some(values) => values.resolve_many(stored).await,
        None => Ok(stored),
    }
}
```

- [ ] **Step 4: Keep explicit inspection/helper APIs raw**

Change async `get_value_ref` and `get_large_value` to start from `get_stored`. Change sync `get_large_value` to run `engine.get_stored` and resolve with its explicit store. Leave sync `get_value_ref_with` unchanged because the borrowed read substrate already inspects stored leaf bytes directly.

- [ ] **Step 5: Add async ordered-read parity assertion**

Extend `async_configured_batch_uses_one_blob_batch_put` or add a dedicated test that calls async `get_many` with duplicate references and asserts one `get_blobs_ordered` override call, one unique requested reference, preserved caller order, and zero point gets.

- [ ] **Step 6: Run sync, async, and legacy large-value tests**

Run: `cargo test --test blob_batch_integration configured_get_many_uses_one_ordered_blob_batch && cargo test --test async_store async_configured && cargo test --test large_value_offload`

Expected: all selected tests pass; explicit helper behavior remains unchanged.

- [ ] **Step 7: Commit**

```bash
git add src/prolly/engine/mod.rs src/prolly/mod.rs tests/blob_batch_integration.rs tests/async_store.rs
git commit -m "feat: resolve configured blob values in engine reads"
```

### Task 5: Harden failure ordering, result validation, GC batching, and compatibility

**Files:**
- Modify: `src/prolly/mod.rs:2784-2855`
- Modify: `src/prolly/mod.rs:5835-5927`
- Modify: `src/prolly/engine/blob_values.rs`
- Test: `tests/blob_batch_integration.rs`
- Test: `tests/large_value_offload.rs`
- Test: `tests/async_store.rs`

**Interfaces:**
- Consumes: prepared mutation pipeline and new blob delete contract.
- Produces: validation-before-node-publication guarantees, one batch delete per GC sweep, and default-method source compatibility evidence.

- [ ] **Step 1: Add failing validation and publication-order tests**

In `tests/blob_batch_integration.rs`, add a `CountingNodeStore` that delegates to
`MemStore` and increments `publication_calls` from `publish_nodes`/`batch_put`.
First add a successful mixed batch assertion:

```rust
let tree = prolly.batch(&prolly.create(), successful_mutations).unwrap();
assert!(tree.root.is_some());
assert_eq!(node_store.publication_calls(), 1);
```

Then add blob-store modes `FailPut`, `WrongCount`, and `WrongReference`. Test
each mode with one large `batch` and assert:

```rust
assert!(result.is_err());
assert_eq!(node_store.publication_calls(), 0);
```

For `WrongReference`, return `BlobRef::from_bytes(b"different")`; for `WrongCount`, return an empty vector. Also add a node-store failure mode and assert that the blob store contains the uploaded value while the mutation returns `Err`, documenting the approved orphan-for-GC behavior.

- [ ] **Step 2: Run validation tests and verify RED where applicable**

Run: `cargo test --test blob_batch_integration blob_failure_prevents_node_publication blob_contract_violation_prevents_node_publication node_failure_can_leave_unreachable_blob`

Expected: any missing checks fail by observing a node publication or accepting an invalid batch result.

- [ ] **Step 3: Complete validation paths**

In `BlobValueStore::prepare_mutations`, keep the cardinality check before indexing references and validate every returned reference before building any `Mutation`. In `resolve_many`, check returned cardinality before expansion, convert a missing referenced value to `Error::NotFound`, and validate bytes before placing them into output slots.

Use messages with exact counts:

```rust
Error::Deserialize(format!(
    "blob batch read returned {} values for {} references",
    values.len(),
    unique_refs.len(),
))
```

- [ ] **Step 4: Add a failing GC batch-delete test**

In `tests/large_value_offload.rs`, add a recording blob store that overrides `delete_blobs`, create two unreachable blobs plus one live blob, run `sweep_blob_gc`, and assert:

```rust
assert_eq!(sweep.deleted_blobs, 2);
assert_eq!(blob_store.batch_delete_calls(), 1);
assert_eq!(blob_store.point_delete_calls(), 0);
```

- [ ] **Step 5: Run the GC test and verify RED**

Run: `cargo test --test large_value_offload blob_gc_sweep_uses_one_batch_delete`

Expected: the current sweep calls `delete_blob` once per reclaimable blob.

- [ ] **Step 6: Batch sync and async GC deletion**

Replace the async deletion loop in `ProllyEngine::sweep_blob_gc` with:

```rust
blob_store
    .delete_blobs(&plan.reclaimable_blobs)
    .await
    .map_err(|err| Error::Store(Box::new(err)))?;
```

The sync facade already delegates to this async method through `SyncBlobStoreAsAsync`, so no second loop is needed. Preserve planned counts and bytes exactly.

- [ ] **Step 7: Add default-only compatibility coverage**

Define `PointOnlyBlobStore` in `tests/blob_batch_integration.rs` implementing only `get_blob`, `put_blob`, and `delete_blob`. Configure it through an `Arc`, run integrated `batch`, `get_many`, and `sweep_blob_gc`, and assert correct behavior. This is the compile-time regression proving that new trait methods are optional.

- [ ] **Step 8: Run hardening tests and verify GREEN**

Run: `cargo test --test blob_batch_integration && cargo test --test large_value_offload && cargo test --test async_store async_configured`

Expected: all selected tests pass.

- [ ] **Step 9: Commit**

```bash
git add src/prolly/engine/blob_values.rs src/prolly/mod.rs tests/blob_batch_integration.rs tests/large_value_offload.rs tests/async_store.rs
git commit -m "fix: harden blob batch publication and gc"
```

### Task 6: Document and benchmark the integrated path

**Files:**
- Create: `benches/prolly_blob_batch_bench.rs`
- Modify: `Cargo.toml:73-end`
- Modify: `README.md:1190-1260`
- Modify: `src/prolly/blob.rs` rustdoc on batch methods
- Modify: `src/prolly/engine/mod.rs` rustdoc on `with_blob_store`, `get`, and `get_many`
- Modify: `src/prolly/mod.rs` rustdoc on sync `with_blob_store`, `put`, and `batch`

**Interfaces:**
- Consumes: completed configured-engine API.
- Produces: runnable `prolly_blob_batch_bench` target and configure-once user/backend documentation.

- [ ] **Step 1: Add the benchmark target and benchmark source**

Append to `Cargo.toml`:

```toml
[[bench]]
name = "prolly_blob_batch_bench"
harness = false
```

Create `benches/prolly_blob_batch_bench.rs` with a `main` that reads `BLOB_BATCH_SIZES` as a comma-separated list defaulting to `"100,1000"`, uses a 64-byte threshold, builds deterministic alternating 32-byte and 4-KiB values, and prints:

```text
batch_size,path,operations,payload_bytes,elapsed_ns,ns_per_operation,validated
```

For `path=repeated_explicit`, loop over entries and call `put_large_value` with
a shared `MemBlobStore`. For `path=integrated_batch`, create a fresh manager with
`.with_blob_store(blob_store, LargeValueConfig::new(64))` and call one `batch`.
After timing, verify every key with the corresponding logical read API before
printing `validated=true`. Wrap measured inputs and outputs with `black_box`.

- [ ] **Step 2: Run the benchmark smoke test**

Run: `BLOB_BATCH_SIZES=10 cargo bench --bench prolly_blob_batch_bench`

Expected: two CSV data rows, both with `operations=10` and `validated=true`.

- [ ] **Step 3: Replace README's helper-first example**

Lead the Large Value Offloading section with:

```rust
use std::sync::Arc;
use prolly::{Config, LargeValueConfig, MemBlobStore, MemStore, Mutation, Prolly};

let blobs = Arc::new(MemBlobStore::new());
let prolly = Prolly::new(MemStore::new(), Config::default())
    .with_blob_store(blobs, LargeValueConfig::new(1024));
let tree = prolly.batch(
    &prolly.create(),
    vec![Mutation::Upsert {
        key: b"doc/body".to_vec(),
        val: vec![42; 8 * 1024],
    }],
)?;
assert_eq!(prolly.get(&tree, b"doc/body")?, Some(vec![42; 8 * 1024]));
```

Explain batch override contracts, publication ordering, possible unreachable blobs after node failure, non-eager deletion, unconfigured envelope behavior, and explicit-helper compatibility. Keep the existing FileBlobStore and GC examples after the integrated example.

- [ ] **Step 4: Update rustdoc**

Document exact order/cardinality requirements on all six sync/async batch methods. Document `.with_blob_store` as consuming configuration, and state that ordinary reads/writes become logical while `get_value_ref` remains raw inspection.

- [ ] **Step 5: Run docs and formatting checks**

Run: `cargo fmt --all --check && cargo test --doc`

Expected: formatting is clean and all doctests pass.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml benches/prolly_blob_batch_bench.rs README.md src/prolly/blob.rs src/prolly/engine/mod.rs src/prolly/mod.rs
git commit -m "docs: explain and benchmark blob-backed batches"
```

### Task 7: Full verification and compatibility audit

**Files:**
- Verify only; modify implementation/test files only if a check exposes a defect.

**Interfaces:**
- Consumes: all prior tasks.
- Produces: evidence that the feature satisfies the approved spec without regressing existing stores or targets.

- [ ] **Step 1: Verify formatting and static checks**

Run: `cargo fmt --all --check && cargo clippy --all-targets --all-features -- -D warnings`

Expected: exit 0 with no warnings.

- [ ] **Step 2: Run the complete default-feature test suite**

Run: `cargo test --all-targets`

Expected: exit 0 with zero failing unit, integration, doctest, and compile tests.

- [ ] **Step 3: Run Tokio adapter coverage**

Run: `cargo test --all-targets --features tokio`

Expected: exit 0, including one-task-per-blob-batch Tokio adapter tests.

- [ ] **Step 4: Check the Rust 1.81-compatible library surface**

Run: `cargo check --lib --no-default-features`

Expected: exit 0 without a dependency or language-feature floor increase.

- [ ] **Step 5: Check WASM when the target is installed**

Run:

```bash
if rustup target list --installed | grep -qx wasm32-unknown-unknown; then
  cargo check --lib --target wasm32-unknown-unknown
else
  echo "wasm32-unknown-unknown not installed; native single-threaded async tests are the fallback evidence"
fi
```

Expected: WASM check exits 0 when installed; otherwise the command reports the explicit skip and exits 0.

- [ ] **Step 6: Re-run the benchmark smoke test**

Run: `BLOB_BATCH_SIZES=10 cargo bench --bench prolly_blob_batch_bench`

Expected: repeated and integrated rows both validate the same ten logical values.

- [ ] **Step 7: Audit the diff against acceptance criteria**

Run:

```bash
git diff --check 86284cba..HEAD
git status --short
```

Confirm manually that:

- configured point/batch writes are transparent;
- one logical batch uses one unique-payload blob batch-put;
- `get_many` uses one unique-reference blob batch-get;
- GC uses one blob batch-delete;
- point-only custom stores still compile;
- explicit helpers do not double encode;
- formats and unconfigured behavior are unchanged;
- unrelated pre-existing working-tree changes remain untouched.

- [ ] **Step 8: Commit any verification-only fixes**

If verification required changes, stage only the feature's known files (unchanged
paths are harmless) and commit them:

```bash
git add Cargo.toml README.md benches/prolly_blob_batch_bench.rs src/prolly/blob.rs src/prolly/engine/blob_values.rs src/prolly/engine/mod.rs src/prolly/mod.rs src/prolly/write.rs tests/async_store.rs tests/blob_batch_integration.rs tests/large_value_offload.rs
git commit -m "fix: complete blob batch integration verification"
```

If no changes were required, do not create an empty commit.
