#![allow(unexpected_cfgs)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::future::Future;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use prolly::{
    AsyncProlly, AsyncStore, BatchOp, Config, MemStore, MemStoreError, Mutation, Prolly, Store,
    SyncStoreAsAsync, Tree,
};
#[cfg(not(feature = "baseline-contract"))]
use prolly::{NodePublication, PublicationOrigin};

struct ThreadCountingAllocator;

thread_local! {
    static COUNT_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATION_COUNT: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let allocation = unsafe { System.alloc(layout) };
        if !allocation.is_null() {
            record_allocation();
        }
        allocation
    }

    unsafe fn dealloc(&self, allocation: *mut u8, layout: Layout) {
        unsafe { System.dealloc(allocation, layout) };
    }

    unsafe fn realloc(&self, allocation: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(allocation, layout, size) };
        if !replacement.is_null() {
            record_allocation();
        }
        replacement
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

fn record_allocation() {
    COUNT_ALLOCATIONS.with(|enabled| {
        if enabled.get() {
            ALLOCATION_COUNT.with(|count| count.set(count.get() + 1));
        }
    });
}

#[derive(Clone, Default)]
struct CountingSyncStore {
    inner: Arc<MemStore>,
    publications: Arc<AtomicUsize>,
}

impl CountingSyncStore {
    fn reset(&self) {
        self.publications.store(0, Ordering::Relaxed);
    }

    fn publication_calls(&self) -> usize {
        self.publications.load(Ordering::Relaxed)
    }

    fn published(&self) {
        self.publications.fetch_add(1, Ordering::Relaxed);
    }
}

impl Store for CountingSyncStore {
    type Error = MemStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get(key)
    }

    fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        self.inner.get_shared(key)
    }

    fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Arc<[u8]>>>, Self::Error> {
        self.inner.batch_get_shared_ordered_unique(keys)
    }

    fn has_native_shared_reads(&self) -> bool {
        true
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.published();
        self.inner.put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key)
    }

    fn batch(&self, operations: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        if operations
            .iter()
            .any(|operation| matches!(operation, BatchOp::Upsert { .. }))
        {
            self.published();
        }
        self.inner.batch(operations)
    }

    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        self.inner.batch_get(keys)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.inner.batch_get_ordered(keys)
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.inner.batch_get_ordered_unique(keys)
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        self.published();
        self.inner.batch_put(entries)
    }
}

#[derive(Clone, Default)]
struct CountingAsyncStore {
    inner: Arc<MemStore>,
    publications: Arc<AtomicUsize>,
}

impl CountingAsyncStore {
    fn reset(&self) {
        self.publications.store(0, Ordering::Relaxed);
    }

    fn publication_calls(&self) -> usize {
        self.publications.load(Ordering::Relaxed)
    }

    fn published(&self) {
        self.publications.fetch_add(1, Ordering::Relaxed);
    }
}

impl AsyncStore for CountingAsyncStore {
    type Error = MemStoreError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get(key)
    }

    async fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        self.inner.get_shared(key)
    }

    async fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Arc<[u8]>>>, Self::Error> {
        self.inner.batch_get_shared_ordered_unique(keys)
    }

    fn has_native_shared_reads(&self) -> bool {
        true
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.published();
        self.inner.put(key, value)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key)
    }

    async fn batch(&self, operations: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        if operations
            .iter()
            .any(|operation| matches!(operation, BatchOp::Upsert { .. }))
        {
            self.published();
        }
        self.inner.batch(operations)
    }

    async fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        self.inner.batch_get(keys)
    }

    async fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.inner.batch_get_ordered(keys)
    }

    async fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.inner.batch_get_ordered_unique(keys)
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    async fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        self.published();
        self.inner.batch_put(entries)
    }
}

struct Measurement {
    elapsed: Vec<u128>,
    publication_calls: usize,
    root: String,
}

fn main() {
    let revision = std::env::var("BENCH_REVISION").unwrap_or_else(|_| "unknown".to_string());
    let records = env_usize("PROLLY_FOUNDATION_RECORDS", 10_000);
    let changes = env_usize("PROLLY_FOUNDATION_CHANGES", 100).min(records.saturating_sub(1));
    let samples = env_usize("PROLLY_FOUNDATION_SAMPLES", 30).max(3);
    let requested_apis = std::env::var("PROLLY_FOUNDATION_APIS")
        .unwrap_or_else(|_| "put,delete,batch,build,merge,range-delete,forward".to_string());
    let enabled = |api: &str| requested_apis.split(',').any(|candidate| candidate == api);
    let config = Config::default();
    let entries = (0..records)
        .map(|index| (key(index), value(index, 0)))
        .collect::<Vec<_>>();

    let sync_store = Arc::new(CountingSyncStore::default());
    let adapted_inner = Arc::new(CountingSyncStore::default());
    let adapted_store = SyncStoreAsAsync::new(adapted_inner.clone());
    let native_store = CountingAsyncStore::default();
    let sync = Prolly::new(sync_store.clone(), config.clone());
    let adapted = AsyncProlly::new(adapted_store.clone(), config.clone());
    let native = AsyncProlly::new(native_store.clone(), config);

    let sync_tree = sync
        .build_from_entries(entries.clone())
        .expect("sync fixture");
    let adapted_tree =
        block_on_ready(adapted.build_from_entries(entries.clone())).expect("adapted fixture");
    let native_tree =
        block_on_ready(native.build_from_entries(entries.clone())).expect("native fixture");
    assert_same_roots(&sync_tree, &adapted_tree, &native_tree, "fixture");

    let update_key = key(records / 2);
    let delete_key = key(records / 3);
    let update_value = value(records / 2, 1);
    let batch_mutation = vec![Mutation::Upsert {
        key: key(records / 4),
        val: value(records / 4, 2),
    }];
    let build_entries = (0..changes)
        .map(|index| (key(records + index), value(records + index, 3)))
        .collect::<Vec<_>>();
    let range_start = key(records / 3);
    let range_end = key((records / 3).saturating_add(changes).min(records));

    let sync_left = sync
        .put(&sync_tree, b"merge-left".to_vec(), b"left".to_vec())
        .expect("sync merge left");
    let sync_right = sync
        .put(&sync_tree, b"merge-right".to_vec(), b"right".to_vec())
        .expect("sync merge right");
    let adapted_left =
        block_on_ready(adapted.put(&adapted_tree, b"merge-left".to_vec(), b"left".to_vec()))
            .expect("adapted merge left");
    let adapted_right =
        block_on_ready(adapted.put(&adapted_tree, b"merge-right".to_vec(), b"right".to_vec()))
            .expect("adapted merge right");
    let native_left =
        block_on_ready(native.put(&native_tree, b"merge-left".to_vec(), b"left".to_vec()))
            .expect("native merge left");
    let native_right =
        block_on_ready(native.put(&native_tree, b"merge-right".to_vec(), b"right".to_vec()))
            .expect("native merge right");

    let request_allocations = publication_request_allocations();
    println!("revision,facade,api,records,items_per_sample,samples,median_ns,p95_ns,throughput_items_per_sec,publication_calls,request_allocations,root");

    macro_rules! run_case {
        ($api:literal, $items:expr, $sync_operation:expr, $adapted_operation:expr, $native_operation:expr) => {
            if enabled($api) {
                let sync_measurement =
                    measure(samples, &sync_store.publications, || $sync_operation);
                let adapted_measurement =
                    measure(samples, &adapted_inner.publications, || $adapted_operation);
                let native_measurement =
                    measure(samples, &native_store.publications, || $native_operation);
                assert_eq!(sync_measurement.root, adapted_measurement.root, "{}", $api);
                assert_eq!(sync_measurement.root, native_measurement.root, "{}", $api);
                emit(
                    &revision,
                    "sync_ready",
                    $api,
                    records,
                    $items,
                    request_allocations,
                    sync_measurement,
                );
                emit(
                    &revision,
                    "async_over_sync",
                    $api,
                    records,
                    $items,
                    request_allocations,
                    adapted_measurement,
                );
                emit(
                    &revision,
                    "native_async",
                    $api,
                    records,
                    $items,
                    request_allocations,
                    native_measurement,
                );
            }
        };
    }

    run_case!(
        "put",
        1,
        sync.put(&sync_tree, update_key.clone(), update_value.clone())
            .expect("sync put"),
        block_on_ready(adapted.put(&adapted_tree, update_key.clone(), update_value.clone()))
            .expect("adapted put"),
        block_on_ready(native.put(&native_tree, update_key.clone(), update_value.clone()))
            .expect("native put")
    );
    run_case!(
        "delete",
        1,
        sync.delete(&sync_tree, &delete_key).expect("sync delete"),
        block_on_ready(adapted.delete(&adapted_tree, &delete_key)).expect("adapted delete"),
        block_on_ready(native.delete(&native_tree, &delete_key)).expect("native delete")
    );
    run_case!(
        "batch",
        1,
        sync.batch(&sync_tree, batch_mutation.clone())
            .expect("sync batch"),
        block_on_ready(adapted.batch(&adapted_tree, batch_mutation.clone()))
            .expect("adapted batch"),
        block_on_ready(native.batch(&native_tree, batch_mutation.clone())).expect("native batch")
    );
    run_case!(
        "build",
        build_entries.len(),
        sync.build_from_entries(build_entries.clone())
            .expect("sync build"),
        block_on_ready(adapted.build_from_entries(build_entries.clone())).expect("adapted build"),
        block_on_ready(native.build_from_entries(build_entries.clone())).expect("native build")
    );
    run_case!(
        "merge",
        2,
        sync.merge(&sync_tree, &sync_left, &sync_right, None)
            .expect("sync merge"),
        block_on_ready(adapted.merge(&adapted_tree, &adapted_left, &adapted_right, None,))
            .expect("adapted merge"),
        block_on_ready(native.merge(&native_tree, &native_left, &native_right, None))
            .expect("native merge")
    );
    run_case!(
        "range-delete",
        changes,
        sync.delete_range(&sync_tree, &range_start, &range_end)
            .expect("sync range delete"),
        block_on_ready(adapted.delete_range(&adapted_tree, &range_start, &range_end))
            .expect("adapted range delete"),
        block_on_ready(native.delete_range(&native_tree, &range_start, &range_end))
            .expect("native range delete")
    );

    let forward_key = b"foundation-forward".as_slice();
    let forward_value = b"immutable-node-bytes".as_slice();
    let forward_entries = [(forward_key, forward_value)];
    run_case!(
        "forward",
        1,
        {
            forward_sync(&sync_store, &forward_entries);
            sync_tree.clone()
        },
        {
            forward_async(&adapted_store, &forward_entries);
            adapted_tree.clone()
        },
        {
            forward_async(&native_store, &forward_entries);
            native_tree.clone()
        }
    );

    // Keep these methods exercised even when a caller filters every case.
    sync_store.reset();
    adapted_inner.reset();
    native_store.reset();
    black_box((
        sync_store.publication_calls(),
        adapted_inner.publication_calls(),
        native_store.publication_calls(),
    ));
}

fn measure(
    samples: usize,
    publications: &AtomicUsize,
    mut operation: impl FnMut() -> Tree,
) -> Measurement {
    publications.store(0, Ordering::Relaxed);
    let mut elapsed = Vec::with_capacity(samples);
    let mut last = None;
    for _ in 0..samples {
        let started = Instant::now();
        last = Some(black_box(operation()));
        elapsed.push(started.elapsed().as_nanos().max(1));
    }
    elapsed.sort_unstable();
    Measurement {
        elapsed,
        publication_calls: publications.load(Ordering::Relaxed),
        root: tree_root(last.as_ref().expect("at least one sample")),
    }
}

fn emit(
    revision: &str,
    facade: &str,
    api: &str,
    records: usize,
    items: usize,
    request_allocations: usize,
    measurement: Measurement,
) {
    let median = measurement.elapsed[measurement.elapsed.len() / 2];
    let p95_index = (measurement.elapsed.len() * 95)
        .div_ceil(100)
        .saturating_sub(1);
    let p95 = measurement.elapsed[p95_index];
    let throughput = items as f64 / (median as f64 / 1_000_000_000.0);
    println!(
        "{revision},{facade},{api},{records},{items},{},{median},{p95},{throughput:.3},{},{request_allocations},{}",
        measurement.elapsed.len(), measurement.publication_calls, measurement.root
    );
}

fn assert_same_roots(sync: &Tree, adapted: &Tree, native: &Tree, context: &str) {
    assert_eq!(sync.root, adapted.root, "{context}: sync/adapted root");
    assert_eq!(sync.root, native.root, "{context}: sync/native root");
}

fn publication_request_allocations() -> usize {
    let key = b"node".as_slice();
    let value = b"bytes".as_slice();
    ALLOCATION_COUNT.with(|count| count.set(0));
    COUNT_ALLOCATIONS.with(|enabled| enabled.set(true));
    let entries = [(key, value)];
    #[cfg(feature = "baseline-contract")]
    black_box(&entries);
    #[cfg(not(feature = "baseline-contract"))]
    black_box(NodePublication::new(&entries, PublicationOrigin::General));
    COUNT_ALLOCATIONS.with(|enabled| enabled.set(false));
    ALLOCATION_COUNT.with(Cell::get)
}

#[cfg(feature = "baseline-contract")]
fn forward_sync<S: Store>(store: &S, entries: &[(&[u8], &[u8])]) {
    store.batch_put(entries).expect("sync forward");
}

#[cfg(not(feature = "baseline-contract"))]
fn forward_sync<S: Store>(store: &S, entries: &[(&[u8], &[u8])]) {
    store
        .publish_nodes(NodePublication::new(entries, PublicationOrigin::General))
        .expect("sync publication forward");
}

#[cfg(feature = "baseline-contract")]
fn forward_async<S: AsyncStore>(store: &S, entries: &[(&[u8], &[u8])]) {
    block_on_ready(store.batch_put(entries)).expect("async forward");
}

#[cfg(not(feature = "baseline-contract"))]
fn forward_async<S: AsyncStore>(store: &S, entries: &[(&[u8], &[u8])]) {
    block_on_ready(store.publish_nodes(NodePublication::new(entries, PublicationOrigin::General)))
        .expect("async publication forward");
}

fn tree_root(tree: &Tree) -> String {
    tree.root
        .as_ref()
        .map(|cid| {
            cid.as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()
        })
        .unwrap_or_else(|| "empty".to_string())
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:016x}").into_bytes()
}

fn value(index: usize, generation: u8) -> Vec<u8> {
    format!("value-{generation:02x}-{index:016x}").into_bytes()
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(1)
}

fn block_on_ready<F: Future>(future: F) -> F::Output {
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("in-memory foundation future returned Pending"),
    }
}
