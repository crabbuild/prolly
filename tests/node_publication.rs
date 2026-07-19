use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use prolly::{
    AsyncManifestStore, AsyncProlly, AsyncSortedBatchBuilder, AsyncStore, BatchBuilder, BatchOp,
    Cid, Config, DistanceMetric, ManifestStore, ManifestUpdate, MemStore, MemStoreError,
    MergeTraceEvent, Mutation, NodePublication, NodePublicationHint, Prolly, ProximityConfig,
    ProximityMap, ProximityMutation, ProximityRecord, PublicationOrigin, Resolution, RootCondition,
    RootManifest, RootWrite, SearchIo, SearchRuntime, SecondaryIndex, SecondaryIndexRegistry,
    Store, SyncStoreAsAsync, TransactionNodeWrite, TransactionUpdate, TransactionalStore, Tree,
};

struct ThreadCountingAllocator;

thread_local! {
    static COUNT_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATION_COUNT: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let allocation = unsafe { System.alloc(layout) };
        if !allocation.is_null() {
            COUNT_ALLOCATIONS.with(|enabled| {
                if enabled.get() {
                    ALLOCATION_COUNT.with(|count| count.set(count.get() + 1));
                }
            });
        }
        allocation
    }

    unsafe fn dealloc(&self, allocation: *mut u8, layout: Layout) {
        unsafe { System.dealloc(allocation, layout) };
    }

    unsafe fn realloc(&self, allocation: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(allocation, layout, new_size) };
        if !replacement.is_null() {
            COUNT_ALLOCATIONS.with(|enabled| {
                if enabled.get() {
                    ALLOCATION_COUNT.with(|count| count.set(count.get() + 1));
                }
            });
        }
        replacement
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

fn allocations_during<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATION_COUNT.with(|count| count.set(0));
    COUNT_ALLOCATIONS.with(|enabled| {
        assert!(
            !enabled.replace(true),
            "allocation counter cannot be nested"
        );
    });
    let output = operation();
    COUNT_ALLOCATIONS.with(|enabled| enabled.set(false));
    let allocations = ALLOCATION_COUNT.with(Cell::get);
    (output, allocations)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordedPublication {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    hint: Option<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    origin: PublicationOrigin,
}

impl RecordedPublication {
    fn from_request(publication: NodePublication<'_>) -> Self {
        Self {
            entries: publication
                .entries()
                .iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect(),
            hint: publication.hint().map(|hint| {
                (
                    hint.namespace().to_vec(),
                    hint.key().to_vec(),
                    hint.value().to_vec(),
                )
            }),
            origin: publication.origin(),
        }
    }
}

#[derive(Clone, Default)]
struct RecordingSyncStore {
    inner: Arc<MemStore>,
    publications: Arc<Mutex<Vec<RecordedPublication>>>,
    supports_hints: bool,
}

impl RecordingSyncStore {
    fn with_hints() -> Self {
        Self {
            supports_hints: true,
            ..Self::default()
        }
    }

    fn take_publications(&self) -> Vec<RecordedPublication> {
        std::mem::take(&mut *self.publications.lock().unwrap())
    }
}

impl Store for RecordingSyncStore {
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
        self.inner.has_native_shared_reads()
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.inner.put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key)
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        self.inner.batch(ops)
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
        self.inner.prefers_batch_reads()
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        self.inner.batch_put(entries)
    }

    fn supports_hints(&self) -> bool {
        self.supports_hints
    }

    fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        self.publications
            .lock()
            .unwrap()
            .push(RecordedPublication::from_request(publication));
        self.inner.publish_nodes(publication)
    }
}

impl ManifestStore for RecordingSyncStore {
    type Error = MemStoreError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        ManifestStore::get_root(&self.inner, name)
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        ManifestStore::put_root(&self.inner, name, manifest)
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        ManifestStore::delete_root(&self.inner, name)
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        ManifestStore::compare_and_swap_root(&self.inner, name, expected, new)
    }
}

impl TransactionalStore for RecordingSyncStore {
    fn supports_transactions(&self) -> bool {
        TransactionalStore::supports_transactions(&self.inner)
    }

    fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, prolly::Error> {
        TransactionalStore::commit_transaction(
            &self.inner,
            node_writes,
            root_conditions,
            root_writes,
        )
    }
}

#[derive(Clone, Default)]
struct RecordingAsyncStore {
    inner: Arc<MemStore>,
    publications: Arc<Mutex<Vec<RecordedPublication>>>,
    supports_hints: bool,
}

impl RecordingAsyncStore {
    fn with_hints() -> Self {
        Self {
            supports_hints: true,
            ..Self::default()
        }
    }

    fn take_publications(&self) -> Vec<RecordedPublication> {
        std::mem::take(&mut *self.publications.lock().unwrap())
    }
}

impl AsyncStore for RecordingAsyncStore {
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
        self.inner.has_native_shared_reads()
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.inner.put(key, value)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key)
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        self.inner.batch(ops)
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
        self.inner.prefers_batch_reads()
    }

    async fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        self.inner.batch_put(entries)
    }

    fn supports_hints(&self) -> bool {
        self.supports_hints
    }

    async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        self.publications
            .lock()
            .unwrap()
            .push(RecordedPublication::from_request(publication));
        self.inner.publish_nodes(publication)
    }
}

#[derive(Default)]
struct NoAllocationPublicationStore {
    publication_batches: AtomicUsize,
}

impl Store for NoAllocationPublicationStore {
    type Error = Infallible;

    fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(None)
    }

    fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }

    fn delete(&self, _key: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }

    fn batch(&self, _ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        Ok(())
    }

    fn batch_put(&self, _entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        self.publication_batches.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Clone, Default)]
struct StrictPublicationStore {
    inner: Arc<MemStore>,
}

impl Store for StrictPublicationStore {
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

    fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        panic!("canonical node publication escaped through Store::put")
    }

    fn delete(&self, _key: &[u8]) -> Result<(), Self::Error> {
        panic!("canonical node publication escaped through Store::delete")
    }

    fn batch(&self, _ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        panic!("canonical node publication escaped through Store::batch")
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

    fn batch_put(&self, _entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        panic!("canonical node publication escaped through Store::batch_put")
    }

    fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        self.inner.publish_nodes(publication)
    }
}

#[derive(Clone)]
struct ControlledPublicationStore {
    inner: Arc<MemStore>,
    fail: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    released: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
    publication_calls: Arc<AtomicUsize>,
}

#[derive(Debug)]
enum ControlledStoreError {
    Inner(MemStoreError),
    InjectedPublicationFailure,
}

impl fmt::Display for ControlledStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inner(error) => error.fmt(formatter),
            Self::InjectedPublicationFailure => formatter.write_str("injected publication failure"),
        }
    }
}

impl std::error::Error for ControlledStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Inner(error) => Some(error),
            Self::InjectedPublicationFailure => None,
        }
    }
}

fn controlled<T>(result: Result<T, MemStoreError>) -> Result<T, ControlledStoreError> {
    result.map_err(ControlledStoreError::Inner)
}

impl Default for ControlledPublicationStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(MemStore::new()),
            fail: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            released: Arc::new(AtomicBool::new(true)),
            started: Arc::new(AtomicBool::new(false)),
            publication_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl ControlledPublicationStore {
    fn set_failure(&self, fail: bool) {
        self.fail.store(fail, Ordering::Release);
    }

    fn pause(&self) {
        self.started.store(false, Ordering::Release);
        self.released.store(false, Ordering::Release);
        self.paused.store(true, Ordering::Release);
    }

    fn release(&self) {
        self.released.store(true, Ordering::Release);
    }

    fn started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    fn publication_calls(&self) -> usize {
        self.publication_calls.load(Ordering::Acquire)
    }

    fn reject_if_configured(&self) -> Result<(), ControlledStoreError> {
        if self.fail.load(Ordering::Acquire) {
            Err(ControlledStoreError::InjectedPublicationFailure)
        } else {
            Ok(())
        }
    }
}

impl Store for ControlledPublicationStore {
    type Error = ControlledStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        controlled(self.inner.get(key))
    }

    fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        controlled(self.inner.get_shared(key))
    }

    fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Arc<[u8]>>>, Self::Error> {
        controlled(self.inner.batch_get_shared_ordered_unique(keys))
    }

    fn has_native_shared_reads(&self) -> bool {
        true
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        controlled(self.inner.put(key, value))
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        controlled(self.inner.delete(key))
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        controlled(self.inner.batch(ops))
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        controlled(self.inner.batch_get_ordered(keys))
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        controlled(self.inner.batch_get_ordered_unique(keys))
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        controlled(self.inner.batch_put(entries))
    }

    fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        self.publication_calls.fetch_add(1, Ordering::AcqRel);
        self.reject_if_configured()?;
        controlled(self.inner.publish_nodes(publication))
    }
}

impl AsyncStore for ControlledPublicationStore {
    type Error = ControlledStoreError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        controlled(Store::get(&*self.inner, key))
    }

    async fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        controlled(Store::get_shared(&*self.inner, key))
    }

    async fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Arc<[u8]>>>, Self::Error> {
        controlled(Store::batch_get_shared_ordered_unique(&*self.inner, keys))
    }

    fn has_native_shared_reads(&self) -> bool {
        true
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        controlled(Store::put(&*self.inner, key, value))
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        controlled(Store::delete(&*self.inner, key))
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        controlled(Store::batch(&*self.inner, ops))
    }

    async fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        controlled(Store::batch_get_ordered(&*self.inner, keys))
    }

    async fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        controlled(Store::batch_get_ordered_unique(&*self.inner, keys))
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    async fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        controlled(Store::batch_put(&*self.inner, entries))
    }

    async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        self.publication_calls.fetch_add(1, Ordering::AcqRel);
        self.started.store(true, Ordering::Release);
        if self.paused.load(Ordering::Acquire) {
            std::future::poll_fn(|_| {
                if self.released.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }
        self.reject_if_configured()?;
        controlled(Store::publish_nodes(&*self.inner, publication))
    }
}

impl ManifestStore for ControlledPublicationStore {
    type Error = ControlledStoreError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        controlled(ManifestStore::get_root(&*self.inner, name))
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        controlled(ManifestStore::put_root(&*self.inner, name, manifest))
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        controlled(ManifestStore::delete_root(&*self.inner, name))
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        controlled(ManifestStore::compare_and_swap_root(
            &*self.inner,
            name,
            expected,
            new,
        ))
    }
}

impl AsyncManifestStore for ControlledPublicationStore {
    type Error = ControlledStoreError;

    async fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        controlled(ManifestStore::get_root(&*self.inner, name))
    }

    async fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        controlled(ManifestStore::put_root(&*self.inner, name, manifest))
    }

    async fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        controlled(ManifestStore::delete_root(&*self.inner, name))
    }

    async fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        controlled(ManifestStore::compare_and_swap_root(
            &*self.inner,
            name,
            expected,
            new,
        ))
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);

    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn mutations(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<Mutation> {
    entries
        .iter()
        .map(|(key, val)| Mutation::Upsert {
            key: key.clone(),
            val: val.clone(),
        })
        .collect()
}

fn sync_pair(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
) -> (
    RecordingSyncStore,
    Prolly<RecordingSyncStore>,
    Tree,
    Prolly<MemStore>,
    Tree,
) {
    let config = Config::default();
    let store = RecordingSyncStore::default();
    let recorded = Prolly::new(store.clone(), config.clone());
    let control = Prolly::new(MemStore::new(), config);
    let recorded_tree = if entries.is_empty() {
        recorded.create()
    } else {
        recorded
            .batch(&recorded.create(), mutations(&entries))
            .unwrap()
    };
    let control_tree = if entries.is_empty() {
        control.create()
    } else {
        control
            .batch(&control.create(), mutations(&entries))
            .unwrap()
    };
    store.take_publications();
    assert_sync_equivalent(&recorded, &recorded_tree, &control, &control_tree);

    (store, recorded, recorded_tree, control, control_tree)
}

fn assert_sync_equivalent(
    recorded: &Prolly<RecordingSyncStore>,
    recorded_tree: &Tree,
    control: &Prolly<MemStore>,
    control_tree: &Tree,
) {
    assert_eq!(recorded_tree, control_tree);
    assert_eq!(
        recorded.export_snapshot(recorded_tree).unwrap(),
        control.export_snapshot(control_tree).unwrap()
    );
}

type AsyncControlStore = SyncStoreAsAsync<Arc<MemStore>>;

async fn async_pair(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
) -> (
    RecordingAsyncStore,
    AsyncProlly<RecordingAsyncStore>,
    Tree,
    AsyncProlly<AsyncControlStore>,
    Tree,
) {
    let config = Config::default();
    let store = RecordingAsyncStore::default();
    let recorded = AsyncProlly::new(store.clone(), config.clone());
    let control = AsyncProlly::new(SyncStoreAsAsync::new(Arc::new(MemStore::new())), config);
    let recorded_tree = if entries.is_empty() {
        recorded.create()
    } else {
        recorded
            .batch(&recorded.create(), mutations(&entries))
            .await
            .unwrap()
    };
    let control_tree = if entries.is_empty() {
        control.create()
    } else {
        control
            .batch(&control.create(), mutations(&entries))
            .await
            .unwrap()
    };
    store.take_publications();
    assert_async_equivalent(&recorded, &recorded_tree, &control, &control_tree).await;

    (store, recorded, recorded_tree, control, control_tree)
}

async fn assert_async_equivalent(
    recorded: &AsyncProlly<RecordingAsyncStore>,
    recorded_tree: &Tree,
    control: &AsyncProlly<AsyncControlStore>,
    control_tree: &Tree,
) {
    assert_eq!(recorded_tree, control_tree);
    assert_eq!(
        recorded.export_snapshot(recorded_tree).await.unwrap(),
        control.export_snapshot(control_tree).await.unwrap()
    );
}

fn assert_publications(publications: Vec<RecordedPublication>, expected: &[PublicationOrigin]) {
    assert_eq!(
        publications
            .iter()
            .map(|publication| publication.origin)
            .collect::<Vec<_>>(),
        expected
    );
    for publication in publications {
        assert!(!publication.entries.is_empty());
        for (key, value) in publication.entries {
            assert_eq!(key.as_slice(), Cid::from_bytes(&value).as_bytes());
        }
    }
}

#[test]
fn publication_request_and_default_dispatch_allocate_nothing() {
    let store = NoAllocationPublicationStore::default();
    let entries = [(b"node".as_slice(), b"bytes".as_slice())];
    let hint = NodePublicationHint::new(b"namespace", b"key", b"value");

    let (unhinted, unhinted_allocations) = allocations_during(|| {
        Store::publish_nodes(
            &store,
            std::hint::black_box(NodePublication::new(
                &entries,
                PublicationOrigin::PointUpsert,
            )),
        )
    });
    unhinted.unwrap();

    let (hinted, hinted_allocations) = allocations_during(|| {
        Store::publish_nodes(
            &store,
            std::hint::black_box(NodePublication::with_hint(
                &entries,
                hint,
                PublicationOrigin::TreeBuild,
            )),
        )
    });
    hinted.unwrap();

    assert_eq!(unhinted_allocations, 0);
    assert_eq!(hinted_allocations, 0);
    assert_eq!(store.publication_batches.load(Ordering::Relaxed), 2);
}

#[test]
fn canonical_sync_writers_use_only_the_classified_publication_boundary() {
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build();
    let store = StrictPublicationStore::default();
    let prolly = Prolly::new(store.clone(), config.clone());

    let built = prolly
        .build_from_entries(vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"b".to_vec(), b"two".to_vec()),
            (b"c".to_vec(), b"three".to_vec()),
            (b"d".to_vec(), b"four".to_vec()),
        ])
        .unwrap();
    let point = prolly.put(&built, b"e".to_vec(), b"five".to_vec()).unwrap();
    let deleted = prolly.delete(&point, b"a").unwrap();
    let batched = prolly
        .batch(
            &deleted,
            vec![
                Mutation::Upsert {
                    key: b"b".to_vec(),
                    val: b"updated".to_vec(),
                },
                Mutation::Upsert {
                    key: b"f".to_vec(),
                    val: b"six".to_vec(),
                },
            ],
        )
        .unwrap();
    let ranged = prolly.delete_range(&batched, b"c", b"e").unwrap();

    let left = prolly
        .put(&ranged, b"left".to_vec(), b"branch".to_vec())
        .unwrap();
    let right = prolly
        .put(&ranged, b"right".to_vec(), b"branch".to_vec())
        .unwrap();
    let merged = prolly.merge(&ranged, &left, &right, None).unwrap();
    assert_eq!(
        prolly.get(&merged, b"left").unwrap(),
        Some(b"branch".to_vec())
    );
    assert_eq!(
        prolly.get(&merged, b"right").unwrap(),
        Some(b"branch".to_vec())
    );

    let copied_store = StrictPublicationStore::default();
    let copied = prolly.copy_missing_nodes(&merged, &copied_store).unwrap();
    assert!(copied.copied_nodes > 0);
    let copied_reader = Prolly::new(copied_store, config.clone());
    assert_eq!(
        copied_reader.export_snapshot(&merged).unwrap(),
        prolly.export_snapshot(&merged).unwrap()
    );

    let bundle = prolly.export_snapshot(&merged).unwrap();
    let imported_store = StrictPublicationStore::default();
    let imported = Prolly::new(imported_store, config);
    let imported_tree = imported.import_snapshot(&bundle).unwrap();
    assert_eq!(imported.export_snapshot(&imported_tree).unwrap(), bundle);
}

#[test]
fn publication_failure_returns_no_tree_and_preserves_the_original() {
    let sync_store = ControlledPublicationStore::default();
    let sync = Prolly::new(sync_store.clone(), Config::default());
    let sync_base = sync
        .put(&sync.create(), b"stable".to_vec(), b"value".to_vec())
        .unwrap();
    sync.publish_named_root(b"main", &sync_base).unwrap();
    sync_store.set_failure(true);

    let sync_error = sync.put(&sync_base, b"new".to_vec(), b"value".to_vec());
    assert!(matches!(sync_error, Err(prolly::Error::Store(_))));
    assert_eq!(
        sync.get(&sync_base, b"stable").unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(sync.load_named_root(b"main").unwrap(), Some(sync_base));

    block_on(async {
        let async_store = ControlledPublicationStore::default();
        let async_prolly = AsyncProlly::new(async_store.clone(), Config::default());
        let async_base = async_prolly
            .put(
                &async_prolly.create(),
                b"stable".to_vec(),
                b"value".to_vec(),
            )
            .await
            .unwrap();
        async_prolly
            .publish_named_root(b"main", &async_base)
            .await
            .unwrap();
        async_store.set_failure(true);

        let async_error = async_prolly
            .put(&async_base, b"new".to_vec(), b"value".to_vec())
            .await;
        assert!(matches!(async_error, Err(prolly::Error::Store(_))));
        assert_eq!(
            async_prolly.get(&async_base, b"stable").await.unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(
            async_prolly.load_named_root(b"main").await.unwrap(),
            Some(async_base)
        );
    });
}

#[test]
fn async_engine_waits_for_publication_ack_and_recovers_after_cancellation() {
    let store = ControlledPublicationStore::default();
    let prolly = AsyncProlly::new(store.clone(), Config::default());
    let base =
        block_on(prolly.put(&prolly.create(), b"stable".to_vec(), b"value".to_vec())).unwrap();
    block_on(prolly.publish_named_root(b"main", &base)).unwrap();

    store.pause();
    let calls_before_ack = store.publication_calls();
    let mut acknowledged = Box::pin(prolly.put(&base, b"acknowledged".to_vec(), b"value".to_vec()));
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        acknowledged.as_mut().poll(&mut context),
        Poll::Pending
    ));
    assert!(store.started());
    assert_eq!(store.publication_calls(), calls_before_ack + 1);
    assert_eq!(
        block_on(prolly.load_named_root(b"main")).unwrap(),
        Some(base.clone())
    );

    store.release();
    let acknowledged_tree = loop {
        match acknowledged.as_mut().poll(&mut context) {
            Poll::Ready(result) => break result.unwrap(),
            Poll::Pending => std::thread::yield_now(),
        }
    };
    drop(acknowledged);
    assert_eq!(
        block_on(prolly.get(&acknowledged_tree, b"acknowledged")).unwrap(),
        Some(b"value".to_vec())
    );

    store.pause();
    let calls_before_cancel = store.publication_calls();
    let mut cancelled =
        Box::pin(prolly.put(&acknowledged_tree, b"cancelled".to_vec(), b"value".to_vec()));
    assert!(matches!(
        cancelled.as_mut().poll(&mut context),
        Poll::Pending
    ));
    assert_eq!(store.publication_calls(), calls_before_cancel + 1);
    drop(cancelled);
    assert_eq!(
        block_on(prolly.load_named_root(b"main")).unwrap(),
        Some(base)
    );

    store.release();
    let recovered =
        block_on(prolly.put(&acknowledged_tree, b"recovered".to_vec(), b"value".to_vec())).unwrap();
    assert_eq!(
        block_on(prolly.get(&recovered, b"recovered")).unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(
        block_on(prolly.get(&recovered, b"cancelled")).unwrap(),
        None
    );
}

#[test]
fn sync_core_routes_origins_without_changing_canonical_content() {
    let (store, recorded, tree, control, control_tree) = sync_pair(Vec::new());
    let tree = recorded
        .put(&tree, b"point".to_vec(), b"value".to_vec())
        .unwrap();
    let control_tree = control
        .put(&control_tree, b"point".to_vec(), b"value".to_vec())
        .unwrap();
    assert_publications(store.take_publications(), &[PublicationOrigin::PointUpsert]);
    assert_sync_equivalent(&recorded, &tree, &control, &control_tree);

    let initial = vec![
        (b"keep".to_vec(), b"stable".to_vec()),
        (b"point".to_vec(), b"value".to_vec()),
    ];
    let (store, recorded, tree, control, control_tree) = sync_pair(initial.clone());
    let tree = recorded.delete(&tree, b"point").unwrap();
    let control_tree = control.delete(&control_tree, b"point").unwrap();
    assert_publications(store.take_publications(), &[PublicationOrigin::PointDelete]);
    assert_sync_equivalent(&recorded, &tree, &control, &control_tree);

    let (store, recorded, tree, control, control_tree) = sync_pair(Vec::new());
    let batch = vec![
        Mutation::Upsert {
            key: b"a".to_vec(),
            val: b"one".to_vec(),
        },
        Mutation::Upsert {
            key: b"b".to_vec(),
            val: b"two".to_vec(),
        },
    ];
    let tree = recorded.batch(&tree, batch.clone()).unwrap();
    let control_tree = control.batch(&control_tree, batch).unwrap();
    assert_publications(
        store.take_publications(),
        &[PublicationOrigin::BatchMutation],
    );
    assert_sync_equivalent(&recorded, &tree, &control, &control_tree);

    let initial = vec![
        (b"a".to_vec(), b"one".to_vec()),
        (b"b".to_vec(), b"two".to_vec()),
        (b"c".to_vec(), b"three".to_vec()),
        (b"d".to_vec(), b"four".to_vec()),
    ];
    let (store, recorded, tree, control, control_tree) = sync_pair(initial.clone());
    let tree = recorded.delete_range(&tree, b"b", b"d").unwrap();
    let control_tree = control.delete_range(&control_tree, b"b", b"d").unwrap();
    assert_publications(store.take_publications(), &[PublicationOrigin::RangeDelete]);
    assert_sync_equivalent(&recorded, &tree, &control, &control_tree);

    let (store, recorded, tree, control, control_tree) =
        sync_pair(vec![(b"point".to_vec(), b"value".to_vec())]);
    let unchanged = recorded
        .put(&tree, b"point".to_vec(), b"value".to_vec())
        .unwrap();
    let control_unchanged = control
        .put(&control_tree, b"point".to_vec(), b"value".to_vec())
        .unwrap();
    assert!(store.take_publications().is_empty());
    assert_sync_equivalent(&recorded, &unchanged, &control, &control_unchanged);
}

#[test]
fn async_core_routes_origins_without_changing_canonical_content() {
    block_on(async {
        let (store, recorded, tree, control, control_tree) = async_pair(Vec::new()).await;
        let tree = recorded
            .put(&tree, b"point".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        let control_tree = control
            .put(&control_tree, b"point".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        assert_publications(store.take_publications(), &[PublicationOrigin::PointUpsert]);
        assert_async_equivalent(&recorded, &tree, &control, &control_tree).await;

        let initial = vec![
            (b"keep".to_vec(), b"stable".to_vec()),
            (b"point".to_vec(), b"value".to_vec()),
        ];
        let (store, recorded, tree, control, control_tree) = async_pair(initial.clone()).await;
        let tree = recorded.delete(&tree, b"point").await.unwrap();
        let control_tree = control.delete(&control_tree, b"point").await.unwrap();
        assert_publications(store.take_publications(), &[PublicationOrigin::PointDelete]);
        assert_async_equivalent(&recorded, &tree, &control, &control_tree).await;

        let (store, recorded, tree, control, control_tree) = async_pair(Vec::new()).await;
        let batch = vec![
            Mutation::Upsert {
                key: b"a".to_vec(),
                val: b"one".to_vec(),
            },
            Mutation::Upsert {
                key: b"b".to_vec(),
                val: b"two".to_vec(),
            },
        ];
        let tree = recorded.batch(&tree, batch.clone()).await.unwrap();
        let control_tree = control.batch(&control_tree, batch).await.unwrap();
        assert_publications(
            store.take_publications(),
            &[PublicationOrigin::BatchMutation],
        );
        assert_async_equivalent(&recorded, &tree, &control, &control_tree).await;

        let initial = vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"b".to_vec(), b"two".to_vec()),
            (b"c".to_vec(), b"three".to_vec()),
            (b"d".to_vec(), b"four".to_vec()),
        ];
        let (store, recorded, tree, control, control_tree) = async_pair(initial.clone()).await;
        let tree = recorded.delete_range(&tree, b"b", b"d").await.unwrap();
        let control_tree = control
            .delete_range(&control_tree, b"b", b"d")
            .await
            .unwrap();
        assert_publications(store.take_publications(), &[PublicationOrigin::RangeDelete]);
        assert_async_equivalent(&recorded, &tree, &control, &control_tree).await;

        let (store, recorded, tree, control, control_tree) =
            async_pair(vec![(b"point".to_vec(), b"value".to_vec())]).await;
        let unchanged = recorded
            .put(&tree, b"point".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        let control_unchanged = control
            .put(&control_tree, b"point".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        assert!(store.take_publications().is_empty());
        assert_async_equivalent(&recorded, &unchanged, &control, &control_unchanged).await;
    });
}

#[test]
fn publication_hint_recording_remains_borrowed_and_lossless() {
    let entries = [(b"node".as_slice(), b"bytes".as_slice())];
    let hint = NodePublicationHint::new(b"namespace", b"key", b"value");
    let recorded = RecordedPublication::from_request(NodePublication::with_hint(
        &entries,
        hint,
        PublicationOrigin::General,
    ));

    assert_eq!(
        recorded.hint,
        Some((b"namespace".to_vec(), b"key".to_vec(), b"value".to_vec()))
    );
}

#[test]
fn sync_hint_capable_point_upserts_publish_the_rightmost_path_atomically() {
    let store = RecordingSyncStore::with_hints();
    let prolly = Prolly::new(store.clone(), Config::default());

    let first = prolly
        .put(&prolly.create(), b"a".to_vec(), b"one".to_vec())
        .unwrap();
    let first_publication = store.take_publications().pop().unwrap();
    assert_eq!(first_publication.origin, PublicationOrigin::PointUpsert);
    let (namespace, key, value) = first_publication.hint.unwrap();
    assert_eq!(namespace, b"prolly:rightmost-path:v1");
    assert_eq!(key.as_slice(), first.root.as_ref().unwrap().as_bytes());
    assert!(!value.is_empty());

    let second = prolly.put(&first, b"b".to_vec(), b"two".to_vec()).unwrap();
    let second_publication = store.take_publications().pop().unwrap();
    assert_eq!(second_publication.origin, PublicationOrigin::PointUpsert);
    let (namespace, key, value) = second_publication.hint.unwrap();
    assert_eq!(namespace, b"prolly:rightmost-path:v1");
    assert_eq!(key.as_slice(), second.root.as_ref().unwrap().as_bytes());
    assert!(!value.is_empty());
}

#[test]
fn async_hint_capable_appends_publish_once_with_the_rightmost_path() {
    block_on(async {
        let store = RecordingAsyncStore::with_hints();
        let prolly = AsyncProlly::new(store.clone(), Config::default());

        let first = prolly
            .put(&prolly.create(), b"a".to_vec(), b"one".to_vec())
            .await
            .unwrap();
        let first_publications = store.take_publications();
        assert_eq!(first_publications.len(), 1);
        assert_eq!(first_publications[0].origin, PublicationOrigin::PointUpsert);
        let (namespace, key, value) = first_publications[0].hint.as_ref().unwrap();
        assert_eq!(namespace, b"prolly:rightmost-path:v1");
        assert_eq!(key.as_slice(), first.root.as_ref().unwrap().as_bytes());
        assert!(!value.is_empty());

        let second = prolly
            .put(&first, b"b".to_vec(), b"two".to_vec())
            .await
            .unwrap();
        let second_publications = store.take_publications();
        assert_eq!(second_publications.len(), 1);
        assert_eq!(
            second_publications[0].origin,
            PublicationOrigin::PointUpsert
        );
        let (namespace, key, value) = second_publications[0].hint.as_ref().unwrap();
        assert_eq!(namespace, b"prolly:rightmost-path:v1");
        assert_eq!(key.as_slice(), second.root.as_ref().unwrap().as_bytes());
        assert!(!value.is_empty());
    });
}

#[test]
fn sync_build_copy_and_import_have_reviewed_origins() {
    let entries = vec![
        (b"a".to_vec(), b"one".to_vec()),
        (b"b".to_vec(), b"two".to_vec()),
        (b"c".to_vec(), b"three".to_vec()),
    ];
    let config = Config::default();
    let build_store = RecordingSyncStore::default();
    let recorded = Prolly::new(build_store.clone(), config.clone());
    let control = Prolly::new(MemStore::new(), config.clone());

    let recorded_tree = recorded.build_from_entries(entries.clone()).unwrap();
    let control_tree = control.build_from_entries(entries).unwrap();
    assert_publications(
        build_store.take_publications(),
        &[PublicationOrigin::TreeBuild],
    );
    assert_sync_equivalent(&recorded, &recorded_tree, &control, &control_tree);

    let copy_store = RecordingSyncStore::default();
    let copy = control
        .copy_missing_nodes(&control_tree, &copy_store)
        .unwrap();
    assert!(copy.copied_nodes > 0);
    assert_publications(
        copy_store.take_publications(),
        &[PublicationOrigin::Replication],
    );
    let copied = Prolly::new(copy_store, config.clone());
    assert_eq!(
        copied.export_snapshot(&control_tree).unwrap(),
        control.export_snapshot(&control_tree).unwrap()
    );

    let bundle = control.export_snapshot(&control_tree).unwrap();
    let import_store = RecordingSyncStore::default();
    let imported = Prolly::new(import_store.clone(), config);
    let imported_tree = imported.import_snapshot(&bundle).unwrap();
    assert_publications(
        import_store.take_publications(),
        &[PublicationOrigin::Replication],
    );
    assert_eq!(imported.export_snapshot(&imported_tree).unwrap(), bundle);
}

#[test]
fn async_build_copy_and_import_have_reviewed_origins() {
    block_on(async {
        let entries = vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"b".to_vec(), b"two".to_vec()),
            (b"c".to_vec(), b"three".to_vec()),
        ];
        let config = Config::default();
        let build_store = RecordingAsyncStore::default();
        let recorded = AsyncProlly::new(build_store.clone(), config.clone());
        let control = AsyncProlly::new(
            SyncStoreAsAsync::new(Arc::new(MemStore::new())),
            config.clone(),
        );

        let recorded_tree = recorded.build_from_entries(entries.clone()).await.unwrap();
        let control_tree = control.build_from_entries(entries).await.unwrap();
        assert_publications(
            build_store.take_publications(),
            &[PublicationOrigin::TreeBuild],
        );
        assert_async_equivalent(&recorded, &recorded_tree, &control, &control_tree).await;

        let copy_store = RecordingAsyncStore::default();
        let copy = control
            .copy_missing_nodes(&control_tree, &copy_store)
            .await
            .unwrap();
        assert!(copy.copied_nodes > 0);
        assert_publications(
            copy_store.take_publications(),
            &[PublicationOrigin::Replication],
        );
        let copied = AsyncProlly::new(copy_store, config.clone());
        assert_eq!(
            copied.export_snapshot(&control_tree).await.unwrap(),
            control.export_snapshot(&control_tree).await.unwrap()
        );

        let bundle = control.export_snapshot(&control_tree).await.unwrap();
        let import_store = RecordingAsyncStore::default();
        let imported = AsyncProlly::new(import_store.clone(), config);
        let imported_tree = imported.import_snapshot(&bundle).await.unwrap();
        assert_publications(
            import_store.take_publications(),
            &[PublicationOrigin::Replication],
        );
        assert_eq!(
            imported.export_snapshot(&imported_tree).await.unwrap(),
            bundle
        );
    });
}

#[test]
fn transparent_search_wrappers_preserve_publication_exactly() {
    let bytes = b"published-node";
    let cid = Cid::from_bytes(bytes);
    let entries = [(cid.as_bytes(), bytes.as_slice())];
    let hint = NodePublicationHint::new(b"namespace", b"key", b"value");
    let expected = RecordedPublication::from_request(NodePublication::with_hint(
        &entries,
        hint,
        PublicationOrigin::TreeBuild,
    ));

    let sync_store = RecordingSyncStore::default();
    let sync_io = SearchIo::new(sync_store.clone(), Arc::new(SearchRuntime::default()));
    Store::publish_nodes(
        &sync_io,
        NodePublication::with_hint(&entries, hint, PublicationOrigin::TreeBuild),
    )
    .unwrap();
    assert_eq!(sync_store.take_publications(), vec![expected.clone()]);

    let async_store = RecordingAsyncStore::default();
    let async_io = SearchIo::new(async_store.clone(), Arc::new(SearchRuntime::default()));
    block_on(AsyncStore::publish_nodes(
        &async_io,
        NodePublication::with_hint(&entries, hint, PublicationOrigin::TreeBuild),
    ))
    .unwrap();
    assert_eq!(async_store.take_publications(), vec![expected]);
}

#[test]
fn standalone_sync_and_async_builders_publish_only_tree_builds() {
    let config = Config::default();
    let sync_store = RecordingSyncStore::default();
    let mut sync_builder = BatchBuilder::new(sync_store.clone(), config.clone());
    sync_builder.add(b"a".to_vec(), b"one".to_vec());
    sync_builder.add(b"b".to_vec(), b"two".to_vec());
    sync_builder.add(b"c".to_vec(), b"three".to_vec());
    let sync_tree = sync_builder.build().unwrap();
    let sync_publications = sync_store.take_publications();
    let expected = vec![PublicationOrigin::TreeBuild; sync_publications.len()];
    assert!(!expected.is_empty());
    assert_publications(sync_publications, &expected);
    let sync_reader = Prolly::new(sync_store, config.clone());
    assert_eq!(
        sync_reader.get(&sync_tree, b"b").unwrap(),
        Some(b"two".to_vec())
    );

    block_on(async {
        let async_store = RecordingAsyncStore::default();
        let mut async_builder = AsyncSortedBatchBuilder::new(async_store.clone(), config);
        async_builder
            .add(b"a".to_vec(), b"one".to_vec())
            .await
            .unwrap();
        async_builder
            .add(b"b".to_vec(), b"two".to_vec())
            .await
            .unwrap();
        async_builder
            .add(b"c".to_vec(), b"three".to_vec())
            .await
            .unwrap();
        let async_tree = async_builder.build().await.unwrap();
        let async_publications = async_store.take_publications();
        let expected = vec![PublicationOrigin::TreeBuild; async_publications.len()];
        assert!(!expected.is_empty());
        assert_publications(async_publications, &expected);
        let async_reader = AsyncProlly::new(async_store, async_tree.config.clone());
        assert_eq!(
            async_reader.get(&async_tree, b"b").await.unwrap(),
            Some(b"two".to_vec())
        );
    });
}

fn merge_config() -> Config {
    Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build()
}

#[test]
fn sync_structural_and_fallback_merge_publish_only_merge_nodes() {
    let config = merge_config();
    let store = RecordingSyncStore::default();
    let recorded = Prolly::new(store.clone(), config.clone());
    let control = Prolly::new(MemStore::new(), config.clone());

    let entries = vec![
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"1".to_vec()),
        (b"c".to_vec(), b"1".to_vec()),
    ];
    let base = recorded.build_from_entries(entries.clone()).unwrap();
    let left = recorded
        .put(&base, b"a".to_vec(), b"left".to_vec())
        .unwrap();
    let right = recorded
        .put(&base, b"b".to_vec(), b"right".to_vec())
        .unwrap();
    let control_base = control.build_from_entries(entries).unwrap();
    let control_left = control
        .put(&control_base, b"a".to_vec(), b"left".to_vec())
        .unwrap();
    let control_right = control
        .put(&control_base, b"b".to_vec(), b"right".to_vec())
        .unwrap();
    store.take_publications();

    let control_explanation =
        control.merge_explain(&control_base, &control_left, &control_right, None);
    assert!(control_explanation
        .trace
        .events
        .iter()
        .any(|event| matches!(event, MergeTraceEvent::RewrittenNode { .. })));
    let control_merged = control_explanation.result.unwrap();
    let merged = recorded.merge(&base, &left, &right, None).unwrap();
    let publications = store.take_publications();
    assert!(!publications.is_empty());
    let expected = vec![PublicationOrigin::Merge; publications.len()];
    assert_publications(publications, &expected);
    assert_sync_equivalent(&recorded, &merged, &control, &control_merged);

    let fallback_store = RecordingSyncStore::default();
    let recorded = Prolly::new(fallback_store.clone(), config.clone());
    let control = Prolly::new(MemStore::new(), config);
    let base = recorded
        .put(&recorded.create(), b"k".to_vec(), b"base".to_vec())
        .unwrap();
    let left = recorded
        .put(&base, b"k".to_vec(), b"left".to_vec())
        .unwrap();
    let left = recorded
        .put(&left, b"z".to_vec(), b"keep".to_vec())
        .unwrap();
    let right = recorded
        .put(&base, b"k".to_vec(), b"right".to_vec())
        .unwrap();
    let control_base = control
        .put(&control.create(), b"k".to_vec(), b"base".to_vec())
        .unwrap();
    let control_left = control
        .put(&control_base, b"k".to_vec(), b"left".to_vec())
        .unwrap();
    let control_left = control
        .put(&control_left, b"z".to_vec(), b"keep".to_vec())
        .unwrap();
    let control_right = control
        .put(&control_base, b"k".to_vec(), b"right".to_vec())
        .unwrap();
    fallback_store.take_publications();

    let control_explanation = control.merge_explain(
        &control_base,
        &control_left,
        &control_right,
        Some(Box::new(|_| Resolution::delete())),
    );
    assert!(control_explanation
        .trace
        .events
        .iter()
        .any(|event| matches!(event, MergeTraceEvent::Fallback { .. })));
    let control_merged = control_explanation.result.unwrap();
    let merged = recorded
        .merge(
            &base,
            &left,
            &right,
            Some(Box::new(|_| Resolution::delete())),
        )
        .unwrap();
    let publications = fallback_store.take_publications();
    assert!(!publications.is_empty());
    let expected = vec![PublicationOrigin::Merge; publications.len()];
    assert_publications(publications, &expected);
    assert_sync_equivalent(&recorded, &merged, &control, &control_merged);
}

#[test]
fn async_structural_and_fallback_merge_publish_only_merge_nodes() {
    block_on(async {
        let config = merge_config();
        let store = RecordingAsyncStore::default();
        let recorded = AsyncProlly::new(store.clone(), config.clone());
        let control = AsyncProlly::new(
            SyncStoreAsAsync::new(Arc::new(MemStore::new())),
            config.clone(),
        );
        let entries = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"1".to_vec()),
        ];
        let base = recorded.build_from_entries(entries.clone()).await.unwrap();
        let left = recorded
            .put(&base, b"a".to_vec(), b"left".to_vec())
            .await
            .unwrap();
        let right = recorded
            .put(&base, b"b".to_vec(), b"right".to_vec())
            .await
            .unwrap();
        let control_base = control.build_from_entries(entries).await.unwrap();
        let control_left = control
            .put(&control_base, b"a".to_vec(), b"left".to_vec())
            .await
            .unwrap();
        let control_right = control
            .put(&control_base, b"b".to_vec(), b"right".to_vec())
            .await
            .unwrap();
        store.take_publications();

        let merged = recorded.merge(&base, &left, &right, None).await.unwrap();
        let control_merged = control
            .merge(&control_base, &control_left, &control_right, None)
            .await
            .unwrap();
        let publications = store.take_publications();
        assert!(!publications.is_empty());
        let expected = vec![PublicationOrigin::Merge; publications.len()];
        assert_publications(publications, &expected);
        assert_async_equivalent(&recorded, &merged, &control, &control_merged).await;

        let fallback_store = RecordingAsyncStore::default();
        let recorded = AsyncProlly::new(fallback_store.clone(), config.clone());
        let control = AsyncProlly::new(SyncStoreAsAsync::new(Arc::new(MemStore::new())), config);
        let base = recorded
            .put(&recorded.create(), b"k".to_vec(), b"base".to_vec())
            .await
            .unwrap();
        let left = recorded
            .put(&base, b"k".to_vec(), b"left".to_vec())
            .await
            .unwrap();
        let left = recorded
            .put(&left, b"z".to_vec(), b"keep".to_vec())
            .await
            .unwrap();
        let right = recorded
            .put(&base, b"k".to_vec(), b"right".to_vec())
            .await
            .unwrap();
        let control_base = control
            .put(&control.create(), b"k".to_vec(), b"base".to_vec())
            .await
            .unwrap();
        let control_left = control
            .put(&control_base, b"k".to_vec(), b"left".to_vec())
            .await
            .unwrap();
        let control_left = control
            .put(&control_left, b"z".to_vec(), b"keep".to_vec())
            .await
            .unwrap();
        let control_right = control
            .put(&control_base, b"k".to_vec(), b"right".to_vec())
            .await
            .unwrap();
        fallback_store.take_publications();

        let merged = recorded
            .merge(
                &base,
                &left,
                &right,
                Some(Box::new(|_| Resolution::delete())),
            )
            .await
            .unwrap();
        let control_merged = control
            .merge(
                &control_base,
                &control_left,
                &control_right,
                Some(Box::new(|_| Resolution::delete())),
            )
            .await
            .unwrap();
        let publications = fallback_store.take_publications();
        assert!(!publications.is_empty());
        let expected = vec![PublicationOrigin::Merge; publications.len()];
        assert_publications(publications, &expected);
        assert_async_equivalent(&recorded, &merged, &control, &control_merged).await;
    });
}

fn proximity_config() -> ProximityConfig {
    let mut config = ProximityConfig::new(2);
    config.metric = DistanceMetric::L2Squared;
    config.hierarchy.log_chunk_size = 1;
    config.hierarchy.level_hash_seed = 7;
    config
}

fn proximity_records() -> Vec<ProximityRecord> {
    (0..32)
        .map(|index| ProximityRecord {
            key: format!("key-{index:03}").into_bytes(),
            vector: vec![index as f32, (index % 7) as f32],
            value: format!("value-{index}").into_bytes(),
        })
        .collect()
}

#[test]
fn proximity_build_and_mutation_publish_only_maintenance_nodes() {
    let store = RecordingSyncStore::default();
    let records = proximity_records();
    let map = ProximityMap::build(store.clone(), proximity_config(), records.clone()).unwrap();
    let control =
        ProximityMap::build(Arc::new(MemStore::new()), proximity_config(), records).unwrap();
    assert_eq!(map.tree(), control.tree());
    let publications = store.take_publications();
    assert!(!publications.is_empty());
    let expected = vec![PublicationOrigin::Maintenance; publications.len()];
    assert_publications(publications, &expected);

    let mutation = ProximityMutation {
        key: b"key-010".to_vec(),
        value: Some((vec![10.25, 3.0], b"moved".to_vec())),
    };
    let (mutated, _) = map.mutate_batch([mutation.clone()]).unwrap();
    let (control_mutated, _) = control.mutate_batch([mutation]).unwrap();
    assert_eq!(mutated.tree(), control_mutated.tree());
    let publications = store.take_publications();
    assert!(!publications.is_empty());
    let expected = vec![PublicationOrigin::Maintenance; publications.len()];
    assert_publications(publications, &expected);
}

#[test]
fn secondary_index_build_and_edit_publish_only_maintenance_nodes() {
    let store = RecordingSyncStore::default();
    let prolly = Prolly::new(store.clone(), Config::default());
    let source = prolly.versioned_map(b"users");
    source.put(b"user-1", b"active").unwrap();
    store.take_publications();

    let by_status =
        SecondaryIndex::non_unique("by-status", 1, "tests.users.by-status/v1", |_, value| {
            Ok(vec![value.to_vec()])
        })
        .unwrap();
    let registry = SecondaryIndexRegistry::new().register(by_status).unwrap();
    let indexed = prolly.indexed_map(b"users", registry).unwrap();
    indexed.ensure_index(b"by-status").unwrap();
    let publications = store.take_publications();
    assert!(!publications.is_empty());
    let expected = vec![PublicationOrigin::Maintenance; publications.len()];
    assert_publications(publications, &expected);

    indexed
        .edit(|edit| {
            edit.put(b"user-2", b"inactive");
        })
        .unwrap();
    let publications = store.take_publications();
    assert!(!publications.is_empty());
    let expected = vec![PublicationOrigin::Maintenance; publications.len()];
    assert_publications(publications, &expected);

    let snapshot = indexed.snapshot().unwrap();
    let verification = indexed.verify_all(&snapshot.id().source_version).unwrap();
    assert!(verification.iter().all(|result| result.is_valid()));
}
