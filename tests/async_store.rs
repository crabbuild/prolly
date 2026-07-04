#![cfg(feature = "async-store")]

mod common;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use common::{
    assert_async_manifest_store_contract, assert_async_store_contract, assert_tree_invariants,
};
use futures_util::StreamExt as _;
use prolly::{
    AsyncBlobStore, AsyncProlly, BatchBuilder, BatchOp, BlobRef, BlobStore, Cid, Config,
    CrdtConfig, CrdtResolution, DeletePolicy, Diff, Error, LargeValueConfig, MemBlobStore,
    MemBlobStoreError, MemStore, MemStoreError, MultiValueSet, Mutation, NamedRootRetention,
    NamedRootUpdate, Prolly, RangeCursor, Resolution, ReverseCursor, Store, SyncBlobStoreAsAsync,
    SyncStoreAsAsync, TimestampedValue, ValueRef,
};
#[cfg(feature = "tokio")]
use prolly::{AsyncStore, TokioBlockingBlobStore, TokioBlockingStore};

const EXPECTED_ASYNC_NODE_PREFETCH_BATCH_CAP: usize = 64;

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = futures_util::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(future);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[cfg(feature = "tokio")]
fn tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .unwrap()
}

struct YieldOnce {
    yielded: bool,
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

async fn assert_async_blob_store_contract<B>(store: &B)
where
    B: AsyncBlobStore,
    B::Error: std::fmt::Debug,
{
    let reference = store.put_blob(b"payload").await.unwrap();
    let duplicate = store.put_blob(b"payload").await.unwrap();
    assert_eq!(reference, duplicate);
    assert_eq!(
        store.get_blob(&reference).await.unwrap(),
        Some(b"payload".to_vec())
    );

    let missing = BlobRef::from_bytes(b"missing");
    let values = store
        .get_blobs_ordered(&[reference.clone(), missing.clone(), reference.clone()])
        .await
        .unwrap();
    assert_eq!(
        values,
        vec![Some(b"payload".to_vec()), None, Some(b"payload".to_vec())]
    );

    store.delete_blob(&reference).await.unwrap();
    assert_eq!(store.get_blob(&reference).await.unwrap(), None);
    store.delete_blob(&missing).await.unwrap();
}

struct ParallelBlobReadStore {
    inner: MemBlobStore,
    get_calls: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    read_parallelism: usize,
}

impl ParallelBlobReadStore {
    fn new(read_parallelism: usize) -> Self {
        Self {
            inner: MemBlobStore::new(),
            get_calls: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            read_parallelism,
        }
    }
}

impl AsyncBlobStore for ParallelBlobReadStore {
    type Error = MemBlobStoreError;

    async fn get_blob(&self, reference: &BlobRef) -> Result<Option<Vec<u8>>, Self::Error> {
        self.get_calls.fetch_add(1, Ordering::Relaxed);
        let current = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_in_flight.fetch_max(current, Ordering::Relaxed);

        YieldOnce { yielded: false }.await;

        let value = self.inner.get_blob(reference)?;
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        Ok(value)
    }

    async fn put_blob(&self, bytes: &[u8]) -> Result<BlobRef, Self::Error> {
        self.inner.put_blob(bytes)
    }

    async fn delete_blob(&self, reference: &BlobRef) -> Result<(), Self::Error> {
        self.inner.delete_blob(reference)
    }

    fn read_parallelism(&self) -> usize {
        self.read_parallelism
    }
}

type HintMap = BTreeMap<(Vec<u8>, Vec<u8>), Vec<u8>>;

#[derive(Default)]
struct CountingBatchStore {
    inner: MemStore,
    hints: Mutex<HintMap>,
    get_calls: AtomicUsize,
    put_calls: AtomicUsize,
    batch_put_calls: AtomicUsize,
    batch_put_with_hint_calls: AtomicUsize,
    get_hint_calls: AtomicUsize,
    put_hint_calls: AtomicUsize,
    max_batch_put_len: AtomicUsize,
    batch_get_ordered_unique_calls: AtomicUsize,
    max_batch_get_ordered_unique_len: AtomicUsize,
}

impl CountingBatchStore {
    fn reset_read_counts(&self) {
        self.get_calls.store(0, Ordering::Relaxed);
        self.batch_get_ordered_unique_calls
            .store(0, Ordering::Relaxed);
        self.max_batch_get_ordered_unique_len
            .store(0, Ordering::Relaxed);
        self.get_hint_calls.store(0, Ordering::Relaxed);
    }

    fn reset_write_counts(&self) {
        self.put_calls.store(0, Ordering::Relaxed);
        self.batch_put_calls.store(0, Ordering::Relaxed);
        self.batch_put_with_hint_calls.store(0, Ordering::Relaxed);
        self.put_hint_calls.store(0, Ordering::Relaxed);
        self.max_batch_put_len.store(0, Ordering::Relaxed);
    }
}

impl Store for CountingBatchStore {
    type Error = MemStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.get_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.put_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key)
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        self.inner.batch(ops)
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        self.batch_put_calls.fetch_add(1, Ordering::Relaxed);
        self.max_batch_put_len
            .fetch_max(entries.len(), Ordering::Relaxed);
        self.inner.batch_put(entries)
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.batch_get_ordered_unique_calls
            .fetch_add(1, Ordering::Relaxed);
        self.max_batch_get_ordered_unique_len
            .fetch_max(keys.len(), Ordering::Relaxed);
        self.inner.batch_get_ordered_unique(keys)
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    fn supports_hints(&self) -> bool {
        true
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.get_hint_calls.fetch_add(1, Ordering::Relaxed);
        Ok(self
            .hints
            .lock()
            .unwrap()
            .get(&(namespace.to_vec(), key.to_vec()))
            .cloned())
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.put_hint_calls.fetch_add(1, Ordering::Relaxed);
        self.hints
            .lock()
            .unwrap()
            .insert((namespace.to_vec(), key.to_vec()), value.to_vec());
        Ok(())
    }

    fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.batch_put_with_hint_calls
            .fetch_add(1, Ordering::Relaxed);
        self.batch_put(entries)?;
        self.put_hint(namespace, key, value)
    }
}

fn build_wide_tree(
    store: Arc<CountingBatchStore>,
    config: &Config,
    entries: usize,
) -> prolly::Tree {
    let mut builder = BatchBuilder::new(store, config.clone());
    for idx in 0..entries {
        builder.add(
            format!("k{idx:05}").into_bytes(),
            format!("value-{idx:05}").into_bytes(),
        );
    }
    builder.build().unwrap()
}

#[test]
fn sync_store_as_async_satisfies_async_store_contract() {
    let store = SyncStoreAsAsync::new(MemStore::new());
    block_on(assert_async_store_contract(&store));
}

#[test]
fn sync_store_as_async_satisfies_async_manifest_store_contract() {
    let store = SyncStoreAsAsync::new(MemStore::new());
    block_on(assert_async_manifest_store_contract(&store));
}

#[test]
fn async_arc_adapter_satisfies_async_store_contract() {
    let store = Arc::new(SyncStoreAsAsync::new(MemStore::new()));
    block_on(assert_async_store_contract(&store));
}

#[test]
fn async_arc_adapter_satisfies_async_manifest_store_contract() {
    let store = Arc::new(SyncStoreAsAsync::new(MemStore::new()));
    block_on(assert_async_manifest_store_contract(&store));
}

#[test]
fn async_prolly_named_root_helpers_publish_load_cas_delete_and_select() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), Config::default());

        let empty = async_prolly.create();
        let first = async_prolly
            .put(&empty, b"project/name".to_vec(), b"crabdb".to_vec())
            .await
            .unwrap();
        let second = async_prolly
            .put(&first, b"project/name".to_vec(), b"prolly-map".to_vec())
            .await
            .unwrap();
        let third = async_prolly
            .put(&second, b"project/name".to_vec(), b"remote-ready".to_vec())
            .await
            .unwrap();

        assert_eq!(async_prolly.load_named_root(b"main").await.unwrap(), None);

        async_prolly
            .publish_named_root_at_millis(b"main", &first, 100)
            .await
            .unwrap();
        assert_eq!(
            async_prolly.load_named_root(b"main").await.unwrap(),
            Some(first.clone())
        );

        let conflict = async_prolly
            .compare_and_swap_named_root_at_millis(b"main", Some(&empty), Some(&second), 150)
            .await
            .unwrap();
        assert_eq!(
            conflict,
            NamedRootUpdate::Conflict {
                current: Some(first.clone())
            }
        );

        assert!(async_prolly
            .compare_and_swap_named_root_at_millis(b"main", Some(&first), Some(&second), 200)
            .await
            .unwrap()
            .is_applied());
        assert_eq!(
            async_prolly.load_named_root(b"main").await.unwrap(),
            Some(second.clone())
        );

        async_prolly
            .publish_named_root_at_millis(b"checkpoint/0001", &first, 100)
            .await
            .unwrap();
        async_prolly
            .publish_named_root_at_millis(b"checkpoint/0002", &second, 200)
            .await
            .unwrap();
        async_prolly
            .publish_named_root_at_millis(b"checkpoint/0003", &third, 300)
            .await
            .unwrap();

        let manifest = async_prolly
            .list_named_root_manifests()
            .await
            .unwrap()
            .into_iter()
            .find(|root| root.name == b"main")
            .unwrap()
            .manifest;
        assert_eq!(manifest.created_at_millis, Some(100));
        assert_eq!(manifest.updated_at_millis, Some(200));

        let exact = async_prolly
            .load_named_roots(vec![
                b"checkpoint/0002".as_slice(),
                b"missing".as_slice(),
                b"checkpoint/0002".as_slice(),
            ])
            .await
            .unwrap();
        assert_eq!(
            exact
                .roots
                .iter()
                .map(|root| root.name.clone())
                .collect::<Vec<_>>(),
            vec![b"checkpoint/0002".to_vec()]
        );
        assert_eq!(exact.missing_names, vec![b"missing".to_vec()]);

        let newest = async_prolly
            .load_retained_named_roots(&NamedRootRetention::newest_by_name(b"checkpoint/", 2))
            .await
            .unwrap();
        assert_eq!(
            newest
                .roots
                .iter()
                .map(|root| root.name.clone())
                .collect::<Vec<_>>(),
            vec![b"checkpoint/0002".to_vec(), b"checkpoint/0003".to_vec()]
        );

        let recent = async_prolly
            .load_retained_named_roots(&NamedRootRetention::updated_since(b"checkpoint/", 250))
            .await
            .unwrap();
        assert_eq!(
            recent
                .roots
                .iter()
                .map(|root| root.name.clone())
                .collect::<Vec<_>>(),
            vec![b"checkpoint/0003".to_vec()]
        );

        assert!(async_prolly
            .compare_and_swap_named_root_at_millis(b"main", Some(&second), None, 350)
            .await
            .unwrap()
            .is_applied());
        assert_eq!(async_prolly.load_named_root(b"main").await.unwrap(), None);

        async_prolly
            .publish_named_root(b"main", &first)
            .await
            .unwrap();
        async_prolly.delete_named_root(b"main").await.unwrap();
        assert_eq!(async_prolly.load_named_root(b"main").await.unwrap(), None);
    });
}

#[test]
fn sync_blob_store_as_async_satisfies_async_blob_store_contract() {
    let store = SyncBlobStoreAsAsync::new(MemBlobStore::new());
    block_on(assert_async_blob_store_contract(&store));
}

#[test]
fn async_blob_arc_adapter_satisfies_async_blob_store_contract() {
    let store = Arc::new(SyncBlobStoreAsAsync::new(MemBlobStore::new()));
    block_on(assert_async_blob_store_contract(&store));
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_blocking_blob_store_satisfies_async_blob_store_contract() {
    let runtime = tokio_runtime();
    let store = TokioBlockingBlobStore::new(MemBlobStore::new());
    runtime.block_on(assert_async_blob_store_contract(&store));
}

#[test]
fn async_blob_default_ordered_reads_deduplicate_and_overlap() {
    let store = ParallelBlobReadStore::new(2);
    let first = block_on(store.put_blob(b"first")).unwrap();
    let second = block_on(store.put_blob(b"second")).unwrap();
    let third = block_on(store.put_blob(b"third")).unwrap();
    let missing = BlobRef::from_bytes(b"missing");

    let refs = vec![first.clone(), second.clone(), first, missing, third.clone()];
    let values = block_on(store.get_blobs_ordered(&refs)).unwrap();

    assert_eq!(
        values,
        vec![
            Some(b"first".to_vec()),
            Some(b"second".to_vec()),
            Some(b"first".to_vec()),
            None,
            Some(b"third".to_vec())
        ]
    );
    assert_eq!(
        store.get_calls.load(Ordering::Relaxed),
        4,
        "duplicate blob references should be fetched once"
    );
    assert_eq!(
        store.max_in_flight.load(Ordering::Relaxed),
        2,
        "default ordered reads should respect async blob read_parallelism"
    );
}

#[test]
fn async_prolly_large_value_helpers_round_trip_with_async_blob_store() {
    let node_store = Arc::new(MemStore::new());
    let prolly = AsyncProlly::new(SyncStoreAsAsync::new(node_store), Config::default());
    let blob_store = SyncBlobStoreAsAsync::new(MemBlobStore::new());
    let policy = LargeValueConfig::new(4);
    let large = b"large async blob payload".to_vec();

    let tree = prolly.create();
    let tree = block_on(prolly.put_large_value(
        &blob_store,
        &tree,
        b"small".to_vec(),
        b"tiny".to_vec(),
        policy.clone(),
    ))
    .unwrap();
    let tree = block_on(prolly.put_large_value(
        &blob_store,
        &tree,
        b"large".to_vec(),
        large.clone(),
        policy,
    ))
    .unwrap();

    assert_eq!(
        block_on(prolly.get_large_value(&blob_store, &tree, b"small")).unwrap(),
        Some(b"tiny".to_vec())
    );
    assert_eq!(
        block_on(prolly.get_large_value(&blob_store, &tree, b"large")).unwrap(),
        Some(large)
    );

    let stored = block_on(prolly.get_value_ref(&tree, b"large"))
        .unwrap()
        .unwrap();
    assert!(matches!(stored, ValueRef::Blob(_)));
}

#[test]
fn async_blob_gc_sweeps_only_unreachable_offloaded_values() {
    let node_store = Arc::new(MemStore::new());
    let prolly = AsyncProlly::new(SyncStoreAsAsync::new(node_store), Config::default());
    let blob_store = SyncBlobStoreAsAsync::new(MemBlobStore::new());
    let policy = LargeValueConfig::new(1);
    let old_value = b"old async payload".to_vec();
    let new_value = b"new async payload".to_vec();

    let base = prolly.create();
    let base = block_on(prolly.put_large_value(
        &blob_store,
        &base,
        b"k".to_vec(),
        old_value.clone(),
        policy.clone(),
    ))
    .unwrap();
    let ValueRef::Blob(old_ref) = block_on(prolly.get_value_ref(&base, b"k"))
        .unwrap()
        .unwrap()
    else {
        panic!("old value should be offloaded");
    };

    let current = block_on(prolly.put_large_value(
        &blob_store,
        &base,
        b"k".to_vec(),
        new_value.clone(),
        policy,
    ))
    .unwrap();
    let ValueRef::Blob(new_ref) = block_on(prolly.get_value_ref(&current, b"k"))
        .unwrap()
        .unwrap()
    else {
        panic!("new value should be offloaded");
    };

    let candidates = vec![old_ref.clone(), new_ref.clone()];
    let plan =
        block_on(prolly.plan_blob_gc(&blob_store, std::slice::from_ref(&current), &candidates))
            .unwrap();
    assert_eq!(plan.reclaimable_blobs(), std::slice::from_ref(&old_ref));

    let sweep =
        block_on(prolly.sweep_blob_gc(&blob_store, std::slice::from_ref(&current), &candidates))
            .unwrap();

    assert_eq!(sweep.deleted_blobs, 1);
    assert_eq!(sweep.deleted_blob_bytes, old_value.len() as u64);
    assert_eq!(block_on(blob_store.get_blob(&old_ref)).unwrap(), None);
    assert_eq!(
        block_on(blob_store.get_blob(&new_ref)).unwrap(),
        Some(new_value.clone())
    );
    assert_eq!(
        block_on(prolly.get_large_value(&blob_store, &current, b"k")).unwrap(),
        Some(new_value)
    );
}

#[test]
fn async_prolly_mutations_create_sync_readable_tree() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(17)
        .build();
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config.clone());
    let sync_prolly = Prolly::new(store.clone(), config.clone());

    let mut expected = BTreeMap::new();
    let mut tree = async_prolly.create();

    for idx in 0..32 {
        let key = format!("k{idx:03}").into_bytes();
        let value = format!("v{idx:03}").into_bytes();
        expected.insert(key.clone(), value.clone());
        tree = block_on(async_prolly.put(&tree, key, value)).unwrap();
    }

    for idx in (0..32).step_by(5) {
        let key = format!("k{idx:03}").into_bytes();
        expected.remove(&key);
        tree = block_on(async_prolly.delete(&tree, &key)).unwrap();
    }

    let batch = vec![
        Mutation::Upsert {
            key: b"k007".to_vec(),
            val: b"updated".to_vec(),
        },
        Mutation::Delete {
            key: b"k009".to_vec(),
        },
        Mutation::Upsert {
            key: b"k100".to_vec(),
            val: b"new".to_vec(),
        },
        Mutation::Upsert {
            key: b"k100".to_vec(),
            val: b"newer".to_vec(),
        },
    ];
    expected.insert(b"k007".to_vec(), b"updated".to_vec());
    expected.remove(b"k009".as_slice());
    expected.insert(b"k100".to_vec(), b"newer".to_vec());

    tree = block_on(async_prolly.batch(&tree, batch)).unwrap();

    assert_eq!(
        block_on(async_prolly.get(&tree, b"k007")).unwrap(),
        Some(b"updated".to_vec())
    );
    assert_eq!(block_on(async_prolly.get(&tree, b"k009")).unwrap(), None);
    assert_eq!(
        block_on(async_prolly.get(&tree, b"k100")).unwrap(),
        Some(b"newer".to_vec())
    );

    let actual = sync_prolly
        .range(&tree, &[], None)
        .unwrap()
        .collect::<Result<BTreeMap<_, _>, _>>()
        .unwrap();
    assert_eq!(actual, expected);
    assert_tree_invariants(&store, &tree, &config);
}

#[test]
fn async_batch_flushes_rebuilt_tree_once_and_matches_batch_semantics() {
    let store = Arc::new(CountingBatchStore::default());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(19)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config.clone());
    let mut tree = sync_prolly.create();
    let mut expected = BTreeMap::new();

    for idx in 0..16 {
        let key = format!("k{idx:03}").into_bytes();
        let value = format!("v{idx:03}").into_bytes();
        expected.insert(key.clone(), value.clone());
        tree = sync_prolly.put(&tree, key, value).unwrap();
    }

    let mutations = vec![
        Mutation::Upsert {
            key: b"k005".to_vec(),
            val: b"temp".to_vec(),
        },
        Mutation::Delete {
            key: b"k003".to_vec(),
        },
        Mutation::Upsert {
            key: b"k020".to_vec(),
            val: b"new".to_vec(),
        },
        Mutation::Delete {
            key: b"missing".to_vec(),
        },
        Mutation::Upsert {
            key: b"k005".to_vec(),
            val: b"final".to_vec(),
        },
    ];
    expected.insert(b"k005".to_vec(), b"final".to_vec());
    expected.remove(b"k003".as_slice());
    expected.insert(b"k020".to_vec(), b"new".to_vec());

    store.reset_read_counts();
    store.reset_write_counts();
    async_prolly.reset_metrics();

    let new_tree = block_on(async_prolly.batch(&tree, mutations)).unwrap();
    let actual = sync_prolly
        .range(&new_tree, &[], None)
        .unwrap()
        .collect::<Result<BTreeMap<_, _>, _>>()
        .unwrap();

    assert_eq!(actual, expected);
    assert_eq!(
        sync_prolly
            .collect_stats(&new_tree)
            .unwrap()
            .total_key_value_pairs,
        expected.len()
    );
    assert_eq!(
        store.batch_put_calls.load(Ordering::Relaxed),
        1,
        "async batch should flush rewritten nodes once"
    );
    assert_eq!(
        store.put_calls.load(Ordering::Relaxed),
        0,
        "async batch should not issue point writes"
    );
    assert!(
        store.max_batch_put_len.load(Ordering::Relaxed) > 1,
        "the single flush should include all rebuilt nodes"
    );

    let metrics = async_prolly.metrics();
    assert_eq!(metrics.store_batch_put_calls, 1);
    assert!(metrics.nodes_written > 1);
}

#[test]
fn async_batch_routes_sparse_single_leaf_update_without_full_rebuild() {
    let store = Arc::new(CountingBatchStore::default());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(149)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config);
    let mut tree = sync_prolly.create();

    for idx in 0..128 {
        tree = sync_prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }
    let original_nodes = sync_prolly.collect_stats(&tree).unwrap().num_nodes;

    store.reset_read_counts();
    store.reset_write_counts();
    async_prolly.clear_cache();
    async_prolly.reset_metrics();

    let updated = block_on(async_prolly.batch(
        &tree,
        vec![
            Mutation::Delete {
                key: b"missing".to_vec(),
            },
            Mutation::Upsert {
                key: b"k042".to_vec(),
                val: b"updated".to_vec(),
            },
        ],
    ))
    .unwrap();

    assert_eq!(
        block_on(async_prolly.get(&updated, b"k042")).unwrap(),
        Some(b"updated".to_vec())
    );
    assert_eq!(
        sync_prolly
            .range(&updated, b"k041", Some(b"k044"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![
            (b"k041".to_vec(), b"v041".to_vec()),
            (b"k042".to_vec(), b"updated".to_vec()),
            (b"k043".to_vec(), b"v043".to_vec()),
        ]
    );
    assert_eq!(store.batch_put_calls.load(Ordering::Relaxed), 1);
    assert_eq!(store.put_calls.load(Ordering::Relaxed), 0);
    assert!(
        store.batch_get_ordered_unique_calls.load(Ordering::Relaxed) > 0,
        "async batch should hydrate mutation routes through ordered async reads"
    );
    assert!(
        store.max_batch_put_len.load(Ordering::Relaxed) < original_nodes,
        "sparse async batch should rewrite the touched path, not the whole tree"
    );
}

#[test]
fn async_batch_routes_multi_leaf_updates_with_batched_frontiers() {
    let store = Arc::new(CountingBatchStore::default());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(151)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config);
    let mut tree = sync_prolly.create();
    let mut expected = BTreeMap::new();

    for idx in 0..192 {
        let key = format!("k{idx:03}").into_bytes();
        let value = format!("v{idx:03}").into_bytes();
        expected.insert(key.clone(), value.clone());
        tree = sync_prolly.put(&tree, key, value).unwrap();
    }
    let original_nodes = sync_prolly.collect_stats(&tree).unwrap().num_nodes;

    let mutations = vec![
        Mutation::Upsert {
            key: b"k010".to_vec(),
            val: b"changed-010".to_vec(),
        },
        Mutation::Upsert {
            key: b"k050".to_vec(),
            val: b"changed-050".to_vec(),
        },
        Mutation::Delete {
            key: b"k090".to_vec(),
        },
        Mutation::Upsert {
            key: b"k191a".to_vec(),
            val: b"inserted".to_vec(),
        },
    ];
    expected.insert(b"k010".to_vec(), b"changed-010".to_vec());
    expected.insert(b"k050".to_vec(), b"changed-050".to_vec());
    expected.remove(b"k090".as_slice());
    expected.insert(b"k191a".to_vec(), b"inserted".to_vec());

    store.reset_read_counts();
    store.reset_write_counts();
    async_prolly.clear_cache();
    async_prolly.reset_metrics();

    let updated = block_on(async_prolly.batch(&tree, mutations)).unwrap();
    let actual = sync_prolly
        .range(&updated, &[], None)
        .unwrap()
        .collect::<Result<BTreeMap<_, _>, _>>()
        .unwrap();

    assert_eq!(actual, expected);
    assert_eq!(store.batch_put_calls.load(Ordering::Relaxed), 1);
    assert!(
        store.batch_get_ordered_unique_calls.load(Ordering::Relaxed) > 0,
        "multi-leaf async batch should route frontiers with ordered batch reads"
    );
    assert!(
        store
            .max_batch_get_ordered_unique_len
            .load(Ordering::Relaxed)
            > 1,
        "multi-leaf routing should hydrate sibling frontiers together"
    );
    assert!(
        store.max_batch_put_len.load(Ordering::Relaxed) < original_nodes,
        "multi-leaf async batch should avoid a full-tree rewrite"
    );
}

#[test]
fn async_append_batch_reuses_cached_rightmost_path_without_reads() {
    let store = Arc::new(CountingBatchStore::default());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(153)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config.clone());
    let tree = async_prolly.create();

    let first_batch = (0..64)
        .map(|idx| Mutation::Upsert {
            key: format!("k{idx:03}").into_bytes(),
            val: format!("v{idx:03}").into_bytes(),
        })
        .collect::<Vec<_>>();
    let tree = block_on(async_prolly.batch(&tree, first_batch)).unwrap();

    store.reset_read_counts();
    store.reset_write_counts();
    async_prolly.reset_metrics();

    let second_batch = (64..80)
        .map(|idx| Mutation::Upsert {
            key: format!("k{idx:03}").into_bytes(),
            val: format!("v{idx:03}").into_bytes(),
        })
        .collect::<Vec<_>>();
    let tree = block_on(async_prolly.batch(&tree, second_batch)).unwrap();

    assert_eq!(
        store.get_calls.load(Ordering::Relaxed),
        0,
        "cached async rightmost path should avoid point reads"
    );
    assert_eq!(
        store.batch_get_ordered_unique_calls.load(Ordering::Relaxed),
        0,
        "cached async rightmost path should avoid route hydration reads"
    );
    assert!(
        store.batch_put_with_hint_calls.load(Ordering::Relaxed) > 0,
        "append batch should publish the new rightmost hint with the node flush"
    );
    assert_eq!(
        block_on(async_prolly.get(&tree, b"k079")).unwrap(),
        Some(b"v079".to_vec())
    );
    assert_tree_invariants(&store, &tree, &config);
    assert_eq!(
        sync_prolly
            .collect_stats(&tree)
            .unwrap()
            .total_key_value_pairs,
        80
    );
}

#[test]
fn async_append_batch_loads_persisted_rightmost_hint_in_new_manager() {
    let store = Arc::new(CountingBatchStore::default());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(154)
        .build();
    let first_manager = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config.clone());
    let tree = first_manager.create();

    let initial_batch = (0..160)
        .map(|idx| Mutation::Upsert {
            key: format!("k{idx:03}").into_bytes(),
            val: format!("v{idx:03}").into_bytes(),
        })
        .collect::<Vec<_>>();
    let tree = block_on(first_manager.batch(&tree, initial_batch)).unwrap();
    assert!(
        store.put_hint_calls.load(Ordering::Relaxed) > 0,
        "initial append should persist a rightmost hint"
    );

    let second_manager = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config.clone());
    store.reset_read_counts();
    store.reset_write_counts();

    let tree = block_on(second_manager.batch(
        &tree,
        vec![Mutation::Upsert {
            key: b"k999".to_vec(),
            val: b"tail".to_vec(),
        }],
    ))
    .unwrap();

    assert!(
        store.get_hint_calls.load(Ordering::Relaxed) > 0,
        "fresh async manager should consult persisted rightmost hints"
    );
    assert!(
        store.batch_get_ordered_unique_calls.load(Ordering::Relaxed) > 0,
        "persisted hint should hydrate the hinted rightmost path in one ordered read"
    );
    assert_eq!(
        store.get_calls.load(Ordering::Relaxed),
        0,
        "persisted hint path should avoid point-reading the right edge"
    );
    assert_eq!(
        block_on(second_manager.get(&tree, b"k999")).unwrap(),
        Some(b"tail".to_vec())
    );
    assert_tree_invariants(&store, &tree, &config);
}

#[test]
fn async_batch_noop_does_not_write() {
    let store = Arc::new(CountingBatchStore::default());
    let sync_prolly = Prolly::new(store.clone(), Config::default());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), Config::default());

    let tree = sync_prolly.create();
    let tree = sync_prolly
        .put(&tree, b"k001".to_vec(), b"v001".to_vec())
        .unwrap();

    store.reset_write_counts();
    async_prolly.reset_metrics();

    let unchanged = block_on(async_prolly.batch(
        &tree,
        vec![
            Mutation::Upsert {
                key: b"k001".to_vec(),
                val: b"v001".to_vec(),
            },
            Mutation::Delete {
                key: b"missing".to_vec(),
            },
        ],
    ))
    .unwrap();

    assert_eq!(unchanged.root, tree.root);
    assert_eq!(store.batch_put_calls.load(Ordering::Relaxed), 0);
    assert_eq!(store.put_calls.load(Ordering::Relaxed), 0);
    assert_eq!(async_prolly.metrics().store_batch_put_calls, 0);
    assert_eq!(async_prolly.metrics().nodes_written, 0);
}

#[test]
fn async_delete_last_key_returns_empty_tree() {
    let store = Arc::new(MemStore::new());
    let config = Config::default();
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let mut tree = async_prolly.create();

    tree = block_on(async_prolly.put(&tree, b"only".to_vec(), b"value".to_vec())).unwrap();
    tree = block_on(async_prolly.delete(&tree, b"only")).unwrap();

    assert!(tree.is_empty());
    assert_eq!(block_on(async_prolly.get(&tree, b"only")).unwrap(), None);
}

#[test]
fn async_manager_metrics_track_cache_reads_writes_and_reset() {
    let store = Arc::new(MemStore::new());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), Config::default());
    assert_eq!(
        async_prolly.metrics(),
        prolly::ProllyMetricsSnapshot::default()
    );

    let tree = async_prolly.create();
    let tree = block_on(async_prolly.put(&tree, b"a".to_vec(), b"1".to_vec())).unwrap();
    let write_metrics = async_prolly.metrics();

    assert!(write_metrics.nodes_written > 0);
    assert!(write_metrics.bytes_written > 0);
    assert!(write_metrics.store_batch_put_calls > 0);

    async_prolly.reset_metrics();
    async_prolly.clear_cache();

    assert_eq!(
        block_on(async_prolly.get(&tree, b"a")).unwrap(),
        Some(b"1".to_vec())
    );
    let cold_metrics = async_prolly.metrics();
    assert!(cold_metrics.node_cache_misses > 0);
    assert!(cold_metrics.nodes_read > 0);
    assert!(cold_metrics.bytes_read > 0);

    assert_eq!(
        block_on(async_prolly.get(&tree, b"a")).unwrap(),
        Some(b"1".to_vec())
    );
    let warm_metrics = async_prolly.metrics();
    assert!(warm_metrics.node_cache_hits > cold_metrics.node_cache_hits);
    assert_eq!(warm_metrics.nodes_read, cold_metrics.nodes_read);

    async_prolly.reset_metrics();
    assert_eq!(
        async_prolly.metrics(),
        prolly::ProllyMetricsSnapshot::default()
    );
}

#[test]
fn async_bounded_node_cache_limits_entries_and_preserves_reads() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .node_cache_max_nodes(2)
        .build();
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let mut tree = async_prolly.create();

    for idx in 0..24 {
        tree = block_on(async_prolly.put(
            &tree,
            format!("k{idx:03}").into_bytes(),
            format!("v{idx:03}").into_bytes(),
        ))
        .unwrap();
        assert!(async_prolly.cache_len() <= 2);
    }

    assert!(async_prolly.metrics().node_cache_evictions > 0);
    async_prolly.reset_metrics();

    for idx in 0..24 {
        assert_eq!(
            block_on(async_prolly.get(&tree, format!("k{idx:03}").as_bytes())).unwrap(),
            Some(format!("v{idx:03}").into_bytes())
        );
        assert!(async_prolly.cache_len() <= 2);
    }

    let metrics = async_prolly.metrics();
    assert!(metrics.node_cache_misses > 0);
    assert!(metrics.node_cache_evictions > 0);
}

#[test]
fn async_pinned_path_can_exceed_cache_limit_until_unpinned() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .node_cache_max_nodes(1)
        .build();
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let mut tree = async_prolly.create();

    for idx in 0..64 {
        tree = block_on(async_prolly.put(
            &tree,
            format!("k{idx:03}").into_bytes(),
            format!("v{idx:03}").into_bytes(),
        ))
        .unwrap();
    }
    assert!(
        block_on(async_prolly.collect_stats(&tree))
            .unwrap()
            .tree_height
            > 0
    );

    async_prolly.clear_cache();
    let pinned = block_on(async_prolly.pin_tree_path(&tree, b"k031")).unwrap();
    assert!(pinned > 1, "multi-level tree should pin root and leaf path");
    assert_eq!(async_prolly.cache_pinned_len(), pinned);
    assert_eq!(async_prolly.cache_len(), pinned);
    assert!(async_prolly.cache_pinned_bytes_len() > 0);

    assert_eq!(
        block_on(async_prolly.get(&tree, b"k031")).unwrap(),
        Some(b"v031".to_vec())
    );
    assert_eq!(async_prolly.cache_pinned_len(), pinned);

    assert_eq!(async_prolly.unpin_all_cache_nodes(), pinned);
    assert_eq!(async_prolly.cache_pinned_len(), 0);
    assert!(async_prolly.cache_len() <= 1);
}

#[test]
fn async_zero_node_cache_max_disables_pinning() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder().node_cache_max_nodes(0).build();
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let tree = async_prolly.create();
    let tree = block_on(async_prolly.put(&tree, b"a".to_vec(), b"1".to_vec())).unwrap();

    assert_eq!(block_on(async_prolly.pin_tree_root(&tree)).unwrap(), 0);
    assert_eq!(
        block_on(async_prolly.pin_tree_path(&tree, b"a")).unwrap(),
        0
    );
    assert_eq!(async_prolly.cache_len(), 0);
    assert_eq!(async_prolly.cache_pinned_len(), 0);
    assert_eq!(async_prolly.unpin_all_cache_nodes(), 0);
}

#[test]
fn async_byte_bounded_node_cache_limits_serialized_weight_and_preserves_reads() {
    const CACHE_BYTES: usize = 512;

    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .node_cache_max_bytes(CACHE_BYTES)
        .build();
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let mut tree = async_prolly.create();

    for idx in 0..48 {
        tree = block_on(async_prolly.put(
            &tree,
            format!("k{idx:03}").into_bytes(),
            format!("value-{idx:03}-payload").into_bytes(),
        ))
        .unwrap();
        assert!(async_prolly.cache_bytes_len() <= CACHE_BYTES);
    }

    assert!(async_prolly.metrics().node_cache_evictions > 0);
    async_prolly.reset_metrics();

    for idx in 0..48 {
        assert_eq!(
            block_on(async_prolly.get(&tree, format!("k{idx:03}").as_bytes())).unwrap(),
            Some(format!("value-{idx:03}-payload").into_bytes())
        );
        assert!(async_prolly.cache_bytes_len() <= CACHE_BYTES);
    }

    let metrics = async_prolly.metrics();
    assert!(metrics.node_cache_misses > 0);
    assert!(metrics.node_cache_evictions > 0);
}

#[test]
fn async_collect_stats_matches_sync_and_batches_frontiers() {
    let store = Arc::new(CountingBatchStore::default());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(31)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config);
    let mut tree = sync_prolly.create();

    for idx in 0..48 {
        tree = sync_prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("value-{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let expected = sync_prolly.collect_stats(&tree).unwrap();
    assert!(expected.num_nodes > 1);
    assert!(expected.num_leaves > 1);

    store.reset_read_counts();
    async_prolly.clear_cache();
    async_prolly.reset_metrics();

    let actual = block_on(async_prolly.collect_stats(&tree)).unwrap();

    assert_eq!(actual, expected);
    assert_eq!(
        store.get_calls.load(Ordering::Relaxed),
        0,
        "a store that prefers batch reads should not use point reads for stats frontiers"
    );
    assert!(
        store.batch_get_ordered_unique_calls.load(Ordering::Relaxed) >= 2,
        "stats should load root and child frontiers through ordered batch reads"
    );
    assert!(
        store
            .max_batch_get_ordered_unique_len
            .load(Ordering::Relaxed)
            > 1,
        "a non-root frontier should be loaded as one ordered batch"
    );

    let metrics = async_prolly.metrics();
    assert!(metrics.store_batch_get_calls >= 2);
    assert_eq!(metrics.nodes_read, actual.num_nodes as u64);
}

#[test]
fn async_collect_stats_splits_wide_frontiers_for_batched_stores() {
    let store = Arc::new(CountingBatchStore::default());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(131)
        .build();
    let tree = build_wide_tree(store.clone(), &config, 512);
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config);

    let expected = sync_prolly.collect_stats(&tree).unwrap();
    assert!(
        expected.num_leaves as usize > EXPECTED_ASYNC_NODE_PREFETCH_BATCH_CAP,
        "test fixture must create a frontier wider than the async prefetch cap"
    );

    store.reset_read_counts();
    async_prolly.clear_cache();

    let actual = block_on(async_prolly.collect_stats(&tree)).unwrap();

    assert_eq!(actual, expected);
    assert!(
        store.batch_get_ordered_unique_calls.load(Ordering::Relaxed) > 2,
        "wide frontiers should be split across multiple ordered batch reads"
    );
    assert!(
        store
            .max_batch_get_ordered_unique_len
            .load(Ordering::Relaxed)
            <= EXPECTED_ASYNC_NODE_PREFETCH_BATCH_CAP,
        "async traversal should cap wide child-frontier prefetch batches"
    );
}

#[test]
fn async_stats_diff_reports_growth_and_unchanged_trees() {
    let store = Arc::new(MemStore::new());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), Config::default());
    let before = async_prolly.create();
    let mut after = before.clone();

    for idx in 0..8 {
        after = block_on(async_prolly.put(
            &after,
            format!("k{idx:03}").into_bytes(),
            format!("v{idx:03}").into_bytes(),
        ))
        .unwrap();
    }

    let growth = block_on(async_prolly.stats_diff(&before, &after)).unwrap();
    assert_eq!(growth.before.total_key_value_pairs, 0);
    assert_eq!(growth.after.total_key_value_pairs, 8);
    assert_eq!(growth.absolute.total_key_value_pairs_diff, 8);

    let unchanged = block_on(async_prolly.stats_diff(&after, &after)).unwrap();
    assert_eq!(unchanged.absolute.total_key_value_pairs_diff, 0);
    assert_eq!(unchanged.absolute.num_nodes_diff, 0);
}

#[test]
fn async_diff_matches_sync_diff_for_mixed_changes() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(44)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let mut base = sync_prolly.create();

    for idx in 0..40 {
        base = sync_prolly
            .put(
                &base,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let mut other = sync_prolly
        .put(&base, b"k003".to_vec(), b"updated-003".to_vec())
        .unwrap();
    other = sync_prolly.delete(&other, b"k012").unwrap();
    other = sync_prolly
        .put(&other, b"k099".to_vec(), b"added-099".to_vec())
        .unwrap();

    let expected = sync_prolly.diff(&base, &other).unwrap();
    let actual = block_on(async_prolly.diff(&base, &other)).unwrap();

    assert_eq!(actual, expected);
    assert!(block_on(async_prolly.diff(&base, &base))
        .unwrap()
        .is_empty());
}

#[test]
fn async_stream_diff_matches_sync_stream_diff_for_mixed_changes() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(144)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let mut base = sync_prolly.create();

    for idx in 0..64 {
        base = sync_prolly
            .put(
                &base,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let mut other = sync_prolly
        .put(&base, b"k003".to_vec(), b"updated-003".to_vec())
        .unwrap();
    other = sync_prolly.delete(&other, b"k012").unwrap();
    other = sync_prolly.delete(&other, b"k021").unwrap();
    other = sync_prolly
        .put(&other, b"k099".to_vec(), b"added-099".to_vec())
        .unwrap();

    let expected = sync_prolly
        .stream_diff(&base, &other)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let actual = block_on(async_prolly.stream_diff(&base, &other).collect()).unwrap();

    assert_eq!(actual, expected);
}

#[test]
fn async_stream_diff_supports_stream_adapter_and_early_stop() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(145)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let base = sync_prolly.create();
    let mut other = base.clone();

    for idx in 0..6 {
        other = sync_prolly
            .put(
                &other,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let first = block_on(async {
        let mut iter = async_prolly.stream_diff(&base, &other);
        iter.next().await.unwrap().unwrap()
    });
    assert_eq!(
        first,
        Diff::Added {
            key: b"k000".to_vec(),
            val: b"v000".to_vec(),
        }
    );

    let streamed = block_on(async {
        let stream = async_prolly.stream_diff(&base, &other).into_stream();
        futures_util::pin_mut!(stream);

        let mut diffs = Vec::new();
        while let Some(item) = stream.next().await {
            diffs.push(item.unwrap());
        }
        diffs
    });

    assert_eq!(streamed.len(), 6);
    assert_eq!(
        streamed
            .into_iter()
            .map(|diff| match diff {
                Diff::Added { key, .. } => key,
                other => panic!("expected added diff, got {other:?}"),
            })
            .collect::<Vec<_>>(),
        vec![
            b"k000".to_vec(),
            b"k001".to_vec(),
            b"k002".to_vec(),
            b"k003".to_vec(),
            b"k004".to_vec(),
            b"k005".to_vec(),
        ]
    );
}

#[test]
fn async_stream_diff_identical_roots_short_circuit_without_reads() {
    let store = Arc::new(CountingBatchStore::default());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(146)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config);
    let mut tree = sync_prolly.create();

    for idx in 0..16 {
        tree = sync_prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    store.reset_read_counts();
    async_prolly.clear_cache();

    let empty = block_on(async {
        let mut iter = async_prolly.stream_diff(&tree, &tree);
        iter.next().await.is_none()
    });

    assert!(empty);
    assert_eq!(store.get_calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        store.batch_get_ordered_unique_calls.load(Ordering::Relaxed),
        0
    );
}

#[test]
fn async_range_diff_matches_sync_range_diff_and_bounds() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(45)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let mut base = sync_prolly.create();

    for idx in 0..48 {
        base = sync_prolly
            .put(
                &base,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let mut other = sync_prolly
        .put(&base, b"k006".to_vec(), b"outside-left".to_vec())
        .unwrap();
    other = sync_prolly
        .put(&other, b"k021".to_vec(), b"inside-update".to_vec())
        .unwrap();
    other = sync_prolly.delete(&other, b"k024").unwrap();
    other = sync_prolly
        .put(&other, b"k026a".to_vec(), b"inside-add".to_vec())
        .unwrap();
    other = sync_prolly
        .put(&other, b"k044".to_vec(), b"outside-right".to_vec())
        .unwrap();

    let start = b"k020";
    let end = b"k030";
    let expected = sync_prolly
        .range_diff(&base, &other, start, Some(end))
        .unwrap();
    let actual = block_on(async_prolly.range_diff(&base, &other, start, Some(end))).unwrap();

    assert_eq!(actual, expected);
    assert!(
        block_on(async_prolly.range_diff(&base, &other, b"k030", Some(b"k020")))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn async_diff_uses_ordered_batch_reads_for_structural_frontiers() {
    let store = Arc::new(CountingBatchStore::default());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(46)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config);
    let mut base = sync_prolly.create();

    for idx in 0..96 {
        base = sync_prolly
            .put(
                &base,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let mut other = base.clone();
    for idx in [8, 19, 37, 58, 71, 90] {
        other = sync_prolly
            .put(
                &other,
                format!("k{idx:03}").into_bytes(),
                format!("changed-{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let expected = sync_prolly.diff(&base, &other).unwrap();
    store.reset_read_counts();
    async_prolly.clear_cache();
    async_prolly.reset_metrics();

    let actual = block_on(async_prolly.diff(&base, &other)).unwrap();

    assert_eq!(actual, expected);
    assert!(
        store.batch_get_ordered_unique_calls.load(Ordering::Relaxed) > 0,
        "async diff should hydrate structural frontiers with ordered batch reads"
    );
    assert!(
        store
            .max_batch_get_ordered_unique_len
            .load(Ordering::Relaxed)
            > 1,
        "async diff should batch more than one frontier node when possible"
    );
    assert!(async_prolly.metrics().store_batch_get_calls > 0);
}

#[test]
fn async_merge_matches_sync_merge_for_disjoint_changes() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(47)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config);

    let base = sync_prolly.create();
    let base = sync_prolly
        .put(&base, b"a".to_vec(), b"base".to_vec())
        .unwrap();
    let left = sync_prolly
        .put(&base, b"b".to_vec(), b"left".to_vec())
        .unwrap();
    let right = sync_prolly
        .put(&base, b"c".to_vec(), b"right".to_vec())
        .unwrap();

    let expected = sync_prolly
        .range(
            &sync_prolly.merge(&base, &left, &right, None).unwrap(),
            &[],
            None,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let merged = block_on(async_prolly.merge(&base, &left, &right, None)).unwrap();
    let actual = sync_prolly
        .range(&merged, &[], None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(actual, expected);
}

#[test]
fn async_merge_conflict_resolver_can_value_delete_or_unresolve() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(48)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config);

    let base = sync_prolly.create();
    let base = sync_prolly
        .put(&base, b"k".to_vec(), b"base".to_vec())
        .unwrap();
    let left = sync_prolly
        .put(&base, b"k".to_vec(), b"left".to_vec())
        .unwrap();
    let right = sync_prolly
        .put(&base, b"k".to_vec(), b"right".to_vec())
        .unwrap();

    assert!(matches!(
        block_on(async_prolly.merge(&base, &left, &right, None)),
        Err(prolly::Error::Conflict(_))
    ));

    let merged = block_on(async_prolly.merge(
        &base,
        &left,
        &right,
        Some(Box::new(|conflict| {
            let mut value = conflict.left.clone().expect("left value");
            value.extend_from_slice(b"+");
            value.extend_from_slice(conflict.right.as_ref().expect("right value"));
            Resolution::value(value)
        })),
    ))
    .unwrap();
    assert_eq!(
        block_on(async_prolly.get(&merged, b"k")).unwrap(),
        Some(b"left+right".to_vec())
    );

    let left_deleted = sync_prolly.delete(&base, b"k").unwrap();
    let right_updated = sync_prolly
        .put(&base, b"k".to_vec(), b"right".to_vec())
        .unwrap();

    let deleted = block_on(async_prolly.merge(
        &base,
        &left_deleted,
        &right_updated,
        Some(Box::new(|conflict| {
            assert_eq!(conflict.base.as_deref(), Some(b"base".as_slice()));
            assert_eq!(conflict.left, None);
            assert_eq!(conflict.right.as_deref(), Some(b"right".as_slice()));
            Resolution::delete()
        })),
    ))
    .unwrap();
    assert_eq!(block_on(async_prolly.get(&deleted, b"k")).unwrap(), None);

    assert!(matches!(
        block_on(async_prolly.merge(
            &base,
            &left,
            &right,
            Some(Box::new(|_| Resolution::unresolved())),
        )),
        Err(prolly::Error::Conflict(_))
    ));
}

#[test]
fn async_crdt_merge_resolves_lww_conflicts_without_error() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(49)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);

    let base = sync_prolly.create();
    let base = sync_prolly
        .put(
            &base,
            b"k".to_vec(),
            TimestampedValue::new(b"base".to_vec(), 100).to_bytes(),
        )
        .unwrap();
    let left = sync_prolly
        .put(
            &base,
            b"k".to_vec(),
            TimestampedValue::new(b"left".to_vec(), 300).to_bytes(),
        )
        .unwrap();
    let right = sync_prolly
        .put(
            &base,
            b"k".to_vec(),
            TimestampedValue::new(b"right".to_vec(), 200).to_bytes(),
        )
        .unwrap();

    assert!(matches!(
        block_on(async_prolly.merge(&base, &left, &right, None)),
        Err(prolly::Error::Conflict(_))
    ));

    let merged =
        block_on(async_prolly.crdt_merge(&base, &left, &right, &CrdtConfig::lww())).unwrap();
    let value = block_on(async_prolly.get(&merged, b"k")).unwrap().unwrap();
    let resolved = TimestampedValue::from_bytes(&value).unwrap();

    assert_eq!(resolved.value, b"left".to_vec());
    assert_eq!(resolved.timestamp, 300);
}

#[test]
fn async_crdt_merge_honors_delete_policies() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(50)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);

    let base = sync_prolly.create();
    let base = sync_prolly
        .put(&base, b"k".to_vec(), b"base".to_vec())
        .unwrap();
    let left = sync_prolly.delete(&base, b"k").unwrap();
    let right = sync_prolly
        .put(&base, b"k".to_vec(), b"right".to_vec())
        .unwrap();

    let update_wins = block_on(async_prolly.crdt_merge(
        &base,
        &left,
        &right,
        &CrdtConfig::lww().with_delete_policy(DeletePolicy::UpdateWins),
    ))
    .unwrap();
    assert_eq!(
        block_on(async_prolly.get(&update_wins, b"k")).unwrap(),
        Some(b"right".to_vec())
    );

    let delete_wins = block_on(async_prolly.crdt_merge(
        &base,
        &left,
        &right,
        &CrdtConfig::lww().with_delete_policy(DeletePolicy::DeleteWins),
    ))
    .unwrap();
    assert_eq!(
        block_on(async_prolly.get(&delete_wins, b"k")).unwrap(),
        None
    );
}

#[test]
fn async_crdt_merge_supports_multi_value_and_custom_delete() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(51)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);

    let base = sync_prolly.create();
    let base = sync_prolly
        .put(&base, b"k".to_vec(), b"base".to_vec())
        .unwrap();
    let left = sync_prolly
        .put(&base, b"k".to_vec(), b"left".to_vec())
        .unwrap();
    let right = sync_prolly
        .put(&base, b"k".to_vec(), b"right".to_vec())
        .unwrap();

    let multi_value =
        block_on(async_prolly.crdt_merge(&base, &left, &right, &CrdtConfig::multi_value()))
            .unwrap();
    let value = block_on(async_prolly.get(&multi_value, b"k"))
        .unwrap()
        .unwrap();
    let values = MultiValueSet::from_bytes(&value).unwrap();
    assert_eq!(values.values, vec![b"left".to_vec(), b"right".to_vec()]);

    let deleted = block_on(async_prolly.crdt_merge(
        &base,
        &left,
        &right,
        &CrdtConfig::custom(|conflict| {
            assert_eq!(conflict.base.as_deref(), Some(b"base".as_slice()));
            assert_eq!(conflict.left.as_deref(), Some(b"left".as_slice()));
            assert_eq!(conflict.right.as_deref(), Some(b"right".as_slice()));
            CrdtResolution::delete()
        }),
    ))
    .unwrap();
    assert_eq!(block_on(async_prolly.get(&deleted, b"k")).unwrap(), None);
}

#[test]
fn async_range_matches_sync_range_and_respects_bounds() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(41)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let mut tree = sync_prolly.create();

    for idx in 0..32 {
        tree = sync_prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let expected_all = sync_prolly
        .range(&tree, &[], None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let async_all = block_on(async {
        async_prolly
            .range(&tree, &[], None)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    });
    assert_eq!(async_all, expected_all);

    let expected_bounded = sync_prolly
        .range(&tree, b"k007", Some(b"k013"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let async_bounded = block_on(async {
        let mut iter = async_prolly
            .range(&tree, b"k007", Some(b"k013"))
            .await
            .unwrap();
        let mut entries = Vec::new();
        while let Some(item) = iter.next().await {
            entries.push(item.unwrap());
        }
        entries
    });
    assert_eq!(async_bounded, expected_bounded);

    let empty = block_on(async {
        async_prolly
            .range(&tree, b"k013", Some(b"k013"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    });
    assert!(empty.is_empty());
}

#[test]
fn async_range_after_resumes_without_duplicate_for_exact_and_gap_keys() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(155)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let mut tree = sync_prolly.create();

    for idx in 0..12 {
        tree = sync_prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let exact_resume = block_on(async {
        async_prolly
            .range_after(&tree, b"k004", Some(b"k008"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    });
    assert_eq!(
        exact_resume,
        vec![
            (b"k005".to_vec(), b"v005".to_vec()),
            (b"k006".to_vec(), b"v006".to_vec()),
            (b"k007".to_vec(), b"v007".to_vec()),
        ]
    );

    let gap_resume = block_on(async {
        async_prolly
            .range_after(&tree, b"k004a", Some(b"k007"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    });
    assert_eq!(
        gap_resume,
        vec![
            (b"k005".to_vec(), b"v005".to_vec()),
            (b"k006".to_vec(), b"v006".to_vec()),
        ]
    );
}

#[test]
fn async_range_pages_resume_to_reconstruct_bounded_scan() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(156)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let mut tree = sync_prolly.create();

    for idx in 0..27 {
        tree = sync_prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let expected = sync_prolly
        .range(&tree, b"k004", Some(b"k021"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let mut cursor = RangeCursor::after_key(b"k003".to_vec());
    let mut actual = Vec::new();
    let mut page_count = 0usize;

    loop {
        let page = block_on(async_prolly.range_page(&tree, &cursor, Some(b"k021"), 5)).unwrap();
        page_count += 1;
        actual.extend(page.entries);

        let Some(next) = page.next_cursor else {
            break;
        };
        assert!(!next.is_start());
        cursor = next;
    }

    assert_eq!(actual, expected);
    assert!(page_count > 1);
}

#[test]
fn async_reverse_pages_resume_to_reconstruct_descending_scan() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(157)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let mut tree = sync_prolly.create();

    for idx in 0..18 {
        tree = sync_prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let mut expected = sync_prolly
        .range(&tree, b"k004", None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    expected.reverse();

    let mut cursor = ReverseCursor::end();
    let mut actual = Vec::new();
    let mut page_count = 0usize;

    loop {
        let page = block_on(async_prolly.reverse_page(&tree, &cursor, b"k004", 4)).unwrap();
        page_count += 1;
        actual.extend(page.entries);

        let Some(next) = page.next_cursor else {
            break;
        };
        assert!(!next.is_end());
        cursor = next;
    }

    assert_eq!(actual, expected);
    assert!(page_count > 1);

    let zero_cursor = ReverseCursor::before_key(b"k010".to_vec());
    let zero = block_on(async_prolly.reverse_page(&tree, &zero_cursor, b"k004", 0)).unwrap();
    assert!(zero.entries.is_empty());
    assert_eq!(zero.next_cursor, Some(zero_cursor));
}

#[test]
fn async_prefix_reverse_pages_resume_inside_prefix() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(158)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
    let mut tree = sync_prolly.create();

    for idx in 0..12 {
        tree = sync_prolly
            .put(
                &tree,
                format!("doc/{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }
    tree = sync_prolly
        .put(&tree, b"doc0/000".to_vec(), b"outside".to_vec())
        .unwrap();
    tree = sync_prolly
        .put(&tree, b"other/999".to_vec(), b"outside".to_vec())
        .unwrap();

    let mut expected = sync_prolly
        .prefix(&tree, b"doc/")
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    expected.reverse();

    let mut cursor = ReverseCursor::end();
    let mut actual = Vec::new();
    let mut page_count = 0usize;

    loop {
        let page = block_on(async_prolly.prefix_reverse_page(&tree, b"doc/", &cursor, 3)).unwrap();
        page_count += 1;
        actual.extend(page.entries);

        let Some(next) = page.next_cursor else {
            break;
        };
        assert!(!next.is_end());
        cursor = next;
    }

    assert_eq!(actual, expected);
    assert!(page_count > 1);

    let clamped = block_on(async_prolly.prefix_reverse_page(
        &tree,
        b"doc/",
        &ReverseCursor::before_key(b"other/999".to_vec()),
        1,
    ))
    .unwrap();
    assert_eq!(clamped.entries[0].0, b"doc/011".to_vec());
}

#[test]
fn async_range_cursor_and_zero_limit_page_are_stable() {
    let store = Arc::new(MemStore::new());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), Config::default());
    let mut tree = async_prolly.create();

    for idx in 0..4 {
        tree = block_on(async_prolly.put(
            &tree,
            format!("k{idx:03}").into_bytes(),
            format!("v{idx:03}").into_bytes(),
        ))
        .unwrap();
    }

    let mut iter =
        block_on(async_prolly.range_from_cursor(&tree, &RangeCursor::start(), None)).unwrap();
    assert!(iter.resume_cursor().is_start());
    let first = block_on(iter.next()).unwrap().unwrap();
    assert_eq!(first.0, b"k000".to_vec());
    assert_eq!(iter.resume_cursor().after(), Some(b"k000".as_slice()));

    let cursor = RangeCursor::after_key(b"k001".to_vec());
    let page = block_on(async_prolly.range_page(&tree, &cursor, None, 0)).unwrap();
    assert!(page.entries.is_empty());
    assert_eq!(page.next_cursor, Some(cursor));
}

#[test]
fn async_range_stream_adapter_yields_ordered_entries() {
    let store = Arc::new(MemStore::new());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), Config::default());
    let mut tree = async_prolly.create();

    for idx in 0..5 {
        tree = block_on(async_prolly.put(
            &tree,
            format!("k{idx:03}").into_bytes(),
            format!("v{idx:03}").into_bytes(),
        ))
        .unwrap();
    }

    let entries = block_on(async {
        let iter = async_prolly
            .range(&tree, b"k001", Some(b"k004"))
            .await
            .unwrap();
        let stream = iter.into_stream();
        futures_util::pin_mut!(stream);

        let mut entries = Vec::new();
        while let Some(item) = stream.next().await {
            entries.push(item.unwrap());
        }
        entries
    });

    assert_eq!(
        entries,
        vec![
            (b"k001".to_vec(), b"v001".to_vec()),
            (b"k002".to_vec(), b"v002".to_vec()),
            (b"k003".to_vec(), b"v003".to_vec()),
        ]
    );
}

#[test]
fn async_range_prefetches_sibling_children_for_batched_stores() {
    let store = Arc::new(CountingBatchStore::default());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(43)
        .build();
    let sync_prolly = Prolly::new(store.clone(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store.clone()), config);
    let mut tree = sync_prolly.create();

    for idx in 0..64 {
        tree = sync_prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }
    assert!(sync_prolly.collect_stats(&tree).unwrap().num_leaves > 4);

    store.reset_read_counts();
    async_prolly.clear_cache();

    let entries = block_on(async {
        async_prolly
            .range(&tree, &[], None)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    });

    assert_eq!(entries.len(), 64);
    assert!(
        store.batch_get_ordered_unique_calls.load(Ordering::Relaxed) > 0,
        "batched stores should hydrate range sibling children through ordered batch reads"
    );
    assert!(
        store
            .max_batch_get_ordered_unique_len
            .load(Ordering::Relaxed)
            > 1,
        "range traversal should prefetch more than one child when possible"
    );
}

#[test]
fn async_copy_missing_nodes_makes_tree_readable_from_destination_store() {
    block_on(async {
        let source_store = Arc::new(MemStore::new());
        let destination_store = Arc::new(MemStore::new());
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(2)
            .hash_seed(101)
            .build();
        let source = AsyncProlly::new(SyncStoreAsAsync::new(source_store), config.clone());
        let destination_async = SyncStoreAsAsync::new(destination_store.clone());
        let destination_sync = Prolly::new(destination_store.clone(), config);

        let mut tree = source.create();
        for idx in 0..32 {
            tree = source
                .put(
                    &tree,
                    format!("k{idx:03}").into_bytes(),
                    format!("v{idx:03}").into_bytes(),
                )
                .await
                .unwrap();
        }

        let reachability = source
            .mark_reachable(std::slice::from_ref(&tree))
            .await
            .unwrap();
        let plan = source
            .plan_missing_nodes(&tree, &destination_async)
            .await
            .unwrap();
        assert_eq!(plan.required_nodes, reachability.live_nodes);
        assert_eq!(plan.required_bytes, reachability.live_bytes);
        assert_eq!(plan.missing_nodes, plan.required_nodes);

        let copied = source
            .copy_missing_nodes(&tree, &destination_async)
            .await
            .unwrap();
        assert_eq!(copied.copied_nodes, plan.missing_nodes);
        assert_eq!(copied.copied_bytes, plan.missing_bytes);

        for idx in 0..32 {
            assert_eq!(
                destination_sync
                    .get(&tree, format!("k{idx:03}").as_bytes())
                    .unwrap(),
                Some(format!("v{idx:03}").into_bytes())
            );
        }
        assert_tree_invariants(&destination_store, &tree, source.config());
    });
}

#[test]
fn async_plan_missing_nodes_splits_wide_source_and_destination_checks() {
    block_on(async {
        let source_store = Arc::new(CountingBatchStore::default());
        let destination_store = Arc::new(CountingBatchStore::default());
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(2)
            .hash_seed(132)
            .build();
        let tree = build_wide_tree(source_store.clone(), &config, 512);
        let source = AsyncProlly::new(SyncStoreAsAsync::new(source_store.clone()), config);
        let destination = SyncStoreAsAsync::new(destination_store.clone());

        let sync_source = Prolly::new(source_store.clone(), tree.config.clone());
        let reachability = sync_source
            .mark_reachable(std::slice::from_ref(&tree))
            .unwrap();
        assert!(
            reachability.live_nodes > EXPECTED_ASYNC_NODE_PREFETCH_BATCH_CAP,
            "test fixture must create more required nodes than the async prefetch cap"
        );

        source_store.reset_read_counts();
        destination_store.reset_read_counts();

        let plan = source
            .plan_missing_nodes(&tree, &destination)
            .await
            .unwrap();

        assert_eq!(plan.required_nodes, reachability.live_nodes);
        assert_eq!(plan.missing_nodes, reachability.live_nodes);
        assert!(
            source_store
                .max_batch_get_ordered_unique_len
                .load(Ordering::Relaxed)
                <= EXPECTED_ASYNC_NODE_PREFETCH_BATCH_CAP,
            "source traversal and source byte fetches should be chunked"
        );
        assert!(
            destination_store
                .max_batch_get_ordered_unique_len
                .load(Ordering::Relaxed)
                <= EXPECTED_ASYNC_NODE_PREFETCH_BATCH_CAP,
            "destination existence checks should be chunked"
        );
        assert!(
            destination_store
                .batch_get_ordered_unique_calls
                .load(Ordering::Relaxed)
                > 1,
            "wide missing-node plans should split destination checks"
        );
    });
}

#[test]
fn async_plan_missing_nodes_rejects_corrupt_destination_bytes() {
    block_on(async {
        let source_store = Arc::new(MemStore::new());
        let destination_store = Arc::new(MemStore::new());
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(2)
            .hash_seed(102)
            .build();
        let source = AsyncProlly::new(SyncStoreAsAsync::new(source_store), config);
        let destination_async = SyncStoreAsAsync::new(destination_store.clone());

        let mut tree = source.create();
        for idx in 0..8 {
            tree = source
                .put(
                    &tree,
                    format!("k{idx:03}").into_bytes(),
                    format!("v{idx:03}").into_bytes(),
                )
                .await
                .unwrap();
        }
        source
            .copy_missing_nodes(&tree, &destination_async)
            .await
            .unwrap();

        let root = tree.root.clone().unwrap();
        destination_store
            .put(root.as_bytes(), b"wrong bytes")
            .unwrap();

        let err = source
            .plan_missing_nodes(&tree, &destination_async)
            .await
            .unwrap_err();
        match err {
            Error::CidMismatch { expected, actual } => {
                assert_eq!(expected, root);
                assert_eq!(actual, Cid::from_bytes(b"wrong bytes"));
            }
            other => panic!("expected CidMismatch, got {other:?}"),
        }
    });
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_blocking_store_satisfies_async_store_contract() {
    let runtime = tokio_runtime();
    let store = TokioBlockingStore::from_arc(Arc::new(MemStore::new()));

    runtime.block_on(async {
        assert_async_store_contract(&store).await;
    });
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_blocking_store_satisfies_async_manifest_store_contract() {
    let runtime = tokio_runtime();
    let store = TokioBlockingStore::from_arc(Arc::new(MemStore::new()));

    runtime.block_on(async {
        assert_async_manifest_store_contract(&store).await;
    });
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_blocking_store_adapts_sync_store_without_blocking_runtime_workers() {
    let runtime = tokio_runtime();
    let store = Arc::new(MemStore::new());
    let tokio_store = TokioBlockingStore::from_arc(store);

    runtime.block_on(async {
        tokio_store.put(b"a", b"1").await.unwrap();
        tokio_store
            .batch(&[
                prolly::BatchOp::Upsert {
                    key: b"b",
                    value: b"2",
                },
                prolly::BatchOp::Delete { key: b"missing" },
            ])
            .await
            .unwrap();

        let keys: Vec<&[u8]> = vec![b"a", b"a", b"b", b"missing"];
        let values = tokio_store.batch_get_ordered(&keys).await.unwrap();

        assert_eq!(
            values,
            vec![
                Some(b"1".to_vec()),
                Some(b"1".to_vec()),
                Some(b"2".to_vec()),
                None
            ]
        );
    });
}

#[cfg(feature = "tokio")]
#[test]
fn async_prolly_runs_over_tokio_blocking_store() {
    let runtime = tokio_runtime();
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(23)
        .build();
    let async_prolly =
        AsyncProlly::new(TokioBlockingStore::from_arc(store.clone()), config.clone());
    let sync_prolly = Prolly::new(store.clone(), config.clone());

    let tree = runtime
        .block_on(async {
            let tree = async_prolly.create();
            let tree = async_prolly
                .put(&tree, b"a".to_vec(), b"1".to_vec())
                .await?;
            let tree = async_prolly
                .put(&tree, b"b".to_vec(), b"2".to_vec())
                .await?;
            let tree = async_prolly.delete(&tree, b"a").await?;
            async_prolly
                .batch(
                    &tree,
                    vec![Mutation::Upsert {
                        key: b"c".to_vec(),
                        val: b"3".to_vec(),
                    }],
                )
                .await
        })
        .unwrap();

    assert_eq!(sync_prolly.get(&tree, b"a").unwrap(), None);
    assert_eq!(sync_prolly.get(&tree, b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(sync_prolly.get(&tree, b"c").unwrap(), Some(b"3".to_vec()));
    assert_tree_invariants(&store, &tree, &config);
}
