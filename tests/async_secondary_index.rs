use std::fmt;
use std::future::Future;
use std::ops::ControlFlow;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use prolly::{
    AsyncProlly, BatchOp, Config, Error, IndexedMapUpdate, ManifestStore, ManifestUpdate, MemStore,
    Mutation, Prolly, RootCondition, RootManifest, RootWrite, SecondaryIndex,
    SecondaryIndexRegistry, Store, SyncStoreAsAsync, TransactionNodeWrite, TransactionUpdate,
    TransactionalStore,
};

#[derive(Debug)]
struct FaultError(&'static str);

impl fmt::Display for FaultError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for FaultError {}

#[derive(Default)]
struct FaultStore {
    inner: MemStore,
    fail_commit: AtomicBool,
}

impl FaultStore {
    fn fail_next_commit(&self) {
        self.fail_commit.store(true, Ordering::SeqCst);
    }
}

impl Store for FaultStore {
    type Error = FaultError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner
            .get(key)
            .map_err(|_| FaultError("node read failed"))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.inner
            .put(key, value)
            .map_err(|_| FaultError("node write failed"))
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner
            .delete(key)
            .map_err(|_| FaultError("node delete failed"))
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        self.inner
            .batch(ops)
            .map_err(|_| FaultError("node batch failed"))
    }
}

impl ManifestStore for FaultStore {
    type Error = FaultError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        ManifestStore::get_root(&self.inner, name).map_err(|_| FaultError("root read failed"))
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        ManifestStore::put_root(&self.inner, name, manifest)
            .map_err(|_| FaultError("root write failed"))
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        ManifestStore::delete_root(&self.inner, name).map_err(|_| FaultError("root delete failed"))
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        ManifestStore::compare_and_swap_root(&self.inner, name, expected, new)
            .map_err(|_| FaultError("root CAS failed"))
    }
}

impl TransactionalStore for FaultStore {
    fn supports_transactions(&self) -> bool {
        true
    }

    fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, Error> {
        if self.fail_commit.swap(false, Ordering::SeqCst) {
            return Err(Error::Store(Box::new(FaultError(
                "injected transaction commit failure",
            ))));
        }
        TransactionalStore::commit_transaction(
            &self.inner,
            node_writes,
            root_conditions,
            root_writes,
        )
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn registry() -> SecondaryIndexRegistry {
    SecondaryIndexRegistry::new()
        .register(
            SecondaryIndex::non_unique("by-status", 1, "tests.by-status/v1", |_, value| {
                Ok(vec![value.to_vec()])
            })
            .unwrap(),
        )
        .unwrap()
}

#[test]
fn native_async_indexed_map_builds_mutates_queries_and_rejects_stale_writes() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let engine = AsyncProlly::new(SyncStoreAsAsync::new(store), Config::default());
        let users = engine
            .indexed_map(b"async-users", registry())
            .await
            .unwrap();

        users.put(b"user-1", b"active").await.unwrap();
        users.put(b"user-2", b"pending").await.unwrap();
        users.ensure_index(b"by-status").await.unwrap();

        let snapshot = users.snapshot().await.unwrap();
        let by_status = snapshot.index(b"by-status").unwrap();
        assert_eq!(
            by_status.primary_keys(b"active").await.unwrap(),
            vec![b"user-1".to_vec()]
        );
        assert_eq!(
            by_status.records(b"pending").await.unwrap(),
            vec![(b"user-2".to_vec(), b"pending".to_vec())]
        );

        let stale = snapshot.source_version().clone();
        users.put(b"user-3", b"active").await.unwrap();
        let active_snapshot = users.snapshot().await.unwrap();
        let active = active_snapshot.index(b"by-status").unwrap();
        let first_page = active.exact_page(b"active", None, 1).await.unwrap();
        assert_eq!(first_page.matches.len(), 1);
        let second_page = active
            .exact_page(b"active", first_page.next_cursor.as_ref(), 1)
            .await
            .unwrap();
        assert_eq!(second_page.matches.len(), 1);
        assert!(second_page.next_cursor.is_none());
        let update = users
            .apply_if(
                Some(&stale),
                vec![Mutation::Upsert {
                    key: b"must-not-publish".to_vec(),
                    val: b"active".to_vec(),
                }],
            )
            .await
            .unwrap();
        assert!(matches!(update, IndexedMapUpdate::Conflict { .. }));
        assert_eq!(users.get(b"must-not-publish").await.unwrap(), None);

        users.put(b"user-1", b"disabled").await.unwrap();
        let snapshot = users.snapshot().await.unwrap();
        let by_status = snapshot.index(b"by-status").unwrap();
        assert!(by_status
            .primary_keys(b"active")
            .await
            .unwrap()
            .iter()
            .all(|key| key != b"user-1"));
        assert_eq!(
            by_status.primary_keys(b"disabled").await.unwrap(),
            vec![b"user-1".to_vec()]
        );
        let stopped = by_status
            .scan_range_until(b"", None, |_| ControlFlow::Break("enough"))
            .await
            .unwrap();
        assert_eq!(stopped.visited, 1);
        assert_eq!(stopped.break_value, Some("enough"));

        users
            .retain_snapshot_pin(b"before-user-3", &stale)
            .await
            .unwrap();
        users.keep_last(1).await.unwrap();
        assert_eq!(
            users.snapshot_at(&stale).await.unwrap().source_version(),
            &stale
        );
        users.release_snapshot_pin(b"before-user-3").await.unwrap();

        let current = users.snapshot().await.unwrap().source_version().clone();
        assert!(users
            .verify_all(&current)
            .await
            .unwrap()
            .iter()
            .all(prolly::IndexVerification::is_valid));
    });
}

#[test]
fn synchronous_and_asynchronous_indexed_maps_share_one_canonical_format() {
    let store = Arc::new(MemStore::new());
    let async_engine =
        AsyncProlly::new(SyncStoreAsAsync::new(Arc::clone(&store)), Config::default());
    block_on(async {
        let users = async_engine
            .indexed_map(b"compatible-users", registry())
            .await
            .unwrap();
        users.put(b"async", b"active").await.unwrap();
        users.ensure_index(b"by-status").await.unwrap();
    });

    let sync_engine = Prolly::new(Arc::clone(&store), Config::default());
    let sync_users = sync_engine
        .indexed_map(b"compatible-users", registry())
        .unwrap();
    assert_eq!(
        sync_users
            .snapshot()
            .unwrap()
            .index(b"by-status")
            .unwrap()
            .primary_keys(b"active")
            .unwrap(),
        vec![b"async".to_vec()]
    );
    sync_users.put(b"sync", b"active").unwrap();

    block_on(async {
        let users = async_engine
            .indexed_map(b"compatible-users", registry())
            .await
            .unwrap();
        assert_eq!(
            users
                .snapshot()
                .await
                .unwrap()
                .index(b"by-status")
                .unwrap()
                .primary_keys(b"active")
                .await
                .unwrap(),
            vec![b"async".to_vec(), b"sync".to_vec()]
        );
    });
}

#[test]
fn concurrent_async_writers_retry_without_losing_independent_records() {
    let store = Arc::new(MemStore::new());
    block_on(async {
        let engine = AsyncProlly::new(SyncStoreAsAsync::new(Arc::clone(&store)), Config::default());
        engine
            .indexed_map(b"concurrent-async-users", registry())
            .await
            .unwrap()
            .ensure_index(b"by-status")
            .await
            .unwrap();
    });

    let writers = (0..8u8)
        .map(|writer| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                block_on(async move {
                    let engine = AsyncProlly::new(SyncStoreAsAsync::new(store), Config::default());
                    let users = engine
                        .indexed_map(b"concurrent-async-users", registry())
                        .await
                        .unwrap();
                    users
                        .put(vec![b'u', writer], b"active".to_vec())
                        .await
                        .unwrap();
                });
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }

    block_on(async {
        let engine = AsyncProlly::new(SyncStoreAsAsync::new(store), Config::default());
        let users = engine
            .indexed_map(b"concurrent-async-users", registry())
            .await
            .unwrap();
        assert_eq!(
            users
                .snapshot()
                .await
                .unwrap()
                .index(b"by-status")
                .unwrap()
                .primary_keys(b"active")
                .await
                .unwrap()
                .len(),
            8
        );
    });
}

#[test]
fn failed_async_root_transaction_never_exposes_candidate_source_or_index_state() {
    block_on(async {
        let store = Arc::new(FaultStore::default());
        let engine = AsyncProlly::new(SyncStoreAsAsync::new(Arc::clone(&store)), Config::default());
        let users = engine
            .indexed_map(b"fault-users", registry())
            .await
            .unwrap();
        users.put(b"stable", b"active").await.unwrap();
        users.ensure_index(b"by-status").await.unwrap();
        let before = users.snapshot().await.unwrap().id().clone();

        store.fail_next_commit();
        assert!(users.put(b"candidate", b"pending").await.is_err());

        let after = users.snapshot().await.unwrap();
        assert_eq!(after.id(), &before);
        assert_eq!(users.get(b"candidate").await.unwrap(), None);
        assert!(after
            .index(b"by-status")
            .unwrap()
            .primary_keys(b"pending")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            after
                .index(b"by-status")
                .unwrap()
                .primary_keys(b"active")
                .await
                .unwrap(),
            vec![b"stable".to_vec()]
        );
    });
}
