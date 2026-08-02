use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use prolly::{
    BatchOp, Config, Error, IndexedStore, ManifestStore, ManifestUpdate, MemStore, Prolly,
    RootCondition, RootManifest, RootWrite, SecondaryIndex, SecondaryIndexRegistry, Store,
    TransactionNodeWrite, TransactionUpdate, TransactionalStore,
};

#[derive(Debug)]
struct BarrierStoreError;

impl fmt::Display for BarrierStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("barrier store failure")
    }
}

impl std::error::Error for BarrierStoreError {}

struct BarrierStore {
    inner: MemStore,
    enabled: AtomicBool,
    arrivals: AtomicUsize,
    arrived: Barrier,
    release: Barrier,
}

impl BarrierStore {
    fn new() -> Self {
        Self {
            inner: MemStore::new(),
            enabled: AtomicBool::new(false),
            arrivals: AtomicUsize::new(0),
            arrived: Barrier::new(3),
            release: Barrier::new(3),
        }
    }

    fn block_next_two_cas(&self) {
        self.arrivals.store(0, Ordering::SeqCst);
        self.enabled.store(true, Ordering::SeqCst);
    }
}

impl Store for BarrierStore {
    type Error = BarrierStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get(key).map_err(|_| BarrierStoreError)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.inner.put(key, value).map_err(|_| BarrierStoreError)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key).map_err(|_| BarrierStoreError)
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        self.inner.batch(ops).map_err(|_| BarrierStoreError)
    }
}

impl ManifestStore for BarrierStore {
    type Error = BarrierStoreError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        ManifestStore::get_root(&self.inner, name).map_err(|_| BarrierStoreError)
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        ManifestStore::put_root(&self.inner, name, manifest).map_err(|_| BarrierStoreError)
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        ManifestStore::delete_root(&self.inner, name).map_err(|_| BarrierStoreError)
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        if self.enabled.load(Ordering::SeqCst) && self.arrivals.fetch_add(1, Ordering::SeqCst) < 2 {
            self.arrived.wait();
            self.release.wait();
        }
        ManifestStore::compare_and_swap_root(&self.inner, name, expected, new)
            .map_err(|_| BarrierStoreError)
    }
}

impl TransactionalStore for BarrierStore {
    fn supports_transactions(&self) -> bool {
        true
    }

    fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, Error> {
        if self.enabled.load(Ordering::SeqCst) && self.arrivals.fetch_add(1, Ordering::SeqCst) < 2 {
            self.arrived.wait();
            self.release.wait();
        }
        TransactionalStore::commit_transaction(
            &self.inner,
            node_writes,
            root_conditions,
            root_writes,
        )
    }
}

impl IndexedStore for BarrierStore {}

fn registry() -> SecondaryIndexRegistry {
    SecondaryIndexRegistry::new()
        .register(
            SecondaryIndex::non_unique("by-value", 1, "concurrency.by-value/1", |_, value| {
                Ok(vec![value.to_vec()])
            })
            .unwrap(),
        )
        .unwrap()
}

#[test]
fn reader_observes_old_state_while_two_writers_wait_then_complete_without_tearing() {
    let store = Arc::new(BarrierStore::new());
    let engine = Arc::new(Prolly::new(store.clone(), Config::default()));
    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    indexed.ensure_index(b"by-value").unwrap();
    indexed.put(b"stable", b"old").unwrap();
    let old = indexed.snapshot().unwrap().id().clone();
    store.block_next_two_cas();

    let first_engine = engine.clone();
    let second_engine = engine.clone();
    let first = std::thread::spawn(move || {
        first_engine
            .indexed_map(b"users", registry())
            .unwrap()
            .put(b"first", b"one")
    });
    let second = std::thread::spawn(move || {
        second_engine
            .indexed_map(b"users", registry())
            .unwrap()
            .put(b"second", b"two")
    });

    store.arrived.wait();
    let blocked = indexed.snapshot().unwrap();
    assert_eq!(blocked.id(), &old);
    assert_eq!(blocked.source().get(b"first").unwrap(), None);
    assert_eq!(blocked.source().get(b"second").unwrap(), None);
    store.release.wait();

    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();
    let current = indexed.snapshot().unwrap();
    assert_ne!(current.id(), &old);
    assert_eq!(
        current.source().get(b"first").unwrap(),
        Some(b"one".to_vec())
    );
    assert_eq!(
        current.source().get(b"second").unwrap(),
        Some(b"two".to_vec())
    );
    assert_eq!(
        current
            .index(b"by-value")
            .unwrap()
            .primary_keys(b"one")
            .unwrap(),
        vec![b"first".to_vec()]
    );
    assert_eq!(
        current
            .index(b"by-value")
            .unwrap()
            .primary_keys(b"two")
            .unwrap(),
        vec![b"second".to_vec()]
    );
}
