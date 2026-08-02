use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use prolly::{
    BatchOp, Config, Error, IndexedMapUpdate, IndexedStore, ManifestStore, ManifestUpdate,
    MemStore, Mutation, Prolly, RootCondition, RootManifest, RootWrite, SecondaryIndexRegistry,
    Store, TransactionNodeWrite, TransactionUpdate, TransactionalStore,
};

#[derive(Debug)]
struct AuditStoreError;

impl fmt::Display for AuditStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("secondary-index transaction audit store failure")
    }
}

impl std::error::Error for AuditStoreError {}

struct TransactionAuditStore {
    inner: MemStore,
    transactions_enabled: AtomicBool,
    commits: AtomicUsize,
    intercept_reads: AtomicBool,
    intercepted_reads: AtomicUsize,
    second_read_reached: Barrier,
    release_second_read: Barrier,
}

impl TransactionAuditStore {
    fn new(transactions_enabled: bool) -> Self {
        Self {
            inner: MemStore::new(),
            transactions_enabled: AtomicBool::new(transactions_enabled),
            commits: AtomicUsize::new(0),
            intercept_reads: AtomicBool::new(false),
            intercepted_reads: AtomicUsize::new(0),
            second_read_reached: Barrier::new(2),
            release_second_read: Barrier::new(2),
        }
    }

    fn intercept_second_root_read(&self) {
        self.intercepted_reads.store(0, Ordering::SeqCst);
        self.intercept_reads.store(true, Ordering::SeqCst);
    }
}

impl Store for TransactionAuditStore {
    type Error = AuditStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get(key).map_err(|_| AuditStoreError)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.inner.put(key, value).map_err(|_| AuditStoreError)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key).map_err(|_| AuditStoreError)
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        self.inner.batch(ops).map_err(|_| AuditStoreError)
    }
}

impl ManifestStore for TransactionAuditStore {
    type Error = AuditStoreError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        if self.intercept_reads.load(Ordering::SeqCst)
            && self.intercepted_reads.fetch_add(1, Ordering::SeqCst) == 1
        {
            self.second_read_reached.wait();
            self.release_second_read.wait();
        }
        ManifestStore::get_root(&self.inner, name).map_err(|_| AuditStoreError)
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        ManifestStore::put_root(&self.inner, name, manifest).map_err(|_| AuditStoreError)
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        ManifestStore::delete_root(&self.inner, name).map_err(|_| AuditStoreError)
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        ManifestStore::compare_and_swap_root(&self.inner, name, expected, new)
            .map_err(|_| AuditStoreError)
    }
}

impl TransactionalStore for TransactionAuditStore {
    fn supports_transactions(&self) -> bool {
        self.transactions_enabled.load(Ordering::SeqCst)
    }

    fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, Error> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        TransactionalStore::commit_transaction(
            &self.inner,
            node_writes,
            root_conditions,
            root_writes,
        )
    }
}

impl IndexedStore for TransactionAuditStore {}

#[test]
fn indexed_map_rejects_a_store_without_strict_transactions() {
    let engine = Prolly::new(TransactionAuditStore::new(false), Config::default());
    assert!(matches!(
        engine.indexed_map(b"users", SecondaryIndexRegistry::new()),
        Err(Error::UnsupportedTransactions { .. })
    ));
}

#[test]
fn indexed_publication_commits_through_the_transaction_store() {
    let store = Arc::new(TransactionAuditStore::new(true));
    let engine = Prolly::new(store.clone(), Config::default());
    let indexed = engine
        .indexed_map(b"users", SecondaryIndexRegistry::new())
        .unwrap();
    let commits_after_initialization = store.commits.load(Ordering::SeqCst);
    assert!(commits_after_initialization > 0);

    indexed.put(b"user-1", b"active").unwrap();
    assert!(store.commits.load(Ordering::SeqCst) > commits_after_initialization);
}

#[test]
fn apply_if_never_applies_after_the_expected_version_is_superseded() {
    let store = Arc::new(TransactionAuditStore::new(true));
    let engine = Arc::new(Prolly::new(store.clone(), Config::default()));
    let indexed = engine
        .indexed_map(b"users", SecondaryIndexRegistry::new())
        .unwrap();
    let expected = indexed.put(b"initial", b"0").unwrap().source.id;
    let conditional_ready = Arc::new(Barrier::new(2));
    let conditional_start = Arc::new(Barrier::new(2));

    let conditional_engine = engine.clone();
    let thread_ready = conditional_ready.clone();
    let thread_start = conditional_start.clone();
    let conditional = std::thread::spawn(move || {
        let conditional_indexed = conditional_engine
            .indexed_map(b"users", SecondaryIndexRegistry::new())
            .unwrap();
        thread_ready.wait();
        thread_start.wait();
        conditional_indexed.apply_if(
            Some(&expected),
            vec![Mutation::Upsert {
                key: b"conditional".to_vec(),
                val: b"1".to_vec(),
            }],
        )
    });

    conditional_ready.wait();
    store.intercept_second_root_read();
    conditional_start.wait();
    store.second_read_reached.wait();
    store.intercept_reads.store(false, Ordering::SeqCst);
    indexed.put(b"concurrent", b"2").unwrap();
    store.release_second_read.wait();

    let update = conditional.join().unwrap().unwrap();
    assert!(matches!(update, IndexedMapUpdate::Conflict { .. }));
    assert_eq!(indexed.get(b"conditional").unwrap(), None);
    assert_eq!(indexed.get(b"concurrent").unwrap(), Some(b"2".to_vec()));
}
