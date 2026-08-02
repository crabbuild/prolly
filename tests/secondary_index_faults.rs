use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use prolly::{
    BatchOp, Config, Error, IndexedStore, ManifestStore, ManifestUpdate, MemStore, Prolly,
    RootCondition, RootManifest, RootWrite, SecondaryIndex, SecondaryIndexRegistry, Store,
    TransactionNodeWrite, TransactionUpdate, TransactionalStore, Tree,
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
    fail_confirmation: AtomicBool,
    fail_commit: AtomicBool,
}

impl FaultStore {
    fn fail_next_confirmation(&self) {
        self.fail_confirmation.store(true, Ordering::SeqCst);
    }

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

impl IndexedStore for FaultStore {
    fn confirm_indexed_publication(&self, trees: &[&Tree]) -> Result<(), Error> {
        if self.fail_confirmation.swap(false, Ordering::SeqCst) {
            return Err(Error::Store(Box::new(FaultError(
                "injected publication confirmation failure",
            ))));
        }
        for tree in trees {
            if let Some(root) = &tree.root {
                if self
                    .get(root.as_bytes())
                    .map_err(|error| Error::Store(Box::new(error)))?
                    .is_none()
                {
                    return Err(Error::NotFound(root.clone()));
                }
            }
        }
        Ok(())
    }
}

fn registry() -> SecondaryIndexRegistry {
    SecondaryIndexRegistry::new()
        .register(
            SecondaryIndex::non_unique("by-value", 1, "fault.by-value/1", |_, value| {
                Ok(vec![value.to_vec()])
            })
            .unwrap(),
        )
        .unwrap()
}

#[test]
fn failures_before_or_during_the_root_transaction_leave_the_old_state_visible() {
    let store = Arc::new(FaultStore::default());
    let engine = Prolly::new(store.clone(), Config::default());
    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    indexed.ensure_index(b"by-value").unwrap();
    indexed.put(b"stable", b"old").unwrap();
    let old_state = indexed.health().unwrap().state_version;

    store.fail_next_confirmation();
    assert!(indexed.put(b"confirmation", b"not-visible").is_err());
    assert_eq!(indexed.health().unwrap().state_version, old_state);
    assert_eq!(indexed.get(b"confirmation").unwrap(), None);

    store.fail_next_commit();
    assert!(indexed.put(b"commit", b"not-visible").is_err());
    assert_eq!(indexed.health().unwrap().state_version, old_state);
    assert_eq!(indexed.get(b"commit").unwrap(), None);

    indexed.put(b"published", b"new").unwrap();
    let snapshot = indexed.snapshot().unwrap();
    assert_ne!(Some(snapshot.state_version().clone()), old_state);
    assert_eq!(
        snapshot.source().get(b"published").unwrap(),
        Some(b"new".to_vec())
    );
    assert_eq!(
        snapshot
            .index(b"by-value")
            .unwrap()
            .primary_keys(b"new")
            .unwrap(),
        vec![b"published".to_vec()]
    );
}
