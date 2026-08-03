use super::super::error::Error;
use super::super::manifest::{AsyncManifestStore, ManifestStore};
use super::super::store::{AsyncStore, FileNodeStore, MemStore, Store};
use super::super::transaction::{AsyncTransactionalStore, TransactionalStore};
use super::super::tree::Tree;
use std::sync::Arc;

/// Store contract used by the canonical single-root index coordinator.
///
/// Immutable nodes may be published before the visibility transition because
/// they are content addressed and unreachable nodes are safe to reclaim. The
/// canonical collection root is always validated and advanced through a strict
/// store transaction, so a successful transaction is the only visibility
/// transition.
pub trait IndexedStore: Store + ManifestStore + TransactionalStore {
    /// Confirm every non-empty candidate tree root is readable before CAS.
    fn confirm_indexed_publication(&self, trees: &[&Tree]) -> Result<(), Error> {
        for tree in trees {
            let Some(root) = &tree.root else {
                continue;
            };
            let present = self
                .get(root.as_bytes())
                .map_err(|error| Error::Store(Box::new(error)))?;
            if present.is_none() {
                return Err(Error::NotFound(root.clone()));
            }
        }
        Ok(())
    }
}

impl IndexedStore for MemStore {}

impl IndexedStore for FileNodeStore {}

impl<T: IndexedStore> IndexedStore for Arc<T> {
    fn confirm_indexed_publication(&self, trees: &[&Tree]) -> Result<(), Error> {
        self.as_ref().confirm_indexed_publication(trees)
    }
}

/// Native asynchronous store contract used by [`AsyncIndexedMap`](
/// super::AsyncIndexedMap).
///
/// The visibility and durability requirements are identical to [`IndexedStore`]:
/// immutable nodes may be published before the canonical collection root, but
/// that root may advance only through a strict transaction.
#[allow(async_fn_in_trait)]
pub trait AsyncIndexedStore: AsyncStore + AsyncManifestStore + AsyncTransactionalStore {
    /// Confirm every non-empty candidate tree root is readable before CAS.
    async fn confirm_async_indexed_publication(&self, trees: &[&Tree]) -> Result<(), Error>
    where
        <Self as AsyncStore>::Error: Send + Sync,
    {
        for tree in trees {
            let Some(root) = &tree.root else {
                continue;
            };
            let present = AsyncStore::get(self, root.as_bytes())
                .await
                .map_err(|error| Error::Store(Box::new(error)))?;
            if present.is_none() {
                return Err(Error::NotFound(root.clone()));
            }
        }
        Ok(())
    }
}

impl<T> AsyncIndexedStore for T where T: AsyncStore + AsyncManifestStore + AsyncTransactionalStore {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, Mutation, Prolly};

    #[test]
    fn default_confirmation_reads_candidate_roots() {
        let prolly = Prolly::new(MemStore::new(), Config::default());
        let tree = prolly
            .batch(
                &prolly.create(),
                vec![Mutation::Upsert {
                    key: b"k".to_vec(),
                    val: b"v".to_vec(),
                }],
            )
            .unwrap();
        prolly
            .store()
            .confirm_indexed_publication(&[&tree])
            .unwrap();
    }
}
