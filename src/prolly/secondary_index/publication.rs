use super::super::error::Error;
use super::super::manifest::ManifestStore;
use super::super::store::{FileNodeStore, MemStore, Store};
use super::super::tree::Tree;

/// Coordination scope proved by a production indexed-store adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexedCoordinationScope {
    Process,
    Host,
    Distributed,
}

/// Immutable-write visibility proved by a production indexed-store adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexedWriteVisibility {
    ReadAfterWrite,
    PublicationBarrier,
}

/// GC safety mechanism provided by an indexed-store deployment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexedGcSafety {
    ExplicitQuiescence,
    GracePeriod,
    Leases,
}

/// Exact capabilities required before a store may host production indexes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProductionIndexedStoreCapabilities {
    pub coordination_scope: IndexedCoordinationScope,
    pub write_visibility: IndexedWriteVisibility,
    pub durable_acknowledgement: bool,
    pub gc_safety: IndexedGcSafety,
}

impl ProductionIndexedStoreCapabilities {
    pub fn validate(&self) -> Result<(), Error> {
        if self.coordination_scope == IndexedCoordinationScope::Process {
            return Err(Error::UnsupportedIndexedStoreProfile {
                store: "indexed store",
                required: "cross-handle production coordination",
                actual: "process-only coordination",
            });
        }
        if !self.durable_acknowledgement {
            return Err(Error::UnsupportedIndexedStoreProfile {
                store: "indexed store",
                required: "durable publication acknowledgement",
                actual: "non-durable acknowledgement",
            });
        }
        Ok(())
    }
}

/// Declared secondary-index support tier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexedStoreProfile {
    Verification,
    Production(ProductionIndexedStoreCapabilities),
}

impl IndexedStoreProfile {
    pub fn is_production(&self) -> bool {
        matches!(self, Self::Production(_))
    }

    pub fn validate(&self) -> Result<(), Error> {
        if let Self::Production(capabilities) = self {
            capabilities.validate()?;
        }
        Ok(())
    }
}

/// Store contract used by the canonical single-root index coordinator.
///
/// General multi-map transactions are deliberately not part of this trait.
/// Immutable nodes are published first; one manifest CAS is the visibility
/// transition.
pub trait IndexedStore: Store + ManifestStore {
    fn indexed_store_profile(&self) -> IndexedStoreProfile;

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

impl IndexedStore for MemStore {
    fn indexed_store_profile(&self) -> IndexedStoreProfile {
        IndexedStoreProfile::Verification
    }
}

impl IndexedStore for FileNodeStore {
    fn indexed_store_profile(&self) -> IndexedStoreProfile {
        IndexedStoreProfile::Verification
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, Mutation, Prolly};

    #[test]
    fn built_in_local_stores_are_verification_only() {
        assert_eq!(
            MemStore::new().indexed_store_profile(),
            IndexedStoreProfile::Verification
        );
        let directory =
            std::env::temp_dir().join(format!("prolly-index-profile-{}", std::process::id()));
        let store = FileNodeStore::open(&directory).unwrap();
        assert_eq!(
            store.indexed_store_profile(),
            IndexedStoreProfile::Verification
        );
        drop(store);
        let _ = std::fs::remove_dir_all(directory);
    }

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
