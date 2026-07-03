//! In-memory storage backend implementation

use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use super::super::manifest::{
    sort_named_root_manifests, ManifestStore, ManifestStoreScan, ManifestUpdate, NamedRootManifest,
    RootManifest,
};
use super::{cid_from_store_key, sort_cids, BatchOp, NodeStoreScan, OrderedBatchReadPlan, Store};

/// In-memory store for testing and simple use cases
#[derive(Debug, Default)]
pub struct MemStore {
    data: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    roots: RwLock<BTreeMap<Vec<u8>, RootManifest>>,
}

/// Error type for MemStore operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemStoreError(String);

impl std::fmt::Display for MemStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemStore error: {}", self.0)
    }
}

impl std::error::Error for MemStoreError {}

impl MemStore {
    /// Create a new empty in-memory store
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
            roots: RwLock::new(BTreeMap::new()),
        }
    }
}

impl Store for MemStore {
    type Error = MemStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let data = self
            .data
            .read()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;
        Ok(data.get(key).cloned())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let mut data = self
            .data
            .write()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        let mut data = self
            .data
            .write()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;
        data.remove(key);
        Ok(())
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        let mut data = self
            .data
            .write()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;

        for op in ops {
            match op {
                BatchOp::Upsert { key, value } => {
                    data.insert(key.to_vec(), value.to_vec());
                }
                BatchOp::Delete { key } => {
                    data.remove(*key);
                }
            }
        }

        Ok(())
    }

    /// Optimized batch_get for MemStore - acquires lock once for all reads
    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        let data = self
            .data
            .read()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;

        let plan = OrderedBatchReadPlan::new(keys);
        let mut results = HashMap::with_capacity(plan.unique_keys().len());
        for key in plan.unique_keys() {
            if let Some(value) = data.get(*key) {
                results.insert(key.to_vec(), value.clone());
            }
        }
        Ok(results)
    }

    /// Optimized batch_get_ordered for MemStore - acquires lock once for all reads
    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let data = self
            .data
            .read()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;

        let plan = OrderedBatchReadPlan::new(keys);
        let unique_values = plan
            .unique_keys()
            .iter()
            .map(|key| data.get(*key).cloned())
            .collect::<Vec<_>>();
        Ok(plan.expand_owned(unique_values))
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let data = self
            .data
            .read()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;

        Ok(keys.iter().map(|key| data.get(*key).cloned()).collect())
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    /// Optimized batch_put for MemStore - acquires lock once for all writes
    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        let mut data = self
            .data
            .write()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;

        for (key, value) in entries {
            data.insert(key.to_vec(), value.to_vec());
        }
        Ok(())
    }
}

impl NodeStoreScan for MemStore {
    type Error = MemStoreError;

    fn list_node_cids(&self) -> Result<Vec<super::super::cid::Cid>, Self::Error> {
        let data = self
            .data
            .read()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;
        let mut cids = data
            .keys()
            .map(|key| cid_from_store_key(key, "MemStore node"))
            .collect::<Result<Vec<_>, _>>()
            .map_err(MemStoreError)?;
        sort_cids(&mut cids);
        Ok(cids)
    }
}

impl ManifestStore for MemStore {
    type Error = MemStoreError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        let roots = self
            .roots
            .read()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;
        Ok(roots.get(name).cloned())
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        let mut roots = self
            .roots
            .write()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;
        roots.insert(name.to_vec(), manifest.clone());
        Ok(())
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        let mut roots = self
            .roots
            .write()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;
        roots.remove(name);
        Ok(())
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let mut roots = self
            .roots
            .write()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;
        let current = roots.get(name).cloned();
        if current.as_ref() != expected {
            return Ok(ManifestUpdate::Conflict { current });
        }

        match new {
            Some(manifest) => {
                roots.insert(name.to_vec(), manifest.clone());
            }
            None => {
                roots.remove(name);
            }
        }

        Ok(ManifestUpdate::Applied)
    }
}

impl ManifestStoreScan for MemStore {
    fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        let roots = self
            .roots
            .read()
            .map_err(|e| MemStoreError(format!("lock poisoned: {}", e)))?;
        let mut roots = roots
            .iter()
            .map(|(name, manifest)| NamedRootManifest::new(name.clone(), manifest.clone()))
            .collect::<Vec<_>>();
        sort_named_root_manifests(&mut roots);
        Ok(roots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memstore_put_get() {
        let store = MemStore::new();
        let key = b"test_key";
        let value = b"test_value";

        store.put(key, value).unwrap();
        let result = store.get(key).unwrap();

        assert_eq!(result, Some(value.to_vec()));
    }

    #[test]
    fn test_memstore_get_nonexistent() {
        let store = MemStore::new();
        let result = store.get(b"nonexistent").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_memstore_delete() {
        let store = MemStore::new();
        let key = b"test_key";
        let value = b"test_value";

        store.put(key, value).unwrap();
        store.delete(key).unwrap();
        let result = store.get(key).unwrap();

        assert_eq!(result, None);
    }

    #[test]
    fn test_memstore_batch() {
        let store = MemStore::new();

        // First put some initial data
        store.put(b"key1", b"value1").unwrap();
        store.put(b"key2", b"value2").unwrap();

        // Batch: update key1, delete key2, add key3
        let ops = vec![
            BatchOp::Upsert {
                key: b"key1",
                value: b"updated",
            },
            BatchOp::Delete { key: b"key2" },
            BatchOp::Upsert {
                key: b"key3",
                value: b"value3",
            },
        ];

        store.batch(&ops).unwrap();

        assert_eq!(store.get(b"key1").unwrap(), Some(b"updated".to_vec()));
        assert_eq!(store.get(b"key2").unwrap(), None);
        assert_eq!(store.get(b"key3").unwrap(), Some(b"value3".to_vec()));
    }

    // ========================================================================
    // Unit tests for batch_get_ordered
    // ========================================================================

    #[test]
    fn test_batch_get_ordered_with_existing_keys() {
        let store = MemStore::new();

        // Store some data
        store.put(b"key1", b"value1").unwrap();
        store.put(b"key2", b"value2").unwrap();
        store.put(b"key3", b"value3").unwrap();

        // Query in a specific order
        let keys: Vec<&[u8]> = vec![b"key3", b"key1", b"key2"];
        let results = store.batch_get_ordered(&keys).unwrap();

        // Verify results are in the same order as input keys
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some(b"value3".to_vec())); // key3
        assert_eq!(results[1], Some(b"value1".to_vec())); // key1
        assert_eq!(results[2], Some(b"value2".to_vec())); // key2
    }

    #[test]
    fn test_batch_get_ordered_with_nonexistent_keys() {
        let store = MemStore::new();

        // Query keys that don't exist
        let keys: Vec<&[u8]> = vec![b"missing1", b"missing2", b"missing3"];
        let results = store.batch_get_ordered(&keys).unwrap();

        // Verify all results are None
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], None);
        assert_eq!(results[1], None);
        assert_eq!(results[2], None);
    }

    #[test]
    fn test_batch_get_ordered_with_mixed_keys() {
        let store = MemStore::new();

        // Store some data
        store.put(b"exists1", b"value1").unwrap();
        store.put(b"exists2", b"value2").unwrap();

        // Query mix of existing and non-existing keys
        let keys: Vec<&[u8]> = vec![b"exists1", b"missing1", b"exists2", b"missing2"];
        let results = store.batch_get_ordered(&keys).unwrap();

        // Verify results are in correct order with Some/None as appropriate
        assert_eq!(results.len(), 4);
        assert_eq!(results[0], Some(b"value1".to_vec())); // exists1
        assert_eq!(results[1], None); // missing1
        assert_eq!(results[2], Some(b"value2".to_vec())); // exists2
        assert_eq!(results[3], None); // missing2
    }

    #[test]
    fn test_batch_get_ordered_empty_keys() {
        let store = MemStore::new();

        // Store some data
        store.put(b"key1", b"value1").unwrap();

        // Query with empty keys list
        let keys: Vec<&[u8]> = vec![];
        let results = store.batch_get_ordered(&keys).unwrap();

        // Should return empty vector
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_batch_get_ordered_duplicate_keys() {
        let store = MemStore::new();

        // Store some data
        store.put(b"key1", b"value1").unwrap();

        // Query with duplicate keys
        let keys: Vec<&[u8]> = vec![b"key1", b"key1", b"key1"];
        let results = store.batch_get_ordered(&keys).unwrap();

        // Should return same value for each duplicate key
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some(b"value1".to_vec()));
        assert_eq!(results[1], Some(b"value1".to_vec()));
        assert_eq!(results[2], Some(b"value1".to_vec()));
    }
}
