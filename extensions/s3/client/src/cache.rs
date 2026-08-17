use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCachePolicy,
    PsyncIoEngineConfig, S3FifoConfig,
};
use prolly_s3_core::{NodeCache, NodeCacheError, NodeCacheKey};

use crate::{Error, ErrorCode, Result};

const CACHE_VALUE_PREFIX: &[u8] = b"PS3C\x01";
const CACHE_TOMBSTONE: &[u8] = b"PS3C\x00";
const FOYER_PAGE_BYTES: usize = 4 * 1024;
// Foyer 0.22.3 block entries contain a 36-byte header, length-prefixed
// key/value vectors, our 96-byte key, and the adapter value prefix.
const FOYER_ENTRY_OVERHEAD_BYTES: usize = 36 + 8 + 96 + 8 + CACHE_VALUE_PREFIX.len();

#[derive(Clone, Debug)]
pub struct FoyerNodeCacheConfig {
    pub directory: PathBuf,
    pub memory_capacity_bytes: usize,
    pub disk_capacity_bytes: usize,
    pub disk_block_size_bytes: usize,
    pub memory_shards: usize,
}

impl FoyerNodeCacheConfig {
    fn effective_disk_block_size_bytes(&self) -> Option<usize> {
        self.disk_block_size_bytes
            .checked_add(FOYER_PAGE_BYTES - 1)
            .map(|bytes| bytes / FOYER_PAGE_BYTES * FOYER_PAGE_BYTES)
    }

    fn max_entry_size_bytes(&self) -> Option<usize> {
        self.effective_disk_block_size_bytes()?
            .checked_sub(FOYER_PAGE_BYTES + FOYER_ENTRY_OVERHEAD_BYTES)
    }

    pub fn validate(&self) -> Result<()> {
        let effective_block_size = self.effective_disk_block_size_bytes();
        if self.memory_capacity_bytes == 0
            || self.disk_capacity_bytes == 0
            || self.disk_block_size_bytes == 0
            || self.memory_shards == 0
            || effective_block_size.is_none()
            || effective_block_size.is_some_and(|bytes| bytes > self.disk_capacity_bytes)
            || self.max_entry_size_bytes().is_none_or(|bytes| bytes == 0)
        {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "Foyer cache capacities, effective 4 KiB-aligned block size, and shard count must be nonzero, and at least one cache entry and disk block must fit",
            ));
        }
        if self.directory.as_os_str().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidKey,
                "Foyer cache directory must not be empty",
            ));
        }
        Ok(())
    }
}

/// Optional hybrid memory/disk cache for verified immutable Prolly nodes.
///
/// The repository core owns integrity verification and treats all adapter
/// failures as cache misses. One Foyer instance should have one filesystem
/// owner; sharing the returned `Arc` within a process is supported.
pub struct FoyerNodeCache {
    cache: HybridCache<Vec<u8>, Vec<u8>>,
    max_entry_size_bytes: usize,
    pinned_capacity_bytes: usize,
    pinned: Mutex<PinnedFoyerState>,
}

#[derive(Default)]
struct PinnedFoyerState {
    entries: BTreeMap<Vec<u8>, Arc<[u8]>>,
    order: VecDeque<Vec<u8>>,
    bytes: usize,
}

impl FoyerNodeCache {
    pub async fn open(config: FoyerNodeCacheConfig) -> Result<Arc<Self>> {
        config.validate()?;
        let max_entry_size_bytes = config
            .max_entry_size_bytes()
            .expect("validated Foyer block size has entry capacity");
        std::fs::create_dir_all(&config.directory).map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("Foyer cache directory could not be created: {error}"),
            )
        })?;
        let device = FsDeviceBuilder::new(&config.directory)
            .with_capacity(config.disk_capacity_bytes)
            .build()
            .map_err(|error| {
                Error::new(
                    ErrorCode::Transport,
                    format!("Foyer cache device could not be opened: {error}"),
                )
            })?;
        let engine = BlockEngineConfig::new(device)
            .with_block_size(config.disk_block_size_bytes)
            .with_flushers(1)
            .with_reclaimers(1);
        let cache = HybridCache::builder()
            .with_name("prolly-s3-node-cache")
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .with_flush_on_close(true)
            .memory(config.memory_capacity_bytes)
            .with_shards(config.memory_shards)
            .with_eviction_config(S3FifoConfig::default())
            .with_weighter(|key: &Vec<u8>, value: &Vec<u8>| key.len().saturating_add(value.len()))
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(engine)
            .build()
            .await
            .map_err(|error| {
                Error::new(
                    ErrorCode::Transport,
                    format!("Foyer cache could not be opened: {error}"),
                )
            })?;
        Ok(Arc::new(Self {
            cache,
            max_entry_size_bytes,
            pinned_capacity_bytes: config.memory_capacity_bytes / 4,
            pinned: Mutex::new(PinnedFoyerState::default()),
        }))
    }

    /// Largest node value guaranteed to fit one configured Foyer disk block.
    pub fn max_entry_size_bytes(&self) -> usize {
        self.max_entry_size_bytes
    }

    pub async fn close(&self) -> Result<()> {
        self.cache.close().await.map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("Foyer cache could not close cleanly: {error}"),
            )
        })
    }
}

#[async_trait::async_trait]
impl NodeCache for FoyerNodeCache {
    fn admits(&self, _key: &NodeCacheKey, value_len: usize) -> bool {
        value_len <= self.max_entry_size_bytes
    }

    fn pinned_usage(&self) -> Option<(usize, usize)> {
        let state = self
            .pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Some((state.entries.len(), state.bytes))
    }

    async fn get(
        &self,
        key: &NodeCacheKey,
    ) -> std::result::Result<Option<Vec<u8>>, NodeCacheError> {
        let encoded = key.encode().to_vec();
        if let Some(value) = self
            .pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(&encoded)
            .cloned()
        {
            return Ok(Some(value.as_ref().to_vec()));
        }
        self.cache
            .get(encoded.as_slice())
            .await
            .map(|entry| {
                entry.and_then(|entry| {
                    entry
                        .value()
                        .strip_prefix(CACHE_VALUE_PREFIX)
                        .map(<[u8]>::to_vec)
                })
            })
            .map_err(|error| NodeCacheError::new(format!("Foyer cache read failed: {error}")))
    }

    async fn insert(
        &self,
        key: NodeCacheKey,
        value: Vec<u8>,
    ) -> std::result::Result<(), NodeCacheError> {
        if !self.admits(&key, value.len()) {
            return Ok(());
        }
        let encoded = key.encode().to_vec();
        let mut encoded_value = Vec::with_capacity(CACHE_VALUE_PREFIX.len() + value.len());
        encoded_value.extend_from_slice(CACHE_VALUE_PREFIX);
        encoded_value.extend(value);
        self.cache.insert(encoded, encoded_value);
        Ok(())
    }

    async fn pin(
        &self,
        key: NodeCacheKey,
        value: Vec<u8>,
    ) -> std::result::Result<(), NodeCacheError> {
        if !self.admits(&key, value.len()) {
            return Ok(());
        }
        self.insert(key.clone(), value.clone()).await?;
        if value.len() > self.pinned_capacity_bytes {
            return Err(NodeCacheError::new(
                "node exceeds the bounded Foyer pinned-memory tier",
            ));
        }
        let encoded = key.encode().to_vec();
        let mut state = self
            .pinned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(previous) = state.entries.remove(&encoded) {
            state.bytes = state.bytes.saturating_sub(previous.len());
        }
        state.order.retain(|candidate| candidate != &encoded);
        state.bytes = state.bytes.saturating_add(value.len());
        state.entries.insert(encoded.clone(), Arc::from(value));
        state.order.push_back(encoded);
        while state.bytes > self.pinned_capacity_bytes {
            let Some(evicted) = state.order.pop_front() else {
                break;
            };
            if let Some(value) = state.entries.remove(&evicted) {
                state.bytes = state.bytes.saturating_sub(value.len());
            }
        }
        Ok(())
    }

    async fn remove(&self, key: &NodeCacheKey) -> std::result::Result<(), NodeCacheError> {
        let encoded = key.encode().to_vec();
        {
            let mut state = self
                .pinned
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(value) = state.entries.remove(&encoded) {
                state.bytes = state.bytes.saturating_sub(value.len());
            }
            state.order.retain(|candidate| candidate != &encoded);
        }
        // Foyer 0.22's delete tombstone is not recovered after every clean
        // reopen. Persist an adapter-level tombstone so a corrupt entry cannot
        // reappear after restart. The storage engine bounds these markers by
        // the configured disk capacity.
        self.cache.insert(encoded, CACHE_TOMBSTONE.to_vec());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use prolly_s3_core::{
        Cid, MemoryObjectPlane, ObjectHeaders, ProviderPerKeyVersionLimit, Repository,
        RepositoryId, RepositoryOptions, TreeFormatDigest,
    };

    use super::*;

    fn key() -> NodeCacheKey {
        NodeCacheKey {
            repository: RepositoryId::from_hash([1; 32]),
            tree_format: TreeFormatDigest::from_hash([2; 32]),
            cid: Cid([3; 32]),
        }
    }

    fn config(directory: PathBuf) -> FoyerNodeCacheConfig {
        FoyerNodeCacheConfig {
            directory,
            memory_capacity_bytes: 1024 * 1024,
            disk_capacity_bytes: 16 * 1024 * 1024,
            disk_block_size_bytes: 1024 * 1024,
            memory_shards: 1,
        }
    }

    #[test]
    fn config_rejects_a_block_that_only_fits_before_foyer_alignment() {
        let directory = tempfile::tempdir().unwrap();
        let invalid = FoyerNodeCacheConfig {
            directory: directory.path().to_path_buf(),
            memory_capacity_bytes: 1024,
            disk_capacity_bytes: 4097,
            disk_block_size_bytes: 4097,
            memory_shards: 1,
        };
        assert_eq!(
            invalid.validate().unwrap_err().code,
            ErrorCode::InvalidLimit
        );
    }

    #[tokio::test]
    async fn hybrid_cache_rejects_nodes_that_cannot_fit_a_disk_block() {
        let directory = tempfile::tempdir().unwrap();
        let cache = FoyerNodeCache::open(config(directory.path().to_path_buf()))
            .await
            .unwrap();
        let oversized = vec![9; cache.max_entry_size_bytes() + 1];
        cache.insert(key(), oversized).await.unwrap();
        assert_eq!(cache.get(&key()).await.unwrap(), None);
        cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn repository_reopen_reads_nodes_from_persisted_foyer_cache() {
        let directory = tempfile::tempdir().unwrap();
        let plane = Arc::new(MemoryObjectPlane::new(true));
        let cache = FoyerNodeCache::open(config(directory.path().to_path_buf()))
            .await
            .unwrap();
        let prefix = ".tests/foyer-repository-restart";
        let repository = Repository::initialize(
            plane.clone(),
            RepositoryOptions {
                repository_prefix: prefix.to_string(),
                writer: "foyer-writer".to_string(),
                node_cache: Some(cache.clone()),
                provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
                ..RepositoryOptions::default()
            },
        )
        .await
        .unwrap();
        repository
            .put_object(
                "main",
                b"cached.txt".to_vec(),
                b"persisted through Foyer".to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
            )
            .await
            .unwrap();
        repository.advance_branch_indexes("main").await.unwrap();
        drop(repository);
        cache.close().await.unwrap();
        drop(cache);

        let reopened_cache = FoyerNodeCache::open(config(directory.path().to_path_buf()))
            .await
            .unwrap();
        let reopened = Repository::open(
            plane,
            RepositoryOptions {
                repository_prefix: prefix.to_string(),
                writer: "foyer-reader".to_string(),
                read_only: true,
                node_cache: Some(reopened_cache.clone()),
                provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
                ..RepositoryOptions::default()
            },
        )
        .await
        .unwrap();
        let before = reopened.node_cache_snapshot();
        let object = reopened
            .get_object("main", b"cached.txt")
            .await
            .unwrap()
            .unwrap();
        let after = reopened.node_cache_snapshot();
        assert_eq!(object.bytes, b"persisted through Foyer");
        assert!(after.hits > before.hits);
        assert_eq!(after.ranged_fetches, before.ranged_fetches);
        drop(reopened);
        reopened_cache.close().await.unwrap();
    }

    #[tokio::test]
    async fn hybrid_cache_persists_verified_node_values() {
        let directory = tempfile::tempdir().unwrap();
        let cache = FoyerNodeCache::open(config(directory.path().to_path_buf()))
            .await
            .unwrap();
        cache.insert(key(), vec![9; 1024]).await.unwrap();
        assert_eq!(cache.get(&key()).await.unwrap(), Some(vec![9; 1024]));
        cache.close().await.unwrap();
        drop(cache);

        let reopened = FoyerNodeCache::open(config(directory.path().to_path_buf()))
            .await
            .unwrap();
        assert_eq!(reopened.get(&key()).await.unwrap(), Some(vec![9; 1024]));
        reopened.remove(&key()).await.unwrap();
        assert_eq!(reopened.get(&key()).await.unwrap(), None);
        reopened.close().await.unwrap();
        drop(reopened);

        let reopened_after_remove = FoyerNodeCache::open(config(directory.path().to_path_buf()))
            .await
            .unwrap();
        assert_eq!(reopened_after_remove.get(&key()).await.unwrap(), None);
        reopened_after_remove
            .insert(key(), vec![7; 1024])
            .await
            .unwrap();
        assert_eq!(
            reopened_after_remove.get(&key()).await.unwrap(),
            Some(vec![7; 1024])
        );
        reopened_after_remove.close().await.unwrap();
        drop(reopened_after_remove);

        let reopened_after_reinsert = FoyerNodeCache::open(config(directory.path().to_path_buf()))
            .await
            .unwrap();
        assert_eq!(
            reopened_after_reinsert.get(&key()).await.unwrap(),
            Some(vec![7; 1024])
        );
        reopened_after_reinsert.close().await.unwrap();
    }

    #[tokio::test]
    async fn pinned_nodes_use_the_bounded_memory_tier() {
        let directory = tempfile::tempdir().unwrap();
        let cache = FoyerNodeCache::open(config(directory.path().to_path_buf()))
            .await
            .unwrap();
        cache.pin(key(), vec![7; 1024]).await.unwrap();
        assert_eq!(cache.pinned_usage(), Some((1, 1024)));
        assert_eq!(cache.get(&key()).await.unwrap(), Some(vec![7; 1024]));
        cache.remove(&key()).await.unwrap();
        assert_eq!(cache.pinned_usage(), Some((0, 0)));
        assert_eq!(cache.get(&key()).await.unwrap(), None);
        cache.close().await.unwrap();
    }
}
