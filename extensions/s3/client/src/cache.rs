use std::{path::PathBuf, sync::Arc};

use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCachePolicy,
    PsyncIoEngineConfig, S3FifoConfig,
};
use prolly_s3_core::{NodeCache, NodeCacheError, NodeCacheKey};

use crate::{Error, ErrorCode, Result};

const CACHE_VALUE_PREFIX: &[u8] = b"PS3C\x01";
const CACHE_TOMBSTONE: &[u8] = b"PS3C\x00";

#[derive(Clone, Debug)]
pub struct FoyerNodeCacheConfig {
    pub directory: PathBuf,
    pub memory_capacity_bytes: usize,
    pub disk_capacity_bytes: usize,
    pub disk_block_size_bytes: usize,
    pub memory_shards: usize,
}

impl FoyerNodeCacheConfig {
    pub fn validate(&self) -> Result<()> {
        if self.memory_capacity_bytes == 0
            || self.disk_capacity_bytes == 0
            || self.disk_block_size_bytes == 0
            || self.memory_shards == 0
            || self.disk_block_size_bytes > self.disk_capacity_bytes
        {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "Foyer cache capacities, block size, and shard count must be nonzero, and the block size must fit the disk capacity",
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
}

impl FoyerNodeCache {
    pub async fn open(config: FoyerNodeCacheConfig) -> Result<Arc<Self>> {
        config.validate()?;
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
        Ok(Arc::new(Self { cache }))
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
    async fn get(
        &self,
        key: &NodeCacheKey,
    ) -> std::result::Result<Option<Vec<u8>>, NodeCacheError> {
        let encoded = key.encode().to_vec();
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
        let encoded = key.encode().to_vec();
        let mut encoded_value = Vec::with_capacity(CACHE_VALUE_PREFIX.len() + value.len());
        encoded_value.extend_from_slice(CACHE_VALUE_PREFIX);
        encoded_value.extend(value);
        self.cache.insert(encoded, encoded_value);
        Ok(())
    }

    async fn remove(&self, key: &NodeCacheKey) -> std::result::Result<(), NodeCacheError> {
        let encoded = key.encode().to_vec();
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
    use prolly_s3_core::{Cid, RepositoryId, TreeFormatDigest};

    use super::*;

    fn key() -> NodeCacheKey {
        NodeCacheKey {
            repository: RepositoryId::from_hash([1; 32]),
            tree_format: TreeFormatDigest::from_hash([2; 32]),
            cid: Cid([3; 32]),
        }
    }

    #[tokio::test]
    async fn hybrid_cache_persists_verified_node_values() {
        let directory = tempfile::tempdir().unwrap();
        let cache = FoyerNodeCache::open(FoyerNodeCacheConfig {
            directory: directory.path().to_path_buf(),
            memory_capacity_bytes: 1024 * 1024,
            disk_capacity_bytes: 16 * 1024 * 1024,
            disk_block_size_bytes: 1024 * 1024,
            memory_shards: 1,
        })
        .await
        .unwrap();
        cache.insert(key(), vec![9; 1024]).await.unwrap();
        assert_eq!(cache.get(&key()).await.unwrap(), Some(vec![9; 1024]));
        cache.close().await.unwrap();
        drop(cache);

        let reopened = FoyerNodeCache::open(FoyerNodeCacheConfig {
            directory: directory.path().to_path_buf(),
            memory_capacity_bytes: 1024 * 1024,
            disk_capacity_bytes: 16 * 1024 * 1024,
            disk_block_size_bytes: 1024 * 1024,
            memory_shards: 1,
        })
        .await
        .unwrap();
        assert_eq!(reopened.get(&key()).await.unwrap(), Some(vec![9; 1024]));
        reopened.remove(&key()).await.unwrap();
        assert_eq!(reopened.get(&key()).await.unwrap(), None);
        reopened.close().await.unwrap();
        drop(reopened);

        let reopened_after_remove = FoyerNodeCache::open(FoyerNodeCacheConfig {
            directory: directory.path().to_path_buf(),
            memory_capacity_bytes: 1024 * 1024,
            disk_capacity_bytes: 16 * 1024 * 1024,
            disk_block_size_bytes: 1024 * 1024,
            memory_shards: 1,
        })
        .await
        .unwrap();
        assert_eq!(reopened_after_remove.get(&key()).await.unwrap(), None);
        reopened_after_remove.close().await.unwrap();
    }
}
