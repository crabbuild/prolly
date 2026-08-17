use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
};

use prolly::Cid;

use crate::{RepositoryId, TreeFormatDigest};

/// Stable identity for an immutable Prolly node in a local cache.
///
/// Repository isolation is intentional: callers may opt into a shared cache,
/// but entries from different repositories never alias by CID alone.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeCacheKey {
    pub repository: RepositoryId,
    pub tree_format: TreeFormatDigest,
    pub cid: Cid,
}

impl NodeCacheKey {
    /// Canonical fixed-width representation suitable for external cache keys.
    pub fn encode(&self) -> [u8; 96] {
        let mut encoded = [0u8; 96];
        encoded[..32].copy_from_slice(self.repository.as_bytes());
        encoded[32..64].copy_from_slice(self.tree_format.as_bytes());
        encoded[64..].copy_from_slice(self.cid.as_bytes());
        encoded
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("{message}")]
pub struct NodeCacheError {
    message: Arc<str>,
}

impl NodeCacheError {
    pub fn new(message: impl Into<Arc<str>>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Best-effort cache for immutable, content-addressed Prolly node bytes.
///
/// Implementations must be safe for concurrent use. The repository verifies
/// every returned value against the CID and treats cache errors as misses, so
/// this trait never becomes part of the repository's correctness authority.
#[async_trait::async_trait]
pub trait NodeCache: Send + Sync + 'static {
    /// Return whether this cache's admission policy can accept a value of this
    /// size. Implementations may override the default to make deliberate
    /// rejections observable without treating them as cache failures.
    fn admits(&self, _key: &NodeCacheKey, _value_len: usize) -> bool {
        true
    }

    /// Current node/byte usage of a distinct pinned tier, when the cache can
    /// report it exactly. `None` asks the repository to use bounded fallback
    /// accounting for successful pin requests.
    fn pinned_usage(&self) -> Option<(usize, usize)> {
        None
    }

    async fn get(&self, key: &NodeCacheKey)
        -> std::result::Result<Option<Vec<u8>>, NodeCacheError>;

    async fn insert(
        &self,
        key: NodeCacheKey,
        value: Vec<u8>,
    ) -> std::result::Result<(), NodeCacheError>;

    /// Retain a verified upper-level node in the fastest available cache tier.
    ///
    /// Pinning is advisory and byte bounded. Implementations that do not have
    /// a distinct pinned tier may treat this as an ordinary insertion. Cache
    /// loss or pin rejection can affect latency but never repository
    /// correctness.
    async fn pin(
        &self,
        key: NodeCacheKey,
        value: Vec<u8>,
    ) -> std::result::Result<(), NodeCacheError> {
        self.insert(key, value).await
    }

    async fn remove(&self, key: &NodeCacheKey) -> std::result::Result<(), NodeCacheError>;
}

struct MemoryNodeCacheState {
    entries: BTreeMap<NodeCacheKey, Arc<[u8]>>,
    order: VecDeque<NodeCacheKey>,
    pinned: BTreeSet<NodeCacheKey>,
    bytes: usize,
}

/// A byte-bounded in-memory node cache used when no external cache is
/// configured. It deliberately favors a small, deterministic implementation;
/// production deployments can inject a hybrid cache through [`NodeCache`].
pub struct MemoryNodeCache {
    max_bytes: usize,
    state: Mutex<MemoryNodeCacheState>,
}

impl MemoryNodeCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            state: Mutex::new(MemoryNodeCacheState {
                entries: BTreeMap::new(),
                order: VecDeque::new(),
                pinned: BTreeSet::new(),
                bytes: 0,
            }),
        }
    }

    pub fn resident_bytes(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bytes
    }

    pub fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn pinned_len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pinned
            .len()
    }

    fn insert_locked(
        &self,
        state: &mut MemoryNodeCacheState,
        key: NodeCacheKey,
        value: Vec<u8>,
        pin: bool,
    ) -> bool {
        if !self.admits(&key, value.len()) {
            return false;
        }
        let was_pinned = state.pinned.contains(&key);
        if let Some(previous) = state.entries.remove(&key) {
            state.bytes = state.bytes.saturating_sub(previous.len());
        }
        state.order.retain(|candidate| candidate != &key);
        state.bytes = state.bytes.saturating_add(value.len());
        state.entries.insert(key.clone(), Arc::from(value));
        state.order.push_back(key.clone());
        if pin || was_pinned {
            state.pinned.insert(key.clone());
        }

        let mut inspected = 0usize;
        while state.bytes > self.max_bytes && inspected < state.order.len() {
            let Some(candidate) = state.order.pop_front() else {
                break;
            };
            if state.pinned.contains(&candidate) {
                state.order.push_back(candidate);
                inspected = inspected.saturating_add(1);
                continue;
            }
            if let Some(removed) = state.entries.remove(&candidate) {
                state.bytes = state.bytes.saturating_sub(removed.len());
            }
            inspected = 0;
        }

        // A pin set larger than the configured capacity is rejected rather
        // than allowing an advisory tier to become unbounded.
        if state.bytes > self.max_bytes {
            state.pinned.remove(&key);
            if let Some(removed) = state.entries.remove(&key) {
                state.bytes = state.bytes.saturating_sub(removed.len());
            }
            state.order.retain(|candidate| candidate != &key);
        }
        state.entries.contains_key(&key) && (!pin || state.pinned.contains(&key))
    }
}

#[async_trait::async_trait]
impl NodeCache for MemoryNodeCache {
    fn admits(&self, _key: &NodeCacheKey, value_len: usize) -> bool {
        self.max_bytes > 0 && value_len <= self.max_bytes
    }

    fn pinned_usage(&self) -> Option<(usize, usize)> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Some((
            state.pinned.len(),
            state
                .pinned
                .iter()
                .filter_map(|key| state.entries.get(key))
                .map(|value| value.len())
                .sum(),
        ))
    }

    async fn get(
        &self,
        key: &NodeCacheKey,
    ) -> std::result::Result<Option<Vec<u8>>, NodeCacheError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(value) = state.entries.get(key).cloned() else {
            return Ok(None);
        };
        state.order.retain(|candidate| candidate != key);
        state.order.push_back(key.clone());
        Ok(Some(value.as_ref().to_vec()))
    }

    async fn insert(
        &self,
        key: NodeCacheKey,
        value: Vec<u8>,
    ) -> std::result::Result<(), NodeCacheError> {
        if !self.admits(&key, value.len()) {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.insert_locked(&mut state, key, value, false);
        Ok(())
    }

    async fn pin(
        &self,
        key: NodeCacheKey,
        value: Vec<u8>,
    ) -> std::result::Result<(), NodeCacheError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.insert_locked(&mut state, key, value, true) {
            Ok(())
        } else {
            Err(NodeCacheError::new(
                "pinned memory-cache capacity is exhausted",
            ))
        }
    }

    async fn remove(&self, key: &NodeCacheKey) -> std::result::Result<(), NodeCacheError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(value) = state.entries.remove(key) {
            state.bytes = state.bytes.saturating_sub(value.len());
        }
        state.pinned.remove(key);
        state.order.retain(|candidate| candidate != key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> NodeCacheKey {
        NodeCacheKey {
            repository: RepositoryId::from_hash([1; 32]),
            tree_format: TreeFormatDigest::from_hash([2; 32]),
            cid: Cid([byte; 32]),
        }
    }

    #[tokio::test]
    async fn memory_cache_is_byte_bounded_and_lru() {
        let cache = MemoryNodeCache::new(6);
        cache.insert(key(1), vec![1; 3]).await.unwrap();
        cache.insert(key(2), vec![2; 3]).await.unwrap();
        assert_eq!(cache.get(&key(1)).await.unwrap(), Some(vec![1; 3]));
        cache.insert(key(3), vec![3; 3]).await.unwrap();
        assert_eq!(cache.get(&key(2)).await.unwrap(), None);
        assert_eq!(cache.get(&key(1)).await.unwrap(), Some(vec![1; 3]));
        assert_eq!(cache.get(&key(3)).await.unwrap(), Some(vec![3; 3]));
        assert_eq!(cache.resident_bytes(), 6);
    }

    #[tokio::test]
    async fn oversized_values_are_not_admitted() {
        let cache = MemoryNodeCache::new(2);
        cache.insert(key(1), vec![1; 3]).await.unwrap();
        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn pinned_nodes_survive_lru_pressure_within_the_byte_bound() {
        let cache = MemoryNodeCache::new(6);
        cache.pin(key(1), vec![1; 3]).await.unwrap();
        cache.insert(key(2), vec![2; 3]).await.unwrap();
        cache.insert(key(3), vec![3; 3]).await.unwrap();

        assert_eq!(cache.get(&key(1)).await.unwrap(), Some(vec![1; 3]));
        assert_eq!(cache.get(&key(2)).await.unwrap(), None);
        assert_eq!(cache.get(&key(3)).await.unwrap(), Some(vec![3; 3]));
        assert_eq!(cache.pinned_len(), 1);
        assert_eq!(cache.pinned_usage(), Some((1, 3)));
        assert_eq!(cache.resident_bytes(), 6);
    }

    #[test]
    fn encoded_key_is_namespaced_and_fixed_width() {
        let first = key(1).encode();
        let second = key(2).encode();
        assert_eq!(first.len(), 96);
        assert_ne!(first, second);
    }
}
