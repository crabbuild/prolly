use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};

use futures_util::stream::{self, StreamExt};
use rayon::prelude::*;

use super::validation;
use super::ProllyEngine;
use crate::prolly::error::{Error, Mutation};
use crate::prolly::node::Node;
use crate::prolly::store::{
    AsyncStore, BatchOp, NodePublication, NodePublicationHint, PublicationOrigin, SharedReadBatch,
    Store,
};
use crate::prolly::tree::Tree;
use crate::prolly::write::CanonicalWriteManager;
use crate::prolly::{Cid, ProllyMetricsSnapshot};
use crate::{Conflict, Resolution};

const MAX_REPLAY_ROUNDS: usize = 256;
type PublicationWrites = Vec<(Cid, Vec<u8>)>;

enum ReadyNodeBytes {
    Owned(Vec<u8>),
    Shared(Arc<[u8]>),
}

impl AsRef<[u8]> for ReadyNodeBytes {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared(bytes) => bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ReplayIo {
    pub(crate) nodes_read: usize,
    pub(crate) bytes_read: usize,
    pub(crate) nodes_written: usize,
    pub(crate) bytes_written: usize,
}

impl<S> ProllyEngine<crate::prolly::store::SyncStoreAsAsync<Arc<S>>>
where
    S: Store,
{
    fn load_rightmost_path_hint_ready(
        &self,
        tree: &Tree,
        root: &Cid,
    ) -> Result<Option<Vec<crate::prolly::AsyncRightmostPathEntry>>, Error> {
        let Some(bytes) = self
            .store
            .inner()
            .get_hint(
                crate::prolly::RIGHTMOST_PATH_HINT_NAMESPACE,
                root.as_bytes(),
            )
            .map_err(|error| Error::Store(Box::new(error)))?
        else {
            return Ok(None);
        };
        let Ok(hint) = serde_cbor::from_slice::<crate::prolly::AsyncRightmostPathHint>(&bytes)
        else {
            return Ok(None);
        };
        if hint.version != 2
            || hint.entries.is_empty()
            || hint.entries.first().map(|entry| &entry.cid) != Some(root)
        {
            return Ok(None);
        }

        let keys = hint
            .entries
            .iter()
            .map(|entry| entry.cid.as_bytes())
            .collect::<Vec<_>>();
        let node_bytes = self
            .store
            .inner()
            .batch_get_ordered_unique(&keys)
            .map_err(|error| Error::Store(Box::new(error)))?;
        if node_bytes.len() != hint.entries.len() || node_bytes.iter().any(Option::is_none) {
            return Ok(None);
        }

        let mut path = Vec::with_capacity(hint.entries.len());
        for (entry, bytes) in hint.entries.into_iter().zip(node_bytes) {
            let Some(bytes) = bytes else {
                return Ok(None);
            };
            let Ok(node) = validation::decode_owned(&entry.cid, &tree.config.format, &bytes) else {
                return Ok(None);
            };
            path.push(crate::prolly::AsyncRightmostPathEntry {
                cid: entry.cid,
                node,
                child_index: entry.child_index,
            });
        }
        if !crate::prolly::rightmost_path_hint_is_valid(root, &path) {
            return Ok(None);
        }
        for entry in &path {
            self.cache_node(entry.cid.clone(), entry.node.clone());
        }
        Ok(Some(path))
    }

    fn should_publish_ready_hint(
        &self,
        tree: &Tree,
        mutations: &[Mutation],
    ) -> Result<bool, Error> {
        if !self.store.inner().supports_hints()
            || !self.store.inner().prefers_rightmost_path_hints()
            || mutations.is_empty()
            || mutations
                .iter()
                .any(|mutation| !matches!(mutation, Mutation::Upsert { .. }))
        {
            return Ok(false);
        }
        let Some(root) = tree.root.as_ref() else {
            return Ok(true);
        };
        let smallest = mutations
            .iter()
            .map(Mutation::key)
            .min()
            .ok_or(Error::InvalidNode)?;
        if self.cached_rightmost_path(root).is_none() {
            if let Some(path) = self.load_rightmost_path_hint_ready(tree, root)? {
                self.cache_rightmost_path(
                    root.clone(),
                    crate::prolly::cached_rightmost_entries(&path),
                );
            }
        }
        let maximum = if let Some(path) = self.cached_rightmost_path(root) {
            path.last()
                .and_then(|entry| entry.node.keys.last())
                .cloned()
                .ok_or(Error::InvalidNode)?
        } else {
            let manager = ReadyWriteManager::new(self, &tree.config, PublicationOrigin::General);
            let path = canonical_rightmost_path(&manager, root)?;
            let maximum = path
                .last()
                .and_then(|entry| entry.node.keys.last())
                .cloned()
                .ok_or(Error::InvalidNode)?;
            self.cache_rightmost_path(root.clone(), crate::prolly::cached_rightmost_entries(&path));
            maximum
        };
        Ok(smallest > maximum.as_slice())
    }

    fn publish_ready_hint(
        &self,
        manager: &ReadyWriteManager<'_, S>,
        tree: &Tree,
    ) -> Result<(), Error> {
        if manager.store.buffer_is_empty() {
            return Ok(());
        }
        let root = tree.root.as_ref().ok_or(Error::InvalidNode)?;
        let path = canonical_rightmost_path(manager, root)?;
        let hint = crate::prolly::encode_rightmost_path_hint(&path)?;
        let entries = manager.store.take_buffered_entries();
        let entry_refs = entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect::<Vec<_>>();
        manager
            .store
            .inner
            .publish_nodes(NodePublication::with_hint(
                &entry_refs,
                NodePublicationHint::new(
                    crate::prolly::RIGHTMOST_PATH_HINT_NAMESPACE,
                    root.as_bytes(),
                    &hint,
                ),
                manager.store.origin,
            ))
            .map_err(|error| Error::Store(Box::new(error)))?;
        self.cache_rightmost_path(root.clone(), crate::prolly::cached_rightmost_entries(&path));
        Ok(())
    }

    pub(crate) fn structural_merge_ready(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        resolver: Option<&(dyn Fn(&Conflict) -> Resolution + Send + Sync)>,
    ) -> Result<Option<Tree>, Error> {
        let manager = ReadyWriteManager::new(self, &base.config, PublicationOrigin::Merge);
        crate::prolly::diff::try_structural_merge(&manager, base, left, right, resolver)
    }

    pub(crate) fn canonical_batch_ready(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
        origin: PublicationOrigin,
    ) -> Result<(Tree, crate::prolly::write::WriteStats), Error> {
        let publish_hint = self.should_publish_ready_hint(tree, &mutations)?;
        let manager = if publish_hint {
            ReadyWriteManager::buffered(self, &tree.config, origin)
        } else {
            ReadyWriteManager::new(self, &tree.config, origin)
        };
        let result = crate::prolly::write::apply(&manager, tree, mutations)?;
        if publish_hint {
            self.publish_ready_hint(&manager, &result.0)?;
        }
        Ok(result)
    }

    pub(crate) fn canonical_batch_tree_ready(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
        origin: PublicationOrigin,
    ) -> Result<Tree, Error> {
        let publish_hint = self.should_publish_ready_hint(tree, &mutations)?;
        let manager = if publish_hint {
            ReadyWriteManager::buffered(self, &tree.config, origin)
        } else {
            ReadyWriteManager::new(self, &tree.config, origin)
        };
        let result = crate::prolly::write::apply_tree(&manager, tree, mutations)?;
        if publish_hint {
            self.publish_ready_hint(&manager, &result)?;
        }
        Ok(result)
    }

    pub(crate) fn canonical_batch_ready_configured(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
        config: &crate::prolly::parallel::ParallelConfig,
        origin: PublicationOrigin,
    ) -> Result<(Tree, crate::prolly::write::WriteStats), Error> {
        let manager = ReadyWriteManager::new(self, &tree.config, origin);
        crate::prolly::write::apply_configured(&manager, tree, mutations, config)
    }

    pub(crate) fn canonical_batch_tree_ready_configured(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
        config: &crate::prolly::parallel::ParallelConfig,
        origin: PublicationOrigin,
    ) -> Result<Tree, Error> {
        let manager = ReadyWriteManager::new(self, &tree.config, origin);
        crate::prolly::write::apply_tree_configured(&manager, tree, mutations, config)
    }

    pub(crate) fn canonical_delete_range_ready(
        &self,
        tree: &Tree,
        start: &[u8],
        end: &[u8],
    ) -> Result<(Tree, crate::prolly::write::WriteStats), Error> {
        let manager = ReadyWriteManager::new(self, &tree.config, PublicationOrigin::RangeDelete);
        crate::prolly::range_delete::apply(&manager, tree, start, end)
    }
}

struct PublicationStore<'a, S: Store> {
    inner: &'a S,
    origin: PublicationOrigin,
    buffer: Option<Arc<Mutex<BufferedPublication>>>,
}

#[derive(Default)]
struct BufferedPublication {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    positions: HashMap<Vec<u8>, usize>,
}

impl<'a, S: Store> PublicationStore<'a, S> {
    const fn new(inner: &'a S, origin: PublicationOrigin) -> Self {
        Self {
            inner,
            origin,
            buffer: None,
        }
    }

    fn buffered(inner: &'a S, origin: PublicationOrigin) -> Self {
        Self {
            inner,
            origin,
            buffer: Some(Arc::new(Mutex::new(BufferedPublication::default()))),
        }
    }

    fn publication<'b>(&self, publication: NodePublication<'b>) -> NodePublication<'b> {
        match publication.hint() {
            Some(hint) => NodePublication::with_hint(publication.entries(), hint, self.origin),
            None => NodePublication::new(publication.entries(), self.origin),
        }
    }

    fn buffer_is_empty(&self) -> bool {
        match self.buffer.as_ref() {
            Some(buffer) => buffer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .is_empty(),
            None => true,
        }
    }

    fn buffered_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        let buffer = self.buffer.as_ref()?;
        let buffer = buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        buffer
            .positions
            .get(key)
            .map(|position| buffer.entries[*position].1.clone())
    }

    fn buffer_publication(&self, publication: NodePublication<'_>) -> bool {
        let Some(buffer) = self.buffer.as_ref() else {
            return false;
        };
        let mut buffer = buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (key, value) in publication.entries() {
            if let Some(position) = buffer.positions.get(*key).copied() {
                buffer.entries[position].1.clear();
                buffer.entries[position].1.extend_from_slice(value);
            } else {
                let position = buffer.entries.len();
                buffer.entries.push((key.to_vec(), value.to_vec()));
                buffer.positions.insert(key.to_vec(), position);
            }
        }
        true
    }

    fn take_buffered_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let Some(buffer) = self.buffer.as_ref() else {
            return Vec::new();
        };
        let mut buffer = buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        buffer.positions.clear();
        std::mem::take(&mut buffer.entries)
    }
}

impl<S: Store> Store for PublicationStore<'_, S> {
    type Error = S::Error;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        if let Some(value) = self.buffered_value(key) {
            return Ok(Some(value));
        }
        self.inner.get(key)
    }

    fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        if let Some(value) = self.buffered_value(key) {
            return Ok(Some(Arc::from(value.into_boxed_slice())));
        }
        self.inner.get_shared(key)
    }

    fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<SharedReadBatch, Self::Error> {
        if !self.buffer_is_empty() {
            return keys.iter().map(|key| self.get_shared(key)).collect();
        }
        self.inner.batch_get_shared_ordered_unique(keys)
    }

    fn has_native_shared_reads(&self) -> bool {
        self.inner.has_native_shared_reads()
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let entries = [(key, value)];
        self.publish_nodes(NodePublication::new(&entries, self.origin))
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key)
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        if self.buffer.is_some() {
            for operation in ops {
                match operation {
                    BatchOp::Upsert { key, value } => self.put(key, value)?,
                    BatchOp::Delete { key } => self.inner.delete(key)?,
                }
            }
            return Ok(());
        }
        self.inner.batch(ops)
    }

    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        if !self.buffer_is_empty() {
            let mut values = HashMap::with_capacity(keys.len());
            for key in keys {
                if let Some(value) = self.get(key)? {
                    values.insert(key.to_vec(), value);
                }
            }
            return Ok(values);
        }
        self.inner.batch_get(keys)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        if !self.buffer_is_empty() {
            return keys.iter().map(|key| self.get(key)).collect();
        }
        self.inner.batch_get_ordered(keys)
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        if !self.buffer_is_empty() {
            return keys.iter().map(|key| self.get(key)).collect();
        }
        self.inner.batch_get_ordered_unique(keys)
    }

    fn prefers_batch_reads(&self) -> bool {
        self.inner.prefers_batch_reads()
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        self.publish_nodes(NodePublication::new(entries, self.origin))
    }

    fn supports_hints(&self) -> bool {
        self.inner.supports_hints()
    }

    fn prefers_rightmost_path_hints(&self) -> bool {
        self.inner.prefers_rightmost_path_hints()
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get_hint(namespace, key)
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.inner.put_hint(namespace, key, value)
    }

    fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.publish_nodes(NodePublication::with_hint(
            entries,
            NodePublicationHint::new(namespace, key, value),
            self.origin,
        ))
    }

    fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        if self.buffer_publication(publication) {
            return Ok(());
        }
        self.inner.publish_nodes(self.publication(publication))
    }
}

struct ReadyWriteManager<'a, S: Store> {
    engine: &'a ProllyEngine<crate::prolly::store::SyncStoreAsAsync<Arc<S>>>,
    store: PublicationStore<'a, S>,
    config: &'a crate::prolly::Config,
}

impl<'a, S: Store> ReadyWriteManager<'a, S> {
    fn new(
        engine: &'a ProllyEngine<crate::prolly::store::SyncStoreAsAsync<Arc<S>>>,
        config: &'a crate::prolly::Config,
        origin: PublicationOrigin,
    ) -> Self {
        Self {
            engine,
            store: PublicationStore::new(engine.store.inner().as_ref(), origin),
            config,
        }
    }

    fn buffered(
        engine: &'a ProllyEngine<crate::prolly::store::SyncStoreAsAsync<Arc<S>>>,
        config: &'a crate::prolly::Config,
        origin: PublicationOrigin,
    ) -> Self {
        Self {
            engine,
            store: PublicationStore::buffered(engine.store.inner().as_ref(), origin),
            config,
        }
    }
}

impl<'a, S> CanonicalWriteManager for ReadyWriteManager<'a, S>
where
    S: Store,
{
    type Store = PublicationStore<'a, S>;

    fn write_store(&self) -> &Self::Store {
        &self.store
    }

    fn write_config(&self) -> &crate::prolly::Config {
        self.config
    }

    fn write_load_arc(&self, cid: &Cid) -> Result<Arc<Node>, Error> {
        // Reads do not mutate bounded-cache recency. Avoiding an exclusive lock
        // keeps the synchronous facade's hot path comparable to the async core;
        // insertion and eviction still enforce the configured bounds.
        let unbounded = if let Ok(cache) = self.engine.node_cache.read() {
            if let Some(node) = cache.peek(cid) {
                super::validate_cached_node(&node, &self.config.format)?;
                self.engine.metrics.add_cache_hits(1);
                return Ok(node);
            }
            cache.is_unbounded()
        } else {
            false
        };
        if !unbounded {
            if let Ok(mut cache) = self.engine.node_cache.write() {
                if let Some(node) = cache.get(cid) {
                    super::validate_cached_node(&node, &self.config.format)?;
                    self.engine.metrics.add_cache_hits(1);
                    return Ok(node);
                }
            }
        }
        self.engine.metrics.add_cache_misses(1);
        let bytes: Arc<[u8]> = if self.write_store().has_native_shared_reads() {
            self.write_store()
                .get_shared(cid.as_bytes())
                .map_err(|error| Error::Store(Box::new(error)))?
                .ok_or_else(|| Error::NotFound(cid.clone()))?
        } else {
            self.write_store()
                .get(cid.as_bytes())
                .map_err(|error| Error::Store(Box::new(error)))?
                .map(|bytes| Arc::from(bytes.into_boxed_slice()))
                .ok_or_else(|| Error::NotFound(cid.clone()))?
        };
        let node = Arc::new(validation::decode_owned(cid, &self.config.format, &bytes)?);
        self.engine.metrics.record_point_read(bytes.len());
        if let Ok(mut cache) = self.engine.node_cache.write() {
            let evictions = cache.insert(cid.clone(), node.clone(), bytes.len());
            self.engine.metrics.add_cache_evictions(evictions);
        }
        Ok(node)
    }

    fn write_load_many_ordered(&self, cids: &[Cid]) -> Result<Vec<Arc<Node>>, Error> {
        if let Ok(cache) = self.engine.node_cache.read() {
            let cached = cids
                .iter()
                .map(|cid| cache.peek(cid))
                .collect::<Option<Vec<_>>>();
            if let Some(nodes) = cached {
                for node in &nodes {
                    super::validate_cached_node(node, &self.config.format)?;
                }
                self.engine.metrics.add_cache_hits(nodes.len());
                return Ok(nodes);
            }
        }
        let mut nodes = vec![None; cids.len()];
        let mut missing_cids = Vec::new();
        let mut missing_positions = Vec::new();
        let unbounded = self
            .engine
            .node_cache
            .read()
            .is_ok_and(|cache| cache.is_unbounded());
        if unbounded {
            if let Ok(cache) = self.engine.node_cache.read() {
                for (position, cid) in cids.iter().enumerate() {
                    if let Some(node) = cache.peek(cid) {
                        super::validate_cached_node(&node, &self.config.format)?;
                        self.engine.metrics.add_cache_hits(1);
                        nodes[position] = Some(node);
                    } else {
                        self.engine.metrics.add_cache_misses(1);
                        missing_cids.push(cid.clone());
                        missing_positions.push(position);
                    }
                }
            }
        } else if let Ok(mut cache) = self.engine.node_cache.write() {
            for (position, cid) in cids.iter().enumerate() {
                if let Some(node) = cache.get(cid) {
                    super::validate_cached_node(&node, &self.config.format)?;
                    self.engine.metrics.add_cache_hits(1);
                    nodes[position] = Some(node);
                } else {
                    self.engine.metrics.add_cache_misses(1);
                    missing_cids.push(cid.clone());
                    missing_positions.push(position);
                }
            }
        } else {
            missing_cids.extend_from_slice(cids);
            missing_positions.extend(0..cids.len());
        }
        if !missing_cids.is_empty() {
            let keys = missing_cids
                .iter()
                .map(|cid| cid.as_bytes())
                .collect::<Vec<_>>();
            let batch_len = keys.len();
            let values = if self.write_store().has_native_shared_reads() {
                self.write_store()
                    .batch_get_shared_ordered_unique(&keys)
                    .map_err(|error| Error::Store(Box::new(error)))?
                    .into_iter()
                    .map(|value| value.map(ReadyNodeBytes::Shared))
                    .collect::<Vec<_>>()
            } else {
                self.write_store()
                    .batch_get_ordered_unique(&keys)
                    .map_err(|error| Error::Store(Box::new(error)))?
                    .into_iter()
                    .map(|value| value.map(ReadyNodeBytes::Owned))
                    .collect::<Vec<_>>()
            };
            drop(keys);
            if values.len() != missing_cids.len() {
                return Err(Error::InvalidNode);
            }
            let mut read_bytes = 0usize;
            for ((cid, position), value) in
                missing_cids.into_iter().zip(missing_positions).zip(values)
            {
                let bytes = value.ok_or_else(|| Error::NotFound(cid.clone()))?;
                let bytes = bytes.as_ref();
                let node = Arc::new(validation::decode_owned(&cid, &self.config.format, bytes)?);
                read_bytes = read_bytes.saturating_add(bytes.len());
                if let Ok(mut cache) = self.engine.node_cache.write() {
                    let evictions = cache.insert(cid, node.clone(), bytes.len());
                    self.engine.metrics.add_cache_evictions(evictions);
                }
                nodes[position] = Some(node);
            }
            self.engine
                .metrics
                .record_batch_read(batch_len, read_bytes, batch_len);
        }
        nodes
            .into_iter()
            .map(|node| node.ok_or(Error::InvalidNode))
            .collect()
    }

    fn write_load_many_ordered_with_parallelism(
        &self,
        cids: &[Cid],
        parallelism: usize,
    ) -> Result<Vec<Arc<Node>>, Error> {
        // Check the whole frontier before partitioning. Structural merge
        // prefetches are commonly already hot; scheduling Rayon work for those
        // hits dominated small in-memory merges.
        let all_cached = self
            .engine
            .node_cache
            .read()
            .is_ok_and(|cache| cids.iter().all(|cid| cache.nodes.contains_key(cid)));
        if all_cached || parallelism <= 1 || cids.len() <= parallelism {
            return self.write_load_many_ordered(cids);
        }

        let width = parallelism.min(cids.len());
        let chunk_size = cids.len().div_ceil(width);
        let partitions = cids
            .par_chunks(chunk_size)
            .map(|chunk| self.write_load_many_ordered(chunk))
            .collect::<Vec<_>>();
        let mut nodes = Vec::with_capacity(cids.len());
        for partition in partitions {
            nodes.extend(partition?);
        }
        Ok(nodes)
    }

    fn write_cache_node(&self, cid: Cid, node: Node) {
        if let Ok(mut cache) = self.engine.node_cache.write() {
            let bytes = node.encoded_len();
            let evictions = cache.insert(cid, Arc::new(node), bytes);
            self.engine.metrics.add_cache_evictions(evictions);
        }
    }

    fn write_metrics(&self) -> ProllyMetricsSnapshot {
        self.engine.metrics.snapshot()
    }

    fn write_record_batch_metrics(&self, nodes: usize, bytes: usize) {
        self.engine.metrics.record_batch_write(nodes, bytes);
    }

    fn write_should_try_batched_value_updates(
        &self,
        tree: &Tree,
        mutation_count: usize,
        policy: crate::prolly::parallel::ExecutionPolicy,
    ) -> bool {
        crate::prolly::batch::should_try_batched_value_updates(self, tree, mutation_count, policy)
    }

    fn write_sampled_value_updates_are_key_stable(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
        policy: crate::prolly::parallel::ExecutionPolicy,
    ) -> Result<bool, Error> {
        crate::prolly::batch::sampled_value_updates_are_likely_key_stable(
            self, tree, mutations, policy,
        )
    }

    fn write_try_apply_batched_value_updates(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
        policy: crate::prolly::parallel::ExecutionPolicy,
    ) -> Result<crate::prolly::batch::KeyStableBatchAttempt, Error> {
        crate::prolly::batch::try_apply_batched_value_updates(self, tree, mutations, policy)
    }
}

#[derive(Debug)]
pub(crate) struct ReplayStoreError(&'static str);

impl fmt::Display for ReplayStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for ReplayStoreError {}

#[derive(Default)]
pub(crate) struct ReplayStore {
    known: RwLock<HashMap<Vec<u8>, Option<Vec<u8>>>>,
    loaded: Mutex<HashSet<Vec<u8>>>,
    generated: Mutex<HashSet<Vec<u8>>>,
    missing: Mutex<BTreeSet<Vec<u8>>>,
}

impl ReplayStore {
    fn begin_attempt(&self) {
        let generated = std::mem::take(&mut *self.generated.lock().unwrap());
        if !generated.is_empty() {
            self.known
                .write()
                .unwrap()
                .retain(|key, _| !generated.contains(key));
        }
        self.missing.lock().unwrap().clear();
    }

    fn insert_loaded(&self, cid: &Cid, bytes: Option<Vec<u8>>) {
        let mut loaded = self.loaded.lock().unwrap();
        if bytes.is_some() {
            loaded.insert(cid.as_bytes().to_vec());
        } else {
            loaded.remove(cid.as_bytes());
        }
        self.known
            .write()
            .unwrap()
            .insert(cid.as_bytes().to_vec(), bytes);
    }

    fn trust_loaded_references(&self, node: &Node) -> Result<(), Error> {
        if node.leaf {
            return Ok(());
        }
        let mut loaded = self.loaded.lock().unwrap();
        for value in &node.vals {
            let child = <[u8; 32]>::try_from(value.as_slice()).map_err(|_| Error::InvalidNode)?;
            loaded.insert(child.to_vec());
        }
        Ok(())
    }

    fn take_missing(&self) -> Vec<Cid> {
        std::mem::take(&mut *self.missing.lock().unwrap())
            .into_iter()
            .filter_map(|bytes| {
                let Ok(bytes) = <[u8; 32]>::try_from(bytes) else {
                    return None;
                };
                Some(Cid(bytes))
            })
            .collect()
    }

    fn publication_writes(
        &self,
        root: Option<&Cid>,
    ) -> Result<(PublicationWrites, Vec<Cid>), Error> {
        let Some(root) = root else {
            return Ok((Vec::new(), Vec::new()));
        };
        let known = self.known.read().unwrap();
        let loaded = self.loaded.lock().unwrap();
        let mut publication = BTreeMap::new();
        let mut missing = BTreeSet::new();
        let mut visited = HashSet::new();
        let mut stack = vec![root.clone()];
        while let Some(cid) = stack.pop() {
            if !visited.insert(cid.clone()) {
                continue;
            }
            if loaded.contains(cid.as_bytes()) {
                continue;
            }
            let Some(value) = known.get(cid.as_bytes()) else {
                missing.insert(cid.as_bytes().to_vec());
                continue;
            };
            let bytes = value.as_ref().ok_or_else(|| Error::NotFound(cid.clone()))?;
            publication.insert(cid.as_bytes().to_vec(), (cid.clone(), bytes.clone()));
            let node = Node::from_bytes(bytes)?;
            if !node.leaf {
                for value in &node.vals {
                    let child =
                        <[u8; 32]>::try_from(value.as_slice()).map_err(|_| Error::InvalidNode)?;
                    stack.push(Cid(child));
                }
            }
        }
        let missing = missing
            .into_iter()
            .map(|bytes| {
                <[u8; 32]>::try_from(bytes)
                    .map(Cid)
                    .map_err(|_| Error::InvalidNode)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((publication.into_values().collect(), missing))
    }
}

impl Store for ReplayStore {
    type Error = ReplayStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        if let Some(value) = self.known.read().unwrap().get(key) {
            return Ok(value.clone());
        }
        self.missing.lock().unwrap().insert(key.to_vec());
        Ok(None)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.generated.lock().unwrap().insert(key.to_vec());
        self.known
            .write()
            .unwrap()
            .insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.known.write().unwrap().insert(key.to_vec(), None);
        Ok(())
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        for operation in ops {
            match operation {
                BatchOp::Upsert { key, value } => self.put(key, value)?,
                BatchOp::Delete { key } => self.delete(key)?,
            }
        }
        Ok(())
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        keys.iter().map(|key| self.get(key)).collect()
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        for (key, value) in entries {
            self.put(key, value)?;
        }
        Ok(())
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }
}

pub(crate) struct ReplayWriteManager {
    store: Arc<ReplayStore>,
    config: crate::prolly::Config,
}

impl crate::prolly::write::CanonicalWriteManager for ReplayWriteManager {
    type Store = Arc<ReplayStore>;
    const EAGER_BATCHED_VALUE_UPDATES: bool = true;

    fn write_store(&self) -> &Self::Store {
        &self.store
    }

    fn write_config(&self) -> &crate::prolly::Config {
        &self.config
    }

    fn write_load_arc(&self, cid: &Cid) -> Result<Arc<Node>, Error> {
        let bytes = self
            .store
            .get(cid.as_bytes())
            .map_err(|error| Error::Store(Box::new(error)))?
            .ok_or_else(|| Error::NotFound(cid.clone()))?;
        Ok(Arc::new(validation::decode_owned(
            cid,
            &self.config.format,
            &bytes,
        )?))
    }

    fn write_load_many_ordered(&self, cids: &[Cid]) -> Result<Vec<Arc<Node>>, Error> {
        let keys = cids.iter().map(|cid| cid.as_bytes()).collect::<Vec<_>>();
        let values = self
            .store
            .batch_get_ordered_unique(&keys)
            .map_err(|error| Error::Store(Box::new(error)))?;
        if values.len() != cids.len() {
            return Err(Error::InvalidNode);
        }
        cids.iter()
            .zip(values)
            .map(|(cid, value)| {
                let bytes = value.ok_or_else(|| Error::NotFound(cid.clone()))?;
                Ok(Arc::new(validation::decode_owned(
                    cid,
                    &self.config.format,
                    &bytes,
                )?))
            })
            .collect()
    }

    fn write_cache_node(&self, _cid: Cid, _node: Node) {}

    fn write_metrics(&self) -> ProllyMetricsSnapshot {
        ProllyMetricsSnapshot::default()
    }

    fn write_record_batch_metrics(&self, _nodes: usize, _bytes: usize) {}

    fn write_should_try_batched_value_updates(
        &self,
        tree: &Tree,
        mutation_count: usize,
        policy: crate::prolly::parallel::ExecutionPolicy,
    ) -> bool {
        crate::prolly::batch::should_try_batched_value_updates(self, tree, mutation_count, policy)
    }

    fn write_sampled_value_updates_are_key_stable(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
        policy: crate::prolly::parallel::ExecutionPolicy,
    ) -> Result<bool, Error> {
        crate::prolly::batch::sampled_value_updates_are_likely_key_stable(
            self, tree, mutations, policy,
        )
    }

    fn write_try_apply_batched_value_updates(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
        policy: crate::prolly::parallel::ExecutionPolicy,
    ) -> Result<crate::prolly::batch::KeyStableBatchAttempt, Error> {
        crate::prolly::batch::try_apply_batched_value_updates(self, tree, mutations, policy)
    }
}

fn update_write_stats(stats: &mut crate::prolly::write::WriteStats, io: ReplayIo) {
    stats.nodes_read = io.nodes_read as u64;
    stats.bytes_read = io.bytes_read as u64;
    stats.nodes_written = io.nodes_written as u64;
    stats.bytes_written = io.bytes_written as u64;
}

impl<S> ProllyEngine<S>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    pub(crate) async fn canonical_batch(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
        origin: PublicationOrigin,
    ) -> Result<(Tree, crate::prolly::write::WriteStats), Error> {
        let all_upserts = !mutations.is_empty()
            && mutations
                .iter()
                .all(|mutation| matches!(mutation, Mutation::Upsert { .. }));
        if let Some(root) = tree.root.as_ref().filter(|_| {
            tree.config.format == self.config.format
                && all_upserts
                && self.store.supports_hints()
                && self.store.prefers_rightmost_path_hints()
        }) {
            if self.cached_rightmost_path(root).is_none() {
                if let Some(path) = self.load_rightmost_path_hint(root).await? {
                    self.cache_rightmost_path(
                        root.clone(),
                        crate::prolly::cached_rightmost_entries(&path),
                    );
                }
            }
        }
        let publish_rightmost_hint = if tree.root.is_none() {
            all_upserts
        } else if all_upserts {
            let smallest_mutation_key = mutations.iter().map(Mutation::key).min();
            let old_maximum_key = tree
                .root
                .as_ref()
                .and_then(|root| self.cached_rightmost_path(root))
                .and_then(|path| path.last().cloned())
                .and_then(|entry| entry.node.keys.last().cloned());
            smallest_mutation_key
                .zip(old_maximum_key.as_deref())
                .is_some_and(|(smallest, maximum)| smallest > maximum)
        } else {
            false
        };
        self.execute_replay(
            tree,
            publish_rightmost_hint,
            origin,
            |manager| crate::prolly::write::apply(manager, tree, mutations.clone()),
            update_write_stats,
        )
        .await
    }

    pub(crate) async fn canonical_delete_range(
        &self,
        tree: &Tree,
        start: &[u8],
        end: &[u8],
    ) -> Result<(Tree, crate::prolly::write::WriteStats), Error> {
        self.execute_replay(
            tree,
            false,
            PublicationOrigin::RangeDelete,
            |manager| crate::prolly::range_delete::apply(manager, tree, start, end),
            update_write_stats,
        )
        .await
    }

    pub(crate) async fn execute_replay<T, F, U>(
        &self,
        tree: &Tree,
        publish_rightmost_hint: bool,
        origin: PublicationOrigin,
        mut operation: F,
        update_io: U,
    ) -> Result<(Tree, T), Error>
    where
        F: FnMut(&ReplayWriteManager) -> Result<(Tree, T), Error>,
        U: FnOnce(&mut T, ReplayIo),
    {
        let replay = Arc::new(ReplayStore::default());
        let manager = ReplayWriteManager {
            store: replay.clone(),
            config: tree.config.clone(),
        };

        let mut replay_rounds = 0usize;
        let mut operation_nodes_read = 0usize;
        let mut operation_bytes_read = 0usize;
        let mut result = loop {
            if replay_rounds == MAX_REPLAY_ROUNDS {
                return Err(Error::InvalidNode);
            }
            replay_rounds += 1;
            replay.begin_attempt();
            match operation(&manager) {
                Ok(result) => break result,
                Err(error) => {
                    let missing = replay.take_missing();
                    if missing.is_empty() {
                        return Err(error);
                    }
                    let (nodes, bytes) = self.hydrate_replay(tree, &replay, &missing).await?;
                    operation_nodes_read = operation_nodes_read.saturating_add(nodes);
                    operation_bytes_read = operation_bytes_read.saturating_add(bytes);
                }
            }
        };

        let writes = loop {
            let (writes, missing) = replay.publication_writes(result.0.root.as_ref())?;
            if missing.is_empty() {
                break writes;
            }
            let (nodes, bytes) = self.hydrate_replay(tree, &replay, &missing).await?;
            operation_nodes_read = operation_nodes_read.saturating_add(nodes);
            operation_bytes_read = operation_bytes_read.saturating_add(bytes);
        };
        if !writes.is_empty() {
            let mut decoded = Vec::with_capacity(writes.len());
            for (cid, bytes) in &writes {
                decoded.push((
                    cid.clone(),
                    validation::decode_owned(cid, &tree.config.format, bytes)?,
                ));
            }
            let entries = writes
                .iter()
                .map(|(cid, bytes)| (cid.as_bytes(), bytes.as_slice()))
                .collect::<Vec<_>>();
            if publish_rightmost_hint
                && self.store.supports_hints()
                && self.store.prefers_rightmost_path_hints()
            {
                let root = result.0.root.as_ref().ok_or(Error::InvalidNode)?;
                let path = canonical_rightmost_path(&manager, root)?;
                let hint = crate::prolly::encode_rightmost_path_hint(&path)?;
                self.store
                    .publish_nodes(NodePublication::with_hint(
                        &entries,
                        NodePublicationHint::new(
                            crate::prolly::RIGHTMOST_PATH_HINT_NAMESPACE,
                            root.as_bytes(),
                            &hint,
                        ),
                        origin,
                    ))
                    .await
                    .map_err(|error| Error::Store(Box::new(error)))?;
                self.cache_rightmost_path(
                    root.clone(),
                    crate::prolly::cached_rightmost_entries(&path),
                );
            } else {
                self.store
                    .publish_nodes(NodePublication::new(&entries, origin))
                    .await
                    .map_err(|error| Error::Store(Box::new(error)))?;
            }
            let bytes_written = writes.iter().map(|(_, bytes)| bytes.len()).sum();
            self.metrics.record_batch_write(writes.len(), bytes_written);
            if let Ok(mut cache) = self.node_cache.write() {
                let mut evictions = 0;
                for (cid, node) in decoded {
                    let bytes = node.encoded_len();
                    evictions += cache.insert(cid, Arc::new(node), bytes);
                }
                self.metrics.add_cache_evictions(evictions);
            }
        }
        update_io(
            &mut result.1,
            ReplayIo {
                nodes_read: operation_nodes_read,
                bytes_read: operation_bytes_read,
                nodes_written: writes.len(),
                bytes_written: writes.iter().map(|(_, bytes)| bytes.len()).sum(),
            },
        );
        Ok(result)
    }

    async fn hydrate_replay(
        &self,
        tree: &Tree,
        replay: &ReplayStore,
        requested: &[Cid],
    ) -> Result<(usize, usize), Error> {
        let mut unique = Vec::with_capacity(requested.len());
        let mut seen = HashSet::with_capacity(requested.len());
        for cid in requested {
            if seen.insert(cid.clone()) {
                unique.push(cid.clone());
            }
        }
        let mut backend = Vec::with_capacity(unique.len());
        for cid in unique {
            let cached = self
                .node_cache
                .write()
                .ok()
                .and_then(|mut cache| cache.get(&cid));
            if let Some(node) = cached {
                self.metrics.add_cache_hits(1);
                replay.trust_loaded_references(&node)?;
                replay.insert_loaded(&cid, Some(node.to_bytes()));
            } else {
                self.metrics.add_cache_misses(1);
                backend.push(cid);
            }
        }
        let (parallelism, chunk_size) = if self.store.prefers_batch_reads() {
            // A native multi-get already coalesces the frontier. Splitting it
            // by point-read parallelism creates many tiny SQL/network calls
            // and defeats the advertised capability.
            (
                1,
                backend
                    .len()
                    .clamp(1, crate::prolly::ASYNC_NODE_PREFETCH_BATCH_SIZE),
            )
        } else {
            let parallelism = self
                .execution
                .read_parallelism()
                .get()
                .min(backend.len().max(1));
            let chunk_size = backend
                .len()
                .div_ceil(parallelism)
                .clamp(1, crate::prolly::ASYNC_NODE_PREFETCH_BATCH_SIZE);
            (parallelism, chunk_size)
        };
        let batches = backend
            .chunks(chunk_size)
            .map(<[Cid]>::to_vec)
            .collect::<Vec<_>>();
        let partitions = stream::iter(batches.into_iter().map(|chunk| async move {
            let keys = chunk
                .iter()
                .map(|cid| cid.as_bytes() as &[u8])
                .collect::<Vec<_>>();
            let values = self
                .store
                .batch_get_ordered_unique(&keys)
                .await
                .map_err(|error| Error::Store(Box::new(error)))?;
            Ok::<_, Error>((chunk, values))
        }))
        .buffered(parallelism)
        .collect::<Vec<_>>()
        .await;
        let mut backend_nodes = 0usize;
        let mut backend_bytes = 0usize;
        for partition in partitions {
            let (chunk, values) = partition?;
            if values.len() != chunk.len() {
                return Err(Error::InvalidNode);
            }
            let mut read_bytes = 0usize;
            for (cid, value) in chunk.iter().zip(values) {
                if let Some(bytes) = value.as_ref() {
                    let node = validation::decode_owned(cid, &tree.config.format, bytes)?;
                    replay.trust_loaded_references(&node)?;
                    read_bytes = read_bytes.saturating_add(bytes.len());
                    if let Ok(mut cache) = self.node_cache.write() {
                        let evictions = cache.insert(cid.clone(), Arc::new(node), bytes.len());
                        self.metrics.add_cache_evictions(evictions);
                    }
                }
                replay.insert_loaded(cid, value);
            }
            self.metrics
                .record_batch_read(chunk.len(), read_bytes, chunk.len());
            backend_nodes = backend_nodes.saturating_add(chunk.len());
            backend_bytes = backend_bytes.saturating_add(read_bytes);
        }
        Ok((backend_nodes, backend_bytes))
    }
}

fn canonical_rightmost_path<M>(
    manager: &M,
    root: &Cid,
) -> Result<Vec<crate::prolly::AsyncRightmostPathEntry>, Error>
where
    M: CanonicalWriteManager,
{
    let mut path = Vec::new();
    let mut cid = root.clone();
    loop {
        let node = manager.write_load_arc(&cid)?;
        if node.is_empty() {
            return Err(Error::InvalidNode);
        }
        let child_index = node.len() - 1;
        let next = (!node.leaf)
            .then(|| crate::prolly::child_cid_at(&node, child_index))
            .transpose()?;
        path.push(crate::prolly::AsyncRightmostPathEntry {
            cid: cid.clone(),
            node: node.as_ref().clone(),
            child_index,
        });
        let Some(next) = next else {
            return Ok(path);
        };
        cid = next;
    }
}
