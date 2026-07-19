use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};

use futures_util::stream::{self, StreamExt};

use super::validation;
use super::ProllyEngine;
use crate::prolly::error::{Error, Mutation};
use crate::prolly::node::Node;
use crate::prolly::store::{AsyncStore, BatchOp, Store};
use crate::prolly::tree::Tree;
use crate::prolly::write::CanonicalWriteManager;
use crate::prolly::{Cid, ProllyMetricsSnapshot};

const MAX_REPLAY_ROUNDS: usize = 256;
type PublicationWrites = Vec<(Cid, Vec<u8>)>;

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
    pub(crate) fn canonical_batch_ready(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
    ) -> Result<(Tree, crate::prolly::write::WriteStats), Error> {
        let manager = ReadyWriteManager {
            engine: self,
            config: tree.config.clone(),
        };
        crate::prolly::write::apply(&manager, tree, mutations)
    }

    pub(crate) fn canonical_batch_ready_configured(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
        config: &crate::prolly::parallel::ParallelConfig,
    ) -> Result<(Tree, crate::prolly::write::WriteStats), Error> {
        let manager = ReadyWriteManager {
            engine: self,
            config: tree.config.clone(),
        };
        crate::prolly::write::apply_configured(&manager, tree, mutations, config)
    }

    pub(crate) fn canonical_delete_range_ready(
        &self,
        tree: &Tree,
        start: &[u8],
        end: &[u8],
    ) -> Result<(Tree, crate::prolly::write::WriteStats), Error> {
        let manager = ReadyWriteManager {
            engine: self,
            config: tree.config.clone(),
        };
        crate::prolly::range_delete::apply(&manager, tree, start, end)
    }
}

struct ReadyWriteManager<'a, S: Store> {
    engine: &'a ProllyEngine<crate::prolly::store::SyncStoreAsAsync<Arc<S>>>,
    config: crate::prolly::Config,
}

impl<S> CanonicalWriteManager for ReadyWriteManager<'_, S>
where
    S: Store,
{
    type Store = Arc<S>;

    fn write_store(&self) -> &Self::Store {
        self.engine.store.inner()
    }

    fn write_config(&self) -> &crate::prolly::Config {
        &self.config
    }

    fn write_load_arc(&self, cid: &Cid) -> Result<Arc<Node>, Error> {
        if let Ok(mut cache) = self.engine.node_cache.write() {
            if let Some(node) = cache.get(cid) {
                super::validate_cached_node(&node, &self.config.format)?;
                self.engine.metrics.add_cache_hits(1);
                return Ok(node);
            }
        }
        self.engine.metrics.add_cache_misses(1);
        let bytes = self
            .write_store()
            .get(cid.as_bytes())
            .map_err(|error| Error::Store(Box::new(error)))?
            .ok_or_else(|| Error::NotFound(cid.clone()))?;
        let node = Arc::new(validation::decode_owned(cid, &self.config.format, &bytes)?);
        self.engine.metrics.record_point_read(bytes.len());
        if let Ok(mut cache) = self.engine.node_cache.write() {
            let evictions = cache.insert(cid.clone(), node.clone(), bytes.len());
            self.engine.metrics.add_cache_evictions(evictions);
        }
        Ok(node)
    }

    fn write_load_many_ordered(&self, cids: &[Cid]) -> Result<Vec<Arc<Node>>, Error> {
        let mut nodes = vec![None; cids.len()];
        let mut missing_cids = Vec::new();
        let mut missing_positions = Vec::new();
        if let Ok(mut cache) = self.engine.node_cache.write() {
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
            let values = self
                .write_store()
                .batch_get_ordered_unique(&keys)
                .map_err(|error| Error::Store(Box::new(error)))?;
            drop(keys);
            if values.len() != missing_cids.len() {
                return Err(Error::InvalidNode);
            }
            let mut read_bytes = 0usize;
            for ((cid, position), value) in
                missing_cids.into_iter().zip(missing_positions).zip(values)
            {
                let bytes = value.ok_or_else(|| Error::NotFound(cid.clone()))?;
                let node = Arc::new(validation::decode_owned(&cid, &self.config.format, &bytes)?);
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
    ) -> Result<(Tree, crate::prolly::write::WriteStats), Error> {
        let all_upserts = !mutations.is_empty()
            && mutations
                .iter()
                .all(|mutation| matches!(mutation, Mutation::Upsert { .. }));
        if let Some(root) = tree.root.as_ref().filter(|_| {
            tree.config.format == self.config.format && all_upserts && self.store.supports_hints()
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
            |manager| crate::prolly::range_delete::apply(manager, tree, start, end),
            update_write_stats,
        )
        .await
    }

    pub(crate) async fn execute_replay<T, F, U>(
        &self,
        tree: &Tree,
        publish_rightmost_hint: bool,
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
            if publish_rightmost_hint && self.store.supports_hints() {
                let root = result.0.root.as_ref().ok_or(Error::InvalidNode)?;
                let path = replay_rightmost_path(&manager, root)?;
                let hint = crate::prolly::encode_rightmost_path_hint(&path)?;
                self.store
                    .batch_put_with_hint(
                        &entries,
                        crate::prolly::RIGHTMOST_PATH_HINT_NAMESPACE,
                        root.as_bytes(),
                        &hint,
                    )
                    .await
                    .map_err(|error| Error::Store(Box::new(error)))?;
                self.cache_rightmost_path(
                    root.clone(),
                    crate::prolly::cached_rightmost_entries(&path),
                );
            } else {
                self.store
                    .batch_put(&entries)
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
        let parallelism = self
            .execution
            .read_parallelism()
            .get()
            .min(backend.len().max(1));
        let chunk_size = backend
            .len()
            .div_ceil(parallelism)
            .clamp(1, crate::prolly::ASYNC_NODE_PREFETCH_BATCH_SIZE);
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

fn replay_rightmost_path(
    manager: &ReplayWriteManager,
    root: &Cid,
) -> Result<Vec<crate::prolly::AsyncRightmostPathEntry>, Error> {
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
