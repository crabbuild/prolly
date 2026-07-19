pub(crate) mod execution;
#[expect(
    dead_code,
    reason = "the ready-only bridge is consumed when ProllyEngine replaces facade-local reads"
)]
pub(crate) mod ready;
pub(crate) mod validation;
pub(crate) mod write;

use std::sync::{Arc, RwLock};

use self::execution::{ExecutionConfig, OperationContext};
use super::error::Error;
use super::node::{Node, ReadNode};
use super::store::AsyncStore;
use super::tree::Tree;
use super::{
    inline_positions_from_range, lower_bound_position_key, plan_cached_nodes, sorted_key_positions,
    Cid, Config, InlinePositions, KeyLookupFrame, MissingNodeBatch, NodeCache, ProllyMetrics,
    GET_MANY_BOUNDARY_ROUTE_MIN_POSITIONS,
};

/// Canonical runtime owner for async prolly algorithms.
pub struct ProllyEngine<S: AsyncStore> {
    pub(super) store: S,
    pub(super) config: Config,
    pub(super) execution: ExecutionConfig,
    pub(super) node_cache: Arc<RwLock<NodeCache>>,
    pub(super) metrics: Arc<ProllyMetrics>,
    pub(super) recent_leaf: RwLock<Option<(Cid, Arc<ReadNode>)>>,
    pub(super) rightmost_path_cache: RwLock<Option<(Cid, Vec<super::CachedRightmostPathEntry>)>>,
}

impl<S> ProllyEngine<S>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    /// Create an async-first engine with bounded default execution limits.
    pub fn new(store: S, config: Config) -> Self {
        Self::with_execution_config(store, config, ExecutionConfig::default())
    }

    /// Create an async-first engine with explicit execution limits.
    pub fn with_execution_config(store: S, config: Config, execution: ExecutionConfig) -> Self {
        let node_cache_max_nodes = config.runtime.node_cache_max_nodes;
        let node_cache_max_bytes = config.runtime.node_cache_max_bytes;
        Self {
            store,
            config,
            execution,
            node_cache: Arc::new(RwLock::new(NodeCache::new(
                node_cache_max_nodes,
                node_cache_max_bytes,
            ))),
            metrics: Arc::new(ProllyMetrics::default()),
            recent_leaf: RwLock::new(None),
            rightmost_path_cache: RwLock::new(None),
        }
    }

    pub(super) fn with_state(
        store: S,
        config: Config,
        execution: ExecutionConfig,
        node_cache: Arc<RwLock<NodeCache>>,
        metrics: Arc<ProllyMetrics>,
    ) -> Self {
        Self {
            store,
            config,
            execution,
            node_cache,
            metrics,
            recent_leaf: RwLock::new(None),
            rightmost_path_cache: RwLock::new(None),
        }
    }

    /// Read one key using the input tree's persisted format.
    pub async fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let Some(root) = &tree.root else {
            return Ok(None);
        };
        let recent_leaf_enabled = self
            .node_cache
            .read()
            .is_ok_and(|cache| !cache.is_disabled());
        let recent_leaf = recent_leaf_enabled
            .then(|| self.recent_leaf.read().ok())
            .flatten()
            .and_then(|recent| {
                recent
                    .as_ref()
                    .filter(|(recent_root, _)| recent_root == root)
                    .map(|(_, node)| node.clone())
            });
        if let Some(leaf) = recent_leaf {
            validate_cached_read_node(&leaf, &tree.config.format)?;
            if leaf
                .key(0)
                .zip(leaf.key(leaf.len().saturating_sub(1)))
                .is_some_and(|(first, last)| key >= first && key <= last)
            {
                self.metrics.add_cache_hits(1);
                return match leaf.search(key) {
                    Ok(index) => Ok(leaf.value(index).map(<[u8]>::to_vec)),
                    Err(_) => Ok(None),
                };
            }
        }
        let mut operation = OperationContext::new(self.execution.clone());
        let mut cid = root.clone();
        loop {
            let node = self.load_read(tree, &cid, &mut operation).await?;
            validate_cached_read_node(&node, &tree.config.format)?;
            let index = match node.search(key) {
                Ok(index) => index,
                Err(0) => return Ok(None),
                Err(index) => index - 1,
            };
            if node.is_leaf() {
                let result = (node.key(index) == Some(key))
                    .then(|| node.value(index).map(<[u8]>::to_vec))
                    .flatten();
                if recent_leaf_enabled {
                    if let Ok(mut recent) = self.recent_leaf.write() {
                        *recent = Some((root.clone(), node));
                    }
                }
                return Ok(result);
            }
            cid = node.child_cid(index)?;
        }
    }

    /// Read keys in input order, preserving duplicates and missing positions.
    pub async fn get_many<K: AsRef<[u8]>>(
        &self,
        tree: &Tree,
        keys: &[K],
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let mut values = vec![None; keys.len()];
        let Some(root) = &tree.root else {
            return Ok(values);
        };
        if keys.is_empty() {
            return Ok(values);
        }

        let positions = InlinePositions::from_vec(sorted_key_positions(keys))
            .expect("keys is non-empty after early return");
        let mut frames = vec![KeyLookupFrame {
            cid: root.clone(),
            positions,
        }];
        let mut operation = OperationContext::new(self.execution.clone());

        while !frames.is_empty() {
            let cids = frames
                .iter()
                .map(|frame| frame.cid.clone())
                .collect::<Vec<_>>();
            let mut nodes = Vec::with_capacity(cids.len());
            let parallelism = self.execution.read_parallelism().get();
            let chunk_size = if cids.len() <= parallelism {
                cids.len()
            } else {
                cids.len()
                    .div_ceil(parallelism)
                    .min(super::ASYNC_NODE_PREFETCH_BATCH_SIZE)
            };
            for chunk in cids.chunks(chunk_size.max(1)) {
                nodes.extend(
                    self.load_many_engine_read_ordered(tree, chunk, &mut operation)
                        .await?,
                );
            }
            let mut next_frames = Vec::new();
            for (frame, node) in frames.into_iter().zip(nodes) {
                validate_cached_read_node(&node, &tree.config.format)?;
                if node.is_leaf() {
                    fill_read_leaf_lookup_values(&node, frame.positions, keys, &mut values)?;
                } else {
                    next_frames.extend(route_read_key_positions_to_children(
                        &node,
                        frame.positions,
                        keys,
                    )?);
                }
            }
            frames = next_frames;
        }
        Ok(values)
    }

    async fn load_read(
        &self,
        tree: &Tree,
        cid: &Cid,
        operation: &mut OperationContext,
    ) -> Result<Arc<ReadNode>, Error> {
        if let Ok(mut cache) = self.node_cache.write() {
            if let Some(node) = cache.get_read(cid) {
                operation.record_cache_hit();
                self.metrics.add_cache_hits(1);
                return Ok(node);
            }
        }

        // A write may have admitted only an owned node. Repack it rather than
        // issuing duplicate I/O when the backend cannot retain shared bytes.
        if !self.store.has_native_shared_reads() {
            let owned = self
                .node_cache
                .write()
                .ok()
                .and_then(|mut cache| cache.get(cid));
            if let Some(owned) = owned {
                validate_cached_node(&owned, &tree.config.format)?;
                let node = Arc::new(validation::decode_read(
                    cid,
                    &tree.config.format,
                    Arc::from(owned.to_bytes()),
                )?);
                if let Ok(mut cache) = self.node_cache.write() {
                    let evictions = cache.insert_read(cid.clone(), node.clone());
                    self.metrics.add_cache_evictions(evictions);
                }
                operation.record_cache_hit();
                self.metrics.add_cache_hits(1);
                return Ok(node);
            }
        }

        operation.record_cache_miss();
        self.metrics.add_cache_misses(1);
        let bytes = self
            .store
            .get_shared(cid.as_bytes())
            .await
            .map_err(|error| Error::Store(Box::new(error)))?
            .ok_or_else(|| Error::NotFound(cid.clone()))?;
        operation.record_read(bytes.len());
        self.metrics.record_point_read(bytes.len());
        let node = Arc::new(validation::decode_read(cid, &tree.config.format, bytes)?);
        if let Ok(mut cache) = self.node_cache.write() {
            let evictions = cache.insert_read(cid.clone(), node.clone());
            self.metrics.add_cache_evictions(evictions);
        }
        Ok(node)
    }

    async fn load_many_engine_read_ordered(
        &self,
        tree: &Tree,
        cids: &[Cid],
        operation: &mut OperationContext,
    ) -> Result<Vec<Arc<ReadNode>>, Error> {
        let (mut nodes, missing, hits) = if let Ok(mut cache) = self.node_cache.write() {
            plan_cached_nodes(cids, |cid| cache.get_read(cid))
        } else {
            plan_cached_nodes(cids, |_| None)
        };
        self.metrics.add_cache_hits(hits);
        for _ in 0..hits {
            operation.record_cache_hit();
        }
        if let Some(MissingNodeBatch {
            cids: missing_cids,
            positions,
            ..
        }) = missing
        {
            for _ in &missing_cids {
                operation.record_cache_miss();
            }
            self.metrics.add_cache_misses(missing_cids.len());
            operation.observe_in_flight_reads(missing_cids.len());
            let keys = missing_cids
                .iter()
                .map(|cid| cid.as_bytes() as &[u8])
                .collect::<Vec<_>>();
            let loaded = self
                .store
                .batch_get_shared_ordered_unique(&keys)
                .await
                .map_err(|error| Error::Store(Box::new(error)))?;
            let key_count = keys.len();
            drop(keys);
            if loaded.len() != missing_cids.len() {
                return Err(Error::InvalidNode);
            }
            let mut decoded = Vec::with_capacity(loaded.len());
            for (cid, bytes) in missing_cids.into_iter().zip(loaded) {
                let bytes = bytes.ok_or_else(|| Error::NotFound(cid.clone()))?;
                let bytes_len = bytes.len();
                operation.record_read(bytes_len);
                decoded.push((
                    cid.clone(),
                    Arc::new(validation::decode_read(&cid, &tree.config.format, bytes)?),
                    bytes_len,
                ));
            }
            let loaded_bytes = decoded.iter().map(|(_, _, bytes)| *bytes).sum();
            self.metrics
                .record_batch_read(key_count, loaded_bytes, decoded.len());
            let mut cache = self.node_cache.write().ok();
            let mut evictions = 0;
            for ((cid, node, _), node_positions) in decoded.into_iter().zip(positions) {
                if let Some(cache) = cache.as_mut() {
                    evictions += cache.insert_read(cid, node.clone());
                }
                for position in node_positions {
                    nodes[position] = Some(node.clone());
                }
            }
            self.metrics.add_cache_evictions(evictions);
        }
        nodes
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(Error::InvalidNode)
    }

    #[allow(dead_code, reason = "canonical mutation phases will use owned nodes")]
    async fn load_owned(
        &self,
        tree: &Tree,
        cid: &Cid,
        operation: &mut OperationContext,
    ) -> Result<Arc<Node>, Error> {
        if let Ok(mut cache) = self.node_cache.write() {
            if let Some(node) = cache.get(cid) {
                operation.record_cache_hit();
                self.metrics.add_cache_hits(1);
                return Ok(node);
            }
        }
        operation.record_cache_miss();
        self.metrics.add_cache_misses(1);
        let bytes = self
            .store
            .get(cid.as_bytes())
            .await
            .map_err(|error| Error::Store(Box::new(error)))?
            .ok_or_else(|| Error::NotFound(cid.clone()))?;
        operation.record_read(bytes.len());
        self.metrics.record_point_read(bytes.len());
        let node = Arc::new(validation::decode_owned(cid, &tree.config.format, &bytes)?);
        if let Ok(mut cache) = self.node_cache.write() {
            let evictions = cache.insert(cid.clone(), node.clone(), bytes.len());
            self.metrics.add_cache_evictions(evictions);
        }
        Ok(node)
    }
}

fn fill_read_leaf_lookup_values<K: AsRef<[u8]>>(
    node: &ReadNode,
    positions: InlinePositions,
    keys: &[K],
    values: &mut [Option<Vec<u8>>],
) -> Result<(), Error> {
    let mut leaf_index = 0usize;
    let mut positions = positions.into_iter().peekable();
    while let Some(position) = positions.next() {
        let key = keys[position].as_ref();
        while leaf_index < node.len() && node.key(leaf_index).ok_or(Error::InvalidNode)? < key {
            leaf_index += 1;
        }
        let found = (leaf_index < node.len() && node.key(leaf_index) == Some(key))
            .then(|| node.value(leaf_index).map(<[u8]>::to_vec))
            .flatten();
        values[position] = found.clone();
        while let Some(next) = positions.next_if(|next| keys[*next].as_ref() == key) {
            values[next] = found.clone();
        }
    }
    Ok(())
}

fn route_read_key_positions_to_children<K: AsRef<[u8]>>(
    node: &ReadNode,
    positions: InlinePositions,
    keys: &[K],
) -> Result<Vec<KeyLookupFrame>, Error> {
    if node.is_empty() {
        return Err(Error::InvalidNode);
    }
    if positions.len() >= GET_MANY_BOUNDARY_ROUTE_MIN_POSITIONS && node.len() > 1 {
        return route_read_key_positions_to_children_by_boundary(node, positions, keys);
    }
    let mut frames: Vec<KeyLookupFrame> = Vec::with_capacity(node.len().min(positions.len()));
    let mut child_index = read_child_index(node, keys[positions.first].as_ref());
    let mut last_child_index = None;
    for position in positions {
        let key = keys[position].as_ref();
        while child_index + 1 < node.len()
            && key >= node.key(child_index + 1).ok_or(Error::InvalidNode)?
        {
            child_index += 1;
        }
        if last_child_index == Some(child_index) {
            frames
                .last_mut()
                .ok_or(Error::InvalidNode)?
                .positions
                .push(position);
        } else {
            frames.push(KeyLookupFrame {
                cid: node.child_cid(child_index)?,
                positions: InlinePositions::new(position),
            });
            last_child_index = Some(child_index);
        }
    }
    Ok(frames)
}

fn route_read_key_positions_to_children_by_boundary<K: AsRef<[u8]>>(
    node: &ReadNode,
    positions: InlinePositions,
    keys: &[K],
) -> Result<Vec<KeyLookupFrame>, Error> {
    let position_count = positions.len();
    let mut frames = Vec::with_capacity(node.len().min(position_count));
    let mut child_index = read_child_index(node, keys[positions.at(0)].as_ref());
    let last_child_index = read_child_index(node, keys[positions.at(position_count - 1)].as_ref());
    let mut bucket_start = 0usize;
    while child_index < last_child_index {
        let boundary = node.key(child_index + 1).ok_or(Error::InvalidNode)?;
        let bucket_end =
            lower_bound_position_key(&positions, keys, bucket_start..position_count, boundary);
        if bucket_start < bucket_end {
            frames.push(KeyLookupFrame {
                cid: node.child_cid(child_index)?,
                positions: inline_positions_from_range(&positions, bucket_start..bucket_end),
            });
        }
        bucket_start = bucket_end;
        child_index += 1;
    }
    if bucket_start < position_count {
        frames.push(KeyLookupFrame {
            cid: node.child_cid(last_child_index)?,
            positions: inline_positions_from_range(&positions, bucket_start..position_count),
        });
    }
    Ok(frames)
}

fn read_child_index(node: &ReadNode, key: &[u8]) -> usize {
    match node.search(key) {
        Ok(index) => index,
        Err(index) => index.saturating_sub(1),
    }
}

fn validate_cached_node(
    node: &Node,
    expected_format: &super::format::TreeFormat,
) -> Result<(), Error> {
    node.validate()?;
    if node.format != *expected_format {
        return Err(Error::FormatMismatch {
            expected: expected_format.digest()?,
            actual: node.format.digest()?,
        });
    }
    Ok(())
}

fn validate_cached_read_node(
    node: &ReadNode,
    expected_format: &super::format::TreeFormat,
) -> Result<(), Error> {
    if node.format() != expected_format {
        return Err(Error::FormatMismatch {
            expected: expected_format.digest()?,
            actual: node.format().digest()?,
        });
    }
    Ok(())
}
