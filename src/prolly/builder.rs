//! Batch builder for parallel tree construction
//!
//! The `BatchBuilder` enables efficient bulk loading of data into a Prolly tree
//! with parallel boundary detection and node creation using rayon.

use super::boundary::is_hash_boundary_config;
use super::cid::Cid;
use super::config::Config;
use super::encoding::INIT_LEVEL;
use super::error::Error;
use super::node::Node;
use super::store::Store;
use super::tree::Tree;

use rayon::prelude::*;

const SORTED_BUILDER_NODE_BATCH: usize = 256;

#[derive(Debug)]
struct BuiltNode {
    cid: Cid,
    first_key: Vec<u8>,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
struct NodeSummary {
    cid: Cid,
    first_key: Vec<u8>,
}

/// Batch builder for parallel tree construction.
///
/// Enables efficient bulk loading of data into a Prolly tree with parallel
/// boundary detection and node creation using rayon.
///
/// # Example
/// ```
/// use prolly::{BatchBuilder, MemStore, Config};
/// use std::sync::Arc;
///
/// let store = Arc::new(MemStore::new());
/// let config = Config::default();
/// let mut builder = BatchBuilder::new(store, config);
///
/// builder.add(b"key1".to_vec(), b"val1".to_vec());
/// builder.add(b"key2".to_vec(), b"val2".to_vec());
/// builder.add(b"key3".to_vec(), b"val3".to_vec());
///
/// let tree = builder.build().unwrap();
/// ```
///
pub struct BatchBuilder<S: Store> {
    store: S,
    config: Config,
    /// Key-value pairs to insert (will be sorted before build)
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Streaming bulk builder for entries that are already sorted by key.
///
/// Unlike [`BatchBuilder`], this builder does not retain all leaf key/value
/// pairs. It flushes leaf nodes as soon as the same content-defined boundary
/// rules used by [`BatchBuilder`] allow it, then builds upper levels from the
/// compact child summaries.
pub struct SortedBatchBuilder<S: Store> {
    store: S,
    config: Config,
    current: Node,
    last_key: Option<Vec<u8>>,
    leaf_nodes: Vec<NodeSummary>,
    pending_nodes: Vec<BuiltNode>,
}

impl<S: Store + Clone + Send + Sync> BatchBuilder<S>
where
    S::Error: Send + Sync,
{
    /// Create a new BatchBuilder with the given store and configuration.
    ///
    /// # Arguments
    /// * `store` - Storage backend implementing the `Store` trait
    /// * `config` - Tree configuration (chunking parameters, encoding, etc.)
    ///
    pub fn new(store: S, config: Config) -> Self {
        Self {
            store,
            config,
            entries: Vec::new(),
        }
    }

    /// Add a key-value pair to the builder.
    ///
    /// Entries will be sorted by key before building the tree.
    ///
    /// # Arguments
    /// * `key` - The key bytes
    /// * `val` - The value bytes
    ///
    pub fn add(&mut self, key: Vec<u8>, val: Vec<u8>) {
        self.entries.push((key, val));
    }

    /// Build the tree from the added entries using parallel chunking.
    ///
    /// This method:
    /// 1. Sorts entries by key
    /// 2. Partitions entries into chunks using parallel boundary detection
    /// 3. Creates leaf nodes in parallel
    /// 4. Builds internal nodes level by level
    ///
    /// # Returns
    /// * `Ok(Tree)` - The constructed tree
    /// * `Err(Error)` - If storage operations fail
    ///
    pub fn build(mut self) -> Result<Tree, Error> {
        // Handle empty case
        if self.entries.is_empty() {
            return Ok(Tree {
                root: None,
                config: self.config,
            });
        }

        // Sort entries by key
        self.entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Parallel chunk building
        let chunks = self.parallel_chunk(&self.entries)?;

        // Build tree bottom-up
        self.build_from_chunks(chunks)
    }

    /// Partition entries into chunks in parallel using boundary detection.
    ///
    /// Uses rayon for parallel boundary detection and leaf node creation.
    ///
    /// # Arguments
    /// * `entries` - Sorted key-value pairs
    ///
    /// # Returns
    /// * `Ok(Vec<NodeSummary>)` - summaries of the created leaf nodes
    /// * `Err(Error)` - If storage operations fail
    ///
    fn parallel_chunk(&self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<NodeSummary>, Error> {
        if entries.is_empty() {
            return Ok(vec![]);
        }

        // Precompute the hash predicate in parallel. Min/max rules depend on
        // the current chunk length, so they are applied in a sequential pass.
        let hash_boundaries: Vec<bool> = entries
            .par_iter()
            .map(|(k, v)| is_hash_boundary_config(&self.config, k, v))
            .collect();

        let chunk_ranges = chunk_ranges_from_hash_boundaries(&self.config, &hash_boundaries);

        // Create leaf nodes in parallel, then persist them in one batched write.
        let config = &self.config;

        let nodes: Vec<BuiltNode> = chunk_ranges
            .par_iter()
            .map(|range| {
                let mut node = new_builder_node(config, true, INIT_LEVEL);
                reserve_node_entries(&mut node, *range.end() - *range.start() + 1);

                for i in range.clone() {
                    node.keys.push(entries[i].0.clone());
                    node.vals.push(entries[i].1.clone());
                }

                let first_key = node.keys.first().cloned().unwrap_or_default();
                let bytes = node.to_bytes();
                let cid = Cid::from_bytes(&bytes);
                BuiltNode {
                    cid,
                    first_key,
                    bytes,
                }
            })
            .collect();

        self.persist_nodes(&nodes)?;
        Ok(nodes
            .into_iter()
            .map(|node| NodeSummary {
                cid: node.cid,
                first_key: node.first_key,
            })
            .collect())
    }

    /// Build internal nodes from leaf CIDs, level by level.
    ///
    /// Constructs the tree bottom-up by creating internal nodes that
    /// reference the nodes from the previous level.
    ///
    /// # Arguments
    /// * `level_nodes` - Summaries of nodes at the current (leaf) level
    ///
    /// # Returns
    /// * `Ok(Tree)` - The constructed tree with root
    /// * `Err(Error)` - If storage operations fail
    ///
    fn build_from_chunks(&self, mut level_nodes: Vec<NodeSummary>) -> Result<Tree, Error> {
        // Handle empty case
        if level_nodes.is_empty() {
            return Ok(Tree {
                root: None,
                config: self.config.clone(),
            });
        }

        // Handle single node case - it becomes the root
        if level_nodes.len() == 1 {
            return Ok(Tree {
                root: Some(level_nodes.remove(0).cid),
                config: self.config.clone(),
            });
        }

        let mut level = INIT_LEVEL;

        // Build internal nodes level by level until we have a single root
        while level_nodes.len() > 1 {
            level += 1;
            level_nodes = self.build_level(level_nodes, level)?;
        }

        Ok(Tree {
            root: level_nodes.into_iter().next().map(|node| node.cid),
            config: self.config.clone(),
        })
    }

    /// Build a single level of internal nodes from child summaries.
    ///
    /// Creates internal nodes that reference the child nodes, using
    /// boundary detection to determine node boundaries.
    ///
    /// # Arguments
    /// * `children` - Summaries of child nodes
    /// * `level` - The level number for the new internal nodes
    ///
    /// # Returns
    /// * `Ok(Vec<NodeSummary>)` - summaries of the created internal nodes
    /// * `Err(Error)` - If storage operations fail
    fn build_level(
        &self,
        children: Vec<NodeSummary>,
        level: u8,
    ) -> Result<Vec<NodeSummary>, Error> {
        if children.is_empty() {
            return Ok(vec![]);
        }

        // Precompute hash predicate in parallel, then apply size rules using
        // chunk-local counts so max_chunk_size does not degenerate into
        // one-child internal nodes after the first full chunk.
        let hash_boundaries: Vec<bool> = children
            .par_iter()
            .map(|child| {
                is_hash_boundary_config(&self.config, &child.first_key, child.cid.as_bytes())
            })
            .collect();
        let chunk_ranges = chunk_ranges_from_hash_boundaries(&self.config, &hash_boundaries);

        let config = &self.config;
        let nodes: Vec<BuiltNode> = chunk_ranges
            .par_iter()
            .map(|range| {
                let start = *range.start();
                let end = *range.end();

                let mut node = new_builder_node(config, false, level);
                reserve_node_entries(&mut node, end - start + 1);

                for child in children.iter().take(end + 1).skip(start) {
                    node.keys.push(child.first_key.clone());
                    node.vals.push(child.cid.0.to_vec());
                }

                let first_key = node.keys.first().cloned().unwrap_or_default();
                let bytes = node.to_bytes();
                let cid = Cid::from_bytes(&bytes);
                BuiltNode {
                    cid,
                    first_key,
                    bytes,
                }
            })
            .collect();

        self.persist_nodes(&nodes)?;
        Ok(nodes
            .into_iter()
            .map(|node| NodeSummary {
                cid: node.cid,
                first_key: node.first_key,
            })
            .collect())
    }

    fn persist_nodes(&self, nodes: &[BuiltNode]) -> Result<(), Error> {
        persist_nodes(&self.store, nodes)
    }
}

impl<S: Store + Clone + Send + Sync> SortedBatchBuilder<S>
where
    S::Error: Send + Sync,
{
    /// Create a sorted streaming builder.
    pub fn new(store: S, config: Config) -> Self {
        let current = new_builder_node(&config, true, INIT_LEVEL);
        Self {
            store,
            config,
            current,
            last_key: None,
            leaf_nodes: Vec::new(),
            pending_nodes: Vec::new(),
        }
    }

    /// Add the next sorted key/value pair.
    ///
    /// Keys must be added in nondecreasing byte order. Duplicate keys are
    /// accepted here for parity with [`BatchBuilder`], though callers usually
    /// provide unique keys.
    pub fn add(&mut self, key: Vec<u8>, val: Vec<u8>) -> Result<(), Error> {
        if let Some(previous) = &self.last_key {
            if key < *previous {
                return Err(Error::UnsortedInput {
                    previous: previous.clone(),
                    next: key,
                });
            }
        }

        let is_hash_boundary = is_hash_boundary_config(&self.config, &key, &val);
        self.last_key = Some(key.clone());
        self.current.keys.push(key);
        self.current.vals.push(val);

        let count = self.current.keys.len();
        if count >= self.config.min_chunk_size
            && (count >= self.config.max_chunk_size || is_hash_boundary)
        {
            self.flush_leaf()?;
        }

        Ok(())
    }

    /// Build a tree from the streamed entries.
    pub fn build(mut self) -> Result<Tree, Error> {
        self.flush_leaf()?;
        self.flush_pending_nodes()?;
        let builder = BatchBuilder::new(self.store.clone(), self.config.clone());
        builder.build_from_chunks(self.leaf_nodes)
    }

    fn flush_leaf(&mut self) -> Result<(), Error> {
        if self.current.keys.is_empty() {
            return Ok(());
        }

        let node = std::mem::replace(
            &mut self.current,
            new_builder_node(&self.config, true, INIT_LEVEL),
        );
        let first_key = node.keys.first().cloned().unwrap_or_default();
        let bytes = node.to_bytes();
        let cid = Cid::from_bytes(&bytes);
        self.leaf_nodes.push(NodeSummary {
            cid: cid.clone(),
            first_key: first_key.clone(),
        });
        self.pending_nodes.push(BuiltNode {
            cid,
            first_key,
            bytes,
        });
        if self.pending_nodes.len() >= SORTED_BUILDER_NODE_BATCH {
            self.flush_pending_nodes()?;
        }
        Ok(())
    }

    fn flush_pending_nodes(&mut self) -> Result<(), Error> {
        persist_nodes(&self.store, &self.pending_nodes)?;
        self.pending_nodes.clear();
        Ok(())
    }
}

fn new_builder_node(config: &Config, leaf: bool, level: u8) -> Node {
    Node::builder()
        .leaf(leaf)
        .level(level)
        .min_chunk_size(config.min_chunk_size)
        .max_chunk_size(config.max_chunk_size)
        .chunking_factor(config.chunking_factor)
        .hash_seed(config.hash_seed)
        .encoding(config.encoding.clone())
        .build()
}

fn reserve_node_entries(node: &mut Node, additional: usize) {
    node.keys.reserve_exact(additional);
    node.vals.reserve_exact(additional);
}

fn persist_nodes<S: Store>(store: &S, nodes: &[BuiltNode]) -> Result<(), Error>
where
    S::Error: Send + Sync,
{
    if nodes.is_empty() {
        return Ok(());
    }

    let entries = nodes
        .iter()
        .map(|node| (node.cid.as_bytes(), node.bytes.as_slice()))
        .collect::<Vec<_>>();
    store
        .batch_put(&entries)
        .map_err(|e| Error::Store(Box::new(e)))
}

pub(crate) fn chunk_ranges_from_hash_boundaries(
    config: &Config,
    hash_boundaries: &[bool],
) -> Vec<std::ops::RangeInclusive<usize>> {
    if hash_boundaries.is_empty() {
        return Vec::new();
    }

    let mut chunk_ranges = Vec::new();
    let mut start = 0;

    for (i, is_hash_boundary) in hash_boundaries.iter().enumerate() {
        let count = i - start + 1;
        if count < config.min_chunk_size {
            continue;
        }

        if count >= config.max_chunk_size || *is_hash_boundary {
            chunk_ranges.push(start..=i);
            start = i + 1;
        }
    }

    if start < hash_boundaries.len() {
        chunk_ranges.push(start..=(hash_boundaries.len() - 1));
    }

    chunk_ranges
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prolly::store::BatchOp;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct CountingStore {
        inner: Arc<Mutex<CountingStoreInner>>,
    }

    #[derive(Default)]
    struct CountingStoreInner {
        data: BTreeMap<Vec<u8>, Vec<u8>>,
        get_calls: usize,
        put_calls: usize,
        batch_put_calls: usize,
    }

    #[derive(Debug)]
    struct CountingStoreError(String);

    impl std::fmt::Display for CountingStoreError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore error: {}", self.0)
        }
    }

    impl std::error::Error for CountingStoreError {}

    impl Store for CountingStore {
        type Error = CountingStoreError;

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| CountingStoreError(format!("lock poisoned: {}", e)))?;
            inner.get_calls += 1;
            Ok(inner.data.get(key).cloned())
        }

        fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| CountingStoreError(format!("lock poisoned: {}", e)))?;
            inner.put_calls += 1;
            inner.data.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| CountingStoreError(format!("lock poisoned: {}", e)))?;
            inner.data.remove(key);
            Ok(())
        }

        fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| CountingStoreError(format!("lock poisoned: {}", e)))?;
            for op in ops {
                match op {
                    BatchOp::Upsert { key, value } => {
                        inner.data.insert(key.to_vec(), value.to_vec());
                    }
                    BatchOp::Delete { key } => {
                        inner.data.remove(*key);
                    }
                }
            }
            Ok(())
        }

        fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| CountingStoreError(format!("lock poisoned: {}", e)))?;
            inner.batch_put_calls += 1;
            for (key, value) in entries {
                inner.data.insert(key.to_vec(), value.to_vec());
            }
            Ok(())
        }
    }

    #[test]
    fn batch_builder_persists_levels_with_batched_writes_without_readback() {
        let store = CountingStore::default();
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(2)
            .build();
        let mut builder = BatchBuilder::new(store.clone(), config);

        for i in 0..64 {
            builder.add(
                format!("k{i:03}").into_bytes(),
                format!("v{i:03}").into_bytes(),
            );
        }

        let tree = builder.build().unwrap();
        assert!(tree.root.is_some());

        let inner = store.inner.lock().unwrap();
        assert_eq!(inner.get_calls, 0);
        assert_eq!(inner.put_calls, 0);
        assert!(inner.batch_put_calls > 1);
    }

    #[test]
    fn batch_builder_applies_max_chunk_size_to_current_chunk_not_global_index() {
        let store = CountingStore::default();
        let config = Config::builder()
            .min_chunk_size(4)
            .max_chunk_size(4)
            .chunking_factor(2)
            .build();
        let mut builder = BatchBuilder::new(store.clone(), config);

        for i in 0..64 {
            builder.add(
                format!("k{i:03}").into_bytes(),
                format!("v{i:03}").into_bytes(),
            );
        }

        let tree = builder.build().unwrap();
        assert!(tree.root.is_some());

        let inner = store.inner.lock().unwrap();
        let mut leaf_lengths = Vec::new();
        let mut level_one_lengths = Vec::new();
        let mut root_length = None;

        for bytes in inner.data.values() {
            let node = Node::from_bytes(bytes).unwrap();
            if node.leaf {
                leaf_lengths.push(node.len());
            } else if node.level == 1 {
                level_one_lengths.push(node.len());
            }

            if Some(Cid::from_bytes(bytes)) == tree.root {
                root_length = Some(node.len());
            }
        }

        leaf_lengths.sort_unstable();
        level_one_lengths.sort_unstable();

        assert_eq!(leaf_lengths, vec![4; 16]);
        assert_eq!(level_one_lengths, vec![4; 4]);
        assert_eq!(root_length, Some(4));
    }

    #[test]
    fn builder_node_entry_reservation_preserves_node_shape() {
        let config = Config::default();
        let mut node = new_builder_node(&config, true, INIT_LEVEL);

        reserve_node_entries(&mut node, 17);

        assert!(node.keys.capacity() >= 17);
        assert!(node.vals.capacity() >= 17);
        assert!(node.keys.is_empty());
        assert!(node.vals.is_empty());
        assert!(node.leaf);
        assert_eq!(node.level, INIT_LEVEL);
    }

    #[test]
    fn batch_builder_parallel_internal_level_preserves_child_order() {
        let store = CountingStore::default();
        let config = Config::builder()
            .min_chunk_size(4)
            .max_chunk_size(4)
            .chunking_factor(u32::MAX)
            .build();
        let builder = BatchBuilder::new(store.clone(), config);
        let children = (0..16)
            .map(|idx| NodeSummary {
                cid: Cid::from_bytes(format!("child-{idx:03}").as_bytes()),
                first_key: format!("k{idx:03}").into_bytes(),
            })
            .collect::<Vec<_>>();

        let level = builder.build_level(children, 1).unwrap();

        assert_eq!(level.len(), 4);
        let inner = store.inner.lock().unwrap();
        for (group_idx, summary) in level.iter().enumerate() {
            assert_eq!(
                summary.first_key,
                format!("k{:03}", group_idx * 4).into_bytes()
            );
            let bytes = inner.data.get(summary.cid.as_bytes()).unwrap();
            let node = Node::from_bytes(bytes).unwrap();
            let expected_keys = (group_idx * 4..group_idx * 4 + 4)
                .map(|idx| format!("k{idx:03}").into_bytes())
                .collect::<Vec<_>>();
            assert_eq!(node.keys, expected_keys);
            assert_eq!(node.vals.len(), 4);
        }
    }

    #[test]
    fn sorted_batch_builder_matches_batch_builder_for_sorted_entries() {
        let config = Config::builder()
            .min_chunk_size(4)
            .max_chunk_size(16)
            .chunking_factor(8)
            .build();
        let batch_store = CountingStore::default();
        let sorted_store = CountingStore::default();
        let mut batch = BatchBuilder::new(batch_store, config.clone());
        let mut sorted = SortedBatchBuilder::new(sorted_store, config);

        for i in 0..257 {
            let key = format!("k{i:04}").into_bytes();
            let val = format!("value-{i:04}").into_bytes();
            batch.add(key.clone(), val.clone());
            sorted.add(key, val).unwrap();
        }

        let batch_tree = batch.build().unwrap();
        let sorted_tree = sorted.build().unwrap();

        assert_eq!(batch_tree.root, sorted_tree.root);
    }

    #[test]
    fn sorted_batch_builder_rejects_out_of_order_keys() {
        let store = CountingStore::default();
        let config = Config::default();
        let mut builder = SortedBatchBuilder::new(store, config);

        builder.add(b"b".to_vec(), b"1".to_vec()).unwrap();
        let err = builder.add(b"a".to_vec(), b"2".to_vec()).unwrap_err();

        assert!(matches!(err, Error::UnsortedInput { .. }));
    }
}
