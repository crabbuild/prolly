//! Batch builder for parallel tree construction
//!
//! The `BatchBuilder` enables efficient bulk loading of data into a Prolly tree
//! with parallel boundary detection and node creation using rayon.

use super::boundary::BoundaryDetector;
use super::cid::Cid;
use super::config::Config;
use super::encoding::INIT_LEVEL;
use super::error::Error;
use super::node::Node;
use super::store::Store;
use super::tree::Tree;

use rayon::prelude::*;

mod size;
pub(crate) use size::EncodedNodeSizer;

const SORTED_BUILDER_NODE_BATCH: usize = 256;
const PARALLEL_BOUNDARY_HASH_MIN_ENTRIES: usize = 1_024;

#[derive(Debug)]
struct BuiltNode {
    cid: Cid,
    first_key: Vec<u8>,
    count: u64,
    bytes: Vec<u8>,
}

pub(crate) struct DeferredNode {
    pub(crate) cid: Cid,
    pub(crate) bytes: Vec<u8>,
    pub(crate) node: Node,
}

#[derive(Clone, Debug)]
pub(crate) struct NodeSummary {
    pub(crate) cid: Cid,
    pub(crate) first_key: Vec<u8>,
    pub(crate) count: u64,
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
    pending_entry: Option<(Vec<u8>, Vec<u8>)>,
    leaf_nodes: Vec<NodeSummary>,
    pending_nodes: Vec<BuiltNode>,
    detector: BoundaryDetector,
    sizer: EncodedNodeSizer,
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
        let mut unique = Vec::with_capacity(self.entries.len());
        for (key, value) in self.entries {
            if unique
                .last()
                .is_some_and(|(previous, _): &(Vec<u8>, Vec<u8>)| previous == &key)
            {
                unique.last_mut().expect("duplicate has a predecessor").1 = value;
            } else {
                unique.push((key, value));
            }
        }
        self.entries = unique;

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

        let chunk_ranges =
            chunk_ranges_for_entries_parallel(&self.config, INIT_LEVEL, entries, None)?;

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
                    count: (range.end() - range.start() + 1) as u64,
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
                count: node.count,
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
    pub(crate) fn build_from_chunks(
        &self,
        mut level_nodes: Vec<NodeSummary>,
    ) -> Result<Tree, Error> {
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

    /// Build upper levels serially and persist them in one store batch.
    ///
    /// Mutation writers generally rebuild only a small internal frontier. For
    /// that workload, avoiding per-level parallel scheduling and store calls is
    /// materially cheaper while producing the same canonical root.
    #[cfg(test)]
    pub(crate) fn build_from_chunks_serial(
        &self,
        level_nodes: Vec<NodeSummary>,
    ) -> Result<(Tree, usize, usize), Error> {
        let (tree, pending) = self.build_from_chunks_serial_deferred(level_nodes)?;
        let written_nodes = pending.len();
        let written_bytes = pending.iter().map(|node| node.bytes.len()).sum();
        let entries = pending
            .iter()
            .map(|node| (node.cid.as_bytes(), node.bytes.as_slice()))
            .collect::<Vec<_>>();
        self.store
            .batch_put(&entries)
            .map_err(|error| Error::Store(Box::new(error)))?;
        Ok((tree, written_nodes, written_bytes))
    }

    pub(crate) fn build_from_chunks_serial_deferred(
        &self,
        mut level_nodes: Vec<NodeSummary>,
    ) -> Result<(Tree, Vec<DeferredNode>), Error> {
        if level_nodes.is_empty() {
            return Ok((
                Tree {
                    root: None,
                    config: self.config.clone(),
                },
                Vec::new(),
            ));
        }
        if level_nodes.len() == 1 {
            return Ok((
                Tree {
                    root: Some(level_nodes.remove(0).cid),
                    config: self.config.clone(),
                },
                Vec::new(),
            ));
        }

        let mut level = INIT_LEVEL;
        let mut pending = Vec::<DeferredNode>::new();
        while level_nodes.len() > 1 {
            level += 1;
            level_nodes = self.build_level_serial(level_nodes, level, &mut pending)?;
        }

        Ok((
            Tree {
                root: level_nodes.into_iter().next().map(|node| node.cid),
                config: self.config.clone(),
            },
            pending,
        ))
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

        let internal_entries = children
            .iter()
            .map(|child| (child.first_key.clone(), child.cid.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        let child_counts = children.iter().map(|child| child.count).collect::<Vec<_>>();
        let chunk_ranges = chunk_ranges_for_entries_parallel(
            &self.config,
            level,
            &internal_entries,
            Some(&child_counts),
        )?;

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
                    node.child_counts.push(child.count);
                }

                let first_key = node.keys.first().cloned().unwrap_or_default();
                let bytes = node.to_bytes();
                let cid = Cid::from_bytes(&bytes);
                BuiltNode {
                    cid,
                    first_key,
                    count: children[start..=end].iter().map(|child| child.count).sum(),
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
                count: node.count,
            })
            .collect())
    }

    fn build_level_serial(
        &self,
        children: Vec<NodeSummary>,
        level: u8,
        pending: &mut Vec<DeferredNode>,
    ) -> Result<Vec<NodeSummary>, Error> {
        let internal_entries = children
            .iter()
            .map(|child| (child.first_key.clone(), child.cid.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        let child_counts = children.iter().map(|child| child.count).collect::<Vec<_>>();
        let chunk_ranges =
            chunk_ranges_for_entries(&self.config, level, &internal_entries, Some(&child_counts))?;
        let mut summaries = Vec::with_capacity(chunk_ranges.len());

        for range in chunk_ranges {
            let start = *range.start();
            let end = *range.end();
            let mut node = new_builder_node(&self.config, false, level);
            reserve_node_entries(&mut node, end - start + 1);
            for child in children.iter().take(end + 1).skip(start) {
                node.keys.push(child.first_key.clone());
                node.vals.push(child.cid.0.to_vec());
                node.child_counts.push(child.count);
            }

            let first_key = node.keys.first().cloned().unwrap_or_default();
            let count = children[start..=end].iter().map(|child| child.count).sum();
            let bytes = node.to_bytes();
            let cid = Cid::from_bytes(&bytes);
            summaries.push(NodeSummary {
                cid: cid.clone(),
                first_key: first_key.clone(),
                count,
            });
            pending.push(DeferredNode { cid, bytes, node });
        }
        Ok(summaries)
    }

    pub(crate) fn build_level_serial_deferred(
        &self,
        children: Vec<NodeSummary>,
        level: u8,
    ) -> Result<(Vec<NodeSummary>, Vec<DeferredNode>), Error> {
        let mut pending = Vec::new();
        let summaries = self.build_level_serial(children, level, &mut pending)?;
        Ok((summaries, pending))
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
        let detector = BoundaryDetector::new(config.format.chunking.clone(), INIT_LEVEL.into())
            .expect("configuration contains a valid persisted chunking policy");
        let sizer = EncodedNodeSizer::new(config.format.clone(), true, INIT_LEVEL)
            .expect("configuration contains a registered persisted node layout");
        Self {
            store,
            config,
            current,
            pending_entry: None,
            leaf_nodes: Vec::new(),
            pending_nodes: Vec::new(),
            detector,
            sizer,
        }
    }

    /// Add the next sorted key/value pair.
    ///
    /// Keys must be added in nondecreasing byte order. Duplicate keys are
    /// accepted here for parity with [`BatchBuilder`], though callers usually
    /// provide unique keys.
    pub fn add(&mut self, key: Vec<u8>, val: Vec<u8>) -> Result<(), Error> {
        if let Some((previous, previous_value)) = &mut self.pending_entry {
            if key < *previous {
                return Err(Error::UnsortedInput {
                    previous: previous.clone(),
                    next: key,
                });
            }
            if key == *previous {
                *previous_value = val;
                return Ok(());
            }
        }

        self.flush_pending_entry()?;
        self.pending_entry = Some((key, val));
        Ok(())
    }

    fn flush_pending_entry(&mut self) -> Result<(), Error> {
        let Some((key, val)) = self.pending_entry.take() else {
            return Ok(());
        };
        let hard_max = self.config.format.chunking.hard_max_node_bytes;
        let mut encoded_size = self.sizer.size_after(&key, &val, None)?;
        if !self.current.is_empty() && encoded_size > hard_max {
            self.flush_leaf()?;
            self.detector.reset();
            encoded_size = self.sizer.size_after(&key, &val, None)?;
        }
        if encoded_size > hard_max {
            return Err(Error::EntryTooLarge {
                encoded_bytes: encoded_size,
                limit: hard_max,
            });
        }
        let encoded_entry_bytes = encoded_size.saturating_sub(self.sizer.size());
        self.sizer.push_sized(&key, &val, None, encoded_size)?;
        let is_boundary = self
            .detector
            .observe(&key, &val, encoded_entry_bytes as usize)?;
        self.current.keys.push(key);
        self.current.vals.push(val);

        if is_boundary {
            self.flush_leaf()?;
        }
        Ok(())
    }

    /// Build a tree from the streamed entries.
    pub fn build(mut self) -> Result<Tree, Error> {
        self.flush_pending_entry()?;
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
        debug_assert!(bytes.len() as u64 <= self.config.format.chunking.hard_max_node_bytes);
        let cid = Cid::from_bytes(&bytes);
        self.leaf_nodes.push(NodeSummary {
            cid: cid.clone(),
            first_key: first_key.clone(),
            count: node.keys.len() as u64,
        });
        self.pending_nodes.push(BuiltNode {
            cid,
            first_key,
            count: node.keys.len() as u64,
            bytes,
        });
        self.sizer.reset();
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
        .tree_format(config.format.clone())
        .build()
}

pub(crate) fn chunk_ranges_for_entries(
    config: &Config,
    level: u8,
    entries: &[(Vec<u8>, Vec<u8>)],
    child_counts: Option<&[u64]>,
) -> Result<Vec<std::ops::RangeInclusive<usize>>, Error> {
    chunk_ranges_for_entries_impl(config, level, entries, child_counts, false)
}

fn chunk_ranges_for_entries_parallel(
    config: &Config,
    level: u8,
    entries: &[(Vec<u8>, Vec<u8>)],
    child_counts: Option<&[u64]>,
) -> Result<Vec<std::ops::RangeInclusive<usize>>, Error> {
    if entries.len() < PARALLEL_BOUNDARY_HASH_MIN_ENTRIES {
        return chunk_ranges_for_entries_impl(config, level, entries, child_counts, false);
    }
    chunk_ranges_for_entries_impl(config, level, entries, child_counts, true)
}

fn chunk_ranges_for_entries_impl(
    config: &Config,
    level: u8,
    entries: &[(Vec<u8>, Vec<u8>)],
    child_counts: Option<&[u64]>,
    parallel_hashing: bool,
) -> Result<Vec<std::ops::RangeInclusive<usize>>, Error> {
    if level == 0 {
        if child_counts.is_some() {
            return Err(Error::InvalidNode);
        }
    } else if child_counts.is_none_or(|counts| counts.len() != entries.len()) {
        return Err(Error::InvalidNode);
    }
    let mut detector = BoundaryDetector::new(config.format.chunking.clone(), level.into())?;
    if parallel_hashing && detector.supports_independent_hashing() {
        let boundaries_and_upper_bytes = entries
            .par_iter()
            .map(|(key, value)| {
                (
                    detector
                        .independent_hash_boundary(key, value)
                        .expect("independent boundary support was checked"),
                    (key.len() as u64)
                        .saturating_add(value.len() as u64)
                        .saturating_add(64),
                )
            })
            .collect::<Vec<_>>();
        let max_upper_bytes = boundaries_and_upper_bytes
            .iter()
            .map(|(_, bytes)| *bytes)
            .max()
            .unwrap_or(0);
        let spec = &config.format.chunking;
        let empty_size = EncodedNodeSizer::new(config.format.clone(), level == 0, level)?.size();
        if empty_size.saturating_add(max_upper_bytes.saturating_mul(spec.max))
            <= spec.hard_max_node_bytes
        {
            return Ok(chunk_ranges_from_independent_counts(
                spec,
                &boundaries_and_upper_bytes,
            ));
        }
        let hash_boundaries = boundaries_and_upper_bytes
            .into_iter()
            .map(|(boundary, _)| boundary)
            .collect::<Vec<_>>();
        return chunk_ranges_from_independent_hashes(
            config,
            level,
            entries,
            child_counts,
            &hash_boundaries,
        );
    }

    let mut ranges = Vec::new();
    let mut start = 0;
    let mut sizer = EncodedNodeSizer::new(config.format.clone(), level == 0, level)?;
    for (index, (key, value)) in entries.iter().enumerate() {
        let child_count = child_counts.map(|counts| counts[index]);
        let mut encoded_size = sizer.size_after(key, value, child_count)?;
        if index > start && encoded_size > config.format.chunking.hard_max_node_bytes {
            ranges.push(start..=(index - 1));
            start = index;
            detector.reset();
            sizer.reset();
            encoded_size = sizer.size_after(key, value, child_count)?;
        }
        if encoded_size > config.format.chunking.hard_max_node_bytes {
            return Err(Error::EntryTooLarge {
                encoded_bytes: encoded_size,
                limit: config.format.chunking.hard_max_node_bytes,
            });
        }
        let encoded_entry_bytes = encoded_size.saturating_sub(sizer.size());
        sizer.push_sized(key, value, child_count, encoded_size)?;
        if detector.observe(key, value, encoded_entry_bytes as usize)? {
            ranges.push(start..=index);
            start = index + 1;
            sizer.reset();
        }
    }
    if start < entries.len() {
        ranges.push(start..=(entries.len() - 1));
    }
    Ok(ranges)
}

fn chunk_ranges_from_independent_counts(
    spec: &super::format::ChunkingSpec,
    boundaries_and_upper_bytes: &[(bool, u64)],
) -> Vec<std::ops::RangeInclusive<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    for (index, (hash_boundary, _)) in boundaries_and_upper_bytes.iter().enumerate() {
        let count = (index - start + 1) as u64;
        if count >= spec.max || (count >= spec.min && *hash_boundary) {
            ranges.push(start..=index);
            start = index + 1;
        }
    }
    if start < boundaries_and_upper_bytes.len() {
        ranges.push(start..=(boundaries_and_upper_bytes.len() - 1));
    }
    ranges
}

fn chunk_ranges_from_independent_hashes(
    config: &Config,
    level: u8,
    entries: &[(Vec<u8>, Vec<u8>)],
    child_counts: Option<&[u64]>,
    hash_boundaries: &[bool],
) -> Result<Vec<std::ops::RangeInclusive<usize>>, Error> {
    debug_assert_eq!(entries.len(), hash_boundaries.len());
    let spec = &config.format.chunking;
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut sizer = EncodedNodeSizer::new(config.format.clone(), level == 0, level)?;

    for (index, ((key, value), hash_boundary)) in entries.iter().zip(hash_boundaries).enumerate() {
        let child_count = child_counts.map(|counts| counts[index]);
        let mut encoded_size = sizer.size_after(key, value, child_count)?;
        if index > start && encoded_size > spec.hard_max_node_bytes {
            ranges.push(start..=(index - 1));
            start = index;
            sizer.reset();
            encoded_size = sizer.size_after(key, value, child_count)?;
        }
        if encoded_size > spec.hard_max_node_bytes {
            return Err(Error::EntryTooLarge {
                encoded_bytes: encoded_size,
                limit: spec.hard_max_node_bytes,
            });
        }
        sizer.push_sized(key, value, child_count, encoded_size)?;
        let count = (index - start + 1) as u64;
        let boundary = encoded_size >= spec.hard_max_node_bytes
            || count >= spec.max
            || (count >= spec.min && *hash_boundary);
        if boundary {
            ranges.push(start..=index);
            start = index + 1;
            sizer.reset();
        }
    }
    if start < entries.len() {
        ranges.push(start..=(entries.len() - 1));
    }
    Ok(ranges)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prolly::store::BatchOp;
    use crate::MemStore;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[test]
    fn exact_incremental_sizer_matches_serialized_nodes() {
        for layout in [
            super::super::format::NodeLayoutSpec::PrefixCompressed,
            super::super::format::NodeLayoutSpec::Plain,
            super::super::format::NodeLayoutSpec::OffsetTable,
        ] {
            for leaf in [true, false] {
                let mut format = super::super::format::TreeFormat::default();
                format.node_layout = layout.clone();
                let level = if leaf { 0 } else { 1 };
                let mut sizer = EncodedNodeSizer::new(format.clone(), leaf, level).unwrap();
                let mut node = Node::builder()
                    .leaf(leaf)
                    .level(level)
                    .tree_format(format)
                    .build();

                assert_eq!(sizer.size(), node.encoded_len() as u64);
                for index in 0..300_u64 {
                    let key = format!("shared-prefix-{index:020}").into_bytes();
                    let value_len = match index {
                        126 => 127,
                        127 => 128,
                        198 => 16_383,
                        199 => 16_384,
                        _ => (index as usize % 41) + 1,
                    };
                    let value = vec![index as u8; value_len];
                    let child_count = (!leaf).then_some(match index {
                        126 => 127,
                        127 => 128,
                        198 => 16_383,
                        199 => 16_384,
                        _ => index + 1,
                    });

                    let predicted = sizer.size_after(&key, &value, child_count).unwrap();
                    sizer.push(&key, &value, child_count).unwrap();
                    node.keys.push(key);
                    node.vals.push(value);
                    if let Some(count) = child_count {
                        node.child_counts.push(count);
                    }

                    assert_eq!(predicted, sizer.size());
                    assert_eq!(
                        sizer.size(),
                        node.encoded_len() as u64,
                        "layout={layout:?} leaf={leaf} index={index}"
                    );
                }

                for index in 300..16_385_u64 {
                    let key = format!("shared-prefix-{index:020}").into_bytes();
                    let value = [b'x'];
                    let child_count = (!leaf).then_some(index + 1);
                    sizer.push(&key, &value, child_count).unwrap();
                    node.keys.push(key);
                    node.vals.push(value.to_vec());
                    if let Some(count) = child_count {
                        node.child_counts.push(count);
                    }
                    if matches!(index, 16_382..=16_384) {
                        assert_eq!(
                            sizer.size(),
                            node.encoded_len() as u64,
                            "count varint transition layout={layout:?} leaf={leaf} index={index}"
                        );
                    }
                }

                sizer.reset();
                assert_eq!(
                    sizer.size(),
                    Node::builder()
                        .leaf(leaf)
                        .level(level)
                        .tree_format(node.format.clone())
                        .build()
                        .encoded_len() as u64
                );
            }
        }
    }

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
                count: 1,
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
    fn serial_internal_build_matches_parallel_root() {
        let config = Config::builder()
            .min_chunk_size(4)
            .max_chunk_size(16)
            .chunking_factor(8)
            .build();
        let parallel_store = CountingStore::default();
        let serial_store = CountingStore::default();
        let children = (0..257)
            .map(|idx| NodeSummary {
                cid: Cid::from_bytes(format!("child-{idx:03}").as_bytes()),
                first_key: format!("k{idx:03}").into_bytes(),
                count: 1,
            })
            .collect::<Vec<_>>();

        let parallel = BatchBuilder::new(parallel_store, config.clone())
            .build_from_chunks(children.clone())
            .unwrap();
        let (serial, written_nodes, written_bytes) =
            BatchBuilder::new(serial_store.clone(), config)
                .build_from_chunks_serial(children)
                .unwrap();

        assert_eq!(serial.root, parallel.root);
        assert!(written_nodes > 1);
        assert!(written_bytes > 0);
        assert_eq!(serial_store.inner.lock().unwrap().batch_put_calls, 1);
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
    fn builders_coalesce_duplicate_keys_with_last_value_winning() {
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(8)
            .chunking_factor(4)
            .build();
        let entries = vec![
            (b"a".to_vec(), b"old".to_vec()),
            (b"a".to_vec(), b"new".to_vec()),
            (b"b".to_vec(), b"value".to_vec()),
        ];

        let batch_store = Arc::new(MemStore::new());
        let mut batch = BatchBuilder::new(batch_store, config.clone());
        for (key, value) in entries.clone() {
            batch.add(key, value);
        }
        let batch_tree = batch.build().unwrap();

        let sorted_store = Arc::new(MemStore::new());
        let mut sorted = SortedBatchBuilder::new(sorted_store, config.clone());
        for (key, value) in entries {
            sorted.add(key, value).unwrap();
        }
        let sorted_tree = sorted.build().unwrap();

        let unique_store = Arc::new(MemStore::new());
        let mut unique = BatchBuilder::new(unique_store, config);
        unique.add(b"a".to_vec(), b"new".to_vec());
        unique.add(b"b".to_vec(), b"value".to_vec());
        let unique_tree = unique.build().unwrap();

        assert_eq!(batch_tree.root, unique_tree.root);
        assert_eq!(sorted_tree.root, unique_tree.root);
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
