//! Deterministic mutation stream and resynchronizing tree writer.

use std::collections::HashSet;

use super::boundary::{entry_count_boundary, BoundaryDetector};
use super::builder::{BatchBuilder, EncodedNodeSizer, NodeSummary};
use super::cid::Cid;
use super::error::{Error, Mutation};
use super::format::{BoundaryInput, ChunkMeasure, NodeLayoutSpec};
use super::node::Node;
use super::store::Store;
use super::{Prolly, Tree};

const LOCAL_WRITE_CACHE_LIMIT: usize = 8;

/// Store-neutral work performed by a tree write.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteStats {
    pub input_mutations: u64,
    pub effective_mutations: u64,
    pub entries_streamed: u64,
    pub nodes_read: u64,
    pub nodes_written: u64,
    pub nodes_reused: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub resync_distance_entries: u64,
    pub resync_distance_nodes: u64,
    pub used_key_stable_fast_path: bool,
    pub used_batched_value_update_path: bool,
}

pub(crate) struct EmittedLeaf {
    pub(crate) summary: NodeSummary,
    pub(crate) bytes: Vec<u8>,
    pub(crate) node: Node,
}

struct EmittedInternal {
    cid: Cid,
    bytes: Vec<u8>,
    node: Node,
}

pub(crate) struct LeafEmitter<'a> {
    config: &'a super::config::Config,
    detector: BoundaryDetector,
    current: Node,
    pub(crate) emitted: Vec<EmittedLeaf>,
    sizer: EncodedNodeSizer,
}

impl<'a> LeafEmitter<'a> {
    pub(crate) fn new(config: &'a super::config::Config) -> Result<Self, Error> {
        Ok(Self {
            config,
            detector: BoundaryDetector::new(config.format.chunking.clone(), 0)?,
            current: Node::builder()
                .leaf(true)
                .level(0)
                .tree_format(config.format.clone())
                .build(),
            emitted: Vec::new(),
            sizer: EncodedNodeSizer::new(config.format.clone(), true, 0)?,
        })
    }

    pub(crate) fn push(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        let hard_max = self.config.format.chunking.hard_max_node_bytes;
        let mut encoded_size = self.sizer.size_after(&key, &value, None)?;
        if !self.current.is_empty() && encoded_size > hard_max {
            self.flush();
            self.detector.reset();
            encoded_size = self.sizer.size_after(&key, &value, None)?;
        }
        if encoded_size > hard_max {
            return Err(Error::EntryTooLarge {
                encoded_bytes: encoded_size,
                limit: hard_max,
            });
        }
        let encoded_entry_bytes = encoded_size.saturating_sub(self.sizer.size());
        self.sizer.push_sized(&key, &value, None, encoded_size)?;
        let boundary = self
            .detector
            .observe(&key, &value, encoded_entry_bytes as usize)?;
        self.current.keys.push(key);
        self.current.vals.push(value);
        if boundary {
            self.flush();
        }
        Ok(())
    }

    pub(crate) fn flush(&mut self) {
        if self.current.is_empty() {
            return;
        }
        let node = std::mem::replace(
            &mut self.current,
            Node::builder()
                .leaf(true)
                .level(0)
                .tree_format(self.config.format.clone())
                .build(),
        );
        let bytes = node.to_bytes();
        debug_assert!(bytes.len() as u64 <= self.config.format.chunking.hard_max_node_bytes);
        self.emitted.push(EmittedLeaf {
            summary: NodeSummary {
                cid: Cid::from_bytes(&bytes),
                first_key: node.keys[0].clone(),
                count: node.keys.len() as u64,
            },
            bytes,
            node,
        });
        self.sizer.reset();
    }

    pub(crate) fn is_aligned_with(&self, old: &NodeSummary) -> bool {
        self.current.is_empty()
            && self
                .emitted
                .last()
                .map(|leaf| leaf.summary.cid == old.cid)
                .unwrap_or(false)
    }
}

/// Apply last-write-wins mutations and emit the unique deterministic tree.
pub(crate) fn apply<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<Mutation>,
) -> Result<(Tree, WriteStats), Error> {
    apply_impl(manager, tree, mutations, true)
}

pub(crate) fn apply_tree<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<Mutation>,
) -> Result<Tree, Error> {
    Ok(apply_impl(manager, tree, mutations, false)?.0)
}

fn apply_impl<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<Mutation>,
    measure_read_bytes: bool,
) -> Result<(Tree, WriteStats), Error> {
    let mut stats = WriteStats {
        input_mutations: mutations.len() as u64,
        ..WriteStats::default()
    };
    if mutations.is_empty() {
        return Ok((tree.clone(), stats));
    }
    if let Some(root) = &tree.root {
        let node = manager.load_arc(root)?;
        if node.format != tree.config.format {
            return Err(Error::FormatMismatch {
                expected: tree.config.format.digest()?,
                actual: node.format.digest()?,
            });
        }
    }

    let mut mutations = normalize(mutations);
    stats.effective_mutations = mutations.len() as u64;
    if tree.root.is_none() {
        return build_empty_base(manager, tree, mutations, stats);
    }
    if let Some(result) = try_append(
        manager,
        tree,
        &mut mutations,
        &mut stats,
        measure_read_bytes,
    )? {
        return Ok(result);
    }
    mutations =
        match try_direct_value_updates(manager, tree, mutations, &mut stats, measure_read_bytes)? {
            DirectValueUpdateAttempt::Applied(result) => return Ok(*result),
            DirectValueUpdateAttempt::Fallback(mutations) => mutations,
        };
    if let Some(result) =
        try_direct_single_delete(manager, tree, &mutations, &mut stats, measure_read_bytes)?
    {
        return Ok(result);
    }
    if let Some(result) =
        try_localized_height_two_deletes(manager, tree, &mutations, &mut stats, measure_read_bytes)?
    {
        return Ok(result);
    }
    let (old_leaves, old_internal_cids) =
        collect_leaf_summaries(manager, tree, &mut stats, measure_read_bytes)?;
    if old_leaves.is_empty() {
        return build_empty_base(manager, tree, mutations, stats);
    }
    let mut mutation_index = 0;
    let mut old_cursor = 0usize;
    let mut summaries = Vec::with_capacity(old_leaves.len());
    let mut emitted = Vec::<EmittedLeaf>::new();

    while mutation_index < mutations.len() {
        let start = old_leaves
            .partition_point(|leaf| {
                leaf.first_key.as_slice() <= mutations[mutation_index].0.as_slice()
            })
            // A hard-cap split occurs immediately before the entry that did
            // not fit. If that entry shrinks, the canonical split can move
            // into the preceding leaf, so replay one predecessor as context.
            .saturating_sub(2)
            .max(old_cursor);
        summaries.extend_from_slice(&old_leaves[old_cursor..start]);
        stats.nodes_reused += start.saturating_sub(old_cursor) as u64;

        let mut emitter = LeafEmitter::new(&tree.config)?;
        let mut resynced_at = None;
        let first_pending_mutation = mutation_index;
        for leaf_index in start..old_leaves.len() {
            let leaf = manager.load_arc(&old_leaves[leaf_index].cid)?;
            stats.nodes_read += 1;
            if measure_read_bytes {
                stats.bytes_read += leaf.encoded_len() as u64;
            }
            if !leaf.leaf || leaf.keys.len() != leaf.vals.len() {
                return Err(Error::InvalidNode);
            }

            for (key, value) in leaf.keys.iter().cloned().zip(leaf.vals.iter().cloned()) {
                while mutation_index < mutations.len() && mutations[mutation_index].0 < key {
                    let (mutation_key, mutation_value) =
                        take_mutation(&mut mutations[mutation_index]);
                    if let Some(value) = mutation_value {
                        emitter.push(mutation_key, value)?;
                        stats.entries_streamed += 1;
                    }
                    mutation_index += 1;
                }
                if mutation_index < mutations.len() && mutations[mutation_index].0 == key {
                    let (_, mutation_value) = take_mutation(&mut mutations[mutation_index]);
                    if let Some(value) = mutation_value {
                        emitter.push(key, value)?;
                        stats.entries_streamed += 1;
                    }
                    mutation_index += 1;
                } else {
                    emitter.push(key, value)?;
                    stats.entries_streamed += 1;
                }
            }

            let next_first = old_leaves.get(leaf_index + 1).map(|leaf| &leaf.first_key);
            while mutation_index < mutations.len()
                && next_first
                    .map(|next| mutations[mutation_index].0 < *next)
                    .unwrap_or(true)
            {
                let (mutation_key, mutation_value) = take_mutation(&mut mutations[mutation_index]);
                if let Some(value) = mutation_value {
                    emitter.push(mutation_key, value)?;
                    stats.entries_streamed += 1;
                }
                mutation_index += 1;
            }

            stats.resync_distance_nodes += 1;
            if mutation_index > first_pending_mutation
                && emitter.is_aligned_with(&old_leaves[leaf_index])
            {
                resynced_at = Some(leaf_index);
                break;
            }
        }

        while mutation_index < mutations.len() && resynced_at.is_none() {
            let (mutation_key, mutation_value) = take_mutation(&mut mutations[mutation_index]);
            if let Some(value) = mutation_value {
                emitter.push(mutation_key, value)?;
                stats.entries_streamed += 1;
            }
            mutation_index += 1;
        }
        emitter.flush();
        summaries.extend(emitter.emitted.iter().map(|leaf| leaf.summary.clone()));
        emitted.extend(emitter.emitted);
        old_cursor = resynced_at.map_or(old_leaves.len(), |index| index + 1);
        if resynced_at.is_none() {
            break;
        }
    }
    summaries.extend_from_slice(&old_leaves[old_cursor..]);
    stats.nodes_reused += old_leaves.len().saturating_sub(old_cursor) as u64;
    stats.resync_distance_entries = stats.entries_streamed;

    if summaries
        .iter()
        .map(|leaf| &leaf.cid)
        .eq(old_leaves.iter().map(|leaf| &leaf.cid))
    {
        return Ok((tree.clone(), stats));
    }

    let old_cids = old_leaves
        .iter()
        .map(|leaf| leaf.cid.clone())
        .collect::<HashSet<_>>();
    let changed_leaves = emitted
        .iter()
        .filter(|leaf| !old_cids.contains(&leaf.summary.cid))
        .collect::<Vec<_>>();
    let fixed_separators = tree.config.format.chunking.measure == ChunkMeasure::EntryCount
        && tree.config.format.chunking.input == BoundaryInput::Key
        && !matches!(
            tree.config.format.node_layout,
            NodeLayoutSpec::Custom { .. }
        )
        && summaries.len() == old_leaves.len()
        && summaries
            .iter()
            .zip(&old_leaves)
            .all(|(new, old)| new.first_key == old.first_key);
    if fixed_separators {
        let changes = changed_leaves
            .iter()
            .map(|leaf| leaf.summary.clone())
            .collect::<Vec<_>>();
        let (written, internal_nodes) = rewrite_fixed_separator_paths(manager, tree, &changes)?;
        let writes = changed_leaves
            .iter()
            .map(|leaf| (leaf.summary.cid.as_bytes(), leaf.bytes.as_slice()))
            .chain(
                internal_nodes
                    .iter()
                    .filter(|node| !old_internal_cids.contains(&node.cid))
                    .map(|node| (node.cid.as_bytes(), node.bytes.as_slice())),
            )
            .collect::<Vec<_>>();
        manager
            .store()
            .batch_put(&writes)
            .map_err(|error| Error::Store(Box::new(error)))?;
        let bytes_written = writes.iter().map(|(_, bytes)| bytes.len()).sum::<usize>();
        manager.record_batch_write_metrics(writes.len(), bytes_written);
        stats.nodes_written += writes.len() as u64;
        stats.bytes_written += bytes_written as u64;
        if writes.len() <= LOCAL_WRITE_CACHE_LIMIT {
            for leaf in &changed_leaves {
                manager.cache_node(leaf.summary.cid.clone(), leaf.node.clone());
            }
            for node in internal_nodes
                .iter()
                .filter(|node| !old_internal_cids.contains(&node.cid))
            {
                manager.cache_node(node.cid.clone(), node.node.clone());
            }
        }
        return Ok((written, stats));
    }
    let builder = BatchBuilder::new(manager.store(), tree.config.clone());
    let (written, internal_nodes) = builder.build_from_chunks_serial_deferred(summaries)?;
    let writes = changed_leaves
        .iter()
        .map(|leaf| (leaf.summary.cid.as_bytes(), leaf.bytes.as_slice()))
        .chain(
            internal_nodes
                .iter()
                .filter(|node| !old_internal_cids.contains(&node.cid))
                .map(|node| (node.cid.as_bytes(), node.bytes.as_slice())),
        )
        .collect::<Vec<_>>();
    if !writes.is_empty() {
        manager
            .store()
            .batch_put(&writes)
            .map_err(|error| Error::Store(Box::new(error)))?;
        let bytes_written = writes.iter().map(|(_, bytes)| bytes.len()).sum::<usize>();
        stats.nodes_written += writes.len() as u64;
        stats.bytes_written += bytes_written as u64;
        manager.record_batch_write_metrics(writes.len(), bytes_written);
        if writes.len() <= LOCAL_WRITE_CACHE_LIMIT {
            for leaf in &changed_leaves {
                manager.cache_node(leaf.summary.cid.clone(), leaf.node.clone());
            }
            for node in internal_nodes
                .iter()
                .filter(|node| !old_internal_cids.contains(&node.cid))
            {
                manager.cache_node(node.cid.clone(), node.node.clone());
            }
        }
    }
    if let Some(root) = &written.root {
        let _ = manager.load_arc(root)?;
    }
    Ok((written, stats))
}

fn try_localized_height_two_deletes<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    mutations: &[(Vec<u8>, Option<Vec<u8>>)],
    stats: &mut WriteStats,
    measure_read_bytes: bool,
) -> Result<Option<(Tree, WriteStats)>, Error> {
    if mutations.len() < 2
        || mutations.iter().any(|(_, value)| value.is_some())
        || matches!(
            tree.config.format.node_layout,
            NodeLayoutSpec::Custom { .. }
        )
    {
        return Ok(None);
    }
    let Some(root_cid) = &tree.root else {
        return Ok(None);
    };
    let root = manager.load_arc(root_cid)?;
    stats.nodes_read += 1;
    if measure_read_bytes {
        stats.bytes_read += root.encoded_len() as u64;
    }
    if root.leaf
        || root.level != 2
        || root.keys.is_empty()
        || root.keys.len() != root.vals.len()
        || root.child_counts.len() != root.len()
    {
        return Ok(None);
    }

    let first_key = &mutations[0].0;
    let last_key = &mutations[mutations.len() - 1].0;
    let first_child = root
        .keys
        .partition_point(|separator| separator.as_slice() <= first_key.as_slice())
        .saturating_sub(1);
    let last_child = root
        .keys
        .partition_point(|separator| separator.as_slice() <= last_key.as_slice())
        .saturating_sub(1);
    let window_start = first_child.saturating_sub(1);
    let window_end = last_child.saturating_add(3).min(root.len());
    if window_start >= window_end {
        return Ok(None);
    }

    let window_cids = root.vals[window_start..window_end]
        .iter()
        .map(|value| child_cid(value))
        .collect::<Result<Vec<_>, _>>()?;
    let window_nodes = if window_cids.len() > 1 {
        manager.load_many_ordered(&window_cids)?
    } else {
        window_cids
            .iter()
            .map(|cid| manager.load_arc(cid))
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut old_leaves = Vec::new();
    for node in &window_nodes {
        stats.nodes_read += 1;
        if measure_read_bytes {
            stats.bytes_read += node.encoded_len() as u64;
        }
        if node.leaf
            || node.level != 1
            || node.format != tree.config.format
            || node.keys.len() != node.vals.len()
            || node.child_counts.len() != node.len()
            || node.child_counts.contains(&0)
        {
            return Ok(None);
        }
        for index in 0..node.len() {
            old_leaves.push(NodeSummary {
                cid: child_cid(&node.vals[index])?,
                first_key: node.keys[index].clone(),
                count: node.child_counts[index],
            });
        }
    }
    if old_leaves.is_empty() {
        return Ok(None);
    }

    let replay_start = old_leaves
        .partition_point(|leaf| leaf.first_key.as_slice() <= first_key.as_slice())
        .saturating_sub(2);
    {
        let last_mutation_leaf = old_leaves
            .partition_point(|leaf| leaf.first_key.as_slice() <= last_key.as_slice())
            .saturating_sub(1);
        let prefetch_end = last_mutation_leaf.saturating_add(2).min(old_leaves.len());
        if prefetch_end.saturating_sub(replay_start) > 1 {
            let leaf_cids = old_leaves[replay_start..prefetch_end]
                .iter()
                .map(|leaf| leaf.cid.clone())
                .collect::<Vec<_>>();
            let _ = manager.load_many_ordered(&leaf_cids)?;
        }
    }
    let mut mutation_index = 0usize;
    let mut emitter = LeafEmitter::new(&tree.config)?;
    let mut resynced_at = None;
    for leaf_index in replay_start..old_leaves.len() {
        let leaf = manager.load_arc(&old_leaves[leaf_index].cid)?;
        stats.nodes_read += 1;
        if measure_read_bytes {
            stats.bytes_read += leaf.encoded_len() as u64;
        }
        if !leaf.leaf || leaf.keys.len() != leaf.vals.len() {
            return Err(Error::InvalidNode);
        }

        for (key, value) in leaf.keys.iter().cloned().zip(leaf.vals.iter().cloned()) {
            while mutation_index < mutations.len() && mutations[mutation_index].0 < key {
                mutation_index += 1;
            }
            if mutation_index < mutations.len() && mutations[mutation_index].0 == key {
                mutation_index += 1;
            } else {
                emitter.push(key, value)?;
                stats.entries_streamed += 1;
            }
        }

        let next_first = old_leaves.get(leaf_index + 1).map(|leaf| &leaf.first_key);
        while mutation_index < mutations.len()
            && next_first
                .map(|next| mutations[mutation_index].0 < *next)
                .unwrap_or(true)
        {
            mutation_index += 1;
        }

        stats.resync_distance_nodes += 1;
        if mutation_index > 0 && emitter.is_aligned_with(&old_leaves[leaf_index]) {
            resynced_at = Some(leaf_index);
            break;
        }
    }
    if mutation_index != mutations.len() {
        return Ok(None);
    }
    if resynced_at.is_none() && window_end < root.len() {
        return Ok(None);
    }
    emitter.flush();

    let old_cursor = resynced_at.map_or(old_leaves.len(), |index| index + 1);
    let mut leaf_summaries = Vec::with_capacity(old_leaves.len());
    leaf_summaries.extend_from_slice(&old_leaves[..replay_start]);
    leaf_summaries.extend(emitter.emitted.iter().map(|leaf| leaf.summary.clone()));
    leaf_summaries.extend_from_slice(&old_leaves[old_cursor..]);
    stats.nodes_reused += replay_start.saturating_add(old_leaves.len() - old_cursor) as u64;
    stats.resync_distance_entries = stats.entries_streamed;

    let builder = BatchBuilder::new(manager.store(), tree.config.clone());
    let (replacement_summaries, internal_nodes) =
        builder.build_level_serial_deferred(leaf_summaries, 1)?;
    if window_end < root.len()
        && replacement_summaries.last().map(|summary| &summary.cid) != window_cids.last()
    {
        return Ok(None);
    }
    if replacement_summaries.is_empty() {
        return Ok(None);
    }

    let mut updated_root = (*root).clone();
    updated_root.keys.splice(
        window_start..window_end,
        replacement_summaries
            .iter()
            .map(|summary| summary.first_key.clone()),
    );
    updated_root.vals.splice(
        window_start..window_end,
        replacement_summaries
            .iter()
            .map(|summary| summary.cid.0.to_vec()),
    );
    updated_root.child_counts.splice(
        window_start..window_end,
        replacement_summaries.iter().map(|summary| summary.count),
    );
    if updated_root.keys.windows(2).any(|pair| pair[0] >= pair[1])
        || updated_root.len() > updated_root.max_chunk_size()
        || updated_root.encoded_len() as u64 > tree.config.format.chunking.hard_max_node_bytes
    {
        return Ok(None);
    }

    let rebuilt_root = if updated_root.len() == 1 {
        None
    } else {
        let candidate_children = updated_root
            .keys
            .iter()
            .zip(&updated_root.vals)
            .zip(&updated_root.child_counts)
            .map(|((key, value), count)| {
                Ok(NodeSummary {
                    cid: child_cid(value)?,
                    first_key: key.clone(),
                    count: *count,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let (root_summaries, mut root_nodes) =
            builder.build_level_serial_deferred(candidate_children, 2)?;
        if root_summaries.len() != 1 || root_nodes.len() != 1 {
            return Ok(None);
        }
        let rebuilt_root = root_nodes.pop().ok_or(Error::InvalidNode)?;
        if root_summaries[0].cid != rebuilt_root.cid || rebuilt_root.node != updated_root {
            return Ok(None);
        }
        Some(rebuilt_root)
    };

    let old_leaf_cids = old_leaves
        .iter()
        .map(|summary| summary.cid.clone())
        .collect::<HashSet<_>>();
    let old_internal_cids = window_cids.iter().cloned().collect::<HashSet<_>>();
    let mut writes = Vec::<(Cid, Vec<u8>, Node)>::new();
    for leaf in emitter.emitted {
        if !old_leaf_cids.contains(&leaf.summary.cid) {
            writes.push((leaf.summary.cid, leaf.bytes, leaf.node));
        }
    }
    for node in internal_nodes {
        if !old_internal_cids.contains(&node.cid) {
            writes.push((node.cid, node.bytes, node.node));
        }
    }

    let root = if updated_root.len() == 1 {
        child_cid(&updated_root.vals[0])?
    } else {
        let rebuilt_root = rebuilt_root.ok_or(Error::InvalidNode)?;
        let cid = rebuilt_root.cid.clone();
        if cid != *root_cid {
            writes.push((cid.clone(), rebuilt_root.bytes, rebuilt_root.node));
        }
        cid
    };
    if !writes.is_empty() {
        let entries = writes
            .iter()
            .map(|(cid, bytes, _)| (cid.as_bytes(), bytes.as_slice()))
            .collect::<Vec<_>>();
        manager
            .store()
            .batch_put(&entries)
            .map_err(|error| Error::Store(Box::new(error)))?;
        let bytes_written = entries.iter().map(|(_, bytes)| bytes.len()).sum::<usize>();
        manager.record_batch_write_metrics(entries.len(), bytes_written);
        stats.nodes_written += entries.len() as u64;
        stats.bytes_written += bytes_written as u64;
        if entries.len() <= LOCAL_WRITE_CACHE_LIMIT {
            drop(entries);
            for (cid, _, node) in writes {
                manager.cache_node(cid, node);
            }
        }
    }

    Ok(Some((
        Tree {
            root: Some(root),
            config: tree.config.clone(),
        },
        *stats,
    )))
}

fn try_append<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    mutations: &mut Vec<(Vec<u8>, Option<Vec<u8>>)>,
    stats: &mut WriteStats,
    measure_read_bytes: bool,
) -> Result<Option<(Tree, WriteStats)>, Error> {
    if mutations.iter().any(|(_, value)| value.is_none()) {
        return Ok(None);
    }
    let path = rightmost_internal_path(manager, tree)?;
    let last_cid = match path.last() {
        Some((_, node)) => child_cid(node.vals.last().ok_or(Error::InvalidNode)?)?,
        None => tree.root.clone().ok_or(Error::InvalidNode)?,
    };
    let last_leaf = manager.load_arc(&last_cid)?;
    stats.nodes_read += 1;
    if measure_read_bytes {
        stats.bytes_read += last_leaf.encoded_len() as u64;
    }
    if !last_leaf.leaf || last_leaf.keys.len() != last_leaf.vals.len() {
        return Err(Error::InvalidNode);
    }
    let Some(max_key) = last_leaf.keys.last() else {
        return Err(Error::InvalidNode);
    };
    if mutations
        .first()
        .map_or(true, |(key, _)| key.as_slice() <= max_key.as_slice())
    {
        return Ok(None);
    }

    let mut emitter = LeafEmitter::new(&tree.config)?;
    for (key, value) in last_leaf
        .keys
        .iter()
        .cloned()
        .zip(last_leaf.vals.iter().cloned())
    {
        emitter.push(key, value)?;
        stats.entries_streamed += 1;
    }
    for (key, value) in mutations.drain(..) {
        emitter.push(
            key,
            value.expect("append path rejects deletes before streaming"),
        )?;
        stats.entries_streamed += 1;
    }
    emitter.flush();

    let builder = BatchBuilder::new(manager.store(), tree.config.clone());
    let mut current = emitter
        .emitted
        .iter()
        .map(|leaf| leaf.summary.clone())
        .collect::<Vec<_>>();
    let mut internal_nodes = Vec::new();
    let mut current_level = 0u8;

    for (_, node) in path.iter().rev() {
        let mut children = internal_child_summaries(node)?;
        children.pop().ok_or(Error::InvalidNode)?;
        children.extend(current);
        let (summaries, pending) = builder.build_level_serial_deferred(children, node.level)?;
        current = summaries;
        internal_nodes.extend(pending);
        current_level = node.level;
    }
    while current.len() > 1 {
        current_level = current_level.checked_add(1).ok_or(Error::InvalidNode)?;
        let (summaries, pending) = builder.build_level_serial_deferred(current, current_level)?;
        current = summaries;
        internal_nodes.extend(pending);
    }
    let root = current.into_iter().next().ok_or(Error::InvalidNode)?.cid;

    let old_internal_cids = path
        .iter()
        .map(|(cid, _)| cid.clone())
        .collect::<HashSet<_>>();
    let changed_leaves = emitter
        .emitted
        .iter()
        .filter(|leaf| leaf.summary.cid != last_cid)
        .collect::<Vec<_>>();
    let writes = changed_leaves
        .iter()
        .map(|leaf| (leaf.summary.cid.as_bytes(), leaf.bytes.as_slice()))
        .chain(
            internal_nodes
                .iter()
                .filter(|node| !old_internal_cids.contains(&node.cid))
                .map(|node| (node.cid.as_bytes(), node.bytes.as_slice())),
        )
        .collect::<Vec<_>>();
    if !writes.is_empty() {
        manager
            .store()
            .batch_put(&writes)
            .map_err(|error| Error::Store(Box::new(error)))?;
        let bytes_written = writes.iter().map(|(_, bytes)| bytes.len()).sum::<usize>();
        manager.record_batch_write_metrics(writes.len(), bytes_written);
        stats.nodes_written += writes.len() as u64;
        stats.bytes_written += bytes_written as u64;
        if writes.len() <= LOCAL_WRITE_CACHE_LIMIT {
            for leaf in changed_leaves {
                manager.cache_node(leaf.summary.cid.clone(), leaf.node.clone());
            }
            for node in internal_nodes
                .iter()
                .filter(|node| !old_internal_cids.contains(&node.cid))
            {
                manager.cache_node(node.cid.clone(), node.node.clone());
            }
        }
    }
    stats.resync_distance_entries = stats.entries_streamed;
    stats.resync_distance_nodes = 1;
    Ok(Some((
        Tree {
            root: Some(root),
            config: tree.config.clone(),
        },
        *stats,
    )))
}

fn rightmost_internal_path<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
) -> Result<Vec<(Cid, std::sync::Arc<Node>)>, Error> {
    let Some(mut cid) = tree.root.clone() else {
        return Ok(Vec::new());
    };
    let mut path = Vec::new();
    loop {
        let node = manager.load_arc(&cid)?;
        if node.leaf {
            return Ok(path);
        }
        let child = node.vals.last().ok_or(Error::InvalidNode)?;
        let next = child_cid(child)?;
        path.push((cid, node));
        cid = next;
    }
}

fn internal_child_summaries(node: &Node) -> Result<Vec<NodeSummary>, Error> {
    if node.leaf || node.keys.len() != node.vals.len() || node.child_counts.len() != node.len() {
        return Err(Error::InvalidNode);
    }
    node.keys
        .iter()
        .zip(&node.vals)
        .zip(&node.child_counts)
        .map(|((key, value), count)| {
            Ok(NodeSummary {
                cid: child_cid(value)?,
                first_key: key.clone(),
                count: *count,
            })
        })
        .collect()
}

enum DirectValueUpdateAttempt {
    Applied(Box<(Tree, WriteStats)>),
    Fallback(Vec<(Vec<u8>, Option<Vec<u8>>)>),
}

fn try_direct_value_updates<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    stats: &mut WriteStats,
    measure_read_bytes: bool,
) -> Result<DirectValueUpdateAttempt, Error> {
    let chunking = &tree.config.format.chunking;
    if chunking.measure != ChunkMeasure::EntryCount
        || chunking.input != BoundaryInput::Key
        || matches!(
            tree.config.format.node_layout,
            NodeLayoutSpec::Custom { .. }
        )
        || mutations.iter().any(|(_, value)| value.is_none())
    {
        return Ok(DirectValueUpdateAttempt::Fallback(mutations));
    }
    let Some(root) = &tree.root else {
        return Ok(DirectValueUpdateAttempt::Fallback(mutations));
    };

    let batched_mutations = mutations
        .into_iter()
        .map(|(key, value)| Mutation::Upsert {
            key,
            val: value.expect("direct value path rejects deletes before routing"),
        })
        .collect::<Vec<_>>();
    let metrics_before = manager.metrics();
    let mutations =
        match super::batch::try_apply_batched_value_updates(manager, tree, batched_mutations)? {
            super::batch::KeyStableBatchAttempt::Applied(result) => {
                let metrics_after = manager.metrics();
                stats.nodes_read += metrics_after
                    .nodes_read
                    .saturating_sub(metrics_before.nodes_read);
                if measure_read_bytes {
                    stats.bytes_read += metrics_after
                        .bytes_read
                        .saturating_sub(metrics_before.bytes_read);
                }
                stats.nodes_written += result.written_nodes as u64;
                stats.bytes_written += result.written_bytes as u64;
                stats.entries_streamed += result.entries_streamed as u64;
                stats.resync_distance_entries = result.entries_streamed as u64;
                stats.resync_distance_nodes = result.affected_leaves as u64;
                stats.used_key_stable_fast_path = true;
                stats.used_batched_value_update_path = true;
                return Ok(DirectValueUpdateAttempt::Applied(Box::new((
                    result.tree,
                    *stats,
                ))));
            }
            super::batch::KeyStableBatchAttempt::Fallback(mutations) => mutations
                .into_iter()
                .map(mutation_parts)
                .collect::<Vec<_>>(),
        };

    let mut leaves = Vec::new();
    let mut internals = Vec::new();
    let root_summary = {
        let mut context = DirectRewriteContext {
            leaves: &mut leaves,
            internals: &mut internals,
            stats,
            measure_read_bytes,
        };
        rewrite_value_update_subtree(manager, root, &mutations, true, &mut context)?
    };
    let Some(root_summary) = root_summary else {
        return Ok(DirectValueUpdateAttempt::Fallback(mutations));
    };

    let writes = leaves
        .iter()
        .map(|leaf: &EmittedLeaf| (leaf.summary.cid.as_bytes(), leaf.bytes.as_slice()))
        .chain(
            internals
                .iter()
                .map(|node: &EmittedInternal| (node.cid.as_bytes(), node.bytes.as_slice())),
        )
        .collect::<Vec<_>>();
    let changed_leaf_count = leaves.len();
    if !writes.is_empty() {
        manager
            .store()
            .batch_put(&writes)
            .map_err(|error| Error::Store(Box::new(error)))?;
        let bytes_written = writes.iter().map(|(_, bytes)| bytes.len()).sum::<usize>();
        manager.record_batch_write_metrics(writes.len(), bytes_written);
        stats.nodes_written += writes.len() as u64;
        stats.bytes_written += bytes_written as u64;
        if writes.len() <= LOCAL_WRITE_CACHE_LIMIT {
            drop(writes);
            for leaf in leaves {
                manager.cache_node(leaf.summary.cid, leaf.node);
            }
            for node in internals {
                manager.cache_node(node.cid, node.node);
            }
        }
    }
    stats.resync_distance_entries = stats.entries_streamed;
    stats.resync_distance_nodes = changed_leaf_count as u64;
    stats.used_key_stable_fast_path = true;
    Ok(DirectValueUpdateAttempt::Applied(Box::new((
        Tree {
            root: Some(root_summary.cid),
            config: tree.config.clone(),
        },
        *stats,
    ))))
}

enum DirectDelete {
    Applied(NodeSummary),
    Unchanged,
    Fallback,
}

fn try_direct_single_delete<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    mutations: &[(Vec<u8>, Option<Vec<u8>>)],
    stats: &mut WriteStats,
    measure_read_bytes: bool,
) -> Result<Option<(Tree, WriteStats)>, Error> {
    let chunking = &tree.config.format.chunking;
    if mutations.len() != 1
        || mutations[0].1.is_some()
        || chunking.measure != ChunkMeasure::EntryCount
        || chunking.input != BoundaryInput::Key
        || !matches!(
            chunking.rule,
            super::format::BoundaryRule::HashThreshold { .. }
        )
        || matches!(
            tree.config.format.node_layout,
            NodeLayoutSpec::Custom { .. }
        )
    {
        return Ok(None);
    }
    let Some(root) = &tree.root else {
        return Ok(Some((tree.clone(), *stats)));
    };
    let mut leaves = Vec::new();
    let mut internals = Vec::new();
    let result = rewrite_single_delete_subtree(
        manager,
        root,
        &mutations[0].0,
        &tree.config,
        &mut leaves,
        &mut internals,
        stats,
        measure_read_bytes,
    )?;
    let root = match result {
        DirectDelete::Applied(summary) => summary.cid,
        DirectDelete::Unchanged => return Ok(Some((tree.clone(), *stats))),
        DirectDelete::Fallback => return Ok(None),
    };

    let writes = leaves
        .iter()
        .map(|leaf: &EmittedLeaf| (leaf.summary.cid.as_bytes(), leaf.bytes.as_slice()))
        .chain(
            internals
                .iter()
                .map(|node: &EmittedInternal| (node.cid.as_bytes(), node.bytes.as_slice())),
        )
        .collect::<Vec<_>>();
    manager
        .store()
        .batch_put(&writes)
        .map_err(|error| Error::Store(Box::new(error)))?;
    let bytes_written = writes.iter().map(|(_, bytes)| bytes.len()).sum::<usize>();
    manager.record_batch_write_metrics(writes.len(), bytes_written);
    stats.nodes_written += writes.len() as u64;
    stats.bytes_written += bytes_written as u64;
    stats.resync_distance_nodes = 1;
    stats.resync_distance_entries = stats.entries_streamed;
    drop(writes);
    for leaf in leaves {
        manager.cache_node(leaf.summary.cid, leaf.node);
    }
    for node in internals {
        manager.cache_node(node.cid, node.node);
    }
    Ok(Some((
        Tree {
            root: Some(root),
            config: tree.config.clone(),
        },
        *stats,
    )))
}

#[allow(clippy::too_many_arguments)]
fn rewrite_single_delete_subtree<S: Store>(
    manager: &Prolly<S>,
    cid: &Cid,
    key: &[u8],
    config: &super::config::Config,
    leaves: &mut Vec<EmittedLeaf>,
    internals: &mut Vec<EmittedInternal>,
    stats: &mut WriteStats,
    measure_read_bytes: bool,
) -> Result<DirectDelete, Error> {
    let node = manager.load_arc(cid)?;
    stats.nodes_read += 1;
    if measure_read_bytes {
        stats.bytes_read += node.encoded_len() as u64;
    }
    if node.keys.is_empty() || node.keys.len() != node.vals.len() {
        return Err(Error::InvalidNode);
    }

    if node.leaf {
        let Ok(index) = node
            .keys
            .binary_search_by(|candidate| candidate.as_slice().cmp(key))
        else {
            return Ok(DirectDelete::Unchanged);
        };
        if index == 0 || node.len() == 1 {
            return Ok(DirectDelete::Fallback);
        }

        let old_closed = entry_count_boundary(
            &config.format.chunking,
            0,
            node.len(),
            node.keys.last().ok_or(Error::InvalidNode)?,
        )?;
        let mut updated = (*node).clone();
        updated.keys.remove(index);
        updated.vals.remove(index);
        let new_closed = entry_count_boundary(
            &config.format.chunking,
            0,
            updated.len(),
            updated.keys.last().ok_or(Error::InvalidNode)?,
        )?;
        if new_closed != old_closed {
            return Ok(DirectDelete::Fallback);
        }
        stats.entries_streamed += updated.len() as u64;
        let bytes = updated.to_bytes();
        let new_cid = Cid::from_bytes(&bytes);
        let summary = NodeSummary {
            cid: new_cid.clone(),
            first_key: updated.keys[0].clone(),
            count: updated.len() as u64,
        };
        leaves.push(EmittedLeaf {
            summary: summary.clone(),
            bytes,
            node: updated,
        });
        return Ok(DirectDelete::Applied(summary));
    }
    if node.child_counts.len() != node.len() {
        return Err(Error::InvalidNode);
    }

    let child_index = node
        .keys
        .partition_point(|separator| separator.as_slice() <= key)
        .saturating_sub(1);
    let child = child_cid(&node.vals[child_index])?;
    let replacement = match rewrite_single_delete_subtree(
        manager,
        &child,
        key,
        config,
        leaves,
        internals,
        stats,
        measure_read_bytes,
    )? {
        DirectDelete::Applied(summary) => summary,
        DirectDelete::Unchanged => return Ok(DirectDelete::Unchanged),
        DirectDelete::Fallback => return Ok(DirectDelete::Fallback),
    };

    let old_child_count = node.child_counts[child_index];
    let mut updated = (*node).clone();
    updated.vals[child_index] = replacement.cid.0.to_vec();
    updated.child_counts[child_index] = replacement.count;
    let first_key = updated.keys[0].clone();
    let count = node
        .child_counts
        .iter()
        .copied()
        .sum::<u64>()
        .saturating_sub(old_child_count)
        .saturating_add(replacement.count);
    let bytes = updated.to_bytes();
    let new_cid = Cid::from_bytes(&bytes);
    internals.push(EmittedInternal {
        cid: new_cid.clone(),
        bytes,
        node: updated,
    });
    Ok(DirectDelete::Applied(NodeSummary {
        cid: new_cid,
        first_key,
        count,
    }))
}

struct DirectRewriteContext<'a> {
    leaves: &'a mut Vec<EmittedLeaf>,
    internals: &'a mut Vec<EmittedInternal>,
    stats: &'a mut WriteStats,
    measure_read_bytes: bool,
}

fn rewrite_value_update_subtree<S: Store>(
    manager: &Prolly<S>,
    cid: &Cid,
    mutations: &[(Vec<u8>, Option<Vec<u8>>)],
    rightmost: bool,
    context: &mut DirectRewriteContext<'_>,
) -> Result<Option<NodeSummary>, Error> {
    let node = manager.load_arc(cid)?;
    context.stats.nodes_read += 1;
    if context.measure_read_bytes {
        context.stats.bytes_read += node.encoded_len() as u64;
    }
    if node.keys.is_empty() || node.keys.len() != node.vals.len() {
        return Err(Error::InvalidNode);
    }

    if node.leaf {
        if !rightmost {
            let Some(last_key) = node.keys.last() else {
                return Err(Error::InvalidNode);
            };
            if !super::boundary::entry_count_boundary(
                &node.format.chunking,
                u16::from(node.level),
                node.len(),
                last_key,
            )? {
                return Ok(None);
            }
        }
        let mut updated = (*node).clone();
        for (key, value) in mutations {
            let Ok(index) = updated
                .keys
                .binary_search_by(|candidate| candidate.as_slice().cmp(key.as_slice()))
            else {
                return Ok(None);
            };
            updated.vals[index] = value
                .clone()
                .expect("direct value path rejects deletes before routing");
        }
        context.stats.entries_streamed += updated.len() as u64;
        let bytes = updated.to_bytes();
        let hard_max =
            usize::try_from(updated.format.chunking.hard_max_node_bytes).unwrap_or(usize::MAX);
        if node.encoded_len() >= hard_max || bytes.len() >= hard_max {
            return Ok(None);
        }
        let new_cid = Cid::from_bytes(&bytes);
        let summary = NodeSummary {
            cid: new_cid.clone(),
            first_key: updated.keys[0].clone(),
            count: updated.len() as u64,
        };
        if new_cid != *cid {
            context.leaves.push(EmittedLeaf {
                summary: summary.clone(),
                bytes,
                node: updated,
            });
        }
        return Ok(Some(summary));
    }
    if node.child_counts.len() != node.len() {
        return Err(Error::InvalidNode);
    }

    let mut updated = (*node).clone();
    let mut start = 0usize;
    let mut touched_children = 0usize;
    while start < mutations.len() {
        let child_index = updated
            .keys
            .partition_point(|key| key.as_slice() <= mutations[start].0.as_slice())
            .saturating_sub(1);
        let end = updated
            .keys
            .get(child_index + 1)
            .map_or(mutations.len(), |boundary| {
                start
                    + mutations[start..]
                        .partition_point(|mutation| mutation.0.as_slice() < boundary.as_slice())
            });
        let child = child_cid(&updated.vals[child_index])?;
        let Some(replacement) = rewrite_value_update_subtree(
            manager,
            &child,
            &mutations[start..end],
            rightmost && child_index.saturating_add(1) == updated.len(),
            context,
        )?
        else {
            return Ok(None);
        };
        updated.keys[child_index] = replacement.first_key;
        updated.vals[child_index] = replacement.cid.0.to_vec();
        updated.child_counts[child_index] = replacement.count;
        touched_children += 1;
        start = end;
    }
    context.stats.nodes_reused += updated.len().saturating_sub(touched_children) as u64;

    let first_key = updated.keys[0].clone();
    let count = updated.child_counts.iter().copied().sum();
    let bytes = updated.to_bytes();
    let new_cid = Cid::from_bytes(&bytes);
    if new_cid != *cid {
        context.internals.push(EmittedInternal {
            cid: new_cid.clone(),
            bytes,
            node: updated,
        });
    }
    Ok(Some(NodeSummary {
        cid: new_cid,
        first_key,
        count,
    }))
}

fn rewrite_fixed_separator_paths<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    changes: &[NodeSummary],
) -> Result<(Tree, Vec<EmittedInternal>), Error> {
    let Some(root) = &tree.root else {
        return Err(Error::InvalidNode);
    };
    let root_node = manager.load_arc(root)?;
    if root_node.leaf {
        let replacement = changes.first().ok_or(Error::InvalidNode)?;
        return Ok((
            Tree {
                root: Some(replacement.cid.clone()),
                config: tree.config.clone(),
            },
            Vec::new(),
        ));
    }

    let mut pending = Vec::new();
    let root = rewrite_internal_node(manager, root, changes, &mut pending)?;
    Ok((
        Tree {
            root: Some(root.cid),
            config: tree.config.clone(),
        },
        pending,
    ))
}

fn rewrite_internal_node<S: Store>(
    manager: &Prolly<S>,
    cid: &Cid,
    changes: &[NodeSummary],
    pending: &mut Vec<EmittedInternal>,
) -> Result<NodeSummary, Error> {
    let node = manager.load_arc(cid)?;
    if node.leaf
        || node.keys.is_empty()
        || node.keys.len() != node.vals.len()
        || node.child_counts.len() != node.len()
    {
        return Err(Error::InvalidNode);
    }

    let mut updated = (*node).clone();
    let mut change_start = 0usize;
    while change_start < changes.len() {
        let child_index = updated
            .keys
            .partition_point(|key| key.as_slice() <= changes[change_start].first_key.as_slice())
            .saturating_sub(1);
        let mut change_end = change_start + 1;
        while change_end < changes.len()
            && updated
                .keys
                .partition_point(|key| key.as_slice() <= changes[change_end].first_key.as_slice())
                .saturating_sub(1)
                == child_index
        {
            change_end += 1;
        }

        let replacement = if updated.level == 1 {
            if change_end != change_start + 1
                || updated.keys[child_index] != changes[change_start].first_key
            {
                return Err(Error::InvalidNode);
            }
            changes[change_start].clone()
        } else {
            let child = child_cid(&updated.vals[child_index])?;
            rewrite_internal_node(manager, &child, &changes[change_start..change_end], pending)?
        };
        updated.keys[child_index] = replacement.first_key;
        updated.vals[child_index] = replacement.cid.0.to_vec();
        updated.child_counts[child_index] = replacement.count;
        change_start = change_end;
    }

    let first_key = updated.keys[0].clone();
    let count = updated.child_counts.iter().copied().sum();
    let bytes = updated.to_bytes();
    let new_cid = Cid::from_bytes(&bytes);
    if new_cid != *cid {
        pending.push(EmittedInternal {
            cid: new_cid.clone(),
            bytes,
            node: updated,
        });
    }
    Ok(NodeSummary {
        cid: new_cid,
        first_key,
        count,
    })
}

fn normalize(mutations: Vec<Mutation>) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    if mutations
        .windows(2)
        .all(|pair| pair[0].key() <= pair[1].key())
    {
        let mut normalized = Vec::<(Vec<u8>, Option<Vec<u8>>)>::with_capacity(mutations.len());
        for mutation in mutations {
            let (key, value) = mutation_parts(mutation);
            match normalized.last_mut() {
                Some((previous_key, previous_value)) if *previous_key == key => {
                    *previous_value = value;
                }
                _ => normalized.push((key, value)),
            }
        }
        return normalized;
    }

    let mut sorted = mutations
        .into_iter()
        .map(mutation_parts)
        .collect::<Vec<_>>();
    sorted.sort_by(|left, right| left.0.cmp(&right.0));
    let mut normalized = Vec::<(Vec<u8>, Option<Vec<u8>>)>::with_capacity(sorted.len());
    for (key, value) in sorted {
        match normalized.last_mut() {
            Some((previous_key, previous_value)) if *previous_key == key => {
                *previous_value = value;
            }
            _ => normalized.push((key, value)),
        }
    }
    normalized
}

fn mutation_parts(mutation: Mutation) -> (Vec<u8>, Option<Vec<u8>>) {
    match mutation {
        Mutation::Upsert { key, val } => (key, Some(val)),
        Mutation::Delete { key } => (key, None),
    }
}

fn take_mutation(mutation: &mut (Vec<u8>, Option<Vec<u8>>)) -> (Vec<u8>, Option<Vec<u8>>) {
    (std::mem::take(&mut mutation.0), mutation.1.take())
}

fn build_empty_base<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    mut stats: WriteStats,
) -> Result<(Tree, WriteStats), Error> {
    let mut writer = super::builder::SortedBatchBuilder::new(manager.store(), tree.config.clone());
    for (key, value) in mutations {
        if let Some(value) = value {
            writer.add(key, value)?;
            stats.entries_streamed += 1;
        }
    }
    let tree = writer.build()?;
    if let Some(root) = &tree.root {
        let node = manager.load_arc(root)?;
        manager.record_batch_write_metrics(1, node.encoded_len());
        stats.nodes_written = 1;
        stats.bytes_written = node.encoded_len() as u64;
        stats.resync_distance_nodes = 1;
    }
    Ok((tree, stats))
}

fn collect_leaf_summaries<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    stats: &mut WriteStats,
    measure_read_bytes: bool,
) -> Result<(Vec<NodeSummary>, HashSet<Cid>), Error> {
    let Some(root) = &tree.root else {
        return Ok((Vec::new(), HashSet::new()));
    };
    let mut leaves = Vec::new();
    let mut internals = HashSet::new();
    collect_from_node(
        manager,
        root,
        &tree.config.format,
        stats,
        &mut leaves,
        &mut internals,
        measure_read_bytes,
    )?;
    Ok((leaves, internals))
}

fn collect_from_node<S: Store>(
    manager: &Prolly<S>,
    cid: &Cid,
    expected_format: &super::format::TreeFormat,
    stats: &mut WriteStats,
    leaves: &mut Vec<NodeSummary>,
    internals: &mut HashSet<Cid>,
    measure_read_bytes: bool,
) -> Result<(), Error> {
    let node = manager.load_arc(cid)?;
    stats.nodes_read += 1;
    if measure_read_bytes {
        stats.bytes_read += node.encoded_len() as u64;
    }
    if node.format != *expected_format {
        return Err(Error::FormatMismatch {
            expected: expected_format.digest()?,
            actual: node.format.digest()?,
        });
    }
    if node.leaf {
        leaves.push(NodeSummary {
            cid: cid.clone(),
            first_key: node.keys.first().cloned().unwrap_or_default(),
            count: node.keys.len() as u64,
        });
        return Ok(());
    }
    internals.insert(cid.clone());
    if node.keys.len() != node.vals.len() {
        return Err(Error::InvalidNode);
    }

    if node.level == 1 {
        for index in 0..node.len() {
            let child = child_cid(&node.vals[index])?;
            let count = node.child_counts.get(index).copied().unwrap_or(0);
            let count = if count == 0 {
                let leaf = manager.load_arc(&child)?;
                stats.nodes_read += 1;
                if measure_read_bytes {
                    stats.bytes_read += leaf.encoded_len() as u64;
                }
                leaf.keys.len() as u64
            } else {
                count
            };
            leaves.push(NodeSummary {
                cid: child,
                first_key: node.keys[index].clone(),
                count,
            });
        }
        return Ok(());
    }

    for value in &node.vals {
        collect_from_node(
            manager,
            &child_cid(value)?,
            expected_format,
            stats,
            leaves,
            internals,
            measure_read_bytes,
        )?;
    }
    Ok(())
}

fn child_cid(bytes: &[u8]) -> Result<Cid, Error> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| Error::InvalidNode)?;
    Ok(Cid(bytes))
}
