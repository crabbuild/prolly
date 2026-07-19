//! Half-open range deletion.

use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use super::builder::{BatchBuilder, NodeSummary, SortedBatchBuilder};
use super::cid::Cid;
use super::error::Error;
use super::format::NodeLayoutSpec;
use super::node::Node;
use super::store::{BatchOp, Store};
use super::write::CanonicalWriteManager;
use super::write::{LeafEmitter, WriteStats};
use super::Tree;

const LOCAL_WRITE_CACHE_LIMIT: usize = 8;
type OwnedEntry = (Vec<u8>, Vec<u8>);

#[derive(Default)]
struct StreamingWriteCounter {
    nodes: AtomicUsize,
    bytes: AtomicUsize,
}

impl StreamingWriteCounter {
    fn record(&self, entries: &[(&[u8], &[u8])]) {
        self.nodes.fetch_add(entries.len(), Ordering::Relaxed);
        self.bytes.fetch_add(
            entries.iter().map(|(_, bytes)| bytes.len()).sum::<usize>(),
            Ordering::Relaxed,
        );
    }

    fn snapshot(&self) -> (usize, usize) {
        (
            self.nodes.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
        )
    }
}

struct CountingStore<'a, S> {
    inner: &'a S,
    writes: Arc<StreamingWriteCounter>,
}

impl<S> Clone for CountingStore<'_, S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            writes: Arc::clone(&self.writes),
        }
    }
}

impl<S: Store> Store for CountingStore<'_, S> {
    type Error = S::Error;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get(key)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.inner.put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key)
    }

    fn batch(&self, operations: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        self.inner.batch(operations)
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        self.inner.batch_put(entries)?;
        self.writes.record(entries);
        Ok(())
    }
}

pub(crate) fn apply<M: CanonicalWriteManager>(
    manager: &M,
    tree: &Tree,
    start: &[u8],
    end: &[u8],
) -> Result<(Tree, WriteStats), Error> {
    if start >= end || tree.root.is_none() {
        return Ok((tree.clone(), WriteStats::default()));
    }
    let metrics_before = manager.write_metrics();
    if let Some(root) = &tree.root {
        let node = manager.write_load_arc(root)?;
        if node.format != tree.config.format {
            return Err(Error::FormatMismatch {
                expected: tree.config.format.digest()?,
                actual: node.format.digest()?,
            });
        }
    }
    if let Some((tree, stats)) = try_localized_height_two(manager, tree, start, end)? {
        return Ok((
            tree,
            with_metric_stats_since(manager, metrics_before, stats),
        ));
    }

    if !range_has_entry(manager, tree, start, end)? {
        return Ok((tree.clone(), metric_stats_since(manager, metrics_before)));
    }

    let writes = Arc::new(StreamingWriteCounter::default());
    let mut builder = SortedBatchBuilder::new(
        CountingStore {
            inner: manager.write_store(),
            writes: Arc::clone(&writes),
        },
        tree.config.clone(),
    );
    let mut deleted_entries = 0u64;
    for (key, value) in collect_entries(manager, tree)? {
        if key.as_slice() >= start && key.as_slice() < end {
            deleted_entries += 1;
        } else {
            builder.add(key, value)?;
        }
    }
    debug_assert!(
        deleted_entries > 0,
        "the existence probe found a key in the range"
    );
    let written = builder.build()?;
    let (nodes_written, bytes_written) = writes.snapshot();
    manager.write_record_batch_metrics(nodes_written, bytes_written);
    Ok((
        written,
        with_metric_stats_since(
            manager,
            metrics_before,
            WriteStats {
                input_mutations: deleted_entries,
                effective_mutations: deleted_entries,
                ..WriteStats::default()
            },
        ),
    ))
}

/// Attempt a height-2 splice whose canonical equivalence is proved by matching
/// both a recreated leaf and the final recreated internal node with unchanged
/// old content. Returning `None` delegates to the full streaming rebuild.
pub(crate) fn try_localized_height_two<M: CanonicalWriteManager>(
    manager: &M,
    tree: &Tree,
    start: &[u8],
    end: &[u8],
) -> Result<Option<(Tree, WriteStats)>, Error> {
    if matches!(
        tree.config.format.node_layout,
        NodeLayoutSpec::Custom { .. }
    ) {
        return Ok(None);
    }
    let Some(root_cid) = &tree.root else {
        return Ok(None);
    };
    let metrics_before = manager.write_metrics();
    let mut stats = WriteStats::default();
    let root = manager.write_load_arc(root_cid)?;
    stats.nodes_read += 1;
    stats.bytes_read += root.encoded_len() as u64;
    if root.format != tree.config.format {
        return Err(Error::FormatMismatch {
            expected: tree.config.format.digest()?,
            actual: root.format.digest()?,
        });
    }
    if root.leaf || root.level != 2 || root.keys.is_empty() || root.validate().is_err() {
        return Ok(None);
    }

    let first_child = separator_floor(&root.keys, start);
    let last_child = separator_floor(&root.keys, end);
    let window_start = first_child.saturating_sub(1);
    let window_end = last_child.saturating_add(3).min(root.len());
    if window_start >= window_end {
        return Ok(None);
    }

    let window_cids = root.vals[window_start..window_end]
        .iter()
        .map(|value| child_cid(value))
        .collect::<Result<Vec<_>, _>>()?;
    let window_nodes = manager.write_load_many_ordered(&window_cids)?;
    let mut old_leaves = Vec::new();
    for (offset, node) in window_nodes.iter().enumerate() {
        stats.nodes_read += 1;
        stats.bytes_read += node.encoded_len() as u64;
        if node.leaf
            || node.level != 1
            || node.format != tree.config.format
            || node.validate().is_err()
        {
            return Ok(None);
        }
        let child_total = node
            .child_counts
            .iter()
            .try_fold(0u64, |total, count| total.checked_add(*count))
            .ok_or(Error::InvalidNode)?;
        if child_total != root.child_counts[window_start + offset] {
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
    if old_leaves.is_empty()
        || old_leaves
            .windows(2)
            .any(|pair| pair[0].first_key >= pair[1].first_key)
    {
        return Ok(None);
    }

    let replay_start = old_leaves
        .partition_point(|leaf| leaf.first_key.as_slice() <= start)
        // A hard-cap split can move into the preceding leaf after deletion.
        .saturating_sub(2);
    let mut emitter = LeafEmitter::new(&tree.config)?;
    let mut deleted_entries = 0u64;
    let mut resynced_at = None;

    for index in replay_start..old_leaves.len() {
        let summary = &old_leaves[index];
        let next_first = old_leaves.get(index + 1).map(|next| &next.first_key);
        let wholly_before = next_first.is_some_and(|next| next.as_slice() <= start);
        let wholly_covered = summary.first_key.as_slice() >= start
            && next_first.is_some_and(|next| next.as_slice() <= end);

        if wholly_covered {
            // `next_first` is required above, so this intentionally never
            // treats the global rightmost leaf as elidable.
            deleted_entries = deleted_entries.saturating_add(summary.count);
            continue;
        }
        debug_assert!(wholly_before || !wholly_covered);

        let leaf = manager.write_load_arc(&summary.cid)?;
        stats.nodes_read += 1;
        stats.bytes_read += leaf.encoded_len() as u64;
        if !leaf.leaf
            || leaf.level != 0
            || leaf.format != tree.config.format
            || leaf.validate().is_err()
        {
            return Err(Error::InvalidNode);
        }

        for (key, value) in leaf.keys.iter().cloned().zip(leaf.vals.iter().cloned()) {
            if key.as_slice() >= start && key.as_slice() < end {
                deleted_entries += 1;
            } else {
                emitter.push(key, value)?;
                stats.entries_streamed += 1;
            }
        }

        stats.resync_distance_nodes += 1;
        if deleted_entries > 0 && emitter.is_aligned_with(summary) {
            resynced_at = Some(index);
            break;
        }
    }

    if deleted_entries == 0 {
        return Ok(Some((
            tree.clone(),
            with_metric_stats_since(manager, metrics_before, stats),
        )));
    }
    if resynced_at.is_none() && window_end < root.len() {
        return Ok(None);
    }
    emitter.flush()?;

    let old_cursor = resynced_at.map_or(old_leaves.len(), |index| index + 1);
    let mut replacement_leaves = Vec::with_capacity(old_leaves.len());
    replacement_leaves.extend_from_slice(&old_leaves[..replay_start]);
    replacement_leaves.extend(emitter.emitted.iter().map(|leaf| leaf.summary.clone()));
    replacement_leaves.extend_from_slice(&old_leaves[old_cursor..]);
    stats.nodes_reused += replay_start.saturating_add(old_leaves.len() - old_cursor) as u64;
    stats.resync_distance_entries = stats.entries_streamed;
    stats.input_mutations = deleted_entries;
    stats.effective_mutations = deleted_entries;

    let builder = BatchBuilder::new(manager.write_store(), tree.config.clone());
    let (replacement_summaries, internal_nodes) =
        builder.build_level_serial_deferred(replacement_leaves, 1)?;
    if replacement_summaries.is_empty()
        || (window_end < root.len()
            && replacement_summaries.last().map(|summary| &summary.cid) != window_cids.last())
    {
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
    if updated_root.validate().is_err()
        || updated_root.len() > updated_root.max_chunk_size()
        || updated_root.encoded_len() as u64 > tree.config.format.chunking.hard_max_node_bytes
    {
        return Ok(None);
    }

    let (new_root, rebuilt_root) = if updated_root.len() == 1 {
        (child_cid(&updated_root.vals[0])?, None)
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
        (rebuilt_root.cid.clone(), Some(rebuilt_root))
    };

    let old_leaf_cids = old_leaves
        .iter()
        .map(|summary| summary.cid.clone())
        .collect::<HashSet<_>>();
    let old_internal_cids = window_cids.iter().cloned().collect::<HashSet<_>>();
    let mut written_cids = HashSet::new();
    let mut writes = Vec::<(Cid, Vec<u8>, Node)>::new();
    for leaf in emitter.emitted {
        if !old_leaf_cids.contains(&leaf.summary.cid)
            && written_cids.insert(leaf.summary.cid.clone())
        {
            writes.push((leaf.summary.cid, leaf.bytes, leaf.node));
        }
    }
    for node in internal_nodes {
        if !old_internal_cids.contains(&node.cid) && written_cids.insert(node.cid.clone()) {
            writes.push((node.cid, node.bytes, node.node));
        }
    }

    if let Some(root) = rebuilt_root {
        if root.cid != *root_cid && written_cids.insert(root.cid.clone()) {
            writes.push((root.cid, root.bytes, root.node));
        }
    }
    if !writes.is_empty() {
        let entries = writes
            .iter()
            .map(|(cid, bytes, _)| (cid.as_bytes(), bytes.as_slice()))
            .collect::<Vec<_>>();
        manager
            .write_store()
            .batch_put(&entries)
            .map_err(|error| Error::Store(Box::new(error)))?;
        let bytes_written = entries.iter().map(|(_, bytes)| bytes.len()).sum::<usize>();
        manager.write_record_batch_metrics(entries.len(), bytes_written);
        stats.nodes_written += entries.len() as u64;
        stats.bytes_written += bytes_written as u64;
        if entries.len() <= LOCAL_WRITE_CACHE_LIMIT {
            drop(entries);
            for (cid, _, node) in writes {
                manager.write_cache_node(cid, node);
            }
        }
    }

    Ok(Some((
        Tree {
            root: Some(new_root),
            config: tree.config.clone(),
        },
        with_metric_stats_since(manager, metrics_before, stats),
    )))
}

fn range_has_entry<M: CanonicalWriteManager>(
    manager: &M,
    tree: &Tree,
    start: &[u8],
    end: &[u8],
) -> Result<bool, Error> {
    let Some(root) = &tree.root else {
        return Ok(false);
    };
    let mut cid = root.clone();
    let mut ancestors = Vec::<(Arc<Node>, usize)>::new();
    loop {
        let node = manager.write_load_arc(&cid)?;
        if node.is_empty() {
            return Err(Error::InvalidNode);
        }
        if node.leaf {
            let position = node.keys.partition_point(|key| key.as_slice() < start);
            if let Some(key) = node.keys.get(position) {
                return Ok(key.as_slice() < end);
            }
            break;
        }
        let child_index = separator_floor(&node.keys, start);
        cid = child_cid(node.vals.get(child_index).ok_or(Error::InvalidNode)?)?;
        ancestors.push((node, child_index));
    }

    while let Some((ancestor, child_index)) = ancestors.pop() {
        if child_index + 1 >= ancestor.len() {
            continue;
        }
        cid = child_cid(
            ancestor
                .vals
                .get(child_index + 1)
                .ok_or(Error::InvalidNode)?,
        )?;
        loop {
            let node = manager.write_load_arc(&cid)?;
            if node.is_empty() {
                return Err(Error::InvalidNode);
            }
            if node.leaf {
                return Ok(node.keys.first().is_some_and(|key| key.as_slice() < end));
            }
            cid = child_cid(node.vals.first().ok_or(Error::InvalidNode)?)?;
        }
    }
    Ok(false)
}

fn collect_entries<M: CanonicalWriteManager>(
    manager: &M,
    tree: &Tree,
) -> Result<Vec<OwnedEntry>, Error> {
    let Some(root) = &tree.root else {
        return Ok(Vec::new());
    };
    let mut entries = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(cid) = stack.pop() {
        let node = manager.write_load_arc(&cid)?;
        if node.leaf {
            if node.keys.len() != node.vals.len() {
                return Err(Error::InvalidNode);
            }
            entries.extend(node.keys.iter().cloned().zip(node.vals.iter().cloned()));
        } else {
            for value in node.vals.iter().rev() {
                stack.push(child_cid(value)?);
            }
        }
    }
    Ok(entries)
}

fn separator_floor(separators: &[Vec<u8>], key: &[u8]) -> usize {
    separators
        .partition_point(|separator| separator.as_slice() <= key)
        .saturating_sub(1)
}

fn child_cid(bytes: &[u8]) -> Result<Cid, Error> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| Error::InvalidNode)?;
    Ok(Cid(bytes))
}

fn metric_stats_since<M: CanonicalWriteManager>(
    manager: &M,
    metrics_before: super::ProllyMetricsSnapshot,
) -> WriteStats {
    let metrics = manager.write_metrics();
    WriteStats {
        nodes_read: metrics.nodes_read.saturating_sub(metrics_before.nodes_read),
        bytes_read: metrics.bytes_read.saturating_sub(metrics_before.bytes_read),
        nodes_written: metrics
            .nodes_written
            .saturating_sub(metrics_before.nodes_written),
        bytes_written: metrics
            .bytes_written
            .saturating_sub(metrics_before.bytes_written),
        ..WriteStats::default()
    }
}

fn with_metric_stats_since<M: CanonicalWriteManager>(
    manager: &M,
    metrics_before: super::ProllyMetricsSnapshot,
    mut stats: WriteStats,
) -> WriteStats {
    let actual_metrics = metric_stats_since(manager, metrics_before);
    stats.nodes_read = actual_metrics.nodes_read;
    stats.bytes_read = actual_metrics.bytes_read;
    stats.nodes_written = actual_metrics.nodes_written;
    stats.bytes_written = actual_metrics.bytes_written;
    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prolly::config::Config;
    use crate::prolly::format::NodeLayoutSpec;
    use crate::prolly::store::MemStore;
    use crate::prolly::Prolly;

    #[test]
    fn custom_layout_is_not_eligible_for_the_height_two_splice() {
        let config = Config::builder()
            .node_layout(NodeLayoutSpec::Custom {
                id: "test-layout".to_owned(),
                parameters: vec![],
            })
            .build();
        let manager = Prolly::new(MemStore::new(), config.clone());
        let tree = Tree {
            root: Some(Cid::from_bytes(b"custom-layout-root-is-never-loaded")),
            config,
        };

        assert!(try_localized_height_two(&manager, &tree, b"a", b"b")
            .unwrap()
            .is_none());
    }
}
