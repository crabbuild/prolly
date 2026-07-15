//! Canonical half-open range deletion.

use std::collections::HashSet;

use super::builder::{BatchBuilder, NodeSummary, SortedBatchBuilder};
use super::canonical::{CanonicalWriteStats, LeafEmitter};
use super::cid::Cid;
use super::error::Error;
use super::format::NodeLayoutSpec;
use super::node::Node;
use super::store::Store;
use super::{Prolly, Tree};

const LOCAL_WRITE_CACHE_LIMIT: usize = 8;

pub(crate) fn apply<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    start: &[u8],
    end: &[u8],
) -> Result<(Tree, CanonicalWriteStats), Error> {
    if start >= end || tree.root.is_none() {
        return Ok((tree.clone(), CanonicalWriteStats::default()));
    }
    let metrics_before = manager.metrics();
    let reads_before = (metrics_before.nodes_read, metrics_before.bytes_read);
    if let Some(root) = &tree.root {
        let node = manager.load_arc(root)?;
        if node.format != tree.config.format {
            return Err(Error::FormatMismatch {
                expected: tree.config.format.digest()?,
                actual: node.format.digest()?,
            });
        }
    }
    if let Some(result) = try_localized_height_two(manager, tree, start, end)? {
        return Ok(result);
    }

    if manager
        .range(tree, start, Some(end))?
        .next()
        .transpose()?
        .is_none()
    {
        return Ok((tree.clone(), read_stats_since(manager, reads_before)));
    }

    let mut saw_deleted = false;
    let mut builder = SortedBatchBuilder::new(manager.store(), tree.config.clone());
    for entry in manager.range(tree, &[], None)? {
        let (key, value) = entry?;
        if key.as_slice() >= start && key.as_slice() < end {
            saw_deleted = true;
        } else {
            builder.add(key, value)?;
        }
    }
    debug_assert!(saw_deleted, "the existence probe found a key in the range");
    let written = builder.build()?;
    Ok((written, read_stats_since(manager, reads_before)))
}

pub(crate) fn apply_tree<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    start: &[u8],
    end: &[u8],
) -> Result<Tree, Error> {
    Ok(apply(manager, tree, start, end)?.0)
}

/// Attempt a height-2 splice whose canonical equivalence is proved by matching
/// both a recreated leaf and the final recreated internal node with unchanged
/// old content. Returning `None` delegates to the full streaming rebuild.
pub(crate) fn try_localized_height_two<S: Store>(
    manager: &Prolly<S>,
    tree: &Tree,
    start: &[u8],
    end: &[u8],
) -> Result<Option<(Tree, CanonicalWriteStats)>, Error> {
    if matches!(
        tree.config.format.node_layout,
        NodeLayoutSpec::Custom { .. }
    ) {
        return Ok(None);
    }
    let Some(root_cid) = &tree.root else {
        return Ok(None);
    };
    let mut stats = CanonicalWriteStats::default();
    let root = manager.load_arc(root_cid)?;
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
    let window_nodes = manager.load_many_ordered(&window_cids)?;
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
    let mut saw_deleted = false;
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
            saw_deleted = true;
            continue;
        }
        debug_assert!(wholly_before || !wholly_covered);

        let leaf = manager.load_arc(&summary.cid)?;
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
                saw_deleted = true;
            } else {
                emitter.push(key, value)?;
                stats.entries_streamed += 1;
            }
        }

        stats.resync_distance_nodes += 1;
        if saw_deleted && emitter.is_aligned_with(summary) {
            resynced_at = Some(index);
            break;
        }
    }

    if !saw_deleted {
        return Ok(Some((tree.clone(), stats)));
    }
    if resynced_at.is_none() && window_end < root.len() {
        return Ok(None);
    }
    emitter.flush();

    let old_cursor = resynced_at.map_or(old_leaves.len(), |index| index + 1);
    let mut replacement_leaves = Vec::with_capacity(old_leaves.len());
    replacement_leaves.extend_from_slice(&old_leaves[..replay_start]);
    replacement_leaves.extend(emitter.emitted.iter().map(|leaf| leaf.summary.clone()));
    replacement_leaves.extend_from_slice(&old_leaves[old_cursor..]);
    stats.nodes_reused += replay_start.saturating_add(old_leaves.len() - old_cursor) as u64;
    stats.resync_distance_entries = stats.entries_streamed;

    let builder = BatchBuilder::new(manager.store(), tree.config.clone());
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

    let new_root = if updated_root.len() == 1 {
        child_cid(&updated_root.vals[0])?
    } else {
        let bytes = updated_root.to_bytes();
        let cid = Cid::from_bytes(&bytes);
        if cid != *root_cid && written_cids.insert(cid.clone()) {
            writes.push((cid.clone(), bytes, updated_root));
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
            root: Some(new_root),
            config: tree.config.clone(),
        },
        stats,
    )))
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

fn read_stats_since<S: Store>(
    manager: &Prolly<S>,
    (nodes_before, bytes_before): (u64, u64),
) -> CanonicalWriteStats {
    let metrics = manager.metrics();
    CanonicalWriteStats {
        nodes_read: metrics.nodes_read.saturating_sub(nodes_before),
        bytes_read: metrics.bytes_read.saturating_sub(bytes_before),
        ..CanonicalWriteStats::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prolly::config::Config;
    use crate::prolly::format::NodeLayoutSpec;
    use crate::prolly::store::MemStore;

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
