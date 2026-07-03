//! Tree rebalancing operations for Prolly trees
//!
//! This module handles all tree rebalancing logic including node splitting,
//! merging, and propagating changes up to the root. Rebalancing ensures that
//! all nodes maintain size constraints (between min_chunk_size and max_chunk_size).
//!
//! # Overview
//!
//! Prolly trees use a probabilistic balancing strategy based on content-defined
//! chunking. Unlike traditional B-trees that split at fixed sizes, Prolly trees
//! use hash-based boundary detection to determine split points. This approach
//! ensures that the tree structure is deterministic for the same content.
//!
//! # Rebalancing Strategy
//!
//! The rebalancing process handles three main scenarios:
//!
//! ## Node Splitting
//!
//! When a node exceeds `max_chunk_size` entries, it must be split:
//!
//! 1. Find a split point using boundary detection (hash-based)
//! 2. Create two new nodes (left and right) from the split
//! 3. Update or create a parent node to reference both children
//! 4. Recursively rebalance the parent if needed
//!
//! ## Node Merging
//!
//! When a node falls below `min_chunk_size` entries, it may be merged:
//!
//! 1. Check if the boundary with adjacent siblings is still valid
//! 2. If not valid, merge with the sibling
//! 3. The merged node may then need splitting if too large
//!
//! ## Empty Node Handling
//!
//! When a node becomes empty (all entries deleted):
//!
//! 1. Remove the empty node from its parent
//! 2. Continue rebalancing the parent
//! 3. If the root becomes empty, the tree becomes empty
//!
//! # Boundary Detection
//!
//! Split points are determined by the `is_boundary` function which uses
//! a hash of the entry to decide if it should be a chunk boundary. This
//! ensures that:
//!
//! - The same content always produces the same tree structure
//! - Small changes only affect nearby nodes (structural sharing)
//! - Average node size is controlled by the chunking factor
//!
//! # Batch Rebalancing
//!
//! For batch operations, this module provides `rebalance_with_collector`
//! which collects all modified nodes for atomic batch writing instead of
//! writing each node immediately. This improves performance and ensures
//! atomicity of batch mutations.

use super::batch::BatchWriteCollector;
use super::boundary::is_boundary;
use super::cid::Cid;
use super::error::Error;
use super::node::Node;
use super::store::Store;
use super::Prolly;

fn reserve_node_entries(node: &mut Node, additional: usize) {
    node.keys.reserve_exact(additional);
    node.vals.reserve_exact(additional);
}

/// Rebalance the tree after modification.
///
/// Handles:
/// - Node splitting when nodes exceed max_chunk_size
/// - Merging adjacent nodes when boundary is no longer valid
/// - Creating new parent when splitting root
/// - Removing empty nodes from parent
/// - Propagating changes up to root
///
/// # Arguments
/// * `prolly` - Reference to the Prolly tree manager
/// * `node` - The modified node to rebalance
/// * `ancestors` - Path from root to the node's parent
///
/// # Returns
/// * `Ok(cid)` - CID of the new root
/// * `Err(Error)` - On storage or processing errors
pub fn rebalance<S: Store>(
    prolly: &Prolly<S>,
    node: Node,
    ancestors: &[(Node, usize)],
) -> Result<Cid, Error> {
    // Handle empty node case
    if node.is_empty() {
        return handle_empty_node(prolly, ancestors);
    }

    // Check for splits based on entry count. `max_chunk_size` is an
    // inclusive capacity; split only after the node exceeds it.
    if node.len() > node.max_chunk_size && node.len() > 1 {
        return split_node(prolly, node, ancestors);
    }

    // Check if we should merge with siblings
    // This happens when a node is below min_chunk_size and can be merged
    if !ancestors.is_empty() && node.len() < node.min_chunk_size {
        if let Some(merged) = try_merge_with_sibling(prolly, &node, ancestors)? {
            return Ok(merged);
        }
    }

    // No split or merge needed, propagate up
    let cid = prolly.save(&node)?;

    if ancestors.is_empty() {
        return Ok(cid);
    }

    let (mut parent, idx) = ancestors.last().unwrap().clone();

    // Update parent's key if the first key of this node changed
    if !node.keys.is_empty() {
        parent.keys[idx] = node.keys[0].clone();
    }
    parent.vals[idx] = cid.0.to_vec();

    rebalance(prolly, parent, &ancestors[..ancestors.len() - 1])
}

/// Handle the case when a node becomes empty.
///
/// Removes the empty node from its parent and continues rebalancing upward.
///
/// # Arguments
/// * `prolly` - Reference to the Prolly tree manager
/// * `ancestors` - Path from root to the empty node's parent
///
/// # Returns
/// * `Ok(cid)` - CID of the new root
/// * `Err(Error)` - On storage or processing errors
fn handle_empty_node<S: Store>(
    prolly: &Prolly<S>,
    ancestors: &[(Node, usize)],
) -> Result<Cid, Error> {
    if ancestors.is_empty() {
        // Empty root - this shouldn't happen in normal operation
        // Return a placeholder CID for an empty node
        let empty_node = prolly.new_leaf_node();
        return prolly.save(&empty_node);
    }

    let (mut parent, idx) = ancestors.last().unwrap().clone();

    // Remove the empty child from parent
    parent.keys.remove(idx);
    parent.vals.remove(idx);

    // If parent is now empty and it's the root, tree becomes empty
    if parent.is_empty() && ancestors.len() == 1 {
        let empty_node = prolly.new_leaf_node();
        return prolly.save(&empty_node);
    }

    // Continue rebalancing with the modified parent
    rebalance(prolly, parent, &ancestors[..ancestors.len() - 1])
}

/// Split a node that exceeds max_chunk_size.
///
/// Finds the split point using boundary detection, creates left/right nodes,
/// and updates or creates the parent node.
///
/// # Arguments
/// * `prolly` - Reference to the Prolly tree manager
/// * `node` - The node to split
/// * `ancestors` - Path from root to the node's parent
///
/// # Returns
/// * `Ok(cid)` - CID of the new root
/// * `Err(Error)` - On storage or processing errors
fn split_node<S: Store>(
    prolly: &Prolly<S>,
    node: Node,
    ancestors: &[(Node, usize)],
) -> Result<Cid, Error> {
    let max_size = node.max_chunk_size;

    // Find split point using boundary detection
    // We need to find a split point that ensures both halves fit within max_chunk_size.
    let mut split_idx = None;
    for i in 0..node.len() {
        if is_boundary(&node, i) {
            // Check if this split point would create valid-sized nodes
            // Both halves must be at or below max_chunk_size.
            let left_size = i + 1;
            let right_size = node.len() - i - 1;
            if left_size <= max_size && right_size > 0 && right_size <= max_size {
                split_idx = Some(i);
                break;
            }
        }
    }

    // If no valid boundary found, split to ensure both halves fit within max_chunk_size.
    let split_idx = split_idx.unwrap_or_else(|| {
        // Split at a point that keeps both sides within max_chunk_size.
        // For a node of size N, we want:
        // left_size = split_idx + 1 <= max_size
        // right_size = N - split_idx - 1 <= max_size

        // Calculate the valid range for split_idx
        let min_split = node.len().saturating_sub(max_size + 1);
        let max_split = max_size.saturating_sub(1).min(node.len().saturating_sub(2));

        // Choose a split point in the valid range, preferring the middle
        if min_split <= max_split {
            (min_split + max_split) / 2
        } else {
            // If no valid range exists (shouldn't happen with reasonable sizes),
            // split in the middle
            node.len() / 2
        }
    });

    // Ensure we don't create empty nodes
    let split_idx = split_idx.min(node.len().saturating_sub(2)).max(0);

    // Split node into left and right
    let mut left = prolly.new_node_like(&node);
    left.keys = node.keys[..=split_idx].to_vec();
    left.vals = node.vals[..=split_idx].to_vec();

    let mut right = prolly.new_node_like(&node);
    right.keys = node.keys[split_idx + 1..].to_vec();
    right.vals = node.vals[split_idx + 1..].to_vec();

    // Handle case where right would be empty
    if right.is_empty() {
        // Can't split, just save the node as-is
        let cid = prolly.save(&node)?;
        if ancestors.is_empty() {
            return Ok(cid);
        }
        let (mut parent, idx) = ancestors.last().unwrap().clone();
        parent.vals[idx] = cid.0.to_vec();
        return rebalance(prolly, parent, &ancestors[..ancestors.len() - 1]);
    }

    // Recursively split if either half is still too large
    // This handles cases where max_chunk_size is very small
    let left_cid = if left.len() > max_size && left.len() > 1 {
        // Left is still too large, need to split it further
        // We'll handle this by creating a temporary parent structure
        split_and_save_oversized(prolly, &left, ancestors)?
    } else {
        prolly.save(&left)?
    };

    let right_cid = if right.len() > max_size && right.len() > 1 {
        // Right is still too large, need to split it further
        split_and_save_oversized(prolly, &right, ancestors)?
    } else {
        prolly.save(&right)?
    };

    // Create or update parent
    if ancestors.is_empty() {
        // Create new root
        let mut parent = prolly.new_internal_node(node.level + 1);
        reserve_node_entries(&mut parent, 2);
        parent.keys.push(left.keys[0].clone());
        parent.vals.push(left_cid.0.to_vec());
        parent.keys.push(right.keys[0].clone());
        parent.vals.push(right_cid.0.to_vec());

        // Check if parent needs splitting too
        if parent.len() > parent.max_chunk_size {
            return split_node(prolly, parent, &[]);
        }
        return prolly.save(&parent);
    }

    // Update existing parent
    let (mut parent, idx) = ancestors.last().unwrap().clone();

    // Update the existing entry to point to left node
    parent.keys[idx] = left.keys[0].clone();
    parent.vals[idx] = left_cid.0.to_vec();

    // Insert new entry for right node
    reserve_node_entries(&mut parent, 1);
    parent.keys.insert(idx + 1, right.keys[0].clone());
    parent.vals.insert(idx + 1, right_cid.0.to_vec());

    // Continue rebalancing with the modified parent
    // This will trigger another split if parent is now too large
    rebalance(prolly, parent, &ancestors[..ancestors.len() - 1])
}

/// Split an oversized node and return the CID of a properly sized result.
/// This is used when a split produces halves that are still too large.
///
/// # Arguments
/// * `prolly` - Reference to the Prolly tree manager
/// * `node` - The oversized node to split
/// * `_ancestors` - Path from root (unused but kept for consistency)
///
/// # Returns
/// * `Ok(cid)` - CID of the resulting node or parent
/// * `Err(Error)` - On storage or processing errors
fn split_and_save_oversized<S: Store>(
    prolly: &Prolly<S>,
    node: &Node,
    _ancestors: &[(Node, usize)],
) -> Result<Cid, Error> {
    let max_size = node.max_chunk_size;

    // If node is small enough, just save it
    if node.len() <= max_size {
        return prolly.save(node);
    }

    // Need to split this node into multiple smaller nodes
    // and create a parent to hold them
    let capacity = max_size.max(1);
    let mut chunks: Vec<Node> = Vec::with_capacity(node.len().div_ceil(capacity));
    let mut start = 0;

    while start < node.len() {
        // Calculate end index for this chunk. Chunks may fill max_size exactly.
        let chunk_size = capacity.min(node.len() - start);
        let end = start + chunk_size;

        let mut chunk = prolly.new_node_like(node);
        chunk.keys = node.keys[start..end].to_vec();
        chunk.vals = node.vals[start..end].to_vec();
        chunks.push(chunk);

        start = end;
    }

    // If we only have one chunk, just save it
    if chunks.len() == 1 {
        return prolly.save(&chunks[0]);
    }

    // Create a parent node to hold all chunks
    let mut parent = prolly.new_internal_node(node.level + 1);
    reserve_node_entries(&mut parent, chunks.len());
    for chunk in &chunks {
        let chunk_cid = prolly.save(chunk)?;
        parent.keys.push(chunk.keys[0].clone());
        parent.vals.push(chunk_cid.0.to_vec());
    }

    // If parent is also too large, recursively split it
    if parent.len() > max_size && parent.len() > 1 {
        return split_and_save_oversized(prolly, &parent, &[]);
    }

    prolly.save(&parent)
}

/// Try to merge a small node with one of its siblings.
///
/// Returns Some(cid) if merge was performed, None if merge not possible.
///
/// # Arguments
/// * `prolly` - Reference to the Prolly tree manager
/// * `node` - The small node to potentially merge
/// * `ancestors` - Path from root to the node's parent
///
/// # Returns
/// * `Ok(Some(cid))` - CID of new root after merge
/// * `Ok(None)` - No merge was possible
/// * `Err(Error)` - On storage or processing errors
fn try_merge_with_sibling<S: Store>(
    prolly: &Prolly<S>,
    node: &Node,
    ancestors: &[(Node, usize)],
) -> Result<Option<Cid>, Error> {
    let (parent, idx) = ancestors.last().unwrap();
    let idx = *idx; // Dereference to get owned usize

    // Try to merge with left sibling
    if idx > 0 {
        let left_cid = Cid(parent.vals[idx - 1]
            .as_slice()
            .try_into()
            .map_err(|_| Error::InvalidNode)?);
        let left_sibling = prolly.load(&left_cid)?;

        // Check if boundary between left sibling and this node is no longer valid
        if !is_valid_boundary_between(prolly, &left_sibling, node) {
            // Merge: combine left sibling and current node
            let merged = merge_nodes(prolly, &left_sibling, node)?;

            // Update parent: remove left sibling entry, update current entry
            let mut new_parent = parent.clone();
            new_parent.keys.remove(idx - 1);
            new_parent.vals.remove(idx - 1);

            // The current entry is now at idx - 1
            let new_idx = idx - 1;

            // Check if merged node needs splitting (might be too large after merge)
            if merged.len() > merged.max_chunk_size && merged.len() > 1 {
                // Need to split the merged node - build new ancestors with updated parent position
                let mut new_ancestors: Vec<(Node, usize)> =
                    ancestors[..ancestors.len() - 1].to_vec();
                new_ancestors.push((new_parent, new_idx));
                return Ok(Some(split_node(prolly, merged, &new_ancestors)?));
            }

            // Save merged node and update parent
            let merged_cid = prolly.save(&merged)?;
            new_parent.keys[new_idx] = merged.keys[0].clone();
            new_parent.vals[new_idx] = merged_cid.0.to_vec();

            // Continue rebalancing with the updated parent
            return Ok(Some(rebalance(
                prolly,
                new_parent,
                &ancestors[..ancestors.len() - 1],
            )?));
        }
    }

    // Try to merge with right sibling
    if idx + 1 < parent.vals.len() {
        let right_cid = Cid(parent.vals[idx + 1]
            .as_slice()
            .try_into()
            .map_err(|_| Error::InvalidNode)?);
        let right_sibling = prolly.load(&right_cid)?;

        // Check if boundary between this node and right sibling is no longer valid
        if !is_valid_boundary_between(prolly, node, &right_sibling) {
            // Merge: combine current node and right sibling
            let merged = merge_nodes(prolly, node, &right_sibling)?;

            // Update parent: remove right sibling entry
            let mut new_parent = parent.clone();
            new_parent.keys.remove(idx + 1);
            new_parent.vals.remove(idx + 1);

            // Check if merged node needs splitting (might be too large after merge)
            if merged.len() > merged.max_chunk_size && merged.len() > 1 {
                // Need to split the merged node - build new ancestors with updated parent position
                let mut new_ancestors: Vec<(Node, usize)> =
                    ancestors[..ancestors.len() - 1].to_vec();
                new_ancestors.push((new_parent, idx));
                return Ok(Some(split_node(prolly, merged, &new_ancestors)?));
            }

            // Save merged node and update parent
            let merged_cid = prolly.save(&merged)?;
            new_parent.keys[idx] = merged.keys[0].clone();
            new_parent.vals[idx] = merged_cid.0.to_vec();

            // Continue rebalancing with the updated parent
            return Ok(Some(rebalance(
                prolly,
                new_parent,
                &ancestors[..ancestors.len() - 1],
            )?));
        }
    }

    // No merge possible
    Ok(None)
}

/// Check if the boundary between two adjacent nodes is still valid.
///
/// A boundary is valid if the last entry of the left node would trigger
/// a boundary according to the chunking algorithm.
///
/// # Arguments
/// * `_prolly` - Reference to the Prolly tree manager (unused but kept for consistency)
/// * `left` - The left node
/// * `_right` - The right node (unused but kept for consistency)
///
/// # Returns
/// `true` if the boundary is valid, `false` otherwise
fn is_valid_boundary_between<S: Store>(_prolly: &Prolly<S>, left: &Node, _right: &Node) -> bool {
    if left.is_empty() {
        return false;
    }

    // Check if the last entry of the left node is a valid boundary
    let last_idx = left.len() - 1;
    is_boundary(left, last_idx)
}

/// Merge two adjacent nodes into one.
///
/// # Arguments
/// * `prolly` - Reference to the Prolly tree manager
/// * `left` - The left node
/// * `right` - The right node
///
/// # Returns
/// * `Ok(merged_node)` - The merged node
/// * `Err(Error)` - On processing errors
fn merge_nodes<S: Store>(prolly: &Prolly<S>, left: &Node, right: &Node) -> Result<Node, Error> {
    let mut merged = prolly.new_node_like(left);
    let merged_len = left.len() + right.len();

    // Combine keys and values from both nodes
    merged.keys = Vec::with_capacity(merged_len);
    merged.keys.extend(left.keys.iter().cloned());
    merged.keys.extend(right.keys.iter().cloned());

    merged.vals = Vec::with_capacity(merged_len);
    merged.vals.extend(left.vals.iter().cloned());
    merged.vals.extend(right.vals.iter().cloned());

    Ok(merged)
}

/// Rebalance the tree after modification, collecting nodes for batch write.
///
/// Similar to `rebalance` but uses a BatchWriteCollector instead of
/// writing directly to the store. This enables atomic batch writes.
///
/// # Arguments
/// * `prolly` - Reference to the Prolly tree manager
/// * `node` - The modified node to rebalance
/// * `ancestors` - Path from root to the node's parent
/// * `collector` - Collector for nodes to be written
///
/// # Returns
/// * `Ok(Some(cid))` - CID of the new root
/// * `Ok(None)` - Tree becomes empty
/// * `Err(Error)` - On processing errors
pub fn rebalance_with_collector<S: Store>(
    prolly: &Prolly<S>,
    node: Node,
    ancestors: &[(Node, usize)],
    collector: &mut BatchWriteCollector,
) -> Result<Option<Cid>, Error> {
    // Handle empty node
    if node.is_empty() {
        if ancestors.is_empty() {
            return Ok(None); // Tree becomes empty
        }
        // Remove from parent and continue rebalancing
        return handle_empty_node_with_collector(prolly, ancestors, collector);
    }

    // Check for splits based on entry count.
    if node.len() > node.max_chunk_size && node.len() > 1 {
        return split_node_with_collector(prolly, node, ancestors, collector);
    }

    // Check if we should merge with siblings
    if !ancestors.is_empty() && node.len() < node.min_chunk_size {
        if let Some(merged_cid) =
            try_merge_with_sibling_collector(prolly, &node, ancestors, collector)?
        {
            return Ok(Some(merged_cid));
        }
    }

    // No split or merge needed - save and propagate up
    let cid = collector.add(&node);

    if ancestors.is_empty() {
        return Ok(Some(cid));
    }

    // Update parent and continue
    let (mut parent, idx) = ancestors.last().unwrap().clone();
    if !node.keys.is_empty() {
        parent.keys[idx] = node.keys[0].clone();
    }
    parent.vals[idx] = cid.0.to_vec();

    rebalance_with_collector(prolly, parent, &ancestors[..ancestors.len() - 1], collector)
}

/// Handle empty node during batch rebalancing.
///
/// Removes the empty node from its parent and continues rebalancing.
///
/// # Arguments
/// * `prolly` - Reference to the Prolly tree manager
/// * `ancestors` - Path from root to the empty node's parent
/// * `collector` - Collector for nodes to be written
///
/// # Returns
/// * `Ok(Some(cid))` - CID of the new root
/// * `Ok(None)` - Tree becomes empty
/// * `Err(Error)` - On processing errors
fn handle_empty_node_with_collector<S: Store>(
    prolly: &Prolly<S>,
    ancestors: &[(Node, usize)],
    collector: &mut BatchWriteCollector,
) -> Result<Option<Cid>, Error> {
    if ancestors.is_empty() {
        return Ok(None);
    }

    let (mut parent, idx) = ancestors.last().unwrap().clone();

    // Remove the empty child from parent
    parent.keys.remove(idx);
    parent.vals.remove(idx);

    // If parent is now empty and it's the root, tree becomes empty
    if parent.is_empty() && ancestors.len() == 1 {
        return Ok(None);
    }

    // Continue rebalancing with the modified parent
    rebalance_with_collector(prolly, parent, &ancestors[..ancestors.len() - 1], collector)
}

/// Split a node during batch rebalancing.
///
/// Finds the split point using boundary detection, creates left/right nodes,
/// and updates or creates the parent node.
///
/// # Arguments
/// * `prolly` - Reference to the Prolly tree manager
/// * `node` - The node to split
/// * `ancestors` - Path from root to the node's parent
/// * `collector` - Collector for nodes to be written
///
/// # Returns
/// * `Ok(Some(cid))` - CID of the new root
/// * `Ok(None)` - Should not happen in normal operation
/// * `Err(Error)` - On processing errors
fn split_node_with_collector<S: Store>(
    prolly: &Prolly<S>,
    node: Node,
    ancestors: &[(Node, usize)],
    collector: &mut BatchWriteCollector,
) -> Result<Option<Cid>, Error> {
    let max_size = node.max_chunk_size;

    // Split the node into multiple chunks that are all at or under max_size.
    let chunks = split_into_chunks(prolly, &node, max_size);

    // If only one chunk (shouldn't happen but handle gracefully)
    if chunks.len() == 1 {
        let cid = collector.add(&chunks[0]);
        if ancestors.is_empty() {
            return Ok(Some(cid));
        }
        let (mut parent, idx) = ancestors.last().unwrap().clone();
        if !chunks[0].keys.is_empty() {
            parent.keys[idx] = chunks[0].keys[0].clone();
        }
        parent.vals[idx] = cid.0.to_vec();
        return rebalance_with_collector(
            prolly,
            parent,
            &ancestors[..ancestors.len() - 1],
            collector,
        );
    }

    // Save all chunks and collect their CIDs and first keys. Use the bulk
    // collector path so large splits can parallelize serialization and CID
    // computation before the atomic store write.
    let first_keys = chunks
        .iter()
        .map(|chunk| chunk.keys.first().cloned().unwrap_or_default())
        .collect::<Vec<_>>();
    let chunk_info: Vec<(Cid, Vec<u8>)> = collector
        .add_many(chunks)
        .into_iter()
        .zip(first_keys)
        .collect();

    // Debug assertion: verify chunk keys don't overlap with siblings (Requirement 2.3, 3.3)
    // Check that chunk first keys are in ascending order
    debug_assert!(
        chunk_info.windows(2).all(|w| w[0].1 < w[1].1),
        "split_node_with_collector: chunk first keys must be in strictly ascending order"
    );

    // Create or update parent
    if ancestors.is_empty() {
        // Create new root
        let mut parent = prolly.new_internal_node(node.level + 1);
        reserve_node_entries(&mut parent, chunk_info.len());
        for (cid, first_key) in &chunk_info {
            parent.keys.push(first_key.clone());
            parent.vals.push(cid.0.to_vec());
        }

        // Check if parent needs splitting too
        if parent.len() > parent.max_chunk_size {
            return split_node_with_collector(prolly, parent, &[], collector);
        }
        let root_cid = collector.add(&parent);
        return Ok(Some(root_cid));
    }

    // Update existing parent
    let (mut parent, idx) = ancestors.last().unwrap().clone();

    // Remove the old entry at idx
    parent.keys.remove(idx);
    parent.vals.remove(idx);

    // Insert all new entries at idx
    let additional_entries = chunk_info.len().saturating_sub(1);
    reserve_node_entries(&mut parent, additional_entries);
    for (i, (cid, first_key)) in chunk_info.iter().enumerate() {
        parent.keys.insert(idx + i, first_key.clone());
        parent.vals.insert(idx + i, cid.0.to_vec());
    }

    // Debug assertion: verify chunk keys don't overlap with siblings (Requirement 2.3, 3.3)
    // After inserting chunks, the parent's keys must remain sorted
    debug_assert!(
        parent.keys.windows(2).all(|w| w[0] < w[1]),
        "split_node_with_collector: parent keys must remain sorted after inserting chunks, \
         indicating chunk keys don't overlap with siblings"
    );

    // Continue rebalancing with the modified parent
    rebalance_with_collector(prolly, parent, &ancestors[..ancestors.len() - 1], collector)
}

/// Split a node into multiple chunks, each at or under max_size.
///
/// This function is exposed for testing purposes.
///
/// # Arguments
/// * `prolly` - Reference to the Prolly tree manager
/// * `node` - The node to split into chunks
/// * `max_size` - The inclusive maximum chunk size
///
/// # Returns
/// * `Vec<Node>` - A vector of chunks, each with length at most `max_size`
///
/// # Invariants
/// - All returned chunks have `chunk.len() <= max_size`
/// - All returned chunks have `chunk.len() > 0` (non-empty)
/// - The concatenation of all chunks equals the original node's entries
pub fn split_into_chunks<S: Store>(prolly: &Prolly<S>, node: &Node, max_size: usize) -> Vec<Node> {
    // Debug assertion: verify input node keys are sorted (Requirement 2.1, 2.2)
    debug_assert!(
        node.keys.windows(2).all(|w| w[0] < w[1]),
        "split_into_chunks: input node keys must be in strictly ascending order"
    );

    let capacity = max_size.max(1);

    if node.len() <= capacity {
        return vec![node.clone()];
    }

    // Calculate the number of chunks needed
    let num_chunks = node.len().div_ceil(capacity);

    let mut chunks: Vec<Node> = Vec::with_capacity(num_chunks);
    let mut start = 0;

    while start < node.len() {
        // Calculate target end for this chunk
        let remaining_chunks = num_chunks - chunks.len();
        let remaining_entries = node.len() - start;

        // Safety check: if remaining_chunks is 0, we've created all expected chunks
        // but still have entries left. This can happen due to rounding in num_chunks calculation.
        // In this case, we need to create additional chunks.
        let target_end = if remaining_chunks == 0 {
            // Create a chunk with remaining entries, respecting capacity.
            (start + remaining_entries).min(start + capacity)
        } else {
            start + (remaining_entries / remaining_chunks).max(1)
        };

        // Ensure we don't exceed capacity.
        // A chunk from start..end has size (end - start), so we want
        // end - start <= capacity.
        let max_end = (start + capacity).min(node.len());
        let min_end = start + 1;

        // Start with target_end, but clamp to valid range
        let mut end = target_end.min(max_end).max(min_end);

        // Look for a boundary point near the target
        let search_start = (target_end.saturating_sub(50)).max(min_end);
        let search_end = (target_end + 50).min(max_end);

        for i in (search_start..=search_end).rev() {
            if i <= max_end && i < node.len() && is_boundary(node, i - 1) {
                end = i;
                break;
            }
        }

        // Ensure we don't create oversized chunks.
        // Chunk size is end - start, must be <= capacity.
        if end - start > capacity {
            end = start + capacity;
        }

        // Ensure we don't leave a tiny remainder
        let remaining_after = node.len() - end;
        if remaining_after > 0
            && remaining_after < capacity / 4
            && (end - start) + remaining_after <= capacity
        {
            // Include the remainder in this chunk
            end = node.len();
        }

        // Final safety check: ensure chunk size is at or below capacity.
        if end - start > capacity {
            end = start + capacity;
        }

        let mut chunk = prolly.new_node_like(node);
        chunk.keys = node.keys[start..end].to_vec();
        chunk.vals = node.vals[start..end].to_vec();

        chunks.push(chunk);

        start = end;
    }

    chunks
}

/// Try to merge a small node with one of its siblings using collector.
///
/// # Arguments
/// * `prolly` - Reference to the Prolly tree manager
/// * `node` - The small node to potentially merge
/// * `ancestors` - Path from root to the node's parent
/// * `collector` - Collector for nodes to be written
///
/// # Returns
/// * `Ok(Some(cid))` - CID of new root after merge
/// * `Ok(None)` - No merge was possible
/// * `Err(Error)` - On storage or processing errors
fn try_merge_with_sibling_collector<S: Store>(
    prolly: &Prolly<S>,
    node: &Node,
    ancestors: &[(Node, usize)],
    collector: &mut BatchWriteCollector,
) -> Result<Option<Cid>, Error> {
    let (parent, idx) = ancestors.last().unwrap();
    let idx = *idx;

    // Try to merge with left sibling
    if idx > 0 {
        let left_cid = Cid(parent.vals[idx - 1]
            .as_slice()
            .try_into()
            .map_err(|_| Error::InvalidNode)?);
        let left_sibling = prolly.load(&left_cid)?;

        if !is_valid_boundary_between(prolly, &left_sibling, node) {
            let merged = merge_nodes(prolly, &left_sibling, node)?;

            let mut new_parent = parent.clone();
            new_parent.keys.remove(idx - 1);
            new_parent.vals.remove(idx - 1);

            let new_idx = idx - 1;

            // Check if merged node needs splitting (might be too large after merge)
            if merged.len() > merged.max_chunk_size && merged.len() > 1 {
                // Need to split the merged node - build new ancestors with updated parent position
                let mut new_ancestors: Vec<(Node, usize)> =
                    ancestors[..ancestors.len() - 1].to_vec();
                new_ancestors.push((new_parent, new_idx));
                return split_node_with_collector(prolly, merged, &new_ancestors, collector);
            }

            // Save merged node and update parent
            let merged_cid = collector.add(&merged);
            new_parent.keys[new_idx] = merged.keys[0].clone();
            new_parent.vals[new_idx] = merged_cid.0.to_vec();

            // Continue rebalancing with the updated parent
            return Ok(Some(
                rebalance_with_collector(
                    prolly,
                    new_parent,
                    &ancestors[..ancestors.len() - 1],
                    collector,
                )?
                .unwrap_or(merged_cid),
            ));
        }
    }

    // Try to merge with right sibling
    if idx + 1 < parent.vals.len() {
        let right_cid = Cid(parent.vals[idx + 1]
            .as_slice()
            .try_into()
            .map_err(|_| Error::InvalidNode)?);
        let right_sibling = prolly.load(&right_cid)?;

        if !is_valid_boundary_between(prolly, node, &right_sibling) {
            let merged = merge_nodes(prolly, node, &right_sibling)?;

            let mut new_parent = parent.clone();
            new_parent.keys.remove(idx + 1);
            new_parent.vals.remove(idx + 1);

            // Check if merged node needs splitting (might be too large after merge)
            if merged.len() > merged.max_chunk_size && merged.len() > 1 {
                // Need to split the merged node - build new ancestors with updated parent position
                let mut new_ancestors: Vec<(Node, usize)> =
                    ancestors[..ancestors.len() - 1].to_vec();
                new_ancestors.push((new_parent, idx));
                return split_node_with_collector(prolly, merged, &new_ancestors, collector);
            }

            // Save merged node and update parent
            let merged_cid = collector.add(&merged);
            new_parent.keys[idx] = merged.keys[0].clone();
            new_parent.vals[idx] = merged_cid.0.to_vec();

            // Continue rebalancing with the updated parent
            return Ok(Some(
                rebalance_with_collector(
                    prolly,
                    new_parent,
                    &ancestors[..ancestors.len() - 1],
                    collector,
                )?
                .unwrap_or(merged_cid),
            ));
        }
    }

    // No merge possible
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::super::config::Config;
    use super::super::store::MemStore;
    use super::*;

    fn leaf_with_entries<S: Store>(prolly: &Prolly<S>, start: usize, end: usize) -> Node {
        let mut node = prolly.new_leaf_node();
        for idx in start..end {
            node.keys.push(format!("k{idx:04}").into_bytes());
            node.vals.push(format!("v{idx:04}").into_bytes());
        }
        node
    }

    #[test]
    fn merge_nodes_preserves_order_and_node_settings() {
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(8)
            .chunking_factor(u32::MAX)
            .build();
        let prolly = Prolly::new(MemStore::new(), config);
        let left = leaf_with_entries(&prolly, 0, 2);
        let right = leaf_with_entries(&prolly, 2, 5);

        let merged = merge_nodes(&prolly, &left, &right).unwrap();

        assert!(merged.leaf);
        assert_eq!(merged.level, left.level);
        assert_eq!(merged.min_chunk_size, left.min_chunk_size);
        assert_eq!(merged.max_chunk_size, left.max_chunk_size);
        assert_eq!(
            merged.keys,
            (0..5)
                .map(|idx| format!("k{idx:04}").into_bytes())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            merged.vals,
            (0..5)
                .map(|idx| format!("v{idx:04}").into_bytes())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn split_into_chunks_preserves_all_entries_in_order() {
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(u32::MAX)
            .build();
        let prolly = Prolly::new(MemStore::new(), config);
        let node = leaf_with_entries(&prolly, 0, 11);

        let chunks = split_into_chunks(&prolly, &node, 4);

        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|chunk| !chunk.is_empty()));
        assert!(chunks.iter().all(|chunk| chunk.len() <= 4));
        assert_eq!(
            chunks
                .iter()
                .flat_map(|chunk| chunk.keys.iter().cloned())
                .collect::<Vec<_>>(),
            node.keys
        );
        assert_eq!(
            chunks
                .iter()
                .flat_map(|chunk| chunk.vals.iter().cloned())
                .collect::<Vec<_>>(),
            node.vals
        );
    }
}
