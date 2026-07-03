//! Shared utility functions for Prolly tree operations
//!
//! This module contains helper functions used across multiple Prolly tree modules.
//! These utilities are internal implementation details and are not part of the
//! public API.
//!
//! # Functions
//!
//! ## Path Comparison
//!
//! The [`same_leaf_path`] function is used during batch operations to determine
//! if consecutive mutations target the same leaf node. This enables efficient
//! grouping of mutations for batch processing.
//!
//! # Design Notes
//!
//! Utility functions are placed here when they:
//!
//! - Are used by multiple modules
//! - Don't fit naturally into a specific module's responsibility
//! - Are implementation details not exposed in the public API
//!
//! As the codebase grows, additional shared utilities may be added here.

use super::node::Node;

/// Check if two paths lead to the same leaf.
///
/// Compares the ancestor paths (excluding the leaf itself) to determine
/// if mutations target the same leaf node. This is used during batch
/// operations to group mutations that affect the same leaf.
///
/// # Arguments
/// * `ancestors` - The ancestor path from a previous mutation group
/// * `path` - The full path to the current mutation's target leaf
///
/// # Returns
/// `true` if both paths lead to the same leaf node, `false` otherwise.
///
/// # Note
/// The comparison is done by comparing CIDs of nodes in the path and their
/// indices, which ensures we're comparing the actual tree structure rather
/// than just key positions.
#[allow(dead_code)]
pub fn same_leaf_path(ancestors: &[(Node, usize)], path: &[(Node, usize)]) -> bool {
    if path.is_empty() {
        return ancestors.is_empty();
    }

    // Compare the path to the leaf (excluding the leaf itself)
    let path_ancestors = &path[..path.len() - 1];

    if ancestors.len() != path_ancestors.len() {
        return false;
    }

    // Compare CIDs of nodes in the path
    ancestors
        .iter()
        .zip(path_ancestors.iter())
        .all(|((n1, i1), (n2, i2))| n1.cid() == n2.cid() && i1 == i2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_same_leaf_path_empty_paths() {
        let empty: Vec<(Node, usize)> = vec![];
        assert!(same_leaf_path(&empty, &empty));
    }

    #[test]
    fn test_same_leaf_path_empty_ancestors_non_empty_path() {
        let empty: Vec<(Node, usize)> = vec![];
        let node = Node::builder().leaf(true).build();
        let path = vec![(node, 0)];
        // Empty ancestors with single-node path (leaf only) should match
        assert!(same_leaf_path(&empty, &path));
    }

    #[test]
    fn test_same_leaf_path_non_empty_ancestors_empty_path() {
        let node = Node::builder().leaf(false).build();
        let ancestors = vec![(node, 0)];
        let empty: Vec<(Node, usize)> = vec![];
        assert!(!same_leaf_path(&ancestors, &empty));
    }
}
