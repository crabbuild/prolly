//! Core trait definitions for Prolly tree operations
//!
//! This module defines internal traits used for organizing the implementation.
//! These traits are designed for future extensibility, allowing alternative
//! implementations of key tree operations.
//!
//! # Overview
//!
//! Currently, the public API is the concrete `Prolly<S>` struct. These traits
//! are not exposed publicly but provide a foundation for:
//!
//! - Custom rebalancing strategies (e.g., different chunking algorithms)
//! - Alternative batch mutation implementations
//! - Different diff/merge algorithms (e.g., streaming vs. collecting)
//!
//! # Design Philosophy
//!
//! The traits follow a composition-based design where the `Prolly<S>` struct
//! delegates to specialized modules. This separation enables:
//!
//! - **Independent testing**: Each component can be tested in isolation
//! - **Future swapping**: Implementations can be changed without affecting the API
//! - **Clear boundaries**: Responsibilities are well-defined and documented
//!
//! # Trait Hierarchy
//!
//! ## TreeRebalancer
//!
//! Handles node splitting and merging to maintain tree balance. The default
//! implementation uses content-defined chunking with probabilistic boundaries.
//!
//! ## BatchMutator
//!
//! Handles bulk modifications with atomic writes. The default implementation
//! uses last-write-wins semantics and groups mutations by target leaf.
//!
//! ## TreeDiffer
//!
//! Handles computing differences and performing merges. The default implementation
//! uses a simple comparison-based approach with pluggable conflict resolvers.
//!
//! # Future Extensibility
//!
//! These traits are currently internal, but could be exposed publicly in the
//! future to allow users to provide custom implementations. For example:
//!
//! - A `StreamingDiffer` that yields differences lazily
//! - A `ParallelRebalancer` that uses multiple threads
//! - A `ConflictFreeMerger` for CRDT-style merging

// Allow dead_code since these traits are defined for future extensibility
#![allow(dead_code)]

use super::cid::Cid;
use super::error::{Diff, Error, Mutation, Resolver};
use super::node::Node;
use super::store::Store;
use super::tree::Tree;

use super::batch::BatchWriteCollector;

/// Trait for tree rebalancing operations.
///
/// Rebalancing ensures that tree nodes stay within configured size bounds
/// after insertions and deletions. This trait abstracts the rebalancing
/// strategy, allowing for alternative implementations.
///
/// # Responsibilities
/// - Splitting oversized nodes
/// - Merging undersized nodes with siblings
/// - Propagating changes up the tree
/// - Maintaining tree balance invariants
///
/// # Default Implementation
/// The default implementation uses content-defined chunking with
/// probabilistic boundaries for deterministic tree structure.
pub trait TreeRebalancer<S: Store> {
    /// Rebalance a node after modification.
    ///
    /// Called after a node has been modified (insertion or deletion).
    /// Handles splitting if the node is too large, merging if too small,
    /// and propagates changes up through the ancestor path.
    ///
    /// # Arguments
    /// * `node` - The modified node to rebalance
    /// * `ancestors` - Path from root to the node's parent (excluding the node itself)
    ///
    /// # Returns
    /// * `Ok(cid)` - CID of the new root after rebalancing
    /// * `Err(Error)` - On storage or processing errors
    fn rebalance(&self, node: Node, ancestors: &[(Node, usize)]) -> Result<Cid, Error>;

    /// Rebalance with batch write collector.
    ///
    /// Similar to `rebalance`, but collects nodes for batch writing instead
    /// of writing immediately. Used during batch mutations for atomic writes.
    ///
    /// # Arguments
    /// * `node` - The modified node to rebalance
    /// * `ancestors` - Path from root to the node's parent
    /// * `collector` - Collector for nodes to be written atomically
    ///
    /// # Returns
    /// * `Ok(Some(cid))` - CID of the new root
    /// * `Ok(None)` - Tree becomes empty
    /// * `Err(Error)` - On processing errors
    fn rebalance_with_collector(
        &self,
        node: Node,
        ancestors: &[(Node, usize)],
        collector: &mut BatchWriteCollector,
    ) -> Result<Option<Cid>, Error>;
}

/// Trait for batch mutation operations.
///
/// Batch mutations enable efficient bulk modifications to a tree by:
/// - Sorting and deduplicating mutations
/// - Grouping mutations by target leaf
/// - Applying all changes atomically
///
/// # Responsibilities
/// - Preprocessing mutations (sort, deduplicate)
/// - Grouping mutations by affected leaf
/// - Coordinating atomic writes
///
/// # Default Implementation
/// The default implementation uses last-write-wins semantics for
/// duplicate keys and writes all modified nodes atomically.
pub trait BatchMutator<S: Store> {
    /// Apply multiple mutations atomically.
    ///
    /// Mutations are preprocessed (sorted, deduplicated), grouped by
    /// target leaf, and applied with a single atomic batch write.
    ///
    /// # Arguments
    /// * `tree` - The tree to modify
    /// * `mutations` - Vector of mutations to apply
    ///
    /// # Returns
    /// * `Ok(Tree)` - New tree with all mutations applied
    /// * `Err(Error)` - On storage or processing errors
    ///
    /// # Semantics
    /// - Mutations are sorted by key (lexicographic order)
    /// - Duplicate keys use last-write-wins
    /// - All changes are written atomically
    fn apply_batch(&self, tree: &Tree, mutations: Vec<Mutation>) -> Result<Tree, Error>;
}

/// Trait for diff and merge operations.
///
/// Diff operations compute the differences between two trees, while
/// merge operations combine changes from divergent branches.
///
/// # Responsibilities
/// - Computing tree differences (Added, Removed, Changed)
/// - Three-way merge with conflict detection
/// - Conflict resolution via resolver functions
///
/// # Default Implementation
/// The default implementation uses a simple comparison-based diff
/// and supports pluggable conflict resolvers for merge operations.
pub trait TreeDiffer<S: Store> {
    /// Compute differences between two trees.
    ///
    /// Returns a vector of `Diff` entries representing the changes
    /// needed to transform `base` into `other`.
    ///
    /// # Arguments
    /// * `base` - The base tree to compare from
    /// * `other` - The other tree to compare to
    ///
    /// # Returns
    /// * `Ok(Vec<Diff>)` - Vector of differences
    /// * `Err(Error)` - On storage or processing errors
    ///
    /// # Diff Types
    /// - `Added`: Key exists in `other` but not in `base`
    /// - `Removed`: Key exists in `base` but not in `other`
    /// - `Changed`: Key exists in both with different values
    fn diff(&self, base: &Tree, other: &Tree) -> Result<Vec<Diff>, Error>;

    /// Merge two trees using three-way merge.
    ///
    /// Combines changes from `left` and `right` branches relative to
    /// their common ancestor `base`.
    ///
    /// # Arguments
    /// * `base` - The common ancestor tree
    /// * `left` - The left branch tree
    /// * `right` - The right branch tree
    /// * `resolver` - Optional conflict resolver function
    ///
    /// # Returns
    /// * `Ok(Tree)` - Merged tree
    /// * `Err(Error::Conflict)` - If conflicts cannot be resolved
    ///
    /// # Conflict Handling
    /// A conflict occurs when both branches modify the same key differently.
    /// If a resolver is provided, it can keep a value, delete the key, or leave
    /// the conflict unresolved. Unresolved conflicts return an error.
    fn merge(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        resolver: Option<Resolver>,
    ) -> Result<Tree, Error>;
}

#[cfg(test)]
mod tests {
    // Trait tests would go here when implementations are created
    // Currently, the concrete Prolly<S> struct implements these behaviors
    // directly through module delegation rather than trait implementation
}
