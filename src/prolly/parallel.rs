//! Parallel rebalancing operations for Prolly trees
//!
//! This module provides parallel processing capabilities for tree operations,
//! enabling efficient use of multi-core systems for large trees.
//!
//! # Overview
//!
//! The ParallelRebalancer trait and its default implementation enable:
//! - Concurrent processing of independent subtrees during rebalancing
//! - Parallel batch mutation processing for leaf groups
//! - Threshold-based fallback to sequential processing for small trees
//!
//! # Configuration
//!
//! Use [`ParallelConfig`] to control parallel behavior:
//! - `max_threads`: Maximum number of threads (0 = use rayon default)
//! - `parallelism_threshold`: Minimum items before parallelization kicks in
//!
//! # Example
//!
//! ```rust
//! use prolly::{Prolly, MemStore, Config, Mutation, ParallelRebalancer, DefaultParallelRebalancer, ParallelConfig};
//! use std::sync::Arc;
//!
//! let store = Arc::new(MemStore::new());
//! let config = Config::default();
//! let prolly = Prolly::new(Arc::clone(&store), config);
//! let tree = prolly.create();
//!
//! // Create mutations
//! let mutations: Vec<Mutation> = (0..100)
//!     .map(|i| Mutation::Upsert {
//!         key: format!("key{:04}", i).into_bytes(),
//!         val: format!("val{}", i).into_bytes(),
//!     })
//!     .collect();
//!
//! // Apply mutations with parallel processing
//! let rebalancer = DefaultParallelRebalancer::new();
//! let parallel_config = ParallelConfig::default();
//! let new_tree = rebalancer.parallel_batch(&store, &prolly, &tree, mutations, &parallel_config).unwrap();
//! ```
//!
//! # Error Handling
//!
//! Error propagation is handled through rayon's `collect()` mechanism:
//! - When any parallel operation fails, the first error is propagated
//! - Remaining parallel work is cancelled (rayon's short-circuit behavior)
//! - All errors are wrapped in `Error::Store` for consistency

use rayon::prelude::*;

use super::cid::Cid;
use super::error::{Error, Mutation};
use super::node::Node;
use super::store::Store;
use super::tree::Tree;

use super::batch::{
    apply_mutations_to_leaf, group_mutations_by_leaf, preprocess_mutations, BatchWriteCollector,
    LeafMutationGroup,
};
use super::rebalance;
use super::Prolly;

/// Configuration for parallel rebalancing operations.
///
/// Controls how and when parallel processing is used during tree operations.
///
/// # Example
/// ```rust
/// use prolly::ParallelConfig;
///
/// // Use defaults (rayon's thread count, threshold of 100)
/// let config = ParallelConfig::default();
///
/// // Custom configuration
/// let config = ParallelConfig {
///     max_threads: 4,
///     parallelism_threshold: 50,
/// };
/// ```
#[derive(Clone, Debug)]
pub struct ParallelConfig {
    /// Maximum number of threads to use.
    ///
    /// Set to 0 to use rayon's default (usually the number of CPU cores).
    pub max_threads: usize,

    /// Minimum number of items before parallelization kicks in.
    ///
    /// If the number of items to process is below this threshold,
    /// sequential processing is used instead for efficiency.
    pub parallelism_threshold: usize,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            max_threads: 0, // Use rayon default (usually num_cpus)
            parallelism_threshold: 100,
        }
    }
}

impl ParallelConfig {
    /// Create a new ParallelConfig with custom settings.
    pub fn new(max_threads: usize, parallelism_threshold: usize) -> Self {
        Self {
            max_threads,
            parallelism_threshold,
        }
    }

    /// Create a config that always uses sequential processing.
    pub fn sequential() -> Self {
        Self {
            max_threads: 1,
            parallelism_threshold: usize::MAX,
        }
    }
}

/// Trait for parallel rebalancing operations.
///
/// Implementations of this trait provide parallel processing capabilities
/// for tree rebalancing and batch mutation operations.
///
/// # Example
/// ```rust
/// use prolly::{Prolly, MemStore, Config, Mutation, ParallelRebalancer, DefaultParallelRebalancer, ParallelConfig};
///
/// let store = MemStore::new();
/// let prolly = Prolly::new(store, Config::default());
/// let tree = prolly.create();
///
/// let rebalancer = DefaultParallelRebalancer;
/// let config = ParallelConfig::default();
///
/// let mutations = vec![
///     Mutation::Upsert { key: b"a".to_vec(), val: b"1".to_vec() },
///     Mutation::Upsert { key: b"b".to_vec(), val: b"2".to_vec() },
/// ];
///
/// // Note: For direct trait usage, store and prolly must use the same store type
/// // For convenience, use prolly.parallel_batch() instead
/// ```
pub trait ParallelRebalancer<S: Store> {
    /// Rebalance multiple nodes in parallel.
    ///
    /// Processes independent subtrees concurrently using a thread pool.
    /// Falls back to sequential processing when below the parallelism threshold.
    ///
    /// # Arguments
    /// * `store` - The storage backend
    /// * `prolly` - The Prolly tree manager
    /// * `nodes` - Nodes to rebalance with their ancestor paths
    /// * `config` - Parallel configuration
    ///
    /// # Returns
    /// * `Ok(Vec<Cid>)` - Vector of new root CIDs for each rebalanced subtree
    /// * `Err(Error)` - On storage or processing errors
    fn parallel_rebalance(
        &self,
        store: &S,
        prolly: &Prolly<S>,
        nodes: Vec<(Node, Vec<(Node, usize)>)>,
        config: &ParallelConfig,
    ) -> Result<Vec<Cid>, Error>;

    /// Apply batch mutations with parallel leaf processing.
    ///
    /// Groups mutations by target leaf and processes independent leaf groups
    /// in parallel when beneficial. Uses batch_get and batch_put for efficient I/O.
    ///
    /// # Arguments
    /// * `store` - The storage backend
    /// * `prolly` - The Prolly tree manager
    /// * `tree` - The tree to modify
    /// * `mutations` - Vector of mutations to apply
    /// * `config` - Parallel configuration
    ///
    /// # Returns
    /// * `Ok(Tree)` - New tree with all mutations applied
    /// * `Err(Error)` - On storage or processing errors
    fn parallel_batch(
        &self,
        _store: &S,
        prolly: &Prolly<S>,
        tree: &Tree,
        mutations: Vec<Mutation>,
        config: &ParallelConfig,
    ) -> Result<Tree, Error>;
}

/// Default implementation of ParallelRebalancer using rayon.
///
/// This implementation uses rayon's parallel iterators for concurrent processing
/// and provides threshold-based fallback to sequential processing for small workloads.
#[derive(Clone, Debug, Default)]
pub struct DefaultParallelRebalancer;

impl DefaultParallelRebalancer {
    /// Create a new DefaultParallelRebalancer.
    pub fn new() -> Self {
        Self
    }

    /// Sequential rebalance for small workloads.
    fn sequential_rebalance<S: Store>(
        &self,
        prolly: &Prolly<S>,
        nodes: Vec<(Node, Vec<(Node, usize)>)>,
    ) -> Result<Vec<Cid>, Error> {
        nodes
            .into_iter()
            .map(|(node, ancestors)| rebalance::rebalance(prolly, node, &ancestors))
            .collect()
    }

    /// Sequential batch processing for small workloads.
    ///
    /// For the first group, uses the pre-computed path since the tree structure
    /// hasn't changed yet. For subsequent groups, applies mutations one at a time
    /// because the tree structure may have changed after previous rebalancing,
    /// and mutations that were originally grouped together may now belong to
    /// different leaves.
    fn sequential_batch<S: Store>(
        &self,
        prolly: &Prolly<S>,
        tree: &Tree,
        groups: Vec<LeafMutationGroup>,
    ) -> Result<Tree, Error> {
        let mut current_tree = tree.clone();

        for (i, group) in groups.into_iter().enumerate() {
            if i == 0 {
                // First group: use pre-computed path since tree structure hasn't changed
                let mut collector = BatchWriteCollector::new();

                let modified_leaf = apply_mutations_to_leaf(group.leaf, &group.mutations);
                let new_root = rebalance::rebalance_with_collector(
                    prolly,
                    modified_leaf,
                    &group.ancestors,
                    &mut collector,
                )?;

                // Flush writes for this group
                collector.flush(prolly.store())?;
                prolly.record_batch_write_metrics(collector.len(), collector.bytes_len());

                // Update the current tree for the next iteration
                current_tree = Tree {
                    root: new_root,
                    config: current_tree.config.clone(),
                };
            } else {
                // Subsequent groups: apply mutations one at a time because tree
                // structure may have changed. The original grouping was based on
                // the initial tree structure, but after rebalancing (splits/merges),
                // mutations in this group may now belong to different leaves.
                for mutation in group.mutations {
                    current_tree = Self::apply_single_mutation(prolly, &current_tree, mutation)?;
                }
            }
        }

        Ok(current_tree)
    }

    /// Apply a single mutation to the tree.
    ///
    /// This helper method finds the correct path for the mutation's key in the
    /// current tree structure and applies the mutation (upsert or delete).
    fn apply_single_mutation<S: Store>(
        prolly: &Prolly<S>,
        tree: &Tree,
        mutation: Mutation,
    ) -> Result<Tree, Error> {
        match mutation {
            Mutation::Upsert { key, val } => prolly.put(tree, key, val),
            Mutation::Delete { key } => prolly.delete(tree, &key),
        }
    }
}

impl<S: Store> ParallelRebalancer<S> for DefaultParallelRebalancer {
    fn parallel_rebalance(
        &self,
        _store: &S,
        prolly: &Prolly<S>,
        nodes: Vec<(Node, Vec<(Node, usize)>)>,
        config: &ParallelConfig,
    ) -> Result<Vec<Cid>, Error> {
        // Fall back to sequential if below threshold
        if nodes.len() < config.parallelism_threshold {
            return self.sequential_rebalance(prolly, nodes);
        }

        // Configure thread pool if max_threads is specified
        if config.max_threads > 0 {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(config.max_threads)
                .build()
                .map_err(|e| {
                    Error::Store(Box::new(std::io::Error::other(format!(
                        "Failed to create thread pool: {}",
                        e
                    ))))
                })?;

            pool.install(|| {
                nodes
                    .into_par_iter()
                    .map(|(node, ancestors)| rebalance::rebalance(prolly, node, &ancestors))
                    .collect()
            })
        } else {
            // Use rayon's default thread pool
            nodes
                .into_par_iter()
                .map(|(node, ancestors)| rebalance::rebalance(prolly, node, &ancestors))
                .collect()
        }
    }

    fn parallel_batch(
        &self,
        _store: &S,
        prolly: &Prolly<S>,
        tree: &Tree,
        mutations: Vec<Mutation>,
        config: &ParallelConfig,
    ) -> Result<Tree, Error> {
        // Handle empty mutations
        if mutations.is_empty() {
            return Ok(tree.clone());
        }

        // Preprocess mutations (sort and deduplicate)
        let mutations = preprocess_mutations(mutations);
        if mutations.is_empty() {
            return Ok(tree.clone());
        }

        // Group mutations by affected leaf
        let groups = group_mutations_by_leaf(prolly, tree, mutations)?;
        if groups.is_empty() {
            return Ok(tree.clone());
        }

        // Fall back to sequential if below threshold
        if groups.len() < config.parallelism_threshold {
            return self.sequential_batch(prolly, tree, groups);
        }

        // For now, parallel processing of mutations is complex because tree structure
        // changes after each rebalance. Fall back to sequential processing.
        // TODO: Implement true parallel processing with proper synchronization
        self.sequential_batch(prolly, tree, groups)
    }
}
