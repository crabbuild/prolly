//! Tree statistics collection and analysis
//!
//! This module provides comprehensive statistics collection for Prolly trees, enabling
//! developers to understand tree structure, size distribution, and performance characteristics.
//!
//! # Overview
//!
//! The statistics system collects detailed metrics about Prolly trees through a single
//! traversal, including:
//!
//! - **Structure metrics**: node counts, tree height, key-value pairs
//! - **Size metrics**: total size, average/min/max node sizes
//! - **Level-based metrics**: distribution of nodes and sizes across tree levels
//! - **Efficiency metrics**: fanout, fill factors
//! - **Key/value metrics**: sizes and distributions
//!
//! # Usage
//!
//! ## Basic Statistics Collection
//!
//! ```rust
//! use prolly::{Config, MemStore, Prolly};
//!
//! let store = MemStore::new();
//! let prolly = Prolly::new(store, Config::default());
//!
//! // Build a tree
//! let mut tree = prolly.create();
//! tree = prolly.put(&tree, b"key".to_vec(), b"value".to_vec()).unwrap();
//!
//! // Collect statistics
//! let stats = prolly.collect_stats(&tree).unwrap();
//!
//! // Display human-readable output
//! println!("{}", stats);
//!
//! // Access specific metrics
//! println!("Total nodes: {}", stats.num_nodes);
//! println!("Tree height: {}", stats.tree_height);
//! println!("Average fill factor: {:.2}%", stats.avg_fill_factor * 100.0);
//! ```
//!
//! ## Serialization
//!
//! Statistics can be serialized to JSON for storage or analysis:
//!
//! ```rust
//! # use prolly::{Config, MemStore, Prolly};
//! # let store = MemStore::new();
//! # let prolly = Prolly::new(store, Config::default());
//! # let tree = prolly.create();
//! let stats = prolly.collect_stats(&tree).unwrap();
//!
//! // Serialize to JSON
//! let json = serde_json::to_string_pretty(&stats).unwrap();
//!
//! // Deserialize back
//! let restored: prolly::TreeStats = serde_json::from_str(&json).unwrap();
//! assert_eq!(stats, restored);
//! ```
//!
//! ## Comparing Statistics
//!
//! Compare statistics between two trees or two points in time:
//!
//! ```rust
//! # use prolly::{Config, MemStore, Prolly};
//! # let store = MemStore::new();
//! # let prolly = Prolly::new(store, Config::default());
//! # let tree1 = prolly.create();
//! # let mut tree2 = tree1.clone();
//! # tree2 = prolly.put(&tree2, b"key".to_vec(), b"value".to_vec()).unwrap();
//! let comparison = prolly.stats_diff(&tree1, &tree2).unwrap();
//! println!("Nodes added: {:+}", comparison.absolute.num_nodes_diff);
//! println!("Size change: {:+.2}%", comparison.percentage.total_tree_size_bytes_pct);
//! ```
//!
//! ## Incremental Updates
//!
//! For scenarios where full tree traversal is expensive, statistics can be updated
//! incrementally as nodes are added, removed, or modified:
//!
//! ```rust
//! # use prolly::{Config, MemStore, Prolly, TreeStats, Node};
//! # let store = MemStore::new();
//! # let prolly = Prolly::new(store, Config::default());
//! # let tree = prolly.create();
//! // Collect initial statistics
//! let stats = prolly.collect_stats(&tree).unwrap();
//!
//! // When adding a node (in your tree modification code):
//! # let node = Node::builder().keys(vec![b"k".to_vec()]).vals(vec![b"v".to_vec()]).leaf(true).level(0).build();
//! let node_size = node.to_bytes().len();
//! let mut updated_stats = stats.clone();
//! updated_stats.add_node(&node, node_size);
//!
//! // After incremental updates, finalize to compute averages
//! updated_stats.finalize();
//!
//! // Note: For validation, you would need the actual modified tree
//! // let is_valid = updated_stats.validate(&prolly, &modified_tree).unwrap();
//! ```
//!
//! # Design
//!
//! For detailed design information, see the [design document](../.kiro/specs/tree-stats/design.md).
//!
//! The statistics collection algorithm uses depth-first traversal to visit each node once,
//! accumulating metrics during the traversal and computing derived metrics (averages, fill
//! factors) in a finalization step.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Statistics about a Prolly tree structure
///
/// This struct contains comprehensive metrics about a Prolly tree, including structure,
/// size, distribution, and efficiency information. Statistics are collected through a
/// single tree traversal using [`Prolly::collect_stats`](crate::prolly::Prolly::collect_stats).
///
/// # Fields
///
/// ## Basic Structure
/// - `num_nodes`: Total number of nodes in the tree
/// - `num_leaves`: Number of leaf nodes (containing key-value pairs)
/// - `num_internal_nodes`: Number of internal nodes (containing child pointers)
/// - `tree_height`: Maximum level in the tree (0 for single-node trees)
/// - `total_key_value_pairs`: Total number of key-value pairs stored
///
/// ## Size Statistics
/// - `total_tree_size_bytes`: Total serialized size of all nodes
/// - `avg_node_size_bytes`: Average node size in bytes
/// - `min_node_size_bytes`: Smallest node size in bytes
/// - `max_node_size_bytes`: Largest node size in bytes
/// - `avg_entries_per_node`: Average number of entries per node
///
/// ## Level-Based Statistics
/// - `nodes_per_level`: Count of nodes at each level
/// - `avg_node_size_per_level`: Average node size at each level
/// - `avg_entries_per_level`: Average entries per node at each level
/// - `min_entries_per_level`: Minimum entries at each level
/// - `max_entries_per_level`: Maximum entries at each level
///
/// ## Fanout and Fill Factor
/// - `avg_fanout`: Average number of children per internal node
/// - `min_fanout`: Minimum fanout value
/// - `max_fanout`: Maximum fanout value
/// - `avg_fill_factor`: Average fill factor (entries / max_chunk_size)
/// - `avg_leaf_fill_factor`: Average fill factor for leaf nodes
/// - `avg_internal_fill_factor`: Average fill factor for internal nodes
///
/// ## Key and Value Sizes
/// - `avg_key_size_bytes`: Average key size in bytes
/// - `avg_value_size_bytes`: Average value size in bytes
/// - `min_key_size_bytes`: Smallest key size
/// - `max_key_size_bytes`: Largest key size
/// - `min_value_size_bytes`: Smallest value size
/// - `max_value_size_bytes`: Largest value size
/// - `total_keys_size_bytes`: Total size of all keys
/// - `total_values_size_bytes`: Total size of all values
///
/// # Example
///
/// ```rust
/// use prolly::{Config, MemStore, Prolly};
///
/// let store = MemStore::new();
/// let prolly = Prolly::new(store, Config::default());
/// let mut tree = prolly.create();
///
/// // Build a tree
/// for i in 0..100 {
///     let key = format!("key_{}", i).into_bytes();
///     let val = format!("value_{}", i).into_bytes();
///     tree = prolly.put(&tree, key, val).unwrap();
/// }
///
/// // Collect statistics
/// let stats = prolly.collect_stats(&tree).unwrap();
///
/// println!("Tree has {} nodes at height {}", stats.num_nodes, stats.tree_height);
/// println!("Average fill factor: {:.2}%", stats.avg_fill_factor * 100.0);
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TreeStats {
    // Basic structure
    pub num_nodes: usize,
    pub num_leaves: usize,
    pub num_internal_nodes: usize,
    pub tree_height: u8,
    pub total_key_value_pairs: usize,

    // Size statistics
    pub total_tree_size_bytes: usize,
    pub avg_node_size_bytes: f64,
    pub min_node_size_bytes: usize,
    pub max_node_size_bytes: usize,
    pub avg_entries_per_node: f64,

    // Level-based statistics
    pub nodes_per_level: BTreeMap<u8, usize>,
    pub avg_node_size_per_level: BTreeMap<u8, f64>,
    pub avg_entries_per_level: BTreeMap<u8, f64>,
    pub min_entries_per_level: BTreeMap<u8, usize>,
    pub max_entries_per_level: BTreeMap<u8, usize>,

    // Fanout and fill factor
    pub avg_fanout: f64,
    pub min_fanout: usize,
    pub max_fanout: usize,
    pub avg_fill_factor: f64,
    pub avg_leaf_fill_factor: f64,
    pub avg_internal_fill_factor: f64,

    // Key and value sizes
    pub avg_key_size_bytes: f64,
    pub avg_value_size_bytes: f64,
    pub min_key_size_bytes: usize,
    pub max_key_size_bytes: usize,
    pub min_value_size_bytes: usize,
    pub max_value_size_bytes: usize,
    pub total_keys_size_bytes: usize,
    pub total_values_size_bytes: usize,

    // Internal fields for tracking intermediate values during accumulation
    #[serde(skip)]
    total_fanout: usize,
    #[serde(skip)]
    total_fill_factor: f64,
    #[serde(skip)]
    total_leaf_fill_factor: f64,
    #[serde(skip)]
    total_internal_fill_factor: f64,
    #[serde(skip)]
    level_total_sizes: BTreeMap<u8, usize>,
    #[serde(skip)]
    level_total_entries: BTreeMap<u8, usize>,
}

// Custom PartialEq implementation that only compares public (serialized) fields
// Internal accumulation fields (marked with #[serde(skip)]) are excluded from comparison
impl PartialEq for TreeStats {
    fn eq(&self, other: &Self) -> bool {
        // Helper function for comparing floats with epsilon
        fn float_eq(a: f64, b: f64) -> bool {
            (a - b).abs() < 1e-10
        }

        self.num_nodes == other.num_nodes
            && self.num_leaves == other.num_leaves
            && self.num_internal_nodes == other.num_internal_nodes
            && self.tree_height == other.tree_height
            && self.total_key_value_pairs == other.total_key_value_pairs
            && self.total_tree_size_bytes == other.total_tree_size_bytes
            && float_eq(self.avg_node_size_bytes, other.avg_node_size_bytes)
            && self.min_node_size_bytes == other.min_node_size_bytes
            && self.max_node_size_bytes == other.max_node_size_bytes
            && float_eq(self.avg_entries_per_node, other.avg_entries_per_node)
            && self.nodes_per_level == other.nodes_per_level
            && self.avg_node_size_per_level.len() == other.avg_node_size_per_level.len()
            && self.avg_node_size_per_level.iter().all(|(k, v)| {
                other
                    .avg_node_size_per_level
                    .get(k)
                    .is_some_and(|ov| float_eq(*v, *ov))
            })
            && self.avg_entries_per_level.len() == other.avg_entries_per_level.len()
            && self.avg_entries_per_level.iter().all(|(k, v)| {
                other
                    .avg_entries_per_level
                    .get(k)
                    .is_some_and(|ov| float_eq(*v, *ov))
            })
            && self.min_entries_per_level == other.min_entries_per_level
            && self.max_entries_per_level == other.max_entries_per_level
            && float_eq(self.avg_fanout, other.avg_fanout)
            && self.min_fanout == other.min_fanout
            && self.max_fanout == other.max_fanout
            && float_eq(self.avg_fill_factor, other.avg_fill_factor)
            && float_eq(self.avg_leaf_fill_factor, other.avg_leaf_fill_factor)
            && float_eq(
                self.avg_internal_fill_factor,
                other.avg_internal_fill_factor,
            )
            && float_eq(self.avg_key_size_bytes, other.avg_key_size_bytes)
            && float_eq(self.avg_value_size_bytes, other.avg_value_size_bytes)
            && self.min_key_size_bytes == other.min_key_size_bytes
            && self.max_key_size_bytes == other.max_key_size_bytes
            && self.min_value_size_bytes == other.min_value_size_bytes
            && self.max_value_size_bytes == other.max_value_size_bytes
            && self.total_keys_size_bytes == other.total_keys_size_bytes
            && self.total_values_size_bytes == other.total_values_size_bytes
        // Note: Internal fields (total_fanout, total_fill_factor, etc.) are intentionally excluded
        // as they are only used during accumulation and are not serialized
    }
}

impl TreeStats {
    /// Create a new TreeStats with zero values
    ///
    /// All counters are initialized to zero, and min values are set to `usize::MAX`
    /// (they will be reset to 0 for empty trees during finalization).
    ///
    /// # Example
    ///
    /// ```rust
    /// use prolly::TreeStats;
    ///
    /// let stats = TreeStats::new();
    /// assert_eq!(stats.num_nodes, 0);
    /// assert_eq!(stats.tree_height, 0);
    /// ```
    pub fn new() -> Self {
        Self {
            num_nodes: 0,
            num_leaves: 0,
            num_internal_nodes: 0,
            tree_height: 0,
            total_key_value_pairs: 0,
            total_tree_size_bytes: 0,
            avg_node_size_bytes: 0.0,
            min_node_size_bytes: usize::MAX,
            max_node_size_bytes: 0,
            avg_entries_per_node: 0.0,
            nodes_per_level: BTreeMap::new(),
            avg_node_size_per_level: BTreeMap::new(),
            avg_entries_per_level: BTreeMap::new(),
            min_entries_per_level: BTreeMap::new(),
            max_entries_per_level: BTreeMap::new(),
            avg_fanout: 0.0,
            min_fanout: usize::MAX,
            max_fanout: 0,
            avg_fill_factor: 0.0,
            avg_leaf_fill_factor: 0.0,
            avg_internal_fill_factor: 0.0,
            avg_key_size_bytes: 0.0,
            avg_value_size_bytes: 0.0,
            min_key_size_bytes: usize::MAX,
            max_key_size_bytes: 0,
            min_value_size_bytes: usize::MAX,
            max_value_size_bytes: 0,
            total_keys_size_bytes: 0,
            total_values_size_bytes: 0,
            total_fanout: 0,
            total_fill_factor: 0.0,
            total_leaf_fill_factor: 0.0,
            total_internal_fill_factor: 0.0,
            level_total_sizes: BTreeMap::new(),
            level_total_entries: BTreeMap::new(),
        }
    }
}

impl Default for TreeStats {
    fn default() -> Self {
        Self::new()
    }
}

impl TreeStats {
    /// Accumulate statistics from a single node
    ///
    /// Updates all relevant counters, min/max values, and level-based statistics
    /// based on the provided node. This method is called during tree traversal
    /// for each visited node.
    ///
    /// After accumulating all nodes, call [`finalize`](TreeStats::finalize) to
    /// compute derived metrics like averages.
    ///
    /// # Arguments
    ///
    /// * `node` - The node to accumulate statistics from
    ///
    /// # Example
    ///
    /// ```rust
    /// use prolly::{TreeStats, Node};
    ///
    /// let mut stats = TreeStats::new();
    /// let node = Node::builder()
    ///     .keys(vec![b"key".to_vec()])
    ///     .vals(vec![b"value".to_vec()])
    ///     .leaf(true)
    ///     .level(0)
    ///     .build();
    ///
    /// stats.accumulate(&node);
    /// assert_eq!(stats.num_nodes, 1);
    /// assert_eq!(stats.num_leaves, 1);
    /// ```
    pub fn accumulate(&mut self, node: &super::node::Node) {
        // Update node counts
        self.num_nodes += 1;
        if node.leaf {
            self.num_leaves += 1;
            self.total_key_value_pairs += node.len();
        } else {
            self.num_internal_nodes += 1;
        }

        // Update tree height
        self.tree_height = self.tree_height.max(node.level);

        // Calculate node size without allocating a serialized buffer.
        let node_size = node.encoded_len();

        // Update size statistics
        self.total_tree_size_bytes += node_size;
        self.min_node_size_bytes = self.min_node_size_bytes.min(node_size);
        self.max_node_size_bytes = self.max_node_size_bytes.max(node_size);

        // Update level-based statistics
        let level = node.level;
        *self.nodes_per_level.entry(level).or_insert(0) += 1;
        *self.level_total_sizes.entry(level).or_insert(0) += node_size;

        let num_entries = node.len();
        *self.level_total_entries.entry(level).or_insert(0) += num_entries;

        // Track level-based min/max for entries
        let current_min = self
            .min_entries_per_level
            .get(&level)
            .copied()
            .unwrap_or(usize::MAX);
        let current_max = self.max_entries_per_level.get(&level).copied().unwrap_or(0);
        self.min_entries_per_level
            .insert(level, current_min.min(num_entries));
        self.max_entries_per_level
            .insert(level, current_max.max(num_entries));

        // Calculate fill factor for this node
        let fill_factor = if node.max_chunk_size > 0 {
            if node.leaf {
                // For leaf nodes, fill factor is based on number of entries (keys)
                num_entries as f64 / node.max_chunk_size as f64
            } else {
                // For internal nodes, fill factor is based on fanout (number of children)
                node.vals.len() as f64 / node.max_chunk_size as f64
            }
        } else {
            0.0
        };
        self.total_fill_factor += fill_factor;

        // Update fanout statistics for internal nodes
        if !node.leaf {
            let fanout = node.vals.len();
            self.min_fanout = self.min_fanout.min(fanout);
            self.max_fanout = self.max_fanout.max(fanout);
            self.total_fanout += fanout;
            self.total_internal_fill_factor += fill_factor;
        } else {
            self.total_leaf_fill_factor += fill_factor;
        }

        // Update key/value size statistics for leaf nodes
        if node.leaf {
            for key in &node.keys {
                let key_size = key.len();
                self.total_keys_size_bytes += key_size;
                self.min_key_size_bytes = self.min_key_size_bytes.min(key_size);
                self.max_key_size_bytes = self.max_key_size_bytes.max(key_size);
            }
            for val in &node.vals {
                let val_size = val.len();
                self.total_values_size_bytes += val_size;
                self.min_value_size_bytes = self.min_value_size_bytes.min(val_size);
                self.max_value_size_bytes = self.max_value_size_bytes.max(val_size);
            }
        }
    }

    /// Finalize statistics by calculating all derived metrics
    ///
    /// This method should be called after all nodes have been accumulated.
    /// It calculates averages (node size, entries, fanout, fill factors, key/value sizes)
    /// and handles edge cases like division by zero.
    ///
    /// For empty trees, min values are reset to 0.
    ///
    /// # Example
    ///
    /// ```rust
    /// use prolly::{TreeStats, Node};
    ///
    /// let mut stats = TreeStats::new();
    /// let node = Node::builder()
    ///     .keys(vec![b"key".to_vec()])
    ///     .vals(vec![b"value".to_vec()])
    ///     .leaf(true)
    ///     .level(0)
    ///     .max_chunk_size(10)
    ///     .build();
    ///
    /// stats.accumulate(&node);
    /// stats.finalize();
    ///
    /// assert!(stats.avg_node_size_bytes > 0.0);
    /// assert_eq!(stats.avg_fill_factor, 0.1); // 1 entry / 10 max
    /// ```
    pub fn finalize(&mut self) {
        // Calculate average node size
        if self.num_nodes > 0 {
            self.avg_node_size_bytes = self.total_tree_size_bytes as f64 / self.num_nodes as f64;
            self.avg_entries_per_node = self.total_key_value_pairs as f64 / self.num_nodes as f64;
        }

        // Calculate average fanout
        if self.num_internal_nodes > 0 {
            self.avg_fanout = self.total_fanout as f64 / self.num_internal_nodes as f64;
        }

        // Calculate fill factors
        if self.num_nodes > 0 {
            self.avg_fill_factor = self.total_fill_factor / self.num_nodes as f64;
        }
        if self.num_leaves > 0 {
            self.avg_leaf_fill_factor = self.total_leaf_fill_factor / self.num_leaves as f64;
        }
        if self.num_internal_nodes > 0 {
            self.avg_internal_fill_factor =
                self.total_internal_fill_factor / self.num_internal_nodes as f64;
        }

        // Calculate average key/value sizes
        if self.total_key_value_pairs > 0 {
            self.avg_key_size_bytes =
                self.total_keys_size_bytes as f64 / self.total_key_value_pairs as f64;
            self.avg_value_size_bytes =
                self.total_values_size_bytes as f64 / self.total_key_value_pairs as f64;
        }

        // Calculate level-based averages
        for (level, count) in &self.nodes_per_level {
            if let Some(&total_size) = self.level_total_sizes.get(level) {
                self.avg_node_size_per_level
                    .insert(*level, total_size as f64 / *count as f64);
            }

            if let Some(&total_entries) = self.level_total_entries.get(level) {
                self.avg_entries_per_level
                    .insert(*level, total_entries as f64 / *count as f64);
            }
        }

        // Handle edge cases for min values (reset to 0 for empty trees)
        if self.num_nodes == 0 {
            self.min_node_size_bytes = 0;
            self.min_fanout = 0;
            self.min_key_size_bytes = 0;
            self.min_value_size_bytes = 0;
        }
    }

    /// Compute the difference between two statistics objects
    ///
    /// Returns a StatsDiff object containing the difference for each numeric metric.
    /// Positive values indicate an increase, negative values indicate a decrease.
    ///
    /// # Arguments
    /// * `other` - The statistics object to compare against (subtracted from self)
    ///
    /// # Returns
    /// A StatsDiff object with differences for all metrics
    pub fn diff(&self, other: &TreeStats) -> StatsDiff {
        StatsDiff {
            num_nodes_diff: self.num_nodes as i64 - other.num_nodes as i64,
            num_leaves_diff: self.num_leaves as i64 - other.num_leaves as i64,
            num_internal_nodes_diff: self.num_internal_nodes as i64
                - other.num_internal_nodes as i64,
            tree_height_diff: self.tree_height as i8 - other.tree_height as i8,
            total_key_value_pairs_diff: self.total_key_value_pairs as i64
                - other.total_key_value_pairs as i64,
            total_tree_size_bytes_diff: self.total_tree_size_bytes as i64
                - other.total_tree_size_bytes as i64,
            avg_node_size_bytes_diff: self.avg_node_size_bytes - other.avg_node_size_bytes,
            min_node_size_bytes_diff: self.min_node_size_bytes as i64
                - other.min_node_size_bytes as i64,
            max_node_size_bytes_diff: self.max_node_size_bytes as i64
                - other.max_node_size_bytes as i64,
            avg_entries_per_node_diff: self.avg_entries_per_node - other.avg_entries_per_node,
            avg_fanout_diff: self.avg_fanout - other.avg_fanout,
            min_fanout_diff: self.min_fanout as i64 - other.min_fanout as i64,
            max_fanout_diff: self.max_fanout as i64 - other.max_fanout as i64,
            avg_fill_factor_diff: self.avg_fill_factor - other.avg_fill_factor,
            avg_leaf_fill_factor_diff: self.avg_leaf_fill_factor - other.avg_leaf_fill_factor,
            avg_internal_fill_factor_diff: self.avg_internal_fill_factor
                - other.avg_internal_fill_factor,
            avg_key_size_bytes_diff: self.avg_key_size_bytes - other.avg_key_size_bytes,
            avg_value_size_bytes_diff: self.avg_value_size_bytes - other.avg_value_size_bytes,
            min_key_size_bytes_diff: self.min_key_size_bytes as i64
                - other.min_key_size_bytes as i64,
            max_key_size_bytes_diff: self.max_key_size_bytes as i64
                - other.max_key_size_bytes as i64,
            min_value_size_bytes_diff: self.min_value_size_bytes as i64
                - other.min_value_size_bytes as i64,
            max_value_size_bytes_diff: self.max_value_size_bytes as i64
                - other.max_value_size_bytes as i64,
            total_keys_size_bytes_diff: self.total_keys_size_bytes as i64
                - other.total_keys_size_bytes as i64,
            total_values_size_bytes_diff: self.total_values_size_bytes as i64
                - other.total_values_size_bytes as i64,
        }
    }

    /// Compute percentage changes between two statistics objects
    ///
    /// Returns a StatsPercentageChange object containing the percentage change for each metric.
    /// Percentage change is calculated as: ((self - other) / other) * 100
    /// Returns 0.0 for metrics where the base value (other) is zero.
    ///
    /// # Arguments
    /// * `other` - The baseline statistics object to compare against
    ///
    /// # Returns
    /// A StatsPercentageChange object with percentage changes for all metrics
    pub fn percentage_change(&self, other: &TreeStats) -> StatsPercentageChange {
        // Helper function to calculate percentage change with division by zero handling
        fn pct_change(current: f64, baseline: f64) -> f64 {
            if baseline == 0.0 {
                0.0
            } else {
                ((current - baseline) / baseline) * 100.0
            }
        }

        StatsPercentageChange {
            num_nodes_pct: pct_change(self.num_nodes as f64, other.num_nodes as f64),
            num_leaves_pct: pct_change(self.num_leaves as f64, other.num_leaves as f64),
            num_internal_nodes_pct: pct_change(
                self.num_internal_nodes as f64,
                other.num_internal_nodes as f64,
            ),
            tree_height_pct: pct_change(self.tree_height as f64, other.tree_height as f64),
            total_key_value_pairs_pct: pct_change(
                self.total_key_value_pairs as f64,
                other.total_key_value_pairs as f64,
            ),
            total_tree_size_bytes_pct: pct_change(
                self.total_tree_size_bytes as f64,
                other.total_tree_size_bytes as f64,
            ),
            avg_node_size_bytes_pct: pct_change(
                self.avg_node_size_bytes,
                other.avg_node_size_bytes,
            ),
            min_node_size_bytes_pct: pct_change(
                self.min_node_size_bytes as f64,
                other.min_node_size_bytes as f64,
            ),
            max_node_size_bytes_pct: pct_change(
                self.max_node_size_bytes as f64,
                other.max_node_size_bytes as f64,
            ),
            avg_entries_per_node_pct: pct_change(
                self.avg_entries_per_node,
                other.avg_entries_per_node,
            ),
            avg_fanout_pct: pct_change(self.avg_fanout, other.avg_fanout),
            min_fanout_pct: pct_change(self.min_fanout as f64, other.min_fanout as f64),
            max_fanout_pct: pct_change(self.max_fanout as f64, other.max_fanout as f64),
            avg_fill_factor_pct: pct_change(self.avg_fill_factor, other.avg_fill_factor),
            avg_leaf_fill_factor_pct: pct_change(
                self.avg_leaf_fill_factor,
                other.avg_leaf_fill_factor,
            ),
            avg_internal_fill_factor_pct: pct_change(
                self.avg_internal_fill_factor,
                other.avg_internal_fill_factor,
            ),
            avg_key_size_bytes_pct: pct_change(self.avg_key_size_bytes, other.avg_key_size_bytes),
            avg_value_size_bytes_pct: pct_change(
                self.avg_value_size_bytes,
                other.avg_value_size_bytes,
            ),
            min_key_size_bytes_pct: pct_change(
                self.min_key_size_bytes as f64,
                other.min_key_size_bytes as f64,
            ),
            max_key_size_bytes_pct: pct_change(
                self.max_key_size_bytes as f64,
                other.max_key_size_bytes as f64,
            ),
            min_value_size_bytes_pct: pct_change(
                self.min_value_size_bytes as f64,
                other.min_value_size_bytes as f64,
            ),
            max_value_size_bytes_pct: pct_change(
                self.max_value_size_bytes as f64,
                other.max_value_size_bytes as f64,
            ),
            total_keys_size_bytes_pct: pct_change(
                self.total_keys_size_bytes as f64,
                other.total_keys_size_bytes as f64,
            ),
            total_values_size_bytes_pct: pct_change(
                self.total_values_size_bytes as f64,
                other.total_values_size_bytes as f64,
            ),
        }
    }

    /// Update statistics after adding a node
    ///
    /// This method incrementally updates statistics when a new node is added to the tree.
    /// It updates all relevant counters, min/max values, and totals. After calling this method,
    /// you should call `finalize()` to recalculate derived metrics (averages).
    ///
    /// # Arguments
    /// * `node` - The node being added
    /// * `node_size_bytes` - The serialized size of the node in bytes
    pub fn add_node(&mut self, node: &super::node::Node, node_size_bytes: usize) {
        // Update node counts
        self.num_nodes += 1;
        if node.leaf {
            self.num_leaves += 1;
            self.total_key_value_pairs += node.len();
        } else {
            self.num_internal_nodes += 1;
        }

        // Update tree height
        self.tree_height = self.tree_height.max(node.level);

        // Update size statistics
        self.total_tree_size_bytes += node_size_bytes;
        self.min_node_size_bytes = self.min_node_size_bytes.min(node_size_bytes);
        self.max_node_size_bytes = self.max_node_size_bytes.max(node_size_bytes);

        // Update level-based statistics
        let level = node.level;
        *self.nodes_per_level.entry(level).or_insert(0) += 1;
        *self.level_total_sizes.entry(level).or_insert(0) += node_size_bytes;

        let num_entries = node.len();
        *self.level_total_entries.entry(level).or_insert(0) += num_entries;

        // Track level-based min/max for entries
        let current_min = self
            .min_entries_per_level
            .get(&level)
            .copied()
            .unwrap_or(usize::MAX);
        let current_max = self.max_entries_per_level.get(&level).copied().unwrap_or(0);
        self.min_entries_per_level
            .insert(level, current_min.min(num_entries));
        self.max_entries_per_level
            .insert(level, current_max.max(num_entries));

        // Calculate fill factor for this node
        let fill_factor = if node.max_chunk_size > 0 {
            if node.leaf {
                num_entries as f64 / node.max_chunk_size as f64
            } else {
                node.vals.len() as f64 / node.max_chunk_size as f64
            }
        } else {
            0.0
        };
        self.total_fill_factor += fill_factor;

        // Update fanout statistics for internal nodes
        if !node.leaf {
            let fanout = node.vals.len();
            self.min_fanout = self.min_fanout.min(fanout);
            self.max_fanout = self.max_fanout.max(fanout);
            self.total_fanout += fanout;
            self.total_internal_fill_factor += fill_factor;
        } else {
            self.total_leaf_fill_factor += fill_factor;
        }

        // Update key/value size statistics for leaf nodes
        if node.leaf {
            for key in &node.keys {
                let key_size = key.len();
                self.total_keys_size_bytes += key_size;
                self.min_key_size_bytes = self.min_key_size_bytes.min(key_size);
                self.max_key_size_bytes = self.max_key_size_bytes.max(key_size);
            }
            for val in &node.vals {
                let val_size = val.len();
                self.total_values_size_bytes += val_size;
                self.min_value_size_bytes = self.min_value_size_bytes.min(val_size);
                self.max_value_size_bytes = self.max_value_size_bytes.max(val_size);
            }
        }
    }

    /// Update statistics after removing a node
    ///
    /// This method incrementally updates statistics when a node is removed from the tree.
    /// It decrements all relevant counters and updates totals. Note that min/max values
    /// cannot be accurately updated without a full traversal, so they are left unchanged.
    /// After calling this method, you should call `finalize()` to recalculate derived metrics.
    ///
    /// # Arguments
    /// * `node` - The node being removed
    /// * `node_size_bytes` - The serialized size of the node in bytes
    pub fn remove_node(&mut self, node: &super::node::Node, node_size_bytes: usize) {
        // Update node counts
        self.num_nodes = self.num_nodes.saturating_sub(1);
        if node.leaf {
            self.num_leaves = self.num_leaves.saturating_sub(1);
            self.total_key_value_pairs = self.total_key_value_pairs.saturating_sub(node.len());
        } else {
            self.num_internal_nodes = self.num_internal_nodes.saturating_sub(1);
        }

        // Update size statistics
        self.total_tree_size_bytes = self.total_tree_size_bytes.saturating_sub(node_size_bytes);

        // Update level-based statistics
        let level = node.level;
        if let Some(count) = self.nodes_per_level.get_mut(&level) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.nodes_per_level.remove(&level);
            }
        }

        if let Some(total_size) = self.level_total_sizes.get_mut(&level) {
            *total_size = total_size.saturating_sub(node_size_bytes);
        }

        let num_entries = node.len();
        if let Some(total_entries) = self.level_total_entries.get_mut(&level) {
            *total_entries = total_entries.saturating_sub(num_entries);
        }

        // Calculate fill factor for this node
        let fill_factor = if node.max_chunk_size > 0 {
            if node.leaf {
                num_entries as f64 / node.max_chunk_size as f64
            } else {
                node.vals.len() as f64 / node.max_chunk_size as f64
            }
        } else {
            0.0
        };
        self.total_fill_factor = (self.total_fill_factor - fill_factor).max(0.0);

        // Update fanout statistics for internal nodes
        if !node.leaf {
            let fanout = node.vals.len();
            self.total_fanout = self.total_fanout.saturating_sub(fanout);
            self.total_internal_fill_factor =
                (self.total_internal_fill_factor - fill_factor).max(0.0);
        } else {
            self.total_leaf_fill_factor = (self.total_leaf_fill_factor - fill_factor).max(0.0);
        }

        // Update key/value size statistics for leaf nodes
        if node.leaf {
            for key in &node.keys {
                let key_size = key.len();
                self.total_keys_size_bytes = self.total_keys_size_bytes.saturating_sub(key_size);
            }
            for val in &node.vals {
                let val_size = val.len();
                self.total_values_size_bytes =
                    self.total_values_size_bytes.saturating_sub(val_size);
            }
        }

        // Note: min/max values cannot be accurately updated without full traversal
        // They are left unchanged and should be recalculated via full collection if needed
    }

    /// Update statistics after modifying a node
    ///
    /// This method incrementally updates statistics when a node is modified in place.
    /// It adjusts size totals and entry counts if they changed. Min/max values are updated
    /// if the new values exceed the current bounds. After calling this method, you should
    /// call `finalize()` to recalculate derived metrics.
    ///
    /// # Arguments
    /// * `old_node` - The node before modification
    /// * `new_node` - The node after modification
    /// * `old_size` - The serialized size of the old node in bytes
    /// * `new_size` - The serialized size of the new node in bytes
    pub fn update_node(
        &mut self,
        old_node: &super::node::Node,
        new_node: &super::node::Node,
        old_size: usize,
        new_size: usize,
    ) {
        // Update size statistics
        self.total_tree_size_bytes = self.total_tree_size_bytes.saturating_sub(old_size);
        self.total_tree_size_bytes += new_size;

        // Update min/max if new size exceeds bounds
        self.min_node_size_bytes = self.min_node_size_bytes.min(new_size);
        self.max_node_size_bytes = self.max_node_size_bytes.max(new_size);

        // Update level-based size statistics
        let level = new_node.level;
        if let Some(total_size) = self.level_total_sizes.get_mut(&level) {
            *total_size = total_size.saturating_sub(old_size);
            *total_size += new_size;
        }

        // Update entry counts if they changed
        let old_entries = old_node.len();
        let new_entries = new_node.len();

        if old_entries != new_entries {
            if new_node.leaf {
                self.total_key_value_pairs = self.total_key_value_pairs.saturating_sub(old_entries);
                self.total_key_value_pairs += new_entries;
            }

            // Update level-based entry statistics
            if let Some(total_entries) = self.level_total_entries.get_mut(&level) {
                *total_entries = total_entries.saturating_sub(old_entries);
                *total_entries += new_entries;
            }

            // Update level-based min/max for entries
            let current_min = self
                .min_entries_per_level
                .get(&level)
                .copied()
                .unwrap_or(usize::MAX);
            let current_max = self.max_entries_per_level.get(&level).copied().unwrap_or(0);
            self.min_entries_per_level
                .insert(level, current_min.min(new_entries));
            self.max_entries_per_level
                .insert(level, current_max.max(new_entries));
        }

        // Update fill factors
        let old_fill_factor = if old_node.max_chunk_size > 0 {
            if old_node.leaf {
                old_entries as f64 / old_node.max_chunk_size as f64
            } else {
                old_node.vals.len() as f64 / old_node.max_chunk_size as f64
            }
        } else {
            0.0
        };

        let new_fill_factor = if new_node.max_chunk_size > 0 {
            if new_node.leaf {
                new_entries as f64 / new_node.max_chunk_size as f64
            } else {
                new_node.vals.len() as f64 / new_node.max_chunk_size as f64
            }
        } else {
            0.0
        };

        self.total_fill_factor = (self.total_fill_factor - old_fill_factor).max(0.0);
        self.total_fill_factor += new_fill_factor;

        // Update fanout statistics for internal nodes
        if !new_node.leaf {
            let old_fanout = old_node.vals.len();
            let new_fanout = new_node.vals.len();

            self.total_fanout = self.total_fanout.saturating_sub(old_fanout);
            self.total_fanout += new_fanout;

            self.min_fanout = self.min_fanout.min(new_fanout);
            self.max_fanout = self.max_fanout.max(new_fanout);

            self.total_internal_fill_factor =
                (self.total_internal_fill_factor - old_fill_factor).max(0.0);
            self.total_internal_fill_factor += new_fill_factor;
        } else {
            self.total_leaf_fill_factor = (self.total_leaf_fill_factor - old_fill_factor).max(0.0);
            self.total_leaf_fill_factor += new_fill_factor;
        }

        // Update key/value size statistics for leaf nodes
        if new_node.leaf {
            // Remove old key/value sizes
            for key in &old_node.keys {
                let key_size = key.len();
                self.total_keys_size_bytes = self.total_keys_size_bytes.saturating_sub(key_size);
            }
            for val in &old_node.vals {
                let val_size = val.len();
                self.total_values_size_bytes =
                    self.total_values_size_bytes.saturating_sub(val_size);
            }

            // Add new key/value sizes
            for key in &new_node.keys {
                let key_size = key.len();
                self.total_keys_size_bytes += key_size;
                self.min_key_size_bytes = self.min_key_size_bytes.min(key_size);
                self.max_key_size_bytes = self.max_key_size_bytes.max(key_size);
            }
            for val in &new_node.vals {
                let val_size = val.len();
                self.total_values_size_bytes += val_size;
                self.min_value_size_bytes = self.min_value_size_bytes.min(val_size);
                self.max_value_size_bytes = self.max_value_size_bytes.max(val_size);
            }
        }
    }

    /// Validate that incremental statistics match full collection
    ///
    /// This method collects fresh statistics from the tree and compares them with
    /// the current statistics object. Returns true if they are equal, false otherwise.
    /// This is useful for verifying that incremental updates have been applied correctly.
    ///
    /// # Arguments
    /// * `prolly` - The Prolly instance to use for collecting statistics
    /// * `tree` - The tree to validate against
    ///
    /// # Returns
    /// * `Ok(true)` - Statistics match
    /// * `Ok(false)` - Statistics do not match
    /// * `Err(Error)` - On storage or deserialization errors
    pub fn validate<S: super::store::Store>(
        &self,
        prolly: &super::Prolly<S>,
        tree: &super::tree::Tree,
    ) -> Result<bool, super::error::Error> {
        // Collect fresh statistics
        let fresh_stats = prolly.collect_stats(tree)?;

        // Compare with self
        Ok(self == &fresh_stats)
    }
}

/// Helper function to format byte sizes with appropriate units
fn format_bytes(bytes: usize) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;

    if bytes == 0 {
        "0 B".to_string()
    } else if bytes < 1024 {
        format!("{} B", bytes)
    } else if (bytes as f64) < MB {
        format!("{:.2} KB", bytes as f64 / KB)
    } else {
        format!("{:.2} MB", bytes as f64 / MB)
    }
}

/// Combined statistics comparison between two tree snapshots.
///
/// `before` and `after` contain the full collected statistics for each tree.
/// `absolute` is computed as `after - before`, and `percentage` uses `before`
/// as the baseline.
///
/// Created by calling [`Prolly::stats_diff`](crate::prolly::Prolly::stats_diff)
/// or [`StatsComparison::new`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatsComparison {
    /// Statistics collected from the baseline tree.
    pub before: TreeStats,
    /// Statistics collected from the candidate/current tree.
    pub after: TreeStats,
    /// Absolute metric deltas computed as `after - before`.
    pub absolute: StatsDiff,
    /// Percentage metric deltas using `before` as the baseline.
    pub percentage: StatsPercentageChange,
}

impl StatsComparison {
    /// Create a comparison from already collected statistics.
    pub fn new(before: TreeStats, after: TreeStats) -> Self {
        let absolute = after.diff(&before);
        let percentage = after.percentage_change(&before);
        Self {
            before,
            after,
            absolute,
            percentage,
        }
    }
}

/// Difference between two statistics objects
///
/// Contains the absolute difference for each numeric metric between two [`TreeStats`] objects.
/// Positive values indicate an increase, negative values indicate a decrease.
///
/// Created by calling [`TreeStats::diff`].
///
/// # Example
///
/// ```rust
/// use prolly::{Config, MemStore, Prolly};
///
/// let store = MemStore::new();
/// let prolly = Prolly::new(store, Config::default());
///
/// let tree1 = prolly.create();
/// let stats1 = prolly.collect_stats(&tree1).unwrap();
///
/// let mut tree2 = tree1.clone();
/// tree2 = prolly.put(&tree2, b"key".to_vec(), b"value".to_vec()).unwrap();
/// let stats2 = prolly.collect_stats(&tree2).unwrap();
///
/// let diff = stats2.diff(&stats1);
/// assert!(diff.num_nodes_diff > 0); // Tree grew
/// assert!(diff.total_key_value_pairs_diff > 0); // Added entries
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatsDiff {
    pub num_nodes_diff: i64,
    pub num_leaves_diff: i64,
    pub num_internal_nodes_diff: i64,
    pub tree_height_diff: i8,
    pub total_key_value_pairs_diff: i64,
    pub total_tree_size_bytes_diff: i64,
    pub avg_node_size_bytes_diff: f64,
    pub min_node_size_bytes_diff: i64,
    pub max_node_size_bytes_diff: i64,
    pub avg_entries_per_node_diff: f64,
    pub avg_fanout_diff: f64,
    pub min_fanout_diff: i64,
    pub max_fanout_diff: i64,
    pub avg_fill_factor_diff: f64,
    pub avg_leaf_fill_factor_diff: f64,
    pub avg_internal_fill_factor_diff: f64,
    pub avg_key_size_bytes_diff: f64,
    pub avg_value_size_bytes_diff: f64,
    pub min_key_size_bytes_diff: i64,
    pub max_key_size_bytes_diff: i64,
    pub min_value_size_bytes_diff: i64,
    pub max_value_size_bytes_diff: i64,
    pub total_keys_size_bytes_diff: i64,
    pub total_values_size_bytes_diff: i64,
}

/// Percentage change between two statistics objects
///
/// Contains the percentage change for each numeric metric between two [`TreeStats`] objects.
/// Percentage change is calculated as: `((current - baseline) / baseline) * 100`
///
/// Returns 0.0 for metrics where the baseline value is zero (to avoid division by zero).
///
/// Created by calling [`TreeStats::percentage_change`].
///
/// # Example
///
/// ```rust
/// use prolly::{Config, MemStore, Prolly};
///
/// let store = MemStore::new();
/// let prolly = Prolly::new(store, Config::default());
///
/// // Build initial tree
/// let mut tree1 = prolly.create();
/// for i in 0..50 {
///     tree1 = prolly.put(&tree1, format!("k{}", i).into_bytes(), b"v".to_vec()).unwrap();
/// }
/// let stats1 = prolly.collect_stats(&tree1).unwrap();
///
/// // Grow tree
/// let mut tree2 = tree1.clone();
/// for i in 50..100 {
///     tree2 = prolly.put(&tree2, format!("k{}", i).into_bytes(), b"v".to_vec()).unwrap();
/// }
/// let stats2 = prolly.collect_stats(&tree2).unwrap();
///
/// let pct = stats2.percentage_change(&stats1);
/// assert!(pct.total_key_value_pairs_pct > 0.0); // Grew by some percentage
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatsPercentageChange {
    pub num_nodes_pct: f64,
    pub num_leaves_pct: f64,
    pub num_internal_nodes_pct: f64,
    pub tree_height_pct: f64,
    pub total_key_value_pairs_pct: f64,
    pub total_tree_size_bytes_pct: f64,
    pub avg_node_size_bytes_pct: f64,
    pub min_node_size_bytes_pct: f64,
    pub max_node_size_bytes_pct: f64,
    pub avg_entries_per_node_pct: f64,
    pub avg_fanout_pct: f64,
    pub min_fanout_pct: f64,
    pub max_fanout_pct: f64,
    pub avg_fill_factor_pct: f64,
    pub avg_leaf_fill_factor_pct: f64,
    pub avg_internal_fill_factor_pct: f64,
    pub avg_key_size_bytes_pct: f64,
    pub avg_value_size_bytes_pct: f64,
    pub min_key_size_bytes_pct: f64,
    pub max_key_size_bytes_pct: f64,
    pub min_value_size_bytes_pct: f64,
    pub max_value_size_bytes_pct: f64,
    pub total_keys_size_bytes_pct: f64,
    pub total_values_size_bytes_pct: f64,
}

impl std::fmt::Display for TreeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Tree Structure Statistics")?;
        writeln!(f, "=========================")?;
        writeln!(f)?;

        // Tree Structure Section
        writeln!(f, "Tree Structure:")?;
        writeln!(f, "  Total nodes:        {}", self.num_nodes)?;
        writeln!(f, "  Leaf nodes:         {}", self.num_leaves)?;
        writeln!(f, "  Internal nodes:     {}", self.num_internal_nodes)?;
        writeln!(f, "  Tree height:        {}", self.tree_height)?;
        writeln!(f, "  Key-value pairs:    {}", self.total_key_value_pairs)?;
        writeln!(f)?;

        // Size Statistics Section
        writeln!(f, "Size Statistics:")?;
        writeln!(
            f,
            "  Total tree size:    {}",
            format_bytes(self.total_tree_size_bytes)
        )?;
        writeln!(
            f,
            "  Avg node size:      {}",
            format_bytes(self.avg_node_size_bytes as usize)
        )?;
        writeln!(
            f,
            "  Min node size:      {}",
            format_bytes(self.min_node_size_bytes)
        )?;
        writeln!(
            f,
            "  Max node size:      {}",
            format_bytes(self.max_node_size_bytes)
        )?;
        writeln!(f, "  Avg entries/node:   {:.2}", self.avg_entries_per_node)?;
        writeln!(f)?;

        // Level Distribution Section
        if !self.nodes_per_level.is_empty() {
            writeln!(f, "Level Distribution:")?;
            writeln!(
                f,
                "  Level | Nodes | Avg Size      | Avg Entries | Min Entries | Max Entries"
            )?;
            writeln!(
                f,
                "  ------|-------|---------------|-------------|-------------|------------"
            )?;

            for level in 0..=self.tree_height {
                if let Some(&node_count) = self.nodes_per_level.get(&level) {
                    let avg_size = self
                        .avg_node_size_per_level
                        .get(&level)
                        .copied()
                        .unwrap_or(0.0);
                    let avg_entries = self
                        .avg_entries_per_level
                        .get(&level)
                        .copied()
                        .unwrap_or(0.0);
                    let min_entries = self.min_entries_per_level.get(&level).copied().unwrap_or(0);
                    let max_entries = self.max_entries_per_level.get(&level).copied().unwrap_or(0);

                    writeln!(
                        f,
                        "  {:5} | {:5} | {:13} | {:11.2} | {:11} | {:11}",
                        level,
                        node_count,
                        format_bytes(avg_size as usize),
                        avg_entries,
                        min_entries,
                        max_entries
                    )?;
                }
            }
            writeln!(f)?;
        }

        // Fanout and Fill Factor Section
        writeln!(f, "Fanout and Fill Factor:")?;
        writeln!(f, "  Avg fanout:         {:.2}", self.avg_fanout)?;
        writeln!(f, "  Min fanout:         {}", self.min_fanout)?;
        writeln!(f, "  Max fanout:         {}", self.max_fanout)?;
        writeln!(
            f,
            "  Avg fill factor:    {:.2}%",
            self.avg_fill_factor * 100.0
        )?;
        writeln!(
            f,
            "  Avg leaf fill:      {:.2}%",
            self.avg_leaf_fill_factor * 100.0
        )?;
        writeln!(
            f,
            "  Avg internal fill:  {:.2}%",
            self.avg_internal_fill_factor * 100.0
        )?;
        writeln!(f)?;

        // Key/Value Statistics Section
        writeln!(f, "Key/Value Statistics:")?;
        writeln!(
            f,
            "  Avg key size:       {}",
            format_bytes(self.avg_key_size_bytes as usize)
        )?;
        writeln!(
            f,
            "  Min key size:       {}",
            format_bytes(self.min_key_size_bytes)
        )?;
        writeln!(
            f,
            "  Max key size:       {}",
            format_bytes(self.max_key_size_bytes)
        )?;
        writeln!(
            f,
            "  Total keys size:    {}",
            format_bytes(self.total_keys_size_bytes)
        )?;
        writeln!(
            f,
            "  Avg value size:     {}",
            format_bytes(self.avg_value_size_bytes as usize)
        )?;
        writeln!(
            f,
            "  Min value size:     {}",
            format_bytes(self.min_value_size_bytes)
        )?;
        writeln!(
            f,
            "  Max value size:     {}",
            format_bytes(self.max_value_size_bytes)
        )?;
        writeln!(
            f,
            "  Total values size:  {}",
            format_bytes(self.total_values_size_bytes)
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::node::Node;
    use super::*;

    #[test]
    fn test_new_creates_zero_valued_stats() {
        let stats = TreeStats::new();

        // Basic structure
        assert_eq!(stats.num_nodes, 0);
        assert_eq!(stats.num_leaves, 0);
        assert_eq!(stats.num_internal_nodes, 0);
        assert_eq!(stats.tree_height, 0);
        assert_eq!(stats.total_key_value_pairs, 0);

        // Size statistics
        assert_eq!(stats.total_tree_size_bytes, 0);
        assert_eq!(stats.avg_node_size_bytes, 0.0);
        assert_eq!(stats.min_node_size_bytes, usize::MAX);
        assert_eq!(stats.max_node_size_bytes, 0);
        assert_eq!(stats.avg_entries_per_node, 0.0);

        // Level-based statistics
        assert!(stats.nodes_per_level.is_empty());
        assert!(stats.avg_node_size_per_level.is_empty());
        assert!(stats.avg_entries_per_level.is_empty());
        assert!(stats.min_entries_per_level.is_empty());
        assert!(stats.max_entries_per_level.is_empty());

        // Fanout and fill factor
        assert_eq!(stats.avg_fanout, 0.0);
        assert_eq!(stats.min_fanout, usize::MAX);
        assert_eq!(stats.max_fanout, 0);
        assert_eq!(stats.avg_fill_factor, 0.0);
        assert_eq!(stats.avg_leaf_fill_factor, 0.0);
        assert_eq!(stats.avg_internal_fill_factor, 0.0);

        // Key and value sizes
        assert_eq!(stats.avg_key_size_bytes, 0.0);
        assert_eq!(stats.avg_value_size_bytes, 0.0);
        assert_eq!(stats.min_key_size_bytes, usize::MAX);
        assert_eq!(stats.max_key_size_bytes, 0);
        assert_eq!(stats.min_value_size_bytes, usize::MAX);
        assert_eq!(stats.max_value_size_bytes, 0);
        assert_eq!(stats.total_keys_size_bytes, 0);
        assert_eq!(stats.total_values_size_bytes, 0);
    }

    #[test]
    fn test_default_matches_new() {
        let stats_new = TreeStats::new();
        let stats_default = TreeStats::default();

        assert_eq!(stats_new, stats_default);
    }

    #[test]
    fn test_all_fields_initialized_correctly() {
        let stats = TreeStats::new();

        // Verify numeric fields are zero or appropriate initial values
        assert_eq!(stats.num_nodes, 0);
        assert_eq!(stats.num_leaves, 0);
        assert_eq!(stats.num_internal_nodes, 0);
        assert_eq!(stats.tree_height, 0);
        assert_eq!(stats.total_key_value_pairs, 0);
        assert_eq!(stats.total_tree_size_bytes, 0);

        // Verify float fields are 0.0
        assert_eq!(stats.avg_node_size_bytes, 0.0);
        assert_eq!(stats.avg_entries_per_node, 0.0);
        assert_eq!(stats.avg_fanout, 0.0);
        assert_eq!(stats.avg_fill_factor, 0.0);
        assert_eq!(stats.avg_leaf_fill_factor, 0.0);
        assert_eq!(stats.avg_internal_fill_factor, 0.0);
        assert_eq!(stats.avg_key_size_bytes, 0.0);
        assert_eq!(stats.avg_value_size_bytes, 0.0);

        // Verify min values are usize::MAX (will be reset to 0 for empty trees)
        assert_eq!(stats.min_node_size_bytes, usize::MAX);
        assert_eq!(stats.min_fanout, usize::MAX);
        assert_eq!(stats.min_key_size_bytes, usize::MAX);
        assert_eq!(stats.min_value_size_bytes, usize::MAX);

        // Verify max values are 0
        assert_eq!(stats.max_node_size_bytes, 0);
        assert_eq!(stats.max_fanout, 0);
        assert_eq!(stats.max_key_size_bytes, 0);
        assert_eq!(stats.max_value_size_bytes, 0);

        // Verify totals are 0
        assert_eq!(stats.total_keys_size_bytes, 0);
        assert_eq!(stats.total_values_size_bytes, 0);

        // Verify BTreeMaps are empty
        assert!(stats.nodes_per_level.is_empty());
        assert!(stats.avg_node_size_per_level.is_empty());
        assert!(stats.avg_entries_per_level.is_empty());
        assert!(stats.min_entries_per_level.is_empty());
        assert!(stats.max_entries_per_level.is_empty());
    }

    #[test]
    fn test_accumulate_single_leaf_node() {
        let mut stats = TreeStats::new();

        let node = Node::builder()
            .keys(vec![b"key1".to_vec(), b"key2".to_vec()])
            .vals(vec![b"value1".to_vec(), b"value2".to_vec()])
            .leaf(true)
            .level(0)
            .build();

        stats.accumulate(&node);

        // Check node counts
        assert_eq!(stats.num_nodes, 1);
        assert_eq!(stats.num_leaves, 1);
        assert_eq!(stats.num_internal_nodes, 0);
        assert_eq!(stats.tree_height, 0);
        assert_eq!(stats.total_key_value_pairs, 2);

        // Check size statistics
        let node_size = node.to_bytes().len();
        assert_eq!(stats.total_tree_size_bytes, node_size);
        assert_eq!(stats.min_node_size_bytes, node_size);
        assert_eq!(stats.max_node_size_bytes, node_size);

        // Check level-based statistics
        assert_eq!(stats.nodes_per_level.get(&0), Some(&1));
        assert_eq!(stats.min_entries_per_level.get(&0), Some(&2));
        assert_eq!(stats.max_entries_per_level.get(&0), Some(&2));

        // Check fanout (should not be updated for leaf nodes)
        assert_eq!(stats.min_fanout, usize::MAX);
        assert_eq!(stats.max_fanout, 0);

        // Check key/value sizes
        assert_eq!(stats.total_keys_size_bytes, 4 + 4); // "key1" + "key2"
        assert_eq!(stats.total_values_size_bytes, 6 + 6); // "value1" + "value2"
        assert_eq!(stats.min_key_size_bytes, 4);
        assert_eq!(stats.max_key_size_bytes, 4);
        assert_eq!(stats.min_value_size_bytes, 6);
        assert_eq!(stats.max_value_size_bytes, 6);
    }

    #[test]
    fn test_accumulate_single_internal_node() {
        let mut stats = TreeStats::new();

        // Create an internal node with 3 children
        let node = Node::builder()
            .keys(vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()])
            .vals(vec![
                vec![0u8; 32], // CID 1
                vec![1u8; 32], // CID 2
                vec![2u8; 32], // CID 3
            ])
            .leaf(false)
            .level(1)
            .build();

        stats.accumulate(&node);

        // Check node counts
        assert_eq!(stats.num_nodes, 1);
        assert_eq!(stats.num_leaves, 0);
        assert_eq!(stats.num_internal_nodes, 1);
        assert_eq!(stats.tree_height, 1);
        assert_eq!(stats.total_key_value_pairs, 0); // Internal nodes don't have key-value pairs

        // Check size statistics
        let node_size = node.to_bytes().len();
        assert_eq!(stats.total_tree_size_bytes, node_size);
        assert_eq!(stats.min_node_size_bytes, node_size);
        assert_eq!(stats.max_node_size_bytes, node_size);

        // Check level-based statistics
        assert_eq!(stats.nodes_per_level.get(&1), Some(&1));
        assert_eq!(stats.min_entries_per_level.get(&1), Some(&3));
        assert_eq!(stats.max_entries_per_level.get(&1), Some(&3));

        // Check fanout (should be updated for internal nodes)
        assert_eq!(stats.min_fanout, 3);
        assert_eq!(stats.max_fanout, 3);

        // Check key/value sizes (should not be updated for internal nodes)
        assert_eq!(stats.total_keys_size_bytes, 0);
        assert_eq!(stats.total_values_size_bytes, 0);
        assert_eq!(stats.min_key_size_bytes, usize::MAX);
        assert_eq!(stats.max_key_size_bytes, 0);
        assert_eq!(stats.min_value_size_bytes, usize::MAX);
        assert_eq!(stats.max_value_size_bytes, 0);
    }

    #[test]
    fn test_accumulate_multiple_nodes() {
        let mut stats = TreeStats::new();

        // Add a leaf node
        let leaf1 = Node::builder()
            .keys(vec![b"a".to_vec()])
            .vals(vec![b"val_a".to_vec()])
            .leaf(true)
            .level(0)
            .build();

        stats.accumulate(&leaf1);

        // Add another leaf node
        let leaf2 = Node::builder()
            .keys(vec![b"b".to_vec(), b"c".to_vec()])
            .vals(vec![b"val_b".to_vec(), b"val_c".to_vec()])
            .leaf(true)
            .level(0)
            .build();

        stats.accumulate(&leaf2);

        // Add an internal node
        let internal = Node::builder()
            .keys(vec![b"b".to_vec()])
            .vals(vec![vec![0u8; 32], vec![1u8; 32]])
            .leaf(false)
            .level(1)
            .build();

        stats.accumulate(&internal);

        // Check node counts
        assert_eq!(stats.num_nodes, 3);
        assert_eq!(stats.num_leaves, 2);
        assert_eq!(stats.num_internal_nodes, 1);
        assert_eq!(stats.tree_height, 1);
        assert_eq!(stats.total_key_value_pairs, 3); // 1 + 2 from leaf nodes

        // Check size statistics
        let total_size =
            leaf1.to_bytes().len() + leaf2.to_bytes().len() + internal.to_bytes().len();
        assert_eq!(stats.total_tree_size_bytes, total_size);

        // Check level-based statistics
        assert_eq!(stats.nodes_per_level.get(&0), Some(&2));
        assert_eq!(stats.nodes_per_level.get(&1), Some(&1));

        // Check fanout
        assert_eq!(stats.min_fanout, 2);
        assert_eq!(stats.max_fanout, 2);

        // Check key/value sizes
        assert_eq!(stats.total_keys_size_bytes, 1 + 1 + 1); // "a" + "b" + "c"
        assert_eq!(stats.total_values_size_bytes, 5 + 5 + 5); // "val_a" + "val_b" + "val_c"
    }

    #[test]
    fn test_accumulate_min_max_updates() {
        let mut stats = TreeStats::new();

        // Add a small leaf node
        let small_leaf = Node::builder()
            .keys(vec![b"a".to_vec()])
            .vals(vec![b"x".to_vec()])
            .leaf(true)
            .level(0)
            .build();

        stats.accumulate(&small_leaf);

        let small_size = small_leaf.to_bytes().len();
        assert_eq!(stats.min_node_size_bytes, small_size);
        assert_eq!(stats.max_node_size_bytes, small_size);
        assert_eq!(stats.min_key_size_bytes, 1);
        assert_eq!(stats.max_key_size_bytes, 1);
        assert_eq!(stats.min_value_size_bytes, 1);
        assert_eq!(stats.max_value_size_bytes, 1);

        // Add a larger leaf node
        let large_leaf = Node::builder()
            .keys(vec![b"longer_key".to_vec()])
            .vals(vec![b"much_longer_value".to_vec()])
            .leaf(true)
            .level(0)
            .build();

        stats.accumulate(&large_leaf);

        let large_size = large_leaf.to_bytes().len();
        assert_eq!(stats.min_node_size_bytes, small_size);
        assert_eq!(stats.max_node_size_bytes, large_size);
        assert_eq!(stats.min_key_size_bytes, 1);
        assert_eq!(stats.max_key_size_bytes, 10); // "longer_key"
        assert_eq!(stats.min_value_size_bytes, 1);
        assert_eq!(stats.max_value_size_bytes, 17); // "much_longer_value"

        // Add internal nodes with different fanouts
        let small_fanout = Node::builder()
            .keys(vec![b"k".to_vec()])
            .vals(vec![vec![0u8; 32]])
            .leaf(false)
            .level(1)
            .build();

        stats.accumulate(&small_fanout);
        assert_eq!(stats.min_fanout, 1);
        assert_eq!(stats.max_fanout, 1);

        let large_fanout = Node::builder()
            .keys(vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()])
            .vals(vec![
                vec![0u8; 32],
                vec![1u8; 32],
                vec![2u8; 32],
                vec![3u8; 32],
            ])
            .leaf(false)
            .level(1)
            .build();

        stats.accumulate(&large_fanout);
        assert_eq!(stats.min_fanout, 1);
        assert_eq!(stats.max_fanout, 4);
    }

    #[test]
    fn test_finalize_with_zero_nodes() {
        let mut stats = TreeStats::new();

        // Finalize without accumulating any nodes
        stats.finalize();

        // All averages should be 0.0
        assert_eq!(stats.avg_node_size_bytes, 0.0);
        assert_eq!(stats.avg_entries_per_node, 0.0);
        assert_eq!(stats.avg_fanout, 0.0);
        assert_eq!(stats.avg_fill_factor, 0.0);
        assert_eq!(stats.avg_leaf_fill_factor, 0.0);
        assert_eq!(stats.avg_internal_fill_factor, 0.0);
        assert_eq!(stats.avg_key_size_bytes, 0.0);
        assert_eq!(stats.avg_value_size_bytes, 0.0);

        // Min values should be reset to 0
        assert_eq!(stats.min_node_size_bytes, 0);
        assert_eq!(stats.min_fanout, 0);
        assert_eq!(stats.min_key_size_bytes, 0);
        assert_eq!(stats.min_value_size_bytes, 0);

        // Max values should remain 0
        assert_eq!(stats.max_node_size_bytes, 0);
        assert_eq!(stats.max_fanout, 0);
        assert_eq!(stats.max_key_size_bytes, 0);
        assert_eq!(stats.max_value_size_bytes, 0);
    }

    #[test]
    fn test_finalize_with_only_leaf_nodes() {
        let mut stats = TreeStats::new();

        // Add two leaf nodes
        let leaf1 = Node::builder()
            .keys(vec![b"key1".to_vec()])
            .vals(vec![b"value1".to_vec()])
            .leaf(true)
            .level(0)
            .max_chunk_size(10)
            .build();

        let leaf2 = Node::builder()
            .keys(vec![b"key2".to_vec(), b"key3".to_vec()])
            .vals(vec![b"value2".to_vec(), b"value3".to_vec()])
            .leaf(true)
            .level(0)
            .max_chunk_size(10)
            .build();

        stats.accumulate(&leaf1);
        stats.accumulate(&leaf2);
        stats.finalize();

        // Check basic averages
        assert_eq!(stats.num_nodes, 2);
        assert_eq!(stats.num_leaves, 2);
        assert_eq!(stats.num_internal_nodes, 0);
        assert_eq!(stats.total_key_value_pairs, 3);

        let total_size = leaf1.to_bytes().len() + leaf2.to_bytes().len();
        assert_eq!(stats.avg_node_size_bytes, total_size as f64 / 2.0);
        assert_eq!(stats.avg_entries_per_node, 3.0 / 2.0);

        // Internal node averages should be 0 (no internal nodes)
        assert_eq!(stats.avg_fanout, 0.0);
        assert_eq!(stats.avg_internal_fill_factor, 0.0);

        // Leaf fill factor should be calculated
        let expected_leaf_fill = (1.0 / 10.0 + 2.0 / 10.0) / 2.0;
        assert!((stats.avg_leaf_fill_factor - expected_leaf_fill).abs() < 0.0001);

        // Overall fill factor should equal leaf fill factor
        assert!((stats.avg_fill_factor - expected_leaf_fill).abs() < 0.0001);

        // Key/value averages
        assert_eq!(stats.avg_key_size_bytes, (4 + 4 + 4) as f64 / 3.0);
        assert_eq!(stats.avg_value_size_bytes, (6 + 6 + 6) as f64 / 3.0);

        // Level-based averages
        assert_eq!(
            stats.avg_node_size_per_level.get(&0),
            Some(&(total_size as f64 / 2.0))
        );
        assert_eq!(stats.avg_entries_per_level.get(&0), Some(&(3.0 / 2.0)));
    }

    #[test]
    fn test_finalize_with_only_internal_nodes() {
        let mut stats = TreeStats::new();

        // Add two internal nodes
        let internal1 = Node::builder()
            .keys(vec![b"key1".to_vec()])
            .vals(vec![vec![0u8; 32], vec![1u8; 32]])
            .leaf(false)
            .level(1)
            .max_chunk_size(10)
            .build();

        let internal2 = Node::builder()
            .keys(vec![b"key2".to_vec(), b"key3".to_vec()])
            .vals(vec![vec![2u8; 32], vec![3u8; 32], vec![4u8; 32]])
            .leaf(false)
            .level(1)
            .max_chunk_size(10)
            .build();

        stats.accumulate(&internal1);
        stats.accumulate(&internal2);
        stats.finalize();

        // Check basic averages
        assert_eq!(stats.num_nodes, 2);
        assert_eq!(stats.num_leaves, 0);
        assert_eq!(stats.num_internal_nodes, 2);
        assert_eq!(stats.total_key_value_pairs, 0); // Internal nodes don't have key-value pairs

        let total_size = internal1.to_bytes().len() + internal2.to_bytes().len();
        assert_eq!(stats.avg_node_size_bytes, total_size as f64 / 2.0);

        // avg_entries_per_node should be 0 since total_key_value_pairs is 0
        assert_eq!(stats.avg_entries_per_node, 0.0);

        // Fanout averages
        assert_eq!(stats.avg_fanout, (2 + 3) as f64 / 2.0);

        // Internal fill factor should be calculated
        let expected_internal_fill = (2.0 / 10.0 + 3.0 / 10.0) / 2.0;
        assert!((stats.avg_internal_fill_factor - expected_internal_fill).abs() < 0.0001);

        // Overall fill factor should equal internal fill factor
        assert!((stats.avg_fill_factor - expected_internal_fill).abs() < 0.0001);

        // Leaf fill factor should be 0 (no leaf nodes)
        assert_eq!(stats.avg_leaf_fill_factor, 0.0);

        // Key/value averages should be 0 (no key-value pairs)
        assert_eq!(stats.avg_key_size_bytes, 0.0);
        assert_eq!(stats.avg_value_size_bytes, 0.0);
    }

    #[test]
    fn test_finalize_division_by_zero_handling() {
        let mut stats = TreeStats::new();

        // Create a node with max_chunk_size = 0 to test division by zero
        let node = Node::builder()
            .keys(vec![b"key".to_vec()])
            .vals(vec![b"value".to_vec()])
            .leaf(true)
            .level(0)
            .max_chunk_size(0)
            .build();

        stats.accumulate(&node);
        stats.finalize();

        // Should not panic, fill factors should be 0
        assert_eq!(stats.avg_fill_factor, 0.0);
        assert_eq!(stats.avg_leaf_fill_factor, 0.0);
        assert_eq!(stats.avg_internal_fill_factor, 0.0);

        // Other averages should still be calculated correctly
        assert!(stats.avg_node_size_bytes > 0.0);
        assert_eq!(stats.avg_entries_per_node, 1.0);
        assert_eq!(stats.avg_key_size_bytes, 3.0);
        assert_eq!(stats.avg_value_size_bytes, 5.0);
    }

    #[test]
    fn test_display_contains_expected_section_headers() {
        let mut stats = TreeStats::new();

        let node = Node::builder()
            .keys(vec![b"key".to_vec()])
            .vals(vec![b"value".to_vec()])
            .leaf(true)
            .level(0)
            .build();

        stats.accumulate(&node);
        stats.finalize();

        let display = format!("{}", stats);

        // Verify output contains expected section headers
        assert!(display.contains("Tree Structure Statistics"));
        assert!(display.contains("Tree Structure:"));
        assert!(display.contains("Size Statistics:"));
        assert!(display.contains("Level Distribution:"));
        assert!(display.contains("Fanout and Fill Factor:"));
        assert!(display.contains("Key/Value Statistics:"));
    }

    #[test]
    fn test_display_byte_size_formatting() {
        // Test the format_bytes helper function through display output
        let mut stats = TreeStats::new();

        // Create a node to get some size data
        let node = Node::builder()
            .keys(vec![b"key".to_vec()])
            .vals(vec![b"value".to_vec()])
            .leaf(true)
            .level(0)
            .build();

        stats.accumulate(&node);
        stats.finalize();

        let display = format!("{}", stats);

        // Should contain byte size formatting with units
        assert!(display.contains(" B") || display.contains(" KB") || display.contains(" MB"));

        // Test format_bytes directly
        assert_eq!(super::format_bytes(0), "0 B");
        assert_eq!(super::format_bytes(100), "100 B");
        assert_eq!(super::format_bytes(1024), "1.00 KB");
        assert_eq!(super::format_bytes(2048), "2.00 KB");
        assert_eq!(super::format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(super::format_bytes(1024 * 1024 * 2), "2.00 MB");
        assert_eq!(super::format_bytes(1536), "1.50 KB");
    }

    #[test]
    fn test_display_percentage_formatting() {
        let mut stats = TreeStats::new();

        let node = Node::builder()
            .keys(vec![b"key".to_vec()])
            .vals(vec![b"value".to_vec()])
            .leaf(true)
            .level(0)
            .max_chunk_size(10)
            .build();

        stats.accumulate(&node);
        stats.finalize();

        let display = format!("{}", stats);

        // Should contain percentage formatting with two decimal places
        // The fill factor should be displayed as a percentage
        assert!(display.contains("%"));

        // Check that percentages are formatted with two decimal places
        // The fill factor for this node should be 1/10 = 10.00%
        assert!(display.contains("10.00%"));
    }

    #[test]
    fn test_display_with_empty_tree() {
        let mut stats = TreeStats::new();
        stats.finalize();

        let display = format!("{}", stats);

        // Should contain all section headers even for empty tree
        assert!(display.contains("Tree Structure Statistics"));
        assert!(display.contains("Tree Structure:"));
        assert!(display.contains("Size Statistics:"));
        assert!(display.contains("Fanout and Fill Factor:"));
        assert!(display.contains("Key/Value Statistics:"));

        // Should show zero values
        assert!(display.contains("Total nodes:        0"));
        assert!(display.contains("Leaf nodes:         0"));
        assert!(display.contains("Internal nodes:     0"));
        assert!(display.contains("Tree height:        0"));
        assert!(display.contains("Key-value pairs:    0"));
        assert!(display.contains("Total tree size:    0 B"));

        // Level Distribution should not be present for empty tree
        assert!(!display.contains("Level | Nodes"));
    }

    #[test]
    fn test_display_with_populated_tree() {
        let mut stats = TreeStats::new();

        // Add multiple nodes at different levels
        let leaf1 = Node::builder()
            .keys(vec![b"key1".to_vec()])
            .vals(vec![b"value1".to_vec()])
            .leaf(true)
            .level(0)
            .max_chunk_size(10)
            .build();

        let leaf2 = Node::builder()
            .keys(vec![b"key2".to_vec(), b"key3".to_vec()])
            .vals(vec![b"value2".to_vec(), b"value3".to_vec()])
            .leaf(true)
            .level(0)
            .max_chunk_size(10)
            .build();

        let internal = Node::builder()
            .keys(vec![b"key".to_vec()])
            .vals(vec![vec![0u8; 32], vec![1u8; 32]])
            .leaf(false)
            .level(1)
            .max_chunk_size(10)
            .build();

        stats.accumulate(&leaf1);
        stats.accumulate(&leaf2);
        stats.accumulate(&internal);
        stats.finalize();

        let display = format!("{}", stats);

        // Should contain all section headers
        assert!(display.contains("Tree Structure Statistics"));
        assert!(display.contains("Tree Structure:"));
        assert!(display.contains("Size Statistics:"));
        assert!(display.contains("Level Distribution:"));
        assert!(display.contains("Fanout and Fill Factor:"));
        assert!(display.contains("Key/Value Statistics:"));

        // Should show correct counts
        assert!(display.contains("Total nodes:        3"));
        assert!(display.contains("Leaf nodes:         2"));
        assert!(display.contains("Internal nodes:     1"));
        assert!(display.contains("Tree height:        1"));
        assert!(display.contains("Key-value pairs:    3"));

        // Should have level distribution table
        assert!(display.contains("Level | Nodes"));
        assert!(display.contains("0"));
        assert!(display.contains("1"));

        // Should have fanout information
        assert!(display.contains("Avg fanout:"));
        assert!(display.contains("Min fanout:"));
        assert!(display.contains("Max fanout:"));

        // Should have fill factor percentages
        assert!(display.contains("Avg fill factor:"));
        assert!(display.contains("Avg leaf fill:"));
        assert!(display.contains("Avg internal fill:"));

        // Should have key/value statistics
        assert!(display.contains("Avg key size:"));
        assert!(display.contains("Min key size:"));
        assert!(display.contains("Max key size:"));
        assert!(display.contains("Total keys size:"));
        assert!(display.contains("Avg value size:"));
        assert!(display.contains("Min value size:"));
        assert!(display.contains("Max value size:"));
        assert!(display.contains("Total values size:"));
    }
}
