//! Prolly tree implementation - modular, trait-based architecture
//!
//! This module provides the main interface for working with Prolly trees through
//! the [`Prolly<S>`] struct. Prolly trees are content-addressable, probabilistically-balanced
//! search trees that combine the efficiency of B+ trees with deterministic merging capabilities.
//!
//! # Overview
//!
//! A Prolly tree is a persistent, immutable data structure that uses content-based chunking
//! to achieve structural sharing and efficient diffs. Key characteristics include:
//!
//! - **Immutable**: All operations return new trees, enabling safe concurrent access
//! - **Content-addressed**: Nodes are identified by their content hash (CID)
//! - **Deterministic**: Same content always produces the same tree structure
//! - **Efficient diffs**: Structural sharing enables fast comparisons between versions
//!
//! # Module Organization
//!
//! The implementation is organized into focused submodules for maintainability:
//!
//! - [`batch`] - Batch mutation operations for efficient bulk modifications
//! - [`diff`] - Tree diff and merge operations for version control semantics
//! - [`range`] - Range iteration for traversing key-value pairs in order
//! - [`rebalance`] - Tree rebalancing logic (node splitting and merging)
//! - [`utils`] - Shared utility functions used across modules
//! - `traits` - Internal trait definitions for future extensibility
//!
//! # Main Types
//!
//! - [`Prolly<S>`] - The main tree manager, generic over storage backend `S`
//! - [`RangeIter`] - Iterator for range queries over key-value pairs
//! - [`BatchWriteCollector`] - Collector for atomic batch writes
//! - [`LeafMutationGroup`] - Mutations grouped by target leaf for batch processing
//!
//! # Quick Start
//!
//! ```rust
//! use prolly::{Prolly, MemStore, Config};
//!
//! // Create a store and tree manager
//! let store = MemStore::new();
//! let prolly = Prolly::new(store, Config::default());
//!
//! // Create an empty tree
//! let tree = prolly.create();
//!
//! // Insert key-value pairs (returns a new tree - immutable)
//! let tree = prolly.put(&tree, b"key".to_vec(), b"value".to_vec()).unwrap();
//!
//! // Retrieve values
//! let value = prolly.get(&tree, b"key").unwrap();
//! assert_eq!(value, Some(b"value".to_vec()));
//!
//! // Delete keys
//! let tree = prolly.delete(&tree, b"key").unwrap();
//! assert!(prolly.get(&tree, b"key").unwrap().is_none());
//! ```
//!
//! # Architecture
//!
//! The `Prolly<S>` struct serves as the main API and delegates to specialized modules:
//!
//! - **Core operations** (get, put, delete) are implemented directly in this module
//! - **Rebalancing** is delegated to the [`rebalance`] module for node splitting/merging
//! - **Batch operations** are delegated to the [`batch`] module for atomic bulk updates
//! - **Range iteration** is delegated to the [`range`] module for ordered traversal
//! - **Diff/merge operations** are delegated to the [`diff`] module for version control
//!
//! This separation of concerns enables:
//! - Independent testing of each component
//! - Clear boundaries between responsibilities
//! - Future extensibility through trait-based interfaces
//!
//! # Storage Backend
//!
//! The tree is generic over a storage backend `S` that implements the [`Store`](crate::store::Store)
//! trait. This allows plugging in different storage implementations:
//!
//! - [`MemStore`](crate::store::MemStore) - In-memory storage for testing
//! - Custom implementations for persistent storage (databases, file systems, etc.)
//!
//! # Thread Safety
//!
//! The `Prolly<S>` struct is `Send` and `Sync` when the underlying store is. The immutable
//! nature of trees means multiple threads can safely read from the same tree simultaneously.

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const PARALLEL_NODE_DECODE_THRESHOLD: usize = 16;
const GET_MANY_PREFETCH_PARALLELISM: usize = 16;
const GET_MANY_BOUNDARY_ROUTE_MIN_POSITIONS: usize = 32;
const STATS_FRONTIER_PREFETCH_PARALLELISM: usize = 16;
#[cfg(feature = "async-store")]
const ASYNC_NODE_PREFETCH_BATCH_SIZE: usize = 64;

fn current_unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
}

// Core modules - moved from root level
pub mod boundary;
pub mod builder;
pub mod cid;
pub mod config;
pub mod cursor;
pub mod debug;
pub mod encoding;
pub mod error;
pub mod gc;
pub mod key;
pub mod manifest;
pub mod node;
pub mod policy;
pub mod proof;
pub mod snapshot;
pub mod stats;
pub mod store;
pub mod sync;
pub mod tombstone;
pub mod tree;
pub mod value;

// Public submodules - each handles a specific concern
pub mod batch;
pub mod blob;
pub mod crdt;
pub mod diff;
pub mod parallel;
pub mod range;
pub mod rebalance;
pub mod streaming;
pub mod utils;

// Internal traits for future extensibility (not exposed publicly)
mod traits;

use self::sync::{MissingNodeCopy, MissingNodePlan};
use blob::{BlobStore, BlobStoreScan, LargeValueConfig};
use cid::Cid;
use config::Config;
use encoding::INIT_LEVEL;
use error::Conflict;
use error::Diff;
use error::Error;
use error::Mutation;
use error::Resolver;
use gc::{BlobGcPlan, BlobGcReachability, BlobGcSweep, GcPlan, GcReachability, GcSweep};
use manifest::{
    ManifestStore, ManifestStoreScan, NamedRoot, NamedRootRetention, NamedRootSelection,
    NamedRootUpdate, RootManifest,
};
use node::Node;
use stats::{StatsComparison, TreeStats};
#[cfg(feature = "async-store")]
use store::AsyncStore;
use store::NodeStoreScan;
use store::Store;
use tree::Tree;

struct KeyLookupFrame {
    cid: Cid,
    positions: InlinePositions,
}

#[derive(Default)]
struct MissingNodeBatch {
    indexes: HashMap<Cid, usize>,
    cids: Vec<Cid>,
    positions: Vec<InlinePositions>,
}

type MissingNodeBytes = Vec<(Cid, Vec<u8>)>;
type PreparedMissingNodes = (MissingNodePlan, MissingNodeBytes);

struct InlinePositions {
    first: usize,
    rest: Vec<usize>,
}

impl InlinePositions {
    fn new(first: usize) -> Self {
        Self {
            first,
            rest: Vec::new(),
        }
    }

    fn with_rest_capacity(first: usize, rest_capacity: usize) -> Self {
        Self {
            first,
            rest: Vec::with_capacity(rest_capacity),
        }
    }

    fn from_vec(positions: Vec<usize>) -> Option<Self> {
        let mut iter = positions.into_iter();
        let first = iter.next()?;
        Some(Self {
            first,
            rest: iter.collect(),
        })
    }

    fn push(&mut self, position: usize) {
        self.rest.push(position);
    }

    fn len(&self) -> usize {
        1 + self.rest.len()
    }

    fn at(&self, offset: usize) -> usize {
        if offset == 0 {
            self.first
        } else {
            self.rest[offset - 1]
        }
    }
}

impl IntoIterator for InlinePositions {
    type Item = usize;
    type IntoIter = std::iter::Chain<std::iter::Once<usize>, std::vec::IntoIter<usize>>;

    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self.first).chain(self.rest)
    }
}

impl MissingNodeBatch {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            indexes: HashMap::with_capacity(capacity),
            cids: Vec::with_capacity(capacity),
            positions: Vec::with_capacity(capacity),
        }
    }

    fn record(&mut self, cid: &Cid, position: usize) {
        match self.indexes.entry(cid.clone()) {
            Entry::Occupied(entry) => {
                self.positions[*entry.get()].push(position);
            }
            Entry::Vacant(entry) => {
                let missing_idx = self.cids.len();
                self.cids.push(cid.clone());
                self.positions.push(InlinePositions::new(position));
                entry.insert(missing_idx);
            }
        }
    }
}

struct NodeCacheEntry {
    node: Arc<Node>,
    generation: u64,
    bytes: usize,
    pinned: bool,
}

struct NodeCache {
    max_nodes: Option<usize>,
    max_bytes: Option<usize>,
    nodes: HashMap<Cid, NodeCacheEntry>,
    access_log: VecDeque<(Cid, u64)>,
    next_generation: u64,
    bytes: usize,
}

impl NodeCache {
    fn new(max_nodes: Option<usize>, max_bytes: Option<usize>) -> Self {
        Self {
            max_nodes,
            max_bytes,
            nodes: HashMap::new(),
            access_log: VecDeque::new(),
            next_generation: 0,
            bytes: 0,
        }
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn bytes_len(&self) -> usize {
        self.bytes
    }

    fn pinned_len(&self) -> usize {
        self.nodes.values().filter(|entry| entry.pinned).count()
    }

    fn pinned_bytes_len(&self) -> usize {
        self.nodes
            .values()
            .filter(|entry| entry.pinned)
            .map(|entry| entry.bytes)
            .sum()
    }

    fn clear(&mut self) -> usize {
        let evicted = self.nodes.len();
        self.nodes.clear();
        self.access_log.clear();
        self.bytes = 0;
        evicted
    }

    fn get(&mut self, cid: &Cid) -> Option<Arc<Node>> {
        if !self.nodes.contains_key(cid) {
            return None;
        }

        let generation = self.next_generation();
        let node = {
            let entry = self
                .nodes
                .get_mut(cid)
                .expect("node was checked before generation update");
            entry.generation = generation;
            entry.node.clone()
        };
        self.access_log.push_back((cid.clone(), generation));
        self.compact_access_log_if_needed();
        Some(node)
    }

    fn insert(&mut self, cid: Cid, node: Arc<Node>, bytes: usize) -> usize {
        self.insert_with_pin(cid, node, bytes, false).1
    }

    fn insert_pinned(&mut self, cid: Cid, node: Arc<Node>, bytes: usize) -> (bool, usize) {
        self.insert_with_pin(cid, node, bytes, true)
    }

    fn insert_with_pin(
        &mut self,
        cid: Cid,
        node: Arc<Node>,
        bytes: usize,
        pinned: bool,
    ) -> (bool, usize) {
        if self.max_nodes == Some(0) || self.max_bytes == Some(0) {
            return (false, self.clear());
        }

        let generation = self.next_generation();
        let mut newly_pinned = pinned;
        if let Some(previous) = self.nodes.insert(
            cid.clone(),
            NodeCacheEntry {
                node,
                generation,
                bytes,
                pinned,
            },
        ) {
            newly_pinned = pinned && !previous.pinned;
            let entry = self
                .nodes
                .get_mut(&cid)
                .expect("entry was inserted before preserving pin state");
            entry.pinned = previous.pinned || pinned;
            self.bytes = self.bytes.saturating_sub(previous.bytes);
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.access_log.push_back((cid, generation));

        let evicted = self.evict_to_limit();
        self.compact_access_log_if_needed();
        (newly_pinned, evicted)
    }

    fn pin_existing(&mut self, cid: &Cid) -> bool {
        let Some(entry) = self.nodes.get_mut(cid) else {
            return false;
        };
        let was_pinned = entry.pinned;
        entry.pinned = true;
        !was_pinned
    }

    fn unpin_all(&mut self) -> (usize, usize) {
        let mut unpinned = 0;
        for entry in self.nodes.values_mut() {
            if entry.pinned {
                entry.pinned = false;
                unpinned += 1;
            }
        }
        let evicted = self.evict_to_limit();
        self.compact_access_log_if_needed();
        (unpinned, evicted)
    }

    fn next_generation(&mut self) -> u64 {
        self.next_generation = self.next_generation.wrapping_add(1);
        self.next_generation
    }

    fn evict_to_limit(&mut self) -> usize {
        let mut evicted = 0;
        let mut scanned_without_eviction = 0usize;
        while self.exceeds_limit() {
            let Some((cid, generation)) = self.access_log.pop_front() else {
                break;
            };

            let Some(entry) = self.nodes.get(&cid) else {
                continue;
            };
            if entry.generation != generation {
                continue;
            }

            if entry.pinned {
                self.access_log.push_back((cid, generation));
                scanned_without_eviction += 1;
                if scanned_without_eviction >= self.access_log.len() {
                    break;
                }
                continue;
            }

            evicted += self.remove_entry(&cid);
            scanned_without_eviction = 0;
        }
        evicted
    }

    fn exceeds_limit(&self) -> bool {
        self.max_nodes
            .map(|max_nodes| self.nodes.len() > max_nodes)
            .unwrap_or(false)
            || self
                .max_bytes
                .map(|max_bytes| self.bytes > max_bytes)
                .unwrap_or(false)
    }

    fn remove_entry(&mut self, cid: &Cid) -> usize {
        if let Some(entry) = self.nodes.remove(cid) {
            self.bytes = self.bytes.saturating_sub(entry.bytes);
            1
        } else {
            0
        }
    }

    fn compact_access_log_if_needed(&mut self) {
        let max_log_len = self.nodes.len().saturating_mul(4).max(64);
        if self.access_log.len() <= max_log_len {
            return;
        }

        self.access_log.retain(|(cid, generation)| {
            self.nodes
                .get(cid)
                .map(|entry| entry.generation == *generation)
                .unwrap_or(false)
        });
    }
}

/// Cumulative cache and node I/O metrics for a prolly manager.
///
/// These counters are store-neutral observations from the tree manager's point
/// of view. Cache hits are requested node slots served from the in-process
/// cache. Cache misses are unique node CIDs fetched from the backing store.
/// Byte counters use serialized node sizes before backend-specific compression,
/// buffering, or object layout.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProllyMetricsSnapshot {
    /// Requested node slots served from the in-process node cache.
    pub node_cache_hits: u64,
    /// Unique node CIDs fetched from the backing store.
    pub node_cache_misses: u64,
    /// Decoded nodes evicted from the in-process node cache.
    pub node_cache_evictions: u64,
    /// Serialized nodes read from the backing store.
    pub nodes_read: u64,
    /// Serialized node bytes read from the backing store.
    pub bytes_read: u64,
    /// Serialized nodes written to the backing store.
    pub nodes_written: u64,
    /// Serialized node bytes written to the backing store.
    pub bytes_written: u64,
    /// Successful point-read calls made by the manager.
    pub store_get_calls: u64,
    /// Successful ordered batch-read calls made by the manager.
    pub store_batch_get_calls: u64,
    /// Unique node keys requested through ordered batch reads.
    pub store_batch_get_keys: u64,
    /// Successful point-write calls made by the manager.
    pub store_put_calls: u64,
    /// Successful batch-write calls made by the manager.
    pub store_batch_put_calls: u64,
    /// Unique serialized nodes passed through batch writes.
    pub store_batch_put_nodes: u64,
}

#[derive(Default)]
struct ProllyMetrics {
    node_cache_hits: AtomicU64,
    node_cache_misses: AtomicU64,
    node_cache_evictions: AtomicU64,
    nodes_read: AtomicU64,
    bytes_read: AtomicU64,
    nodes_written: AtomicU64,
    bytes_written: AtomicU64,
    store_get_calls: AtomicU64,
    store_batch_get_calls: AtomicU64,
    store_batch_get_keys: AtomicU64,
    store_put_calls: AtomicU64,
    store_batch_put_calls: AtomicU64,
    store_batch_put_nodes: AtomicU64,
}

impl ProllyMetrics {
    fn snapshot(&self) -> ProllyMetricsSnapshot {
        ProllyMetricsSnapshot {
            node_cache_hits: self.node_cache_hits.load(Ordering::Relaxed),
            node_cache_misses: self.node_cache_misses.load(Ordering::Relaxed),
            node_cache_evictions: self.node_cache_evictions.load(Ordering::Relaxed),
            nodes_read: self.nodes_read.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            nodes_written: self.nodes_written.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            store_get_calls: self.store_get_calls.load(Ordering::Relaxed),
            store_batch_get_calls: self.store_batch_get_calls.load(Ordering::Relaxed),
            store_batch_get_keys: self.store_batch_get_keys.load(Ordering::Relaxed),
            store_put_calls: self.store_put_calls.load(Ordering::Relaxed),
            store_batch_put_calls: self.store_batch_put_calls.load(Ordering::Relaxed),
            store_batch_put_nodes: self.store_batch_put_nodes.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.node_cache_hits.store(0, Ordering::Relaxed);
        self.node_cache_misses.store(0, Ordering::Relaxed);
        self.node_cache_evictions.store(0, Ordering::Relaxed);
        self.nodes_read.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.nodes_written.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.store_get_calls.store(0, Ordering::Relaxed);
        self.store_batch_get_calls.store(0, Ordering::Relaxed);
        self.store_batch_get_keys.store(0, Ordering::Relaxed);
        self.store_put_calls.store(0, Ordering::Relaxed);
        self.store_batch_put_calls.store(0, Ordering::Relaxed);
        self.store_batch_put_nodes.store(0, Ordering::Relaxed);
    }

    fn add_cache_hits(&self, count: usize) {
        add_metric(&self.node_cache_hits, count);
    }

    fn add_cache_misses(&self, count: usize) {
        add_metric(&self.node_cache_misses, count);
    }

    fn add_cache_evictions(&self, count: usize) {
        add_metric(&self.node_cache_evictions, count);
    }

    fn record_point_read(&self, bytes: usize) {
        add_metric(&self.store_get_calls, 1);
        add_metric(&self.nodes_read, 1);
        add_metric(&self.bytes_read, bytes);
    }

    fn record_batch_read(&self, keys: usize, loaded_bytes: usize, loaded_nodes: usize) {
        add_metric(&self.store_batch_get_calls, 1);
        add_metric(&self.store_batch_get_keys, keys);
        add_metric(&self.nodes_read, loaded_nodes);
        add_metric(&self.bytes_read, loaded_bytes);
    }

    fn record_point_write(&self, bytes: usize) {
        add_metric(&self.store_put_calls, 1);
        add_metric(&self.nodes_written, 1);
        add_metric(&self.bytes_written, bytes);
    }

    fn record_batch_write(&self, nodes: usize, bytes: usize) {
        if nodes == 0 {
            return;
        }

        add_metric(&self.store_batch_put_calls, 1);
        add_metric(&self.store_batch_put_nodes, nodes);
        add_metric(&self.nodes_written, nodes);
        add_metric(&self.bytes_written, bytes);
    }
}

fn add_metric(counter: &AtomicU64, value: usize) {
    if value > 0 {
        counter.fetch_add(value as u64, Ordering::Relaxed);
    }
}

fn loaded_node_totals(loaded: &[Option<Vec<u8>>]) -> (usize, usize) {
    loaded
        .iter()
        .filter_map(|bytes| bytes.as_ref())
        .fold((0, 0), |(nodes, bytes), value| {
            (nodes + 1, bytes + value.len())
        })
}

#[cfg(feature = "async-store")]
async fn async_batch_get_ordered_unique_bounded<S>(
    store: &S,
    keys: &[&[u8]],
    max_batch_len: usize,
) -> Result<Vec<Option<Vec<u8>>>, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let max_batch_len = max_batch_len.max(1);
    if keys.len() <= max_batch_len {
        let values = store
            .batch_get_ordered_unique(keys)
            .await
            .map_err(|err| Error::Store(Box::new(err)))?;
        if values.len() != keys.len() {
            return Err(Error::InvalidNode);
        }
        return Ok(values);
    }

    let mut values = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(max_batch_len) {
        let chunk_values = store
            .batch_get_ordered_unique(chunk)
            .await
            .map_err(|err| Error::Store(Box::new(err)))?;
        if chunk_values.len() != chunk.len() {
            return Err(Error::InvalidNode);
        }
        values.extend(chunk_values);
    }
    Ok(values)
}

/// Prolly tree manager
///
/// Provides the high-level API for working with Prolly trees.
/// Generic over the storage backend `S`.
///
/// # Example
/// ```
/// use prolly::{Prolly, MemStore, Config};
///
/// let store = MemStore::new();
/// let prolly = Prolly::new(store, Config::default());
/// let tree = prolly.create();
/// ```
pub struct Prolly<S: Store> {
    store: S,
    config: Config,
    node_cache: RwLock<NodeCache>,
    rightmost_path_cache: RwLock<Option<(Cid, Vec<CachedRightmostPathEntry>)>>,
    metrics: ProllyMetrics,
}

/// Async Prolly tree manager.
///
/// `AsyncProlly` is available behind the `async-store` feature. It keeps the
/// synchronous [`Prolly`] API untouched while allowing remote, browser, and
/// object-store backends to serve tree reads without blocking on the core
/// `Store` trait.
///
/// The async surface covers reads, writes, range scans, diff, merge, CRDT merge,
/// stats, cache pinning, large-value helpers, and route-planned batch mutation
/// without requiring a Tokio dependency.
#[cfg(feature = "async-store")]
pub struct AsyncProlly<S: AsyncStore> {
    store: S,
    config: Config,
    node_cache: RwLock<NodeCache>,
    rightmost_path_cache: RwLock<Option<(Cid, Vec<CachedRightmostPathEntry>)>>,
    metrics: ProllyMetrics,
}

#[cfg(feature = "async-store")]
struct AsyncWriteCollector {
    nodes: Vec<(Cid, Vec<u8>)>,
    seen_cids: HashSet<Cid>,
    cache_nodes: Vec<(Cid, Node)>,
}

#[cfg(feature = "async-store")]
struct AsyncBuildNodeSummary {
    cid: Cid,
    first_key: Vec<u8>,
}

#[cfg(feature = "async-store")]
struct AsyncBatchLeafGroup {
    leaf: Node,
    route_path: Option<Arc<AsyncBatchRoutePath>>,
    mutations: Arc<Vec<Mutation>>,
    range: Range<usize>,
}

#[cfg(feature = "async-store")]
struct AsyncBatchRouteFrame {
    cid: Cid,
    path: Option<Arc<AsyncBatchRoutePath>>,
    mutations: Arc<Vec<Mutation>>,
    range: Range<usize>,
}

#[cfg(feature = "async-store")]
struct AsyncBatchRoutePath {
    parent: Option<Arc<AsyncBatchRoutePath>>,
    node: Arc<Node>,
    cid: Cid,
    child_index: usize,
}

#[cfg(feature = "async-store")]
#[derive(Clone)]
struct AsyncBatchChildRef {
    cid: Cid,
    first_key: Vec<u8>,
    level: u8,
}

#[cfg(feature = "async-store")]
type AsyncBatchChildReplacements = Vec<(usize, Vec<AsyncBatchChildRef>)>;

#[cfg(feature = "async-store")]
#[derive(Clone)]
struct AsyncBatchParentLink {
    parent_cid: Cid,
    child_index: usize,
}

#[cfg(feature = "async-store")]
#[derive(Clone)]
struct AsyncBatchAncestorContext {
    node: Node,
    parent: Option<AsyncBatchParentLink>,
}

#[cfg(feature = "async-store")]
struct AsyncBatchApplyResult {
    root: Option<Cid>,
    changed_leaves: usize,
}

#[cfg(feature = "async-store")]
#[derive(Clone)]
struct AsyncRightmostPathEntry {
    cid: Cid,
    node: Node,
    child_index: usize,
}

#[cfg(feature = "async-store")]
struct AsyncAppendTreeUpdate {
    root: Cid,
    rightmost_path: Vec<AsyncRightmostPathEntry>,
}

#[cfg(feature = "async-store")]
const RIGHTMOST_PATH_HINT_NAMESPACE: &[u8] = b"prolly:rightmost-path:v1";

#[cfg(feature = "async-store")]
#[derive(Serialize, Deserialize)]
struct AsyncRightmostPathHint {
    version: u8,
    entries: Vec<AsyncRightmostPathHintEntry>,
}

#[cfg(feature = "async-store")]
#[derive(Serialize, Deserialize)]
struct AsyncRightmostPathHintEntry {
    cid: Cid,
    child_index: usize,
}

#[cfg(feature = "async-store")]
impl AsyncWriteCollector {
    fn new_cached() -> Self {
        Self {
            nodes: Vec::new(),
            seen_cids: HashSet::new(),
            cache_nodes: Vec::new(),
        }
    }

    fn add(&mut self, node: &Node) -> Cid {
        let bytes = node.to_bytes();
        let cid = Cid::from_bytes(&bytes);
        if !self.seen_cids.insert(cid.clone()) {
            return cid;
        }

        self.cache_nodes.push((cid.clone(), node.clone()));
        self.nodes.push((cid.clone(), bytes));
        cid
    }

    fn add_many(&mut self, nodes: Vec<Node>) -> Vec<Cid> {
        nodes.iter().map(|node| self.add(node)).collect()
    }

    async fn flush<S>(&self, store: &S) -> Result<(), Error>
    where
        S: AsyncStore,
        S::Error: Send + Sync,
    {
        if self.nodes.is_empty() {
            return Ok(());
        }

        let entries = self
            .nodes
            .iter()
            .map(|(cid, bytes)| (cid.as_bytes(), bytes.as_slice()))
            .collect::<Vec<_>>();
        store
            .batch_put(&entries)
            .await
            .map_err(|e| Error::Store(Box::new(e)))
    }

    async fn flush_with_hint<S>(
        &self,
        store: &S,
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Error>
    where
        S: AsyncStore,
        S::Error: Send + Sync,
    {
        let entries = self
            .nodes
            .iter()
            .map(|(cid, bytes)| (cid.as_bytes(), bytes.as_slice()))
            .collect::<Vec<_>>();
        store
            .batch_put_with_hint(&entries, namespace, key, value)
            .await
            .map_err(|e| Error::Store(Box::new(e)))
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn bytes_len(&self) -> usize {
        self.nodes.iter().map(|(_, bytes)| bytes.len()).sum()
    }

    fn cache_nodes<S: AsyncStore>(&self, prolly: &AsyncProlly<S>) {
        let mut evictions = 0;
        if let Ok(mut cache) = prolly.node_cache.write() {
            for (cid, node) in &self.cache_nodes {
                evictions += cache.insert(cid.clone(), Arc::new(node.clone()), node.encoded_len());
            }
        }
        prolly.metrics.add_cache_evictions(evictions);
    }
}

#[derive(Clone)]
pub(crate) struct CachedRightmostPathEntry {
    pub cid: Cid,
    pub node: Node,
    pub child_index: usize,
}

/// Half-open key range that was recently changed.
///
/// `start` is inclusive and `end` is exclusive. `end == None` means the span
/// extends to the end of the keyspace. Changed-span hints are performance
/// sidecars for sync and indexing jobs; they do not change tree semantics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangedSpan {
    /// Inclusive lower key bound.
    pub start: Vec<u8>,
    /// Exclusive upper key bound, or `None` for the rest of the keyspace.
    pub end: Option<Vec<u8>>,
}

impl ChangedSpan {
    /// Create a half-open changed span `[start, end)`.
    pub fn new(start: impl Into<Vec<u8>>, end: Option<Vec<u8>>) -> Self {
        Self {
            start: start.into(),
            end,
        }
    }

    /// Create a changed span for one exact key.
    pub fn from_key(key: impl Into<Vec<u8>>) -> Self {
        let start = key.into();
        let mut end = start.clone();
        end.push(0);
        Self {
            start,
            end: Some(end),
        }
    }

    /// Create a changed span for every key under `prefix`.
    pub fn for_prefix(prefix: impl Into<Vec<u8>>) -> Self {
        let start = prefix.into();
        let end = key::prefix_end(&start);
        Self { start, end }
    }
}

/// Recently changed key spans for a specific root transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangedSpanHint {
    /// Root before the change.
    pub base_root: Option<Cid>,
    /// Root after the change.
    pub changed_root: Option<Cid>,
    /// Normalized, sorted, non-overlapping changed spans.
    pub spans: Vec<ChangedSpan>,
}

#[derive(Serialize, Deserialize)]
struct ChangedSpanHintWire {
    version: u8,
    base_root: Option<Cid>,
    changed_root: Option<Cid>,
    spans: Vec<ChangedSpan>,
}

struct PrefixPathHintEntryWithNode {
    cid: Cid,
    node: Arc<Node>,
    child_index: usize,
}

const PREFIX_PATH_HINT_NAMESPACE: &[u8] = b"prolly:prefix-path:v1";
const PREFIX_PATH_HINT_VERSION: u8 = 1;
const CHANGED_SPANS_HINT_NAMESPACE: &[u8] = b"prolly:changed-spans:v1";
const CHANGED_SPANS_HINT_VERSION: u8 = 1;

#[derive(Serialize, Deserialize)]
struct PrefixPathHint {
    version: u8,
    root: Cid,
    prefix: Vec<u8>,
    entries: Vec<PrefixPathHintEntry>,
}

#[derive(Serialize, Deserialize)]
struct PrefixPathHintEntry {
    cid: Cid,
    child_index: usize,
}

fn is_rightmost_append_path(path: &[(Node, usize)], key: &[u8]) -> bool {
    let Some((leaf, _)) = path.last() else {
        return true;
    };

    if !leaf.leaf || leaf.search(key) != Err(leaf.len()) {
        return false;
    }

    path.iter()
        .all(|(node, child_index)| *child_index + 1 == node.len())
}

impl<S: Store> Prolly<S> {
    /// Create a new Prolly tree manager with the given store and configuration.
    ///
    /// # Arguments
    /// * `store` - Storage backend implementing the `Store` trait
    /// * `config` - Tree configuration (chunking parameters, encoding, etc.)
    pub fn new(store: S, config: Config) -> Self {
        let node_cache_max_nodes = config.node_cache_max_nodes;
        let node_cache_max_bytes = config.node_cache_max_bytes;
        Self {
            store,
            config,
            node_cache: RwLock::new(NodeCache::new(node_cache_max_nodes, node_cache_max_bytes)),
            rightmost_path_cache: RwLock::new(None),
            metrics: ProllyMetrics::default(),
        }
    }

    /// Create a new empty tree.
    ///
    /// Returns a `Tree` with no root (empty tree).
    pub fn create(&self) -> Tree {
        Tree {
            root: None,
            config: self.config.clone(),
        }
    }

    /// Build a tree from key/value entries using [`BatchBuilder`].
    ///
    /// The builder sorts entries by byte-lexicographic key order before
    /// chunking, so callers may provide unsorted input. Duplicate keys are
    /// preserved with the same semantics as [`BatchBuilder`].
    pub fn build_from_entries(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<Tree, Error>
    where
        S: Clone + Send + Sync,
        S::Error: Send + Sync,
    {
        let mut builder = builder::BatchBuilder::new(self.store.clone(), self.config.clone());
        for (key, value) in entries {
            builder.add(key, value);
        }
        builder.build()
    }

    /// Build a tree from entries that are already sorted by key.
    ///
    /// This delegates to [`SortedBatchBuilder`] and returns
    /// [`Error::UnsortedInput`] if any key is lower than the previous key.
    pub fn build_from_sorted_entries(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<Tree, Error>
    where
        S: Clone + Send + Sync,
        S::Error: Send + Sync,
    {
        let mut builder = builder::SortedBatchBuilder::new(self.store.clone(), self.config.clone());
        for (key, value) in entries {
            builder.add(key, value)?;
        }
        builder.build()
    }

    /// Get value by key from the tree.
    ///
    /// Traverses from root to leaf using binary search at each level.
    ///
    /// # Arguments
    /// * `tree` - The tree to search
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// * `Ok(Some(value))` if the key exists
    /// * `Ok(None)` if the key does not exist
    /// * `Err` on storage or deserialization errors
    pub fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let Some(root_cid) = &tree.root else {
            return Ok(None);
        };

        let mut cid = root_cid.clone();
        loop {
            let node = self.load_arc(&cid)?;
            let idx = match node.search(key) {
                Ok(i) => i,
                Err(i) => {
                    if i == 0 {
                        return Ok(None);
                    } else {
                        i - 1
                    }
                }
            };

            if node.leaf {
                if node.keys.get(idx).map(|k| k.as_slice()) == Some(key) {
                    return Ok(Some(leaf_value_at(&node, idx)?));
                }
                return Ok(None);
            }

            // Descend to child
            cid = child_cid_at(&node, idx)?;
        }
    }

    /// Get a stored large-value reference by key.
    ///
    /// Non-envelope values are returned as [`blob::ValueRef::Inline`], so this
    /// can inspect trees that mix ordinary raw values and offloaded blob
    /// references.
    pub fn get_value_ref(&self, tree: &Tree, key: &[u8]) -> Result<Option<blob::ValueRef>, Error> {
        self.get(tree, key)?
            .map(|value| blob::ValueRef::from_stored_bytes(&value))
            .transpose()
    }

    /// Get a value by key, resolving offloaded blob references when present.
    pub fn get_large_value<B>(
        &self,
        blob_store: &B,
        tree: &Tree,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, Error>
    where
        B: BlobStore,
    {
        self.get(tree, key)?
            .map(|value| blob::resolve_stored_value(blob_store, &value))
            .transpose()
    }

    /// Get multiple values from the tree while preserving caller order.
    ///
    /// This descends the tree level-by-level and batches node loads for the
    /// current lookup frontier. It is useful for random read-after-write
    /// verification and merge conflict checks because shared ancestors and
    /// leaves are loaded once instead of once per key.
    ///
    /// Duplicate keys are allowed; each output slot corresponds to the input
    /// key at the same index.
    ///
    /// # Arguments
    /// * `tree` - The tree to search
    /// * `keys` - Keys to look up
    ///
    /// # Returns
    /// A vector of values in the same order as `keys`.
    ///
    /// # Example
    /// ```rust
    /// use prolly::{Config, MemStore, Prolly};
    ///
    /// let prolly = Prolly::new(MemStore::new(), Config::default());
    /// let tree = prolly.create();
    /// let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
    /// let tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
    ///
    /// let values = prolly.get_many(&tree, &[b"b".to_vec(), b"missing".to_vec(), b"a".to_vec()]).unwrap();
    /// assert_eq!(values, vec![Some(b"2".to_vec()), None, Some(b"1".to_vec())]);
    /// ```
    pub fn get_many<K: AsRef<[u8]>>(
        &self,
        tree: &Tree,
        keys: &[K],
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let mut values = vec![None; keys.len()];
        let Some(root_cid) = &tree.root else {
            return Ok(values);
        };

        if keys.is_empty() {
            return Ok(values);
        }

        let positions = InlinePositions::from_vec(sorted_key_positions(keys))
            .expect("keys is non-empty after early return");

        let mut frames = vec![KeyLookupFrame {
            cid: root_cid.clone(),
            positions,
        }];

        while !frames.is_empty() {
            let cids = frames
                .iter()
                .map(|frame| frame.cid.clone())
                .collect::<Vec<_>>();
            let nodes = if self.store.prefers_batch_reads() {
                self.load_many_ordered_with_parallelism(&cids, GET_MANY_PREFETCH_PARALLELISM)?
            } else {
                self.load_many_ordered(&cids)?
            };
            let mut next_frames = Vec::new();

            for (frame, node) in frames.into_iter().zip(nodes) {
                if node.leaf {
                    fill_leaf_lookup_values(&node, frame.positions, keys, &mut values)?;
                    continue;
                }

                next_frames.extend(route_key_positions_to_children(
                    &node,
                    frame.positions,
                    keys,
                )?);
            }

            frames = next_frames;
        }

        Ok(values)
    }

    /// Insert or update a key-value pair in the tree.
    ///
    /// This operation is immutable - it returns a new tree rather than
    /// modifying the existing one.
    ///
    /// # Arguments
    /// * `tree` - The tree to modify
    /// * `key` - The key to insert/update
    /// * `val` - The value to associate with the key
    ///
    /// # Returns
    /// * `Ok(new_tree)` with the updated tree
    /// * `Err` on storage or deserialization errors
    ///
    /// # Idempotence
    /// If the key already exists with the same value, returns the original tree unchanged.
    pub fn put(&self, tree: &Tree, key: Vec<u8>, val: Vec<u8>) -> Result<Tree, Error> {
        // Build path to leaf
        let path = self.find_path(tree, &key)?;

        if is_rightmost_append_path(&path, &key) {
            return batch::append_upsert_with_path(self, tree, key, val, &path);
        }

        // Insert into leaf
        let mut node = path
            .last()
            .map(|(n, _)| n.clone())
            .unwrap_or_else(|| self.new_leaf_node());

        match node.search(&key) {
            Ok(i) => {
                if node.vals[i] == val {
                    return Ok(tree.clone()); // No change (idempotent)
                }
                node.vals[i] = val;
            }
            Err(i) => {
                node.keys.insert(i, key);
                node.vals.insert(i, val);
            }
        }

        // Rebalance and persist the O(height) changed path atomically. This
        // keeps random updates localized while avoiding one disk transaction
        // per rewritten node on durable stores.
        let mut collector = batch::BatchWriteCollector::new_cached();
        let new_root = rebalance::rebalance_with_collector(
            self,
            node,
            &path[..path.len().saturating_sub(1)],
            &mut collector,
        )?
        .ok_or(Error::InvalidNode)?;
        collector.flush(self.store())?;
        self.record_batch_write_metrics(collector.len(), collector.bytes_len());
        collector.cache_nodes(self)?;

        Ok(Tree {
            root: Some(new_root),
            config: tree.config.clone(),
        })
    }

    /// Insert or update a value, offloading large payloads to a blob store.
    ///
    /// Values larger than `config.inline_threshold` are written to `blob_store`
    /// and represented in the tree by a compact content-addressed reference.
    /// Smaller values are stored as raw leaf bytes unless they start with the
    /// value-reference magic prefix, in which case they are escaped with an
    /// inline envelope.
    pub fn put_large_value<B>(
        &self,
        blob_store: &B,
        tree: &Tree,
        key: Vec<u8>,
        value: Vec<u8>,
        config: LargeValueConfig,
    ) -> Result<Tree, Error>
    where
        B: BlobStore,
    {
        let stored = blob::encode_stored_value(blob_store, value, &config)?;
        self.put(tree, key, stored)
    }

    /// Delete a key from the tree.
    ///
    /// This operation is immutable - it returns a new tree rather than
    /// modifying the existing one.
    ///
    /// # Arguments
    /// * `tree` - The tree to modify
    /// * `key` - The key to delete
    ///
    /// # Returns
    /// * `Ok(new_tree)` with the key removed (or unchanged if key didn't exist)
    /// * `Err` on storage or deserialization errors
    ///
    /// # Idempotence
    /// If the key doesn't exist, returns the original tree unchanged.
    pub fn delete(&self, tree: &Tree, key: &[u8]) -> Result<Tree, Error> {
        let Some(_) = &tree.root else {
            return Ok(tree.clone());
        };

        let path = self.find_path(tree, key)?;
        let Some((mut node, _)) = path.last().cloned() else {
            return Ok(tree.clone());
        };

        // Remove from leaf
        if let Ok(i) = node.search(key) {
            node.keys.remove(i);
            node.vals.remove(i);
        } else {
            return Ok(tree.clone()); // Key not found (idempotent)
        }

        // Handle empty tree
        if node.is_empty() && path.len() == 1 {
            return Ok(Tree {
                root: None,
                config: tree.config.clone(),
            });
        }

        // Rebalance and persist the O(height) changed path atomically, matching
        // the localized write behavior used by put.
        let mut collector = batch::BatchWriteCollector::new_cached();
        let new_root = rebalance::rebalance_with_collector(
            self,
            node,
            &path[..path.len().saturating_sub(1)],
            &mut collector,
        )?;
        collector.flush(self.store())?;
        self.record_batch_write_metrics(collector.len(), collector.bytes_len());
        collector.cache_nodes(self)?;

        Ok(Tree {
            root: new_root,
            config: tree.config.clone(),
        })
    }

    /// Iterate over a range of key-value pairs.
    ///
    /// Returns an iterator that yields `(key, value)` pairs in lexicographic order,
    /// starting from `start` (inclusive) up to `end` (exclusive). Supports both
    /// inclusive start bounds and exclusive end bounds.
    ///
    /// # Arguments
    /// * `tree` - The tree to iterate over
    /// * `start` - The starting key (inclusive). Use `&[]` to start from the beginning.
    /// * `end` - Optional ending key (exclusive). Use `None` to iterate to the end.
    ///
    /// # Returns
    /// * `Ok(RangeIter)` - An iterator over the range
    /// * `Err` on storage or deserialization errors during path finding
    ///
    /// # Example
    /// ```
    /// use prolly::{Prolly, MemStore, Config};
    ///
    /// let store = MemStore::new();
    /// let prolly = Prolly::new(store, Config::default());
    /// let tree = prolly.create();
    ///
    /// // Insert some data
    /// let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
    /// let tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
    /// let tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();
    ///
    /// // Iterate over all keys
    /// for result in prolly.range(&tree, &[], None).unwrap() {
    ///     let (key, val) = result.unwrap();
    ///     println!("{:?} -> {:?}", key, val);
    /// }
    ///
    /// // Iterate over a specific range [b, c)
    /// for result in prolly.range(&tree, b"b", Some(b"c")).unwrap() {
    ///     let (key, val) = result.unwrap();
    ///     println!("{:?} -> {:?}", key, val);
    /// }
    /// ```
    pub fn range<'a>(
        &'a self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<range::RangeIter<'a, S>, Error> {
        range::create_range_iter(self, tree, start, end)
    }

    /// Create a range iterator that resumes strictly after `after_key`.
    ///
    /// Persist the last key successfully processed, then resume with this
    /// method to avoid yielding that key again. The `end` bound remains
    /// exclusive.
    pub fn range_after<'a>(
        &'a self,
        tree: &Tree,
        after_key: &[u8],
        end: Option<&[u8]>,
    ) -> Result<range::RangeIter<'a, S>, Error> {
        range::create_range_after_iter(self, tree, after_key, end)
    }

    /// Create a range iterator from a stable range cursor.
    pub fn range_from_cursor<'a>(
        &'a self,
        tree: &Tree,
        cursor: &range::RangeCursor,
        end: Option<&[u8]>,
    ) -> Result<range::RangeIter<'a, S>, Error> {
        match cursor.after() {
            Some(after_key) => self.range_after(tree, after_key, end),
            None => self.range(tree, &[], end),
        }
    }

    /// Read a bounded page from a range scan.
    ///
    /// `cursor` is either [`RangeCursor::start`](crate::RangeCursor::start) or
    /// a cursor returned by a previous page. `end` is exclusive. When `limit`
    /// is zero this returns an empty page with the original cursor so callers
    /// can treat zero-sized requests as no-ops.
    pub fn range_page(
        &self,
        tree: &Tree,
        cursor: &range::RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<range::RangePage, Error> {
        if limit == 0 {
            return Ok(range::RangePage {
                entries: Vec::new(),
                next_cursor: Some(cursor.clone()),
            });
        }

        let mut iter = self.range_from_cursor(tree, cursor, end)?;
        let mut entries = Vec::with_capacity(limit);

        for _ in 0..limit {
            let Some(item) = iter.next() else {
                return Ok(range::RangePage {
                    entries,
                    next_cursor: None,
                });
            };
            entries.push(item?);
        }

        let next_cursor = entries
            .last()
            .map(|(key, _)| range::RangeCursor::after_key(key.clone()));
        Ok(range::RangePage {
            entries,
            next_cursor,
        })
    }

    /// Compute the difference between two trees.
    ///
    /// Returns a vector of `Diff` entries representing the changes needed to
    /// transform `base` into `other`. Yields Added entries for keys that exist in
    /// other but not in base, Changed entries for keys with different values, and
    /// Removed entries for keys that exist in base but not in other.
    ///
    /// # Arguments
    /// * `base` - The base tree to compare from
    /// * `other` - The other tree to compare to
    ///
    /// # Returns
    /// * `Ok(Vec<Diff>)` - A vector of differences
    /// * `Err` on storage or deserialization errors
    ///
    /// # Short-circuit
    /// If both trees have the same root CID, returns an empty vector immediately.
    ///
    /// # Example
    /// ```
    /// use prolly::{Prolly, MemStore, Config};
    ///
    /// let store = MemStore::new();
    /// let prolly = Prolly::new(store, Config::default());
    /// let base = prolly.create();
    ///
    /// let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
    /// let other = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
    ///
    /// let diffs = prolly.diff(&base, &other).unwrap();
    /// // diffs contains Added { key: b"b", val: b"2" }
    /// ```
    pub fn diff(&self, base: &Tree, other: &Tree) -> Result<Vec<Diff>, Error> {
        diff::compute_diff(self, base, other)
    }

    /// Compute the difference between two trees within a half-open key range.
    ///
    /// Returns only changes whose key is in `[start, end)`. Unlike collecting
    /// two ranges and comparing them, this walks the tree shape directly and
    /// skips equal or out-of-range subtrees by CID and key span.
    pub fn range_diff(
        &self,
        base: &Tree,
        other: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Vec<Diff>, Error> {
        diff::compute_range_diff(self, base, other, start, end)
    }

    /// Compute diffs from a stable cursor.
    ///
    /// This resumes strictly after the cursor key, so callers can persist the
    /// last processed diff key and avoid re-processing it on the next scan.
    /// `end` remains an exclusive upper bound.
    pub fn diff_from_cursor(
        &self,
        base: &Tree,
        other: &Tree,
        cursor: &range::RangeCursor,
        end: Option<&[u8]>,
    ) -> Result<Vec<Diff>, Error> {
        let start = cursor.after().unwrap_or(&[]);
        let mut diffs = self.range_diff(base, other, start, end)?;
        if let Some(after_key) = cursor.after() {
            diffs.retain(|diff| diff.key() > after_key);
        }
        Ok(diffs)
    }

    /// Read a bounded page of diffs from a stable cursor.
    ///
    /// `cursor` is either [`RangeCursor::start`](crate::RangeCursor::start) or
    /// a cursor returned by a previous page. `end` is exclusive. When `limit`
    /// is zero this returns an empty page with the original cursor so callers
    /// can treat zero-sized requests as no-ops.
    pub fn diff_page(
        &self,
        base: &Tree,
        other: &Tree,
        cursor: &range::RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<diff::DiffPage, Error> {
        if limit == 0 {
            return Ok(diff::DiffPage {
                diffs: Vec::new(),
                next_cursor: Some(cursor.clone()),
            });
        }

        let mut diffs = self.diff_from_cursor(base, other, cursor, end)?;
        let has_more = diffs.len() > limit;
        if has_more {
            diffs.truncate(limit);
        }

        let next_cursor = if has_more {
            diffs
                .last()
                .map(|diff| range::RangeCursor::after_key(diff.key().to_vec()))
        } else {
            None
        };

        Ok(diff::DiffPage { diffs, next_cursor })
    }

    /// Read a bounded page from the structural diff traversal.
    ///
    /// This preserves the CID frontier between pages instead of resuming from
    /// a key. It is better suited to long-running sync or indexing jobs where
    /// preserving subtree pruning across checkpoints matters. Pass `None` to
    /// start, then pass the returned cursor until `next_cursor` is `None`.
    pub fn structural_diff_page(
        &self,
        base: &Tree,
        other: &Tree,
        cursor: Option<&diff::StructuralDiffCursor>,
        limit: usize,
    ) -> Result<diff::StructuralDiffPage, Error> {
        diff::structural_diff_page(self, base, other, cursor, limit)
    }

    /// Merge two trees using three-way merge.
    ///
    /// Performs a three-way merge using `base` as the common ancestor.
    /// Changes from both `left` and `right` are combined into a single tree.
    /// Uses the diff algorithm to efficiently identify entries to add.
    ///
    /// # Arguments
    /// * `base` - The common ancestor tree
    /// * `left` - The left branch tree
    /// * `right` - The right branch tree
    /// * `resolver` - Optional conflict resolver function
    ///
    /// # Returns
    /// * `Ok(merged_tree)` - The merged tree
    /// * `Err(Error::Conflict)` - If a conflict occurs and no resolver is provided
    ///   or the resolver returns `Resolution::Unresolved`
    ///
    /// # Conflict Handling
    /// A conflict occurs when both `left` and `right` modify the same key differently
    /// from `base`. When this happens:
    /// - If a resolver is provided, it's called with the conflict information
    /// - If the resolver returns `Resolution::Value`, that value is used
    /// - If the resolver returns `Resolution::Delete`, the key is removed
    /// - If the resolver returns `Resolution::Unresolved` or no resolver is provided,
    ///   an error is returned
    ///
    /// Keys that have the same value in both trees are included once in the result.
    ///
    /// # Example
    /// ```
    /// use prolly::{Prolly, MemStore, Config};
    ///
    /// let store = MemStore::new();
    /// let prolly = Prolly::new(store, Config::default());
    /// let base = prolly.create();
    ///
    /// // Create base tree
    /// let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
    ///
    /// // Create divergent branches
    /// let left = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
    /// let right = prolly.put(&base, b"c".to_vec(), b"3".to_vec()).unwrap();
    ///
    /// // Merge without conflicts
    /// let merged = prolly.merge(&base, &left, &right, None).unwrap();
    ///
    /// // Merged tree has all keys
    /// assert!(prolly.get(&merged, b"a").unwrap().is_some());
    /// assert!(prolly.get(&merged, b"b").unwrap().is_some());
    /// assert!(prolly.get(&merged, b"c").unwrap().is_some());
    /// ```
    pub fn merge(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        resolver: Option<Resolver>,
    ) -> Result<Tree, Error> {
        diff::merge_trees(self, base, left, right, resolver)
    }

    /// Perform a three-way merge and return structured diagnostic trace events.
    ///
    /// This is the diagnostics-oriented counterpart to [`Prolly::merge`]. The
    /// returned [`crate::MergeExplanation`] keeps its trace even when the merge
    /// result is an error, which is useful for custom resolver debugging and
    /// sync-job observability.
    ///
    /// # Example
    /// ```
    /// use prolly::{Config, MemStore, MergeTraceEvent, Prolly};
    ///
    /// let prolly = Prolly::new(MemStore::new(), Config::default());
    /// let base = prolly
    ///     .put(&prolly.create(), b"a".to_vec(), b"1".to_vec())
    ///     .unwrap();
    /// let left = prolly
    ///     .put(&base, b"b".to_vec(), b"2".to_vec())
    ///     .unwrap();
    /// let right = prolly
    ///     .put(&base, b"c".to_vec(), b"3".to_vec())
    ///     .unwrap();
    ///
    /// let explanation = prolly.merge_explain(&base, &left, &right, None);
    /// assert!(explanation
    ///     .trace
    ///     .events
    ///     .iter()
    ///     .any(|event| matches!(event, MergeTraceEvent::BatchMerge { .. })));
    ///
    /// let merged = explanation.into_result().unwrap();
    /// assert!(prolly.get(&merged, b"c").unwrap().is_some());
    /// ```
    pub fn merge_explain(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        resolver: Option<Resolver>,
    ) -> diff::MergeExplanation {
        diff::merge_trees_explain(self, base, left, right, resolver)
    }

    /// Merge only right-side changes whose keys are in `[start, end)`.
    ///
    /// Keys outside the range are left exactly as they are in `left`. Conflict
    /// detection and resolver behavior match [`Prolly::merge`], but only for
    /// keys inside the requested range.
    ///
    /// # Example
    /// ```
    /// use prolly::{Config, MemStore, Prolly};
    ///
    /// let prolly = Prolly::new(MemStore::new(), Config::default());
    /// let base = prolly
    ///     .put(&prolly.create(), b"doc/1/title".to_vec(), b"old".to_vec())
    ///     .unwrap();
    /// let left = prolly
    ///     .put(&base, b"doc/2/title".to_vec(), b"local".to_vec())
    ///     .unwrap();
    /// let right = prolly
    ///     .put(&base, b"doc/1/title".to_vec(), b"remote".to_vec())
    ///     .unwrap();
    ///
    /// let merged = prolly
    ///     .merge_range(&base, &left, &right, b"doc/1/", Some(b"doc/2/"), None)
    ///     .unwrap();
    ///
    /// assert_eq!(
    ///     prolly.get(&merged, b"doc/1/title").unwrap(),
    ///     Some(b"remote".to_vec())
    /// );
    /// assert_eq!(
    ///     prolly.get(&merged, b"doc/2/title").unwrap(),
    ///     Some(b"local".to_vec())
    /// );
    /// ```
    pub fn merge_range(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        resolver: Option<Resolver>,
    ) -> Result<Tree, Error> {
        diff::merge_trees_range(self, base, left, right, start, end, resolver)
    }

    /// Merge only right-side changes whose keys start with `prefix`.
    ///
    /// This is a convenience wrapper over [`Prolly::merge_range`] using the
    /// lexicographic prefix bounds from [`crate::prefix_range`].
    pub fn merge_prefix(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        prefix: &[u8],
        resolver: Option<Resolver>,
    ) -> Result<Tree, Error> {
        let (start, end) = key::prefix_range(prefix);
        self.merge_range(base, left, right, &start, end.as_deref(), resolver)
    }

    /// Collect comprehensive statistics about a tree
    ///
    /// Traverses the entire tree once, gathering metrics about structure,
    /// size, distribution, and efficiency.
    ///
    /// # Arguments
    /// * `tree` - The tree to analyze
    ///
    /// # Returns
    /// * `Ok(TreeStats)` - Collected statistics
    /// * `Err(Error)` - On storage or deserialization errors
    ///
    /// # Example
    /// ```
    /// use prolly::{Prolly, MemStore, Config};
    ///
    /// let store = MemStore::new();
    /// let prolly = Prolly::new(store, Config::default());
    /// let tree = prolly.create();
    ///
    /// let tree = prolly.put(&tree, b"key".to_vec(), b"value".to_vec()).unwrap();
    /// let stats = prolly.collect_stats(&tree).unwrap();
    /// println!("{:?}", stats);
    /// ```
    pub fn collect_stats(&self, tree: &Tree) -> Result<TreeStats, Error> {
        // Handle empty tree case
        let Some(root_cid) = &tree.root else {
            let mut stats = TreeStats::new();
            stats.finalize();
            return Ok(stats);
        };

        // Initialize TreeStats
        let mut stats = TreeStats::new();

        self.collect_stats_from_frontier(root_cid, &mut stats)?;

        // Finalize statistics
        stats.finalize();

        // Return result
        Ok(stats)
    }

    /// Return a deterministic debug view of the tree grouped by level.
    ///
    /// This is intended for diagnostics, CLI inspection, and tests that need
    /// to inspect tree shape without depending on private node traversal code.
    /// Levels are ordered from root to leaves, and each node includes its CID,
    /// entry count, fill factor, encoded size, and key range.
    ///
    /// # Example
    /// ```
    /// use prolly::{Config, MemStore, Prolly};
    ///
    /// let prolly = Prolly::new(MemStore::new(), Config::default());
    /// let tree = prolly.put(&prolly.create(), b"k".to_vec(), b"v".to_vec()).unwrap();
    ///
    /// let view = prolly.debug_tree(&tree).unwrap();
    /// assert!(view.to_text().contains("level 0"));
    /// ```
    pub fn debug_tree(&self, tree: &Tree) -> Result<debug::TreeDebugView, Error> {
        debug::collect_tree_debug_view(self, tree)
    }

    /// Compare two trees by CID sharing and rewritten subtrees.
    ///
    /// Shared nodes are counted once. Left-only and right-only nodes represent
    /// subtrees that were rewritten, added, or removed between the two roots.
    ///
    /// # Example
    /// ```
    /// use prolly::{Config, MemStore, Prolly};
    ///
    /// let prolly = Prolly::new(MemStore::new(), Config::default());
    /// let before = prolly.put(&prolly.create(), b"k".to_vec(), b"v1".to_vec()).unwrap();
    /// let after = prolly.put(&before, b"k".to_vec(), b"v2".to_vec()).unwrap();
    ///
    /// let comparison = prolly.debug_compare_trees(&before, &after).unwrap();
    /// assert_eq!(comparison.left_only_nodes, 1);
    /// assert_eq!(comparison.right_only_nodes, 1);
    /// ```
    pub fn debug_compare_trees(
        &self,
        left: &Tree,
        right: &Tree,
    ) -> Result<debug::TreeDebugComparison, Error> {
        debug::compare_tree_debug_views(self, left, right)
    }

    /// Compare structural statistics between two trees.
    ///
    /// This collects [`TreeStats`] for both trees and returns a combined report
    /// with the baseline stats, candidate stats, absolute deltas, and
    /// percentage deltas. Deltas are computed as `after - before`.
    ///
    /// # Example
    /// ```
    /// use prolly::{Config, MemStore, Prolly};
    ///
    /// let prolly = Prolly::new(MemStore::new(), Config::default());
    /// let before = prolly.create();
    /// let after = prolly.put(&before, b"key".to_vec(), b"value".to_vec()).unwrap();
    ///
    /// let comparison = prolly.stats_diff(&before, &after).unwrap();
    /// assert_eq!(comparison.absolute.total_key_value_pairs_diff, 1);
    /// ```
    pub fn stats_diff(&self, before: &Tree, after: &Tree) -> Result<StatsComparison, Error> {
        let before_stats = self.collect_stats(before)?;
        let after_stats = self.collect_stats(after)?;
        Ok(StatsComparison::new(before_stats, after_stats))
    }

    /// Mark all content-addressed nodes reachable from retained tree roots.
    ///
    /// This is the safe first phase of garbage collection. Empty roots are
    /// ignored, duplicate roots and shared subtrees are counted once, and the
    /// returned CID list is sorted for deterministic planning.
    ///
    /// # Example
    /// ```
    /// use prolly::{Config, MemStore, Prolly};
    ///
    /// let prolly = Prolly::new(MemStore::new(), Config::default());
    /// let tree = prolly.create();
    /// let tree = prolly.put(&tree, b"k".to_vec(), b"v".to_vec()).unwrap();
    ///
    /// let reachable = prolly.mark_reachable(&[tree]).unwrap();
    /// assert_eq!(reachable.live_nodes, reachable.cids().len());
    /// ```
    pub fn mark_reachable(&self, roots: &[Tree]) -> Result<GcReachability, Error> {
        let parallelism = if self.store.prefers_batch_reads() {
            STATS_FRONTIER_PREFETCH_PARALLELISM
        } else {
            1
        };
        let mut seen = HashSet::new();
        let mut frontier = Vec::new();

        for tree in roots {
            if let Some(root_cid) = &tree.root {
                if seen.insert(root_cid.clone()) {
                    frontier.push(root_cid.clone());
                }
            }
        }

        let mut live_cids = Vec::new();
        let mut live_bytes = 0usize;
        let mut leaf_nodes = 0usize;
        let mut internal_nodes = 0usize;

        while !frontier.is_empty() {
            let current = std::mem::take(&mut frontier);
            let nodes = self.load_many_ordered_with_parallelism(&current, parallelism)?;

            for (cid, node) in current.into_iter().zip(nodes) {
                if node.keys.len() != node.vals.len() {
                    return Err(Error::InvalidNode);
                }

                live_bytes += node.encoded_len();
                if node.leaf {
                    leaf_nodes += 1;
                } else {
                    internal_nodes += 1;
                    frontier.reserve(node.vals.len());
                    for idx in 0..node.len() {
                        let child_cid = child_cid_at(&node, idx)?;
                        if seen.insert(child_cid.clone()) {
                            frontier.push(child_cid);
                        }
                    }
                }
                live_cids.push(cid);
            }
        }

        gc::sort_cids(&mut live_cids);
        Ok(GcReachability {
            live_nodes: live_cids.len(),
            live_cids,
            live_bytes,
            leaf_nodes,
            internal_nodes,
        })
    }

    /// Plan which content-addressed nodes a destination store is missing for a tree.
    ///
    /// This is the dry-run phase for Merkle-style store synchronization. The
    /// source tree is walked from its root, the destination is checked with an
    /// ordered batch read, and any present destination bytes are verified against
    /// their requested CID. Missing source bytes are also verified before their
    /// byte weight is counted.
    ///
    /// # Example
    /// ```
    /// use prolly::{Config, MemStore, Prolly};
    /// use std::sync::Arc;
    ///
    /// let source = Arc::new(MemStore::new());
    /// let destination = Arc::new(MemStore::new());
    /// let prolly = Prolly::new(source, Config::default());
    ///
    /// let tree = prolly.create();
    /// let tree = prolly.put(&tree, b"k".to_vec(), b"v".to_vec()).unwrap();
    ///
    /// let plan = prolly.plan_missing_nodes(&tree, &destination).unwrap();
    /// assert_eq!(plan.missing_nodes, plan.missing_cids().len());
    /// ```
    pub fn plan_missing_nodes<D>(
        &self,
        tree: &Tree,
        destination: &D,
    ) -> Result<MissingNodePlan, Error>
    where
        D: Store,
    {
        let (plan, _) = self.prepare_missing_nodes(tree, destination)?;
        Ok(plan)
    }

    /// Copy all destination-missing nodes required by `tree`.
    ///
    /// The destination receives only immutable content-addressed node bytes it
    /// does not already have. Source and destination bytes are verified by CID
    /// before the copy succeeds.
    ///
    /// # Example
    /// ```
    /// use prolly::{Config, MemStore, Prolly};
    /// use std::sync::Arc;
    ///
    /// let source = Arc::new(MemStore::new());
    /// let destination = Arc::new(MemStore::new());
    /// let source_prolly = Prolly::new(source, Config::default());
    ///
    /// let tree = source_prolly.create();
    /// let tree = source_prolly
    ///     .put(&tree, b"k".to_vec(), b"v".to_vec())
    ///     .unwrap();
    ///
    /// let copied = source_prolly.copy_missing_nodes(&tree, &destination).unwrap();
    /// assert_eq!(copied.copied_nodes, copied.plan.missing_nodes);
    ///
    /// let destination_prolly = Prolly::new(destination, tree.config.clone());
    /// assert_eq!(destination_prolly.get(&tree, b"k").unwrap(), Some(b"v".to_vec()));
    /// ```
    pub fn copy_missing_nodes<D>(
        &self,
        tree: &Tree,
        destination: &D,
    ) -> Result<MissingNodeCopy, Error>
    where
        D: Store,
    {
        let (plan, node_bytes) = self.prepare_missing_nodes(tree, destination)?;
        let copied_nodes = node_bytes.len();
        let copied_bytes = node_bytes
            .iter()
            .map(|(_, bytes)| bytes.len())
            .sum::<usize>();

        if !node_bytes.is_empty() {
            let entries = node_bytes
                .iter()
                .map(|(cid, bytes)| (cid.as_bytes(), bytes.as_slice()))
                .collect::<Vec<_>>();
            destination
                .batch_put(&entries)
                .map_err(|err| Error::Store(Box::new(err)))?;
        }

        Ok(MissingNodeCopy {
            plan,
            copied_nodes,
            copied_bytes,
        })
    }

    fn prepare_missing_nodes<D>(
        &self,
        tree: &Tree,
        destination: &D,
    ) -> Result<PreparedMissingNodes, Error>
    where
        D: Store,
    {
        let reachability = self.mark_reachable(std::slice::from_ref(tree))?;
        let required_nodes = reachability.live_nodes;
        let required_bytes = reachability.live_bytes;
        let required_cids = reachability.live_cids;

        if required_cids.is_empty() {
            return Ok((
                MissingNodePlan {
                    required_cids,
                    required_nodes,
                    required_bytes,
                    missing_cids: Vec::new(),
                    missing_nodes: 0,
                    missing_bytes: 0,
                },
                Vec::new(),
            ));
        }

        let destination_keys = required_cids
            .iter()
            .map(|cid| cid.as_bytes())
            .collect::<Vec<_>>();
        let destination_values = destination
            .batch_get_ordered_unique(&destination_keys)
            .map_err(|err| Error::Store(Box::new(err)))?;
        if destination_values.len() != required_cids.len() {
            return Err(Error::InvalidNode);
        }

        let mut missing_cids = Vec::new();
        for (cid, value) in required_cids.iter().zip(destination_values) {
            match value {
                Some(bytes) => self::sync::verify_node_bytes(cid, &bytes)?,
                None => missing_cids.push(cid.clone()),
            }
        }

        let missing_keys = missing_cids
            .iter()
            .map(|cid| cid.as_bytes())
            .collect::<Vec<_>>();
        let source_values = self
            .store
            .batch_get_ordered_unique(&missing_keys)
            .map_err(|err| Error::Store(Box::new(err)))?;
        if source_values.len() != missing_cids.len() {
            return Err(Error::InvalidNode);
        }

        let mut missing_bytes = 0usize;
        let mut node_bytes = Vec::with_capacity(missing_cids.len());
        for (cid, value) in missing_cids.iter().zip(source_values) {
            let bytes = value.ok_or_else(|| Error::NotFound(cid.clone()))?;
            self::sync::verify_node_bytes(cid, &bytes)?;
            missing_bytes += bytes.len();
            node_bytes.push((cid.clone(), bytes));
        }

        Ok((
            MissingNodePlan {
                required_cids,
                required_nodes,
                required_bytes,
                missing_nodes: missing_cids.len(),
                missing_cids,
                missing_bytes,
            },
            node_bytes,
        ))
    }

    /// Build a dry-run garbage-collection plan from retained roots and
    /// caller-supplied candidate CIDs.
    ///
    /// The generic [`Store`] trait cannot list all stored nodes, so callers
    /// provide the candidate set. Pass every content CID that may be swept, and
    /// pass every tree root that must be retained. Unreachable candidates that
    /// are present in the store are reported as reclaimable; missing candidates
    /// are counted separately and never treated as reclaimable bytes.
    pub fn plan_gc<I, C>(&self, roots: &[Tree], candidates: I) -> Result<GcPlan, Error>
    where
        I: IntoIterator<Item = C>,
        C: Borrow<Cid>,
    {
        let reachability = self.mark_reachable(roots)?;
        let live_cids = reachability
            .live_cids
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut seen_candidates = HashSet::new();
        let mut reclaimable_cids = Vec::new();
        let mut reclaimable_bytes = 0usize;
        let mut missing_candidates = 0usize;
        let mut candidate_nodes = 0usize;

        for candidate in candidates {
            let cid = candidate.borrow();
            if !seen_candidates.insert(cid.clone()) {
                continue;
            }
            candidate_nodes += 1;

            if live_cids.contains(cid) {
                continue;
            }

            match self
                .store
                .get(cid.as_bytes())
                .map_err(|err| Error::Store(Box::new(err)))?
            {
                Some(bytes) => {
                    reclaimable_bytes += bytes.len();
                    reclaimable_cids.push(cid.clone());
                }
                None => {
                    missing_candidates += 1;
                }
            }
        }

        gc::sort_cids(&mut reclaimable_cids);
        Ok(GcPlan {
            reachability,
            candidate_nodes,
            reclaimable_nodes: reclaimable_cids.len(),
            reclaimable_cids,
            reclaimable_bytes,
            missing_candidates,
        })
    }

    /// Delete unreachable candidate nodes from the backing store.
    ///
    /// This runs [`Prolly::plan_gc`] first, then deletes exactly the plan's
    /// reclaimable candidates with a single store batch. The manager cache is
    /// cleared after deletion so swept nodes are not still readable from this
    /// process' in-memory cache.
    pub fn sweep_gc<I, C>(&self, roots: &[Tree], candidates: I) -> Result<GcSweep, Error>
    where
        I: IntoIterator<Item = C>,
        C: Borrow<Cid>,
    {
        let plan = self.plan_gc(roots, candidates)?;
        let deleted_nodes = plan.reclaimable_nodes;
        let deleted_bytes = plan.reclaimable_bytes;

        if !plan.reclaimable_cids.is_empty() {
            let ops = plan
                .reclaimable_cids
                .iter()
                .map(|cid| store::BatchOp::Delete {
                    key: cid.as_bytes(),
                })
                .collect::<Vec<_>>();
            self.store
                .batch(&ops)
                .map_err(|err| Error::Store(Box::new(err)))?;
            self.clear_cache();
        }

        Ok(GcSweep {
            plan,
            deleted_nodes,
            deleted_bytes,
        })
    }

    /// Mark all offloaded blobs reachable from retained tree roots.
    ///
    /// This scans reachable leaf values for [`blob::ValueRef::Blob`] envelopes.
    /// Ordinary raw values and escaped inline values are ignored. Empty roots,
    /// duplicate roots, shared subtrees, and duplicate blob references are
    /// counted once where appropriate.
    pub fn mark_reachable_blobs(&self, roots: &[Tree]) -> Result<BlobGcReachability, Error> {
        let parallelism = if self.store.prefers_batch_reads() {
            STATS_FRONTIER_PREFETCH_PARALLELISM
        } else {
            1
        };
        let mut seen_nodes = HashSet::new();
        let mut frontier = Vec::new();

        for tree in roots {
            if let Some(root_cid) = &tree.root {
                if seen_nodes.insert(root_cid.clone()) {
                    frontier.push(root_cid.clone());
                }
            }
        }

        let mut live_blobs_by_cid = HashMap::<Cid, blob::BlobRef>::new();
        let mut scanned_nodes = 0usize;
        let mut scanned_values = 0usize;

        while !frontier.is_empty() {
            let current = std::mem::take(&mut frontier);
            let nodes = self.load_many_ordered_with_parallelism(&current, parallelism)?;

            for node in nodes {
                if node.keys.len() != node.vals.len() {
                    return Err(Error::InvalidNode);
                }
                scanned_nodes += 1;

                if node.leaf {
                    scanned_values += node.vals.len();
                    for value in &node.vals {
                        if let blob::ValueRef::Blob(reference) =
                            blob::ValueRef::from_stored_bytes(value)?
                        {
                            match live_blobs_by_cid.entry(reference.cid.clone()) {
                                Entry::Occupied(entry) => {
                                    if entry.get().len != reference.len {
                                        return Err(Error::Deserialize(
                                            "conflicting blob reference lengths for same CID"
                                                .to_string(),
                                        ));
                                    }
                                }
                                Entry::Vacant(entry) => {
                                    entry.insert(reference);
                                }
                            }
                        }
                    }
                } else {
                    frontier.reserve(node.vals.len());
                    for idx in 0..node.len() {
                        let child_cid = child_cid_at(&node, idx)?;
                        if seen_nodes.insert(child_cid.clone()) {
                            frontier.push(child_cid);
                        }
                    }
                }
            }
        }

        let mut live_blobs = live_blobs_by_cid.into_values().collect::<Vec<_>>();
        gc::sort_blob_refs(&mut live_blobs);
        let live_blob_bytes = live_blobs
            .iter()
            .map(|reference| reference.len)
            .sum::<u64>();

        Ok(BlobGcReachability {
            live_blob_count: live_blobs.len(),
            live_blobs,
            live_blob_bytes,
            scanned_nodes,
            scanned_values,
        })
    }

    /// Build a dry-run garbage-collection plan for offloaded blobs.
    ///
    /// The generic [`BlobStore`] trait does not require blob listing, so callers
    /// provide candidate blob references. Pass every blob reference that may be
    /// swept, and every tree root that must be retained. Unreachable candidates
    /// that are present in the blob store are reported as reclaimable; missing
    /// candidates are counted separately.
    pub fn plan_blob_gc<B, I, C>(
        &self,
        blob_store: &B,
        roots: &[Tree],
        candidates: I,
    ) -> Result<BlobGcPlan, Error>
    where
        B: BlobStore,
        I: IntoIterator<Item = C>,
        C: Borrow<blob::BlobRef>,
    {
        let reachability = self.mark_reachable_blobs(roots)?;
        let live_cids = reachability
            .live_blobs
            .iter()
            .map(|reference| reference.cid.clone())
            .collect::<HashSet<_>>();
        let mut seen_candidates = HashSet::new();
        let mut reclaimable_blobs = Vec::new();
        let mut reclaimable_blob_bytes = 0u64;
        let mut missing_candidates = 0usize;
        let mut candidate_blobs = 0usize;

        for candidate in candidates {
            let reference = candidate.borrow();
            if !seen_candidates.insert(reference.cid.clone()) {
                continue;
            }
            candidate_blobs += 1;

            if live_cids.contains(&reference.cid) {
                continue;
            }

            match blob_store
                .get_blob(reference)
                .map_err(|err| Error::Store(Box::new(err)))?
            {
                Some(bytes) => {
                    reference.validate_bytes(&bytes)?;
                    reclaimable_blob_bytes += bytes.len() as u64;
                    reclaimable_blobs.push(reference.clone());
                }
                None => {
                    missing_candidates += 1;
                }
            }
        }

        gc::sort_blob_refs(&mut reclaimable_blobs);
        Ok(BlobGcPlan {
            reachability,
            candidate_blobs,
            reclaimable_blob_count: reclaimable_blobs.len(),
            reclaimable_blobs,
            reclaimable_blob_bytes,
            missing_candidates,
        })
    }

    /// Delete unreachable candidate blobs from the backing blob store.
    ///
    /// This runs [`Prolly::plan_blob_gc`] first, then deletes exactly the
    /// plan's reclaimable blob references. Missing candidates are ignored.
    pub fn sweep_blob_gc<B, I, C>(
        &self,
        blob_store: &B,
        roots: &[Tree],
        candidates: I,
    ) -> Result<BlobGcSweep, Error>
    where
        B: BlobStore,
        I: IntoIterator<Item = C>,
        C: Borrow<blob::BlobRef>,
    {
        let plan = self.plan_blob_gc(blob_store, roots, candidates)?;
        let deleted_blobs = plan.reclaimable_blob_count;
        let deleted_blob_bytes = plan.reclaimable_blob_bytes;

        for reference in &plan.reclaimable_blobs {
            blob_store
                .delete_blob(reference)
                .map_err(|err| Error::Store(Box::new(err)))?;
        }

        Ok(BlobGcSweep {
            plan,
            deleted_blobs,
            deleted_blob_bytes,
        })
    }

    /// Build a dry-run blob garbage-collection plan using the blob store's full
    /// blob-reference listing.
    ///
    /// This is available only when the blob backend implements
    /// [`BlobStoreScan`]. It is equivalent to calling
    /// [`BlobStoreScan::list_blob_refs`] and then [`Prolly::plan_blob_gc`].
    pub fn plan_blob_store_gc<B>(&self, blob_store: &B, roots: &[Tree]) -> Result<BlobGcPlan, Error>
    where
        B: BlobStoreScan,
    {
        let candidates = blob_store
            .list_blob_refs()
            .map_err(|err| Error::Store(Box::new(err)))?;
        self.plan_blob_gc(blob_store, roots, candidates)
    }

    /// Sweep unreachable blobs from every blob reference listed by the blob
    /// store.
    ///
    /// This is available only when the blob backend implements
    /// [`BlobStoreScan`].
    pub fn sweep_blob_store_gc<B>(
        &self,
        blob_store: &B,
        roots: &[Tree],
    ) -> Result<BlobGcSweep, Error>
    where
        B: BlobStoreScan,
    {
        let candidates = blob_store
            .list_blob_refs()
            .map_err(|err| Error::Store(Box::new(err)))?;
        self.sweep_blob_gc(blob_store, roots, candidates)
    }

    /// Build a dry-run garbage-collection plan using the store's full node-CID
    /// listing.
    ///
    /// This is available only when the backing store implements
    /// [`NodeStoreScan`]. It is equivalent to calling
    /// [`NodeStoreScan::list_node_cids`] and then [`Prolly::plan_gc`].
    pub fn plan_store_gc(&self, roots: &[Tree]) -> Result<GcPlan, Error>
    where
        S: NodeStoreScan,
    {
        let candidates = self
            .store
            .list_node_cids()
            .map_err(|err| Error::Store(Box::new(err)))?;
        self.plan_gc(roots, candidates)
    }

    /// Sweep unreachable nodes from every node CID listed by the backing store.
    ///
    /// This is available only when the backing store implements
    /// [`NodeStoreScan`]. The manager cache is cleared if any nodes are deleted.
    pub fn sweep_store_gc(&self, roots: &[Tree]) -> Result<GcSweep, Error>
    where
        S: NodeStoreScan,
    {
        let candidates = self
            .store
            .list_node_cids()
            .map_err(|err| Error::Store(Box::new(err)))?;
        self.sweep_gc(roots, candidates)
    }

    /// Build a store-wide GC plan using roots selected from named-root manifests.
    ///
    /// This combines [`Prolly::load_retained_named_roots`] with
    /// [`Prolly::plan_store_gc`]. Exact-name policies fail with
    /// [`Error::MissingNamedRoots`] if any requested name is absent so a typo
    /// cannot silently drop a retained branch from the GC plan.
    pub fn plan_store_gc_for_retention(
        &self,
        retention: &NamedRootRetention,
    ) -> Result<GcPlan, Error>
    where
        S: NodeStoreScan + ManifestStoreScan,
    {
        let selection = self.load_retained_named_roots(retention)?;
        Self::ensure_retention_selection_complete(&selection)?;
        let roots = selection.trees();
        self.plan_store_gc(&roots)
    }

    /// Sweep store-wide unreachable nodes using roots selected from manifests.
    ///
    /// This combines [`Prolly::load_retained_named_roots`] with
    /// [`Prolly::sweep_store_gc`]. Exact-name policies fail with
    /// [`Error::MissingNamedRoots`] if any requested name is absent.
    pub fn sweep_store_gc_for_retention(
        &self,
        retention: &NamedRootRetention,
    ) -> Result<GcSweep, Error>
    where
        S: NodeStoreScan + ManifestStoreScan,
    {
        let selection = self.load_retained_named_roots(retention)?;
        Self::ensure_retention_selection_complete(&selection)?;
        let roots = selection.trees();
        self.sweep_store_gc(&roots)
    }

    fn ensure_retention_selection_complete(selection: &NamedRootSelection) -> Result<(), Error> {
        if selection.is_complete() {
            Ok(())
        } else {
            Err(Error::MissingNamedRoots {
                names: selection.missing_names.clone(),
            })
        }
    }

    /// Frontier helper for collecting statistics
    ///
    /// Traverses the tree level-by-level, accumulating statistics at each node
    /// and batching each frontier's child loads when the store supports it.
    ///
    /// # Arguments
    /// * `root_cid` - The CID of the root node
    /// * `stats` - Mutable reference to the statistics accumulator
    ///
    /// # Returns
    /// * `Ok(())` - Statistics accumulated successfully
    /// * `Err(Error)` - On storage or deserialization errors
    fn collect_stats_from_frontier(
        &self,
        root_cid: &Cid,
        stats: &mut TreeStats,
    ) -> Result<(), Error> {
        let parallelism = if self.store.prefers_batch_reads() {
            STATS_FRONTIER_PREFETCH_PARALLELISM
        } else {
            1
        };
        let mut frontier = vec![root_cid.clone()];

        while !frontier.is_empty() {
            let nodes = self.load_many_ordered_with_parallelism(&frontier, parallelism)?;
            let mut next_frontier = Vec::new();

            for node in nodes {
                if node.keys.len() != node.vals.len() {
                    return Err(Error::InvalidNode);
                }
                stats.accumulate(&node);

                if !node.leaf {
                    next_frontier.reserve(node.vals.len());
                    for idx in 0..node.len() {
                        next_frontier.push(child_cid_at(&node, idx)?);
                    }
                }
            }

            frontier = next_frontier;
        }

        Ok(())
    }

    /// Create a cursor positioned at the given key.
    ///
    /// Returns a cursor that can be used for efficient traversal through the tree.
    /// The cursor is positioned at the key if it exists, or at the greatest key
    /// less than or equal to the target key.
    ///
    /// # Arguments
    /// * `tree` - The tree to navigate
    /// * `key` - The key to position at
    ///
    /// # Returns
    /// * `Ok(Cursor)` - A cursor positioned at or near the key
    /// * `Err` - If a storage error occurs
    ///
    /// # Example
    /// ```
    /// use prolly::{Prolly, MemStore, Config};
    ///
    /// let store = std::sync::Arc::new(MemStore::new());
    /// let prolly = Prolly::new(store.clone(), Config::default());
    /// let mut tree = prolly.create();
    ///
    /// tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
    /// tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
    ///
    /// let cursor = prolly.cursor(&tree, b"a").unwrap();
    /// assert!(cursor.is_valid());
    /// assert_eq!(cursor.get_key(), Some(b"a".as_slice()));
    /// ```
    pub fn cursor(&self, tree: &Tree, key: &[u8]) -> Result<cursor::Cursor, Error> {
        cursor::Cursor::at_item(&self.store, tree, key)
    }

    /// Create a cursor iterator for range queries.
    ///
    /// Returns an iterator that yields (key, value) pairs in lexicographic order,
    /// starting from `start` (inclusive) up to `end` (exclusive).
    ///
    /// # Arguments
    /// * `tree` - The tree to iterate over
    /// * `start` - The starting key (inclusive). Use `&[]` to start from the beginning.
    /// * `end` - Optional ending key (exclusive). Use `None` to iterate to the end.
    ///
    /// # Returns
    /// * `Ok(CursorIterator)` - An iterator over the range
    /// * `Err` - If a storage error occurs during cursor creation
    ///
    /// # Example
    /// ```
    /// use prolly::{Prolly, MemStore, Config};
    ///
    /// let store = std::sync::Arc::new(MemStore::new());
    /// let prolly = Prolly::new(store.clone(), Config::default());
    /// let mut tree = prolly.create();
    ///
    /// tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
    /// tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
    /// tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();
    ///
    /// // Iterate over range [a, c)
    /// let iter = prolly.range_cursor(&tree, b"a", Some(b"c")).unwrap();
    /// let entries: Vec<_> = iter.collect();
    /// assert_eq!(entries.len(), 2); // "a" and "b"
    /// ```
    pub fn range_cursor<'a>(
        &'a self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<cursor::CursorIterator<'a, S>, Error> {
        if end.is_some_and(|end| end <= start) {
            return Ok(cursor::CursorIterator::with_bounds(
                cursor::Cursor::invalid(),
                &self.store,
                Some(start.to_vec()),
                end.map(|e| e.to_vec()),
            ));
        }

        let cursor = cursor::Cursor::at_item(&self.store, tree, start)?;
        Ok(cursor::CursorIterator::with_bounds(
            cursor,
            &self.store,
            Some(start.to_vec()),
            end.map(|e| e.to_vec()),
        ))
    }

    /// Create a streaming diff iterator between two trees.
    ///
    /// Returns an iterator that yields `Diff` entries representing the changes
    /// needed to transform `base` into `other`. More memory-efficient than
    /// `diff()` for large trees as it doesn't collect all differences upfront.
    ///
    /// # Arguments
    /// * `base` - The base tree to compare from
    /// * `other` - The other tree to compare to
    ///
    /// # Returns
    /// * `Ok(DiffCursor)` - A streaming diff iterator
    /// * `Err(Error)` - If cursor initialization fails
    ///
    /// # Example
    /// ```rust
    /// use prolly::{Prolly, MemStore, Config, Diff};
    /// use std::sync::Arc;
    ///
    /// let store = Arc::new(MemStore::new());
    /// let prolly = Prolly::new(store.clone(), Config::default());
    ///
    /// let base = prolly.create();
    /// let other = prolly.put(&base, b"key".to_vec(), b"val".to_vec()).unwrap();
    ///
    /// // Stream differences
    /// for diff in prolly.diff_cursor(&base, &other).unwrap() {
    ///     println!("{:?}", diff);
    /// }
    ///
    /// // Or collect into Vec (equivalent to diff())
    /// let diffs: Vec<Diff> = prolly.diff_cursor(&base, &other).unwrap().collect();
    /// ```
    pub fn diff_cursor<'a>(
        &'a self,
        base: &Tree,
        other: &Tree,
    ) -> Result<cursor::DiffCursor<'a, S>, Error> {
        cursor::DiffCursor::new(&self.store, base, other)
    }

    /// Create a streaming diff iterator between two trees.
    ///
    /// Returns an iterator that yields `Result<Diff, Error>` entries representing
    /// the changes needed to transform `base` into `other`. This method walks
    /// the same content-addressed structure as eager diff, so equal subtrees are
    /// skipped by CID and only changed subtrees are visited.
    ///
    /// # Arguments
    /// * `base` - The base tree to compare from
    /// * `other` - The other tree to compare to
    ///
    /// # Returns
    /// * `Ok(impl Iterator)` - A streaming diff iterator yielding `Result<Diff, Error>`
    /// * `Err(Error)` - If cursor initialization fails
    ///
    /// # Short-circuit
    /// If both trees have the same root CID, returns an empty iterator immediately.
    ///
    /// # Example
    /// ```rust
    /// use prolly::{Prolly, MemStore, Config, Diff};
    /// use std::sync::Arc;
    ///
    /// let store = Arc::new(MemStore::new());
    /// let prolly = Prolly::new(store.clone(), Config::default());
    ///
    /// let base = prolly.create();
    /// let other = prolly.put(&base, b"key".to_vec(), b"val".to_vec()).unwrap();
    ///
    /// // Stream differences with error handling
    /// for diff_result in prolly.stream_diff(&base, &other).unwrap() {
    ///     match diff_result {
    ///         Ok(diff) => println!("{:?}", diff),
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    ///
    /// // Collect successful diffs
    /// let diffs: Vec<Diff> = prolly.stream_diff(&base, &other)
    ///     .unwrap()
    ///     .filter_map(|r| r.ok())
    ///     .collect();
    /// ```
    pub fn stream_diff<'a>(
        &'a self,
        base: &Tree,
        other: &Tree,
    ) -> Result<Box<dyn Iterator<Item = Result<Diff, Error>> + 'a>, Error> {
        Ok(Box::new(diff::stream_diff(self, base, other)))
    }

    /// Create a streaming merge-conflict iterator for a three-way merge.
    ///
    /// This walks the same structural diff path as [`Prolly::stream_diff`],
    /// compares each right-side change with `left`, and yields only keys that
    /// would require a resolver during [`Prolly::merge`]. Non-conflicting
    /// changes are skipped, and each yielded conflict preserves absence and
    /// deletion as `None`.
    ///
    /// This is useful for UIs, background agents, and sync workflows that need
    /// to inspect or ask about conflicts before choosing a resolver strategy.
    ///
    /// # Example
    /// ```rust
    /// use prolly::{Config, MemStore, Prolly};
    ///
    /// let prolly = Prolly::new(MemStore::new(), Config::default());
    /// let base = prolly
    ///     .put(&prolly.create(), b"title".to_vec(), b"base".to_vec())
    ///     .unwrap();
    /// let left = prolly
    ///     .put(&base, b"title".to_vec(), b"left".to_vec())
    ///     .unwrap();
    /// let right = prolly
    ///     .put(&base, b"title".to_vec(), b"right".to_vec())
    ///     .unwrap();
    ///
    /// let conflicts = prolly
    ///     .stream_conflicts(&base, &left, &right)
    ///     .unwrap()
    ///     .collect::<Result<Vec<_>, _>>()
    ///     .unwrap();
    ///
    /// assert_eq!(conflicts.len(), 1);
    /// assert_eq!(conflicts[0].key, b"title".to_vec());
    /// ```
    pub fn stream_conflicts<'a>(
        &'a self,
        base: &Tree,
        left: &'a Tree,
        right: &Tree,
    ) -> Result<Box<dyn Iterator<Item = Result<Conflict, Error>> + 'a>, Error> {
        Ok(Box::new(diff::stream_conflicts(self, base, left, right)))
    }

    /// Load a node by its CID from the store.
    pub(crate) fn load(&self, cid: &Cid) -> Result<Node, Error> {
        Ok(self.load_arc(cid)?.as_ref().clone())
    }

    /// Load a node by its CID, reusing the in-process immutable node cache.
    pub(crate) fn load_arc(&self, cid: &Cid) -> Result<Arc<Node>, Error> {
        if let Ok(mut cache) = self.node_cache.write() {
            if let Some(node) = cache.get(cid) {
                self.metrics.add_cache_hits(1);
                return Ok(node);
            }
        }

        self.metrics.add_cache_misses(1);
        let bytes = self
            .store
            .get(cid.as_bytes())
            .map_err(|e| Error::Store(Box::new(e)))?
            .ok_or_else(|| Error::NotFound(cid.clone()))?;
        self.metrics.record_point_read(bytes.len());
        let node = Arc::new(Node::from_bytes(&bytes)?);

        if let Ok(mut cache) = self.node_cache.write() {
            let evictions = cache.insert(cid.clone(), node.clone(), bytes.len());
            self.metrics.add_cache_evictions(evictions);
        }

        Ok(node)
    }

    fn load_arc_pinned(&self, cid: &Cid) -> Result<(Arc<Node>, bool), Error> {
        if let Ok(mut cache) = self.node_cache.write() {
            if let Some(node) = cache.get(cid) {
                let newly_pinned = cache.pin_existing(cid);
                self.metrics.add_cache_hits(1);
                return Ok((node, newly_pinned));
            }
        }

        self.metrics.add_cache_misses(1);
        let bytes = self
            .store
            .get(cid.as_bytes())
            .map_err(|e| Error::Store(Box::new(e)))?
            .ok_or_else(|| Error::NotFound(cid.clone()))?;
        self.metrics.record_point_read(bytes.len());
        let node = Arc::new(Node::from_bytes(&bytes)?);

        let mut newly_pinned = false;
        if let Ok(mut cache) = self.node_cache.write() {
            let (inserted_pinned, evictions) =
                cache.insert_pinned(cid.clone(), node.clone(), bytes.len());
            newly_pinned = inserted_pinned;
            self.metrics.add_cache_evictions(evictions);
        }

        Ok((node, newly_pinned))
    }

    /// Load nodes by CID in input order, batching cache misses through the store.
    pub(crate) fn load_many_ordered(&self, cids: &[Cid]) -> Result<Vec<Arc<Node>>, Error> {
        self.load_many_ordered_with_parallelism(cids, 1)
    }

    /// Load nodes by CID in input order, splitting cache misses across up to
    /// `parallelism` concurrent ordered batch reads.
    pub(crate) fn load_many_ordered_with_parallelism(
        &self,
        cids: &[Cid],
        parallelism: usize,
    ) -> Result<Vec<Arc<Node>>, Error> {
        if cids.is_empty() {
            return Ok(Vec::new());
        }

        let mut nodes: Vec<Option<Arc<Node>>>;
        let mut missing: Option<MissingNodeBatch>;
        if let Ok(mut cache) = self.node_cache.write() {
            let mut cache_hits = 0usize;
            let mut cached_nodes = Vec::with_capacity(cids.len());
            let mut first_miss = None;
            for (idx, cid) in cids.iter().enumerate() {
                if let Some(node) = cache.get(cid) {
                    cache_hits += 1;
                    cached_nodes.push(node.clone());
                } else {
                    first_miss = Some(idx);
                    break;
                }
            }

            let Some(first_miss) = first_miss else {
                self.metrics.add_cache_hits(cache_hits);
                return Ok(cached_nodes);
            };

            nodes = Vec::with_capacity(cids.len());
            nodes.extend(cached_nodes.into_iter().map(Some));
            nodes.resize_with(cids.len(), || None);
            missing = Some(MissingNodeBatch::with_capacity(cids.len() - first_miss));
            if let Some(missing_batch) = missing.as_mut() {
                missing_batch.record(&cids[first_miss], first_miss);
                for (idx, cid) in cids.iter().enumerate().skip(first_miss + 1) {
                    if let Some(node) = cache.get(cid) {
                        cache_hits += 1;
                        nodes[idx] = Some(node.clone());
                    } else {
                        missing_batch.record(cid, idx);
                    }
                }
            }
            self.metrics.add_cache_hits(cache_hits);
        } else {
            nodes = vec![None; cids.len()];
            let mut missing_batch = MissingNodeBatch::with_capacity(cids.len());
            for (idx, cid) in cids.iter().enumerate() {
                missing_batch.record(cid, idx);
            }
            missing = Some(missing_batch);
        }

        if let Some(MissingNodeBatch {
            cids: missing_cids,
            positions: missing_positions,
            ..
        }) = missing
        {
            if missing_cids.len() == 1 && !self.store.prefers_batch_reads() {
                let node = self.load_arc(&missing_cids[0])?;
                let positions = missing_positions
                    .into_iter()
                    .next()
                    .ok_or(Error::InvalidNode)?;
                for idx in positions {
                    nodes[idx] = Some(node.clone());
                }

                return nodes
                    .into_iter()
                    .collect::<Option<Vec<_>>>()
                    .ok_or(Error::InvalidNode);
            }

            let loaded = if parallelism <= 1 || missing_cids.len() <= parallelism {
                let keys = missing_cids
                    .iter()
                    .map(|cid| cid.as_bytes())
                    .collect::<Vec<_>>();
                self.metrics.add_cache_misses(keys.len());
                let loaded = self
                    .store
                    .batch_get_ordered_unique(&keys)
                    .map_err(|e| Error::Store(Box::new(e)))?;
                if loaded.len() != missing_cids.len() {
                    return Err(Error::InvalidNode);
                }
                let (loaded_nodes, loaded_bytes) = loaded_node_totals(&loaded);
                self.metrics
                    .record_batch_read(keys.len(), loaded_bytes, loaded_nodes);
                loaded
            } else {
                let chunk_size = missing_cids.len().div_ceil(parallelism);
                missing_cids
                    .par_chunks(chunk_size)
                    .map(|chunk| {
                        let keys = chunk.iter().map(|cid| cid.as_bytes()).collect::<Vec<_>>();
                        self.metrics.add_cache_misses(keys.len());
                        let loaded = self
                            .store
                            .batch_get_ordered_unique(&keys)
                            .map_err(|e| Error::Store(Box::new(e)))?;
                        if loaded.len() != chunk.len() {
                            return Err(Error::InvalidNode);
                        }
                        let (loaded_nodes, loaded_bytes) = loaded_node_totals(&loaded);
                        self.metrics
                            .record_batch_read(keys.len(), loaded_bytes, loaded_nodes);
                        Ok(loaded)
                    })
                    .collect::<Result<Vec<_>, Error>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
            };

            let decoded = if loaded.len() >= PARALLEL_NODE_DECODE_THRESHOLD {
                missing_cids
                    .into_par_iter()
                    .zip(loaded.into_par_iter())
                    .map(|(cid, bytes)| {
                        let bytes = bytes.ok_or_else(|| Error::NotFound(cid.clone()))?;
                        let node = Arc::new(Node::from_bytes(&bytes)?);
                        Ok((cid, node))
                    })
                    .collect::<Result<Vec<_>, Error>>()?
            } else {
                missing_cids
                    .into_iter()
                    .zip(loaded)
                    .map(|(cid, bytes)| {
                        let bytes = bytes.ok_or_else(|| Error::NotFound(cid.clone()))?;
                        let node = Arc::new(Node::from_bytes(&bytes)?);
                        Ok((cid, node))
                    })
                    .collect::<Result<Vec<_>, Error>>()?
            };

            let mut cache = self.node_cache.write().ok();
            let mut evictions = 0usize;
            for ((cid, node), positions) in decoded.into_iter().zip(missing_positions) {
                if let Some(cache) = cache.as_mut() {
                    evictions += cache.insert(cid, node.clone(), node.encoded_len());
                }
                for idx in positions {
                    nodes[idx] = Some(node.clone());
                }
            }
            self.metrics.add_cache_evictions(evictions);
        }

        nodes
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(Error::InvalidNode)
    }

    /// Save a node to the store and return its CID.
    pub(crate) fn save(&self, node: &Node) -> Result<Cid, Error> {
        let bytes = node.to_bytes();
        let cid = Cid::from_bytes(&bytes);
        self.store
            .put(cid.as_bytes(), &bytes)
            .map_err(|e| Error::Store(Box::new(e)))?;
        self.metrics.record_point_write(bytes.len());
        if let Ok(mut cache) = self.node_cache.write() {
            let evictions = cache.insert(cid.clone(), Arc::new(node.clone()), bytes.len());
            self.metrics.add_cache_evictions(evictions);
        }
        Ok(cid)
    }

    pub(crate) fn cache_node(&self, cid: Cid, node: Node) {
        if let Ok(mut cache) = self.node_cache.write() {
            let bytes = node.encoded_len();
            let evictions = cache.insert(cid, Arc::new(node), bytes);
            self.metrics.add_cache_evictions(evictions);
        }
    }

    pub(crate) fn cached_node_arc(&self, cid: &Cid) -> Option<Arc<Node>> {
        let node = self
            .node_cache
            .write()
            .ok()
            .and_then(|mut cache| cache.get(cid));
        if node.is_some() {
            self.metrics.add_cache_hits(1);
        }
        node
    }

    pub(crate) fn cached_rightmost_path(
        &self,
        root: &Cid,
    ) -> Option<Vec<CachedRightmostPathEntry>> {
        self.rightmost_path_cache
            .read()
            .ok()
            .and_then(|cached| match cached.as_ref() {
                Some((cached_root, path)) if cached_root == root => Some(path.clone()),
                _ => None,
            })
    }

    pub(crate) fn cache_rightmost_path(&self, root: Cid, path: Vec<CachedRightmostPathEntry>) {
        if let Ok(mut cache) = self.rightmost_path_cache.write() {
            *cache = Some((root, path));
        }
    }

    /// Clear the in-process immutable node cache.
    ///
    /// This is mostly useful after external store maintenance or tests that
    /// intentionally mutate the backing store outside the Prolly API.
    pub fn clear_cache(&self) {
        if let Ok(mut cache) = self.node_cache.write() {
            let evictions = cache.clear();
            self.metrics.add_cache_evictions(evictions);
        }
        if let Ok(mut cache) = self.rightmost_path_cache.write() {
            *cache = None;
        }
    }

    /// Return the number of cached nodes in this Prolly manager.
    pub fn cache_len(&self) -> usize {
        self.node_cache.read().map(|cache| cache.len()).unwrap_or(0)
    }

    /// Return the serialized-node byte weight retained by this manager cache.
    pub fn cache_bytes_len(&self) -> usize {
        self.node_cache
            .read()
            .map(|cache| cache.bytes_len())
            .unwrap_or(0)
    }

    /// Return the number of pinned nodes currently retained by this manager.
    ///
    /// Pinned nodes are a cache hint only. They may temporarily keep the cache
    /// above configured node or byte limits, and cache misses still fall back to
    /// the backing store.
    pub fn cache_pinned_len(&self) -> usize {
        self.node_cache
            .read()
            .map(|cache| cache.pinned_len())
            .unwrap_or(0)
    }

    /// Return the serialized-node byte weight of pinned cache entries.
    pub fn cache_pinned_bytes_len(&self) -> usize {
        self.node_cache
            .read()
            .map(|cache| cache.pinned_bytes_len())
            .unwrap_or(0)
    }

    /// Pin the root node of a tree in this manager's node cache.
    ///
    /// This is useful for hot snapshots where repeated reads are expected to
    /// start from the same root. Empty trees pin nothing. The return value is
    /// the number of nodes that became newly pinned.
    pub fn pin_tree_root(&self, tree: &Tree) -> Result<usize, Error> {
        let Some(root_cid) = &tree.root else {
            return Ok(0);
        };

        let (_, newly_pinned) = self.load_arc_pinned(root_cid)?;
        Ok(usize::from(newly_pinned))
    }

    /// Pin the root-to-leaf lookup path for `key` in this manager's node cache.
    ///
    /// The path is the same traversal that a lookup or point mutation would use
    /// for the key, including the would-be leaf for missing keys. Empty trees
    /// pin nothing. The return value is the number of nodes that became newly
    /// pinned.
    pub fn pin_tree_path(&self, tree: &Tree, key: &[u8]) -> Result<usize, Error> {
        let Some(root_cid) = &tree.root else {
            return Ok(0);
        };

        let mut cid = root_cid.clone();
        let mut newly_pinned = 0usize;

        loop {
            let (node, was_newly_pinned) = self.load_arc_pinned(&cid)?;
            newly_pinned += usize::from(was_newly_pinned);

            if node.leaf {
                break;
            }

            let idx = match node.search(key) {
                Ok(i) => i,
                Err(i) => i.saturating_sub(1),
            };
            cid = child_cid_at(&node, idx)?;
        }

        Ok(newly_pinned)
    }

    /// Persist a correctness-optional root-to-leaf path hint for a hot prefix.
    ///
    /// The hint records the path a range scan would use to seek to `prefix`.
    /// Durable stores that support hints can use it to warm a fresh manager's
    /// cache before repeatedly scanning a hot tenant, document, workspace, or
    /// index shard. Empty trees and stores without hint support return
    /// `Ok(false)`.
    ///
    /// Hints are performance sidecars only. A missing, stale, or malformed hint
    /// is ignored by [`Prolly::hydrate_prefix_path_hint`], and all tree
    /// operations retain their normal traversal fallback.
    pub fn publish_prefix_path_hint(&self, tree: &Tree, prefix: &[u8]) -> Result<bool, Error> {
        let Some(root_cid) = &tree.root else {
            return Ok(false);
        };

        if !self.store.supports_hints() {
            return Ok(false);
        }

        let path = self.find_path_hint_entries(tree, prefix)?;
        if path.is_empty() {
            return Ok(false);
        }

        let bytes = encode_prefix_path_hint(root_cid, prefix, &path)?;
        self.store
            .put_hint(
                PREFIX_PATH_HINT_NAMESPACE,
                &prefix_path_hint_key(root_cid, prefix),
                &bytes,
            )
            .map_err(|err| Error::Store(Box::new(err)))?;
        Ok(true)
    }

    /// Hydrate this manager's node cache from a persisted prefix path hint.
    ///
    /// Returns `Ok(true)` when a valid hint was found and loaded. Returns
    /// `Ok(false)` when the tree is empty, the store does not support hints, no
    /// hint exists, or the hint cannot be validated for this exact root and
    /// prefix. Store I/O errors are still returned.
    pub fn hydrate_prefix_path_hint(&self, tree: &Tree, prefix: &[u8]) -> Result<bool, Error> {
        let Some(root_cid) = &tree.root else {
            return Ok(false);
        };

        if !self.store.supports_hints() {
            return Ok(false);
        }

        load_prefix_path_hint(self, root_cid, prefix)
    }

    /// Persist recently changed key spans for a root transition.
    ///
    /// This is useful when a writer already knows the affected key ranges and
    /// wants a later sync or indexing job to prioritize those ranges. Spans are
    /// sorted and coalesced before storage. Empty span sets, unchanged roots,
    /// and stores without hint support return `Ok(false)`.
    ///
    /// Changed-span hints are correctness-optional. Callers that need an
    /// authoritative diff should still use [`Prolly::diff`],
    /// [`Prolly::range_diff`], or [`Prolly::structural_diff_page`].
    pub fn publish_changed_spans_hint<I>(
        &self,
        base: &Tree,
        changed: &Tree,
        spans: I,
    ) -> Result<bool, Error>
    where
        I: IntoIterator<Item = ChangedSpan>,
    {
        if base.root == changed.root || !self.store.supports_hints() {
            return Ok(false);
        }

        let spans = normalize_changed_spans(spans);
        if spans.is_empty() {
            return Ok(false);
        }

        let hint = ChangedSpanHint {
            base_root: base.root.clone(),
            changed_root: changed.root.clone(),
            spans,
        };
        let bytes = encode_changed_span_hint(&hint)?;
        self.store
            .put_hint(
                CHANGED_SPANS_HINT_NAMESPACE,
                &changed_span_hint_key(base.root.as_ref(), changed.root.as_ref()),
                &bytes,
            )
            .map_err(|err| Error::Store(Box::new(err)))?;
        Ok(true)
    }

    /// Load recently changed key spans for a root transition.
    ///
    /// Returns `Ok(Some(_))` only when a well-formed hint exists for this exact
    /// `(base, changed)` root pair. Missing, stale, malformed, or invalid span
    /// hints return `Ok(None)`, preserving the caller's normal diff fallback.
    pub fn load_changed_spans_hint(
        &self,
        base: &Tree,
        changed: &Tree,
    ) -> Result<Option<ChangedSpanHint>, Error> {
        if !self.store.supports_hints() {
            return Ok(None);
        }

        load_changed_span_hint(self, base.root.as_ref(), changed.root.as_ref())
    }

    /// Unpin all pinned node-cache entries for this manager.
    ///
    /// After unpinning, normal cache eviction runs immediately. Returns the
    /// number of entries that were pinned before this call.
    pub fn unpin_all_cache_nodes(&self) -> usize {
        if let Ok(mut cache) = self.node_cache.write() {
            let (unpinned, evictions) = cache.unpin_all();
            self.metrics.add_cache_evictions(evictions);
            unpinned
        } else {
            0
        }
    }

    /// Return cumulative cache and node I/O metrics for this manager.
    pub fn metrics(&self) -> ProllyMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Reset cumulative manager metrics to zero.
    ///
    /// This does not clear the node cache; call [`Prolly::clear_cache`] when
    /// you want the next operation to run from a cold manager cache.
    pub fn reset_metrics(&self) {
        self.metrics.reset();
    }

    /// Load a named root as a [`Tree`] through the underlying manifest store.
    ///
    /// This is available when the store implements both [`Store`] and
    /// [`ManifestStore`]. Missing names return `Ok(None)`.
    pub fn load_named_root(&self, name: &[u8]) -> Result<Option<Tree>, Error>
    where
        S: ManifestStore,
    {
        self.store
            .get_root(name)
            .map(|manifest| manifest.map(RootManifest::into_tree))
            .map_err(|err| Error::Store(Box::new(err)))
    }

    /// Load explicit named roots and report names that were absent.
    ///
    /// Duplicate names are ignored after their first occurrence. Missing names
    /// are reported in [`NamedRootSelection::missing_names`] instead of being
    /// silently dropped so callers can decide whether to continue.
    pub fn load_named_roots<I, N>(&self, names: I) -> Result<NamedRootSelection, Error>
    where
        S: ManifestStore,
        I: IntoIterator<Item = N>,
        N: AsRef<[u8]>,
    {
        let mut seen = HashSet::new();
        let mut roots = Vec::new();
        let mut missing_names = Vec::new();

        for name in names {
            let name = name.as_ref().to_vec();
            if !seen.insert(name.clone()) {
                continue;
            }

            match self.load_named_root(&name)? {
                Some(tree) => roots.push(NamedRoot::new(name, tree)),
                None => missing_names.push(name),
            }
        }

        Ok(NamedRootSelection::new(roots, missing_names))
    }

    /// List every named root in the manifest store.
    ///
    /// Results are sorted by raw name bytes when the backing store implements
    /// the [`ManifestStoreScan`] contract.
    pub fn list_named_root_manifests(&self) -> Result<Vec<manifest::NamedRootManifest>, Error>
    where
        S: ManifestStoreScan,
    {
        self.store
            .list_roots()
            .map_err(|err| Error::Store(Box::new(err)))
    }

    /// List every named root in the manifest store.
    ///
    /// Results are sorted by raw name bytes when the backing store implements
    /// the [`ManifestStoreScan`] contract.
    pub fn list_named_roots(&self) -> Result<Vec<NamedRoot>, Error>
    where
        S: ManifestStoreScan,
    {
        Ok(self
            .list_named_root_manifests()?
            .into_iter()
            .map(|root| root.into_named_root())
            .collect())
    }

    /// Select named roots according to a retention policy.
    ///
    /// Prefix and newest-by-name policies use manifest scanning. Exact policies
    /// load the requested names and report absent names explicitly.
    pub fn load_retained_named_roots(
        &self,
        retention: &NamedRootRetention,
    ) -> Result<NamedRootSelection, Error>
    where
        S: ManifestStoreScan,
    {
        match retention {
            NamedRootRetention::All => Ok(NamedRootSelection::new(
                self.list_named_roots()?,
                Vec::new(),
            )),
            NamedRootRetention::Exact { names } => self.load_named_roots(names.iter()),
            NamedRootRetention::Prefix { prefix } => {
                let roots = self
                    .list_named_roots()?
                    .into_iter()
                    .filter(|root| root.name.starts_with(prefix))
                    .collect();
                Ok(NamedRootSelection::new(roots, Vec::new()))
            }
            NamedRootRetention::NewestByName { prefix, count } => {
                if *count == 0 {
                    return Ok(NamedRootSelection::default());
                }

                let mut roots = self
                    .list_named_roots()?
                    .into_iter()
                    .filter(|root| root.name.starts_with(prefix))
                    .collect::<Vec<_>>();
                if roots.len() > *count {
                    roots = roots.split_off(roots.len() - *count);
                }
                Ok(NamedRootSelection::new(roots, Vec::new()))
            }
            NamedRootRetention::UpdatedSince {
                prefix,
                min_updated_at_millis,
            } => {
                let roots = self
                    .list_named_root_manifests()?
                    .into_iter()
                    .filter(|root| root.name.starts_with(prefix))
                    .filter(|root| {
                        root.manifest
                            .updated_at_millis
                            .map(|updated_at| updated_at >= *min_updated_at_millis)
                            .unwrap_or(false)
                    })
                    .map(|root| root.into_named_root())
                    .collect();
                Ok(NamedRootSelection::new(roots, Vec::new()))
            }
        }
    }

    /// Publish a tree handle under a durable name.
    ///
    /// This unconditionally replaces the existing named root. Use
    /// [`Prolly::compare_and_swap_named_root`] when coordinating concurrent
    /// writers.
    pub fn publish_named_root(&self, name: &[u8], tree: &Tree) -> Result<(), Error>
    where
        S: ManifestStore,
    {
        self.publish_named_root_at_millis(name, tree, current_unix_time_millis())
    }

    /// Publish a tree handle under a durable name with explicit timestamp
    /// metadata.
    ///
    /// `created_at_millis` is preserved from the existing manifest when present;
    /// otherwise it is initialized to `timestamp_millis`. `updated_at_millis` is
    /// always set to `timestamp_millis`.
    pub fn publish_named_root_at_millis(
        &self,
        name: &[u8],
        tree: &Tree,
        timestamp_millis: u64,
    ) -> Result<(), Error>
    where
        S: ManifestStore,
    {
        let created_at_millis = self
            .store
            .get_root(name)
            .map_err(|err| Error::Store(Box::new(err)))?
            .and_then(|manifest| manifest.created_at_millis)
            .unwrap_or(timestamp_millis);
        let manifest = RootManifest::from_tree_with_timestamps_millis(
            tree,
            Some(created_at_millis),
            Some(timestamp_millis),
        );
        self.store
            .put_root(name, &manifest)
            .map_err(|err| Error::Store(Box::new(err)))
    }

    /// Delete a durable named root.
    ///
    /// Deleting a missing name is not an error. This removes the mutable name
    /// only; it does not garbage-collect content-addressed nodes.
    pub fn delete_named_root(&self, name: &[u8]) -> Result<(), Error>
    where
        S: ManifestStore,
    {
        self.store
            .delete_root(name)
            .map_err(|err| Error::Store(Box::new(err)))
    }

    /// Atomically update a named root when the current tree matches `expected`.
    ///
    /// `expected == None` means the name must be absent. `new == None` deletes
    /// the name after a successful compare.
    pub fn compare_and_swap_named_root(
        &self,
        name: &[u8],
        expected: Option<&Tree>,
        new: Option<&Tree>,
    ) -> Result<NamedRootUpdate, Error>
    where
        S: ManifestStore,
    {
        self.compare_and_swap_named_root_at_millis(name, expected, new, current_unix_time_millis())
    }

    /// Atomically update a named root with explicit timestamp metadata.
    ///
    /// Tree-level compare-and-swap compares `expected` against the current tree
    /// handle, then performs the backend CAS against the exact current manifest.
    /// This keeps tree CAS stable even as manifests gain metadata fields.
    pub fn compare_and_swap_named_root_at_millis(
        &self,
        name: &[u8],
        expected: Option<&Tree>,
        new: Option<&Tree>,
        timestamp_millis: u64,
    ) -> Result<NamedRootUpdate, Error>
    where
        S: ManifestStore,
    {
        let current = self
            .store
            .get_root(name)
            .map_err(|err| Error::Store(Box::new(err)))?;
        let expected_manifest = match (expected, current) {
            (None, None) => None,
            (None, Some(current)) => {
                return Ok(NamedRootUpdate::Conflict {
                    current: Some(current.into_tree()),
                });
            }
            (Some(expected_tree), Some(current)) if current.to_tree() == *expected_tree => {
                Some(current)
            }
            (Some(_), current) => {
                return Ok(NamedRootUpdate::Conflict {
                    current: current.map(RootManifest::into_tree),
                });
            }
        };

        let new_manifest = new.map(|tree| {
            let created_at_millis = expected_manifest
                .as_ref()
                .and_then(|manifest| manifest.created_at_millis)
                .unwrap_or(timestamp_millis);
            RootManifest::from_tree_with_timestamps_millis(
                tree,
                Some(created_at_millis),
                Some(timestamp_millis),
            )
        });
        self.store
            .compare_and_swap_root(name, expected_manifest.as_ref(), new_manifest.as_ref())
            .map(NamedRootUpdate::from)
            .map_err(|err| Error::Store(Box::new(err)))
    }

    /// Borrow the underlying store.
    pub fn store(&self) -> &S {
        &self.store
    }

    pub(crate) fn record_batch_write_metrics(&self, nodes: usize, bytes: usize) {
        self.metrics.record_batch_write(nodes, bytes);
    }

    /// Find the path from root to the key.
    ///
    /// Returns a vector of (node, index) pairs representing the traversal path.
    /// The last element is the leaf node where the key would be found/inserted.
    pub(crate) fn find_path(&self, tree: &Tree, key: &[u8]) -> Result<Vec<(Node, usize)>, Error> {
        let mut path = Vec::new();

        let Some(root_cid) = &tree.root else {
            return Ok(path);
        };

        let mut cid = root_cid.clone();
        loop {
            let node = self.load(&cid)?;
            let idx = match node.search(key) {
                Ok(i) => i,
                Err(i) => i.saturating_sub(1),
            };

            let is_leaf = node.leaf;
            path.push((node.clone(), idx));

            if is_leaf {
                break;
            }

            cid = child_cid_at(&node, idx)?;
        }

        Ok(path)
    }

    /// Find the path from root to the key using shared cached nodes.
    pub(crate) fn find_path_arcs(
        &self,
        tree: &Tree,
        key: &[u8],
    ) -> Result<Vec<(Arc<Node>, usize)>, Error> {
        let mut path = Vec::new();

        let Some(root_cid) = &tree.root else {
            return Ok(path);
        };

        let mut cid = root_cid.clone();
        loop {
            let node = self.load_arc(&cid)?;
            let idx = match node.search(key) {
                Ok(i) => i,
                Err(i) => i.saturating_sub(1),
            };

            path.push((node.clone(), idx));

            if node.leaf {
                break;
            }

            cid = child_cid_at(&node, idx)?;
        }

        Ok(path)
    }

    fn find_path_hint_entries(
        &self,
        tree: &Tree,
        key: &[u8],
    ) -> Result<Vec<PrefixPathHintEntryWithNode>, Error> {
        let mut path = Vec::new();

        let Some(root_cid) = &tree.root else {
            return Ok(path);
        };

        let mut cid = root_cid.clone();
        loop {
            let node = self.load_arc(&cid)?;
            let idx = path_index_for_key(&node, key);

            path.push(PrefixPathHintEntryWithNode {
                cid: cid.clone(),
                node: node.clone(),
                child_index: idx,
            });

            if node.leaf {
                break;
            }

            cid = child_cid_at(&node, idx)?;
        }

        Ok(path)
    }

    /// Create a new leaf node with config settings.
    pub(crate) fn new_leaf_node(&self) -> Node {
        Node::builder()
            .leaf(true)
            .level(INIT_LEVEL)
            .min_chunk_size(self.config.min_chunk_size)
            .max_chunk_size(self.config.max_chunk_size)
            .chunking_factor(self.config.chunking_factor)
            .hash_seed(self.config.hash_seed)
            .encoding(self.config.encoding.clone())
            .build()
    }

    /// Create a new internal node with config settings.
    pub(crate) fn new_internal_node(&self, level: u8) -> Node {
        Node::builder()
            .leaf(false)
            .level(level)
            .min_chunk_size(self.config.min_chunk_size)
            .max_chunk_size(self.config.max_chunk_size)
            .chunking_factor(self.config.chunking_factor)
            .hash_seed(self.config.hash_seed)
            .encoding(self.config.encoding.clone())
            .build()
    }

    /// Create a new node with the same settings as an existing node.
    pub(crate) fn new_node_like(&self, template: &Node) -> Node {
        Node::builder()
            .leaf(template.leaf)
            .level(template.level)
            .min_chunk_size(template.min_chunk_size)
            .max_chunk_size(template.max_chunk_size)
            .chunking_factor(template.chunking_factor)
            .hash_seed(template.hash_seed)
            .encoding(template.encoding.clone())
            .build()
    }

    /// Apply multiple mutations to a tree in a single optimized operation.
    ///
    /// This method enables efficient bulk modifications (upserts and deletes) to an
    /// existing tree. Mutations are sorted by key, deduplicated (last-write-wins),
    /// grouped by affected leaf, and applied with a single atomic batch write.
    ///
    /// # Arguments
    /// * `tree` - The tree to modify
    /// * `mutations` - Vector of mutations to apply
    ///
    /// # Returns
    /// * `Ok(Tree)` - New tree with all mutations applied
    /// * `Err(Error)` - On storage or processing errors
    ///
    /// # Behavior
    /// - Mutations are sorted by key for efficient processing
    /// - Duplicate keys use last-write-wins semantics
    /// - All new nodes are written atomically via Store::batch
    /// - The input tree is not modified (immutable operation)
    ///
    /// # Example
    /// ```
    /// use prolly::{Prolly, MemStore, Config, Mutation};
    ///
    /// let store = MemStore::new();
    /// let prolly = Prolly::new(store, Config::default());
    /// let tree = prolly.create();
    ///
    /// let mutations = vec![
    ///     Mutation::Upsert { key: b"a".to_vec(), val: b"1".to_vec() },
    ///     Mutation::Upsert { key: b"b".to_vec(), val: b"2".to_vec() },
    ///     Mutation::Delete { key: b"c".to_vec() },
    /// ];
    ///
    /// let new_tree = prolly.batch(&tree, mutations).unwrap();
    /// ```
    pub fn batch(&self, tree: &Tree, mutations: Vec<Mutation>) -> Result<Tree, Error> {
        batch::apply_batch(self, tree, mutations)
    }

    /// Apply batch mutations and return store-neutral execution stats.
    ///
    /// The returned counters describe tree-level work such as affected leaves,
    /// write amplification, and which internal write path was selected.
    pub fn batch_with_stats(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
    ) -> Result<batch::BatchApplyResult, Error> {
        batch::BatchWriter::new().apply_batch_with_stats(self, tree, mutations)
    }

    /// Apply append-heavy mutations using the optimized append path when safe.
    ///
    /// If the mutations overlap existing data or cannot be applied as a pure
    /// append, this falls back to the regular batch implementation for
    /// correctness.
    pub fn append_batch(&self, tree: &Tree, mutations: Vec<Mutation>) -> Result<Tree, Error> {
        batch::append_batch(self, tree, mutations)
    }

    /// Apply append-heavy mutations and return store-neutral execution stats.
    ///
    /// If the append fast path cannot be used, the operation falls back to the
    /// regular batch implementation and reports the fallback path stats.
    pub fn append_batch_with_stats(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
    ) -> Result<batch::BatchApplyResult, Error> {
        batch::append_batch_with_stats(self, tree, mutations)
    }

    /// Merge two trees using CRDT semantics for automatic conflict resolution.
    ///
    /// Unlike the standard `merge()` method which can return `Error::Conflict`,
    /// this method uses CRDT (Conflict-free Replicated Data Type) semantics to
    /// automatically resolve all conflicts. This makes it suitable for distributed
    /// systems where concurrent modifications are common.
    ///
    /// # Arguments
    /// * `base` - The common ancestor tree
    /// * `left` - The left branch tree
    /// * `right` - The right branch tree
    /// * `config` - CRDT configuration specifying merge strategy and policies
    ///
    /// # Returns
    /// * `Ok(Tree)` - The merged tree (never returns `Error::Conflict`)
    /// * `Err(Error)` - Only on storage or deserialization errors
    ///
    /// # Merge Strategies
    /// - **LastWriterWins (LWW)**: Value with higher timestamp wins
    /// - **MultiValue (MV)**: Preserve all concurrent values as a set
    /// - **Custom**: User-provided merge function
    ///
    /// # Example
    /// ```rust
    /// use prolly::{Prolly, MemStore, Config, CrdtConfig, MergeStrategy};
    /// use std::sync::Arc;
    ///
    /// let store = Arc::new(MemStore::new());
    /// let prolly = Prolly::new(store.clone(), Config::default());
    ///
    /// let base = prolly.create();
    /// let base = prolly.put(&base, b"key".to_vec(), b"value".to_vec()).unwrap();
    ///
    /// // Create divergent branches
    /// let left = prolly.put(&base, b"key".to_vec(), b"left".to_vec()).unwrap();
    /// let right = prolly.put(&base, b"key".to_vec(), b"right".to_vec()).unwrap();
    ///
    /// // CRDT merge - never fails with conflict
    /// let config = CrdtConfig::default();
    /// let merged = prolly.crdt_merge(&base, &left, &right, &config).unwrap();
    /// ```
    pub fn crdt_merge(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        config: &crdt::CrdtConfig,
    ) -> Result<Tree, Error> {
        let merger = crdt::DefaultConflictFreeMerger::new();
        crdt::ConflictFreeMerger::crdt_merge(&merger, &self.store, base, left, right, config)
    }

    /// Apply batch mutations with parallel processing.
    ///
    /// This method enables efficient bulk modifications using parallel processing
    /// for large trees. It groups mutations by target leaf and processes independent
    /// leaf groups in parallel when beneficial.
    ///
    /// # Arguments
    /// * `tree` - The tree to modify
    /// * `mutations` - Vector of mutations to apply
    /// * `config` - Parallel configuration controlling thread count and threshold
    ///
    /// # Returns
    /// * `Ok(Tree)` - New tree with all mutations applied
    /// * `Err(Error)` - On storage or processing errors
    ///
    /// # Behavior
    /// - Falls back to sequential processing when below the parallelism threshold
    /// - Uses batch_put for efficient I/O
    /// - Maintains all tree invariants
    ///
    /// # Example
    /// ```rust
    /// use prolly::{Prolly, MemStore, Config, Mutation, ParallelConfig};
    /// use std::sync::Arc;
    ///
    /// let store = Arc::new(MemStore::new());
    /// let prolly = Prolly::new(store.clone(), Config::default());
    /// let tree = prolly.create();
    ///
    /// let mutations = vec![
    ///     Mutation::Upsert { key: b"a".to_vec(), val: b"1".to_vec() },
    ///     Mutation::Upsert { key: b"b".to_vec(), val: b"2".to_vec() },
    /// ];
    ///
    /// let config = ParallelConfig::default();
    /// let new_tree = prolly.parallel_batch(&tree, mutations, &config).unwrap();
    /// ```
    pub fn parallel_batch(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
        config: &parallel::ParallelConfig,
    ) -> Result<Tree, Error> {
        let rebalancer = parallel::DefaultParallelRebalancer::new();
        parallel::ParallelRebalancer::parallel_batch(
            &rebalancer,
            &self.store,
            self,
            tree,
            mutations,
            config,
        )
    }
}

#[cfg(feature = "async-store")]
impl<S> AsyncProlly<S>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    /// Create a new async Prolly tree manager.
    pub fn new(store: S, config: Config) -> Self {
        let node_cache_max_nodes = config.node_cache_max_nodes;
        let node_cache_max_bytes = config.node_cache_max_bytes;
        Self {
            store,
            config,
            node_cache: RwLock::new(NodeCache::new(node_cache_max_nodes, node_cache_max_bytes)),
            rightmost_path_cache: RwLock::new(None),
            metrics: ProllyMetrics::default(),
        }
    }

    /// Create a new empty tree.
    pub fn create(&self) -> Tree {
        Tree {
            root: None,
            config: self.config.clone(),
        }
    }

    /// Borrow the underlying async store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Borrow this manager's tree configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get value by key from the tree.
    pub async fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let Some(root_cid) = &tree.root else {
            return Ok(None);
        };

        let mut cid = root_cid.clone();
        loop {
            let node = self.load_arc(&cid).await?;
            let idx = match node.search(key) {
                Ok(i) => i,
                Err(i) => {
                    if i == 0 {
                        return Ok(None);
                    } else {
                        i - 1
                    }
                }
            };

            if node.leaf {
                if node.keys.get(idx).map(|k| k.as_slice()) == Some(key) {
                    return Ok(Some(leaf_value_at(&node, idx)?));
                }
                return Ok(None);
            }

            cid = child_cid_at(&node, idx)?;
        }
    }

    /// Get a stored large-value reference by key.
    ///
    /// Non-envelope values are returned as [`blob::ValueRef::Inline`], so this
    /// can inspect trees that mix ordinary raw values and offloaded blob
    /// references.
    pub async fn get_value_ref(
        &self,
        tree: &Tree,
        key: &[u8],
    ) -> Result<Option<blob::ValueRef>, Error> {
        self.get(tree, key)
            .await?
            .map(|value| blob::ValueRef::from_stored_bytes(&value))
            .transpose()
    }

    /// Get a value by key, resolving offloaded async blob references when
    /// present.
    pub async fn get_large_value<B>(
        &self,
        blob_store: &B,
        tree: &Tree,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, Error>
    where
        B: blob::AsyncBlobStore,
        B::Error: Send + Sync,
    {
        match self.get(tree, key).await? {
            Some(value) => Ok(Some(
                blob::resolve_stored_value_async(blob_store, &value).await?,
            )),
            None => Ok(None),
        }
    }

    /// Get multiple values while preserving caller order.
    ///
    /// This mirrors [`Prolly::get_many`] but loads frontier nodes through
    /// [`AsyncStore::batch_get_ordered_unique`], allowing async stores to
    /// overlap remote reads according to their `read_parallelism()`.
    pub async fn get_many<K: AsRef<[u8]>>(
        &self,
        tree: &Tree,
        keys: &[K],
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let mut values = vec![None; keys.len()];
        let Some(root_cid) = &tree.root else {
            return Ok(values);
        };

        if keys.is_empty() {
            return Ok(values);
        }

        let positions = InlinePositions::from_vec(sorted_key_positions(keys))
            .expect("keys is non-empty after early return");

        let mut frames = vec![KeyLookupFrame {
            cid: root_cid.clone(),
            positions,
        }];

        while !frames.is_empty() {
            let cids = frames
                .iter()
                .map(|frame| frame.cid.clone())
                .collect::<Vec<_>>();
            let nodes = self.load_child_frontier_ordered(&cids).await?;
            let mut next_frames = Vec::new();

            for (frame, node) in frames.into_iter().zip(nodes) {
                if node.leaf {
                    fill_leaf_lookup_values(&node, frame.positions, keys, &mut values)?;
                    continue;
                }

                next_frames.extend(route_key_positions_to_children(
                    &node,
                    frame.positions,
                    keys,
                )?);
            }

            frames = next_frames;
        }

        Ok(values)
    }

    /// Insert or update a key-value pair in the tree.
    ///
    /// This is the async counterpart to [`Prolly::put`]. It rewrites only the
    /// affected path, persists rewritten nodes through [`AsyncStore::batch_put`],
    /// and returns a new immutable tree handle.
    pub async fn put(&self, tree: &Tree, key: Vec<u8>, val: Vec<u8>) -> Result<Tree, Error> {
        let path = self.find_path(tree, &key).await?;
        let mut node = path
            .last()
            .map(|(node, _)| node.clone())
            .unwrap_or_else(|| self.new_leaf_node());

        match node.search(&key) {
            Ok(idx) => {
                if node.vals[idx] == val {
                    return Ok(tree.clone());
                }
                node.vals[idx] = val;
            }
            Err(idx) => {
                node.keys.insert(idx, key);
                node.vals.insert(idx, val);
            }
        }

        let mut collector = AsyncWriteCollector::new_cached();
        let new_root = self
            .rebalance_with_collector(
                node,
                path[..path.len().saturating_sub(1)].to_vec(),
                &mut collector,
            )
            .await?
            .ok_or(Error::InvalidNode)?;
        collector.flush(&self.store).await?;
        self.metrics
            .record_batch_write(collector.len(), collector.bytes_len());
        collector.cache_nodes(self);

        Ok(Tree {
            root: Some(new_root),
            config: tree.config.clone(),
        })
    }

    /// Insert or update a value, offloading large payloads to an async blob
    /// store.
    ///
    /// Values larger than `config.inline_threshold` are written to `blob_store`
    /// and represented in the tree by a compact content-addressed reference.
    pub async fn put_large_value<B>(
        &self,
        blob_store: &B,
        tree: &Tree,
        key: Vec<u8>,
        value: Vec<u8>,
        config: LargeValueConfig,
    ) -> Result<Tree, Error>
    where
        B: blob::AsyncBlobStore,
        B::Error: Send + Sync,
    {
        let stored = blob::encode_stored_value_async(blob_store, value, &config).await?;
        self.put(tree, key, stored).await
    }

    /// Delete a key from the tree.
    ///
    /// Missing keys are idempotent and return the original tree unchanged.
    pub async fn delete(&self, tree: &Tree, key: &[u8]) -> Result<Tree, Error> {
        let Some(_) = &tree.root else {
            return Ok(tree.clone());
        };

        let path = self.find_path(tree, key).await?;
        let Some((mut node, _)) = path.last().cloned() else {
            return Ok(tree.clone());
        };

        if let Ok(idx) = node.search(key) {
            node.keys.remove(idx);
            node.vals.remove(idx);
        } else {
            return Ok(tree.clone());
        }

        if node.is_empty() && path.len() == 1 {
            return Ok(Tree {
                root: None,
                config: tree.config.clone(),
            });
        }

        let mut collector = AsyncWriteCollector::new_cached();
        let new_root = self
            .rebalance_with_collector(
                node,
                path[..path.len().saturating_sub(1)].to_vec(),
                &mut collector,
            )
            .await?;
        collector.flush(&self.store).await?;
        self.metrics
            .record_batch_write(collector.len(), collector.bytes_len());
        collector.cache_nodes(self);

        Ok(Tree {
            root: new_root,
            config: tree.config.clone(),
        })
    }

    /// Apply multiple mutations using one async batch write.
    ///
    /// Mutations are sorted and deduplicated with last-write-wins semantics.
    /// The async batch planner routes those mutations to affected leaves using
    /// ordered async node loads, applies each touched leaf once, rebuilds only
    /// touched ancestors, and flushes all rewritten nodes through a single
    /// [`AsyncStore::batch_put`] call.
    pub async fn batch(&self, tree: &Tree, mutations: Vec<Mutation>) -> Result<Tree, Error> {
        let mutations = batch::preprocess_mutations(mutations);
        if mutations.is_empty() {
            return Ok(tree.clone());
        }

        if mutations
            .iter()
            .all(|mutation| matches!(mutation, Mutation::Upsert { .. }))
        {
            if let Some(appended) = self.try_append_batch(tree, &mutations).await? {
                return Ok(appended);
            }
        }

        let groups = self.group_batch_mutations_by_leaf(tree, mutations).await?;
        if groups.is_empty() {
            return Ok(tree.clone());
        }

        let mut collector = AsyncWriteCollector::new_cached();
        let result = self.apply_batch_groups_coalesced(tree, groups, &mut collector)?;
        if result.changed_leaves == 0 {
            return Ok(tree.clone());
        }

        collector.flush(&self.store).await?;
        self.metrics
            .record_batch_write(collector.len(), collector.bytes_len());
        collector.cache_nodes(self);

        Ok(Tree {
            root: result.root,
            config: tree.config.clone(),
        })
    }

    async fn try_append_batch(
        &self,
        tree: &Tree,
        mutations: &[Mutation],
    ) -> Result<Option<Tree>, Error> {
        if mutations.is_empty() {
            return Ok(Some(tree.clone()));
        }

        let rightmost_path = if tree.root.is_some() {
            self.find_rightmost_path(tree).await?
        } else {
            Vec::new()
        };

        if let Some(max_key) = rightmost_path
            .last()
            .and_then(|entry| entry.node.keys.last())
        {
            if mutations[0].key() <= max_key.as_slice() {
                return Ok(None);
            }
        }

        let mut collector = AsyncWriteCollector::new_cached();

        if tree.root.is_none() {
            let new_leaves = self.build_append_leaf_chunks(None, mutations);
            if new_leaves.is_empty() {
                return Ok(Some(tree.clone()));
            }

            let leaf_cids = new_leaves
                .iter()
                .map(|leaf| collector.add(leaf))
                .collect::<Vec<_>>();
            let update =
                self.build_tree_from_append_leaves(&leaf_cids, &new_leaves, &mut collector)?;
            self.flush_append_collector(&collector, Some((&update.root, &update.rightmost_path)))
                .await?;

            return Ok(Some(Tree {
                root: Some(update.root),
                config: tree.config.clone(),
            }));
        }

        let existing_tail = rightmost_path.last().ok_or(Error::InvalidNode)?;
        let existing_tail_leaf = existing_tail.node.clone();
        let existing_tail_cid = existing_tail.cid.clone();
        let new_leaves = self.build_append_leaf_chunks(Some(existing_tail_leaf.clone()), mutations);
        let new_leaf_cids = self.collect_append_leaf_cids(
            &existing_tail_cid,
            &existing_tail_leaf,
            &new_leaves,
            &mut collector,
        );

        let update = self.append_leaves_to_rightmost_path(
            &rightmost_path,
            &new_leaf_cids,
            &new_leaves,
            &mut collector,
        )?;
        self.flush_append_collector(&collector, Some((&update.root, &update.rightmost_path)))
            .await?;

        Ok(Some(Tree {
            root: Some(update.root),
            config: tree.config.clone(),
        }))
    }

    async fn find_rightmost_path(
        &self,
        tree: &Tree,
    ) -> Result<Vec<AsyncRightmostPathEntry>, Error> {
        let Some(root_cid) = &tree.root else {
            return Ok(Vec::new());
        };

        if let Some(cached) = self.cached_rightmost_path(root_cid) {
            return Ok(async_rightmost_entries_from_cache(cached));
        }

        if let Some(path) = self.load_rightmost_path_hint(root_cid).await? {
            self.cache_rightmost_path(root_cid.clone(), cached_rightmost_entries(&path));
            return Ok(path);
        }

        let mut path = Vec::new();
        let mut cid = root_cid.clone();

        loop {
            let node = self.load_arc(&cid).await?;
            if node.keys.len() != node.vals.len() || node.is_empty() {
                return Err(Error::InvalidNode);
            }

            let child_index = node.len().saturating_sub(1);
            let node_cid = cid.clone();
            let next_cid = if node.leaf {
                None
            } else {
                Some(child_cid_at(&node, child_index)?)
            };

            path.push(AsyncRightmostPathEntry {
                cid: node_cid,
                node: node.as_ref().clone(),
                child_index,
            });

            let Some(next_cid) = next_cid else {
                break;
            };
            cid = next_cid;
        }

        self.publish_rightmost_path_hint(root_cid, &path).await;
        self.cache_rightmost_path(root_cid.clone(), cached_rightmost_entries(&path));

        Ok(path)
    }

    fn cached_rightmost_path(&self, root: &Cid) -> Option<Vec<CachedRightmostPathEntry>> {
        self.rightmost_path_cache
            .read()
            .ok()
            .and_then(|cached| match cached.as_ref() {
                Some((cached_root, path)) if cached_root == root => Some(path.clone()),
                _ => None,
            })
    }

    fn cache_rightmost_path(&self, root: Cid, path: Vec<CachedRightmostPathEntry>) {
        if let Ok(mut cache) = self.rightmost_path_cache.write() {
            *cache = Some((root, path));
        }
    }

    async fn load_rightmost_path_hint(
        &self,
        root: &Cid,
    ) -> Result<Option<Vec<AsyncRightmostPathEntry>>, Error> {
        let Some(bytes) = self
            .store
            .get_hint(RIGHTMOST_PATH_HINT_NAMESPACE, root.as_bytes())
            .await
            .map_err(|err| Error::Store(Box::new(err)))?
        else {
            return Ok(None);
        };

        let Ok(hint) = serde_cbor::from_slice::<AsyncRightmostPathHint>(&bytes) else {
            return Ok(None);
        };

        if hint.version != 2
            || hint.entries.is_empty()
            || hint.entries.first().map(|entry| &entry.cid) != Some(root)
        {
            return Ok(None);
        }

        let keys = hint
            .entries
            .iter()
            .map(|entry| entry.cid.as_bytes())
            .collect::<Vec<_>>();
        let node_bytes = self
            .store
            .batch_get_ordered_unique(&keys)
            .await
            .map_err(|err| Error::Store(Box::new(err)))?;

        if node_bytes.len() != hint.entries.len() || node_bytes.iter().any(Option::is_none) {
            return Ok(None);
        }

        let mut path = Vec::with_capacity(hint.entries.len());
        for (entry, bytes) in hint.entries.into_iter().zip(node_bytes) {
            let Some(bytes) = bytes else {
                return Ok(None);
            };
            let Ok(node) = Node::from_bytes(&bytes) else {
                return Ok(None);
            };
            path.push(AsyncRightmostPathEntry {
                cid: entry.cid,
                node,
                child_index: entry.child_index,
            });
        }

        if !rightmost_path_hint_is_valid(root, &path) {
            return Ok(None);
        }

        for entry in &path {
            self.cache_node(entry.cid.clone(), entry.node.clone());
        }

        Ok(Some(path))
    }

    async fn publish_rightmost_path_hint(&self, root: &Cid, path: &[AsyncRightmostPathEntry]) {
        if !self.store.supports_hints() {
            return;
        }
        let Ok(bytes) = encode_rightmost_path_hint(path) else {
            return;
        };
        let _ = self
            .store
            .put_hint(RIGHTMOST_PATH_HINT_NAMESPACE, root.as_bytes(), &bytes)
            .await;
    }

    async fn flush_append_collector(
        &self,
        collector: &AsyncWriteCollector,
        rightmost_hint: Option<(&Cid, &[AsyncRightmostPathEntry])>,
    ) -> Result<(), Error> {
        if let Some((root, path)) = rightmost_hint {
            if self.store.supports_hints() {
                match encode_rightmost_path_hint(path) {
                    Ok(bytes) => {
                        collector
                            .flush_with_hint(
                                &self.store,
                                RIGHTMOST_PATH_HINT_NAMESPACE,
                                root.as_bytes(),
                                &bytes,
                            )
                            .await?;
                    }
                    Err(_) => collector.flush(&self.store).await?,
                }
            } else {
                collector.flush(&self.store).await?;
            }

            self.cache_rightmost_path(root.clone(), cached_rightmost_entries(path));
        } else {
            collector.flush(&self.store).await?;
        }

        self.metrics
            .record_batch_write(collector.len(), collector.bytes_len());
        collector.cache_nodes(self);

        Ok(())
    }

    fn build_append_leaf_chunks(
        &self,
        existing_tail_leaf: Option<Node>,
        mutations: &[Mutation],
    ) -> Vec<Node> {
        let mut leaves = Vec::new();
        let mut current_leaf = existing_tail_leaf.unwrap_or_else(|| self.new_leaf_node());
        let max_chunk_size = current_leaf.max_chunk_size;

        if should_close_append_leaf(&current_leaf, max_chunk_size) {
            leaves.push(current_leaf);
            current_leaf = self.new_leaf_node();
        }

        for mutation in mutations {
            let Mutation::Upsert { key, val } = mutation else {
                continue;
            };

            current_leaf.keys.push(key.clone());
            current_leaf.vals.push(val.clone());

            if should_close_append_leaf(&current_leaf, max_chunk_size) {
                leaves.push(current_leaf);
                current_leaf = self.new_leaf_node();
            }
        }

        if !current_leaf.is_empty() {
            leaves.push(current_leaf);
        }

        leaves
    }

    fn collect_append_leaf_cids(
        &self,
        existing_tail_cid: &Cid,
        existing_tail_leaf: &Node,
        new_leaves: &[Node],
        collector: &mut AsyncWriteCollector,
    ) -> Vec<Cid> {
        let mut cids = Vec::with_capacity(new_leaves.len());
        let start_idx = if new_leaves.first() == Some(existing_tail_leaf) {
            cids.push(existing_tail_cid.clone());
            1
        } else {
            0
        };

        for leaf in &new_leaves[start_idx..] {
            cids.push(collector.add(leaf));
        }

        cids
    }

    fn build_tree_from_append_leaves(
        &self,
        leaf_cids: &[Cid],
        leaves: &[Node],
        collector: &mut AsyncWriteCollector,
    ) -> Result<AsyncAppendTreeUpdate, Error> {
        if leaf_cids.len() != leaves.len() || leaf_cids.is_empty() {
            return Err(Error::InvalidNode);
        }

        let mut current_level = leaf_cids
            .iter()
            .cloned()
            .zip(leaves.iter().cloned())
            .collect::<Vec<_>>();
        let mut rightmost_path = vec![async_rightmost_entry_from_node_ref(
            current_level.last().ok_or(Error::InvalidNode)?,
        )];

        if current_level.len() == 1 {
            return Ok(AsyncAppendTreeUpdate {
                root: current_level[0].0.clone(),
                rightmost_path,
            });
        }

        let mut level = 1;
        loop {
            current_level = self.build_append_parent_level(&current_level, level, collector);
            rightmost_path.insert(
                0,
                async_rightmost_entry_from_node_ref(
                    current_level.last().ok_or(Error::InvalidNode)?,
                ),
            );

            if current_level.len() == 1 {
                return Ok(AsyncAppendTreeUpdate {
                    root: current_level[0].0.clone(),
                    rightmost_path,
                });
            }

            level += 1;
        }
    }

    fn append_leaves_to_rightmost_path(
        &self,
        rightmost_path: &[AsyncRightmostPathEntry],
        new_leaf_cids: &[Cid],
        new_leaves: &[Node],
        collector: &mut AsyncWriteCollector,
    ) -> Result<AsyncAppendTreeUpdate, Error> {
        if rightmost_path.is_empty() || new_leaf_cids.is_empty() {
            return Err(Error::InvalidNode);
        }

        let mut current_level = new_leaf_cids
            .iter()
            .cloned()
            .zip(new_leaves.iter().cloned())
            .collect::<Vec<_>>();
        let mut new_rightmost_path = vec![async_rightmost_entry_from_node_ref(
            current_level.last().ok_or(Error::InvalidNode)?,
        )];

        if rightmost_path.len() == 1 && rightmost_path[0].node.leaf {
            return self.build_tree_from_append_leaves(new_leaf_cids, new_leaves, collector);
        }

        for entry in rightmost_path.iter().rev() {
            let node = &entry.node;
            if node.leaf {
                continue;
            }

            let idx = entry.child_index;
            let mut updated_node = node.clone();
            updated_node.keys.remove(idx);
            updated_node.vals.remove(idx);

            for (offset, (cid, child)) in current_level.iter().enumerate() {
                updated_node.keys.insert(
                    idx + offset,
                    child.keys.first().cloned().unwrap_or_default(),
                );
                updated_node.vals.insert(idx + offset, cid.0.to_vec());
            }

            current_level = if updated_node.len() > updated_node.max_chunk_size {
                self.split_append_internal_node(&updated_node, collector)
            } else {
                let cid = collector.add(&updated_node);
                vec![(cid, updated_node)]
            };

            new_rightmost_path.insert(
                0,
                async_rightmost_entry_from_node_ref(
                    current_level.last().ok_or(Error::InvalidNode)?,
                ),
            );
        }

        if current_level.len() == 1 {
            return Ok(AsyncAppendTreeUpdate {
                root: current_level[0].0.clone(),
                rightmost_path: new_rightmost_path,
            });
        }

        let root_level = rightmost_path
            .first()
            .map(|entry| entry.node.level + 1)
            .unwrap_or(1);
        let mut root = self.new_internal_node(root_level);
        reserve_node_entries(&mut root, current_level.len());
        for (cid, node) in &current_level {
            root.keys
                .push(node.keys.first().cloned().unwrap_or_default());
            root.vals.push(cid.0.to_vec());
        }

        let root_cid = collector.add(&root);
        new_rightmost_path.insert(
            0,
            AsyncRightmostPathEntry {
                cid: root_cid.clone(),
                node: root,
                child_index: current_level.len() - 1,
            },
        );

        Ok(AsyncAppendTreeUpdate {
            root: root_cid,
            rightmost_path: new_rightmost_path,
        })
    }

    fn build_append_parent_level(
        &self,
        children: &[(Cid, Node)],
        level: u8,
        collector: &mut AsyncWriteCollector,
    ) -> Vec<(Cid, Node)> {
        let mut parents = Vec::new();
        let mut current_parent = self.new_internal_node(level);
        let parent_capacity = children.len().min(current_parent.max_chunk_size.max(1));
        reserve_node_entries(&mut current_parent, parent_capacity);

        for (idx, (cid, child)) in children.iter().enumerate() {
            current_parent
                .keys
                .push(child.keys.first().cloned().unwrap_or_default());
            current_parent.vals.push(cid.0.to_vec());

            if boundary::is_boundary(&current_parent, current_parent.len() - 1) {
                parents.push(current_parent);
                current_parent = self.new_internal_node(level);
                let remaining = children.len().saturating_sub(idx + 1);
                let parent_capacity = remaining.min(current_parent.max_chunk_size.max(1));
                reserve_node_entries(&mut current_parent, parent_capacity);
            }
        }

        if !current_parent.is_empty() {
            parents.push(current_parent);
        }

        parents
            .into_iter()
            .map(|parent| {
                let cid = collector.add(&parent);
                (cid, parent)
            })
            .collect()
    }

    fn split_append_internal_node(
        &self,
        node: &Node,
        collector: &mut AsyncWriteCollector,
    ) -> Vec<(Cid, Node)> {
        self.split_node_chunks(node)
            .into_iter()
            .map(|chunk| {
                let cid = collector.add(&chunk);
                (cid, chunk)
            })
            .collect()
    }

    async fn group_batch_mutations_by_leaf(
        &self,
        tree: &Tree,
        mutations: Vec<Mutation>,
    ) -> Result<Vec<AsyncBatchLeafGroup>, Error> {
        if mutations.is_empty() {
            return Ok(Vec::new());
        }

        let mutations = Arc::new(mutations);
        let Some(root_cid) = &tree.root else {
            return Ok(vec![AsyncBatchLeafGroup {
                leaf: self.new_leaf_node(),
                route_path: None,
                range: 0..mutations.len(),
                mutations,
            }]);
        };

        let mut frames = vec![AsyncBatchRouteFrame {
            cid: root_cid.clone(),
            path: None,
            range: 0..mutations.len(),
            mutations,
        }];
        let mut groups = Vec::new();

        while !frames.is_empty() {
            let cids = frames
                .iter()
                .map(|frame| frame.cid.clone())
                .collect::<Vec<_>>();
            let nodes = self.load_child_frontier_ordered(&cids).await?;
            let mut next_frames = Vec::new();

            for (frame, node) in frames.into_iter().zip(nodes) {
                if node.leaf {
                    groups.push(AsyncBatchLeafGroup {
                        leaf: node.as_ref().clone(),
                        route_path: frame.path,
                        mutations: frame.mutations,
                        range: frame.range,
                    });
                    continue;
                }

                let parent_path = frame.path.clone();
                let parent_node = node.clone();
                let parent_cid = frame.cid.clone();
                let mutations = frame.mutations.clone();

                batch::route_sorted_mutation_ranges_to_children_each(
                    &node,
                    &mutations,
                    frame.range,
                    |child_index, child_range| {
                        let child_cid = child_cid_at(&node, child_index)?;
                        let path = Arc::new(AsyncBatchRoutePath {
                            parent: parent_path.clone(),
                            node: parent_node.clone(),
                            cid: parent_cid.clone(),
                            child_index,
                        });
                        next_frames.push(AsyncBatchRouteFrame {
                            cid: child_cid,
                            path: Some(path),
                            mutations: mutations.clone(),
                            range: child_range,
                        });
                        Ok(())
                    },
                )?;
            }

            frames = next_frames;
        }

        Ok(groups)
    }

    fn apply_batch_groups_coalesced(
        &self,
        tree: &Tree,
        groups: Vec<AsyncBatchLeafGroup>,
        collector: &mut AsyncWriteCollector,
    ) -> Result<AsyncBatchApplyResult, Error> {
        let group_count = groups.len();
        let mut contexts =
            HashMap::<Cid, AsyncBatchAncestorContext>::with_capacity(group_count.saturating_mul(2));
        let mut pending = HashMap::<Cid, AsyncBatchChildReplacements>::with_capacity(group_count);
        let mut root_replacement: Option<Vec<AsyncBatchChildRef>> = None;
        let mut changed_leaves = 0usize;

        for group in groups {
            let mutation_slice = &group.mutations[group.range.clone()];
            let (modified_leaf, leaf_changed, _) =
                batch::apply_leaf_mutations_with_change(group.leaf, mutation_slice, true);

            if !leaf_changed {
                continue;
            }

            changed_leaves += 1;
            let child_refs =
                self.async_batch_child_refs_from_modified_node(modified_leaf, collector);

            if let Some(path) = group.route_path {
                collect_async_batch_route_contexts(&path, &mut contexts);
                pending
                    .entry(path.cid.clone())
                    .or_default()
                    .push((path.child_index, child_refs));
            } else {
                if root_replacement.is_some() || !pending.is_empty() {
                    return Err(Error::InvalidNode);
                }
                root_replacement = Some(child_refs);
            }
        }

        if changed_leaves == 0 {
            return Ok(AsyncBatchApplyResult {
                root: tree.root.clone(),
                changed_leaves,
            });
        }

        if let Some(replacement) = root_replacement {
            return Ok(AsyncBatchApplyResult {
                root: self.build_root_from_async_child_refs(replacement, collector)?,
                changed_leaves,
            });
        }

        let mut root_refs: Option<Vec<AsyncBatchChildRef>> = None;
        while !pending.is_empty() {
            let current = std::mem::take(&mut pending);

            for (node_cid, replacements) in current {
                let context = contexts.get(&node_cid).ok_or(Error::InvalidNode)?;
                let replacement_refs = self.apply_async_batch_child_replacements(
                    &context.node,
                    replacements,
                    collector,
                )?;

                if let Some(parent) = &context.parent {
                    pending
                        .entry(parent.parent_cid.clone())
                        .or_default()
                        .push((parent.child_index, replacement_refs));
                } else {
                    if root_refs.is_some() {
                        return Err(Error::InvalidNode);
                    }
                    root_refs = Some(replacement_refs);
                }
            }
        }

        let root_refs = root_refs.ok_or(Error::InvalidNode)?;
        Ok(AsyncBatchApplyResult {
            root: self.build_root_from_async_child_refs(root_refs, collector)?,
            changed_leaves,
        })
    }

    fn async_batch_child_refs_from_modified_node(
        &self,
        node: Node,
        collector: &mut AsyncWriteCollector,
    ) -> Vec<AsyncBatchChildRef> {
        if node.is_empty() {
            return Vec::new();
        }

        if node.len() <= node.max_chunk_size || node.len() == 1 {
            let first_key = node.keys.first().cloned().unwrap_or_default();
            let level = node.level;
            let cid = collector.add(&node);
            return vec![AsyncBatchChildRef {
                cid,
                first_key,
                level,
            }];
        }

        let chunks = self.split_node_chunks(&node);
        let metadata = chunks
            .iter()
            .map(|chunk| (chunk.keys.first().cloned().unwrap_or_default(), chunk.level))
            .collect::<Vec<_>>();

        metadata
            .into_iter()
            .zip(collector.add_many(chunks))
            .map(|((first_key, level), cid)| AsyncBatchChildRef {
                cid,
                first_key,
                level,
            })
            .collect()
    }

    fn apply_async_batch_child_replacements(
        &self,
        node: &Node,
        mut replacements: AsyncBatchChildReplacements,
        collector: &mut AsyncWriteCollector,
    ) -> Result<Vec<AsyncBatchChildRef>, Error> {
        if node.keys.len() != node.vals.len() {
            return Err(Error::InvalidNode);
        }

        replacements.sort_by_key(|(idx, _)| *idx);
        let mut previous_idx = None;
        for (idx, _) in &replacements {
            if *idx >= node.len() || previous_idx == Some(*idx) {
                return Err(Error::InvalidNode);
            }
            previous_idx = Some(*idx);
        }

        if replacements.iter().all(|(_, children)| children.len() == 1) {
            let mut updated = node.clone();
            for (idx, children) in replacements {
                let child = &children[0];
                updated.keys[idx] = child.first_key.clone();
                updated.vals[idx] = child.cid.0.to_vec();
            }

            debug_assert!(
                updated.keys.windows(2).all(|pair| pair[0] < pair[1]),
                "async coalesced batch rebuild must preserve parent key order"
            );

            return Ok(self.async_batch_child_refs_from_modified_node(updated, collector));
        }

        let replacement_len = node.len() - replacements.len()
            + replacements
                .iter()
                .map(|(_, children)| children.len())
                .sum::<usize>();
        let mut updated = self.new_node_like(node);
        reserve_node_entries(&mut updated, replacement_len);
        let mut replacements = replacements.into_iter().peekable();

        for idx in 0..node.len() {
            if replacements
                .peek()
                .map(|(replacement_idx, _)| *replacement_idx == idx)
                .unwrap_or(false)
            {
                let (_, children) = replacements.next().ok_or(Error::InvalidNode)?;
                for child in children {
                    updated.keys.push(child.first_key);
                    updated.vals.push(child.cid.0.to_vec());
                }
            } else {
                updated.keys.push(node.keys[idx].clone());
                updated.vals.push(node.vals[idx].clone());
            }
        }

        debug_assert!(
            updated.keys.windows(2).all(|pair| pair[0] < pair[1]),
            "async coalesced batch rebuild must preserve parent key order"
        );

        Ok(self.async_batch_child_refs_from_modified_node(updated, collector))
    }

    fn build_root_from_async_child_refs(
        &self,
        child_refs: Vec<AsyncBatchChildRef>,
        collector: &mut AsyncWriteCollector,
    ) -> Result<Option<Cid>, Error> {
        if child_refs.is_empty() {
            return Ok(None);
        }

        if child_refs.len() == 1 {
            return Ok(Some(child_refs[0].cid.clone()));
        }

        let first_level = child_refs[0].level;
        if child_refs.iter().any(|child| child.level != first_level) {
            return Err(Error::InvalidNode);
        }

        let mut level = first_level.saturating_add(1);
        let mut summaries = child_refs
            .into_iter()
            .map(|child| AsyncBuildNodeSummary {
                cid: child.cid,
                first_key: child.first_key,
            })
            .collect::<Vec<_>>();

        loop {
            summaries = self.build_internal_level_from_summaries(summaries, level, collector)?;

            if summaries.len() == 1 {
                return Ok(Some(summaries[0].cid.clone()));
            }

            level = level.saturating_add(1);
        }
    }

    /// Create an async range iterator over key-value pairs.
    ///
    /// The iterator yields keys in lexicographic order from `start`
    /// inclusive to `end` exclusive. It is lazy: each call to
    /// [`AsyncRangeIter::next`](crate::AsyncRangeIter::next) reads only the nodes needed to advance,
    /// while stores that prefer batch reads can prefetch nearby child nodes.
    pub async fn range<'a>(
        &'a self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<range::AsyncRangeIter<'a, S>, Error> {
        range::create_async_range_iter(self, tree, start, end).await
    }

    /// Create an async range iterator that resumes strictly after `after_key`.
    ///
    /// This is useful for checkpointed background jobs: persist the last key
    /// successfully processed, then resume with this method to avoid yielding
    /// that key again.
    pub async fn range_after<'a>(
        &'a self,
        tree: &Tree,
        after_key: &[u8],
        end: Option<&[u8]>,
    ) -> Result<range::AsyncRangeIter<'a, S>, Error> {
        range::create_async_range_after_iter(self, tree, after_key, end).await
    }

    /// Create an async range iterator from a stable range cursor.
    pub async fn range_from_cursor<'a>(
        &'a self,
        tree: &Tree,
        cursor: &range::RangeCursor,
        end: Option<&[u8]>,
    ) -> Result<range::AsyncRangeIter<'a, S>, Error> {
        match cursor.after() {
            Some(after_key) => self.range_after(tree, after_key, end).await,
            None => self.range(tree, &[], end).await,
        }
    }

    /// Read a bounded page from an async range scan.
    ///
    /// `cursor` is either [`RangeCursor::start`](crate::RangeCursor::start) or
    /// a cursor returned by a previous page. `end` is still exclusive. When
    /// `limit` is zero this returns an empty page with the original cursor so
    /// callers can treat zero-sized requests as no-ops.
    pub async fn range_page(
        &self,
        tree: &Tree,
        cursor: &range::RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<range::AsyncRangePage, Error> {
        if limit == 0 {
            return Ok(range::AsyncRangePage {
                entries: Vec::new(),
                next_cursor: Some(cursor.clone()),
            });
        }

        let mut iter = self.range_from_cursor(tree, cursor, end).await?;
        let mut entries = Vec::with_capacity(limit);

        for _ in 0..limit {
            let Some(item) = iter.next().await else {
                return Ok(range::AsyncRangePage {
                    entries,
                    next_cursor: None,
                });
            };
            entries.push(item?);
        }

        let next_cursor = entries
            .last()
            .map(|(key, _)| range::RangeCursor::after_key(key.clone()));
        Ok(range::AsyncRangePage {
            entries,
            next_cursor,
        })
    }

    /// Compute the difference between two trees through the async store.
    ///
    /// This mirrors [`Prolly::diff`] and preserves structural subtree pruning:
    /// identical CIDs are skipped, aligned internal nodes are compared by child
    /// CID, and stores that prefer batch reads hydrate sibling frontiers through
    /// ordered async batch reads.
    pub async fn diff(&self, base: &Tree, other: &Tree) -> Result<Vec<Diff>, Error> {
        diff::compute_async_diff(self, base, other).await
    }

    /// Compute the difference between two trees within a half-open key range.
    ///
    /// Returns only changes whose key is in `[start, end)`. This mirrors
    /// [`Prolly::range_diff`] but loads nodes through [`AsyncStore`].
    pub async fn range_diff(
        &self,
        base: &Tree,
        other: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Vec<Diff>, Error> {
        diff::compute_async_range_diff(self, base, other, start, end).await
    }

    /// Compute diffs from a stable cursor through the async store.
    ///
    /// This resumes strictly after the cursor key, so callers can persist the
    /// last processed diff key and avoid re-processing it on the next scan.
    pub async fn diff_from_cursor(
        &self,
        base: &Tree,
        other: &Tree,
        cursor: &range::RangeCursor,
        end: Option<&[u8]>,
    ) -> Result<Vec<Diff>, Error> {
        let start = cursor.after().unwrap_or(&[]);
        let mut diffs = self.range_diff(base, other, start, end).await?;
        if let Some(after_key) = cursor.after() {
            diffs.retain(|diff| diff.key() > after_key);
        }
        Ok(diffs)
    }

    /// Read a bounded page of diffs through the async store.
    pub async fn diff_page(
        &self,
        base: &Tree,
        other: &Tree,
        cursor: &range::RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<diff::DiffPage, Error> {
        if limit == 0 {
            return Ok(diff::DiffPage {
                diffs: Vec::new(),
                next_cursor: Some(cursor.clone()),
            });
        }

        let mut diffs = self.diff_from_cursor(base, other, cursor, end).await?;
        let has_more = diffs.len() > limit;
        if has_more {
            diffs.truncate(limit);
        }

        let next_cursor = if has_more {
            diffs
                .last()
                .map(|diff| range::RangeCursor::after_key(diff.key().to_vec()))
        } else {
            None
        };

        Ok(diff::DiffPage { diffs, next_cursor })
    }

    /// Read a bounded page from the async structural diff traversal.
    ///
    /// This is the async counterpart to [`Prolly::structural_diff_page`].
    /// Pass `None` to start, then pass the returned cursor until
    /// `next_cursor` is `None`.
    pub async fn structural_diff_page(
        &self,
        base: &Tree,
        other: &Tree,
        cursor: Option<&diff::StructuralDiffCursor>,
        limit: usize,
    ) -> Result<diff::StructuralDiffPage, Error> {
        diff::structural_diff_page_async(self, base, other, cursor, limit).await
    }

    /// Create an async streaming diff iterator between two trees.
    ///
    /// This is the async counterpart to [`Prolly::stream_diff`]. The iterator
    /// preserves structural subtree pruning and yields one diff at a time
    /// through [`AsyncDiffIter::next`](crate::AsyncDiffIter::next), so callers
    /// can stop early without materializing every change.
    pub fn stream_diff<'a>(&'a self, base: &Tree, other: &Tree) -> diff::AsyncDiffIter<'a, S> {
        diff::AsyncDiffIter::new(self, base, other)
    }

    /// Create an async streaming merge-conflict iterator for a three-way merge.
    ///
    /// This is the async counterpart to [`Prolly::stream_conflicts`]. It walks
    /// the async structural diff path, skips non-conflicting right-side
    /// changes, and yields delete-aware [`Conflict`] values through
    /// [`AsyncConflictIter::next`](crate::AsyncConflictIter::next).
    pub fn stream_conflicts<'a>(
        &'a self,
        base: &Tree,
        left: &'a Tree,
        right: &Tree,
    ) -> diff::AsyncConflictIter<'a, S> {
        diff::AsyncConflictIter::new(self, base, left, right)
    }

    /// Merge two trees using async three-way merge.
    ///
    /// This mirrors [`Prolly::merge`]: `base` is the common ancestor, changes
    /// from `right` are applied to `left`, and conflicting edits are passed to
    /// the optional delete-aware resolver. The implementation loads changed
    /// keys through [`AsyncStore`] and writes the merged tree through the async
    /// batch path.
    pub async fn merge(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        resolver: Option<Resolver>,
    ) -> Result<Tree, Error> {
        diff::merge_trees_async(self, base, left, right, resolver).await
    }

    /// Perform an async three-way merge and return structured diagnostic trace events.
    ///
    /// This is the async diagnostics-oriented counterpart to
    /// [`AsyncProlly::merge`]. The returned [`crate::MergeExplanation`] keeps
    /// its trace even when the merge result is an error, which is useful for
    /// remote sync jobs, object-store backends, and custom resolver debugging.
    pub async fn merge_explain(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        resolver: Option<Resolver>,
    ) -> diff::MergeExplanation {
        diff::merge_trees_explain_async(self, base, left, right, resolver).await
    }

    /// Merge only right-side changes whose keys are in `[start, end)` through
    /// the async store.
    ///
    /// Keys outside the range are left exactly as they are in `left`. Conflict
    /// detection and resolver behavior match [`AsyncProlly::merge`], but only
    /// for keys inside the requested range.
    pub async fn merge_range(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        resolver: Option<Resolver>,
    ) -> Result<Tree, Error> {
        diff::merge_trees_range_async(self, base, left, right, start, end, resolver).await
    }

    /// Merge only right-side changes whose keys start with `prefix` through
    /// the async store.
    pub async fn merge_prefix(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        prefix: &[u8],
        resolver: Option<Resolver>,
    ) -> Result<Tree, Error> {
        let (start, end) = key::prefix_range(prefix);
        self.merge_range(base, left, right, &start, end.as_deref(), resolver)
            .await
    }

    /// Merge two trees with async CRDT-style conflict-free resolution.
    ///
    /// This mirrors [`Prolly::crdt_merge`]. Built-in and custom CRDT
    /// strategies always choose a value or delete for conflicts, so this method
    /// never returns [`Error::Conflict`] unless a lower layer violates the merge
    /// contract.
    pub async fn crdt_merge(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        config: &crdt::CrdtConfig,
    ) -> Result<Tree, Error> {
        let config = config.clone();
        let resolver: Resolver = Box::new(move |conflict| {
            crdt::resolve_conflict(&config, conflict)
                .map(error::Resolution::value)
                .unwrap_or_else(error::Resolution::delete)
        });
        self.merge(base, left, right, Some(resolver)).await
    }

    /// Collect comprehensive statistics about a tree through the async store.
    ///
    /// This mirrors [`Prolly::collect_stats`], traversing the tree
    /// level-by-level and hydrating each child frontier through ordered async
    /// batch reads.
    pub async fn collect_stats(&self, tree: &Tree) -> Result<TreeStats, Error> {
        let Some(root_cid) = &tree.root else {
            let mut stats = TreeStats::new();
            stats.finalize();
            return Ok(stats);
        };

        let mut stats = TreeStats::new();
        self.collect_stats_from_frontier(root_cid, &mut stats)
            .await?;
        stats.finalize();
        Ok(stats)
    }

    /// Return a deterministic debug view of the tree through the async store.
    ///
    /// This mirrors [`Prolly::debug_tree`] and loads each child frontier through
    /// ordered async batch reads.
    pub async fn debug_tree(&self, tree: &Tree) -> Result<debug::TreeDebugView, Error> {
        debug::collect_tree_debug_view_async(self, tree).await
    }

    /// Compare two trees by CID sharing and rewritten subtrees through the
    /// async store.
    ///
    /// This mirrors [`Prolly::debug_compare_trees`]. Shared nodes are counted
    /// once; side-only nodes show which subtrees were rewritten, added, or
    /// removed.
    pub async fn debug_compare_trees(
        &self,
        left: &Tree,
        right: &Tree,
    ) -> Result<debug::TreeDebugComparison, Error> {
        debug::compare_tree_debug_views_async(self, left, right).await
    }

    /// Compare structural statistics between two trees through the async store.
    ///
    /// Deltas are computed as `after - before`, matching
    /// [`Prolly::stats_diff`].
    pub async fn stats_diff(&self, before: &Tree, after: &Tree) -> Result<StatsComparison, Error> {
        let before_stats = self.collect_stats(before).await?;
        let after_stats = self.collect_stats(after).await?;
        Ok(StatsComparison::new(before_stats, after_stats))
    }

    /// Mark all content-addressed nodes reachable from retained tree roots.
    ///
    /// This mirrors [`Prolly::mark_reachable`] while loading changed frontiers
    /// through [`AsyncStore::batch_get_ordered_unique`].
    pub async fn mark_reachable(&self, roots: &[Tree]) -> Result<GcReachability, Error> {
        let mut seen = HashSet::new();
        let mut frontier = Vec::new();

        for tree in roots {
            if let Some(root_cid) = &tree.root {
                if seen.insert(root_cid.clone()) {
                    frontier.push(root_cid.clone());
                }
            }
        }

        let mut live_cids = Vec::new();
        let mut live_bytes = 0usize;
        let mut leaf_nodes = 0usize;
        let mut internal_nodes = 0usize;

        while !frontier.is_empty() {
            let current = std::mem::take(&mut frontier);
            let nodes = self.load_child_frontier_ordered(&current).await?;

            for (cid, node) in current.into_iter().zip(nodes) {
                if node.keys.len() != node.vals.len() {
                    return Err(Error::InvalidNode);
                }

                live_bytes += node.encoded_len();
                if node.leaf {
                    leaf_nodes += 1;
                } else {
                    internal_nodes += 1;
                    frontier.reserve(node.vals.len());
                    for idx in 0..node.len() {
                        let child_cid = child_cid_at(&node, idx)?;
                        if seen.insert(child_cid.clone()) {
                            frontier.push(child_cid);
                        }
                    }
                }
                live_cids.push(cid);
            }
        }

        gc::sort_cids(&mut live_cids);
        Ok(GcReachability {
            live_nodes: live_cids.len(),
            live_cids,
            live_bytes,
            leaf_nodes,
            internal_nodes,
        })
    }

    /// Plan which content-addressed nodes an async destination store is missing.
    ///
    /// This is the async equivalent of [`Prolly::plan_missing_nodes`]. The
    /// destination and source are read through ordered async batch APIs so
    /// remote stores can overlap fetches internally.
    pub async fn plan_missing_nodes<D>(
        &self,
        tree: &Tree,
        destination: &D,
    ) -> Result<MissingNodePlan, Error>
    where
        D: AsyncStore,
        D::Error: Send + Sync,
    {
        let (plan, _) = self.prepare_missing_nodes(tree, destination).await?;
        Ok(plan)
    }

    /// Copy all async-destination-missing nodes required by `tree`.
    ///
    /// Source and destination node bytes are verified against their CIDs before
    /// the copy succeeds.
    pub async fn copy_missing_nodes<D>(
        &self,
        tree: &Tree,
        destination: &D,
    ) -> Result<MissingNodeCopy, Error>
    where
        D: AsyncStore,
        D::Error: Send + Sync,
    {
        let (plan, node_bytes) = self.prepare_missing_nodes(tree, destination).await?;
        let copied_nodes = node_bytes.len();
        let copied_bytes = node_bytes
            .iter()
            .map(|(_, bytes)| bytes.len())
            .sum::<usize>();

        if !node_bytes.is_empty() {
            let entries = node_bytes
                .iter()
                .map(|(cid, bytes)| (cid.as_bytes(), bytes.as_slice()))
                .collect::<Vec<_>>();
            destination
                .batch_put(&entries)
                .await
                .map_err(|err| Error::Store(Box::new(err)))?;
        }

        Ok(MissingNodeCopy {
            plan,
            copied_nodes,
            copied_bytes,
        })
    }

    async fn prepare_missing_nodes<D>(
        &self,
        tree: &Tree,
        destination: &D,
    ) -> Result<PreparedMissingNodes, Error>
    where
        D: AsyncStore,
        D::Error: Send + Sync,
    {
        let reachability = self.mark_reachable(std::slice::from_ref(tree)).await?;
        let required_nodes = reachability.live_nodes;
        let required_bytes = reachability.live_bytes;
        let required_cids = reachability.live_cids;

        if required_cids.is_empty() {
            return Ok((
                MissingNodePlan {
                    required_cids,
                    required_nodes,
                    required_bytes,
                    missing_cids: Vec::new(),
                    missing_nodes: 0,
                    missing_bytes: 0,
                },
                Vec::new(),
            ));
        }

        let destination_keys = required_cids
            .iter()
            .map(|cid| cid.as_bytes())
            .collect::<Vec<_>>();
        let destination_values = async_batch_get_ordered_unique_bounded(
            destination,
            &destination_keys,
            ASYNC_NODE_PREFETCH_BATCH_SIZE,
        )
        .await?;

        let mut missing_cids = Vec::new();
        for (cid, value) in required_cids.iter().zip(destination_values) {
            match value {
                Some(bytes) => self::sync::verify_node_bytes(cid, &bytes)?,
                None => missing_cids.push(cid.clone()),
            }
        }

        let missing_keys = missing_cids
            .iter()
            .map(|cid| cid.as_bytes())
            .collect::<Vec<_>>();
        let source_values = async_batch_get_ordered_unique_bounded(
            &self.store,
            &missing_keys,
            ASYNC_NODE_PREFETCH_BATCH_SIZE,
        )
        .await?;

        let mut missing_bytes = 0usize;
        let mut node_bytes = Vec::with_capacity(missing_cids.len());
        for (cid, value) in missing_cids.iter().zip(source_values) {
            let bytes = value.ok_or_else(|| Error::NotFound(cid.clone()))?;
            self::sync::verify_node_bytes(cid, &bytes)?;
            missing_bytes += bytes.len();
            node_bytes.push((cid.clone(), bytes));
        }

        Ok((
            MissingNodePlan {
                required_cids,
                required_nodes,
                required_bytes,
                missing_nodes: missing_cids.len(),
                missing_cids,
                missing_bytes,
            },
            node_bytes,
        ))
    }

    /// Mark all offloaded blobs reachable from retained tree roots through the
    /// async node store.
    pub async fn mark_reachable_blobs(&self, roots: &[Tree]) -> Result<BlobGcReachability, Error> {
        let mut seen_nodes = HashSet::new();
        let mut frontier = Vec::new();

        for tree in roots {
            if let Some(root_cid) = &tree.root {
                if seen_nodes.insert(root_cid.clone()) {
                    frontier.push(root_cid.clone());
                }
            }
        }

        let mut live_blobs_by_cid = HashMap::<Cid, blob::BlobRef>::new();
        let mut scanned_nodes = 0usize;
        let mut scanned_values = 0usize;

        while !frontier.is_empty() {
            let nodes = self.load_child_frontier_ordered(&frontier).await?;
            let mut next_frontier = Vec::new();

            for node in nodes {
                if node.keys.len() != node.vals.len() {
                    return Err(Error::InvalidNode);
                }
                scanned_nodes += 1;

                if node.leaf {
                    scanned_values += node.vals.len();
                    for value in &node.vals {
                        if let blob::ValueRef::Blob(reference) =
                            blob::ValueRef::from_stored_bytes(value)?
                        {
                            match live_blobs_by_cid.entry(reference.cid.clone()) {
                                Entry::Occupied(entry) => {
                                    if entry.get().len != reference.len {
                                        return Err(Error::Deserialize(
                                            "conflicting blob reference lengths for same CID"
                                                .to_string(),
                                        ));
                                    }
                                }
                                Entry::Vacant(entry) => {
                                    entry.insert(reference);
                                }
                            }
                        }
                    }
                } else {
                    next_frontier.reserve(node.vals.len());
                    for idx in 0..node.len() {
                        let child_cid = child_cid_at(&node, idx)?;
                        if seen_nodes.insert(child_cid.clone()) {
                            next_frontier.push(child_cid);
                        }
                    }
                }
            }

            frontier = next_frontier;
        }

        let mut live_blobs = live_blobs_by_cid.into_values().collect::<Vec<_>>();
        gc::sort_blob_refs(&mut live_blobs);
        let live_blob_bytes = live_blobs
            .iter()
            .map(|reference| reference.len)
            .sum::<u64>();

        Ok(BlobGcReachability {
            live_blob_count: live_blobs.len(),
            live_blobs,
            live_blob_bytes,
            scanned_nodes,
            scanned_values,
        })
    }

    /// Build a dry-run garbage-collection plan for offloaded blobs through an
    /// async blob store.
    pub async fn plan_blob_gc<B, I, C>(
        &self,
        blob_store: &B,
        roots: &[Tree],
        candidates: I,
    ) -> Result<BlobGcPlan, Error>
    where
        B: blob::AsyncBlobStore,
        B::Error: Send + Sync,
        I: IntoIterator<Item = C>,
        C: Borrow<blob::BlobRef>,
    {
        let reachability = self.mark_reachable_blobs(roots).await?;
        let live_cids = reachability
            .live_blobs
            .iter()
            .map(|reference| reference.cid.clone())
            .collect::<HashSet<_>>();
        let mut seen_candidates = HashSet::new();
        let mut reclaimable_blobs = Vec::new();
        let mut reclaimable_blob_bytes = 0u64;
        let mut missing_candidates = 0usize;
        let mut candidate_blobs = 0usize;

        for candidate in candidates {
            let reference = candidate.borrow();
            if !seen_candidates.insert(reference.cid.clone()) {
                continue;
            }
            candidate_blobs += 1;

            if live_cids.contains(&reference.cid) {
                continue;
            }

            match blob_store
                .get_blob(reference)
                .await
                .map_err(|err| Error::Store(Box::new(err)))?
            {
                Some(bytes) => {
                    reference.validate_bytes(&bytes)?;
                    reclaimable_blob_bytes += bytes.len() as u64;
                    reclaimable_blobs.push(reference.clone());
                }
                None => {
                    missing_candidates += 1;
                }
            }
        }

        gc::sort_blob_refs(&mut reclaimable_blobs);
        Ok(BlobGcPlan {
            reachability,
            candidate_blobs,
            reclaimable_blob_count: reclaimable_blobs.len(),
            reclaimable_blobs,
            reclaimable_blob_bytes,
            missing_candidates,
        })
    }

    /// Delete unreachable candidate blobs from an async blob store.
    pub async fn sweep_blob_gc<B, I, C>(
        &self,
        blob_store: &B,
        roots: &[Tree],
        candidates: I,
    ) -> Result<BlobGcSweep, Error>
    where
        B: blob::AsyncBlobStore,
        B::Error: Send + Sync,
        I: IntoIterator<Item = C>,
        C: Borrow<blob::BlobRef>,
    {
        let plan = self.plan_blob_gc(blob_store, roots, candidates).await?;
        let deleted_blobs = plan.reclaimable_blob_count;
        let deleted_blob_bytes = plan.reclaimable_blob_bytes;

        for reference in &plan.reclaimable_blobs {
            blob_store
                .delete_blob(reference)
                .await
                .map_err(|err| Error::Store(Box::new(err)))?;
        }

        Ok(BlobGcSweep {
            plan,
            deleted_blobs,
            deleted_blob_bytes,
        })
    }

    /// Clear in-process async manager caches.
    pub fn clear_cache(&self) {
        if let Ok(mut cache) = self.node_cache.write() {
            let evictions = cache.clear();
            self.metrics.add_cache_evictions(evictions);
        }
        if let Ok(mut cache) = self.rightmost_path_cache.write() {
            *cache = None;
        }
    }

    /// Return the current node-cache entry count.
    pub fn cache_len(&self) -> usize {
        self.node_cache.read().map(|cache| cache.len()).unwrap_or(0)
    }

    /// Return the serialized-node byte weight retained by this async manager cache.
    pub fn cache_bytes_len(&self) -> usize {
        self.node_cache
            .read()
            .map(|cache| cache.bytes_len())
            .unwrap_or(0)
    }

    /// Return the number of pinned nodes currently retained by this async manager.
    ///
    /// Pinned nodes are a cache hint only. They may temporarily keep the cache
    /// above configured node or byte limits, and cache misses still fall back to
    /// the backing store.
    pub fn cache_pinned_len(&self) -> usize {
        self.node_cache
            .read()
            .map(|cache| cache.pinned_len())
            .unwrap_or(0)
    }

    /// Return the serialized-node byte weight of pinned async cache entries.
    pub fn cache_pinned_bytes_len(&self) -> usize {
        self.node_cache
            .read()
            .map(|cache| cache.pinned_bytes_len())
            .unwrap_or(0)
    }

    /// Pin the root node of a tree in this async manager's node cache.
    ///
    /// This is useful for hot snapshots where repeated reads are expected to
    /// start from the same root. Empty trees pin nothing. The return value is
    /// the number of nodes that became newly pinned.
    pub async fn pin_tree_root(&self, tree: &Tree) -> Result<usize, Error> {
        let Some(root_cid) = &tree.root else {
            return Ok(0);
        };

        let (_, newly_pinned) = self.load_arc_pinned(root_cid).await?;
        Ok(usize::from(newly_pinned))
    }

    /// Pin the root-to-leaf lookup path for `key` in this async manager's cache.
    ///
    /// The path is the same traversal that a lookup or point mutation would use
    /// for the key, including the would-be leaf for missing keys. Empty trees
    /// pin nothing. The return value is the number of nodes that became newly
    /// pinned.
    pub async fn pin_tree_path(&self, tree: &Tree, key: &[u8]) -> Result<usize, Error> {
        let Some(root_cid) = &tree.root else {
            return Ok(0);
        };

        let mut cid = root_cid.clone();
        let mut newly_pinned = 0usize;

        loop {
            let (node, was_newly_pinned) = self.load_arc_pinned(&cid).await?;
            newly_pinned += usize::from(was_newly_pinned);

            if node.leaf {
                break;
            }

            let idx = match node.search(key) {
                Ok(i) => i,
                Err(i) => i.saturating_sub(1),
            };
            cid = child_cid_at(&node, idx)?;
        }

        Ok(newly_pinned)
    }

    /// Unpin all pinned node-cache entries for this async manager.
    ///
    /// After unpinning, normal cache eviction runs immediately. Returns the
    /// number of entries that were pinned before this call.
    pub fn unpin_all_cache_nodes(&self) -> usize {
        if let Ok(mut cache) = self.node_cache.write() {
            let (unpinned, evictions) = cache.unpin_all();
            self.metrics.add_cache_evictions(evictions);
            unpinned
        } else {
            0
        }
    }

    /// Return cumulative cache and node I/O metrics for this async manager.
    pub fn metrics(&self) -> ProllyMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Reset cumulative async manager metrics to zero.
    ///
    /// This does not clear the node cache; call [`AsyncProlly::clear_cache`]
    /// when you want the next operation to run from a cold manager cache.
    pub fn reset_metrics(&self) {
        self.metrics.reset();
    }

    pub(crate) async fn load_arc(&self, cid: &Cid) -> Result<Arc<Node>, Error> {
        if let Ok(mut cache) = self.node_cache.write() {
            if let Some(node) = cache.get(cid) {
                self.metrics.add_cache_hits(1);
                return Ok(node);
            }
        }

        self.metrics.add_cache_misses(1);
        let bytes = self
            .store
            .get(cid.as_bytes())
            .await
            .map_err(|e| Error::Store(Box::new(e)))?
            .ok_or_else(|| Error::NotFound(cid.clone()))?;
        self.metrics.record_point_read(bytes.len());
        let node = Arc::new(Node::from_bytes(&bytes)?);

        if let Ok(mut cache) = self.node_cache.write() {
            let evictions = cache.insert(cid.clone(), node.clone(), bytes.len());
            self.metrics.add_cache_evictions(evictions);
        }

        Ok(node)
    }

    async fn load_arc_pinned(&self, cid: &Cid) -> Result<(Arc<Node>, bool), Error> {
        if let Ok(mut cache) = self.node_cache.write() {
            if let Some(node) = cache.get(cid) {
                let newly_pinned = cache.pin_existing(cid);
                self.metrics.add_cache_hits(1);
                return Ok((node, newly_pinned));
            }
        }

        self.metrics.add_cache_misses(1);
        let bytes = self
            .store
            .get(cid.as_bytes())
            .await
            .map_err(|e| Error::Store(Box::new(e)))?
            .ok_or_else(|| Error::NotFound(cid.clone()))?;
        self.metrics.record_point_read(bytes.len());
        let node = Arc::new(Node::from_bytes(&bytes)?);

        let mut newly_pinned = false;
        if let Ok(mut cache) = self.node_cache.write() {
            let (inserted_pinned, evictions) =
                cache.insert_pinned(cid.clone(), node.clone(), bytes.len());
            newly_pinned = inserted_pinned;
            self.metrics.add_cache_evictions(evictions);
        }

        Ok((node, newly_pinned))
    }

    async fn collect_stats_from_frontier(
        &self,
        root_cid: &Cid,
        stats: &mut TreeStats,
    ) -> Result<(), Error> {
        let mut frontier = vec![root_cid.clone()];

        while !frontier.is_empty() {
            let nodes = self.load_child_frontier_ordered(&frontier).await?;
            let mut next_frontier = Vec::new();

            for node in nodes {
                if node.keys.len() != node.vals.len() {
                    return Err(Error::InvalidNode);
                }
                stats.accumulate(&node);

                if !node.leaf {
                    next_frontier.reserve(node.vals.len());
                    for idx in 0..node.len() {
                        next_frontier.push(child_cid_at(&node, idx)?);
                    }
                }
            }

            frontier = next_frontier;
        }

        Ok(())
    }

    pub(crate) async fn load_child_frontier_ordered(
        &self,
        cids: &[Cid],
    ) -> Result<Vec<Arc<Node>>, Error> {
        if cids.len() <= ASYNC_NODE_PREFETCH_BATCH_SIZE {
            return self.load_many_ordered(cids).await;
        }

        let mut nodes = Vec::with_capacity(cids.len());
        for chunk in cids.chunks(ASYNC_NODE_PREFETCH_BATCH_SIZE) {
            nodes.extend(self.load_many_ordered(chunk).await?);
        }
        Ok(nodes)
    }

    pub(crate) async fn load_many_ordered(&self, cids: &[Cid]) -> Result<Vec<Arc<Node>>, Error> {
        if cids.is_empty() {
            return Ok(Vec::new());
        }

        let mut nodes: Vec<Option<Arc<Node>>>;
        let mut missing: Option<MissingNodeBatch>;
        if let Ok(mut cache) = self.node_cache.write() {
            let mut cache_hits = 0usize;
            let mut cached_nodes = Vec::with_capacity(cids.len());
            let mut first_miss = None;
            for (idx, cid) in cids.iter().enumerate() {
                if let Some(node) = cache.get(cid) {
                    cache_hits += 1;
                    cached_nodes.push(node.clone());
                } else {
                    first_miss = Some(idx);
                    break;
                }
            }

            let Some(first_miss) = first_miss else {
                self.metrics.add_cache_hits(cache_hits);
                return Ok(cached_nodes);
            };

            nodes = Vec::with_capacity(cids.len());
            nodes.extend(cached_nodes.into_iter().map(Some));
            nodes.resize_with(cids.len(), || None);
            missing = Some(MissingNodeBatch::with_capacity(cids.len() - first_miss));
            if let Some(missing_batch) = missing.as_mut() {
                missing_batch.record(&cids[first_miss], first_miss);
                for (idx, cid) in cids.iter().enumerate().skip(first_miss + 1) {
                    if let Some(node) = cache.get(cid) {
                        cache_hits += 1;
                        nodes[idx] = Some(node.clone());
                    } else {
                        missing_batch.record(cid, idx);
                    }
                }
            }
            self.metrics.add_cache_hits(cache_hits);
        } else {
            nodes = vec![None; cids.len()];
            let mut missing_batch = MissingNodeBatch::with_capacity(cids.len());
            for (idx, cid) in cids.iter().enumerate() {
                missing_batch.record(cid, idx);
            }
            missing = Some(missing_batch);
        }

        if let Some(MissingNodeBatch {
            cids: missing_cids,
            positions: missing_positions,
            ..
        }) = missing
        {
            if missing_cids.len() == 1 && !self.store.prefers_batch_reads() {
                let node = self.load_arc(&missing_cids[0]).await?;
                let positions = missing_positions
                    .into_iter()
                    .next()
                    .ok_or(Error::InvalidNode)?;
                for idx in positions {
                    nodes[idx] = Some(node.clone());
                }

                return nodes
                    .into_iter()
                    .collect::<Option<Vec<_>>>()
                    .ok_or(Error::InvalidNode);
            }

            let keys = missing_cids
                .iter()
                .map(|cid| cid.as_bytes())
                .collect::<Vec<_>>();
            self.metrics.add_cache_misses(keys.len());
            let loaded = self
                .store
                .batch_get_ordered_unique(&keys)
                .await
                .map_err(|e| Error::Store(Box::new(e)))?;
            if loaded.len() != missing_cids.len() {
                return Err(Error::InvalidNode);
            }
            let (loaded_nodes, loaded_bytes) = loaded_node_totals(&loaded);
            self.metrics
                .record_batch_read(keys.len(), loaded_bytes, loaded_nodes);

            let decoded = missing_cids
                .into_iter()
                .zip(loaded)
                .map(|(cid, bytes)| {
                    let bytes = bytes.ok_or_else(|| Error::NotFound(cid.clone()))?;
                    let node = Arc::new(Node::from_bytes(&bytes)?);
                    Ok((cid, node))
                })
                .collect::<Result<Vec<_>, Error>>()?;

            let mut cache = self.node_cache.write().ok();
            let mut evictions = 0usize;
            for ((cid, node), positions) in decoded.into_iter().zip(missing_positions) {
                if let Some(cache) = cache.as_mut() {
                    evictions += cache.insert(cid, node.clone(), node.encoded_len());
                }
                for idx in positions {
                    nodes[idx] = Some(node.clone());
                }
            }
            self.metrics.add_cache_evictions(evictions);
        }

        nodes
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(Error::InvalidNode)
    }

    pub(crate) fn cached_node_arc(&self, cid: &Cid) -> Option<Arc<Node>> {
        let node = self
            .node_cache
            .write()
            .ok()
            .and_then(|mut cache| cache.get(cid));
        if node.is_some() {
            self.metrics.add_cache_hits(1);
        }
        node
    }

    fn cache_node(&self, cid: Cid, node: Node) {
        if let Ok(mut cache) = self.node_cache.write() {
            let bytes = node.encoded_len();
            let evictions = cache.insert(cid, Arc::new(node), bytes);
            self.metrics.add_cache_evictions(evictions);
        }
    }

    async fn find_path(&self, tree: &Tree, key: &[u8]) -> Result<Vec<(Node, usize)>, Error> {
        let mut path = Vec::new();

        let Some(root_cid) = &tree.root else {
            return Ok(path);
        };

        let mut cid = root_cid.clone();
        loop {
            let node = self.load_arc(&cid).await?;
            let idx = match node.search(key) {
                Ok(idx) => idx,
                Err(idx) => idx.saturating_sub(1),
            };

            path.push((node.as_ref().clone(), idx));

            if node.leaf {
                break;
            }

            cid = child_cid_at(&node, idx)?;
        }

        Ok(path)
    }

    pub(crate) async fn find_path_arcs(
        &self,
        tree: &Tree,
        key: &[u8],
    ) -> Result<Vec<(Arc<Node>, usize)>, Error> {
        let mut path = Vec::new();

        let Some(root_cid) = &tree.root else {
            return Ok(path);
        };

        let mut cid = root_cid.clone();
        loop {
            let node = self.load_arc(&cid).await?;
            let idx = match node.search(key) {
                Ok(i) => i,
                Err(i) => i.saturating_sub(1),
            };

            path.push((node.clone(), idx));

            if node.leaf {
                break;
            }

            cid = child_cid_at(&node, idx)?;
        }

        Ok(path)
    }

    fn build_internal_level_from_summaries(
        &self,
        children: Vec<AsyncBuildNodeSummary>,
        level: u8,
        collector: &mut AsyncWriteCollector,
    ) -> Result<Vec<AsyncBuildNodeSummary>, Error> {
        if children.is_empty() {
            return Ok(Vec::new());
        }

        let hash_boundaries = children
            .iter()
            .map(|child| {
                boundary::is_hash_boundary_config(
                    &self.config,
                    &child.first_key,
                    child.cid.as_bytes(),
                )
            })
            .collect::<Vec<_>>();
        let chunk_ranges =
            builder::chunk_ranges_from_hash_boundaries(&self.config, &hash_boundaries);
        let mut summaries = Vec::with_capacity(chunk_ranges.len());

        for range in chunk_ranges {
            let start = *range.start();
            let end = *range.end();
            let mut node = self.new_internal_node(level);
            reserve_node_entries(&mut node, end - start + 1);

            for child in children.iter().take(end + 1).skip(start) {
                node.keys.push(child.first_key.clone());
                node.vals.push(child.cid.0.to_vec());
            }

            summaries.push(self.collect_build_node(node, collector)?);
        }

        Ok(summaries)
    }

    fn collect_build_node(
        &self,
        node: Node,
        collector: &mut AsyncWriteCollector,
    ) -> Result<AsyncBuildNodeSummary, Error> {
        let first_key = node.keys.first().cloned().ok_or(Error::InvalidNode)?;
        let cid = collector.add(&node);
        Ok(AsyncBuildNodeSummary { cid, first_key })
    }

    fn new_leaf_node(&self) -> Node {
        Node::builder()
            .leaf(true)
            .level(INIT_LEVEL)
            .min_chunk_size(self.config.min_chunk_size)
            .max_chunk_size(self.config.max_chunk_size)
            .chunking_factor(self.config.chunking_factor)
            .hash_seed(self.config.hash_seed)
            .encoding(self.config.encoding.clone())
            .build()
    }

    fn new_internal_node(&self, level: u8) -> Node {
        Node::builder()
            .leaf(false)
            .level(level)
            .min_chunk_size(self.config.min_chunk_size)
            .max_chunk_size(self.config.max_chunk_size)
            .chunking_factor(self.config.chunking_factor)
            .hash_seed(self.config.hash_seed)
            .encoding(self.config.encoding.clone())
            .build()
    }

    fn new_node_like(&self, template: &Node) -> Node {
        Node::builder()
            .leaf(template.leaf)
            .level(template.level)
            .min_chunk_size(template.min_chunk_size)
            .max_chunk_size(template.max_chunk_size)
            .chunking_factor(template.chunking_factor)
            .hash_seed(template.hash_seed)
            .encoding(template.encoding.clone())
            .build()
    }

    async fn rebalance_with_collector(
        &self,
        mut node: Node,
        mut ancestors: Vec<(Node, usize)>,
        collector: &mut AsyncWriteCollector,
    ) -> Result<Option<Cid>, Error> {
        loop {
            if node.is_empty() {
                let Some((mut parent, idx)) = ancestors.pop() else {
                    return Ok(None);
                };

                parent.keys.remove(idx);
                parent.vals.remove(idx);

                if parent.is_empty() && ancestors.is_empty() {
                    return Ok(None);
                }

                node = parent;
                continue;
            }

            if node.len() > node.max_chunk_size && node.len() > 1 {
                let chunks = self.split_node_chunks(&node);

                if chunks.len() == 1 {
                    node = chunks.into_iter().next().ok_or(Error::InvalidNode)?;
                } else {
                    let first_keys = chunks
                        .iter()
                        .map(|chunk| chunk.keys.first().cloned().unwrap_or_default())
                        .collect::<Vec<_>>();
                    let chunk_info = collector
                        .add_many(chunks)
                        .into_iter()
                        .zip(first_keys)
                        .collect::<Vec<_>>();

                    if ancestors.is_empty() {
                        let mut parent = self.new_internal_node(node.level + 1);
                        reserve_node_entries(&mut parent, chunk_info.len());
                        for (cid, first_key) in &chunk_info {
                            parent.keys.push(first_key.clone());
                            parent.vals.push(cid.0.to_vec());
                        }
                        node = parent;
                        continue;
                    }

                    let (mut parent, idx) = ancestors.pop().ok_or(Error::InvalidNode)?;
                    parent.keys.remove(idx);
                    parent.vals.remove(idx);
                    reserve_node_entries(&mut parent, chunk_info.len().saturating_sub(1));
                    for (offset, (cid, first_key)) in chunk_info.iter().enumerate() {
                        parent.keys.insert(idx + offset, first_key.clone());
                        parent.vals.insert(idx + offset, cid.0.to_vec());
                    }
                    node = parent;
                    continue;
                }
            }

            if !ancestors.is_empty() && node.len() < node.min_chunk_size {
                if let Some((merged_node, merged_ancestors)) = self
                    .try_merge_with_sibling(&node, &ancestors, collector)
                    .await?
                {
                    node = merged_node;
                    ancestors = merged_ancestors;
                    continue;
                }
            }

            let cid = collector.add(&node);

            let Some((mut parent, idx)) = ancestors.pop() else {
                return Ok(Some(cid));
            };

            if !node.keys.is_empty() {
                parent.keys[idx] = node.keys[0].clone();
            }
            parent.vals[idx] = cid.0.to_vec();
            node = parent;
        }
    }

    async fn try_merge_with_sibling(
        &self,
        node: &Node,
        ancestors: &[(Node, usize)],
        collector: &mut AsyncWriteCollector,
    ) -> Result<Option<(Node, Vec<(Node, usize)>)>, Error> {
        let (parent, idx) = ancestors.last().ok_or(Error::InvalidNode)?;
        let idx = *idx;

        if idx > 0 {
            let left_cid = child_cid_at(parent, idx - 1)?;
            let left_sibling = self.load_arc(&left_cid).await?;

            if !is_valid_boundary_between(&left_sibling, node) {
                let merged = self.merge_nodes(&left_sibling, node);
                let mut new_parent = parent.clone();
                new_parent.keys.remove(idx - 1);
                new_parent.vals.remove(idx - 1);
                let new_idx = idx - 1;

                if merged.len() > merged.max_chunk_size && merged.len() > 1 {
                    let mut new_ancestors = ancestors[..ancestors.len() - 1].to_vec();
                    new_ancestors.push((new_parent, new_idx));
                    return Ok(Some((merged, new_ancestors)));
                }

                let merged_cid = collector.add(&merged);
                new_parent.keys[new_idx] = merged.keys[0].clone();
                new_parent.vals[new_idx] = merged_cid.0.to_vec();
                return Ok(Some((
                    new_parent,
                    ancestors[..ancestors.len() - 1].to_vec(),
                )));
            }
        }

        if idx + 1 < parent.vals.len() {
            let right_cid = child_cid_at(parent, idx + 1)?;
            let right_sibling = self.load_arc(&right_cid).await?;

            if !is_valid_boundary_between(node, &right_sibling) {
                let merged = self.merge_nodes(node, &right_sibling);
                let mut new_parent = parent.clone();
                new_parent.keys.remove(idx + 1);
                new_parent.vals.remove(idx + 1);

                if merged.len() > merged.max_chunk_size && merged.len() > 1 {
                    let mut new_ancestors = ancestors[..ancestors.len() - 1].to_vec();
                    new_ancestors.push((new_parent, idx));
                    return Ok(Some((merged, new_ancestors)));
                }

                let merged_cid = collector.add(&merged);
                new_parent.keys[idx] = merged.keys[0].clone();
                new_parent.vals[idx] = merged_cid.0.to_vec();
                return Ok(Some((
                    new_parent,
                    ancestors[..ancestors.len() - 1].to_vec(),
                )));
            }
        }

        Ok(None)
    }

    fn split_node_chunks(&self, node: &Node) -> Vec<Node> {
        let capacity = node.max_chunk_size.max(1);
        if node.len() <= capacity {
            return vec![node.clone()];
        }

        let num_chunks = node.len().div_ceil(capacity);
        let mut chunks = Vec::with_capacity(num_chunks);
        let mut start = 0;

        while start < node.len() {
            let remaining_chunks = num_chunks - chunks.len();
            let remaining_entries = node.len() - start;
            let target_end = if remaining_chunks == 0 {
                (start + remaining_entries).min(start + capacity)
            } else {
                start + (remaining_entries / remaining_chunks).max(1)
            };

            let max_end = (start + capacity).min(node.len());
            let min_end = start + 1;
            let mut end = target_end.min(max_end).max(min_end);
            let search_start = target_end.saturating_sub(50).max(min_end);
            let search_end = (target_end + 50).min(max_end);

            for idx in (search_start..=search_end).rev() {
                if idx <= max_end && idx < node.len() && boundary::is_boundary(node, idx - 1) {
                    end = idx;
                    break;
                }
            }

            if end - start > capacity {
                end = start + capacity;
            }

            let remaining_after = node.len() - end;
            if remaining_after > 0
                && remaining_after < capacity / 4
                && (end - start) + remaining_after <= capacity
            {
                end = node.len();
            }

            if end - start > capacity {
                end = start + capacity;
            }

            let mut chunk = self.new_node_like(node);
            chunk.keys = node.keys[start..end].to_vec();
            chunk.vals = node.vals[start..end].to_vec();
            chunks.push(chunk);
            start = end;
        }

        chunks
    }

    fn merge_nodes(&self, left: &Node, right: &Node) -> Node {
        let mut merged = self.new_node_like(left);
        let merged_len = left.len() + right.len();
        merged.keys = Vec::with_capacity(merged_len);
        merged.keys.extend(left.keys.iter().cloned());
        merged.keys.extend(right.keys.iter().cloned());
        merged.vals = Vec::with_capacity(merged_len);
        merged.vals.extend(left.vals.iter().cloned());
        merged.vals.extend(right.vals.iter().cloned());
        merged
    }
}

#[cfg(feature = "async-store")]
fn collect_async_batch_route_contexts(
    path: &Arc<AsyncBatchRoutePath>,
    contexts: &mut HashMap<Cid, AsyncBatchAncestorContext>,
) {
    let mut current = Some(path.clone());

    while let Some(path) = current {
        let parent = path.parent.as_ref().map(|parent| AsyncBatchParentLink {
            parent_cid: parent.cid.clone(),
            child_index: parent.child_index,
        });

        contexts
            .entry(path.cid.clone())
            .or_insert_with(|| AsyncBatchAncestorContext {
                node: path.node.as_ref().clone(),
                parent,
            });

        current = path.parent.clone();
    }
}

#[cfg(feature = "async-store")]
fn should_close_append_leaf(node: &Node, max_chunk_size: usize) -> bool {
    if node.is_empty() {
        return false;
    }

    if node.len() >= max_chunk_size {
        return true;
    }

    boundary::is_boundary(node, node.len() - 1)
}

#[cfg(feature = "async-store")]
fn async_rightmost_entry_from_node_ref((cid, node): &(Cid, Node)) -> AsyncRightmostPathEntry {
    AsyncRightmostPathEntry {
        cid: cid.clone(),
        node: node.clone(),
        child_index: node.len().saturating_sub(1),
    }
}

#[cfg(feature = "async-store")]
fn cached_rightmost_entries(path: &[AsyncRightmostPathEntry]) -> Vec<CachedRightmostPathEntry> {
    path.iter()
        .map(|entry| CachedRightmostPathEntry {
            cid: entry.cid.clone(),
            node: entry.node.clone(),
            child_index: entry.child_index,
        })
        .collect()
}

#[cfg(feature = "async-store")]
fn async_rightmost_entries_from_cache(
    path: Vec<CachedRightmostPathEntry>,
) -> Vec<AsyncRightmostPathEntry> {
    path.into_iter()
        .map(|entry| AsyncRightmostPathEntry {
            cid: entry.cid,
            node: entry.node,
            child_index: entry.child_index,
        })
        .collect()
}

#[cfg(feature = "async-store")]
fn encode_rightmost_path_hint(path: &[AsyncRightmostPathEntry]) -> Result<Vec<u8>, Error> {
    let hint = AsyncRightmostPathHint {
        version: 2,
        entries: path
            .iter()
            .map(|entry| AsyncRightmostPathHintEntry {
                cid: entry.cid.clone(),
                child_index: entry.child_index,
            })
            .collect(),
    };
    serde_cbor::ser::to_vec_packed(&hint).map_err(|err| Error::Deserialize(err.to_string()))
}

#[cfg(feature = "async-store")]
fn rightmost_path_hint_is_valid(root: &Cid, path: &[AsyncRightmostPathEntry]) -> bool {
    if path.first().map(|entry| &entry.cid) != Some(root) {
        return false;
    }

    for (idx, entry) in path.iter().enumerate() {
        if entry.node.keys.len() != entry.node.vals.len() || entry.node.is_empty() {
            return false;
        }

        if entry.child_index != entry.node.len() - 1 {
            return false;
        }

        let is_last = idx + 1 == path.len();
        if is_last != entry.node.leaf {
            return false;
        }

        if !is_last {
            let Some(child) = entry.node.vals.get(entry.child_index) else {
                return false;
            };
            let child_bytes: [u8; 32] = match child.as_slice().try_into() {
                Ok(bytes) => bytes,
                Err(_) => return false,
            };
            if Cid(child_bytes) != path[idx + 1].cid {
                return false;
            }
        }
    }

    true
}

fn prefix_path_hint_key(root: &Cid, prefix: &[u8]) -> Vec<u8> {
    let prefix_hash = Cid::from_bytes(prefix);
    let mut key = Vec::with_capacity(root.as_bytes().len() + prefix_hash.as_bytes().len());
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(prefix_hash.as_bytes());
    key
}

fn changed_span_hint_key(base_root: Option<&Cid>, changed_root: Option<&Cid>) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + 32 + 32);
    append_optional_root_to_key(&mut key, base_root);
    append_optional_root_to_key(&mut key, changed_root);
    key
}

fn append_optional_root_to_key(key: &mut Vec<u8>, root: Option<&Cid>) {
    match root {
        Some(cid) => {
            key.push(1);
            key.extend_from_slice(cid.as_bytes());
        }
        None => key.push(0),
    }
}

fn encode_prefix_path_hint(
    root: &Cid,
    prefix: &[u8],
    path: &[PrefixPathHintEntryWithNode],
) -> Result<Vec<u8>, Error> {
    let hint = PrefixPathHint {
        version: PREFIX_PATH_HINT_VERSION,
        root: root.clone(),
        prefix: prefix.to_vec(),
        entries: path
            .iter()
            .map(|entry| PrefixPathHintEntry {
                cid: entry.cid.clone(),
                child_index: entry.child_index,
            })
            .collect(),
    };
    serde_cbor::ser::to_vec_packed(&hint).map_err(|err| Error::Deserialize(err.to_string()))
}

fn encode_changed_span_hint(hint: &ChangedSpanHint) -> Result<Vec<u8>, Error> {
    let wire = ChangedSpanHintWire {
        version: CHANGED_SPANS_HINT_VERSION,
        base_root: hint.base_root.clone(),
        changed_root: hint.changed_root.clone(),
        spans: hint.spans.clone(),
    };
    serde_cbor::ser::to_vec_packed(&wire).map_err(|err| Error::Deserialize(err.to_string()))
}

fn load_prefix_path_hint<S: Store>(
    prolly: &Prolly<S>,
    root: &Cid,
    prefix: &[u8],
) -> Result<bool, Error> {
    let Some(bytes) = prolly
        .store()
        .get_hint(
            PREFIX_PATH_HINT_NAMESPACE,
            &prefix_path_hint_key(root, prefix),
        )
        .map_err(|err| Error::Store(Box::new(err)))?
    else {
        return Ok(false);
    };

    let Ok(hint) = serde_cbor::from_slice::<PrefixPathHint>(&bytes) else {
        return Ok(false);
    };

    if hint.version != PREFIX_PATH_HINT_VERSION
        || hint.root != *root
        || hint.prefix != prefix
        || hint.entries.is_empty()
        || hint.entries.first().map(|entry| &entry.cid) != Some(root)
    {
        return Ok(false);
    }

    let keys = hint
        .entries
        .iter()
        .map(|entry| entry.cid.as_bytes())
        .collect::<Vec<_>>();
    let node_bytes = prolly
        .store()
        .batch_get_ordered(&keys)
        .map_err(|err| Error::Store(Box::new(err)))?;

    if node_bytes.len() != hint.entries.len() || node_bytes.iter().any(Option::is_none) {
        return Ok(false);
    }

    let mut path = Vec::with_capacity(hint.entries.len());
    for (entry, bytes) in hint.entries.into_iter().zip(node_bytes) {
        let Some(bytes) = bytes else {
            return Ok(false);
        };
        if Cid::from_bytes(&bytes) != entry.cid {
            return Ok(false);
        }
        let Ok(node) = Node::from_bytes(&bytes) else {
            return Ok(false);
        };
        path.push(PrefixPathHintEntryWithNode {
            cid: entry.cid,
            node: Arc::new(node),
            child_index: entry.child_index,
        });
    }

    if !prefix_path_hint_is_valid(root, prefix, &path) {
        return Ok(false);
    }

    for entry in path {
        prolly.cache_node(entry.cid, entry.node.as_ref().clone());
    }

    Ok(true)
}

fn load_changed_span_hint<S: Store>(
    prolly: &Prolly<S>,
    base_root: Option<&Cid>,
    changed_root: Option<&Cid>,
) -> Result<Option<ChangedSpanHint>, Error> {
    let Some(bytes) = prolly
        .store()
        .get_hint(
            CHANGED_SPANS_HINT_NAMESPACE,
            &changed_span_hint_key(base_root, changed_root),
        )
        .map_err(|err| Error::Store(Box::new(err)))?
    else {
        return Ok(None);
    };

    let Ok(wire) = serde_cbor::from_slice::<ChangedSpanHintWire>(&bytes) else {
        return Ok(None);
    };

    if wire.version != CHANGED_SPANS_HINT_VERSION
        || wire.base_root.as_ref() != base_root
        || wire.changed_root.as_ref() != changed_root
    {
        return Ok(None);
    }

    let spans = normalize_changed_spans(wire.spans);
    if spans.is_empty() {
        return Ok(None);
    }

    Ok(Some(ChangedSpanHint {
        base_root: wire.base_root,
        changed_root: wire.changed_root,
        spans,
    }))
}

fn prefix_path_hint_is_valid(
    root: &Cid,
    prefix: &[u8],
    path: &[PrefixPathHintEntryWithNode],
) -> bool {
    if path.first().map(|entry| &entry.cid) != Some(root) {
        return false;
    }

    for (idx, entry) in path.iter().enumerate() {
        if entry.node.keys.len() != entry.node.vals.len()
            || entry.node.is_empty()
            || entry.child_index >= entry.node.len()
            || entry.child_index != path_index_for_key(&entry.node, prefix)
        {
            return false;
        }

        let is_last = idx + 1 == path.len();
        if is_last != entry.node.leaf {
            return false;
        }

        if !is_last {
            let Ok(child_cid) = child_cid_at(&entry.node, entry.child_index) else {
                return false;
            };
            if path.get(idx + 1).map(|next| &next.cid) != Some(&child_cid) {
                return false;
            }
        }
    }

    true
}

fn normalize_changed_spans<I>(spans: I) -> Vec<ChangedSpan>
where
    I: IntoIterator<Item = ChangedSpan>,
{
    let mut spans = spans
        .into_iter()
        .filter(changed_span_is_valid)
        .collect::<Vec<_>>();
    spans.sort_by(|left, right| {
        left.start
            .cmp(&right.start)
            .then_with(|| compare_span_end(&left.end, &right.end))
    });

    let mut normalized: Vec<ChangedSpan> = Vec::with_capacity(spans.len());
    for span in spans {
        let Some(last) = normalized.last_mut() else {
            normalized.push(span);
            continue;
        };

        if span_starts_before_or_at_end(&span.start, &last.end) {
            last.end = max_span_end(last.end.take(), span.end);
        } else {
            normalized.push(span);
        }
    }

    normalized
}

fn changed_span_is_valid(span: &ChangedSpan) -> bool {
    span.end
        .as_ref()
        .map_or(true, |end| end.as_slice() > span.start.as_slice())
}

fn span_starts_before_or_at_end(start: &[u8], end: &Option<Vec<u8>>) -> bool {
    end.as_ref().map_or(true, |end| start <= end.as_slice())
}

fn max_span_end(left: Option<Vec<u8>>, right: Option<Vec<u8>>) -> Option<Vec<u8>> {
    match (left, right) {
        (None, _) | (_, None) => None,
        (Some(left), Some(right)) if right > left => Some(right),
        (Some(left), Some(_)) => Some(left),
    }
}

fn compare_span_end(left: &Option<Vec<u8>>, right: &Option<Vec<u8>>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(right),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn path_index_for_key(node: &Node, key: &[u8]) -> usize {
    match node.search(key) {
        Ok(idx) => idx,
        Err(idx) => idx.saturating_sub(1),
    }
}

fn fill_leaf_lookup_values<K: AsRef<[u8]>>(
    node: &Node,
    positions: InlinePositions,
    keys: &[K],
    values: &mut [Option<Vec<u8>>],
) -> Result<(), Error> {
    if node.keys.len() != node.vals.len() {
        return Err(Error::InvalidNode);
    }

    let mut leaf_idx = 0usize;
    let mut positions = positions.into_iter().peekable();
    while let Some(position) = positions.next() {
        let key = keys[position].as_ref();

        while leaf_idx < node.keys.len() && node.keys[leaf_idx].as_slice() < key {
            leaf_idx += 1;
        }

        let found_value = if leaf_idx < node.keys.len() && node.keys[leaf_idx].as_slice() == key {
            Some(&node.vals[leaf_idx])
        } else {
            None
        };

        if let Some(value) = found_value {
            values[position] = Some(value.clone());
        }

        while let Some(next_position) =
            positions.next_if(|next_position| keys[*next_position].as_ref() == key)
        {
            if let Some(value) = found_value {
                values[next_position] = Some(value.clone());
            }
        }
    }

    Ok(())
}

fn sorted_key_positions<K: AsRef<[u8]>>(keys: &[K]) -> Vec<usize> {
    let mut positions = (0..keys.len()).collect::<Vec<_>>();
    if keys_are_sorted(keys) {
        return positions;
    }

    positions.sort_by(|left, right| {
        keys[*left]
            .as_ref()
            .cmp(keys[*right].as_ref())
            .then_with(|| left.cmp(right))
    });
    positions
}

fn keys_are_sorted<K: AsRef<[u8]>>(keys: &[K]) -> bool {
    keys.windows(2)
        .all(|pair| pair[0].as_ref() <= pair[1].as_ref())
}

fn route_key_positions_to_children<K: AsRef<[u8]>>(
    node: &Node,
    positions: InlinePositions,
    keys: &[K],
) -> Result<Vec<KeyLookupFrame>, Error> {
    if node.is_empty() {
        return Err(Error::InvalidNode);
    }

    if positions.len() >= GET_MANY_BOUNDARY_ROUTE_MIN_POSITIONS && node.len() > 1 {
        return route_key_positions_to_children_by_boundary(node, positions, keys);
    }

    let mut frames: Vec<KeyLookupFrame> = Vec::with_capacity(node.len().min(positions.len()));
    let mut child_index = child_index_for_lookup_key(node, keys[positions.first].as_ref());
    let mut last_child_index = None;

    for position in positions {
        let key = keys[position].as_ref();
        while child_index + 1 < node.len() && key >= node.keys[child_index + 1].as_slice() {
            child_index += 1;
        }

        if last_child_index == Some(child_index) {
            let frame = frames.last_mut().ok_or(Error::InvalidNode)?;
            frame.positions.push(position);
        } else {
            frames.push(KeyLookupFrame {
                cid: child_cid_at(node, child_index)?,
                positions: InlinePositions::new(position),
            });
            last_child_index = Some(child_index);
        }
    }

    Ok(frames)
}

fn route_key_positions_to_children_by_boundary<K: AsRef<[u8]>>(
    node: &Node,
    positions: InlinePositions,
    keys: &[K],
) -> Result<Vec<KeyLookupFrame>, Error> {
    let position_count = positions.len();
    let mut frames = Vec::with_capacity(node.len().min(position_count));
    let mut child_index = child_index_for_lookup_key(node, keys[positions.at(0)].as_ref());
    let last_child_index =
        child_index_for_lookup_key(node, keys[positions.at(position_count - 1)].as_ref());
    let mut bucket_start = 0usize;

    while child_index < last_child_index {
        let boundary = node.keys.get(child_index + 1).ok_or(Error::InvalidNode)?;
        let bucket_end = lower_bound_position_key(
            &positions,
            keys,
            bucket_start..position_count,
            boundary.as_slice(),
        );

        if bucket_start < bucket_end {
            frames.push(KeyLookupFrame {
                cid: child_cid_at(node, child_index)?,
                positions: inline_positions_from_range(&positions, bucket_start..bucket_end),
            });
        }

        bucket_start = bucket_end;
        child_index += 1;
    }

    if bucket_start < position_count {
        frames.push(KeyLookupFrame {
            cid: child_cid_at(node, last_child_index)?,
            positions: inline_positions_from_range(&positions, bucket_start..position_count),
        });
    }

    Ok(frames)
}

fn lower_bound_position_key<K: AsRef<[u8]>>(
    positions: &InlinePositions,
    keys: &[K],
    range: Range<usize>,
    key: &[u8],
) -> usize {
    let mut left = range.start;
    let mut right = range.end;

    while left < right {
        let mid = left + (right - left) / 2;
        if keys[positions.at(mid)].as_ref() < key {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    left
}

fn inline_positions_from_range(
    positions: &InlinePositions,
    range: Range<usize>,
) -> InlinePositions {
    debug_assert!(range.start < range.end);
    let first = positions.at(range.start);
    let mut bucket = InlinePositions::with_rest_capacity(first, range.end - range.start - 1);

    for offset in range.start + 1..range.end {
        bucket.push(positions.at(offset));
    }

    bucket
}

fn child_index_for_lookup_key(node: &Node, key: &[u8]) -> usize {
    node.keys
        .partition_point(|candidate| candidate.as_slice() <= key)
        .saturating_sub(1)
}

fn leaf_value_at(node: &Node, idx: usize) -> Result<Vec<u8>, Error> {
    node.vals.get(idx).cloned().ok_or(Error::InvalidNode)
}

fn child_cid_at(node: &Node, idx: usize) -> Result<Cid, Error> {
    let child = node.vals.get(idx).ok_or(Error::InvalidNode)?;
    Ok(Cid(child
        .as_slice()
        .try_into()
        .map_err(|_| Error::InvalidNode)?))
}

#[cfg(feature = "async-store")]
fn reserve_node_entries(node: &mut Node, additional: usize) {
    node.keys.reserve(additional);
    node.vals.reserve(additional);
}

#[cfg(feature = "async-store")]
fn is_valid_boundary_between(left: &Node, _right: &Node) -> bool {
    if left.is_empty() {
        return false;
    }

    boundary::is_boundary(left, left.len() - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use error::Diff;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    #[cfg(feature = "async-store")]
    use std::{
        future::Future,
        task::{Context, Poll},
    };
    #[cfg(feature = "async-store")]
    use store::SyncStoreAsAsync;
    use store::{BatchOp, MemStore};

    #[cfg(feature = "async-store")]
    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut future = Box::pin(future);

        loop {
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[derive(Debug)]
    struct CountingStoreError;

    impl std::fmt::Display for CountingStoreError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("counting store error")
        }
    }

    impl std::error::Error for CountingStoreError {}

    #[derive(Default)]
    struct CountingStore {
        data: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
        prefer_batch_reads: bool,
        get_calls: AtomicUsize,
        put_calls: AtomicUsize,
        batch_calls: AtomicUsize,
        batch_put_calls: AtomicUsize,
        batch_get_ordered_calls: AtomicUsize,
        max_batch_get_ordered_len: AtomicUsize,
    }

    impl Store for CountingStore {
        type Error = CountingStoreError;

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            let data = self.data.lock().unwrap();
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            Ok(data.get(key).cloned())
        }

        fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            let mut data = self.data.lock().unwrap();
            self.put_calls.fetch_add(1, Ordering::Relaxed);
            data.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
            let mut data = self.data.lock().unwrap();
            data.remove(key);
            Ok(())
        }

        fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
            let mut data = self.data.lock().unwrap();
            self.batch_calls.fetch_add(1, Ordering::Relaxed);
            for op in ops {
                match op {
                    BatchOp::Upsert { key, value } => {
                        data.insert(key.to_vec(), value.to_vec());
                    }
                    BatchOp::Delete { key } => {
                        data.remove(*key);
                    }
                }
            }
            Ok(())
        }

        fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
            let mut data = self.data.lock().unwrap();
            self.batch_put_calls.fetch_add(1, Ordering::Relaxed);
            for (key, value) in entries {
                data.insert(key.to_vec(), value.to_vec());
            }
            Ok(())
        }

        fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            self.batch_get_ordered_calls.fetch_add(1, Ordering::Relaxed);
            self.max_batch_get_ordered_len
                .fetch_max(keys.len(), Ordering::Relaxed);
            let data = self.data.lock().unwrap();
            Ok(keys.iter().map(|key| data.get(*key).cloned()).collect())
        }

        fn prefers_batch_reads(&self) -> bool {
            self.prefer_batch_reads
        }
    }

    #[test]
    fn test_prolly_new() {
        let store = MemStore::new();
        let config = Config::default();
        let _prolly = Prolly::new(store, config);
    }

    #[test]
    fn test_create_empty_tree() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        assert!(tree.is_empty());
        assert!(tree.root.is_none());
    }

    #[cfg(feature = "async-store")]
    #[test]
    fn async_prolly_get_reads_tree_from_async_store() {
        let store = Arc::new(MemStore::new());
        let config = Config::default();
        let prolly = Prolly::new(store.clone(), config.clone());
        let tree = prolly.create();
        let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();

        let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);

        let value = block_on(async_prolly.get(&tree, b"b")).unwrap();
        assert_eq!(value, Some(b"2".to_vec()));
        assert_eq!(block_on(async_prolly.get(&tree, b"missing")).unwrap(), None);
        assert!(
            async_prolly.cache_len() > 0,
            "async reads should populate the async manager node cache"
        );

        async_prolly.clear_cache();
        assert_eq!(async_prolly.cache_len(), 0);
    }

    #[cfg(feature = "async-store")]
    #[test]
    fn async_prolly_get_many_preserves_order_duplicates_and_missing_keys() {
        let store = Arc::new(MemStore::new());
        let config = Config::default();
        let prolly = Prolly::new(store.clone(), config.clone());
        let mut tree = prolly.create();
        for (key, value) in [
            (b"a".as_slice(), b"1".as_slice()),
            (b"b".as_slice(), b"2".as_slice()),
            (b"c".as_slice(), b"3".as_slice()),
        ] {
            tree = prolly.put(&tree, key.to_vec(), value.to_vec()).unwrap();
        }

        let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
        let keys = vec![
            b"c".to_vec(),
            b"missing".to_vec(),
            b"a".to_vec(),
            b"c".to_vec(),
        ];

        let values = block_on(async_prolly.get_many(&tree, &keys)).unwrap();

        assert_eq!(
            values,
            vec![
                Some(b"3".to_vec()),
                None,
                Some(b"1".to_vec()),
                Some(b"3".to_vec())
            ]
        );
    }

    #[test]
    fn missing_node_batch_keeps_unique_positions_inline() {
        let cid_a = Cid::from_bytes(b"a");
        let cid_b = Cid::from_bytes(b"b");
        let mut batch = MissingNodeBatch::with_capacity(2);

        batch.record(&cid_a, 3);
        batch.record(&cid_b, 9);

        assert_eq!(batch.cids, vec![cid_a.clone(), cid_b]);
        assert_eq!(batch.positions[0].first, 3);
        assert_eq!(batch.positions[0].rest.capacity(), 0);
        assert_eq!(batch.positions[1].first, 9);
        assert_eq!(batch.positions[1].rest.capacity(), 0);

        batch.record(&cid_a, 11);
        assert_eq!(
            batch.positions.remove(0).into_iter().collect::<Vec<_>>(),
            vec![3, 11]
        );
    }

    #[test]
    fn load_many_ordered_deduplicates_missing_cids() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store.clone(), Config::default());
        let tree = prolly
            .put(&prolly.create(), b"key".to_vec(), b"value".to_vec())
            .unwrap();
        let root = tree.root.clone().unwrap();
        prolly.clear_cache();

        let nodes = prolly
            .load_many_ordered(&[root.clone(), root.clone(), root.clone()])
            .unwrap();

        assert_eq!(nodes.len(), 3);
        assert!(Arc::ptr_eq(&nodes[0], &nodes[1]));
        assert!(Arc::ptr_eq(&nodes[1], &nodes[2]));
        assert_eq!(prolly.cache_len(), 1);
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            1,
            "duplicate CIDs should collapse to one point read"
        );
        assert_eq!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed),
            0,
            "single-CID miss batches should not pay ordered batch overhead"
        );
    }

    #[test]
    fn load_many_ordered_serves_cache_hits_without_store_reads() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut cids = Vec::new();

        for idx in 0..3 {
            let mut node = Node::new_leaf();
            node.keys.push(format!("k{idx:02}").into_bytes());
            node.vals.push(format!("v{idx:02}").into_bytes());
            cids.push(prolly.save(&node).unwrap());
        }

        let calls_before = store.batch_get_ordered_calls.load(Ordering::Relaxed);
        let nodes = prolly
            .load_many_ordered_with_parallelism(
                &[cids[2].clone(), cids[0].clone(), cids[2].clone()],
                4,
            )
            .unwrap();

        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].keys[0], b"k02".to_vec());
        assert_eq!(nodes[1].keys[0], b"k00".to_vec());
        assert!(Arc::ptr_eq(&nodes[0], &nodes[2]));
        assert_eq!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed),
            calls_before,
            "all-cache-hit frontiers should not allocate miss work or call the store"
        );
    }

    #[test]
    fn load_many_ordered_reuses_cached_prefix_and_deduplicates_later_misses() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut nodes_to_cache = Vec::new();
        let mut cids = Vec::new();

        for idx in 0..3 {
            let mut node = Node::new_leaf();
            node.keys.push(format!("k{idx:02}").into_bytes());
            node.vals.push(format!("v{idx:02}").into_bytes());
            cids.push(prolly.save(&node).unwrap());
            nodes_to_cache.push(node);
        }

        prolly.clear_cache();
        prolly.cache_node(cids[0].clone(), nodes_to_cache[0].clone());
        prolly.cache_node(cids[2].clone(), nodes_to_cache[2].clone());

        let loaded = prolly
            .load_many_ordered(&[
                cids[0].clone(),
                cids[1].clone(),
                cids[0].clone(),
                cids[2].clone(),
                cids[1].clone(),
            ])
            .unwrap();

        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded[0].keys[0], b"k00".to_vec());
        assert_eq!(loaded[1].keys[0], b"k01".to_vec());
        assert_eq!(loaded[3].keys[0], b"k02".to_vec());
        assert!(Arc::ptr_eq(&loaded[0], &loaded[2]));
        assert!(Arc::ptr_eq(&loaded[1], &loaded[4]));
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            1,
            "only the single cold CID should be point-read, even when it appears twice"
        );
        assert_eq!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed),
            0,
            "default stores should avoid ordered batch overhead for one cold CID"
        );
    }

    #[test]
    fn load_many_ordered_unique_misses_use_point_reads_for_non_batched_stores() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut cids = Vec::new();

        for idx in 0..3 {
            let mut node = Node::new_leaf();
            node.keys.push(format!("k{idx:02}").into_bytes());
            node.vals.push(format!("v{idx:02}").into_bytes());
            cids.push(prolly.save(&node).unwrap());
        }
        prolly.clear_cache();

        let loaded = prolly.load_many_ordered(&cids).unwrap();

        assert_eq!(loaded.len(), cids.len());
        for (idx, node) in loaded.iter().enumerate() {
            assert_eq!(node.keys[0], format!("k{idx:02}").into_bytes());
        }
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            cids.len(),
            "point-read stores should avoid duplicate ordered-batch planning for unique misses"
        );
        assert_eq!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed),
            0,
            "point-read stores should not route already-unique misses through ordered batch reads"
        );
    }

    #[test]
    fn load_many_ordered_with_parallelism_splits_wide_misses() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut cids = Vec::new();

        for idx in 0..12 {
            let mut node = Node::new_leaf();
            node.keys.push(format!("k{idx:02}").into_bytes());
            node.vals.push(format!("v{idx:02}").into_bytes());
            cids.push(prolly.save(&node).unwrap());
        }
        prolly.clear_cache();

        let nodes = prolly.load_many_ordered_with_parallelism(&cids, 3).unwrap();

        assert_eq!(nodes.len(), cids.len());
        for (idx, node) in nodes.iter().enumerate() {
            assert_eq!(node.keys[0], format!("k{idx:02}").into_bytes());
        }
        assert_eq!(prolly.cache_len(), cids.len());
        assert_eq!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed),
            3,
            "12 misses with parallelism 3 should split into 3 ordered batch reads"
        );
        assert_eq!(
            store.max_batch_get_ordered_len.load(Ordering::Relaxed),
            4,
            "wide miss sets should be split into roughly even ordered batches"
        );
    }

    #[test]
    fn load_many_ordered_parallel_decode_preserves_order_cache_and_duplicates() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let prolly = Prolly::new(store.clone(), Config::default());
        let unique_count = PARALLEL_NODE_DECODE_THRESHOLD + 4;
        let mut cids = Vec::new();

        for idx in 0..unique_count {
            let mut node = Node::new_leaf();
            node.keys.push(format!("k{idx:02}").into_bytes());
            node.vals.push(format!("v{idx:02}").into_bytes());
            cids.push(prolly.save(&node).unwrap());
        }

        let mut requested = Vec::with_capacity(unique_count + 2);
        requested.push(cids[3].clone());
        requested.extend(cids.iter().cloned());
        requested.push(cids[3].clone());
        prolly.clear_cache();

        let nodes = prolly
            .load_many_ordered_with_parallelism(&requested, 4)
            .unwrap();

        assert_eq!(nodes.len(), requested.len());
        assert!(Arc::ptr_eq(&nodes[0], nodes.last().unwrap()));
        assert!(Arc::ptr_eq(&nodes[0], &nodes[4]));
        for (idx, node) in nodes[1..=unique_count].iter().enumerate() {
            assert_eq!(node.keys[0], format!("k{idx:02}").into_bytes());
        }
        assert_eq!(prolly.cache_len(), unique_count);
        assert_eq!(store.batch_get_ordered_calls.load(Ordering::Relaxed), 4);
        assert!(
            store.max_batch_get_ordered_len.load(Ordering::Relaxed) <= unique_count.div_ceil(4),
            "wide parallel-decode misses should still use bounded ordered batches"
        );
    }

    #[test]
    fn collect_stats_batches_child_frontiers_for_batched_read_stores() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut child_cids = Vec::new();

        for idx in 0..4 {
            let mut leaf = prolly.new_leaf_node();
            leaf.keys.push(format!("k{idx:02}").into_bytes());
            leaf.vals.push(format!("v{idx:02}").into_bytes());
            child_cids.push(prolly.save(&leaf).unwrap());
        }

        let mut root = prolly.new_internal_node(1);
        root.keys = (0..4)
            .map(|idx| format!("k{idx:02}").into_bytes())
            .collect();
        root.vals = child_cids.iter().map(|cid| cid.0.to_vec()).collect();
        let tree = Tree {
            root: Some(prolly.save(&root).unwrap()),
            config: Config::default(),
        };

        prolly.clear_cache();
        let stats = prolly.collect_stats(&tree).unwrap();

        assert_eq!(stats.num_nodes, 5);
        assert_eq!(stats.num_internal_nodes, 1);
        assert_eq!(stats.num_leaves, 4);
        assert_eq!(stats.total_key_value_pairs, 4);
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            0,
            "batched stats collection should hydrate frontiers through ordered batch reads"
        );
        assert_eq!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed),
            2,
            "stats should load the root frontier and then all leaf children as one child frontier"
        );
        assert_eq!(
            store.max_batch_get_ordered_len.load(Ordering::Relaxed),
            4,
            "the child frontier should be loaded as a single ordered batch"
        );
        assert_eq!(prolly.cache_len(), 5);
    }

    #[test]
    fn collect_stats_rejects_nodes_with_mismatched_values() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store, Config::default());
        let mut child = prolly.new_leaf_node();
        child.keys.push(b"k".to_vec());
        child.vals.push(b"v".to_vec());
        let child_cid = prolly.save(&child).unwrap();

        let mut malformed = prolly.new_internal_node(1);
        malformed.keys = vec![b"a".to_vec(), b"m".to_vec()];
        malformed.vals = vec![child_cid.0.to_vec()];
        let tree = Tree {
            root: Some(prolly.save(&malformed).unwrap()),
            config: Config::default(),
        };
        prolly.clear_cache();

        let err = prolly.collect_stats(&tree).unwrap_err();

        assert!(
            matches!(err, Error::InvalidNode | Error::Deserialize(_)),
            "malformed stats roots should not be silently accepted: {err:?}"
        );
    }

    #[test]
    fn sorted_key_positions_keeps_already_sorted_inputs_in_place() {
        let keys = vec![b"a".to_vec(), b"b".to_vec(), b"b".to_vec(), b"c".to_vec()];

        let positions = sorted_key_positions(&keys);

        assert_eq!(positions, vec![0, 1, 2, 3]);
    }

    #[test]
    fn sorted_key_positions_sorts_unsorted_inputs_stably() {
        let keys = vec![b"c".to_vec(), b"a".to_vec(), b"b".to_vec(), b"a".to_vec()];

        let positions = sorted_key_positions(&keys);

        assert_eq!(positions, vec![1, 3, 2, 0]);
    }

    #[test]
    fn get_many_child_routing_keeps_singleton_positions_inline() {
        let child_cids = [
            Cid::from_bytes(b"child-0"),
            Cid::from_bytes(b"child-1"),
            Cid::from_bytes(b"child-2"),
        ];
        let mut node = Node::new_internal(1);
        node.keys = vec![b"a".to_vec(), b"d".to_vec(), b"g".to_vec()];
        node.vals = child_cids.iter().map(|cid| cid.0.to_vec()).collect();
        let keys = vec![b"a".to_vec(), b"d".to_vec(), b"g".to_vec()];
        let positions = InlinePositions::from_vec(vec![0, 1, 2]).unwrap();

        let frames = route_key_positions_to_children(&node, positions, &keys).unwrap();

        assert_eq!(frames.len(), 3);
        for (idx, frame) in frames.iter().enumerate() {
            assert_eq!(frame.cid, child_cids[idx]);
            assert_eq!(frame.positions.first, idx);
            assert_eq!(frame.positions.rest.capacity(), 0);
        }
    }

    #[test]
    fn lookup_child_index_uses_separator_floor() {
        let mut node = Node::new_internal(1);
        node.keys = vec![b"a".to_vec(), b"d".to_vec(), b"g".to_vec()];

        assert_eq!(child_index_for_lookup_key(&node, b"0"), 0);
        assert_eq!(child_index_for_lookup_key(&node, b"a"), 0);
        assert_eq!(child_index_for_lookup_key(&node, b"c"), 0);
        assert_eq!(child_index_for_lookup_key(&node, b"d"), 1);
        assert_eq!(child_index_for_lookup_key(&node, b"f"), 1);
        assert_eq!(child_index_for_lookup_key(&node, b"g"), 2);
        assert_eq!(child_index_for_lookup_key(&node, b"z"), 2);
    }

    #[test]
    fn get_many_child_routing_starts_at_first_target_child() {
        let child_cids = [
            Cid::from_bytes(b"child-0"),
            Cid::from_bytes(b"child-1"),
            Cid::from_bytes(b"child-2"),
            Cid::from_bytes(b"child-3"),
        ];
        let mut node = Node::new_internal(1);
        node.keys = vec![b"a".to_vec(), b"d".to_vec(), b"g".to_vec(), b"m".to_vec()];
        node.vals = child_cids.iter().map(|cid| cid.0.to_vec()).collect();
        let keys = vec![b"h".to_vec(), b"z".to_vec()];
        let positions = InlinePositions::from_vec(vec![0, 1]).unwrap();

        let frames = route_key_positions_to_children(&node, positions, &keys).unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].cid, child_cids[2]);
        assert_eq!(frames[0].positions.first, 0);
        assert_eq!(frames[1].cid, child_cids[3]);
        assert_eq!(frames[1].positions.first, 1);
    }

    #[test]
    fn get_many_boundary_routing_skips_empty_children_and_routes_separator_keys_right() {
        let child_cids = [
            Cid::from_bytes(b"child-0"),
            Cid::from_bytes(b"child-1"),
            Cid::from_bytes(b"child-2"),
            Cid::from_bytes(b"child-3"),
            Cid::from_bytes(b"child-4"),
            Cid::from_bytes(b"child-5"),
        ];
        let mut node = Node::new_internal(1);
        node.keys = [0, 10, 20, 30, 40, 50]
            .into_iter()
            .map(|idx| format!("k{idx:03}").into_bytes())
            .collect();
        node.vals = child_cids.iter().map(|cid| cid.0.to_vec()).collect();
        let lookup_keys = [0, 1, 2, 10, 11, 49, 50, 51]
            .into_iter()
            .map(|idx| format!("k{idx:03}").into_bytes())
            .collect::<Vec<_>>();
        let positions = InlinePositions::from_vec((0..lookup_keys.len()).collect()).unwrap();

        let frames =
            route_key_positions_to_children_by_boundary(&node, positions, &lookup_keys).unwrap();
        let routed = frames
            .into_iter()
            .map(|frame| (frame.cid, frame.positions.into_iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        assert_eq!(
            routed,
            vec![
                (child_cids[0].clone(), vec![0, 1, 2]),
                (child_cids[1].clone(), vec![3, 4]),
                (child_cids[4].clone(), vec![5]),
                (child_cids[5].clone(), vec![6, 7]),
            ]
        );
    }

    #[test]
    fn large_get_many_child_routing_keeps_clustered_positions_together() {
        let child_cids = (0..10)
            .map(|idx| Cid::from_bytes(format!("child-{idx}").as_bytes()))
            .collect::<Vec<_>>();
        let mut node = Node::new_internal(1);
        node.keys = (0..10)
            .map(|idx| format!("k{:03}", idx * 100).into_bytes())
            .collect();
        node.vals = child_cids.iter().map(|cid| cid.0.to_vec()).collect();
        let lookup_keys = (500..580)
            .map(|idx| format!("k{idx:03}").into_bytes())
            .collect::<Vec<_>>();
        let positions = InlinePositions::from_vec((0..lookup_keys.len()).collect()).unwrap();

        let frames = route_key_positions_to_children(&node, positions, &lookup_keys).unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].cid, child_cids[5]);
        assert_eq!(frames[0].positions.len(), lookup_keys.len());
        assert_eq!(frames[0].positions.first, 0);
        assert_eq!(
            frames[0].positions.rest.last(),
            Some(&(lookup_keys.len() - 1))
        );
    }

    #[test]
    fn get_many_preserves_input_order_duplicates_and_missing_keys() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();
        let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();
        let keys = vec![
            b"c".to_vec(),
            b"missing".to_vec(),
            b"a".to_vec(),
            b"c".to_vec(),
        ];

        let values = prolly.get_many(&tree, &keys).unwrap();

        assert_eq!(
            values,
            vec![
                Some(b"3".to_vec()),
                None,
                Some(b"1".to_vec()),
                Some(b"3".to_vec()),
            ]
        );
    }

    #[test]
    fn clustered_get_many_uses_point_reads_for_singleton_frontiers_without_batched_read_preference()
    {
        let store = Arc::new(CountingStore::default());
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(u32::MAX)
            .build();

        let mut builder = builder::BatchBuilder::new(store.clone(), config.clone());
        for idx in 0..128 {
            builder.add(
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            );
        }
        let tree = builder.build().unwrap();
        let prolly = Prolly::new(store.clone(), config);
        prolly.clear_cache();
        let batch_gets_before = store.batch_get_ordered_calls.load(Ordering::Relaxed);

        let values = prolly
            .get_many(
                &tree,
                &[b"k001".to_vec(), b"k002".to_vec(), b"k003".to_vec()],
            )
            .unwrap();

        assert_eq!(
            values,
            vec![
                Some(b"v001".to_vec()),
                Some(b"v002".to_vec()),
                Some(b"v003".to_vec())
            ]
        );
        assert_eq!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed),
            batch_gets_before,
            "clustered get_many should avoid one-key ordered batch reads at each level"
        );
        assert!(
            store.get_calls.load(Ordering::Relaxed) > 0,
            "clustered get_many should still hydrate the singleton path"
        );
    }

    #[test]
    fn get_many_rejects_leaf_with_mismatched_values() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let mut leaf = prolly.new_leaf_node();
        leaf.keys.push(b"a".to_vec());
        let tree = Tree {
            root: Some(prolly.save(&leaf).unwrap()),
            config: Config::default(),
        };

        let err = prolly.get_many(&tree, &[b"a".to_vec()]).unwrap_err();

        assert!(matches!(err, Error::InvalidNode));
    }

    #[test]
    fn get_rejects_leaf_with_mismatched_values() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let mut leaf = prolly.new_leaf_node();
        leaf.keys.push(b"a".to_vec());
        let tree = Tree {
            root: Some(prolly.save(&leaf).unwrap()),
            config: Config::default(),
        };

        let err = match prolly.get(&tree, b"a") {
            Ok(_) => panic!("malformed leaf should be rejected"),
            Err(err) => err,
        };

        assert!(matches!(err, Error::InvalidNode));
    }

    #[test]
    fn get_and_find_path_reject_internal_node_with_missing_child_cid() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let mut root = Node::new_internal(1);
        root.keys.push(b"a".to_vec());
        let tree = Tree {
            root: Some(prolly.save(&root).unwrap()),
            config: Config::default(),
        };

        let get_err = match prolly.get(&tree, b"a") {
            Ok(_) => panic!("malformed internal node should be rejected by get"),
            Err(err) => err,
        };
        let path_err = match prolly.find_path(&tree, b"a") {
            Ok(_) => panic!("malformed internal node should be rejected by find_path"),
            Err(err) => err,
        };

        assert!(matches!(get_err, Error::InvalidNode));
        assert!(matches!(path_err, Error::InvalidNode));
    }

    #[test]
    fn get_many_rejects_internal_node_with_missing_child_cid() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let mut root = Node::new_internal(1);
        root.keys.push(b"a".to_vec());
        let tree = Tree {
            root: Some(prolly.save(&root).unwrap()),
            config: Config::default(),
        };

        let err = prolly.get_many(&tree, &[b"a".to_vec()]).unwrap_err();

        assert!(matches!(err, Error::InvalidNode));
    }

    #[test]
    fn get_many_splits_wide_frontiers_for_batched_read_stores() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(u32::MAX)
            .build();
        let key_for = |idx: usize| format!("k{idx:04}").into_bytes();

        let mut builder = builder::BatchBuilder::new(store.clone(), config.clone());
        for idx in 0..4096 {
            builder.add(key_for(idx), format!("v{idx:04}").into_bytes());
        }
        let tree = builder.build().unwrap();
        let prolly = Prolly::new(store.clone(), config);
        let indices = (0..4096).step_by(8).rev().collect::<Vec<_>>();
        let keys = indices.iter().map(|idx| key_for(*idx)).collect::<Vec<_>>();

        prolly.clear_cache();
        let calls_before = store.batch_get_ordered_calls.load(Ordering::Relaxed);
        let values = prolly.get_many(&tree, &keys).unwrap();

        assert_eq!(values.len(), keys.len());
        for (idx, value) in values.into_iter().enumerate() {
            assert_eq!(value, Some(format!("v{:04}", indices[idx]).into_bytes()));
        }
        assert!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed)
                > calls_before + GET_MANY_PREFETCH_PARALLELISM,
            "wide get_many should split frontier reads into parallel ordered batches"
        );
        assert!(
            store.max_batch_get_ordered_len.load(Ordering::Relaxed) <= 64,
            "bounded parallel get_many should avoid one huge ordered batch for hundreds of misses"
        );
    }

    #[test]
    fn test_get_empty_tree() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let result = prolly.get(&tree, b"key").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_put_and_get() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly
            .put(&tree, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        let result = prolly.get(&tree, b"key1").unwrap();

        assert_eq!(result, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_put_multiple_keys() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly
            .put(&tree, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        let tree = prolly
            .put(&tree, b"key2".to_vec(), b"value2".to_vec())
            .unwrap();
        let tree = prolly
            .put(&tree, b"key3".to_vec(), b"value3".to_vec())
            .unwrap();

        assert_eq!(
            prolly.get(&tree, b"key1").unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            prolly.get(&tree, b"key2").unwrap(),
            Some(b"value2".to_vec())
        );
        assert_eq!(
            prolly.get(&tree, b"key3").unwrap(),
            Some(b"value3".to_vec())
        );
    }

    #[test]
    fn test_put_update_existing() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly
            .put(&tree, b"key".to_vec(), b"value1".to_vec())
            .unwrap();
        let tree = prolly
            .put(&tree, b"key".to_vec(), b"value2".to_vec())
            .unwrap();

        assert_eq!(prolly.get(&tree, b"key").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_put_idempotent() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree1 = prolly
            .put(&tree, b"key".to_vec(), b"value".to_vec())
            .unwrap();
        let tree2 = prolly
            .put(&tree1, b"key".to_vec(), b"value".to_vec())
            .unwrap();

        // Same value should return same tree (same root)
        assert_eq!(tree1.root, tree2.root);
    }

    #[test]
    fn test_delete_existing_key() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly
            .put(&tree, b"key".to_vec(), b"value".to_vec())
            .unwrap();
        let tree = prolly.delete(&tree, b"key").unwrap();

        assert_eq!(prolly.get(&tree, b"key").unwrap(), None);
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly
            .put(&tree, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        let tree2 = prolly.delete(&tree, b"nonexistent").unwrap();

        // Should return same tree (idempotent)
        assert_eq!(tree.root, tree2.root);
    }

    #[test]
    fn test_delete_empty_tree() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree2 = prolly.delete(&tree, b"key").unwrap();

        // Should return same empty tree
        assert!(tree2.is_empty());
    }

    #[test]
    fn test_delete_last_key_makes_empty() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly
            .put(&tree, b"key".to_vec(), b"value".to_vec())
            .unwrap();
        let tree = prolly.delete(&tree, b"key").unwrap();

        assert!(tree.is_empty());
    }

    #[test]
    fn test_get_nonexistent_key() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly
            .put(&tree, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();
        let result = prolly.get(&tree, b"nonexistent").unwrap();

        assert_eq!(result, None);
    }

    #[test]
    fn test_immutability() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree1 = prolly.create();

        let tree2 = prolly
            .put(&tree1, b"key".to_vec(), b"value".to_vec())
            .unwrap();

        // Original tree should still be empty
        assert!(tree1.is_empty());
        // New tree should have the key
        assert!(!tree2.is_empty());
    }

    #[test]
    fn test_keys_sorted_order() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        // Insert in reverse order
        let tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();

        // All keys should be retrievable
        assert_eq!(prolly.get(&tree, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(prolly.get(&tree, b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(prolly.get(&tree, b"c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn test_put_batches_non_append_rebalance_writes() {
        let store = Arc::new(CountingStore::default());
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(1000000)
            .build();
        let prolly = Prolly::new(store.clone(), config);
        let mut tree = prolly.create();

        for i in 0..20 {
            tree = prolly
                .put(
                    &tree,
                    format!("k{i:03}").into_bytes(),
                    format!("v{i:03}").into_bytes(),
                )
                .unwrap();
        }

        let put_calls_before = store.put_calls.load(Ordering::Relaxed);
        let batch_put_calls_before = store.batch_put_calls.load(Ordering::Relaxed);

        let tree = prolly
            .put(&tree, b"k010".to_vec(), b"changed".to_vec())
            .unwrap();

        assert_eq!(
            prolly.get(&tree, b"k010").unwrap(),
            Some(b"changed".to_vec())
        );
        assert_eq!(
            store.put_calls.load(Ordering::Relaxed),
            put_calls_before,
            "non-append put should avoid per-node store.put calls"
        );
        assert_eq!(
            store.batch_put_calls.load(Ordering::Relaxed) - batch_put_calls_before,
            1,
            "non-append put should flush rewritten path in one batch"
        );
    }

    #[test]
    fn test_delete_batches_rebalance_writes() {
        let store = Arc::new(CountingStore::default());
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(1000000)
            .build();
        let prolly = Prolly::new(store.clone(), config);
        let mut tree = prolly.create();

        for i in 0..20 {
            tree = prolly
                .put(
                    &tree,
                    format!("k{i:03}").into_bytes(),
                    format!("v{i:03}").into_bytes(),
                )
                .unwrap();
        }

        let put_calls_before = store.put_calls.load(Ordering::Relaxed);
        let batch_put_calls_before = store.batch_put_calls.load(Ordering::Relaxed);

        let tree = prolly.delete(&tree, b"k010").unwrap();

        assert_eq!(prolly.get(&tree, b"k010").unwrap(), None);
        assert_eq!(
            store.put_calls.load(Ordering::Relaxed),
            put_calls_before,
            "delete should avoid per-node store.put calls"
        );
        assert_eq!(
            store.batch_put_calls.load(Ordering::Relaxed) - batch_put_calls_before,
            1,
            "delete should flush rewritten path in one batch"
        );
    }

    #[test]
    fn test_rebalance_split_on_max_chunk_size() {
        let store = MemStore::new();
        // Use a small max_chunk_size to force splits
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(1000000) // High factor to minimize hash-based splits
            .build();
        let prolly = Prolly::new(store, config);
        let tree = prolly.create();

        // Insert enough keys to trigger a split
        let mut tree = tree;
        for i in 0..10 {
            let key = format!("key{:02}", i).into_bytes();
            let val = format!("val{:02}", i).into_bytes();
            tree = prolly.put(&tree, key, val).unwrap();
        }

        // All keys should still be retrievable after splits
        for i in 0..10 {
            let key = format!("key{:02}", i).into_bytes();
            let expected = format!("val{:02}", i).into_bytes();
            assert_eq!(prolly.get(&tree, &key).unwrap(), Some(expected));
        }
    }

    #[test]
    fn test_rebalance_creates_new_root_on_split() {
        let store = MemStore::new();
        // Use a very small max_chunk_size to force root split
        let config = Config::builder()
            .min_chunk_size(1)
            .max_chunk_size(3)
            .chunking_factor(1000000) // High factor to minimize hash-based splits
            .build();
        let prolly = Prolly::new(store, config);
        let tree = prolly.create();

        // Insert enough keys to trigger root split
        let mut tree = tree;
        for i in 0..10 {
            let key = format!("k{:02}", i).into_bytes();
            let val = format!("v{:02}", i).into_bytes();
            tree = prolly.put(&tree, key, val).unwrap();
        }

        // Tree should have a root
        assert!(tree.root.is_some());

        // All keys should be retrievable
        for i in 0..10 {
            let key = format!("k{:02}", i).into_bytes();
            let expected = format!("v{:02}", i).into_bytes();
            assert_eq!(
                prolly.get(&tree, &key).unwrap(),
                Some(expected),
                "Key k{:02} not found",
                i
            );
        }
    }

    #[test]
    fn test_rebalance_propagates_changes_to_root() {
        let store = MemStore::new();
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(5)
            .chunking_factor(1000000)
            .build();
        let prolly = Prolly::new(store, config);
        let tree = prolly.create();

        // Build a tree with multiple levels
        let mut tree = tree;
        for i in 0..20 {
            let key = format!("key{:03}", i).into_bytes();
            let val = format!("val{:03}", i).into_bytes();
            tree = prolly.put(&tree, key, val).unwrap();
        }

        // Verify all keys are accessible
        for i in 0..20 {
            let key = format!("key{:03}", i).into_bytes();
            let expected = format!("val{:03}", i).into_bytes();
            assert_eq!(prolly.get(&tree, &key).unwrap(), Some(expected));
        }
    }

    #[test]
    fn test_delete_triggers_rebalance() {
        let store = MemStore::new();
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(5)
            .chunking_factor(1000000)
            .build();
        let prolly = Prolly::new(store, config);
        let tree = prolly.create();

        // Build a tree
        let mut tree = tree;
        for i in 0..10 {
            let key = format!("key{:02}", i).into_bytes();
            let val = format!("val{:02}", i).into_bytes();
            tree = prolly.put(&tree, key, val).unwrap();
        }

        // Delete some keys
        for i in 0..5 {
            let key = format!("key{:02}", i).into_bytes();
            tree = prolly.delete(&tree, &key).unwrap();
        }

        // Remaining keys should still be accessible
        for i in 5..10 {
            let key = format!("key{:02}", i).into_bytes();
            let expected = format!("val{:02}", i).into_bytes();
            assert_eq!(prolly.get(&tree, &key).unwrap(), Some(expected));
        }

        // Deleted keys should not be found
        for i in 0..5 {
            let key = format!("key{:02}", i).into_bytes();
            assert_eq!(prolly.get(&tree, &key).unwrap(), None);
        }
    }

    #[test]
    fn test_boundary_based_splitting() {
        let store = MemStore::new();
        // Use a low chunking factor to increase boundary probability
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(100)
            .chunking_factor(4) // Low factor = more boundaries
            .build();
        let prolly = Prolly::new(store, config);
        let tree = prolly.create();

        // Insert many keys
        let mut tree = tree;
        for i in 0..50 {
            let key = format!("key{:03}", i).into_bytes();
            let val = format!("val{:03}", i).into_bytes();
            tree = prolly.put(&tree, key, val).unwrap();
        }

        // All keys should be retrievable
        for i in 0..50 {
            let key = format!("key{:03}", i).into_bytes();
            let expected = format!("val{:03}", i).into_bytes();
            assert_eq!(prolly.get(&tree, &key).unwrap(), Some(expected));
        }
    }

    // ========== Range Iteration Tests ==========

    #[test]
    fn test_range_empty_tree() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let results: Vec<_> = prolly.range(&tree, &[], None).unwrap().collect();
        assert!(results.is_empty());
    }

    #[test]
    fn range_empty_half_open_bounds_skip_tree_seek() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store.clone(), Config::default());
        let tree = prolly
            .put(&prolly.create(), b"k001".to_vec(), b"v001".to_vec())
            .unwrap();
        prolly.clear_cache();
        let get_calls_before = store.get_calls.load(Ordering::Relaxed);
        let batch_get_calls_before = store.batch_get_ordered_calls.load(Ordering::Relaxed);

        let results = prolly
            .range(&tree, b"k010", Some(b"k001"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(results.is_empty());
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            get_calls_before,
            "empty half-open ranges should not seek into the tree"
        );
        assert_eq!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed),
            batch_get_calls_before,
            "empty half-open ranges should not batch-load nodes"
        );
    }

    #[test]
    fn test_range_single_element() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly
            .put(&tree, b"key".to_vec(), b"value".to_vec())
            .unwrap();

        let results: Vec<_> = prolly
            .range(&tree, &[], None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (b"key".to_vec(), b"value".to_vec()));
    }

    #[test]
    fn test_range_all_elements() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();

        let results: Vec<_> = prolly
            .range(&tree, &[], None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(results[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(results[2], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn test_range_with_start_bound() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();

        // Start from "b"
        let results: Vec<_> = prolly
            .range(&tree, b"b", None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(results[1], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn test_range_with_end_bound() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();

        // End at "c" (exclusive)
        let results: Vec<_> = prolly
            .range(&tree, &[], Some(b"c"))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(results[1], (b"b".to_vec(), b"2".to_vec()));
    }

    #[test]
    fn range_rejects_leaf_with_mismatched_values() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let mut leaf = prolly.new_leaf_node();
        leaf.keys.push(b"a".to_vec());
        let tree = Tree {
            root: Some(prolly.save(&leaf).unwrap()),
            config: Config::default(),
        };

        let err = prolly.range(&tree, &[], None).unwrap().next().unwrap();

        assert!(matches!(err, Err(Error::InvalidNode)));
    }

    #[test]
    fn range_rejects_internal_node_with_missing_next_child_cid() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let mut first_leaf = prolly.new_leaf_node();
        first_leaf.keys.push(b"a".to_vec());
        first_leaf.vals.push(b"1".to_vec());
        let first_cid = prolly.save(&first_leaf).unwrap();

        let mut root = prolly.new_internal_node(1);
        root.keys = vec![b"a".to_vec(), b"m".to_vec()];
        root.vals = vec![first_cid.0.to_vec()];
        let tree = Tree {
            root: Some(prolly.save(&root).unwrap()),
            config: Config::default(),
        };

        let mut iter = prolly.range(&tree, &[], None).unwrap();
        assert_eq!(
            iter.next().unwrap().unwrap(),
            (b"a".to_vec(), b"1".to_vec())
        );
        let err = iter.next().unwrap();

        assert!(matches!(err, Err(Error::InvalidNode)));
    }

    #[test]
    fn range_end_bound_skips_loading_next_child_subtree() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store.clone(), Config::default());

        let mut first = prolly.new_leaf_node();
        first.keys = vec![b"a".to_vec(), b"b".to_vec()];
        first.vals = vec![b"1".to_vec(), b"2".to_vec()];
        let first_cid = prolly.save(&first).unwrap();

        let mut second = prolly.new_leaf_node();
        second.keys = vec![b"c".to_vec(), b"d".to_vec()];
        second.vals = vec![b"3".to_vec(), b"4".to_vec()];
        let second_cid = prolly.save(&second).unwrap();

        let mut third = prolly.new_leaf_node();
        third.keys = vec![b"e".to_vec(), b"f".to_vec()];
        third.vals = vec![b"5".to_vec(), b"6".to_vec()];
        let third_cid = prolly.save(&third).unwrap();

        let mut root = prolly.new_internal_node(1);
        root.keys = vec![b"a".to_vec(), b"c".to_vec(), b"e".to_vec()];
        root.vals = vec![
            first_cid.0.to_vec(),
            second_cid.0.to_vec(),
            third_cid.0.to_vec(),
        ];
        let tree = Tree {
            root: Some(prolly.save(&root).unwrap()),
            config: Config::default(),
        };

        prolly.clear_cache();
        let gets_before = store.get_calls.load(Ordering::Relaxed);
        let results = prolly
            .range(&tree, &[], Some(b"e"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            results,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
                (b"d".to_vec(), b"4".to_vec()),
            ]
        );
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed) - gets_before,
            3,
            "bounded range should load root and in-range leaves, not the first leaf at the exclusive end"
        );
    }

    #[test]
    fn range_batches_in_range_sibling_hydration_for_batched_read_stores() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let prolly = Prolly::new(store.clone(), Config::default());

        let mut child_cids = Vec::new();
        let mut expected = Vec::new();
        for leaf_idx in 0..5 {
            let mut leaf = prolly.new_leaf_node();
            for entry_idx in 0..2 {
                let idx = leaf_idx * 2 + entry_idx;
                leaf.keys.push(format!("k{idx:02}").into_bytes());
                leaf.vals.push(format!("v{idx:02}").into_bytes());
                if idx < 6 {
                    expected.push((
                        format!("k{idx:02}").into_bytes(),
                        format!("v{idx:02}").into_bytes(),
                    ));
                }
            }
            child_cids.push(prolly.save(&leaf).unwrap());
        }

        let mut root = prolly.new_internal_node(1);
        root.keys = (0..5)
            .map(|leaf_idx| format!("k{:02}", leaf_idx * 2).into_bytes())
            .collect();
        root.vals = child_cids.iter().map(|cid| cid.0.to_vec()).collect();
        let tree = Tree {
            root: Some(prolly.save(&root).unwrap()),
            config: Config::default(),
        };

        prolly.clear_cache();
        let gets_before = store.get_calls.load(Ordering::Relaxed);
        let ordered_gets_before = store.batch_get_ordered_calls.load(Ordering::Relaxed);
        let results = prolly
            .range(&tree, &[], Some(b"k06"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(results, expected);
        assert!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed) > ordered_gets_before,
            "batched-read range scans should hydrate upcoming in-range siblings together"
        );
        assert_eq!(
            store.max_batch_get_ordered_len.load(Ordering::Relaxed),
            2,
            "prefetch should include the two remaining in-range leaves but stop before the exclusive end"
        );
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed) - gets_before,
            2,
            "range should only point-read the root and initial seek leaf"
        );
        assert_eq!(
            prolly.cache_len(),
            4,
            "cache should contain root plus the three in-range leaves, not the leaf at the exclusive end"
        );
    }

    #[test]
    fn range_reuses_cached_nodes_on_repeated_scan() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store.clone(), Config::default());

        let mut first = prolly.new_leaf_node();
        first.keys = vec![b"a".to_vec(), b"b".to_vec()];
        first.vals = vec![b"1".to_vec(), b"2".to_vec()];
        let first_cid = prolly.save(&first).unwrap();

        let mut second = prolly.new_leaf_node();
        second.keys = vec![b"c".to_vec(), b"d".to_vec()];
        second.vals = vec![b"3".to_vec(), b"4".to_vec()];
        let second_cid = prolly.save(&second).unwrap();

        let mut root = prolly.new_internal_node(1);
        root.keys = vec![b"a".to_vec(), b"c".to_vec()];
        root.vals = vec![first_cid.0.to_vec(), second_cid.0.to_vec()];
        let tree = Tree {
            root: Some(prolly.save(&root).unwrap()),
            config: Config::default(),
        };

        prolly.clear_cache();
        let first_results = prolly
            .range(&tree, &[], None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let gets_after_first = store.get_calls.load(Ordering::Relaxed);
        let second_results = prolly
            .range(&tree, &[], None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(first_results, second_results);
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            gets_after_first,
            "second range scan should reuse cached Arc<Node> path and child leaves"
        );
    }

    #[test]
    fn range_cursor_end_bound_skips_loading_next_child_subtree() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store.clone(), Config::default());

        let mut first = prolly.new_leaf_node();
        first.keys = vec![b"a".to_vec(), b"b".to_vec()];
        first.vals = vec![b"1".to_vec(), b"2".to_vec()];
        let first_cid = prolly.save(&first).unwrap();

        let mut second = prolly.new_leaf_node();
        second.keys = vec![b"c".to_vec(), b"d".to_vec()];
        second.vals = vec![b"3".to_vec(), b"4".to_vec()];
        let second_cid = prolly.save(&second).unwrap();

        let mut third = prolly.new_leaf_node();
        third.keys = vec![b"e".to_vec(), b"f".to_vec()];
        third.vals = vec![b"5".to_vec(), b"6".to_vec()];
        let third_cid = prolly.save(&third).unwrap();

        let mut root = prolly.new_internal_node(1);
        root.keys = vec![b"a".to_vec(), b"c".to_vec(), b"e".to_vec()];
        root.vals = vec![
            first_cid.0.to_vec(),
            second_cid.0.to_vec(),
            third_cid.0.to_vec(),
        ];
        let tree = Tree {
            root: Some(prolly.save(&root).unwrap()),
            config: Config::default(),
        };

        let gets_before = store.get_calls.load(Ordering::Relaxed);
        let results = prolly
            .range_cursor(&tree, &[], Some(b"e"))
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(
            results,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
                (b"d".to_vec(), b"4".to_vec()),
            ]
        );
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed) - gets_before,
            3,
            "bounded cursor range should load root and in-range leaves, not the first leaf at the exclusive end"
        );
    }

    #[test]
    fn range_cursor_empty_half_open_bounds_skip_tree_seek() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store.clone(), Config::default());
        let tree = prolly
            .put(&prolly.create(), b"k001".to_vec(), b"v001".to_vec())
            .unwrap();
        let get_calls_before = store.get_calls.load(Ordering::Relaxed);

        let results = prolly
            .range_cursor(&tree, b"k010", Some(b"k001"))
            .unwrap()
            .collect::<Vec<_>>();

        assert!(results.is_empty());
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            get_calls_before,
            "empty cursor ranges should not load the root node"
        );
    }

    #[test]
    fn test_range_with_both_bounds() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"d".to_vec(), b"4".to_vec()).unwrap();

        // Range [b, d)
        let results: Vec<_> = prolly
            .range(&tree, b"b", Some(b"d"))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(results[1], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn test_range_start_not_found() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"e".to_vec(), b"5".to_vec()).unwrap();

        // Start from "b" (doesn't exist, should start at "c")
        let results: Vec<_> = prolly
            .range(&tree, b"b", None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (b"c".to_vec(), b"3".to_vec()));
        assert_eq!(results[1], (b"e".to_vec(), b"5".to_vec()));
    }

    #[test]
    fn test_range_lexicographic_order() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        // Insert in random order
        let tree = prolly.put(&tree, b"zebra".to_vec(), b"z".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"apple".to_vec(), b"a".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"mango".to_vec(), b"m".to_vec()).unwrap();
        let tree = prolly
            .put(&tree, b"banana".to_vec(), b"b".to_vec())
            .unwrap();

        let results: Vec<_> = prolly
            .range(&tree, &[], None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        // Should be in lexicographic order
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0, b"apple".to_vec());
        assert_eq!(results[1].0, b"banana".to_vec());
        assert_eq!(results[2].0, b"mango".to_vec());
        assert_eq!(results[3].0, b"zebra".to_vec());
    }

    #[test]
    fn test_range_across_multiple_nodes() {
        let store = MemStore::new();
        // Use small max_chunk_size to force multiple nodes
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(1000000)
            .build();
        let prolly = Prolly::new(store, config);
        let tree = prolly.create();

        // Insert enough keys to span multiple nodes
        let mut tree = tree;
        for i in 0..20 {
            let key = format!("key{:02}", i).into_bytes();
            let val = format!("val{:02}", i).into_bytes();
            tree = prolly.put(&tree, key, val).unwrap();
        }

        // Iterate over all and verify order
        let results: Vec<_> = prolly
            .range(&tree, &[], None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(results.len(), 20);

        // Verify keys are in order
        for (i, item) in results.iter().enumerate().take(20) {
            let expected_key = format!("key{:02}", i).into_bytes();
            let expected_val = format!("val{:02}", i).into_bytes();
            assert_eq!(item, &(expected_key, expected_val));
        }
    }

    #[test]
    fn test_range_empty_result() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();

        // Range that doesn't match anything
        let results: Vec<_> = prolly
            .range(&tree, b"c", Some(b"d"))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert!(results.is_empty());
    }

    #[test]
    fn test_range_start_past_all_keys() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        let tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();

        // Start past all keys
        let results: Vec<_> = prolly
            .range(&tree, b"z", None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert!(results.is_empty());
    }

    // ========== Diff Tests ==========

    #[test]
    fn test_diff_same_tree() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();

        // Diff of same tree should be empty
        let diffs = prolly.diff(&tree, &tree).unwrap();
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_diff_empty_trees() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();
        let other = prolly.create();

        // Diff of two empty trees should be empty
        let diffs = prolly.diff(&base, &other).unwrap();
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_diff_added_entries() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let other = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();

        let diffs = prolly.diff(&base, &other).unwrap();

        // Should have one Added entry
        assert_eq!(diffs.len(), 1);
        assert!(matches!(
            &diffs[0],
            Diff::Added { key, val } if key == b"b" && val == b"2"
        ));
    }

    #[test]
    fn test_diff_removed_entries() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let base = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
        let other = prolly.delete(&base, b"b").unwrap();

        let diffs = prolly.diff(&base, &other).unwrap();

        // Should have one Removed entry
        assert_eq!(diffs.len(), 1);
        assert!(matches!(
            &diffs[0],
            Diff::Removed { key, val } if key == b"b" && val == b"2"
        ));
    }

    #[test]
    fn test_diff_changed_entries() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let other = prolly.put(&base, b"a".to_vec(), b"2".to_vec()).unwrap();

        let diffs = prolly.diff(&base, &other).unwrap();

        // Should have one Changed entry
        assert_eq!(diffs.len(), 1);
        assert!(matches!(
            &diffs[0],
            Diff::Changed { key, old, new } if key == b"a" && old == b"1" && new == b"2"
        ));
    }

    #[test]
    fn test_diff_mixed_changes() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        // Base tree: a=1, b=2, c=3
        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let base = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
        let base = prolly.put(&base, b"c".to_vec(), b"3".to_vec()).unwrap();

        // Other tree: a=1 (unchanged), b=X (changed), d=4 (added), c removed
        let other = prolly.put(&base, b"b".to_vec(), b"X".to_vec()).unwrap();
        let other = prolly.put(&other, b"d".to_vec(), b"4".to_vec()).unwrap();
        let other = prolly.delete(&other, b"c").unwrap();

        let diffs = prolly.diff(&base, &other).unwrap();

        // Should have 3 diffs: Changed(b), Added(d), Removed(c)
        assert_eq!(diffs.len(), 3);

        // Check for Changed entry
        assert!(diffs.iter().any(|d| matches!(
            d,
            Diff::Changed { key, old, new } if key == b"b" && old == b"2" && new == b"X"
        )));

        // Check for Added entry
        assert!(diffs.iter().any(|d| matches!(
            d,
            Diff::Added { key, val } if key == b"d" && val == b"4"
        )));

        // Check for Removed entry
        assert!(diffs.iter().any(|d| matches!(
            d,
            Diff::Removed { key, val } if key == b"c" && val == b"3"
        )));
    }

    #[test]
    fn test_diff_from_empty_base() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let other = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let other = prolly.put(&other, b"b".to_vec(), b"2".to_vec()).unwrap();

        let diffs = prolly.diff(&base, &other).unwrap();

        // All entries in other should be Added
        assert_eq!(diffs.len(), 2);
        assert!(diffs.iter().all(|d| matches!(d, Diff::Added { .. })));
    }

    #[test]
    fn test_diff_to_empty_other() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let base = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();

        let other = prolly.create();

        let diffs = prolly.diff(&base, &other).unwrap();

        // All entries in base should be Removed
        assert_eq!(diffs.len(), 2);
        assert!(diffs.iter().all(|d| matches!(d, Diff::Removed { .. })));
    }

    #[test]
    fn test_diff_no_changes() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();

        // Create other by putting same key-value (should have same root due to idempotence)
        let other = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();

        let diffs = prolly.diff(&base, &other).unwrap();

        // Should be empty since trees are identical
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_diff_across_multiple_nodes() {
        let store = MemStore::new();
        // Use default config to avoid triggering rebalancing edge cases
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        // Build base tree
        let mut base = base;
        for i in 0..10 {
            let key = format!("key{:02}", i).into_bytes();
            let val = format!("val{:02}", i).into_bytes();
            base = prolly.put(&base, key, val).unwrap();
        }

        // Create other with some changes starting from base
        // Change key05
        let other = prolly
            .put(&base, b"key05".to_vec(), b"changed".to_vec())
            .unwrap();
        // Add key10
        let other = prolly
            .put(&other, b"key10".to_vec(), b"val10".to_vec())
            .unwrap();

        let diffs = prolly.diff(&base, &other).unwrap();

        // Should have 2 diffs: Changed(key05), Added(key10)
        assert_eq!(diffs.len(), 2);

        // Check for Changed entry
        assert!(diffs.iter().any(|d| matches!(
            d,
            Diff::Changed { key, old, new } if key == b"key05" && old == b"val05" && new == b"changed"
        )), "Expected Changed entry for key05");

        // Check for Added entry
        assert!(
            diffs.iter().any(|d| matches!(
                d,
                Diff::Added { key, val } if key == b"key10" && val == b"val10"
            )),
            "Expected Added entry for key10"
        );
    }

    // ========== Merge Tests ==========

    #[test]
    fn test_merge_no_changes() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();

        // Both branches are identical to base
        let merged = prolly.merge(&base, &base, &base, None).unwrap();

        // Merged should be same as base
        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"1".to_vec()));
    }

    #[test]
    fn test_merge_only_left_changes() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let left = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();

        // Right is same as base
        let merged = prolly.merge(&base, &left, &base, None).unwrap();

        // Merged should have both keys
        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(prolly.get(&merged, b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_merge_only_right_changes() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let right = prolly.put(&base, b"c".to_vec(), b"3".to_vec()).unwrap();

        // Left is same as base
        let merged = prolly.merge(&base, &base, &right, None).unwrap();

        // Merged should have both keys
        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(prolly.get(&merged, b"c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn test_merge_both_add_different_keys() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let left = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
        let right = prolly.put(&base, b"c".to_vec(), b"3".to_vec()).unwrap();

        // No conflict - different keys
        let merged = prolly.merge(&base, &left, &right, None).unwrap();

        // Merged should have all keys
        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(prolly.get(&merged, b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(prolly.get(&merged, b"c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn test_merge_both_add_same_key_same_value() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let left = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
        let right = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();

        // No conflict - same key with same value
        let merged = prolly.merge(&base, &left, &right, None).unwrap();

        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(prolly.get(&merged, b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_merge_conflict_without_resolver() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let left = prolly.put(&base, b"a".to_vec(), b"left".to_vec()).unwrap();
        let right = prolly.put(&base, b"a".to_vec(), b"right".to_vec()).unwrap();

        // Conflict - same key with different values, no resolver
        let result = prolly.merge(&base, &left, &right, None);

        assert!(matches!(result, Err(Error::Conflict(_))));
        if let Err(Error::Conflict(conflict)) = result {
            assert_eq!(conflict.key, b"a".to_vec());
            assert_eq!(conflict.base, Some(b"1".to_vec()));
            assert_eq!(conflict.left, Some(b"left".to_vec()));
            assert_eq!(conflict.right, Some(b"right".to_vec()));
        }
    }

    #[test]
    fn test_merge_conflict_with_resolver_prefer_left() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let left = prolly.put(&base, b"a".to_vec(), b"left".to_vec()).unwrap();
        let right = prolly.put(&base, b"a".to_vec(), b"right".to_vec()).unwrap();

        // Resolver that prefers left
        let resolver: error::Resolver =
            Box::new(|c| error::Resolution::value(c.left.clone().expect("left value")));
        let merged = prolly.merge(&base, &left, &right, Some(resolver)).unwrap();

        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"left".to_vec()));
    }

    #[test]
    fn test_merge_conflict_with_resolver_prefer_right() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let left = prolly.put(&base, b"a".to_vec(), b"left".to_vec()).unwrap();
        let right = prolly.put(&base, b"a".to_vec(), b"right".to_vec()).unwrap();

        // Resolver that prefers right
        let resolver: error::Resolver =
            Box::new(|c| error::Resolution::value(c.right.clone().expect("right value")));
        let merged = prolly.merge(&base, &left, &right, Some(resolver)).unwrap();

        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"right".to_vec()));
    }

    #[test]
    fn test_merge_conflict_with_resolver_returns_none() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let left = prolly.put(&base, b"a".to_vec(), b"left".to_vec()).unwrap();
        let right = prolly.put(&base, b"a".to_vec(), b"right".to_vec()).unwrap();

        // Resolver that leaves the conflict unresolved
        let resolver: error::Resolver = Box::new(|_| error::Resolution::unresolved());
        let result = prolly.merge(&base, &left, &right, Some(resolver));

        assert!(matches!(result, Err(Error::Conflict(_))));
    }

    #[test]
    fn test_merge_left_deletes_right_modifies() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let left = prolly.delete(&base, b"a").unwrap();
        let right = prolly
            .put(&base, b"a".to_vec(), b"modified".to_vec())
            .unwrap();

        // Conflict - left deletes, right modifies
        let result = prolly.merge(&base, &left, &right, None);

        assert!(matches!(result, Err(Error::Conflict(_))));
    }

    #[test]
    fn test_merge_right_deletes_only() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let base = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
        let right = prolly.delete(&base, b"b").unwrap();

        // Left is same as base, right deletes b
        let merged = prolly.merge(&base, &base, &right, None).unwrap();

        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(prolly.get(&merged, b"b").unwrap(), None);
    }

    #[test]
    fn test_merge_both_delete_same_key() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let base = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
        let left = prolly.delete(&base, b"b").unwrap();
        let right = prolly.delete(&base, b"b").unwrap();

        // Both delete same key - no conflict
        let merged = prolly.merge(&base, &left, &right, None).unwrap();

        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(prolly.get(&merged, b"b").unwrap(), None);
    }

    #[test]
    fn test_merge_empty_base() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let left = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let right = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();

        let merged = prolly.merge(&base, &left, &right, None).unwrap();

        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(prolly.get(&merged, b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_merge_complex_scenario() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        // Base: a=1, b=2, c=3, d=4
        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let base = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
        let base = prolly.put(&base, b"c".to_vec(), b"3".to_vec()).unwrap();
        let base = prolly.put(&base, b"d".to_vec(), b"4".to_vec()).unwrap();

        // Left: a=1 (unchanged), b=left (modified), c deleted, e=5 (added)
        let left = prolly.put(&base, b"b".to_vec(), b"left".to_vec()).unwrap();
        let left = prolly.delete(&left, b"c").unwrap();
        let left = prolly.put(&left, b"e".to_vec(), b"5".to_vec()).unwrap();

        // Right: a=1 (unchanged), d=right (modified), f=6 (added)
        let right = prolly.put(&base, b"d".to_vec(), b"right".to_vec()).unwrap();
        let right = prolly.put(&right, b"f".to_vec(), b"6".to_vec()).unwrap();

        let merged = prolly.merge(&base, &left, &right, None).unwrap();

        // Check merged state
        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"1".to_vec())); // unchanged
        assert_eq!(prolly.get(&merged, b"b").unwrap(), Some(b"left".to_vec())); // left modified
        assert_eq!(prolly.get(&merged, b"c").unwrap(), None); // left deleted
        assert_eq!(prolly.get(&merged, b"d").unwrap(), Some(b"right".to_vec())); // right modified
        assert_eq!(prolly.get(&merged, b"e").unwrap(), Some(b"5".to_vec())); // left added
        assert_eq!(prolly.get(&merged, b"f").unwrap(), Some(b"6".to_vec())); // right added
    }
}
