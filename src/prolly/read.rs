//! Borrowed, callback-scoped read APIs.
//!
//! This module implements the core substrate specified by
//! `docs/superpowers/specs/2026-07-15-prolly-zero-copy-read-architecture-design.md`.
//! Values are borrowed from immutable retained nodes only for the duration of a
//! synchronous callback. Existing owned APIs remain compatibility wrappers.

use std::ops::ControlFlow;
use std::sync::{Arc, Weak};

use super::blob;
use super::cid::Cid;
use super::error::{Conflict, Diff, Error};
use super::key;
use super::node::Node;
#[cfg(feature = "async-store")]
use super::store::AsyncStore;
use super::store::Store;
use super::tree::Tree;
#[cfg(feature = "async-store")]
use super::AsyncProlly;
use super::{
    child_cid_at, route_key_positions_to_children, sorted_key_positions, InlinePositions, KeyValue,
    Prolly, GET_MANY_PREFETCH_PARALLELISM,
};

/// A callback-scoped borrowed key/value entry.
///
/// The fields are private so the backing representation can evolve without
/// changing callers. An `EntryRef` cannot outlive the callback that receives it.
#[derive(Clone, Copy, Debug)]
pub struct EntryRef<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> EntryRef<'a> {
    pub(crate) fn new(key: &'a [u8], value: &'a [u8]) -> Self {
        Self { key, value }
    }

    /// Borrow the entry key.
    #[inline]
    pub fn key(&self) -> &'a [u8] {
        self.key
    }

    /// Borrow the entry value.
    #[inline]
    pub fn value(&self) -> &'a [u8] {
        self.value
    }

    /// Explicitly copy this borrowed entry into the legacy owned representation.
    pub fn to_owned(self) -> KeyValue {
        (self.key.to_vec(), self.value.to_vec())
    }
}

/// The result of a callback traversal that supports early termination.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanOutcome<B> {
    /// Number of entries delivered to the callback, including the entry that
    /// returned `ControlFlow::Break`.
    pub visited: u64,
    /// The callback's break value, or `None` when traversal reached its bound.
    pub break_value: Option<B>,
}

impl<B> ScanOutcome<B> {
    pub(crate) fn complete(visited: u64) -> Self {
        Self {
            visited,
            break_value: None,
        }
    }

    pub(crate) fn stopped(visited: u64, break_value: B) -> Self {
        Self {
            visited,
            break_value: Some(break_value),
        }
    }
}

/// Borrowed view of a stored large-value envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueRefView<'a> {
    /// Inline application bytes.
    Inline(&'a [u8]),
    /// Validated content-addressed blob reference.
    Blob { cid: Cid, len: u64 },
}

impl ValueRefView<'_> {
    /// Explicitly copy this view into the legacy owned value-reference type.
    pub fn to_owned(self) -> blob::ValueRef {
        match self {
            Self::Inline(value) => blob::ValueRef::Inline(value.to_vec()),
            Self::Blob { cid, len } => blob::ValueRef::Blob(blob::BlobRef { cid, len }),
        }
    }
}

/// A callback-scoped borrowed difference between two trees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiffRef<'a> {
    Added {
        key: &'a [u8],
        value: &'a [u8],
    },
    Removed {
        key: &'a [u8],
        value: &'a [u8],
    },
    Changed {
        key: &'a [u8],
        old: &'a [u8],
        new: &'a [u8],
    },
}

impl DiffRef<'_> {
    /// Borrow the affected key.
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Added { key, .. } | Self::Removed { key, .. } | Self::Changed { key, .. } => key,
        }
    }

    /// Explicitly copy this event into the legacy owned diff representation.
    pub fn to_owned(self) -> Diff {
        match self {
            Self::Added { key, value } => Diff::Added {
                key: key.to_vec(),
                val: value.to_vec(),
            },
            Self::Removed { key, value } => Diff::Removed {
                key: key.to_vec(),
                val: value.to_vec(),
            },
            Self::Changed { key, old, new } => Diff::Changed {
                key: key.to_vec(),
                old: old.to_vec(),
                new: new.to_vec(),
            },
        }
    }
}

/// Callback-scoped values for one genuine three-way merge conflict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConflictRef<'a> {
    pub key: &'a [u8],
    pub base: Option<&'a [u8]>,
    pub left: Option<&'a [u8]>,
    pub right: Option<&'a [u8]>,
}

impl ConflictRef<'_> {
    /// Explicitly copy this view into the legacy owned conflict type.
    pub fn to_owned(self) -> Conflict {
        Conflict {
            key: self.key.to_vec(),
            base: self.base.map(<[u8]>::to_vec),
            left: self.left.map(<[u8]>::to_vec),
            right: self.right.map(<[u8]>::to_vec),
        }
    }
}

/// A borrowed merge resolver's selection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeDecision {
    UseBase,
    UseLeft,
    UseRight,
    Value(Vec<u8>),
    Delete,
    Unresolved,
}

/// Resolver that inspects borrowed conflict values and can select an existing
/// branch without first copying it.
pub trait BorrowedMergeResolver: Send + Sync {
    fn resolve(&self, conflict: ConflictRef<'_>) -> MergeDecision;
}

struct PathFrame {
    node: Arc<Node>,
    index: usize,
}

// A fixed-size, direct-mapped table keeps hot route nodes out of the shared
// cache lock without becoming a second unbounded cache. Weak references mean
// a session never prevents normal cache eviction.
const SESSION_NODE_SLOTS: usize = 1024;
const SESSION_NODE_WAYS: usize = 4;
const SESSION_NODE_SETS: usize = SESSION_NODE_SLOTS / SESSION_NODE_WAYS;
type SessionNodeSlot = Option<(Cid, Weak<Node>)>;

struct SessionNodeTable {
    slots: Box<[SessionNodeSlot]>,
    next_way: Box<[u8]>,
}

impl SessionNodeTable {
    fn new() -> Self {
        Self {
            slots: std::iter::repeat_with(|| None)
                .take(SESSION_NODE_SLOTS)
                .collect(),
            next_way: vec![0; SESSION_NODE_SETS].into_boxed_slice(),
        }
    }

    #[inline]
    fn set(cid: &Cid) -> usize {
        let prefix = u64::from_ne_bytes(
            cid.as_bytes()[..8]
                .try_into()
                .expect("CID has a fixed 32-byte representation"),
        );
        prefix as usize & (SESSION_NODE_SETS - 1)
    }

    #[inline]
    fn get(&self, cid: &Cid) -> Option<Arc<Node>> {
        let start = Self::set(cid) * SESSION_NODE_WAYS;
        self.slots[start..start + SESSION_NODE_WAYS]
            .iter()
            .flatten()
            .find(|(cached, _)| cached == cid)
            .and_then(|(_, node)| node.upgrade())
    }

    #[inline]
    fn insert(&mut self, cid: Cid, node: &Arc<Node>) {
        let set = Self::set(&cid);
        let start = set * SESSION_NODE_WAYS;
        let ways = &mut self.slots[start..start + SESSION_NODE_WAYS];
        let slot = ways
            .iter()
            .position(|entry| entry.as_ref().is_some_and(|(cached, _)| cached == &cid))
            .or_else(|| ways.iter().position(Option::is_none))
            .unwrap_or_else(|| {
                let way = usize::from(self.next_way[set]);
                self.next_way[set] = ((way + 1) % SESSION_NODE_WAYS) as u8;
                way
            });
        ways[slot] = Some((cid, Arc::downgrade(node)));
    }
}

/// A reusable root-bound read context for one immutable tree.
///
/// The session retains the decoded root and a session-local recent leaf. It is
/// intended to be reused by one worker; methods that update traversal state
/// require `&mut self`.
pub struct ReadSession<'manager, 'tree, S: Store> {
    manager: &'manager Prolly<S>,
    tree: &'tree Tree,
    root: Option<Arc<Node>>,
    recent_leaf: Option<Arc<Node>>,
    local_nodes: SessionNodeTable,
}

/// Reusable root-bound read context for an asynchronous store.
///
/// All visitors are synchronous. A node is fully loaded before a visitor sees
/// a borrowed slice, and the visitor returns before traversal can await again.
#[cfg(feature = "async-store")]
pub struct AsyncReadSession<'manager, 'tree, S: AsyncStore> {
    manager: &'manager AsyncProlly<S>,
    tree: &'tree Tree,
    root: Option<Arc<Node>>,
    recent_leaf: Option<Arc<Node>>,
    local_nodes: SessionNodeTable,
}

impl<S: Store> Prolly<S> {
    /// Open a reusable borrowed read session over an immutable tree.
    pub fn read<'manager, 'tree>(
        &'manager self,
        tree: &'tree Tree,
    ) -> Result<ReadSession<'manager, 'tree, S>, Error> {
        if tree.config.format != self.config.format {
            return Err(Error::FormatMismatch {
                expected: self.config.format.digest()?,
                actual: tree.config.format.digest()?,
            });
        }

        let root = tree
            .root
            .as_ref()
            .map(|cid| self.load_arc(cid))
            .transpose()?;
        if let Some(root) = root.as_ref() {
            if root.format != tree.config.format {
                return Err(Error::FormatMismatch {
                    expected: tree.config.format.digest()?,
                    actual: root.format.digest()?,
                });
            }
        }

        let recent_leaf_enabled = self
            .node_cache
            .read()
            .is_ok_and(|cache| !cache.is_disabled());
        let recent_leaf = recent_leaf_enabled
            .then_some(tree.root.as_ref())
            .flatten()
            .and_then(|root_cid| {
                self.recent_leaf.read().ok().and_then(|recent| {
                    recent
                        .as_ref()
                        .filter(|entry| &entry.root == root_cid)
                        .map(|entry| entry.node.clone())
                })
            });

        Ok(ReadSession {
            manager: self,
            tree,
            root,
            recent_leaf,
            local_nodes: SessionNodeTable::new(),
        })
    }

    /// Read a value without allocating an owned result unless the callback does.
    pub fn get_with<R>(
        &self,
        tree: &Tree,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error> {
        if tree.config.format != self.config.format {
            return Err(Error::FormatMismatch {
                expected: self.config.format.digest()?,
                actual: tree.config.format.digest()?,
            });
        }
        let Some(root) = tree.root.as_ref() else {
            return Ok(None);
        };
        let recent_leaf_enabled = self
            .node_cache
            .read()
            .is_ok_and(|cache| !cache.is_disabled());
        let recent_leaf = recent_leaf_enabled
            .then(|| self.recent_leaf.read().ok())
            .flatten()
            .and_then(|recent| {
                recent
                    .as_ref()
                    .filter(|entry| &entry.root == root)
                    .map(|entry| entry.node.clone())
            });
        if let Some(leaf) = recent_leaf {
            ReadSession::<S>::validate_leaf(&leaf)?;
            if leaf
                .keys
                .first()
                .zip(leaf.keys.last())
                .is_some_and(|(first, last)| key >= first.as_slice() && key <= last.as_slice())
            {
                self.metrics.add_cache_hits(1);
                return match leaf.search(key) {
                    Ok(index) => Ok(Some(read(leaf.vals.get(index).ok_or(Error::InvalidNode)?))),
                    Err(_) => Ok(None),
                };
            }
        }
        let mut session = self.read(tree)?;
        let result = session.get_with(key, read);
        if result.is_ok() {
            if let (Some(root), Some(leaf)) = (tree.root.as_ref(), session.recent_leaf) {
                self.maybe_cache_recent_leaf(root, leaf);
            }
        }
        result
    }

    /// Check key membership without copying its value.
    pub fn contains_key(&self, tree: &Tree, key: &[u8]) -> Result<bool, Error> {
        Ok(self.get_with(tree, key, |_| ())?.is_some())
    }

    /// Inspect a stored large-value reference without copying inline bytes.
    pub fn get_value_ref_with<R>(
        &self,
        tree: &Tree,
        key: &[u8],
        read: impl for<'value> FnOnce(ValueRefView<'value>) -> R,
    ) -> Result<Option<R>, Error> {
        self.get_with(tree, key, |bytes| {
            blob::value_ref_view_from_stored_bytes(bytes).map(read)
        })?
        .transpose()
    }

    /// Visit point-read results in caller order without owning hit values.
    pub fn get_many_with<K, F>(&self, tree: &Tree, keys: &[K], visit: F) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        F: for<'value> FnMut(usize, &[u8], Option<&'value [u8]>),
    {
        self.read(tree)?.get_many_with(keys, visit)
    }

    /// Visit the entry at a zero-based ordinal without copying it.
    pub fn select_with<R>(
        &self,
        tree: &Tree,
        ordinal: u64,
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error> {
        self.read(tree)?.select_with(ordinal, read)
    }

    /// Visit the first entry without copying it.
    pub fn first_entry_with<R>(
        &self,
        tree: &Tree,
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error> {
        self.read(tree)?.first_entry_with(read)
    }

    /// Visit the last entry without copying it.
    pub fn last_entry_with<R>(
        &self,
        tree: &Tree,
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error> {
        self.read(tree)?.last_entry_with(read)
    }

    /// Visit the first entry with key greater than or equal to `key`.
    pub fn lower_bound_with<R>(
        &self,
        tree: &Tree,
        key: &[u8],
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error> {
        self.read(tree)?.lower_bound_with(key, read)
    }

    /// Visit the first entry with key strictly greater than `key`.
    pub fn upper_bound_with<R>(
        &self,
        tree: &Tree,
        key: &[u8],
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error> {
        self.read(tree)?.upper_bound_with(key, read)
    }

    /// Visit a half-open range in ascending key order.
    pub fn scan_range(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        self.read(tree)?.scan_range(start, end, visit)
    }

    /// Visit a half-open range in ascending order with early termination.
    pub fn scan_range_until<B>(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.read(tree)?.scan_range_until(start, end, visit)
    }

    /// Visit every entry under `prefix` in ascending key order.
    pub fn scan_prefix(
        &self,
        tree: &Tree,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        self.read(tree)?.scan_prefix(prefix, visit)
    }

    /// Visit every entry under `prefix` with early termination.
    pub fn scan_prefix_until<B>(
        &self,
        tree: &Tree,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.read(tree)?.scan_prefix_until(prefix, visit)
    }

    /// Visit a half-open range in descending key order.
    pub fn scan_range_reverse(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        self.read(tree)?.scan_range_reverse(start, end, visit)
    }

    /// Visit a half-open descending range with early termination.
    pub fn scan_range_reverse_until<B>(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.read(tree)?.scan_range_reverse_until(start, end, visit)
    }

    /// Visit a prefix in descending key order.
    pub fn scan_prefix_reverse(
        &self,
        tree: &Tree,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        self.read(tree)?.scan_prefix_reverse(prefix, visit)
    }

    /// Visit a descending prefix with early termination.
    pub fn scan_prefix_reverse_until<B>(
        &self,
        tree: &Tree,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.read(tree)?.scan_prefix_reverse_until(prefix, visit)
    }
}

impl<'manager, 'tree, S: Store> ReadSession<'manager, 'tree, S> {
    /// The immutable tree bound to this session.
    pub fn tree(&self) -> &Tree {
        self.tree
    }

    /// Return the number of logical entries using the retained root.
    pub fn len(&self) -> Result<u64, Error> {
        let Some(root) = self.root.as_ref() else {
            return Ok(0);
        };
        if root.leaf {
            return Ok(root.len() as u64);
        }
        if root.child_counts.len() == root.len() && !root.child_counts.contains(&0) {
            return Ok(root.child_counts.iter().copied().sum());
        }
        self.manager.subtree_count(
            self.tree
                .root
                .as_ref()
                .expect("a retained root has a tree root CID"),
        )
    }

    /// Return whether the bound tree contains no entries.
    pub fn is_empty(&self) -> Result<bool, Error> {
        Ok(self.len()? == 0)
    }

    /// Return the number of keys strictly less than `key`.
    pub fn rank(&mut self, key: &[u8]) -> Result<u64, Error> {
        let Some(mut node) = self.root.clone() else {
            return Ok(0);
        };
        let mut rank = 0u64;
        loop {
            if node.leaf {
                Self::validate_leaf(&node)?;
                return Ok(rank.saturating_add(
                    node.keys
                        .partition_point(|candidate| candidate.as_slice() < key)
                        as u64,
                ));
            }
            Self::validate_internal(&node)?;
            let insertion = node
                .keys
                .partition_point(|candidate| candidate.as_slice() <= key);
            if insertion == 0 {
                return Ok(rank);
            }
            let child_index = insertion - 1;
            for index in 0..child_index {
                rank = rank.saturating_add(self.child_count(&node, index)?);
            }
            node = self.manager.load_arc(&child_cid_at(&node, child_index)?)?;
        }
    }

    /// Read a value without copying it.
    pub fn get_with<R>(
        &mut self,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error> {
        if let Some(leaf) = self.recent_leaf.as_ref() {
            Self::validate_leaf(leaf)?;
            if leaf
                .keys
                .first()
                .zip(leaf.keys.last())
                .is_some_and(|(first, last)| key >= first.as_slice() && key <= last.as_slice())
            {
                return match leaf.search(key) {
                    Ok(index) => Ok(Some(read(leaf.vals.get(index).ok_or(Error::InvalidNode)?))),
                    Err(_) => Ok(None),
                };
            }
        }

        let Some(mut node) = self.root.clone() else {
            return Ok(None);
        };
        loop {
            let index = Self::route_index(&node, key)?;
            if node.leaf {
                self.recent_leaf = Some(node.clone());
                return if node.keys.get(index).map(Vec::as_slice) == Some(key) {
                    Ok(Some(read(node.vals.get(index).ok_or(Error::InvalidNode)?)))
                } else {
                    Ok(None)
                };
            }
            let cid = child_cid_at(&node, index)?;
            node = match self.local_nodes.get(&cid) {
                Some(node) => node,
                None => {
                    let node = self.manager.load_arc(&cid)?;
                    self.local_nodes.insert(cid, &node);
                    node
                }
            };
        }
    }

    /// Inspect a stored large-value envelope without copying inline bytes.
    pub fn get_value_ref_with<R>(
        &mut self,
        key: &[u8],
        read: impl for<'value> FnOnce(ValueRefView<'value>) -> R,
    ) -> Result<Option<R>, Error> {
        self.get_with(key, |bytes| {
            blob::value_ref_view_from_stored_bytes(bytes).map(read)
        })?
        .transpose()
    }

    /// Check membership without copying the value.
    pub fn contains_key(&mut self, key: &[u8]) -> Result<bool, Error> {
        Ok(self.get_with(key, |_| ())?.is_some())
    }

    /// Visit point-read results exactly once per input position and in order.
    pub fn get_many_with<K, F>(&mut self, keys: &[K], mut visit: F) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        F: for<'value> FnMut(usize, &[u8], Option<&'value [u8]>),
    {
        if keys.is_empty() {
            return Ok(());
        }
        let Some(root) = self.root.clone() else {
            for (position, key) in keys.iter().enumerate() {
                visit(position, key.as_ref(), None);
            }
            return Ok(());
        };

        let positions = InlinePositions::from_vec(sorted_key_positions(keys))
            .expect("keys are non-empty after early return");
        let mut frames = vec![(root, positions)];
        let mut locations: Vec<Option<(Arc<Node>, usize)>> = vec![None; keys.len()];

        while !frames.is_empty() {
            let mut children = Vec::new();
            for (node, positions) in frames {
                if node.leaf {
                    Self::fill_leaf_locations(node, positions, keys, &mut locations)?;
                } else {
                    children.extend(route_key_positions_to_children(&node, positions, keys)?);
                }
            }
            if children.is_empty() {
                break;
            }
            let cids = children
                .iter()
                .map(|frame| frame.cid.clone())
                .collect::<Vec<_>>();
            let nodes = if self.manager.store.prefers_batch_reads() {
                self.manager
                    .load_many_ordered_with_parallelism(&cids, GET_MANY_PREFETCH_PARALLELISM)?
            } else {
                self.manager.load_many_ordered(&cids)?
            };
            frames = children
                .into_iter()
                .zip(nodes)
                .map(|(frame, node)| (node, frame.positions))
                .collect();
        }

        for (position, key) in keys.iter().enumerate() {
            let value = match locations[position].as_ref() {
                Some((node, index)) => {
                    Some(node.vals.get(*index).ok_or(Error::InvalidNode)?.as_slice())
                }
                None => None,
            };
            visit(position, key.as_ref(), value);
        }
        Ok(())
    }

    /// Visit the zero-based entry at `ordinal` without copying it.
    pub fn select_with<R>(
        &mut self,
        mut ordinal: u64,
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error> {
        let Some(mut node) = self.root.clone() else {
            return Ok(None);
        };
        loop {
            if node.leaf {
                Self::validate_leaf(&node)?;
                let index = usize::try_from(ordinal).map_err(|_| Error::InvalidNode)?;
                let Some(key) = node.keys.get(index) else {
                    return Ok(None);
                };
                let value = node.vals.get(index).ok_or(Error::InvalidNode)?;
                return Ok(Some(read(EntryRef::new(key, value))));
            }
            Self::validate_internal(&node)?;
            let mut selected = None;
            for index in 0..node.len() {
                let count = self.child_count(&node, index)?;
                if ordinal < count {
                    selected = Some(index);
                    break;
                }
                ordinal -= count;
            }
            let Some(index) = selected else {
                return Ok(None);
            };
            node = self.manager.load_arc(&child_cid_at(&node, index)?)?;
        }
    }

    /// Visit the first entry in key order.
    pub fn first_entry_with<R>(
        &mut self,
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error> {
        self.lower_bound_with(&[], read)
    }

    /// Visit the last entry in key order.
    pub fn last_entry_with<R>(
        &mut self,
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error> {
        let mut read = Some(read);
        let outcome = self.scan_range_reverse_until(&[], None, |entry| {
            ControlFlow::Break(read.take().expect("single-entry callback")(entry))
        })?;
        Ok(outcome.break_value)
    }

    /// Visit the first entry whose key is at least `key`.
    pub fn lower_bound_with<R>(
        &mut self,
        key: &[u8],
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error> {
        let mut read = Some(read);
        let outcome = self.scan_forward_until(key, None, false, |entry| {
            ControlFlow::Break(read.take().expect("single-entry callback")(entry))
        })?;
        Ok(outcome.break_value)
    }

    /// Visit the first entry whose key is strictly greater than `key`.
    pub fn upper_bound_with<R>(
        &mut self,
        key: &[u8],
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error> {
        let mut read = Some(read);
        let outcome = self.scan_forward_until(key, None, true, |entry| {
            ControlFlow::Break(read.take().expect("single-entry callback")(entry))
        })?;
        Ok(outcome.break_value)
    }

    /// Visit a half-open range in ascending order.
    pub fn scan_range(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_range_until(start, end, |entry| {
                visit(entry);
                ControlFlow::<()>::Continue(())
            })?
            .visited)
    }

    /// Visit a half-open range in ascending order with early termination.
    pub fn scan_range_until<B>(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_forward_until(start, end, false, visit)
    }

    /// Visit all entries under `prefix` in ascending order.
    pub fn scan_prefix(
        &mut self,
        prefix: &[u8],
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_prefix_until(prefix, |entry| {
                visit(entry);
                ControlFlow::<()>::Continue(())
            })?
            .visited)
    }

    /// Visit all entries under `prefix` with early termination.
    pub fn scan_prefix_until<B>(
        &mut self,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        let (start, end) = key::prefix_range(prefix);
        self.scan_range_until(&start, end.as_deref(), visit)
    }

    /// Visit a half-open range in descending order.
    pub fn scan_range_reverse(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_range_reverse_until(start, end, |entry| {
                visit(entry);
                ControlFlow::<()>::Continue(())
            })?
            .visited)
    }

    /// Visit a half-open range in descending order with early termination.
    pub fn scan_range_reverse_until<B>(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        if end.is_some_and(|end| end <= start) {
            return Ok(ScanOutcome::complete(0));
        }
        let mut stack = self.seek_reverse(end)?;
        let mut visited = 0u64;

        loop {
            let Some(frame) = stack.last_mut() else {
                return Ok(ScanOutcome::complete(visited));
            };
            if !frame.node.leaf {
                return Err(Error::InvalidNode);
            }
            Self::validate_leaf(&frame.node)?;
            let key = frame.node.keys.get(frame.index).ok_or(Error::InvalidNode)?;
            if key.as_slice() < start {
                return Ok(ScanOutcome::complete(visited));
            }
            let value = frame.node.vals.get(frame.index).ok_or(Error::InvalidNode)?;
            visited = visited.saturating_add(1);
            if let ControlFlow::Break(value) = visit(EntryRef::new(key, value)) {
                return Ok(ScanOutcome::stopped(visited, value));
            }
            if frame.index > 0 {
                frame.index -= 1;
            } else if !Self::advance_reverse(self.manager, &mut stack)? {
                return Ok(ScanOutcome::complete(visited));
            }
        }
    }

    /// Visit a prefix in descending order.
    pub fn scan_prefix_reverse(
        &mut self,
        prefix: &[u8],
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_prefix_reverse_until(prefix, |entry| {
                visit(entry);
                ControlFlow::<()>::Continue(())
            })?
            .visited)
    }

    /// Visit a prefix in descending order with early termination.
    pub fn scan_prefix_reverse_until<B>(
        &mut self,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        let (start, end) = key::prefix_range(prefix);
        self.scan_range_reverse_until(&start, end.as_deref(), visit)
    }

    fn scan_forward_until<B>(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        strict_start: bool,
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        if end.is_some_and(|end| end <= start) {
            return Ok(ScanOutcome::complete(0));
        }
        let mut stack = self.seek_forward(start, strict_start)?;
        let mut visited = 0u64;

        loop {
            let Some(frame) = stack.last_mut() else {
                return Ok(ScanOutcome::complete(visited));
            };
            if !frame.node.leaf {
                return Err(Error::InvalidNode);
            }
            Self::validate_leaf(&frame.node)?;
            if frame.index >= frame.node.len() {
                if !Self::advance_forward(self.manager, &mut stack)? {
                    return Ok(ScanOutcome::complete(visited));
                }
                continue;
            }
            let key = frame.node.keys.get(frame.index).ok_or(Error::InvalidNode)?;
            if end.is_some_and(|end| key.as_slice() >= end) {
                return Ok(ScanOutcome::complete(visited));
            }
            let value = frame.node.vals.get(frame.index).ok_or(Error::InvalidNode)?;
            frame.index += 1;
            visited = visited.saturating_add(1);
            if let ControlFlow::Break(value) = visit(EntryRef::new(key, value)) {
                return Ok(ScanOutcome::stopped(visited, value));
            }
        }
    }

    fn seek_forward(&self, key: &[u8], strict: bool) -> Result<Vec<PathFrame>, Error> {
        let Some(mut node) = self.root.clone() else {
            return Ok(Vec::new());
        };
        let mut stack = Vec::new();
        loop {
            if node.leaf {
                Self::validate_leaf(&node)?;
                let index = if strict {
                    node.keys
                        .partition_point(|candidate| candidate.as_slice() <= key)
                } else {
                    node.keys
                        .partition_point(|candidate| candidate.as_slice() < key)
                };
                stack.push(PathFrame { node, index });
                return Ok(stack);
            }
            Self::validate_internal(&node)?;
            let index = node
                .keys
                .partition_point(|candidate| candidate.as_slice() <= key)
                .saturating_sub(1);
            let cid = child_cid_at(&node, index)?;
            stack.push(PathFrame { node, index });
            node = self.manager.load_arc(&cid)?;
        }
    }

    fn seek_reverse(&self, end: Option<&[u8]>) -> Result<Vec<PathFrame>, Error> {
        let Some(mut node) = self.root.clone() else {
            return Ok(Vec::new());
        };
        let mut stack = Vec::new();
        loop {
            if node.leaf {
                Self::validate_leaf(&node)?;
                let exclusive = match end {
                    Some(end) => node
                        .keys
                        .partition_point(|candidate| candidate.as_slice() < end),
                    None => node.len(),
                };
                if exclusive == 0 {
                    return Ok(Vec::new());
                }
                stack.push(PathFrame {
                    node,
                    index: exclusive - 1,
                });
                return Ok(stack);
            }
            Self::validate_internal(&node)?;
            let exclusive = match end {
                Some(end) => node
                    .keys
                    .partition_point(|candidate| candidate.as_slice() < end),
                None => node.len(),
            };
            if exclusive == 0 {
                return Ok(Vec::new());
            }
            let index = exclusive - 1;
            let cid = child_cid_at(&node, index)?;
            stack.push(PathFrame { node, index });
            node = self.manager.load_arc(&cid)?;
        }
    }

    fn advance_forward(manager: &Prolly<S>, stack: &mut Vec<PathFrame>) -> Result<bool, Error> {
        stack.pop();
        while let Some(parent) = stack.last_mut() {
            Self::validate_internal(&parent.node)?;
            parent.index += 1;
            if parent.index >= parent.node.len() {
                stack.pop();
                continue;
            }
            let cid = child_cid_at(&parent.node, parent.index)?;
            let mut node = manager.load_arc(&cid)?;
            loop {
                if node.leaf {
                    Self::validate_leaf(&node)?;
                    stack.push(PathFrame { node, index: 0 });
                    return Ok(true);
                }
                Self::validate_internal(&node)?;
                let cid = child_cid_at(&node, 0)?;
                stack.push(PathFrame { node, index: 0 });
                node = manager.load_arc(&cid)?;
            }
        }
        Ok(false)
    }

    fn advance_reverse(manager: &Prolly<S>, stack: &mut Vec<PathFrame>) -> Result<bool, Error> {
        stack.pop();
        while let Some(parent) = stack.last_mut() {
            Self::validate_internal(&parent.node)?;
            if parent.index == 0 {
                stack.pop();
                continue;
            }
            parent.index -= 1;
            let cid = child_cid_at(&parent.node, parent.index)?;
            let mut node = manager.load_arc(&cid)?;
            loop {
                if node.leaf {
                    Self::validate_leaf(&node)?;
                    let index = node.len().checked_sub(1).ok_or(Error::InvalidNode)?;
                    stack.push(PathFrame { node, index });
                    return Ok(true);
                }
                Self::validate_internal(&node)?;
                let index = node.len().checked_sub(1).ok_or(Error::InvalidNode)?;
                let cid = child_cid_at(&node, index)?;
                stack.push(PathFrame { node, index });
                node = manager.load_arc(&cid)?;
            }
        }
        Ok(false)
    }

    fn route_index(node: &Node, key: &[u8]) -> Result<usize, Error> {
        if node.leaf {
            Self::validate_leaf(node)?;
        } else {
            Self::validate_internal(node)?;
        }
        Ok(match node.search(key) {
            Ok(index) => index,
            Err(index) => index.saturating_sub(1),
        })
    }

    fn validate_leaf(node: &Node) -> Result<(), Error> {
        if !node.leaf || node.keys.len() != node.vals.len() {
            return Err(Error::InvalidNode);
        }
        Ok(())
    }

    fn validate_internal(node: &Node) -> Result<(), Error> {
        if node.leaf || node.keys.is_empty() || node.keys.len() != node.vals.len() {
            return Err(Error::InvalidNode);
        }
        Ok(())
    }

    fn child_count(&self, node: &Node, index: usize) -> Result<u64, Error> {
        match node.child_counts.get(index).copied() {
            Some(count) if count > 0 => Ok(count),
            _ => self.manager.subtree_count(&child_cid_at(node, index)?),
        }
    }

    fn fill_leaf_locations<K: AsRef<[u8]>>(
        node: Arc<Node>,
        positions: InlinePositions,
        keys: &[K],
        locations: &mut [Option<(Arc<Node>, usize)>],
    ) -> Result<(), Error> {
        Self::validate_leaf(&node)?;
        let mut leaf_index = 0usize;
        let mut positions = positions.into_iter().peekable();
        while let Some(position) = positions.next() {
            let key = keys[position].as_ref();
            while leaf_index < node.len() && node.keys[leaf_index].as_slice() < key {
                leaf_index += 1;
            }
            let found = (leaf_index < node.len() && node.keys[leaf_index].as_slice() == key)
                .then_some(leaf_index);
            if let Some(index) = found {
                locations[position] = Some((node.clone(), index));
            }
            while let Some(duplicate) =
                positions.next_if(|candidate| keys[*candidate].as_ref() == key)
            {
                if let Some(index) = found {
                    locations[duplicate] = Some((node.clone(), index));
                }
            }
        }
        Ok(())
    }
}

#[cfg(feature = "async-store")]
impl<S> AsyncProlly<S>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    /// Open a reusable borrowed async read session over an immutable tree.
    pub async fn read<'manager, 'tree>(
        &'manager self,
        tree: &'tree Tree,
    ) -> Result<AsyncReadSession<'manager, 'tree, S>, Error> {
        if tree.config.format != self.config().format {
            return Err(Error::FormatMismatch {
                expected: self.config().format.digest()?,
                actual: tree.config.format.digest()?,
            });
        }
        let root = tree.root.as_ref().map(|cid| self.load_arc(cid));
        let root = match root {
            Some(load) => Some(load.await?),
            None => None,
        };
        if let Some(root) = root.as_ref() {
            if root.format != tree.config.format {
                return Err(Error::FormatMismatch {
                    expected: tree.config.format.digest()?,
                    actual: root.format.digest()?,
                });
            }
        }
        Ok(AsyncReadSession {
            manager: self,
            tree,
            root,
            recent_leaf: None,
            local_nodes: SessionNodeTable::new(),
        })
    }

    /// Read one async-store value without allocating an owned result.
    pub async fn get_with<R>(
        &self,
        tree: &Tree,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error> {
        self.read(tree).await?.get_with(key, read).await
    }

    /// Inspect a stored large-value envelope without copying inline bytes.
    pub async fn get_value_ref_with<R>(
        &self,
        tree: &Tree,
        key: &[u8],
        read: impl for<'value> FnOnce(ValueRefView<'value>) -> R,
    ) -> Result<Option<R>, Error> {
        self.read(tree).await?.get_value_ref_with(key, read).await
    }

    /// Visit async point-read results exactly once per input position.
    pub async fn get_many_with<K, F>(&self, tree: &Tree, keys: &[K], visit: F) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        F: for<'value> FnMut(usize, &[u8], Option<&'value [u8]>),
    {
        self.read(tree).await?.get_many_with(keys, visit).await
    }

    /// Visit an async half-open range without allocating entries.
    pub async fn scan_range(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        self.read(tree).await?.scan_range(start, end, visit).await
    }

    pub async fn scan_range_until<B>(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.read(tree)
            .await?
            .scan_range_until(start, end, visit)
            .await
    }

    /// Visit an async prefix without allocating entries.
    pub async fn scan_prefix(
        &self,
        tree: &Tree,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        self.read(tree).await?.scan_prefix(prefix, visit).await
    }

    pub async fn scan_prefix_until<B>(
        &self,
        tree: &Tree,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.read(tree)
            .await?
            .scan_prefix_until(prefix, visit)
            .await
    }

    pub async fn scan_range_reverse(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        self.read(tree)
            .await?
            .scan_range_reverse(start, end, visit)
            .await
    }

    pub async fn scan_range_reverse_until<B>(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.read(tree)
            .await?
            .scan_range_reverse_until(start, end, visit)
            .await
    }

    pub async fn scan_prefix_reverse(
        &self,
        tree: &Tree,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        self.read(tree)
            .await?
            .scan_prefix_reverse(prefix, visit)
            .await
    }

    pub async fn scan_prefix_reverse_until<B>(
        &self,
        tree: &Tree,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.read(tree)
            .await?
            .scan_prefix_reverse_until(prefix, visit)
            .await
    }
}

#[cfg(feature = "async-store")]
impl<'manager, 'tree, S> AsyncReadSession<'manager, 'tree, S>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    /// Return the immutable tree bound to this session.
    pub fn tree(&self) -> &Tree {
        self.tree
    }

    /// Read a value after all required I/O and before any subsequent await.
    pub async fn get_with<R>(
        &mut self,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error> {
        if let Some(leaf) = self.recent_leaf.as_ref() {
            ReadSession::<super::store::MemStore>::validate_leaf(leaf)?;
            if leaf
                .keys
                .first()
                .zip(leaf.keys.last())
                .is_some_and(|(first, last)| key >= first.as_slice() && key <= last.as_slice())
            {
                return match leaf.search(key) {
                    Ok(index) => Ok(Some(read(leaf.vals.get(index).ok_or(Error::InvalidNode)?))),
                    Err(_) => Ok(None),
                };
            }
        }

        let Some(mut node) = self.root.clone() else {
            return Ok(None);
        };
        loop {
            let index = async_route_index(&node, key)?;
            if node.leaf {
                self.recent_leaf = Some(node.clone());
                return if node.keys.get(index).map(Vec::as_slice) == Some(key) {
                    Ok(Some(read(node.vals.get(index).ok_or(Error::InvalidNode)?)))
                } else {
                    Ok(None)
                };
            }
            let cid = child_cid_at(&node, index)?;
            node = match self.local_nodes.get(&cid) {
                Some(node) => node,
                None => {
                    let node = self.manager.load_arc(&cid).await?;
                    self.local_nodes.insert(cid, &node);
                    node
                }
            };
        }
    }

    /// Inspect an async-store large-value envelope without copying inline data.
    pub async fn get_value_ref_with<R>(
        &mut self,
        key: &[u8],
        read: impl for<'value> FnOnce(ValueRefView<'value>) -> R,
    ) -> Result<Option<R>, Error> {
        self.get_with(key, |bytes| {
            blob::value_ref_view_from_stored_bytes(bytes).map(read)
        })
        .await?
        .transpose()
    }

    /// Visit async multi-get results in input order without owning hit values.
    pub async fn get_many_with<K, F>(&mut self, keys: &[K], mut visit: F) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        F: for<'value> FnMut(usize, &[u8], Option<&'value [u8]>),
    {
        if keys.is_empty() {
            return Ok(());
        }
        let Some(root) = self.root.clone() else {
            for (position, key) in keys.iter().enumerate() {
                visit(position, key.as_ref(), None);
            }
            return Ok(());
        };

        let positions = InlinePositions::from_vec(sorted_key_positions(keys))
            .expect("keys are non-empty after early return");
        let mut frames = vec![(root, positions)];
        let mut locations: Vec<Option<(Arc<Node>, usize)>> = vec![None; keys.len()];

        while !frames.is_empty() {
            let mut children = Vec::new();
            for (node, positions) in frames {
                if node.leaf {
                    fill_async_leaf_locations(node, positions, keys, &mut locations)?;
                } else {
                    children.extend(route_key_positions_to_children(&node, positions, keys)?);
                }
            }
            if children.is_empty() {
                break;
            }
            let cids = children
                .iter()
                .map(|frame| frame.cid.clone())
                .collect::<Vec<_>>();
            let nodes = self.manager.load_child_frontier_ordered(&cids).await?;
            frames = children
                .into_iter()
                .zip(nodes)
                .map(|(frame, node)| (node, frame.positions))
                .collect();
        }

        for (position, key) in keys.iter().enumerate() {
            let value = locations[position]
                .as_ref()
                .map(|(node, index)| node.vals.get(*index).ok_or(Error::InvalidNode))
                .transpose()?
                .map(Vec::as_slice);
            visit(position, key.as_ref(), value);
        }
        Ok(())
    }

    /// Visit a half-open range, never holding a borrowed entry across await.
    pub async fn scan_range(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_range_until(start, end, |entry| {
                visit(entry);
                ControlFlow::<()>::Continue(())
            })
            .await?
            .visited)
    }

    pub async fn scan_range_until<B>(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        let mut range = self.manager.range(self.tree, start, end).await?;
        let mut visited = 0u64;
        while let Some(result) = range.next_with(&mut visit).await {
            visited = visited.saturating_add(1);
            if let ControlFlow::Break(value) = result? {
                return Ok(ScanOutcome::stopped(visited, value));
            }
        }
        Ok(ScanOutcome::complete(visited))
    }

    /// Visit every key under a prefix without allocating entries.
    pub async fn scan_prefix(
        &mut self,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        let (start, end) = key::prefix_range(prefix);
        self.scan_range(&start, end.as_deref(), visit).await
    }

    pub async fn scan_prefix_until<B>(
        &mut self,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        let (start, end) = key::prefix_range(prefix);
        self.scan_range_until(&start, end.as_deref(), visit).await
    }

    pub async fn scan_range_reverse(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_range_reverse_until(start, end, |entry| {
                visit(entry);
                ControlFlow::<()>::Continue(())
            })
            .await?
            .visited)
    }

    pub async fn scan_range_reverse_until<B>(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        if end.is_some_and(|end| end <= start) {
            return Ok(ScanOutcome::complete(0));
        }
        let mut stack = self.seek_reverse(end).await?;
        let mut visited = 0u64;
        loop {
            let Some(frame) = stack.last_mut() else {
                return Ok(ScanOutcome::complete(visited));
            };
            if !frame.node.leaf || frame.node.keys.len() != frame.node.vals.len() {
                return Err(Error::InvalidNode);
            }
            let key = frame.node.keys.get(frame.index).ok_or(Error::InvalidNode)?;
            if key.as_slice() < start {
                return Ok(ScanOutcome::complete(visited));
            }
            let value = frame.node.vals.get(frame.index).ok_or(Error::InvalidNode)?;
            visited = visited.saturating_add(1);
            if let ControlFlow::Break(value) = visit(EntryRef::new(key, value)) {
                return Ok(ScanOutcome::stopped(visited, value));
            }
            if frame.index > 0 {
                frame.index -= 1;
            } else if !self.advance_reverse(&mut stack).await? {
                return Ok(ScanOutcome::complete(visited));
            }
        }
    }

    pub async fn scan_prefix_reverse(
        &mut self,
        prefix: &[u8],
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_prefix_reverse_until(prefix, |entry| {
                visit(entry);
                ControlFlow::<()>::Continue(())
            })
            .await?
            .visited)
    }

    pub async fn scan_prefix_reverse_until<B>(
        &mut self,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        let (start, end) = key::prefix_range(prefix);
        self.scan_range_reverse_until(&start, end.as_deref(), visit)
            .await
    }

    async fn seek_reverse(&self, end: Option<&[u8]>) -> Result<Vec<PathFrame>, Error> {
        let Some(mut node) = self.root.clone() else {
            return Ok(Vec::new());
        };
        let mut stack = Vec::new();
        loop {
            if node.leaf {
                if node.keys.len() != node.vals.len() {
                    return Err(Error::InvalidNode);
                }
                let exclusive = match end {
                    Some(end) => node
                        .keys
                        .partition_point(|candidate| candidate.as_slice() < end),
                    None => node.len(),
                };
                if exclusive == 0 {
                    return Ok(Vec::new());
                }
                stack.push(PathFrame {
                    node,
                    index: exclusive - 1,
                });
                return Ok(stack);
            }
            if node.keys.is_empty() || node.keys.len() != node.vals.len() {
                return Err(Error::InvalidNode);
            }
            let exclusive = match end {
                Some(end) => node
                    .keys
                    .partition_point(|candidate| candidate.as_slice() < end),
                None => node.len(),
            };
            if exclusive == 0 {
                return Ok(Vec::new());
            }
            let index = exclusive - 1;
            let cid = child_cid_at(&node, index)?;
            stack.push(PathFrame { node, index });
            node = self.manager.load_arc(&cid).await?;
        }
    }

    async fn advance_reverse(&self, stack: &mut Vec<PathFrame>) -> Result<bool, Error> {
        stack.pop();
        while let Some(parent) = stack.last_mut() {
            if parent.node.leaf
                || parent.node.keys.is_empty()
                || parent.node.keys.len() != parent.node.vals.len()
            {
                return Err(Error::InvalidNode);
            }
            if parent.index == 0 {
                stack.pop();
                continue;
            }
            parent.index -= 1;
            let cid = child_cid_at(&parent.node, parent.index)?;
            let mut node = self.manager.load_arc(&cid).await?;
            loop {
                if node.leaf {
                    if node.keys.len() != node.vals.len() {
                        return Err(Error::InvalidNode);
                    }
                    let index = node.len().checked_sub(1).ok_or(Error::InvalidNode)?;
                    stack.push(PathFrame { node, index });
                    return Ok(true);
                }
                if node.keys.is_empty() || node.keys.len() != node.vals.len() {
                    return Err(Error::InvalidNode);
                }
                let index = node.len() - 1;
                let cid = child_cid_at(&node, index)?;
                stack.push(PathFrame { node, index });
                node = self.manager.load_arc(&cid).await?;
            }
        }
        Ok(false)
    }
}

#[cfg(feature = "async-store")]
fn async_route_index(node: &Node, key: &[u8]) -> Result<usize, Error> {
    if node.leaf {
        if node.keys.len() != node.vals.len() {
            return Err(Error::InvalidNode);
        }
    } else if node.keys.is_empty() || node.keys.len() != node.vals.len() {
        return Err(Error::InvalidNode);
    }
    Ok(match node.search(key) {
        Ok(index) => index,
        Err(index) => index.saturating_sub(1),
    })
}

#[cfg(feature = "async-store")]
fn fill_async_leaf_locations<K: AsRef<[u8]>>(
    node: Arc<Node>,
    positions: InlinePositions,
    keys: &[K],
    locations: &mut [Option<(Arc<Node>, usize)>],
) -> Result<(), Error> {
    if !node.leaf || node.keys.len() != node.vals.len() {
        return Err(Error::InvalidNode);
    }
    let mut leaf_index = 0usize;
    let mut positions = positions.into_iter().peekable();
    while let Some(position) = positions.next() {
        let key = keys[position].as_ref();
        while leaf_index < node.len() && node.keys[leaf_index].as_slice() < key {
            leaf_index += 1;
        }
        let found = (leaf_index < node.len() && node.keys[leaf_index].as_slice() == key)
            .then_some(leaf_index);
        if let Some(index) = found {
            locations[position] = Some((node.clone(), index));
        }
        while let Some(duplicate) = positions.next_if(|candidate| keys[*candidate].as_ref() == key)
        {
            if let Some(index) = found {
                locations[duplicate] = Some((node.clone(), index));
            }
        }
    }
    Ok(())
}
