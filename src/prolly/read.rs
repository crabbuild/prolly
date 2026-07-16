//! Borrowed, callback-scoped read APIs.
//!
//! This module implements the core substrate specified by
//! `docs/superpowers/specs/2026-07-15-prolly-zero-copy-read-architecture-design.md`.
//! Values are borrowed from immutable retained nodes only for the duration of a
//! synchronous callback. Existing owned APIs remain compatibility wrappers.

use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use super::blob;
use super::cid::Cid;
use super::error::{Conflict, Diff, Error};
use super::key;
use super::node::ReadNode;
#[cfg(feature = "async-store")]
use super::store::AsyncStore;
use super::store::Store;
use super::tree::Tree;
#[cfg(feature = "async-store")]
use super::AsyncProlly;
use super::{sorted_key_positions, InlinePositions, KeyLookupFrame, KeyValue, Prolly};

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
    node: Arc<ReadNode>,
    index: usize,
}

/// Retained location of one value in an immutable packed leaf.
///
/// This is crate-private so higher-level engines such as proximity reranking
/// can keep a bounded shortlist without copying application values. The value
/// borrow is tied to `&self`; eviction of the shared cache cannot invalidate it.
#[derive(Clone, Debug)]
pub(crate) struct ReadValueHandle {
    node: Arc<ReadNode>,
    index: usize,
}

/// An owned lease over one value in an immutable packed leaf.
///
/// The byte slice remains valid for the lifetime of this object even if the
/// shared node cache evicts the leaf. Native bindings use this as the owner
/// behind callback-scoped zero-copy value views.
#[derive(Clone, Debug)]
pub struct OwnedValueLease {
    handle: ReadValueHandle,
}

impl OwnedValueLease {
    /// Borrow the leased value bytes.
    #[inline]
    pub fn as_bytes(&self) -> Result<&[u8], Error> {
        self.handle.value()
    }

    /// Account the complete retained packed leaf, not only the value slice.
    #[inline]
    pub fn retained_bytes(&self) -> usize {
        self.handle.retained_bytes()
    }
}

impl ReadValueHandle {
    #[inline]
    pub(crate) fn key(&self) -> Result<&[u8], Error> {
        self.node.key(self.index).ok_or(Error::InvalidNode)
    }

    #[inline]
    pub(crate) fn value(&self) -> Result<&[u8], Error> {
        self.node.value(self.index).ok_or(Error::InvalidNode)
    }

    #[inline]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.node.retained_bytes()
    }

    #[inline]
    pub(crate) fn backing_id(&self) -> usize {
        Arc::as_ptr(&self.node) as usize
    }
}

// A fixed-size, direct-mapped table keeps hot route nodes out of the shared
// cache lock without becoming a second unbounded cache. Strong handles avoid
// a weak-count compare/exchange on every routed node while the fixed slot count
// keeps session retention strictly bounded.
const SESSION_NODE_SLOTS: usize = 4096;
const SESSION_NODE_WAYS: usize = 4;
const RECENT_LEAF_DISABLE_AFTER_MISSES: u8 = 8;
const SESSION_NODE_SETS: usize = SESSION_NODE_SLOTS / SESSION_NODE_WAYS;
type SessionNodeSlot = Option<(Cid, Arc<ReadNode>)>;

struct SessionNodeTable {
    slots: Box<[SessionNodeSlot]>,
    next_way: Box<[u8]>,
}

impl SessionNodeTable {
    #[inline]
    fn prefix(cid: &Cid) -> u64 {
        u64::from_ne_bytes(
            cid.as_bytes()[..8]
                .try_into()
                .expect("CID has a fixed 32-byte representation"),
        )
    }

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
        Self::prefix(cid) as usize & (SESSION_NODE_SETS - 1)
    }

    #[inline]
    fn get(&self, cid: &Cid) -> Option<Arc<ReadNode>> {
        let start = Self::set(cid) * SESSION_NODE_WAYS;
        let prefix = Self::prefix(cid);
        self.slots[start..start + SESSION_NODE_WAYS]
            .iter()
            .flatten()
            .find(|(cached, _)| Self::prefix(cached) == prefix && cached == cid)
            .map(|(_, node)| node.clone())
    }

    #[inline]
    fn insert(&mut self, cid: Cid, node: &Arc<ReadNode>) {
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
        ways[slot] = Some((cid, node.clone()));
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
    root: Option<Arc<ReadNode>>,
    recent_leaf: Option<Arc<ReadNode>>,
    recent_leaf_misses: u8,
    recent_leaf_disabled: bool,
    local_nodes: SessionNodeTable,
}

struct OwnedReadSessionState {
    root: Option<Arc<ReadNode>>,
    recent_leaf: Option<Arc<ReadNode>>,
    recent_leaf_misses: u8,
    recent_leaf_disabled: bool,
    local_nodes: SessionNodeTable,
}

/// An owned, root-bound read context suitable for long-lived native binding
/// handles. Unlike [`ReadSession`], this type owns both manager and tree state,
/// so foreign-language adapters can reuse retained routing state across calls.
///
/// Stateful point reads are synchronized. High-concurrency callers should use
/// one session per worker rather than sharing one session across all workers.
pub struct OwnedReadSession<S: Store> {
    manager: Arc<Prolly<S>>,
    tree: Tree,
    state: Mutex<OwnedReadSessionState>,
}

/// Retained forward traversal over one owned read session.
pub struct OwnedRangeScanSession<S: Store> {
    manager: Arc<Prolly<S>>,
    end: Option<Vec<u8>>,
    stack: Vec<PathFrame>,
    done: bool,
}

/// Reusable root-bound read context for an asynchronous store.
///
/// All visitors are synchronous. A node is fully loaded before a visitor sees
/// a borrowed slice, and the visitor returns before traversal can await again.
#[cfg(feature = "async-store")]
pub struct AsyncReadSession<'manager, 'tree, S: AsyncStore> {
    manager: &'manager AsyncProlly<S>,
    tree: &'tree Tree,
    root: Option<Arc<ReadNode>>,
    recent_leaf: Option<Arc<ReadNode>>,
    recent_leaf_misses: u8,
    recent_leaf_disabled: bool,
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
            .map(|cid| self.load_read_arc(cid))
            .transpose()?;
        if let Some(root) = root.as_ref() {
            if root.format() != &tree.config.format {
                return Err(Error::FormatMismatch {
                    expected: tree.config.format.digest()?,
                    actual: root.format().digest()?,
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
            recent_leaf_misses: 0,
            recent_leaf_disabled: false,
            local_nodes: SessionNodeTable::new(),
        })
    }

    /// Open an owned read session for a long-lived native handle.
    pub fn read_owned(self: &Arc<Self>, tree: Tree) -> Result<OwnedReadSession<S>, Error> {
        let session = self.read(&tree)?;
        let state = OwnedReadSessionState {
            root: session.root,
            recent_leaf: session.recent_leaf,
            recent_leaf_misses: session.recent_leaf_misses,
            recent_leaf_disabled: session.recent_leaf_disabled,
            local_nodes: session.local_nodes,
        };
        Ok(OwnedReadSession {
            manager: self.clone(),
            tree,
            state: Mutex::new(state),
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
                .key(0)
                .zip(leaf.key(leaf.len().saturating_sub(1)))
                .is_some_and(|(first, last)| key >= first && key <= last)
            {
                self.metrics.add_cache_hits(1);
                return match leaf.search(key) {
                    Ok(index) => Ok(Some(read(leaf.value(index).ok_or(Error::InvalidNode)?))),
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

impl<S: Store> OwnedReadSession<S> {
    fn get_handle_with_state(
        &self,
        state: &mut OwnedReadSessionState,
        key: &[u8],
    ) -> Result<Option<ReadValueHandle>, Error> {
        if let Some(leaf) = state
            .recent_leaf
            .as_ref()
            .filter(|_| !state.recent_leaf_disabled)
        {
            ReadSession::<S>::validate_leaf(leaf)?;
            if leaf
                .key(0)
                .zip(leaf.key(leaf.len().saturating_sub(1)))
                .is_some_and(|(first, last)| key >= first && key <= last)
            {
                state.recent_leaf_misses = 0;
                return Ok(leaf.search(key).ok().map(|index| ReadValueHandle {
                    node: leaf.clone(),
                    index,
                }));
            }
            state.recent_leaf_misses = state.recent_leaf_misses.saturating_add(1);
            if state.recent_leaf_misses >= RECENT_LEAF_DISABLE_AFTER_MISSES {
                state.recent_leaf = None;
                state.recent_leaf_misses = 0;
                state.recent_leaf_disabled = true;
            }
        }

        let Some(mut node) = state.root.clone() else {
            return Ok(None);
        };
        loop {
            if node.is_leaf() {
                ReadSession::<S>::validate_leaf(&node)?;
                if !state.recent_leaf_disabled {
                    state.recent_leaf = Some(node.clone());
                }
                return Ok(node
                    .search(key)
                    .ok()
                    .map(|index| ReadValueHandle { node, index }));
            }
            let index = ReadSession::<S>::route_index(&node, key)?;
            let cid = node.child_cid(index)?;
            node = match state.local_nodes.get(&cid) {
                Some(node) => node,
                None => {
                    let node = self.manager.load_read_arc(&cid)?;
                    if !node.is_leaf() {
                        state.local_nodes.insert(cid, &node);
                    }
                    node
                }
            };
        }
    }

    /// Read a value without allocating it while retaining root and route state
    /// across calls.
    pub fn get_with<R>(
        &self,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error> {
        let mut state = self.state.lock().map_err(|_| Error::InvalidNode)?;
        self.get_handle_with_state(&mut state, key)?
            .map(|handle| handle.value().map(read))
            .transpose()
    }

    /// Retain the packed leaf containing a value. This is intended for native
    /// adapters that expose a callback-scoped view and release it
    /// deterministically when the callback returns.
    pub fn get_lease(&self, key: &[u8]) -> Result<Option<OwnedValueLease>, Error> {
        let mut state = self.state.lock().map_err(|_| Error::InvalidNode)?;
        self.get_handle_with_state(&mut state, key)
            .map(|handle| handle.map(|handle| OwnedValueLease { handle }))
    }

    /// Read keys in caller order while holding the session state once.
    pub fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let mut values = vec![None; keys.len()];
        self.get_many_with(keys, |position, _, value| {
            values[position] = value.map(<[u8]>::to_vec);
        })?;
        Ok(values)
    }

    /// Visit batch point-read results in input order while sharing route work.
    pub fn get_many_with<K, F>(&self, keys: &[K], mut visit: F) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        F: for<'value> FnMut(usize, &[u8], Option<&'value [u8]>),
    {
        if keys.is_empty() {
            return Ok(());
        }
        let root = self
            .state
            .lock()
            .map_err(|_| Error::InvalidNode)?
            .root
            .clone();
        let Some(root) = root else {
            for (position, key) in keys.iter().enumerate() {
                visit(position, key.as_ref(), None);
            }
            return Ok(());
        };
        let positions =
            InlinePositions::from_vec(sorted_key_positions(keys)).expect("non-empty key positions");
        let mut frames = vec![(root, positions)];
        let mut locations: Vec<Option<(Arc<ReadNode>, usize)>> = vec![None; keys.len()];
        while !frames.is_empty() {
            let mut children = Vec::new();
            for (node, positions) in frames {
                if node.is_leaf() {
                    fill_packed_leaf_locations(node, positions, keys, &mut locations)?;
                } else {
                    children.extend(route_packed_positions(&node, positions, keys)?);
                }
            }
            if children.is_empty() {
                break;
            }
            let cids = children
                .iter()
                .map(|frame| frame.cid.clone())
                .collect::<Vec<_>>();
            let nodes = self.manager.load_many_read_ordered(&cids)?;
            frames = children
                .into_iter()
                .zip(nodes)
                .map(|(frame, node)| (node, frame.positions))
                .collect();
        }
        for (position, key) in keys.iter().enumerate() {
            let value = locations[position]
                .as_ref()
                .map(|(node, index)| node.value(*index).ok_or(Error::InvalidNode))
                .transpose()?;
            visit(position, key.as_ref(), value);
        }
        Ok(())
    }

    /// Visit a half-open range. The owned session keeps the root warm; the
    /// traversal itself remains borrowed and callback-scoped.
    pub fn scan_range_until<B>(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_range_session(start, end)?.next_until(visit)
    }

    fn ensure_same_manager(&self, other: &Self) -> Result<(), Error> {
        if Arc::ptr_eq(&self.manager, &other.manager) {
            Ok(())
        } else {
            Err(Error::InvalidFormat(
                "read sessions belong to different prolly managers".to_string(),
            ))
        }
    }

    /// Visit differences between two root-bound sessions without copying diff
    /// fields. Both sessions must come from the same manager/store identity.
    pub fn scan_range_diff_until<B>(
        &self,
        other: &Self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'diff> FnMut(DiffRef<'diff>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.ensure_same_manager(other)?;
        self.manager
            .scan_range_diff_until(&self.tree, &other.tree, start, end, visit)
    }

    /// Visit genuine three-way conflicts between compatible root-bound
    /// sessions. The receiver is the merge base.
    pub fn scan_conflicts_until<B>(
        &self,
        left: &Self,
        right: &Self,
        visit: impl for<'conflict> FnMut(ConflictRef<'conflict>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.ensure_same_manager(left)?;
        self.ensure_same_manager(right)?;
        self.manager
            .scan_conflicts_until(&self.tree, &left.tree, &right.tree, visit)
    }

    /// Open a retained forward traversal that seeks once and continues from
    /// the same native stack across pages.
    pub fn scan_range_session(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<OwnedRangeScanSession<S>, Error> {
        let root = self
            .state
            .lock()
            .map_err(|_| Error::InvalidNode)?
            .root
            .clone();
        OwnedRangeScanSession::new(self.manager.clone(), root, start, end)
    }

    /// The immutable tree bound to this session.
    pub fn tree(&self) -> &Tree {
        &self.tree
    }
}

impl<S: Store> OwnedRangeScanSession<S> {
    fn new(
        manager: Arc<Prolly<S>>,
        root: Option<Arc<ReadNode>>,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Self, Error> {
        let done = end.is_some_and(|end| end <= start);
        let stack = if done {
            Vec::new()
        } else {
            Self::seek_forward(manager.as_ref(), root, start)?
        };
        Ok(Self {
            manager,
            end: end.map(<[u8]>::to_vec),
            done,
            stack,
        })
    }

    fn seek_forward(
        manager: &Prolly<S>,
        root: Option<Arc<ReadNode>>,
        key: &[u8],
    ) -> Result<Vec<PathFrame>, Error> {
        let Some(mut node) = root else {
            return Ok(Vec::new());
        };
        let mut stack = Vec::new();
        loop {
            if node.is_leaf() {
                ReadSession::<S>::validate_leaf(&node)?;
                let index = packed_partition_point(&node, |candidate| candidate < key);
                stack.push(PathFrame { node, index });
                return Ok(stack);
            }
            ReadSession::<S>::validate_internal(&node)?;
            let index =
                packed_partition_point(&node, |candidate| candidate <= key).saturating_sub(1);
            let cid = node.child_cid(index)?;
            stack.push(PathFrame { node, index });
            node = manager.load_read_arc(&cid)?;
        }
    }

    /// Continue the retained traversal until its bound or callback stop.
    pub fn next_until<B>(
        &mut self,
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        if self.done {
            return Ok(ScanOutcome::complete(0));
        }
        let mut visited = 0u64;
        loop {
            let Some(frame) = self.stack.last_mut() else {
                self.done = true;
                return Ok(ScanOutcome::complete(visited));
            };
            if !frame.node.is_leaf() {
                return Err(Error::InvalidNode);
            }
            ReadSession::<S>::validate_leaf(&frame.node)?;
            if frame.index >= frame.node.len() {
                if !ReadSession::<S>::advance_forward(self.manager.as_ref(), &mut self.stack)? {
                    self.done = true;
                    return Ok(ScanOutcome::complete(visited));
                }
                continue;
            }
            let key = frame.node.key(frame.index).ok_or(Error::InvalidNode)?;
            if self.end.as_deref().is_some_and(|end| key >= end) {
                self.done = true;
                return Ok(ScanOutcome::complete(visited));
            }
            let value = frame.node.value(frame.index).ok_or(Error::InvalidNode)?;
            frame.index += 1;
            visited = visited.saturating_add(1);
            if let ControlFlow::Break(value) = visit(EntryRef::new(key, value)) {
                return Ok(ScanOutcome::stopped(visited, value));
            }
        }
    }

    pub fn is_done(&self) -> bool {
        self.done
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
        if root.is_leaf() {
            return Ok(root.len() as u64);
        }
        if (0..root.len()).all(|index| root.child_count(index).is_some_and(|count| count > 0)) {
            return Ok((0..root.len())
                .map(|index| root.child_count(index).expect("checked child count"))
                .sum());
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
            if node.is_leaf() {
                Self::validate_leaf(&node)?;
                let insertion = packed_partition_point(&node, |candidate| candidate < key);
                return Ok(rank.saturating_add(insertion as u64));
            }
            Self::validate_internal(&node)?;
            let insertion = packed_partition_point(&node, |candidate| candidate <= key);
            if insertion == 0 {
                return Ok(rank);
            }
            let child_index = insertion - 1;
            for index in 0..child_index {
                rank = rank.saturating_add(self.child_count(&node, index)?);
            }
            node = self.manager.load_read_arc(&node.child_cid(child_index)?)?;
        }
    }

    /// Read a value without copying it.
    pub fn get_with<R>(
        &mut self,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error> {
        if let Some(leaf) = self
            .recent_leaf
            .as_ref()
            .filter(|_| !self.recent_leaf_disabled)
        {
            Self::validate_leaf(leaf)?;
            if leaf
                .key(0)
                .zip(leaf.key(leaf.len().saturating_sub(1)))
                .is_some_and(|(first, last)| key >= first && key <= last)
            {
                self.recent_leaf_misses = 0;
                return match leaf.search(key) {
                    Ok(index) => Ok(Some(read(leaf.value(index).ok_or(Error::InvalidNode)?))),
                    Err(_) => Ok(None),
                };
            }
            self.recent_leaf_misses = self.recent_leaf_misses.saturating_add(1);
            if self.recent_leaf_misses >= RECENT_LEAF_DISABLE_AFTER_MISSES {
                self.recent_leaf = None;
                self.recent_leaf_misses = 0;
                self.recent_leaf_disabled = true;
            }
        }

        let Some(mut node) = self.root.clone() else {
            return Ok(None);
        };
        loop {
            if node.is_leaf() {
                Self::validate_leaf(&node)?;
                if !self.recent_leaf_disabled {
                    self.recent_leaf = Some(node.clone());
                }
                return match node.search(key) {
                    Ok(index) => Ok(Some(read(node.value(index).ok_or(Error::InvalidNode)?))),
                    Err(_) => Ok(None),
                };
            }
            let index = Self::route_index(&node, key)?;
            let cid = node.child_cid(index)?;
            node = match self.local_nodes.get(&cid) {
                Some(node) => node,
                None => {
                    let node = self.manager.load_read_arc(&cid)?;
                    if !node.is_leaf() {
                        self.local_nodes.insert(cid, &node);
                    }
                    node
                }
            };
        }
    }

    /// Retain the packed leaf location for a bounded internal consumer.
    ///
    /// Public callers use `get_with`; this handle exists for algorithms that
    /// must rank several records before deciding which final values to own.
    pub(crate) fn get_handle(&mut self, key: &[u8]) -> Result<Option<ReadValueHandle>, Error> {
        if let Some(leaf) = self
            .recent_leaf
            .as_ref()
            .filter(|_| !self.recent_leaf_disabled)
        {
            Self::validate_leaf(leaf)?;
            if leaf
                .key(0)
                .zip(leaf.key(leaf.len().saturating_sub(1)))
                .is_some_and(|(first, last)| key >= first && key <= last)
            {
                self.recent_leaf_misses = 0;
                return Ok(leaf.search(key).ok().map(|index| ReadValueHandle {
                    node: leaf.clone(),
                    index,
                }));
            }
            self.recent_leaf_misses = self.recent_leaf_misses.saturating_add(1);
            if self.recent_leaf_misses >= RECENT_LEAF_DISABLE_AFTER_MISSES {
                self.recent_leaf = None;
                self.recent_leaf_misses = 0;
                self.recent_leaf_disabled = true;
            }
        }

        let Some(mut node) = self.root.clone() else {
            return Ok(None);
        };
        loop {
            if node.is_leaf() {
                Self::validate_leaf(&node)?;
                if !self.recent_leaf_disabled {
                    self.recent_leaf = Some(node.clone());
                }
                return Ok(match node.search(key) {
                    Ok(index) => Some(ReadValueHandle { node, index }),
                    Err(_) => None,
                });
            }
            let index = Self::route_index(&node, key)?;
            let cid = node.child_cid(index)?;
            node = match self.local_nodes.get(&cid) {
                Some(node) => node,
                None => {
                    let node = self.manager.load_read_arc(&cid)?;
                    if !node.is_leaf() {
                        self.local_nodes.insert(cid, &node);
                    }
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
        let positions =
            InlinePositions::from_vec(sorted_key_positions(keys)).expect("non-empty key positions");
        let mut frames = vec![(root, positions)];
        let mut locations: Vec<Option<(Arc<ReadNode>, usize)>> = vec![None; keys.len()];
        while !frames.is_empty() {
            let mut children = Vec::new();
            for (node, positions) in frames {
                if node.is_leaf() {
                    fill_packed_leaf_locations(node, positions, keys, &mut locations)?;
                } else {
                    children.extend(route_packed_positions(&node, positions, keys)?);
                }
            }
            if children.is_empty() {
                break;
            }
            let cids = children
                .iter()
                .map(|frame| frame.cid.clone())
                .collect::<Vec<_>>();
            let nodes = self.manager.load_many_read_ordered(&cids)?;
            frames = children
                .into_iter()
                .zip(nodes)
                .map(|(frame, node)| (node, frame.positions))
                .collect();
        }
        for (position, key) in keys.iter().enumerate() {
            let value = locations[position]
                .as_ref()
                .map(|(node, index)| node.value(*index).ok_or(Error::InvalidNode))
                .transpose()?;
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
            if node.is_leaf() {
                Self::validate_leaf(&node)?;
                let index = usize::try_from(ordinal).map_err(|_| Error::InvalidNode)?;
                let Some(key) = node.key(index) else {
                    return Ok(None);
                };
                let value = node.value(index).ok_or(Error::InvalidNode)?;
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
            node = self.manager.load_read_arc(&node.child_cid(index)?)?;
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
            if !frame.node.is_leaf() {
                return Err(Error::InvalidNode);
            }
            Self::validate_leaf(&frame.node)?;
            let key = frame.node.key(frame.index).ok_or(Error::InvalidNode)?;
            if key < start {
                return Ok(ScanOutcome::complete(visited));
            }
            let value = frame.node.value(frame.index).ok_or(Error::InvalidNode)?;
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
            if !frame.node.is_leaf() {
                return Err(Error::InvalidNode);
            }
            Self::validate_leaf(&frame.node)?;
            if frame.index >= frame.node.len() {
                if !Self::advance_forward(self.manager, &mut stack)? {
                    return Ok(ScanOutcome::complete(visited));
                }
                continue;
            }
            let key = frame.node.key(frame.index).ok_or(Error::InvalidNode)?;
            if end.is_some_and(|end| key >= end) {
                return Ok(ScanOutcome::complete(visited));
            }
            let value = frame.node.value(frame.index).ok_or(Error::InvalidNode)?;
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
            if node.is_leaf() {
                Self::validate_leaf(&node)?;
                let index = if strict {
                    packed_partition_point(&node, |candidate| candidate <= key)
                } else {
                    packed_partition_point(&node, |candidate| candidate < key)
                };
                stack.push(PathFrame { node, index });
                return Ok(stack);
            }
            Self::validate_internal(&node)?;
            let index =
                packed_partition_point(&node, |candidate| candidate <= key).saturating_sub(1);
            let cid = node.child_cid(index)?;
            stack.push(PathFrame { node, index });
            node = self.manager.load_read_arc(&cid)?;
        }
    }

    fn seek_reverse(&self, end: Option<&[u8]>) -> Result<Vec<PathFrame>, Error> {
        let Some(mut node) = self.root.clone() else {
            return Ok(Vec::new());
        };
        let mut stack = Vec::new();
        loop {
            if node.is_leaf() {
                Self::validate_leaf(&node)?;
                let exclusive = match end {
                    Some(end) => packed_partition_point(&node, |candidate| candidate < end),
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
                Some(end) => packed_partition_point(&node, |candidate| candidate < end),
                None => node.len(),
            };
            if exclusive == 0 {
                return Ok(Vec::new());
            }
            let index = exclusive - 1;
            let cid = node.child_cid(index)?;
            stack.push(PathFrame { node, index });
            node = self.manager.load_read_arc(&cid)?;
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
            let cid = parent.node.child_cid(parent.index)?;
            let mut node = manager.load_read_arc(&cid)?;
            loop {
                if node.is_leaf() {
                    Self::validate_leaf(&node)?;
                    stack.push(PathFrame { node, index: 0 });
                    return Ok(true);
                }
                Self::validate_internal(&node)?;
                let cid = node.child_cid(0)?;
                stack.push(PathFrame { node, index: 0 });
                node = manager.load_read_arc(&cid)?;
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
            let cid = parent.node.child_cid(parent.index)?;
            let mut node = manager.load_read_arc(&cid)?;
            loop {
                if node.is_leaf() {
                    Self::validate_leaf(&node)?;
                    let index = node.len().checked_sub(1).ok_or(Error::InvalidNode)?;
                    stack.push(PathFrame { node, index });
                    return Ok(true);
                }
                Self::validate_internal(&node)?;
                let index = node.len().checked_sub(1).ok_or(Error::InvalidNode)?;
                let cid = node.child_cid(index)?;
                stack.push(PathFrame { node, index });
                node = manager.load_read_arc(&cid)?;
            }
        }
        Ok(false)
    }

    fn route_index(node: &ReadNode, key: &[u8]) -> Result<usize, Error> {
        if node.is_leaf() {
            Self::validate_leaf(node)?;
        } else {
            Self::validate_internal(node)?;
        }
        Ok(match node.search(key) {
            Ok(index) => index,
            Err(index) => index.saturating_sub(1),
        })
    }

    fn validate_leaf(node: &ReadNode) -> Result<(), Error> {
        if !node.is_leaf() {
            return Err(Error::InvalidNode);
        }
        Ok(())
    }

    fn validate_internal(node: &ReadNode) -> Result<(), Error> {
        if node.is_leaf() || node.is_empty() {
            return Err(Error::InvalidNode);
        }
        Ok(())
    }

    fn child_count(&self, node: &ReadNode, index: usize) -> Result<u64, Error> {
        match node.child_count(index) {
            Some(count) if count > 0 => Ok(count),
            _ => self.manager.subtree_count(&node.child_cid(index)?),
        }
    }
}

#[inline]
fn packed_partition_point(node: &ReadNode, mut predicate: impl FnMut(&[u8]) -> bool) -> usize {
    let mut left = 0usize;
    let mut right = node.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if predicate(node.key(mid).expect("validated read node metadata")) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

fn route_packed_positions<K: AsRef<[u8]>>(
    node: &ReadNode,
    positions: InlinePositions,
    keys: &[K],
) -> Result<Vec<KeyLookupFrame>, Error> {
    if node.is_leaf() || node.is_empty() {
        return Err(Error::InvalidNode);
    }
    let mut frames = Vec::<KeyLookupFrame>::with_capacity(node.len().min(positions.len()));
    let mut child_index = match node.search(keys[positions.first].as_ref()) {
        Ok(index) => index,
        Err(index) => index.saturating_sub(1),
    };
    let mut last_child = None;
    for position in positions {
        let key = keys[position].as_ref();
        while child_index + 1 < node.len()
            && key >= node.key(child_index + 1).ok_or(Error::InvalidNode)?
        {
            child_index += 1;
        }
        if last_child == Some(child_index) {
            frames
                .last_mut()
                .ok_or(Error::InvalidNode)?
                .positions
                .push(position);
        } else {
            frames.push(KeyLookupFrame {
                cid: node.child_cid(child_index)?,
                positions: InlinePositions::new(position),
            });
            last_child = Some(child_index);
        }
    }
    Ok(frames)
}

fn fill_packed_leaf_locations<K: AsRef<[u8]>>(
    node: Arc<ReadNode>,
    positions: InlinePositions,
    keys: &[K],
    locations: &mut [Option<(Arc<ReadNode>, usize)>],
) -> Result<(), Error> {
    if !node.is_leaf() {
        return Err(Error::InvalidNode);
    }
    let mut leaf_index = 0usize;
    let mut positions = positions.into_iter().peekable();
    while let Some(position) = positions.next() {
        let key = keys[position].as_ref();
        while leaf_index < node.len() && node.key(leaf_index).ok_or(Error::InvalidNode)? < key {
            leaf_index += 1;
        }
        let found =
            (leaf_index < node.len() && node.key(leaf_index) == Some(key)).then_some(leaf_index);
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
        let root = tree.root.as_ref().map(|cid| self.load_read_arc(cid));
        let root = match root {
            Some(load) => Some(load.await?),
            None => None,
        };
        if let Some(root) = root.as_ref() {
            if root.format() != &tree.config.format {
                return Err(Error::FormatMismatch {
                    expected: tree.config.format.digest()?,
                    actual: root.format().digest()?,
                });
            }
        }
        Ok(AsyncReadSession {
            manager: self,
            tree,
            root,
            recent_leaf: None,
            recent_leaf_misses: 0,
            recent_leaf_disabled: false,
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
        if let Some(leaf) = self
            .recent_leaf
            .as_ref()
            .filter(|_| !self.recent_leaf_disabled)
        {
            ReadSession::<super::store::MemStore>::validate_leaf(leaf)?;
            if leaf
                .key(0)
                .zip(leaf.key(leaf.len().saturating_sub(1)))
                .is_some_and(|(first, last)| key >= first && key <= last)
            {
                self.recent_leaf_misses = 0;
                return match leaf.search(key) {
                    Ok(index) => Ok(Some(read(leaf.value(index).ok_or(Error::InvalidNode)?))),
                    Err(_) => Ok(None),
                };
            }
            self.recent_leaf_misses = self.recent_leaf_misses.saturating_add(1);
            if self.recent_leaf_misses >= RECENT_LEAF_DISABLE_AFTER_MISSES {
                self.recent_leaf = None;
                self.recent_leaf_misses = 0;
                self.recent_leaf_disabled = true;
            }
        }

        let Some(mut node) = self.root.clone() else {
            return Ok(None);
        };
        loop {
            if node.is_leaf() {
                ReadSession::<super::store::MemStore>::validate_leaf(&node)?;
                if !self.recent_leaf_disabled {
                    self.recent_leaf = Some(node.clone());
                }
                return match node.search(key) {
                    Ok(index) => Ok(Some(read(node.value(index).ok_or(Error::InvalidNode)?))),
                    Err(_) => Ok(None),
                };
            }
            let index = async_route_index(&node, key)?;
            let cid = node.child_cid(index)?;
            node = match self.local_nodes.get(&cid) {
                Some(node) => node,
                None => {
                    let node = self.manager.load_read_arc(&cid).await?;
                    if !node.is_leaf() {
                        self.local_nodes.insert(cid, &node);
                    }
                    node
                }
            };
        }
    }

    /// Retain one packed async leaf location after all required I/O completes.
    pub(crate) async fn get_handle(
        &mut self,
        key: &[u8],
    ) -> Result<Option<ReadValueHandle>, Error> {
        if let Some(leaf) = self
            .recent_leaf
            .as_ref()
            .filter(|_| !self.recent_leaf_disabled)
        {
            ReadSession::<super::store::MemStore>::validate_leaf(leaf)?;
            if leaf
                .key(0)
                .zip(leaf.key(leaf.len().saturating_sub(1)))
                .is_some_and(|(first, last)| key >= first && key <= last)
            {
                self.recent_leaf_misses = 0;
                return Ok(leaf.search(key).ok().map(|index| ReadValueHandle {
                    node: leaf.clone(),
                    index,
                }));
            }
            self.recent_leaf_misses = self.recent_leaf_misses.saturating_add(1);
            if self.recent_leaf_misses >= RECENT_LEAF_DISABLE_AFTER_MISSES {
                self.recent_leaf = None;
                self.recent_leaf_misses = 0;
                self.recent_leaf_disabled = true;
            }
        }

        let Some(mut node) = self.root.clone() else {
            return Ok(None);
        };
        loop {
            if node.is_leaf() {
                ReadSession::<super::store::MemStore>::validate_leaf(&node)?;
                if !self.recent_leaf_disabled {
                    self.recent_leaf = Some(node.clone());
                }
                return Ok(match node.search(key) {
                    Ok(index) => Some(ReadValueHandle { node, index }),
                    Err(_) => None,
                });
            }
            let index = async_route_index(&node, key)?;
            let cid = node.child_cid(index)?;
            node = match self.local_nodes.get(&cid) {
                Some(node) => node,
                None => {
                    let node = self.manager.load_read_arc(&cid).await?;
                    if !node.is_leaf() {
                        self.local_nodes.insert(cid, &node);
                    }
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
        let positions =
            InlinePositions::from_vec(sorted_key_positions(keys)).expect("non-empty key positions");
        let mut frames = vec![(root, positions)];
        let mut locations: Vec<Option<(Arc<ReadNode>, usize)>> = vec![None; keys.len()];
        while !frames.is_empty() {
            let mut children = Vec::new();
            for (node, positions) in frames {
                if node.is_leaf() {
                    fill_packed_leaf_locations(node, positions, keys, &mut locations)?;
                } else {
                    children.extend(route_packed_positions(&node, positions, keys)?);
                }
            }
            if children.is_empty() {
                break;
            }
            let cids = children
                .iter()
                .map(|frame| frame.cid.clone())
                .collect::<Vec<_>>();
            let nodes = self.manager.load_many_read_ordered(&cids).await?;
            frames = children
                .into_iter()
                .zip(nodes)
                .map(|(frame, node)| (node, frame.positions))
                .collect();
        }
        for (position, key) in keys.iter().enumerate() {
            let value = locations[position]
                .as_ref()
                .map(|(node, index)| node.value(*index).ok_or(Error::InvalidNode))
                .transpose()?;
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
        if end.is_some_and(|end| end <= start) {
            return Ok(ScanOutcome::complete(0));
        }
        let mut stack = self.seek_forward(start).await?;
        let mut visited = 0u64;
        loop {
            let Some(frame) = stack.last_mut() else {
                return Ok(ScanOutcome::complete(visited));
            };
            if !frame.node.is_leaf() {
                return Err(Error::InvalidNode);
            }
            if frame.index >= frame.node.len() {
                if !self.advance_forward(&mut stack).await? {
                    return Ok(ScanOutcome::complete(visited));
                }
                continue;
            }
            let key = frame.node.key(frame.index).ok_or(Error::InvalidNode)?;
            if end.is_some_and(|end| key >= end) {
                return Ok(ScanOutcome::complete(visited));
            }
            let value = frame.node.value(frame.index).ok_or(Error::InvalidNode)?;
            frame.index += 1;
            visited = visited.saturating_add(1);
            if let ControlFlow::Break(value) = visit(EntryRef::new(key, value)) {
                return Ok(ScanOutcome::stopped(visited, value));
            }
        }
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
            if !frame.node.is_leaf() {
                return Err(Error::InvalidNode);
            }
            let key = frame.node.key(frame.index).ok_or(Error::InvalidNode)?;
            if key < start {
                return Ok(ScanOutcome::complete(visited));
            }
            let value = frame.node.value(frame.index).ok_or(Error::InvalidNode)?;
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
            if node.is_leaf() {
                let exclusive = match end {
                    Some(end) => packed_partition_point(&node, |candidate| candidate < end),
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
            if node.is_empty() {
                return Err(Error::InvalidNode);
            }
            let exclusive = match end {
                Some(end) => packed_partition_point(&node, |candidate| candidate < end),
                None => node.len(),
            };
            if exclusive == 0 {
                return Ok(Vec::new());
            }
            let index = exclusive - 1;
            let cid = node.child_cid(index)?;
            stack.push(PathFrame { node, index });
            node = self.manager.load_read_arc(&cid).await?;
        }
    }

    async fn seek_forward(&self, start: &[u8]) -> Result<Vec<PathFrame>, Error> {
        let Some(mut node) = self.root.clone() else {
            return Ok(Vec::new());
        };
        let mut stack = Vec::new();
        loop {
            if node.is_leaf() {
                let index = packed_partition_point(&node, |candidate| candidate < start);
                stack.push(PathFrame { node, index });
                return Ok(stack);
            }
            if node.is_empty() {
                return Err(Error::InvalidNode);
            }
            let index =
                packed_partition_point(&node, |candidate| candidate <= start).saturating_sub(1);
            let cid = node.child_cid(index)?;
            stack.push(PathFrame { node, index });
            node = self.manager.load_read_arc(&cid).await?;
        }
    }

    async fn advance_forward(&self, stack: &mut Vec<PathFrame>) -> Result<bool, Error> {
        stack.pop();
        while let Some(parent) = stack.last_mut() {
            if parent.node.is_leaf() || parent.node.is_empty() {
                return Err(Error::InvalidNode);
            }
            parent.index += 1;
            if parent.index >= parent.node.len() {
                stack.pop();
                continue;
            }
            let cid = parent.node.child_cid(parent.index)?;
            let mut node = self.manager.load_read_arc(&cid).await?;
            loop {
                if node.is_leaf() {
                    stack.push(PathFrame { node, index: 0 });
                    return Ok(true);
                }
                if node.is_empty() {
                    return Err(Error::InvalidNode);
                }
                let cid = node.child_cid(0)?;
                stack.push(PathFrame { node, index: 0 });
                node = self.manager.load_read_arc(&cid).await?;
            }
        }
        Ok(false)
    }

    async fn advance_reverse(&self, stack: &mut Vec<PathFrame>) -> Result<bool, Error> {
        stack.pop();
        while let Some(parent) = stack.last_mut() {
            if parent.node.is_leaf() || parent.node.is_empty() {
                return Err(Error::InvalidNode);
            }
            if parent.index == 0 {
                stack.pop();
                continue;
            }
            parent.index -= 1;
            let cid = parent.node.child_cid(parent.index)?;
            let mut node = self.manager.load_read_arc(&cid).await?;
            loop {
                if node.is_leaf() {
                    let index = node.len().checked_sub(1).ok_or(Error::InvalidNode)?;
                    stack.push(PathFrame { node, index });
                    return Ok(true);
                }
                if node.is_empty() {
                    return Err(Error::InvalidNode);
                }
                let index = node.len() - 1;
                let cid = node.child_cid(index)?;
                stack.push(PathFrame { node, index });
                node = self.manager.load_read_arc(&cid).await?;
            }
        }
        Ok(false)
    }
}

#[cfg(feature = "async-store")]
fn async_route_index(node: &ReadNode, key: &[u8]) -> Result<usize, Error> {
    if !node.is_leaf() && node.is_empty() {
        return Err(Error::InvalidNode);
    }
    Ok(match node.search(key) {
        Ok(index) => index,
        Err(index) => index.saturating_sub(1),
    })
}
