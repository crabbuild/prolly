//! Range iteration operations for Prolly trees
//!
//! This module handles range queries and iteration over key-value pairs
//! within specified bounds. It provides efficient traversal of the tree
//! in lexicographic order with support for start and end bounds.
//!
//! # Overview
//!
//! Range iteration allows traversing key-value pairs in sorted order within
//! a specified key range. The iterator efficiently navigates the tree structure,
//! handling node boundaries transparently.
//!
//! # Iteration Behavior
//!
//! ## Start Bound (Inclusive)
//!
//! The iterator begins at the first key greater than or equal to the start bound.
//! If the start key exists, iteration begins there. If not, iteration begins at
//! the next key in lexicographic order.
//!
//! Use an empty slice (`&[]`) to start from the beginning of the tree.
//!
//! ## End Bound (Exclusive)
//!
//! The iterator stops before reaching the end bound. Keys equal to or greater
//! than the end bound are not yielded.
//!
//! Use `None` to iterate to the end of the tree.
//!
//! # Implementation Details
//!
//! The iterator maintains a stack of (node, index) pairs representing the
//! current position in the tree. This allows efficient traversal across
//! node boundaries without restarting from the root.
//!
//! ## Traversal Algorithm
//!
//! 1. **Initial positioning**: Find the path to the start key
//! 2. **Leaf iteration**: Yield entries from the current leaf
//! 3. **Node advancement**: When a leaf is exhausted, backtrack to find the next leaf
//! 4. **Bound checking**: Stop when the end bound is reached
//!
//! # Example
//!
//! ```rust
//! use prolly::{Prolly, MemStore, Config};
//!
//! let store = MemStore::new();
//! let prolly = Prolly::new(store, Config::default());
//! let mut tree = prolly.create();
//!
//! // Insert some data
//! tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
//! tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
//! tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();
//! tree = prolly.put(&tree, b"d".to_vec(), b"4".to_vec()).unwrap();
//!
//! // Iterate over all keys
//! for result in prolly.range(&tree, &[], None).unwrap() {
//!     let (key, val) = result.unwrap();
//!     println!("{:?} -> {:?}", key, val);
//! }
//!
//! // Iterate over range [b, d) - yields "b" and "c"
//! for result in prolly.range(&tree, b"b", Some(b"d")).unwrap() {
//!     let (key, val) = result.unwrap();
//!     println!("{:?} -> {:?}", key, val);
//! }
//! ```
//!
//! # Performance
//!
//! - **Initial seek**: O(log n) to find the starting position
//! - **Per-entry**: O(1) amortized for sequential access within a leaf
//! - **Node transitions**: O(log n) worst case, but typically O(1) amortized
//!
//! The iterator is lazy and only loads nodes as needed, making it memory-efficient
//! for large trees.

use super::error::Error;
use super::node::ReadNode;
use super::read::EntryRef;
use super::store::Store;
use super::tree::Tree;

use super::Prolly;
use super::{store::AsyncStore, AsyncProlly};
use futures_util::stream::{self, Stream};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

type RangeItem = Result<(Vec<u8>, Vec<u8>), Error>;
type LeafEntry = (Vec<u8>, Vec<u8>);
pub(crate) const RANGE_CHILD_PREFETCH_PARALLELISM: usize = 16;

/// Stable cursor token for resumable range scans.
///
/// The token is independent of in-memory traversal state: it records the last
/// emitted key, and the next scan resumes strictly after that key. This makes it
/// suitable for checkpointing background indexing or sync jobs for an immutable
/// tree snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeCursor {
    after_key: Option<Vec<u8>>,
}

impl RangeCursor {
    /// Start scanning from the beginning of the requested range.
    pub fn start() -> Self {
        Self { after_key: None }
    }

    /// Resume scanning strictly after `key`.
    pub fn after_key(key: impl Into<Vec<u8>>) -> Self {
        Self {
            after_key: Some(key.into()),
        }
    }

    /// Return the key this cursor resumes after, if any.
    pub fn after(&self) -> Option<&[u8]> {
        self.after_key.as_deref()
    }

    /// Whether this cursor represents the beginning of a range.
    pub fn is_start(&self) -> bool {
        self.after_key.is_none()
    }
}

/// A bounded page of range-scan results.
///
/// `next_cursor` is `Some` when another call should resume after the last entry
/// in this page. It is `None` when the scan reached the end bound or the end of
/// the tree.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RangePage {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub next_cursor: Option<RangeCursor>,
}

/// Stable cursor token for resumable reverse scans.
///
/// The token records the next exclusive upper bound. A start cursor scans from
/// the end of the requested range; a resumed cursor scans keys strictly before
/// `before_key`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReverseCursor {
    before_key: Option<Vec<u8>>,
}

impl ReverseCursor {
    /// Start scanning from the end of the requested range.
    pub fn end() -> Self {
        Self { before_key: None }
    }

    /// Resume scanning strictly before `key`.
    pub fn before_key(key: impl Into<Vec<u8>>) -> Self {
        Self {
            before_key: Some(key.into()),
        }
    }

    /// Return the key this cursor resumes before, if any.
    pub fn before(&self) -> Option<&[u8]> {
        self.before_key.as_deref()
    }

    /// Whether this cursor represents the end of a range.
    pub fn is_end(&self) -> bool {
        self.before_key.is_none()
    }
}

/// A bounded page of reverse-scan results.
///
/// Entries are returned in descending key order. `next_cursor` is `Some` when
/// another call should resume before the last entry in this page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReversePage {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub next_cursor: Option<ReverseCursor>,
}

/// Bounded result for a stateless cursor seek.
///
/// `position_key`/`position_value` report where the internal cursor lands for
/// the requested seek key. This is the exact key when `found` is true; otherwise
/// it is the closest leaf entry chosen by cursor navigation. `entries` are the
/// forward window starting at the first key greater than or equal to the seek
/// key, and `next_cursor` resumes after the last emitted entry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CursorWindow {
    pub position_key: Option<Vec<u8>>,
    pub position_value: Option<Vec<u8>>,
    pub found: bool,
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub next_cursor: Option<RangeCursor>,
}

/// Backward-compatible name for async range pages.
pub type AsyncRangePage = RangePage;

/// Backward-compatible name for async reverse range pages.
pub type AsyncReversePage = ReversePage;

/// Create a range iterator over key-value pairs.
///
/// Returns an iterator that yields `(key, value)` pairs in lexicographic order,
/// starting from `start` (inclusive) up to `end` (exclusive).
///
/// # Arguments
/// * `prolly` - Reference to the Prolly tree manager
/// * `tree` - The tree to iterate over
/// * `start` - The starting key (inclusive). Use `&[]` to start from the beginning.
/// * `end` - Optional ending key (exclusive). Use `None` to iterate to the end.
///
/// # Returns
/// * `Ok(RangeIter)` - An iterator over the range
/// * `Err` on storage or deserialization errors during path finding
pub fn create_range_iter<'a, S: Store>(
    prolly: &'a Prolly<S>,
    tree: &Tree,
    start: &[u8],
    end: Option<&[u8]>,
) -> Result<RangeIter<'a, S>, Error> {
    let ready_store = prolly.engine.store.clone();
    let future = create_async_range_iter(&prolly.engine, tree, start, end);
    let inner = super::engine::ready::run_ready(ready_store.ready(future))?;
    Ok(RangeIter { inner })
}

/// Create a range iterator that starts strictly after `after_key`.
pub fn create_range_after_iter<'a, S: Store>(
    prolly: &'a Prolly<S>,
    tree: &Tree,
    after_key: &[u8],
    end: Option<&[u8]>,
) -> Result<RangeIter<'a, S>, Error> {
    let ready_store = prolly.engine.store.clone();
    let future = create_async_range_after_iter(&prolly.engine, tree, after_key, end);
    let inner = super::engine::ready::run_ready(ready_store.ready(future))?;
    Ok(RangeIter { inner })
}

/// Create an async range iterator over key-value pairs.
pub async fn create_async_range_iter<'a, S>(
    prolly: &'a AsyncProlly<S>,
    tree: &Tree,
    start: &[u8],
    end: Option<&[u8]>,
) -> Result<AsyncRangeIter<'a, S>, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if end.is_some_and(|end| end <= start) {
        return Ok(AsyncRangeIter::new(prolly, Vec::new(), start, end));
    }

    let path = prolly.find_read_path_arcs(tree, start).await?;
    Ok(AsyncRangeIter::new(prolly, path, start, end))
}

/// Create an async range iterator that starts strictly after `after_key`.
pub async fn create_async_range_after_iter<'a, S>(
    prolly: &'a AsyncProlly<S>,
    tree: &Tree,
    after_key: &[u8],
    end: Option<&[u8]>,
) -> Result<AsyncRangeIter<'a, S>, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if end.is_some_and(|end| end <= after_key) {
        return Ok(AsyncRangeIter::new_after(
            prolly,
            Vec::new(),
            after_key,
            end,
        ));
    }

    let path = prolly.find_read_path_arcs(tree, after_key).await?;
    Ok(AsyncRangeIter::new_after(prolly, path, after_key, end))
}

/// Iterator over key-value pairs in a range.
///
/// Created by [`Prolly::range`]. Yields `(key, value)` pairs in lexicographic
/// order.
///
/// Maintains a stack of (node, index) pairs to track the current position in the tree
/// and supports optional end bounds for range queries.
pub struct RangeIter<'a, S: Store> {
    inner: AsyncRangeIter<'a, super::store::SyncStoreAsAsync<Arc<S>>>,
}

impl<S: Store> RangeIter<'_, S> {
    /// Return a resumable cursor for the last key yielded by this iterator.
    pub fn resume_cursor(&self) -> RangeCursor {
        self.inner.resume_cursor()
    }
}

impl<S: Store> Iterator for RangeIter<'_, S> {
    type Item = RangeItem;

    fn next(&mut self) -> Option<Self::Item> {
        let ready_store = self.inner.prolly.store.clone();
        let future = self.inner.next();
        super::engine::ready::run_ready(ready_store.ready(future))
    }
}
/// Async iterator over key-value pairs in a range.
///
/// Created by [`AsyncProlly::range`](crate::AsyncProlly::range). Call
/// [`AsyncRangeIter::next`] to lazily read one item at a time, or
/// [`AsyncRangeIter::into_stream`] to adapt it to a `futures_util::Stream`.
pub struct AsyncRangeIter<'a, S: AsyncStore> {
    prolly: &'a AsyncProlly<S>,
    stack: Vec<(Arc<ReadNode>, usize)>,
    end: Option<Vec<u8>>,
    started: bool,
    start_key: Vec<u8>,
    skip_start_key: bool,
    last_location: Option<(Arc<ReadNode>, usize)>,
}
impl<'a, S> AsyncRangeIter<'a, S>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    pub(crate) fn new(
        prolly: &'a AsyncProlly<S>,
        stack: Vec<(Arc<ReadNode>, usize)>,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Self {
        Self {
            prolly,
            stack,
            end: end.map(|e| e.to_vec()),
            started: false,
            start_key: start.to_vec(),
            skip_start_key: false,
            last_location: None,
        }
    }

    pub(crate) fn new_after(
        prolly: &'a AsyncProlly<S>,
        stack: Vec<(Arc<ReadNode>, usize)>,
        after_key: &[u8],
        end: Option<&[u8]>,
    ) -> Self {
        Self {
            prolly,
            stack,
            end: end.map(|e| e.to_vec()),
            started: false,
            start_key: after_key.to_vec(),
            skip_start_key: true,
            last_location: None,
        }
    }

    /// Return the next key-value pair in lexicographic order.
    pub async fn next(&mut self) -> Option<RangeItem> {
        self.next_with(|entry| entry.to_owned()).await
    }

    /// Visit the next entry without copying its key or value.
    ///
    /// The callback is synchronous and runs after any required child load has
    /// completed, so its borrowed entry cannot cross an `.await` boundary.
    pub async fn next_with<R>(
        &mut self,
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Option<Result<R, Error>> {
        self.position_at_start();

        let mut read = Some(read);

        loop {
            let (node, idx) = self.stack.last_mut()?;

            if *idx >= node.len() {
                match self.advance_to_next_sibling() {
                    Ok(true) => continue,
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }

            if node.is_leaf() {
                let index = *idx;
                let key = match node.key(index) {
                    Some(key) => key,
                    None => return Some(Err(Error::InvalidNode)),
                };
                if self.end.as_deref().is_some_and(|end| key >= end) {
                    return None;
                }
                let value = match node.value(index) {
                    Some(value) => value,
                    None => return Some(Err(Error::InvalidNode)),
                };
                *idx += 1;
                self.last_location = Some((node.clone(), index));
                return Some(Ok(read
                    .take()
                    .expect("async range callback is invoked at most once")(
                    EntryRef::new(key, value),
                )));
            }

            match child_starts_at_or_after_end(self.end.as_deref(), node, *idx) {
                Ok(true) => return None,
                Ok(false) => {}
                Err(e) => return Some(Err(e)),
            }

            let child = {
                let (node, idx) = self.stack.last()?;
                self.load_child_for_descent(node, *idx).await
            };

            match child {
                Ok(child) => self.stack.push((child, 0)),
                Err(e) => return Some(Err(e)),
            }
        }
    }

    /// Collect all remaining range entries into memory.
    pub async fn collect(mut self) -> Result<Vec<LeafEntry>, Error> {
        let mut entries = Vec::new();
        while let Some(item) = self.next().await {
            entries.push(item?);
        }
        Ok(entries)
    }

    /// Return a resumable cursor for the last key yielded by this iterator.
    ///
    /// If the iterator has not yielded an item yet, this returns
    /// [`RangeCursor::start`].
    pub fn resume_cursor(&self) -> RangeCursor {
        self.last_location
            .as_ref()
            .and_then(|(node, index)| node.key(*index))
            .map(<[u8]>::to_vec)
            .map(RangeCursor::after_key)
            .unwrap_or_else(RangeCursor::start)
    }

    /// Convert this iterator into a `futures_util::Stream`.
    pub fn into_stream(self) -> impl Stream<Item = RangeItem> + 'a {
        stream::unfold(self, |mut iter| async move {
            iter.next().await.map(|item| (item, iter))
        })
    }

    fn position_at_start(&mut self) {
        if self.started {
            return;
        }

        self.started = true;
        let Some((node, idx)) = self.stack.last_mut() else {
            return;
        };

        if node.is_leaf() {
            *idx = match node.search(&self.start_key) {
                Ok(i) if self.skip_start_key => i.saturating_add(1),
                Ok(i) | Err(i) => i,
            };
        }
    }

    fn advance_to_next_sibling(&mut self) -> Result<bool, Error> {
        loop {
            self.stack.pop();
            let Some((parent, parent_idx)) = self.stack.last_mut() else {
                return Ok(false);
            };

            *parent_idx += 1;

            if *parent_idx < parent.len() {
                if child_starts_at_or_after_end(self.end.as_deref(), parent, *parent_idx)? {
                    return Ok(false);
                }
                return Ok(true);
            }
        }
    }

    async fn load_child_for_descent(
        &self,
        node: &ReadNode,
        idx: usize,
    ) -> Result<Arc<ReadNode>, Error> {
        let child_cid = node.child_cid(idx)?;

        if !self.prolly.store().prefers_batch_reads() {
            return self.prolly.load_read_arc(&child_cid).await;
        }

        let max_child_idx = node
            .len()
            .min(idx.saturating_add(RANGE_CHILD_PREFETCH_PARALLELISM));
        let mut child_cids = Vec::with_capacity(max_child_idx.saturating_sub(idx));
        child_cids.push(child_cid);

        for child_idx in idx + 1..max_child_idx {
            if child_starts_at_or_after_end(self.end.as_deref(), node, child_idx).unwrap_or(true) {
                break;
            }

            match node.child_cid(child_idx) {
                Ok(cid) => child_cids.push(cid),
                Err(_) => break,
            }
        }

        if child_cids.len() == 1 {
            return self.prolly.load_read_arc(&child_cids[0]).await;
        }

        let children = self.prolly.load_many_read_ordered(&child_cids).await?;
        children.into_iter().next().ok_or(Error::InvalidNode)
    }
}

fn child_starts_at_or_after_end(
    end: Option<&[u8]>,
    node: &ReadNode,
    child_index: usize,
) -> Result<bool, Error> {
    let Some(end) = end else {
        return Ok(false);
    };

    let first_key = node.key(child_index).ok_or(Error::InvalidNode)?;
    Ok(first_key >= end)
}
