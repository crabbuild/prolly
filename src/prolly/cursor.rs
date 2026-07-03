//! Cursor-based tree navigation for Prolly Trees
//!
//! Cursors provide efficient, non-recursive traversal through the tree structure.
//! They maintain a reference to the current node and an index within that node,
//! with parent linkage for upward navigation.
//!
//! # Example
//!
//! ```rust
//! use prolly::{Prolly, MemStore, Config, Cursor};
//!
//! let store = std::sync::Arc::new(MemStore::new());
//! let prolly = Prolly::new(store.clone(), Config::default());
//! let mut tree = prolly.create();
//!
//! // Insert some data
//! tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
//! tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
//!
//! // Create a cursor at key "a"
//! let cursor = Cursor::at_item(&store, &tree, b"a").unwrap();
//! assert!(cursor.is_valid());
//! assert_eq!(cursor.get_key(), Some(b"a".as_slice()));
//! ```

use std::cmp::Ordering;
use std::sync::Arc;

use super::cid::Cid;
use super::error::Diff;
use super::error::Error;
use super::node::Node;
use super::store::Store;
use super::tree::Tree;

/// Iterator wrapper for cursor-based traversal.
///
/// Provides a Rust iterator interface over tree entries using cursors.
/// Yields (key, value) pairs in lexicographic key order.
///
/// # Example
///
/// ```rust
/// use prolly::{Prolly, MemStore, Config, Cursor, CursorIterator};
/// use std::sync::Arc;
///
/// let store = Arc::new(MemStore::new());
/// let prolly = Prolly::new(store.clone(), Config::default());
/// let mut tree = prolly.create();
///
/// tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
/// tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
/// tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();
///
/// // Iterate over all entries
/// let cursor = Cursor::at_item(store.as_ref(), &tree, b"").unwrap();
/// let iter = CursorIterator::new(cursor, store.as_ref(), None);
/// for (key, value) in iter {
///     println!("{:?} -> {:?}", key, value);
/// }
///
/// // Iterate with an end bound (exclusive)
/// let cursor = Cursor::at_item(store.as_ref(), &tree, b"a").unwrap();
/// let iter = CursorIterator::new(cursor, store.as_ref(), Some(b"c".to_vec()));
/// // Only yields "a" -> "1" and "b" -> "2"
/// ```
pub struct CursorIterator<'a, S: Store> {
    /// The cursor for navigation
    cursor: Cursor,
    /// Reference to the store for loading nodes during advance
    store: &'a S,
    /// Optional start key (inclusive) - only yield entries >= this key
    start_key: Option<Vec<u8>>,
    /// Optional end key (exclusive) - iteration stops before this key
    end_key: Option<Vec<u8>>,
    /// Flag to track if we've already yielded the first item
    started: bool,
}

impl<'a, S: Store> CursorIterator<'a, S> {
    /// Create a new cursor iterator.
    ///
    /// # Arguments
    /// * `cursor` - The cursor to iterate from
    /// * `store` - Reference to the store for loading nodes
    /// * `end_key` - Optional end bound (exclusive) - iteration stops before this key
    ///
    /// # Returns
    /// A new CursorIterator that yields (key, value) pairs.
    pub fn new(cursor: Cursor, store: &'a S, end_key: Option<Vec<u8>>) -> Self {
        Self {
            cursor,
            store,
            start_key: None,
            end_key,
            started: false,
        }
    }

    /// Create a new cursor iterator with start and end bounds.
    ///
    /// # Arguments
    /// * `cursor` - The cursor to iterate from
    /// * `store` - Reference to the store for loading nodes
    /// * `start_key` - Optional start bound (inclusive) - only yield entries >= this key
    /// * `end_key` - Optional end bound (exclusive) - iteration stops before this key
    ///
    /// # Returns
    /// A new CursorIterator that yields (key, value) pairs within the specified range.
    pub fn with_bounds(
        cursor: Cursor,
        store: &'a S,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    ) -> Self {
        Self {
            cursor,
            store,
            start_key,
            end_key,
            started: false,
        }
    }

    /// Check if the current cursor position is at or after the start bound.
    ///
    /// Returns true if:
    /// - There is no start bound, or
    /// - The current key is >= the start bound
    fn is_at_or_after_start(&self) -> bool {
        match (&self.start_key, self.cursor.get_key()) {
            (Some(start), Some(current)) => current >= start.as_slice(),
            (None, Some(_)) => true,
            _ => false,
        }
    }

    /// Check if the current cursor position is before the end bound.
    ///
    /// Returns true if:
    /// - There is no end bound, or
    /// - The current key is strictly less than the end bound
    fn is_before_end(&self) -> bool {
        match (&self.end_key, self.cursor.get_key()) {
            (Some(end), Some(current)) => current < end.as_slice(),
            (None, Some(_)) => true,
            _ => false,
        }
    }
}

impl<'a, S: Store> Iterator for CursorIterator<'a, S> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // If cursor is invalid, we're done
        if !self.cursor.is_valid() {
            return None;
        }

        // If we've already started, advance to the next entry
        if self.started {
            if !self
                .cursor
                .advance_before(self.store, self.end_key.as_deref())
            {
                return None;
            }
        } else {
            self.started = true;
            // On first call, skip entries that are before the start bound
            // (cursor may be positioned at greatest key <= start, but we need >= start)
            while self.cursor.is_valid() && !self.is_at_or_after_start() {
                if !self
                    .cursor
                    .advance_before(self.store, self.end_key.as_deref())
                {
                    return None;
                }
            }
        }

        // Check if cursor is still valid after potential advance
        if !self.cursor.is_valid() {
            return None;
        }

        // Check end bound before yielding
        if !self.is_before_end() {
            return None;
        }

        // Get key and value from cursor
        let key = self.cursor.get_key()?.to_vec();
        let value = self.cursor.get_value()?.to_vec();

        Some((key, value))
    }
}

/// Streaming diff iterator using dual cursor traversal.
///
/// Provides memory-efficient diff by yielding results as they're found
/// rather than allocating a Vec. Supports early termination.
///
/// # Example
///
/// ```rust
/// use prolly::{Prolly, MemStore, Config, DiffCursor};
/// use std::sync::Arc;
///
/// let store = Arc::new(MemStore::new());
/// let prolly = Prolly::new(store.clone(), Config::default());
/// let mut base = prolly.create();
/// let mut other = prolly.create();
///
/// base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
/// other = prolly.put(&other, b"a".to_vec(), b"1".to_vec()).unwrap();
/// other = prolly.put(&other, b"b".to_vec(), b"2".to_vec()).unwrap();
///
/// // Stream differences
/// let diff_cursor = DiffCursor::new(store.as_ref(), &base, &other).unwrap();
/// for diff in diff_cursor {
///     println!("{:?}", diff);
/// }
/// ```
pub struct DiffCursor<'a, S: Store> {
    /// Cursor for the base tree
    cursor_base: Cursor,
    /// Cursor for the other tree
    cursor_other: Cursor,
    /// Reference to the store (shared between both trees)
    store: &'a S,
    /// Whether iteration is complete
    done: bool,
}

impl<'a, S: Store> DiffCursor<'a, S> {
    /// Create a new streaming diff iterator.
    ///
    /// # Arguments
    /// * `store` - The storage backend (shared by both trees)
    /// * `base` - The base tree to compare from
    /// * `other` - The other tree to compare to
    ///
    /// # Returns
    /// * `Ok(DiffCursor)` - A new diff iterator
    /// * `Err(Error)` - If cursor initialization fails
    ///
    /// # Behavior
    /// - If both trees are empty, the iterator yields no differences
    /// - If base is empty, all entries from other are yielded as Added
    /// - If other is empty, all entries from base are yielded as Removed
    pub fn new(store: &'a S, base: &Tree, other: &Tree) -> Result<Self, Error> {
        if base.root == other.root {
            let invalid = Cursor::invalid();
            return Ok(DiffCursor {
                cursor_base: invalid.clone(),
                cursor_other: invalid,
                store,
                done: true,
            });
        }

        // Initialize cursors at leftmost positions (empty key = start)
        let cursor_base = Cursor::at_item(store, base, &[])?;
        let cursor_other = Cursor::at_item(store, other, &[])?;

        Ok(DiffCursor {
            cursor_base,
            cursor_other,
            store,
            done: false,
        })
    }
}

impl<'a, S: Store> Iterator for DiffCursor<'a, S> {
    type Item = Diff;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let base_valid = self.cursor_base.is_valid();
            let other_valid = self.cursor_other.is_valid();

            match (base_valid, other_valid) {
                // Both exhausted - done
                (false, false) => {
                    self.done = true;
                    return None;
                }

                // Only other has entries - all additions
                (false, true) => {
                    let key = self.cursor_other.get_key()?.to_vec();
                    let val = self.cursor_other.get_value()?.to_vec();
                    self.cursor_other.advance(self.store);
                    return Some(Diff::Added { key, val });
                }

                // Only base has entries - all removals
                (true, false) => {
                    let key = self.cursor_base.get_key()?.to_vec();
                    let val = self.cursor_base.get_value()?.to_vec();
                    self.cursor_base.advance(self.store);
                    return Some(Diff::Removed { key, val });
                }

                // Both have entries - compare keys
                (true, true) => {
                    let base_key = self.cursor_base.get_key()?.to_vec();
                    let other_key = self.cursor_other.get_key()?.to_vec();

                    match base_key.cmp(&other_key) {
                        Ordering::Less => {
                            // Key only in base - removed
                            let val = self.cursor_base.get_value()?.to_vec();
                            self.cursor_base.advance(self.store);
                            return Some(Diff::Removed { key: base_key, val });
                        }
                        Ordering::Greater => {
                            // Key only in other - added
                            let val = self.cursor_other.get_value()?.to_vec();
                            self.cursor_other.advance(self.store);
                            return Some(Diff::Added {
                                key: other_key,
                                val,
                            });
                        }
                        Ordering::Equal => {
                            // Same key - compare values
                            let base_val = self.cursor_base.get_value()?.to_vec();
                            let other_val = self.cursor_other.get_value()?.to_vec();

                            self.cursor_base.advance(self.store);
                            self.cursor_other.advance(self.store);

                            if base_val != other_val {
                                return Some(Diff::Changed {
                                    key: base_key,
                                    old: base_val,
                                    new: other_val,
                                });
                            }
                            // Values equal - continue to next
                            continue;
                        }
                    }
                }
            }
        }
    }
}

/// A cursor pointing to a specific position in a prolly tree.
///
/// Cursors provide efficient navigation through the tree structure
/// without recursion. They maintain a reference to the current node
/// and an index within that node, with optional parent linkage for
/// upward navigation.
///
/// # Cloning
///
/// Cursors are cloneable. Cloned cursors are independent of the original -
/// advancing one does not affect the other.
#[derive(Clone)]
pub struct Cursor {
    /// Current position within the node (0-based index)
    pub index: usize,
    /// Current node (Arc for efficient cloning)
    pub node: Arc<Node>,
    /// Parent cursor for upward navigation
    pub parent: Option<Box<Cursor>>,
}

impl Cursor {
    pub(crate) fn invalid() -> Self {
        Self {
            index: 0,
            node: Arc::new(Node::new_leaf()),
            parent: None,
        }
    }

    /// Navigate to the position of a key in the tree.
    ///
    /// Follows the Arbor spec's CursorAtItem algorithm:
    /// 1. Find index in current node
    /// 2. If leaf, done
    /// 3. Otherwise, recurse to child with parent linkage
    ///
    /// # Arguments
    /// * `store` - The storage backend to load nodes from
    /// * `tree` - The tree to navigate
    /// * `key` - The key to position at
    ///
    /// # Returns
    /// * `Ok(Cursor)` - A cursor positioned at or near the key
    /// * `Err` - If a storage error occurs
    ///
    /// # Behavior
    /// - If the tree is empty, returns a cursor with an empty node (is_valid() == false)
    /// - If the key exists, positions at that exact key
    /// - If the key doesn't exist, positions at the greatest key <= target
    pub fn at_item<S: Store>(store: &S, tree: &Tree, key: &[u8]) -> Result<Self, Error> {
        // Handle empty tree
        let Some(root_cid) = &tree.root else {
            return Ok(Self::invalid());
        };

        // Load root node
        let root_node = Self::load_node(store, root_cid)?;

        // Navigate from root to leaf
        Self::navigate_to_key(store, root_node, key, None)
    }

    /// Navigate from a node to the leaf containing the key.
    ///
    /// Recursively descends through the tree, building parent linkage.
    fn navigate_to_key<S: Store>(
        store: &S,
        node: Node,
        key: &[u8],
        parent: Option<Box<Cursor>>,
    ) -> Result<Self, Error> {
        let index = Self::key_index(&node.keys, key);

        if node.leaf {
            // At leaf - we're done
            Ok(Self {
                index,
                node: Arc::new(node),
                parent,
            })
        } else {
            let child_cid = child_cid_at(&node, index)?;

            // Internal node - descend to child
            // Create cursor for this internal node (will be parent of child cursor)
            let current_cursor = Self {
                index,
                node: Arc::new(node),
                parent,
            };

            // Load child node
            let child_node = Self::load_node(store, &child_cid)?;

            // Recurse with current cursor as parent
            Self::navigate_to_key(store, child_node, key, Some(Box::new(current_cursor)))
        }
    }

    /// Load a node from the store by its CID.
    fn load_node<S: Store>(store: &S, cid: &Cid) -> Result<Node, Error> {
        let bytes = store
            .get(cid.as_bytes())
            .map_err(|e| Error::Store(Box::new(e)))?
            .ok_or_else(|| Error::NotFound(cid.clone()))?;
        Node::from_bytes(&bytes)
    }

    /// Get the current key at the cursor position.
    ///
    /// # Returns
    /// * `Some(&[u8])` - The key at the current position
    /// * `None` - If the cursor is invalid (empty node or index out of bounds)
    pub fn get_key(&self) -> Option<&[u8]> {
        if self.is_valid() {
            Some(&self.node.keys[self.index])
        } else {
            None
        }
    }

    /// Get the current value at the cursor position.
    ///
    /// Only returns a value for leaf nodes. Internal nodes store CIDs, not values.
    ///
    /// # Returns
    /// * `Some(&[u8])` - The value at the current position (leaf nodes only)
    /// * `None` - If the cursor is invalid or at an internal node
    pub fn get_value(&self) -> Option<&[u8]> {
        if self.is_valid() && self.node.leaf {
            self.node.vals.get(self.index).map(|value| value.as_slice())
        } else {
            None
        }
    }

    /// Check if the cursor is valid.
    ///
    /// A cursor is valid if:
    /// - The node is not empty
    /// - The index is within bounds
    ///
    /// # Returns
    /// `true` if the cursor points to a valid entry, `false` otherwise.
    pub fn is_valid(&self) -> bool {
        !self.node.is_empty() && self.index < self.node.len()
    }

    /// Check if the cursor is at the end of its current node.
    ///
    /// # Returns
    /// `true` if the cursor is at the last entry of the node, `false` otherwise.
    pub fn is_at_end(&self) -> bool {
        self.node.is_empty() || self.index >= self.node.len() - 1
    }

    /// Advance the cursor to the next entry in the tree.
    ///
    /// Follows the Arbor spec's AdvanceCursor algorithm:
    /// 1. Try to advance within current node
    /// 2. If at end, navigate up to parent
    /// 3. Advance parent and load next child
    /// 4. Descend to leftmost leaf of next subtree
    ///
    /// # Arguments
    /// * `store` - The storage backend to load nodes from
    ///
    /// # Returns
    /// * `true` if successfully advanced to next entry
    /// * `false` if at end of tree (no more entries)
    ///
    /// # Behavior
    /// After returning `false`, the cursor becomes invalid (`is_valid()` returns `false`).
    pub fn advance<S: Store>(&mut self, store: &S) -> bool {
        self.advance_before(store, None)
    }

    fn advance_before<S: Store>(&mut self, store: &S, end: Option<&[u8]>) -> bool {
        // If cursor is already invalid, can't advance
        if !self.is_valid() {
            return false;
        }

        // Try to advance within current node
        if self.index + 1 < self.node.len() {
            if let Some(end) = end {
                if self.node.keys[self.index + 1].as_slice() >= end {
                    self.index = self.node.len();
                    return false;
                }
            }
            self.index += 1;
            return true;
        }

        // At end of current node - need to navigate up and find next subtree
        self.advance_via_parent_before(store, end)
    }

    fn advance_via_parent_before<S: Store>(&mut self, store: &S, end: Option<&[u8]>) -> bool {
        // Take ownership of parent to navigate up
        let Some(mut parent) = self.parent.take() else {
            // No parent - we're at root and exhausted, mark as invalid
            self.index = self.node.len(); // Mark as invalid
            return false;
        };

        // Try to advance the parent
        if parent.index + 1 < parent.node.len() {
            // Parent can advance - move to next child
            let next_index = parent.index + 1;
            if child_starts_at_or_after_end(end, &parent.node, next_index) {
                self.index = self.node.len();
                return false;
            }
            parent.index = next_index;

            // Load the next child and descend to its leftmost leaf
            match self.descend_to_leftmost_leaf(store, &parent) {
                Ok(new_cursor) => {
                    *self = new_cursor;
                    true
                }
                Err(_) => {
                    // Storage error - mark as invalid
                    self.index = self.node.len();
                    false
                }
            }
        } else {
            // Parent is also at end - recurse up
            // Temporarily set self to parent to continue navigation
            let mut temp_cursor = *parent;
            if temp_cursor.advance_via_parent_before(store, end) {
                *self = temp_cursor;
                true
            } else {
                // No more entries in tree
                self.index = self.node.len();
                false
            }
        }
    }

    /// Descend from a parent cursor to the leftmost leaf of its current child.
    fn descend_to_leftmost_leaf<S: Store>(
        &self,
        store: &S,
        parent: &Cursor,
    ) -> Result<Cursor, Error> {
        // Load the child node at parent's current index
        let child_cid = child_cid_at(&parent.node, parent.index)?;
        let child_node = Self::load_node(store, &child_cid)?;

        if child_node.leaf {
            // Child is a leaf - create cursor at index 0
            Ok(Cursor {
                index: 0,
                node: Arc::new(child_node),
                parent: Some(Box::new(parent.clone())),
            })
        } else {
            // Child is internal - continue descending
            let child_cursor = Cursor {
                index: 0,
                node: Arc::new(child_node),
                parent: Some(Box::new(parent.clone())),
            };
            child_cursor.descend_to_leftmost_leaf_from_self(store)
        }
    }

    /// Descend from the current cursor (at an internal node) to the leftmost leaf.
    fn descend_to_leftmost_leaf_from_self<S: Store>(self, store: &S) -> Result<Cursor, Error> {
        if self.node.leaf {
            return Ok(self);
        }

        // Load child at index 0
        let child_cid = child_cid_at(&self.node, self.index)?;
        let child_node = Self::load_node(store, &child_cid)?;

        let child_cursor = Cursor {
            index: 0,
            node: Arc::new(child_node),
            parent: Some(Box::new(self)),
        };

        child_cursor.descend_to_leftmost_leaf_from_self(store)
    }

    /// Find the index in keys closest to but not larger than the given key.
    ///
    /// Uses binary search to find the position. Returns:
    /// - The exact index if the key exists
    /// - The index of the greatest key less than the target if key doesn't exist
    /// - 0 if the target is smaller than all keys (or keys is empty)
    ///
    /// # Arguments
    /// * `keys` - Sorted slice of keys to search
    /// * `key` - The target key to find
    ///
    /// # Returns
    /// Index of the key closest to but not larger than the target.
    fn key_index(keys: &[Vec<u8>], key: &[u8]) -> usize {
        if keys.is_empty() {
            return 0;
        }

        match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            // Exact match found
            Ok(i) => i,
            // Key not found, Err(i) is the insertion point
            // We want the greatest key less than target, which is i - 1
            // But if i == 0, target is smaller than all keys, return 0
            Err(i) => {
                if i == 0 {
                    0
                } else {
                    i - 1
                }
            }
        }
    }
}

fn child_cid_at(node: &Node, index: usize) -> Result<Cid, Error> {
    let child_cid_bytes = node.vals.get(index).ok_or(Error::InvalidNode)?;
    Ok(Cid(child_cid_bytes
        .as_slice()
        .try_into()
        .map_err(|_| Error::InvalidNode)?))
}

fn child_starts_at_or_after_end(end: Option<&[u8]>, node: &Node, child_index: usize) -> bool {
    let Some(end) = end else {
        return false;
    };

    match node.keys.get(child_index) {
        Some(first_key) => first_key.as_slice() >= end,
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::super::config::Config;
    use super::super::store::{BatchOp, MemStore};
    use super::super::Prolly;
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

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
        get_calls: AtomicUsize,
    }

    impl Store for CountingStore {
        type Error = CountingStoreError;

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.data.lock().unwrap().get(key).cloned())
        }

        fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            self.data
                .lock()
                .unwrap()
                .insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
            self.data.lock().unwrap().remove(key);
            Ok(())
        }

        fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
            let mut data = self.data.lock().unwrap();
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
    }

    #[test]
    fn test_key_index_empty_keys() {
        let keys: Vec<Vec<u8>> = vec![];
        assert_eq!(Cursor::key_index(&keys, b"any"), 0);
    }

    #[test]
    fn test_key_index_exact_match() {
        let keys = vec![b"a".to_vec(), b"c".to_vec(), b"e".to_vec()];
        assert_eq!(Cursor::key_index(&keys, b"a"), 0);
        assert_eq!(Cursor::key_index(&keys, b"c"), 1);
        assert_eq!(Cursor::key_index(&keys, b"e"), 2);
    }

    #[test]
    fn test_key_index_between_keys() {
        let keys = vec![b"a".to_vec(), b"c".to_vec(), b"e".to_vec()];
        // "b" is between "a" and "c", should return index of "a" (0)
        assert_eq!(Cursor::key_index(&keys, b"b"), 0);
        // "d" is between "c" and "e", should return index of "c" (1)
        assert_eq!(Cursor::key_index(&keys, b"d"), 1);
    }

    #[test]
    fn test_key_index_smaller_than_all() {
        let keys = vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()];
        // "a" is smaller than all keys, should return 0
        assert_eq!(Cursor::key_index(&keys, b"a"), 0);
    }

    #[test]
    fn test_key_index_larger_than_all() {
        let keys = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        // "z" is larger than all keys, should return index of last key (2)
        assert_eq!(Cursor::key_index(&keys, b"z"), 2);
    }

    #[test]
    fn test_key_index_single_key() {
        let keys = vec![b"m".to_vec()];
        assert_eq!(Cursor::key_index(&keys, b"a"), 0); // smaller
        assert_eq!(Cursor::key_index(&keys, b"m"), 0); // exact
        assert_eq!(Cursor::key_index(&keys, b"z"), 0); // larger
    }

    #[test]
    fn test_at_item_empty_tree() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let tree = prolly.create();

        let cursor = Cursor::at_item(store.as_ref(), &tree, b"any").unwrap();
        assert!(!cursor.is_valid());
        assert_eq!(cursor.get_key(), None);
        assert_eq!(cursor.get_value(), None);
    }

    #[test]
    fn diff_cursor_skips_identical_roots_without_reads() {
        let store = std::sync::Arc::new(CountingStore::default());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();
        tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
        let gets_before = store.get_calls.load(Ordering::Relaxed);

        let mut diff = DiffCursor::new(store.as_ref(), &tree, &tree).unwrap();

        assert_eq!(diff.next(), None);
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            gets_before,
            "identical-root cursor diffs should avoid root and leaf reads"
        );
    }

    #[test]
    fn get_value_returns_none_for_leaf_with_missing_value() {
        let mut leaf = Node::new_leaf();
        leaf.keys.push(b"a".to_vec());
        let cursor = Cursor {
            index: 0,
            node: Arc::new(leaf),
            parent: None,
        };

        assert!(cursor.is_valid());
        assert_eq!(cursor.get_key(), Some(b"a".as_slice()));
        assert_eq!(cursor.get_value(), None);
    }

    #[test]
    fn internal_descent_rejects_missing_child_cid() {
        let store = std::sync::Arc::new(MemStore::new());
        let mut root = Node::new_internal(1);
        root.keys.push(b"a".to_vec());
        let cursor = Cursor {
            index: 0,
            node: Arc::new(root),
            parent: None,
        };

        let err = match cursor.descend_to_leftmost_leaf_from_self(store.as_ref()) {
            Ok(_) => panic!("malformed internal node should be rejected"),
            Err(err) => err,
        };

        assert!(matches!(err, Error::InvalidNode));
    }

    #[test]
    fn test_at_item_exact_key() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
        tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();

        let cursor = Cursor::at_item(store.as_ref(), &tree, b"b").unwrap();
        assert!(cursor.is_valid());
        assert_eq!(cursor.get_key(), Some(b"b".as_slice()));
        assert_eq!(cursor.get_value(), Some(b"2".as_slice()));
    }

    #[test]
    fn test_at_item_nonexistent_key() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();
        tree = prolly.put(&tree, b"e".to_vec(), b"5".to_vec()).unwrap();

        // "b" doesn't exist, should position at "a" (greatest key <= "b")
        let cursor = Cursor::at_item(store.as_ref(), &tree, b"b").unwrap();
        assert!(cursor.is_valid());
        assert_eq!(cursor.get_key(), Some(b"a".as_slice()));

        // "d" doesn't exist, should position at "c"
        let cursor = Cursor::at_item(store.as_ref(), &tree, b"d").unwrap();
        assert!(cursor.is_valid());
        assert_eq!(cursor.get_key(), Some(b"c".as_slice()));
    }

    #[test]
    fn test_at_item_key_smaller_than_all() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
        tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();

        // "a" is smaller than all keys, should position at first key "b"
        let cursor = Cursor::at_item(store.as_ref(), &tree, b"a").unwrap();
        assert!(cursor.is_valid());
        assert_eq!(cursor.get_key(), Some(b"b".as_slice()));
    }

    #[test]
    fn test_at_item_key_larger_than_all() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();

        // "z" is larger than all keys, should position at last key "b"
        let cursor = Cursor::at_item(store.as_ref(), &tree, b"z").unwrap();
        assert!(cursor.is_valid());
        assert_eq!(cursor.get_key(), Some(b"b".as_slice()));
    }

    #[test]
    fn test_cursor_is_at_end() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();

        // Cursor at "a" is not at end (there's "b" after)
        let cursor = Cursor::at_item(store.as_ref(), &tree, b"a").unwrap();
        assert!(!cursor.is_at_end());

        // Cursor at "b" is at end (last key)
        let cursor = Cursor::at_item(store.as_ref(), &tree, b"b").unwrap();
        assert!(cursor.is_at_end());
    }

    #[test]
    fn test_cursor_clone_independence() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();

        let cursor1 = Cursor::at_item(store.as_ref(), &tree, b"a").unwrap();
        let cursor2 = cursor1.clone();

        // Both should point to the same key
        assert_eq!(cursor1.get_key(), cursor2.get_key());
        assert_eq!(cursor1.get_value(), cursor2.get_value());
    }

    #[test]
    fn test_advance_within_node() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
        tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();

        let mut cursor = Cursor::at_item(store.as_ref(), &tree, b"a").unwrap();
        assert_eq!(cursor.get_key(), Some(b"a".as_slice()));

        // Advance to "b"
        assert!(cursor.advance(store.as_ref()));
        assert_eq!(cursor.get_key(), Some(b"b".as_slice()));
        assert_eq!(cursor.get_value(), Some(b"2".as_slice()));

        // Advance to "c"
        assert!(cursor.advance(store.as_ref()));
        assert_eq!(cursor.get_key(), Some(b"c".as_slice()));
        assert_eq!(cursor.get_value(), Some(b"3".as_slice()));
    }

    #[test]
    fn test_advance_at_end_of_tree() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();

        // Start at last key
        let mut cursor = Cursor::at_item(store.as_ref(), &tree, b"b").unwrap();
        assert_eq!(cursor.get_key(), Some(b"b".as_slice()));

        // Advance should return false (no more entries)
        assert!(!cursor.advance(store.as_ref()));
        assert!(!cursor.is_valid());
    }

    #[test]
    fn test_advance_full_iteration() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        // Insert entries
        let entries = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ];

        for (k, v) in &entries {
            tree = prolly.put(&tree, k.clone(), v.clone()).unwrap();
        }

        // Start at first key
        let mut cursor = Cursor::at_item(store.as_ref(), &tree, b"a").unwrap();
        let mut collected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        // Collect all entries via cursor
        while cursor.is_valid() {
            let key = cursor.get_key().unwrap().to_vec();
            let val = cursor.get_value().unwrap().to_vec();
            collected.push((key, val));
            if !cursor.advance(store.as_ref()) {
                break;
            }
        }

        assert_eq!(collected, entries);
    }

    #[test]
    fn test_advance_single_entry() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly
            .put(&tree, b"only".to_vec(), b"one".to_vec())
            .unwrap();

        let mut cursor = Cursor::at_item(store.as_ref(), &tree, b"only").unwrap();
        assert!(cursor.is_valid());
        assert_eq!(cursor.get_key(), Some(b"only".as_slice()));

        // Advance should return false (only one entry)
        assert!(!cursor.advance(store.as_ref()));
        assert!(!cursor.is_valid());
    }

    #[test]
    fn test_advance_invalid_cursor() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let tree = prolly.create();

        // Empty tree cursor
        let mut cursor = Cursor::at_item(store.as_ref(), &tree, b"any").unwrap();
        assert!(!cursor.is_valid());

        // Advance on invalid cursor should return false
        assert!(!cursor.advance(store.as_ref()));
    }

    // CursorIterator tests

    #[test]
    fn test_cursor_iterator_full_iteration() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        // Insert entries
        let entries = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ];

        for (k, v) in &entries {
            tree = prolly.put(&tree, k.clone(), v.clone()).unwrap();
        }

        // Create cursor at beginning and iterate
        let cursor = Cursor::at_item(store.as_ref(), &tree, b"").unwrap();
        let iter = CursorIterator::new(cursor, store.as_ref(), None);
        let collected: Vec<(Vec<u8>, Vec<u8>)> = iter.collect();

        assert_eq!(collected, entries);
    }

    #[test]
    fn test_cursor_iterator_with_end_bound() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
        tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();
        tree = prolly.put(&tree, b"d".to_vec(), b"4".to_vec()).unwrap();

        // Iterate from "a" to "c" (exclusive)
        let cursor = Cursor::at_item(store.as_ref(), &tree, b"a").unwrap();
        let iter = CursorIterator::new(cursor, store.as_ref(), Some(b"c".to_vec()));
        let collected: Vec<(Vec<u8>, Vec<u8>)> = iter.collect();

        let expected = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ];
        assert_eq!(collected, expected);
    }

    #[test]
    fn test_cursor_iterator_empty_tree() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let tree = prolly.create();

        let cursor = Cursor::at_item(store.as_ref(), &tree, b"any").unwrap();
        let iter = CursorIterator::new(cursor, store.as_ref(), None);
        let collected: Vec<(Vec<u8>, Vec<u8>)> = iter.collect();

        assert!(collected.is_empty());
    }

    #[test]
    fn test_cursor_iterator_single_entry() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly
            .put(&tree, b"only".to_vec(), b"one".to_vec())
            .unwrap();

        let cursor = Cursor::at_item(store.as_ref(), &tree, b"").unwrap();
        let iter = CursorIterator::new(cursor, store.as_ref(), None);
        let collected: Vec<(Vec<u8>, Vec<u8>)> = iter.collect();

        assert_eq!(collected, vec![(b"only".to_vec(), b"one".to_vec())]);
    }

    #[test]
    fn test_cursor_iterator_end_bound_at_start() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
        tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();

        // End bound "a" is before all keys, should yield nothing
        let cursor = Cursor::at_item(store.as_ref(), &tree, b"").unwrap();
        let iter = CursorIterator::new(cursor, store.as_ref(), Some(b"a".to_vec()));
        let collected: Vec<(Vec<u8>, Vec<u8>)> = iter.collect();

        assert!(collected.is_empty());
    }

    #[test]
    fn test_cursor_iterator_start_from_middle() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
        tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();
        tree = prolly.put(&tree, b"d".to_vec(), b"4".to_vec()).unwrap();

        // Start from "b" and iterate to end
        let cursor = Cursor::at_item(store.as_ref(), &tree, b"b").unwrap();
        let iter = CursorIterator::new(cursor, store.as_ref(), None);
        let collected: Vec<(Vec<u8>, Vec<u8>)> = iter.collect();

        let expected = vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ];
        assert_eq!(collected, expected);
    }

    #[test]
    fn test_cursor_iterator_range_middle() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
        tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();
        tree = prolly.put(&tree, b"d".to_vec(), b"4".to_vec()).unwrap();

        // Range [b, d) - should yield b and c
        let cursor = Cursor::at_item(store.as_ref(), &tree, b"b").unwrap();
        let iter = CursorIterator::new(cursor, store.as_ref(), Some(b"d".to_vec()));
        let collected: Vec<(Vec<u8>, Vec<u8>)> = iter.collect();

        let expected = vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];
        assert_eq!(collected, expected);
    }

    // ============================================================
    // Edge Case Unit Tests (Task 9.1)
    // Requirements: 1.5, 3.3, 3.4
    // ============================================================

    /// Test: Empty tree cursor creation
    #[test]
    fn test_empty_tree_cursor_is_invalid() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let tree = prolly.create();

        // Create cursor on empty tree
        let cursor = Cursor::at_item(store.as_ref(), &tree, b"any_key").unwrap();

        // Cursor should be invalid
        assert!(!cursor.is_valid(), "Cursor on empty tree should be invalid");

        // get_key should return None for invalid cursor
        assert_eq!(
            cursor.get_key(),
            None,
            "get_key should return None for empty tree cursor"
        );

        // get_value should return None for invalid cursor
        assert_eq!(
            cursor.get_value(),
            None,
            "get_value should return None for empty tree cursor"
        );

        // is_at_end should handle empty node gracefully
        assert!(
            cursor.is_at_end(),
            "is_at_end should return true for empty tree cursor"
        );
    }

    /// Test: Single-entry tree navigation
    #[test]
    fn test_single_entry_tree_navigation() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        // Insert single entry
        tree = prolly
            .put(&tree, b"only_key".to_vec(), b"only_value".to_vec())
            .unwrap();

        // Create cursor at the key
        let mut cursor = Cursor::at_item(store.as_ref(), &tree, b"only_key").unwrap();

        // Cursor should be valid and at the correct position
        assert!(
            cursor.is_valid(),
            "Cursor should be valid for single-entry tree"
        );
        assert_eq!(
            cursor.get_key(),
            Some(b"only_key".as_slice()),
            "Cursor should be at the only key"
        );
        assert_eq!(
            cursor.get_value(),
            Some(b"only_value".as_slice()),
            "Cursor should return the only value"
        );

        // Single entry means cursor is at end of node
        assert!(
            cursor.is_at_end(),
            "Cursor should be at end for single-entry tree"
        );

        // Advance should return false (no more entries)
        assert!(
            !cursor.advance(store.as_ref()),
            "advance should return false for single-entry tree"
        );

        // After advance past end, cursor should be invalid
        assert!(
            !cursor.is_valid(),
            "Cursor should be invalid after advancing past end"
        );
        assert_eq!(
            cursor.get_key(),
            None,
            "get_key should return None after advancing past end"
        );
        assert_eq!(
            cursor.get_value(),
            None,
            "get_value should return None after advancing past end"
        );
    }

    /// Test: Cursor at internal node (get_value returns None)
    #[test]
    fn test_cursor_at_internal_node_get_value_returns_none() {
        use super::super::node::Node;

        // Create an internal node directly (not a leaf)
        let internal_node = Node::builder()
            .keys(vec![b"key1".to_vec(), b"key2".to_vec()])
            .vals(vec![vec![0u8; 32], vec![1u8; 32]]) // CID-like values (32 bytes)
            .leaf(false) // Internal node
            .level(1)
            .build();

        // Create a cursor pointing to this internal node
        let cursor = Cursor {
            index: 0,
            node: Arc::new(internal_node),
            parent: None,
        };

        // Cursor should be valid (node is not empty, index is in bounds)
        assert!(cursor.is_valid(), "Cursor at internal node should be valid");

        // get_key should return the key
        assert_eq!(
            cursor.get_key(),
            Some(b"key1".as_slice()),
            "get_key should return key for internal node"
        );

        // get_value should return None for internal node (not a leaf)
        assert_eq!(
            cursor.get_value(),
            None,
            "get_value should return None for internal node"
        );

        // Test at second index
        let cursor_at_second = Cursor {
            index: 1,
            node: cursor.node.clone(),
            parent: None,
        };

        assert!(
            cursor_at_second.is_valid(),
            "Cursor at second index should be valid"
        );
        assert_eq!(
            cursor_at_second.get_key(),
            Some(b"key2".as_slice()),
            "get_key should return second key"
        );
        assert_eq!(
            cursor_at_second.get_value(),
            None,
            "get_value should still return None for internal node"
        );
    }

    /// Test: Invalid cursor accessors return None
    #[test]
    fn test_invalid_cursor_accessors_return_none() {
        use super::super::node::Node;

        // Case 1: Cursor with empty node
        let empty_node = Node::new_leaf();
        let cursor_empty = Cursor {
            index: 0,
            node: Arc::new(empty_node),
            parent: None,
        };

        assert!(
            !cursor_empty.is_valid(),
            "Cursor with empty node should be invalid"
        );
        assert_eq!(
            cursor_empty.get_key(),
            None,
            "get_key should return None for cursor with empty node"
        );
        assert_eq!(
            cursor_empty.get_value(),
            None,
            "get_value should return None for cursor with empty node"
        );

        // Case 2: Cursor with index out of bounds
        let node_with_data = Node::builder()
            .keys(vec![b"a".to_vec(), b"b".to_vec()])
            .vals(vec![b"1".to_vec(), b"2".to_vec()])
            .leaf(true)
            .build();

        let cursor_out_of_bounds = Cursor {
            index: 5, // Out of bounds (only 2 entries)
            node: Arc::new(node_with_data.clone()),
            parent: None,
        };

        assert!(
            !cursor_out_of_bounds.is_valid(),
            "Cursor with out-of-bounds index should be invalid"
        );
        assert_eq!(
            cursor_out_of_bounds.get_key(),
            None,
            "get_key should return None for out-of-bounds cursor"
        );
        assert_eq!(
            cursor_out_of_bounds.get_value(),
            None,
            "get_value should return None for out-of-bounds cursor"
        );

        // Case 3: Cursor at exactly the boundary (index == len)
        let cursor_at_boundary = Cursor {
            index: 2, // Exactly at len (2 entries, so index 2 is out of bounds)
            node: Arc::new(node_with_data),
            parent: None,
        };

        assert!(
            !cursor_at_boundary.is_valid(),
            "Cursor at boundary should be invalid"
        );
        assert_eq!(
            cursor_at_boundary.get_key(),
            None,
            "get_key should return None for cursor at boundary"
        );
        assert_eq!(
            cursor_at_boundary.get_value(),
            None,
            "get_value should return None for cursor at boundary"
        );
    }

    /// Test: Advance on invalid cursor returns false
    #[test]
    fn test_advance_on_invalid_cursor_returns_false() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let tree = prolly.create();

        // Create cursor on empty tree (invalid cursor)
        let mut cursor = Cursor::at_item(store.as_ref(), &tree, b"any").unwrap();
        assert!(
            !cursor.is_valid(),
            "Cursor should be invalid for empty tree"
        );

        // Advance on invalid cursor should return false
        assert!(
            !cursor.advance(store.as_ref()),
            "advance on invalid cursor should return false"
        );

        // Cursor should still be invalid
        assert!(
            !cursor.is_valid(),
            "Cursor should remain invalid after advance"
        );
    }

    /// Test: Multiple advances past end keep cursor invalid
    #[test]
    fn test_multiple_advances_past_end() {
        let store = std::sync::Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut tree = prolly.create();

        tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();

        let mut cursor = Cursor::at_item(store.as_ref(), &tree, b"a").unwrap();
        assert!(cursor.is_valid());

        // First advance past end
        assert!(
            !cursor.advance(store.as_ref()),
            "First advance past end should return false"
        );
        assert!(
            !cursor.is_valid(),
            "Cursor should be invalid after first advance past end"
        );

        // Second advance on already invalid cursor
        assert!(
            !cursor.advance(store.as_ref()),
            "Second advance should also return false"
        );
        assert!(!cursor.is_valid(), "Cursor should remain invalid");

        // Third advance
        assert!(
            !cursor.advance(store.as_ref()),
            "Third advance should also return false"
        );
        assert!(!cursor.is_valid(), "Cursor should still be invalid");

        // Accessors should consistently return None
        assert_eq!(cursor.get_key(), None);
        assert_eq!(cursor.get_value(), None);
    }
}
