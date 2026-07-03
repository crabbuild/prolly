//! Streaming diff operations for Prolly trees
//!
//! This module provides lazy iteration over tree differences through the
//! [`StreamingDiffer`] trait. Unlike the collecting `diff()` method, streaming
//! diffs yield results one at a time without pre-collecting all differences,
//! making them memory-efficient for large trees.
//!
//! # Overview
//!
//! The streaming diff approach is beneficial when:
//! - Processing large trees where collecting all diffs would use too much memory
//! - Early termination is desired (e.g., finding the first N differences)
//! - Differences need to be processed incrementally
//!
//! # Example
//!
//! ```rust
//! use prolly::{Prolly, MemStore, Config, Diff};
//! use std::sync::Arc;
//!
//! let store = Arc::new(MemStore::new());
//! let prolly = Prolly::new(store.clone(), Config::default());
//!
//! let base = prolly.create();
//! let other = prolly.put(&base, b"key".to_vec(), b"val".to_vec()).unwrap();
//!
//! // Stream differences lazily
//! for diff_result in prolly.stream_diff(&base, &other).unwrap() {
//!     match diff_result {
//!         Ok(diff) => println!("{:?}", diff),
//!         Err(e) => eprintln!("Error: {}", e),
//!     }
//! }
//! ```

use super::cursor::DiffCursor;
use super::error::{Diff, Error};
use super::store::Store;
use super::tree::Tree;

/// Trait for streaming diff operations.
///
/// Implementations yield differences lazily without collecting all results upfront.
/// This is memory-efficient for large trees and supports early termination.
///
/// # Type Parameters
/// * `S` - The storage backend type implementing [`Store`]
///
/// # Example
///
/// ```rust
/// use prolly::{Prolly, MemStore, Config};
/// use std::sync::Arc;
///
/// let store = Arc::new(MemStore::new());
/// let prolly = Prolly::new(store.clone(), Config::default());
///
/// let base = prolly.create();
/// let other = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
///
/// // Use the streaming diff
/// let diffs: Vec<_> = prolly.stream_diff(&base, &other)
///     .unwrap()
///     .filter_map(|r| r.ok())
///     .collect();
/// ```
pub trait StreamingDiffer<S: Store> {
    /// Create a streaming diff iterator between two trees.
    ///
    /// # Arguments
    /// * `store` - The storage backend
    /// * `base` - The base tree to compare from
    /// * `other` - The other tree to compare to
    ///
    /// # Returns
    /// An iterator yielding `Result<Diff, Error>` entries in lexicographic key order.
    ///
    /// # Short-circuit
    /// If both trees have the same root CID, returns an empty iterator immediately.
    ///
    /// # Errors
    /// Returns an error if cursor initialization fails.
    fn stream_diff<'a>(
        &'a self,
        store: &'a S,
        base: &Tree,
        other: &Tree,
    ) -> Result<Box<dyn Iterator<Item = Result<Diff, Error>> + 'a>, Error>;
}

/// Default streaming differ using dual cursor traversal.
///
/// This implementation wraps the existing [`DiffCursor`] to provide
/// streaming diff functionality with proper error handling.
///
/// # Features
/// - Short-circuits for identical trees (same root CID)
/// - Yields differences in lexicographic key order
/// - Memory-efficient lazy iteration
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultStreamingDiffer;

impl DefaultStreamingDiffer {
    /// Create a new DefaultStreamingDiffer.
    pub fn new() -> Self {
        Self
    }
}

impl<S: Store> StreamingDiffer<S> for DefaultStreamingDiffer {
    fn stream_diff<'a>(
        &'a self,
        store: &'a S,
        base: &Tree,
        other: &Tree,
    ) -> Result<Box<dyn Iterator<Item = Result<Diff, Error>> + 'a>, Error> {
        // Short-circuit for identical trees (same root CID)
        if base.root == other.root {
            return Ok(Box::new(std::iter::empty()));
        }

        // Use existing DiffCursor implementation
        let diff_cursor = DiffCursor::new(store, base, other)?;

        // Wrap DiffCursor items in Ok() since DiffCursor yields Diff directly
        Ok(Box::new(diff_cursor.map(Ok)))
    }
}

#[cfg(test)]
mod tests {
    use super::super::config::Config;
    use super::super::store::MemStore;
    use super::super::Prolly;
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_streaming_diff_identical_trees() {
        let store = Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let tree = prolly
            .put(&prolly.create(), b"a".to_vec(), b"1".to_vec())
            .unwrap();

        let differ = DefaultStreamingDiffer::new();
        let diffs: Vec<_> = differ
            .stream_diff(store.as_ref(), &tree, &tree)
            .unwrap()
            .collect();

        assert!(diffs.is_empty());
    }

    #[test]
    fn test_streaming_diff_empty_base() {
        let store = Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let base = prolly.create();
        let other = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();

        let differ = DefaultStreamingDiffer::new();
        let diffs: Vec<_> = differ
            .stream_diff(store.as_ref(), &base, &other)
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(diffs.len(), 1);
        assert!(matches!(
            &diffs[0],
            Diff::Added { key, val } if key == b"a" && val == b"1"
        ));
    }

    #[test]
    fn test_streaming_diff_empty_other() {
        let store = Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let base = prolly
            .put(&prolly.create(), b"a".to_vec(), b"1".to_vec())
            .unwrap();
        let other = prolly.create();

        let differ = DefaultStreamingDiffer::new();
        let diffs: Vec<_> = differ
            .stream_diff(store.as_ref(), &base, &other)
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(diffs.len(), 1);
        assert!(matches!(
            &diffs[0],
            Diff::Removed { key, val } if key == b"a" && val == b"1"
        ));
    }

    #[test]
    fn test_streaming_diff_changed_value() {
        let store = Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let base = prolly
            .put(&prolly.create(), b"a".to_vec(), b"1".to_vec())
            .unwrap();
        let other = prolly.put(&base, b"a".to_vec(), b"2".to_vec()).unwrap();

        let differ = DefaultStreamingDiffer::new();
        let diffs: Vec<_> = differ
            .stream_diff(store.as_ref(), &base, &other)
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(diffs.len(), 1);
        assert!(matches!(
            &diffs[0],
            Diff::Changed { key, old, new } if key == b"a" && old == b"1" && new == b"2"
        ));
    }

    #[test]
    fn test_streaming_diff_lexicographic_order() {
        let store = Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let base = prolly.create();
        let mut other = base.clone();

        // Add keys in non-sorted order
        other = prolly.put(&other, b"c".to_vec(), b"3".to_vec()).unwrap();
        other = prolly.put(&other, b"a".to_vec(), b"1".to_vec()).unwrap();
        other = prolly.put(&other, b"b".to_vec(), b"2".to_vec()).unwrap();

        let differ = DefaultStreamingDiffer::new();
        let diffs: Vec<_> = differ
            .stream_diff(store.as_ref(), &base, &other)
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert_eq!(diffs.len(), 3);

        // Verify lexicographic order
        let keys: Vec<_> = diffs
            .iter()
            .map(|d| match d {
                Diff::Added { key, .. } => key.clone(),
                Diff::Removed { key, .. } => key.clone(),
                Diff::Changed { key, .. } => key.clone(),
            })
            .collect();

        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn test_streaming_diff_both_empty() {
        let store = Arc::new(MemStore::new());
        let prolly = Prolly::new(store.clone(), Config::default());
        let base = prolly.create();
        let other = prolly.create();

        let differ = DefaultStreamingDiffer::new();
        let diffs: Vec<_> = differ
            .stream_diff(store.as_ref(), &base, &other)
            .unwrap()
            .collect();

        assert!(diffs.is_empty());
    }
}
