//! Diff and merge operations for Prolly trees
//!
//! This module handles computing differences between trees and performing
//! three-way merges with conflict resolution. These operations enable
//! version control semantics for Prolly trees.
//!
//! # Overview
//!
//! Prolly trees are well-suited for version control because their content-addressed
//! structure enables efficient comparison. Two trees with the same root CID are
//! identical, and trees with different roots can be compared by examining their
//! differences.
//!
//! # Diff Operations
//!
//! The [`compute_diff`] function compares two trees and returns a list of differences:
//!
//! - **Added**: Keys that exist in the `other` tree but not in `base`
//! - **Removed**: Keys that exist in `base` but not in `other`
//! - **Changed**: Keys that exist in both trees but with different values
//!
//! ## Short-Circuit Optimization
//!
//! If both trees have the same root CID, the diff returns immediately with an
//! empty result. This makes comparing identical trees O(1).
//!
//! ## Example
//!
//! ```rust
//! use prolly::{Prolly, MemStore, Config, Diff};
//!
//! let store = MemStore::new();
//! let prolly = Prolly::new(store, Config::default());
//!
//! let base = prolly.create();
//! let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
//!
//! let modified = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
//!
//! let diffs = prolly.diff(&base, &modified).unwrap();
//! // diffs contains: Added { key: "b", val: "2" }
//! ```
//!
//! # Merge Operations
//!
//! The [`merge_trees`] function performs a three-way merge using a common ancestor (base).
//! This is the standard merge algorithm used in version control systems.
//!
//! ## Three-Way Merge Algorithm
//!
//! 1. Return immediately when one branch is unchanged or both branches match
//! 2. Compute the diff from `base` to `right`
//! 3. Batch-lookup the right-changed keys in `left`
//! 4. Apply changes from `right` that don't conflict with `left`
//! 5. Detect and handle conflicts
//!
//! ## Conflict Detection
//!
//! A conflict occurs when both branches modify the same key differently from the base:
//!
//! - Both branches change the same key to different values
//! - One branch changes a key while the other deletes it
//! - Both branches add the same key with different values
//!
//! ## Conflict Resolution
//!
//! When a conflict is detected:
//!
//! 1. If a resolver function is provided, it's called with conflict details
//! 2. If the resolver returns `Resolution::Value`, that value is used
//! 3. If the resolver returns `Resolution::Delete`, the key is removed
//! 4. If the resolver returns `Resolution::Unresolved` or no resolver is provided,
//!    an error is returned
//!
//! ## Example
//!
//! ```rust
//! use prolly::{Prolly, MemStore, Config, Resolution, Resolver};
//!
//! let store = MemStore::new();
//! let prolly = Prolly::new(store, Config::default());
//!
//! // Create base and divergent branches
//! let base = prolly.create();
//! let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
//!
//! let left = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
//! let right = prolly.put(&base, b"c".to_vec(), b"3".to_vec()).unwrap();
//!
//! // Merge without conflicts
//! let merged = prolly.merge(&base, &left, &right, None).unwrap();
//!
//! // With conflict resolution
//! let resolver: Resolver = Box::new(|conflict| {
//!     conflict
//!         .left
//!         .clone()
//!         .map_or_else(Resolution::delete, Resolution::value)
//! });
//! let merged = prolly.merge(&base, &left, &right, Some(resolver)).unwrap();
//! ```
//!
//! # Performance Considerations
//!
//! - **Same root**: O(1) - immediate return for identical trees
//! - **Different roots**: O(changed subtrees) when chunk boundaries align, with
//!   a local full-scan fallback when boundaries diverge

#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::{HashSet, VecDeque};

#[cfg(feature = "async-store")]
use futures_util::stream::{self, Stream};
use serde::{Deserialize, Serialize};

use super::batch::{get_max_key, BatchWriteCollector};
use super::cid::Cid;
use super::error::{Conflict, Diff, Error, Mutation, Resolution, Resolver};
use super::node::Node;
use super::range::RangeCursor;
#[cfg(feature = "async-store")]
use super::store::AsyncStore;
use super::store::Store;
use super::tree::Tree;

#[cfg(feature = "async-store")]
use super::AsyncProlly;
use super::Prolly;

type ChildSpanCid<'a> = (Option<&'a [u8]>, Cid);
const DIFF_COLLECTION_PREFETCH_PARALLELISM: usize = 16;
const DIFF_FRAME_PREFETCH_PARALLELISM: usize = 16;
const MERGE_FRONTIER_PREFETCH_PARALLELISM: usize = 16;

/// A bounded page of diff results.
///
/// `next_cursor` is `Some` when another call should resume after the last diff
/// entry in this page. It is `None` when the scan reached the end bound or the
/// end of both trees.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiffPage {
    pub diffs: Vec<Diff>,
    pub next_cursor: Option<RangeCursor>,
}

/// A bounded page from the structural diff iterator.
///
/// Unlike [`DiffPage`], this page resumes from a structural checkpoint instead
/// of recomputing from a key cursor. It is useful for long-running sync,
/// indexing, or replication jobs that want to preserve CID-based subtree
/// pruning across checkpoints.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuralDiffPage {
    /// Diff entries emitted by this page.
    pub diffs: Vec<Diff>,
    /// Checkpoint for the next page, or `None` when the traversal is complete.
    pub next_cursor: Option<StructuralDiffCursor>,
    /// Traversal counters collected while producing this page.
    pub stats: DiffTraversalStats,
}

/// Stable checkpoint token for structural diff traversal.
///
/// The cursor records the tree roots it belongs to, the unvisited structural
/// frontier, and any already-expanded diff entries that have not been emitted
/// yet. Reusing it with different roots returns an error.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuralDiffCursor {
    /// Base tree root CID for cursor validation.
    pub base_root: Option<Cid>,
    /// Other tree root CID for cursor validation.
    pub other_root: Option<Cid>,
    /// Remaining structural frames in iterator stack order.
    pub markers: Vec<StructuralDiffMarker>,
    /// Already-expanded diffs that still need to be emitted.
    pub pending: Vec<Diff>,
}

impl StructuralDiffCursor {
    /// Create a cursor positioned at the beginning of the structural diff.
    pub fn start(base: &Tree, other: &Tree) -> Self {
        Self {
            base_root: base.root.clone(),
            other_root: other.root.clone(),
            markers: initial_diff_stack(base, other)
                .iter()
                .map(DiffFrame::to_marker)
                .collect(),
            pending: Vec::new(),
        }
    }

    /// Return whether this cursor has no more work to resume.
    pub fn is_empty(&self) -> bool {
        self.markers.is_empty() && self.pending.is_empty()
    }

    fn matches_trees(&self, base: &Tree, other: &Tree) -> bool {
        self.base_root == base.root && self.other_root == other.root
    }
}

/// Public marker for one suspended structural diff frame.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StructuralDiffMarker {
    /// Compare two subtrees with the same structural span.
    Compare {
        base_cid: Cid,
        other_cid: Cid,
        span_end: Option<Vec<u8>>,
    },
    /// Emit all entries from a subtree added on the `other` side.
    Added { cid: Cid },
    /// Emit all entries from a subtree removed from the `base` side.
    Removed { cid: Cid },
}

/// Result and trace from [`Prolly::merge_explain`](crate::Prolly::merge_explain)
/// and its async counterpart when the `async-store` feature is enabled.
///
/// The merge result is kept inside this struct so callers can inspect
/// [`MergeTrace`] even when the merge fails with [`Error::Conflict`] or a store
/// error.
#[derive(Debug)]
pub struct MergeExplanation {
    /// The merge result.
    pub result: Result<Tree, Error>,
    /// Structured trace events collected while planning and applying the merge.
    pub trace: MergeTrace,
}

impl MergeExplanation {
    /// Consume the explanation and return only the merge result.
    pub fn into_result(self) -> Result<Tree, Error> {
        self.result
    }
}

/// Structured diagnostic events for a merge operation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeTrace {
    /// Ordered events produced by the merge.
    pub events: Vec<MergeTraceEvent>,
}

impl MergeTrace {
    fn push(&mut self, event: MergeTraceEvent) {
        self.events.push(event);
    }
}

/// A single event emitted while explaining a merge.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeTraceEvent {
    /// The merge returned from a root-level fast path.
    FastPath { reason: MergeFastPath },
    /// The structural merge fast path started.
    StructuralMergeStarted,
    /// An existing subtree CID was reused without rewriting that subtree.
    ReusedSubtree { cid: Cid, reason: MergeReuseReason },
    /// A new node was written by the structural merge path.
    RewrittenNode {
        cid: Cid,
        level: u8,
        entries: usize,
        first_key: Option<Vec<u8>>,
        last_key: Option<Vec<u8>>,
    },
    /// The caller-provided resolver was invoked for a conflict.
    ResolverCalled {
        stage: MergeTraceStage,
        key: Vec<u8>,
        resolution: MergeResolutionKind,
    },
    /// A fast path could not finish and merge fell back to a broader path.
    Fallback { reason: MergeFallbackReason },
    /// Counters from the structural diff traversal used by the diff/batch path.
    DiffTraversal { stats: DiffTraversalStats },
    /// The diff/batch merge path applied right-side changes to left.
    BatchMerge {
        right_changes: usize,
        mutations: usize,
        append_only: bool,
    },
}

/// Root-level fast path used by a merge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeFastPath {
    /// `left` and `right` already reference the same root.
    BranchesEqual,
    /// `left` is unchanged from `base`, so `right` is the merge result.
    LeftUnchanged,
    /// `right` is unchanged from `base`, so `left` is the merge result.
    RightUnchanged,
}

/// Why a subtree CID could be reused by the structural merge path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeReuseReason {
    /// Left and right child CIDs already matched.
    BranchesEqual,
    /// Left matched base, so the right child CID was reused.
    LeftUnchanged,
    /// Right matched base, so the left child CID was reused.
    RightUnchanged,
    /// Merging produced the original base values.
    UnchangedAfterMerge,
    /// Merging produced the existing left node values.
    MatchesLeft,
    /// Merging produced the existing right node values.
    MatchesRight,
}

/// Where in the merge algorithm a trace event occurred.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeTraceStage {
    /// The structural merge fast path.
    Structural,
    /// The diff/batch merge path.
    Batch,
}

/// Compact form of a resolver result for trace events.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeResolutionKind {
    /// Resolver returned a value.
    Value,
    /// Resolver chose to delete the key.
    Delete,
    /// Resolver left the conflict unresolved.
    Unresolved,
}

impl From<&Resolution> for MergeResolutionKind {
    fn from(resolution: &Resolution) -> Self {
        match resolution {
            Resolution::Value(_) => Self::Value,
            Resolution::Delete => Self::Delete,
            Resolution::Unresolved => Self::Unresolved,
        }
    }
}

/// Why merge left a fast path and continued through a broader path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeFallbackReason {
    /// At least one side had an empty root, so structural node comparison could
    /// not run.
    MissingRoot,
    /// Structural nodes did not have compatible shape, level, or key layout.
    ShapeMismatch,
    /// Same-layout internal nodes had different child counts.
    NodeLengthMismatch,
    /// A child subtree could not be structurally merged.
    ChildFallback,
    /// A structural conflict resolved to delete, which requires the batch path
    /// to keep rebalancing correct.
    DeleteResolution,
    /// Merge continued through the diff/batch path.
    DiffBatch,
}

/// Counters collected while traversing two trees for diff.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffTraversalStats {
    /// Number of non-identical node pairs loaded and compared.
    pub compared_nodes: usize,
    /// Number of equal subtree CIDs skipped without loading the subtree.
    pub reused_subtrees: usize,
    /// Number of added subtree frames expanded.
    pub added_subtrees: usize,
    /// Number of removed subtree frames expanded.
    pub removed_subtrees: usize,
    /// Number of times diff fell back to collecting and comparing a whole
    /// subtree because node shapes or child spans did not align.
    pub collected_fallbacks: usize,
    /// Number of diff entries yielded by the traversal.
    pub emitted_diffs: usize,
}

struct MergeTraceRecorder<'a> {
    trace: Option<&'a mut MergeTrace>,
}

impl<'a> MergeTraceRecorder<'a> {
    fn disabled() -> Self {
        Self { trace: None }
    }

    fn new(trace: &'a mut MergeTrace) -> Self {
        Self { trace: Some(trace) }
    }

    fn record(&mut self, event: MergeTraceEvent) {
        if let Some(trace) = self.trace.as_deref_mut() {
            trace.push(event);
        }
    }

    fn record_reuse(&mut self, cid: &Cid, reason: MergeReuseReason) {
        self.record(MergeTraceEvent::ReusedSubtree {
            cid: cid.clone(),
            reason,
        });
    }

    fn record_rewrite(&mut self, cid: &Cid, node: &Node) {
        self.record(MergeTraceEvent::RewrittenNode {
            cid: cid.clone(),
            level: node.level,
            entries: node.len(),
            first_key: node.keys.first().cloned(),
            last_key: node.keys.last().cloned(),
        });
    }

    fn record_resolver(
        &mut self,
        stage: MergeTraceStage,
        conflict: &Conflict,
        resolution: &Resolution,
    ) {
        self.record(MergeTraceEvent::ResolverCalled {
            stage,
            key: conflict.key.clone(),
            resolution: MergeResolutionKind::from(resolution),
        });
    }

    fn record_fallback(&mut self, reason: MergeFallbackReason) {
        self.record(MergeTraceEvent::Fallback { reason });
    }
}

#[derive(Clone, Copy)]
struct MergeChangeRef<'a> {
    key: &'a [u8],
    base: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DiffFrame {
    Compare {
        base_cid: Cid,
        other_cid: Cid,
        span_end: Option<Vec<u8>>,
    },
    Added {
        cid: Cid,
    },
    Removed {
        cid: Cid,
    },
}

impl DiffFrame {
    fn to_marker(&self) -> StructuralDiffMarker {
        match self {
            Self::Compare {
                base_cid,
                other_cid,
                span_end,
            } => StructuralDiffMarker::Compare {
                base_cid: base_cid.clone(),
                other_cid: other_cid.clone(),
                span_end: span_end.clone(),
            },
            Self::Added { cid } => StructuralDiffMarker::Added { cid: cid.clone() },
            Self::Removed { cid } => StructuralDiffMarker::Removed { cid: cid.clone() },
        }
    }

    fn from_marker(marker: &StructuralDiffMarker) -> Self {
        match marker {
            StructuralDiffMarker::Compare {
                base_cid,
                other_cid,
                span_end,
            } => Self::Compare {
                base_cid: base_cid.clone(),
                other_cid: other_cid.clone(),
                span_end: span_end.clone(),
            },
            StructuralDiffMarker::Added { cid } => Self::Added { cid: cid.clone() },
            StructuralDiffMarker::Removed { cid } => Self::Removed { cid: cid.clone() },
        }
    }
}

#[derive(Clone, Copy)]
enum DiffFrameKind {
    Added,
    Removed,
}

fn initial_diff_stack(base: &Tree, other: &Tree) -> Vec<DiffFrame> {
    match (&base.root, &other.root) {
        (Some(base_cid), Some(other_cid)) if base_cid != other_cid => {
            vec![DiffFrame::Compare {
                base_cid: base_cid.clone(),
                other_cid: other_cid.clone(),
                span_end: None,
            }]
        }
        (Some(base_cid), None) => vec![DiffFrame::Removed {
            cid: base_cid.clone(),
        }],
        (None, Some(other_cid)) => vec![DiffFrame::Added {
            cid: other_cid.clone(),
        }],
        _ => Vec::new(),
    }
}

impl DiffFrameKind {
    fn frame(self, cid: Cid) -> DiffFrame {
        match self {
            Self::Added => DiffFrame::Added { cid },
            Self::Removed => DiffFrame::Removed { cid },
        }
    }
}

/// Iterator over tree differences that preserves the subtree-pruning behavior
/// of eager diff without collecting the whole result upfront.
pub(crate) struct StructuralDiffIter<'a, S: Store> {
    prolly: &'a Prolly<S>,
    base_root: Option<Cid>,
    other_root: Option<Cid>,
    stack: Vec<DiffFrame>,
    pending: VecDeque<Diff>,
    failed: bool,
    stats: DiffTraversalStats,
}

impl<'a, S: Store> StructuralDiffIter<'a, S> {
    fn new(prolly: &'a Prolly<S>, base: &Tree, other: &Tree) -> Self {
        Self {
            prolly,
            base_root: base.root.clone(),
            other_root: other.root.clone(),
            stack: initial_diff_stack(base, other),
            pending: VecDeque::new(),
            failed: false,
            stats: DiffTraversalStats::default(),
        }
    }

    fn from_cursor(
        prolly: &'a Prolly<S>,
        base: &Tree,
        other: &Tree,
        cursor: &StructuralDiffCursor,
    ) -> Result<Self, Error> {
        if !cursor.matches_trees(base, other) {
            return Err(Error::InvalidNode);
        }

        Ok(Self {
            prolly,
            base_root: base.root.clone(),
            other_root: other.root.clone(),
            stack: cursor.markers.iter().map(DiffFrame::from_marker).collect(),
            pending: cursor.pending.iter().cloned().collect(),
            failed: false,
            stats: DiffTraversalStats::default(),
        })
    }

    fn checkpoint(&self) -> Option<StructuralDiffCursor> {
        if self.stack.is_empty() && self.pending.is_empty() {
            return None;
        }

        Some(StructuralDiffCursor {
            base_root: self.base_root.clone(),
            other_root: self.other_root.clone(),
            markers: self.stack.iter().map(DiffFrame::to_marker).collect(),
            pending: self.pending.iter().cloned().collect(),
        })
    }

    fn fill_pending(&mut self) -> Result<(), Error> {
        while self.pending.is_empty() {
            let Some(frame) = self.stack.pop() else {
                return Ok(());
            };

            match frame {
                DiffFrame::Compare {
                    base_cid,
                    other_cid,
                    span_end,
                } => self.process_compare(base_cid, other_cid, span_end.as_deref())?,
                DiffFrame::Added { cid } => self.process_added(cid)?,
                DiffFrame::Removed { cid } => self.process_removed(cid)?,
            }
        }

        Ok(())
    }

    fn process_compare(
        &mut self,
        base_cid: Cid,
        other_cid: Cid,
        span_end: Option<&[u8]>,
    ) -> Result<(), Error> {
        if base_cid == other_cid {
            self.stats.reused_subtrees += 1;
            return Ok(());
        }

        self.stats.compared_nodes += 1;
        let nodes = self
            .prolly
            .load_many_ordered(&[base_cid.clone(), other_cid.clone()])?;
        let base = nodes[0].clone();
        let other = nodes[1].clone();

        match (base.leaf, other.leaf) {
            (true, true) => {
                let mut diffs = Vec::new();
                diff_leaf_nodes(&base, &other, &mut diffs)?;
                self.pending.extend(diffs);
            }
            (false, false) if base.level == other.level => {
                self.enqueue_internal_diff(&base, &other, span_end)?;
            }
            _ => {
                self.stats.collected_fallbacks += 1;
                let mut diffs = Vec::new();
                diff_collected_nodes(self.prolly, &base, &other, &mut diffs)?;
                self.pending.extend(diffs);
            }
        }

        Ok(())
    }

    fn enqueue_internal_diff(
        &mut self,
        base: &Node,
        other: &Node,
        span_end: Option<&[u8]>,
    ) -> Result<(), Error> {
        ensure_node_value_count(base)?;
        ensure_node_value_count(other)?;
        let mut frames = Vec::with_capacity(base.len().max(other.len()));
        let mut base_idx = 0;
        let mut other_idx = 0;

        while base_idx < base.len() && other_idx < other.len() {
            let base_start = base.keys[base_idx].as_slice();
            let other_start = other.keys[other_idx].as_slice();
            let base_end = child_span_end(base, base_idx, span_end);
            let other_end = child_span_end(other, other_idx, span_end);

            if base_start == other_start && base_end == other_end {
                let base_cid = child_cid_validated(base, base_idx)?;
                let other_cid = child_cid_validated(other, other_idx)?;
                if base_cid != other_cid {
                    frames.push(DiffFrame::Compare {
                        base_cid,
                        other_cid,
                        span_end: base_end.map(<[u8]>::to_vec),
                    });
                } else {
                    self.stats.reused_subtrees += 1;
                }
                base_idx += 1;
                other_idx += 1;
            } else if span_ends_before_or_at(base_end, other_start) {
                frames.push(DiffFrame::Removed {
                    cid: child_cid_validated(base, base_idx)?,
                });
                base_idx += 1;
            } else if span_ends_before_or_at(other_end, base_start) {
                frames.push(DiffFrame::Added {
                    cid: child_cid_validated(other, other_idx)?,
                });
                other_idx += 1;
            } else {
                self.stats.collected_fallbacks += 1;
                let mut diffs = Vec::new();
                diff_collected_nodes(self.prolly, base, other, &mut diffs)?;
                self.pending.extend(diffs);
                return Ok(());
            }
        }

        while base_idx < base.len() {
            frames.push(DiffFrame::Removed {
                cid: child_cid_validated(base, base_idx)?,
            });
            base_idx += 1;
        }

        while other_idx < other.len() {
            frames.push(DiffFrame::Added {
                cid: child_cid_validated(other, other_idx)?,
            });
            other_idx += 1;
        }

        self.prefetch_frame_roots(&frames)?;
        self.stack.extend(frames.into_iter().rev());
        Ok(())
    }

    fn process_added(&mut self, cid: Cid) -> Result<(), Error> {
        self.stats.added_subtrees += 1;
        let node = self.prolly.load_arc(&cid)?;
        if node.leaf {
            ensure_node_value_count(&node)?;
            for idx in 0..node.len() {
                self.pending.push_back(Diff::Added {
                    key: node.keys[idx].clone(),
                    val: node_value(&node, idx)?.clone(),
                });
            }
        } else {
            let mut frames = child_diff_frames(&node, DiffFrameKind::Added)?;
            self.prefetch_frame_roots(&frames)?;
            frames.reverse();
            self.stack.extend(frames);
        }

        Ok(())
    }

    fn process_removed(&mut self, cid: Cid) -> Result<(), Error> {
        self.stats.removed_subtrees += 1;
        let node = self.prolly.load_arc(&cid)?;
        if node.leaf {
            ensure_node_value_count(&node)?;
            for idx in 0..node.len() {
                self.pending.push_back(Diff::Removed {
                    key: node.keys[idx].clone(),
                    val: node_value(&node, idx)?.clone(),
                });
            }
        } else {
            let mut frames = child_diff_frames(&node, DiffFrameKind::Removed)?;
            self.prefetch_frame_roots(&frames)?;
            frames.reverse();
            self.stack.extend(frames);
        }

        Ok(())
    }

    fn prefetch_frame_roots(&self, frames: &[DiffFrame]) -> Result<(), Error> {
        if frames.len() <= 1 || !self.prolly.store().prefers_batch_reads() {
            return Ok(());
        }

        let mut seen = HashSet::with_capacity(frames.len().saturating_mul(2));
        let mut cids = Vec::with_capacity(frames.len().saturating_mul(2));
        for frame in frames {
            match frame {
                DiffFrame::Compare {
                    base_cid,
                    other_cid,
                    ..
                } => {
                    if seen.insert(base_cid.clone()) {
                        cids.push(base_cid.clone());
                    }
                    if seen.insert(other_cid.clone()) {
                        cids.push(other_cid.clone());
                    }
                }
                DiffFrame::Added { cid } | DiffFrame::Removed { cid } => {
                    if seen.insert(cid.clone()) {
                        cids.push(cid.clone());
                    }
                }
            }
        }

        if !cids.is_empty() {
            let _ = self
                .prolly
                .load_many_ordered_with_parallelism(&cids, DIFF_FRAME_PREFETCH_PARALLELISM)?;
        }

        Ok(())
    }
}

impl<S: Store> Iterator for StructuralDiffIter<'_, S> {
    type Item = Result<Diff, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }

        if let Err(err) = self.fill_pending() {
            self.failed = true;
            self.stack.clear();
            self.pending.clear();
            return Some(Err(err));
        }

        let diff = self.pending.pop_front();
        if diff.is_some() {
            self.stats.emitted_diffs += 1;
        }
        diff.map(Ok)
    }
}

pub(crate) struct ConflictIter<'a, S: Store> {
    prolly: &'a Prolly<S>,
    left: &'a Tree,
    diffs: StructuralDiffIter<'a, S>,
    failed: bool,
}

impl<'a, S: Store> ConflictIter<'a, S> {
    fn new(prolly: &'a Prolly<S>, base: &Tree, left: &'a Tree, right: &Tree) -> Self {
        Self {
            prolly,
            left,
            diffs: StructuralDiffIter::new(prolly, base, right),
            failed: false,
        }
    }
}

impl<S: Store> Iterator for ConflictIter<'_, S> {
    type Item = Result<Conflict, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }

        for diff in self.diffs.by_ref() {
            let diff = match diff {
                Ok(diff) => diff,
                Err(err) => {
                    self.failed = true;
                    return Some(Err(err));
                }
            };

            match conflict_from_right_diff(self.prolly, self.left, &diff) {
                Ok(Some(conflict)) => return Some(Ok(conflict)),
                Ok(None) => continue,
                Err(err) => {
                    self.failed = true;
                    return Some(Err(err));
                }
            }
        }

        None
    }
}

pub(crate) fn stream_diff<'a, S: Store>(
    prolly: &'a Prolly<S>,
    base: &Tree,
    other: &Tree,
) -> StructuralDiffIter<'a, S> {
    StructuralDiffIter::new(prolly, base, other)
}

pub(crate) fn structural_diff_page<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    other: &Tree,
    cursor: Option<&StructuralDiffCursor>,
    limit: usize,
) -> Result<StructuralDiffPage, Error> {
    if let Some(cursor) = cursor {
        if !cursor.matches_trees(base, other) {
            return Err(Error::InvalidNode);
        }
    }

    if limit == 0 {
        return Ok(StructuralDiffPage {
            diffs: Vec::new(),
            next_cursor: Some(
                cursor
                    .cloned()
                    .unwrap_or_else(|| StructuralDiffCursor::start(base, other)),
            ),
            stats: DiffTraversalStats::default(),
        });
    }

    let mut iter = match cursor {
        Some(cursor) => StructuralDiffIter::from_cursor(prolly, base, other, cursor)?,
        None => StructuralDiffIter::new(prolly, base, other),
    };
    let mut diffs = Vec::with_capacity(limit);

    for _ in 0..limit {
        let Some(diff) = iter.next() else {
            break;
        };
        diffs.push(diff?);
    }

    Ok(StructuralDiffPage {
        diffs,
        next_cursor: iter.checkpoint(),
        stats: iter.stats,
    })
}

pub(crate) fn stream_conflicts<'a, S: Store>(
    prolly: &'a Prolly<S>,
    base: &Tree,
    left: &'a Tree,
    right: &Tree,
) -> ConflictIter<'a, S> {
    ConflictIter::new(prolly, base, left, right)
}

/// Async iterator over merge conflicts.
///
/// Created by [`AsyncProlly::stream_conflicts`](crate::AsyncProlly::stream_conflicts).
/// Each call to [`AsyncConflictIter::next`] advances the async structural diff
/// just far enough to find the next conflicting key.
#[cfg(feature = "async-store")]
pub struct AsyncConflictIter<'a, S: AsyncStore> {
    prolly: &'a AsyncProlly<S>,
    left: &'a Tree,
    diffs: AsyncDiffIter<'a, S>,
    failed: bool,
}

#[cfg(feature = "async-store")]
impl<'a, S> AsyncConflictIter<'a, S>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    pub(crate) fn new(
        prolly: &'a AsyncProlly<S>,
        base: &Tree,
        left: &'a Tree,
        right: &Tree,
    ) -> Self {
        Self {
            prolly,
            left,
            diffs: AsyncDiffIter::new(prolly, base, right),
            failed: false,
        }
    }

    /// Return the next conflicting key, or `None` when all right-side changes
    /// have been inspected.
    pub async fn next(&mut self) -> Option<Result<Conflict, Error>> {
        if self.failed {
            return None;
        }

        while let Some(diff) = self.diffs.next().await {
            let diff = match diff {
                Ok(diff) => diff,
                Err(err) => {
                    self.failed = true;
                    return Some(Err(err));
                }
            };

            match conflict_from_right_diff_async(self.prolly, self.left, &diff).await {
                Ok(Some(conflict)) => return Some(Ok(conflict)),
                Ok(None) => continue,
                Err(err) => {
                    self.failed = true;
                    return Some(Err(err));
                }
            }
        }

        None
    }

    /// Collect all remaining conflicts into memory.
    pub async fn collect(mut self) -> Result<Vec<Conflict>, Error> {
        let mut conflicts = Vec::new();
        while let Some(conflict) = self.next().await {
            conflicts.push(conflict?);
        }
        Ok(conflicts)
    }

    /// Convert this iterator into a `futures_util::Stream`.
    pub fn into_stream(self) -> impl Stream<Item = Result<Conflict, Error>> + 'a {
        stream::unfold(self, |mut iter| async move {
            iter.next().await.map(|item| (item, iter))
        })
    }
}

/// Async iterator over tree differences.
///
/// Created by [`AsyncProlly::stream_diff`](crate::AsyncProlly::stream_diff).
/// Each call to [`AsyncDiffIter::next`] advances the structural diff just far
/// enough to yield the next change, preserving CID-based subtree pruning while
/// avoiding allocation of the full diff result.
#[cfg(feature = "async-store")]
pub struct AsyncDiffIter<'a, S: AsyncStore> {
    prolly: &'a AsyncProlly<S>,
    base_root: Option<Cid>,
    other_root: Option<Cid>,
    stack: Vec<DiffFrame>,
    pending: VecDeque<Diff>,
    failed: bool,
    stats: DiffTraversalStats,
}

#[cfg(feature = "async-store")]
impl<'a, S> AsyncDiffIter<'a, S>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    pub(crate) fn new(prolly: &'a AsyncProlly<S>, base: &Tree, other: &Tree) -> Self {
        Self {
            prolly,
            base_root: base.root.clone(),
            other_root: other.root.clone(),
            stack: initial_diff_stack(base, other),
            pending: VecDeque::new(),
            failed: false,
            stats: DiffTraversalStats::default(),
        }
    }

    pub(crate) fn from_cursor(
        prolly: &'a AsyncProlly<S>,
        base: &Tree,
        other: &Tree,
        cursor: &StructuralDiffCursor,
    ) -> Result<Self, Error> {
        if !cursor.matches_trees(base, other) {
            return Err(Error::InvalidNode);
        }

        Ok(Self {
            prolly,
            base_root: base.root.clone(),
            other_root: other.root.clone(),
            stack: cursor.markers.iter().map(DiffFrame::from_marker).collect(),
            pending: cursor.pending.iter().cloned().collect(),
            failed: false,
            stats: DiffTraversalStats::default(),
        })
    }

    fn checkpoint(&self) -> Option<StructuralDiffCursor> {
        if self.stack.is_empty() && self.pending.is_empty() {
            return None;
        }

        Some(StructuralDiffCursor {
            base_root: self.base_root.clone(),
            other_root: self.other_root.clone(),
            markers: self.stack.iter().map(DiffFrame::to_marker).collect(),
            pending: self.pending.iter().cloned().collect(),
        })
    }

    /// Return the next diff entry in lexicographic key order.
    pub async fn next(&mut self) -> Option<Result<Diff, Error>> {
        if self.failed {
            return None;
        }

        if let Err(err) = self.fill_pending().await {
            self.failed = true;
            self.stack.clear();
            self.pending.clear();
            return Some(Err(err));
        }

        let diff = self.pending.pop_front();
        if diff.is_some() {
            self.stats.emitted_diffs += 1;
        }
        diff.map(Ok)
    }

    /// Collect all remaining diff entries into memory.
    pub async fn collect(mut self) -> Result<Vec<Diff>, Error> {
        let mut diffs = Vec::new();
        while let Some(diff) = self.next().await {
            diffs.push(diff?);
        }
        Ok(diffs)
    }

    /// Convert this iterator into a `futures_util::Stream`.
    pub fn into_stream(self) -> impl Stream<Item = Result<Diff, Error>> + 'a {
        stream::unfold(self, |mut iter| async move {
            iter.next().await.map(|item| (item, iter))
        })
    }

    async fn fill_pending(&mut self) -> Result<(), Error> {
        while self.pending.is_empty() {
            let Some(frame) = self.stack.pop() else {
                return Ok(());
            };

            let mut diffs = Vec::new();
            match frame {
                DiffFrame::Compare {
                    base_cid,
                    other_cid,
                    span_end,
                } => {
                    process_async_diff_compare(
                        self.prolly,
                        base_cid,
                        other_cid,
                        span_end.as_deref(),
                        &mut self.stack,
                        &mut diffs,
                        Some(&mut self.stats),
                    )
                    .await?;
                }
                DiffFrame::Added { cid } => {
                    process_async_added(
                        self.prolly,
                        cid,
                        &mut self.stack,
                        &mut diffs,
                        Some(&mut self.stats),
                    )
                    .await?;
                }
                DiffFrame::Removed { cid } => {
                    process_async_removed(
                        self.prolly,
                        cid,
                        &mut self.stack,
                        &mut diffs,
                        Some(&mut self.stats),
                    )
                    .await?;
                }
            }
            self.pending.extend(diffs);
        }

        Ok(())
    }
}

/// Compute the difference between two trees.
///
/// Returns a vector of `Diff` entries representing the changes needed to
/// transform `base` into `other`. Yields Added entries for keys that exist in
/// other but not in base, Changed entries for keys with different values, and
/// Removed entries for keys that exist in base but not in other.
///
/// # Arguments
/// * `prolly` - The Prolly tree manager
/// * `base` - The base tree to compare from
/// * `other` - The other tree to compare to
///
/// # Returns
/// * `Ok(Vec<Diff>)` - A vector of differences
/// * `Err` on storage or deserialization errors
///
/// # Short-circuit
/// If both trees have the same root CID, returns an empty vector immediately.
pub fn compute_diff<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    other: &Tree,
) -> Result<Vec<Diff>, Error> {
    if let Some(diffs) = try_append_only_diff(prolly, base, other)? {
        return Ok(diffs);
    }

    let mut diffs = Vec::new();
    for diff in stream_diff(prolly, base, other) {
        diffs.push(diff?);
    }

    Ok(diffs)
}

fn compute_diff_with_stats<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    other: &Tree,
) -> Result<(Vec<Diff>, DiffTraversalStats), Error> {
    let mut iter = stream_diff(prolly, base, other);
    let mut diffs = Vec::new();
    for diff in iter.by_ref() {
        diffs.push(diff?);
    }
    Ok((diffs, iter.stats))
}

/// Compute the difference between two trees within the half-open key range
/// `[start, end)`.
///
/// This mirrors [`compute_diff`] but prunes whole child spans that cannot
/// overlap the requested range, so narrow range diffs do not have to
/// materialize both sides of the range first.
pub fn compute_range_diff<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    other: &Tree,
    start: &[u8],
    end: Option<&[u8]>,
) -> Result<Vec<Diff>, Error> {
    if end.is_some_and(|end| end <= start) || base.root == other.root {
        return Ok(Vec::new());
    }

    let mut diffs = Vec::new();

    match (&base.root, &other.root) {
        (Some(base_cid), Some(other_cid)) => {
            diff_range_nodes(prolly, base_cid, other_cid, None, start, end, &mut diffs)?;
        }
        (Some(base_cid), None) => {
            collect_removed_range_from_cid(prolly, base_cid, None, start, end, &mut diffs)?;
        }
        (None, Some(other_cid)) => {
            collect_added_range_from_cid(prolly, other_cid, None, start, end, &mut diffs)?;
        }
        (None, None) => {}
    }

    Ok(diffs)
}

#[cfg(feature = "async-store")]
pub async fn compute_async_diff<S>(
    prolly: &AsyncProlly<S>,
    base: &Tree,
    other: &Tree,
) -> Result<Vec<Diff>, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if base.root == other.root {
        return Ok(Vec::new());
    }

    let mut diffs = Vec::new();
    let mut stack = initial_diff_stack(base, other);

    while let Some(frame) = stack.pop() {
        match frame {
            DiffFrame::Compare {
                base_cid,
                other_cid,
                span_end,
            } => {
                process_async_diff_compare(
                    prolly,
                    base_cid,
                    other_cid,
                    span_end.as_deref(),
                    &mut stack,
                    &mut diffs,
                    None,
                )
                .await?;
            }
            DiffFrame::Added { cid } => {
                process_async_added(prolly, cid, &mut stack, &mut diffs, None).await?;
            }
            DiffFrame::Removed { cid } => {
                process_async_removed(prolly, cid, &mut stack, &mut diffs, None).await?;
            }
        }
    }

    Ok(diffs)
}

#[cfg(feature = "async-store")]
async fn compute_async_diff_with_stats<S>(
    prolly: &AsyncProlly<S>,
    base: &Tree,
    other: &Tree,
) -> Result<(Vec<Diff>, DiffTraversalStats), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let mut iter = AsyncDiffIter::new(prolly, base, other);
    let mut diffs = Vec::new();
    while let Some(diff) = iter.next().await {
        diffs.push(diff?);
    }
    Ok((diffs, iter.stats))
}

#[cfg(feature = "async-store")]
pub(crate) async fn structural_diff_page_async<S>(
    prolly: &AsyncProlly<S>,
    base: &Tree,
    other: &Tree,
    cursor: Option<&StructuralDiffCursor>,
    limit: usize,
) -> Result<StructuralDiffPage, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if let Some(cursor) = cursor {
        if !cursor.matches_trees(base, other) {
            return Err(Error::InvalidNode);
        }
    }

    if limit == 0 {
        return Ok(StructuralDiffPage {
            diffs: Vec::new(),
            next_cursor: Some(
                cursor
                    .cloned()
                    .unwrap_or_else(|| StructuralDiffCursor::start(base, other)),
            ),
            stats: DiffTraversalStats::default(),
        });
    }

    let mut iter = match cursor {
        Some(cursor) => AsyncDiffIter::from_cursor(prolly, base, other, cursor)?,
        None => AsyncDiffIter::new(prolly, base, other),
    };
    let mut diffs = Vec::with_capacity(limit);

    for _ in 0..limit {
        let Some(diff) = iter.next().await else {
            break;
        };
        diffs.push(diff?);
    }

    Ok(StructuralDiffPage {
        diffs,
        next_cursor: iter.checkpoint(),
        stats: iter.stats,
    })
}

#[cfg(feature = "async-store")]
pub async fn compute_async_range_diff<S>(
    prolly: &AsyncProlly<S>,
    base: &Tree,
    other: &Tree,
    start: &[u8],
    end: Option<&[u8]>,
) -> Result<Vec<Diff>, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if end.is_some_and(|end| end <= start) || base.root == other.root {
        return Ok(Vec::new());
    }

    let mut diffs = Vec::new();
    let mut stack = match (&base.root, &other.root) {
        (Some(base_cid), Some(other_cid)) => vec![RangeDiffFrame::Compare {
            base_cid: base_cid.clone(),
            other_cid: other_cid.clone(),
            span_end: None,
        }],
        (Some(base_cid), None) => vec![RangeDiffFrame::Removed {
            cid: base_cid.clone(),
            span_end: None,
        }],
        (None, Some(other_cid)) => vec![RangeDiffFrame::Added {
            cid: other_cid.clone(),
            span_end: None,
        }],
        (None, None) => Vec::new(),
    };

    while let Some(frame) = stack.pop() {
        match frame {
            RangeDiffFrame::Compare {
                base_cid,
                other_cid,
                span_end,
            } => {
                process_async_range_compare(
                    prolly,
                    base_cid,
                    other_cid,
                    span_end.as_deref(),
                    start,
                    end,
                    &mut stack,
                    &mut diffs,
                )
                .await?;
            }
            RangeDiffFrame::Added { cid, span_end } => {
                let node = prolly.load_arc(&cid).await?;
                collect_added_range_from_node_async(
                    prolly,
                    &node,
                    span_end.as_deref(),
                    start,
                    end,
                    &mut diffs,
                )
                .await?;
            }
            RangeDiffFrame::Removed { cid, span_end } => {
                let node = prolly.load_arc(&cid).await?;
                collect_removed_range_from_node_async(
                    prolly,
                    &node,
                    span_end.as_deref(),
                    start,
                    end,
                    &mut diffs,
                )
                .await?;
            }
        }
    }

    Ok(diffs)
}

#[cfg(feature = "async-store")]
pub async fn merge_trees_async<S>(
    prolly: &AsyncProlly<S>,
    base: &Tree,
    left: &Tree,
    right: &Tree,
    resolver: Option<Resolver>,
) -> Result<Tree, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if left.root == right.root {
        return Ok(left.clone());
    }
    if left.root == base.root {
        return Ok(right.clone());
    }
    if right.root == base.root {
        return Ok(left.clone());
    }

    let right_diff = compute_async_diff(prolly, base, right).await?;
    merge_trees_with_right_diff_async(prolly, left, &right_diff, resolver).await
}

/// Perform an async three-way merge and return structured trace events with the result.
#[cfg(feature = "async-store")]
pub async fn merge_trees_explain_async<S>(
    prolly: &AsyncProlly<S>,
    base: &Tree,
    left: &Tree,
    right: &Tree,
    resolver: Option<Resolver>,
) -> MergeExplanation
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let mut trace = MergeTrace::default();
    let result =
        merge_trees_explain_async_result(prolly, base, left, right, resolver, &mut trace).await;
    MergeExplanation { result, trace }
}

#[cfg(feature = "async-store")]
async fn merge_trees_explain_async_result<S>(
    prolly: &AsyncProlly<S>,
    base: &Tree,
    left: &Tree,
    right: &Tree,
    resolver: Option<Resolver>,
    trace: &mut MergeTrace,
) -> Result<Tree, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if left.root == right.root {
        trace.push(MergeTraceEvent::FastPath {
            reason: MergeFastPath::BranchesEqual,
        });
        return Ok(left.clone());
    }
    if left.root == base.root {
        trace.push(MergeTraceEvent::FastPath {
            reason: MergeFastPath::LeftUnchanged,
        });
        return Ok(right.clone());
    }
    if right.root == base.root {
        trace.push(MergeTraceEvent::FastPath {
            reason: MergeFastPath::RightUnchanged,
        });
        return Ok(left.clone());
    }

    trace.push(MergeTraceEvent::Fallback {
        reason: MergeFallbackReason::DiffBatch,
    });
    let (right_diff, stats) = compute_async_diff_with_stats(prolly, base, right).await?;
    trace.push(MergeTraceEvent::DiffTraversal { stats });
    let mut recorder = MergeTraceRecorder::new(trace);
    merge_trees_with_right_diff_async_traced(prolly, left, &right_diff, resolver, &mut recorder)
        .await
}

/// Merge only right-side changes whose keys are in `[start, end)` through an async store.
#[cfg(feature = "async-store")]
pub async fn merge_trees_range_async<S>(
    prolly: &AsyncProlly<S>,
    base: &Tree,
    left: &Tree,
    right: &Tree,
    start: &[u8],
    end: Option<&[u8]>,
    resolver: Option<Resolver>,
) -> Result<Tree, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if left.root == right.root || right.root == base.root || end.is_some_and(|end| end <= start) {
        return Ok(left.clone());
    }

    let right_diff = compute_async_range_diff(prolly, base, right, start, end).await?;
    merge_trees_with_right_diff_async(prolly, left, &right_diff, resolver).await
}

#[cfg(feature = "async-store")]
async fn merge_trees_with_right_diff_async<S>(
    prolly: &AsyncProlly<S>,
    left: &Tree,
    right_diff: &[Diff],
    resolver: Option<Resolver>,
) -> Result<Tree, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let mut recorder = MergeTraceRecorder::disabled();
    merge_trees_with_right_diff_async_traced(prolly, left, right_diff, resolver, &mut recorder)
        .await
}

#[cfg(feature = "async-store")]
async fn merge_trees_with_right_diff_async_traced<S>(
    prolly: &AsyncProlly<S>,
    left: &Tree,
    right_diff: &[Diff],
    resolver: Option<Resolver>,
    recorder: &mut MergeTraceRecorder<'_>,
) -> Result<Tree, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let right_changes = build_merge_change_refs(right_diff);
    let mut mutations = Vec::with_capacity(right_changes.len());
    let keys = right_changes
        .iter()
        .map(|entry| entry.key)
        .collect::<Vec<_>>();
    let left_values = prolly.get_many(left, &keys).await?;

    for (entry, left_val) in right_changes.iter().zip(left_values) {
        let key = entry.key;
        let base_val = entry.base;
        let right_val = entry.value;

        if left_val.as_deref() == base_val {
            push_change_mutation(&mut mutations, key, right_val);
            continue;
        }

        if option_bytes_eq(&left_val, right_val) {
            continue;
        }

        let conflict = build_conflict_from_values(
            key,
            base_val.map(|value| value.to_vec()),
            left_val,
            right_val.map(|value| value.to_vec()),
        );
        if let Some(ref resolve) = resolver {
            let resolution = resolve(&conflict);
            recorder.record_resolver(MergeTraceStage::Batch, &conflict, &resolution);
            match resolution {
                Resolution::Value(resolved) => {
                    mutations.push(Mutation::Upsert {
                        key: key.to_vec(),
                        val: resolved,
                    });
                    continue;
                }
                Resolution::Delete => {
                    mutations.push(Mutation::Delete { key: key.to_vec() });
                    continue;
                }
                Resolution::Unresolved => {}
            }
        }

        return Err(Error::Conflict(conflict));
    }

    recorder.record(MergeTraceEvent::BatchMerge {
        right_changes: right_changes.len(),
        mutations: mutations.len(),
        append_only: false,
    });
    if mutations.is_empty() {
        Ok(left.clone())
    } else {
        prolly.batch(left, mutations).await
    }
}

#[cfg(feature = "async-store")]
#[derive(Clone)]
enum RangeDiffFrame {
    Compare {
        base_cid: Cid,
        other_cid: Cid,
        span_end: Option<Vec<u8>>,
    },
    Added {
        cid: Cid,
        span_end: Option<Vec<u8>>,
    },
    Removed {
        cid: Cid,
        span_end: Option<Vec<u8>>,
    },
}

#[cfg(feature = "async-store")]
async fn process_async_diff_compare<S>(
    prolly: &AsyncProlly<S>,
    base_cid: Cid,
    other_cid: Cid,
    span_end: Option<&[u8]>,
    stack: &mut Vec<DiffFrame>,
    diffs: &mut Vec<Diff>,
    mut stats: Option<&mut DiffTraversalStats>,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if base_cid == other_cid {
        if let Some(stats) = &mut stats {
            stats.reused_subtrees += 1;
        }
        return Ok(());
    }

    if let Some(stats) = &mut stats {
        stats.compared_nodes += 1;
    }
    let nodes = prolly
        .load_many_ordered(&[base_cid.clone(), other_cid.clone()])
        .await?;
    let base = nodes[0].clone();
    let other = nodes[1].clone();

    match (base.leaf, other.leaf) {
        (true, true) => diff_leaf_nodes(&base, &other, diffs)?,
        (false, false) if base.level == other.level => {
            enqueue_async_internal_diff(prolly, &base, &other, span_end, stack, diffs, stats)
                .await?;
        }
        _ => {
            if let Some(stats) = &mut stats {
                stats.collected_fallbacks += 1;
            }
            diff_collected_nodes_async(prolly, &base, &other, diffs).await?;
        }
    }

    Ok(())
}

#[cfg(feature = "async-store")]
async fn enqueue_async_internal_diff<S>(
    prolly: &AsyncProlly<S>,
    base: &Node,
    other: &Node,
    span_end: Option<&[u8]>,
    stack: &mut Vec<DiffFrame>,
    diffs: &mut Vec<Diff>,
    mut stats: Option<&mut DiffTraversalStats>,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    ensure_node_value_count(base)?;
    ensure_node_value_count(other)?;
    let mut frames = Vec::with_capacity(base.len().max(other.len()));
    let mut base_idx = 0;
    let mut other_idx = 0;

    while base_idx < base.len() && other_idx < other.len() {
        let base_start = base.keys[base_idx].as_slice();
        let other_start = other.keys[other_idx].as_slice();
        let base_end = child_span_end(base, base_idx, span_end);
        let other_end = child_span_end(other, other_idx, span_end);

        if base_start == other_start && base_end == other_end {
            let base_cid = child_cid_validated(base, base_idx)?;
            let other_cid = child_cid_validated(other, other_idx)?;
            if base_cid != other_cid {
                frames.push(DiffFrame::Compare {
                    base_cid,
                    other_cid,
                    span_end: base_end.map(<[u8]>::to_vec),
                });
            } else if let Some(stats) = &mut stats {
                stats.reused_subtrees += 1;
            }
            base_idx += 1;
            other_idx += 1;
        } else if span_ends_before_or_at(base_end, other_start) {
            frames.push(DiffFrame::Removed {
                cid: child_cid_validated(base, base_idx)?,
            });
            base_idx += 1;
        } else if span_ends_before_or_at(other_end, base_start) {
            frames.push(DiffFrame::Added {
                cid: child_cid_validated(other, other_idx)?,
            });
            other_idx += 1;
        } else {
            if let Some(stats) = &mut stats {
                stats.collected_fallbacks += 1;
            }
            diff_collected_nodes_async(prolly, base, other, diffs).await?;
            return Ok(());
        }
    }

    while base_idx < base.len() {
        frames.push(DiffFrame::Removed {
            cid: child_cid_validated(base, base_idx)?,
        });
        base_idx += 1;
    }

    while other_idx < other.len() {
        frames.push(DiffFrame::Added {
            cid: child_cid_validated(other, other_idx)?,
        });
        other_idx += 1;
    }

    prefetch_async_diff_frame_roots(prolly, &frames).await?;
    stack.extend(frames.into_iter().rev());
    Ok(())
}

#[cfg(feature = "async-store")]
async fn process_async_added<S>(
    prolly: &AsyncProlly<S>,
    cid: Cid,
    stack: &mut Vec<DiffFrame>,
    diffs: &mut Vec<Diff>,
    stats: Option<&mut DiffTraversalStats>,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if let Some(stats) = stats {
        stats.added_subtrees += 1;
    }
    let node = prolly.load_arc(&cid).await?;
    if node.leaf {
        ensure_node_value_count(&node)?;
        for idx in 0..node.len() {
            diffs.push(Diff::Added {
                key: node.keys[idx].clone(),
                val: node_value(&node, idx)?.clone(),
            });
        }
    } else {
        let mut frames = child_diff_frames(&node, DiffFrameKind::Added)?;
        prefetch_async_diff_frame_roots(prolly, &frames).await?;
        frames.reverse();
        stack.extend(frames);
    }

    Ok(())
}

#[cfg(feature = "async-store")]
async fn process_async_removed<S>(
    prolly: &AsyncProlly<S>,
    cid: Cid,
    stack: &mut Vec<DiffFrame>,
    diffs: &mut Vec<Diff>,
    stats: Option<&mut DiffTraversalStats>,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if let Some(stats) = stats {
        stats.removed_subtrees += 1;
    }
    let node = prolly.load_arc(&cid).await?;
    if node.leaf {
        ensure_node_value_count(&node)?;
        for idx in 0..node.len() {
            diffs.push(Diff::Removed {
                key: node.keys[idx].clone(),
                val: node_value(&node, idx)?.clone(),
            });
        }
    } else {
        let mut frames = child_diff_frames(&node, DiffFrameKind::Removed)?;
        prefetch_async_diff_frame_roots(prolly, &frames).await?;
        frames.reverse();
        stack.extend(frames);
    }

    Ok(())
}

#[cfg(feature = "async-store")]
#[allow(clippy::too_many_arguments)]
async fn process_async_range_compare<S>(
    prolly: &AsyncProlly<S>,
    base_cid: Cid,
    other_cid: Cid,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    stack: &mut Vec<RangeDiffFrame>,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if base_cid == other_cid {
        return Ok(());
    }

    let nodes = prolly.load_many_ordered(&[base_cid, other_cid]).await?;
    let base_node = nodes[0].clone();
    let other_node = nodes[1].clone();

    match (base_node.leaf, other_node.leaf) {
        (true, true) => {
            diff_leaf_nodes_range(&base_node, &other_node, range_start, range_end, diffs)?;
        }
        (false, false) if base_node.level == other_node.level => {
            enqueue_async_internal_range_diff(
                prolly,
                &base_node,
                &other_node,
                span_end,
                range_start,
                range_end,
                stack,
                diffs,
            )
            .await?;
        }
        _ => {
            diff_collected_nodes_range_async(
                prolly,
                &base_node,
                &other_node,
                span_end,
                range_start,
                range_end,
                diffs,
            )
            .await?;
        }
    }

    Ok(())
}

#[cfg(feature = "async-store")]
#[allow(clippy::too_many_arguments)]
async fn enqueue_async_internal_range_diff<S>(
    prolly: &AsyncProlly<S>,
    base: &Node,
    other: &Node,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    stack: &mut Vec<RangeDiffFrame>,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    ensure_node_value_count(base)?;
    ensure_node_value_count(other)?;
    let mut frames = Vec::with_capacity(base.len().max(other.len()));
    let mut base_idx = first_potentially_overlapping_child_index(base, range_start);
    let mut other_idx = first_potentially_overlapping_child_index(other, range_start);

    while base_idx < base.len() && other_idx < other.len() {
        let base_start = base.keys[base_idx].as_slice();
        let other_start = other.keys[other_idx].as_slice();
        let base_end = child_span_end(base, base_idx, span_end);
        let other_end = child_span_end(other, other_idx, span_end);

        if span_ends_before_or_at(base_end, range_start) {
            base_idx += 1;
            continue;
        }
        if span_ends_before_or_at(other_end, range_start) {
            other_idx += 1;
            continue;
        }
        if range_ends_before_or_at(range_end, base_start)
            && range_ends_before_or_at(range_end, other_start)
        {
            break;
        }

        if base_start == other_start && base_end == other_end {
            if span_overlaps_range(base_start, base_end, range_start, range_end) {
                let base_cid = child_cid_validated(base, base_idx)?;
                let other_cid = child_cid_validated(other, other_idx)?;
                frames.push(RangeDiffFrame::Compare {
                    base_cid,
                    other_cid,
                    span_end: base_end.map(<[u8]>::to_vec),
                });
            }
            base_idx += 1;
            other_idx += 1;
        } else if span_ends_before_or_at(base_end, other_start) {
            if span_overlaps_range(base_start, base_end, range_start, range_end) {
                frames.push(RangeDiffFrame::Removed {
                    cid: child_cid_validated(base, base_idx)?,
                    span_end: base_end.map(<[u8]>::to_vec),
                });
            }
            base_idx += 1;
        } else if span_ends_before_or_at(other_end, base_start) {
            if span_overlaps_range(other_start, other_end, range_start, range_end) {
                frames.push(RangeDiffFrame::Added {
                    cid: child_cid_validated(other, other_idx)?,
                    span_end: other_end.map(<[u8]>::to_vec),
                });
            }
            other_idx += 1;
        } else {
            diff_collected_nodes_range_async(
                prolly,
                base,
                other,
                span_end,
                range_start,
                range_end,
                diffs,
            )
            .await?;
            return Ok(());
        }
    }

    while base_idx < base.len() {
        let base_start = base.keys[base_idx].as_slice();
        let base_end = child_span_end(base, base_idx, span_end);
        if span_overlaps_range(base_start, base_end, range_start, range_end) {
            frames.push(RangeDiffFrame::Removed {
                cid: child_cid_validated(base, base_idx)?,
                span_end: base_end.map(<[u8]>::to_vec),
            });
        } else if range_ends_before_or_at(range_end, base_start) {
            break;
        }
        base_idx += 1;
    }

    while other_idx < other.len() {
        let other_start = other.keys[other_idx].as_slice();
        let other_end = child_span_end(other, other_idx, span_end);
        if span_overlaps_range(other_start, other_end, range_start, range_end) {
            frames.push(RangeDiffFrame::Added {
                cid: child_cid_validated(other, other_idx)?,
                span_end: other_end.map(<[u8]>::to_vec),
            });
        } else if range_ends_before_or_at(range_end, other_start) {
            break;
        }
        other_idx += 1;
    }

    prefetch_async_range_frame_roots(prolly, &frames).await?;
    stack.extend(frames.into_iter().rev());
    Ok(())
}

#[cfg(feature = "async-store")]
async fn prefetch_async_diff_frame_roots<S>(
    prolly: &AsyncProlly<S>,
    frames: &[DiffFrame],
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if frames.len() <= 1 || !prolly.store().prefers_batch_reads() {
        return Ok(());
    }

    let mut seen = HashSet::with_capacity(frames.len().saturating_mul(2));
    let mut cids = Vec::with_capacity(frames.len().saturating_mul(2));
    for frame in frames {
        match frame {
            DiffFrame::Compare {
                base_cid,
                other_cid,
                ..
            } => {
                if seen.insert(base_cid.clone()) {
                    cids.push(base_cid.clone());
                }
                if seen.insert(other_cid.clone()) {
                    cids.push(other_cid.clone());
                }
            }
            DiffFrame::Added { cid } | DiffFrame::Removed { cid } => {
                if seen.insert(cid.clone()) {
                    cids.push(cid.clone());
                }
            }
        }
    }

    if !cids.is_empty() {
        let _ = prolly.load_child_frontier_ordered(&cids).await?;
    }

    Ok(())
}

#[cfg(feature = "async-store")]
async fn prefetch_async_range_frame_roots<S>(
    prolly: &AsyncProlly<S>,
    frames: &[RangeDiffFrame],
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if frames.len() <= 1 || !prolly.store().prefers_batch_reads() {
        return Ok(());
    }

    let mut seen = HashSet::with_capacity(frames.len().saturating_mul(2));
    let mut cids = Vec::with_capacity(frames.len().saturating_mul(2));
    for frame in frames {
        match frame {
            RangeDiffFrame::Compare {
                base_cid,
                other_cid,
                ..
            } => {
                if seen.insert(base_cid.clone()) {
                    cids.push(base_cid.clone());
                }
                if seen.insert(other_cid.clone()) {
                    cids.push(other_cid.clone());
                }
            }
            RangeDiffFrame::Added { cid, .. } | RangeDiffFrame::Removed { cid, .. } => {
                if seen.insert(cid.clone()) {
                    cids.push(cid.clone());
                }
            }
        }
    }

    if !cids.is_empty() {
        let _ = prolly.load_child_frontier_ordered(&cids).await?;
    }

    Ok(())
}

fn diff_range_nodes<S: Store>(
    prolly: &Prolly<S>,
    base_cid: &Cid,
    other_cid: &Cid,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error> {
    if base_cid == other_cid {
        return Ok(());
    }

    let (base_node, other_node) = load_range_diff_node_pair(prolly, base_cid, other_cid)?;

    match (base_node.leaf, other_node.leaf) {
        (true, true) => {
            diff_leaf_nodes_range(&base_node, &other_node, range_start, range_end, diffs)
        }
        (false, false) if base_node.level == other_node.level => diff_internal_nodes_range(
            prolly,
            &base_node,
            &other_node,
            span_end,
            range_start,
            range_end,
            diffs,
        ),
        _ => diff_collected_nodes_range(
            prolly,
            &base_node,
            &other_node,
            span_end,
            range_start,
            range_end,
            diffs,
        ),
    }
}

fn load_range_diff_node_pair<S: Store>(
    prolly: &Prolly<S>,
    base_cid: &Cid,
    other_cid: &Cid,
) -> Result<(std::sync::Arc<Node>, std::sync::Arc<Node>), Error> {
    if prolly.store().prefers_batch_reads() {
        let nodes = prolly.load_many_ordered(&[base_cid.clone(), other_cid.clone()])?;
        return Ok((nodes[0].clone(), nodes[1].clone()));
    }

    Ok((prolly.load_arc(base_cid)?, prolly.load_arc(other_cid)?))
}

fn diff_internal_nodes_range<S: Store>(
    prolly: &Prolly<S>,
    base: &Node,
    other: &Node,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error> {
    ensure_node_value_count(base)?;
    ensure_node_value_count(other)?;
    let original_diff_len = diffs.len();
    let mut base_idx = first_potentially_overlapping_child_index(base, range_start);
    let mut other_idx = first_potentially_overlapping_child_index(other, range_start);

    while base_idx < base.len() && other_idx < other.len() {
        let base_start = base.keys[base_idx].as_slice();
        let other_start = other.keys[other_idx].as_slice();
        let base_end = child_span_end(base, base_idx, span_end);
        let other_end = child_span_end(other, other_idx, span_end);

        if span_ends_before_or_at(base_end, range_start) {
            base_idx += 1;
            continue;
        }
        if span_ends_before_or_at(other_end, range_start) {
            other_idx += 1;
            continue;
        }
        if range_ends_before_or_at(range_end, base_start)
            && range_ends_before_or_at(range_end, other_start)
        {
            break;
        }

        if base_start == other_start && base_end == other_end {
            if span_overlaps_range(base_start, base_end, range_start, range_end) {
                let base_cid = child_cid_validated(base, base_idx)?;
                let other_cid = child_cid_validated(other, other_idx)?;
                diff_range_nodes(
                    prolly,
                    &base_cid,
                    &other_cid,
                    base_end,
                    range_start,
                    range_end,
                    diffs,
                )?;
            }
            base_idx += 1;
            other_idx += 1;
        } else if span_ends_before_or_at(base_end, other_start) {
            if span_overlaps_range(base_start, base_end, range_start, range_end) {
                let base_cid = child_cid_validated(base, base_idx)?;
                collect_removed_range_from_cid(
                    prolly,
                    &base_cid,
                    base_end,
                    range_start,
                    range_end,
                    diffs,
                )?;
            }
            base_idx += 1;
        } else if span_ends_before_or_at(other_end, base_start) {
            if span_overlaps_range(other_start, other_end, range_start, range_end) {
                let other_cid = child_cid_validated(other, other_idx)?;
                collect_added_range_from_cid(
                    prolly,
                    &other_cid,
                    other_end,
                    range_start,
                    range_end,
                    diffs,
                )?;
            }
            other_idx += 1;
        } else {
            // Boundaries overlap without lining up. Keep the fallback local to
            // this subtree, but filter and prune by the requested range.
            diffs.truncate(original_diff_len);
            return diff_collected_nodes_range(
                prolly,
                base,
                other,
                span_end,
                range_start,
                range_end,
                diffs,
            );
        }
    }

    while base_idx < base.len() {
        let base_start = base.keys[base_idx].as_slice();
        let base_end = child_span_end(base, base_idx, span_end);
        if span_overlaps_range(base_start, base_end, range_start, range_end) {
            let base_cid = child_cid_validated(base, base_idx)?;
            collect_removed_range_from_cid(
                prolly,
                &base_cid,
                base_end,
                range_start,
                range_end,
                diffs,
            )?;
        } else if range_ends_before_or_at(range_end, base_start) {
            break;
        }
        base_idx += 1;
    }

    while other_idx < other.len() {
        let other_start = other.keys[other_idx].as_slice();
        let other_end = child_span_end(other, other_idx, span_end);
        if span_overlaps_range(other_start, other_end, range_start, range_end) {
            let other_cid = child_cid_validated(other, other_idx)?;
            collect_added_range_from_cid(
                prolly,
                &other_cid,
                other_end,
                range_start,
                range_end,
                diffs,
            )?;
        } else if range_ends_before_or_at(range_end, other_start) {
            break;
        }
        other_idx += 1;
    }

    Ok(())
}

fn diff_leaf_nodes(base: &Node, other: &Node, diffs: &mut Vec<Diff>) -> Result<(), Error> {
    ensure_node_value_count(base)?;
    ensure_node_value_count(other)?;

    let mut base_idx = 0;
    let mut other_idx = 0;

    while base_idx < base.len() && other_idx < other.len() {
        let base_key = &base.keys[base_idx];
        let other_key = &other.keys[other_idx];

        match base_key.cmp(other_key) {
            std::cmp::Ordering::Less => {
                diffs.push(Diff::Removed {
                    key: base_key.clone(),
                    val: node_value(base, base_idx)?.clone(),
                });
                base_idx += 1;
            }
            std::cmp::Ordering::Greater => {
                diffs.push(Diff::Added {
                    key: other_key.clone(),
                    val: node_value(other, other_idx)?.clone(),
                });
                other_idx += 1;
            }
            std::cmp::Ordering::Equal => {
                let old = node_value(base, base_idx)?;
                let new = node_value(other, other_idx)?;
                if old != new {
                    diffs.push(Diff::Changed {
                        key: base_key.clone(),
                        old: old.clone(),
                        new: new.clone(),
                    });
                }
                base_idx += 1;
                other_idx += 1;
            }
        }
    }

    while base_idx < base.len() {
        diffs.push(Diff::Removed {
            key: base.keys[base_idx].clone(),
            val: node_value(base, base_idx)?.clone(),
        });
        base_idx += 1;
    }

    while other_idx < other.len() {
        diffs.push(Diff::Added {
            key: other.keys[other_idx].clone(),
            val: node_value(other, other_idx)?.clone(),
        });
        other_idx += 1;
    }

    Ok(())
}

fn diff_leaf_nodes_range(
    base: &Node,
    other: &Node,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error> {
    ensure_node_value_count(base)?;
    ensure_node_value_count(other)?;

    let mut base_idx = lower_bound(&base.keys, range_start);
    let mut other_idx = lower_bound(&other.keys, range_start);

    while base_idx < base.len()
        && other_idx < other.len()
        && key_in_range(&base.keys[base_idx], range_start, range_end)
        && key_in_range(&other.keys[other_idx], range_start, range_end)
    {
        let base_key = &base.keys[base_idx];
        let other_key = &other.keys[other_idx];

        match base_key.cmp(other_key) {
            std::cmp::Ordering::Less => {
                diffs.push(Diff::Removed {
                    key: base_key.clone(),
                    val: node_value(base, base_idx)?.clone(),
                });
                base_idx += 1;
            }
            std::cmp::Ordering::Greater => {
                diffs.push(Diff::Added {
                    key: other_key.clone(),
                    val: node_value(other, other_idx)?.clone(),
                });
                other_idx += 1;
            }
            std::cmp::Ordering::Equal => {
                let old = node_value(base, base_idx)?;
                let new = node_value(other, other_idx)?;
                if old != new {
                    diffs.push(Diff::Changed {
                        key: base_key.clone(),
                        old: old.clone(),
                        new: new.clone(),
                    });
                }
                base_idx += 1;
                other_idx += 1;
            }
        }
    }

    while base_idx < base.len() && key_in_range(&base.keys[base_idx], range_start, range_end) {
        diffs.push(Diff::Removed {
            key: base.keys[base_idx].clone(),
            val: node_value(base, base_idx)?.clone(),
        });
        base_idx += 1;
    }

    while other_idx < other.len() && key_in_range(&other.keys[other_idx], range_start, range_end) {
        diffs.push(Diff::Added {
            key: other.keys[other_idx].clone(),
            val: node_value(other, other_idx)?.clone(),
        });
        other_idx += 1;
    }

    Ok(())
}

fn diff_collected_nodes<S: Store>(
    prolly: &Prolly<S>,
    base: &Node,
    other: &Node,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error> {
    let mut base_entries = Vec::new();
    let mut other_entries = Vec::new();
    collect_entries_from_node(prolly, base, &mut base_entries)?;
    collect_entries_from_node(prolly, other, &mut other_entries)?;
    diff_entry_slices(&base_entries, &other_entries, diffs);
    Ok(())
}

fn diff_collected_nodes_range<S: Store>(
    prolly: &Prolly<S>,
    base: &Node,
    other: &Node,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error> {
    let mut base_entries = Vec::new();
    let mut other_entries = Vec::new();
    collect_entries_range_from_node(
        prolly,
        base,
        span_end,
        range_start,
        range_end,
        &mut base_entries,
    )?;
    collect_entries_range_from_node(
        prolly,
        other,
        span_end,
        range_start,
        range_end,
        &mut other_entries,
    )?;
    diff_entry_slices(&base_entries, &other_entries, diffs);
    Ok(())
}

fn diff_entry_slices(
    base: &[(Vec<u8>, Vec<u8>)],
    other: &[(Vec<u8>, Vec<u8>)],
    diffs: &mut Vec<Diff>,
) {
    let mut base_idx = 0;
    let mut other_idx = 0;

    while base_idx < base.len() && other_idx < other.len() {
        let (base_key, base_val) = &base[base_idx];
        let (other_key, other_val) = &other[other_idx];

        match base_key.cmp(other_key) {
            std::cmp::Ordering::Less => {
                diffs.push(Diff::Removed {
                    key: base_key.clone(),
                    val: base_val.clone(),
                });
                base_idx += 1;
            }
            std::cmp::Ordering::Greater => {
                diffs.push(Diff::Added {
                    key: other_key.clone(),
                    val: other_val.clone(),
                });
                other_idx += 1;
            }
            std::cmp::Ordering::Equal => {
                if base_val != other_val {
                    diffs.push(Diff::Changed {
                        key: base_key.clone(),
                        old: base_val.clone(),
                        new: other_val.clone(),
                    });
                }
                base_idx += 1;
                other_idx += 1;
            }
        }
    }

    for (key, val) in &base[base_idx..] {
        diffs.push(Diff::Removed {
            key: key.clone(),
            val: val.clone(),
        });
    }

    for (key, val) in &other[other_idx..] {
        diffs.push(Diff::Added {
            key: key.clone(),
            val: val.clone(),
        });
    }
}

fn collect_entries_from_node<S: Store>(
    prolly: &Prolly<S>,
    node: &Node,
    entries: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<(), Error> {
    if node.leaf {
        ensure_node_value_count(node)?;
        for idx in 0..node.len() {
            entries.push((node.keys[idx].clone(), node_value(node, idx)?.clone()));
        }
        return Ok(());
    }

    let child_cids = child_cids(node)?;
    for child_node in load_child_nodes(prolly, &child_cids)? {
        collect_entries_from_node(prolly, &child_node, entries)?;
    }

    Ok(())
}

fn collect_entries_range_from_node<S: Store>(
    prolly: &Prolly<S>,
    node: &Node,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    entries: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<(), Error> {
    if node.leaf {
        ensure_node_value_count(node)?;
        let mut idx = lower_bound(&node.keys, range_start);
        while idx < node.len() && key_in_range(&node.keys[idx], range_start, range_end) {
            entries.push((node.keys[idx].clone(), node_value(node, idx)?.clone()));
            idx += 1;
        }
        return Ok(());
    }

    let child_spans = overlapping_child_cids(node, span_end, range_start, range_end)?;
    let child_cids = child_spans
        .iter()
        .map(|(_, cid)| cid.clone())
        .collect::<Vec<_>>();
    for ((child_end, _), child_node) in child_spans
        .into_iter()
        .zip(load_child_nodes(prolly, &child_cids)?)
    {
        collect_entries_range_from_node(
            prolly,
            &child_node,
            child_end,
            range_start,
            range_end,
            entries,
        )?;
    }

    Ok(())
}

fn collect_added_from_cid<S: Store>(
    prolly: &Prolly<S>,
    cid: &Cid,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error> {
    let node = prolly.load_arc(cid)?;
    collect_added_from_node(prolly, &node, diffs)
}

fn collect_added_from_node<S: Store>(
    prolly: &Prolly<S>,
    node: &Node,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error> {
    if node.leaf {
        ensure_node_value_count(node)?;
        for idx in 0..node.len() {
            diffs.push(Diff::Added {
                key: node.keys[idx].clone(),
                val: node_value(node, idx)?.clone(),
            });
        }
        return Ok(());
    }

    let child_cids = child_cids(node)?;
    for child_node in load_child_nodes(prolly, &child_cids)? {
        collect_added_from_node(prolly, &child_node, diffs)?;
    }

    Ok(())
}

fn collect_added_range_from_cid<S: Store>(
    prolly: &Prolly<S>,
    cid: &Cid,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error> {
    let node = prolly.load_arc(cid)?;
    collect_added_range_from_node(prolly, &node, span_end, range_start, range_end, diffs)
}

fn collect_added_range_from_node<S: Store>(
    prolly: &Prolly<S>,
    node: &Node,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error> {
    if node.leaf {
        ensure_node_value_count(node)?;
        let mut idx = lower_bound(&node.keys, range_start);
        while idx < node.len() && key_in_range(&node.keys[idx], range_start, range_end) {
            diffs.push(Diff::Added {
                key: node.keys[idx].clone(),
                val: node_value(node, idx)?.clone(),
            });
            idx += 1;
        }
        return Ok(());
    }

    let child_spans = overlapping_child_cids(node, span_end, range_start, range_end)?;
    let child_cids = child_spans
        .iter()
        .map(|(_, cid)| cid.clone())
        .collect::<Vec<_>>();
    for ((child_end, _), child_node) in child_spans
        .into_iter()
        .zip(load_child_nodes(prolly, &child_cids)?)
    {
        collect_added_range_from_node(
            prolly,
            &child_node,
            child_end,
            range_start,
            range_end,
            diffs,
        )?;
    }

    Ok(())
}

fn collect_removed_range_from_cid<S: Store>(
    prolly: &Prolly<S>,
    cid: &Cid,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error> {
    let node = prolly.load_arc(cid)?;
    collect_removed_range_from_node(prolly, &node, span_end, range_start, range_end, diffs)
}

fn collect_removed_range_from_node<S: Store>(
    prolly: &Prolly<S>,
    node: &Node,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error> {
    if node.leaf {
        ensure_node_value_count(node)?;
        let mut idx = lower_bound(&node.keys, range_start);
        while idx < node.len() && key_in_range(&node.keys[idx], range_start, range_end) {
            diffs.push(Diff::Removed {
                key: node.keys[idx].clone(),
                val: node_value(node, idx)?.clone(),
            });
            idx += 1;
        }
        return Ok(());
    }

    let child_spans = overlapping_child_cids(node, span_end, range_start, range_end)?;
    let child_cids = child_spans
        .iter()
        .map(|(_, cid)| cid.clone())
        .collect::<Vec<_>>();
    for ((child_end, _), child_node) in child_spans
        .into_iter()
        .zip(load_child_nodes(prolly, &child_cids)?)
    {
        collect_removed_range_from_node(
            prolly,
            &child_node,
            child_end,
            range_start,
            range_end,
            diffs,
        )?;
    }

    Ok(())
}

fn load_child_nodes<S: Store>(
    prolly: &Prolly<S>,
    child_cids: &[Cid],
) -> Result<Vec<std::sync::Arc<Node>>, Error> {
    if prolly.store().prefers_batch_reads() {
        prolly.load_many_ordered_with_parallelism(child_cids, DIFF_COLLECTION_PREFETCH_PARALLELISM)
    } else {
        prolly.load_many_ordered(child_cids)
    }
}

#[cfg(feature = "async-store")]
async fn load_child_nodes_async<S>(
    prolly: &AsyncProlly<S>,
    child_cids: &[Cid],
) -> Result<Vec<std::sync::Arc<Node>>, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    prolly.load_child_frontier_ordered(child_cids).await
}

#[cfg(feature = "async-store")]
async fn diff_collected_nodes_async<S>(
    prolly: &AsyncProlly<S>,
    base: &Node,
    other: &Node,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let mut base_entries = Vec::new();
    let mut other_entries = Vec::new();
    collect_entries_from_node_async(prolly, base, &mut base_entries).await?;
    collect_entries_from_node_async(prolly, other, &mut other_entries).await?;
    diff_entry_slices(&base_entries, &other_entries, diffs);
    Ok(())
}

#[cfg(feature = "async-store")]
async fn diff_collected_nodes_range_async<S>(
    prolly: &AsyncProlly<S>,
    base: &Node,
    other: &Node,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let mut base_entries = Vec::new();
    let mut other_entries = Vec::new();
    collect_entries_range_from_node_async(
        prolly,
        base,
        span_end,
        range_start,
        range_end,
        &mut base_entries,
    )
    .await?;
    collect_entries_range_from_node_async(
        prolly,
        other,
        span_end,
        range_start,
        range_end,
        &mut other_entries,
    )
    .await?;
    diff_entry_slices(&base_entries, &other_entries, diffs);
    Ok(())
}

#[cfg(feature = "async-store")]
async fn collect_entries_from_node_async<S>(
    prolly: &AsyncProlly<S>,
    node: &Node,
    entries: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let mut stack = vec![std::sync::Arc::new(node.clone())];

    while let Some(node) = stack.pop() {
        if node.leaf {
            ensure_node_value_count(&node)?;
            for idx in 0..node.len() {
                entries.push((node.keys[idx].clone(), node_value(&node, idx)?.clone()));
            }
            continue;
        }

        let child_cids = child_cids(&node)?;
        let mut child_nodes = load_child_nodes_async(prolly, &child_cids).await?;
        child_nodes.reverse();
        stack.extend(child_nodes);
    }

    Ok(())
}

#[cfg(feature = "async-store")]
async fn collect_entries_range_from_node_async<S>(
    prolly: &AsyncProlly<S>,
    node: &Node,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    entries: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let mut stack = vec![(
        std::sync::Arc::new(node.clone()),
        span_end.map(<[u8]>::to_vec),
    )];

    while let Some((node, span_end)) = stack.pop() {
        if node.leaf {
            ensure_node_value_count(&node)?;
            let mut idx = lower_bound(&node.keys, range_start);
            while idx < node.len() && key_in_range(&node.keys[idx], range_start, range_end) {
                entries.push((node.keys[idx].clone(), node_value(&node, idx)?.clone()));
                idx += 1;
            }
            continue;
        }

        let child_spans =
            overlapping_child_cids(&node, span_end.as_deref(), range_start, range_end)?;
        let child_cids = child_spans
            .iter()
            .map(|(_, cid)| cid.clone())
            .collect::<Vec<_>>();
        let child_nodes = load_child_nodes_async(prolly, &child_cids).await?;
        let mut child_work = child_spans
            .into_iter()
            .zip(child_nodes)
            .map(|((child_end, _), child_node)| (child_node, child_end.map(<[u8]>::to_vec)))
            .collect::<Vec<_>>();
        child_work.reverse();
        stack.extend(child_work);
    }

    Ok(())
}

#[cfg(feature = "async-store")]
async fn collect_added_range_from_node_async<S>(
    prolly: &AsyncProlly<S>,
    node: &Node,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let mut entries = Vec::new();
    collect_entries_range_from_node_async(
        prolly,
        node,
        span_end,
        range_start,
        range_end,
        &mut entries,
    )
    .await?;

    diffs.extend(
        entries
            .into_iter()
            .map(|(key, val)| Diff::Added { key, val }),
    );
    Ok(())
}

#[cfg(feature = "async-store")]
async fn collect_removed_range_from_node_async<S>(
    prolly: &AsyncProlly<S>,
    node: &Node,
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
    diffs: &mut Vec<Diff>,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let mut entries = Vec::new();
    collect_entries_range_from_node_async(
        prolly,
        node,
        span_end,
        range_start,
        range_end,
        &mut entries,
    )
    .await?;

    diffs.extend(
        entries
            .into_iter()
            .map(|(key, val)| Diff::Removed { key, val }),
    );
    Ok(())
}

fn child_cids(node: &Node) -> Result<Vec<Cid>, Error> {
    ensure_node_value_count(node)?;
    let mut cids = Vec::with_capacity(node.vals.len());
    for child in &node.vals {
        cids.push(child_cid_from_bytes(child)?);
    }
    Ok(cids)
}

fn child_diff_frames(node: &Node, kind: DiffFrameKind) -> Result<Vec<DiffFrame>, Error> {
    ensure_node_value_count(node)?;
    let mut frames = Vec::with_capacity(node.vals.len());
    for child in &node.vals {
        frames.push(kind.frame(child_cid_from_bytes(child)?));
    }
    Ok(frames)
}

fn overlapping_child_cids<'a>(
    node: &'a Node,
    span_end: Option<&'a [u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
) -> Result<Vec<ChildSpanCid<'a>>, Error> {
    ensure_node_value_count(node)?;
    let child_range = potentially_overlapping_child_index_range(node, range_start, range_end);
    let mut children = Vec::with_capacity(child_range.len());

    for idx in child_range {
        let child_start = node.keys[idx].as_slice();
        let child_end = child_span_end(node, idx, span_end);
        if span_overlaps_range(child_start, child_end, range_start, range_end) {
            children.push((child_end, child_cid_validated(node, idx)?));
        } else if range_ends_before_or_at(range_end, child_start) {
            break;
        }
    }
    Ok(children)
}

fn first_potentially_overlapping_child_index(node: &Node, range_start: &[u8]) -> usize {
    lower_bound(&node.keys, range_start).saturating_sub(1)
}

fn potentially_overlapping_child_index_range(
    node: &Node,
    range_start: &[u8],
    range_end: Option<&[u8]>,
) -> std::ops::Range<usize> {
    let start = first_potentially_overlapping_child_index(node, range_start);
    let end = range_end.map_or(node.len(), |end| lower_bound(&node.keys, end));
    start..end.max(start).min(node.len())
}

fn child_cid(node: &Node, idx: usize) -> Result<Cid, Error> {
    ensure_node_value_count(node)?;
    child_cid_validated(node, idx)
}

fn child_cid_validated(node: &Node, idx: usize) -> Result<Cid, Error> {
    let child = node.vals.get(idx).ok_or(Error::InvalidNode)?;
    child_cid_from_bytes(child)
}

fn child_cid_from_bytes(child: &[u8]) -> Result<Cid, Error> {
    Ok(Cid(child.try_into().map_err(|_| Error::InvalidNode)?))
}

fn node_value(node: &Node, idx: usize) -> Result<&Vec<u8>, Error> {
    node.vals.get(idx).ok_or(Error::InvalidNode)
}

fn ensure_node_value_count(node: &Node) -> Result<(), Error> {
    if node.keys.len() == node.vals.len() {
        Ok(())
    } else {
        Err(Error::InvalidNode)
    }
}

fn child_span_end<'a>(node: &'a Node, idx: usize, span_end: Option<&'a [u8]>) -> Option<&'a [u8]> {
    node.keys.get(idx + 1).map(Vec::as_slice).or(span_end)
}

fn span_ends_before_or_at(end: Option<&[u8]>, start: &[u8]) -> bool {
    end.is_some_and(|end| end <= start)
}

fn range_ends_before_or_at(end: Option<&[u8]>, start: &[u8]) -> bool {
    end.is_some_and(|end| end <= start)
}

fn span_overlaps_range(
    span_start: &[u8],
    span_end: Option<&[u8]>,
    range_start: &[u8],
    range_end: Option<&[u8]>,
) -> bool {
    !span_ends_before_or_at(span_end, range_start)
        && !range_ends_before_or_at(range_end, span_start)
}

fn key_in_range(key: &[u8], start: &[u8], end: Option<&[u8]>) -> bool {
    key >= start
        && match end {
            Some(end) => key < end,
            None => true,
        }
}

fn lower_bound(keys: &[Vec<u8>], key: &[u8]) -> usize {
    keys.partition_point(|candidate| candidate.as_slice() < key)
}

/// Merge two trees using three-way merge.
///
/// Performs a three-way merge using `base` as the common ancestor.
/// Changes from both `left` and `right` are combined into a single tree.
/// Uses the diff algorithm to efficiently identify entries to add.
///
/// # Arguments
/// * `prolly` - The Prolly tree manager
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
pub fn merge_trees<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    left: &Tree,
    right: &Tree,
    resolver: Option<Resolver>,
) -> Result<Tree, Error> {
    if left.root == right.root {
        return Ok(left.clone());
    }
    if left.root == base.root {
        return Ok(right.clone());
    }
    if right.root == base.root {
        return Ok(left.clone());
    }

    if let Some(merged) = try_structural_merge(prolly, base, left, right, resolver.as_deref())? {
        return Ok(merged);
    }

    let right_diff = compute_diff(prolly, base, right)?;
    merge_trees_with_right_diff(prolly, base, left, &right_diff, resolver)
}

/// Perform a three-way merge and return structured trace events with the result.
///
/// Unlike [`merge_trees`], this function keeps the trace even if the merge
/// fails. That makes it suitable for diagnostics, tests, and operator-facing
/// tooling around custom resolvers.
pub fn merge_trees_explain<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    left: &Tree,
    right: &Tree,
    resolver: Option<Resolver>,
) -> MergeExplanation {
    let mut trace = MergeTrace::default();
    let result = merge_trees_explain_result(prolly, base, left, right, resolver, &mut trace);
    MergeExplanation { result, trace }
}

fn merge_trees_explain_result<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    left: &Tree,
    right: &Tree,
    resolver: Option<Resolver>,
    trace: &mut MergeTrace,
) -> Result<Tree, Error> {
    if left.root == right.root {
        trace.push(MergeTraceEvent::FastPath {
            reason: MergeFastPath::BranchesEqual,
        });
        return Ok(left.clone());
    }
    if left.root == base.root {
        trace.push(MergeTraceEvent::FastPath {
            reason: MergeFastPath::LeftUnchanged,
        });
        return Ok(right.clone());
    }
    if right.root == base.root {
        trace.push(MergeTraceEvent::FastPath {
            reason: MergeFastPath::RightUnchanged,
        });
        return Ok(left.clone());
    }

    let structural = {
        let mut recorder = MergeTraceRecorder::new(trace);
        try_structural_merge_traced(
            prolly,
            base,
            left,
            right,
            resolver.as_deref(),
            &mut recorder,
        )?
    };
    if let Some(merged) = structural {
        return Ok(merged);
    }

    trace.push(MergeTraceEvent::Fallback {
        reason: MergeFallbackReason::DiffBatch,
    });
    let (right_diff, stats) = compute_diff_with_stats(prolly, base, right)?;
    trace.push(MergeTraceEvent::DiffTraversal { stats });
    let mut recorder = MergeTraceRecorder::new(trace);
    merge_trees_with_right_diff_traced(prolly, base, left, &right_diff, resolver, &mut recorder)
}

/// Merge only right-side changes whose keys are in `[start, end)`.
pub fn merge_trees_range<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    left: &Tree,
    right: &Tree,
    start: &[u8],
    end: Option<&[u8]>,
    resolver: Option<Resolver>,
) -> Result<Tree, Error> {
    if left.root == right.root || right.root == base.root || end.is_some_and(|end| end <= start) {
        return Ok(left.clone());
    }

    let right_diff = compute_range_diff(prolly, base, right, start, end)?;
    merge_trees_with_right_diff(prolly, base, left, &right_diff, resolver)
}

fn try_structural_merge<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    left: &Tree,
    right: &Tree,
    resolver: Option<&dyn Fn(&Conflict) -> Resolution>,
) -> Result<Option<Tree>, Error> {
    let mut recorder = MergeTraceRecorder::disabled();
    try_structural_merge_traced(prolly, base, left, right, resolver, &mut recorder)
}

fn try_structural_merge_traced<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    left: &Tree,
    right: &Tree,
    resolver: Option<&dyn Fn(&Conflict) -> Resolution>,
    recorder: &mut MergeTraceRecorder<'_>,
) -> Result<Option<Tree>, Error> {
    let (Some(base_cid), Some(left_cid), Some(right_cid)) = (&base.root, &left.root, &right.root)
    else {
        recorder.record_fallback(MergeFallbackReason::MissingRoot);
        return Ok(None);
    };

    recorder.record(MergeTraceEvent::StructuralMergeStarted);
    let mut collector = BatchWriteCollector::new_cached();
    let Some(root) = try_structural_merge_cids(
        prolly,
        base_cid,
        left_cid,
        right_cid,
        resolver,
        &mut collector,
        recorder,
    )?
    else {
        return Ok(None);
    };
    collector.flush(prolly.store())?;
    prolly.record_batch_write_metrics(collector.len(), collector.bytes_len());
    collector.cache_nodes(prolly)?;

    Ok(Some(Tree {
        root: Some(root),
        config: base.config.clone(),
    }))
}

fn try_structural_merge_cids<S: Store>(
    prolly: &Prolly<S>,
    base_cid: &Cid,
    left_cid: &Cid,
    right_cid: &Cid,
    resolver: Option<&dyn Fn(&Conflict) -> Resolution>,
    collector: &mut BatchWriteCollector,
    recorder: &mut MergeTraceRecorder<'_>,
) -> Result<Option<Cid>, Error> {
    if left_cid == right_cid {
        recorder.record_reuse(left_cid, MergeReuseReason::BranchesEqual);
        return Ok(Some(left_cid.clone()));
    }
    if left_cid == base_cid {
        recorder.record_reuse(right_cid, MergeReuseReason::LeftUnchanged);
        return Ok(Some(right_cid.clone()));
    }
    if right_cid == base_cid {
        recorder.record_reuse(left_cid, MergeReuseReason::RightUnchanged);
        return Ok(Some(left_cid.clone()));
    }

    let nodes =
        prolly.load_many_ordered(&[base_cid.clone(), left_cid.clone(), right_cid.clone()])?;
    let base = nodes[0].clone();
    let left = nodes[1].clone();
    let right = nodes[2].clone();

    if base.leaf != left.leaf
        || base.leaf != right.leaf
        || base.level != left.level
        || base.level != right.level
        || base.keys != left.keys
        || base.keys != right.keys
    {
        recorder.record_fallback(MergeFallbackReason::ShapeMismatch);
        return Ok(None);
    }

    if base.leaf {
        return try_structural_merge_leaf(
            prolly, &base, &left, &right, base_cid, left_cid, right_cid, resolver, collector,
            recorder,
        );
    }

    try_structural_merge_internal(
        prolly, &base, &left, &right, base_cid, left_cid, right_cid, resolver, collector, recorder,
    )
}

#[allow(clippy::too_many_arguments)]
fn try_structural_merge_internal<S: Store>(
    prolly: &Prolly<S>,
    base: &Node,
    left: &Node,
    right: &Node,
    base_cid: &Cid,
    left_cid: &Cid,
    right_cid: &Cid,
    resolver: Option<&dyn Fn(&Conflict) -> Resolution>,
    collector: &mut BatchWriteCollector,
    recorder: &mut MergeTraceRecorder<'_>,
) -> Result<Option<Cid>, Error> {
    ensure_node_value_count(base)?;
    ensure_node_value_count(left)?;
    ensure_node_value_count(right)?;
    if base.len() != left.len() || base.len() != right.len() {
        recorder.record_fallback(MergeFallbackReason::NodeLengthMismatch);
        return Ok(None);
    }

    let mut merged_vals = Vec::with_capacity(base.len());
    let mut differs_from_base = false;
    prefetch_structural_merge_frontier(prolly, base, left, right);

    for idx in 0..base.len() {
        let base_child = child_cid_validated(base, idx)?;
        let left_child = child_cid_validated(left, idx)?;
        let right_child = child_cid_validated(right, idx)?;
        let Some(merged_child) = try_structural_merge_cids(
            prolly,
            &base_child,
            &left_child,
            &right_child,
            resolver,
            collector,
            recorder,
        )?
        else {
            recorder.record_fallback(MergeFallbackReason::ChildFallback);
            return Ok(None);
        };

        if merged_child != base_child {
            differs_from_base = true;
        }
        merged_vals.push(merged_child.0.to_vec());
    }

    if !differs_from_base {
        recorder.record_reuse(base_cid, MergeReuseReason::UnchangedAfterMerge);
        return Ok(Some(base_cid.clone()));
    }
    if merged_vals == left.vals {
        recorder.record_reuse(left_cid, MergeReuseReason::MatchesLeft);
        return Ok(Some(left_cid.clone()));
    }
    if merged_vals == right.vals {
        recorder.record_reuse(right_cid, MergeReuseReason::MatchesRight);
        return Ok(Some(right_cid.clone()));
    }

    let mut merged = prolly.new_node_like(base);
    merged.keys = base.keys.clone();
    merged.vals = merged_vals;
    let cid = collector.add(&merged);
    recorder.record_rewrite(&cid, &merged);
    Ok(Some(cid))
}

fn prefetch_structural_merge_frontier<S: Store>(
    prolly: &Prolly<S>,
    base: &Node,
    left: &Node,
    right: &Node,
) {
    if !prolly.store().prefers_batch_reads() || base.len() <= 1 {
        return;
    }

    let mut cids = Vec::with_capacity(base.len().saturating_mul(3));
    for idx in 0..base.len() {
        let (Ok(base_child), Ok(left_child), Ok(right_child)) = (
            child_cid(base, idx),
            child_cid(left, idx),
            child_cid(right, idx),
        ) else {
            continue;
        };

        if left_child == right_child || left_child == base_child || right_child == base_child {
            continue;
        }

        cids.push(base_child);
        cids.push(left_child);
        cids.push(right_child);
    }

    if cids.len() > 3 {
        let _ =
            prolly.load_many_ordered_with_parallelism(&cids, MERGE_FRONTIER_PREFETCH_PARALLELISM);
    }
}

#[allow(clippy::too_many_arguments)]
fn try_structural_merge_leaf<S: Store>(
    prolly: &Prolly<S>,
    base: &Node,
    left: &Node,
    right: &Node,
    base_cid: &Cid,
    left_cid: &Cid,
    right_cid: &Cid,
    resolver: Option<&dyn Fn(&Conflict) -> Resolution>,
    collector: &mut BatchWriteCollector,
    recorder: &mut MergeTraceRecorder<'_>,
) -> Result<Option<Cid>, Error> {
    ensure_node_value_count(base)?;
    ensure_node_value_count(left)?;
    ensure_node_value_count(right)?;

    let mut merged_keys = Vec::with_capacity(base.len());
    let mut merged_vals = Vec::with_capacity(base.len());
    let mut deleted_any = false;

    for idx in 0..base.len() {
        let base_val = node_value(base, idx)?;
        let left_val = node_value(left, idx)?;
        let right_val = node_value(right, idx)?;
        let merged_val = if left_val == right_val {
            Some(left_val.clone())
        } else if left_val == base_val {
            Some(right_val.clone())
        } else if right_val == base_val {
            Some(left_val.clone())
        } else {
            let conflict = Conflict {
                key: base.keys[idx].clone(),
                base: Some(base_val.clone()),
                left: Some(left_val.clone()),
                right: Some(right_val.clone()),
            };
            if let Some(resolve) = resolver {
                let resolution = resolve(&conflict);
                recorder.record_resolver(MergeTraceStage::Structural, &conflict, &resolution);
                match resolution {
                    Resolution::Value(value) => Some(value),
                    Resolution::Delete => {
                        deleted_any = true;
                        None
                    }
                    Resolution::Unresolved => return Err(Error::Conflict(conflict)),
                }
            } else {
                return Err(Error::Conflict(conflict));
            }
        };

        if let Some(merged_val) = merged_val {
            merged_keys.push(base.keys[idx].clone());
            merged_vals.push(merged_val);
        }
    }

    if deleted_any {
        if merged_keys.is_empty()
            || merged_keys.len() < base.min_chunk_size
            || merged_keys.first() != base.keys.first()
        {
            recorder.record_fallback(MergeFallbackReason::DeleteResolution);
            return Ok(None);
        }

        let mut merged = prolly.new_node_like(base);
        merged.keys = merged_keys;
        merged.vals = merged_vals;
        let cid = collector.add(&merged);
        recorder.record_rewrite(&cid, &merged);
        return Ok(Some(cid));
    }

    if merged_vals == base.vals {
        recorder.record_reuse(base_cid, MergeReuseReason::UnchangedAfterMerge);
        return Ok(Some(base_cid.clone()));
    }
    if merged_vals == left.vals {
        recorder.record_reuse(left_cid, MergeReuseReason::MatchesLeft);
        return Ok(Some(left_cid.clone()));
    }
    if merged_vals == right.vals {
        recorder.record_reuse(right_cid, MergeReuseReason::MatchesRight);
        return Ok(Some(right_cid.clone()));
    }

    let mut merged = prolly.new_node_like(base);
    merged.keys = base.keys.clone();
    merged.vals = merged_vals;
    let cid = collector.add(&merged);
    recorder.record_rewrite(&cid, &merged);
    Ok(Some(cid))
}

pub(crate) fn try_append_only_diff<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    other: &Tree,
) -> Result<Option<Vec<Diff>>, Error> {
    if base.root == other.root {
        return Ok(Some(Vec::new()));
    }

    let mut diffs = Vec::new();
    match (&base.root, &other.root) {
        (None, Some(other_cid)) => {
            collect_added_from_cid(prolly, other_cid, &mut diffs)?;
            Ok(Some(diffs))
        }
        (Some(_), None) => Ok(None),
        (Some(base_cid), Some(other_cid)) => {
            let nodes = prolly.load_many_ordered(&[base_cid.clone(), other_cid.clone()])?;
            let base_node = nodes[0].clone();
            let other_node = nodes[1].clone();
            if append_only_diff_nodes(prolly, &base_node, &other_node, &mut diffs)? {
                Ok(Some(diffs))
            } else {
                Ok(None)
            }
        }
        (None, None) => Ok(Some(Vec::new())),
    }
}

fn append_only_diff_nodes<S: Store>(
    prolly: &Prolly<S>,
    base: &Node,
    other: &Node,
    diffs: &mut Vec<Diff>,
) -> Result<bool, Error> {
    if other.level > base.level {
        if other.leaf || other.is_empty() {
            return Ok(false);
        }

        ensure_node_value_count(other)?;
        let first_child = child_cid_validated(other, 0)?;
        let first_child_node = prolly.load_arc(&first_child)?;
        if !append_only_diff_nodes(prolly, base, &first_child_node, diffs)? {
            return Ok(false);
        }

        for idx in 1..other.len() {
            let child = child_cid_validated(other, idx)?;
            collect_added_from_cid(prolly, &child, diffs)?;
        }

        return Ok(true);
    }

    if base.level != other.level || base.leaf != other.leaf || other.len() < base.len() {
        return Ok(false);
    }

    if base.leaf {
        ensure_node_value_count(base)?;
        ensure_node_value_count(other)?;

        for idx in 0..base.len() {
            if base.keys[idx] != other.keys[idx]
                || node_value(base, idx)? != node_value(other, idx)?
            {
                return Ok(false);
            }
        }

        for idx in base.len()..other.len() {
            diffs.push(Diff::Added {
                key: other.keys[idx].clone(),
                val: node_value(other, idx)?.clone(),
            });
        }

        return Ok(true);
    }

    if base.is_empty() {
        ensure_node_value_count(other)?;
        for idx in 0..other.len() {
            let child = child_cid_validated(other, idx)?;
            collect_added_from_cid(prolly, &child, diffs)?;
        }
        return Ok(true);
    }

    ensure_node_value_count(base)?;
    ensure_node_value_count(other)?;
    let right_edge_idx = base.len() - 1;
    for idx in 0..right_edge_idx {
        if base.keys[idx] != other.keys[idx]
            || child_cid_validated(base, idx)? != child_cid_validated(other, idx)?
        {
            return Ok(false);
        }
    }

    if base.keys[right_edge_idx] != other.keys[right_edge_idx] {
        return Ok(false);
    }

    let base_child = child_cid_validated(base, right_edge_idx)?;
    let other_child = child_cid_validated(other, right_edge_idx)?;
    if base_child != other_child {
        let nodes = prolly.load_many_ordered(&[base_child, other_child])?;
        let base_child_node = nodes[0].clone();
        let other_child_node = nodes[1].clone();
        if !append_only_diff_nodes(prolly, &base_child_node, &other_child_node, diffs)? {
            return Ok(false);
        }
    } else {
        prolly.load_arc(&base_child)?;
    }

    for idx in base.len()..other.len() {
        let child = child_cid_validated(other, idx)?;
        collect_added_from_cid(prolly, &child, diffs)?;
    }

    Ok(true)
}

fn merge_trees_with_right_diff<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    left: &Tree,
    right_diff: &[Diff],
    resolver: Option<Resolver>,
) -> Result<Tree, Error> {
    let mut recorder = MergeTraceRecorder::disabled();
    merge_trees_with_right_diff_traced(prolly, base, left, right_diff, resolver, &mut recorder)
}

fn merge_trees_with_right_diff_traced<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    left: &Tree,
    right_diff: &[Diff],
    resolver: Option<Resolver>,
    recorder: &mut MergeTraceRecorder<'_>,
) -> Result<Tree, Error> {
    let right_changes = build_merge_change_refs(right_diff);
    if right_changes_are_append_only_after(prolly, base, left, &right_changes)? {
        let mutations = right_changes
            .iter()
            .map(|entry| Mutation::Upsert {
                key: entry.key.to_vec(),
                val: entry
                    .value
                    .expect("append-only merge changes should contain values")
                    .to_vec(),
            })
            .collect::<Vec<_>>();
        recorder.record(MergeTraceEvent::BatchMerge {
            right_changes: right_changes.len(),
            mutations: mutations.len(),
            append_only: true,
        });
        return prolly.batch(left, mutations);
    }

    let mut mutations = Vec::with_capacity(right_changes.len());
    let keys = right_changes
        .iter()
        .map(|entry| entry.key)
        .collect::<Vec<_>>();
    let left_values = prolly.get_many(left, &keys)?;

    for (entry, left_val) in right_changes.iter().zip(left_values) {
        let key = entry.key;
        let base_val = entry.base;
        let right_val = entry.value;

        if left_val.as_deref() == base_val {
            push_change_mutation(&mut mutations, key, right_val);
            continue;
        }

        if option_bytes_eq(&left_val, right_val) {
            continue;
        }

        let conflict = build_conflict_from_values(
            key,
            base_val.map(|value| value.to_vec()),
            left_val,
            right_val.map(|value| value.to_vec()),
        );
        if let Some(ref resolve) = resolver {
            let resolution = resolve(&conflict);
            recorder.record_resolver(MergeTraceStage::Batch, &conflict, &resolution);
            match resolution {
                Resolution::Value(resolved) => {
                    mutations.push(Mutation::Upsert {
                        key: entry.key.to_vec(),
                        val: resolved,
                    });
                    continue;
                }
                Resolution::Delete => {
                    mutations.push(Mutation::Delete {
                        key: entry.key.to_vec(),
                    });
                    continue;
                }
                Resolution::Unresolved => {}
            }
        }

        return Err(Error::Conflict(conflict));
    }

    recorder.record(MergeTraceEvent::BatchMerge {
        right_changes: right_changes.len(),
        mutations: mutations.len(),
        append_only: false,
    });
    if mutations.is_empty() {
        Ok(left.clone())
    } else {
        prolly.batch(left, mutations)
    }
}

fn push_change_mutation(mutations: &mut Vec<Mutation>, key: &[u8], value: Option<&[u8]>) {
    match value {
        Some(val) => mutations.push(Mutation::Upsert {
            key: key.to_vec(),
            val: val.to_vec(),
        }),
        None => mutations.push(Mutation::Delete { key: key.to_vec() }),
    }
}

fn option_bytes_eq(left: &Option<Vec<u8>>, right: Option<&[u8]>) -> bool {
    left.as_deref() == right
}

fn right_changes_are_append_only_after<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    left: &Tree,
    right_changes: &[MergeChangeRef<'_>],
) -> Result<bool, Error> {
    let Some(first_key) = right_changes.iter().map(|entry| entry.key).min() else {
        return Ok(false);
    };

    if right_changes.iter().any(|entry| entry.value.is_none()) {
        return Ok(false);
    }

    Ok(key_is_after_tree(prolly, first_key, base)? && key_is_after_tree(prolly, first_key, left)?)
}

fn key_is_after_tree<S: Store>(prolly: &Prolly<S>, key: &[u8], tree: &Tree) -> Result<bool, Error> {
    let Some(root_cid) = &tree.root else {
        return Ok(true);
    };

    Ok(get_max_key(prolly, root_cid)?
        .as_deref()
        .map_or(true, |max_key| key > max_key))
}

/// Build a change map from a list of diffs.
///
/// Maps each key to its new value (Some for additions/changes, None for deletions).
#[cfg(test)]
pub(crate) fn build_change_map(diffs: &[Diff]) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    diffs
        .iter()
        .map(|d| match d {
            Diff::Added { key, val } | Diff::Changed { key, new: val, .. } => {
                (key.clone(), Some(val.clone()))
            }
            Diff::Removed { key, .. } => (key.clone(), None),
        })
        .collect()
}

fn build_merge_change_refs(diffs: &[Diff]) -> Vec<MergeChangeRef<'_>> {
    diffs
        .iter()
        .map(|d| match d {
            Diff::Added { key, val } => MergeChangeRef {
                key,
                base: None,
                value: Some(val),
            },
            Diff::Removed { key, val } => MergeChangeRef {
                key,
                base: Some(val),
                value: None,
            },
            Diff::Changed { key, old, new } => MergeChangeRef {
                key,
                base: Some(old),
                value: Some(new),
            },
        })
        .collect()
}

fn conflict_from_right_diff<S: Store>(
    prolly: &Prolly<S>,
    left: &Tree,
    diff: &Diff,
) -> Result<Option<Conflict>, Error> {
    let (key, base_val, right_val) = match diff {
        Diff::Added { key, val } => (key.as_slice(), None, Some(val.as_slice())),
        Diff::Removed { key, val } => (key.as_slice(), Some(val.as_slice()), None),
        Diff::Changed { key, old, new } => {
            (key.as_slice(), Some(old.as_slice()), Some(new.as_slice()))
        }
    };
    let left_val = prolly.get(left, key)?;

    if left_val.as_deref() == base_val || option_bytes_eq(&left_val, right_val) {
        return Ok(None);
    }

    Ok(Some(build_conflict_from_values(
        key,
        base_val.map(<[u8]>::to_vec),
        left_val,
        right_val.map(<[u8]>::to_vec),
    )))
}

#[cfg(feature = "async-store")]
async fn conflict_from_right_diff_async<S>(
    prolly: &AsyncProlly<S>,
    left: &Tree,
    diff: &Diff,
) -> Result<Option<Conflict>, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let (key, base_val, right_val) = match diff {
        Diff::Added { key, val } => (key.as_slice(), None, Some(val.as_slice())),
        Diff::Removed { key, val } => (key.as_slice(), Some(val.as_slice()), None),
        Diff::Changed { key, old, new } => {
            (key.as_slice(), Some(old.as_slice()), Some(new.as_slice()))
        }
    };
    let left_val = prolly.get(left, key).await?;

    if left_val.as_deref() == base_val || option_bytes_eq(&left_val, right_val) {
        return Ok(None);
    }

    Ok(Some(build_conflict_from_values(
        key,
        base_val.map(<[u8]>::to_vec),
        left_val,
        right_val.map(<[u8]>::to_vec),
    )))
}

fn build_conflict_from_values(
    key: &[u8],
    base: Option<Vec<u8>>,
    left: Option<Vec<u8>>,
    right: Option<Vec<u8>>,
) -> Conflict {
    Conflict {
        key: key.to_vec(),
        base,
        left,
        right,
    }
}

#[cfg(test)]
mod tests {
    use super::super::builder::BatchBuilder;
    use super::super::config::Config;
    use super::super::store::{BatchOp, MemStore, Store};
    use super::*;
    use std::collections::{BTreeMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

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
        blocked_get_keys: Mutex<HashSet<Vec<u8>>>,
        prefer_batch_reads: bool,
        get_calls: AtomicUsize,
        write_calls: AtomicUsize,
        batch_get_ordered_calls: AtomicUsize,
        max_batch_get_ordered_len: AtomicUsize,
    }

    impl CountingStore {
        fn reset_counts(&self) {
            self.get_calls.store(0, Ordering::Relaxed);
            self.write_calls.store(0, Ordering::Relaxed);
            self.batch_get_ordered_calls.store(0, Ordering::Relaxed);
            self.max_batch_get_ordered_len.store(0, Ordering::Relaxed);
        }

        fn block_get_key(&self, key: &[u8]) {
            self.blocked_get_keys.lock().unwrap().insert(key.to_vec());
        }

        fn clear_blocked_get_keys(&self) {
            self.blocked_get_keys.lock().unwrap().clear();
        }

        fn ensure_get_allowed(&self, key: &[u8]) -> Result<(), CountingStoreError> {
            if self.blocked_get_keys.lock().unwrap().contains(key) {
                return Err(CountingStoreError);
            }
            Ok(())
        }
    }

    impl Store for CountingStore {
        type Error = CountingStoreError;

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            self.ensure_get_allowed(key)?;
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.data.lock().unwrap().get(key).cloned())
        }

        fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            self.write_calls.fetch_add(1, Ordering::Relaxed);
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
            self.write_calls.fetch_add(1, Ordering::Relaxed);
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

        fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            for key in keys {
                self.ensure_get_allowed(key)?;
            }
            self.batch_get_ordered_calls.fetch_add(1, Ordering::Relaxed);
            self.max_batch_get_ordered_len
                .fetch_max(keys.len(), Ordering::Relaxed);
            let data = self.data.lock().unwrap();
            Ok(keys.iter().map(|key| data.get(*key).cloned()).collect())
        }

        fn batch_get_ordered_unique(
            &self,
            keys: &[&[u8]],
        ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            self.batch_get_ordered(keys)
        }

        fn prefers_batch_reads(&self) -> bool {
            self.prefer_batch_reads
        }
    }

    fn malformed_internal_and_valid_peer(
        prolly: &Prolly<Arc<CountingStore>>,
        config: Config,
    ) -> (Tree, Tree) {
        let mut malformed_root = prolly.new_internal_node(1);
        malformed_root.keys.push(b"a".to_vec());

        let mut leaf = prolly.new_leaf_node();
        leaf.keys.push(b"a".to_vec());
        leaf.vals.push(b"1".to_vec());
        let leaf_cid = prolly.save(&leaf).unwrap();

        let mut valid_root = prolly.new_internal_node(1);
        valid_root.keys.push(b"a".to_vec());
        valid_root.vals.push(leaf_cid.0.to_vec());

        (
            Tree {
                root: Some(prolly.save(&malformed_root).unwrap()),
                config: config.clone(),
            },
            Tree {
                root: Some(prolly.save(&valid_root).unwrap()),
                config,
            },
        )
    }

    fn malformed_leaf_tree(prolly: &Prolly<Arc<CountingStore>>, config: Config) -> Tree {
        let mut malformed_leaf = prolly.new_leaf_node();
        malformed_leaf.keys.push(b"a".to_vec());

        Tree {
            root: Some(prolly.save(&malformed_leaf).unwrap()),
            config,
        }
    }

    fn valid_leaf_tree(prolly: &Prolly<Arc<CountingStore>>, config: Config, value: &[u8]) -> Tree {
        let mut valid_leaf = prolly.new_leaf_node();
        valid_leaf.keys.push(b"a".to_vec());
        valid_leaf.vals.push(value.to_vec());

        Tree {
            root: Some(prolly.save(&valid_leaf).unwrap()),
            config,
        }
    }

    #[test]
    fn diff_rejects_internal_node_with_missing_child_cid() {
        let store = Arc::new(CountingStore::default());
        let config = Config::default();
        let prolly = Prolly::new(store, config.clone());
        let (malformed, valid) = malformed_internal_and_valid_peer(&prolly, config);

        let err = compute_diff(&prolly, &malformed, &valid).unwrap_err();

        assert!(matches!(err, Error::InvalidNode));
    }

    #[test]
    fn stream_diff_rejects_internal_node_with_missing_child_cid() {
        let store = Arc::new(CountingStore::default());
        let config = Config::default();
        let prolly = Prolly::new(store, config.clone());
        let (malformed, valid) = malformed_internal_and_valid_peer(&prolly, config);

        let err = stream_diff(&prolly, &malformed, &valid)
            .collect::<Result<Vec<_>, _>>()
            .unwrap_err();

        assert!(matches!(err, Error::InvalidNode));
    }

    #[test]
    fn diff_rejects_leaf_with_mismatched_values() {
        let store = Arc::new(CountingStore::default());
        let config = Config::default();
        let prolly = Prolly::new(store, config.clone());
        let malformed = malformed_leaf_tree(&prolly, config.clone());
        let valid = valid_leaf_tree(&prolly, config, b"1");

        let err = compute_diff(&prolly, &malformed, &valid).unwrap_err();

        assert!(matches!(err, Error::InvalidNode));
    }

    #[test]
    fn range_diff_rejects_leaf_with_mismatched_values() {
        let store = Arc::new(CountingStore::default());
        let config = Config::default();
        let prolly = Prolly::new(store, config.clone());
        let malformed = malformed_leaf_tree(&prolly, config.clone());
        let valid = valid_leaf_tree(&prolly, config, b"1");

        let err = compute_range_diff(&prolly, &malformed, &valid, b"a", None).unwrap_err();

        assert!(matches!(err, Error::InvalidNode));
    }

    #[test]
    fn range_diff_batches_node_pairs_for_batched_read_stores() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(u32::MAX)
            .build();
        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for i in 0..128 {
            builder.add(
                format!("k{i:03}").into_bytes(),
                format!("v{i:03}").into_bytes(),
            );
        }
        let base = builder.build().unwrap();
        let prolly = Prolly::new(store.clone(), config);
        let other = prolly
            .batch(
                &base,
                (0..128)
                    .step_by(17)
                    .map(|i| Mutation::Upsert {
                        key: format!("k{i:03}").into_bytes(),
                        val: format!("changed-{i:03}").into_bytes(),
                    })
                    .collect(),
            )
            .unwrap();

        prolly.clear_cache();
        store.reset_counts();
        let diffs = compute_range_diff(&prolly, &base, &other, b"k020", Some(b"k090")).unwrap();

        assert_eq!(diffs.len(), 4);
        assert!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed) > 0,
            "range diff should hydrate compared node pairs through ordered batched reads"
        );
        assert!(
            store.max_batch_get_ordered_len.load(Ordering::Relaxed) >= 2,
            "range diff should batch at least the base/other pair at each compare step"
        );
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            0,
            "range diff on batched-read stores should avoid point reads after cache clear"
        );
    }

    #[test]
    fn overlapping_child_cids_seeks_to_range_start_and_keeps_previous_span() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store, Config::default());
        let mut node = prolly.new_internal_node(1);
        let cids = (0..10)
            .map(|idx| {
                node.keys.push(format!("k{:03}", idx * 10).into_bytes());
                Cid::from_bytes(format!("child-{idx:03}").as_bytes())
            })
            .collect::<Vec<_>>();
        node.vals = cids.iter().map(|cid| cid.0.to_vec()).collect();

        let overlapping = overlapping_child_cids(&node, None, b"k055", Some(b"k075")).unwrap();

        assert_eq!(
            overlapping
                .iter()
                .map(|(_, cid)| cid.clone())
                .collect::<Vec<_>>(),
            vec![cids[5].clone(), cids[6].clone(), cids[7].clone()]
        );
    }

    #[test]
    fn range_diff_child_start_index_seeks_to_previous_possible_span() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store, Config::default());
        let mut node = prolly.new_internal_node(1);
        node.keys = (0..10)
            .map(|idx| format!("k{:03}", idx * 10).into_bytes())
            .collect();
        node.vals = (0..10)
            .map(|idx| {
                Cid::from_bytes(format!("child-{idx:03}").as_bytes())
                    .0
                    .to_vec()
            })
            .collect();

        assert_eq!(first_potentially_overlapping_child_index(&node, b"a"), 0);
        assert_eq!(first_potentially_overlapping_child_index(&node, b"k000"), 0);
        assert_eq!(first_potentially_overlapping_child_index(&node, b"k001"), 0);
        assert_eq!(first_potentially_overlapping_child_index(&node, b"k055"), 5);
        assert_eq!(first_potentially_overlapping_child_index(&node, b"k090"), 8);
        assert_eq!(first_potentially_overlapping_child_index(&node, b"z"), 9);
    }

    #[test]
    fn range_diff_child_index_range_bounds_narrow_ranges() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store, Config::default());
        let mut node = prolly.new_internal_node(1);
        node.keys = (0..10)
            .map(|idx| format!("k{:03}", idx * 10).into_bytes())
            .collect();
        node.vals = (0..10)
            .map(|idx| {
                Cid::from_bytes(format!("child-{idx:03}").as_bytes())
                    .0
                    .to_vec()
            })
            .collect();

        assert_eq!(
            potentially_overlapping_child_index_range(&node, b"k055", Some(b"k075")),
            5..8
        );
        assert_eq!(
            potentially_overlapping_child_index_range(&node, b"k060", Some(b"k070")),
            5..7
        );
        assert_eq!(
            potentially_overlapping_child_index_range(&node, b"a", Some(b"k000")),
            0..0
        );
        assert_eq!(
            potentially_overlapping_child_index_range(&node, b"k090", None),
            8..10
        );
        assert_eq!(
            potentially_overlapping_child_index_range(&node, b"z", None),
            9..10
        );
    }

    #[test]
    fn overlapping_child_cids_returns_empty_when_range_ends_before_first_child() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store, Config::default());
        let mut node = prolly.new_internal_node(1);
        let cids = (0..3)
            .map(|idx| {
                node.keys.push(format!("k{:03}", idx * 10).into_bytes());
                Cid::from_bytes(format!("child-{idx:03}").as_bytes())
            })
            .collect::<Vec<_>>();
        node.vals = cids.iter().map(|cid| cid.0.to_vec()).collect();

        let overlapping = overlapping_child_cids(&node, None, b"a", Some(b"k000")).unwrap();

        assert!(overlapping.is_empty());
    }

    #[test]
    fn child_diff_frames_preserve_child_order_and_kind() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store, Config::default());
        let mut node = prolly.new_internal_node(1);
        let cids = (0..4)
            .map(|idx| {
                node.keys.push(format!("k{idx:03}").into_bytes());
                Cid::from_bytes(format!("child-{idx:03}").as_bytes())
            })
            .collect::<Vec<_>>();
        node.vals = cids.iter().map(|cid| cid.0.to_vec()).collect();

        let added = child_diff_frames(&node, DiffFrameKind::Added).unwrap();
        let removed = child_diff_frames(&node, DiffFrameKind::Removed).unwrap();

        for (frame, expected_cid) in added.iter().zip(&cids) {
            match frame {
                DiffFrame::Added { cid } => assert_eq!(cid, expected_cid),
                _ => panic!("added child frames should keep added frame kind"),
            }
        }
        for (frame, expected_cid) in removed.iter().zip(&cids) {
            match frame {
                DiffFrame::Removed { cid } => assert_eq!(cid, expected_cid),
                _ => panic!("removed child frames should keep removed frame kind"),
            }
        }
    }

    #[test]
    fn child_diff_frames_reject_malformed_internal_node() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store, Config::default());
        let mut node = prolly.new_internal_node(1);
        node.keys.push(b"k000".to_vec());

        let err = match child_diff_frames(&node, DiffFrameKind::Added) {
            Ok(_) => panic!("malformed internal node should be rejected"),
            Err(err) => err,
        };

        assert!(matches!(err, Error::InvalidNode));
    }

    #[test]
    fn stream_diff_rejects_leaf_with_mismatched_values() {
        let store = Arc::new(CountingStore::default());
        let config = Config::default();
        let prolly = Prolly::new(store, config.clone());
        let malformed = malformed_leaf_tree(&prolly, config.clone());
        let valid = valid_leaf_tree(&prolly, config, b"1");

        let err = stream_diff(&prolly, &malformed, &valid)
            .collect::<Result<Vec<_>, _>>()
            .unwrap_err();

        assert!(matches!(err, Error::InvalidNode));
    }

    #[test]
    fn append_only_diff_rejects_leaf_with_mismatched_values() {
        let store = Arc::new(CountingStore::default());
        let config = Config::default();
        let prolly = Prolly::new(store, config.clone());
        let malformed = malformed_leaf_tree(&prolly, config.clone());
        let valid = valid_leaf_tree(&prolly, config, b"1");

        let err = try_append_only_diff(&prolly, &malformed, &valid).unwrap_err();

        assert!(matches!(err, Error::InvalidNode));
    }

    #[test]
    fn structural_merge_rejects_leaf_with_mismatched_values() {
        let store = Arc::new(CountingStore::default());
        let config = Config::default();
        let prolly = Prolly::new(store, config.clone());
        let malformed = malformed_leaf_tree(&prolly, config.clone());
        let left = valid_leaf_tree(&prolly, config.clone(), b"left");
        let right = valid_leaf_tree(&prolly, config, b"right");

        let err = try_structural_merge(&prolly, &malformed, &left, &right, None).unwrap_err();

        assert!(matches!(err, Error::InvalidNode));
    }

    #[test]
    fn diff_collectors_batch_hydrate_added_subtrees() {
        let store = Arc::new(CountingStore::default());
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(u32::MAX)
            .build();
        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for i in 0..96 {
            builder.add(
                format!("k{i:03}").into_bytes(),
                format!("v{i:03}").into_bytes(),
            );
        }
        let tree = builder.build().unwrap();
        let empty = Tree {
            root: None,
            config: config.clone(),
        };

        let prolly = Prolly::new(store.clone(), config);
        store.reset_counts();
        let diffs = compute_diff(&prolly, &empty, &tree).unwrap();

        assert_eq!(diffs.len(), 96);
        assert!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed) > 0,
            "added subtree collection should hydrate internal children in ordered batches"
        );
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            1,
            "only the added root should require a single-key get"
        );
    }

    #[test]
    fn diff_collectors_split_wide_child_hydration_for_batched_read_stores() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let prolly = Prolly::new(store.clone(), Config::default());
        let mut child_cids = Vec::new();

        for idx in 0..64 {
            let mut leaf = prolly.new_leaf_node();
            leaf.keys.push(format!("k{idx:03}").into_bytes());
            leaf.vals.push(format!("v{idx:03}").into_bytes());
            child_cids.push(prolly.save(&leaf).unwrap());
        }

        let mut root = prolly.new_internal_node(1);
        root.keys = (0..64)
            .map(|idx| format!("k{idx:03}").into_bytes())
            .collect();
        root.vals = child_cids.into_iter().map(|cid| cid.0.to_vec()).collect();
        let tree = Tree {
            root: Some(prolly.save(&root).unwrap()),
            config: Config::default(),
        };
        let empty = Tree {
            root: None,
            config: Config::default(),
        };

        prolly.clear_cache();
        store.reset_counts();
        let diffs = compute_diff(&prolly, &empty, &tree).unwrap();

        assert_eq!(diffs.len(), 64);
        assert!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed) >= DIFF_COLLECTION_PREFETCH_PARALLELISM,
            "wide added-subtree collection should split child hydration into parallel ordered batches"
        );
        assert!(
            store.max_batch_get_ordered_len.load(Ordering::Relaxed) <= 4,
            "64 children at parallelism 16 should hydrate in bounded chunks"
        );
    }

    #[test]
    fn structural_stream_diff_skips_unchanged_subtrees() {
        let store = Arc::new(CountingStore::default());
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(u32::MAX)
            .build();
        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for i in 0..256 {
            builder.add(
                format!("k{i:03}").into_bytes(),
                format!("v{i:03}").into_bytes(),
            );
        }
        let base = builder.build().unwrap();
        let prolly = Prolly::new(store.clone(), config);
        let other = prolly
            .put(&base, b"k173".to_vec(), b"changed-173".to_vec())
            .unwrap();

        prolly.clear_cache();
        store.reset_counts();
        let diffs = stream_diff(&prolly, &base, &other)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            diffs,
            vec![Diff::Changed {
                key: b"k173".to_vec(),
                old: b"v173".to_vec(),
                new: b"changed-173".to_vec(),
            }]
        );
        assert!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed) > 0,
            "structural streaming should hydrate compared node pairs in ordered batches"
        );
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            0,
            "structural streaming should not cursor-scan entries with single-key gets"
        );
    }

    #[test]
    fn structural_stream_diff_prefetches_sibling_frames_for_batched_stores() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(u32::MAX)
            .build();
        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for i in 0..512 {
            builder.add(
                format!("k{i:03}").into_bytes(),
                format!("v{i:03}").into_bytes(),
            );
        }
        let base = builder.build().unwrap();
        let prolly = Prolly::new(store.clone(), config);
        let other = prolly
            .batch(
                &base,
                (0..512)
                    .step_by(29)
                    .map(|i| Mutation::Upsert {
                        key: format!("k{i:03}").into_bytes(),
                        val: format!("changed-{i:03}").into_bytes(),
                    })
                    .collect(),
            )
            .unwrap();

        prolly.clear_cache();
        store.reset_counts();
        let diffs = stream_diff(&prolly, &base, &other)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(diffs.len(), 18);
        assert!(
            store.max_batch_get_ordered_len.load(Ordering::Relaxed) > 2,
            "batched-read stores should prefetch sibling diff frames wider than one node pair"
        );
    }

    #[test]
    fn structural_stream_diff_splits_wide_frame_prefetch_for_batched_stores() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let prolly = Prolly::new(store.clone(), Config::default());

        let mut base_child_cids = Vec::new();
        let mut other_child_cids = Vec::new();
        for idx in 0..64 {
            let key = format!("k{idx:03}").into_bytes();

            let mut base_leaf = prolly.new_leaf_node();
            base_leaf.keys.push(key.clone());
            base_leaf.vals.push(format!("base-{idx:03}").into_bytes());
            base_child_cids.push(prolly.save(&base_leaf).unwrap());

            let mut other_leaf = prolly.new_leaf_node();
            other_leaf.keys.push(key);
            other_leaf.vals.push(format!("other-{idx:03}").into_bytes());
            other_child_cids.push(prolly.save(&other_leaf).unwrap());
        }

        let keys = (0..64)
            .map(|idx| format!("k{idx:03}").into_bytes())
            .collect::<Vec<_>>();
        let mut base_root = prolly.new_internal_node(1);
        base_root.keys = keys.clone();
        base_root.vals = base_child_cids
            .into_iter()
            .map(|cid| cid.0.to_vec())
            .collect();
        let mut other_root = prolly.new_internal_node(1);
        other_root.keys = keys;
        other_root.vals = other_child_cids
            .into_iter()
            .map(|cid| cid.0.to_vec())
            .collect();

        let base = Tree {
            root: Some(prolly.save(&base_root).unwrap()),
            config: Config::default(),
        };
        let other = Tree {
            root: Some(prolly.save(&other_root).unwrap()),
            config: Config::default(),
        };

        prolly.clear_cache();
        store.reset_counts();
        let diffs = stream_diff(&prolly, &base, &other)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(diffs.len(), 64);
        assert!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed)
                >= DIFF_FRAME_PREFETCH_PARALLELISM,
            "wide structural diff frame prefetch should split into parallel ordered batches"
        );
        assert!(
            store.max_batch_get_ordered_len.load(Ordering::Relaxed) <= 8,
            "128 frame-root CIDs at parallelism 16 should hydrate in bounded chunks"
        );
    }

    #[test]
    fn eager_diff_uses_structural_prefetch_for_batched_stores() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(u32::MAX)
            .build();
        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for i in 0..512 {
            builder.add(
                format!("k{i:03}").into_bytes(),
                format!("v{i:03}").into_bytes(),
            );
        }
        let base = builder.build().unwrap();
        let prolly = Prolly::new(store.clone(), config);
        let other = prolly
            .batch(
                &base,
                (0..512)
                    .step_by(29)
                    .map(|i| Mutation::Upsert {
                        key: format!("k{i:03}").into_bytes(),
                        val: format!("changed-{i:03}").into_bytes(),
                    })
                    .collect(),
            )
            .unwrap();

        prolly.clear_cache();
        store.reset_counts();
        let diffs = compute_diff(&prolly, &base, &other).unwrap();

        assert_eq!(diffs.len(), 18);
        assert!(
            store.max_batch_get_ordered_len.load(Ordering::Relaxed) > 2,
            "eager diff should reuse structural sibling prefetch for batched-read stores"
        );
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            0,
            "eager structural diff should avoid point reads after append-only fallback"
        );
    }

    #[test]
    fn test_compute_diff_same_tree() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let tree = prolly.create();

        let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();

        // Diff of same tree should be empty
        let diffs = compute_diff(&prolly, &tree, &tree).unwrap();
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_compute_diff_added_entries() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let other = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();

        let diffs = compute_diff(&prolly, &base, &other).unwrap();

        assert_eq!(diffs.len(), 1);
        assert!(matches!(
            &diffs[0],
            Diff::Added { key, val } if key == b"b" && val == b"2"
        ));
    }

    #[test]
    fn test_compute_diff_removed_entries() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let base = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
        let other = prolly.delete(&base, b"b").unwrap();

        let diffs = compute_diff(&prolly, &base, &other).unwrap();

        assert_eq!(diffs.len(), 1);
        assert!(matches!(
            &diffs[0],
            Diff::Removed { key, val } if key == b"b" && val == b"2"
        ));
    }

    #[test]
    fn test_compute_diff_changed_entries() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let other = prolly.put(&base, b"a".to_vec(), b"2".to_vec()).unwrap();

        let diffs = compute_diff(&prolly, &base, &other).unwrap();

        assert_eq!(diffs.len(), 1);
        assert!(matches!(
            &diffs[0],
            Diff::Changed { key, old, new } if key == b"a" && old == b"1" && new == b"2"
        ));
    }

    #[test]
    fn append_only_diff_detects_right_suffix_additions() {
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(u32::MAX)
            .hash_seed(31)
            .build();
        let prolly = Prolly::new(MemStore::new(), config);
        let mut base = prolly.create();

        for i in 0..32 {
            base = prolly
                .put(
                    &base,
                    format!("k{i:03}").into_bytes(),
                    format!("v{i:03}").into_bytes(),
                )
                .unwrap();
        }

        let mut other = base.clone();
        for i in 32..48 {
            other = prolly
                .put(
                    &other,
                    format!("k{i:03}").into_bytes(),
                    format!("v{i:03}").into_bytes(),
                )
                .unwrap();
        }

        let diffs = try_append_only_diff(&prolly, &base, &other)
            .unwrap()
            .unwrap();
        assert_eq!(diffs.len(), 16);
        assert!(diffs.iter().all(|diff| matches!(diff, Diff::Added { .. })));
        assert!(matches!(
            &diffs[0],
            Diff::Added { key, val } if key == b"k032" && val == b"v032"
        ));

        assert_eq!(compute_diff(&prolly, &base, &other).unwrap(), diffs);
    }

    #[test]
    fn append_only_diff_batches_changed_right_edge_child_pair() {
        let store = Arc::new(CountingStore::default());
        let config = Config::default();
        let prolly = Prolly::new(store.clone(), config.clone());

        let mut leaf_a = prolly.new_leaf_node();
        leaf_a.keys.push(b"a".to_vec());
        leaf_a.vals.push(b"1".to_vec());
        let leaf_a_cid = prolly.save(&leaf_a).unwrap();

        let mut leaf_b = prolly.new_leaf_node();
        leaf_b.keys.push(b"b".to_vec());
        leaf_b.vals.push(b"2".to_vec());
        let leaf_b_cid = prolly.save(&leaf_b).unwrap();

        let mut leaf_bc = prolly.new_leaf_node();
        leaf_bc.keys = vec![b"b".to_vec(), b"c".to_vec()];
        leaf_bc.vals = vec![b"2".to_vec(), b"3".to_vec()];
        let leaf_bc_cid = prolly.save(&leaf_bc).unwrap();

        let mut base_root = prolly.new_internal_node(1);
        base_root.keys = vec![b"a".to_vec(), b"b".to_vec()];
        base_root.vals = vec![leaf_a_cid.0.to_vec(), leaf_b_cid.0.to_vec()];
        let base_root_cid = prolly.save(&base_root).unwrap();

        let mut other_root = prolly.new_internal_node(1);
        other_root.keys = vec![b"a".to_vec(), b"b".to_vec()];
        other_root.vals = vec![leaf_a_cid.0.to_vec(), leaf_bc_cid.0.to_vec()];
        let other_root_cid = prolly.save(&other_root).unwrap();

        let base = Tree {
            root: Some(base_root_cid),
            config: config.clone(),
        };
        let other = Tree {
            root: Some(other_root_cid),
            config,
        };

        prolly.clear_cache();
        store.reset_counts();
        let diffs = try_append_only_diff(&prolly, &base, &other)
            .unwrap()
            .unwrap();

        assert_eq!(
            diffs,
            vec![Diff::Added {
                key: b"c".to_vec(),
                val: b"3".to_vec()
            }]
        );
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            0,
            "changed right-edge child nodes should be loaded through ordered batches"
        );
        assert_eq!(store.batch_get_ordered_calls.load(Ordering::Relaxed), 2);
        assert_eq!(store.max_batch_get_ordered_len.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn append_only_diff_rejects_existing_key_updates() {
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(u32::MAX)
            .hash_seed(37)
            .build();
        let prolly = Prolly::new(MemStore::new(), config);
        let mut base = prolly.create();

        for i in 0..32 {
            base = prolly
                .put(
                    &base,
                    format!("k{i:03}").into_bytes(),
                    format!("v{i:03}").into_bytes(),
                )
                .unwrap();
        }

        let other = prolly
            .put(&base, b"k010".to_vec(), b"updated".to_vec())
            .unwrap();

        assert!(try_append_only_diff(&prolly, &base, &other)
            .unwrap()
            .is_none());

        assert!(matches!(
            &compute_diff(&prolly, &base, &other).unwrap()[0],
            Diff::Changed { key, old, new }
                if key == b"k010" && old == b"v010" && new == b"updated"
        ));
    }

    #[test]
    fn test_merge_trees_no_conflicts() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let left = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
        let right = prolly.put(&base, b"c".to_vec(), b"3".to_vec()).unwrap();

        let merged = merge_trees(&prolly, &base, &left, &right, None).unwrap();

        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(prolly.get(&merged, b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(prolly.get(&merged, b"c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn merge_returns_changed_branch_without_reads_when_other_branch_unchanged() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store.clone(), Config::default());
        let base = prolly
            .put(&prolly.create(), b"a".to_vec(), b"1".to_vec())
            .unwrap();
        let right = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();

        prolly.clear_cache();
        store.reset_counts();
        let merged = merge_trees(&prolly, &base, &base, &right, None).unwrap();

        assert_eq!(merged.root, right.root);
        assert_eq!(store.get_calls.load(Ordering::Relaxed), 0);
        assert_eq!(store.batch_get_ordered_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn structural_merge_reuses_disjoint_changed_subtrees_without_reading_them() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let prolly = Prolly::new(store.clone(), Config::default());

        let mut base_left_leaf = prolly.new_leaf_node();
        base_left_leaf.keys.push(b"a".to_vec());
        base_left_leaf.vals.push(b"1".to_vec());
        let base_left_leaf_cid = prolly.save(&base_left_leaf).unwrap();

        let mut base_right_leaf = prolly.new_leaf_node();
        base_right_leaf.keys.push(b"m".to_vec());
        base_right_leaf.vals.push(b"1".to_vec());
        let base_right_leaf_cid = prolly.save(&base_right_leaf).unwrap();

        let mut base_root = prolly.new_internal_node(1);
        base_root.keys = vec![b"a".to_vec(), b"m".to_vec()];
        base_root.vals = vec![
            base_left_leaf_cid.0.to_vec(),
            base_right_leaf_cid.0.to_vec(),
        ];
        let base_root_cid = prolly.save(&base_root).unwrap();

        let mut left_leaf = prolly.new_leaf_node();
        left_leaf.keys.push(b"a".to_vec());
        left_leaf.vals.push(b"left".to_vec());
        let left_leaf_cid = prolly.save(&left_leaf).unwrap();

        let mut left_root = prolly.new_internal_node(1);
        left_root.keys = base_root.keys.clone();
        left_root.vals = vec![left_leaf_cid.0.to_vec(), base_right_leaf_cid.0.to_vec()];
        let left_root_cid = prolly.save(&left_root).unwrap();

        let mut right_leaf = prolly.new_leaf_node();
        right_leaf.keys.push(b"m".to_vec());
        right_leaf.vals.push(b"right".to_vec());
        let right_leaf_cid = prolly.save(&right_leaf).unwrap();

        let mut right_root = prolly.new_internal_node(1);
        right_root.keys = base_root.keys.clone();
        right_root.vals = vec![base_left_leaf_cid.0.to_vec(), right_leaf_cid.0.to_vec()];
        let right_root_cid = prolly.save(&right_root).unwrap();

        let base = Tree {
            root: Some(base_root_cid),
            config: Config::default(),
        };
        let left = Tree {
            root: Some(left_root_cid),
            config: Config::default(),
        };
        let right = Tree {
            root: Some(right_root_cid),
            config: Config::default(),
        };

        store.block_get_key(left_leaf_cid.as_bytes());
        store.block_get_key(right_leaf_cid.as_bytes());
        prolly.clear_cache();
        store.reset_counts();

        let merged = merge_trees(&prolly, &base, &left, &right, None).unwrap();

        assert!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed) > 0,
            "structural merge should batch-read only the internal merge frontier"
        );

        store.clear_blocked_get_keys();
        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"left".to_vec()));
        assert_eq!(prolly.get(&merged, b"m").unwrap(), Some(b"right".to_vec()));
    }

    #[test]
    fn structural_merge_caches_written_root_for_immediate_reads() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let prolly = Prolly::new(store.clone(), Config::default());

        let mut base_left_leaf = prolly.new_leaf_node();
        base_left_leaf.keys.push(b"a".to_vec());
        base_left_leaf.vals.push(b"1".to_vec());
        let base_left_leaf_cid = prolly.save(&base_left_leaf).unwrap();

        let mut base_right_leaf = prolly.new_leaf_node();
        base_right_leaf.keys.push(b"m".to_vec());
        base_right_leaf.vals.push(b"1".to_vec());
        let base_right_leaf_cid = prolly.save(&base_right_leaf).unwrap();

        let mut base_root = prolly.new_internal_node(1);
        base_root.keys = vec![b"a".to_vec(), b"m".to_vec()];
        base_root.vals = vec![
            base_left_leaf_cid.0.to_vec(),
            base_right_leaf_cid.0.to_vec(),
        ];
        let base_root_cid = prolly.save(&base_root).unwrap();

        let mut left_leaf = prolly.new_leaf_node();
        left_leaf.keys.push(b"a".to_vec());
        left_leaf.vals.push(b"left".to_vec());
        let left_leaf_cid = prolly.save(&left_leaf).unwrap();

        let mut left_root = prolly.new_internal_node(1);
        left_root.keys = base_root.keys.clone();
        left_root.vals = vec![left_leaf_cid.0.to_vec(), base_right_leaf_cid.0.to_vec()];
        let left_root_cid = prolly.save(&left_root).unwrap();

        let mut right_leaf = prolly.new_leaf_node();
        right_leaf.keys.push(b"m".to_vec());
        right_leaf.vals.push(b"right".to_vec());
        let right_leaf_cid = prolly.save(&right_leaf).unwrap();

        let mut right_root = prolly.new_internal_node(1);
        right_root.keys = base_root.keys.clone();
        right_root.vals = vec![base_left_leaf_cid.0.to_vec(), right_leaf_cid.0.to_vec()];
        let right_root_cid = prolly.save(&right_root).unwrap();

        let base = Tree {
            root: Some(base_root_cid),
            config: Config::default(),
        };
        let left = Tree {
            root: Some(left_root_cid),
            config: Config::default(),
        };
        let right = Tree {
            root: Some(right_root_cid),
            config: Config::default(),
        };

        prolly.clear_cache();
        let merged = merge_trees(&prolly, &base, &left, &right, None).unwrap();
        let merged_root = merged.root.clone().unwrap();

        store.block_get_key(merged_root.as_bytes());
        store.reset_counts();
        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"left".to_vec()));
        assert!(
            store.get_calls.load(Ordering::Relaxed) <= 1,
            "the merged root should come from cache; only the reused leaf may need a store read"
        );
        store.clear_blocked_get_keys();
    }

    #[test]
    fn structural_merge_reuses_resolved_base_leaf_without_writes() {
        let store = Arc::new(CountingStore::default());
        let prolly = Prolly::new(store.clone(), Config::default());
        let base = prolly
            .put(&prolly.create(), b"a".to_vec(), b"base".to_vec())
            .unwrap();
        let left = prolly.put(&base, b"a".to_vec(), b"left".to_vec()).unwrap();
        let right = prolly.put(&base, b"a".to_vec(), b"right".to_vec()).unwrap();
        let resolver: Resolver = Box::new(|conflict| {
            conflict
                .base
                .clone()
                .map_or_else(Resolution::delete, Resolution::value)
        });

        store.reset_counts();
        let merged = merge_trees(&prolly, &base, &left, &right, Some(resolver)).unwrap();

        assert_eq!(merged.root, base.root);
        assert_eq!(
            store.write_calls.load(Ordering::Relaxed),
            0,
            "structural merge should reuse an existing resolved leaf instead of writing a clone"
        );
    }

    #[test]
    fn merge_lookup_splits_wide_frontiers_for_batched_read_stores() {
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

        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for idx in 0..4096 {
            builder.add(key_for(idx), format!("v{idx:04}").into_bytes());
        }
        let tree = builder.build().unwrap();
        let prolly = Prolly::new(store.clone(), config);
        let keys = (0..4096).step_by(8).map(key_for).collect::<Vec<_>>();

        prolly.clear_cache();
        store.reset_counts();
        let values = prolly.get_many(&tree, &keys).unwrap();

        assert_eq!(values.len(), keys.len());
        for (idx, value) in values.into_iter().enumerate() {
            assert_eq!(value, Some(format!("v{:04}", idx * 8).into_bytes()));
        }
        assert!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed) > 16,
            "wide get_many lookups should split frontier reads into parallel ordered batches"
        );
        assert!(
            store.max_batch_get_ordered_len.load(Ordering::Relaxed) <= 64,
            "bounded parallel lookup should avoid one huge ordered batch for hundreds of misses"
        );
    }

    #[test]
    fn structural_merge_prefetches_sibling_merge_frontier() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let prolly = Prolly::new(store.clone(), Config::default());

        let save_leaf = |key: &[u8], val: &[u8]| {
            let mut leaf = prolly.new_leaf_node();
            leaf.keys.push(key.to_vec());
            leaf.vals.push(val.to_vec());
            prolly.save(&leaf).unwrap()
        };
        let save_internal = |level, keys: Vec<Vec<u8>>, child_cids: Vec<Cid>| {
            let mut node = prolly.new_internal_node(level);
            node.keys = keys;
            node.vals = child_cids
                .into_iter()
                .map(|cid| cid.0.to_vec())
                .collect::<Vec<_>>();
            prolly.save(&node).unwrap()
        };

        let base_a = save_leaf(b"a", b"base-a");
        let base_g = save_leaf(b"g", b"base-g");
        let base_m = save_leaf(b"m", b"base-m");
        let base_t = save_leaf(b"t", b"base-t");
        let base_left_internal = save_internal(
            1,
            vec![b"a".to_vec(), b"g".to_vec()],
            vec![base_a.clone(), base_g.clone()],
        );
        let base_right_internal = save_internal(
            1,
            vec![b"m".to_vec(), b"t".to_vec()],
            vec![base_m.clone(), base_t.clone()],
        );
        let base_root = save_internal(
            2,
            vec![b"a".to_vec(), b"m".to_vec()],
            vec![base_left_internal.clone(), base_right_internal.clone()],
        );

        let left_a = save_leaf(b"a", b"left-a");
        let left_m = save_leaf(b"m", b"left-m");
        let left_left_internal = save_internal(
            1,
            vec![b"a".to_vec(), b"g".to_vec()],
            vec![left_a, base_g.clone()],
        );
        let left_right_internal = save_internal(
            1,
            vec![b"m".to_vec(), b"t".to_vec()],
            vec![left_m, base_t.clone()],
        );
        let left_root = save_internal(
            2,
            vec![b"a".to_vec(), b"m".to_vec()],
            vec![left_left_internal, left_right_internal],
        );

        let right_g = save_leaf(b"g", b"right-g");
        let right_t = save_leaf(b"t", b"right-t");
        let right_left_internal =
            save_internal(1, vec![b"a".to_vec(), b"g".to_vec()], vec![base_a, right_g]);
        let right_right_internal =
            save_internal(1, vec![b"m".to_vec(), b"t".to_vec()], vec![base_m, right_t]);
        let right_root = save_internal(
            2,
            vec![b"a".to_vec(), b"m".to_vec()],
            vec![right_left_internal, right_right_internal],
        );

        let base = Tree {
            root: Some(base_root),
            config: Config::default(),
        };
        let left = Tree {
            root: Some(left_root),
            config: Config::default(),
        };
        let right = Tree {
            root: Some(right_root),
            config: Config::default(),
        };

        prolly.clear_cache();
        store.reset_counts();
        let merged = merge_trees(&prolly, &base, &left, &right, None).unwrap();

        assert!(
            store.max_batch_get_ordered_len.load(Ordering::Relaxed) >= 6,
            "structural merge should prefetch sibling child triples wider than one subtree"
        );
        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"left-a".to_vec()));
        assert_eq!(
            prolly.get(&merged, b"g").unwrap(),
            Some(b"right-g".to_vec())
        );
        assert_eq!(prolly.get(&merged, b"m").unwrap(), Some(b"left-m".to_vec()));
        assert_eq!(
            prolly.get(&merged, b"t").unwrap(),
            Some(b"right-t".to_vec())
        );
    }

    #[test]
    fn structural_merge_splits_wide_frontier_prefetch_for_batched_stores() {
        let store = Arc::new(CountingStore {
            prefer_batch_reads: true,
            ..CountingStore::default()
        });
        let prolly = Prolly::new(store.clone(), Config::default());

        let mut base_child_cids = Vec::new();
        let mut left_child_cids = Vec::new();
        let mut right_child_cids = Vec::new();
        for idx in 0..32 {
            let key = format!("k{idx:03}").into_bytes();

            let mut base_leaf = prolly.new_leaf_node();
            base_leaf.keys.push(key.clone());
            base_leaf.vals.push(format!("base-{idx:03}").into_bytes());
            base_child_cids.push(prolly.save(&base_leaf).unwrap());

            let mut left_leaf = prolly.new_leaf_node();
            left_leaf.keys.push(key.clone());
            left_leaf.vals.push(format!("left-{idx:03}").into_bytes());
            left_child_cids.push(prolly.save(&left_leaf).unwrap());

            let mut right_leaf = prolly.new_leaf_node();
            right_leaf.keys.push(key);
            right_leaf.vals.push(format!("right-{idx:03}").into_bytes());
            right_child_cids.push(prolly.save(&right_leaf).unwrap());
        }

        let keys = (0..32)
            .map(|idx| format!("k{idx:03}").into_bytes())
            .collect::<Vec<_>>();
        let save_root = |child_cids: Vec<Cid>| {
            let mut root = prolly.new_internal_node(1);
            root.keys = keys.clone();
            root.vals = child_cids.into_iter().map(|cid| cid.0.to_vec()).collect();
            prolly.save(&root).unwrap()
        };

        let base = Tree {
            root: Some(save_root(base_child_cids)),
            config: Config::default(),
        };
        let left = Tree {
            root: Some(save_root(left_child_cids)),
            config: Config::default(),
        };
        let right = Tree {
            root: Some(save_root(right_child_cids)),
            config: Config::default(),
        };
        let resolver: Resolver =
            Box::new(|conflict| Resolution::value(conflict.right.clone().expect("right value")));

        prolly.clear_cache();
        store.reset_counts();
        let merged = merge_trees(&prolly, &base, &left, &right, Some(resolver)).unwrap();

        assert_eq!(merged.root, right.root);
        assert!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed)
                >= MERGE_FRONTIER_PREFETCH_PARALLELISM,
            "wide structural merge frontier prefetch should split into parallel ordered batches"
        );
        assert!(
            store.max_batch_get_ordered_len.load(Ordering::Relaxed) <= 6,
            "96 merge-frontier CIDs at parallelism 16 should hydrate in bounded chunks"
        );
    }

    #[test]
    fn broad_merge_does_not_read_left_only_changed_subtrees() {
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

        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for i in 0..4096 {
            builder.add(key_for(i), format!("v{i:04}").into_bytes());
        }
        let base = builder.build().unwrap();
        let prolly = Prolly::new(store.clone(), config);

        let left = prolly
            .batch(
                &base,
                (0..32)
                    .map(|i| Mutation::Upsert {
                        key: key_for(i),
                        val: format!("left-{i:04}").into_bytes(),
                    })
                    .collect(),
            )
            .unwrap();
        let right = prolly
            .batch(
                &base,
                (2000..3100)
                    .map(|i| Mutation::Upsert {
                        key: key_for(i),
                        val: format!("right-{i:04}").into_bytes(),
                    })
                    .collect(),
            )
            .unwrap();

        let left_only_leaf = prolly
            .find_path(&left, &key_for(0))
            .unwrap()
            .last()
            .map(|(node, _)| node.clone())
            .unwrap();
        let blocked_cid = Cid::from_bytes(&left_only_leaf.to_bytes());
        store.block_get_key(blocked_cid.as_bytes());
        prolly.clear_cache();
        store.reset_counts();

        let merged = merge_trees(&prolly, &base, &left, &right, None).unwrap();

        assert!(
            store.batch_get_ordered_calls.load(Ordering::Relaxed) > 0,
            "merge should check left values through batched tree reads"
        );

        store.clear_blocked_get_keys();
        assert_eq!(
            prolly.get(&merged, &key_for(0)).unwrap(),
            Some(b"left-0000".to_vec())
        );
        assert_eq!(
            prolly.get(&merged, &key_for(2500)).unwrap(),
            Some(b"right-2500".to_vec())
        );
    }

    #[test]
    fn test_merge_trees_with_conflict() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let left = prolly.put(&base, b"a".to_vec(), b"left".to_vec()).unwrap();
        let right = prolly.put(&base, b"a".to_vec(), b"right".to_vec()).unwrap();

        let result = merge_trees(&prolly, &base, &left, &right, None);

        assert!(matches!(result, Err(Error::Conflict(_))));
    }

    #[test]
    fn test_merge_trees_with_resolver() {
        let store = MemStore::new();
        let prolly = Prolly::new(store, Config::default());
        let base = prolly.create();

        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
        let left = prolly.put(&base, b"a".to_vec(), b"left".to_vec()).unwrap();
        let right = prolly.put(&base, b"a".to_vec(), b"right".to_vec()).unwrap();

        // Resolver that prefers left
        let resolver: Resolver =
            Box::new(|c| Resolution::value(c.left.clone().expect("left value")));
        let merged = merge_trees(&prolly, &base, &left, &right, Some(resolver)).unwrap();

        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"left".to_vec()));
    }

    #[test]
    fn test_build_change_map() {
        let diffs = vec![
            Diff::Added {
                key: b"a".to_vec(),
                val: b"1".to_vec(),
            },
            Diff::Changed {
                key: b"b".to_vec(),
                old: b"old".to_vec(),
                new: b"new".to_vec(),
            },
            Diff::Removed {
                key: b"c".to_vec(),
                val: b"3".to_vec(),
            },
        ];

        let change_map = build_change_map(&diffs);

        assert_eq!(change_map.get(b"a".as_slice()), Some(&Some(b"1".to_vec())));
        assert_eq!(
            change_map.get(b"b".as_slice()),
            Some(&Some(b"new".to_vec()))
        );
        assert_eq!(change_map.get(b"c".as_slice()), Some(&None));
    }

    #[test]
    fn merge_change_refs_borrow_diff_payloads() {
        let diffs = vec![Diff::Changed {
            key: b"k".to_vec(),
            old: b"old".to_vec(),
            new: b"new".to_vec(),
        }];

        let changes = build_merge_change_refs(&diffs);
        let Diff::Changed { key, old, new } = &diffs[0] else {
            unreachable!();
        };

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].key.as_ptr(), key.as_ptr());
        assert_eq!(changes[0].base.unwrap().as_ptr(), old.as_ptr());
        assert_eq!(changes[0].value.unwrap().as_ptr(), new.as_ptr());
    }

    #[test]
    fn append_only_merge_guard_uses_min_changed_key() {
        let store = Arc::new(CountingStore::default());
        let config = Config::default();
        let prolly = Prolly::new(store, config.clone());
        let base = valid_leaf_tree(&prolly, config.clone(), b"1");
        let left = base.clone();
        let diffs = vec![
            Diff::Added {
                key: b"z".to_vec(),
                val: b"z".to_vec(),
            },
            Diff::Changed {
                key: b"a".to_vec(),
                old: b"1".to_vec(),
                new: b"right".to_vec(),
            },
        ];
        let changes = build_merge_change_refs(&diffs);

        assert!(!right_changes_are_append_only_after(&prolly, &base, &left, &changes).unwrap());
    }
}
