//! Garbage-collection planning for immutable prolly trees.
//!
//! The core [`Store`](crate::Store) trait does not require key listing, so the
//! generic GC API works from explicit root and candidate sets:
//!
//! - mark live nodes from the trees an application wants to retain;
//! - plan which caller-supplied candidate CIDs are unreachable;
//! - optionally sweep those unreachable candidates.

use super::blob::BlobRef;
use super::cid::Cid;

/// Live node set discovered from one or more retained tree roots.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcReachability {
    /// Reachable content-addressed node CIDs, sorted by CID bytes.
    pub live_cids: Vec<Cid>,
    /// Number of reachable nodes.
    pub live_nodes: usize,
    /// Serialized byte weight of reachable nodes as encoded by the current
    /// node serializer.
    pub live_bytes: usize,
    /// Number of reachable leaf nodes.
    pub leaf_nodes: usize,
    /// Number of reachable internal nodes.
    pub internal_nodes: usize,
}

impl GcReachability {
    /// Return reachable node CIDs in stable byte order.
    pub fn cids(&self) -> &[Cid] {
        &self.live_cids
    }

    /// Whether `cid` is reachable from the retained roots.
    pub fn contains(&self, cid: &Cid) -> bool {
        self.live_cids.iter().any(|probe| probe == cid)
    }

    /// Consume this report and return the reachable node CIDs.
    pub fn into_cids(self) -> Vec<Cid> {
        self.live_cids
    }
}

/// Dry-run garbage-collection plan for an explicit candidate set.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcPlan {
    /// Reachability report for the retained roots.
    pub reachability: GcReachability,
    /// Number of unique candidate CIDs inspected.
    pub candidate_nodes: usize,
    /// Unreachable candidate CIDs present in the store, sorted by CID bytes.
    pub reclaimable_cids: Vec<Cid>,
    /// Number of reclaimable candidate nodes.
    pub reclaimable_nodes: usize,
    /// Serialized bytes reclaimable from present unreachable candidates.
    pub reclaimable_bytes: usize,
    /// Candidate CIDs that were neither reachable nor present in the store.
    pub missing_candidates: usize,
}

impl GcPlan {
    /// Return reclaimable candidate CIDs in stable byte order.
    pub fn reclaimable_cids(&self) -> &[Cid] {
        &self.reclaimable_cids
    }

    /// Whether this plan would delete no nodes.
    pub fn is_empty(&self) -> bool {
        self.reclaimable_cids.is_empty()
    }

    /// Number of candidate nodes retained because they are still reachable.
    pub fn retained_candidate_nodes(&self) -> usize {
        self.candidate_nodes
            .saturating_sub(self.reclaimable_nodes)
            .saturating_sub(self.missing_candidates)
    }
}

/// Result of sweeping a garbage-collection plan.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcSweep {
    /// Plan used to decide which candidates were unreachable.
    pub plan: GcPlan,
    /// Number of nodes deleted from the backing store.
    pub deleted_nodes: usize,
    /// Serialized bytes deleted from the backing store.
    pub deleted_bytes: usize,
}

/// Live blob set discovered from one or more retained tree roots.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlobGcReachability {
    /// Reachable content-addressed blob references, sorted by CID bytes.
    pub live_blobs: Vec<BlobRef>,
    /// Number of unique reachable blobs.
    pub live_blob_count: usize,
    /// Total byte weight of unique reachable blobs.
    pub live_blob_bytes: u64,
    /// Number of reachable tree nodes scanned while marking blob references.
    pub scanned_nodes: usize,
    /// Number of reachable leaf values inspected while marking blob references.
    pub scanned_values: usize,
}

impl BlobGcReachability {
    /// Return reachable blob references in stable CID order.
    pub fn blobs(&self) -> &[BlobRef] {
        &self.live_blobs
    }

    /// Whether `reference` is reachable from the retained roots.
    pub fn contains(&self, reference: &BlobRef) -> bool {
        self.live_blobs
            .iter()
            .any(|probe| probe.cid == reference.cid)
    }

    /// Consume this report and return the reachable blob references.
    pub fn into_blobs(self) -> Vec<BlobRef> {
        self.live_blobs
    }
}

/// Dry-run garbage-collection plan for offloaded blobs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlobGcPlan {
    /// Reachability report for the retained roots.
    pub reachability: BlobGcReachability,
    /// Number of unique candidate blob CIDs inspected.
    pub candidate_blobs: usize,
    /// Unreachable candidate blobs present in the blob store, sorted by CID bytes.
    pub reclaimable_blobs: Vec<BlobRef>,
    /// Number of reclaimable candidate blobs.
    pub reclaimable_blob_count: usize,
    /// Bytes reclaimable from present unreachable candidates.
    pub reclaimable_blob_bytes: u64,
    /// Candidate blob CIDs that were neither reachable nor present in the blob
    /// store.
    pub missing_candidates: usize,
}

impl BlobGcPlan {
    /// Return reclaimable blob references in stable CID order.
    pub fn reclaimable_blobs(&self) -> &[BlobRef] {
        &self.reclaimable_blobs
    }

    /// Whether this plan would delete no blobs.
    pub fn is_empty(&self) -> bool {
        self.reclaimable_blobs.is_empty()
    }

    /// Number of candidate blobs retained because they are still reachable.
    pub fn retained_candidate_blobs(&self) -> usize {
        self.candidate_blobs
            .saturating_sub(self.reclaimable_blob_count)
            .saturating_sub(self.missing_candidates)
    }
}

/// Result of sweeping a blob garbage-collection plan.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlobGcSweep {
    /// Plan used to decide which candidate blobs were unreachable.
    pub plan: BlobGcPlan,
    /// Number of blobs deleted from the backing blob store.
    pub deleted_blobs: usize,
    /// Blob bytes deleted from the backing blob store.
    pub deleted_blob_bytes: u64,
}

pub(crate) fn sort_cids(cids: &mut [Cid]) {
    cids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
}

pub(crate) fn sort_blob_refs(blobs: &mut [BlobRef]) {
    blobs.sort_by(|left, right| left.cid.as_bytes().cmp(right.cid.as_bytes()));
}
