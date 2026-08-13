use serde::{Deserialize, Serialize};

use crate::{
    CommitClosureCursor, CommitId, ObjectVersionId, OperationId, RepositoryId, RootManifest,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistoryTransferPhase {
    UnionParentVersions,
    ApplyTransitions,
    FinalizeCommit,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingHistoryTransferCommit {
    pub source: CommitId,
    pub next_closure: CommitClosureCursor,
    pub mapped_parents: Vec<CommitId>,
    pub objects: RootManifest,
    pub versions: RootManifest,
    pub delta: RootManifest,
    pub phase: HistoryTransferPhase,
    pub union_parent_index: usize,
    pub union_base: Option<RootManifest>,
    pub union_diff: Option<prolly::StructuralDiffCursor>,
    pub inline_index: usize,
    pub external_after: Option<Vec<u8>>,
    pub transitions_applied: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryTransferReport {
    pub imported_commits: u64,
    pub rebound_versions: u64,
    pub copied_payloads: u64,
    pub copied_payload_bytes: u64,
}

/// Constant-size checkpoint for a logical, history-preserving repository
/// transfer. Traversal work and source-to-destination mappings live in
/// immutable Prolly trees named by this cursor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryTransferCursor {
    pub source_repository: RepositoryId,
    pub destination_repository: RepositoryId,
    pub job: OperationId,
    pub source_branch: String,
    pub destination_branch: String,
    pub source_head: CommitId,
    pub expected_destination_head: CommitId,
    pub closure: CommitClosureCursor,
    pub mappings: RootManifest,
    pub pending: Option<PendingHistoryTransferCommit>,
    pub mapped_head: Option<CommitId>,
    pub report: HistoryTransferReport,
    pub complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryTransferPage {
    pub cursor: HistoryTransferCursor,
    pub traversal_steps: usize,
    pub mutation_steps: usize,
    pub imported_commits: usize,
    pub complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryTransferMapping {
    pub source: CommitId,
    pub destination: CommitId,
}

pub(crate) fn commit_mapping_key(id: CommitId) -> Vec<u8> {
    let mut key = Vec::with_capacity(34);
    key.extend_from_slice(b"c/");
    key.extend_from_slice(id.as_bytes());
    key
}

pub(crate) fn version_mapping_key(id: ObjectVersionId) -> Vec<u8> {
    let mut key = Vec::with_capacity(34);
    key.extend_from_slice(b"v/");
    key.extend_from_slice(id.as_bytes());
    key
}
