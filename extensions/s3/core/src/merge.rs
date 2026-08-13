use serde::{Deserialize, Serialize};

use crate::{CommitId, ObjectVersionId, OperationId, RepositoryId, RootManifest};

/// Conflict policy applied while constructing a repository merge plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergePolicy {
    /// Persist conflicts in the plan and refuse publication until they are
    /// resolved by starting a new plan with an explicit policy.
    Fail,
    /// Keep the target branch value for conflicting keys.
    Ours,
    /// Select the source branch value for conflicting keys.
    Theirs,
}

/// Durable phase of a repository merge job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergePhase {
    DiscoveringBases,
    CollectingBases,
    AwaitingBase,
    Planning,
    BuildingVersions,
    BuildingObjects,
    Conflicted,
    ReadyToPublish,
}

/// Constant-size handle for a restartable repository merge.
///
/// The potentially unbounded graph frontier, planned changes, conflicts, and
/// output delta live in immutable Prolly trees named by this cursor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeCursor {
    pub repository: RepositoryId,
    pub job: OperationId,
    pub target_branch: String,
    pub source_branch: String,
    pub ours: CommitId,
    pub theirs: CommitId,
    pub requested_base: Option<CommitId>,
    pub selected_base: Option<CommitId>,
    pub policy: MergePolicy,
    pub operation: OperationId,
    pub message: String,
    pub created_at_millis: u64,
    pub phase: MergePhase,
    pub plan_root: RootManifest,
    pub ours_diff: Option<prolly::StructuralDiffCursor>,
    pub theirs_diff: Option<prolly::StructuralDiffCursor>,
    pub ours_pending: Option<prolly::Diff>,
    pub theirs_pending: Option<prolly::Diff>,
    pub ours_finished: bool,
    pub theirs_finished: bool,
    pub version_diff: Option<prolly::StructuralDiffCursor>,
    pub version_diff_finished: bool,
    pub build_after: Option<Vec<u8>>,
    pub final_objects: Option<RootManifest>,
    pub final_versions: Option<RootManifest>,
    pub delta_root: Option<RootManifest>,
    pub visited_commits: u64,
    pub best_base_count: u64,
    pub planned_changes: u64,
    pub conflicts: u64,
    pub built_changes: u64,
}

/// One logical object change selected by a durable merge plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeChange {
    pub key: Vec<u8>,
    pub from: Option<ObjectVersionId>,
    pub to: Option<ObjectVersionId>,
}

/// One three-way object conflict persisted by a durable merge plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeConflict {
    pub key: Vec<u8>,
    pub base: Option<ObjectVersionId>,
    pub ours: Option<ObjectVersionId>,
    pub theirs: Option<ObjectVersionId>,
}

/// Bounded work result returned while advancing a merge job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeAdvancePage {
    pub cursor: MergeCursor,
    pub processed: usize,
    pub changes: Vec<MergeChange>,
    pub conflicts: Vec<MergeConflict>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeBaseCursor {
    pub repository: RepositoryId,
    pub job: OperationId,
    pub plan_root: RootManifest,
    pub after: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeBasePage {
    pub bases: Vec<CommitId>,
    pub continuation: Option<MergeBaseCursor>,
}

/// One page of persisted merge-plan changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeChangePage {
    pub changes: Vec<MergeChange>,
    pub continuation: Option<MergeChangeCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeChangeCursor {
    pub repository: RepositoryId,
    pub job: OperationId,
    pub plan_root: RootManifest,
    pub after: Vec<u8>,
}

/// One page of persisted merge-plan conflicts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeConflictPage {
    pub conflicts: Vec<MergeConflict>,
    pub continuation: Option<MergeConflictCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeConflictCursor {
    pub repository: RepositoryId,
    pub job: OperationId,
    pub plan_root: RootManifest,
    pub after: Vec<u8>,
}

/// Successful publication of a durable merge plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeReceipt {
    pub id: CommitId,
    pub operation: OperationId,
    pub branch: String,
    pub parents: [CommitId; 2],
    pub changed_keys: u64,
    pub conflicts: u64,
    pub idempotent_replay: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeCleanupCursor {
    pub repository: RepositoryId,
    pub job: OperationId,
    pub provider_continuation: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeCleanupPage {
    pub deleted: usize,
    pub continuation: Option<MergeCleanupCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MergeQueueEntry {
    pub commit: CommitId,
    pub generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MergeSeenEntry {
    pub generation: u64,
    pub flags: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MergeBaseCandidate {
    pub generation: u64,
    pub stale: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MergePlanEntry {
    pub key: Vec<u8>,
    pub base: Option<Vec<u8>>,
    pub ours: Option<Vec<u8>>,
    pub theirs: Option<Vec<u8>>,
    pub selected: Option<Vec<u8>>,
    pub conflict: bool,
}
