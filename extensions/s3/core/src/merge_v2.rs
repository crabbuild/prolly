use serde::{Deserialize, Serialize};

use crate::{CommitIdV2, ObjectVersionIdV2, OperationId, RepositoryId, TreeRootV1};

/// Conflict policy applied while constructing a native protocol-v2 merge plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergePolicyV2 {
    /// Persist conflicts in the plan and refuse publication until they are
    /// resolved by starting a new plan with an explicit policy.
    Fail,
    /// Keep the target branch value for conflicting keys.
    Ours,
    /// Select the source branch value for conflicting keys.
    Theirs,
}

/// Durable phase of a native protocol-v2 merge job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergePhaseV2 {
    DiscoveringBases,
    CollectingBases,
    AwaitingBase,
    Planning,
    BuildingVersions,
    BuildingObjects,
    Conflicted,
    ReadyToPublish,
}

/// Constant-size handle for a restartable native protocol-v2 merge.
///
/// The potentially unbounded graph frontier, planned changes, conflicts, and
/// output delta live in immutable Prolly trees named by this cursor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeCursorV2 {
    pub repository: RepositoryId,
    pub job: OperationId,
    pub target_branch: String,
    pub source_branch: String,
    pub ours: CommitIdV2,
    pub theirs: CommitIdV2,
    pub requested_base: Option<CommitIdV2>,
    pub selected_base: Option<CommitIdV2>,
    pub policy: MergePolicyV2,
    pub operation: OperationId,
    pub message: String,
    pub created_at_millis: u64,
    pub phase: MergePhaseV2,
    pub plan_root: TreeRootV1,
    pub ours_diff: Option<prolly::StructuralDiffCursor>,
    pub theirs_diff: Option<prolly::StructuralDiffCursor>,
    pub ours_pending: Option<prolly::Diff>,
    pub theirs_pending: Option<prolly::Diff>,
    pub ours_finished: bool,
    pub theirs_finished: bool,
    pub version_diff: Option<prolly::StructuralDiffCursor>,
    pub version_diff_finished: bool,
    pub build_after: Option<Vec<u8>>,
    pub final_objects: Option<TreeRootV1>,
    pub final_versions: Option<TreeRootV1>,
    pub delta_root: Option<TreeRootV1>,
    pub visited_commits: u64,
    pub best_base_count: u64,
    pub planned_changes: u64,
    pub conflicts: u64,
    pub built_changes: u64,
}

/// One logical object change selected by a durable merge plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeChangeV2 {
    pub key: Vec<u8>,
    pub from: Option<ObjectVersionIdV2>,
    pub to: Option<ObjectVersionIdV2>,
}

/// One three-way object conflict persisted by a durable merge plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeConflictV2 {
    pub key: Vec<u8>,
    pub base: Option<ObjectVersionIdV2>,
    pub ours: Option<ObjectVersionIdV2>,
    pub theirs: Option<ObjectVersionIdV2>,
}

/// Bounded work result returned while advancing a merge job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeAdvancePageV2 {
    pub cursor: MergeCursorV2,
    pub processed: usize,
    pub changes: Vec<MergeChangeV2>,
    pub conflicts: Vec<MergeConflictV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeBaseCursorV2 {
    pub repository: RepositoryId,
    pub job: OperationId,
    pub plan_root: TreeRootV1,
    pub after: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeBasePageV2 {
    pub bases: Vec<CommitIdV2>,
    pub continuation: Option<MergeBaseCursorV2>,
}

/// One page of persisted merge-plan changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeChangePageV2 {
    pub changes: Vec<MergeChangeV2>,
    pub continuation: Option<MergeChangeCursorV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeChangeCursorV2 {
    pub repository: RepositoryId,
    pub job: OperationId,
    pub plan_root: TreeRootV1,
    pub after: Vec<u8>,
}

/// One page of persisted merge-plan conflicts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeConflictPageV2 {
    pub conflicts: Vec<MergeConflictV2>,
    pub continuation: Option<MergeConflictCursorV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeConflictCursorV2 {
    pub repository: RepositoryId,
    pub job: OperationId,
    pub plan_root: TreeRootV1,
    pub after: Vec<u8>,
}

/// Successful publication of a durable merge plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeReceiptV2 {
    pub id: CommitIdV2,
    pub operation: OperationId,
    pub branch: String,
    pub parents: [CommitIdV2; 2],
    pub changed_keys: u64,
    pub conflicts: u64,
    pub idempotent_replay: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeCleanupCursorV2 {
    pub repository: RepositoryId,
    pub job: OperationId,
    pub provider_continuation: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeCleanupPageV2 {
    pub deleted: usize,
    pub continuation: Option<MergeCleanupCursorV2>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MergeQueueEntryV2 {
    pub commit: CommitIdV2,
    pub generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MergeSeenEntryV2 {
    pub generation: u64,
    pub flags: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MergeBaseCandidateV2 {
    pub generation: u64,
    pub stale: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MergePlanEntryV2 {
    pub key: Vec<u8>,
    pub base: Option<Vec<u8>>,
    pub ours: Option<Vec<u8>>,
    pub theirs: Option<Vec<u8>>,
    pub selected: Option<Vec<u8>>,
    pub conflict: bool,
}
