use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    CommitId, ObjectPath, OperationId, PhysicalVersion, RefGeneration, RepositoryId, RootManifest,
};

/// Payload candidate discovery policy for a GC epoch.
///
/// `LegacyScan` is the compatibility-safe default because direct/single-object
/// writers from older clients do not emit creation intents. `JournalOnly` is
/// intended for repositories whose payload ingestion uses the journaled batch
/// APIs; it avoids listing the payload namespace entirely. Unjournaled payloads
/// are retained until a legacy-scan epoch discovers them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum GcCandidateDiscovery {
    #[default]
    LegacyScan,
    JournalOnly,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum GcCandidateNamespace {
    #[default]
    Repository,
    Commits,
    Nodes,
    Complete,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum GcInventorySource {
    #[default]
    Completions,
    Intents,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GcPhase {
    DiscoverBranches,
    DiscoverTags,
    MarkCommits,
    MarkNodes,
    ScanInventory,
    ScanCandidates,
    CatchUpDirtyRoots,
    Ready,
    Sweeping,
    Cleanup,
    Complete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcCursor {
    pub repository: RepositoryId,
    pub epoch: OperationId,
    pub cutoff_millis: u64,
    pub phase: GcPhase,
    pub continuation: Option<String>,
    #[serde(default)]
    pub payload_discovery: GcCandidateDiscovery,
    #[serde(default)]
    pub candidate_namespace: GcCandidateNamespace,
    #[serde(default)]
    pub journal_object_offset: usize,
    #[serde(default)]
    pub inventory_source: GcInventorySource,
    pub work: RootManifest,
    pub dirty_sequence: u64,
    pub dirty_target_sequence: u64,
    pub initial_scan_complete: bool,
    #[serde(default)]
    pub publication_barrier_drained: bool,
    pub sweep_after: Option<Vec<u8>>,
    pub report: GcReport,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcReport {
    pub roots: u64,
    pub commits: u64,
    pub nodes: u64,
    pub logical_versions: u64,
    pub candidates: u64,
    pub candidate_bytes: u64,
    pub dirty_roots: u64,
    pub deleted_versions: u64,
    pub deleted_bytes: u64,
    pub already_missing: u64,
    pub skipped_reachable: u64,
    #[serde(default)]
    pub protected_versions: u64,
    #[serde(default)]
    pub protected_bytes: u64,
    pub candidates_by_kind: BTreeMap<String, u64>,
    pub deleted_by_kind: BTreeMap<String, u64>,
    #[serde(default)]
    pub protected_by_kind: BTreeMap<String, u64>,
    #[serde(default)]
    pub journal_batches: u64,
    #[serde(default)]
    pub journal_objects: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcPage {
    pub cursor: GcCursor,
    pub processed: usize,
    pub complete: bool,
    pub restarted_for_new_roots: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GcCoordinator {
    pub repository: RepositoryId,
    pub generation: u64,
    pub active_epoch: Option<OperationId>,
    #[serde(default)]
    pub admission_closed: bool,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GcPublicationTicket {
    pub repository: RepositoryId,
    pub instance: OperationId,
    pub request_digest: [u8; 32],
    pub expires_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GcDirtyRoot {
    pub repository: RepositoryId,
    pub epoch: OperationId,
    pub sequence: u64,
    pub namespace: String,
    pub name: String,
    pub target: CommitId,
    pub previous_target: Option<CommitId>,
    pub generation: RefGeneration,
    pub operation: OperationId,
    pub created_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GcNodeWork {
    pub cid: prolly::Cid,
    pub scan_versions: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GcCandidate {
    pub path: ObjectPath,
    pub physical_version: PhysicalVersion,
    pub len: u64,
    pub last_modified_millis: u64,
    pub kind: String,
}
