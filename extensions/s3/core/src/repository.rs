use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock, RwLock, Weak,
    },
    time::Duration,
};

use futures_util::{stream, StreamExt};
use md5::{Digest as _, Md5};
use prolly::{
    AsyncProlly, AsyncStore, Config, Diff, Mutation, Node, RuntimeConfig, Tree, TreeFormat,
};
use sha2::Sha256;

use crate::gc::{GcCandidate, GcCoordinator, GcDirtyRoot, GcNodeWork, GcPublicationTicket};
use crate::merge::{MergeBaseCandidate, MergePlanEntry, MergeQueueEntry, MergeSeenEntry};
use crate::publication::BranchMovement;
use crate::store::{LocatedPackedNode, NodeCacheNamespace, NodeLocator, PreparedNodePack};
use crate::transfer::{commit_mapping_key, version_mapping_key};
use crate::{
    decode_canonical, encode_canonical, tree_format_digest, AuthorityPermit, AuthorityScope,
    BucketCommit, BucketDelta, BucketState, CanonicalLimits, Checksums, Clock, CommitGeneration,
    CommitId, CommitObject, CommitPublication, CommitSessionCheckpoint, CommitSessionCleanupReport,
    CommitSessionManifest, CommitSessionState, CommitSessionStore, CompareExchange,
    CompareExchangeOutcome, CurrentObject, DeleteOutcome, Error, ErrorCode, GcCursor, GcPage,
    GcPhase, GetRequest, HistoryTransferCursor, HistoryTransferPage, HistoryTransferPhase,
    HistoryTransferReport, IdSource, IdempotencyRetention, ImmutablePayloadStore,
    InitializationIntent, JournalCommitGraphEntry, JournalDerivedIndexes,
    JournalIndexAdvanceReport, JournalIndexRebuildCleanup, JournalIndexRebuildCursor,
    JournalIndexRebuildPhase, JournalIndexRebuildStep, ListRequest, LoadedRef,
    LogicalObjectVersionBody, LogicalObjectVersionKind, MemoryNodeCache, MergeAdvancePage,
    MergeBaseCursor, MergeBasePage, MergeChange, MergeChangeCursor, MergeChangePage,
    MergeCleanupCursor, MergeCleanupPage, MergeConflict, MergeConflictCursor, MergeConflictPage,
    MergeCursor, MergePhase, MergePolicy, MergeReceipt, MutableControlKind, MutableControlObserver,
    MutableControlStore, MutationIdentity, NodeCache, ObjectHeaders, ObjectPath, ObjectPlane,
    ObjectTransition, ObjectVersion, ObjectVersionId, ObjectVersionOrder, OperationId,
    OperationIndexAdvanceReport, OperationIndexRebuildCursor, OperationIndexRebuildStep,
    PendingHistoryTransferCommit, PhysicalVersion, ProllyObjectStore, ProviderPerKeyVersionLimit,
    RandomIdSource, RefCatalogCursor, RefGeneration, RefKind, RepositoryFormat, Result,
    RootManifest, SegmentedOperationIndex, ShardWriterAuthority, ShardedBranchPublisher,
    ShardedRefCatalog, StagedMutation, StagedMutationBody, StagedPut, SystemClock, TagStore,
    TakeoverRequest,
};

/// Keep ordinary commit descriptors small enough for one bounded metadata
/// range read. Larger transition sets live in the same immutable Prolly node
/// pack as the state roots and remain content-addressed by the commit.
const INLINE_COMMIT_DELTA_LIMIT: usize = 128;

#[derive(Clone)]
pub struct RepositoryOptions {
    pub repository_prefix: String,
    pub default_branch: String,
    pub writer: String,
    pub limits: CanonicalLimits,
    pub state_tree_format: TreeFormat,
    pub authority_lease_millis: u64,
    pub read_only: bool,
    pub max_cached_node_pack_bytes: usize,
    pub max_cached_node_locations: usize,
    pub max_cached_node_bytes: usize,
    pub node_cache: Option<Arc<dyn NodeCache>>,
    pub mutable_control_versions_to_retain: usize,
    pub journal_index_max_unindexed_events: usize,
    pub operation_index_leaf_entries: usize,
    pub operation_index_merge_fanout: usize,
    pub operation_index_max_unindexed_events: usize,
    pub idempotency_retention: IdempotencyRetention,
    pub provider_per_key_version_limit: ProviderPerKeyVersionLimit,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdSource>,
}

impl Default for RepositoryOptions {
    fn default() -> Self {
        Self {
            repository_prefix: ".prolly".to_string(),
            default_branch: "main".to_string(),
            writer: "anonymous".to_string(),
            limits: CanonicalLimits::default(),
            state_tree_format: TreeFormat::default(),
            authority_lease_millis: 60_000,
            read_only: false,
            max_cached_node_pack_bytes: 64 * 1024 * 1024,
            max_cached_node_locations: 65_536,
            max_cached_node_bytes: 64 * 1024 * 1024,
            node_cache: None,
            mutable_control_versions_to_retain: crate::DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
            journal_index_max_unindexed_events: crate::DEFAULT_JOURNAL_INDEX_MAX_UNINDEXED_EVENTS,
            operation_index_leaf_entries: crate::DEFAULT_OPERATION_INDEX_LEAF_ENTRIES,
            operation_index_merge_fanout: crate::DEFAULT_OPERATION_INDEX_MERGE_FANOUT,
            operation_index_max_unindexed_events:
                crate::DEFAULT_OPERATION_INDEX_MAX_UNINDEXED_EVENTS,
            idempotency_retention: IdempotencyRetention::default(),
            provider_per_key_version_limit: ProviderPerKeyVersionLimit::Unknown,
            clock: Arc::new(SystemClock),
            ids: Arc::new(RandomIdSource),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitReceipt {
    pub id: CommitId,
    pub operation: OperationId,
    pub branch: String,
    pub parents: Vec<CommitId>,
    pub changed_keys: u64,
    pub object_versions: Vec<ObjectVersionId>,
    pub idempotent_replay: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectData {
    pub key: Vec<u8>,
    pub version: ObjectVersion,
    pub bytes: Vec<u8>,
    pub snapshot: CommitId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectRangeData {
    pub key: Vec<u8>,
    pub version: ObjectVersion,
    pub bytes: Vec<u8>,
    pub snapshot: CommitId,
    pub range: std::ops::RangeInclusive<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectSummary {
    pub key: Vec<u8>,
    pub version: ObjectVersion,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct ListObjectsCursor {
    repository: crate::RepositoryId,
    branch: String,
    snapshot: CommitId,
    prefix: Vec<u8>,
    traversal: prolly::RangeCursor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListObjectsPage {
    pub snapshot: CommitId,
    pub objects: Vec<ObjectSummary>,
    /// Opaque, snapshot-bound continuation. `None` means traversal completed.
    pub continuation: Option<String>,
}

/// One logical object accepted by a bounded commit-session staging window.
pub type CommitSessionPutInput = (Vec<u8>, Vec<u8>, ObjectHeaders, BTreeMap<String, String>);
pub type CommitSessionRepackInput = (
    Vec<u8>,
    Vec<u8>,
    ObjectHeaders,
    BTreeMap<String, String>,
    BTreeMap<String, String>,
);

struct CommitMetadataCache {
    entries: BTreeMap<CommitId, (Arc<BucketCommit>, usize)>,
    order: VecDeque<CommitId>,
    bytes: usize,
    max_bytes: usize,
}

impl CommitMetadataCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            max_bytes,
        }
    }

    fn get(&mut self, id: CommitId) -> Option<Arc<BucketCommit>> {
        let commit = self.entries.get(&id)?.0.clone();
        self.order.retain(|candidate| *candidate != id);
        self.order.push_back(id);
        Some(commit)
    }

    fn insert(&mut self, id: CommitId, commit: Arc<BucketCommit>, bytes: usize) {
        if bytes > self.max_bytes {
            return;
        }
        if let Some((_, previous_bytes)) = self.entries.remove(&id) {
            self.bytes = self.bytes.saturating_sub(previous_bytes);
            self.order.retain(|candidate| *candidate != id);
        }
        while self.bytes.saturating_add(bytes) > self.max_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some((_, evicted_bytes)) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(evicted_bytes);
            }
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.insert(id, (commit, bytes));
        self.order.push_back(id);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelimitedObjectPage {
    pub snapshot: CommitId,
    pub objects: Vec<ObjectSummary>,
    pub common_prefixes: Vec<Vec<u8>>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionSummary {
    pub key: Vec<u8>,
    pub version: ObjectVersion,
    pub cursor: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectDiff {
    pub key: Vec<u8>,
    pub from: Option<ObjectVersionId>,
    pub to: Option<ObjectVersionId>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObjectDiffCursor {
    repository: crate::RepositoryId,
    branch: String,
    from: CommitId,
    to: CommitId,
    traversal: prolly::StructuralDiffCursor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectDiffPage {
    pub changes: Vec<ObjectDiff>,
    pub continuation: Option<ObjectDiffCursor>,
    pub compared_nodes: usize,
    pub reused_subtrees: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraversalBudget {
    pub max_commits: usize,
    pub max_decoded_bytes: u64,
    pub max_elapsed: Duration,
}

impl Default for TraversalBudget {
    fn default() -> Self {
        Self {
            max_commits: 10_000,
            max_decoded_bytes: 64 * 1024 * 1024,
            max_elapsed: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HistoryCursor {
    repository: crate::RepositoryId,
    branch: String,
    root: CommitId,
    next: CommitId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitPage {
    pub commits: Vec<(CommitId, BucketCommit)>,
    pub continuation: Option<HistoryCursor>,
    pub visited_commits: usize,
    pub decoded_bytes: u64,
    pub budget_exhausted: bool,
}

/// Constant-size checkpoint for a durable parent-before-child traversal of a
/// commit DAG. The work stack and visited set live in immutable Prolly nodes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CommitClosureCursor {
    pub repository: crate::RepositoryId,
    pub traversal: OperationId,
    pub state: RootManifest,
    pub next_stack_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitClosurePage {
    pub commits: Vec<(CommitId, BucketCommit)>,
    pub cursor: CommitClosureCursor,
    pub steps: usize,
    pub complete: bool,
    pub budget_exhausted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CommitClosureWork {
    commit: CommitId,
    finish: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FsckPhase {
    DiscoverCommits,
    VerifyObjects,
    VerifyVersions,
    Complete,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsckReport {
    pub commits: u64,
    pub reachable_nodes: u64,
    pub current_objects: u64,
    pub logical_versions: u64,
    pub payloads_verified: u64,
    pub payload_bytes_verified: u64,
    pub deep_content_bytes_verified: u64,
    #[serde(default)]
    pub packed_payloads_verified: u64,
    #[serde(default)]
    pub packed_logical_bytes_verified: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsckCursor {
    pub repository: crate::RepositoryId,
    pub branch: String,
    pub snapshot: CommitId,
    pub closure: CommitClosureCursor,
    pub phase: FsckPhase,
    pub after: Option<Vec<u8>>,
    pub deep: bool,
    pub report: FsckReport,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FsckPage {
    pub cursor: FsckCursor,
    pub processed: usize,
    pub complete: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PayloadPackStats {
    pub current_objects: u64,
    pub logical_bytes: u64,
    pub direct_objects: u64,
    pub packed_objects: u64,
    pub packed_logical_bytes: u64,
    pub unique_physical_objects: u64,
    pub unique_physical_bytes: u64,
    pub unique_pack_objects: u64,
    pub unique_pack_bytes: u64,
    pub unique_packed_extents: u64,
    pub unique_packed_extent_bytes: u64,
}

impl PayloadPackStats {
    pub fn pack_utilization_basis_points(&self) -> u64 {
        if self.unique_pack_bytes == 0 {
            return 10_000;
        }
        self.unique_packed_extent_bytes
            .saturating_mul(10_000)
            .checked_div(self.unique_pack_bytes)
            .unwrap_or_default()
            .min(10_000)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PayloadPackStatsCursor {
    pub repository: crate::RepositoryId,
    pub branch: String,
    pub snapshot: CommitId,
    pub job: OperationId,
    pub after: Option<Vec<u8>>,
    pub seen: RootManifest,
    pub report: PayloadPackStats,
    pub complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadPackStatsPage {
    pub cursor: PayloadPackStatsCursor,
    pub processed: usize,
    pub complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefMoveReceipt {
    pub branch: String,
    pub old_target: CommitId,
    pub new_target: CommitId,
    pub operation: OperationId,
    pub generation: RefGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RestoreCursor {
    pub repository: crate::RepositoryId,
    pub branch: String,
    pub source: CommitId,
    pub original_head: CommitId,
    pub expected_head: CommitId,
    pub batch: crate::BatchId,
    pub checkpoint_sequence: u64,
    pub diff: Option<ObjectDiffCursor>,
    pub message: String,
    pub complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestorePage {
    pub cursor: RestoreCursor,
    pub processed: usize,
    pub receipt: Option<CommitReceipt>,
    pub complete: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RepairPhase {
    CopySource,
    DeleteDestinationOnly,
    Complete,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RepairReport {
    pub scanned_source_objects: u64,
    pub copied_objects: u64,
    pub copied_bytes: u64,
    pub scanned_destination_objects: u64,
    pub deleted_objects: u64,
    pub published_commits: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RepairCursor {
    pub source_repository: crate::RepositoryId,
    pub destination_repository: crate::RepositoryId,
    pub source_branch: String,
    pub destination_branch: String,
    pub source_snapshot: CommitId,
    pub destination_snapshot: CommitId,
    pub expected_head: CommitId,
    pub source_after: Option<Vec<u8>>,
    pub destination_after: Option<Vec<u8>>,
    pub phase: RepairPhase,
    pub batch: crate::BatchId,
    pub checkpoint_sequence: u64,
    pub message: String,
    pub report: RepairReport,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairPage {
    pub cursor: RepairCursor,
    pub processed: usize,
    pub receipt: Option<CommitReceipt>,
    pub complete: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BackupVerificationReport {
    pub objects_verified: u64,
    pub content_bytes_verified: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BackupVerificationCursor {
    pub source_repository: crate::RepositoryId,
    pub destination_repository: crate::RepositoryId,
    pub source_branch: String,
    pub destination_branch: String,
    pub source_snapshot: CommitId,
    pub destination_snapshot: CommitId,
    pub source_after: Option<Vec<u8>>,
    pub destination_after: Option<Vec<u8>>,
    pub report: BackupVerificationReport,
    pub complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupVerificationPage {
    pub cursor: BackupVerificationCursor,
    pub processed: usize,
    pub complete: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeCachePrewarmReport {
    pub snapshot: CommitId,
    pub object_nodes: usize,
    pub version_nodes: usize,
    pub before: crate::NodeCacheSnapshot,
    pub after: crate::NodeCacheSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchHead {
    pub name: String,
    pub target: CommitId,
    pub generation: RefGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tag {
    pub name: String,
    pub target: CommitId,
    pub generation: RefGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionPin {
    pub name: String,
    pub target: CommitId,
    pub generation: RefGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionPinPage {
    pub pins: Vec<RetentionPin>,
    pub continuation: Option<RefCatalogCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchCatalogPage {
    pub branches: Vec<BranchHead>,
    pub continuation: Option<RefCatalogCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagCatalogPage {
    pub tags: Vec<Tag>,
    pub continuation: Option<RefCatalogCursor>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RefCatalogRepairPage {
    pub scanned: usize,
    pub indexed: usize,
    pub continuation: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchIndexAdvanceReport {
    pub operations: OperationIndexAdvanceReport,
    pub journal: JournalIndexAdvanceReport,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchIndexHealth {
    pub branch: String,
    pub target: CommitId,
    pub ref_generation: RefGeneration,
    pub indexed_target: Option<CommitId>,
    pub indexed_generation: Option<RefGeneration>,
    pub lag_generations: u64,
    pub ready: bool,
    pub locally_registered: bool,
    pub last_error: Option<String>,
}

pub struct BranchIndexMaintenance {
    task: tokio::task::JoinHandle<()>,
}

pub struct ShardAuthorityMaintenance {
    task: tokio::task::JoinHandle<()>,
}

impl ShardAuthorityMaintenance {
    pub(crate) fn from_task(task: tokio::task::JoinHandle<()>) -> Self {
        Self { task }
    }
}

impl Drop for ShardAuthorityMaintenance {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Drop for BranchIndexMaintenance {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct JournalNodeLocator<P: ObjectPlane> {
    indexes: Arc<JournalDerivedIndexes<P>>,
    branches: RwLock<BTreeSet<String>>,
}

impl<P: ObjectPlane> JournalNodeLocator<P> {
    fn register(&self, branch: &str) -> Result<()> {
        self.branches
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "locator lock poisoned"))?
            .insert(branch.to_string());
        Ok(())
    }

    fn registered_branches(&self) -> Result<Vec<String>> {
        Ok(self
            .branches
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "locator lock poisoned"))?
            .iter()
            .cloned()
            .collect())
    }
}

#[async_trait::async_trait]
impl<P: ObjectPlane> NodeLocator for JournalNodeLocator<P> {
    async fn locate(&self, cid: &prolly::Cid) -> Result<Option<LocatedPackedNode>> {
        let branches = self
            .branches
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "locator lock poisoned"))?
            .clone();
        for branch in branches {
            if let Some(entry) = self.indexes.node_location(&branch, cid).await? {
                return Ok(Some(entry.into()));
            }
        }
        Ok(None)
    }
}

struct GcDirtyRootObserver<P: ObjectPlane> {
    plane: Arc<P>,
    prefix: String,
    repository: crate::RepositoryId,
    instance: OperationId,
    clock: Arc<dyn Clock>,
    ticket_ttl_millis: u64,
    tickets: Mutex<BTreeMap<[u8; 32], HeldPublicationTicket>>,
}

struct HeldPublicationTicket {
    path: ObjectPath,
    version: PhysicalVersion,
    references: usize,
}

struct GcProcessState {
    active_epoch: Arc<RwLock<Option<OperationId>>>,
    sequence: Arc<AtomicU64>,
    publication_barrier: Arc<tokio::sync::RwLock<()>>,
}

fn gc_process_state(repository: crate::RepositoryId) -> Arc<GcProcessState> {
    static STATES: OnceLock<std::sync::Mutex<BTreeMap<crate::RepositoryId, Weak<GcProcessState>>>> =
        OnceLock::new();
    let states = STATES.get_or_init(|| std::sync::Mutex::new(BTreeMap::new()));
    let mut states = states
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    states.retain(|_, state| state.strong_count() > 0);
    if let Some(state) = states.get(&repository).and_then(Weak::upgrade) {
        return state;
    }
    let state = Arc::new(GcProcessState {
        active_epoch: Arc::new(RwLock::new(None)),
        sequence: Arc::new(AtomicU64::new(0)),
        publication_barrier: Arc::new(tokio::sync::RwLock::new(())),
    });
    states.insert(repository, Arc::downgrade(&state));
    state
}

#[async_trait::async_trait]
impl<P: ObjectPlane> MutableControlObserver for GcDirtyRootObserver<P> {
    async fn before_compare_exchange(
        &self,
        kind: MutableControlKind,
        request: &CompareExchange,
    ) -> Result<()> {
        if !matches!(
            kind,
            MutableControlKind::BranchRef | MutableControlKind::TagRef
        ) {
            return Ok(());
        }
        let request_digest = publication_ticket_digest(request);
        let now = self.clock.now_millis()?;
        let ticket = GcPublicationTicket {
            repository: self.repository,
            instance: self.instance,
            request_digest,
            expires_at_millis: now.checked_add(self.ticket_ttl_millis).ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "publication ticket expiry overflow",
                )
            })?,
        };
        let bytes = encode_canonical(&ticket)?;
        let path = gc_publication_ticket_path(
            &self.prefix,
            self.repository,
            self.instance,
            request_digest,
        )?;
        let stored = self
            .plane
            .put_immutable(crate::ImmutablePut {
                path: path.clone(),
                expected_sha256: crate::codec::sha256(&bytes),
                bytes,
            })
            .await?;
        let metadata = match stored {
            crate::ImmutablePutOutcome::Created(metadata)
            | crate::ImmutablePutOutcome::AlreadyPresent(metadata) => metadata,
        };
        let version = metadata.token.version_id.clone().map_or_else(
            || PhysicalVersion::Unversioned {
                token: Some(metadata.token.clone()),
            },
            |version_id| PhysicalVersion::Versioned { version_id },
        );

        let coordinator = self
            .plane
            .load_mutable(&gc_coordinator_path(&self.prefix)?)
            .await?
            .map(|stored| decode_canonical::<GcCoordinator>(&stored.bytes))
            .transpose()?;
        if coordinator.as_ref().is_some_and(|coordinator| {
            coordinator.repository != self.repository
                || coordinator.active_epoch.is_some()
                || coordinator.admission_closed
        }) {
            let _ = self.plane.delete_exact(&path, version).await;
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "repository publication admission is closed for maintenance",
            )
            .retry(crate::RetryAdvice::After(Duration::from_millis(250))));
        }
        let mut tickets = self.tickets.lock().map_err(|_| {
            Error::new(
                ErrorCode::InternalInvariant,
                "publication-ticket lock poisoned",
            )
        })?;
        tickets
            .entry(request_digest)
            .and_modify(|held| held.references = held.references.saturating_add(1))
            .or_insert(HeldPublicationTicket {
                path,
                version,
                references: 1,
            });
        Ok(())
    }

    async fn after_compare_exchange(
        &self,
        kind: MutableControlKind,
        request: &CompareExchange,
    ) -> Result<()> {
        if !matches!(
            kind,
            MutableControlKind::BranchRef | MutableControlKind::TagRef
        ) {
            return Ok(());
        }
        let digest = publication_ticket_digest(request);
        let release = {
            let mut tickets = self.tickets.lock().map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "publication-ticket lock poisoned",
                )
            })?;
            let Some(ticket) = tickets.get_mut(&digest) else {
                return Ok(());
            };
            ticket.references = ticket.references.saturating_sub(1);
            (ticket.references == 0)
                .then(|| tickets.remove(&digest))
                .flatten()
        };
        if let Some(ticket) = release {
            match self
                .plane
                .delete_exact(&ticket.path, ticket.version)
                .await?
            {
                DeleteOutcome::Deleted | DeleteOutcome::NotFound => {}
                DeleteOutcome::TokenMismatch => {
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "publication ticket changed before release",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Authoritative repository over one reserved object-store prefix.
///
/// This type reads and writes the sole repository format. It does not
/// negotiate, dual-write, or migrate alternative formats.
pub struct Repository<P: ObjectPlane> {
    plane: Arc<P>,
    options: RepositoryOptions,
    format: RepositoryFormat,
    node_store: ProllyObjectStore<P>,
    node_cache: Arc<dyn NodeCache>,
    authority: Arc<ShardWriterAuthority<P>>,
    publisher: ShardedBranchPublisher<P>,
    payloads: ImmutablePayloadStore<P>,
    commit_sessions: CommitSessionStore<P>,
    tags: TagStore<P>,
    ref_catalog: Arc<ShardedRefCatalog<P>>,
    operation_index: SegmentedOperationIndex<P>,
    journal_indexes: Arc<JournalDerivedIndexes<P>>,
    locator: Arc<JournalNodeLocator<P>>,
    permits: RwLock<BTreeMap<AuthorityScope, AuthorityPermit>>,
    fenced_scopes: RwLock<BTreeSet<AuthorityScope>>,
    authority_renewal: tokio::sync::Mutex<()>,
    commit_metadata_cache: std::sync::Mutex<CommitMetadataCache>,
    commit_metadata_fetch: tokio::sync::Mutex<()>,
    publication_lanes: std::sync::Mutex<BTreeMap<String, Weak<tokio::sync::Mutex<()>>>>,
    index_lanes: std::sync::Mutex<BTreeMap<String, Weak<tokio::sync::Mutex<()>>>>,
    local_index_heads: RwLock<BTreeMap<String, CommitId>>,
    index_errors: RwLock<BTreeMap<String, String>>,
    active_gc_epoch: Arc<RwLock<Option<OperationId>>>,
    gc_dirty_sequence: Arc<AtomicU64>,
    gc_publication_barrier: Arc<tokio::sync::RwLock<()>>,
    writable: AtomicBool,
}

impl<P: ObjectPlane> Repository<P> {
    pub async fn initialize(plane: Arc<P>, options: RepositoryOptions) -> Result<Self> {
        validate_options(&options)?;
        if options.read_only {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "cannot initialize a repository read-only",
            ));
        }
        let format_path = format_path(&options.repository_prefix)?;
        let operation = options.ids.operation();
        let created_at_millis = options.clock.now_millis()?;
        let repository_id = crate::model::derive_repository_id(operation);
        let proposed_format = RepositoryFormat {
            repository_id,
            state_tree_format: options.state_tree_format.clone(),
            canonical_limits: options.limits.clone(),
            idempotency_retention: options.idempotency_retention,
            provider_per_key_version_limit: options.provider_per_key_version_limit,
            created_at_millis,
        };
        let proposed_intent = InitializationIntent {
            repository_id,
            format: proposed_format,
            operation,
        };
        let intent_bytes = encode_canonical(&proposed_intent)?;
        let intent = match plane
            .compare_exchange(CompareExchange {
                path: intent_path(&options.repository_prefix)?,
                expected: None,
                bytes: intent_bytes,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => proposed_intent,
            CompareExchangeOutcome::Conflict(Some(existing)) => decode_canonical(&existing.bytes)?,
            CompareExchangeOutcome::Conflict(None) => {
                return Err(Error::new(
                    ErrorCode::RefConflict,
                    "initialization intent create returned an empty conflict",
                ))
            }
        };
        validate_format_compatibility(&intent.format, &options)?;

        // Advertise the  repository before creating any  ref or commit.
        let format_bytes = encode_canonical(&intent.format)?;
        match plane
            .compare_exchange(CompareExchange {
                path: format_path,
                expected: None,
                bytes: format_bytes.clone(),
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => {}
            CompareExchangeOutcome::Conflict(Some(existing)) if existing.bytes == format_bytes => {}
            CompareExchangeOutcome::Conflict(_) => {
                return Err(Error::new(
                    ErrorCode::RepositoryFormatConflict,
                    "a different repository format already exists",
                ))
            }
        }

        let repository = Self::from_format(plane, options, intent.format)?;
        repository.restore_gc_state().await?;
        let default_branch = repository.options.default_branch.clone();
        match repository.publisher.load(&default_branch).await {
            Ok(_) => {
                repository.locator.register(&default_branch)?;
                repository.advance_branch_indexes(&default_branch).await?;
                return Ok(repository);
            }
            Err(error) if error.code == ErrorCode::InvalidRevision => {}
            Err(error) => return Err(error),
        }
        let now_millis = repository.options.clock.now_millis()?;
        let permit = repository
            .authority
            .acquire(
                AuthorityScope::Branch {
                    name: default_branch.clone(),
                },
                &repository.options.writer,
                now_millis,
                repository.options.ids.operation(),
            )
            .await?;
        repository.install_permit(permit.clone())?;

        let empty = repository.engine(repository.node_store.clone()).create();
        let repository_created_at = repository.format.created_at_millis;
        let commit = BucketCommit {
            state: BucketState {
                objects: RootManifest::from_tree(&empty)?,
                versions: RootManifest::from_tree(&empty)?,
            },
            parents: Vec::new(),
            generation: CommitGeneration(0),
            delta: BucketDelta {
                input_digest: crate::model::derive_input_digest(&[b"initialize"]),
                changes: Vec::new(),
                changes_root: None,
                change_count: 0,
            },
            node_pack: None,
            authority: permit.stamp(),
            author: repository.options.writer.clone(),
            message: Some("initialize repository".to_string()),
            created_at_millis: repository_created_at,
            metadata: BTreeMap::new(),
        };
        repository
            .publisher
            .create(CommitPublication {
                permit: &permit,
                branch: &default_branch,
                commit: &commit,
                node_pack: None,
                operation: intent.operation,
                message: "initialize",
                now_millis,
            })
            .await?;
        repository.locator.register(&default_branch)?;
        repository.advance_branch_indexes(&default_branch).await?;
        Ok(repository)
    }

    pub async fn open(plane: Arc<P>, options: RepositoryOptions) -> Result<Self> {
        validate_options(&options)?;
        let stored = plane
            .get(GetRequest {
                path: format_path(&options.repository_prefix)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::RepositoryNotInitialized,
                    "repository format marker does not exist",
                )
            })?;
        let format: RepositoryFormat = decode_canonical(&stored.bytes)?;
        validate_format_compatibility(&format, &options)?;
        let repository = Self::from_format(plane, options, format)?;
        repository.restore_gc_state().await?;
        let branch = repository.options.default_branch.clone();
        repository.locator.register(&branch)?;
        if !repository.options.read_only {
            let permit = repository
                .authority
                .acquire(
                    AuthorityScope::Branch {
                        name: branch.clone(),
                    },
                    &repository.options.writer,
                    repository.options.clock.now_millis()?,
                    repository.options.ids.operation(),
                )
                .await?;
            repository.install_permit(permit)?;
        }
        Ok(repository)
    }

    fn from_format(
        plane: Arc<P>,
        options: RepositoryOptions,
        format: RepositoryFormat,
    ) -> Result<Self> {
        let node_cache = options.node_cache.clone().unwrap_or_else(|| {
            Arc::new(MemoryNodeCache::new(options.max_cached_node_bytes)) as Arc<dyn NodeCache>
        });
        let node_store = ProllyObjectStore::new_packed_with_node_cache(
            plane.clone(),
            options.repository_prefix.clone(),
            options.max_cached_node_pack_bytes,
            options.max_cached_node_locations,
            NodeCacheNamespace {
                repository: format.repository_id,
                tree_format: tree_format_digest(&format.state_tree_format)?,
            },
            node_cache.clone(),
        );
        let authority = Arc::new(ShardWriterAuthority::new_with_control_retention(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            Duration::from_millis(options.authority_lease_millis),
            options.mutable_control_versions_to_retain,
        )?);
        let gc_state = gc_process_state(format.repository_id);
        let active_gc_epoch = gc_state.active_epoch.clone();
        let gc_dirty_sequence = gc_state.sequence.clone();
        let gc_publication_barrier = gc_state.publication_barrier.clone();
        let gc_observer = Arc::new(GcDirtyRootObserver {
            plane: plane.clone(),
            prefix: options.repository_prefix.clone(),
            repository: format.repository_id,
            instance: options.ids.operation(),
            clock: options.clock.clone(),
            ticket_ttl_millis: options.authority_lease_millis,
            tickets: Mutex::new(BTreeMap::new()),
        });
        let publisher = ShardedBranchPublisher::new_with_gc_controls(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            authority.clone(),
            options.mutable_control_versions_to_retain,
            Some(gc_observer.clone()),
            Some(gc_publication_barrier.clone()),
        )?;
        let payloads = ImmutablePayloadStore::new(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
        );
        let tags = TagStore::new_with_gc_controls(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            authority.clone(),
            options.mutable_control_versions_to_retain,
            Some(gc_observer),
            Some(gc_publication_barrier.clone()),
        )?;
        let ref_catalog = Arc::new(ShardedRefCatalog::new_with_limits(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            format.state_tree_format.clone(),
            node_cache.clone(),
            options.mutable_control_versions_to_retain,
        )?);
        let commit_sessions = CommitSessionStore::new(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            format.canonical_limits.max_mutations_per_commit as usize,
        )?;
        let operation_index = SegmentedOperationIndex::new_with_limits(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            format.idempotency_retention,
            options.operation_index_leaf_entries,
            options.operation_index_merge_fanout,
            options.operation_index_max_unindexed_events,
            options.mutable_control_versions_to_retain,
        )?;
        let journal_indexes = Arc::new(JournalDerivedIndexes::new_with_limits(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            format.state_tree_format.clone(),
            node_cache.clone(),
            options.journal_index_max_unindexed_events,
            options.mutable_control_versions_to_retain,
        )?);
        let locator = Arc::new(JournalNodeLocator {
            indexes: journal_indexes.clone(),
            branches: RwLock::new(BTreeSet::new()),
        });
        node_store.set_node_locator(locator.clone())?;
        let writable = !options.read_only;
        let commit_metadata_cache_bytes = options.max_cached_node_bytes;
        Ok(Self {
            plane,
            options,
            format,
            node_store,
            node_cache,
            authority,
            publisher,
            payloads,
            commit_sessions,
            tags,
            ref_catalog,
            operation_index,
            journal_indexes,
            locator,
            permits: RwLock::new(BTreeMap::new()),
            fenced_scopes: RwLock::new(BTreeSet::new()),
            authority_renewal: tokio::sync::Mutex::new(()),
            commit_metadata_cache: std::sync::Mutex::new(CommitMetadataCache::new(
                commit_metadata_cache_bytes,
            )),
            commit_metadata_fetch: tokio::sync::Mutex::new(()),
            publication_lanes: std::sync::Mutex::new(BTreeMap::new()),
            index_lanes: std::sync::Mutex::new(BTreeMap::new()),
            local_index_heads: RwLock::new(BTreeMap::new()),
            index_errors: RwLock::new(BTreeMap::new()),
            active_gc_epoch,
            gc_dirty_sequence,
            gc_publication_barrier,
            writable: AtomicBool::new(writable),
        })
    }

    pub fn format(&self) -> &RepositoryFormat {
        &self.format
    }

    pub fn repository_id(&self) -> crate::RepositoryId {
        self.format.repository_id
    }

    pub fn max_object_bytes(&self) -> u64 {
        self.format.canonical_limits.max_object_bytes
    }

    pub fn plane(&self) -> Arc<P> {
        self.plane.clone()
    }

    pub fn node_cache_snapshot(&self) -> crate::NodeCacheSnapshot {
        self.node_store
            .node_cache_snapshot()
            .saturating_add(self.ref_catalog.node_cache_snapshot())
            .saturating_add(self.journal_indexes.node_cache_snapshot())
    }

    /// Traverse both current-state trees to populate the configured node
    /// cache. This is an explicit full-snapshot operation; use it during
    /// startup or controlled prewarming rather than on request paths.
    pub async fn prewarm_node_cache(
        &self,
        branch: &str,
        snapshot: CommitId,
    ) -> Result<NodeCachePrewarmReport> {
        validate_branch(branch)?;
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        let before = self.node_store.node_cache_snapshot();
        let commit = self.load_commit_object(snapshot).await?.commit;
        let engine = self.engine(self.node_store.clone());
        let object_stats = engine
            .collect_stats(&self.tree_from_root(&commit.state.objects)?)
            .await?;
        let version_stats = engine
            .collect_stats(&self.tree_from_root(&commit.state.versions)?)
            .await?;
        Ok(NodeCachePrewarmReport {
            snapshot,
            object_nodes: object_stats.num_nodes,
            version_nodes: version_stats.num_nodes,
            before,
            after: self.node_store.node_cache_snapshot(),
        })
    }

    pub async fn head(&self, branch: &str) -> Result<CommitId> {
        self.locator.register(branch)?;
        Ok(self.publisher.load(branch).await?.value.target)
    }

    pub async fn create_branch(&self, name: &str, from: CommitId) -> Result<BranchHead> {
        self.create_branch_from(&self.options.default_branch, name, from)
            .await
    }

    pub async fn create_branch_from(
        &self,
        source_branch: &str,
        name: &str,
        from: CommitId,
    ) -> Result<BranchHead> {
        crate::repository::validate_branch(name)?;
        crate::repository::validate_branch(source_branch)?;
        self.locator.register(source_branch)?;
        self.advance_branch_indexes(source_branch).await?;
        self.journal_indexes
            .require_branch_covers(source_branch, from)
            .await?;
        let _lane = self.lock_branch(name).await;
        let now = self.options.clock.now_millis()?;
        let permit = self.active_permit(name, now).await?;
        let reference = self
            .publisher
            .create_at_target(
                &permit,
                name,
                from,
                self.options.ids.operation(),
                "create branch",
                now,
            )
            .await?;
        self.locator.register(name)?;
        self.journal_indexes
            .initialize_branch_from(source_branch, name, &reference, now)
            .await?;
        self.advance_branch_indexes(name).await?;
        Ok(BranchHead {
            name: name.to_string(),
            target: reference.value.target,
            generation: reference.value.generation,
        })
    }

    pub async fn delete_branch(&self, name: &str, expected: CommitId) -> Result<()> {
        crate::repository::validate_branch(name)?;
        let _lane = self.lock_branch(name).await;
        let current = self.publisher.load(name).await?;
        let now = self.options.clock.now_millis()?;
        let permit = self.active_permit(name, now).await?;
        let deleted = self
            .publisher
            .delete(
                &permit,
                name,
                current,
                expected,
                self.options.ids.operation(),
                now,
            )
            .await?;
        self.record_branch_catalog(&deleted).await?;
        self.local_index_heads
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "local-index lock poisoned"))?
            .remove(name);
        Ok(())
    }

    pub async fn tag(&self, name: &str) -> Result<Tag> {
        let loaded = self.tags.load(name).await?;
        Ok(Tag {
            name: name.to_string(),
            target: loaded.value.target,
            generation: loaded.value.generation,
        })
    }

    pub async fn create_tag(&self, name: &str, target: CommitId) -> Result<Tag> {
        crate::repository::validate_branch(name)?;
        let _lane = self.lock_branch(&format!("tag:{name}")).await;
        self.load_commit_metadata(target).await?;
        let now = self.options.clock.now_millis()?;
        let permit = self.active_system_permit("tags", now).await?;
        let tag = self
            .tags
            .create(
                &permit,
                name,
                target,
                self.options.ids.operation(),
                &self.options.writer,
                now,
            )
            .await?;
        self.record_tag_catalog(name, &tag.value).await?;
        Ok(Tag {
            name: name.to_string(),
            target,
            generation: tag.value.generation,
        })
    }

    pub async fn delete_tag(&self, name: &str, expected: CommitId) -> Result<()> {
        crate::repository::validate_branch(name)?;
        let _lane = self.lock_branch(&format!("tag:{name}")).await;
        let current = self.tags.load(name).await?;
        let now = self.options.clock.now_millis()?;
        let permit = self.active_system_permit("tags", now).await?;
        let deleted = self
            .tags
            .delete(
                &permit,
                name,
                current,
                expected,
                self.options.ids.operation(),
                now,
            )
            .await?;
        self.record_tag_catalog(name, &deleted.value).await?;
        Ok(())
    }

    pub async fn create_retention_pin(&self, name: &str, target: CommitId) -> Result<RetentionPin> {
        let tag_name = retention_pin_tag(name)?;
        let tag = self.create_tag(&tag_name, target).await?;
        Ok(RetentionPin {
            name: name.to_string(),
            target: tag.target,
            generation: tag.generation,
        })
    }

    pub async fn retention_pin(&self, name: &str) -> Result<RetentionPin> {
        let tag = self.tag(&retention_pin_tag(name)?).await?;
        Ok(RetentionPin {
            name: name.to_string(),
            target: tag.target,
            generation: tag.generation,
        })
    }

    pub async fn delete_retention_pin(&self, name: &str, expected: CommitId) -> Result<()> {
        self.delete_tag(&retention_pin_tag(name)?, expected).await
    }

    pub async fn list_retention_pins_page(
        &self,
        cursor: Option<RefCatalogCursor>,
        limit: usize,
    ) -> Result<RetentionPinPage> {
        let page = self.list_tag_catalog_page(cursor, limit).await?;
        let mut pins = Vec::new();
        for tag in page.tags {
            if let Some(name) = decode_retention_pin_tag(&tag.name)? {
                pins.push(RetentionPin {
                    name,
                    target: tag.target,
                    generation: tag.generation,
                });
            }
        }
        Ok(RetentionPinPage {
            pins,
            continuation: page.continuation,
        })
    }

    pub async fn list_branch_catalog_page(
        &self,
        cursor: Option<RefCatalogCursor>,
        limit: usize,
    ) -> Result<BranchCatalogPage> {
        let page = self
            .ref_catalog
            .list(RefKind::Branch, cursor, limit)
            .await?;
        Ok(BranchCatalogPage {
            branches: page
                .entries
                .into_iter()
                .map(|entry| BranchHead {
                    name: entry.name,
                    target: entry.target,
                    generation: entry.generation,
                })
                .collect(),
            continuation: page.continuation,
        })
    }

    pub async fn list_tag_catalog_page(
        &self,
        cursor: Option<RefCatalogCursor>,
        limit: usize,
    ) -> Result<TagCatalogPage> {
        let page = self.ref_catalog.list(RefKind::Tag, cursor, limit).await?;
        Ok(TagCatalogPage {
            tags: page
                .entries
                .into_iter()
                .map(|entry| Tag {
                    name: entry.name,
                    target: entry.target,
                    generation: entry.generation,
                })
                .collect(),
            continuation: page.continuation,
        })
    }

    /// Explicit bounded repair for a catalog shard stream. Normal lifecycle
    /// maintenance is event-driven and never calls this namespace scanner.
    pub async fn repair_ref_catalog_page(
        &self,
        kind: RefKind,
        continuation: Option<String>,
        limit: usize,
    ) -> Result<RefCatalogRepairPage> {
        if !(1..=1_000).contains(&limit) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "ref-catalog repair page must contain 1 to 1,000 refs",
            ));
        }
        let namespace = match kind {
            RefKind::Branch => "heads",
            RefKind::Tag => "tags",
        };
        let prefix = format!("{}/refs/{namespace}/", self.options.repository_prefix);
        let page = self
            .plane
            .list(ListRequest {
                prefix: prefix.clone(),
                continuation,
                limit,
                include_versions: false,
            })
            .await?;
        let mut report = RefCatalogRepairPage {
            continuation: page.continuation,
            ..RefCatalogRepairPage::default()
        };
        for entry in page.entries {
            report.scanned += 1;
            let encoded_name = entry.path.as_str().strip_prefix(&prefix).ok_or_else(|| {
                Error::new(ErrorCode::CorruptCommit, "ref repair escaped its namespace")
            })?;
            let name = String::from_utf8(hex::decode(encoded_name).map_err(|_| {
                Error::new(
                    ErrorCode::CorruptCommit,
                    "ref path name is not canonical hex",
                )
            })?)
            .map_err(|_| Error::new(ErrorCode::CorruptCommit, "ref path name is not UTF-8"))?;
            let Some(stored) = self.plane.load_mutable(&entry.path).await? else {
                continue;
            };
            match kind {
                RefKind::Branch => {
                    let value: crate::RefValue = decode_canonical(&stored.bytes)?;
                    value.validate(self.format.repository_id, &name)?;
                    self.ref_catalog
                        .record(
                            kind,
                            &name,
                            value.target,
                            value.generation,
                            value.operation,
                            value.tombstone,
                            value.updated_at_millis,
                        )
                        .await?;
                }
                RefKind::Tag => {
                    let value: crate::TagValue = decode_canonical(&stored.bytes)?;
                    value.validate(self.format.repository_id, &name)?;
                    self.ref_catalog
                        .record(
                            kind,
                            &name,
                            value.target,
                            value.generation,
                            value.operation,
                            value.tombstone,
                            value.updated_at_millis,
                        )
                        .await?;
                }
            }
            report.indexed += 1;
        }
        Ok(report)
    }

    /// Explicitly transfer one branch shard to this repository process.
    ///
    /// Open read-only before takeover so no existing local permit or derived
    /// client can race the branch-ref barrier.
    pub async fn takeover_branch_writer(
        &self,
        branch: &str,
        expected_writer: &str,
        expected_generation: u64,
        handoff_evidence: &str,
    ) -> Result<u64> {
        if self.writable.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "repository takeover requires a read-only repository handle",
            ));
        }
        let _lane = self.lock_branch(branch).await;
        let now = self.options.clock.now_millis()?;
        let pending = self
            .authority
            .begin_takeover(TakeoverRequest {
                scope: AuthorityScope::Branch {
                    name: branch.to_string(),
                },
                expected_writer: expected_writer.to_string(),
                expected_generation,
                next_writer: self.options.writer.clone(),
                handoff_evidence: handoff_evidence.to_string(),
                now_millis: now,
                nonce: self.options.ids.operation(),
            })
            .await?;
        let generation = pending.stamp().generation;
        let current = self.publisher.load(branch).await?;
        let applied = self
            .publisher
            .publish_takeover_barrier(
                branch,
                current,
                &pending,
                self.options.ids.operation(),
                "branch authority takeover",
                now,
            )
            .await?;
        let permit = self
            .authority
            .activate_after_barrier(pending, applied.into_barrier(), now)
            .await?;
        self.install_permit(permit)?;
        self.writable.store(true, Ordering::Release);
        Ok(generation)
    }

    pub async fn begin_commit_session(
        &self,
        branch: &str,
        message: impl Into<String>,
        expires_after_millis: u64,
    ) -> Result<CommitSessionManifest> {
        crate::repository::validate_branch(branch)?;
        let message = message.into();
        let now = self.options.clock.now_millis()?;
        let expires_at_millis = now.checked_add(expires_after_millis).ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidLimit,
                "repository commit-session expiry overflow",
            )
        })?;
        let permit = self.active_permit(branch, now).await?;
        let session = CommitSessionManifest {
            id: self.options.ids.batch(),
            branch: branch.to_string(),
            base_commit: self.publisher.load(branch).await?.value.target,
            identity: MutationIdentity {
                repository: self.format.repository_id,
                operation: self.options.ids.operation(),
                authority: permit.stamp(),
            },
            message,
            created_at_millis: now,
            expires_at_millis,
        };
        session.validate(self.format.repository_id)?;
        Ok(session)
    }

    pub async fn begin_durable_commit_session(
        &self,
        branch: &str,
        message: impl Into<String>,
        expires_after_millis: u64,
    ) -> Result<CommitSessionCheckpoint> {
        let session = self
            .begin_commit_session(branch, message, expires_after_millis)
            .await?;
        let checkpoint = CommitSessionCheckpoint {
            session,
            sequence: 0,
            mutations: Vec::new(),
            state: CommitSessionState::Open,
        };
        self.commit_sessions.save(&checkpoint).await?;
        Ok(checkpoint)
    }

    pub async fn checkpoint_commit_session(
        &self,
        session: &CommitSessionManifest,
        mutations: Vec<StagedMutation>,
        sequence: u64,
    ) -> Result<CommitSessionCheckpoint> {
        self.validate_commit_session(session).await?;
        let checkpoint = CommitSessionCheckpoint {
            session: session.clone(),
            sequence,
            mutations: self.canonical_session_mutations(mutations, true)?,
            state: CommitSessionState::Open,
        };
        self.commit_sessions.save(&checkpoint).await?;
        Ok(checkpoint)
    }

    /// Resume the newest durable checkpoint and adopt it into this process's
    /// current branch-authority epoch. Adoption is allowed only while the
    /// original base commit is still the branch head.
    pub async fn resume_commit_session(
        &self,
        batch: crate::BatchId,
    ) -> Result<CommitSessionCheckpoint> {
        let mut checkpoint = self.commit_sessions.latest(batch).await?.ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidRequest,
                "repository commit session does not exist",
            )
        })?;
        if checkpoint.state != CommitSessionState::Open {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "repository commit session was aborted",
            ));
        }
        let now = self.options.clock.now_millis()?;
        if checkpoint.session.expires_at_millis < now {
            return Err(Error::new(
                ErrorCode::BatchExpired,
                "repository commit session expired",
            ));
        }
        let permit = self.active_permit(&checkpoint.session.branch, now).await?;
        if checkpoint.session.identity.authority.writer_id != permit.stamp().writer_id {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "another writer cannot adopt a durable repository commit session",
            )
            .operation(checkpoint.session.identity.operation.to_string()));
        }
        let current = self.publisher.load(&checkpoint.session.branch).await?;
        if current.value.target != checkpoint.session.base_commit {
            return Err(Error::new(
                ErrorCode::BatchConflict,
                "repository branch moved since the durable session checkpoint",
            )
            .operation(checkpoint.session.identity.operation.to_string()));
        }
        if checkpoint.session.identity.authority != permit.stamp() {
            checkpoint.sequence = checkpoint.sequence.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InvalidLimit, "checkpoint sequence overflow")
            })?;
            checkpoint.session.identity.authority = permit.stamp();
            self.commit_sessions.save(&checkpoint).await?;
        }
        Ok(checkpoint)
    }

    pub async fn abort_commit_session(
        &self,
        session: CommitSessionManifest,
        mutations: Vec<StagedMutation>,
        sequence: u64,
    ) -> Result<()> {
        self.validate_commit_session(&session).await?;
        let checkpoint = CommitSessionCheckpoint {
            session,
            sequence,
            mutations: self.canonical_session_mutations(mutations, true)?,
            state: CommitSessionState::Aborted,
        };
        self.commit_sessions.save(&checkpoint).await
    }

    pub async fn cleanup_expired_commit_sessions(
        &self,
        continuation: Option<String>,
        limit: usize,
    ) -> Result<CommitSessionCleanupReport> {
        self.commit_sessions
            .cleanup_expired_page(self.options.clock.now_millis()?, continuation, limit)
            .await
    }

    pub async fn stage_commit_session_put(
        &self,
        session: &CommitSessionManifest,
        key: Vec<u8>,
        bytes: Vec<u8>,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<StagedMutation> {
        self.validate_commit_session(session).await?;
        self.validate_key(&key)?;
        if bytes.len() as u64 > self.format.canonical_limits.max_object_bytes {
            return Err(Error::new(
                ErrorCode::EntityTooLarge,
                "object exceeds the repository object-size limit",
            ));
        }
        let size = bytes.len() as u64;
        let checksum_md5: [u8; 16] = Md5::digest(&bytes).into();
        let checksum_sha256: [u8; 32] = Sha256::digest(&bytes).into();
        let binding = self.payloads.put(bytes).await?;
        Ok(StagedMutation {
            body: StagedMutationBody::Put(Box::new(StagedPut {
                key,
                size,
                logical_etag: format!("\"{}\"", hex::encode(checksum_md5)),
                checksums: Checksums {
                    md5: Some(checksum_md5),
                    sha256: Some(checksum_sha256),
                    algorithm_values: BTreeMap::new(),
                },
                headers,
                user_metadata,
                tags: BTreeMap::new(),
                binding,
            })),
        })
    }

    /// Stage a bounded input window, packing non-empty payloads up to 4 KiB
    /// into deterministic immutable segments no larger than 4 MiB. Larger and
    /// empty payloads retain the direct content-addressed representation.
    pub async fn stage_commit_session_put_batch(
        &self,
        session: &CommitSessionManifest,
        mut objects: Vec<CommitSessionPutInput>,
        concurrency: usize,
    ) -> Result<Vec<StagedMutation>> {
        self.validate_commit_session(session).await?;
        if concurrency == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "payload staging concurrency is zero",
            ));
        }
        objects.sort_by(|left, right| left.0.cmp(&right.0));
        for pair in objects.windows(2) {
            if pair[0].0 == pair[1].0 {
                return Err(Error::new(
                    ErrorCode::InvalidRequest,
                    "payload staging window contains a duplicate key",
                ));
            }
        }
        for (key, bytes, _, _) in &objects {
            self.validate_key(key)?;
            if bytes.len() as u64 > self.format.canonical_limits.max_object_bytes {
                return Err(Error::new(
                    ErrorCode::EntityTooLarge,
                    "object exceeds the repository object-size limit",
                ));
            }
        }

        const SMALL_OBJECT_MAX: usize = 4 * 1024;
        const PACK_MAX: usize = 4 * 1024 * 1024;
        let mut packed_groups = Vec::new();
        let mut current = Vec::new();
        let mut current_bytes = 0_usize;
        let mut direct = Vec::new();
        for object in objects {
            if object.1.is_empty() || object.1.len() > SMALL_OBJECT_MAX {
                direct.push(object);
                continue;
            }
            if !current.is_empty() && current_bytes.saturating_add(object.1.len()) > PACK_MAX {
                packed_groups.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current_bytes = current_bytes.saturating_add(object.1.len());
            current.push(object);
        }
        if !current.is_empty() {
            packed_groups.push(current);
        }

        let mut staged = Vec::new();
        for group in packed_groups {
            let pack_inputs = group
                .iter()
                .map(|(_, bytes, _, _)| (crate::codec::sha256(bytes), bytes.clone()))
                .collect();
            let bindings = self.payloads.put_pack(pack_inputs).await?;
            for ((key, bytes, headers, user_metadata), binding) in group.into_iter().zip(bindings) {
                staged.push(staged_put(key, bytes, headers, user_metadata, binding));
            }
        }
        let direct = stream::iter(direct)
            .map(|(key, bytes, headers, user_metadata)| async move {
                self.stage_commit_session_put(session, key, bytes, headers, user_metadata)
                    .await
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
        staged.extend(direct.into_iter().collect::<Result<Vec<_>>>()?);
        staged.sort_by(|left, right| left.key().cmp(right.key()));
        Ok(staged)
    }

    /// Rebind existing logical content into fresh small-object packs while
    /// preserving object headers, metadata, and tags.
    pub async fn stage_commit_session_repack_batch(
        &self,
        session: &CommitSessionManifest,
        objects: Vec<CommitSessionRepackInput>,
        concurrency: usize,
    ) -> Result<Vec<StagedMutation>> {
        let mut tags = objects
            .iter()
            .map(|(key, _, _, _, tags)| (key.clone(), tags.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut staged = self
            .stage_commit_session_put_batch(
                session,
                objects
                    .into_iter()
                    .map(|(key, bytes, headers, metadata, _)| (key, bytes, headers, metadata))
                    .collect(),
                concurrency,
            )
            .await?;
        for mutation in &mut staged {
            let StagedMutationBody::Put(put) = &mut mutation.body else {
                continue;
            };
            put.tags = tags.remove(&put.key).unwrap_or_default();
        }
        Ok(staged)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn stage_commit_session_file(
        &self,
        session: &CommitSessionManifest,
        key: Vec<u8>,
        body_path: PathBuf,
        size: u64,
        checksum_sha256: [u8; 32],
        checksum_md5: [u8; 16],
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<StagedMutation> {
        self.validate_commit_session(session).await?;
        self.validate_key(&key)?;
        if size > self.format.canonical_limits.max_object_bytes {
            return Err(Error::new(
                ErrorCode::EntityTooLarge,
                "object exceeds the repository object-size limit",
            ));
        }
        let binding = self
            .payloads
            .put_file(body_path, size, checksum_sha256)
            .await?;
        Ok(StagedMutation {
            body: StagedMutationBody::Put(Box::new(StagedPut {
                key,
                size,
                logical_etag: format!("\"{}\"", hex::encode(checksum_md5)),
                checksums: Checksums {
                    md5: Some(checksum_md5),
                    sha256: Some(checksum_sha256),
                    algorithm_values: BTreeMap::new(),
                },
                headers,
                user_metadata,
                tags: BTreeMap::new(),
                binding,
            })),
        })
    }

    pub async fn publish_commit_session(
        &self,
        session: CommitSessionManifest,
        mutations: Vec<StagedMutation>,
    ) -> Result<CommitReceipt> {
        session.validate(self.format.repository_id)?;
        if session.expires_at_millis < self.options.clock.now_millis()? {
            return Err(Error::new(
                ErrorCode::BatchExpired,
                "repository commit session is expired",
            ));
        }
        let canonical_mutations = self.canonical_session_mutations(mutations, false)?;
        let ordered = canonical_mutations
            .iter()
            .cloned()
            .map(|mutation| (mutation.key().to_vec(), mutation))
            .collect::<BTreeMap<_, _>>();
        let input_digest = crate::model::derive_input_digest(&[
            b"commit-session",
            session.branch.as_bytes(),
            session.base_commit.as_bytes(),
            &encode_canonical(&canonical_mutations)?,
        ]);
        let _lane = self.lock_branch(&session.branch).await;
        let now = self.options.clock.now_millis()?;
        if let Some(receipt) = self
            .reconcile_operation(
                &session.branch,
                session.identity.operation,
                input_digest,
                now,
            )
            .await?
        {
            return Ok(receipt);
        }
        let permit = self.active_permit(&session.branch, now).await?;
        if permit.stamp() != session.identity.authority {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "repository commit session belongs to another authority epoch",
            ));
        }
        self.require_branch_indexes_ready(&session.branch).await?;
        let current = self.publisher.load(&session.branch).await?;
        if current.value.target != session.base_commit {
            return Err(Error::new(
                ErrorCode::BatchConflict,
                "repository branch moved since commit-session creation",
            )
            .operation(session.identity.operation.to_string()));
        }
        let base = self.load_commit_object(current.value.target).await?.commit;
        let write_store = self.node_store.isolated_write_session();
        let engine = self.engine(write_store.clone());
        let objects = self.tree_from_root(&base.state.objects)?;
        let versions = self.tree_from_root(&base.state.versions)?;
        let keys = ordered.keys().cloned().collect::<Vec<_>>();
        let previous_values = engine.get_many(&objects, &keys).await?;
        let generation = CommitGeneration(base.generation.0.checked_add(1).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "commit generation overflow")
        })?);
        let mut object_mutations = Vec::with_capacity(ordered.len());
        let mut version_mutations = Vec::with_capacity(ordered.len());
        let mut transitions = Vec::with_capacity(ordered.len());
        let mut object_versions = Vec::with_capacity(ordered.len());
        for (ordinal, ((key, mutation), previous)) in
            ordered.iter().zip(previous_values).enumerate()
        {
            let previous = previous
                .map(|encoded| decode_canonical::<CurrentObject>(&encoded))
                .transpose()?
                .map(|current| current.version.id);
            let (kind, binding) = match &mutation.body {
                StagedMutationBody::Put(staged) => {
                    let StagedPut {
                        size,
                        logical_etag,
                        checksums,
                        headers,
                        user_metadata,
                        tags,
                        binding,
                        ..
                    } = staged.as_ref();
                    (
                        LogicalObjectVersionKind::Live {
                            size: *size,
                            logical_etag: logical_etag.clone(),
                            headers: headers.clone(),
                            checksums: checksums.clone(),
                            user_metadata: user_metadata.clone(),
                            tags: tags.clone(),
                        },
                        Some(binding.clone()),
                    )
                }
                StagedMutationBody::Delete { .. } => (LogicalObjectVersionKind::DeleteMarker, None),
            };
            let version = ObjectVersion::derive(
                self.format.repository_id,
                key,
                session.identity.operation,
                LogicalObjectVersionBody {
                    order: ObjectVersionOrder {
                        commit_generation: generation,
                        mutation_ordinal: u32::try_from(ordinal).map_err(|_| {
                            Error::new(ErrorCode::InvalidLimit, "mutation ordinal overflow")
                        })?,
                    },
                    created_at_millis: now,
                    kind,
                },
                binding,
            )?;
            let delete_marker = matches!(version.body.kind, LogicalObjectVersionKind::DeleteMarker);
            if delete_marker {
                object_mutations.push(Mutation::Delete { key: key.clone() });
            } else {
                object_mutations.push(Mutation::Upsert {
                    key: key.clone(),
                    val: encode_canonical(&CurrentObject {
                        version: version.clone(),
                    })?,
                });
            }
            version_mutations.push(Mutation::Upsert {
                key: version_tree_key(key, version.body.order, version.id),
                val: encode_canonical(&version)?,
            });
            transitions.push(ObjectTransition {
                key: key.clone(),
                previous,
                next: version.id,
                delete_marker,
            });
            object_versions.push(version.id);
        }
        let objects = engine.batch(&objects, object_mutations).await?;
        let versions = engine.batch(&versions, version_mutations).await?;
        let (inline_transitions, changes_root, change_count) =
            if transitions.len() > INLINE_COMMIT_DELTA_LIMIT {
                let delta = engine.create();
                let delta = engine
                    .batch(
                        &delta,
                        transitions
                            .iter()
                            .map(|transition| {
                                Ok(Mutation::Upsert {
                                    key: transition.key.clone(),
                                    val: encode_canonical(transition)?,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )
                    .await?;
                (
                    Vec::new(),
                    Some(RootManifest::from_tree(&delta)?),
                    transitions.len() as u64,
                )
            } else {
                (transitions, None, 0)
            };
        let prepared = write_store.prepare_node_pack(
            tree_format_digest(&self.format.state_tree_format)?,
            Vec::new(),
        )?;
        let commit = BucketCommit {
            state: BucketState {
                objects: RootManifest::from_tree(&objects)?,
                versions: RootManifest::from_tree(&versions)?,
            },
            parents: vec![current.value.target],
            generation,
            delta: BucketDelta {
                input_digest,
                changes: inline_transitions,
                changes_root,
                change_count,
            },
            node_pack: prepared.as_ref().map(PreparedNodePack::reference),
            authority: permit.stamp(),
            author: self.options.writer.clone(),
            message: Some(session.message.clone()),
            created_at_millis: now,
            metadata: BTreeMap::new(),
        };
        let publication = self
            .publisher
            .store_and_publish(
                current,
                CommitPublication {
                    permit: &permit,
                    branch: &session.branch,
                    commit: &commit,
                    node_pack: prepared.as_ref().map(PreparedNodePack::pack),
                    operation: session.identity.operation,
                    message: &session.message,
                    now_millis: now,
                },
            )
            .await;
        match publication {
            Ok(published) => {
                self.finalize_pack(published.value.target, &commit, prepared)
                    .await?;
                self.mark_local_index_head(&session.branch, published.value.target)?;
                Ok(CommitReceipt {
                    id: published.value.target,
                    operation: session.identity.operation,
                    branch: session.branch,
                    parents: commit.parents,
                    changed_keys: object_versions.len() as u64,
                    object_versions,
                    idempotent_replay: false,
                })
            }
            Err(error) => {
                if let Some(receipt) = self
                    .reconcile_operation(
                        &session.branch,
                        session.identity.operation,
                        input_digest,
                        now,
                    )
                    .await?
                {
                    self.finalize_pack(receipt.id, &commit, prepared).await?;
                    return Ok(receipt);
                }
                self.fence_branch(&session.branch)?;
                Err(error)
            }
        }
    }

    pub async fn put_object(
        &self,
        branch: &str,
        key: Vec<u8>,
        bytes: Vec<u8>,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<CommitReceipt> {
        let operation = self.options.ids.operation();
        // The operation ID was allocated inside this process and cannot be a
        // retry of an earlier publication. Avoid walking the unindexed journal
        // to prove absence on every hot-branch write; caller-stable operation
        // IDs still take the full reconciliation path below.
        self.put_object_inner(branch, key, bytes, headers, user_metadata, operation, false)
            .await
    }

    async fn validate_commit_session(&self, session: &CommitSessionManifest) -> Result<()> {
        session.validate(self.format.repository_id)?;
        let now = self.options.clock.now_millis()?;
        if session.expires_at_millis < now {
            return Err(Error::new(
                ErrorCode::BatchExpired,
                "repository commit session expired",
            ));
        }
        let permit = self.active_permit(&session.branch, now).await?;
        if permit.stamp() != session.identity.authority {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "repository commit session belongs to another authority epoch",
            ));
        }
        Ok(())
    }

    fn canonical_session_mutations(
        &self,
        mutations: Vec<StagedMutation>,
        allow_empty: bool,
    ) -> Result<Vec<StagedMutation>> {
        if (!allow_empty && mutations.is_empty())
            || mutations.len() > self.format.canonical_limits.max_mutations_per_commit as usize
        {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "repository commit session has an invalid mutation count",
            ));
        }
        let mut ordered = BTreeMap::new();
        for mutation in mutations {
            self.validate_key(mutation.key())?;
            if let StagedMutationBody::Put(staged) = &mutation.body {
                self.validate_staged_put(staged)?;
            }
            if ordered.insert(mutation.key().to_vec(), mutation).is_some() {
                return Err(Error::new(
                    ErrorCode::InvalidRequest,
                    "repository commit session contains the same key more than once",
                ));
            }
        }
        Ok(ordered.into_values().collect())
    }

    fn validate_staged_put(&self, staged: &StagedPut) -> Result<()> {
        staged.binding.validate()?;
        let expected_etag = staged
            .checksums
            .md5
            .map(|md5| format!("\"{}\"", hex::encode(md5)));
        if staged.size > self.format.canonical_limits.max_object_bytes
            || staged.binding.path != self.payloads.expected_path(&staged.binding)?
            || staged.checksums.sha256 != Some(staged.binding.checksum_sha256)
            || expected_etag.as_deref() != Some(staged.logical_etag.as_str())
        {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "staged payload identity does not match its immutable binding",
            ));
        }
        Ok(())
    }

    pub async fn put_object_with_operation(
        &self,
        branch: &str,
        key: Vec<u8>,
        bytes: Vec<u8>,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        operation: OperationId,
    ) -> Result<CommitReceipt> {
        self.put_object_inner(branch, key, bytes, headers, user_metadata, operation, true)
            .await
    }

    async fn put_object_inner(
        &self,
        branch: &str,
        key: Vec<u8>,
        bytes: Vec<u8>,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        operation: OperationId,
        reconcile_before_publication: bool,
    ) -> Result<CommitReceipt> {
        if !self.writable.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "repository is read-only",
            ));
        }
        self.validate_key(&key)?;
        if bytes.len() as u64 > self.format.canonical_limits.max_object_bytes {
            return Err(Error::new(
                ErrorCode::EntityTooLarge,
                "object exceeds the repository object-size limit",
            ));
        }
        let metadata_bytes = encode_canonical(&user_metadata)?;
        let headers_bytes = encode_canonical(&headers)?;
        let size = bytes.len() as u64;
        let checksum_md5: [u8; 16] = Md5::digest(&bytes).into();
        let checksum_sha256: [u8; 32] = Sha256::digest(&bytes).into();
        let input_digest = crate::model::derive_input_digest(&[
            b"put",
            branch.as_bytes(),
            &key,
            &checksum_sha256,
            &headers_bytes,
            &metadata_bytes,
        ]);
        // Resolve idempotent replays and reject a stale authority before any
        // payload bytes enter the object plane. Payload upload is deliberately
        // outside the branch publication lane: independent callers can hash
        // and upload immutable content concurrently while only the tree update
        // and ref CAS remain serialized.
        let prepare_now = self.options.clock.now_millis()?;
        let prepare_permit = self.active_permit(branch, prepare_now).await?;
        self.authority
            .validate_active(&prepare_permit, prepare_now)
            .await?;
        self.require_branch_indexes_ready(branch).await?;
        if reconcile_before_publication {
            if let Some(receipt) = self
                .reconcile_operation(branch, operation, input_digest, prepare_now)
                .await?
            {
                return Ok(receipt);
            }
        }
        let binding = self.payloads.put(bytes).await?;

        let _lane = self.lock_branch(branch).await;
        let now = self.options.clock.now_millis()?;
        let permit = self.active_permit(branch, now).await?;
        self.authority.validate_active(&permit, now).await?;
        if reconcile_before_publication {
            if let Some(receipt) = self
                .reconcile_operation(branch, operation, input_digest, now)
                .await?
            {
                return Ok(receipt);
            }
        }
        self.require_branch_indexes_ready(branch).await?;

        let current = self.publisher.load(branch).await?;
        let base = self.load_commit_object(current.value.target).await?.commit;
        let write_store = self.node_store.isolated_write_session();
        let engine = self.engine(write_store.clone());
        let mut objects = self.tree_from_root(&base.state.objects)?;
        let mut versions = self.tree_from_root(&base.state.versions)?;
        let previous = engine
            .get(&objects, &key)
            .await?
            .map(|encoded| decode_canonical::<CurrentObject>(&encoded))
            .transpose()?
            .map(|current| current.version.id);
        let generation = CommitGeneration(base.generation.0.checked_add(1).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "commit generation overflow")
        })?);
        let body = LogicalObjectVersionBody {
            order: ObjectVersionOrder {
                commit_generation: generation,
                mutation_ordinal: 0,
            },
            created_at_millis: now,
            kind: LogicalObjectVersionKind::Live {
                size,
                logical_etag: format!("\"{}\"", hex::encode(checksum_md5)),
                headers,
                checksums: Checksums {
                    md5: Some(checksum_md5),
                    sha256: Some(checksum_sha256),
                    algorithm_values: BTreeMap::new(),
                },
                user_metadata,
                tags: BTreeMap::new(),
            },
        };
        let version = ObjectVersion::derive(
            self.format.repository_id,
            &key,
            operation,
            body,
            Some(binding),
        )?;
        objects = engine
            .put(
                &objects,
                key.clone(),
                encode_canonical(&CurrentObject {
                    version: version.clone(),
                })?,
            )
            .await?;
        versions = engine
            .put(
                &versions,
                version_tree_key(&key, version.body.order, version.id),
                encode_canonical(&version)?,
            )
            .await?;
        let prepared = write_store.prepare_node_pack(
            tree_format_digest(&self.format.state_tree_format)?,
            Vec::new(),
        )?;
        let node_pack = prepared.as_ref().map(PreparedNodePack::reference);
        let commit = BucketCommit {
            state: BucketState {
                objects: RootManifest::from_tree(&objects)?,
                versions: RootManifest::from_tree(&versions)?,
            },
            parents: vec![current.value.target],
            generation,
            delta: BucketDelta {
                input_digest,
                changes: vec![ObjectTransition {
                    key: key.clone(),
                    previous,
                    next: version.id,
                    delete_marker: false,
                }],
                changes_root: None,
                change_count: 0,
            },
            node_pack,
            authority: permit.stamp(),
            author: self.options.writer.clone(),
            message: Some("PutObject".to_string()),
            created_at_millis: now,
            metadata: BTreeMap::new(),
        };
        let published = self
            .publisher
            .store_and_publish(
                current,
                CommitPublication {
                    permit: &permit,
                    branch,
                    commit: &commit,
                    node_pack: prepared.as_ref().map(PreparedNodePack::pack),
                    operation,
                    message: "PutObject",
                    now_millis: now,
                },
            )
            .await?;
        self.finalize_pack(published.value.target, &commit, prepared)
            .await?;
        self.mark_local_index_head(branch, published.value.target)?;
        Ok(CommitReceipt {
            id: published.value.target,
            operation,
            branch: branch.to_string(),
            parents: commit.parents,
            changed_keys: 1,
            object_versions: vec![version.id],
            idempotent_replay: false,
        })
    }

    pub async fn get_object(&self, branch: &str, key: &[u8]) -> Result<Option<ObjectData>> {
        self.validate_key(key)?;
        self.locator.register(branch)?;
        let reference = self.publisher.load(branch).await?;
        self.require_branch_indexes_ready_for(branch, &reference)
            .await?;
        let commit = self.load_commit_metadata(reference.value.target).await?;
        let objects = self.tree_from_root(&commit.state.objects)?;
        let Some(encoded) = self
            .engine(self.node_store.clone())
            .get(&objects, key)
            .await?
        else {
            return Ok(None);
        };
        let current: CurrentObject = decode_canonical(&encoded)?;
        current.version.validate()?;
        let binding = current.version.binding.as_ref().ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "live object has no immutable payload binding",
            )
        })?;
        let bytes = self.payloads.get(binding).await?;
        Ok(Some(ObjectData {
            key: key.to_vec(),
            version: current.version,
            bytes,
            snapshot: reference.value.target,
        }))
    }

    pub async fn get_object_at(
        &self,
        branch: &str,
        snapshot: CommitId,
        key: &[u8],
    ) -> Result<Option<ObjectData>> {
        self.validate_key(key)?;
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        let commit = self.load_commit_metadata(snapshot).await?;
        let objects = self.tree_from_root(&commit.state.objects)?;
        let Some(encoded) = self
            .engine(self.node_store.clone())
            .get(&objects, key)
            .await?
        else {
            return Ok(None);
        };
        let current: CurrentObject = decode_canonical(&encoded)?;
        current.version.validate()?;
        let binding = current.version.binding.as_ref().ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "live historical object has no immutable payload binding",
            )
        })?;
        let bytes = self.payloads.get(binding).await?;
        Ok(Some(ObjectData {
            key: key.to_vec(),
            version: current.version,
            bytes,
            snapshot,
        }))
    }

    /// Read object metadata without fetching its immutable payload.
    pub async fn head_object(
        &self,
        branch: &str,
        key: &[u8],
    ) -> Result<Option<(CommitId, ObjectSummary)>> {
        let snapshot = self.head(branch).await?;
        Ok(self
            .head_object_at(branch, snapshot, key)
            .await?
            .map(|summary| (snapshot, summary)))
    }

    pub async fn head_object_at(
        &self,
        branch: &str,
        snapshot: CommitId,
        key: &[u8],
    ) -> Result<Option<ObjectSummary>> {
        self.validate_key(key)?;
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        let commit = self.load_commit_metadata(snapshot).await?;
        let objects = self.tree_from_root(&commit.state.objects)?;
        let Some(encoded) = self
            .engine(self.node_store.clone())
            .get(&objects, key)
            .await?
        else {
            return Ok(None);
        };
        let current: CurrentObject = decode_canonical(&encoded)?;
        current.version.validate()?;
        Ok(Some(ObjectSummary {
            key: key.to_vec(),
            version: current.version,
        }))
    }

    /// Fetch an inclusive byte range from the immutable payload bound to a
    /// logical snapshot. Range reads validate the binding and provider token;
    /// callers wanting a full content-hash check should use `get_object`.
    pub async fn get_object_range(
        &self,
        branch: &str,
        snapshot: CommitId,
        key: &[u8],
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<Option<ObjectRangeData>> {
        if range.start() > range.end() {
            return Err(Error::new(
                ErrorCode::InvalidRange,
                "range start exceeds end",
            ));
        }
        let Some(summary) = self.head_object_at(branch, snapshot, key).await? else {
            return Ok(None);
        };
        let LogicalObjectVersionKind::Live { size, .. } = summary.version.body.kind else {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "current object resolves to a delete marker",
            ));
        };
        if *range.start() >= size {
            return Err(Error::new(
                ErrorCode::InvalidRange,
                "range starts beyond the object payload",
            ));
        }
        let binding = summary.version.binding.as_ref().ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "live object has no payload binding",
            )
        })?;
        if binding.path != self.payloads.expected_path(binding)? {
            return Err(Error::new(
                ErrorCode::CorruptContent,
                "payload binding path does not match its checksum",
            ));
        }
        let physical_version =
            binding
                .provider_version_id
                .as_ref()
                .map(|version_id| PhysicalVersion::Versioned {
                    version_id: version_id.clone(),
                });
        let logical_range = *range.start()..=(*range.end()).min(size - 1);
        let translated = if let Some((offset, pack_end)) = binding.pack_range {
            let start = offset.checked_add(*logical_range.start()).ok_or_else(|| {
                Error::new(ErrorCode::InvalidRange, "packed payload range overflow")
            })?;
            let end = offset
                .checked_add(*logical_range.end())
                .filter(|end| *end <= pack_end)
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::CorruptContent,
                        "packed payload range exceeds its logical extent",
                    )
                })?;
            start..=end
        } else {
            logical_range.clone()
        };
        let stored = self
            .plane
            .get(GetRequest {
                path: binding.path.clone(),
                range: Some(translated),
                physical_version,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "payload is missing"))?;
        if stored.metadata.sha256 != binding.physical_checksum_sha256()
            || stored.metadata.token.etag != binding.provider_etag
            || stored.metadata.token.version_id != binding.provider_version_id
        {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "range response metadata does not match its logical binding",
            ));
        }
        Ok(Some(ObjectRangeData {
            key: key.to_vec(),
            version: summary.version,
            bytes: stored.bytes,
            snapshot,
            range: logical_range,
        }))
    }

    /// Copy an object by reusing its immutable payload binding. Only metadata
    /// nodes and one branch publication are written; the payload is not read
    /// or uploaded again.
    pub async fn copy_object(
        &self,
        branch: &str,
        source_snapshot: CommitId,
        source_key: &[u8],
        destination_key: Vec<u8>,
    ) -> Result<CommitReceipt> {
        self.validate_key(&destination_key)?;
        let source = self
            .head_object_at(branch, source_snapshot, source_key)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::InvalidKey, "copy source does not exist"))?;
        let LogicalObjectVersionKind::Live {
            size,
            logical_etag,
            headers,
            checksums,
            user_metadata,
            tags,
        } = source.version.body.kind
        else {
            return Err(Error::new(
                ErrorCode::InvalidRevision,
                "copy source is a delete marker",
            ));
        };
        let binding = source.version.binding.ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "copy source has no payload binding",
            )
        })?;
        let session = self
            .begin_commit_session(branch, "CopyObject", 60_000)
            .await?;
        let mutation = StagedMutation {
            body: StagedMutationBody::Put(Box::new(StagedPut {
                key: destination_key,
                size,
                logical_etag,
                checksums,
                headers,
                user_metadata,
                tags,
                binding,
            })),
        };
        self.publish_commit_session(session, vec![mutation]).await
    }

    /// Delete up to the repository's canonical multi-delete bound in one
    /// atomic commit.
    pub async fn delete_objects(&self, branch: &str, keys: Vec<Vec<u8>>) -> Result<CommitReceipt> {
        if keys.is_empty() || keys.len() > self.format.canonical_limits.max_delete_objects as usize
        {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "multi-delete requires between one key and the canonical delete limit",
            ));
        }
        for key in &keys {
            self.validate_key(key)?;
        }
        let session = self
            .begin_commit_session(branch, "DeleteObjects", 60_000)
            .await?;
        let mutations = keys.into_iter().map(StagedMutation::delete).collect();
        self.publish_commit_session(session, mutations).await
    }

    pub async fn delete_object(&self, branch: &str, key: Vec<u8>) -> Result<CommitReceipt> {
        let operation = self.options.ids.operation();
        self.delete_object_with_operation(branch, key, operation)
            .await
    }

    pub async fn delete_object_with_operation(
        &self,
        branch: &str,
        key: Vec<u8>,
        operation: OperationId,
    ) -> Result<CommitReceipt> {
        if !self.writable.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "repository is read-only",
            ));
        }
        self.validate_key(&key)?;
        let input_digest = crate::model::derive_input_digest(&[b"delete", branch.as_bytes(), &key]);
        let _lane = self.lock_branch(branch).await;
        let now = self.options.clock.now_millis()?;
        if let Some(receipt) = self
            .reconcile_operation(branch, operation, input_digest, now)
            .await?
        {
            return Ok(receipt);
        }
        let permit = self.active_permit(branch, now).await?;
        self.authority.validate_active(&permit, now).await?;
        self.require_branch_indexes_ready(branch).await?;
        let current = self.publisher.load(branch).await?;
        let base = self.load_commit_object(current.value.target).await?.commit;
        let write_store = self.node_store.isolated_write_session();
        let engine = self.engine(write_store.clone());
        let mut objects = self.tree_from_root(&base.state.objects)?;
        let mut versions = self.tree_from_root(&base.state.versions)?;
        let previous = engine
            .get(&objects, &key)
            .await?
            .map(|encoded| decode_canonical::<CurrentObject>(&encoded))
            .transpose()?
            .map(|current| current.version.id);
        let generation = CommitGeneration(base.generation.0.checked_add(1).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "commit generation overflow")
        })?);
        let version = ObjectVersion::derive(
            self.format.repository_id,
            &key,
            operation,
            LogicalObjectVersionBody {
                order: ObjectVersionOrder {
                    commit_generation: generation,
                    mutation_ordinal: 0,
                },
                created_at_millis: now,
                kind: LogicalObjectVersionKind::DeleteMarker,
            },
            None,
        )?;
        objects = engine.delete(&objects, &key).await?;
        versions = engine
            .put(
                &versions,
                version_tree_key(&key, version.body.order, version.id),
                encode_canonical(&version)?,
            )
            .await?;
        let prepared = write_store.prepare_node_pack(
            tree_format_digest(&self.format.state_tree_format)?,
            Vec::new(),
        )?;
        let commit = BucketCommit {
            state: BucketState {
                objects: RootManifest::from_tree(&objects)?,
                versions: RootManifest::from_tree(&versions)?,
            },
            parents: vec![current.value.target],
            generation,
            delta: BucketDelta {
                input_digest,
                changes: vec![ObjectTransition {
                    key: key.clone(),
                    previous,
                    next: version.id,
                    delete_marker: true,
                }],
                changes_root: None,
                change_count: 0,
            },
            node_pack: prepared.as_ref().map(PreparedNodePack::reference),
            authority: permit.stamp(),
            author: self.options.writer.clone(),
            message: Some("DeleteObject".to_string()),
            created_at_millis: now,
            metadata: BTreeMap::new(),
        };
        let published = self
            .publisher
            .store_and_publish(
                current,
                CommitPublication {
                    permit: &permit,
                    branch,
                    commit: &commit,
                    node_pack: prepared.as_ref().map(PreparedNodePack::pack),
                    operation,
                    message: "DeleteObject",
                    now_millis: now,
                },
            )
            .await?;
        self.finalize_pack(published.value.target, &commit, prepared)
            .await?;
        self.mark_local_index_head(branch, published.value.target)?;
        Ok(CommitReceipt {
            id: published.value.target,
            operation,
            branch: branch.to_string(),
            parents: commit.parents,
            changed_keys: 1,
            object_versions: vec![version.id],
            idempotent_replay: false,
        })
    }

    pub async fn list_objects(
        &self,
        branch: &str,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(CommitId, Vec<ObjectSummary>, bool)> {
        std::str::from_utf8(prefix)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "list prefix is not UTF-8"))?;
        let snapshot = self.head(branch).await?;
        let (objects, truncated) = self
            .list_objects_at(branch, snapshot, prefix, after, limit)
            .await?;
        Ok((snapshot, objects, truncated))
    }

    /// List a stable snapshot using an opaque traversal cursor. Resumption
    /// seeks directly to the saved key in O(log n), rather than replaying and
    /// discarding every earlier prefix entry. Cursors remain valid while their
    /// immutable snapshot is retained; callers spanning GC should hold a
    /// retention pin for `snapshot`.
    pub async fn list_objects_page(
        &self,
        branch: &str,
        prefix: &[u8],
        continuation: Option<&str>,
        requested_limit: usize,
    ) -> Result<ListObjectsPage> {
        self.list_objects_page_from_snapshot(branch, None, prefix, continuation, requested_limit)
            .await
    }

    /// List an explicit immutable snapshot using the same opaque traversal
    /// cursor as [`Self::list_objects_page`]. The first page is seeded from
    /// `snapshot`; every continuation is validated against that commit.
    pub async fn list_objects_page_at(
        &self,
        branch: &str,
        snapshot: CommitId,
        prefix: &[u8],
        continuation: Option<&str>,
        requested_limit: usize,
    ) -> Result<ListObjectsPage> {
        self.list_objects_page_from_snapshot(
            branch,
            Some(snapshot),
            prefix,
            continuation,
            requested_limit,
        )
        .await
    }

    async fn list_objects_page_from_snapshot(
        &self,
        branch: &str,
        requested_snapshot: Option<CommitId>,
        prefix: &[u8],
        continuation: Option<&str>,
        requested_limit: usize,
    ) -> Result<ListObjectsPage> {
        std::str::from_utf8(prefix)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "list prefix is not UTF-8"))?;
        let limit = requested_limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "list cursor page size is zero",
            ));
        }
        let cursor = match continuation {
            Some(token) => {
                let bytes = hex::decode(token).map_err(|_| {
                    Error::new(
                        ErrorCode::InvalidContinuationToken,
                        "list continuation is not canonical hex",
                    )
                })?;
                let cursor: ListObjectsCursor = decode_canonical(&bytes).map_err(|_| {
                    Error::new(
                        ErrorCode::InvalidContinuationToken,
                        "list continuation is malformed",
                    )
                })?;
                if cursor.repository != self.format.repository_id
                    || cursor.branch != branch
                    || cursor.prefix != prefix
                    || requested_snapshot.is_some_and(|snapshot| cursor.snapshot != snapshot)
                {
                    return Err(Error::new(
                        ErrorCode::InvalidContinuationToken,
                        "list continuation belongs to another repository, branch, snapshot, or prefix",
                    ));
                }
                cursor
            }
            None => ListObjectsCursor {
                repository: self.format.repository_id,
                branch: branch.to_string(),
                snapshot: match requested_snapshot {
                    Some(snapshot) => snapshot,
                    None => self.head(branch).await?,
                },
                prefix: prefix.to_vec(),
                traversal: prolly::RangeCursor::start(),
            },
        };
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        let commit = self.load_commit_metadata(cursor.snapshot).await?;
        let objects = self.tree_from_root(&commit.state.objects)?;
        let engine = self.engine(self.node_store.clone());
        let mut page = engine
            .prefix_page(&objects, prefix, &cursor.traversal, limit.saturating_add(1))
            .await?;
        let truncated = page.entries.len() > limit;
        page.entries.truncate(limit);
        let next = if truncated {
            page.entries.last().map(|(key, _)| ListObjectsCursor {
                traversal: prolly::RangeCursor::after_key(key.clone()),
                ..cursor.clone()
            })
        } else {
            None
        };
        let objects = page
            .entries
            .into_iter()
            .map(|(key, encoded)| {
                let current: CurrentObject = decode_canonical(&encoded)?;
                current.version.validate()?;
                Ok(ObjectSummary {
                    key,
                    version: current.version,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ListObjectsPage {
            snapshot: cursor.snapshot,
            objects,
            continuation: next
                .map(|cursor| encode_canonical(&cursor).map(hex::encode))
                .transpose()?,
        })
    }

    /// S3-style delimiter projection over a stable ordered object page.
    pub async fn list_objects_delimited(
        &self,
        branch: &str,
        prefix: &[u8],
        delimiter: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<DelimitedObjectPage> {
        let snapshot = self.head(branch).await?;
        self.list_objects_delimited_at(branch, snapshot, prefix, delimiter, after, limit)
            .await
    }

    /// S3-style delimiter projection over an explicit immutable snapshot.
    pub async fn list_objects_delimited_at(
        &self,
        branch: &str,
        snapshot: CommitId,
        prefix: &[u8],
        delimiter: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<DelimitedObjectPage> {
        if delimiter.is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "list delimiter must not be empty",
            ));
        }
        let (candidates, truncated) = self
            .list_objects_at(branch, snapshot, prefix, after, limit)
            .await?;
        let mut objects = Vec::new();
        let mut common_prefixes = BTreeSet::new();
        for object in candidates {
            let suffix = object.key.get(prefix.len()..).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "prefix listing returned a key outside its prefix",
                )
            })?;
            if let Some(offset) = find_subslice(suffix, delimiter) {
                let end = prefix
                    .len()
                    .checked_add(offset)
                    .and_then(|end| end.checked_add(delimiter.len()))
                    .ok_or_else(|| {
                        Error::new(ErrorCode::EntityTooLarge, "common prefix length overflow")
                    })?;
                common_prefixes.insert(object.key[..end].to_vec());
            } else {
                objects.push(object);
            }
        }
        Ok(DelimitedObjectPage {
            snapshot,
            objects,
            common_prefixes: common_prefixes.into_iter().collect(),
            truncated,
        })
    }

    pub async fn list_objects_at(
        &self,
        branch: &str,
        snapshot: CommitId,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<ObjectSummary>, bool)> {
        std::str::from_utf8(prefix)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "list prefix is not UTF-8"))?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        let commit = self.load_commit_metadata(snapshot).await?;
        let objects = self.tree_from_root(&commit.state.objects)?;
        let engine = self.engine(self.node_store.clone());
        let mut iter = engine.prefix(&objects, prefix).await?;
        let mut result = Vec::with_capacity(limit);
        while result.len() <= limit {
            let Some(entry) = iter.next().await else {
                break;
            };
            let (key, encoded) = entry?;
            if after.is_some_and(|after| key.as_slice() <= after) {
                continue;
            }
            let current: CurrentObject = decode_canonical(&encoded)?;
            current.version.validate()?;
            result.push(ObjectSummary {
                key,
                version: current.version,
            });
        }
        let truncated = result.len() > limit;
        result.truncate(limit);
        Ok((result, truncated))
    }

    pub async fn list_object_versions(
        &self,
        branch: &str,
        key: &[u8],
        limit: usize,
    ) -> Result<(CommitId, Vec<ObjectVersion>)> {
        let snapshot = self.head(branch).await?;
        let versions = self
            .list_object_versions_at(branch, snapshot, key, limit)
            .await?;
        Ok((snapshot, versions))
    }

    pub async fn list_object_versions_at(
        &self,
        branch: &str,
        snapshot: CommitId,
        key: &[u8],
        limit: usize,
    ) -> Result<Vec<ObjectVersion>> {
        self.validate_key(key)?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        let commit = self.load_commit_object(snapshot).await?.commit;
        let versions = self.tree_from_root(&commit.state.versions)?;
        let prefix = version_tree_prefix(key);
        let engine = self.engine(self.node_store.clone());
        let mut iter = engine.prefix(&versions, &prefix).await?;
        let mut result = Vec::with_capacity(limit);
        while result.len() < limit {
            let Some(entry) = iter.next().await else {
                break;
            };
            let (_, encoded) = entry?;
            let version: ObjectVersion = decode_canonical(&encoded)?;
            version.validate()?;
            result.push(version);
        }
        Ok(result)
    }

    pub async fn list_versions_prefix(
        &self,
        branch: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<(CommitId, Vec<VersionSummary>)> {
        std::str::from_utf8(prefix)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "version prefix is not UTF-8"))?;
        let snapshot = self.head(branch).await?;
        let (versions, _) = self
            .list_versions_at(branch, snapshot, prefix, None, limit)
            .await?;
        Ok((snapshot, versions))
    }

    pub async fn list_versions_at(
        &self,
        branch: &str,
        snapshot: CommitId,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<VersionSummary>, bool)> {
        std::str::from_utf8(prefix)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "version prefix is not UTF-8"))?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        let commit = self.load_commit_object(snapshot).await?.commit;
        let versions = self.tree_from_root(&commit.state.versions)?;
        let encoded_prefix = version_tree_partial_prefix(prefix);
        let engine = self.engine(self.node_store.clone());
        let mut iter = engine.prefix(&versions, &encoded_prefix).await?;
        let mut result = Vec::with_capacity(limit);
        while result.len() <= limit {
            let Some(entry) = iter.next().await else {
                break;
            };
            let (encoded_key, encoded) = entry?;
            if after.is_some_and(|after| encoded_key.as_slice() <= after) {
                continue;
            }
            let key = decode_version_tree_logical_key(&encoded_key)?;
            let version: ObjectVersion = decode_canonical(&encoded)?;
            version.validate()?;
            result.push(VersionSummary {
                key,
                version,
                cursor: encoded_key,
            });
        }
        let truncated = result.len() > limit;
        result.truncate(limit);
        Ok((result, truncated))
    }

    /// Load and verify one immutable commit object.
    pub async fn commit(&self, id: CommitId) -> Result<BucketCommit> {
        Ok(self.load_commit_object(id).await?.commit)
    }

    /// Return the first-parent history of a branch, newest first.
    pub async fn log(&self, branch: &str, limit: usize) -> Result<Vec<(CommitId, BucketCommit)>> {
        let head = self.head(branch).await?;
        Ok(self
            .log_page_bounded(branch, head, None, limit, TraversalBudget::default())
            .await?
            .commits)
    }

    /// Traverse first-parent history with a constant-size continuation and
    /// explicit work, byte, and wall-clock budgets.
    pub async fn log_page_bounded(
        &self,
        branch: &str,
        start: CommitId,
        cursor: Option<&HistoryCursor>,
        requested_limit: usize,
        budget: TraversalBudget,
    ) -> Result<CommitPage> {
        validate_branch(branch)?;
        if budget.max_commits == 0 || budget.max_decoded_bytes == 0 || budget.max_elapsed.is_zero()
        {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "history traversal budgets must be greater than zero",
            ));
        }
        if cursor.is_some_and(|cursor| {
            cursor.repository != self.format.repository_id
                || cursor.branch != branch
                || cursor.root != start
        }) {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "history cursor belongs to another repository, branch, or root",
            ));
        }
        let limit = requested_limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Ok(CommitPage {
                commits: Vec::new(),
                continuation: cursor.cloned(),
                visited_commits: 0,
                decoded_bytes: 0,
                budget_exhausted: false,
            });
        }
        self.locator.register(branch)?;
        let started = std::time::Instant::now();
        let mut current = cursor.map_or(start, |cursor| cursor.next);
        let mut commits = Vec::with_capacity(limit.min(64));
        let mut visited_commits = 0usize;
        let mut decoded_bytes = 0u64;
        let mut budget_exhausted = false;
        let continuation = loop {
            if commits.len() >= limit {
                break Some(HistoryCursor {
                    repository: self.format.repository_id,
                    branch: branch.to_string(),
                    root: start,
                    next: current,
                });
            }
            if visited_commits >= budget.max_commits || started.elapsed() >= budget.max_elapsed {
                budget_exhausted = true;
                break Some(HistoryCursor {
                    repository: self.format.repository_id,
                    branch: branch.to_string(),
                    root: start,
                    next: current,
                });
            }
            let commit = self.load_commit_object(current).await?.commit;
            let encoded_len = u64::try_from(encode_canonical(&commit)?.len()).map_err(|_| {
                Error::new(
                    ErrorCode::EntityTooLarge,
                    "encoded commit length exceeds u64",
                )
            })?;
            if decoded_bytes.saturating_add(encoded_len) > budget.max_decoded_bytes {
                if commits.is_empty() {
                    return Err(Error::new(
                        ErrorCode::InvalidLimit,
                        "history byte budget cannot hold one commit",
                    ));
                }
                budget_exhausted = true;
                break Some(HistoryCursor {
                    repository: self.format.repository_id,
                    branch: branch.to_string(),
                    root: start,
                    next: current,
                });
            }
            let parent = commit.parents.first().copied();
            decoded_bytes = decoded_bytes.checked_add(encoded_len).ok_or_else(|| {
                Error::new(ErrorCode::EntityTooLarge, "history byte counter overflow")
            })?;
            visited_commits += 1;
            commits.push((current, commit));
            let Some(parent) = parent else { break None };
            current = parent;
        };
        Ok(CommitPage {
            commits,
            continuation,
            visited_commits,
            decoded_bytes,
            budget_exhausted,
        })
    }

    /// Start a durable traversal over one or more commit roots.
    pub async fn start_commit_closure(&self, roots: &[CommitId]) -> Result<CommitClosureCursor> {
        if roots.is_empty() || roots.len() > 1_000 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "commit-closure start requires between 1 and 1,000 roots",
            ));
        }
        let traversal = self.options.ids.operation();
        let engine = self.commit_closure_engine(traversal)?;
        let mut cursor = CommitClosureCursor {
            repository: self.format.repository_id,
            traversal,
            state: RootManifest::from_tree(&engine.create())?,
            next_stack_sequence: u64::MAX,
        };
        self.extend_commit_closure(&mut cursor, roots).await?;
        Ok(cursor)
    }

    /// Attach a bounded page of additional roots to a durable traversal.
    pub async fn extend_commit_closure(
        &self,
        cursor: &mut CommitClosureCursor,
        roots: &[CommitId],
    ) -> Result<()> {
        self.validate_commit_closure_cursor(cursor)?;
        if roots.is_empty() || roots.len() > 1_000 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "commit-closure extension requires between 1 and 1,000 roots",
            ));
        }
        let engine = self.commit_closure_engine(cursor.traversal)?;
        let mut tree = self.tree_from_root(&cursor.state)?;
        let mut unique = roots.to_vec();
        unique.sort_unstable();
        unique.dedup();
        let mut mutations = Vec::with_capacity(unique.len());
        for commit in unique.into_iter().rev() {
            if engine
                .get(&tree, &commit_closure_seen_key(commit))
                .await?
                .is_some()
            {
                continue;
            }
            mutations.push(Mutation::Upsert {
                key: commit_closure_stack_key(cursor.next_stack_sequence),
                val: encode_canonical(&CommitClosureWork {
                    commit,
                    finish: false,
                })?,
            });
            cursor.next_stack_sequence =
                cursor.next_stack_sequence.checked_sub(1).ok_or_else(|| {
                    Error::new(
                        ErrorCode::HistoryLimitExceeded,
                        "commit-closure stack sequence is exhausted",
                    )
                })?;
        }
        if !mutations.is_empty() {
            tree = engine.batch(&tree, mutations).await?;
            cursor.state = RootManifest::from_tree(&tree)?;
        }
        Ok(())
    }

    /// Advance a durable DAG traversal under explicit work and output bounds.
    /// Commits are emitted parent-before-child and exactly once.
    pub async fn commit_closure_page(
        &self,
        cursor: &CommitClosureCursor,
        max_steps: usize,
        max_commits: usize,
    ) -> Result<CommitClosurePage> {
        self.validate_commit_closure_cursor(cursor)?;
        if !(1..=100_000).contains(&max_steps) || !(1..=1_000).contains(&max_commits) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "commit-closure page requires 1..=100,000 steps and 1..=1,000 commits",
            ));
        }
        let engine = self.commit_closure_engine(cursor.traversal)?;
        let mut tree = self.tree_from_root(&cursor.state)?;
        let mut next_cursor = cursor.clone();
        let mut commits = Vec::with_capacity(max_commits.min(64));
        let mut steps = 0usize;
        while steps < max_steps && commits.len() < max_commits {
            let mut queue = engine.prefix(&tree, COMMIT_CLOSURE_QUEUE_PREFIX).await?;
            let Some(entry) = queue.next().await else {
                break;
            };
            let (stack_key, encoded) = entry?;
            drop(queue);
            let work: CommitClosureWork = decode_canonical(&encoded)?;
            let seen_key = commit_closure_seen_key(work.commit);
            let state = engine.get(&tree, &seen_key).await?;
            let mut mutations = vec![Mutation::Delete { key: stack_key }];
            if work.finish {
                match state.as_deref() {
                    Some([1]) => {}
                    Some([0]) => {
                        let commit = self.load_commit_object(work.commit).await?.commit;
                        mutations.push(Mutation::Upsert {
                            key: seen_key,
                            val: vec![1],
                        });
                        commits.push((work.commit, commit));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorCode::CorruptCommit,
                            "commit-closure finish record has invalid state",
                        ));
                    }
                }
            } else {
                match state.as_deref() {
                    Some([1]) => {}
                    Some([0]) => {
                        return Err(Error::new(
                            ErrorCode::CorruptCommit,
                            "commit graph contains a cycle",
                        ));
                    }
                    None => {
                        let commit = self.load_commit_object(work.commit).await?.commit;
                        mutations.push(Mutation::Upsert {
                            key: seen_key,
                            val: vec![0],
                        });
                        push_commit_closure_work(
                            &mut next_cursor,
                            &mut mutations,
                            work.commit,
                            true,
                        )?;
                        for parent in commit.parents.iter().rev() {
                            push_commit_closure_work(
                                &mut next_cursor,
                                &mut mutations,
                                *parent,
                                false,
                            )?;
                        }
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorCode::CorruptCommit,
                            "commit-closure visited state is malformed",
                        ));
                    }
                }
            }
            tree = engine.batch(&tree, mutations).await?;
            steps += 1;
        }
        next_cursor.state = RootManifest::from_tree(&tree)?;
        let mut remaining = engine.prefix(&tree, COMMIT_CLOSURE_QUEUE_PREFIX).await?;
        let complete = remaining.next().await.is_none();
        Ok(CommitClosurePage {
            commits,
            cursor: next_cursor,
            steps,
            complete,
            budget_exhausted: !complete && steps == max_steps,
        })
    }

    /// Start a bounded concurrent collector for immutable repository data.
    /// `grace_millis` must exceed the longest allowed unpublished operation.
    pub async fn start_gc(&self, grace_millis: u64) -> Result<GcCursor> {
        if grace_millis == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "GC grace period must be greater than zero",
            ));
        }
        if !self.writable.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "GC requires a writable repository handle",
            ));
        }
        let epoch = self.options.ids.operation();
        let now = self.options.clock.now_millis()?;
        let cutoff_millis = now.checked_sub(grace_millis).ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidLimit,
                "GC grace period exceeds the current repository clock",
            )
        })?;
        {
            let mut active = self
                .active_gc_epoch
                .write()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "active GC lock poisoned"))?;
            if active.is_some() {
                return Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "another GC epoch is active",
                ));
            }
            *active = Some(epoch);
        }
        let result = self.publish_gc_coordinator(Some(epoch), now).await;
        if let Err(error) = result {
            *self.active_gc_epoch.write().map_err(|_| {
                Error::new(ErrorCode::InternalInvariant, "active GC lock poisoned")
            })? = None;
            return Err(error);
        }
        let work = self.gc_work_engine(epoch)?.create();
        Ok(GcCursor {
            repository: self.format.repository_id,
            epoch,
            cutoff_millis,
            phase: GcPhase::DiscoverBranches,
            continuation: None,
            work: RootManifest::from_tree(&work)?,
            dirty_sequence: self.gc_dirty_sequence.load(Ordering::Acquire),
            dirty_target_sequence: 0,
            initial_scan_complete: false,
            publication_barrier_drained: false,
            sweep_after: None,
            report: crate::GcReport::default(),
        })
    }

    /// Advance root discovery, reachability marking, candidate discovery, or
    /// dirty-root catch-up by a bounded number of physical/tree records.
    pub async fn advance_gc(&self, cursor: &GcCursor, max_steps: usize) -> Result<GcPage> {
        self.validate_gc_cursor(cursor)?;
        if !(1..=1_000).contains(&max_steps) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "GC page size must be between 1 and 1,000",
            ));
        }
        if matches!(cursor.phase, GcPhase::Ready | GcPhase::Sweeping) {
            return Ok(GcPage {
                cursor: cursor.clone(),
                processed: 0,
                complete: false,
                restarted_for_new_roots: false,
            });
        }
        if cursor.phase == GcPhase::Complete {
            return Ok(GcPage {
                cursor: cursor.clone(),
                processed: 0,
                complete: true,
                restarted_for_new_roots: false,
            });
        }
        let mut next = cursor.clone();
        if !next.publication_barrier_drained {
            let (processed, continuation, drained) = self
                .gc_drain_publication_tickets(next.continuation.clone(), max_steps)
                .await?;
            next.continuation = continuation;
            next.publication_barrier_drained = drained;
            return Ok(GcPage {
                complete: false,
                cursor: next,
                processed,
                restarted_for_new_roots: false,
            });
        }
        let processed = match next.phase {
            GcPhase::DiscoverBranches => self.gc_discover_refs(&mut next, false, max_steps).await?,
            GcPhase::DiscoverTags => self.gc_discover_refs(&mut next, true, max_steps).await?,
            GcPhase::MarkCommits => self.gc_mark_commits(&mut next, max_steps).await?,
            GcPhase::MarkNodes => self.gc_mark_nodes(&mut next, max_steps).await?,
            GcPhase::ScanCandidates => self.gc_scan_candidates(&mut next, max_steps).await?,
            GcPhase::CatchUpDirtyRoots => {
                self.gc_catch_up_dirty_roots(&mut next, max_steps).await?
            }
            GcPhase::Cleanup => self.gc_cleanup(&mut next, max_steps).await?,
            GcPhase::Ready | GcPhase::Sweeping | GcPhase::Complete => 0,
        };
        Ok(GcPage {
            complete: next.phase == GcPhase::Complete,
            cursor: next,
            processed,
            restarted_for_new_roots: false,
        })
    }

    /// Delete at most `max_candidates` exact immutable physical versions.
    /// A publication that appeared after marking schedules catch-up first.
    pub async fn sweep_gc(&self, cursor: &GcCursor, max_candidates: usize) -> Result<GcPage> {
        self.validate_gc_cursor(cursor)?;
        if !(1..=1_000).contains(&max_candidates) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "GC sweep batch must contain between 1 and 1,000 candidates",
            ));
        }
        if !matches!(cursor.phase, GcPhase::Ready | GcPhase::Sweeping) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "GC must finish marking before sweeping",
            ));
        }
        let _barrier = self.gc_publication_barrier.write().await;
        let dirty_target = self.gc_dirty_sequence.load(Ordering::Acquire);
        if dirty_target > cursor.dirty_sequence {
            let mut next = cursor.clone();
            next.phase = GcPhase::CatchUpDirtyRoots;
            next.dirty_target_sequence = dirty_target;
            next.continuation = None;
            return Ok(GcPage {
                cursor: next,
                processed: 0,
                complete: false,
                restarted_for_new_roots: true,
            });
        }
        let engine = self.gc_work_engine(cursor.epoch)?;
        let tree = self.tree_from_root(&cursor.work)?;
        let start = cursor.sweep_after.as_deref().unwrap_or(b"d/");
        let mut entries = engine.range(&tree, start, None).await?;
        let mut next = cursor.clone();
        next.phase = GcPhase::Sweeping;
        let mut processed = 0usize;
        let mut exhausted = true;
        while processed < max_candidates {
            let Some(entry) = entries.next().await else {
                break;
            };
            let (key, encoded) = entry?;
            if !key.starts_with(b"d/") {
                break;
            }
            if next
                .sweep_after
                .as_ref()
                .is_some_and(|after| key.as_slice() <= after.as_slice())
            {
                continue;
            }
            exhausted = false;
            let candidate: GcCandidate = decode_canonical(&encoded)?;
            let retained = engine
                .get(&tree, &gc_path_mark_key(&candidate.path))
                .await?
                .is_some()
                || match &candidate.physical_version {
                    PhysicalVersion::Versioned { version_id } => engine
                        .get(&tree, &gc_physical_mark_key(&candidate.path, version_id))
                        .await?
                        .is_some(),
                    PhysicalVersion::Unversioned { .. } => false,
                };
            if retained {
                next.report.skipped_reachable = next.report.skipped_reachable.saturating_add(1);
            } else {
                match self
                    .plane
                    .delete_exact(&candidate.path, candidate.physical_version)
                    .await?
                {
                    DeleteOutcome::Deleted => {
                        next.report.deleted_versions =
                            next.report.deleted_versions.saturating_add(1);
                        next.report.deleted_bytes =
                            next.report.deleted_bytes.saturating_add(candidate.len);
                        *next
                            .report
                            .deleted_by_kind
                            .entry(candidate.kind)
                            .or_default() += 1;
                    }
                    DeleteOutcome::NotFound => {
                        next.report.already_missing = next.report.already_missing.saturating_add(1);
                    }
                    DeleteOutcome::TokenMismatch => {
                        return Err(Error::new(
                            ErrorCode::PreconditionFailed,
                            "GC candidate changed before exact deletion",
                        ));
                    }
                }
            }
            next.sweep_after = Some(key);
            processed += 1;
        }
        if processed < max_candidates && !exhausted {
            let mut probe = engine
                .range(&tree, next.sweep_after.as_deref().unwrap(), None)
                .await?;
            exhausted = loop {
                let Some(entry) = probe.next().await else {
                    break true;
                };
                let (key, _) = entry?;
                if key.starts_with(b"d/") && Some(&key) != next.sweep_after.as_ref() {
                    break false;
                }
                if !key.starts_with(b"d/") {
                    break true;
                }
            };
        }
        if exhausted {
            next.phase = GcPhase::Cleanup;
            next.continuation = None;
        }
        Ok(GcPage {
            complete: false,
            cursor: next,
            processed,
            restarted_for_new_roots: false,
        })
    }

    /// Start a restartable integrity check for one stable branch snapshot.
    /// Deep mode additionally streams every reachable payload and verifies its
    /// content checksum; metadata mode verifies immutable paths and provider
    /// metadata without downloading object bodies.
    pub async fn start_fsck(&self, branch: &str, deep: bool) -> Result<FsckCursor> {
        validate_branch(branch)?;
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        let snapshot = self.head(branch).await?;
        Ok(FsckCursor {
            repository: self.format.repository_id,
            branch: branch.to_string(),
            snapshot,
            closure: self.start_commit_closure(&[snapshot]).await?,
            phase: FsckPhase::DiscoverCommits,
            after: None,
            deep,
            report: FsckReport::default(),
        })
    }

    /// Advance an integrity check by at most `max_steps` commit-work records,
    /// current objects, or logical versions. Persist the returned cursor after
    /// every page.
    pub async fn advance_fsck(&self, cursor: &FsckCursor, max_steps: usize) -> Result<FsckPage> {
        if !(1..=1_000).contains(&max_steps) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "fsck page size must be between 1 and 1,000",
            ));
        }
        if cursor.repository != self.format.repository_id {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "fsck cursor belongs to another repository",
            ));
        }
        validate_branch(&cursor.branch)?;
        self.validate_commit_closure_cursor(&cursor.closure)?;
        self.locator.register(&cursor.branch)?;
        let mut next = cursor.clone();
        let mut processed = 0usize;
        match next.phase {
            FsckPhase::DiscoverCommits => {
                let page = self
                    .commit_closure_page(&next.closure, max_steps, max_steps.min(1_000))
                    .await?;
                processed = page.steps;
                for (_, commit) in &page.commits {
                    next.report.commits = next.report.commits.checked_add(1).ok_or_else(|| {
                        Error::new(ErrorCode::EntityTooLarge, "fsck commit counter overflow")
                    })?;
                    let nodes = commit
                        .node_pack
                        .as_ref()
                        .map_or(0_u64, |pack| u64::from(pack.node_count));
                    next.report.reachable_nodes = next
                        .report
                        .reachable_nodes
                        .checked_add(nodes)
                        .ok_or_else(|| {
                            Error::new(ErrorCode::EntityTooLarge, "fsck node counter overflow")
                        })?;
                }
                next.closure = page.cursor;
                if page.complete {
                    next.phase = FsckPhase::VerifyObjects;
                }
            }
            FsckPhase::VerifyObjects => {
                let commit = self.load_commit_object(next.snapshot).await?.commit;
                let tree = self.tree_from_root(&commit.state.objects)?;
                let engine = self.engine(self.node_store.clone());
                let mut entries = match next.after.as_deref() {
                    Some(after) => engine.range_after(&tree, after, None).await?,
                    None => engine.prefix(&tree, b"").await?,
                };
                while processed < max_steps {
                    let Some(entry) = entries.next().await else {
                        next.phase = FsckPhase::VerifyVersions;
                        next.after = None;
                        break;
                    };
                    let (key, encoded) = entry?;
                    let current: CurrentObject = decode_canonical(&encoded)?;
                    current.version.validate()?;
                    let (payloads, bytes, deep_bytes) = self
                        .verify_payload_metadata(&current.version, next.deep)
                        .await?;
                    next.report.current_objects =
                        checked_fsck_add(next.report.current_objects, 1, "current-object")?;
                    next.report.payloads_verified =
                        checked_fsck_add(next.report.payloads_verified, payloads, "payload")?;
                    next.report.payload_bytes_verified = checked_fsck_add(
                        next.report.payload_bytes_verified,
                        bytes,
                        "payload-byte",
                    )?;
                    next.report.deep_content_bytes_verified = checked_fsck_add(
                        next.report.deep_content_bytes_verified,
                        deep_bytes,
                        "deep-content-byte",
                    )?;
                    record_fsck_packed_payload(&mut next.report, &current.version)?;
                    next.after = Some(key);
                    processed += 1;
                }
            }
            FsckPhase::VerifyVersions => {
                let commit = self.load_commit_object(next.snapshot).await?.commit;
                let tree = self.tree_from_root(&commit.state.versions)?;
                let engine = self.engine(self.node_store.clone());
                let mut entries = match next.after.as_deref() {
                    Some(after) => engine.range_after(&tree, after, None).await?,
                    None => engine.prefix(&tree, b"").await?,
                };
                while processed < max_steps {
                    let Some(entry) = entries.next().await else {
                        next.phase = FsckPhase::Complete;
                        next.after = None;
                        break;
                    };
                    let (key, encoded) = entry?;
                    let version: ObjectVersion = decode_canonical(&encoded)?;
                    version.validate()?;
                    let (payloads, bytes, deep_bytes) =
                        self.verify_payload_metadata(&version, next.deep).await?;
                    next.report.logical_versions =
                        checked_fsck_add(next.report.logical_versions, 1, "logical-version")?;
                    next.report.payloads_verified =
                        checked_fsck_add(next.report.payloads_verified, payloads, "payload")?;
                    next.report.payload_bytes_verified = checked_fsck_add(
                        next.report.payload_bytes_verified,
                        bytes,
                        "payload-byte",
                    )?;
                    next.report.deep_content_bytes_verified = checked_fsck_add(
                        next.report.deep_content_bytes_verified,
                        deep_bytes,
                        "deep-content-byte",
                    )?;
                    record_fsck_packed_payload(&mut next.report, &version)?;
                    next.after = Some(key);
                    processed += 1;
                }
            }
            FsckPhase::Complete => {}
        }
        Ok(FsckPage {
            complete: next.phase == FsckPhase::Complete,
            cursor: next,
            processed,
        })
    }

    /// Start a restartable inventory of the current snapshot's direct payloads
    /// and packed extents. The report deduplicates physical objects and logical
    /// extents, making pack utilization meaningful even when many keys share
    /// content.
    pub async fn start_payload_pack_stats(&self, branch: &str) -> Result<PayloadPackStatsCursor> {
        validate_branch(branch)?;
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        let snapshot = self.head(branch).await?;
        let job = self.options.ids.operation();
        let seen = self.payload_pack_stats_engine(job)?.create();
        Ok(PayloadPackStatsCursor {
            repository: self.format.repository_id,
            branch: branch.to_string(),
            snapshot,
            job,
            after: None,
            seen: RootManifest::from_tree(&seen)?,
            report: PayloadPackStats::default(),
            complete: false,
        })
    }

    /// Inspect at most `max_objects` current logical objects and persist the
    /// unique-physical/extent set in the returned cursor.
    pub async fn advance_payload_pack_stats(
        &self,
        cursor: &PayloadPackStatsCursor,
        max_objects: usize,
    ) -> Result<PayloadPackStatsPage> {
        if !(1..=1_000).contains(&max_objects) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "payload-pack stats page size must be between 1 and 1,000",
            ));
        }
        if cursor.repository != self.format.repository_id
            || cursor.job.is_nil()
            || cursor.seen.format_digest != tree_format_digest(&self.format.state_tree_format)?
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "payload-pack stats cursor is malformed",
            ));
        }
        validate_branch(&cursor.branch)?;
        if cursor.complete {
            return Ok(PayloadPackStatsPage {
                cursor: cursor.clone(),
                processed: 0,
                complete: true,
            });
        }

        let commit = self.load_commit_object(cursor.snapshot).await?.commit;
        let objects = self.tree_from_root(&commit.state.objects)?;
        let object_engine = self.engine(self.node_store.clone());
        let mut entries = match cursor.after.as_deref() {
            Some(after) => object_engine.range_after(&objects, after, None).await?,
            None => object_engine.prefix(&objects, b"").await?,
        };
        let stats_engine = self.payload_pack_stats_engine(cursor.job)?;
        let mut seen = self.tree_from_root(&cursor.seen)?;
        let mut mutations = Vec::new();
        let mut pending_physical = BTreeSet::new();
        let mut pending_extents = BTreeSet::new();
        let mut next = cursor.clone();
        let mut processed = 0usize;
        while processed < max_objects {
            let Some(entry) = entries.next().await else {
                next.complete = true;
                next.after = None;
                break;
            };
            let (key, encoded) = entry?;
            let current: CurrentObject = decode_canonical(&encoded)?;
            current.version.validate()?;
            let LogicalObjectVersionKind::Live { size, .. } = &current.version.body.kind else {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "current object tree contains a delete marker",
                ));
            };
            let binding = current.version.binding.as_ref().ok_or_else(|| {
                Error::new(
                    ErrorCode::CorruptCommit,
                    "current live object has no payload binding",
                )
            })?;
            next.report.current_objects =
                checked_pack_add(next.report.current_objects, 1, "current object")?;
            next.report.logical_bytes =
                checked_pack_add(next.report.logical_bytes, *size, "logical byte")?;
            if binding.is_packed() {
                next.report.packed_objects =
                    checked_pack_add(next.report.packed_objects, 1, "packed object")?;
                next.report.packed_logical_bytes = checked_pack_add(
                    next.report.packed_logical_bytes,
                    *size,
                    "packed logical byte",
                )?;
            } else {
                next.report.direct_objects =
                    checked_pack_add(next.report.direct_objects, 1, "direct object")?;
            }

            let physical_key = payload_pack_physical_key(binding);
            if pending_physical.insert(physical_key.clone())
                && stats_engine.get(&seen, &physical_key).await?.is_none()
            {
                let metadata = self.plane.head(&binding.path).await?.ok_or_else(|| {
                    Error::new(ErrorCode::MissingClosure, "payload-pack object is missing")
                })?;
                if metadata.sha256 != binding.physical_checksum_sha256()
                    || metadata.token.etag != binding.provider_etag
                    || metadata.token.version_id != binding.provider_version_id
                {
                    return Err(Error::new(
                        ErrorCode::ChecksumMismatch,
                        "payload-pack physical metadata does not match its binding",
                    ));
                }
                next.report.unique_physical_objects = checked_pack_add(
                    next.report.unique_physical_objects,
                    1,
                    "unique physical object",
                )?;
                next.report.unique_physical_bytes = checked_pack_add(
                    next.report.unique_physical_bytes,
                    metadata.len,
                    "unique physical byte",
                )?;
                if binding.is_packed() {
                    next.report.unique_pack_objects =
                        checked_pack_add(next.report.unique_pack_objects, 1, "unique pack object")?;
                    next.report.unique_pack_bytes = checked_pack_add(
                        next.report.unique_pack_bytes,
                        metadata.len,
                        "unique pack byte",
                    )?;
                }
                mutations.push(Mutation::Upsert {
                    key: physical_key,
                    val: metadata.len.to_be_bytes().to_vec(),
                });
            }
            if let Some((start, end)) = binding.pack_range {
                let extent_key = payload_pack_extent_key(binding, start, end);
                if pending_extents.insert(extent_key.clone())
                    && stats_engine.get(&seen, &extent_key).await?.is_none()
                {
                    let extent_bytes = end
                        .checked_sub(start)
                        .and_then(|length| length.checked_add(1))
                        .ok_or_else(|| {
                            Error::new(ErrorCode::CorruptContent, "payload-pack extent is invalid")
                        })?;
                    next.report.unique_packed_extents = checked_pack_add(
                        next.report.unique_packed_extents,
                        1,
                        "unique packed extent",
                    )?;
                    next.report.unique_packed_extent_bytes = checked_pack_add(
                        next.report.unique_packed_extent_bytes,
                        extent_bytes,
                        "unique packed extent byte",
                    )?;
                    mutations.push(Mutation::Upsert {
                        key: extent_key,
                        val: Vec::new(),
                    });
                }
            }
            next.after = Some(key);
            processed += 1;
        }
        if !mutations.is_empty() {
            seen = stats_engine.batch(&seen, mutations).await?;
        }
        next.seen = RootManifest::from_tree(&seen)?;
        Ok(PayloadPackStatsPage {
            complete: next.complete,
            cursor: next,
            processed,
        })
    }

    /// Start a restartable logical transfer that preserves the complete source
    /// commit DAG. Commit and object-version IDs are mapped because repository
    /// identity, authority stamps, and provider bindings are destination-local.
    pub async fn start_history_transfer_from<Q: ObjectPlane>(
        &self,
        source: &Repository<Q>,
        source_branch: &str,
        source_head: CommitId,
        destination_branch: &str,
        expected_destination_head: CommitId,
    ) -> Result<HistoryTransferCursor> {
        validate_branch(source_branch)?;
        validate_branch(destination_branch)?;
        source.locator.register(source_branch)?;
        source.require_branch_indexes_ready(source_branch).await?;
        source.load_commit_object(source_head).await?;
        self.locator.register(destination_branch)?;
        self.require_branch_indexes_ready(destination_branch)
            .await?;
        if self.head(destination_branch).await? != expected_destination_head {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "destination head does not match history-transfer expectation",
            ));
        }
        let job = self.options.ids.operation();
        let mappings = self.history_transfer_mapping_engine(job)?.create();
        let cursor = HistoryTransferCursor {
            source_repository: source.format.repository_id,
            destination_repository: self.format.repository_id,
            job,
            source_branch: source_branch.to_string(),
            destination_branch: destination_branch.to_string(),
            source_head,
            expected_destination_head,
            closure: source.start_commit_closure(&[source_head]).await?,
            mappings: RootManifest::from_tree(&mappings)?,
            pending: None,
            mapped_head: None,
            report: HistoryTransferReport::default(),
            complete: false,
        };
        self.validate_history_transfer_cursor(source, &cursor)?;
        Ok(cursor)
    }

    /// Advance a history transfer by bounded traversal or tree-mutation work.
    /// Persist the returned cursor before discarding its predecessor.
    pub async fn advance_history_transfer_from<Q: ObjectPlane>(
        &self,
        source: &Repository<Q>,
        cursor: &HistoryTransferCursor,
        max_steps: usize,
    ) -> Result<HistoryTransferPage> {
        if !(1..=1_000).contains(&max_steps) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "history-transfer page size must be between 1 and 1,000",
            ));
        }
        self.validate_history_transfer_cursor(source, cursor)?;
        if cursor.complete {
            return Ok(HistoryTransferPage {
                cursor: cursor.clone(),
                traversal_steps: 0,
                mutation_steps: 0,
                imported_commits: 0,
                complete: true,
            });
        }
        if self.head(&cursor.destination_branch).await? != cursor.expected_destination_head {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "destination branch moved during history transfer",
            ));
        }
        let mut next = cursor.clone();
        if next.pending.is_none() {
            let page = source
                .commit_closure_page(&next.closure, max_steps, 1)
                .await?;
            let traversal_steps = page.steps;
            let Some((source_id, source_commit)) = page.commits.into_iter().next() else {
                next.closure = page.cursor;
                if page.complete {
                    if next.mapped_head.is_none() {
                        return Err(Error::new(
                            ErrorCode::MissingClosure,
                            "history transfer completed without mapping its source head",
                        ));
                    }
                    next.complete = true;
                }
                return Ok(HistoryTransferPage {
                    complete: next.complete,
                    cursor: next,
                    traversal_steps,
                    mutation_steps: 0,
                    imported_commits: 0,
                });
            };
            let mapping_engine = self.history_transfer_mapping_engine(next.job)?;
            let mapping_tree = self.tree_from_root(&next.mappings)?;
            let mut mapped_parents = Vec::with_capacity(source_commit.parents.len());
            for parent in &source_commit.parents {
                let encoded = mapping_engine
                    .get(&mapping_tree, &commit_mapping_key(*parent))
                    .await?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorCode::MissingClosure,
                            "parent commit was not mapped before its child",
                        )
                    })?;
                let hash: [u8; 32] = encoded.try_into().map_err(|_| {
                    Error::new(
                        ErrorCode::CorruptCommit,
                        "mapped commit ID has wrong length",
                    )
                })?;
                mapped_parents.push(CommitId::from_hash(hash));
            }
            let (objects, versions) = if let Some(first_parent) = mapped_parents.first() {
                let parent = self.load_commit_object(*first_parent).await?.commit;
                (parent.state.objects, parent.state.versions)
            } else {
                let engine = self.merge_state_engine();
                (
                    RootManifest::from_tree(&engine.create())?,
                    RootManifest::from_tree(&engine.create())?,
                )
            };
            let delta =
                RootManifest::from_tree(&self.history_transfer_delta_engine(next.job)?.create())?;
            next.pending = Some(PendingHistoryTransferCommit {
                source: source_id,
                next_closure: page.cursor,
                mapped_parents,
                objects,
                versions,
                delta,
                phase: if source_commit.parents.len() > 1 {
                    HistoryTransferPhase::UnionParentVersions
                } else {
                    HistoryTransferPhase::ApplyTransitions
                },
                union_parent_index: 1,
                union_base: None,
                union_diff: None,
                inline_index: 0,
                external_after: None,
                transitions_applied: 0,
            });
            return Ok(HistoryTransferPage {
                cursor: next,
                traversal_steps,
                mutation_steps: 0,
                imported_commits: 0,
                complete: false,
            });
        }
        let phase = next
            .pending
            .as_ref()
            .expect("pending transfer checked")
            .phase;
        let (mutation_steps, imported_commits) = match phase {
            HistoryTransferPhase::UnionParentVersions => (
                self.advance_history_transfer_union(&mut next, max_steps)
                    .await?,
                0,
            ),
            HistoryTransferPhase::ApplyTransitions => (
                self.advance_history_transfer_transitions(source, &mut next, max_steps)
                    .await?,
                0,
            ),
            HistoryTransferPhase::FinalizeCommit => {
                self.finalize_history_transfer_commit(source, &mut next)
                    .await?;
                (0, 1)
            }
        };
        Ok(HistoryTransferPage {
            complete: next.complete,
            cursor: next,
            traversal_steps: 0,
            mutation_steps,
            imported_commits,
        })
    }

    /// Publish the mapped source head by an audited ref movement after every
    /// source commit is durable at the destination.
    pub async fn publish_history_transfer(
        &self,
        cursor: &HistoryTransferCursor,
        reason: &str,
    ) -> Result<RefMoveReceipt> {
        if !cursor.complete || cursor.pending.is_some() {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "history transfer is not complete",
            ));
        }
        let mapped = cursor
            .mapped_head
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "mapped source head is absent"))?;
        self.reset_branch(
            &cursor.destination_branch,
            mapped,
            cursor.expected_destination_head,
            reason,
        )
        .await
    }

    pub async fn history_transfer_mapping(
        &self,
        cursor: &HistoryTransferCursor,
        source: CommitId,
    ) -> Result<Option<crate::HistoryTransferMapping>> {
        if cursor.destination_repository != self.format.repository_id || cursor.job.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "history-transfer cursor belongs to another destination",
            ));
        }
        let engine = self.history_transfer_mapping_engine(cursor.job)?;
        let tree = self.tree_from_root(&cursor.mappings)?;
        let Some(encoded) = engine.get(&tree, &commit_mapping_key(source)).await? else {
            return Ok(None);
        };
        let hash: [u8; 32] = encoded.try_into().map_err(|_| {
            Error::new(
                ErrorCode::CorruptCommit,
                "mapped commit ID has wrong length",
            )
        })?;
        Ok(Some(crate::HistoryTransferMapping {
            source,
            destination: CommitId::from_hash(hash),
        }))
    }

    /// Start a logical source-assisted repair. Provider-specific bindings are
    /// never copied: payload bytes are verified by the source and rebound by
    /// the destination into new logical versions.
    pub async fn start_repair_from<Q: ObjectPlane>(
        &self,
        source: &Repository<Q>,
        source_branch: &str,
        source_snapshot: CommitId,
        destination_branch: &str,
        expected_head: CommitId,
        message: impl Into<String>,
    ) -> Result<RepairCursor> {
        validate_branch(source_branch)?;
        validate_branch(destination_branch)?;
        let message = message.into();
        if message.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "repair message must not be empty",
            ));
        }
        source.locator.register(source_branch)?;
        source.require_branch_indexes_ready(source_branch).await?;
        source.load_commit_object(source_snapshot).await?;
        self.locator.register(destination_branch)?;
        self.require_branch_indexes_ready(destination_branch)
            .await?;
        if self.head(destination_branch).await? != expected_head {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "destination head does not match repair expectation",
            ));
        }
        let checkpoint = self
            .begin_durable_commit_session(destination_branch, message.clone(), 24 * 60 * 60 * 1_000)
            .await?;
        Ok(RepairCursor {
            source_repository: source.format.repository_id,
            destination_repository: self.format.repository_id,
            source_branch: source_branch.to_string(),
            destination_branch: destination_branch.to_string(),
            source_snapshot,
            destination_snapshot: expected_head,
            expected_head,
            source_after: None,
            destination_after: None,
            phase: RepairPhase::CopySource,
            batch: checkpoint.session.id,
            checkpoint_sequence: checkpoint.sequence,
            message,
            report: RepairReport::default(),
        })
    }

    pub async fn advance_repair_from<Q: ObjectPlane>(
        &self,
        source: &Repository<Q>,
        cursor: &RepairCursor,
        max_steps: usize,
    ) -> Result<RepairPage> {
        if cursor.source_repository != source.format.repository_id
            || cursor.destination_repository != self.format.repository_id
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "repair cursor belongs to another source or destination",
            ));
        }
        if cursor.phase == RepairPhase::Complete {
            return Ok(RepairPage {
                cursor: cursor.clone(),
                processed: 0,
                receipt: None,
                complete: true,
            });
        }
        if max_steps == 0 || max_steps > self.format.canonical_limits.max_list_page as usize {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "repair page size exceeds the canonical list limit",
            ));
        }
        if self.head(&cursor.destination_branch).await? != cursor.expected_head {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "destination branch moved during repair",
            ));
        }
        let checkpoint = self.resume_commit_session(cursor.batch).await?;
        if checkpoint.sequence != cursor.checkpoint_sequence
            || checkpoint.session.base_commit != cursor.expected_head
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "repair cursor is stale relative to its checkpoint",
            ));
        }
        let remaining = (self.format.canonical_limits.max_mutations_per_commit as usize)
            .checked_sub(checkpoint.mutations.len())
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::CorruptCommit,
                    "repair checkpoint exceeds its mutation limit",
                )
            })?;
        if remaining == 0 {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "repair checkpoint is full but unpublished",
            ));
        }
        let limit = max_steps.min(remaining);
        let mut next = cursor.clone();
        let mut mutations = checkpoint.mutations;
        let processed;
        match cursor.phase {
            RepairPhase::CopySource => {
                let (objects, truncated) = source
                    .list_objects_at(
                        &cursor.source_branch,
                        cursor.source_snapshot,
                        b"",
                        cursor.source_after.as_deref(),
                        limit,
                    )
                    .await?;
                processed = objects.len();
                next.source_after = objects.last().map(|object| object.key.clone());
                next.report.scanned_source_objects = checked_fsck_add(
                    next.report.scanned_source_objects,
                    processed as u64,
                    "repair-source-object",
                )?;
                for object in objects {
                    let existing = self
                        .head_object_at(
                            &cursor.destination_branch,
                            cursor.destination_snapshot,
                            &object.key,
                        )
                        .await?;
                    if existing
                        .as_ref()
                        .is_some_and(|value| value.version.body.kind == object.version.body.kind)
                    {
                        continue;
                    }
                    let data = source
                        .get_object_at(&cursor.source_branch, cursor.source_snapshot, &object.key)
                        .await?
                        .ok_or_else(|| {
                            Error::new(
                                ErrorCode::CorruptCommit,
                                "repair listing points to a missing source object",
                            )
                        })?;
                    let size = u64::try_from(data.bytes.len()).map_err(|_| {
                        Error::new(ErrorCode::EntityTooLarge, "repair payload exceeds u64")
                    })?;
                    let binding = self.payloads.put(data.bytes).await?;
                    let LogicalObjectVersionKind::Live {
                        logical_etag,
                        headers,
                        checksums,
                        user_metadata,
                        tags,
                        ..
                    } = data.version.body.kind
                    else {
                        return Err(Error::new(
                            ErrorCode::CorruptCommit,
                            "repair source current object is a delete marker",
                        ));
                    };
                    mutations.push(StagedMutation {
                        body: StagedMutationBody::Put(Box::new(StagedPut {
                            key: object.key,
                            size,
                            logical_etag,
                            checksums,
                            headers,
                            user_metadata,
                            tags,
                            binding,
                        })),
                    });
                    next.report.copied_objects =
                        checked_fsck_add(next.report.copied_objects, 1, "repair-copied-object")?;
                    next.report.copied_bytes =
                        checked_fsck_add(next.report.copied_bytes, size, "repair-copied-byte")?;
                }
                if !truncated {
                    next.phase = RepairPhase::DeleteDestinationOnly;
                    next.source_after = None;
                }
            }
            RepairPhase::DeleteDestinationOnly => {
                let (objects, truncated) = self
                    .list_objects_at(
                        &cursor.destination_branch,
                        cursor.destination_snapshot,
                        b"",
                        cursor.destination_after.as_deref(),
                        limit,
                    )
                    .await?;
                processed = objects.len();
                next.destination_after = objects.last().map(|object| object.key.clone());
                next.report.scanned_destination_objects = checked_fsck_add(
                    next.report.scanned_destination_objects,
                    processed as u64,
                    "repair-destination-object",
                )?;
                for object in objects {
                    if source
                        .head_object_at(&cursor.source_branch, cursor.source_snapshot, &object.key)
                        .await?
                        .is_none()
                    {
                        mutations.push(StagedMutation::delete(object.key));
                        next.report.deleted_objects = checked_fsck_add(
                            next.report.deleted_objects,
                            1,
                            "repair-deleted-object",
                        )?;
                    }
                }
                if !truncated {
                    next.phase = RepairPhase::Complete;
                    next.destination_after = None;
                }
            }
            RepairPhase::Complete => unreachable!(),
        }
        let full =
            mutations.len() >= self.format.canonical_limits.max_mutations_per_commit as usize;
        if !full && next.phase != RepairPhase::Complete {
            let sequence = checkpoint.sequence.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "repair checkpoint sequence overflow",
                )
            })?;
            let saved = self
                .checkpoint_commit_session(&checkpoint.session, mutations, sequence)
                .await?;
            next.checkpoint_sequence = saved.sequence;
            return Ok(RepairPage {
                cursor: next,
                processed,
                receipt: None,
                complete: false,
            });
        }
        if mutations.is_empty() {
            self.abort_commit_session(checkpoint.session, mutations, checkpoint.sequence)
                .await?;
            return Ok(RepairPage {
                cursor: next,
                processed,
                receipt: None,
                complete: true,
            });
        }
        let receipt = self
            .publish_commit_session(checkpoint.session, mutations)
            .await?;
        next.expected_head = receipt.id;
        next.report.published_commits =
            checked_fsck_add(next.report.published_commits, 1, "repair-published-commit")?;
        let complete = next.phase == RepairPhase::Complete;
        if !complete {
            let new_checkpoint = self
                .begin_durable_commit_session(
                    &cursor.destination_branch,
                    cursor.message.clone(),
                    24 * 60 * 60 * 1_000,
                )
                .await?;
            next.batch = new_checkpoint.session.id;
            next.checkpoint_sequence = new_checkpoint.sequence;
        }
        Ok(RepairPage {
            cursor: next,
            processed,
            receipt: Some(receipt),
            complete,
        })
    }

    pub async fn start_backup_verification<Q: ObjectPlane>(
        &self,
        destination: &Repository<Q>,
        source_branch: &str,
        source_snapshot: CommitId,
        destination_branch: &str,
        destination_snapshot: CommitId,
    ) -> Result<BackupVerificationCursor> {
        validate_branch(source_branch)?;
        validate_branch(destination_branch)?;
        self.locator.register(source_branch)?;
        destination.locator.register(destination_branch)?;
        self.require_branch_indexes_ready(source_branch).await?;
        destination
            .require_branch_indexes_ready(destination_branch)
            .await?;
        self.load_commit_object(source_snapshot).await?;
        destination.load_commit_object(destination_snapshot).await?;
        Ok(BackupVerificationCursor {
            source_repository: self.format.repository_id,
            destination_repository: destination.format.repository_id,
            source_branch: source_branch.to_string(),
            destination_branch: destination_branch.to_string(),
            source_snapshot,
            destination_snapshot,
            source_after: None,
            destination_after: None,
            report: BackupVerificationReport::default(),
            complete: false,
        })
    }

    /// Deeply compare one bounded page of two logical snapshots, including
    /// payload bytes. This qualifies a logical backup even when provider
    /// version IDs and immutable physical paths differ.
    pub async fn advance_backup_verification<Q: ObjectPlane>(
        &self,
        destination: &Repository<Q>,
        cursor: &BackupVerificationCursor,
        limit: usize,
    ) -> Result<BackupVerificationPage> {
        if cursor.complete {
            return Ok(BackupVerificationPage {
                cursor: cursor.clone(),
                processed: 0,
                complete: true,
            });
        }
        if cursor.source_repository != self.format.repository_id
            || cursor.destination_repository != destination.format.repository_id
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "backup verification cursor belongs to another repository pair",
            ));
        }
        if limit == 0 || limit > self.format.canonical_limits.max_list_page as usize {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "backup verification page size exceeds the canonical list limit",
            ));
        }
        let (source_objects, source_truncated) = self
            .list_objects_at(
                &cursor.source_branch,
                cursor.source_snapshot,
                b"",
                cursor.source_after.as_deref(),
                limit,
            )
            .await?;
        let (destination_objects, destination_truncated) = destination
            .list_objects_at(
                &cursor.destination_branch,
                cursor.destination_snapshot,
                b"",
                cursor.destination_after.as_deref(),
                limit,
            )
            .await?;
        if source_objects.len() != destination_objects.len()
            || source_truncated != destination_truncated
        {
            return Err(Error::new(
                ErrorCode::CorruptContent,
                "backup snapshots have different object-key cardinality",
            ));
        }
        let processed = source_objects.len();
        let mut next = cursor.clone();
        next.source_after = source_objects.last().map(|object| object.key.clone());
        next.destination_after = destination_objects.last().map(|object| object.key.clone());
        for (source_object, destination_object) in
            source_objects.into_iter().zip(destination_objects)
        {
            if source_object.key != destination_object.key
                || source_object.version.body.kind != destination_object.version.body.kind
            {
                return Err(Error::new(
                    ErrorCode::CorruptContent,
                    "backup snapshots have different logical object metadata",
                ));
            }
            let source_data = self
                .get_object_at(
                    &cursor.source_branch,
                    cursor.source_snapshot,
                    &source_object.key,
                )
                .await?
                .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "source object is missing"))?;
            let destination_data = destination
                .get_object_at(
                    &cursor.destination_branch,
                    cursor.destination_snapshot,
                    &destination_object.key,
                )
                .await?
                .ok_or_else(|| {
                    Error::new(ErrorCode::MissingClosure, "destination object is missing")
                })?;
            if source_data.bytes != destination_data.bytes {
                return Err(Error::new(
                    ErrorCode::CorruptContent,
                    "backup snapshots have different payload bytes",
                ));
            }
            next.report.objects_verified =
                checked_fsck_add(next.report.objects_verified, 1, "backup-verified-object")?;
            next.report.content_bytes_verified = checked_fsck_add(
                next.report.content_bytes_verified,
                source_data.bytes.len() as u64,
                "backup-verified-byte",
            )?;
        }
        next.complete = !source_truncated;
        if next.complete {
            next.source_after = None;
            next.destination_after = None;
        }
        Ok(BackupVerificationPage {
            complete: next.complete,
            cursor: next,
            processed,
        })
    }

    /// Return all current-object changes between two immutable snapshots.
    pub async fn diff(
        &self,
        branch: &str,
        from: CommitId,
        to: CommitId,
    ) -> Result<Vec<ObjectDiff>> {
        validate_branch(branch)?;
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        let from_commit = self.load_commit_metadata(from).await?;
        let to_commit = self.load_commit_metadata(to).await?;
        let from_tree = self.tree_from_root(&from_commit.state.objects)?;
        let to_tree = self.tree_from_root(&to_commit.state.objects)?;
        self.engine(self.node_store.clone())
            .diff(&from_tree, &to_tree)
            .await?
            .into_iter()
            .map(object_diff_from_prolly)
            .collect()
    }

    /// Return one CID-pruned diff page without rediscovering the previous
    /// structural frontier when the caller resumes.
    pub async fn diff_page_bounded(
        &self,
        branch: &str,
        from: CommitId,
        to: CommitId,
        cursor: Option<&ObjectDiffCursor>,
        requested_limit: usize,
    ) -> Result<ObjectDiffPage> {
        validate_branch(branch)?;
        if cursor.is_some_and(|cursor| {
            cursor.repository != self.format.repository_id
                || cursor.branch != branch
                || cursor.from != from
                || cursor.to != to
        }) {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "diff cursor belongs to another repository, branch, or snapshot pair",
            ));
        }
        let limit = requested_limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "bounded diff page limit must be greater than zero",
            ));
        }
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        let from_commit = self.load_commit_metadata(from).await?;
        let to_commit = self.load_commit_metadata(to).await?;
        let from_tree = self.tree_from_root(&from_commit.state.objects)?;
        let to_tree = self.tree_from_root(&to_commit.state.objects)?;
        let page = self
            .engine(self.node_store.clone())
            .structural_diff_page(
                &from_tree,
                &to_tree,
                cursor.map(|cursor| &cursor.traversal),
                limit,
            )
            .await?;
        let changes = page
            .diffs
            .into_iter()
            .map(object_diff_from_prolly)
            .collect::<Result<Vec<_>>>()?;
        Ok(ObjectDiffPage {
            changes,
            continuation: page.next_cursor.map(|traversal| ObjectDiffCursor {
                repository: self.format.repository_id,
                branch: branch.to_string(),
                from,
                to,
                traversal,
            }),
            compared_nodes: page.stats.compared_nodes,
            reused_subtrees: page.stats.reused_subtrees,
        })
    }

    /// Open a stable snapshot of the immutable branch reflog.
    pub async fn open_reflog(&self, branch: &str) -> Result<crate::PublicationJournalCursor> {
        self.publisher.open_journal(branch).await
    }

    /// Read one newest-to-oldest page from a stable reflog snapshot.
    pub async fn read_reflog_page(
        &self,
        cursor: &crate::PublicationJournalCursor,
        limit: usize,
    ) -> Result<crate::PublicationJournalPage> {
        self.publisher.read_journal_page(cursor, limit).await
    }

    /// Administratively move a branch ref to an existing commit. The move is
    /// fenced, CAS-protected, and recorded in the immutable publication log.
    pub async fn reset_branch(
        &self,
        branch: &str,
        to: CommitId,
        expected_head: CommitId,
        reason: &str,
    ) -> Result<RefMoveReceipt> {
        if !self.writable.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "repository is read-only",
            ));
        }
        validate_branch(branch)?;
        if reason.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "administrative ref movement requires a non-empty reason",
            ));
        }
        let _lane = self.lock_branch(branch).await;
        let now = self.options.clock.now_millis()?;
        let permit = self.active_permit(branch, now).await?;
        self.authority.validate_active(&permit, now).await?;
        self.load_commit_object(to).await?;
        let current = self.publisher.load(branch).await?;
        if current.value.target != expected_head {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "branch head does not match reset expectation",
            ));
        }
        let operation = self.options.ids.operation();
        let old_target = current.value.target;
        let moved = self
            .publisher
            .move_target(
                current,
                BranchMovement {
                    permit: &permit,
                    branch,
                    target: to,
                    operation,
                    message: reason,
                    now_millis: now,
                },
            )
            .await?;
        self.record_branch_catalog(&moved).await?;
        self.advance_branch_indexes(branch).await?;
        Ok(RefMoveReceipt {
            branch: branch.to_string(),
            old_target,
            new_target: to,
            operation,
            generation: moved.value.generation,
        })
    }

    /// Recover the previous target named by a reflog entry. The selected entry
    /// is found through bounded pages of the immutable publication journal.
    pub async fn recover_branch(
        &self,
        branch: &str,
        reflog: crate::ReflogEntryId,
        expected_head: CommitId,
        reason: &str,
    ) -> Result<RefMoveReceipt> {
        let mut cursor = Some(self.open_reflog(branch).await?);
        let mut target = None;
        while let Some(current) = cursor {
            let page = self.read_reflog_page(&current, 1_000).await?;
            if let Some(event) = page
                .entries
                .iter()
                .find(|entry| entry.event.reflog == reflog)
            {
                target = event.event.old_target;
                break;
            }
            cursor = page.continuation;
        }
        let target = target.ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidRevision,
                "selected reflog entry has no recoverable previous target",
            )
        })?;
        self.reset_branch(branch, target, expected_head, reason)
            .await
    }

    /// Start a restartable snapshot restore. Changed keys reuse immutable
    /// payload bindings, receive fresh logical versions, and are published in
    /// bounded atomic commits while preserving the branch's existing history.
    pub async fn start_restore(
        &self,
        branch: &str,
        source: CommitId,
        expected_head: CommitId,
        message: impl Into<String>,
    ) -> Result<RestoreCursor> {
        validate_branch(branch)?;
        let message = message.into();
        if message.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "restore message must not be empty",
            ));
        }
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        self.load_commit_object(source).await?;
        if self.head(branch).await? != expected_head {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "branch head does not match restore expectation",
            ));
        }
        let checkpoint = self
            .begin_durable_commit_session(branch, message.clone(), 24 * 60 * 60 * 1_000)
            .await?;
        Ok(RestoreCursor {
            repository: self.format.repository_id,
            branch: branch.to_string(),
            source,
            original_head: expected_head,
            expected_head,
            batch: checkpoint.session.id,
            checkpoint_sequence: checkpoint.sequence,
            diff: None,
            message,
            complete: false,
        })
    }

    /// Advance a restore by one structural-diff page. A full atomic batch is
    /// published as soon as it reaches the canonical mutation limit; larger
    /// restores continue in subsequent commits from the same diff cursor.
    pub async fn advance_restore(
        &self,
        cursor: &RestoreCursor,
        max_steps: usize,
    ) -> Result<RestorePage> {
        if cursor.complete {
            return Ok(RestorePage {
                cursor: cursor.clone(),
                processed: 0,
                receipt: None,
                complete: true,
            });
        }
        if cursor.repository != self.format.repository_id {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "restore cursor belongs to another repository",
            ));
        }
        validate_branch(&cursor.branch)?;
        if max_steps == 0 || max_steps > self.format.canonical_limits.max_list_page as usize {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "restore page size exceeds the canonical list limit",
            ));
        }
        if self.head(&cursor.branch).await? != cursor.expected_head {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "branch moved while restore was in progress",
            ));
        }
        let checkpoint = self.resume_commit_session(cursor.batch).await?;
        if checkpoint.sequence != cursor.checkpoint_sequence
            || checkpoint.session.base_commit != cursor.expected_head
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "restore cursor is stale relative to its durable checkpoint",
            ));
        }
        let remaining = (self.format.canonical_limits.max_mutations_per_commit as usize)
            .checked_sub(checkpoint.mutations.len())
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::CorruptCommit,
                    "restore checkpoint exceeds the canonical mutation limit",
                )
            })?;
        if remaining == 0 {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "restore checkpoint is full without having been published",
            ));
        }
        let page = self
            .diff_page_bounded(
                &cursor.branch,
                cursor.original_head,
                cursor.source,
                cursor.diff.as_ref(),
                max_steps.min(remaining),
            )
            .await?;
        let processed = page.changes.len();
        let mut mutations = checkpoint.mutations;
        for change in page.changes {
            if change.to.is_none() {
                mutations.push(StagedMutation::delete(change.key));
                continue;
            }
            let source = self
                .head_object_at(&cursor.branch, cursor.source, &change.key)
                .await?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::CorruptCommit,
                        "restore diff points to a missing source object",
                    )
                })?;
            if Some(source.version.id) != change.to {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "restore diff and source object version disagree",
                ));
            }
            let LogicalObjectVersionKind::Live {
                size,
                logical_etag,
                headers,
                checksums,
                user_metadata,
                tags,
            } = source.version.body.kind
            else {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "restore source current object is a delete marker",
                ));
            };
            let binding = source.version.binding.ok_or_else(|| {
                Error::new(
                    ErrorCode::CorruptCommit,
                    "restore source object has no payload binding",
                )
            })?;
            mutations.push(StagedMutation {
                body: StagedMutationBody::Put(Box::new(StagedPut {
                    key: change.key,
                    size,
                    logical_etag,
                    checksums,
                    headers,
                    user_metadata,
                    tags,
                    binding,
                })),
            });
        }
        let mut next = cursor.clone();
        next.diff = page.continuation;
        let at_batch_limit =
            mutations.len() >= self.format.canonical_limits.max_mutations_per_commit as usize;
        let diff_complete = next.diff.is_none();
        if !at_batch_limit && !diff_complete {
            let sequence = checkpoint.sequence.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "restore checkpoint sequence overflow",
                )
            })?;
            let checkpoint = self
                .checkpoint_commit_session(&checkpoint.session, mutations, sequence)
                .await?;
            next.checkpoint_sequence = checkpoint.sequence;
            return Ok(RestorePage {
                cursor: next,
                processed,
                receipt: None,
                complete: false,
            });
        }
        if mutations.is_empty() {
            self.abort_commit_session(checkpoint.session, mutations, checkpoint.sequence)
                .await?;
            next.complete = true;
            return Ok(RestorePage {
                cursor: next,
                processed,
                receipt: None,
                complete: true,
            });
        }
        let receipt = self
            .publish_commit_session(checkpoint.session, mutations)
            .await?;
        next.expected_head = receipt.id;
        if diff_complete {
            next.complete = true;
        } else {
            let checkpoint = self
                .begin_durable_commit_session(
                    &cursor.branch,
                    cursor.message.clone(),
                    24 * 60 * 60 * 1_000,
                )
                .await?;
            next.batch = checkpoint.session.id;
            next.checkpoint_sequence = checkpoint.sequence;
        }
        Ok(RestorePage {
            complete: next.complete,
            cursor: next,
            processed,
            receipt: Some(receipt),
        })
    }

    /// Start a durable merge between two branch snapshots.
    ///
    /// The returned cursor is process-independent. Callers must persist the
    /// cursor returned by every successful `advance_merge` call before
    /// discarding the previous one.
    pub async fn start_merge(
        &self,
        target_branch: &str,
        source_branch: &str,
        requested_base: Option<CommitId>,
        policy: MergePolicy,
        message: impl Into<String>,
    ) -> Result<MergeCursor> {
        crate::repository::validate_branch(target_branch)?;
        crate::repository::validate_branch(source_branch)?;
        if target_branch == source_branch {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "merge source and target branches must differ",
            ));
        }
        let message = message.into();
        if message.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "merge message is empty",
            ));
        }
        self.locator.register(target_branch)?;
        self.locator.register(source_branch)?;
        self.advance_branch_indexes(target_branch).await?;
        self.advance_branch_indexes(source_branch).await?;
        self.require_branch_indexes_ready(target_branch).await?;
        self.require_branch_indexes_ready(source_branch).await?;
        let ours = self.head(target_branch).await?;
        let theirs = self.head(source_branch).await?;
        let ours_entry = self
            .merge_graph_entry(target_branch, source_branch, ours)
            .await?;
        let theirs_entry = self
            .merge_graph_entry(target_branch, source_branch, theirs)
            .await?;
        let job = self.options.ids.operation();
        let operation = self.options.ids.operation();
        let engine = self.merge_plan_engine(job)?;
        let mut tree = engine.create();
        let now = self.options.clock.now_millis()?;
        let mut cursor = MergeCursor {
            repository: self.format.repository_id,
            job,
            target_branch: target_branch.to_string(),
            source_branch: source_branch.to_string(),
            ours,
            theirs,
            requested_base,
            selected_base: None,
            policy,
            operation,
            message,
            created_at_millis: now,
            phase: MergePhase::DiscoveringBases,
            plan_root: RootManifest::from_tree(&tree)?,
            ours_diff: None,
            theirs_diff: None,
            ours_pending: None,
            theirs_pending: None,
            ours_finished: false,
            theirs_finished: false,
            version_diff: None,
            version_diff_finished: false,
            build_after: None,
            final_objects: None,
            final_versions: None,
            delta_root: None,
            visited_commits: 0,
            best_base_count: 0,
            planned_changes: 0,
            conflicts: 0,
            built_changes: 0,
        };

        // First-parent ancestry is the common fast-forward case. The helper
        // consumes binary-lifting pointers from the journal-derived graph and
        // avoids creating a general frontier when one head is already the
        // selected base.
        let fast_base = if self
            .is_first_parent_ancestor(target_branch, source_branch, ours, theirs)
            .await?
        {
            Some(ours)
        } else if self
            .is_first_parent_ancestor(target_branch, source_branch, theirs, ours)
            .await?
        {
            Some(theirs)
        } else {
            None
        };
        if let Some(base) = fast_base {
            tree = engine
                .batch(
                    &tree,
                    vec![Mutation::Upsert {
                        key: merge_base_result_key(base),
                        val: Vec::new(),
                    }],
                )
                .await?;
            cursor.plan_root = RootManifest::from_tree(&tree)?;
            cursor.best_base_count = 1;
            cursor.selected_base = Some(base);
            self.validate_requested_merge_base(&cursor, base)?;
            cursor.phase = MergePhase::Planning;
            self.seal_merge_cursor(&mut cursor).await?;
            return Ok(cursor);
        }

        let mut mutations = Vec::new();
        self.seed_merge_frontier(&mut mutations, ours_entry, MERGE_LEFT)?;
        self.seed_merge_frontier(&mut mutations, theirs_entry, MERGE_RIGHT)?;
        tree = engine.batch(&tree, mutations).await?;
        cursor.plan_root = RootManifest::from_tree(&tree)?;
        self.seal_merge_cursor(&mut cursor).await?;
        Ok(cursor)
    }

    /// Advance a durable merge by at most `max_steps` graph or tree records.
    pub async fn advance_merge(
        &self,
        cursor: &MergeCursor,
        max_steps: usize,
    ) -> Result<MergeAdvancePage> {
        if !(1..=10_000).contains(&max_steps) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "merge advance must process 1 to 10,000 records",
            ));
        }
        self.validate_merge_cursor(cursor).await?;
        let mut next = cursor.clone();
        let mut processed = 0usize;
        let mut changes = Vec::new();
        let mut conflicts = Vec::new();
        while processed < max_steps {
            let before = next.phase;
            let advanced = match next.phase {
                MergePhase::DiscoveringBases => self.advance_merge_base_one(&mut next).await?,
                MergePhase::CollectingBases => self.collect_merge_base_one(&mut next).await?,
                MergePhase::Planning => {
                    self.advance_merge_plan(
                        &mut next,
                        max_steps - processed,
                        &mut changes,
                        &mut conflicts,
                    )
                    .await?
                }
                MergePhase::BuildingVersions => {
                    self.advance_merge_version_union(&mut next, max_steps - processed)
                        .await?
                }
                MergePhase::BuildingObjects => {
                    self.advance_merge_object_build(&mut next, max_steps - processed)
                        .await?
                }
                MergePhase::AwaitingBase | MergePhase::Conflicted | MergePhase::ReadyToPublish => 0,
            };
            processed = processed.checked_add(advanced).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "merge work counter overflow")
            })?;
            if advanced == 0 && next.phase == before {
                break;
            }
        }
        self.seal_merge_cursor(&mut next).await?;
        Ok(MergeAdvancePage {
            cursor: next,
            processed,
            changes,
            conflicts,
        })
    }

    /// Select one of several best merge bases discovered by the frontier.
    pub async fn select_merge_base(
        &self,
        cursor: &MergeCursor,
        base: CommitId,
    ) -> Result<MergeCursor> {
        self.validate_merge_cursor(cursor).await?;
        if cursor.phase != MergePhase::AwaitingBase {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "merge is not awaiting an explicit merge base",
            ));
        }
        let engine = self.merge_plan_engine(cursor.job)?;
        let tree = self.tree_from_merge_root(&cursor.plan_root)?;
        if engine
            .get(&tree, &merge_base_result_key(base))
            .await?
            .is_none()
        {
            return Err(Error::new(
                ErrorCode::InvalidRevision,
                "selected merge base is not a best common ancestor",
            ));
        }
        let mut next = cursor.clone();
        next.selected_base = Some(base);
        next.phase = MergePhase::Planning;
        self.seal_merge_cursor(&mut next).await?;
        Ok(next)
    }

    pub async fn merge_changes_page(
        &self,
        cursor: &MergeCursor,
        continuation: Option<&MergeChangeCursor>,
        limit: usize,
    ) -> Result<MergeChangePage> {
        self.merge_change_page(cursor, continuation, limit).await
    }

    pub async fn merge_bases_page(
        &self,
        cursor: &MergeCursor,
        continuation: Option<&MergeBaseCursor>,
        limit: usize,
    ) -> Result<MergeBasePage> {
        self.merge_base_page(cursor, continuation, limit).await
    }

    pub async fn merge_conflicts_page(
        &self,
        cursor: &MergeCursor,
        continuation: Option<&MergeConflictCursor>,
        limit: usize,
    ) -> Result<MergeConflictPage> {
        self.merge_conflict_page(cursor, continuation, limit).await
    }

    /// CAS-publish a completely built merge plan. Replaying this call with the
    /// same cursor and operation ID reconciles an ambiguous prior publication.
    pub async fn publish_merge(&self, cursor: &MergeCursor) -> Result<MergeReceipt> {
        self.validate_merge_cursor(cursor).await?;
        if cursor.phase != MergePhase::ReadyToPublish {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "merge plan is not ready to publish",
            ));
        }
        if cursor.policy == MergePolicy::Fail && cursor.conflicts != 0 {
            return Err(Error::new(
                ErrorCode::MergeConflict,
                "merge plan contains unresolved conflicts",
            ));
        }
        let base = cursor.selected_base.ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "merge plan has no selected base",
            )
        })?;
        let input_digest = self.merge_input_digest(cursor, base);
        let _lane = self.lock_branch(&cursor.target_branch).await;
        let now = self.options.clock.now_millis()?;
        if let Some(receipt) = self
            .reconcile_merge_operation(cursor, input_digest, now)
            .await?
        {
            return Ok(receipt);
        }
        let current = self.publisher.load(&cursor.target_branch).await?;
        if current.value.target != cursor.ours {
            return Err(Error::new(
                ErrorCode::RefConflict,
                "merge target moved after planning",
            ));
        }
        let permit = self.active_permit(&cursor.target_branch, now).await?;
        let ours = self.load_commit_metadata(cursor.ours).await?;
        let theirs = self.load_commit_metadata(cursor.theirs).await?;
        let generation = CommitGeneration(
            ours.generation
                .0
                .max(theirs.generation.0)
                .checked_add(1)
                .ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "merge generation overflow")
                })?,
        );
        let commit = BucketCommit {
            state: BucketState {
                objects: cursor.final_objects.clone().ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "merge object root is absent")
                })?,
                versions: cursor.final_versions.clone().ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "merge version root is absent")
                })?,
            },
            parents: vec![cursor.ours, cursor.theirs],
            generation,
            delta: BucketDelta {
                input_digest,
                changes: Vec::new(),
                changes_root: cursor.delta_root.clone(),
                change_count: cursor.built_changes,
            },
            node_pack: None,
            authority: permit.stamp(),
            author: self.options.writer.clone(),
            message: Some(cursor.message.clone()),
            created_at_millis: cursor.created_at_millis,
            metadata: BTreeMap::new(),
        };
        let publication = self
            .publisher
            .store_and_publish(
                current,
                CommitPublication {
                    permit: &permit,
                    branch: &cursor.target_branch,
                    commit: &commit,
                    node_pack: None,
                    operation: cursor.operation,
                    message: &cursor.message,
                    now_millis: now,
                },
            )
            .await;
        match publication {
            Ok(published) => {
                self.mark_local_index_head(&cursor.target_branch, published.value.target)?;
                Ok(MergeReceipt {
                    id: published.value.target,
                    operation: cursor.operation,
                    branch: cursor.target_branch.clone(),
                    parents: [cursor.ours, cursor.theirs],
                    changed_keys: cursor.built_changes,
                    conflicts: cursor.conflicts,
                    idempotent_replay: false,
                })
            }
            Err(error) => match self
                .reconcile_merge_operation(cursor, input_digest, now)
                .await?
            {
                Some(receipt) => Ok(receipt),
                None => {
                    self.fence_branch(&cursor.target_branch)?;
                    Err(error)
                }
            },
        }
    }

    /// Exact-delete one bounded page of job-scoped merge-plan nodes after a
    /// successful publication or an explicitly abandoned plan. State and
    /// delta nodes referenced by a published commit use the repository node
    /// namespace and are not touched here.
    pub async fn cleanup_merge(
        &self,
        cursor: &MergeCursor,
        continuation: Option<&MergeCleanupCursor>,
        limit: usize,
    ) -> Result<MergeCleanupPage> {
        if continuation.is_none() {
            self.validate_merge_cursor(cursor).await?;
        } else if cursor.repository != self.format.repository_id || cursor.job.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "merge cleanup cursor belongs to another repository",
            ));
        }
        if !(1..=1_000).contains(&limit) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "merge cleanup page must contain 1 to 1,000 objects",
            ));
        }
        if continuation.is_some_and(|continuation| {
            continuation.repository != cursor.repository || continuation.job != cursor.job
        }) {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "merge cleanup cursor belongs to another job",
            ));
        }
        let prefix = format!(
            "{}/administration/merge/{}/plan/",
            self.options.repository_prefix, cursor.job
        );
        let page = self
            .plane
            .list(ListRequest {
                prefix,
                continuation: continuation.map(|cursor| cursor.provider_continuation.clone()),
                limit,
                include_versions: true,
            })
            .await?;
        let mut deleted = 0usize;
        for entry in page.entries {
            let physical_version = entry
                .metadata
                .token
                .version_id
                .clone()
                .map(|version_id| PhysicalVersion::Versioned { version_id })
                .unwrap_or_else(|| PhysicalVersion::Unversioned {
                    token: Some(entry.metadata.token),
                });
            match self
                .plane
                .delete_exact(&entry.path, physical_version)
                .await?
            {
                DeleteOutcome::Deleted | DeleteOutcome::NotFound => deleted += 1,
                DeleteOutcome::TokenMismatch => {
                    return Err(Error::new(
                        ErrorCode::RefConflict,
                        "merge cleanup object changed concurrently",
                    ))
                }
            }
        }
        Ok(MergeCleanupPage {
            deleted,
            continuation: page
                .continuation
                .map(|provider_continuation| MergeCleanupCursor {
                    repository: cursor.repository,
                    job: cursor.job,
                    provider_continuation,
                }),
        })
    }

    pub async fn advance_branch_indexes(&self, branch: &str) -> Result<BranchIndexAdvanceReport> {
        self.locator.register(branch)?;
        let _lane = self.lock_index_branch(branch).await;
        let now = self.options.clock.now_millis()?;
        let operations = self
            .operation_index
            .advance(&self.publisher, branch, now)
            .await?;
        let journal = self
            .journal_indexes
            .advance(&self.publisher, branch, now)
            .await?;
        let report = BranchIndexAdvanceReport {
            operations,
            journal,
        };
        self.index_errors
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "index-error lock poisoned"))?
            .remove(branch);
        let reference = self.publisher.load(branch).await?;
        self.record_branch_catalog(&reference).await?;
        Ok(report)
    }

    pub async fn start_branch_index_rebuild(
        &self,
        branch: &str,
    ) -> Result<JournalIndexRebuildCursor> {
        self.locator.register(branch)?;
        let _lane = self.lock_index_branch(branch).await;
        self.journal_indexes
            .start_rebuild(&self.publisher, branch, self.options.ids.operation())
            .await
    }

    pub async fn advance_branch_index_rebuild(
        &self,
        cursor: &JournalIndexRebuildCursor,
        max_events: usize,
    ) -> Result<JournalIndexRebuildStep> {
        self.locator.register(&cursor.branch)?;
        let _lane = self.lock_index_branch(&cursor.branch).await;
        let step = self
            .journal_indexes
            .advance_rebuild(
                &self.publisher,
                cursor,
                max_events,
                self.options.clock.now_millis()?,
            )
            .await?;
        if step.complete {
            self.index_errors
                .write()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "index-error lock poisoned"))?
                .remove(&cursor.branch);
        }
        Ok(step)
    }

    pub async fn cleanup_branch_index_rebuild(
        &self,
        journal: &JournalIndexRebuildCursor,
        operations: &OperationIndexRebuildCursor,
        limit: usize,
    ) -> Result<JournalIndexRebuildCleanup> {
        if !operations.complete
            || operations.next_chunk.is_some()
            || operations.repository != journal.repository
            || operations.branch != journal.branch
            || operations.job != journal.job
            || operations.snapshot != journal.snapshot
            || operations.snapshot_generation != journal.snapshot_generation
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "journal rebuild chunks remain live until the matching operation-index rebuild completes",
            ));
        }
        self.journal_indexes.cleanup_rebuild(journal, limit).await
    }

    pub async fn start_operation_index_rebuild(
        &self,
        journal: &JournalIndexRebuildCursor,
    ) -> Result<OperationIndexRebuildCursor> {
        if journal.phase != JournalIndexRebuildPhase::Complete {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "operation-index rebuild starts after journal-index application completes",
            ));
        }
        let oldest_chunk = journal.oldest_chunk.ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidContinuationToken,
                "completed journal-index rebuild has no discovery chunks",
            )
        })?;
        let _lane = self.lock_index_branch(&journal.branch).await;
        self.operation_index
            .start_rebuild(
                &journal.branch,
                journal.job,
                journal.snapshot,
                journal.snapshot_generation,
                oldest_chunk,
            )
            .await
    }

    pub async fn advance_operation_index_rebuild(
        &self,
        cursor: &OperationIndexRebuildCursor,
        max_events: usize,
    ) -> Result<OperationIndexRebuildStep> {
        if cursor.complete {
            return Ok(OperationIndexRebuildStep {
                cursor: cursor.clone(),
                indexed_events: 0,
                segments_written: 0,
                complete: true,
            });
        }
        let expected = cursor.next_chunk.ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidContinuationToken,
                "completed operation-index rebuild has no next chunk",
            )
        })?;
        let _lane = self.lock_index_branch(&cursor.branch).await;
        let chunk = self
            .journal_indexes
            .load_rebuild_chunk(&cursor.branch, cursor.job, expected)
            .await?;
        self.operation_index
            .advance_rebuild(
                &self.publisher,
                cursor,
                &chunk,
                expected,
                max_events,
                self.options.clock.now_millis()?,
            )
            .await
    }

    pub async fn branch_index_health(&self, branch: &str) -> Result<BranchIndexHealth> {
        self.locator.register(branch)?;
        let reference = self.publisher.load(branch).await?;
        self.branch_index_health_for(branch, &reference).await
    }

    async fn branch_index_health_for(
        &self,
        branch: &str,
        reference: &LoadedRef,
    ) -> Result<BranchIndexHealth> {
        let indexed = self.journal_indexes.head(branch).await?;
        if indexed
            .as_ref()
            .is_some_and(|head| head.checkpoint_generation.0 > reference.value.generation.0)
        {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "journal index is ahead of the branch ref",
            ));
        }
        let locally_registered = self
            .local_index_heads
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "local-index lock poisoned"))?
            .get(branch)
            .is_some_and(|target| *target == reference.value.target);
        // Node closure is ready when the durable index has already covered
        // this exact target. A takeover barrier may advance the publication
        // journal without changing the target; background maintenance still
        // consumes that event, but reads need not wait for it.
        let durable_ready = indexed
            .as_ref()
            .is_some_and(|head| head.target == reference.value.target);
        let indexed_generation = indexed.as_ref().map(|head| head.checkpoint_generation);
        let lag_generations = reference
            .value
            .generation
            .0
            .saturating_sub(indexed_generation.map_or(0, |generation| generation.0));
        let last_error = self
            .index_errors
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "index-error lock poisoned"))?
            .get(branch)
            .cloned();
        Ok(BranchIndexHealth {
            branch: branch.to_string(),
            target: reference.value.target,
            ref_generation: reference.value.generation,
            indexed_target: indexed.as_ref().map(|head| head.target),
            indexed_generation,
            lag_generations,
            ready: durable_ready || locally_registered,
            locally_registered,
            last_error,
        })
    }

    pub fn start_branch_index_maintenance(
        self: &Arc<Self>,
        interval: Duration,
    ) -> Result<BranchIndexMaintenance> {
        if interval < Duration::from_millis(10) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "branch-index maintenance interval must be at least 10 milliseconds",
            ));
        }
        let repository = Arc::downgrade(self);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let Some(repository) = repository.upgrade() else {
                    return;
                };
                let branches = match repository.locator.registered_branches() {
                    Ok(branches) => branches,
                    Err(_) => return,
                };
                for branch in branches {
                    if let Err(error) = repository.advance_branch_indexes(&branch).await {
                        if let Ok(mut errors) = repository.index_errors.write() {
                            errors.insert(branch, error.to_string());
                        }
                    }
                }
            }
        });
        Ok(BranchIndexMaintenance { task })
    }

    /// Renew every locally held branch-authority permit. A failed or ambiguous
    /// renewal fences only that branch in this repository instance; permits for
    /// independent branches continue to renew.
    pub async fn renew_shard_authorities(&self) -> Result<()> {
        if !self.writable.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "shard-authority renewal requires a writable repository",
            ));
        }
        let _renewal = self.authority_renewal.lock().await;
        let now = self.options.clock.now_millis()?;
        let permits = self
            .permits
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "permit lock poisoned"))?
            .clone();
        let authority = self.authority.clone();
        let results = stream::iter(permits)
            .map(|(scope, permit)| {
                let authority = authority.clone();
                async move { (scope, authority.renew(permit, now).await) }
            })
            .buffer_unordered(32)
            .collect::<Vec<_>>()
            .await;
        let mut first_error = None;
        for (scope, result) in results {
            match result {
                Ok(renewed) => self.install_permit(renewed)?,
                Err(error) => {
                    self.fence_scope(&scope)?;
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Run independent branch-authority renewal until the returned handle is
    /// dropped. A fenced shard is removed from renewal while healthy shards
    /// continue to make progress.
    pub fn start_shard_authority_maintenance(
        self: &Arc<Self>,
    ) -> Result<crate::ShardAuthorityMaintenance> {
        if !self.writable.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "shard-authority maintenance requires a writable repository",
            ));
        }
        let interval = Duration::from_millis((self.options.authority_lease_millis / 3).max(100));
        let weak = Arc::downgrade(self);
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(repository) = weak.upgrade() else {
                    break;
                };
                // Renewal failures fence only their branch. Keep the task alive
                // so independent branch shards do not lose their authorities.
                let _ = repository.renew_shard_authorities().await;
            }
        });
        Ok(crate::ShardAuthorityMaintenance::from_task(task))
    }

    pub fn fenced_branches(&self) -> Result<Vec<String>> {
        Ok(self
            .fenced_scopes
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "fenced-branch lock poisoned"))?
            .iter()
            .filter_map(|scope| match scope {
                AuthorityScope::Branch { name } => Some(name.clone()),
                AuthorityScope::System { .. } => None,
            })
            .collect())
    }

    async fn reconcile_operation(
        &self,
        branch: &str,
        operation: OperationId,
        input_digest: [u8; 32],
        now: u64,
    ) -> Result<Option<CommitReceipt>> {
        let Some(indexed) = self
            .operation_index
            .lookup(&self.publisher, branch, operation, now)
            .await?
        else {
            return Ok(None);
        };
        let commit = self.load_commit_object(indexed.target).await?.commit;
        if commit.delta.input_digest != input_digest {
            return Err(Error::new(
                ErrorCode::IdempotencyConflict,
                "repository operation ID was reused with different input",
            )
            .operation(operation.to_string()));
        }
        Ok(Some(CommitReceipt {
            id: indexed.target,
            operation,
            branch: branch.to_string(),
            parents: commit.parents,
            changed_keys: commit.delta.logical_change_count(),
            object_versions: commit
                .delta
                .changes
                .iter()
                .map(|change| change.next)
                .collect(),
            idempotent_replay: true,
        }))
    }

    async fn active_permit(&self, branch: &str, now: u64) -> Result<AuthorityPermit> {
        self.active_scope_permit(
            AuthorityScope::Branch {
                name: branch.to_string(),
            },
            now,
        )
        .await
    }

    async fn active_system_permit(&self, namespace: &str, now: u64) -> Result<AuthorityPermit> {
        self.active_scope_permit(
            AuthorityScope::System {
                namespace: namespace.to_string(),
            },
            now,
        )
        .await
    }

    async fn active_scope_permit(
        &self,
        scope: AuthorityScope,
        now: u64,
    ) -> Result<AuthorityPermit> {
        if !self.writable.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "repository is read-only",
            ));
        }
        if self.is_scope_fenced(&scope)? {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "repository branch authority is fenced in this repository instance",
            ));
        }
        // Renewal changes the mutable object's storage token before the
        // renewed permit can be installed locally. Serialize the complete
        // cached-permit read and remote validation with renewal so foreground
        // operations can never observe that transient stale-token window and
        // fence an otherwise healthy authority epoch.
        let _renewal = self.authority_renewal.lock().await;
        if self.is_scope_fenced(&scope)? {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "repository branch authority is fenced in this repository instance",
            ));
        }
        let cached = self
            .permits
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "permit lock poisoned"))?
            .get(&scope)
            .cloned();
        let permit = if let Some(permit) = cached.filter(|permit| permit.expires_at_millis() > now)
        {
            permit
        } else {
            let current = self
                .permits
                .read()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "permit lock poisoned"))?
                .get(&scope)
                .cloned();
            let acquired = match current {
                Some(permit) if permit.expires_at_millis() > now => permit,
                Some(_) => {
                    self.fence_scope(&scope)?;
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "repository branch authority expired; explicit takeover is required",
                    ));
                }
                None => match self
                    .authority
                    .acquire(
                        scope.clone(),
                        &self.options.writer,
                        now,
                        self.options.ids.operation(),
                    )
                    .await
                {
                    Ok(permit) => permit,
                    Err(error) => {
                        self.fence_scope(&scope)?;
                        return Err(error);
                    }
                },
            };
            self.install_permit(acquired.clone())?;
            acquired
        };
        match self.authority.validate_active(&permit, now).await {
            Ok(_) => Ok(permit),
            Err(error) => {
                self.fence_scope(&scope)?;
                Err(error)
            }
        }
    }

    fn install_permit(&self, permit: AuthorityPermit) -> Result<()> {
        let scope = permit.stamp().scope;
        self.permits
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "permit lock poisoned"))?
            .insert(scope.clone(), permit);
        self.fenced_scopes
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "fenced-branch lock poisoned"))?
            .remove(&scope);
        Ok(())
    }

    fn fence_branch(&self, branch: &str) -> Result<()> {
        self.fence_scope(&AuthorityScope::Branch {
            name: branch.to_string(),
        })
    }

    fn fence_scope(&self, scope: &AuthorityScope) -> Result<()> {
        self.permits
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "permit lock poisoned"))?
            .remove(scope);
        self.fenced_scopes
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "fenced-branch lock poisoned"))?
            .insert(scope.clone());
        Ok(())
    }

    fn is_scope_fenced(&self, scope: &AuthorityScope) -> Result<bool> {
        Ok(self
            .fenced_scopes
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "fenced-branch lock poisoned"))?
            .contains(scope))
    }

    async fn require_branch_indexes_ready(&self, branch: &str) -> Result<()> {
        let health = self.branch_index_health(branch).await?;
        Self::check_branch_index_health(health)
    }

    async fn require_branch_indexes_ready_for(
        &self,
        branch: &str,
        reference: &LoadedRef,
    ) -> Result<()> {
        let health = self.branch_index_health_for(branch, reference).await?;
        Self::check_branch_index_health(health)
    }

    fn check_branch_index_health(health: BranchIndexHealth) -> Result<()> {
        if health.ready {
            return Ok(());
        }
        Err(Error::new(
            ErrorCode::MissingClosure,
            format!(
                "repository branch indexes lag {} generation(s); background catch-up is required",
                health.lag_generations
            ),
        )
        .retry(crate::RetryAdvice::After(Duration::from_millis(250))))
    }

    fn mark_local_index_head(&self, branch: &str, target: CommitId) -> Result<()> {
        self.local_index_heads
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "local-index lock poisoned"))?
            .insert(branch.to_string(), target);
        Ok(())
    }

    async fn record_branch_catalog(&self, reference: &LoadedRef) -> Result<()> {
        self.ref_catalog
            .record(
                RefKind::Branch,
                &reference.value.inline_reflog.branch,
                reference.value.target,
                reference.value.generation,
                reference.value.operation,
                reference.value.tombstone,
                reference.value.updated_at_millis,
            )
            .await?;
        Ok(())
    }

    async fn record_tag_catalog(&self, name: &str, value: &crate::TagValue) -> Result<()> {
        self.ref_catalog
            .record(
                RefKind::Tag,
                name,
                value.target,
                value.generation,
                value.operation,
                value.tombstone,
                value.updated_at_millis,
            )
            .await?;
        Ok(())
    }

    async fn load_commit_object(&self, id: CommitId) -> Result<CommitObject> {
        let object = self.publisher.load_commit_object(id).await?;
        let encoded = object.encode_object()?;
        self.node_store
            .register_commit_object(id, &object, &encoded)?;
        Ok(object)
    }

    async fn load_commit_metadata(&self, id: CommitId) -> Result<Arc<BucketCommit>> {
        if let Some(commit) = self
            .commit_metadata_cache
            .lock()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "commit cache lock poisoned"))?
            .get(id)
        {
            return Ok(commit);
        }

        // A commit is immutable. Serializing misses prevents a cold burst for the
        // same branch head from downloading and decoding the same large metadata
        // envelope once per request; the second lookup lets waiters share it.
        let _fetch = self.commit_metadata_fetch.lock().await;
        if let Some(commit) = self
            .commit_metadata_cache
            .lock()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "commit cache lock poisoned"))?
            .get(id)
        {
            return Ok(commit);
        }
        let commit = Arc::new(self.publisher.load_commit(id).await?);
        let encoded_bytes = encode_canonical(commit.as_ref())?.len();
        self.commit_metadata_cache
            .lock()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "commit cache lock poisoned"))?
            .insert(id, commit.clone(), encoded_bytes);
        Ok(commit)
    }

    /// Administrative traversal fallback for commits that may not exist in a
    /// live branch's journal-derived node index (notably GC history closure).
    async fn load_commit_object_with_pack(&self, id: CommitId) -> Result<CommitObject> {
        self.load_commit_object(id).await
    }

    async fn finalize_pack(
        &self,
        id: CommitId,
        commit: &BucketCommit,
        prepared: Option<PreparedNodePack>,
    ) -> Result<()> {
        let Some(prepared) = prepared else {
            return Ok(());
        };
        let object = CommitObject::new(commit.clone(), Some(prepared.pack().clone()))?;
        let encoded = object.encode_object()?;
        let offset = CommitObject::node_payload_offset(&encoded)?.ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "prepared node pack has no payload offset",
            )
        })?;
        self.node_store.commit_node_pack(id, prepared, offset).await
    }

    fn engine(&self, store: ProllyObjectStore<P>) -> AsyncProlly<ProllyObjectStore<P>> {
        AsyncProlly::new(
            store,
            Config {
                format: self.format.state_tree_format.clone(),
                runtime: RuntimeConfig::default(),
            },
        )
    }

    fn tree_from_root(&self, root: &RootManifest) -> Result<Tree> {
        if root.format_digest != tree_format_digest(&self.format.state_tree_format)? {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "repository state root uses another tree format",
            ));
        }
        Ok(Tree {
            root: root.root.clone(),
            config: Config {
                format: self.format.state_tree_format.clone(),
                runtime: RuntimeConfig::default(),
            },
        })
    }

    async fn verify_payload_metadata(
        &self,
        version: &ObjectVersion,
        deep: bool,
    ) -> Result<(u64, u64, u64)> {
        let LogicalObjectVersionKind::Live { size, .. } = &version.body.kind else {
            return Ok((0, 0, 0));
        };
        let binding = version.binding.as_ref().ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "live object version has no payload binding",
            )
        })?;
        if binding.path != self.payloads.expected_path(binding)? {
            return Err(Error::new(
                ErrorCode::CorruptContent,
                "payload binding path does not match its content checksum",
            ));
        }
        let metadata =
            self.plane.head(&binding.path).await?.ok_or_else(|| {
                Error::new(ErrorCode::MissingClosure, "immutable payload is missing")
            })?;
        let expected_physical_len = binding
            .pack_range
            .map(|(_, end)| end.saturating_add(1))
            .unwrap_or(*size);
        if metadata.len < expected_physical_len
            || (!binding.is_packed() && metadata.len != *size)
            || metadata.sha256 != binding.physical_checksum_sha256()
            || metadata.token.etag != binding.provider_etag
            || metadata.token.version_id != binding.provider_version_id
        {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "immutable payload metadata does not match its logical binding",
            ));
        }
        let deep_bytes = if deep {
            u64::try_from(self.payloads.get(binding).await?.len())
                .map_err(|_| Error::new(ErrorCode::EntityTooLarge, "payload length exceeds u64"))?
        } else {
            0
        };
        Ok((1, *size, deep_bytes))
    }

    async fn gc_drain_publication_tickets(
        &self,
        continuation: Option<String>,
        max_steps: usize,
    ) -> Result<(usize, Option<String>, bool)> {
        let page = self
            .plane
            .list(ListRequest {
                prefix: gc_publication_ticket_prefix(
                    &self.options.repository_prefix,
                    self.format.repository_id,
                ),
                continuation: continuation.clone(),
                limit: max_steps.min(1_000),
                include_versions: false,
            })
            .await?;
        let now = self.options.clock.now_millis()?;
        let mut processed = 0usize;
        for entry in page.entries {
            let Some(stored) = self
                .plane
                .get(GetRequest {
                    path: entry.path.clone(),
                    range: None,
                    physical_version: None,
                })
                .await?
            else {
                continue;
            };
            let ticket: GcPublicationTicket = decode_canonical(&stored.bytes)?;
            if ticket.repository != self.format.repository_id {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "publication ticket belongs to another repository",
                ));
            }
            if ticket.expires_at_millis > now {
                return Ok((processed, continuation, false));
            }
            let version = stored.metadata.token.version_id.clone().map_or_else(
                || PhysicalVersion::Unversioned {
                    token: Some(stored.metadata.token.clone()),
                },
                |version_id| PhysicalVersion::Versioned { version_id },
            );
            match self.plane.delete_exact(&entry.path, version).await? {
                DeleteOutcome::Deleted | DeleteOutcome::NotFound => {}
                DeleteOutcome::TokenMismatch => {
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "expired publication ticket changed during maintenance admission",
                    ));
                }
            }
            processed = processed.saturating_add(1);
        }
        match page.continuation {
            Some(next) => Ok((processed, Some(next), false)),
            None => Ok((processed, None, true)),
        }
    }

    fn gc_work_engine(&self, epoch: OperationId) -> Result<AsyncProlly<ProllyObjectStore<P>>> {
        if epoch.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "GC epoch ID is nil",
            ));
        }
        Ok(self.engine(ProllyObjectStore::new(
            self.plane.clone(),
            format!(
                "{}/administration/gc/{epoch}/tree",
                self.options.repository_prefix
            ),
        )))
    }

    fn payload_pack_stats_engine(
        &self,
        job: OperationId,
    ) -> Result<AsyncProlly<ProllyObjectStore<P>>> {
        if job.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "payload-pack stats job ID is nil",
            ));
        }
        Ok(self.engine(ProllyObjectStore::new(
            self.plane.clone(),
            format!(
                "{}/administration/payload-pack-stats/{job}/tree",
                self.options.repository_prefix
            ),
        )))
    }

    fn validate_gc_cursor(&self, cursor: &GcCursor) -> Result<()> {
        if cursor.repository != self.format.repository_id
            || cursor.epoch.is_nil()
            || cursor.work.format_digest != tree_format_digest(&self.format.state_tree_format)?
            || *self
                .active_gc_epoch
                .read()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "active GC lock poisoned"))?
                != Some(cursor.epoch)
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "GC cursor is malformed or is not the active epoch",
            ));
        }
        self.tree_from_root(&cursor.work)?;
        Ok(())
    }

    async fn gc_discover_refs(
        &self,
        cursor: &mut GcCursor,
        tags: bool,
        max_steps: usize,
    ) -> Result<usize> {
        let prefix = format!(
            "{}/refs/{}/",
            self.options.repository_prefix,
            if tags { "tags" } else { "heads" }
        );
        let page = self
            .plane
            .list(ListRequest {
                prefix,
                continuation: cursor.continuation.clone(),
                limit: max_steps,
                include_versions: false,
            })
            .await?;
        let engine = self.gc_work_engine(cursor.epoch)?;
        let mut tree = self.tree_from_root(&cursor.work)?;
        let mut mutations = Vec::new();
        for entry in &page.entries {
            let Some(stored) = self.plane.load_mutable(&entry.path).await? else {
                continue;
            };
            let (target, previous, tombstone) = if tags {
                let value: crate::TagValue = decode_canonical(&stored.bytes)?;
                value.validate(self.format.repository_id, &value.inline_reflog.branch)?;
                (value.target, value.previous_target, value.tombstone)
            } else {
                let value: crate::RefValue = decode_canonical(&stored.bytes)?;
                value.validate(self.format.repository_id, &value.inline_reflog.branch)?;
                self.locator.register(&value.inline_reflog.branch)?;
                (value.target, value.previous_target, value.tombstone)
            };
            if !tombstone {
                for root in std::iter::once(target).chain(previous) {
                    mutations.push(Mutation::Upsert {
                        key: gc_commit_queue_key(root),
                        val: root.as_bytes().to_vec(),
                    });
                    cursor.report.roots = cursor.report.roots.saturating_add(1);
                }
            }
        }
        if !mutations.is_empty() {
            tree = engine.batch(&tree, mutations).await?;
            cursor.work = RootManifest::from_tree(&tree)?;
        }
        cursor.continuation = page.continuation;
        if cursor.continuation.is_none() {
            cursor.phase = if tags {
                GcPhase::MarkCommits
            } else {
                GcPhase::DiscoverTags
            };
        }
        Ok(page.entries.len())
    }

    async fn gc_mark_commits(&self, cursor: &mut GcCursor, max_steps: usize) -> Result<usize> {
        let engine = self.gc_work_engine(cursor.epoch)?;
        let mut tree = self.tree_from_root(&cursor.work)?;
        let mut processed = 0usize;
        while processed < max_steps {
            let mut queue = engine.prefix(&tree, b"cq/").await?;
            let Some(entry) = queue.next().await else {
                cursor.phase = GcPhase::MarkNodes;
                break;
            };
            let (queue_key, encoded) = entry?;
            drop(queue);
            let hash: [u8; 32] = encoded.try_into().map_err(|_| {
                Error::new(
                    ErrorCode::CorruptCommit,
                    "GC commit work ID has wrong length",
                )
            })?;
            let id = CommitId::from_hash(hash);
            let mark_key = gc_commit_mark_key(id);
            let mut mutations = vec![Mutation::Delete { key: queue_key }];
            if engine.get(&tree, &mark_key).await?.is_none() {
                let commit = self.load_commit_object_with_pack(id).await?.commit;
                mutations.push(Mutation::Upsert {
                    key: mark_key,
                    val: Vec::new(),
                });
                mutations.push(Mutation::Upsert {
                    key: gc_path_mark_key(&commit_path(&self.options.repository_prefix, id)?),
                    val: Vec::new(),
                });
                for parent in commit.parents {
                    mutations.push(Mutation::Upsert {
                        key: gc_commit_queue_key(parent),
                        val: parent.as_bytes().to_vec(),
                    });
                }
                if let Some(root) = commit.state.objects.root {
                    mutations.push(Mutation::Upsert {
                        key: gc_node_queue_key(&root, false),
                        val: encode_canonical(&GcNodeWork {
                            cid: root,
                            scan_versions: false,
                        })?,
                    });
                }
                if let Some(root) = commit.state.versions.root {
                    mutations.push(Mutation::Upsert {
                        key: gc_node_queue_key(&root, true),
                        val: encode_canonical(&GcNodeWork {
                            cid: root,
                            scan_versions: true,
                        })?,
                    });
                }
                cursor.report.commits = cursor.report.commits.saturating_add(1);
            }
            tree = engine.batch(&tree, mutations).await?;
            processed += 1;
        }
        cursor.work = RootManifest::from_tree(&tree)?;
        Ok(processed)
    }

    async fn gc_mark_nodes(&self, cursor: &mut GcCursor, max_steps: usize) -> Result<usize> {
        let engine = self.gc_work_engine(cursor.epoch)?;
        let mut tree = self.tree_from_root(&cursor.work)?;
        let mut processed = 0usize;
        while processed < max_steps {
            let mut queue = engine.prefix(&tree, b"nq/").await?;
            let Some(entry) = queue.next().await else {
                cursor.phase = if cursor.initial_scan_complete {
                    GcPhase::CatchUpDirtyRoots
                } else {
                    GcPhase::ScanCandidates
                };
                break;
            };
            let (queue_key, encoded) = entry?;
            drop(queue);
            let work: GcNodeWork = decode_canonical(&encoded)?;
            let mark_key = gc_node_mark_key(&work.cid, work.scan_versions);
            let mut mutations = vec![Mutation::Delete { key: queue_key }];
            if engine.get(&tree, &mark_key).await?.is_none() {
                mutations.push(Mutation::Upsert {
                    key: mark_key,
                    val: Vec::new(),
                });
                mutations.push(Mutation::Upsert {
                    key: gc_path_mark_key(&self.node_store.direct_node_path(&work.cid)?),
                    val: Vec::new(),
                });
                let bytes = self
                    .node_store
                    .get(work.cid.as_bytes())
                    .await?
                    .ok_or_else(|| {
                        Error::new(ErrorCode::MissingClosure, "reachable node is missing")
                    })?;
                let node = Node::from_bytes_with_format(&bytes, &self.format.state_tree_format)
                    .map_err(|error| {
                        Error::new(
                            ErrorCode::CorruptNode,
                            format!("reachable node could not be decoded: {error}"),
                        )
                    })?;
                if node.leaf {
                    if work.scan_versions {
                        for encoded in node.vals {
                            let version: ObjectVersion = decode_canonical(&encoded)?;
                            version.validate()?;
                            if let Some(binding) = version.binding {
                                mutations.push(Mutation::Upsert {
                                    key: binding.provider_version_id.as_ref().map_or_else(
                                        || gc_path_mark_key(&binding.path),
                                        |version| gc_physical_mark_key(&binding.path, version),
                                    ),
                                    val: Vec::new(),
                                });
                            }
                            cursor.report.logical_versions =
                                cursor.report.logical_versions.saturating_add(1);
                        }
                    }
                } else {
                    for child in node.vals {
                        let cid = prolly::Cid(child.try_into().map_err(|_| {
                            Error::new(ErrorCode::CorruptNode, "internal node child CID is invalid")
                        })?);
                        mutations.push(Mutation::Upsert {
                            key: gc_node_queue_key(&cid, work.scan_versions),
                            val: encode_canonical(&GcNodeWork {
                                cid,
                                scan_versions: work.scan_versions,
                            })?,
                        });
                    }
                }
                cursor.report.nodes = cursor.report.nodes.saturating_add(1);
            }
            tree = engine.batch(&tree, mutations).await?;
            processed += 1;
        }
        cursor.work = RootManifest::from_tree(&tree)?;
        Ok(processed)
    }

    async fn gc_scan_candidates(&self, cursor: &mut GcCursor, max_steps: usize) -> Result<usize> {
        let page = self
            .plane
            .list(ListRequest {
                prefix: format!("{}/", self.options.repository_prefix),
                continuation: cursor.continuation.clone(),
                limit: max_steps,
                include_versions: true,
            })
            .await?;
        let engine = self.gc_work_engine(cursor.epoch)?;
        let mut tree = self.tree_from_root(&cursor.work)?;
        for entry in &page.entries {
            let Some(kind) = gc_managed_kind(&self.options.repository_prefix, &entry.path) else {
                continue;
            };
            if entry.metadata.last_modified_millis > cursor.cutoff_millis {
                continue;
            }
            let path_retained = engine
                .get(&tree, &gc_path_mark_key(&entry.path))
                .await?
                .is_some();
            let physical_retained = match entry.metadata.token.version_id.as_deref() {
                Some(version) => engine
                    .get(&tree, &gc_physical_mark_key(&entry.path, version))
                    .await?
                    .is_some(),
                None => false,
            };
            let retained = path_retained || physical_retained;
            if retained {
                continue;
            }
            let physical_version = entry
                .metadata
                .token
                .version_id
                .clone()
                .map(|version_id| PhysicalVersion::Versioned { version_id })
                .unwrap_or_else(|| PhysicalVersion::Unversioned {
                    token: Some(entry.metadata.token.clone()),
                });
            let candidate = GcCandidate {
                path: entry.path.clone(),
                physical_version,
                len: entry.metadata.len,
                last_modified_millis: entry.metadata.last_modified_millis,
                kind: kind.to_string(),
            };
            let key = gc_candidate_key(&candidate)?;
            if engine.get(&tree, &key).await?.is_none() {
                tree = engine
                    .put(&tree, key, encode_canonical(&candidate)?)
                    .await?;
                cursor.report.candidates = cursor.report.candidates.saturating_add(1);
                cursor.report.candidate_bytes =
                    cursor.report.candidate_bytes.saturating_add(candidate.len);
                *cursor
                    .report
                    .candidates_by_kind
                    .entry(kind.to_string())
                    .or_default() += 1;
            }
        }
        cursor.work = RootManifest::from_tree(&tree)?;
        cursor.continuation = page.continuation;
        if cursor.continuation.is_none() {
            cursor.initial_scan_complete = true;
            cursor.phase = GcPhase::CatchUpDirtyRoots;
        }
        Ok(page.entries.len())
    }

    async fn gc_catch_up_dirty_roots(
        &self,
        cursor: &mut GcCursor,
        max_steps: usize,
    ) -> Result<usize> {
        if cursor.dirty_target_sequence == 0 {
            let _barrier = self.gc_publication_barrier.write().await;
            cursor.dirty_target_sequence = self.gc_dirty_sequence.load(Ordering::Acquire);
        }
        let engine = self.gc_work_engine(cursor.epoch)?;
        let mut tree = self.tree_from_root(&cursor.work)?;
        let mut processed = 0usize;
        while processed < max_steps && cursor.dirty_sequence < cursor.dirty_target_sequence {
            let sequence = cursor.dirty_sequence.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "GC dirty sequence overflow")
            })?;
            let page = self
                .plane
                .list(ListRequest {
                    prefix: gc_dirty_root_sequence_prefix(
                        &self.options.repository_prefix,
                        cursor.epoch,
                        sequence,
                    ),
                    continuation: None,
                    limit: 2,
                    include_versions: false,
                })
                .await?;
            for listed in page.entries {
                let stored = self
                    .plane
                    .get(GetRequest {
                        path: listed.path,
                        range: None,
                        physical_version: None,
                    })
                    .await?
                    .ok_or_else(|| {
                        Error::new(ErrorCode::MissingClosure, "GC dirty-root event disappeared")
                    })?;
                let event: GcDirtyRoot = decode_canonical(&stored.bytes)?;
                if event.repository != self.format.repository_id
                    || event.epoch != cursor.epoch
                    || event.sequence != sequence
                {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "GC dirty-root event does not match its path",
                    ));
                }
                for root in std::iter::once(event.target).chain(event.previous_target) {
                    tree = engine
                        .put(&tree, gc_commit_queue_key(root), root.as_bytes().to_vec())
                        .await?;
                    cursor.report.dirty_roots = cursor.report.dirty_roots.saturating_add(1);
                }
            }
            cursor.dirty_sequence = sequence;
            processed += 1;
        }
        cursor.work = RootManifest::from_tree(&tree)?;
        if engine.prefix(&tree, b"cq/").await?.next().await.is_some() {
            cursor.phase = GcPhase::MarkCommits;
        } else if cursor.dirty_sequence >= cursor.dirty_target_sequence {
            cursor.phase = GcPhase::Ready;
            cursor.dirty_target_sequence = 0;
        }
        Ok(processed)
    }

    async fn gc_cleanup(&self, cursor: &mut GcCursor, max_steps: usize) -> Result<usize> {
        self.publish_gc_coordinator(None, self.options.clock.now_millis()?)
            .await?;
        let page = self
            .plane
            .list(ListRequest {
                prefix: gc_dirty_root_prefix(&self.options.repository_prefix, cursor.epoch),
                continuation: None,
                limit: max_steps,
                include_versions: false,
            })
            .await?;
        let targets = page
            .entries
            .into_iter()
            .map(|entry| {
                let token = entry.metadata.token;
                let physical = token
                    .version_id
                    .clone()
                    .map(|version_id| PhysicalVersion::Versioned { version_id })
                    .unwrap_or_else(|| PhysicalVersion::Unversioned { token: Some(token) });
                (entry.path, physical)
            })
            .collect::<Vec<_>>();
        let processed = targets.len();
        for outcome in self.plane.delete_exact_batch(targets).await? {
            if outcome == DeleteOutcome::TokenMismatch {
                return Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "GC dirty-root event changed during cleanup",
                ));
            }
        }
        if processed == 0 {
            cursor.phase = GcPhase::Complete;
            *self.active_gc_epoch.write().map_err(|_| {
                Error::new(ErrorCode::InternalInvariant, "active GC lock poisoned")
            })? = None;
        }
        Ok(processed)
    }

    async fn publish_gc_coordinator(
        &self,
        active_epoch: Option<OperationId>,
        now_millis: u64,
    ) -> Result<()> {
        let path = gc_coordinator_path(&self.options.repository_prefix)?;
        let current = self.plane.load_mutable(&path).await?;
        let (expected, generation, current_epoch) = match current {
            Some(stored) => {
                let value: GcCoordinator = decode_canonical(&stored.bytes)?;
                if value.repository != self.format.repository_id || value.generation == 0 {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "GC coordinator is malformed",
                    ));
                }
                (
                    Some(stored.metadata.token),
                    value.generation.checked_add(1).ok_or_else(|| {
                        Error::new(ErrorCode::InternalInvariant, "GC coordinator overflow")
                    })?,
                    value.active_epoch,
                )
            }
            None => (None, 1, None),
        };
        if active_epoch.is_some() && current_epoch.is_some() {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "another GC epoch is active",
            ));
        }
        if active_epoch.is_none()
            && current_epoch.is_some()
            && current_epoch
                != *self.active_gc_epoch.read().map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "active GC lock poisoned")
                })?
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "GC coordinator belongs to another epoch",
            ));
        }
        let value = GcCoordinator {
            repository: self.format.repository_id,
            generation,
            active_epoch,
            admission_closed: active_epoch.is_some(),
            updated_at_millis: now_millis,
        };
        let controls = MutableControlStore::new(
            self.plane.clone(),
            self.options.repository_prefix.clone(),
            self.options.mutable_control_versions_to_retain,
        )?;
        match controls
            .compare_exchange(CompareExchange {
                path,
                expected,
                bytes: encode_canonical(&value)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => Ok(()),
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::RefConflict,
                "GC coordinator changed concurrently",
            )),
        }
    }

    async fn restore_gc_state(&self) -> Result<()> {
        let active = match self
            .plane
            .load_mutable(&gc_coordinator_path(&self.options.repository_prefix)?)
            .await?
        {
            Some(stored) => {
                let value: GcCoordinator = decode_canonical(&stored.bytes)?;
                if value.repository != self.format.repository_id || value.generation == 0 {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "GC coordinator is malformed",
                    ));
                }
                value.active_epoch
            }
            None => None,
        };
        if active.is_some() {
            *self.active_gc_epoch.write().map_err(|_| {
                Error::new(ErrorCode::InternalInvariant, "active GC lock poisoned")
            })? = active;
        }
        if let Some(epoch) = active {
            let mut continuation = None;
            let mut maximum = 0_u64;
            loop {
                let page = self
                    .plane
                    .list(ListRequest {
                        prefix: gc_dirty_root_prefix(&self.options.repository_prefix, epoch),
                        continuation,
                        limit: 1_000,
                        include_versions: false,
                    })
                    .await?;
                for entry in &page.entries {
                    let sequence = entry
                        .path
                        .as_str()
                        .rsplit('/')
                        .nth(2)
                        .and_then(|value| value.parse::<u64>().ok())
                        .ok_or_else(|| {
                            Error::new(ErrorCode::CorruptCommit, "GC dirty-root path is malformed")
                        })?;
                    maximum = maximum.max(sequence);
                }
                continuation = page.continuation;
                if continuation.is_none() {
                    break;
                }
            }
            self.gc_dirty_sequence.fetch_max(maximum, Ordering::AcqRel);
        }
        Ok(())
    }

    fn commit_closure_engine(
        &self,
        traversal: OperationId,
    ) -> Result<AsyncProlly<ProllyObjectStore<P>>> {
        if traversal.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "commit-closure traversal ID is nil",
            ));
        }
        Ok(self.engine(ProllyObjectStore::new(
            self.plane.clone(),
            format!(
                "{}/administration/closure/{traversal}/tree",
                self.options.repository_prefix
            ),
        )))
    }

    fn history_transfer_mapping_engine(
        &self,
        job: OperationId,
    ) -> Result<AsyncProlly<ProllyObjectStore<P>>> {
        if job.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "history-transfer job ID is nil",
            ));
        }
        Ok(self.engine(ProllyObjectStore::new(
            self.plane.clone(),
            format!(
                "{}/administration/transfer/{job}/mappings",
                self.options.repository_prefix
            ),
        )))
    }

    fn history_transfer_delta_engine(
        &self,
        job: OperationId,
    ) -> Result<AsyncProlly<ProllyObjectStore<P>>> {
        if job.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "history-transfer job ID is nil",
            ));
        }
        Ok(self.engine(ProllyObjectStore::new(
            self.plane.clone(),
            format!(
                "{}/administration/transfer/{job}/delta",
                self.options.repository_prefix
            ),
        )))
    }

    fn validate_history_transfer_cursor<Q: ObjectPlane>(
        &self,
        source: &Repository<Q>,
        cursor: &HistoryTransferCursor,
    ) -> Result<()> {
        validate_branch(&cursor.source_branch)?;
        validate_branch(&cursor.destination_branch)?;
        source.validate_commit_closure_cursor(&cursor.closure)?;
        if cursor.source_repository != source.format.repository_id
            || cursor.destination_repository != self.format.repository_id
            || cursor.job.is_nil()
            || cursor.mappings.format_digest != tree_format_digest(&self.format.state_tree_format)?
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "history-transfer cursor belongs to another repository pair",
            ));
        }
        if let Some(pending) = &cursor.pending {
            self.tree_from_root(&pending.objects)?;
            self.tree_from_root(&pending.versions)?;
            self.tree_from_root(&pending.delta)?;
            if let Some(union_base) = &pending.union_base {
                self.tree_from_root(union_base)?;
            }
        }
        Ok(())
    }

    async fn advance_history_transfer_union(
        &self,
        cursor: &mut HistoryTransferCursor,
        max_steps: usize,
    ) -> Result<usize> {
        let mut pending = cursor.pending.take().ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "transfer pending commit is absent",
            )
        })?;
        if pending.union_parent_index >= pending.mapped_parents.len() {
            pending.phase = HistoryTransferPhase::ApplyTransitions;
            cursor.pending = Some(pending);
            return Ok(0);
        }
        let parent = self
            .load_commit_object(pending.mapped_parents[pending.union_parent_index])
            .await?
            .commit;
        let current_tree = self.tree_from_root(&pending.versions)?;
        let union_base = pending
            .union_base
            .get_or_insert_with(|| pending.versions.clone())
            .clone();
        let base_tree = self.tree_from_root(&union_base)?;
        let parent_tree = self.tree_from_root(&parent.state.versions)?;
        let read_engine = self.engine(self.node_store.clone());
        let page = read_engine
            .structural_diff_page(
                &base_tree,
                &parent_tree,
                pending.union_diff.as_ref(),
                max_steps,
            )
            .await?;
        let processed = page.diffs.len();
        let mut mutations = Vec::new();
        for diff in page.diffs {
            match diff {
                Diff::Added { key, val } => mutations.push(Mutation::Upsert { key, val }),
                Diff::Removed { .. } => {}
                Diff::Changed { .. } => {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "mapped parent version trees disagree on an immutable version key",
                    ));
                }
            }
        }
        let mut versions = current_tree;
        if !mutations.is_empty() {
            versions = self
                .merge_state_engine()
                .batch(&versions, mutations)
                .await?;
            pending.versions = RootManifest::from_tree(&versions)?;
        }
        pending.union_diff = page.next_cursor;
        if pending.union_diff.is_none() {
            pending.union_parent_index += 1;
            pending.union_base = None;
            if pending.union_parent_index >= pending.mapped_parents.len() {
                pending.phase = HistoryTransferPhase::ApplyTransitions;
            }
        }
        cursor.pending = Some(pending);
        Ok(processed)
    }

    async fn advance_history_transfer_transitions<Q: ObjectPlane>(
        &self,
        source: &Repository<Q>,
        cursor: &mut HistoryTransferCursor,
        max_steps: usize,
    ) -> Result<usize> {
        let mut pending = cursor.pending.take().ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "transfer pending commit is absent",
            )
        })?;
        let source_commit = source.load_commit_object(pending.source).await?.commit;
        let mut transitions = Vec::new();
        let mut complete = false;
        if let Some(root) = &source_commit.delta.changes_root {
            let tree = source.tree_from_root(root)?;
            let engine = source.engine(source.node_store.clone());
            let mut entries = match pending.external_after.as_deref() {
                Some(after) => engine.range_after(&tree, after, None).await?,
                None => engine.prefix(&tree, b"").await?,
            };
            while transitions.len() < max_steps {
                let Some(entry) = entries.next().await else {
                    complete = true;
                    break;
                };
                let (key, encoded) = entry?;
                let transition: ObjectTransition = decode_canonical(&encoded)?;
                if transition.key != key {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "external transfer delta key disagrees with its transition",
                    ));
                }
                pending.external_after = Some(key);
                transitions.push(transition);
            }
        } else {
            let start = pending.inline_index;
            let end = start
                .saturating_add(max_steps)
                .min(source_commit.delta.changes.len());
            transitions.extend_from_slice(&source_commit.delta.changes[start..end]);
            pending.inline_index = end;
            complete = end == source_commit.delta.changes.len();
        }
        let processed = transitions.len();
        cursor.pending = Some(pending);
        for transition in transitions {
            self.import_history_transition(source, cursor, &source_commit, transition)
                .await?;
        }
        let pending = cursor.pending.as_mut().ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "transfer pending commit disappeared",
            )
        })?;
        if complete {
            if pending.transitions_applied != source_commit.delta.logical_change_count() {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "history transfer did not consume the source commit delta exactly",
                ));
            }
            pending.phase = HistoryTransferPhase::FinalizeCommit;
        }
        Ok(processed)
    }

    async fn import_history_transition<Q: ObjectPlane>(
        &self,
        source: &Repository<Q>,
        cursor: &mut HistoryTransferCursor,
        source_commit: &BucketCommit,
        transition: ObjectTransition,
    ) -> Result<()> {
        let mut pending = cursor.pending.take().ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "transfer pending commit is absent",
            )
        })?;
        let ordinal = u32::try_from(pending.transitions_applied).map_err(|_| {
            Error::new(
                ErrorCode::InvalidLimit,
                "history-transfer mutation ordinal exceeds u32",
            )
        })?;
        let source_version = if transition.delete_marker {
            let versions = source.tree_from_root(&source_commit.state.versions)?;
            let encoded = source
                .engine(source.node_store.clone())
                .get(
                    &versions,
                    &version_tree_key(
                        &transition.key,
                        ObjectVersionOrder {
                            commit_generation: source_commit.generation,
                            mutation_ordinal: ordinal,
                        },
                        transition.next,
                    ),
                )
                .await?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::MissingClosure,
                        "source delete version is missing from its commit",
                    )
                })?;
            decode_canonical::<ObjectVersion>(&encoded)?
        } else {
            let objects = source.tree_from_root(&source_commit.state.objects)?;
            let encoded = source
                .engine(source.node_store.clone())
                .get(&objects, &transition.key)
                .await?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::MissingClosure,
                        "source live version is missing from its commit",
                    )
                })?;
            decode_canonical::<CurrentObject>(&encoded)?.version
        };
        source_version.validate()?;
        if source_version.id != transition.next {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "source transition and object version disagree",
            ));
        }
        let mapping_engine = self.history_transfer_mapping_engine(cursor.job)?;
        let mut mapping_tree = self.tree_from_root(&cursor.mappings)?;
        let mapped_version = if let Some(encoded) = mapping_engine
            .get(&mapping_tree, &version_mapping_key(source_version.id))
            .await?
        {
            let version: ObjectVersion = decode_canonical(&encoded)?;
            version.validate()?;
            version
        } else {
            let binding = match &source_version.body.kind {
                LogicalObjectVersionKind::Live { size, .. } => {
                    let source_binding = source_version.binding.as_ref().ok_or_else(|| {
                        Error::new(
                            ErrorCode::CorruptCommit,
                            "source live version has no payload binding",
                        )
                    })?;
                    let bytes = source.payloads.get(source_binding).await?;
                    if bytes.len() as u64 != *size {
                        return Err(Error::new(
                            ErrorCode::ChecksumMismatch,
                            "source payload length disagrees with its logical version",
                        ));
                    }
                    cursor.report.copied_payloads = checked_fsck_add(
                        cursor.report.copied_payloads,
                        1,
                        "history-transfer-payload",
                    )?;
                    cursor.report.copied_payload_bytes = checked_fsck_add(
                        cursor.report.copied_payload_bytes,
                        *size,
                        "history-transfer-payload-byte",
                    )?;
                    Some(self.payloads.put(bytes).await?)
                }
                LogicalObjectVersionKind::DeleteMarker => None,
            };
            let version = ObjectVersion::derive(
                self.format.repository_id,
                &transition.key,
                history_transfer_version_operation(self.format.repository_id, source_version.id),
                source_version.body.clone(),
                binding,
            )?;
            mapping_tree = mapping_engine
                .put(
                    &mapping_tree,
                    version_mapping_key(source_version.id),
                    encode_canonical(&version)?,
                )
                .await?;
            cursor.mappings = RootManifest::from_tree(&mapping_tree)?;
            cursor.report.rebound_versions = checked_fsck_add(
                cursor.report.rebound_versions,
                1,
                "history-transfer-version",
            )?;
            version
        };
        let state_engine = self.merge_state_engine();
        let mut objects = self.tree_from_root(&pending.objects)?;
        let mut versions = self.tree_from_root(&pending.versions)?;
        let previous = state_engine
            .get(&objects, &transition.key)
            .await?
            .map(|encoded| decode_canonical::<CurrentObject>(&encoded))
            .transpose()?
            .map(|current| current.version.id);
        if transition.delete_marker {
            objects = state_engine.delete(&objects, &transition.key).await?;
        } else {
            objects = state_engine
                .put(
                    &objects,
                    transition.key.clone(),
                    encode_canonical(&CurrentObject {
                        version: mapped_version.clone(),
                    })?,
                )
                .await?;
        }
        versions = state_engine
            .put(
                &versions,
                version_tree_key(
                    &transition.key,
                    mapped_version.body.order,
                    mapped_version.id,
                ),
                encode_canonical(&mapped_version)?,
            )
            .await?;
        let mapped_transition = ObjectTransition {
            key: transition.key.clone(),
            previous,
            next: mapped_version.id,
            delete_marker: transition.delete_marker,
        };
        let delta_engine = self.history_transfer_delta_engine(cursor.job)?;
        let delta = delta_engine
            .put(
                &self.tree_from_root(&pending.delta)?,
                transition.key,
                encode_canonical(&mapped_transition)?,
            )
            .await?;
        pending.objects = RootManifest::from_tree(&objects)?;
        pending.versions = RootManifest::from_tree(&versions)?;
        pending.delta = RootManifest::from_tree(&delta)?;
        pending.transitions_applied =
            pending.transitions_applied.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::EntityTooLarge,
                    "history-transfer transition counter overflow",
                )
            })?;
        cursor.pending = Some(pending);
        Ok(())
    }

    async fn finalize_history_transfer_commit<Q: ObjectPlane>(
        &self,
        source: &Repository<Q>,
        cursor: &mut HistoryTransferCursor,
    ) -> Result<()> {
        let pending = cursor.pending.take().ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "transfer pending commit is absent",
            )
        })?;
        let source_commit = source.load_commit_object(pending.source).await?.commit;
        let now = self.options.clock.now_millis()?;
        let permit = self.active_permit(&cursor.destination_branch, now).await?;
        self.authority.validate_active(&permit, now).await?;
        let expected_generation = if pending.mapped_parents.is_empty() {
            0
        } else {
            let mut newest = 0_u64;
            for parent in &pending.mapped_parents {
                newest = newest.max(self.load_commit_object(*parent).await?.commit.generation.0);
            }
            newest.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "transfer generation overflow")
            })?
        };
        if source_commit.generation.0 != expected_generation {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "source commit generation does not match its mapped parents",
            ));
        }
        let mut metadata = source_commit.metadata.clone();
        metadata.insert(
            "prolly.transfer.source_repository".to_string(),
            source.format.repository_id.as_bytes().to_vec(),
        );
        metadata.insert(
            "prolly.transfer.source_commit".to_string(),
            pending.source.as_bytes().to_vec(),
        );
        let commit = BucketCommit {
            state: BucketState {
                objects: pending.objects,
                versions: pending.versions,
            },
            parents: pending.mapped_parents,
            generation: source_commit.generation,
            delta: BucketDelta {
                input_digest: crate::model::derive_input_digest(&[
                    b"history-transfer",
                    source.format.repository_id.as_bytes(),
                    pending.source.as_bytes(),
                    self.format.repository_id.as_bytes(),
                ]),
                changes: Vec::new(),
                changes_root: (pending.transitions_applied != 0).then_some(pending.delta),
                change_count: pending.transitions_applied,
            },
            node_pack: None,
            authority: permit.stamp(),
            author: self.options.writer.clone(),
            message: source_commit.message,
            created_at_millis: source_commit.created_at_millis,
            metadata,
        };
        let destination = self.publisher.store_commit(&commit, None).await?;
        let mapping_engine = self.history_transfer_mapping_engine(cursor.job)?;
        let mapping_tree = mapping_engine
            .put(
                &self.tree_from_root(&cursor.mappings)?,
                commit_mapping_key(pending.source),
                destination.as_bytes().to_vec(),
            )
            .await?;
        cursor.mappings = RootManifest::from_tree(&mapping_tree)?;
        cursor.closure = pending.next_closure;
        cursor.report.imported_commits =
            checked_fsck_add(cursor.report.imported_commits, 1, "history-transfer-commit")?;
        if pending.source == cursor.source_head {
            cursor.mapped_head = Some(destination);
        }
        Ok(())
    }

    fn validate_commit_closure_cursor(&self, cursor: &CommitClosureCursor) -> Result<()> {
        if cursor.repository != self.format.repository_id
            || cursor.traversal.is_nil()
            || cursor.state.format_digest != tree_format_digest(&self.format.state_tree_format)?
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "commit-closure cursor is malformed or belongs to another repository",
            ));
        }
        Ok(())
    }

    fn merge_plan_engine(&self, job: OperationId) -> Result<AsyncProlly<ProllyObjectStore<P>>> {
        if job.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "merge job ID is nil",
            ));
        }
        Ok(self.engine(ProllyObjectStore::new_cached_direct(
            self.plane.clone(),
            format!(
                "{}/administration/merge/{job}/plan",
                self.options.repository_prefix
            ),
            self.format.repository_id,
            tree_format_digest(&self.format.state_tree_format)?,
            self.node_cache.clone(),
        )))
    }

    fn merge_state_engine(&self) -> AsyncProlly<ProllyObjectStore<P>> {
        self.engine(self.node_store.durable_direct_write_session())
    }

    fn tree_from_merge_root(&self, root: &RootManifest) -> Result<Tree> {
        self.tree_from_root(root).map_err(|_| {
            Error::new(
                ErrorCode::InvalidContinuationToken,
                "merge cursor uses another tree format",
            )
        })
    }

    async fn validate_merge_cursor(&self, cursor: &MergeCursor) -> Result<()> {
        crate::repository::validate_branch(&cursor.target_branch)?;
        crate::repository::validate_branch(&cursor.source_branch)?;
        self.locator.register(&cursor.target_branch)?;
        self.locator.register(&cursor.source_branch)?;
        self.require_branch_indexes_ready(&cursor.target_branch)
            .await?;
        self.require_branch_indexes_ready(&cursor.source_branch)
            .await?;
        self.tree_from_merge_root(&cursor.plan_root)?;
        if cursor.repository != self.format.repository_id
            || cursor.job.is_nil()
            || cursor.operation.is_nil()
            || cursor.target_branch == cursor.source_branch
            || cursor.message.trim().is_empty()
            || cursor.best_base_count == 0
                && !matches!(
                    cursor.phase,
                    MergePhase::DiscoveringBases | MergePhase::CollectingBases
                )
            || cursor.selected_base.is_none()
                && matches!(
                    cursor.phase,
                    MergePhase::Planning
                        | MergePhase::BuildingVersions
                        | MergePhase::BuildingObjects
                        | MergePhase::Conflicted
                        | MergePhase::ReadyToPublish
                )
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "merge cursor is malformed or belongs to another repository",
            ));
        }
        let engine = self.merge_plan_engine(cursor.job)?;
        let tree = self.tree_from_merge_root(&cursor.plan_root)?;
        let stored = engine
            .get(&tree, MERGE_CURSOR_KEY)
            .await?
            .map(|bytes| decode_canonical::<MergeCursor>(&bytes))
            .transpose()?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidContinuationToken,
                    "merge cursor is not anchored by its durable plan",
                )
            })?;
        if normalized_merge_cursor(&stored)? != normalized_merge_cursor(cursor)? {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "merge cursor state disagrees with its durable plan",
            ));
        }
        Ok(())
    }

    async fn seal_merge_cursor(&self, cursor: &mut MergeCursor) -> Result<()> {
        let engine = self.merge_plan_engine(cursor.job)?;
        let mut tree = self.tree_from_merge_root(&cursor.plan_root)?;
        tree = engine
            .batch(
                &tree,
                vec![Mutation::Upsert {
                    key: MERGE_CURSOR_KEY.to_vec(),
                    val: normalized_merge_cursor(cursor)?,
                }],
            )
            .await?;
        cursor.plan_root = RootManifest::from_tree(&tree)?;
        Ok(())
    }

    fn validate_requested_merge_base(
        &self,
        cursor: &MergeCursor,
        discovered: CommitId,
    ) -> Result<()> {
        if cursor
            .requested_base
            .is_some_and(|requested| requested != discovered)
        {
            return Err(Error::new(
                ErrorCode::InvalidRevision,
                "requested merge base is not a best common ancestor",
            ));
        }
        Ok(())
    }

    async fn merge_graph_entry(
        &self,
        target_branch: &str,
        source_branch: &str,
        commit: CommitId,
    ) -> Result<JournalCommitGraphEntry> {
        if let Some(entry) = self
            .journal_indexes
            .commit_graph_entry(target_branch, commit)
            .await?
        {
            return Ok(entry);
        }
        if let Some(entry) = self
            .journal_indexes
            .commit_graph_entry(source_branch, commit)
            .await?
        {
            return Ok(entry);
        }
        let commit_object = self.load_commit_metadata(commit).await?;
        Ok(JournalCommitGraphEntry {
            commit,
            generation: commit_object.generation,
            parents: commit_object.parents.clone(),
            first_parent_jumps: Vec::new(),
        })
    }

    async fn is_first_parent_ancestor(
        &self,
        target_branch: &str,
        source_branch: &str,
        ancestor: CommitId,
        mut descendant: CommitId,
    ) -> Result<bool> {
        let ancestor_entry = self
            .merge_graph_entry(target_branch, source_branch, ancestor)
            .await?;
        let target_generation = ancestor_entry.generation.0;
        loop {
            if descendant == ancestor {
                return Ok(true);
            }
            let entry = self
                .merge_graph_entry(target_branch, source_branch, descendant)
                .await?;
            if entry.generation.0 <= target_generation {
                return Ok(false);
            }
            let mut selected = entry.parents.first().copied();
            for jump in entry.first_parent_jumps.iter().rev().copied() {
                let jump_entry = self
                    .merge_graph_entry(target_branch, source_branch, jump)
                    .await?;
                if jump_entry.generation.0 >= target_generation {
                    selected = Some(jump);
                    break;
                }
            }
            let Some(next) = selected else {
                return Ok(false);
            };
            descendant = next;
        }
    }

    fn seed_merge_frontier(
        &self,
        mutations: &mut Vec<Mutation>,
        entry: JournalCommitGraphEntry,
        flags: u8,
    ) -> Result<()> {
        let seen_key = merge_seen_key(entry.commit);
        let queue_key = merge_queue_key(entry.generation.0, entry.commit);
        mutations.push(Mutation::Upsert {
            key: seen_key,
            val: encode_canonical(&MergeSeenEntry {
                generation: entry.generation.0,
                flags,
            })?,
        });
        mutations.push(Mutation::Upsert {
            key: queue_key,
            val: encode_canonical(&MergeQueueEntry {
                commit: entry.commit,
                generation: entry.generation.0,
            })?,
        });
        Ok(())
    }

    async fn advance_merge_base_one(&self, cursor: &mut MergeCursor) -> Result<usize> {
        let engine = self.merge_plan_engine(cursor.job)?;
        let mut tree = self.tree_from_merge_root(&cursor.plan_root)?;
        let mut queue = engine.prefix(&tree, MERGE_QUEUE_PREFIX).await?;
        let Some(entry) = queue.next().await else {
            cursor.phase = MergePhase::CollectingBases;
            return Ok(0);
        };
        let (queue_key, encoded) = entry?;
        let queued: MergeQueueEntry = decode_canonical(&encoded)?;
        let seen_key = merge_seen_key(queued.commit);
        let seen: MergeSeenEntry = engine
            .get(&tree, &seen_key)
            .await?
            .map(|bytes| decode_canonical(&bytes))
            .transpose()?
            .ok_or_else(|| {
                Error::new(ErrorCode::CorruptCommit, "merge queue has no seen record")
            })?;
        if seen.generation != queued.generation {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "merge queue generation disagrees with seen state",
            ));
        }
        let candidate_key = merge_base_candidate_key(queued.commit);
        let candidate = engine
            .get(&tree, &candidate_key)
            .await?
            .map(|bytes| decode_canonical::<MergeBaseCandidate>(&bytes))
            .transpose()?;
        let is_common = seen.flags & MERGE_BOTH == MERGE_BOTH;
        let is_stale = seen.flags & MERGE_STALE != 0;
        let mut mutations = vec![Mutation::Delete { key: queue_key }];
        if is_common && !is_stale && candidate.is_none() {
            mutations.push(Mutation::Upsert {
                key: candidate_key.clone(),
                val: encode_canonical(&MergeBaseCandidate {
                    generation: seen.generation,
                    stale: false,
                })?,
            });
        } else if is_stale && candidate.as_ref().is_some_and(|candidate| !candidate.stale) {
            mutations.push(Mutation::Upsert {
                key: candidate_key,
                val: encode_canonical(&MergeBaseCandidate {
                    generation: seen.generation,
                    stale: true,
                })?,
            });
        }
        let propagated = if is_common {
            seen.flags | MERGE_STALE
        } else {
            seen.flags
        };
        let graph = self
            .merge_graph_entry(&cursor.target_branch, &cursor.source_branch, queued.commit)
            .await?;
        for parent in graph.parents {
            let parent_graph = self
                .merge_graph_entry(&cursor.target_branch, &cursor.source_branch, parent)
                .await?;
            let parent_seen_key = merge_seen_key(parent);
            let previous = engine
                .get(&tree, &parent_seen_key)
                .await?
                .map(|bytes| decode_canonical::<MergeSeenEntry>(&bytes))
                .transpose()?;
            let next_flags = previous
                .as_ref()
                .map_or(propagated, |entry| entry.flags | propagated);
            if previous
                .as_ref()
                .is_some_and(|entry| entry.flags == next_flags)
            {
                continue;
            }
            mutations.push(Mutation::Upsert {
                key: parent_seen_key,
                val: encode_canonical(&MergeSeenEntry {
                    generation: parent_graph.generation.0,
                    flags: next_flags,
                })?,
            });
            mutations.push(Mutation::Upsert {
                key: merge_queue_key(parent_graph.generation.0, parent),
                val: encode_canonical(&MergeQueueEntry {
                    commit: parent,
                    generation: parent_graph.generation.0,
                })?,
            });
            if next_flags & MERGE_STALE != 0 {
                let key = merge_base_candidate_key(parent);
                if let Some(candidate) = engine
                    .get(&tree, &key)
                    .await?
                    .map(|bytes| decode_canonical::<MergeBaseCandidate>(&bytes))
                    .transpose()?
                {
                    if !candidate.stale {
                        mutations.push(Mutation::Upsert {
                            key,
                            val: encode_canonical(&MergeBaseCandidate {
                                generation: candidate.generation,
                                stale: true,
                            })?,
                        });
                    }
                }
            }
        }
        tree = engine.batch(&tree, mutations).await?;
        cursor.plan_root = RootManifest::from_tree(&tree)?;
        cursor.visited_commits = cursor.visited_commits.checked_add(1).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "merge visited count overflow")
        })?;
        Ok(1)
    }

    async fn collect_merge_base_one(&self, cursor: &mut MergeCursor) -> Result<usize> {
        let engine = self.merge_plan_engine(cursor.job)?;
        let mut tree = self.tree_from_merge_root(&cursor.plan_root)?;
        let mut candidates = engine.prefix(&tree, MERGE_BASE_CANDIDATE_PREFIX).await?;
        let Some(entry) = candidates.next().await else {
            if cursor.best_base_count == 0 {
                return Err(Error::new(
                    ErrorCode::NoMergeBase,
                    "commits have no common ancestor",
                ));
            }
            if let Some(requested) = cursor.requested_base {
                if engine
                    .get(&tree, &merge_base_result_key(requested))
                    .await?
                    .is_none()
                {
                    return Err(Error::new(
                        ErrorCode::InvalidRevision,
                        "requested merge base is not a best common ancestor",
                    ));
                }
                cursor.selected_base = Some(requested);
                cursor.phase = MergePhase::Planning;
            } else if cursor.best_base_count == 1 {
                let mut bases = engine.prefix(&tree, MERGE_BASE_RESULT_PREFIX).await?;
                let (key, _) = bases.next().await.ok_or_else(|| {
                    Error::new(ErrorCode::CorruptCommit, "best-base result is absent")
                })??;
                cursor.selected_base = Some(commit_from_suffix(&key, MERGE_BASE_RESULT_PREFIX)?);
                cursor.phase = MergePhase::Planning;
            } else {
                cursor.phase = MergePhase::AwaitingBase;
            }
            return Ok(0);
        };
        let (key, encoded) = entry?;
        let candidate: MergeBaseCandidate = decode_canonical(&encoded)?;
        let commit = commit_from_suffix(&key, MERGE_BASE_CANDIDATE_PREFIX)?;
        let mut mutations = vec![Mutation::Delete { key }];
        if !candidate.stale {
            mutations.push(Mutation::Upsert {
                key: merge_base_result_key(commit),
                val: Vec::new(),
            });
            cursor.best_base_count = cursor.best_base_count.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "best-base count overflow")
            })?;
        }
        tree = engine.batch(&tree, mutations).await?;
        cursor.plan_root = RootManifest::from_tree(&tree)?;
        Ok(1)
    }

    async fn advance_merge_plan(
        &self,
        cursor: &mut MergeCursor,
        max_steps: usize,
        emitted_changes: &mut Vec<MergeChange>,
        emitted_conflicts: &mut Vec<MergeConflict>,
    ) -> Result<usize> {
        let base = cursor.selected_base.ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "merge plan has no selected base",
            )
        })?;
        let base_commit = self.load_commit_metadata(base).await?;
        let ours_commit = self.load_commit_metadata(cursor.ours).await?;
        let theirs_commit = self.load_commit_metadata(cursor.theirs).await?;
        let base_tree = self.tree_from_root(&base_commit.state.objects)?;
        let ours_tree = self.tree_from_root(&ours_commit.state.objects)?;
        let theirs_tree = self.tree_from_root(&theirs_commit.state.objects)?;
        let state_engine = self.engine(self.node_store.clone());
        let plan_engine = self.merge_plan_engine(cursor.job)?;
        let mut plan_tree = self.tree_from_merge_root(&cursor.plan_root)?;
        let mut processed = 0usize;
        let mut page_mutations = Vec::new();
        let mut ours_buffer = VecDeque::new();
        let mut theirs_buffer = VecDeque::new();
        if let Some(pending) = cursor.ours_pending.take() {
            ours_buffer.push_back(pending);
        }
        if let Some(pending) = cursor.theirs_pending.take() {
            theirs_buffer.push_back(pending);
        }
        if !cursor.ours_finished {
            let page = state_engine
                .structural_diff_page(&base_tree, &ours_tree, cursor.ours_diff.as_ref(), max_steps)
                .await?;
            ours_buffer.extend(page.diffs);
            cursor.ours_diff = page.next_cursor;
        }
        if !cursor.theirs_finished {
            let page = state_engine
                .structural_diff_page(
                    &base_tree,
                    &theirs_tree,
                    cursor.theirs_diff.as_ref(),
                    max_steps,
                )
                .await?;
            theirs_buffer.extend(page.diffs);
            cursor.theirs_diff = page.next_cursor;
        }
        while processed < max_steps {
            let key_order = match (ours_buffer.front(), theirs_buffer.front()) {
                (None, None) => break,
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some(ours), Some(theirs)) => ours.key().cmp(theirs.key()),
            };
            let (key, base_value, ours_value, theirs_value) = match key_order {
                std::cmp::Ordering::Less => {
                    let ours = ours_buffer.pop_front().expect("matched pending ours");
                    let (key, base, ours) = merge_diff_values(ours);
                    (key, base.clone(), ours, base)
                }
                std::cmp::Ordering::Greater => {
                    let theirs = theirs_buffer.pop_front().expect("matched pending theirs");
                    let (key, base, theirs) = merge_diff_values(theirs);
                    (key, base.clone(), base, theirs)
                }
                std::cmp::Ordering::Equal => {
                    let ours = ours_buffer.pop_front().expect("matched pending ours");
                    let theirs = theirs_buffer.pop_front().expect("matched pending theirs");
                    let (key, ours_base, ours_value) = merge_diff_values(ours);
                    let (theirs_key, theirs_base, theirs_value) = merge_diff_values(theirs);
                    if key != theirs_key || ours_base != theirs_base {
                        return Err(Error::new(
                            ErrorCode::CorruptCommit,
                            "structural merge streams disagree on their base value",
                        ));
                    }
                    (key, ours_base, ours_value, theirs_value)
                }
            };
            let conflict = ours_value != theirs_value
                && ours_value != base_value
                && theirs_value != base_value;
            let selected = if ours_value == theirs_value {
                ours_value.clone()
            } else if ours_value == base_value {
                theirs_value.clone()
            } else if theirs_value == base_value {
                ours_value.clone()
            } else {
                match cursor.policy {
                    MergePolicy::Fail | MergePolicy::Ours => ours_value.clone(),
                    MergePolicy::Theirs => theirs_value.clone(),
                }
            };
            let record = MergePlanEntry {
                key: key.clone(),
                base: base_value.clone(),
                ours: ours_value.clone(),
                theirs: theirs_value.clone(),
                selected: selected.clone(),
                conflict,
            };
            if selected != ours_value {
                page_mutations.push(Mutation::Upsert {
                    key: merge_change_key(&key),
                    val: encode_canonical(&record)?,
                });
                let change = merge_change_from_record(&record)?;
                emitted_changes.push(change);
                cursor.planned_changes =
                    cursor.planned_changes.checked_add(1).ok_or_else(|| {
                        Error::new(ErrorCode::InternalInvariant, "merge change count overflow")
                    })?;
            }
            if conflict {
                page_mutations.push(Mutation::Upsert {
                    key: merge_conflict_key(&key),
                    val: encode_canonical(&record)?,
                });
                let conflict = merge_conflict_from_record(&record)?;
                emitted_conflicts.push(conflict);
                cursor.conflicts = cursor.conflicts.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "merge conflict count overflow",
                    )
                })?;
            }
            processed += 1;
        }
        cursor.ours_diff = structural_cursor_with_pending(
            cursor.ours_diff.take(),
            &base_tree,
            &ours_tree,
            ours_buffer.into(),
        );
        cursor.theirs_diff = structural_cursor_with_pending(
            cursor.theirs_diff.take(),
            &base_tree,
            &theirs_tree,
            theirs_buffer.into(),
        );
        cursor.ours_finished = cursor.ours_diff.is_none();
        cursor.theirs_finished = cursor.theirs_diff.is_none();
        if !page_mutations.is_empty() {
            plan_tree = plan_engine.batch(&plan_tree, page_mutations).await?;
            cursor.plan_root = RootManifest::from_tree(&plan_tree)?;
        }
        if cursor.ours_pending.is_none()
            && cursor.theirs_pending.is_none()
            && cursor.ours_finished
            && cursor.theirs_finished
        {
            if cursor.policy == MergePolicy::Fail && cursor.conflicts != 0 {
                cursor.phase = MergePhase::Conflicted;
            } else {
                cursor.final_objects = Some(ours_commit.state.objects.clone());
                cursor.final_versions = Some(ours_commit.state.versions.clone());
                let empty_delta = self.merge_state_engine().create();
                cursor.delta_root = Some(RootManifest::from_tree(&empty_delta)?);
                cursor.phase = MergePhase::BuildingVersions;
            }
        }
        Ok(processed)
    }

    async fn advance_merge_version_union(
        &self,
        cursor: &mut MergeCursor,
        max_steps: usize,
    ) -> Result<usize> {
        let ours_commit = self.load_commit_metadata(cursor.ours).await?;
        let theirs_commit = self.load_commit_metadata(cursor.theirs).await?;
        let ours_versions = self.tree_from_root(&ours_commit.state.versions)?;
        let theirs_versions = self.tree_from_root(&theirs_commit.state.versions)?;
        let read_engine = self.engine(self.node_store.clone());
        let page = read_engine
            .structural_diff_page(
                &ours_versions,
                &theirs_versions,
                cursor.version_diff.as_ref(),
                max_steps,
            )
            .await?;
        let processed = page.diffs.len();
        let mut mutations = Vec::new();
        for diff in page.diffs {
            match diff {
                Diff::Added { key, val } => mutations.push(Mutation::Upsert { key, val }),
                Diff::Removed { .. } => {}
                Diff::Changed { .. } => {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "same version-tree key has unequal immutable values",
                    ))
                }
            }
        }
        if !mutations.is_empty() {
            let state_engine = self.merge_state_engine();
            let versions =
                self.tree_from_root(cursor.final_versions.as_ref().ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "merge version root is absent")
                })?)?;
            let versions = state_engine.batch(&versions, mutations).await?;
            cursor.final_versions = Some(RootManifest::from_tree(&versions)?);
        }
        cursor.version_diff = page.next_cursor;
        if cursor.version_diff.is_none() {
            cursor.version_diff_finished = true;
            cursor.phase = MergePhase::BuildingObjects;
            cursor.build_after = None;
        }
        Ok(processed)
    }

    async fn advance_merge_object_build(
        &self,
        cursor: &mut MergeCursor,
        max_steps: usize,
    ) -> Result<usize> {
        let plan_engine = self.merge_plan_engine(cursor.job)?;
        let plan_tree = self.tree_from_merge_root(&cursor.plan_root)?;
        let end = prolly::prefix_range(MERGE_CHANGE_PREFIX).1;
        let mut entries = match &cursor.build_after {
            Some(after) => {
                plan_engine
                    .range_after(&plan_tree, after, end.as_deref())
                    .await?
            }
            None => plan_engine.prefix(&plan_tree, MERGE_CHANGE_PREFIX).await?,
        };
        let mut records = Vec::with_capacity(max_steps);
        while records.len() < max_steps {
            let Some(entry) = entries.next().await else {
                break;
            };
            let (key, encoded) = entry?;
            if !key.starts_with(MERGE_CHANGE_PREFIX) {
                break;
            }
            let record: MergePlanEntry = decode_canonical(&encoded)?;
            if merge_change_key(&record.key) != key {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "merge-plan change key disagrees with its record",
                ));
            }
            records.push((key, record));
        }
        if records.is_empty() {
            if cursor.built_changes != cursor.planned_changes {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "merge build did not consume every planned change",
                ));
            }
            if cursor.built_changes == 0 {
                cursor.delta_root = None;
            }
            cursor.phase = MergePhase::ReadyToPublish;
            return Ok(0);
        }
        let ours = self.load_commit_metadata(cursor.ours).await?;
        let theirs = self.load_commit_metadata(cursor.theirs).await?;
        let generation = CommitGeneration(
            ours.generation
                .0
                .max(theirs.generation.0)
                .checked_add(1)
                .ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "merge generation overflow")
                })?,
        );
        let mut object_mutations = Vec::with_capacity(records.len());
        let mut version_mutations = Vec::new();
        let mut delta_mutations = Vec::with_capacity(records.len());
        for (_, record) in &records {
            let previous = current_id(record.ours.as_deref())?;
            let (next, delete_marker) = if let Some(selected) = &record.selected {
                let current: CurrentObject = decode_canonical(selected)?;
                current.version.validate()?;
                object_mutations.push(Mutation::Upsert {
                    key: record.key.clone(),
                    val: selected.clone(),
                });
                (current.version.id, false)
            } else {
                object_mutations.push(Mutation::Delete {
                    key: record.key.clone(),
                });
                let ordinal = u32::try_from(cursor.built_changes + delta_mutations.len() as u64)
                    .map_err(|_| {
                        Error::new(ErrorCode::InvalidLimit, "merge delete ordinal exceeds u32")
                    })?;
                let version = ObjectVersion::derive(
                    self.format.repository_id,
                    &record.key,
                    cursor.operation,
                    LogicalObjectVersionBody {
                        order: ObjectVersionOrder {
                            commit_generation: generation,
                            mutation_ordinal: ordinal,
                        },
                        created_at_millis: cursor.created_at_millis,
                        kind: LogicalObjectVersionKind::DeleteMarker,
                    },
                    None,
                )?;
                version_mutations.push(Mutation::Upsert {
                    key: version_tree_key(&record.key, version.body.order, version.id),
                    val: encode_canonical(&version)?,
                });
                (version.id, true)
            };
            let transition = ObjectTransition {
                key: record.key.clone(),
                previous,
                next,
                delete_marker,
            };
            delta_mutations.push(Mutation::Upsert {
                key: record.key.clone(),
                val: encode_canonical(&transition)?,
            });
        }
        let state_engine = self.merge_state_engine();
        let objects = self.tree_from_root(cursor.final_objects.as_ref().ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "merge object root is absent")
        })?)?;
        let versions = self.tree_from_root(cursor.final_versions.as_ref().ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "merge version root is absent")
        })?)?;
        let delta = self.tree_from_root(cursor.delta_root.as_ref().ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "merge delta root is absent")
        })?)?;
        let objects = state_engine.batch(&objects, object_mutations).await?;
        let versions = if version_mutations.is_empty() {
            versions
        } else {
            state_engine.batch(&versions, version_mutations).await?
        };
        let delta = state_engine.batch(&delta, delta_mutations).await?;
        cursor.final_objects = Some(RootManifest::from_tree(&objects)?);
        cursor.final_versions = Some(RootManifest::from_tree(&versions)?);
        cursor.delta_root = Some(RootManifest::from_tree(&delta)?);
        cursor.built_changes = cursor
            .built_changes
            .checked_add(records.len() as u64)
            .ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "merge build count overflow")
            })?;
        cursor.build_after = records.last().map(|(key, _)| key.clone());
        Ok(records.len())
    }

    async fn merge_base_page(
        &self,
        cursor: &MergeCursor,
        continuation: Option<&MergeBaseCursor>,
        limit: usize,
    ) -> Result<MergeBasePage> {
        self.validate_merge_cursor(cursor).await?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "merge-base page limit must be greater than zero",
            ));
        }
        if continuation.is_some_and(|continuation| {
            continuation.repository != cursor.repository
                || continuation.job != cursor.job
                || continuation.plan_root != cursor.plan_root
                || !continuation.after.starts_with(MERGE_BASE_RESULT_PREFIX)
        }) {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "merge-base cursor belongs to another plan",
            ));
        }
        let engine = self.merge_plan_engine(cursor.job)?;
        let tree = self.tree_from_merge_root(&cursor.plan_root)?;
        let end = prolly::prefix_range(MERGE_BASE_RESULT_PREFIX).1;
        let mut iter = match continuation {
            Some(continuation) => {
                engine
                    .range_after(&tree, &continuation.after, end.as_deref())
                    .await?
            }
            None => engine.prefix(&tree, MERGE_BASE_RESULT_PREFIX).await?,
        };
        let mut bases = Vec::with_capacity(limit);
        let mut last = None;
        while bases.len() < limit {
            let Some(entry) = iter.next().await else {
                break;
            };
            let (key, _) = entry?;
            if !key.starts_with(MERGE_BASE_RESULT_PREFIX) {
                break;
            }
            bases.push(commit_from_suffix(&key, MERGE_BASE_RESULT_PREFIX)?);
            last = Some(key);
        }
        Ok(MergeBasePage {
            continuation: (bases.len() == limit).then(|| MergeBaseCursor {
                repository: cursor.repository,
                job: cursor.job,
                plan_root: cursor.plan_root.clone(),
                after: last.expect("full page has a last merge base"),
            }),
            bases,
        })
    }

    async fn merge_change_page(
        &self,
        cursor: &MergeCursor,
        continuation: Option<&MergeChangeCursor>,
        limit: usize,
    ) -> Result<MergeChangePage> {
        self.validate_merge_cursor(cursor).await?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "merge change page limit must be greater than zero",
            ));
        }
        if continuation.is_some_and(|continuation| {
            continuation.repository != cursor.repository
                || continuation.job != cursor.job
                || continuation.plan_root != cursor.plan_root
                || !continuation.after.starts_with(MERGE_CHANGE_PREFIX)
        }) {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "merge change cursor belongs to another plan",
            ));
        }
        let engine = self.merge_plan_engine(cursor.job)?;
        let tree = self.tree_from_merge_root(&cursor.plan_root)?;
        let end = prolly::prefix_range(MERGE_CHANGE_PREFIX).1;
        let mut iter = match continuation {
            Some(continuation) => {
                engine
                    .range_after(&tree, &continuation.after, end.as_deref())
                    .await?
            }
            None => engine.prefix(&tree, MERGE_CHANGE_PREFIX).await?,
        };
        let mut changes = Vec::with_capacity(limit);
        let mut last = None;
        while changes.len() < limit {
            let Some(entry) = iter.next().await else {
                break;
            };
            let (key, encoded) = entry?;
            if !key.starts_with(MERGE_CHANGE_PREFIX) {
                break;
            }
            let record: MergePlanEntry = decode_canonical(&encoded)?;
            changes.push(merge_change_from_record(&record)?);
            last = Some(key);
        }
        Ok(MergeChangePage {
            continuation: (changes.len() == limit).then(|| MergeChangeCursor {
                repository: cursor.repository,
                job: cursor.job,
                plan_root: cursor.plan_root.clone(),
                after: last.expect("full page has a last merge change"),
            }),
            changes,
        })
    }

    async fn merge_conflict_page(
        &self,
        cursor: &MergeCursor,
        continuation: Option<&MergeConflictCursor>,
        limit: usize,
    ) -> Result<MergeConflictPage> {
        self.validate_merge_cursor(cursor).await?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "merge conflict page limit must be greater than zero",
            ));
        }
        if continuation.is_some_and(|continuation| {
            continuation.repository != cursor.repository
                || continuation.job != cursor.job
                || continuation.plan_root != cursor.plan_root
                || !continuation.after.starts_with(MERGE_CONFLICT_PREFIX)
        }) {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "merge conflict cursor belongs to another plan",
            ));
        }
        let engine = self.merge_plan_engine(cursor.job)?;
        let tree = self.tree_from_merge_root(&cursor.plan_root)?;
        let end = prolly::prefix_range(MERGE_CONFLICT_PREFIX).1;
        let mut iter = match continuation {
            Some(continuation) => {
                engine
                    .range_after(&tree, &continuation.after, end.as_deref())
                    .await?
            }
            None => engine.prefix(&tree, MERGE_CONFLICT_PREFIX).await?,
        };
        let mut conflicts = Vec::with_capacity(limit);
        let mut last = None;
        while conflicts.len() < limit {
            let Some(entry) = iter.next().await else {
                break;
            };
            let (key, encoded) = entry?;
            if !key.starts_with(MERGE_CONFLICT_PREFIX) {
                break;
            }
            let record: MergePlanEntry = decode_canonical(&encoded)?;
            conflicts.push(merge_conflict_from_record(&record)?);
            last = Some(key);
        }
        Ok(MergeConflictPage {
            continuation: (conflicts.len() == limit).then(|| MergeConflictCursor {
                repository: cursor.repository,
                job: cursor.job,
                plan_root: cursor.plan_root.clone(),
                after: last.expect("full page has a last merge conflict"),
            }),
            conflicts,
        })
    }

    fn merge_input_digest(&self, cursor: &MergeCursor, base: CommitId) -> [u8; 32] {
        let policy = match cursor.policy {
            MergePolicy::Fail => [0],
            MergePolicy::Ours => [1],
            MergePolicy::Theirs => [2],
        };
        crate::model::derive_input_digest(&[
            b"merge-",
            self.format.repository_id.as_bytes(),
            cursor.target_branch.as_bytes(),
            cursor.ours.as_bytes(),
            cursor.theirs.as_bytes(),
            base.as_bytes(),
            &policy,
        ])
    }

    async fn reconcile_merge_operation(
        &self,
        cursor: &MergeCursor,
        input_digest: [u8; 32],
        now: u64,
    ) -> Result<Option<MergeReceipt>> {
        let Some(indexed) = self
            .operation_index
            .lookup(
                &self.publisher,
                &cursor.target_branch,
                cursor.operation,
                now,
            )
            .await?
        else {
            return Ok(None);
        };
        let commit = self.load_commit_object(indexed.target).await?.commit;
        if commit.delta.input_digest != input_digest
            || commit.parents.as_slice() != [cursor.ours, cursor.theirs]
        {
            return Err(Error::new(
                ErrorCode::IdempotencyConflict,
                "merge operation ID was reused with different input",
            )
            .operation(cursor.operation.to_string()));
        }
        Ok(Some(MergeReceipt {
            id: indexed.target,
            operation: cursor.operation,
            branch: cursor.target_branch.clone(),
            parents: [cursor.ours, cursor.theirs],
            changed_keys: commit.delta.logical_change_count(),
            conflicts: cursor.conflicts,
            idempotent_replay: true,
        }))
    }

    fn validate_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty()
            || key.len() > self.format.canonical_limits.max_key_bytes as usize
            || std::str::from_utf8(key).is_err()
            || key.starts_with(self.options.repository_prefix.as_bytes())
        {
            return Err(Error::new(
                ErrorCode::InvalidKey,
                "logical key violates the repository key contract",
            ));
        }
        ObjectPath::new(std::str::from_utf8(key).expect("validated UTF-8"))?;
        Ok(())
    }

    async fn lock_branch(&self, branch: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lane = {
            let mut lanes = self
                .publication_lanes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            lanes.retain(|_, lane| lane.strong_count() > 0);
            if let Some(lane) = lanes.get(branch).and_then(Weak::upgrade) {
                lane
            } else {
                let lane = Arc::new(tokio::sync::Mutex::new(()));
                lanes.insert(branch.to_string(), Arc::downgrade(&lane));
                lane
            }
        };
        lane.lock_owned().await
    }

    async fn lock_index_branch(&self, branch: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lane = {
            let mut lanes = self
                .index_lanes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            lanes.retain(|_, lane| lane.strong_count() > 0);
            if let Some(lane) = lanes.get(branch).and_then(Weak::upgrade) {
                lane
            } else {
                let lane = Arc::new(tokio::sync::Mutex::new(()));
                lanes.insert(branch.to_string(), Arc::downgrade(&lane));
                lane
            }
        };
        lane.lock_owned().await
    }
}

const COMMIT_CLOSURE_QUEUE_PREFIX: &[u8] = b"q/";
const COMMIT_CLOSURE_SEEN_PREFIX: &[u8] = b"s/";
const MERGE_LEFT: u8 = 1;
const MERGE_RIGHT: u8 = 2;
const MERGE_BOTH: u8 = MERGE_LEFT | MERGE_RIGHT;
const MERGE_STALE: u8 = 4;
const MERGE_QUEUE_PREFIX: &[u8] = b"q/";
const MERGE_SEEN_PREFIX: &[u8] = b"s/";
const MERGE_BASE_CANDIDATE_PREFIX: &[u8] = b"b/";
const MERGE_BASE_RESULT_PREFIX: &[u8] = b"r/";
const MERGE_CHANGE_PREFIX: &[u8] = b"c/";
const MERGE_CONFLICT_PREFIX: &[u8] = b"f/";
const MERGE_CURSOR_KEY: &[u8] = b"x/cursor";

fn normalized_merge_cursor(cursor: &MergeCursor) -> Result<Vec<u8>> {
    let mut normalized = cursor.clone();
    normalized.plan_root.root = None;
    encode_canonical(&normalized)
}

fn commit_closure_stack_key(sequence: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(COMMIT_CLOSURE_QUEUE_PREFIX.len() + 8);
    key.extend_from_slice(COMMIT_CLOSURE_QUEUE_PREFIX);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn commit_closure_seen_key(commit: CommitId) -> Vec<u8> {
    let mut key = Vec::with_capacity(COMMIT_CLOSURE_SEEN_PREFIX.len() + 32);
    key.extend_from_slice(COMMIT_CLOSURE_SEEN_PREFIX);
    key.extend_from_slice(commit.as_bytes());
    key
}

fn push_commit_closure_work(
    cursor: &mut CommitClosureCursor,
    mutations: &mut Vec<Mutation>,
    commit: CommitId,
    finish: bool,
) -> Result<()> {
    mutations.push(Mutation::Upsert {
        key: commit_closure_stack_key(cursor.next_stack_sequence),
        val: encode_canonical(&CommitClosureWork { commit, finish })?,
    });
    cursor.next_stack_sequence = cursor.next_stack_sequence.checked_sub(1).ok_or_else(|| {
        Error::new(
            ErrorCode::HistoryLimitExceeded,
            "commit-closure stack sequence is exhausted",
        )
    })?;
    Ok(())
}

fn checked_fsck_add(current: u64, add: u64, counter: &str) -> Result<u64> {
    current.checked_add(add).ok_or_else(|| {
        Error::new(
            ErrorCode::EntityTooLarge,
            format!("fsck {counter} counter overflow"),
        )
    })
}

fn record_fsck_packed_payload(report: &mut FsckReport, version: &ObjectVersion) -> Result<()> {
    let (LogicalObjectVersionKind::Live { size, .. }, Some(binding)) =
        (&version.body.kind, version.binding.as_ref())
    else {
        return Ok(());
    };
    if !binding.is_packed() {
        return Ok(());
    }
    report.packed_payloads_verified =
        checked_fsck_add(report.packed_payloads_verified, 1, "packed-payload")?;
    report.packed_logical_bytes_verified = checked_fsck_add(
        report.packed_logical_bytes_verified,
        *size,
        "packed-logical-byte",
    )?;
    Ok(())
}

fn checked_pack_add(current: u64, add: u64, counter: &str) -> Result<u64> {
    current.checked_add(add).ok_or_else(|| {
        Error::new(
            ErrorCode::EntityTooLarge,
            format!("payload-pack {counter} counter overflow"),
        )
    })
}

fn payload_pack_physical_key(binding: &crate::PayloadBinding) -> Vec<u8> {
    let mut identity = binding.path.as_str().as_bytes().to_vec();
    identity.push(0);
    if let Some(version) = binding.provider_version_id.as_deref() {
        identity.extend_from_slice(version.as_bytes());
    } else {
        identity.extend_from_slice(binding.provider_etag.as_bytes());
    }
    let mut key = b"p/".to_vec();
    key.extend_from_slice(&crate::codec::sha256(&identity));
    key
}

fn payload_pack_extent_key(binding: &crate::PayloadBinding, start: u64, end: u64) -> Vec<u8> {
    let mut identity = payload_pack_physical_key(binding);
    identity.extend_from_slice(&start.to_be_bytes());
    identity.extend_from_slice(&end.to_be_bytes());
    let mut key = b"e/".to_vec();
    key.extend_from_slice(&crate::codec::sha256(&identity));
    key
}

fn history_transfer_version_operation(
    repository: crate::RepositoryId,
    source: ObjectVersionId,
) -> OperationId {
    let digest = crate::model::derive_input_digest(&[
        b"history-transfer-version",
        repository.as_bytes(),
        source.as_bytes(),
    ]);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    OperationId(uuid::Uuid::from_bytes(bytes))
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn merge_queue_key(generation: u64, commit: CommitId) -> Vec<u8> {
    let mut key = Vec::with_capacity(MERGE_QUEUE_PREFIX.len() + 8 + 32);
    key.extend_from_slice(MERGE_QUEUE_PREFIX);
    key.extend_from_slice(&(u64::MAX - generation).to_be_bytes());
    key.extend_from_slice(commit.as_bytes());
    key
}

fn merge_seen_key(commit: CommitId) -> Vec<u8> {
    merge_commit_key(MERGE_SEEN_PREFIX, commit)
}

fn merge_base_candidate_key(commit: CommitId) -> Vec<u8> {
    merge_commit_key(MERGE_BASE_CANDIDATE_PREFIX, commit)
}

fn merge_base_result_key(commit: CommitId) -> Vec<u8> {
    merge_commit_key(MERGE_BASE_RESULT_PREFIX, commit)
}

fn merge_commit_key(prefix: &[u8], commit: CommitId) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 32);
    key.extend_from_slice(prefix);
    key.extend_from_slice(commit.as_bytes());
    key
}

fn merge_change_key(logical_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(MERGE_CHANGE_PREFIX.len() + logical_key.len());
    key.extend_from_slice(MERGE_CHANGE_PREFIX);
    key.extend_from_slice(logical_key);
    key
}

fn merge_conflict_key(logical_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(MERGE_CONFLICT_PREFIX.len() + logical_key.len());
    key.extend_from_slice(MERGE_CONFLICT_PREFIX);
    key.extend_from_slice(logical_key);
    key
}

fn commit_from_suffix(key: &[u8], prefix: &[u8]) -> Result<CommitId> {
    let suffix = key.strip_prefix(prefix).ok_or_else(|| {
        Error::new(
            ErrorCode::CorruptCommit,
            "merge-state key uses the wrong namespace",
        )
    })?;
    let hash: [u8; 32] = suffix.try_into().map_err(|_| {
        Error::new(
            ErrorCode::CorruptCommit,
            "merge-state commit key has the wrong length",
        )
    })?;
    Ok(CommitId::from_hash(hash))
}

fn merge_diff_values(diff: Diff) -> (Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>) {
    match diff {
        Diff::Added { key, val } => (key, None, Some(val)),
        Diff::Removed { key, val } => (key, Some(val), None),
        Diff::Changed { key, old, new } => (key, Some(old), Some(new)),
    }
}

fn structural_cursor_with_pending(
    cursor: Option<prolly::StructuralDiffCursor>,
    base: &Tree,
    other: &Tree,
    pending: Vec<Diff>,
) -> Option<prolly::StructuralDiffCursor> {
    if pending.is_empty() {
        return cursor;
    }
    let mut cursor = cursor.unwrap_or_else(|| prolly::StructuralDiffCursor {
        base_root: base.root.clone(),
        other_root: other.root.clone(),
        markers: Vec::new(),
        pending: Vec::new(),
    });
    let mut combined = pending;
    combined.append(&mut cursor.pending);
    cursor.pending = combined;
    Some(cursor)
}

fn object_diff_from_prolly(diff: Diff) -> Result<ObjectDiff> {
    let (key, from, to) = merge_diff_values(diff);
    Ok(ObjectDiff {
        key,
        from: current_id(from.as_deref())?,
        to: current_id(to.as_deref())?,
    })
}

fn current_id(value: Option<&[u8]>) -> Result<Option<ObjectVersionId>> {
    value
        .map(|value| {
            let current: CurrentObject = decode_canonical(value)?;
            current.version.validate()?;
            Ok(current.version.id)
        })
        .transpose()
}

fn merge_change_from_record(record: &MergePlanEntry) -> Result<MergeChange> {
    Ok(MergeChange {
        key: record.key.clone(),
        from: current_id(record.ours.as_deref())?,
        to: current_id(record.selected.as_deref())?,
    })
}

fn merge_conflict_from_record(record: &MergePlanEntry) -> Result<MergeConflict> {
    if !record.conflict {
        return Err(Error::new(
            ErrorCode::CorruptCommit,
            "merge conflict index points to a non-conflict record",
        ));
    }
    Ok(MergeConflict {
        key: record.key.clone(),
        base: current_id(record.base.as_deref())?,
        ours: current_id(record.ours.as_deref())?,
        theirs: current_id(record.theirs.as_deref())?,
    })
}

pub fn validate_branch(branch: &str) -> Result<()> {
    let invalid_char = |value: char| {
        value.is_control()
            || value == ' '
            || matches!(value, '~' | '^' | ':' | '?' | '*' | '[' | '\\')
    };
    if branch.is_empty()
        || branch.len() > 255
        || branch == "HEAD"
        || branch.starts_with('/')
        || branch.ends_with('/')
        || branch.contains("//")
        || branch.contains("..")
        || branch.contains("@{")
        || branch.ends_with('.')
        || branch.chars().any(invalid_char)
        || branch
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == ".." || part.ends_with(".lock"))
    {
        return Err(Error::new(
            ErrorCode::InvalidBranch,
            "branch name violates the canonical ref contract",
        ));
    }
    Ok(())
}

const RETENTION_PIN_TAG_PREFIX: &str = "retention-pins/";

fn retention_pin_tag(name: &str) -> Result<String> {
    if name.trim().is_empty() || name.len() > 100 {
        return Err(Error::new(
            ErrorCode::InvalidBranch,
            "retention pin name must contain 1 to 100 bytes",
        ));
    }
    Ok(format!("{RETENTION_PIN_TAG_PREFIX}{}", hex::encode(name)))
}

fn decode_retention_pin_tag(tag: &str) -> Result<Option<String>> {
    let Some(encoded) = tag.strip_prefix(RETENTION_PIN_TAG_PREFIX) else {
        return Ok(None);
    };
    let bytes = hex::decode(encoded).map_err(|_| {
        Error::new(
            ErrorCode::CorruptCommit,
            "retention pin tag has invalid hex encoding",
        )
    })?;
    String::from_utf8(bytes).map(Some).map_err(|_| {
        Error::new(
            ErrorCode::CorruptCommit,
            "retention pin tag name is not UTF-8",
        )
    })
}

fn staged_put(
    key: Vec<u8>,
    bytes: Vec<u8>,
    headers: ObjectHeaders,
    user_metadata: BTreeMap<String, String>,
    binding: crate::PayloadBinding,
) -> StagedMutation {
    let size = bytes.len() as u64;
    let checksum_md5: [u8; 16] = Md5::digest(&bytes).into();
    let checksum_sha256: [u8; 32] = Sha256::digest(&bytes).into();
    StagedMutation {
        body: StagedMutationBody::Put(Box::new(StagedPut {
            key,
            size,
            logical_etag: format!("\"{}\"", hex::encode(checksum_md5)),
            checksums: Checksums {
                md5: Some(checksum_md5),
                sha256: Some(checksum_sha256),
                algorithm_values: BTreeMap::new(),
            },
            headers,
            user_metadata,
            tags: BTreeMap::new(),
            binding,
        })),
    }
}

fn validate_options(options: &RepositoryOptions) -> Result<()> {
    crate::repository::validate_branch(&options.default_branch)?;
    options.idempotency_retention.validate()?;
    options
        .provider_per_key_version_limit
        .validate_immutable_payload_profile(options.mutable_control_versions_to_retain)?;
    if options.repository_prefix.is_empty()
        || options.repository_prefix.ends_with('/')
        || options.writer.trim().is_empty()
        || !(10_000..=86_400_000).contains(&options.authority_lease_millis)
        || options.max_cached_node_pack_bytes == 0
        || options.max_cached_node_locations == 0
        || options.max_cached_node_bytes == 0
        || options.mutable_control_versions_to_retain < 2
        || !(1..=1_000_000).contains(&options.journal_index_max_unindexed_events)
        || !(1..=65_536).contains(&options.operation_index_leaf_entries)
        || !(2..=32).contains(&options.operation_index_merge_fanout)
        || options.operation_index_max_unindexed_events < options.operation_index_leaf_entries
        || options.operation_index_max_unindexed_events > 1_000_000
    {
        return Err(Error::new(
            ErrorCode::InvalidRequest,
            "repository options are invalid",
        ));
    }
    Ok(())
}

fn validate_format_compatibility(
    format: &RepositoryFormat,
    options: &RepositoryOptions,
) -> Result<()> {
    format.idempotency_retention.validate()?;
    if format.state_tree_format != options.state_tree_format
        || format.canonical_limits != options.limits
        || format.idempotency_retention != options.idempotency_retention
        || format.provider_per_key_version_limit != options.provider_per_key_version_limit
    {
        return Err(Error::new(
            ErrorCode::RepositoryFormatConflict,
            "repository format does not match the requested canonical settings",
        ));
    }
    Ok(())
}

fn format_path(prefix: &str) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/format/repository.cbor"))
}

fn gc_coordinator_path(prefix: &str) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/gc/coordinator.cbor"))
}

fn gc_publication_ticket_prefix(prefix: &str, repository: crate::RepositoryId) -> String {
    format!(
        "{prefix}/gc/publications/{}/",
        hex::encode(repository.as_bytes())
    )
}

fn gc_publication_ticket_path(
    prefix: &str,
    repository: crate::RepositoryId,
    instance: OperationId,
    request_digest: [u8; 32],
) -> Result<ObjectPath> {
    ObjectPath::new(format!(
        "{}{instance}/{}",
        gc_publication_ticket_prefix(prefix, repository),
        hex::encode(request_digest)
    ))
}

fn publication_ticket_digest(request: &CompareExchange) -> [u8; 32] {
    crate::model::derive_input_digest(&[
        b"publication-ticket",
        request.path.as_str().as_bytes(),
        &request.bytes,
    ])
}

fn gc_dirty_root_prefix(prefix: &str, epoch: OperationId) -> String {
    format!("{prefix}/gc/dirty/{epoch}/")
}

fn gc_dirty_root_sequence_prefix(prefix: &str, epoch: OperationId, sequence: u64) -> String {
    format!("{}{sequence:020}/", gc_dirty_root_prefix(prefix, epoch))
}

fn commit_path(prefix: &str, id: CommitId) -> Result<ObjectPath> {
    let encoded = hex::encode(id.as_bytes());
    ObjectPath::new(format!(
        "{prefix}/commits/sha256/{}/{}/{}",
        &encoded[..2],
        &encoded[2..4],
        encoded
    ))
}

fn gc_commit_queue_key(id: CommitId) -> Vec<u8> {
    let mut key = b"cq/".to_vec();
    key.extend_from_slice(id.as_bytes());
    key
}

fn gc_commit_mark_key(id: CommitId) -> Vec<u8> {
    let mut key = b"cm/".to_vec();
    key.extend_from_slice(id.as_bytes());
    key
}

fn gc_node_queue_key(cid: &prolly::Cid, scan_versions: bool) -> Vec<u8> {
    let mut key = vec![b'n', b'q', b'/', u8::from(scan_versions), b'/'];
    key.extend_from_slice(cid.as_bytes());
    key
}

fn gc_node_mark_key(cid: &prolly::Cid, scan_versions: bool) -> Vec<u8> {
    let mut key = vec![b'n', b'm', b'/', u8::from(scan_versions), b'/'];
    key.extend_from_slice(cid.as_bytes());
    key
}

fn gc_path_mark_key(path: &ObjectPath) -> Vec<u8> {
    let mut key = b"pm/".to_vec();
    key.extend_from_slice(&crate::codec::sha256(path.as_str().as_bytes()));
    key
}

fn gc_physical_mark_key(path: &ObjectPath, version: &str) -> Vec<u8> {
    let mut identity = path.as_str().as_bytes().to_vec();
    identity.push(0);
    identity.extend_from_slice(version.as_bytes());
    let mut key = b"vm/".to_vec();
    key.extend_from_slice(&crate::codec::sha256(&identity));
    key
}

fn gc_candidate_key(candidate: &GcCandidate) -> Result<Vec<u8>> {
    let mut key = b"d/".to_vec();
    key.extend_from_slice(&crate::codec::sha256(&encode_canonical(candidate)?));
    Ok(key)
}

fn gc_managed_kind(prefix: &str, path: &ObjectPath) -> Option<&'static str> {
    let relative = path.as_str().strip_prefix(prefix)?.strip_prefix('/')?;
    if relative.starts_with("commits/sha256/") {
        Some("commits")
    } else if relative.starts_with("nodes/sha256/") {
        Some("nodes")
    } else if relative.starts_with("payloads/") || relative.starts_with("payload-packs/") {
        Some("payloads")
    } else {
        None
    }
}

fn intent_path(prefix: &str) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/format/initialization.cbor"))
}

fn version_tree_key(key: &[u8], order: ObjectVersionOrder, version: ObjectVersionId) -> Vec<u8> {
    let mut output = version_tree_prefix(key);
    output.reserve(8 + 4 + 32);
    output.extend(order.commit_generation.0.to_be_bytes().map(|byte| !byte));
    output.extend(order.mutation_ordinal.to_be_bytes().map(|byte| !byte));
    output.extend(version.as_bytes().iter().map(|byte| !byte));
    output
}

fn version_tree_prefix(key: &[u8]) -> Vec<u8> {
    let mut output = version_tree_partial_prefix(key);
    output.extend_from_slice(&[0, 0]);
    output
}

fn version_tree_partial_prefix(key: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(key.len() + 2);
    for byte in key {
        if *byte == 0 {
            output.extend_from_slice(&[0, 0xff]);
        } else {
            output.push(*byte);
        }
    }
    output
}

fn decode_version_tree_logical_key(encoded: &[u8]) -> Result<Vec<u8>> {
    let mut key = Vec::new();
    let mut index = 0;
    while index < encoded.len() {
        match encoded[index] {
            0 if encoded.get(index + 1) == Some(&0) => return Ok(key),
            0 if encoded.get(index + 1) == Some(&0xff) => {
                key.push(0);
                index += 2;
            }
            0 => {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "noncanonical version-tree key escape",
                ))
            }
            byte => {
                key.push(byte);
                index += 1;
            }
        }
    }
    Err(Error::new(
        ErrorCode::CorruptCommit,
        "unterminated version-tree logical key",
    ))
}
