use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    io::Write as _,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock, Weak,
    },
    time::Duration,
};

use crate::store::{NodeCacheNamespace, NodeLocator, PreparedNodePack};
use crate::{
    decode_canonical, derive_input_digest, derive_repository_id, encode_canonical,
    tree_format_digest, BatchId, BucketCommitV1, BucketDeltaV1, BucketStateV1, CanonicalLimits,
    CanonicalOperationResult, ChecksumExpectation, Clock, CommitGeneration, CommitGraphEntryV2,
    CommitGraphHeadV2, CommitId, CommitObjectV1, CommitReceipt, CompareExchange,
    CompareExchangeOutcome, CurrentObjectV1, DeleteOutcome, Error, ErrorCode, EtagPredicateV1,
    GcCandidateV1, GcCommitWorkV2, GcCoordinatorV2, GcDirtyRootIdV2, GcDirtyRootV2, GcEpochPhaseV2,
    GcEpochV2, GcFenceV1, GcMarkRunStateV1, GcMarkRunV1, GcPlanBodyV1, GcPlanId, GcPlanV1,
    GcRunStateV1, GcRunV1, GcVersionWorkV2, GetRequest, IdSource, ImmutablePut,
    InitializationIntentV1, ListRequest, LogicalObjectVersionBodyV1, LogicalObjectVersionKindV1,
    MemoryNodeCache, MutableControlKind, MutableControlObserver, NodeCache, NodeIndexEntryV1,
    NodeIndexHeadV2, ObjectData, ObjectHeaders, ObjectPath, ObjectPlane, ObjectTransition,
    ObjectVersionId, ObjectVersionOrder, ObjectVersionV1, ObjectWriteConditionV1, OperationId,
    OperationKind, OperationRecordV1, PhysicalBatchV1, PhysicalPreparedMutationV1, PhysicalVersion,
    ProllyObjectStore, RandomIdSource, RefCatalogEntryV2, RefCatalogHeadV2, RefGeneration,
    ReflogEntryV1, RepositoryFormatV1, RepositoryId, Result, RetentionPinV1, RetryAdvice,
    StorageToken, SystemClock, TreeRootV1,
};
use futures_util::{stream::BoxStream, Stream, StreamExt};
use md5::{Digest as _, Md5};
use prolly::{AsyncProlly, AsyncStore, Config, Mutation, Node, RuntimeConfig, Tree, TreeFormat};
use sha2::Sha256;

const MIN_NONFINAL_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024;
const MAX_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const MAX_GC_CAS_RETRIES: usize = 16;
const FSCK_OBJECT_TREE: u8 = 0;
const FSCK_VERSION_TREE: u8 = 1;
const FSCK_OPERATION_TREE: u8 = 2;

#[derive(Clone)]
pub struct RepositoryOptions {
    pub repository_prefix: String,
    pub default_branch: String,
    pub writer: String,
    pub limits: CanonicalLimits,
    pub state_tree_format: TreeFormat,
    /// Duration of each branch/system writer-authority lease.
    pub writer_lease_millis: u64,
    /// Open without acquiring mutation authority.
    pub read_only: bool,
    pub reflog_retention_millis: u64,
    pub history_traversal_limit: usize,
    /// Repository-wide maximum payload PUT, COPY, DELETE, or multipart-part
    /// requests in flight across independent calls and atomic batches.
    pub max_parallel_payload_writes: usize,
    /// In-process metadata cache bounds. These affect performance only and do
    /// not change persisted repository semantics.
    pub max_cached_commits: usize,
    pub max_cached_branches: usize,
    pub max_cached_node_pack_bytes: usize,
    /// Maximum CID-to-envelope range mappings retained in process. The v2
    /// index resolves evicted entries lazily.
    pub max_cached_node_locations: usize,
    /// Maximum bytes in the default verified node cache. Ignored when
    /// `node_cache` supplies an external implementation.
    pub max_cached_node_bytes: usize,
    /// Optional shared memory/disk cache. Cache failures are fail-open and all
    /// returned bytes are verified by CID before use.
    pub node_cache: Option<Arc<dyn NodeCache>>,
    /// Compact obsolete physical versions of a hot branch ref at this
    /// generation interval. Zero disables automatic compaction.
    pub branch_ref_compaction_interval: u64,
    /// Number of physical ref versions retained during compaction. Logical
    /// history remains in immutable commits and is unaffected.
    pub branch_ref_versions_to_retain: usize,
    /// Maximum physical versions retained for every recurring mutable control
    /// object. Compaction runs before each successful CAS update, reserving
    /// one slot for the new version.
    pub mutable_control_versions_to_retain: usize,
    /// Maximum exact physical deletions per second during GC. Zero disables
    /// pacing. The physical format accepts 1..=1,000 when configured.
    pub gc_delete_rate_limit_per_second: u32,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdSource>,
}

impl Default for RepositoryOptions {
    fn default() -> Self {
        Self {
            repository_prefix: ".prolly/v1".to_string(),
            default_branch: "main".to_string(),
            writer: "anonymous".to_string(),
            limits: CanonicalLimits::default(),
            state_tree_format: TreeFormat::default(),
            writer_lease_millis: 60_000,
            read_only: false,
            reflog_retention_millis: 90 * 24 * 60 * 60 * 1_000,
            history_traversal_limit: 100_000,
            max_parallel_payload_writes: 16,
            max_cached_commits: 4_096,
            max_cached_branches: 1_024,
            max_cached_node_pack_bytes: 64 * 1024 * 1024,
            max_cached_node_locations: 65_536,
            max_cached_node_bytes: 64 * 1024 * 1024,
            node_cache: None,
            branch_ref_compaction_interval: 5_000,
            branch_ref_versions_to_retain: 100,
            mutable_control_versions_to_retain: crate::DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
            gc_delete_rate_limit_per_second: 0,
            clock: Arc::new(SystemClock),
            ids: Arc::new(RandomIdSource),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectSummary {
    pub key: Vec<u8>,
    pub version: ObjectVersionV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionSummary {
    pub key: Vec<u8>,
    pub version: ObjectVersionV1,
    pub cursor: Vec<u8>,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectDiff {
    pub key: Vec<u8>,
    pub from: Option<ObjectVersionId>,
    pub to: Option<ObjectVersionId>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObjectDiffCursor {
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MergePolicy {
    #[default]
    Fail,
    Ours,
    Theirs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeConflict {
    pub key: Vec<u8>,
    pub base: Option<ObjectVersionId>,
    pub ours: Option<ObjectVersionId>,
    pub theirs: Option<ObjectVersionId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergePlan {
    pub ours: CommitId,
    pub theirs: CommitId,
    pub best_bases: Vec<CommitId>,
    pub selected_base: Option<CommitId>,
    pub changes: Vec<ObjectDiff>,
    pub conflicts: Vec<MergeConflict>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefMoveReceipt {
    pub branch: String,
    pub old_target: Option<CommitId>,
    pub new_target: CommitId,
    pub operation: OperationId,
    pub generation: RefGeneration,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RefVersionCompactionReport {
    pub scanned: usize,
    pub retained: usize,
    pub deleted: usize,
    pub already_missing: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcDryRun {
    pub plan: GcPlanV1,
    pub retained_paths: usize,
    pub candidate_bytes: u64,
    pub candidates_by_kind: BTreeMap<String, usize>,
    pub candidate_bytes_by_kind: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcSweepReport {
    pub plan: GcPlanId,
    pub deleted_versions: u64,
    pub deleted_bytes: u64,
    pub skipped_reachable: u64,
    pub already_missing: u64,
    pub complete: bool,
    pub next_index: u64,
    pub deleted_by_kind: BTreeMap<String, u64>,
    pub deleted_bytes_by_kind: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcEpochStepReport {
    pub epoch: GcEpochV2,
    pub processed: usize,
    pub restarted_for_new_roots: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CloneReport {
    pub immutable_objects: usize,
    pub immutable_bytes: u64,
    pub refs: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub source_head: Option<CommitId>,
    pub copied_objects: usize,
    pub copied_bytes: u64,
    pub already_present: usize,
    pub ref_move: Option<RefMoveReceipt>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsckReport {
    pub branches: usize,
    pub tags: usize,
    pub commits: usize,
    pub deltas: usize,
    pub reachable_nodes: usize,
    pub reachable_node_bytes: usize,
    pub logical_versions: usize,
    pub content_bytes_verified: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ResumableFsckPhase {
    DiscoverCommits,
    VerifyNodes,
    VerifyVersions,
    Complete,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResumableFsckCursor {
    pub closure: CommitClosureCursor,
    pub report: FsckReport,
    pub phase: ResumableFsckPhase,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResumableFsckPage {
    pub cursor: ResumableFsckCursor,
    pub processed_commits: usize,
    pub processed_nodes: usize,
    pub processed_versions: usize,
    pub traversal_steps: usize,
    pub complete: bool,
    pub budget_exhausted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct FsckNodeWork {
    kind: u8,
    cid: prolly::Cid,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct FsckVersionWork {
    key: Vec<u8>,
    version: ObjectVersionV1,
    continuation: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairReport {
    pub sync: SyncReport,
    pub fsck: FsckReport,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeIndexAdvanceReport {
    pub generation: u64,
    pub scan_epoch: u64,
    pub indexed_commit_objects: usize,
    pub indexed_node_entries: usize,
    pub completed_scan: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InternalNodePrewarmReport {
    pub roots: usize,
    pub internal_nodes: usize,
    pub root_leaves: usize,
    pub leaves_skipped: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexFreshness {
    pub generation: u64,
    pub scan_epoch: u64,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefCatalogAdvanceReport {
    pub freshness: IndexFreshness,
    pub indexed_ref_objects: usize,
    pub completed_scan: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitGraphAdvanceReport {
    pub freshness: IndexFreshness,
    pub indexed_commit_objects: usize,
    pub completed_scan: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogBranchPage {
    pub branches: Vec<BranchHead>,
    pub continuation: Option<String>,
    pub freshness: IndexFreshness,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogTagPage {
    pub tags: Vec<Tag>,
    pub continuation: Option<String>,
    pub freshness: IndexFreshness,
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
    root: CommitId,
    next: CommitId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitPage {
    pub commits: Vec<(CommitId, BucketCommitV1)>,
    pub continuation: Option<HistoryCursor>,
    pub visited_commits: usize,
    pub decoded_bytes: u64,
    pub budget_exhausted: bool,
}

/// Durable state for a parent-before-child traversal of an arbitrary commit
/// DAG closure. The large stack and visited set live in immutable Prolly nodes;
/// serializing this cursor remains constant-size.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CommitClosureCursor {
    pub repository: RepositoryId,
    pub traversal: OperationId,
    pub state: TreeRootV1,
    pub next_stack_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitClosurePage {
    /// Parent-before-child commits, each emitted exactly once per traversal.
    pub commits: Vec<(CommitId, BucketCommitV1)>,
    pub cursor: CommitClosureCursor,
    pub steps: usize,
    pub complete: bool,
    pub budget_exhausted: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitClosureCleanupReport {
    pub deleted_objects: usize,
    pub complete: bool,
}

/// Constant-size checkpoint for an interruptible physical history transfer.
/// The source traversal and source-to-destination mappings live in the
/// closure tree named by this cursor.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PhysicalTransferCursor {
    pub closure: CommitClosureCursor,
    destination_scope: [u8; 32],
    force_rebind: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalTransferPage {
    pub cursor: PhysicalTransferCursor,
    pub sync: SyncReport,
    pub processed_commits: usize,
    pub traversal_steps: usize,
    pub complete: bool,
    pub budget_exhausted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CommitClosureWork {
    commit: CommitId,
    finish: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FirstParentCursor {
    root: CommitId,
    requested_distance: u64,
    current: CommitId,
    remaining: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FirstParentPage {
    /// Present when the requested ancestor was reached.
    pub ancestor: Option<CommitId>,
    pub continuation: Option<FirstParentCursor>,
    pub edges_advanced: u64,
    pub index_reads: usize,
    pub fallback_commit_reads: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchPage {
    pub branches: Vec<BranchHead>,
    pub continuation: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagPage {
    pub tags: Vec<Tag>,
    pub continuation: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionPinPage {
    pub pins: Vec<RetentionPinV1>,
    pub continuation: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagReflogPage {
    pub entries: Vec<(crate::ReflogEntryId, ReflogEntryV1)>,
    pub continuation: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchReflogPage {
    pub entries: Vec<(crate::ReflogEntryId, ReflogEntryV1)>,
    pub continuation: Option<BranchReflogCursor>,
    pub budget_exhausted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BranchReflogCursor {
    branch: String,
    root: CommitId,
    history: Option<HistoryCursor>,
    inline_id: crate::ReflogEntryId,
    inline_emitted: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RepositoryPerformanceSnapshot {
    pub publication_acquisitions: u64,
    pub publication_wait_nanos: u64,
    pub publication_queue_depth: u64,
    pub publication_max_queue_depth: u64,
    pub node_cache_hits: u64,
    pub node_cache_misses: u64,
    pub node_cache_insertions: u64,
    pub node_cache_errors: u64,
    pub node_cache_corruptions: u64,
    pub node_fetch_coalesced_waits: u64,
    pub node_ranged_fetches: u64,
    pub node_index_cache_hits: u64,
    pub node_index_cache_misses: u64,
    pub node_index_page_fetches: u64,
    pub node_index_advances: u64,
    pub node_index_advance_errors: u64,
    pub node_index_entries_indexed: u64,
}

#[derive(Default)]
struct RepositoryPerformanceCounters {
    publication_acquisitions: AtomicU64,
    publication_wait_nanos: AtomicU64,
    publication_queue_depth: AtomicU64,
    publication_max_queue_depth: AtomicU64,
    node_index_advances: AtomicU64,
    node_index_advance_errors: AtomicU64,
    node_index_entries_indexed: AtomicU64,
}

struct GcDirtyRootObserver<P: ObjectPlane> {
    plane: Arc<P>,
    prefix: String,
    repository: RepositoryId,
    active_epoch: Arc<RwLock<Option<OperationId>>>,
    dirty_sequence: Arc<AtomicU64>,
    process_session: OperationId,
}

#[async_trait::async_trait]
impl<P: ObjectPlane> MutableControlObserver for GcDirtyRootObserver<P> {
    async fn before_compare_exchange(
        &self,
        kind: MutableControlKind,
        request: &CompareExchange,
    ) -> Result<()> {
        let active_epoch = *self
            .active_epoch
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "active GC lock poisoned"))?;
        let Some(epoch) = active_epoch else {
            return Ok(());
        };
        let (
            namespace,
            name,
            target,
            previous_target,
            ref_generation,
            operation,
            created_at_millis,
        ) = match kind {
            MutableControlKind::BranchRefV1 => {
                let value: crate::RefValueV1 = decode_canonical(&request.bytes)?;
                (
                    "branch".to_string(),
                    value.inline_reflog.branch.clone(),
                    value.target,
                    value.previous_target,
                    value.generation,
                    value.operation,
                    value.updated_at_millis,
                )
            }
            MutableControlKind::TagRefV1 => {
                let value: crate::TagValueV1 = decode_canonical(&request.bytes)?;
                (
                    "tag".to_string(),
                    request.path.as_str().to_string(),
                    value.target,
                    value.previous_target,
                    value.generation,
                    value.operation,
                    value.created_at_millis,
                )
            }
            MutableControlKind::RetentionPinV1 => {
                let value: RetentionPinV1 = decode_canonical(&request.bytes)?;
                (
                    "pin".to_string(),
                    value.name.clone(),
                    value.target,
                    None,
                    RefGeneration(value.generation),
                    self.process_session,
                    value.created_at_millis,
                )
            }
            _ => return Ok(()),
        };
        let publication_sequence = self
            .dirty_sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "GC dirty-root sequence overflow",
                )
            })?;
        let event = GcDirtyRootV2 {
            repository: self.repository,
            epoch,
            process_session: self.process_session,
            publication_sequence,
            namespace,
            name,
            target,
            previous_target,
            ref_generation,
            operation,
            created_at_millis,
        };
        let id = event.id()?;
        let bytes = encode_canonical(&event)?;
        self.plane
            .put_immutable(ImmutablePut {
                path: gc_dirty_root_v2_path(&self.prefix, &event, id)?,
                expected_sha256: crate::codec::sha256(&bytes),
                bytes,
            })
            .await?;
        Ok(())
    }
}

/// Returns the exclusive version-tree cursor immediately after every version
/// of `key`. This lets AWS-shaped `key_marker` requests skip the complete key
/// while keeping the physical Prolly encoding private everywhere else.
pub fn version_cursor_after_key(key: &[u8]) -> Vec<u8> {
    let mut cursor = version_tree_prefix(key);
    cursor.extend([u8::MAX; 8 + 4 + 32]);
    cursor
}

struct LoadedRef {
    value: crate::RefValueV1,
    token: StorageToken,
}

#[derive(Clone)]
struct HeldWriterLease {
    value: crate::ExclusiveWriterLeaseV1,
    token: StorageToken,
}

#[derive(Clone)]
struct WarmBranchState {
    reference: crate::RefValueV1,
    token: StorageToken,
    commit: BucketCommitV1,
}

struct BoundedCache<K, V> {
    entries: BTreeMap<K, V>,
    order: VecDeque<K>,
    capacity: usize,
}

struct StoredCommit {
    id: CommitId,
    pending_pack: Option<(PreparedNodePack, u64)>,
}

impl<K: Ord + Clone, V: Clone> BoundedCache<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        let value = self.entries.get(key)?.clone();
        self.order.retain(|candidate| candidate != key);
        self.order.push_back(key.clone());
        Some(value)
    }

    fn insert(&mut self, key: K, value: V) {
        self.entries.insert(key.clone(), value);
        self.order.retain(|candidate| candidate != &key);
        self.order.push_back(key);
        while self.entries.len() > self.capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.entries.remove(&evicted);
            }
        }
    }

    fn remove(&mut self, key: &K) {
        self.entries.remove(key);
        self.order.retain(|candidate| candidate != key);
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

#[derive(Default)]
struct RetainedClosure {
    paths: BTreeSet<ObjectPath>,
    physical_versions: BTreeSet<(ObjectPath, String)>,
}

impl RetainedClosure {
    fn contains(&self, path: &ObjectPath, version: Option<&str>) -> bool {
        self.paths.contains(path)
            || version.is_some_and(|version| {
                self.physical_versions
                    .contains(&(path.clone(), version.to_string()))
            })
    }

    fn len(&self) -> usize {
        self.paths.len() + self.physical_versions.len()
    }
}

struct LoadedGcRun {
    value: GcRunV1,
    token: StorageToken,
}

struct LoadedGcMarkRun {
    value: GcMarkRunV1,
    token: StorageToken,
}

struct LoadedGcEpoch {
    value: GcEpochV2,
    token: StorageToken,
}

struct ProllyNodeIndex<P: ObjectPlane> {
    store: ProllyObjectStore<P>,
    engine: AsyncProlly<ProllyObjectStore<P>>,
    tree: RwLock<Tree>,
}

struct ProllyMetadataIndex<P: ObjectPlane> {
    _store: ProllyObjectStore<P>,
    engine: AsyncProlly<ProllyObjectStore<P>>,
    tree: RwLock<Tree>,
    name: &'static str,
}

struct MetadataIndexSpec<'a> {
    path: &'a str,
    protocol_version: u32,
    name: &'static str,
}

impl<P: ObjectPlane> ProllyMetadataIndex<P> {
    fn new(
        plane: Arc<P>,
        prefix: &str,
        repository: RepositoryId,
        format: TreeFormat,
        node_cache: Arc<dyn NodeCache>,
        spec: MetadataIndexSpec<'_>,
    ) -> Result<Self> {
        let config = Config {
            format: format.clone(),
            runtime: RuntimeConfig::default(),
        };
        let store = ProllyObjectStore::new_cached_direct(
            plane,
            format!("{prefix}/{}", spec.path),
            repository,
            spec.protocol_version,
            tree_format_digest(&format)?,
            node_cache,
        );
        let engine = AsyncProlly::new(store.clone(), config);
        let tree = engine.create();
        Ok(Self {
            _store: store,
            engine,
            tree: RwLock::new(tree),
            name: spec.name,
        })
    }

    fn tree(&self) -> Result<Tree> {
        self.tree
            .read()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    format!("{} lock poisoned", self.name),
                )
            })
            .map(|tree| tree.clone())
    }

    fn install_root(&self, root: Option<prolly::Cid>) -> Result<()> {
        self.tree
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    format!("{} lock poisoned", self.name),
                )
            })?
            .root = root;
        Ok(())
    }
}

impl<P: ObjectPlane> ProllyNodeIndex<P> {
    fn new(
        plane: Arc<P>,
        prefix: &str,
        repository: RepositoryId,
        format: TreeFormat,
        node_cache: Arc<dyn NodeCache>,
    ) -> Result<Self> {
        let config = Config {
            format: format.clone(),
            runtime: RuntimeConfig::default(),
        };
        let store = ProllyObjectStore::new_cached_direct(
            plane,
            format!("{prefix}/node-index/v2/tree"),
            repository,
            2,
            tree_format_digest(&format)?,
            node_cache,
        );
        let engine = AsyncProlly::new(store.clone(), config);
        let tree = engine.create();
        Ok(Self {
            store,
            engine,
            tree: RwLock::new(tree),
        })
    }

    fn tree(&self) -> Result<Tree> {
        self.tree
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "node-index lock poisoned"))
            .map(|tree| tree.clone())
    }

    fn install_root(&self, root: Option<prolly::Cid>) -> Result<()> {
        self.tree
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "node-index lock poisoned"))?
            .root = root;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<P: ObjectPlane> NodeLocator for ProllyNodeIndex<P> {
    async fn locate(&self, cid: &prolly::Cid) -> Result<Option<NodeIndexEntryV1>> {
        let tree = self.tree()?;
        self.engine
            .get(&tree, cid.as_bytes())
            .await?
            .map(|bytes| decode_canonical::<NodeIndexEntryV1>(&bytes))
            .transpose()
    }
}

pub struct ShardAuthorityMaintenance {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ShardAuthorityMaintenance {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct NodeIndexMaintenance {
    task: tokio::task::JoinHandle<()>,
}

struct PublicationLaneGuard<'a> {
    _barrier: tokio::sync::RwLockReadGuard<'a, ()>,
    _lane: tokio::sync::OwnedMutexGuard<()>,
}

impl Drop for NodeIndexMaintenance {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct Repository<P: ObjectPlane> {
    plane: Arc<P>,
    controls: crate::MutableControlStore<P>,
    options: RepositoryOptions,
    format: RepositoryFormatV1,
    node_store: ProllyObjectStore<P>,
    node_cache: Arc<dyn NodeCache>,
    node_index: Arc<ProllyNodeIndex<P>>,
    ref_catalog: Arc<ProllyMetadataIndex<P>>,
    commit_graph: Arc<ProllyMetadataIndex<P>>,
    engine: AsyncProlly<ProllyObjectStore<P>>,
    shard_authority: Arc<crate::ShardWriterAuthorityV2<P>>,
    authority_permits: Arc<RwLock<BTreeMap<crate::AuthorityScopeV2, crate::AuthorityPermitV2>>>,
    authority_renewal: Arc<tokio::sync::Mutex<()>>,
    writer_lease: Arc<RwLock<Option<HeldWriterLease>>>,
    warm_branches: Arc<RwLock<BoundedCache<String, WarmBranchState>>>,
    commit_cache: Arc<RwLock<BoundedCache<CommitId, BucketCommitV1>>>,
    publication_barrier: Arc<tokio::sync::RwLock<()>>,
    publication_lanes: Arc<std::sync::Mutex<BTreeMap<String, Weak<tokio::sync::Mutex<()>>>>>,
    payload_writes: Arc<tokio::sync::Semaphore>,
    operation_locks: Arc<std::sync::Mutex<BTreeMap<OperationId, Weak<tokio::sync::Mutex<()>>>>>,
    lease_renewal: Arc<tokio::sync::Mutex<()>>,
    performance: Arc<RepositoryPerformanceCounters>,
    active_gc_epoch: Arc<RwLock<Option<OperationId>>>,
    gc_dirty_sequence: Arc<AtomicU64>,
    process_session: OperationId,
}

impl<P: ObjectPlane> Repository<P> {
    pub async fn initialize(plane: Arc<P>, options: RepositoryOptions) -> Result<Self> {
        validate_options(&options)?;
        if plane
            .head(&format_path(&options.repository_prefix)?)
            .await?
            .is_some()
        {
            return Self::open(plane, options).await;
        }

        let operation = options.ids.operation();
        let created_at_millis = options.clock.now_millis()?;
        let repository_id = derive_repository_id(operation);
        let proposed_format = RepositoryFormatV1 {
            repository_id,
            format_version: RepositoryFormatV1::VERSION,
            state_tree_format: options.state_tree_format.clone(),
            canonical_limits: options.limits.clone(),
            min_reader_version: RepositoryFormatV1::PROLLY_S3_PROTOCOL_VERSION,
            min_writer_version: RepositoryFormatV1::PROLLY_S3_PROTOCOL_VERSION,
            created_at_millis,
            required_capability_profile: RepositoryFormatV1::PROLLY_S3_CAPABILITY_PROFILE,
        };
        let proposed_intent = InitializationIntentV1 {
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
        let repository = Self::from_format(plane.clone(), options.clone(), intent.format.clone())?;

        let empty = repository.engine.create();
        let empty_state = BucketStateV1 {
            objects: TreeRootV1::from_tree(&empty)?,
            versions: TreeRootV1::from_tree(&empty)?,
            operations: TreeRootV1::from_tree(&empty)?,
        };
        let delta = BucketDeltaV1 {
            operation_ids: Vec::new(),
            changes: Vec::new(),
        };
        let writer_fence_generation = repository
            .branch_writer_generation(&options.default_branch)
            .await?;
        let commit = BucketCommitV1 {
            state: empty_state,
            parents: Vec::new(),
            generation: CommitGeneration(0),
            delta,
            node_pack: None,
            writer_fence_generation,
            author: options.writer.clone(),
            message: Some("initialize versioned S3 repository".to_string()),
            created_at_millis: intent.format.created_at_millis,
            metadata: BTreeMap::new(),
        };
        let stored = repository.store_commit(&commit, None).await?;
        let commit_id = stored.id;
        repository.finalize_stored_commit(stored).await?;

        let reflog = ReflogEntryV1 {
            branch: options.default_branch.clone(),
            old_target: None,
            new_target: commit_id,
            operation: intent.operation,
            actor: options.writer.clone(),
            message: "initialize".to_string(),
            created_at_millis: intent.format.created_at_millis,
        };
        let reflog_id = reflog.id()?;

        let format_bytes = encode_canonical(&intent.format)?;
        match plane
            .compare_exchange(CompareExchange {
                path: format_path(&options.repository_prefix)?,
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

        let initial_ref = crate::RefValueV1 {
            target: commit_id,
            previous_target: None,
            generation: RefGeneration(0),
            operation: intent.operation,
            reflog: reflog_id,
            writer: options.writer.clone(),
            updated_at_millis: intent.format.created_at_millis,
            tombstone: false,
            writer_fence_generation,
            inline_reflog: reflog,
        };
        let initial_ref_bytes = encode_canonical(&initial_ref)?;
        match repository
            .controls
            .compare_exchange(CompareExchange {
                path: branch_path(&options.repository_prefix, &options.default_branch)?,
                expected: None,
                bytes: initial_ref_bytes,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(metadata) => {
                repository.cache_branch(
                    &options.default_branch,
                    initial_ref,
                    metadata.token,
                    commit,
                )?;
                Ok(repository)
            }
            CompareExchangeOutcome::Conflict(Some(existing)) => {
                let existing: crate::RefValueV1 = decode_canonical(&existing.bytes)?;
                if existing.target != commit_id || existing.tombstone {
                    return Err(Error::new(
                        ErrorCode::RepositoryFormatConflict,
                        "default branch exists with a divergent initial value",
                    ));
                }
                let loaded = repository
                    .load_ref_including_tombstone(&options.default_branch)
                    .await?;
                repository.cache_branch(
                    &options.default_branch,
                    loaded.value,
                    loaded.token,
                    commit,
                )?;
                Ok(repository)
            }
            CompareExchangeOutcome::Conflict(None) => Err(Error::new(
                ErrorCode::RefConflict,
                "default branch create returned an empty conflict",
            )),
        }
    }

    pub async fn open(plane: Arc<P>, options: RepositoryOptions) -> Result<Self> {
        validate_options(&options)?;
        let object = plane
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
        let format = decode_repository_format(&object.bytes)?;
        validate_format_compatibility(&format, &options)?;
        let repository = Self::from_format(plane, options, format)?;
        repository.load_latest_node_index_checkpoint().await?;
        repository.load_latest_scale_metadata().await?;
        repository.restore_gc_coordinator_v2().await?;
        Ok(repository)
    }

    fn from_format(
        plane: Arc<P>,
        options: RepositoryOptions,
        format: RepositoryFormatV1,
    ) -> Result<Self> {
        let config = Config {
            format: format.state_tree_format.clone(),
            runtime: RuntimeConfig::default(),
        };
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
                protocol_version: RepositoryFormatV1::PROLLY_S3_PROTOCOL_VERSION,
                tree_format: tree_format_digest(&format.state_tree_format)?,
            },
            node_cache.clone(),
        );
        let node_index = Arc::new(ProllyNodeIndex::new(
            plane.clone(),
            &options.repository_prefix,
            format.repository_id,
            format.state_tree_format.clone(),
            node_cache.clone(),
        )?);
        let ref_catalog = Arc::new(ProllyMetadataIndex::new(
            plane.clone(),
            &options.repository_prefix,
            format.repository_id,
            format.state_tree_format.clone(),
            node_cache.clone(),
            MetadataIndexSpec {
                path: "ref-catalog/v2/tree",
                protocol_version: 3,
                name: "ref-catalog",
            },
        )?);
        let commit_graph = Arc::new(ProllyMetadataIndex::new(
            plane.clone(),
            &options.repository_prefix,
            format.repository_id,
            format.state_tree_format.clone(),
            node_cache.clone(),
            MetadataIndexSpec {
                path: "commit-graph/v2/tree",
                protocol_version: 4,
                name: "commit-graph",
            },
        )?);
        node_store.set_node_locator(node_index.clone())?;
        let engine = AsyncProlly::new(node_store.clone(), config);
        let max_cached_branches = options.max_cached_branches;
        let max_cached_commits = options.max_cached_commits;
        let max_parallel_payload_writes = options.max_parallel_payload_writes;
        let performance = Arc::new(RepositoryPerformanceCounters::default());
        let active_gc_epoch = Arc::new(RwLock::new(None));
        let gc_dirty_sequence = Arc::new(AtomicU64::new(0));
        let process_session = OperationId(uuid::Uuid::new_v4());
        let dirty_root_observer = Arc::new(GcDirtyRootObserver {
            plane: plane.clone(),
            prefix: options.repository_prefix.clone(),
            repository: format.repository_id,
            active_epoch: active_gc_epoch.clone(),
            dirty_sequence: gc_dirty_sequence.clone(),
            process_session,
        });
        let controls = crate::MutableControlStore::new(
            plane.clone(),
            options.repository_prefix.clone(),
            options.mutable_control_versions_to_retain,
        )?
        .with_observer(dirty_root_observer);
        let shard_authority = Arc::new(crate::ShardWriterAuthorityV2::new_with_control_retention(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            Duration::from_millis(options.writer_lease_millis),
            options.mutable_control_versions_to_retain,
        )?);
        Ok(Self {
            plane,
            controls,
            options,
            format,
            node_store,
            node_cache,
            node_index,
            ref_catalog,
            commit_graph,
            engine,
            shard_authority,
            authority_permits: Arc::new(RwLock::new(BTreeMap::new())),
            authority_renewal: Arc::new(tokio::sync::Mutex::new(())),
            writer_lease: Arc::new(RwLock::new(None)),
            warm_branches: Arc::new(RwLock::new(BoundedCache::new(max_cached_branches))),
            commit_cache: Arc::new(RwLock::new(BoundedCache::new(max_cached_commits))),
            publication_barrier: Arc::new(tokio::sync::RwLock::new(())),
            publication_lanes: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            payload_writes: Arc::new(tokio::sync::Semaphore::new(max_parallel_payload_writes)),
            operation_locks: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            lease_renewal: Arc::new(tokio::sync::Mutex::new(())),
            performance,
            active_gc_epoch,
            gc_dirty_sequence,
            process_session,
        })
    }

    pub fn performance_snapshot(&self) -> RepositoryPerformanceSnapshot {
        let node_cache = self.node_store.node_cache_snapshot();
        let node_index_cache = self.node_index.store.node_cache_snapshot();
        RepositoryPerformanceSnapshot {
            publication_acquisitions: self
                .performance
                .publication_acquisitions
                .load(Ordering::Relaxed),
            publication_wait_nanos: self
                .performance
                .publication_wait_nanos
                .load(Ordering::Relaxed),
            publication_queue_depth: self
                .performance
                .publication_queue_depth
                .load(Ordering::Relaxed),
            publication_max_queue_depth: self
                .performance
                .publication_max_queue_depth
                .load(Ordering::Relaxed),
            node_cache_hits: node_cache.hits,
            node_cache_misses: node_cache.misses,
            node_cache_insertions: node_cache.insertions,
            node_cache_errors: node_cache.errors,
            node_cache_corruptions: node_cache.corruptions,
            node_fetch_coalesced_waits: node_cache.coalesced_waits,
            node_ranged_fetches: node_cache.ranged_fetches,
            node_index_cache_hits: node_index_cache.hits,
            node_index_cache_misses: node_index_cache.misses,
            node_index_page_fetches: node_index_cache.ranged_fetches,
            node_index_advances: self.performance.node_index_advances.load(Ordering::Relaxed),
            node_index_advance_errors: self
                .performance
                .node_index_advance_errors
                .load(Ordering::Relaxed),
            node_index_entries_indexed: self
                .performance
                .node_index_entries_indexed
                .load(Ordering::Relaxed),
        }
    }

    /// Fetch only the three state-tree roots and their internal descendants.
    /// Leaf nodes and object payloads are intentionally not read.
    pub async fn prewarm_internal_nodes(
        &self,
        snapshot: CommitId,
    ) -> Result<InternalNodePrewarmReport> {
        let commit = self.load_commit_metadata(snapshot).await?;
        let mut pending = Vec::new();
        for root in [
            &commit.state.objects,
            &commit.state.versions,
            &commit.state.operations,
        ] {
            if root.format_digest != tree_format_digest(&self.format.state_tree_format)? {
                return Err(Error::new(
                    ErrorCode::CorruptNode,
                    "prewarm root uses an incompatible tree format",
                ));
            }
            if let Some(cid) = root.root.clone() {
                pending.push(cid);
            }
        }
        let roots = pending.len();
        let mut seen = HashSet::new();
        let mut internal_nodes = 0usize;
        let mut root_leaves = 0usize;
        let mut leaves_skipped = 0usize;
        while let Some(cid) = pending.pop() {
            if !seen.insert(cid.clone()) {
                continue;
            }
            let bytes = self
                .node_store
                .get_indexed_packed(cid.as_bytes())
                .await?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::MissingClosure,
                        "prewarm node is absent from the bounded node index",
                    )
                })?;
            let node = Node::from_bytes_with_format(&bytes, &self.format.state_tree_format)
                .map_err(|error| {
                    Error::new(
                        ErrorCode::CorruptNode,
                        format!("prewarm could not decode a Prolly node: {error}"),
                    )
                })?;
            if node.leaf {
                root_leaves = root_leaves.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "prewarm root-leaf counter overflow",
                    )
                })?;
                continue;
            }
            internal_nodes = internal_nodes.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "prewarm internal-node counter overflow",
                )
            })?;
            if node.level == 1 {
                leaves_skipped = leaves_skipped.checked_add(node.vals.len()).ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "prewarm leaf counter overflow",
                    )
                })?;
                continue;
            }
            for encoded in node.vals {
                let child = prolly::Cid(encoded.try_into().map_err(|_| {
                    Error::new(
                        ErrorCode::CorruptNode,
                        "prewarm internal node contains a malformed child CID",
                    )
                })?);
                pending.push(child);
            }
        }
        Ok(InternalNodePrewarmReport {
            roots,
            internal_nodes,
            root_leaves,
            leaves_skipped,
        })
    }

    fn begin_publication_wait(&self) -> std::time::Instant {
        let depth = self
            .performance
            .publication_queue_depth
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        self.performance
            .publication_max_queue_depth
            .fetch_max(depth, Ordering::Relaxed);
        std::time::Instant::now()
    }

    fn finish_publication_wait(&self, started: std::time::Instant) {
        self.performance
            .publication_queue_depth
            .fetch_sub(1, Ordering::Relaxed);
        self.performance
            .publication_acquisitions
            .fetch_add(1, Ordering::Relaxed);
        self.performance.publication_wait_nanos.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn publication_lane(&self, scope: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut lanes = self
            .publication_lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lanes.retain(|_, lane| lane.strong_count() > 0);
        if let Some(lane) = lanes.get(scope).and_then(Weak::upgrade) {
            return lane;
        }
        let lane = Arc::new(tokio::sync::Mutex::new(()));
        lanes.insert(scope.to_string(), Arc::downgrade(&lane));
        lane
    }

    async fn lock_publication_lane(&self, scope: &str) -> PublicationLaneGuard<'_> {
        let started = self.begin_publication_wait();
        // Take the keyed lane first. Otherwise multiple waiters for one branch
        // could hold read locks and unnecessarily starve global maintenance.
        let lane = self.publication_lane(scope).lock_owned().await;
        let barrier = self.publication_barrier.read().await;
        self.finish_publication_wait(started);
        PublicationLaneGuard {
            _barrier: barrier,
            _lane: lane,
        }
    }

    async fn lock_branch_publication(&self, branch: &str) -> PublicationLaneGuard<'_> {
        self.lock_publication_lane(&format!("branch:{branch}"))
            .await
    }

    async fn lock_named_publication(
        &self,
        namespace: &str,
        name: &str,
    ) -> PublicationLaneGuard<'_> {
        self.lock_publication_lane(&format!("{namespace}:{name}"))
            .await
    }

    async fn lock_global_publication(&self) -> tokio::sync::RwLockWriteGuard<'_, ()> {
        let started = self.begin_publication_wait();
        let barrier = self.publication_barrier.write().await;
        self.finish_publication_wait(started);
        barrier
    }

    async fn preserve_history_for_gc(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.publication_barrier.read().await
    }

    /// Serialize requests that reuse an idempotency key before they touch the
    /// data plane. This prevents concurrent retries in one writer process from
    /// creating duplicate, unreachable physical S3 versions.
    fn operation_lock(&self, operation: OperationId) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .operation_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(&operation).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(operation, Arc::downgrade(&lock));
        lock
    }

    async fn payload_write_permit(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.payload_writes
            .clone()
            .acquire_owned()
            .await
            .expect("repository payload-write semaphore is never closed")
    }

    pub fn repository_id(&self) -> RepositoryId {
        self.format.repository_id
    }

    pub fn format(&self) -> &RepositoryFormatV1 {
        &self.format
    }

    pub fn plane(&self) -> Arc<P> {
        self.plane.clone()
    }

    /// Persist an advisory packed-node locator. It is safe to delete: cold
    /// reads and repair can rebuild it from immutable commit envelopes.
    pub async fn create_node_index_checkpoint(
        &self,
        branch: &str,
    ) -> Result<crate::NodeIndexCheckpointV1> {
        validate_branch(branch)?;
        self.system_writer_generation("node-index-v1").await?;
        self.node_store.rebuild_node_index().await?;
        let head = self.head(branch).await?;
        let commit = self.load_commit(head).await?;
        let checkpoint = crate::NodeIndexCheckpointV1::derive(
            self.format.repository_id,
            branch.to_string(),
            head,
            commit.generation,
            self.node_store.export_node_index()?,
            self.now_millis()?,
        )?;
        checkpoint.validate()?;
        self.store_immutable(
            node_checkpoint_path(
                &self.options.repository_prefix,
                checkpoint.generation,
                checkpoint.id,
            )?,
            encode_canonical(&checkpoint)?,
        )
        .await?;
        let pointer_path = node_index_head_path(&self.options.repository_prefix)?;
        let current = self.plane.load_mutable(&pointer_path).await?;
        let pointer = crate::NodeIndexHeadV1 {
            checkpoint: checkpoint.id,
            head: checkpoint.head,
            generation: checkpoint.generation,
            updated_at_millis: self.now_millis()?,
        };
        match self
            .controls
            .compare_exchange(CompareExchange {
                path: pointer_path,
                expected: current.map(|stored| stored.metadata.token),
                bytes: encode_canonical(&pointer)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => {}
            CompareExchangeOutcome::Conflict(_) => {
                return Err(Error::new(
                    ErrorCode::RefConflict,
                    "node-index head changed while publishing a checkpoint",
                ))
            }
        }
        Ok(checkpoint)
    }

    /// Advance the scalable node-location index through a bounded page of
    /// immutable commit envelopes. Index nodes are written as an independent
    /// Prolly tree; only the small mutable head is CAS-published.
    ///
    /// Completing a scan starts a new epoch on the next invocation so commits
    /// inserted behind an earlier provider continuation are eventually seen.
    pub async fn advance_node_index_v2(
        &self,
        max_commit_objects: usize,
    ) -> Result<NodeIndexAdvanceReport> {
        if !(1..=1_000).contains(&max_commit_objects) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "node-index advance must process between 1 and 1,000 commit objects",
            ));
        }
        self.system_writer_generation("node-index-v2").await?;
        let head_path = node_index_v2_head_path(&self.options.repository_prefix)?;
        let loaded = self.plane.load_mutable(&head_path).await?;
        let expected = loaded.as_ref().map(|stored| stored.metadata.token.clone());
        let expected_format = tree_format_digest(&self.format.state_tree_format)?;
        let empty_head = NodeIndexHeadV2 {
            repository: self.format.repository_id,
            root: TreeRootV1::from_tree(&self.node_index.engine.create())?,
            generation: 0,
            scan_continuation: None,
            scan_epoch: 0,
            indexed_commit_objects: 0,
            updated_at_millis: self.now_millis()?,
        };
        let head = match loaded {
            Some(stored) => match decode_canonical::<NodeIndexHeadV2>(&stored.bytes) {
                Ok(head)
                    if head
                        .validate(self.format.repository_id, expected_format)
                        .is_ok() =>
                {
                    head
                }
                _ => empty_head.clone(),
            },
            None => empty_head,
        };
        let mut tree = Tree {
            root: head.root.root.clone(),
            config: Config {
                format: self.format.state_tree_format.clone(),
                runtime: RuntimeConfig::default(),
            },
        };
        let page = self
            .plane
            .list(ListRequest {
                prefix: format!("{}/commits/sha256/", self.options.repository_prefix),
                continuation: head.scan_continuation.clone(),
                limit: max_commit_objects,
                include_versions: false,
            })
            .await?;
        let mut indexed_node_entries = 0usize;
        let mut indexed_commit_objects = 0usize;
        for listed in page.entries {
            let encoded = listed.path.as_str().rsplit('/').next().unwrap_or_default();
            let raw = hex::decode(encoded).map_err(|_| {
                Error::new(ErrorCode::CorruptCommit, "commit path has an invalid ID")
            })?;
            let commit_id = CommitId::from_hash(raw.try_into().map_err(|_| {
                Error::new(ErrorCode::CorruptCommit, "commit ID has the wrong length")
            })?);
            let stored = self
                .plane
                .get(GetRequest {
                    path: listed.path,
                    range: None,
                    physical_version: None,
                })
                .await?
                .ok_or_else(|| {
                    Error::new(ErrorCode::MissingClosure, "listed commit disappeared")
                })?;
            let object = CommitObjectV1::decode_object(&stored.bytes)?;
            if object.commit.id()? != commit_id {
                return Err(Error::new(ErrorCode::CorruptCommit, "commit ID mismatch"));
            }
            let mut mutations = Vec::new();
            if let Some(pack) = object.node_pack.as_ref() {
                let payload_offset = CommitObjectV1::node_payload_offset(&stored.bytes)?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorCode::CorruptCommit,
                            "indexed commit node pack has no payload offset",
                        )
                    })?;
                let pack_id = pack.reference()?.id;
                mutations.reserve(pack.entries.len());
                for entry in &pack.entries {
                    let absolute_offset =
                        payload_offset.checked_add(entry.offset).ok_or_else(|| {
                            Error::new(ErrorCode::CorruptNode, "node-index offset overflow")
                        })?;
                    let location = NodeIndexEntryV1 {
                        cid: entry.cid.clone(),
                        container: commit_id,
                        pack: pack_id,
                        absolute_offset,
                        len: entry.len,
                        sha256: entry.sha256,
                    };
                    mutations.push(Mutation::Upsert {
                        key: entry.cid.as_bytes().to_vec(),
                        val: encode_canonical(&location)?,
                    });
                }
            }
            indexed_node_entries = indexed_node_entries
                .checked_add(mutations.len())
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::EntityTooLarge,
                        "node-index entry counter overflow",
                    )
                })?;
            if !mutations.is_empty() {
                tree = self.node_index.engine.batch(&tree, mutations).await?;
            }
            indexed_commit_objects += 1;
        }
        let completed_scan = page.continuation.is_none();
        let next = NodeIndexHeadV2 {
            repository: head.repository,
            root: TreeRootV1::from_tree(&tree)?,
            generation: head.generation.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "node-index generation overflow",
                )
            })?,
            scan_continuation: page.continuation,
            scan_epoch: if completed_scan {
                head.scan_epoch.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "node-index scan epoch overflow",
                    )
                })?
            } else {
                head.scan_epoch
            },
            indexed_commit_objects: head
                .indexed_commit_objects
                .checked_add(u64::try_from(indexed_commit_objects).map_err(|_| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "node-index commit count exceeds u64",
                    )
                })?)
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "node-index commit counter overflow",
                    )
                })?,
            updated_at_millis: self.now_millis()?,
        };
        match self
            .controls
            .compare_exchange(CompareExchange {
                path: head_path,
                expected,
                bytes: encode_canonical(&next)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => {
                if completed_scan {
                    self.node_store.clear_node_locations()?;
                }
                self.node_index.install_root(next.root.root.clone())?;
                self.performance
                    .node_index_advances
                    .fetch_add(1, Ordering::Relaxed);
                self.performance.node_index_entries_indexed.fetch_add(
                    u64::try_from(indexed_node_entries).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
            }
            CompareExchangeOutcome::Conflict(_) => {
                return Err(Error::new(
                    ErrorCode::RefConflict,
                    "node-index v2 head changed while publishing an advance",
                )
                .retry(RetryAdvice::ReloadHead));
            }
        }
        Ok(NodeIndexAdvanceReport {
            generation: next.generation,
            scan_epoch: next.scan_epoch,
            indexed_commit_objects,
            indexed_node_entries,
            completed_scan,
        })
    }

    /// Advances the rebuildable ref catalog by one bounded provider page.
    /// Branch and tag objects remain authoritative; this tree serves only
    /// scalable, ordered enumeration.
    pub async fn advance_ref_catalog_v2(
        &self,
        max_ref_objects: usize,
    ) -> Result<RefCatalogAdvanceReport> {
        if !(1..=1_000).contains(&max_ref_objects) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "ref-catalog advance must process between 1 and 1,000 refs",
            ));
        }
        self.system_writer_generation("ref-catalog-v2").await?;
        let head_path = ref_catalog_v2_head_path(&self.options.repository_prefix)?;
        let loaded = self.plane.load_mutable(&head_path).await?;
        let expected = loaded.as_ref().map(|stored| stored.metadata.token.clone());
        let expected_format = tree_format_digest(&self.format.state_tree_format)?;
        let head = match loaded {
            Some(stored) => match decode_canonical::<RefCatalogHeadV2>(&stored.bytes) {
                Ok(head)
                    if head
                        .validate(self.format.repository_id, expected_format)
                        .is_ok() =>
                {
                    head
                }
                _ => RefCatalogHeadV2 {
                    repository: self.format.repository_id,
                    root: TreeRootV1::from_tree(&self.ref_catalog.engine.create())?,
                    generation: 0,
                    scanning_tags: false,
                    scan_continuation: None,
                    scan_epoch: 0,
                    indexed_ref_objects: 0,
                    updated_at_millis: self.now_millis()?,
                },
            },
            None => RefCatalogHeadV2 {
                repository: self.format.repository_id,
                root: TreeRootV1::from_tree(&self.ref_catalog.engine.create())?,
                generation: 0,
                scanning_tags: false,
                scan_continuation: None,
                scan_epoch: 0,
                indexed_ref_objects: 0,
                updated_at_millis: self.now_millis()?,
            },
        };
        let mut tree = Tree {
            root: head.root.root.clone(),
            config: Config {
                format: self.format.state_tree_format.clone(),
                runtime: RuntimeConfig::default(),
            },
        };
        let namespace = if head.scanning_tags { "tags" } else { "heads" };
        let prefix = format!("{}/refs/{namespace}/", self.options.repository_prefix);
        let page = self
            .plane
            .list(ListRequest {
                prefix: prefix.clone(),
                continuation: head.scan_continuation.clone(),
                limit: max_ref_objects,
                include_versions: false,
            })
            .await?;
        let mut mutations = Vec::with_capacity(page.entries.len());
        for listed in &page.entries {
            let encoded = listed.path.as_str().strip_prefix(&prefix).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "ref scan escaped its prefix")
            })?;
            let name = String::from_utf8(hex::decode(encoded).map_err(|_| {
                Error::new(ErrorCode::CorruptCommit, "ref path is not canonical hex")
            })?)
            .map_err(|_| Error::new(ErrorCode::CorruptCommit, "ref name is not UTF-8"))?;
            validate_branch(&name)?;
            let stored = self
                .plane
                .load_mutable(&listed.path)
                .await?
                .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "listed ref disappeared"))?;
            let key = ref_catalog_key(head.scanning_tags, &name);
            if head.scanning_tags {
                let value: crate::TagValueV1 = decode_canonical(&stored.bytes)?;
                if value.tombstone {
                    mutations.push(Mutation::Delete { key });
                } else {
                    mutations.push(Mutation::Upsert {
                        key,
                        val: encode_canonical(&RefCatalogEntryV2::Tag {
                            target: value.target,
                            generation: value.generation,
                        })?,
                    });
                }
            } else {
                let value: crate::RefValueV1 = decode_canonical(&stored.bytes)?;
                if value.tombstone {
                    mutations.push(Mutation::Delete { key });
                } else {
                    mutations.push(Mutation::Upsert {
                        key,
                        val: encode_canonical(&RefCatalogEntryV2::Branch {
                            target: value.target,
                            generation: value.generation,
                        })?,
                    });
                }
            }
        }
        if !mutations.is_empty() {
            tree = self.ref_catalog.engine.batch(&tree, mutations).await?;
        }
        let namespace_complete = page.continuation.is_none();
        let completed_scan = head.scanning_tags && namespace_complete;
        let next = RefCatalogHeadV2 {
            repository: head.repository,
            root: TreeRootV1::from_tree(&tree)?,
            generation: head.generation.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "ref-catalog generation overflow",
                )
            })?,
            scanning_tags: if namespace_complete {
                !head.scanning_tags
            } else {
                head.scanning_tags
            },
            scan_continuation: if namespace_complete {
                None
            } else {
                page.continuation
            },
            scan_epoch: if completed_scan {
                head.scan_epoch.checked_add(1).ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "ref-catalog epoch overflow")
                })?
            } else {
                head.scan_epoch
            },
            indexed_ref_objects: head
                .indexed_ref_objects
                .checked_add(u64::try_from(page.entries.len()).map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "ref count exceeds u64")
                })?)
                .ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "ref-catalog counter overflow")
                })?,
            updated_at_millis: self.now_millis()?,
        };
        match self
            .controls
            .compare_exchange(CompareExchange {
                path: head_path,
                expected,
                bytes: encode_canonical(&next)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => {
                self.ref_catalog.install_root(next.root.root.clone())?;
            }
            CompareExchangeOutcome::Conflict(_) => {
                return Err(Error::new(
                    ErrorCode::RefConflict,
                    "ref-catalog head changed while publishing an advance",
                )
                .retry(RetryAdvice::ReloadHead));
            }
        }
        Ok(RefCatalogAdvanceReport {
            freshness: IndexFreshness {
                generation: next.generation,
                scan_epoch: next.scan_epoch,
                updated_at_millis: next.updated_at_millis,
            },
            indexed_ref_objects: page.entries.len(),
            completed_scan,
        })
    }

    /// Advances commit generation and binary-lifting metadata through one
    /// bounded commit-object page. Missing higher jumps are filled by later
    /// scan epochs after their ancestors have been indexed.
    pub async fn advance_commit_graph_v2(
        &self,
        max_commit_objects: usize,
    ) -> Result<CommitGraphAdvanceReport> {
        if !(1..=1_000).contains(&max_commit_objects) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "commit-graph advance must process between 1 and 1,000 commits",
            ));
        }
        self.system_writer_generation("commit-graph-v2").await?;
        let head_path = commit_graph_v2_head_path(&self.options.repository_prefix)?;
        let loaded = self.plane.load_mutable(&head_path).await?;
        let expected = loaded.as_ref().map(|stored| stored.metadata.token.clone());
        let expected_format = tree_format_digest(&self.format.state_tree_format)?;
        let head = match loaded {
            Some(stored) => match decode_canonical::<CommitGraphHeadV2>(&stored.bytes) {
                Ok(head)
                    if head
                        .validate(self.format.repository_id, expected_format)
                        .is_ok() =>
                {
                    head
                }
                _ => CommitGraphHeadV2 {
                    repository: self.format.repository_id,
                    root: TreeRootV1::from_tree(&self.commit_graph.engine.create())?,
                    generation: 0,
                    scan_continuation: None,
                    scan_epoch: 0,
                    indexed_commit_objects: 0,
                    updated_at_millis: self.now_millis()?,
                },
            },
            None => CommitGraphHeadV2 {
                repository: self.format.repository_id,
                root: TreeRootV1::from_tree(&self.commit_graph.engine.create())?,
                generation: 0,
                scan_continuation: None,
                scan_epoch: 0,
                indexed_commit_objects: 0,
                updated_at_millis: self.now_millis()?,
            },
        };
        let mut tree = Tree {
            root: head.root.root.clone(),
            config: Config {
                format: self.format.state_tree_format.clone(),
                runtime: RuntimeConfig::default(),
            },
        };
        let page = self
            .plane
            .list(ListRequest {
                prefix: format!("{}/commits/sha256/", self.options.repository_prefix),
                continuation: head.scan_continuation.clone(),
                limit: max_commit_objects,
                include_versions: false,
            })
            .await?;
        for listed in &page.entries {
            let commit_id = commit_id_from_path(&listed.path)?;
            let commit = self.load_commit(commit_id).await?;
            let mut jumps = Vec::new();
            if let Some(first_parent) = commit.parents.first().copied() {
                jumps.push(first_parent);
                for level in 1..64usize {
                    let ancestor = jumps[level - 1];
                    let Some(encoded) = self
                        .commit_graph
                        .engine
                        .get(&tree, ancestor.as_bytes())
                        .await?
                    else {
                        break;
                    };
                    let entry: CommitGraphEntryV2 = decode_canonical(&encoded)?;
                    let Some(next) = entry.first_parent_jumps.get(level - 1).copied() else {
                        break;
                    };
                    jumps.push(next);
                }
            }
            let entry = CommitGraphEntryV2 {
                commit: commit_id,
                generation: commit.generation,
                parents: commit.parents,
                first_parent_jumps: jumps,
            };
            tree = self
                .commit_graph
                .engine
                .batch(
                    &tree,
                    vec![Mutation::Upsert {
                        key: commit_id.as_bytes().to_vec(),
                        val: encode_canonical(&entry)?,
                    }],
                )
                .await?;
        }
        let completed_scan = page.continuation.is_none();
        let next = CommitGraphHeadV2 {
            repository: head.repository,
            root: TreeRootV1::from_tree(&tree)?,
            generation: head.generation.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "commit-graph generation overflow",
                )
            })?,
            scan_continuation: page.continuation,
            scan_epoch: if completed_scan {
                head.scan_epoch.checked_add(1).ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "commit-graph epoch overflow")
                })?
            } else {
                head.scan_epoch
            },
            indexed_commit_objects: head
                .indexed_commit_objects
                .checked_add(u64::try_from(page.entries.len()).map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "commit count exceeds u64")
                })?)
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "commit-graph counter overflow",
                    )
                })?,
            updated_at_millis: self.now_millis()?,
        };
        match self
            .controls
            .compare_exchange(CompareExchange {
                path: head_path,
                expected,
                bytes: encode_canonical(&next)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => {
                self.commit_graph.install_root(next.root.root.clone())?;
            }
            CompareExchangeOutcome::Conflict(_) => {
                return Err(Error::new(
                    ErrorCode::RefConflict,
                    "commit-graph head changed while publishing an advance",
                )
                .retry(RetryAdvice::ReloadHead));
            }
        }
        Ok(CommitGraphAdvanceReport {
            freshness: IndexFreshness {
                generation: next.generation,
                scan_epoch: next.scan_epoch,
                updated_at_millis: next.updated_at_millis,
            },
            indexed_commit_objects: page.entries.len(),
            completed_scan,
        })
    }

    async fn load_latest_node_index_checkpoint(&self) -> Result<()> {
        if let Some(stored) = self
            .plane
            .load_mutable(&node_index_v2_head_path(&self.options.repository_prefix)?)
            .await?
        {
            if let Ok(head) = decode_canonical::<NodeIndexHeadV2>(&stored.bytes) {
                let expected_format = tree_format_digest(&self.format.state_tree_format)?;
                if head
                    .validate(self.format.repository_id, expected_format)
                    .is_ok()
                {
                    self.node_index.install_root(head.root.root)?;
                    return Ok(());
                }
            }
        }
        let Some(head_object) = self
            .plane
            .load_mutable(&node_index_head_path(&self.options.repository_prefix)?)
            .await?
        else {
            return Ok(());
        };
        let head = match decode_canonical::<crate::NodeIndexHeadV1>(&head_object.bytes) {
            Ok(head) => head,
            Err(_) => return Ok(()),
        };
        let checkpoint = self
            .plane
            .get(GetRequest {
                path: node_checkpoint_path(
                    &self.options.repository_prefix,
                    head.generation,
                    head.checkpoint,
                )?,
                range: None,
                physical_version: None,
            })
            .await?;
        let Some(checkpoint) = checkpoint else {
            return Ok(());
        };
        let checkpoint = match decode_canonical::<crate::NodeIndexCheckpointV1>(&checkpoint.bytes) {
            Ok(checkpoint) => checkpoint,
            Err(_) => return Ok(()),
        };
        if checkpoint.repository != self.format.repository_id
            || checkpoint.validate().is_err()
            || head.validate(&checkpoint).is_err()
        {
            return Ok(());
        }
        self.node_store.import_node_index(&checkpoint.entries)
    }

    async fn load_latest_scale_metadata(&self) -> Result<()> {
        let expected_format = tree_format_digest(&self.format.state_tree_format)?;
        if let Some(stored) = self
            .plane
            .load_mutable(&ref_catalog_v2_head_path(&self.options.repository_prefix)?)
            .await?
        {
            if let Ok(head) = decode_canonical::<RefCatalogHeadV2>(&stored.bytes) {
                if head
                    .validate(self.format.repository_id, expected_format)
                    .is_ok()
                {
                    self.ref_catalog.install_root(head.root.root)?;
                }
            }
        }
        if let Some(stored) = self
            .plane
            .load_mutable(&commit_graph_v2_head_path(&self.options.repository_prefix)?)
            .await?
        {
            if let Ok(head) = decode_canonical::<CommitGraphHeadV2>(&stored.bytes) {
                if head
                    .validate(self.format.repository_id, expected_format)
                    .is_ok()
                {
                    self.commit_graph.install_root(head.root.root)?;
                }
            }
        }
        Ok(())
    }

    fn now_millis(&self) -> Result<u64> {
        self.options.clock.now_millis()
    }

    async fn authority_generation(&self, scope: crate::AuthorityScopeV2) -> Result<u64> {
        if self.options.read_only {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "repository was opened read-only",
            ));
        }
        let now = self.now_millis()?;
        let cached = self
            .authority_permits
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "authority cache poisoned"))?
            .get(&scope)
            .cloned();
        let permit = match cached {
            Some(permit) if permit.expires_at_millis() > now => permit,
            _ => {
                // Cold acquisition and renewal are rare control-plane work.
                // Serialize only those transitions, then let steady-state
                // validation and branch publication proceed independently.
                let _renewal = self.authority_renewal.lock().await;
                let current = self
                    .authority_permits
                    .read()
                    .map_err(|_| {
                        Error::new(ErrorCode::InternalInvariant, "authority cache poisoned")
                    })?
                    .get(&scope)
                    .cloned();
                let permit = match current {
                    Some(permit) if permit.expires_at_millis() > now => permit,
                    Some(permit) => self.shard_authority.renew(permit, now).await?,
                    None => {
                        self.shard_authority
                            .acquire(
                                scope.clone(),
                                &self.options.writer,
                                now,
                                self.new_operation(),
                            )
                            .await?
                    }
                };
                self.authority_permits
                    .write()
                    .map_err(|_| {
                        Error::new(ErrorCode::InternalInvariant, "authority cache poisoned")
                    })?
                    .insert(scope.clone(), permit.clone());
                permit
            }
        };
        match self.shard_authority.validate_active(&permit, now).await {
            Ok(stamp) => Ok(stamp.generation),
            Err(error) => {
                self.authority_permits
                    .write()
                    .map_err(|_| {
                        Error::new(ErrorCode::InternalInvariant, "authority cache poisoned")
                    })?
                    .remove(&scope);
                Err(error)
            }
        }
    }

    async fn branch_writer_generation(&self, branch: &str) -> Result<u64> {
        validate_branch(branch)?;
        self.authority_generation(crate::AuthorityScopeV2::Branch {
            name: branch.to_string(),
        })
        .await
    }

    async fn system_writer_generation(&self, namespace: &str) -> Result<u64> {
        self.authority_generation(crate::AuthorityScopeV2::System {
            namespace: namespace.to_string(),
        })
        .await
    }

    #[allow(dead_code)] // Legacy v1 migration adapter; normal opens use sharded authority.
    async fn acquire_physical_writer(&mut self) -> Result<()> {
        if self.options.read_only {
            return Ok(());
        }
        let path = writer_lease_path(&self.options.repository_prefix)?;
        let now = self.now_millis()?;
        let expires_at_millis = now
            .checked_add(self.options.writer_lease_millis)
            .ok_or_else(|| Error::new(ErrorCode::InvalidLimit, "writer lease expiry overflow"))?;
        let existing = self.plane.load_mutable(&path).await?;
        let (next, expected) = match existing {
            None => {
                let operation = self.new_operation();
                let fencing_token = crate::codec::sha256(
                    &[
                        self.format.repository_id.as_bytes().as_slice(),
                        self.options.writer.as_bytes(),
                        operation.as_bytes().as_slice(),
                    ]
                    .concat(),
                );
                (
                    crate::ExclusiveWriterLeaseV1 {
                        repository: self.format.repository_id,
                        writer_id: self.options.writer.clone(),
                        generation: 1,
                        fencing_token,
                        expires_at_millis,
                        updated_at_millis: now,
                    },
                    None,
                )
            }
            Some(stored) => {
                let current: crate::ExclusiveWriterLeaseV1 = decode_canonical(&stored.bytes)?;
                current.validate(self.format.repository_id)?;
                if current.writer_id != self.options.writer {
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "physical repository is owned by another writer; takeover requires an explicit credential-isolated handoff",
                    ));
                }
                if current.expires_at_millis <= now {
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "physical writer lease expired; automatic reacquisition is forbidden",
                    ));
                }
                let mut renewed = current;
                renewed.updated_at_millis = now;
                renewed.expires_at_millis = expires_at_millis;
                (renewed, Some(stored.metadata.token))
            }
        };
        match self
            .controls
            .compare_exchange(CompareExchange {
                path,
                expected,
                bytes: encode_canonical(&next)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(metadata) => {
                *self.writer_lease.write().map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "writer-lease lock poisoned")
                })? = Some(HeldWriterLease {
                    value: next,
                    token: metadata.token,
                });
                Ok(())
            }
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::PreconditionFailed,
                "physical writer lease changed during acquisition",
            )),
        }
    }

    /// Renew every cached branch/system authority permit. The repository-wide
    /// v1 lease is renewed only when this instance was opened through the
    /// legacy migration adapter.
    pub async fn renew_shard_authorities(&self) -> Result<()> {
        let _authority_renewal = self.authority_renewal.lock().await;
        if self
            .writer_lease
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "writer-lease lock poisoned"))?
            .is_none()
        {
            let now = self.now_millis()?;
            let permits = self
                .authority_permits
                .read()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "authority cache poisoned"))?
                .clone();
            for (scope, permit) in permits {
                let renewed = match self.shard_authority.renew(permit, now).await {
                    Ok(renewed) => renewed,
                    Err(error) => {
                        self.authority_permits
                            .write()
                            .map_err(|_| {
                                Error::new(ErrorCode::InternalInvariant, "authority cache poisoned")
                            })?
                            .remove(&scope);
                        return Err(error);
                    }
                };
                self.authority_permits
                    .write()
                    .map_err(|_| {
                        Error::new(ErrorCode::InternalInvariant, "authority cache poisoned")
                    })?
                    .insert(scope, renewed);
            }
            return Ok(());
        }
        let _renewal = self.lease_renewal.lock().await;
        self.renew_writer_lease_inner().await
    }

    #[deprecated(
        since = "0.1.0",
        note = "use renew_shard_authorities; this name is retained for source compatibility"
    )]
    pub async fn renew_writer_lease(&self) -> Result<()> {
        self.renew_shard_authorities().await
    }

    async fn renew_writer_lease_inner(&self) -> Result<()> {
        let held = self
            .writer_lease
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "writer-lease lock poisoned"))?
            .clone()
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::PreconditionFailed,
                    "repository was opened read-only or has no writer lease",
                )
            })?;
        let now = self.now_millis()?;
        if held.value.expires_at_millis <= now {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "physical writer lease expired; publication is fenced",
            ));
        }
        let mut renewed = held.value;
        renewed.updated_at_millis = now;
        renewed.expires_at_millis = now
            .checked_add(self.options.writer_lease_millis)
            .ok_or_else(|| Error::new(ErrorCode::InvalidLimit, "writer lease expiry overflow"))?;
        let renewal = self
            .controls
            .compare_exchange(CompareExchange {
                path: writer_lease_path(&self.options.repository_prefix)?,
                expected: Some(held.token),
                bytes: encode_canonical(&renewed)?,
            })
            .await;
        match renewal {
            Ok(CompareExchangeOutcome::Applied(metadata)) => {
                *self.writer_lease.write().map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "writer-lease lock poisoned")
                })? = Some(HeldWriterLease {
                    value: renewed,
                    token: metadata.token,
                });
                Ok(())
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => {
                *self.writer_lease.write().map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "writer-lease lock poisoned")
                })? = None;
                Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "physical writer lease was lost; publication is fenced",
                ))
            }
            Err(error) => {
                *self.writer_lease.write().map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "writer-lease lock poisoned")
                })? = None;
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("writer lease renewal outcome is unknown; writer is fenced: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation))
            }
        }
    }

    /// Run independent shard-authority renewal until the returned handle is
    /// dropped. A failed or ambiguous renewal fences the affected writer
    /// instance before the task exits.
    pub fn start_shard_authority_maintenance(
        self: &Arc<Self>,
    ) -> Result<ShardAuthorityMaintenance> {
        if self.options.read_only {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "shard-authority maintenance requires a writable repository",
            ));
        }
        let interval = Duration::from_millis((self.options.writer_lease_millis / 3).max(100));
        let weak = Arc::downgrade(self);
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(repository) = weak.upgrade() else {
                    break;
                };
                if repository.renew_shard_authorities().await.is_err() {
                    break;
                }
            }
        });
        Ok(ShardAuthorityMaintenance { task })
    }

    #[deprecated(
        since = "0.1.0",
        note = "use start_shard_authority_maintenance; this name is retained for source compatibility"
    )]
    pub fn start_writer_lease_maintenance(self: &Arc<Self>) -> Result<ShardAuthorityMaintenance> {
        self.start_shard_authority_maintenance()
    }

    /// Continuously advance the rebuildable v2 node index outside foreground
    /// publication. The task is single-writer friendly but remains safe if a
    /// second worker races it because the index head is CAS-protected.
    pub fn start_node_index_maintenance(
        self: &Arc<Self>,
        interval: Duration,
        max_commit_objects: usize,
    ) -> Result<NodeIndexMaintenance> {
        if self.options.read_only {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "node-index maintenance requires a writable repository",
            ));
        }
        if interval.is_zero() || !(1..=1_000).contains(&max_commit_objects) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "node-index maintenance requires a nonzero interval and a 1..=1,000 commit batch",
            ));
        }
        let weak = Arc::downgrade(self);
        let task = tokio::spawn(async move {
            loop {
                // Keep derived-index traffic out of the foreground publication
                // window. Operators that need an immediate catch-up can call
                // `advance_node_index_v2` explicitly.
                tokio::time::sleep(interval).await;
                let Some(repository) = weak.upgrade() else {
                    break;
                };
                let node = repository.advance_node_index_v2(max_commit_objects).await;
                let refs = repository.advance_ref_catalog_v2(max_commit_objects).await;
                let graph = repository.advance_commit_graph_v2(max_commit_objects).await;
                if node.is_err() || refs.is_err() || graph.is_err() {
                    repository
                        .performance
                        .node_index_advance_errors
                        .fetch_add(1, Ordering::Relaxed);
                }
                drop(repository);
            }
        });
        Ok(NodeIndexMaintenance { task })
    }

    /// Explicitly take over an expired or credential-revoked physical writer.
    /// The caller must have independently stopped/revoked the old writer; S3
    /// cannot make ref CAS conditional on this separate lease object.
    pub async fn takeover_branch_writer(
        &mut self,
        branch: &str,
        expected_writer: &str,
        expected_generation: u64,
        handoff_evidence: &str,
    ) -> Result<u64> {
        if !self.options.read_only || handoff_evidence.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "branch takeover requires a read-only open and credential-isolation evidence",
            ));
        }
        validate_branch(branch)?;
        let scope = crate::AuthorityScopeV2::Branch {
            name: branch.to_string(),
        };
        let pending = self
            .shard_authority
            .begin_takeover(crate::TakeoverRequestV2 {
                scope: scope.clone(),
                expected_writer: expected_writer.to_string(),
                expected_generation,
                next_writer: self.options.writer.clone(),
                handoff_evidence: handoff_evidence.to_string(),
                now_millis: self.now_millis()?,
                nonce: self.new_operation(),
            })
            .await?;
        let stamp = pending.stamp();
        let _publication = self.lock_branch_publication(branch).await;
        let loaded = self.load_ref_including_tombstone(branch).await?;
        if loaded.value.writer_fence_generation != expected_generation
            && loaded.value.writer_fence_generation != stamp.generation
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "branch ref fence does not match the takeover expectation",
            ));
        }
        if loaded.value.writer_fence_generation != stamp.generation {
            let operation = self.new_operation();
            let created_at_millis = self.now_millis()?;
            let reflog = ReflogEntryV1 {
                branch: branch.to_string(),
                old_target: Some(loaded.value.target),
                new_target: loaded.value.target,
                operation,
                actor: self.options.writer.clone(),
                message: format!("branch writer takeover: {}", handoff_evidence.trim()),
                created_at_millis,
            };
            let mut barrier = loaded.value;
            barrier.previous_target = Some(barrier.target);
            barrier.generation =
                RefGeneration(barrier.generation.0.checked_add(1).ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "ref generation overflow")
                })?);
            barrier.operation = operation;
            barrier.reflog = reflog.id()?;
            barrier.inline_reflog = reflog;
            barrier.writer = self.options.writer.clone();
            barrier.updated_at_millis = created_at_millis;
            barrier.writer_fence_generation = stamp.generation;
            match self
                .controls
                .compare_exchange(CompareExchange {
                    path: branch_path(&self.options.repository_prefix, branch)?,
                    expected: Some(loaded.token),
                    bytes: encode_canonical(&barrier)?,
                })
                .await?
            {
                CompareExchangeOutcome::Applied(_) => {}
                CompareExchangeOutcome::Conflict(_) => {
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "branch changed during writer takeover barrier",
                    ));
                }
            }
        }
        let active = self
            .shard_authority
            .activate_after_barrier(
                pending,
                crate::BranchRefBarrierV2::new(stamp.clone()),
                self.now_millis()?,
            )
            .await?;
        self.authority_permits
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "authority cache poisoned"))?
            .insert(scope, active);
        drop(_publication);
        self.options.read_only = false;
        self.invalidate_branch_cache(branch)?;
        Ok(stamp.generation)
    }

    /// Legacy repository-wide takeover adapter. New deployments should call
    /// `takeover_branch_writer` independently for each authority shard.
    #[deprecated(
        since = "0.1.0",
        note = "use takeover_branch_writer independently for each branch authority scope"
    )]
    pub async fn takeover_physical_writer(
        &mut self,
        expected_writer: &str,
        expected_generation: u64,
        handoff_evidence: &str,
    ) -> Result<u64> {
        if !self.options.read_only || handoff_evidence.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "takeover requires a read-only open and non-empty credential-isolation evidence",
            ));
        }
        let _publication = self.publication_barrier.clone().write_owned().await;
        let path = writer_lease_path(&self.options.repository_prefix)?;
        let stored = self.plane.load_mutable(&path).await?.ok_or_else(|| {
            Error::new(
                ErrorCode::MissingClosure,
                "physical writer lease is missing",
            )
        })?;
        let current: crate::ExclusiveWriterLeaseV1 = decode_canonical(&stored.bytes)?;
        current.validate(self.format.repository_id)?;
        let next_generation = expected_generation.checked_add(1).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "writer generation overflow")
        })?;
        let (next, token) = if current.writer_id == self.options.writer
            && current.generation == next_generation
        {
            // Resume a barrier that acquired the lease but did not finish all refs.
            (current, stored.metadata.token)
        } else {
            if current.writer_id != expected_writer || current.generation != expected_generation {
                return Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "writer lease does not match the explicit takeover expectation",
                ));
            }
            let now = self.now_millis()?;
            let operation = self.new_operation();
            let fencing_token = crate::codec::sha256(
                &[
                    self.format.repository_id.as_bytes().as_slice(),
                    self.options.writer.as_bytes(),
                    operation.as_bytes().as_slice(),
                    handoff_evidence.as_bytes(),
                ]
                .concat(),
            );
            let next = crate::ExclusiveWriterLeaseV1 {
                repository: self.format.repository_id,
                writer_id: self.options.writer.clone(),
                generation: next_generation,
                fencing_token,
                expires_at_millis: now
                    .checked_add(self.options.writer_lease_millis)
                    .ok_or_else(|| {
                        Error::new(ErrorCode::InvalidLimit, "writer lease expiry overflow")
                    })?,
                updated_at_millis: now,
            };
            let token = match self
                .controls
                .compare_exchange(CompareExchange {
                    path: path.clone(),
                    expected: Some(stored.metadata.token),
                    bytes: encode_canonical(&next)?,
                })
                .await?
            {
                CompareExchangeOutcome::Applied(metadata) => metadata.token,
                CompareExchangeOutcome::Conflict(_) => {
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "writer lease changed during explicit takeover",
                    ))
                }
            };
            (next, token)
        };

        for branch in self.list_branches().await? {
            let loaded = self.load_ref_including_tombstone(&branch.name).await?;
            let observed_fence = loaded.value.writer_fence_generation;
            if observed_fence == 0 {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "branch ref has a zero writer fence",
                ));
            }
            if observed_fence == next_generation {
                continue;
            }
            if observed_fence > expected_generation {
                return Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "branch ref carries a writer fence newer than the takeover expectation",
                ));
            }
            let operation = self.new_operation();
            let now = self.now_millis()?;
            let reflog = ReflogEntryV1 {
                branch: branch.name.clone(),
                old_target: Some(loaded.value.target),
                new_target: loaded.value.target,
                operation,
                actor: self.options.writer.clone(),
                message: format!("writer takeover: {}", handoff_evidence.trim()),
                created_at_millis: now,
            };
            let mut barrier = loaded.value;
            barrier.previous_target = Some(barrier.target);
            barrier.generation =
                RefGeneration(barrier.generation.0.checked_add(1).ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "ref generation overflow")
                })?);
            barrier.operation = operation;
            barrier.reflog = reflog.id()?;
            barrier.inline_reflog = reflog;
            barrier.writer = self.options.writer.clone();
            barrier.updated_at_millis = now;
            barrier.writer_fence_generation = next_generation;
            match self
                .controls
                .compare_exchange(CompareExchange {
                    path: branch_path(&self.options.repository_prefix, &branch.name)?,
                    expected: Some(loaded.token),
                    bytes: encode_canonical(&barrier)?,
                })
                .await?
            {
                CompareExchangeOutcome::Applied(_) => {}
                CompareExchangeOutcome::Conflict(_) => {
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "branch changed during writer takeover barrier",
                    ))
                }
            }
        }
        *self.writer_lease.write().map_err(|_| {
            Error::new(ErrorCode::InternalInvariant, "writer-lease lock poisoned")
        })? = Some(HeldWriterLease { value: next, token });
        self.options.read_only = false;
        self.warm_branches
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "branch-cache lock poisoned"))?
            .clear();
        Ok(next_generation)
    }

    fn cache_branch(
        &self,
        branch: &str,
        reference: crate::RefValueV1,
        token: StorageToken,
        commit: BucketCommitV1,
    ) -> Result<()> {
        self.warm_branches
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "branch-cache lock poisoned"))?
            .insert(
                branch.to_string(),
                WarmBranchState {
                    reference,
                    token,
                    commit,
                },
            );
        Ok(())
    }

    fn invalidate_branch_cache(&self, branch: &str) -> Result<()> {
        self.warm_branches
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "branch-cache lock poisoned"))?
            .remove(&branch.to_string());
        Ok(())
    }

    /// Delete obsolete physical versions of one branch-ref object while
    /// retaining the current CAS token. Immutable commits remain the logical
    /// history and are never removed by this maintenance operation.
    pub async fn compact_branch_ref_versions(
        &self,
        branch: &str,
    ) -> Result<RefVersionCompactionReport> {
        validate_branch(branch)?;
        self.branch_writer_generation(branch).await?;
        let _publication = self.lock_branch_publication(branch).await;
        let loaded = self.load_ref_including_tombstone(branch).await?;
        self.compact_branch_ref_versions_inner(branch, &loaded)
            .await
    }

    async fn maybe_compact_branch_ref_versions(
        &self,
        branch: &str,
        loaded: &LoadedRef,
    ) -> Result<()> {
        let interval = self.options.branch_ref_compaction_interval;
        if interval != 0
            && loaded.value.generation.0 != 0
            && loaded.value.generation.0.is_multiple_of(interval)
        {
            self.compact_branch_ref_versions_inner(branch, loaded)
                .await?;
        }
        Ok(())
    }

    async fn compact_branch_ref_versions_inner(
        &self,
        branch: &str,
        _loaded: &LoadedRef,
    ) -> Result<RefVersionCompactionReport> {
        let path = branch_path(&self.options.repository_prefix, branch)?;
        let report = self
            .controls
            .compact_path_with_retention(&path, self.options.branch_ref_versions_to_retain)
            .await?;
        Ok(RefVersionCompactionReport {
            scanned: report.scanned,
            retained: report.retained,
            deleted: report.deleted,
            already_missing: report.already_missing,
        })
    }

    async fn warm_branch_state(&self, branch: &str) -> Result<WarmBranchState> {
        if let Some(cached) = self
            .warm_branches
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "branch-cache lock poisoned"))?
            .get(&branch.to_string())
        {
            return Ok(cached);
        }
        let loaded = self.load_ref(branch).await?;
        let commit = self.load_commit(loaded.value.target).await?;
        let state = WarmBranchState {
            reference: loaded.value,
            token: loaded.token,
            commit,
        };
        self.warm_branches
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "branch-cache lock poisoned"))?
            .insert(branch.to_string(), state.clone());
        Ok(state)
    }

    async fn replay_warm_operation(
        &self,
        branch: &str,
        operation: OperationId,
        input_digest: [u8; 32],
    ) -> Result<Option<CommitReceipt>> {
        let warm = self.warm_branch_state(branch).await?;
        let operations = self.tree_from_root(
            &warm.commit.state.operations,
            &self.format.state_tree_format,
        )?;
        let Some(value) = self.engine.get(&operations, operation.as_bytes()).await? else {
            return Ok(None);
        };
        let record: OperationRecordV1 = decode_canonical(&value)?;
        if record.input_digest != input_digest {
            return Err(Error::new(
                ErrorCode::IdempotencyConflict,
                "operation ID was already used with different input",
            )
            .operation(operation.to_string()));
        }
        Ok(Some(CommitReceipt {
            id: warm.reference.target,
            operation,
            branch: branch.to_string(),
            parents: warm.commit.parents,
            changed_keys: record.result.changed_keys,
            object_versions: record.result.object_versions,
            idempotent_replay: true,
        }))
    }

    fn new_operation(&self) -> OperationId {
        self.options.ids.operation()
    }

    fn new_batch(&self) -> BatchId {
        self.options.ids.batch()
    }

    /// Replay the complete logical history into an empty prolly-s3
    /// destination. Provider attestations and maintenance state remain local.
    pub async fn clone_to<Q: ObjectPlane>(
        &self,
        destination: Arc<Q>,
        destination_prefix: &str,
    ) -> Result<CloneReport> {
        self.clone_physical_to(destination, destination_prefix)
            .await
    }

    async fn clone_physical_to<Q: ObjectPlane>(
        &self,
        destination: Arc<Q>,
        destination_prefix: &str,
    ) -> Result<CloneReport> {
        // A transfer may walk roots that source GC would otherwise collect.
        // This read barrier permits ordinary publications but excludes sweep.
        let _source_history = self.preserve_history_for_gc().await;
        let format_path = format_path(destination_prefix)?;
        let existing_format = destination
            .get(GetRequest {
                path: format_path.clone(),
                range: None,
                physical_version: None,
            })
            .await?;
        let format_created = if let Some(existing) = existing_format {
            let format = decode_repository_format(&existing.bytes)?;
            if format != self.format {
                return Err(Error::new(
                    ErrorCode::RepositoryFormatConflict,
                    "physical clone destination has a different repository format",
                ));
            }
            false
        } else {
            let prefix = format!("{destination_prefix}/");
            let mut continuation = None;
            loop {
                let page = destination
                    .list(ListRequest {
                        prefix: prefix.clone(),
                        continuation,
                        limit: 1_000,
                        include_versions: false,
                    })
                    .await?;
                if page.entries.iter().any(|entry| {
                    entry
                        .path
                        .as_str()
                        .strip_prefix(&prefix)
                        .is_some_and(|relative| {
                            is_portable_clone_path(relative) || relative.starts_with("writers/")
                        })
                }) {
                    return Err(Error::new(
                        ErrorCode::RepositoryFormatConflict,
                        "physical clone destination contains repository data without a format marker",
                    ));
                }
                continuation = page.continuation;
                if continuation.is_none() {
                    break;
                }
            }
            match destination
                .compare_exchange(CompareExchange {
                    path: format_path,
                    expected: None,
                    bytes: encode_canonical(&self.format)?,
                })
                .await?
            {
                CompareExchangeOutcome::Applied(_) => true,
                CompareExchangeOutcome::Conflict(Some(existing))
                    if decode_repository_format(&existing.bytes)? == self.format =>
                {
                    false
                }
                CompareExchangeOutcome::Conflict(_) => {
                    return Err(Error::new(
                        ErrorCode::RepositoryFormatConflict,
                        "physical clone destination format was created concurrently",
                    ))
                }
            }
        };

        let mut target_options = self.options.clone();
        target_options.repository_prefix = destination_prefix.to_string();
        target_options.read_only = false;
        let target = Arc::new(Repository::<Q>::from_format(
            destination,
            target_options,
            self.format.clone(),
        )?);

        let branches = self.list_branches().await?;
        let tags = self.list_tags().await?;
        let roots = branches
            .iter()
            .map(|branch| branch.target)
            .chain(tags.iter().map(|tag| tag.target))
            .collect::<Vec<_>>();
        let (commit_map, sync) = self
            .replay_physical_history_to(target.as_ref(), &roots, false)
            .await?;
        let mut report = CloneReport {
            immutable_objects: sync.copied_objects + usize::from(format_created),
            immutable_bytes: sync.copied_bytes,
            refs: 0,
        };

        for branch in branches {
            let _publication = target.lock_branch_publication(&branch.name).await;
            let writer_fence_generation = target.branch_writer_generation(&branch.name).await?;
            let target_id = *commit_map.get(&branch.target).ok_or_else(|| {
                Error::new(
                    ErrorCode::MissingClosure,
                    "physical clone branch target was not replayed",
                )
            })?;
            let path = branch_path(destination_prefix, &branch.name)?;
            if let Some(existing) = target.plane.load_mutable(&path).await? {
                let value: crate::RefValueV1 = decode_canonical(&existing.bytes)?;
                if value.target != target_id || value.tombstone {
                    return Err(Error::new(
                        ErrorCode::RefConflict,
                        "physical clone destination branch has a divergent target",
                    ));
                }
                report.refs += 1;
                continue;
            }
            let operation = target.new_operation();
            let created_at_millis = target.now_millis()?;
            let reflog = ReflogEntryV1 {
                branch: branch.name.clone(),
                old_target: None,
                new_target: target_id,
                operation,
                actor: target.options.writer.clone(),
                message: "physical logical clone".to_string(),
                created_at_millis,
            };
            let value = crate::RefValueV1 {
                target: target_id,
                previous_target: None,
                generation: branch.generation,
                operation,
                reflog: reflog.id()?,
                writer: target.options.writer.clone(),
                updated_at_millis: created_at_millis,
                tombstone: false,
                writer_fence_generation,
                inline_reflog: reflog,
            };
            match target
                .controls
                .compare_exchange(CompareExchange {
                    path,
                    expected: None,
                    bytes: encode_canonical(&value)?,
                })
                .await?
            {
                CompareExchangeOutcome::Applied(metadata) => {
                    let commit = target.load_commit(target_id).await?;
                    target.cache_branch(&branch.name, value, metadata.token, commit)?;
                    report.refs += 1;
                }
                CompareExchangeOutcome::Conflict(Some(existing)) => {
                    let current: crate::RefValueV1 = decode_canonical(&existing.bytes)?;
                    if current.target != target_id || current.tombstone {
                        return Err(Error::new(
                            ErrorCode::RefConflict,
                            "physical clone destination branch was created concurrently",
                        ));
                    }
                    report.refs += 1;
                }
                CompareExchangeOutcome::Conflict(None) => {
                    return Err(Error::new(
                        ErrorCode::RefConflict,
                        "physical clone branch create returned an empty conflict",
                    ))
                }
            }
        }
        for tag in tags {
            let _publication = target.lock_named_publication("tag", &tag.name).await;
            target
                .system_writer_generation(&format!("tag/{}", tag.name))
                .await?;
            let target_id = *commit_map.get(&tag.target).ok_or_else(|| {
                Error::new(
                    ErrorCode::MissingClosure,
                    "physical clone tag target was not replayed",
                )
            })?;
            let path = tag_path(destination_prefix, &tag.name)?;
            if let Some(existing) = target.plane.load_mutable(&path).await? {
                let value: crate::TagValueV1 = decode_canonical(&existing.bytes)?;
                if value.target != target_id || value.tombstone {
                    return Err(Error::new(
                        ErrorCode::RefConflict,
                        "physical clone destination tag has a divergent target",
                    ));
                }
                report.refs += 1;
                continue;
            }
            let operation = target.new_operation();
            let created_at_millis = target.now_millis()?;
            let reflog = ReflogEntryV1 {
                branch: tag.name.clone(),
                old_target: None,
                new_target: target_id,
                operation,
                actor: target.options.writer.clone(),
                message: "physical logical clone tag".to_string(),
                created_at_millis,
            };
            let reflog_id = target.store_tag_reflog(&reflog).await?;
            report.immutable_objects += 1;
            let value = crate::TagValueV1 {
                target: target_id,
                previous_target: None,
                generation: RefGeneration(0),
                operation,
                reflog: reflog_id,
                writer: target.options.writer.clone(),
                created_at_millis,
                tombstone: false,
            };
            match target
                .controls
                .compare_exchange(CompareExchange {
                    path,
                    expected: None,
                    bytes: encode_canonical(&value)?,
                })
                .await?
            {
                CompareExchangeOutcome::Applied(_) => report.refs += 1,
                CompareExchangeOutcome::Conflict(Some(existing)) => {
                    let current: crate::TagValueV1 = decode_canonical(&existing.bytes)?;
                    if current.target != target_id || current.tombstone {
                        return Err(Error::new(
                            ErrorCode::RefConflict,
                            "physical clone destination tag was created concurrently",
                        ));
                    }
                    report.refs += 1;
                }
                CompareExchangeOutcome::Conflict(None) => {
                    return Err(Error::new(
                        ErrorCode::RefConflict,
                        "physical clone tag create returned an empty conflict",
                    ))
                }
            }
        }
        target.fsck().await?;
        Ok(report)
    }

    async fn clone_physical_version_binding<Q: ObjectPlane>(
        &self,
        target: &Repository<Q>,
        key: &[u8],
        version: &ObjectVersionV1,
        operation: OperationId,
        writer_fence_generation: u64,
    ) -> Result<crate::PhysicalObjectBindingV1> {
        let path = ObjectPath::new(std::str::from_utf8(key).map_err(|_| {
            Error::new(ErrorCode::CorruptCommit, "physical clone key is not UTF-8")
        })?)?;
        match (&version.body.kind, &version.binding) {
            (
                LogicalObjectVersionKindV1::Live {
                    size,
                    headers,
                    checksums,
                    user_metadata,
                    ..
                },
                crate::PhysicalObjectBindingV1::Live {
                    version_id,
                    checksum_sha256,
                    ..
                },
            ) => {
                let spool = tempfile::NamedTempFile::new().map_err(|error| {
                    Error::new(
                        ErrorCode::Transport,
                        format!("could not create physical clone spool: {error}"),
                    )
                })?;
                let source = self
                    .plane
                    .get_physical_file(crate::PhysicalFileGet {
                        path: path.clone(),
                        version_id: version_id.clone(),
                        body_path: spool.path().to_path_buf(),
                    })
                    .await?;
                if source.size != *size
                    || source.checksum_sha256 != *checksum_sha256
                    || checksums.sha256 != Some(*checksum_sha256)
                {
                    return Err(Error::new(
                        ErrorCode::ChecksumMismatch,
                        "physical clone source object failed logical checksum verification",
                    ));
                }
                let _payload_permit = target.payload_write_permit().await;
                let write = target
                    .plane
                    .put_physical_file(crate::PhysicalFilePut {
                        path,
                        body_path: spool.path().to_path_buf(),
                        size: source.size,
                        checksum_sha256: source.checksum_sha256,
                        checksum_md5: source.checksum_md5,
                        headers: headers.clone(),
                        user_metadata: user_metadata.clone(),
                        repository: target.format.repository_id,
                        operation,
                        writer_fence_generation,
                    })
                    .await?;
                if write.size != *size
                    || write.checksums.sha256 != Some(*checksum_sha256)
                    || !matches!(write.binding, crate::PhysicalObjectBindingV1::Live { .. })
                {
                    return Err(Error::new(
                        ErrorCode::ChecksumMismatch,
                        "physical clone destination object failed logical checksum verification",
                    ));
                }
                Ok(write.binding)
            }
            (
                LogicalObjectVersionKindV1::DeleteMarker,
                crate::PhysicalObjectBindingV1::DeleteMarker { .. },
            ) => {
                let _payload_permit = target.payload_write_permit().await;
                match target
                    .plane
                    .delete_physical(crate::PhysicalDelete {
                        path: path.clone(),
                        repository: target.format.repository_id,
                        operation,
                        writer_fence_generation,
                    })
                    .await
                {
                    Ok(binding) => Ok(binding),
                    Err(error) => match target.reconcile_physical_delete(&path).await? {
                        Some(binding) => Ok(binding),
                        None => Err(error),
                    },
                }
            }
            _ => Err(Error::new(
                ErrorCode::CorruptCommit,
                "physical clone source version has an invalid binding",
            )),
        }
    }

    /// Start an interruptible physical clone/fetch/push/repair transfer.
    /// Callers must keep every root reachable by a ref or retention pin until
    /// the transfer and closure cleanup have completed.
    pub async fn start_physical_transfer<Q: ObjectPlane>(
        &self,
        target: &Repository<Q>,
        roots: &[CommitId],
        force_rebind: bool,
    ) -> Result<PhysicalTransferCursor> {
        self.validate_sync_identity(target)?;
        let closure = self.start_commit_closure(roots).await?;
        Ok(PhysicalTransferCursor {
            closure,
            destination_scope: physical_transfer_destination_scope(target),
            force_rebind,
        })
    }

    /// Attach another bounded root page to an existing transfer job.
    pub async fn extend_physical_transfer(
        &self,
        cursor: &mut PhysicalTransferCursor,
        roots: &[CommitId],
    ) -> Result<()> {
        self.extend_commit_closure(&mut cursor.closure, roots).await
    }

    /// Resolve one source commit after the page containing it has completed.
    pub async fn physical_transfer_mapping(
        &self,
        cursor: &PhysicalTransferCursor,
        source: CommitId,
    ) -> Result<Option<CommitId>> {
        self.validate_commit_closure_cursor(&cursor.closure)?;
        let index = self.commit_closure_index(cursor.closure.traversal)?;
        index.install_root(cursor.closure.state.root.clone())?;
        index
            .engine
            .get(&index.tree()?, &commit_closure_mapping_key(source))
            .await?
            .map(|bytes| decode_canonical(&bytes))
            .transpose()
    }

    /// Copy one bounded parent-before-child page. Destination side effects are
    /// idempotent; the returned cursor includes their durable commit mappings
    /// and is the only cursor the caller should checkpoint.
    pub async fn physical_transfer_page<Q: ObjectPlane>(
        &self,
        target: &Repository<Q>,
        cursor: &PhysicalTransferCursor,
        max_steps: usize,
        max_commits: usize,
    ) -> Result<PhysicalTransferPage> {
        self.validate_sync_identity(target)?;
        if cursor.destination_scope != physical_transfer_destination_scope(target) {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "physical transfer cursor belongs to a different destination scope",
            ));
        }
        let page = self
            .commit_closure_page(&cursor.closure, max_steps, max_commits)
            .await?;
        let processed_commits = page.commits.len();
        let index = self.commit_closure_index(cursor.closure.traversal)?;
        index.install_root(cursor.closure.state.root.clone())?;
        let prior_tree = index.tree()?;
        let mut page_mappings = BTreeMap::new();
        let mut mutations = Vec::with_capacity(processed_commits);
        let mut sync = SyncReport::default();
        let writer_fence_generation = target.system_writer_generation("transfer").await?;
        for (source_id, source_commit) in page.commits {
            if !cursor.force_rebind {
                if let Some(destination_id) =
                    target.load_physical_transfer_mapping(source_id).await?
                {
                    match target.load_commit(destination_id).await {
                        Ok(_) => {
                            sync.already_present += 1;
                            page_mappings.insert(source_id, destination_id);
                            mutations.push(Mutation::Upsert {
                                key: commit_closure_mapping_key(source_id),
                                val: encode_canonical(&destination_id)?,
                            });
                            continue;
                        }
                        Err(error) if error.code == ErrorCode::MissingClosure => {
                            target.delete_physical_transfer_mapping(source_id).await?;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            let mut mapped_parents = Vec::with_capacity(source_commit.parents.len());
            for parent in &source_commit.parents {
                let mapped = match page_mappings.get(parent) {
                    Some(mapped) => *mapped,
                    None => index
                        .engine
                        .get(&prior_tree, &commit_closure_mapping_key(*parent))
                        .await?
                        .map(|bytes| decode_canonical(&bytes))
                        .transpose()?
                        .ok_or_else(|| {
                            Error::new(
                                ErrorCode::MissingClosure,
                                "physical transfer parent was not durably mapped",
                            )
                        })?,
                };
                mapped_parents.push(mapped);
            }
            let (destination_id, copied_objects, copied_bytes, already_present) = self
                .replay_physical_commit_to(
                    target,
                    source_id,
                    source_commit,
                    mapped_parents,
                    cursor.force_rebind,
                    writer_fence_generation,
                )
                .await?;
            sync.copied_objects += copied_objects;
            sync.copied_bytes = sync.copied_bytes.checked_add(copied_bytes).ok_or_else(|| {
                Error::new(
                    ErrorCode::EntityTooLarge,
                    "physical transfer byte count overflow",
                )
            })?;
            sync.already_present += usize::from(already_present);
            if !cursor.force_rebind {
                target
                    .store_physical_transfer_mapping(source_id, destination_id)
                    .await?;
            }
            page_mappings.insert(source_id, destination_id);
            mutations.push(Mutation::Upsert {
                key: commit_closure_mapping_key(source_id),
                val: encode_canonical(&destination_id)?,
            });
        }
        let mut next_closure = page.cursor;
        if !mutations.is_empty() {
            index.install_root(next_closure.state.root.clone())?;
            let tree = index.engine.batch(&index.tree()?, mutations).await?;
            next_closure.state = TreeRootV1::from_tree(&tree)?;
        }
        Ok(PhysicalTransferPage {
            cursor: PhysicalTransferCursor {
                closure: next_closure,
                destination_scope: cursor.destination_scope,
                force_rebind: cursor.force_rebind,
            },
            sync,
            processed_commits,
            traversal_steps: page.steps,
            complete: page.complete,
            budget_exhausted: page.budget_exhausted,
        })
    }

    async fn replay_physical_history_to<Q: ObjectPlane>(
        &self,
        target: &Repository<Q>,
        source_roots: &[CommitId],
        force_rebind: bool,
    ) -> Result<(BTreeMap<CommitId, CommitId>, SyncReport)> {
        if source_roots.is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "physical transfer requires at least one root",
            ));
        }
        let mut root_pages = source_roots.chunks(1_000);
        let mut cursor = self
            .start_physical_transfer(
                target,
                root_pages.next().expect("roots are non-empty"),
                force_rebind,
            )
            .await?;
        for roots in root_pages {
            self.extend_physical_transfer(&mut cursor, roots).await?;
        }
        let mut report = SyncReport::default();
        loop {
            let page = self
                .physical_transfer_page(target, &cursor, 4_096, 256)
                .await?;
            report.copied_objects += page.sync.copied_objects;
            report.copied_bytes = report
                .copied_bytes
                .checked_add(page.sync.copied_bytes)
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::EntityTooLarge,
                        "physical transfer byte count overflow",
                    )
                })?;
            report.already_present += page.sync.already_present;
            cursor = page.cursor;
            if page.complete {
                break;
            }
        }
        let mut root_map = BTreeMap::new();
        for source in source_roots.iter().copied().collect::<BTreeSet<_>>() {
            let destination = self
                .physical_transfer_mapping(&cursor, source)
                .await?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::MissingClosure,
                        "physical transfer root was not durably mapped",
                    )
                })?;
            root_map.insert(source, destination);
        }
        loop {
            let cleanup = self.cleanup_commit_closure(&cursor.closure, 1_000).await?;
            if cleanup.complete {
                break;
            }
        }
        Ok((root_map, report))
    }

    async fn replay_physical_commit_to<Q: ObjectPlane>(
        &self,
        target: &Repository<Q>,
        source_id: CommitId,
        source_commit: BucketCommitV1,
        mapped_parents: Vec<CommitId>,
        force_rebind: bool,
        writer_fence_generation: u64,
    ) -> Result<(CommitId, usize, u64, bool)> {
        let target_write_store = target.node_store.isolated_write_session();
        let target_engine = AsyncProlly::new(
            target_write_store.clone(),
            Config {
                format: target.format.state_tree_format.clone(),
                runtime: RuntimeConfig::default(),
            },
        );
        let base = match mapped_parents.first() {
            Some(parent) => Some(target.load_commit(*parent).await?),
            None => None,
        };
        let empty = target_engine.create();
        let mut objects = match &base {
            Some(commit) => {
                target.tree_from_root(&commit.state.objects, &target.format.state_tree_format)?
            }
            None => empty.clone(),
        };
        let mut versions = match &base {
            Some(commit) => {
                target.tree_from_root(&commit.state.versions, &target.format.state_tree_format)?
            }
            None => empty.clone(),
        };
        let mut operations = match &base {
            Some(commit) => {
                target.tree_from_root(&commit.state.operations, &target.format.state_tree_format)?
            }
            None => empty,
        };
        let source_operations = self.tree_from_root(
            &source_commit.state.operations,
            &self.format.state_tree_format,
        )?;
        let delta = self.load_commit_delta(&source_commit).await?;
        let physical_operation = delta.operation_ids.first().copied().unwrap_or_else(|| {
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&source_id.as_bytes()[..16]);
            OperationId(uuid::Uuid::from_bytes(bytes))
        });
        let mut copied_payloads = 0usize;
        let mut copied_bytes = 0u64;
        for transition in &delta.changes {
            let mut version = self
                .find_version(&source_commit, &transition.key, transition.next)
                .await?;
            let mut reusable = None;
            if !force_rebind {
                for parent in &mapped_parents {
                    let parent = target.load_commit(*parent).await?;
                    match target
                        .find_version(&parent, &transition.key, transition.next)
                        .await
                    {
                        Ok(existing) => {
                            reusable = Some(existing.binding);
                            break;
                        }
                        Err(error) if error.code == ErrorCode::NoSuchVersion => {}
                        Err(error) => return Err(error),
                    }
                }
            }
            let binding = match reusable {
                Some(binding) => binding,
                None => {
                    let binding = self
                        .clone_physical_version_binding(
                            target,
                            &transition.key,
                            &version,
                            physical_operation,
                            writer_fence_generation,
                        )
                        .await?;
                    let size = match &version.body.kind {
                        LogicalObjectVersionKindV1::Live { size, .. } => *size,
                        LogicalObjectVersionKindV1::DeleteMarker => 0,
                    };
                    copied_bytes = copied_bytes.checked_add(size).ok_or_else(|| {
                        Error::new(
                            ErrorCode::EntityTooLarge,
                            "physical transfer byte count overflow",
                        )
                    })?;
                    copied_payloads += 1;
                    binding
                }
            };
            version.binding = binding;
            version.validate()?;
            versions = target_engine
                .put(
                    &versions,
                    version_tree_key(&transition.key, version.body.order, version.id),
                    encode_canonical(&version)?,
                )
                .await?;
            objects = if transition.delete_marker {
                target_engine.delete(&objects, &transition.key).await?
            } else {
                target_engine
                    .put(
                        &objects,
                        transition.key.clone(),
                        encode_canonical(&CurrentObjectV1 {
                            version: version.clone(),
                        })?,
                    )
                    .await?
            };
        }
        for operation in &delta.operation_ids {
            let record = self
                .engine
                .get(&source_operations, operation.as_bytes())
                .await?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::CorruptCommit,
                        "physical transfer delta names a missing operation",
                    )
                })?;
            operations = target_engine
                .put(&operations, operation.as_bytes().to_vec(), record)
                .await?;
        }
        let state = BucketStateV1 {
            objects: TreeRootV1::from_tree(&objects)?,
            versions: TreeRootV1::from_tree(&versions)?,
            operations: TreeRootV1::from_tree(&operations)?,
        };
        if state.operations != source_commit.state.operations {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "physical transfer replay did not reproduce the logical operation state",
            ));
        }
        let prepared = target_write_store.prepare_node_pack(
            tree_format_digest(&target.format.state_tree_format)?,
            Vec::new(),
        )?;
        let node_pack = prepared.as_ref().map(PreparedNodePack::reference);
        let destination_commit = BucketCommitV1 {
            state,
            parents: mapped_parents,
            generation: source_commit.generation,
            delta,
            node_pack,
            writer_fence_generation,
            author: source_commit.author,
            message: source_commit.message,
            created_at_millis: source_commit.created_at_millis,
            metadata: source_commit.metadata,
        };
        let destination_id = destination_commit.id()?;
        if !force_rebind {
            match target.load_commit(destination_id).await {
                Ok(existing) if existing == destination_commit => {
                    return Ok((destination_id, 0, 0, true));
                }
                Ok(_) => {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "physical transfer destination commit ID collided",
                    ));
                }
                Err(error) if error.code == ErrorCode::MissingClosure => {}
                Err(error) => return Err(error),
            }
        }
        let stored = target.store_commit(&destination_commit, prepared).await?;
        target.finalize_stored_commit(stored).await?;
        Ok((destination_id, copied_payloads + 1, copied_bytes, false))
    }

    async fn load_physical_transfer_mapping(&self, source: CommitId) -> Result<Option<CommitId>> {
        self.plane
            .get(GetRequest {
                path: physical_transfer_mapping_path(&self.options.repository_prefix, source)?,
                range: None,
                physical_version: None,
            })
            .await?
            .map(|object| decode_canonical(&object.bytes))
            .transpose()
    }

    async fn store_physical_transfer_mapping(
        &self,
        source: CommitId,
        destination: CommitId,
    ) -> Result<()> {
        self.store_immutable(
            physical_transfer_mapping_path(&self.options.repository_prefix, source)?,
            encode_canonical(&destination)?,
        )
        .await
    }

    async fn delete_physical_transfer_mapping(&self, source: CommitId) -> Result<()> {
        let path = physical_transfer_mapping_path(&self.options.repository_prefix, source)?;
        let Some(object) = self
            .plane
            .get(GetRequest {
                path: path.clone(),
                range: None,
                physical_version: None,
            })
            .await?
        else {
            return Ok(());
        };
        let token = object.metadata.token;
        let physical = token
            .version_id
            .clone()
            .map(|version_id| PhysicalVersion::Versioned { version_id })
            .unwrap_or_else(|| PhysicalVersion::Unversioned { token: Some(token) });
        match self.plane.delete_exact(&path, physical).await? {
            DeleteOutcome::Deleted | DeleteOutcome::NotFound => Ok(()),
            DeleteOutcome::TokenMismatch => Err(Error::new(
                ErrorCode::PreconditionFailed,
                "physical transfer mapping changed while removing a stale entry",
            )),
        }
    }

    /// Import portable immutable repository objects without moving a local
    /// ref. The returned source head may then be inspected or merged.
    pub async fn fetch_from<Q: ObjectPlane>(
        &self,
        source: &Repository<Q>,
        source_branch: &str,
    ) -> Result<SyncReport> {
        self.validate_sync_identity(source)?;
        let _source_history = source.preserve_history_for_gc().await;
        let source_head = source.head(source_branch).await?;
        let (mapped, mut report) = source
            .replay_physical_history_to(self, &[source_head], false)
            .await?;
        report.source_head = Some(*mapped.get(&source_head).ok_or_else(|| {
            Error::new(
                ErrorCode::MissingClosure,
                "fetch did not map its selected source head",
            )
        })?);
        Ok(report)
    }

    pub async fn push_to<Q: ObjectPlane>(
        &self,
        destination: &Repository<Q>,
        source_branch: &str,
        destination_branch: &str,
        expected_destination: CommitId,
        reason: &str,
    ) -> Result<SyncReport> {
        self.validate_sync_identity(destination)?;
        let _source_history = self.preserve_history_for_gc().await;
        let source_head = self.head(source_branch).await?;
        let _publication = destination
            .lock_branch_publication(destination_branch)
            .await;
        let (mapped, mut report) = self
            .replay_physical_history_to(destination, &[source_head], false)
            .await?;
        let mapped_head = *mapped.get(&source_head).ok_or_else(|| {
            Error::new(
                ErrorCode::MissingClosure,
                "physical push did not map its selected source head",
            )
        })?;
        if reason.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "administrative ref movement requires a non-empty reason",
            ));
        }
        let loaded = destination.load_ref(destination_branch).await?;
        if loaded.value.target != expected_destination {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "branch head does not match push expectation",
            ));
        }
        let movement = destination
            .move_ref_inner(destination_branch, loaded, mapped_head, reason)
            .await?;
        report.source_head = Some(mapped_head);
        report.ref_move = Some(movement);
        Ok(report)
    }

    fn validate_sync_identity<Q: ObjectPlane>(&self, other: &Repository<Q>) -> Result<()> {
        if self.format != other.format {
            return Err(Error::new(
                ErrorCode::RepositoryFormatConflict,
                "sync requires identical repository identity and canonical format",
            ));
        }
        Ok(())
    }

    pub async fn head(&self, branch: &str) -> Result<CommitId> {
        Ok(self.load_ref(branch).await?.value.target)
    }

    pub async fn create_branch(&self, name: &str, from: CommitId) -> Result<BranchHead> {
        validate_branch(name)?;
        let _physical_publication = self.lock_branch_publication(name).await;
        let commit = self.load_commit(from).await?;
        let operation = self.new_operation();
        let writer_fence_generation = self.branch_writer_generation(name).await?;
        let created_at_millis = self.now_millis()?;
        let reflog = ReflogEntryV1 {
            branch: name.to_string(),
            old_target: None,
            new_target: from,
            operation,
            actor: self.options.writer.clone(),
            message: "create branch".to_string(),
            created_at_millis,
        };
        let reflog_id = reflog.id()?;
        let value = crate::RefValueV1 {
            target: from,
            previous_target: None,
            generation: RefGeneration(0),
            operation,
            reflog: reflog_id,
            writer: self.options.writer.clone(),
            updated_at_millis: created_at_millis,
            tombstone: false,
            writer_fence_generation,
            inline_reflog: reflog,
        };
        let publication = self
            .controls
            .compare_exchange(CompareExchange {
                path: branch_path(&self.options.repository_prefix, name)?,
                expected: None,
                bytes: encode_canonical(&value)?,
            })
            .await;
        match publication {
            Ok(CompareExchangeOutcome::Applied(metadata)) => {
                self.cache_branch(name, value.clone(), metadata.token, commit)?;
                Ok(BranchHead {
                    name: name.to_string(),
                    target: from,
                    generation: value.generation,
                })
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => {
                Err(Error::new(ErrorCode::RefConflict, "branch already exists"))
            }
            Err(error) => {
                if let Ok(current) = self.load_ref_including_tombstone(name).await {
                    if current.value.operation == operation
                        && current.value.target == from
                        && !current.value.tombstone
                    {
                        return Ok(BranchHead {
                            name: name.to_string(),
                            target: from,
                            generation: value.generation,
                        });
                    }
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("branch creation outcome is unknown: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation)
                .operation(operation.to_string()))
            }
        }
    }

    pub async fn delete_branch(&self, name: &str, expected: CommitId) -> Result<()> {
        let _physical_publication = self.lock_branch_publication(name).await;
        let writer_fence_generation = self.branch_writer_generation(name).await?;
        let loaded = self.load_ref(name).await?;
        if loaded.value.target != expected {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "branch head does not match expected commit",
            ));
        }
        let operation = self.new_operation();
        let created_at_millis = self.now_millis()?;
        let reflog_entry = ReflogEntryV1 {
            branch: name.to_string(),
            old_target: Some(expected),
            new_target: expected,
            operation,
            actor: self.options.writer.clone(),
            message: "delete branch".to_string(),
            created_at_millis,
        };
        let reflog = reflog_entry.id()?;
        let value = crate::RefValueV1 {
            target: expected,
            previous_target: Some(expected),
            generation: RefGeneration(loaded.value.generation.0.checked_add(1).ok_or_else(
                || Error::new(ErrorCode::InternalInvariant, "ref generation overflow"),
            )?),
            operation,
            reflog,
            writer: self.options.writer.clone(),
            updated_at_millis: created_at_millis,
            tombstone: true,
            writer_fence_generation,
            inline_reflog: reflog_entry,
        };
        let publication = self
            .controls
            .compare_exchange(CompareExchange {
                path: branch_path(&self.options.repository_prefix, name)?,
                expected: Some(loaded.token),
                bytes: encode_canonical(&value)?,
            })
            .await;
        match publication {
            Ok(CompareExchangeOutcome::Applied(_)) => {
                self.warm_branches
                    .write()
                    .map_err(|_| {
                        Error::new(ErrorCode::InternalInvariant, "branch-cache lock poisoned")
                    })?
                    .remove(&name.to_string());
                Ok(())
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                ErrorCode::RefConflict,
                "branch moved while deleting",
            )
            .retry(RetryAdvice::ReloadHead)),
            Err(error) => {
                if let Ok(current) = self.load_ref_including_tombstone(name).await {
                    if current.value.operation == operation
                        && current.value.target == expected
                        && current.value.tombstone
                    {
                        return Ok(());
                    }
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("branch deletion outcome is unknown: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation)
                .operation(operation.to_string()))
            }
        }
    }

    /// Administratively move a branch without creating a bucket commit.
    /// This is intentionally separate from S3-shaped mutation APIs.
    pub async fn reset_branch(
        &self,
        branch: &str,
        to: CommitId,
        expected_head: CommitId,
        reason: &str,
    ) -> Result<RefMoveReceipt> {
        if reason.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "administrative ref movement requires a non-empty reason",
            ));
        }
        self.load_commit(to).await?;
        let loaded = self.load_ref(branch).await?;
        if loaded.value.target != expected_head {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "branch head does not match reset expectation",
            ));
        }
        self.move_ref(branch, loaded, to, reason).await
    }

    /// Recover the old target recorded by an immutable branch reflog entry.
    /// Tombstoned refs are accepted, but the caller must still name the target
    /// currently stored in the ref value.
    pub async fn recover_branch(
        &self,
        branch: &str,
        reflog: crate::ReflogEntryId,
        expected_head: CommitId,
        reason: &str,
    ) -> Result<RefMoveReceipt> {
        if reason.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "branch recovery requires a non-empty reason",
            ));
        }
        let entry = self.reflog(branch, reflog).await?;
        let target = entry.old_target.ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidRevision,
                "selected reflog entry has no previous target",
            )
        })?;
        self.load_commit(target).await?;
        let loaded = self.load_ref_including_tombstone(branch).await?;
        if loaded.value.target != expected_head {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "stored branch target does not match recovery expectation",
            ));
        }
        self.move_ref(branch, loaded, target, reason).await
    }

    pub async fn reflog(&self, branch: &str, id: crate::ReflogEntryId) -> Result<ReflogEntryV1> {
        if let Some((_, entry)) = self
            .physical_reflog_history(branch)
            .await?
            .into_iter()
            .find(|(entry_id, _)| *entry_id == id)
        {
            return Ok(entry);
        }
        Err(Error::new(
            ErrorCode::InvalidRevision,
            "reflog entry is missing",
        ))
    }

    pub async fn list_reflog(
        &self,
        branch: &str,
    ) -> Result<Vec<(crate::ReflogEntryId, ReflogEntryV1)>> {
        let mut entries = self.physical_reflog_history(branch).await?;
        entries.sort_by(|left, right| {
            left.1
                .created_at_millis
                .cmp(&right.1.created_at_millis)
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(entries)
    }

    /// Bounded newest-to-oldest branch reflog traversal. Administrative ref
    /// movements are emitted from the inline ref record; commit publications
    /// then resume through the first-parent history cursor.
    pub async fn list_branch_reflog_page(
        &self,
        branch: &str,
        cursor: Option<&BranchReflogCursor>,
        limit: usize,
        budget: TraversalBudget,
    ) -> Result<BranchReflogPage> {
        validate_branch(branch)?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "branch reflog page limit must be greater than zero",
            ));
        }
        let (mut state, inline) = if let Some(cursor) = cursor {
            (cursor.clone(), None)
        } else {
            let loaded = self.load_ref_including_tombstone(branch).await?;
            (
                BranchReflogCursor {
                    branch: branch.to_string(),
                    root: loaded.value.target,
                    history: Some(HistoryCursor {
                        root: loaded.value.target,
                        next: loaded.value.target,
                    }),
                    inline_id: loaded.value.reflog,
                    inline_emitted: false,
                },
                Some((loaded.value.reflog, loaded.value.inline_reflog)),
            )
        };
        if state.branch != branch {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "branch reflog cursor belongs to another branch",
            ));
        }
        let mut entries = Vec::with_capacity(limit);
        if !state.inline_emitted {
            let (id, entry) = inline.ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidContinuationToken,
                    "branch reflog cursor omitted its inline-entry state",
                )
            })?;
            if entry.id()? != id {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "ref inline reflog identity mismatch",
                ));
            }
            entries.push((id, entry));
            state.inline_emitted = true;
        }
        let mut budget_exhausted = false;
        if entries.len() < limit {
            if let Some(history) = state.history.as_ref() {
                let page = self
                    .log_page_bounded(state.root, Some(history), limit - entries.len(), budget)
                    .await?;
                for (id, commit) in page.commits {
                    let delta = self.load_commit_delta(&commit).await?;
                    if let Some(operation) = delta.operation_ids.first().copied() {
                        let entry = ReflogEntryV1 {
                            branch: branch.to_string(),
                            old_target: commit.parents.first().copied(),
                            new_target: id,
                            operation,
                            actor: commit.author,
                            message: commit.message.unwrap_or_default(),
                            created_at_millis: commit.created_at_millis,
                        };
                        let id = entry.id()?;
                        if id != state.inline_id
                            && !entries.iter().any(|(existing, _)| *existing == id)
                        {
                            entries.push((id, entry));
                        }
                    }
                }
                state.history = page.continuation;
                budget_exhausted = page.budget_exhausted;
            }
        }
        let continuation = state.history.is_some().then_some(state);
        Ok(BranchReflogPage {
            entries,
            continuation,
            budget_exhausted,
        })
    }

    async fn physical_reflog_history(
        &self,
        branch: &str,
    ) -> Result<Vec<(crate::ReflogEntryId, ReflogEntryV1)>> {
        validate_branch(branch)?;
        let loaded = self.load_ref_including_tombstone(branch).await?;
        let mut entries = BTreeMap::new();
        if loaded.value.inline_reflog.branch != branch
            || loaded.value.inline_reflog.id()? != loaded.value.reflog
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "ref inline reflog identity mismatch",
            ));
        }
        entries.insert(loaded.value.reflog, loaded.value.inline_reflog.clone());
        let mut next = Some(loaded.value.target);
        let mut seen = BTreeSet::new();
        while let Some(id) = next {
            if !seen.insert(id) {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "physical first-parent history contains a cycle",
                ));
            }
            if seen.len() > self.options.history_traversal_limit {
                return Err(Error::new(
                    ErrorCode::HistoryLimitExceeded,
                    "physical reflog traversal exceeded its configured limit",
                ));
            }
            let commit = self.load_commit(id).await?;
            let delta = self.load_commit_delta(&commit).await?;
            if let Some(operation) = delta.operation_ids.first().copied() {
                let entry = ReflogEntryV1 {
                    branch: branch.to_string(),
                    old_target: commit.parents.first().copied(),
                    new_target: id,
                    operation,
                    actor: commit.author.clone(),
                    message: commit.message.clone().unwrap_or_default(),
                    created_at_millis: commit.created_at_millis,
                };
                entries.entry(entry.id()?).or_insert(entry);
            }
            next = commit.parents.first().copied();
        }
        Ok(entries.into_iter().collect())
    }

    async fn move_ref(
        &self,
        branch: &str,
        loaded: LoadedRef,
        target: CommitId,
        reason: &str,
    ) -> Result<RefMoveReceipt> {
        let _physical_publication = self.lock_branch_publication(branch).await;
        self.move_ref_inner(branch, loaded, target, reason).await
    }

    async fn move_ref_inner(
        &self,
        branch: &str,
        loaded: LoadedRef,
        target: CommitId,
        reason: &str,
    ) -> Result<RefMoveReceipt> {
        self.load_commit(target).await?;
        let writer_fence_generation = self.branch_writer_generation(branch).await?;
        let operation = self.new_operation();
        let created_at_millis = self.now_millis()?;
        let reflog_entry = ReflogEntryV1 {
            branch: branch.to_string(),
            old_target: Some(loaded.value.target),
            new_target: target,
            operation,
            actor: self.options.writer.clone(),
            message: reason.to_string(),
            created_at_millis,
        };
        let reflog = reflog_entry.id()?;
        let generation =
            RefGeneration(loaded.value.generation.0.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "ref generation overflow")
            })?);
        let value = crate::RefValueV1 {
            target,
            previous_target: Some(loaded.value.target),
            generation,
            operation,
            reflog,
            writer: self.options.writer.clone(),
            updated_at_millis: created_at_millis,
            tombstone: false,
            writer_fence_generation,
            inline_reflog: reflog_entry,
        };
        let publication = self
            .controls
            .compare_exchange(CompareExchange {
                path: branch_path(&self.options.repository_prefix, branch)?,
                expected: Some(loaded.token),
                bytes: encode_canonical(&value)?,
            })
            .await;
        match publication {
            Ok(CompareExchangeOutcome::Applied(metadata)) => {
                let commit = self.load_commit(target).await?;
                self.cache_branch(branch, value, metadata.token, commit)?;
                Ok(RefMoveReceipt {
                    branch: branch.to_string(),
                    old_target: Some(loaded.value.target),
                    new_target: target,
                    operation,
                    generation,
                })
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                ErrorCode::RefConflict,
                "branch moved during administrative ref update",
            )
            .retry(RetryAdvice::ReloadHead)),
            Err(error) => {
                let current = self.load_ref_including_tombstone(branch).await?;
                if current.value.operation == operation
                    && current.value.target == target
                    && !current.value.tombstone
                {
                    return Ok(RefMoveReceipt {
                        branch: branch.to_string(),
                        old_target: Some(loaded.value.target),
                        new_target: target,
                        operation,
                        generation,
                    });
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("administrative ref publication outcome is unknown: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation)
                .operation(operation.to_string()))
            }
        }
    }

    /// Lists an ordered page from the rebuildable branch catalog. Results may
    /// lag authoritative refs by the returned freshness watermark; callers
    /// must resolve a selected branch through `head` before acting on it.
    pub async fn list_branch_catalog_page(
        &self,
        after: Option<&str>,
        requested_limit: usize,
    ) -> Result<CatalogBranchPage> {
        if let Some(after) = after {
            validate_branch(after)?;
        }
        let limit = requested_limit
            .min(self.format.canonical_limits.max_list_page as usize)
            .min(1_000);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "branch catalog page limit must be greater than zero",
            ));
        }
        let stored = self
            .plane
            .load_mutable(&ref_catalog_v2_head_path(&self.options.repository_prefix)?)
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::MissingCapability,
                    "ref catalog has not been built; advance it before listing",
                )
            })?;
        let head: RefCatalogHeadV2 = decode_canonical(&stored.bytes)?;
        head.validate(
            self.format.repository_id,
            tree_format_digest(&self.format.state_tree_format)?,
        )?;
        self.ref_catalog.install_root(head.root.root.clone())?;
        let tree = self.ref_catalog.tree()?;
        let prefix = b"h\0";
        let after_key = after.map(|name| ref_catalog_key(false, name));
        let mut iter = self.ref_catalog.engine.prefix(&tree, prefix).await?;
        let mut branches = Vec::with_capacity(limit.saturating_add(1));
        while branches.len() <= limit {
            let Some(entry) = iter.next().await else {
                break;
            };
            let (key, encoded) = entry?;
            if after_key
                .as_ref()
                .is_some_and(|after| key.as_slice() <= after.as_slice())
            {
                continue;
            }
            let name = String::from_utf8(key[prefix.len()..].to_vec())
                .map_err(|_| Error::new(ErrorCode::CorruptCommit, "catalog branch is not UTF-8"))?;
            let value: RefCatalogEntryV2 = decode_canonical(&encoded)?;
            let RefCatalogEntryV2::Branch { target, generation } = value else {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "branch catalog contains a tag value",
                ));
            };
            branches.push(BranchHead {
                name,
                target,
                generation,
            });
        }
        let truncated = branches.len() > limit;
        branches.truncate(limit);
        let continuation = truncated
            .then(|| branches.last().map(|branch| branch.name.clone()))
            .flatten();
        Ok(CatalogBranchPage {
            branches,
            continuation,
            freshness: IndexFreshness {
                generation: head.generation,
                scan_epoch: head.scan_epoch,
                updated_at_millis: head.updated_at_millis,
            },
        })
    }

    /// Lists one bounded page of branch heads. The continuation token is owned
    /// by the object plane and must be passed back unchanged.
    pub async fn list_branches_page(
        &self,
        continuation: Option<String>,
        requested_limit: usize,
    ) -> Result<BranchPage> {
        let limit = requested_limit.min(1_000);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "branch page limit must be greater than zero",
            ));
        }
        let prefix = format!("{}/refs/heads/", self.options.repository_prefix);
        let page = self
            .plane
            .list(ListRequest {
                prefix: prefix.clone(),
                continuation,
                limit,
                include_versions: false,
            })
            .await?;
        let mut branches = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let encoded = entry.path.as_str().strip_prefix(&prefix).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "branch list escaped its prefix",
                )
            })?;
            let name = String::from_utf8(hex::decode(encoded).map_err(|_| {
                Error::new(ErrorCode::CorruptCommit, "branch path is not canonical hex")
            })?)
            .map_err(|_| Error::new(ErrorCode::CorruptCommit, "branch name is not UTF-8"))?;
            let Some(stored) = self.plane.load_mutable(&entry.path).await? else {
                continue;
            };
            let value: crate::RefValueV1 = decode_canonical(&stored.bytes)?;
            if !value.tombstone {
                branches.push(BranchHead {
                    name,
                    target: value.target,
                    generation: value.generation,
                });
            }
        }
        branches.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(BranchPage {
            branches,
            continuation: page.continuation,
        })
    }

    pub async fn list_branches(&self) -> Result<Vec<BranchHead>> {
        let mut continuation = None;
        let mut result = Vec::new();
        loop {
            let page = self.list_branches_page(continuation, 1_000).await?;
            result.extend(page.branches);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        result.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(result)
    }

    pub async fn create_tag(&self, name: &str, target: CommitId) -> Result<Tag> {
        validate_branch(name)?;
        let _publication = self.lock_named_publication("tag", name).await;
        self.load_commit(target).await?;
        self.system_writer_generation(&format!("tag/{name}"))
            .await?;
        let operation = self.new_operation();
        let created_at_millis = self.now_millis()?;
        let reflog = self
            .store_tag_reflog(&ReflogEntryV1 {
                branch: name.to_string(),
                old_target: None,
                new_target: target,
                operation,
                actor: self.options.writer.clone(),
                message: "create tag".to_string(),
                created_at_millis,
            })
            .await?;
        let value = crate::TagValueV1 {
            target,
            previous_target: None,
            generation: RefGeneration(0),
            operation,
            reflog,
            writer: self.options.writer.clone(),
            created_at_millis,
            tombstone: false,
        };
        let publication = self
            .controls
            .compare_exchange(CompareExchange {
                path: tag_path(&self.options.repository_prefix, name)?,
                expected: None,
                bytes: encode_canonical(&value)?,
            })
            .await;
        match publication {
            Ok(CompareExchangeOutcome::Applied(_)) => Ok(Tag {
                name: name.to_string(),
                target,
            }),
            Ok(CompareExchangeOutcome::Conflict(_)) => {
                Err(Error::new(ErrorCode::RefConflict, "tag already exists"))
            }
            Err(error) => {
                let current = self
                    .plane
                    .load_mutable(&tag_path(&self.options.repository_prefix, name)?)
                    .await?;
                if let Some(current) = current {
                    let current: crate::TagValueV1 = decode_canonical(&current.bytes)?;
                    if current.operation == operation
                        && current.target == target
                        && !current.tombstone
                    {
                        return Ok(Tag {
                            name: name.to_string(),
                            target,
                        });
                    }
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("tag creation outcome is unknown: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation)
                .operation(operation.to_string()))
            }
        }
    }

    /// Lists an ordered page from the rebuildable tag catalog. The freshness
    /// watermark makes asynchronous catalog lag explicit.
    pub async fn list_tag_catalog_page(
        &self,
        after: Option<&str>,
        requested_limit: usize,
    ) -> Result<CatalogTagPage> {
        if let Some(after) = after {
            validate_branch(after)?;
        }
        let limit = requested_limit
            .min(self.format.canonical_limits.max_list_page as usize)
            .min(1_000);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "tag catalog page limit must be greater than zero",
            ));
        }
        let stored = self
            .plane
            .load_mutable(&ref_catalog_v2_head_path(&self.options.repository_prefix)?)
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::MissingCapability,
                    "ref catalog has not been built; advance it before listing",
                )
            })?;
        let head: RefCatalogHeadV2 = decode_canonical(&stored.bytes)?;
        head.validate(
            self.format.repository_id,
            tree_format_digest(&self.format.state_tree_format)?,
        )?;
        self.ref_catalog.install_root(head.root.root.clone())?;
        let tree = self.ref_catalog.tree()?;
        let prefix = b"t\0";
        let after_key = after.map(|name| ref_catalog_key(true, name));
        let mut iter = self.ref_catalog.engine.prefix(&tree, prefix).await?;
        let mut tags = Vec::with_capacity(limit.saturating_add(1));
        while tags.len() <= limit {
            let Some(entry) = iter.next().await else {
                break;
            };
            let (key, encoded) = entry?;
            if after_key
                .as_ref()
                .is_some_and(|after| key.as_slice() <= after.as_slice())
            {
                continue;
            }
            let name = String::from_utf8(key[prefix.len()..].to_vec())
                .map_err(|_| Error::new(ErrorCode::CorruptCommit, "catalog tag is not UTF-8"))?;
            let value: RefCatalogEntryV2 = decode_canonical(&encoded)?;
            let RefCatalogEntryV2::Tag { target, .. } = value else {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "tag catalog contains a branch value",
                ));
            };
            tags.push(Tag { name, target });
        }
        let truncated = tags.len() > limit;
        tags.truncate(limit);
        let continuation = truncated
            .then(|| tags.last().map(|tag| tag.name.clone()))
            .flatten();
        Ok(CatalogTagPage {
            tags,
            continuation,
            freshness: IndexFreshness {
                generation: head.generation,
                scan_epoch: head.scan_epoch,
                updated_at_millis: head.updated_at_millis,
            },
        })
    }

    /// Lists one bounded page of tags. Tombstones are omitted while still
    /// advancing the underlying object-plane continuation.
    pub async fn list_tags_page(
        &self,
        continuation: Option<String>,
        requested_limit: usize,
    ) -> Result<TagPage> {
        let limit = requested_limit.min(1_000);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "tag page limit must be greater than zero",
            ));
        }
        let prefix = format!("{}/refs/tags/", self.options.repository_prefix);
        let page = self
            .plane
            .list(ListRequest {
                prefix: prefix.clone(),
                continuation,
                limit,
                include_versions: false,
            })
            .await?;
        let mut tags = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let encoded = entry.path.as_str().strip_prefix(&prefix).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "tag list escaped prefix")
            })?;
            let name = String::from_utf8(
                hex::decode(encoded)
                    .map_err(|_| Error::new(ErrorCode::CorruptCommit, "tag path is not hex"))?,
            )
            .map_err(|_| Error::new(ErrorCode::CorruptCommit, "tag name is not UTF-8"))?;
            let Some(stored) = self.plane.load_mutable(&entry.path).await? else {
                continue;
            };
            let value: crate::TagValueV1 = decode_canonical(&stored.bytes)?;
            if !value.tombstone {
                tags.push(Tag {
                    name,
                    target: value.target,
                });
            }
        }
        tags.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(TagPage {
            tags,
            continuation: page.continuation,
        })
    }

    pub async fn list_tags(&self) -> Result<Vec<Tag>> {
        let mut continuation = None;
        let mut result = Vec::new();
        loop {
            let page = self.list_tags_page(continuation, 1_000).await?;
            result.extend(page.tags);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        result.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(result)
    }

    pub async fn delete_tag(&self, name: &str, expected: CommitId) -> Result<()> {
        validate_branch(name)?;
        let _publication = self.lock_named_publication("tag", name).await;
        self.system_writer_generation(&format!("tag/{name}"))
            .await?;
        let path = tag_path(&self.options.repository_prefix, name)?;
        let stored = self
            .plane
            .load_mutable(&path)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::InvalidRevision, "tag does not exist"))?;
        let value: crate::TagValueV1 = decode_canonical(&stored.bytes)?;
        if value.tombstone || value.target != expected {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "tag target does not match expected commit",
            ));
        }
        let operation = self.new_operation();
        let created_at_millis = self.now_millis()?;
        let reflog = self
            .store_tag_reflog(&ReflogEntryV1 {
                branch: name.to_string(),
                old_target: Some(expected),
                new_target: expected,
                operation,
                actor: self.options.writer.clone(),
                message: "delete tag".to_string(),
                created_at_millis,
            })
            .await?;
        let tombstone = crate::TagValueV1 {
            target: expected,
            previous_target: Some(expected),
            generation: RefGeneration(value.generation.0.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "tag generation overflow")
            })?),
            operation,
            reflog,
            writer: self.options.writer.clone(),
            created_at_millis,
            tombstone: true,
        };
        let publication = self
            .controls
            .compare_exchange(CompareExchange {
                path: path.clone(),
                expected: Some(stored.metadata.token),
                bytes: encode_canonical(&tombstone)?,
            })
            .await;
        match publication {
            Ok(CompareExchangeOutcome::Applied(_)) => Ok(()),
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                ErrorCode::RefConflict,
                "tag changed while deleting",
            )),
            Err(error) => {
                if let Some(current) = self.plane.load_mutable(&path).await? {
                    let current: crate::TagValueV1 = decode_canonical(&current.bytes)?;
                    if current.operation == operation
                        && current.target == expected
                        && current.tombstone
                    {
                        return Ok(());
                    }
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("tag deletion outcome is unknown: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation)
                .operation(operation.to_string()))
            }
        }
    }

    /// Creates a named retention root. Pins are mutable tombstoned records so
    /// deleting one never reveals an older physical S3 version.
    pub async fn create_retention_pin(
        &self,
        name: &str,
        target: CommitId,
        owner: &str,
        reason: &str,
        ttl_millis: Option<u64>,
    ) -> Result<RetentionPinV1> {
        validate_branch(name)?;
        if owner.trim().is_empty() || reason.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "retention pin owner and reason must be non-empty",
            ));
        }
        let _publication = self.lock_named_publication("pin", name).await;
        self.load_commit(target).await?;
        self.system_writer_generation(&format!("pin/{name}"))
            .await?;
        let path = retention_pin_path(&self.options.repository_prefix, name)?;
        let current = self.plane.load_mutable(&path).await?;
        let now = self.now_millis()?;
        let (expected, generation) = if let Some(stored) = current {
            let pin: RetentionPinV1 = decode_canonical(&stored.bytes)?;
            let active =
                !pin.tombstone && (pin.expires_at_millis == 0 || pin.expires_at_millis > now);
            if active {
                return Err(Error::new(
                    ErrorCode::RefConflict,
                    "retention pin already exists",
                ));
            }
            (
                Some(stored.metadata.token),
                pin.generation.checked_add(1).ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "pin generation overflow")
                })?,
            )
        } else {
            (None, 0)
        };
        let expires_at_millis = match ttl_millis {
            Some(0) | None => 0,
            Some(ttl) => now.checked_add(ttl).ok_or_else(|| {
                Error::new(ErrorCode::InvalidRequest, "retention pin expiry overflows")
            })?,
        };
        let pin = RetentionPinV1 {
            name: name.to_string(),
            target,
            owner: owner.to_string(),
            reason: reason.to_string(),
            created_at_millis: now,
            expires_at_millis,
            generation,
            tombstone: false,
        };
        match self
            .controls
            .compare_exchange(CompareExchange {
                path,
                expected,
                bytes: encode_canonical(&pin)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => Ok(pin),
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::RefConflict,
                "retention pin changed concurrently",
            )),
        }
    }

    pub async fn delete_retention_pin(&self, name: &str, expected: CommitId) -> Result<()> {
        validate_branch(name)?;
        let _publication = self.lock_named_publication("pin", name).await;
        self.system_writer_generation(&format!("pin/{name}"))
            .await?;
        let path = retention_pin_path(&self.options.repository_prefix, name)?;
        let stored =
            self.plane.load_mutable(&path).await?.ok_or_else(|| {
                Error::new(ErrorCode::InvalidRevision, "retention pin is missing")
            })?;
        let mut pin: RetentionPinV1 = decode_canonical(&stored.bytes)?;
        if pin.tombstone || pin.target != expected {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "retention pin target does not match expectation",
            ));
        }
        pin.tombstone = true;
        pin.generation = pin
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::new(ErrorCode::InternalInvariant, "pin generation overflow"))?;
        match self
            .controls
            .compare_exchange(CompareExchange {
                path,
                expected: Some(stored.metadata.token),
                bytes: encode_canonical(&pin)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => Ok(()),
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::RefConflict,
                "retention pin changed concurrently",
            )),
        }
    }

    pub async fn list_retention_pins(&self) -> Result<Vec<RetentionPinV1>> {
        let mut continuation = None;
        let mut pins = Vec::new();
        loop {
            let page = self.list_retention_pins_page(continuation, 1_000).await?;
            pins.extend(page.pins);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        pins.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(pins)
    }

    pub async fn list_retention_pins_page(
        &self,
        continuation: Option<String>,
        limit: usize,
    ) -> Result<RetentionPinPage> {
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "retention-pin page limit must be greater than zero",
            ));
        }
        let now = self.now_millis()?;
        let page = self
            .plane
            .list(ListRequest {
                prefix: format!("{}/retention/pins/", self.options.repository_prefix),
                continuation,
                limit,
                include_versions: false,
            })
            .await?;
        let mut pins = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let Some(stored) = self.plane.load_mutable(&entry.path).await? else {
                continue;
            };
            let pin: RetentionPinV1 = decode_canonical(&stored.bytes)?;
            if !pin.tombstone && (pin.expires_at_millis == 0 || pin.expires_at_millis > now) {
                pins.push(pin);
            }
        }
        pins.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(RetentionPinPage {
            pins,
            continuation: page.continuation,
        })
    }

    pub async fn list_tag_reflog(
        &self,
        tag: &str,
    ) -> Result<Vec<(crate::ReflogEntryId, ReflogEntryV1)>> {
        validate_branch(tag)?;
        let prefix = format!(
            "{}/reflogs/tags/{}/",
            self.options.repository_prefix,
            hex::encode(tag.as_bytes())
        );
        self.list_reflog_prefix(tag, prefix).await
    }

    pub async fn list_tag_reflog_page(
        &self,
        tag: &str,
        continuation: Option<String>,
        limit: usize,
    ) -> Result<TagReflogPage> {
        validate_branch(tag)?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "tag reflog page limit must be greater than zero",
            ));
        }
        let page = self
            .plane
            .list(ListRequest {
                prefix: format!(
                    "{}/reflogs/tags/{}/",
                    self.options.repository_prefix,
                    hex::encode(tag.as_bytes())
                ),
                continuation,
                limit,
                include_versions: false,
            })
            .await?;
        let mut entries = Vec::with_capacity(page.entries.len());
        for listed in page.entries {
            let object = self
                .plane
                .get(GetRequest {
                    path: listed.path,
                    range: None,
                    physical_version: None,
                })
                .await?
                .ok_or_else(|| {
                    Error::new(ErrorCode::MissingClosure, "listed reflog entry disappeared")
                })?;
            let entry: ReflogEntryV1 = decode_canonical(&object.bytes)?;
            if entry.branch != tag {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "reflog entry escaped its ref namespace",
                ));
            }
            entries.push((entry.id()?, entry));
        }
        entries.sort_by(|left, right| {
            left.1
                .created_at_millis
                .cmp(&right.1.created_at_millis)
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(TagReflogPage {
            entries,
            continuation: page.continuation,
        })
    }

    pub async fn recover_tag(
        &self,
        tag: &str,
        reflog: crate::ReflogEntryId,
        expected_target: CommitId,
        reason: &str,
    ) -> Result<Tag> {
        if reason.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "tag recovery requires a non-empty reason",
            ));
        }
        let _publication = self.lock_named_publication("tag", tag).await;
        self.system_writer_generation(&format!("tag/{tag}")).await?;
        let entry = self.tag_reflog(tag, reflog).await?;
        let target = entry.old_target.ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidRevision,
                "selected tag reflog has no previous target",
            )
        })?;
        self.load_commit(target).await?;
        let path = tag_path(&self.options.repository_prefix, tag)?;
        let stored = self
            .plane
            .load_mutable(&path)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::InvalidRevision, "tag does not exist"))?;
        let current: crate::TagValueV1 = decode_canonical(&stored.bytes)?;
        if current.target != expected_target {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "tag target does not match recovery expectation",
            ));
        }
        let operation = self.new_operation();
        let created_at_millis = self.now_millis()?;
        let recovery_reflog = self
            .store_tag_reflog(&ReflogEntryV1 {
                branch: tag.to_string(),
                old_target: Some(current.target),
                new_target: target,
                operation,
                actor: self.options.writer.clone(),
                message: reason.to_string(),
                created_at_millis,
            })
            .await?;
        let next = crate::TagValueV1 {
            target,
            previous_target: Some(current.target),
            generation: RefGeneration(current.generation.0.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "tag generation overflow")
            })?),
            operation,
            reflog: recovery_reflog,
            writer: self.options.writer.clone(),
            created_at_millis,
            tombstone: false,
        };
        let publication = self
            .controls
            .compare_exchange(CompareExchange {
                path: path.clone(),
                expected: Some(stored.metadata.token),
                bytes: encode_canonical(&next)?,
            })
            .await;
        match publication {
            Ok(CompareExchangeOutcome::Applied(_)) => Ok(Tag {
                name: tag.to_string(),
                target,
            }),
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                ErrorCode::RefConflict,
                "tag moved during recovery",
            )),
            Err(error) => {
                if let Some(stored) = self.plane.load_mutable(&path).await? {
                    let value: crate::TagValueV1 = decode_canonical(&stored.bytes)?;
                    if value.operation == operation && value.target == target && !value.tombstone {
                        return Ok(Tag {
                            name: tag.to_string(),
                            target,
                        });
                    }
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("tag recovery outcome is unknown: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation)
                .operation(operation.to_string()))
            }
        }
    }

    async fn tag_reflog(&self, tag: &str, id: crate::ReflogEntryId) -> Result<ReflogEntryV1> {
        let object = self
            .plane
            .get(GetRequest {
                path: tag_reflog_path(&self.options.repository_prefix, tag, id)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::InvalidRevision, "tag reflog is missing"))?;
        let entry: ReflogEntryV1 = decode_canonical(&object.bytes)?;
        if entry.id()? != id || entry.branch != tag {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "tag reflog identity mismatch",
            ));
        }
        Ok(entry)
    }

    async fn list_reflog_prefix(
        &self,
        expected_name: &str,
        prefix: String,
    ) -> Result<Vec<(crate::ReflogEntryId, ReflogEntryV1)>> {
        let mut continuation = None;
        let mut entries = Vec::new();
        loop {
            let page = self
                .plane
                .list(ListRequest {
                    prefix: prefix.clone(),
                    continuation,
                    limit: 1_000,
                    include_versions: false,
                })
                .await?;
            for listed in page.entries {
                let object = self
                    .plane
                    .get(GetRequest {
                        path: listed.path,
                        range: None,
                        physical_version: None,
                    })
                    .await?
                    .ok_or_else(|| {
                        Error::new(ErrorCode::MissingClosure, "listed reflog entry disappeared")
                    })?;
                let entry: ReflogEntryV1 = decode_canonical(&object.bytes)?;
                if entry.branch != expected_name {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "reflog entry escaped its ref namespace",
                    ));
                }
                entries.push((entry.id()?, entry));
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        entries.sort_by(|left, right| {
            left.1
                .created_at_millis
                .cmp(&right.1.created_at_millis)
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(entries)
    }

    pub async fn put_bytes(
        &self,
        branch: &str,
        key: Vec<u8>,
        bytes: Vec<u8>,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        operation: Option<OperationId>,
    ) -> Result<CommitReceipt> {
        self.validate_key(&key)?;
        let operation = operation.unwrap_or_else(|| self.new_operation());
        self.put_physical_bytes_checked(
            branch,
            key,
            bytes,
            headers,
            user_metadata,
            operation,
            ObjectWriteConditionV1::default(),
            ChecksumExpectation::default(),
        )
        .await
    }

    /// Spool a stream once, then upload it as one physical S3 object version.
    pub async fn put_stream<S, B, E>(
        &self,
        branch: &str,
        key: Vec<u8>,
        stream: S,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        operation: Option<OperationId>,
    ) -> Result<CommitReceipt>
    where
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]>,
        E: std::fmt::Display,
    {
        self.put_stream_checked(
            branch,
            key,
            stream,
            headers,
            user_metadata,
            operation,
            ObjectWriteConditionV1::default(),
            ChecksumExpectation::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn put_stream_checked<S, B, E>(
        &self,
        branch: &str,
        key: Vec<u8>,
        stream: S,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        operation: Option<OperationId>,
        condition: ObjectWriteConditionV1,
        expected_checksums: ChecksumExpectation,
    ) -> Result<CommitReceipt>
    where
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]>,
        E: std::fmt::Display,
    {
        self.validate_key(&key)?;
        let operation = operation.unwrap_or_else(|| self.new_operation());
        futures_util::pin_mut!(stream);
        let mut spool = tempfile::NamedTempFile::new().map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("could not create physical upload spool: {error}"),
            )
        })?;
        let mut size = 0_u64;
        let mut sha256 = Sha256::new();
        let mut md5 = Md5::new();
        while let Some(next) = stream.next().await {
            let next = next.map_err(|error| {
                Error::new(
                    ErrorCode::Transport,
                    format!("physical object input stream failed: {error}"),
                )
            })?;
            let next = next.as_ref();
            size = size.checked_add(next.len() as u64).ok_or_else(|| {
                Error::new(ErrorCode::EntityTooLarge, "physical object length overflow")
            })?;
            if size > self.format.canonical_limits.max_object_bytes {
                return Err(Error::new(
                    ErrorCode::EntityTooLarge,
                    "physical object exceeds the repository size limit",
                ));
            }
            spool.write_all(next).map_err(|error| {
                Error::new(
                    ErrorCode::Transport,
                    format!("physical upload spool write failed: {error}"),
                )
            })?;
            sha256.update(next);
            md5.update(next);
        }
        spool.flush().map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("physical upload spool flush failed: {error}"),
            )
        })?;
        let checksum_sha256: [u8; 32] = sha256.finalize().into();
        let checksum_md5: [u8; 16] = md5.finalize().into();
        self.put_physical_file_checked(
            branch,
            key,
            spool.path().to_path_buf(),
            size,
            checksum_sha256,
            checksum_md5,
            headers,
            user_metadata,
            operation,
            condition,
            expected_checksums,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_physical_bytes_checked(
        &self,
        branch: &str,
        key: Vec<u8>,
        bytes: Vec<u8>,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        operation: OperationId,
        condition: ObjectWriteConditionV1,
        expected_checksums: ChecksumExpectation,
    ) -> Result<CommitReceipt> {
        let expected_size = bytes.len() as u64;
        let expected_sha256 = crate::codec::sha256(&bytes);
        let expected_md5: [u8; 16] = Md5::digest(&bytes).into();
        if expected_checksums
            .md5
            .is_some_and(|expected| expected != expected_md5)
            || expected_checksums
                .sha256
                .is_some_and(|expected| expected != expected_sha256)
        {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "request checksum does not match the physical object body",
            ));
        }
        let kind = LogicalObjectVersionKindV1::Live {
            size: expected_size,
            logical_etag: format!("\"{}\"", hex::encode(expected_md5)),
            headers: headers.clone(),
            checksums: crate::Checksums {
                md5: Some(expected_md5),
                sha256: Some(expected_sha256),
                algorithm_values: BTreeMap::new(),
            },
            user_metadata: user_metadata.clone(),
            tags: BTreeMap::new(),
        };
        let input_digest = derive_input_digest(&[
            self.format.repository_id.as_bytes(),
            branch.as_bytes(),
            b"put",
            &key,
            &encode_canonical(&kind)?,
            &encode_canonical(&condition)?,
        ]);
        let operation_lock = self.operation_lock(operation);
        let _operation = operation_lock.lock().await;
        if let Some(receipt) = self
            .replay_warm_operation(branch, operation, input_digest)
            .await?
        {
            return Ok(receipt);
        }
        let writer_fence_generation = self.branch_writer_generation(branch).await?;
        let path =
            ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let _payload_permit = self.payload_write_permit().await;
        let physical = self
            .plane
            .put_physical(crate::PhysicalPut {
                path: path.clone(),
                bytes,
                headers: headers.clone(),
                user_metadata: user_metadata.clone(),
                repository: self.format.repository_id,
                operation,
                writer_fence_generation,
            })
            .await;
        let physical = match physical {
            Ok(value) => value,
            Err(error) => match self
                .reconcile_physical_payload(&path, operation, expected_sha256)
                .await?
            {
                Some(value) => value,
                None => return Err(error),
            },
        };
        drop(_payload_permit);
        if physical.size != expected_size
            || physical.checksums.sha256 != Some(expected_sha256)
            || physical.checksums.md5 != Some(expected_md5)
        {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "physical provider result disagrees with the uploaded object identity",
            ));
        }
        let _publication = self.lock_branch_publication(branch).await;
        self.commit_one(
            branch,
            key,
            kind,
            physical.binding,
            writer_fence_generation,
            OperationKind::Put,
            operation,
            input_digest,
            "PutObject",
            condition,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_physical_file_checked(
        &self,
        branch: &str,
        key: Vec<u8>,
        body_path: std::path::PathBuf,
        expected_size: u64,
        expected_sha256: [u8; 32],
        expected_md5: [u8; 16],
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        operation: OperationId,
        condition: ObjectWriteConditionV1,
        expected_checksums: ChecksumExpectation,
    ) -> Result<CommitReceipt> {
        if expected_checksums
            .md5
            .is_some_and(|expected| expected != expected_md5)
            || expected_checksums
                .sha256
                .is_some_and(|expected| expected != expected_sha256)
        {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "request checksum does not match the physical object body",
            ));
        }
        let kind = LogicalObjectVersionKindV1::Live {
            size: expected_size,
            logical_etag: format!("\"{}\"", hex::encode(expected_md5)),
            headers: headers.clone(),
            checksums: crate::Checksums {
                md5: Some(expected_md5),
                sha256: Some(expected_sha256),
                algorithm_values: BTreeMap::new(),
            },
            user_metadata: user_metadata.clone(),
            tags: BTreeMap::new(),
        };
        let input_digest = derive_input_digest(&[
            self.format.repository_id.as_bytes(),
            branch.as_bytes(),
            b"put",
            &key,
            &encode_canonical(&kind)?,
            &encode_canonical(&condition)?,
        ]);
        let operation_lock = self.operation_lock(operation);
        let _operation = operation_lock.lock().await;
        if let Some(receipt) = self
            .replay_warm_operation(branch, operation, input_digest)
            .await?
        {
            return Ok(receipt);
        }
        let writer_fence_generation = self.branch_writer_generation(branch).await?;
        let path =
            ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let _payload_permit = self.payload_write_permit().await;
        let physical = self
            .plane
            .put_physical_file(crate::PhysicalFilePut {
                path: path.clone(),
                body_path,
                size: expected_size,
                checksum_sha256: expected_sha256,
                checksum_md5: expected_md5,
                headers: headers.clone(),
                user_metadata: user_metadata.clone(),
                repository: self.format.repository_id,
                operation,
                writer_fence_generation,
            })
            .await;
        let physical = match physical {
            Ok(value) => value,
            Err(error) => match self
                .reconcile_physical_payload(&path, operation, expected_sha256)
                .await?
            {
                Some(value) => value,
                None => return Err(error),
            },
        };
        drop(_payload_permit);
        if physical.size != expected_size
            || physical.checksums.sha256 != Some(expected_sha256)
            || physical.checksums.md5 != Some(expected_md5)
        {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "physical provider result disagrees with the uploaded object identity",
            ));
        }
        let _publication = self.lock_branch_publication(branch).await;
        self.commit_one(
            branch,
            key,
            kind,
            physical.binding,
            writer_fence_generation,
            OperationKind::Put,
            operation,
            input_digest,
            "PutObject",
            condition,
        )
        .await
    }

    pub fn read_version_stream(
        &self,
        key: &[u8],
        version: ObjectVersionV1,
        range: Option<(u64, u64)>,
    ) -> BoxStream<'static, Result<bytes::Bytes>> {
        let crate::PhysicalObjectBindingV1::Live {
            version_id,
            checksum_sha256,
            ..
        } = version.binding
        else {
            return Box::pin(futures_util::stream::once(async {
                Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "live object version is missing its provider binding",
                ))
            }));
        };
        let plane = self.plane.clone();
        let path = ObjectPath::new(String::from_utf8_lossy(key).into_owned());
        Box::pin(async_stream::try_stream! {
            let path = path?;
            let object = plane
                .get(GetRequest {
                    path,
                    range: range.map(|(start, end)| start..=end),
                    physical_version: Some(PhysicalVersion::Versioned { version_id }),
                })
                .await?
                .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "physical object version is missing"))?;
            if range.is_none() && object.metadata.sha256 != checksum_sha256 {
                Err(Error::new(
                    ErrorCode::CorruptContent,
                    "physical object bytes do not match the committed checksum",
                ))?;
            }
            yield bytes::Bytes::from(object.bytes);
        })
    }

    pub async fn create_physical_multipart_upload(
        &self,
        branch: &str,
        key: Vec<u8>,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        operation: Option<OperationId>,
    ) -> Result<crate::PhysicalMultipartSessionV1> {
        validate_branch(branch)?;
        self.validate_key(&key)?;
        let operation = operation.unwrap_or_else(|| self.new_operation());
        let writer_fence_generation = self.branch_writer_generation(branch).await?;
        let path =
            ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let provider_upload_id = self
            .plane
            .create_physical_multipart(crate::PhysicalMultipartCreate {
                path,
                headers: headers.clone(),
                user_metadata: user_metadata.clone(),
                repository: self.format.repository_id,
                operation,
                writer_fence_generation,
            })
            .await?;
        let session = crate::PhysicalMultipartSessionV1 {
            repository: self.format.repository_id,
            branch: branch.to_string(),
            key,
            headers,
            user_metadata,
            provider_upload_id,
            operation,
            writer_fence_generation,
            created_at_millis: self.now_millis()?,
            discovered: false,
        };
        session.validate_address(self.format.repository_id)?;
        Ok(session)
    }

    pub async fn upload_physical_multipart_part(
        &self,
        session: &crate::PhysicalMultipartSessionV1,
        part_number: u32,
        bytes: Vec<u8>,
    ) -> Result<crate::PhysicalMultipartPartResult> {
        session.validate_address(self.format.repository_id)?;
        if !(1..=10_000).contains(&part_number) || bytes.len() as u64 > MAX_MULTIPART_PART_BYTES {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "physical multipart part number or size is invalid",
            ));
        }
        let path =
            ObjectPath::new(std::str::from_utf8(&session.key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let _payload_permit = self.payload_write_permit().await;
        self.plane
            .upload_physical_multipart_part(crate::PhysicalMultipartUploadPart {
                path,
                upload_id: session.provider_upload_id.clone(),
                part_number,
                bytes,
            })
            .await
    }

    pub async fn upload_physical_multipart_part_stream<S, B, E>(
        &self,
        session: &crate::PhysicalMultipartSessionV1,
        part_number: u32,
        stream: S,
    ) -> Result<crate::PhysicalMultipartPartResult>
    where
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]>,
        E: std::fmt::Display,
    {
        session.validate_address(self.format.repository_id)?;
        if !(1..=10_000).contains(&part_number) {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "part number must be between 1 and 10000",
            ));
        }
        futures_util::pin_mut!(stream);
        let mut spool = tempfile::NamedTempFile::new().map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("could not create physical multipart spool: {error}"),
            )
        })?;
        let mut size = 0_u64;
        let mut checksum = Sha256::new();
        while let Some(next) = stream.next().await {
            let next = next.map_err(|error| {
                Error::new(
                    ErrorCode::Transport,
                    format!("physical multipart part body failed: {error}"),
                )
            })?;
            let next = next.as_ref();
            size = size.checked_add(next.len() as u64).ok_or_else(|| {
                Error::new(ErrorCode::EntityTooLarge, "multipart part length overflow")
            })?;
            if size > MAX_MULTIPART_PART_BYTES {
                return Err(Error::new(
                    ErrorCode::EntityTooLarge,
                    "multipart part exceeds the 5 GiB S3 limit",
                ));
            }
            spool.write_all(next).map_err(|error| {
                Error::new(
                    ErrorCode::Transport,
                    format!("physical multipart spool write failed: {error}"),
                )
            })?;
            checksum.update(next);
        }
        spool.flush().map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("physical multipart spool flush failed: {error}"),
            )
        })?;
        let path =
            ObjectPath::new(std::str::from_utf8(&session.key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let _payload_permit = self.payload_write_permit().await;
        self.plane
            .upload_physical_multipart_file_part(crate::PhysicalMultipartFilePart {
                path,
                upload_id: session.provider_upload_id.clone(),
                part_number,
                body_path: spool.path().to_path_buf(),
                size,
                checksum_sha256: checksum.finalize().into(),
            })
            .await
    }

    pub async fn upload_physical_multipart_part_copy(
        &self,
        session: &crate::PhysicalMultipartSessionV1,
        part_number: u32,
        source_branch: &str,
        source_key: &[u8],
        source_version: Option<ObjectVersionId>,
        range: Option<(u64, u64)>,
    ) -> Result<crate::PhysicalMultipartPartResult> {
        session.validate_address(self.format.repository_id)?;
        if !(1..=10_000).contains(&part_number) {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "part number must be between 1 and 10000",
            ));
        }
        let (_, source) = match source_version {
            Some(version) => {
                self.head_version(source_branch, source_key, version)
                    .await?
            }
            None => self.head_current_at(source_branch, source_key).await?,
        };
        let LogicalObjectVersionKindV1::Live {
            size, checksums, ..
        } = &source.version.body.kind
        else {
            return Err(Error::new(
                ErrorCode::NoSuchKey,
                "multipart copy source is a delete marker",
            ));
        };
        let crate::PhysicalObjectBindingV1::Live { version_id, .. } = &source.version.binding
        else {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "physical multipart live source has a delete-marker binding",
            ));
        };
        let (physical_range, part_size) = match range {
            None if *size <= MAX_MULTIPART_PART_BYTES => (None, *size),
            None => {
                return Err(Error::new(
                    ErrorCode::EntityTooLarge,
                    "multipart copy part exceeds 5 GiB",
                ))
            }
            Some((start, end)) if start <= end && end < *size => {
                let part_size = end
                    .checked_sub(start)
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| {
                        Error::new(ErrorCode::EntityTooLarge, "multipart copy range overflow")
                    })?;
                if part_size > MAX_MULTIPART_PART_BYTES {
                    return Err(Error::new(
                        ErrorCode::EntityTooLarge,
                        "multipart copy part exceeds 5 GiB",
                    ));
                }
                (Some(start..=end), part_size)
            }
            Some(_) => {
                return Err(Error::new(
                    ErrorCode::InvalidRequest,
                    "multipart copy range is not satisfiable",
                ))
            }
        };
        let _payload_permit = self.payload_write_permit().await;
        let result = self
            .plane
            .upload_physical_multipart_part_copy(crate::PhysicalMultipartUploadPartCopy {
                source: ObjectPath::new(std::str::from_utf8(source_key).map_err(|_| {
                    Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
                })?)?,
                source_version_id: version_id.clone(),
                destination: ObjectPath::new(std::str::from_utf8(&session.key).map_err(|_| {
                    Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
                })?)?,
                upload_id: session.provider_upload_id.clone(),
                part_number,
                range: physical_range,
                size: part_size,
            })
            .await?;
        if range.is_none()
            && checksums.sha256.is_some()
            && checksums.sha256 != result.checksum_sha256
        {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "physical multipart copied part checksum differs from its source",
            ));
        }
        Ok(result)
    }

    pub async fn complete_physical_multipart_upload(
        &self,
        session: crate::PhysicalMultipartSessionV1,
        parts: Vec<crate::PhysicalMultipartCompletedPart>,
        checksum_sha256: [u8; 32],
        checksum_md5: [u8; 16],
        size: u64,
        operation: Option<OperationId>,
    ) -> Result<CommitReceipt> {
        session.validate(self.format.repository_id)?;
        if operation.is_some_and(|operation| operation != session.operation) {
            return Err(Error::new(
                ErrorCode::IdempotencyConflict,
                "physical multipart completion must reuse its create operation ID",
            ));
        }
        if parts.is_empty()
            || parts.len() > 10_000
            || parts
                .windows(2)
                .any(|pair| pair[0].part_number >= pair[1].part_number)
            || parts
                .iter()
                .take(parts.len().saturating_sub(1))
                .any(|part| part.size < MIN_NONFINAL_MULTIPART_PART_BYTES)
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "physical multipart completion has invalid ordering, count, or nonfinal part size",
            ));
        }
        let summed_size = parts.iter().try_fold(0_u64, |total, part| {
            total.checked_add(part.size).ok_or_else(|| {
                Error::new(
                    ErrorCode::EntityTooLarge,
                    "multipart object length overflow",
                )
            })
        })?;
        if summed_size != size || size > self.format.canonical_limits.max_object_bytes {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "physical multipart declared size does not match its part receipts",
            ));
        }
        let kind = LogicalObjectVersionKindV1::Live {
            size,
            logical_etag: format!("\"{}\"", hex::encode(checksum_md5)),
            headers: session.headers.clone(),
            checksums: crate::Checksums {
                md5: Some(checksum_md5),
                sha256: Some(checksum_sha256),
                algorithm_values: BTreeMap::new(),
            },
            user_metadata: session.user_metadata.clone(),
            tags: BTreeMap::new(),
        };
        let input_digest = derive_input_digest(&[
            self.format.repository_id.as_bytes(),
            session.branch.as_bytes(),
            b"physical-multipart-complete",
            &session.key,
            session.provider_upload_id.as_bytes(),
            &encode_canonical(&parts)?,
            &encode_canonical(&kind)?,
        ]);
        let operation_lock = self.operation_lock(session.operation);
        let _operation = operation_lock.lock().await;
        if let Some(receipt) = self
            .replay_warm_operation(&session.branch, session.operation, input_digest)
            .await?
        {
            return Ok(receipt);
        }
        if self.branch_writer_generation(&session.branch).await? != session.writer_fence_generation
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "physical multipart upload belongs to an older writer fence",
            ));
        }
        let path =
            ObjectPath::new(std::str::from_utf8(&session.key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let _payload_permit = self.payload_write_permit().await;
        let completed = self
            .plane
            .complete_physical_multipart(crate::PhysicalMultipartComplete {
                path: path.clone(),
                upload_id: session.provider_upload_id,
                parts,
                checksum_sha256,
                checksum_md5,
                size,
            })
            .await;
        let completed = match completed {
            Ok(value) => value,
            Err(error) => match self
                .reconcile_physical_payload(&path, session.operation, checksum_sha256)
                .await?
            {
                Some(value) => value,
                None => return Err(error),
            },
        };
        drop(_payload_permit);
        if completed.size != size
            || completed.logical_etag != format!("\"{}\"", hex::encode(checksum_md5))
            || completed.checksums.sha256 != Some(checksum_sha256)
            || completed.checksums.md5 != Some(checksum_md5)
        {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "physical multipart result disagrees with its declared object identity",
            ));
        }
        let _publication = self.lock_branch_publication(&session.branch).await;
        self.commit_one(
            &session.branch,
            session.key,
            kind,
            completed.binding,
            session.writer_fence_generation,
            OperationKind::Put,
            session.operation,
            input_digest,
            "CompleteMultipartUpload",
            ObjectWriteConditionV1::default(),
        )
        .await
    }

    pub async fn abort_physical_multipart_upload(
        &self,
        session: &crate::PhysicalMultipartSessionV1,
    ) -> Result<()> {
        session.validate_address(self.format.repository_id)?;
        let path =
            ObjectPath::new(std::str::from_utf8(&session.key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        self.plane
            .abort_physical_multipart(crate::PhysicalMultipartAbort {
                path,
                upload_id: session.provider_upload_id.clone(),
            })
            .await
    }

    pub async fn begin_physical_batch(
        &self,
        branch: &str,
        message: impl Into<String>,
        expires_after_millis: u64,
    ) -> Result<PhysicalBatchV1> {
        validate_branch(branch)?;
        let base_commit = self.warm_branch_state(branch).await?.reference.target;
        let now = self.now_millis()?;
        Ok(PhysicalBatchV1 {
            id: self.new_batch(),
            branch: branch.to_string(),
            base_commit,
            operation: self.new_operation(),
            message: message.into(),
            created_at_millis: now,
            expires_at_millis: now.checked_add(expires_after_millis).ok_or_else(|| {
                Error::new(ErrorCode::InvalidRequest, "physical batch expiry overflow")
            })?,
        })
    }

    pub async fn publish_physical_batch(
        &self,
        batch: PhysicalBatchV1,
        mutations: Vec<crate::PhysicalBatchMutationV1>,
    ) -> Result<CommitReceipt> {
        if mutations.is_empty()
            || mutations.len() > self.format.canonical_limits.max_mutations_per_commit as usize
            || batch.expires_at_millis < self.now_millis()?
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "physical batch is empty, expired, or exceeds the mutation limit",
            ));
        }
        let mut unique_keys = BTreeSet::new();
        for mutation in &mutations {
            self.validate_key(mutation.key())?;
            if !unique_keys.insert(mutation.key().to_vec()) {
                return Err(Error::new(
                    ErrorCode::InvalidRequest,
                    "physical batch contains the same key more than once",
                ));
            }
        }
        let request_digest = derive_input_digest(&[
            b"physical-batch",
            &encode_canonical(&mutations)?,
            batch.base_commit.as_bytes(),
        ]);
        let operation_lock = self.operation_lock(batch.operation);
        let _operation = operation_lock.lock().await;
        let warm = self.warm_branch_state(&batch.branch).await?;
        if warm.reference.target != batch.base_commit {
            if let Some(receipt) = self
                .reconcile_operation(&batch.branch, batch.operation, request_digest)
                .await?
            {
                return Ok(receipt);
            }
            return Err(Error::new(
                ErrorCode::BatchConflict,
                "branch moved since physical batch creation",
            ));
        }
        let writer_fence_generation = self.branch_writer_generation(&batch.branch).await?;
        let results =
            futures_util::stream::iter(mutations.into_iter().map(|mutation| async move {
                self.prepare_physical_batch_mutation(
                    mutation,
                    batch.operation,
                    writer_fence_generation,
                )
                .await
            }))
            .buffer_unordered(self.options.max_parallel_payload_writes)
            .collect::<Vec<_>>()
            .await;
        let mut prepared = BTreeMap::new();
        for result in results {
            let (key, mutation) = result?;
            prepared.insert(key, mutation);
        }
        let _publication = self.lock_branch_publication(&batch.branch).await;
        self.commit_batch(&batch, &prepared, request_digest, writer_fence_generation)
            .await
    }

    async fn prepare_physical_batch_mutation(
        &self,
        mutation: crate::PhysicalBatchMutationV1,
        operation: OperationId,
        writer_fence_generation: u64,
    ) -> Result<(Vec<u8>, PhysicalPreparedMutationV1)> {
        match mutation {
            crate::PhysicalBatchMutationV1::Put {
                key,
                bytes,
                headers,
                user_metadata,
            } => {
                let path = ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                    Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
                })?)?;
                let _payload_permit = self.payload_write_permit().await;
                let physical = self
                    .plane
                    .put_physical(crate::PhysicalPut {
                        path,
                        bytes,
                        headers: headers.clone(),
                        user_metadata: user_metadata.clone(),
                        repository: self.format.repository_id,
                        operation,
                        writer_fence_generation,
                    })
                    .await?;
                Ok((
                    key.clone(),
                    PhysicalPreparedMutationV1::PhysicalPut {
                        key,
                        size: physical.size,
                        logical_etag: physical.logical_etag,
                        checksums: physical.checksums,
                        headers,
                        user_metadata,
                        binding: physical.binding,
                    },
                ))
            }
            crate::PhysicalBatchMutationV1::Delete { key } => {
                let path = ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                    Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
                })?)?;
                let _payload_permit = self.payload_write_permit().await;
                let binding = match self
                    .plane
                    .delete_physical(crate::PhysicalDelete {
                        path: path.clone(),
                        repository: self.format.repository_id,
                        operation,
                        writer_fence_generation,
                    })
                    .await
                {
                    Ok(binding) => binding,
                    Err(error) => match self.reconcile_physical_delete(&path).await? {
                        Some(binding) => binding,
                        None => return Err(error),
                    },
                };
                Ok((
                    key.clone(),
                    PhysicalPreparedMutationV1::PhysicalDelete { key, binding },
                ))
            }
        }
    }

    pub async fn delete_object(
        &self,
        branch: &str,
        key: Vec<u8>,
        operation: Option<OperationId>,
    ) -> Result<CommitReceipt> {
        self.validate_key(&key)?;
        let operation = operation.unwrap_or_else(|| self.new_operation());
        let kind = LogicalObjectVersionKindV1::DeleteMarker;
        let input_digest = derive_input_digest(&[
            self.format.repository_id.as_bytes(),
            branch.as_bytes(),
            b"delete",
            &key,
        ]);
        let operation_lock = self.operation_lock(operation);
        let _operation = operation_lock.lock().await;
        if let Some(receipt) = self
            .replay_warm_operation(branch, operation, input_digest)
            .await?
        {
            return Ok(receipt);
        }
        let writer_fence_generation = self.branch_writer_generation(branch).await?;
        let path =
            ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let _payload_permit = self.payload_write_permit().await;
        let binding = match self
            .plane
            .delete_physical(crate::PhysicalDelete {
                path: path.clone(),
                repository: self.format.repository_id,
                operation,
                writer_fence_generation,
            })
            .await
        {
            Ok(binding) => binding,
            Err(error) => match self.reconcile_physical_delete(&path).await? {
                Some(binding) => binding,
                None => return Err(error),
            },
        };
        drop(_payload_permit);
        let _publication = self.lock_branch_publication(branch).await;
        self.commit_one(
            branch,
            key,
            kind,
            binding,
            writer_fence_generation,
            OperationKind::Delete,
            operation,
            input_digest,
            "DeleteObject",
            ObjectWriteConditionV1::default(),
        )
        .await
    }

    /// Publish up to the repository limit of delete markers in one bucket commit.
    pub async fn delete_objects(
        &self,
        branch: &str,
        keys: Vec<Vec<u8>>,
        operation: Option<OperationId>,
    ) -> Result<CommitReceipt> {
        validate_branch(branch)?;
        if keys.is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "delete list is empty",
            ));
        }
        if keys.len() > self.format.canonical_limits.max_delete_objects as usize {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "too many keys in DeleteObjects",
            ));
        }
        for key in &keys {
            self.validate_key(key)?;
        }
        let mut unique_keys = BTreeSet::new();
        if keys.iter().any(|key| !unique_keys.insert(key.clone())) {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "DeleteObjects contains the same key more than once",
            ));
        }
        let operation = operation.unwrap_or_else(|| self.new_operation());
        let encoded_keys = encode_canonical(&keys)?;
        let input_digest = derive_input_digest(&[
            self.format.repository_id.as_bytes(),
            branch.as_bytes(),
            b"multi-delete",
            &encoded_keys,
        ]);
        self.delete_objects_physical(branch, keys, operation, input_digest)
            .await
    }

    async fn delete_objects_physical(
        &self,
        branch: &str,
        keys: Vec<Vec<u8>>,
        operation: OperationId,
        input_digest: [u8; 32],
    ) -> Result<CommitReceipt> {
        let operation_lock = self.operation_lock(operation);
        let _operation = operation_lock.lock().await;
        if let Some(receipt) = self
            .replay_warm_operation(branch, operation, input_digest)
            .await?
        {
            return Ok(receipt);
        }
        let writer_fence_generation = self.branch_writer_generation(branch).await?;
        let results = futures_util::stream::iter(keys.iter().map(|key| async move {
            let path = ObjectPath::new(std::str::from_utf8(key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
            let _payload_permit = self.payload_write_permit().await;
            let binding = match self
                .plane
                .delete_physical(crate::PhysicalDelete {
                    path: path.clone(),
                    repository: self.format.repository_id,
                    operation,
                    writer_fence_generation,
                })
                .await
            {
                Ok(binding) => binding,
                Err(error) => match self.reconcile_physical_delete(&path).await? {
                    Some(binding) => binding,
                    None => return Err(error),
                },
            };
            Ok::<_, Error>((key.clone(), binding))
        }))
        .buffer_unordered(self.options.max_parallel_payload_writes)
        .collect::<Vec<_>>()
        .await;
        let mut bindings = BTreeMap::new();
        for result in results {
            let (key, binding) = result?;
            bindings.insert(key, binding);
        }

        let _publication = self.lock_branch_publication(branch).await;
        let warm = self.warm_branch_state(branch).await?;
        let loaded_ref = LoadedRef {
            value: warm.reference,
            token: warm.token,
        };
        let base = warm.commit;
        let write_store = self.node_store.isolated_write_session();
        let engine = AsyncProlly::new(
            write_store.clone(),
            Config {
                format: self.format.state_tree_format.clone(),
                runtime: RuntimeConfig::default(),
            },
        );
        let mut objects =
            self.tree_from_root(&base.state.objects, &self.format.state_tree_format)?;
        let mut versions =
            self.tree_from_root(&base.state.versions, &self.format.state_tree_format)?;
        let operations =
            self.tree_from_root(&base.state.operations, &self.format.state_tree_format)?;
        if let Some(existing) = engine.get(&operations, operation.as_bytes()).await? {
            let existing: OperationRecordV1 = decode_canonical(&existing)?;
            if existing.input_digest != input_digest {
                return Err(Error::new(
                    ErrorCode::IdempotencyConflict,
                    "operation ID was already used with different input",
                )
                .operation(operation.to_string()));
            }
            return Ok(CommitReceipt {
                id: loaded_ref.value.target,
                operation,
                branch: branch.to_string(),
                parents: base.parents,
                changed_keys: existing.result.changed_keys,
                object_versions: existing.result.object_versions,
                idempotent_replay: true,
            });
        }
        self.maybe_compact_branch_ref_versions(branch, &loaded_ref)
            .await?;
        let created_at_millis = self.now_millis()?;
        let generation = CommitGeneration(base.generation.0.checked_add(1).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "commit generation overflow")
        })?);
        let mut transitions = Vec::with_capacity(keys.len());
        let mut object_versions = Vec::with_capacity(keys.len());
        for (ordinal, key) in keys.iter().enumerate() {
            let previous = engine
                .get(&objects, key)
                .await?
                .map(|bytes| decode_canonical::<CurrentObjectV1>(&bytes))
                .transpose()?
                .map(|current| current.version.id);
            let binding = bindings.remove(key).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "prepared multi-delete binding is missing",
                )
            })?;
            let body = LogicalObjectVersionBodyV1 {
                order: ObjectVersionOrder {
                    commit_generation: generation,
                    mutation_ordinal: u32::try_from(ordinal).map_err(|_| {
                        Error::new(ErrorCode::InvalidLimit, "mutation ordinal overflow")
                    })?,
                },
                created_at_millis,
                kind: LogicalObjectVersionKindV1::DeleteMarker,
            };
            let version =
                ObjectVersionV1::derive(self.format.repository_id, key, operation, body, binding)?;
            objects = engine.delete(&objects, key).await?;
            versions = engine
                .put(
                    &versions,
                    version_tree_key(key, version.body.order, version.id),
                    encode_canonical(&version)?,
                )
                .await?;
            transitions.push(ObjectTransition {
                key: key.clone(),
                previous,
                next: version.id,
                delete_marker: true,
            });
            object_versions.push(version.id);
        }
        let result = CanonicalOperationResult {
            kind: OperationKind::MultiDelete,
            object_versions: object_versions.clone(),
            changed_keys: keys.len() as u64,
        };
        let operations = engine
            .put(
                &operations,
                operation.as_bytes().to_vec(),
                encode_canonical(&OperationRecordV1 {
                    input_digest,
                    result: result.clone(),
                    commit_generation: generation,
                    created_at_millis,
                })?,
            )
            .await?;
        let delta = BucketDeltaV1 {
            operation_ids: vec![operation],
            changes: transitions,
        };
        let prepared = write_store.prepare_node_pack(
            tree_format_digest(&self.format.state_tree_format)?,
            Vec::new(),
        )?;
        let node_pack = prepared.as_ref().map(PreparedNodePack::reference);
        let commit = BucketCommitV1 {
            state: BucketStateV1 {
                objects: TreeRootV1::from_tree(&objects)?,
                versions: TreeRootV1::from_tree(&versions)?,
                operations: TreeRootV1::from_tree(&operations)?,
            },
            parents: vec![loaded_ref.value.target],
            generation,
            delta,
            node_pack,
            writer_fence_generation,
            author: self.options.writer.clone(),
            message: Some("DeleteObjects".to_string()),
            created_at_millis,
            metadata: BTreeMap::new(),
        };
        let stored = self.store_commit(&commit, prepared).await?;
        let commit_id = stored.id;
        let reflog = ReflogEntryV1 {
            branch: branch.to_string(),
            old_target: Some(loaded_ref.value.target),
            new_target: commit_id,
            operation,
            actor: self.options.writer.clone(),
            message: "DeleteObjects".to_string(),
            created_at_millis,
        };
        let next_ref = crate::RefValueV1 {
            target: commit_id,
            previous_target: Some(loaded_ref.value.target),
            generation: RefGeneration(loaded_ref.value.generation.0.checked_add(1).ok_or_else(
                || Error::new(ErrorCode::InternalInvariant, "ref generation overflow"),
            )?),
            operation,
            reflog: reflog.id()?,
            writer: self.options.writer.clone(),
            updated_at_millis: created_at_millis,
            tombstone: false,
            writer_fence_generation,
            inline_reflog: reflog,
        };
        let publication = self
            .controls
            .compare_exchange(CompareExchange {
                path: branch_path(&self.options.repository_prefix, branch)?,
                expected: Some(loaded_ref.token),
                bytes: encode_canonical(&next_ref)?,
            })
            .await;
        match publication {
            Ok(CompareExchangeOutcome::Applied(metadata)) => {
                self.finalize_stored_commit(stored).await?;
                self.cache_branch(branch, next_ref, metadata.token, commit.clone())?;
                Ok(CommitReceipt {
                    id: commit_id,
                    operation,
                    branch: branch.to_string(),
                    parents: commit.parents,
                    changed_keys: keys.len() as u64,
                    object_versions,
                    idempotent_replay: false,
                })
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => {
                self.invalidate_branch_cache(branch)?;
                if let Some(receipt) = self
                    .reconcile_operation(branch, operation, input_digest)
                    .await?
                {
                    self.finalize_stored_commit(stored).await?;
                    return Ok(receipt);
                }
                Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "physical branch CAS conflicted; writer is fenced and must reopen",
                )
                .retry(RetryAdvice::ReloadHead)
                .operation(operation.to_string()))
            }
            Err(error) => {
                self.invalidate_branch_cache(branch)?;
                if let Some(receipt) = self
                    .reconcile_operation(branch, operation, input_digest)
                    .await?
                {
                    self.finalize_stored_commit(stored).await?;
                    return Ok(receipt);
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("branch publication outcome is unknown: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation)
                .operation(operation.to_string()))
            }
        }
    }

    pub async fn copy_object(
        &self,
        branch: &str,
        source_key: &[u8],
        source_version: Option<ObjectVersionId>,
        destination_key: Vec<u8>,
        operation: Option<OperationId>,
    ) -> Result<CommitReceipt> {
        self.validate_key(source_key)?;
        self.validate_key(&destination_key)?;
        let (_, source) = match source_version {
            Some(version) => self.head_version(branch, source_key, version).await?,
            None => self.head_current_at(branch, source_key).await?,
        };
        let LogicalObjectVersionKindV1::Live { .. } = &source.version.body.kind else {
            return Err(Error::new(
                ErrorCode::NoSuchKey,
                "copy source is a delete marker",
            ));
        };
        let operation = operation.unwrap_or_else(|| self.new_operation());
        let kind = source.version.body.kind.clone();
        let kind_bytes = encode_canonical(&kind)?;
        let input_digest = derive_input_digest(&[
            self.format.repository_id.as_bytes(),
            branch.as_bytes(),
            b"copy",
            source_key,
            source.version.id.as_bytes(),
            &destination_key,
            &kind_bytes,
        ]);
        let operation_lock = self.operation_lock(operation);
        let _operation = operation_lock.lock().await;
        if let Some(receipt) = self
            .replay_warm_operation(branch, operation, input_digest)
            .await?
        {
            return Ok(receipt);
        }
        let writer_fence_generation = self.branch_writer_generation(branch).await?;
        let crate::PhysicalObjectBindingV1::Live {
            version_id,
            checksum_sha256,
            ..
        } = source.version.binding.clone()
        else {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "copy source points to a delete marker",
            ));
        };
        let LogicalObjectVersionKindV1::Live {
            size,
            logical_etag,
            headers,
            checksums,
            user_metadata,
            ..
        } = &kind
        else {
            unreachable!("copy source was validated as live")
        };
        let source_path =
            ObjectPath::new(std::str::from_utf8(source_key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let destination_path =
            ObjectPath::new(std::str::from_utf8(&destination_key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let _payload_permit = self.payload_write_permit().await;
        let binding = match self
            .plane
            .copy_physical(crate::PhysicalCopy {
                source: source_path,
                source_version_id: version_id,
                destination: destination_path.clone(),
                headers: headers.clone(),
                user_metadata: user_metadata.clone(),
                repository: self.format.repository_id,
                operation,
                writer_fence_generation,
                checksum_sha256,
                size: *size,
                logical_etag: logical_etag.clone(),
                checksums: checksums.clone(),
            })
            .await
        {
            Ok(result) => result.binding,
            Err(error) => match self
                .reconcile_physical_payload(&destination_path, operation, checksum_sha256)
                .await?
            {
                Some(result) => result.binding,
                None => return Err(error),
            },
        };
        drop(_payload_permit);
        let _publication = self.lock_branch_publication(branch).await;
        self.commit_one(
            branch,
            destination_key,
            kind,
            binding,
            writer_fence_generation,
            OperationKind::Copy,
            operation,
            input_digest,
            "CopyObject",
            ObjectWriteConditionV1::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_one(
        &self,
        branch: &str,
        key: Vec<u8>,
        kind: LogicalObjectVersionKindV1,
        binding: crate::PhysicalObjectBindingV1,
        writer_fence_generation: u64,
        operation_kind: OperationKind,
        operation: OperationId,
        input_digest: [u8; 32],
        message: &str,
        condition: ObjectWriteConditionV1,
    ) -> Result<CommitReceipt> {
        validate_branch(branch)?;
        let created_at_millis = self.now_millis()?;
        let write_store = self.node_store.isolated_write_session();
        let engine = AsyncProlly::new(
            write_store.clone(),
            Config {
                format: self.format.state_tree_format.clone(),
                runtime: RuntimeConfig::default(),
            },
        );
        let warm = self.warm_branch_state(branch).await?;
        let loaded_ref = LoadedRef {
            value: warm.reference,
            token: warm.token,
        };
        let base = warm.commit;
        if condition
            .expected_head
            .is_some_and(|expected| expected != loaded_ref.value.target)
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "branch head does not match the atomic write expectation",
            ));
        }
        let objects = self.tree_from_root(&base.state.objects, &self.format.state_tree_format)?;
        let versions = self.tree_from_root(&base.state.versions, &self.format.state_tree_format)?;
        let operations =
            self.tree_from_root(&base.state.operations, &self.format.state_tree_format)?;

        if let Some(existing) = engine.get(&operations, operation.as_bytes()).await? {
            let existing: OperationRecordV1 = decode_canonical(&existing)?;
            if existing.input_digest != input_digest {
                return Err(Error::new(
                    ErrorCode::IdempotencyConflict,
                    "operation ID was already used with different input",
                )
                .operation(operation.to_string()));
            }
            let receipt = CommitReceipt {
                id: loaded_ref.value.target,
                operation,
                branch: branch.to_string(),
                parents: base.parents,
                changed_keys: existing.result.changed_keys,
                object_versions: existing.result.object_versions,
                idempotent_replay: true,
            };
            return Ok(receipt);
        }

        self.maybe_compact_branch_ref_versions(branch, &loaded_ref)
            .await?;

        let generation = CommitGeneration(base.generation.0.checked_add(1).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "commit generation overflow")
        })?);
        let previous_current = engine
            .get(&objects, &key)
            .await?
            .map(|bytes| decode_canonical::<CurrentObjectV1>(&bytes))
            .transpose()?;
        let current_etag = match previous_current.as_ref() {
            Some(current) => match &current.version.body.kind {
                LogicalObjectVersionKindV1::Live { logical_etag, .. } => Some(logical_etag),
                LogicalObjectVersionKindV1::DeleteMarker => None,
            },
            None => None,
        };
        validate_write_condition(&condition, current_etag.map(String::as_str))?;
        let previous = previous_current.map(|current| current.version.id);
        let body = LogicalObjectVersionBodyV1 {
            order: ObjectVersionOrder {
                commit_generation: generation,
                mutation_ordinal: 0,
            },
            created_at_millis,
            kind: kind.clone(),
        };
        let version = ObjectVersionV1::derive(
            self.format.repository_id,
            &key,
            operation,
            body,
            binding.clone(),
        )?;
        let version_key = version_tree_key(&key, version.body.order, version.id);

        let objects = match &version.body.kind {
            LogicalObjectVersionKindV1::Live { .. } => {
                engine
                    .put(
                        &objects,
                        key.clone(),
                        encode_canonical(&CurrentObjectV1 {
                            version: version.clone(),
                        })?,
                    )
                    .await?
            }
            LogicalObjectVersionKindV1::DeleteMarker => engine.delete(&objects, &key).await?,
        };
        let versions = engine
            .put(&versions, version_key, encode_canonical(&version)?)
            .await?;
        let operation_result = CanonicalOperationResult {
            kind: operation_kind.clone(),
            object_versions: vec![version.id],
            changed_keys: 1,
        };
        let operation_record = OperationRecordV1 {
            input_digest,
            result: operation_result.clone(),
            commit_generation: generation,
            created_at_millis,
        };
        let operations = engine
            .put(
                &operations,
                operation.as_bytes().to_vec(),
                encode_canonical(&operation_record)?,
            )
            .await?;

        let state = BucketStateV1 {
            objects: TreeRootV1::from_tree(&objects)?,
            versions: TreeRootV1::from_tree(&versions)?,
            operations: TreeRootV1::from_tree(&operations)?,
        };
        let delta = BucketDeltaV1 {
            operation_ids: vec![operation],
            changes: vec![ObjectTransition {
                key: key.clone(),
                previous,
                next: version.id,
                delete_marker: matches!(
                    version.body.kind,
                    LogicalObjectVersionKindV1::DeleteMarker
                ),
            }],
        };
        let prepared = write_store.prepare_node_pack(
            tree_format_digest(&self.format.state_tree_format)?,
            Vec::new(),
        )?;
        let node_pack = prepared.as_ref().map(PreparedNodePack::reference);
        let commit = BucketCommitV1 {
            state,
            parents: vec![loaded_ref.value.target],
            generation,
            delta,
            node_pack,
            writer_fence_generation,
            author: self.options.writer.clone(),
            message: Some(message.to_string()),
            created_at_millis,
            metadata: BTreeMap::new(),
        };
        let stored = self.store_commit(&commit, prepared).await?;
        let commit_id = stored.id;
        let reflog = ReflogEntryV1 {
            branch: branch.to_string(),
            old_target: Some(loaded_ref.value.target),
            new_target: commit_id,
            operation,
            actor: self.options.writer.clone(),
            message: message.to_string(),
            created_at_millis,
        };
        let reflog_id = reflog.id()?;
        let next_ref = crate::RefValueV1 {
            target: commit_id,
            previous_target: Some(loaded_ref.value.target),
            generation: RefGeneration(loaded_ref.value.generation.0.checked_add(1).ok_or_else(
                || Error::new(ErrorCode::InternalInvariant, "ref generation overflow"),
            )?),
            operation,
            reflog: reflog_id,
            writer: self.options.writer.clone(),
            updated_at_millis: created_at_millis,
            tombstone: false,
            writer_fence_generation,
            inline_reflog: reflog,
        };
        let publication = self
            .controls
            .compare_exchange(CompareExchange {
                path: branch_path(&self.options.repository_prefix, branch)?,
                expected: Some(loaded_ref.token),
                bytes: encode_canonical(&next_ref)?,
            })
            .await;
        match publication {
            Ok(CompareExchangeOutcome::Applied(metadata)) => {
                self.finalize_stored_commit(stored).await?;
                let receipt = CommitReceipt {
                    id: commit_id,
                    operation,
                    branch: branch.to_string(),
                    parents: commit.parents.clone(),
                    changed_keys: 1,
                    object_versions: operation_result.object_versions,
                    idempotent_replay: false,
                };
                self.cache_branch(branch, next_ref, metadata.token, commit.clone())?;
                Ok(receipt)
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => {
                self.invalidate_branch_cache(branch)?;
                if let Some(receipt) = self
                    .reconcile_operation(branch, operation, input_digest)
                    .await?
                {
                    self.finalize_stored_commit(stored).await?;
                    return Ok(receipt);
                }
                Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "physical branch CAS conflicted; writer is fenced and must reopen",
                )
                .retry(RetryAdvice::ReloadHead)
                .operation(operation.to_string()))
            }
            Err(error) => {
                self.invalidate_branch_cache(branch)?;
                if let Some(receipt) = self
                    .reconcile_operation(branch, operation, input_digest)
                    .await?
                {
                    self.finalize_stored_commit(stored).await?;
                    return Ok(receipt);
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("branch publication outcome is unknown: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation)
                .operation(operation.to_string()))
            }
        }
    }

    async fn commit_batch(
        &self,
        batch: &PhysicalBatchV1,
        mutations: &BTreeMap<Vec<u8>, PhysicalPreparedMutationV1>,
        input_digest: [u8; 32],
        writer_fence_generation: u64,
    ) -> Result<CommitReceipt> {
        let warm = self.warm_branch_state(&batch.branch).await?;
        let loaded_ref = LoadedRef {
            value: warm.reference,
            token: warm.token,
        };
        let base = warm.commit;
        if loaded_ref.value.target != batch.base_commit {
            if let Some(receipt) = self
                .reconcile_operation(&batch.branch, batch.operation, input_digest)
                .await?
            {
                return Ok(receipt);
            }
            return Err(Error::new(
                ErrorCode::BatchConflict,
                "branch moved since batch creation",
            ));
        }
        let write_store = self.node_store.isolated_write_session();
        let engine = AsyncProlly::new(
            write_store.clone(),
            Config {
                format: self.format.state_tree_format.clone(),
                runtime: RuntimeConfig::default(),
            },
        );
        let objects = self.tree_from_root(&base.state.objects, &self.format.state_tree_format)?;
        let versions = self.tree_from_root(&base.state.versions, &self.format.state_tree_format)?;
        let operations =
            self.tree_from_root(&base.state.operations, &self.format.state_tree_format)?;
        let generation = CommitGeneration(base.generation.0.checked_add(1).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "commit generation overflow")
        })?);
        let now = self.now_millis()?;
        let mut transitions = Vec::with_capacity(mutations.len());
        let mut version_ids = Vec::with_capacity(mutations.len());
        let mut object_mutations = Vec::with_capacity(mutations.len());
        let mut version_mutations = Vec::with_capacity(mutations.len());
        for (ordinal, mutation) in mutations.values().enumerate() {
            let key = mutation.key();
            let previous = engine
                .get(&objects, key)
                .await?
                .map(|bytes| decode_canonical::<CurrentObjectV1>(&bytes))
                .transpose()?
                .map(|current| current.version.id);
            let (kind, binding) = match mutation {
                PhysicalPreparedMutationV1::PhysicalPut {
                    size,
                    logical_etag,
                    checksums,
                    headers,
                    user_metadata,
                    binding,
                    ..
                } => (
                    LogicalObjectVersionKindV1::Live {
                        size: *size,
                        logical_etag: logical_etag.clone(),
                        headers: headers.clone(),
                        checksums: checksums.clone(),
                        user_metadata: user_metadata.clone(),
                        tags: BTreeMap::new(),
                    },
                    binding.clone(),
                ),
                PhysicalPreparedMutationV1::PhysicalDelete { binding, .. } => {
                    (LogicalObjectVersionKindV1::DeleteMarker, binding.clone())
                }
            };
            let body = LogicalObjectVersionBodyV1 {
                order: ObjectVersionOrder {
                    commit_generation: generation,
                    mutation_ordinal: u32::try_from(ordinal).map_err(|_| {
                        Error::new(ErrorCode::InvalidLimit, "batch ordinal overflow")
                    })?,
                },
                created_at_millis: now,
                kind,
            };
            let version = ObjectVersionV1::derive(
                self.format.repository_id,
                key,
                batch.operation,
                body,
                binding,
            )?;
            let delete_marker =
                matches!(version.body.kind, LogicalObjectVersionKindV1::DeleteMarker);
            if delete_marker {
                object_mutations.push(Mutation::Delete { key: key.to_vec() });
            } else {
                object_mutations.push(Mutation::Upsert {
                    key: key.to_vec(),
                    val: encode_canonical(&CurrentObjectV1 {
                        version: version.clone(),
                    })?,
                });
            }
            version_mutations.push(Mutation::Upsert {
                key: version_tree_key(key, version.body.order, version.id),
                val: encode_canonical(&version)?,
            });
            transitions.push(ObjectTransition {
                key: key.to_vec(),
                previous,
                next: version.id,
                delete_marker,
            });
            version_ids.push(version.id);
        }
        let objects = engine.batch(&objects, object_mutations).await?;
        let versions = engine.batch(&versions, version_mutations).await?;
        let result = CanonicalOperationResult {
            kind: OperationKind::CommitSession,
            object_versions: version_ids.clone(),
            changed_keys: mutations.len() as u64,
        };
        let operations = engine
            .put(
                &operations,
                batch.operation.as_bytes().to_vec(),
                encode_canonical(&OperationRecordV1 {
                    input_digest,
                    result: result.clone(),
                    commit_generation: generation,
                    created_at_millis: now,
                })?,
            )
            .await?;
        let delta = BucketDeltaV1 {
            operation_ids: vec![batch.operation],
            changes: transitions,
        };
        let prepared = write_store.prepare_node_pack(
            tree_format_digest(&self.format.state_tree_format)?,
            Vec::new(),
        )?;
        let node_pack = prepared.as_ref().map(PreparedNodePack::reference);
        let commit = BucketCommitV1 {
            state: BucketStateV1 {
                objects: TreeRootV1::from_tree(&objects)?,
                versions: TreeRootV1::from_tree(&versions)?,
                operations: TreeRootV1::from_tree(&operations)?,
            },
            parents: vec![batch.base_commit],
            generation,
            delta,
            node_pack,
            writer_fence_generation,
            author: self.options.writer.clone(),
            message: Some(batch.message.clone()),
            created_at_millis: now,
            metadata: BTreeMap::new(),
        };
        self.publish_prepared_commit(
            &batch.branch,
            loaded_ref,
            batch.operation,
            input_digest,
            commit,
            prepared,
            result,
            &batch.message,
        )
        .await
    }

    pub async fn reconcile_operation(
        &self,
        branch: &str,
        operation: OperationId,
        input_digest: [u8; 32],
    ) -> Result<Option<CommitReceipt>> {
        let mut id = self.head(branch).await?;
        loop {
            let commit = self.load_commit(id).await?;
            let operations =
                self.tree_from_root(&commit.state.operations, &self.format.state_tree_format)?;
            if let Some(bytes) = self.engine.get(&operations, operation.as_bytes()).await? {
                let record: OperationRecordV1 = decode_canonical(&bytes)?;
                if record.input_digest != input_digest {
                    return Err(Error::new(
                        ErrorCode::IdempotencyConflict,
                        "operation ID has different input",
                    ));
                }
                if commit.generation == record.commit_generation {
                    return Ok(Some(CommitReceipt {
                        id,
                        operation,
                        branch: branch.to_string(),
                        parents: commit.parents,
                        changed_keys: record.result.changed_keys,
                        object_versions: record.result.object_versions,
                        idempotent_replay: true,
                    }));
                }
            }
            let Some(parent) = commit.parents.first().copied() else {
                return Ok(None);
            };
            id = parent;
        }
    }

    /// Resolve an operation whose caller lost or canceled the publication
    /// response. The operation ID is the durable handle; no request body is
    /// required.
    pub async fn lookup_operation(
        &self,
        branch: &str,
        operation: OperationId,
    ) -> Result<Option<CommitReceipt>> {
        let mut id = self.head(branch).await?;
        loop {
            let commit = self.load_commit(id).await?;
            let operations =
                self.tree_from_root(&commit.state.operations, &self.format.state_tree_format)?;
            if let Some(bytes) = self.engine.get(&operations, operation.as_bytes()).await? {
                let record: OperationRecordV1 = decode_canonical(&bytes)?;
                if commit.generation == record.commit_generation {
                    let receipt = CommitReceipt {
                        id,
                        operation,
                        branch: branch.to_string(),
                        parents: commit.parents,
                        changed_keys: record.result.changed_keys,
                        object_versions: record.result.object_versions,
                        idempotent_replay: true,
                    };
                    return Ok(Some(receipt));
                }
            }
            let Some(parent) = commit.parents.first().copied() else {
                return Ok(None);
            };
            id = parent;
        }
    }

    pub async fn get_current(&self, branch: &str, key: &[u8]) -> Result<ObjectData> {
        self.get_at(branch, key, None).await
    }

    pub async fn head_current(&self, branch: &str, key: &[u8]) -> Result<ObjectSummary> {
        Ok(self.head_current_at(branch, key).await?.1)
    }

    pub async fn head_current_at(
        &self,
        branch: &str,
        key: &[u8],
    ) -> Result<(CommitId, ObjectSummary)> {
        self.validate_key(key)?;
        let snapshot = self.head(branch).await?;
        Ok((snapshot, self.head_current_in(snapshot, key).await?))
    }

    pub async fn head_current_in(&self, snapshot: CommitId, key: &[u8]) -> Result<ObjectSummary> {
        self.validate_key(key)?;
        let commit = self.load_commit(snapshot).await?;
        let objects = self.tree_from_root(&commit.state.objects, &self.format.state_tree_format)?;
        let current = self
            .engine
            .get(&objects, key)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::NoSuchKey, "logical key is absent"))?;
        let current: CurrentObjectV1 = decode_canonical(&current)?;
        current.version.validate()?;
        Ok(ObjectSummary {
            key: key.to_vec(),
            version: current.version,
        })
    }

    pub async fn head_version(
        &self,
        branch: &str,
        key: &[u8],
        version: ObjectVersionId,
    ) -> Result<(CommitId, ObjectSummary)> {
        self.validate_key(key)?;
        let snapshot = self.head(branch).await?;
        Ok((
            snapshot,
            self.head_version_in(snapshot, key, version).await?,
        ))
    }

    pub async fn head_version_in(
        &self,
        snapshot: CommitId,
        key: &[u8],
        version: ObjectVersionId,
    ) -> Result<ObjectSummary> {
        self.validate_key(key)?;
        let commit = self.load_commit(snapshot).await?;
        let version = self.find_version(&commit, key, version).await?;
        Ok(ObjectSummary {
            key: key.to_vec(),
            version,
        })
    }

    pub async fn get_version(
        &self,
        branch: &str,
        key: &[u8],
        version: ObjectVersionId,
    ) -> Result<ObjectData> {
        self.get_at(branch, key, Some(version)).await
    }

    async fn get_at(
        &self,
        branch: &str,
        key: &[u8],
        selected: Option<ObjectVersionId>,
    ) -> Result<ObjectData> {
        self.validate_key(key)?;
        let snapshot = self.head(branch).await?;
        let commit = self.load_commit(snapshot).await?;
        let version = if let Some(selected) = selected {
            self.find_version(&commit, key, selected).await?
        } else {
            let objects =
                self.tree_from_root(&commit.state.objects, &self.format.state_tree_format)?;
            let current = self
                .engine
                .get(&objects, key)
                .await?
                .ok_or_else(|| Error::new(ErrorCode::NoSuchKey, "logical key is absent"))?;
            let current: CurrentObjectV1 = decode_canonical(&current)?;
            current.version.validate()?;
            current.version
        };
        let bytes = match (&version.body.kind, &version.binding) {
            (
                LogicalObjectVersionKindV1::Live { .. },
                crate::PhysicalObjectBindingV1::Live {
                    version_id,
                    checksum_sha256,
                    ..
                },
            ) => {
                let path = ObjectPath::new(std::str::from_utf8(key).map_err(|_| {
                    Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
                })?)?;
                let object = self
                    .plane
                    .get(GetRequest {
                        path,
                        range: None,
                        physical_version: Some(PhysicalVersion::Versioned {
                            version_id: version_id.clone(),
                        }),
                    })
                    .await?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorCode::MissingClosure,
                            "physical object version is missing",
                        )
                    })?;
                if object.metadata.sha256 != *checksum_sha256 {
                    return Err(Error::new(
                        ErrorCode::CorruptContent,
                        "physical object bytes do not match the committed checksum",
                    ));
                }
                object.bytes
            }
            (LogicalObjectVersionKindV1::Live { .. }, _) => {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "live logical object has an invalid provider binding",
                ))
            }
            (LogicalObjectVersionKindV1::DeleteMarker, _) if selected.is_some() => Vec::new(),
            (LogicalObjectVersionKindV1::DeleteMarker, _) => {
                return Err(Error::new(
                    ErrorCode::NoSuchKey,
                    "current version is a delete marker",
                ))
            }
        };
        Ok(ObjectData {
            key: key.to_vec(),
            version,
            bytes,
            snapshot,
        })
    }

    pub async fn list_objects(
        &self,
        branch: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<(CommitId, Vec<ObjectSummary>)> {
        let snapshot = self.head(branch).await?;
        let (objects, _) = self.list_objects_at(snapshot, prefix, None, limit).await?;
        Ok((snapshot, objects))
    }

    pub async fn list_objects_at(
        &self,
        snapshot: CommitId,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<ObjectSummary>, bool)> {
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        let commit = self.load_commit(snapshot).await?;
        let objects = self.tree_from_root(&commit.state.objects, &self.format.state_tree_format)?;
        let mut iter = self.engine.prefix(&objects, prefix).await?;
        let mut result = Vec::with_capacity(limit);
        while result.len() <= limit {
            let Some(entry) = iter.next().await else {
                break;
            };
            let (key, current) = entry?;
            if after.is_some_and(|after| key.as_slice() <= after) {
                continue;
            }
            let current: CurrentObjectV1 = decode_canonical(&current)?;
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
    ) -> Result<(CommitId, Vec<ObjectVersionV1>)> {
        self.validate_key(key)?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        let snapshot = self.head(branch).await?;
        let commit = self.load_commit(snapshot).await?;
        let versions =
            self.tree_from_root(&commit.state.versions, &self.format.state_tree_format)?;
        let prefix = version_tree_prefix(key);
        let mut iter = self.engine.prefix(&versions, &prefix).await?;
        let mut result = Vec::with_capacity(limit);
        while result.len() < limit {
            let Some(entry) = iter.next().await else {
                break;
            };
            let (_, value) = entry?;
            result.push(decode_canonical(&value)?);
        }
        Ok((snapshot, result))
    }

    pub async fn list_versions_prefix(
        &self,
        branch: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<(CommitId, Vec<VersionSummary>)> {
        std::str::from_utf8(prefix)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "prefix is not valid UTF-8"))?;
        let snapshot = self.head(branch).await?;
        let (versions, _) = self.list_versions_at(snapshot, prefix, None, limit).await?;
        Ok((snapshot, versions))
    }

    pub async fn list_versions_at(
        &self,
        snapshot: CommitId,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<VersionSummary>, bool)> {
        std::str::from_utf8(prefix)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "prefix is not valid UTF-8"))?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        let commit = self.load_commit(snapshot).await?;
        let versions =
            self.tree_from_root(&commit.state.versions, &self.format.state_tree_format)?;
        let encoded_prefix = version_tree_partial_prefix(prefix);
        let mut iter = self.engine.prefix(&versions, &encoded_prefix).await?;
        let mut result = Vec::with_capacity(limit);
        while result.len() <= limit {
            let Some(entry) = iter.next().await else {
                break;
            };
            let (encoded_key, value) = entry?;
            if after.is_some_and(|after| encoded_key.as_slice() <= after) {
                continue;
            }
            let key = decode_version_tree_logical_key(&encoded_key)?;
            let version = decode_canonical(&value)?;
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

    pub async fn log(&self, branch: &str, limit: usize) -> Result<Vec<(CommitId, BucketCommitV1)>> {
        self.log_at(self.head(branch).await?, None, limit).await
    }

    /// Start a constant-size-cursor traversal over one or more commit roots.
    /// Additional paged branch/tag roots may be attached later with
    /// [`Self::extend_commit_closure`].
    pub async fn start_commit_closure(&self, roots: &[CommitId]) -> Result<CommitClosureCursor> {
        if roots.is_empty() || roots.len() > 1_000 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "commit-closure start requires between 1 and 1,000 roots",
            ));
        }
        let traversal = self.new_operation();
        let index = self.commit_closure_index(traversal)?;
        let mut cursor = CommitClosureCursor {
            repository: self.format.repository_id,
            traversal,
            state: TreeRootV1::from_tree(&index.engine.create())?,
            next_stack_sequence: u64::MAX,
        };
        self.extend_commit_closure(&mut cursor, roots).await?;
        Ok(cursor)
    }

    /// Attach another bounded page of roots to an existing traversal. This is
    /// how repository-wide fsck/clone/repair first page refs without retaining
    /// all ref targets in memory.
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
        let index = self.commit_closure_index(cursor.traversal)?;
        index.install_root(cursor.state.root.clone())?;
        let mut tree = index.tree()?;
        let mut unique = roots.to_vec();
        unique.sort_unstable();
        unique.dedup();
        let mut mutations = Vec::with_capacity(unique.len());
        for commit in unique.into_iter().rev() {
            if index
                .engine
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
            tree = index.engine.batch(&tree, mutations).await?;
            cursor.state = TreeRootV1::from_tree(&tree)?;
        }
        Ok(())
    }

    /// Advance a durable DAG traversal under explicit work and output bounds.
    /// Commits are emitted parent-before-child so clone/repair pipelines can
    /// materialize parent mappings without buffering the complete history.
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
        let index = self.commit_closure_index(cursor.traversal)?;
        index.install_root(cursor.state.root.clone())?;
        let mut tree = index.tree()?;
        let mut next_cursor = cursor.clone();
        let mut commits = Vec::with_capacity(max_commits.min(64));
        let mut steps = 0usize;
        while steps < max_steps && commits.len() < max_commits {
            let mut queue = index.engine.prefix(&tree, b"q/").await?;
            let Some(entry) = queue.next().await else {
                break;
            };
            let (stack_key, encoded) = entry?;
            drop(queue);
            let work: CommitClosureWork = decode_canonical(&encoded)?;
            let seen_key = commit_closure_seen_key(work.commit);
            let state = index.engine.get(&tree, &seen_key).await?;
            let mut mutations = vec![Mutation::Delete { key: stack_key }];
            if work.finish {
                match state.as_deref() {
                    Some([1]) => {}
                    Some([0]) => {
                        let commit = self.load_commit(work.commit).await?;
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
                        let commit = self.load_commit(work.commit).await?;
                        mutations.push(Mutation::Upsert {
                            key: seen_key,
                            val: vec![0],
                        });
                        self.push_commit_closure_work(
                            &mut next_cursor,
                            &mut mutations,
                            work.commit,
                            true,
                        )?;
                        for parent in commit.parents.iter().rev() {
                            self.push_commit_closure_work(
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
            tree = index.engine.batch(&tree, mutations).await?;
            steps += 1;
        }
        next_cursor.state = TreeRootV1::from_tree(&tree)?;
        let mut remaining = index.engine.prefix(&tree, b"q/").await?;
        let complete = remaining.next().await.is_none();
        Ok(CommitClosurePage {
            commits,
            cursor: next_cursor,
            steps,
            complete,
            budget_exhausted: !complete && steps == max_steps,
        })
    }

    /// Exact-delete one bounded page of immutable traversal-state nodes after
    /// a clone/fsck/repair job has durably committed its final result.
    pub async fn cleanup_commit_closure(
        &self,
        cursor: &CommitClosureCursor,
        limit: usize,
    ) -> Result<CommitClosureCleanupReport> {
        self.validate_commit_closure_cursor(cursor)?;
        if !(1..=1_000).contains(&limit) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "commit-closure cleanup limit must be between 1 and 1,000",
            ));
        }
        let prefix = format!(
            "{}/administration/v2/closure/{}/tree/nodes/sha256/",
            self.options.repository_prefix,
            hex::encode(cursor.traversal.as_bytes())
        );
        let page = self
            .plane
            .list(ListRequest {
                prefix,
                continuation: None,
                limit,
                include_versions: false,
            })
            .await?;
        if page.entries.is_empty() {
            return Ok(CommitClosureCleanupReport {
                deleted_objects: 0,
                complete: true,
            });
        }
        let mut targets = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let token = entry.metadata.token;
            let physical = token
                .version_id
                .clone()
                .map(|version_id| PhysicalVersion::Versioned { version_id })
                .unwrap_or_else(|| PhysicalVersion::Unversioned { token: Some(token) });
            targets.push((entry.path, physical));
        }
        let deleted_objects = targets.len();
        for outcome in self.plane.delete_exact_batch(targets).await? {
            if matches!(outcome, DeleteOutcome::TokenMismatch) {
                return Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "commit-closure state changed during exact cleanup",
                ));
            }
        }
        Ok(CommitClosureCleanupReport {
            deleted_objects,
            complete: page.continuation.is_none(),
        })
    }

    fn push_commit_closure_work(
        &self,
        cursor: &mut CommitClosureCursor,
        mutations: &mut Vec<Mutation>,
        commit: CommitId,
        finish: bool,
    ) -> Result<()> {
        mutations.push(Mutation::Upsert {
            key: commit_closure_stack_key(cursor.next_stack_sequence),
            val: encode_canonical(&CommitClosureWork { commit, finish })?,
        });
        cursor.next_stack_sequence =
            cursor.next_stack_sequence.checked_sub(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::HistoryLimitExceeded,
                    "commit-closure stack sequence is exhausted",
                )
            })?;
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

    fn commit_closure_index(&self, traversal: OperationId) -> Result<ProllyMetadataIndex<P>> {
        let path = format!(
            "administration/v2/closure/{}/tree",
            hex::encode(traversal.as_bytes())
        );
        ProllyMetadataIndex::new(
            self.plane.clone(),
            &self.options.repository_prefix,
            self.format.repository_id,
            self.format.state_tree_format.clone(),
            self.node_cache.clone(),
            MetadataIndexSpec {
                path: &path,
                protocol_version: 6,
                name: "commit-closure",
            },
        )
    }

    /// Bounded, resumable first-parent traversal. Unlike `log_at`, resuming
    /// starts directly at the cursor commit and never walks from the root to
    /// rediscover the previous page boundary.
    pub async fn log_page_bounded(
        &self,
        start: CommitId,
        cursor: Option<&HistoryCursor>,
        requested_limit: usize,
        budget: TraversalBudget,
    ) -> Result<CommitPage> {
        if budget.max_commits == 0 || budget.max_decoded_bytes == 0 || budget.max_elapsed.is_zero()
        {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "history traversal budgets must be greater than zero",
            ));
        }
        if cursor.is_some_and(|cursor| cursor.root != start) {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "history cursor belongs to a different root commit",
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
        let started = std::time::Instant::now();
        let mut current = cursor.map_or(start, |cursor| cursor.next);
        let mut commits = Vec::with_capacity(limit);
        let mut visited_commits = 0usize;
        let mut decoded_bytes = 0u64;
        let mut budget_exhausted = false;
        let continuation = loop {
            if commits.len() >= limit {
                break Some(HistoryCursor {
                    root: start,
                    next: current,
                });
            }
            if visited_commits >= budget.max_commits || started.elapsed() >= budget.max_elapsed {
                budget_exhausted = true;
                break Some(HistoryCursor {
                    root: start,
                    next: current,
                });
            }
            let commit = self.load_commit(current).await?;
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
            let Some(parent) = parent else {
                break None;
            };
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

    /// Advances toward a first-parent ancestor using binary-lifting entries
    /// when available. Each invocation performs at most `max_reads` metadata
    /// or fallback commit reads and returns a durable continuation otherwise.
    pub async fn first_parent_ancestor_bounded(
        &self,
        start: CommitId,
        distance: u64,
        cursor: Option<&FirstParentCursor>,
        max_reads: usize,
    ) -> Result<FirstParentPage> {
        if max_reads == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "first-parent traversal read budget must be greater than zero",
            ));
        }
        if cursor
            .is_some_and(|cursor| cursor.root != start || cursor.requested_distance != distance)
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "first-parent cursor belongs to a different traversal",
            ));
        }
        let mut current = cursor.map_or(start, |cursor| cursor.current);
        let mut remaining = cursor.map_or(distance, |cursor| cursor.remaining);
        let initial_remaining = remaining;
        let mut index_reads = 0usize;
        let mut fallback_commit_reads = 0usize;
        let tree = self.commit_graph.tree()?;
        while remaining > 0 && index_reads + fallback_commit_reads < max_reads {
            index_reads += 1;
            let indexed = self
                .commit_graph
                .engine
                .get(&tree, current.as_bytes())
                .await?
                .map(|bytes| decode_canonical::<CommitGraphEntryV2>(&bytes))
                .transpose()?;
            let mut advanced = false;
            if let Some(entry) = indexed {
                if entry.commit != current {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "commit-graph entry identity mismatch",
                    ));
                }
                let max_level = (63 - remaining.leading_zeros()) as usize;
                if let Some(ancestor) = entry.first_parent_jumps.get(max_level).copied() {
                    current = ancestor;
                    remaining -= 1_u64 << max_level;
                    advanced = true;
                }
            }
            if advanced {
                continue;
            }
            if index_reads + fallback_commit_reads >= max_reads {
                break;
            }
            fallback_commit_reads += 1;
            let commit = self.load_commit(current).await?;
            current = commit.parents.first().copied().ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidRevision,
                    "requested first-parent distance exceeds history",
                )
            })?;
            remaining -= 1;
        }
        let continuation = (remaining > 0).then_some(FirstParentCursor {
            root: start,
            requested_distance: distance,
            current,
            remaining,
        });
        Ok(FirstParentPage {
            ancestor: (remaining == 0).then_some(current),
            continuation,
            edges_advanced: initial_remaining - remaining,
            index_reads,
            fallback_commit_reads,
        })
    }

    /// Bounded first-parent history traversal. `after` is an exclusive commit
    /// cursor and must occur on the selected first-parent chain.
    pub async fn log_at(
        &self,
        start: CommitId,
        after: Option<CommitId>,
        limit: usize,
    ) -> Result<Vec<(CommitId, BucketCommitV1)>> {
        let mut current = start;
        let mut result = Vec::with_capacity(limit);
        let mut cursor_found = after.is_none();
        let mut traversed = 0usize;
        while result.len() < limit {
            let commit = self.load_commit(current).await?;
            let parent = commit.parents.first().copied();
            if cursor_found {
                result.push((current, commit));
            } else if after == Some(current) {
                cursor_found = true;
            }
            traversed += 1;
            if traversed > self.options.history_traversal_limit {
                return Err(Error::new(
                    ErrorCode::HistoryLimitExceeded,
                    "log traversal exceeded its configured limit",
                ));
            }
            let Some(parent) = parent else { break };
            current = parent;
        }
        if !cursor_found {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "log cursor is not on the selected first-parent chain",
            ));
        }
        Ok(result)
    }

    pub async fn commit(&self, id: CommitId) -> Result<BucketCommitV1> {
        self.load_commit(id).await
    }

    pub async fn diff(&self, from: CommitId, to: CommitId) -> Result<Vec<ObjectDiff>> {
        let from_commit = self.load_commit(from).await?;
        let to_commit = self.load_commit(to).await?;
        let from_tree =
            self.tree_from_root(&from_commit.state.objects, &self.format.state_tree_format)?;
        let to_tree =
            self.tree_from_root(&to_commit.state.objects, &self.format.state_tree_format)?;
        self.engine
            .diff(&from_tree, &to_tree)
            .await?
            .into_iter()
            .map(object_diff_from_prolly)
            .collect()
    }

    /// Returns a deterministic, bounded diff page ordered by raw object key.
    pub async fn diff_at(
        &self,
        from: CommitId,
        to: CommitId,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<ObjectDiff>, bool)> {
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        let from_commit = self.load_commit(from).await?;
        let to_commit = self.load_commit(to).await?;
        let from_tree =
            self.tree_from_root(&from_commit.state.objects, &self.format.state_tree_format)?;
        let to_tree =
            self.tree_from_root(&to_commit.state.objects, &self.format.state_tree_format)?;
        let mut differences = Vec::with_capacity(limit.saturating_add(1));
        let mut stream = self.engine.stream_diff(&from_tree, &to_tree);
        while differences.len() <= limit {
            let Some(difference) = stream.next().await else {
                break;
            };
            let difference = object_diff_from_prolly(difference?)?;
            if after.is_some_and(|after| difference.key.as_slice() <= after) {
                continue;
            }
            differences.push(difference);
        }
        let truncated = differences.len() > limit;
        differences.truncate(limit);
        Ok((differences, truncated))
    }

    /// Bounded structural diff that preserves CID-pruning state across pages.
    /// This is the scale-safe alternative to the legacy key-only `diff_at`
    /// cursor, which must rediscover the earlier structural frontier.
    pub async fn diff_page_bounded(
        &self,
        from: CommitId,
        to: CommitId,
        cursor: Option<&ObjectDiffCursor>,
        requested_limit: usize,
    ) -> Result<ObjectDiffPage> {
        if cursor.is_some_and(|cursor| cursor.from != from || cursor.to != to) {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "diff cursor belongs to different commits",
            ));
        }
        let limit = requested_limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "bounded diff page limit must be greater than zero",
            ));
        }
        let from_commit = self.load_commit(from).await?;
        let to_commit = self.load_commit(to).await?;
        let from_tree =
            self.tree_from_root(&from_commit.state.objects, &self.format.state_tree_format)?;
        let to_tree =
            self.tree_from_root(&to_commit.state.objects, &self.format.state_tree_format)?;
        let page = self
            .engine
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
                from,
                to,
                traversal,
            }),
            compared_nodes: page.stats.compared_nodes,
            reused_subtrees: page.stats.reused_subtrees,
        })
    }

    /// Return every best common ancestor in stable commit-ID order.
    ///
    /// A candidate is "best" when it is not an ancestor of another common
    /// ancestor. Callers must select a candidate explicitly when this returns
    /// more than one entry; v1 never synthesizes a recursive virtual base.
    pub async fn merge_bases(&self, left: CommitId, right: CommitId) -> Result<Vec<CommitId>> {
        let left_ancestors = self.ancestor_set(left).await?;
        let right_ancestors = self.ancestor_set(right).await?;
        let common: Vec<_> = left_ancestors
            .intersection(&right_ancestors)
            .copied()
            .collect();
        if common.is_empty() {
            return Err(Error::new(
                ErrorCode::NoMergeBase,
                "commits have no common ancestor",
            ));
        }
        let mut best = Vec::new();
        for candidate in &common {
            let mut shadowed = false;
            for other in &common {
                if candidate != other && self.is_ancestor(*candidate, *other).await? {
                    shadowed = true;
                    break;
                }
            }
            if !shadowed {
                best.push(*candidate);
            }
        }
        best.sort();
        Ok(best)
    }

    pub async fn plan_merge(
        &self,
        target: &str,
        source: CommitId,
        selected_base: Option<CommitId>,
        policy: MergePolicy,
    ) -> Result<MergePlan> {
        let ours = self.head(target).await?;
        self.load_commit(source).await?;
        let best_bases = self.merge_bases(ours, source).await?;
        let base = match selected_base {
            Some(base) if best_bases.contains(&base) => base,
            Some(_) => {
                return Err(Error::new(
                    ErrorCode::InvalidRevision,
                    "selected merge base is not a best common ancestor",
                ))
            }
            None if best_bases.len() == 1 => best_bases[0],
            None => {
                return Err(Error::new(
                    ErrorCode::AmbiguousMergeBase,
                    format!("multiple best merge bases require explicit selection: {best_bases:?}"),
                ))
            }
        };
        let base_commit = self.load_commit(base).await?;
        let ours_commit = self.load_commit(ours).await?;
        let theirs_commit = self.load_commit(source).await?;
        let base_objects = self.current_object_map(&base_commit).await?;
        let ours_objects = self.current_object_map(&ours_commit).await?;
        let theirs_objects = self.current_object_map(&theirs_commit).await?;
        let mut keys = BTreeSet::new();
        keys.extend(base_objects.keys().cloned());
        keys.extend(ours_objects.keys().cloned());
        keys.extend(theirs_objects.keys().cloned());
        let mut changes = Vec::new();
        let mut conflicts = Vec::new();
        for key in keys {
            let base_value = base_objects.get(&key).copied();
            let ours_value = ours_objects.get(&key).copied();
            let theirs_value = theirs_objects.get(&key).copied();
            let selected = if ours_value == theirs_value {
                ours_value
            } else if ours_value == base_value {
                theirs_value
            } else if theirs_value == base_value {
                ours_value
            } else {
                conflicts.push(MergeConflict {
                    key: key.clone(),
                    base: base_value,
                    ours: ours_value,
                    theirs: theirs_value,
                });
                match policy {
                    MergePolicy::Fail | MergePolicy::Ours => ours_value,
                    MergePolicy::Theirs => theirs_value,
                }
            };
            if selected != ours_value {
                changes.push(ObjectDiff {
                    key,
                    from: ours_value,
                    to: selected,
                });
            }
        }
        Ok(MergePlan {
            ours,
            theirs: source,
            best_bases,
            selected_base: Some(base),
            changes,
            conflicts,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn merge(
        &self,
        target: &str,
        source: CommitId,
        selected_base: Option<CommitId>,
        policy: MergePolicy,
        operation: Option<OperationId>,
        message: Option<String>,
    ) -> Result<CommitReceipt> {
        let supplied_operation = operation;
        let operation = operation.unwrap_or_else(|| self.new_operation());
        let policy_byte = match policy {
            MergePolicy::Fail => 0,
            MergePolicy::Ours => 1,
            MergePolicy::Theirs => 2,
        };
        if supplied_operation.is_some() {
            if let Some(existing) = self.lookup_operation(target, operation).await? {
                if existing.parents.len() != 2 || existing.parents[1] != source {
                    return Err(Error::new(
                        ErrorCode::IdempotencyConflict,
                        "merge operation ID was already used with different parents",
                    )
                    .operation(operation.to_string()));
                }
                let original_ours = existing.parents[0];
                let best_bases = self.merge_bases(original_ours, source).await?;
                let base = match selected_base {
                    Some(base) if best_bases.contains(&base) => base,
                    Some(_) => {
                        return Err(Error::new(
                            ErrorCode::IdempotencyConflict,
                            "merge operation replay selected a different base",
                        )
                        .operation(operation.to_string()))
                    }
                    None if best_bases.len() == 1 => best_bases[0],
                    None => return Err(Error::new(
                        ErrorCode::AmbiguousMergeBase,
                        "merge replay with multiple best bases requires the original explicit base",
                    )),
                };
                let input_digest = derive_input_digest(&[
                    self.format.repository_id.as_bytes(),
                    target.as_bytes(),
                    b"merge",
                    original_ours.as_bytes(),
                    source.as_bytes(),
                    base.as_bytes(),
                    &[policy_byte],
                ]);
                return self
                    .reconcile_operation(target, operation, input_digest)
                    .await?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorCode::InternalInvariant,
                            "discovered merge operation disappeared during replay",
                        )
                    });
            }
        }
        // Keep every historical commit and node consulted by the merge live
        // until the resulting branch ref is published. GC sweep takes the
        // write side of this barrier before deleting a candidate batch.
        let _physical_publication = self.lock_branch_publication(target).await;
        let plan = self
            .plan_merge(target, source, selected_base, policy)
            .await?;
        if policy == MergePolicy::Fail && !plan.conflicts.is_empty() {
            return Err(Error::new(
                ErrorCode::MergeConflict,
                format!(
                    "merge has {} unresolved object conflicts",
                    plan.conflicts.len()
                ),
            ));
        }
        let loaded_ref = self.load_ref(target).await?;
        if loaded_ref.value.target != plan.ours {
            return Err(Error::new(
                ErrorCode::RefConflict,
                "target branch moved after merge planning",
            )
            .retry(RetryAdvice::ReloadHead));
        }
        let ours_commit = self.load_commit(plan.ours).await?;
        let theirs_commit = self.load_commit(plan.theirs).await?;
        let base = plan.selected_base.ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "merge plan omitted selected base",
            )
        })?;
        let input_digest = derive_input_digest(&[
            self.format.repository_id.as_bytes(),
            target.as_bytes(),
            b"merge",
            plan.ours.as_bytes(),
            plan.theirs.as_bytes(),
            base.as_bytes(),
            &[policy_byte],
        ]);
        let writer_fence_generation = self.branch_writer_generation(target).await?;
        let write_store = self.node_store.isolated_write_session();
        let engine = AsyncProlly::new(
            write_store.clone(),
            Config {
                format: self.format.state_tree_format.clone(),
                runtime: RuntimeConfig::default(),
            },
        );
        let ours_objects =
            self.tree_from_root(&ours_commit.state.objects, &self.format.state_tree_format)?;
        let ours_versions =
            self.tree_from_root(&ours_commit.state.versions, &self.format.state_tree_format)?;
        let ours_operations = self.tree_from_root(
            &ours_commit.state.operations,
            &self.format.state_tree_format,
        )?;
        let theirs_versions = self.tree_from_root(
            &theirs_commit.state.versions,
            &self.format.state_tree_format,
        )?;
        let theirs_operations = self.tree_from_root(
            &theirs_commit.state.operations,
            &self.format.state_tree_format,
        )?;
        let mut objects = ours_objects;
        let mut versions = self
            .union_tree(&engine, &ours_versions, &theirs_versions, "version")
            .await?;
        let mut operations = self
            .union_tree(&engine, &ours_operations, &theirs_operations, "operation")
            .await?;
        if engine
            .get(&operations, operation.as_bytes())
            .await?
            .is_some()
        {
            return Err(Error::new(
                ErrorCode::IdempotencyConflict,
                "merge operation ID already exists in source history",
            )
            .operation(operation.to_string()));
        }
        let generation = CommitGeneration(
            ours_commit
                .generation
                .0
                .max(theirs_commit.generation.0)
                .checked_add(1)
                .ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "commit generation overflow")
                })?,
        );
        let created_at_millis = self.now_millis()?;
        let mut transitions = Vec::with_capacity(plan.changes.len());
        let mut result_versions = Vec::with_capacity(plan.changes.len());
        for (ordinal, change) in plan.changes.iter().enumerate() {
            let next = match change.to {
                Some(version) => {
                    objects = engine
                        .put(
                            &objects,
                            change.key.clone(),
                            encode_canonical(&CurrentObjectV1 {
                                version: match self
                                    .find_version(&theirs_commit, &change.key, version)
                                    .await
                                {
                                    Ok(found) => found,
                                    Err(error) if error.code == ErrorCode::NoSuchVersion => {
                                        self.find_version(&ours_commit, &change.key, version)
                                            .await?
                                    }
                                    Err(error) => return Err(error),
                                },
                            })?,
                        )
                        .await?;
                    version
                }
                None => {
                    objects = engine.delete(&objects, &change.key).await?;
                    let body = LogicalObjectVersionBodyV1 {
                        order: ObjectVersionOrder {
                            commit_generation: generation,
                            mutation_ordinal: u32::try_from(ordinal).map_err(|_| {
                                Error::new(ErrorCode::InvalidLimit, "merge ordinal overflow")
                            })?,
                        },
                        created_at_millis,
                        kind: LogicalObjectVersionKindV1::DeleteMarker,
                    };
                    let binding = match self
                        .latest_physical_delete_binding(&theirs_commit, &change.key)
                        .await?
                    {
                        Some(binding) => binding,
                        None => {
                            let path = ObjectPath::new(std::str::from_utf8(&change.key).map_err(
                                |_| {
                                    Error::new(
                                        ErrorCode::InvalidKey,
                                        "logical key is not valid UTF-8",
                                    )
                                },
                            )?)?;
                            match self
                                .plane
                                .delete_physical(crate::PhysicalDelete {
                                    path: path.clone(),
                                    repository: self.format.repository_id,
                                    operation,
                                    writer_fence_generation,
                                })
                                .await
                            {
                                Ok(binding) => binding,
                                Err(error) => match self.reconcile_physical_delete(&path).await? {
                                    Some(binding) => binding,
                                    None => return Err(error),
                                },
                            }
                        }
                    };
                    let version = ObjectVersionV1::derive(
                        self.format.repository_id,
                        &change.key,
                        operation,
                        body,
                        binding,
                    )?;
                    versions = engine
                        .put(
                            &versions,
                            version_tree_key(&change.key, version.body.order, version.id),
                            encode_canonical(&version)?,
                        )
                        .await?;
                    version.id
                }
            };
            result_versions.push(next);
            transitions.push(ObjectTransition {
                key: change.key.clone(),
                previous: change.from,
                next,
                delete_marker: change.to.is_none(),
            });
        }
        let operation_result = CanonicalOperationResult {
            kind: OperationKind::Merge,
            object_versions: result_versions.clone(),
            changed_keys: transitions.len() as u64,
        };
        operations = engine
            .put(
                &operations,
                operation.as_bytes().to_vec(),
                encode_canonical(&OperationRecordV1 {
                    input_digest,
                    result: operation_result.clone(),
                    commit_generation: generation,
                    created_at_millis,
                })?,
            )
            .await?;
        let delta = BucketDeltaV1 {
            operation_ids: vec![operation],
            changes: transitions,
        };
        let prepared = write_store.prepare_node_pack(
            tree_format_digest(&self.format.state_tree_format)?,
            Vec::new(),
        )?;
        let node_pack = prepared.as_ref().map(PreparedNodePack::reference);
        let commit = BucketCommitV1 {
            state: BucketStateV1 {
                objects: TreeRootV1::from_tree(&objects)?,
                versions: TreeRootV1::from_tree(&versions)?,
                operations: TreeRootV1::from_tree(&operations)?,
            },
            parents: vec![plan.ours, plan.theirs],
            generation,
            delta,
            node_pack,
            writer_fence_generation,
            author: self.options.writer.clone(),
            message: Some(message.unwrap_or_else(|| "merge".to_string())),
            created_at_millis,
            metadata: BTreeMap::new(),
        };
        self.publish_prepared_commit(
            target,
            loaded_ref,
            operation,
            input_digest,
            commit,
            prepared,
            operation_result,
            "merge",
        )
        .await
    }

    /// Create a new child of `expected_head` whose current-object view matches
    /// `source`. Every changed key receives a fresh logical version; existing
    /// target history is retained.
    pub async fn restore(
        &self,
        branch: &str,
        source: CommitId,
        expected_head: CommitId,
        operation: Option<OperationId>,
        message: Option<String>,
    ) -> Result<CommitReceipt> {
        validate_branch(branch)?;
        let supplied_operation = operation;
        let operation = operation.unwrap_or_else(|| self.new_operation());
        let input_digest = derive_input_digest(&[
            self.format.repository_id.as_bytes(),
            branch.as_bytes(),
            b"restore",
            source.as_bytes(),
            expected_head.as_bytes(),
        ]);
        if supplied_operation.is_some() {
            if let Some(receipt) = self
                .reconcile_operation(branch, operation, input_digest)
                .await?
            {
                return Ok(receipt);
            }
        }
        // A restore may resurrect versions reachable only from an old commit.
        // Hold the publication barrier while loading that history and until
        // the new branch ref makes it reachable again.
        let _physical_publication = self.lock_branch_publication(branch).await;
        let source_commit = self.load_commit(source).await?;
        let loaded_ref = self.load_ref(branch).await?;
        if loaded_ref.value.target != expected_head {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "branch head does not match restore expectation",
            ));
        }
        let ours_commit = self.load_commit(expected_head).await?;
        let ours_map = self.current_object_map(&ours_commit).await?;
        let source_map = self.current_object_map(&source_commit).await?;
        let mut keys = BTreeSet::new();
        keys.extend(ours_map.keys().cloned());
        keys.extend(source_map.keys().cloned());
        let changed: Vec<_> = keys
            .into_iter()
            .filter(|key| ours_map.get(key) != source_map.get(key))
            .collect();
        let writer_fence_generation = self.branch_writer_generation(branch).await?;
        let write_store = self.node_store.isolated_write_session();
        let engine = AsyncProlly::new(
            write_store.clone(),
            Config {
                format: self.format.state_tree_format.clone(),
                runtime: RuntimeConfig::default(),
            },
        );
        let mut objects =
            self.tree_from_root(&ours_commit.state.objects, &self.format.state_tree_format)?;
        let mut versions =
            self.tree_from_root(&ours_commit.state.versions, &self.format.state_tree_format)?;
        let mut operations = self.tree_from_root(
            &ours_commit.state.operations,
            &self.format.state_tree_format,
        )?;
        if let Some(existing) = engine.get(&operations, operation.as_bytes()).await? {
            let record: OperationRecordV1 = decode_canonical(&existing)?;
            if record.input_digest != input_digest {
                return Err(Error::new(
                    ErrorCode::IdempotencyConflict,
                    "restore operation ID was already used with different input",
                )
                .operation(operation.to_string()));
            }
        }
        let generation =
            CommitGeneration(ours_commit.generation.0.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "commit generation overflow")
            })?);
        let created_at_millis = self.now_millis()?;
        let mut transitions = Vec::with_capacity(changed.len());
        let mut version_ids = Vec::with_capacity(changed.len());
        for (ordinal, key) in changed.iter().enumerate() {
            let (kind, binding) = match source_map.get(key).copied() {
                Some(source_version) => {
                    let source_version = self
                        .find_version(&source_commit, key, source_version)
                        .await?;
                    match &source_version.body.kind {
                        LogicalObjectVersionKindV1::Live { .. } => (
                            source_version.body.kind.clone(),
                            source_version.binding.clone(),
                        ),
                        LogicalObjectVersionKindV1::DeleteMarker => {
                            return Err(Error::new(
                                ErrorCode::CorruptCommit,
                                "source current-object root points to a delete marker",
                            ))
                        }
                    }
                }
                None => {
                    let binding = match self
                        .latest_physical_delete_binding(&source_commit, key)
                        .await?
                    {
                        Some(binding) => binding,
                        None => {
                            let path =
                                ObjectPath::new(std::str::from_utf8(key).map_err(|_| {
                                    Error::new(
                                        ErrorCode::InvalidKey,
                                        "logical key is not valid UTF-8",
                                    )
                                })?)?;
                            match self
                                .plane
                                .delete_physical(crate::PhysicalDelete {
                                    path: path.clone(),
                                    repository: self.format.repository_id,
                                    operation,
                                    writer_fence_generation,
                                })
                                .await
                            {
                                Ok(binding) => binding,
                                Err(error) => match self.reconcile_physical_delete(&path).await? {
                                    Some(binding) => binding,
                                    None => return Err(error),
                                },
                            }
                        }
                    };
                    (LogicalObjectVersionKindV1::DeleteMarker, binding)
                }
            };
            let body = LogicalObjectVersionBodyV1 {
                order: ObjectVersionOrder {
                    commit_generation: generation,
                    mutation_ordinal: u32::try_from(ordinal).map_err(|_| {
                        Error::new(ErrorCode::InvalidLimit, "restore ordinal overflow")
                    })?,
                },
                created_at_millis,
                kind,
            };
            let version =
                ObjectVersionV1::derive(self.format.repository_id, key, operation, body, binding)?;
            objects = if matches!(version.body.kind, LogicalObjectVersionKindV1::DeleteMarker) {
                engine.delete(&objects, key).await?
            } else {
                engine
                    .put(
                        &objects,
                        key.clone(),
                        encode_canonical(&CurrentObjectV1 {
                            version: version.clone(),
                        })?,
                    )
                    .await?
            };
            versions = engine
                .put(
                    &versions,
                    version_tree_key(key, version.body.order, version.id),
                    encode_canonical(&version)?,
                )
                .await?;
            transitions.push(ObjectTransition {
                key: key.clone(),
                previous: ours_map.get(key).copied(),
                next: version.id,
                delete_marker: matches!(
                    version.body.kind,
                    LogicalObjectVersionKindV1::DeleteMarker
                ),
            });
            version_ids.push(version.id);
        }
        let operation_result = CanonicalOperationResult {
            kind: OperationKind::Restore,
            object_versions: version_ids,
            changed_keys: transitions.len() as u64,
        };
        operations = engine
            .put(
                &operations,
                operation.as_bytes().to_vec(),
                encode_canonical(&OperationRecordV1 {
                    input_digest,
                    result: operation_result.clone(),
                    commit_generation: generation,
                    created_at_millis,
                })?,
            )
            .await?;
        let delta = BucketDeltaV1 {
            operation_ids: vec![operation],
            changes: transitions,
        };
        let prepared = write_store.prepare_node_pack(
            tree_format_digest(&self.format.state_tree_format)?,
            Vec::new(),
        )?;
        let node_pack = prepared.as_ref().map(PreparedNodePack::reference);
        let commit = BucketCommitV1 {
            state: BucketStateV1 {
                objects: TreeRootV1::from_tree(&objects)?,
                versions: TreeRootV1::from_tree(&versions)?,
                operations: TreeRootV1::from_tree(&operations)?,
            },
            parents: vec![expected_head],
            generation,
            delta,
            node_pack,
            writer_fence_generation,
            author: self.options.writer.clone(),
            message: Some(message.unwrap_or_else(|| format!("restore {source}"))),
            created_at_millis,
            metadata: BTreeMap::new(),
        };
        self.publish_prepared_commit(
            branch,
            loaded_ref,
            operation,
            input_digest,
            commit,
            prepared,
            operation_result,
            "restore",
        )
        .await
    }

    async fn ancestor_set(&self, start: CommitId) -> Result<BTreeSet<CommitId>> {
        let mut seen = BTreeSet::new();
        let mut stack = vec![start];
        while let Some(id) = stack.pop() {
            if !seen.insert(id) {
                continue;
            }
            if seen.len() > self.options.history_traversal_limit {
                return Err(Error::new(
                    ErrorCode::HistoryLimitExceeded,
                    "commit ancestry traversal exceeded its configured limit",
                ));
            }
            stack.extend(self.load_commit(id).await?.parents);
        }
        Ok(seen)
    }

    async fn is_ancestor(&self, ancestor: CommitId, descendant: CommitId) -> Result<bool> {
        if ancestor == descendant {
            return Ok(true);
        }
        let mut seen = BTreeSet::new();
        let mut stack = vec![descendant];
        while let Some(id) = stack.pop() {
            if !seen.insert(id) {
                continue;
            }
            if seen.len() > self.options.history_traversal_limit {
                return Err(Error::new(
                    ErrorCode::HistoryLimitExceeded,
                    "commit ancestry traversal exceeded its configured limit",
                ));
            }
            let commit = self.load_commit(id).await?;
            if commit.parents.contains(&ancestor) {
                return Ok(true);
            }
            stack.extend(commit.parents);
        }
        Ok(false)
    }

    async fn current_object_map(
        &self,
        commit: &BucketCommitV1,
    ) -> Result<BTreeMap<Vec<u8>, ObjectVersionId>> {
        let tree = self.tree_from_root(&commit.state.objects, &self.format.state_tree_format)?;
        let mut iter = self.engine.range(&tree, &[], None).await?;
        let mut result = BTreeMap::new();
        while let Some(entry) = iter.next().await {
            let (key, value) = entry?;
            let current: CurrentObjectV1 = decode_canonical(&value)?;
            result.insert(key, current.version.id);
        }
        Ok(result)
    }

    async fn union_tree(
        &self,
        engine: &AsyncProlly<ProllyObjectStore<P>>,
        left: &Tree,
        right: &Tree,
        label: &str,
    ) -> Result<Tree> {
        let mut result = left.clone();
        let mut iter = self.engine.range(right, &[], None).await?;
        while let Some(entry) = iter.next().await {
            let (key, value) = entry?;
            if let Some(existing) = engine.get(&result, &key).await? {
                if existing != value {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        format!("same {label} tree key has unequal immutable values"),
                    ));
                }
            } else {
                result = engine.put(&result, key, value).await?;
            }
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_prepared_commit(
        &self,
        branch: &str,
        loaded_ref: LoadedRef,
        operation: OperationId,
        input_digest: [u8; 32],
        commit: BucketCommitV1,
        prepared: Option<PreparedNodePack>,
        operation_result: CanonicalOperationResult,
        reflog_message: &str,
    ) -> Result<CommitReceipt> {
        let writer_fence_generation = commit.writer_fence_generation;
        if writer_fence_generation == 0 {
            return Err(Error::new(
                ErrorCode::InternalInvariant,
                "prepared commit has a zero writer fence generation",
            ));
        }
        self.maybe_compact_branch_ref_versions(branch, &loaded_ref)
            .await?;
        let stored = self.store_commit(&commit, prepared).await?;
        let commit_id = stored.id;
        let reflog = ReflogEntryV1 {
            branch: branch.to_string(),
            old_target: Some(loaded_ref.value.target),
            new_target: commit_id,
            operation,
            actor: self.options.writer.clone(),
            message: reflog_message.to_string(),
            created_at_millis: commit.created_at_millis,
        };
        let reflog_id = reflog.id()?;
        let next_ref = crate::RefValueV1 {
            target: commit_id,
            previous_target: Some(loaded_ref.value.target),
            generation: RefGeneration(loaded_ref.value.generation.0.checked_add(1).ok_or_else(
                || Error::new(ErrorCode::InternalInvariant, "ref generation overflow"),
            )?),
            operation,
            reflog: reflog_id,
            writer: self.options.writer.clone(),
            updated_at_millis: commit.created_at_millis,
            tombstone: false,
            writer_fence_generation,
            inline_reflog: reflog,
        };
        let publication = self
            .controls
            .compare_exchange(CompareExchange {
                path: branch_path(&self.options.repository_prefix, branch)?,
                expected: Some(loaded_ref.token),
                bytes: encode_canonical(&next_ref)?,
            })
            .await;
        match publication {
            Ok(CompareExchangeOutcome::Applied(metadata)) => {
                self.finalize_stored_commit(stored).await?;
                let receipt = CommitReceipt {
                    id: commit_id,
                    operation,
                    branch: branch.to_string(),
                    parents: commit.parents.clone(),
                    changed_keys: operation_result.changed_keys,
                    object_versions: operation_result.object_versions,
                    idempotent_replay: false,
                };
                self.cache_branch(branch, next_ref, metadata.token, commit)?;
                Ok(receipt)
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => {
                self.invalidate_branch_cache(branch)?;
                if let Some(receipt) = self
                    .reconcile_operation(branch, operation, input_digest)
                    .await?
                {
                    self.finalize_stored_commit(stored).await?;
                    return Ok(receipt);
                }
                Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "physical branch CAS conflicted; writer is fenced and must reopen",
                )
                .retry(RetryAdvice::ReloadHead)
                .operation(operation.to_string()))
            }
            Err(error) => {
                self.invalidate_branch_cache(branch)?;
                if let Some(receipt) = self
                    .reconcile_operation(branch, operation, input_digest)
                    .await?
                {
                    self.finalize_stored_commit(stored).await?;
                    return Ok(receipt);
                }
                Err(Error::new(
                    ErrorCode::OutcomeUnknown,
                    format!("branch publication outcome is unknown: {error}"),
                )
                .retry(RetryAdvice::ReconcileOperation)
                .operation(operation.to_string()))
            }
        }
    }

    pub async fn fsck(&self) -> Result<FsckReport> {
        let mut cursor = None;
        let mut continuation = None;
        loop {
            let page = self.list_branches_page(continuation, 1_000).await?;
            let roots = page
                .branches
                .iter()
                .map(|branch| branch.target)
                .collect::<Vec<_>>();
            if !roots.is_empty() {
                match cursor.as_mut() {
                    Some(cursor) => {
                        self.extend_resumable_fsck(cursor, &roots, roots.len(), 0)
                            .await?;
                    }
                    None => {
                        cursor = Some(self.start_resumable_fsck(&roots, roots.len(), 0).await?);
                    }
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        continuation = None;
        loop {
            let page = self.list_tags_page(continuation, 1_000).await?;
            let roots = page.tags.iter().map(|tag| tag.target).collect::<Vec<_>>();
            if !roots.is_empty() {
                match cursor.as_mut() {
                    Some(cursor) => {
                        self.extend_resumable_fsck(cursor, &roots, 0, roots.len())
                            .await?;
                    }
                    None => {
                        cursor = Some(self.start_resumable_fsck(&roots, 0, roots.len()).await?);
                    }
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        let cursor = cursor.ok_or_else(|| {
            Error::new(
                ErrorCode::MissingClosure,
                "repository fsck found no live branch or tag roots",
            )
        })?;
        self.run_resumable_fsck(cursor).await
    }

    /// Verifies one selected commit closure. This is the incremental fsck
    /// primitive used after fetch/push or by a caller walking new heads.
    pub async fn fsck_commit(&self, head: CommitId) -> Result<FsckReport> {
        let cursor = self.start_resumable_fsck(&[head], 0, 0).await?;
        self.run_resumable_fsck(cursor).await
    }

    /// Copies only missing objects in the selected source closure, then
    /// verifies the repaired destination head. Immutable objects that are
    /// present but corrupt still fail closed and require operator recovery
    /// from a physically versioned bucket or backup.
    pub async fn repair_missing_from<Q: ObjectPlane>(
        &self,
        source: &Repository<Q>,
        source_branch: &str,
    ) -> Result<RepairReport> {
        self.validate_sync_identity(source)?;
        let _source_history = source.preserve_history_for_gc().await;
        let current = self.head(source_branch).await?;
        if let Ok(fsck) = self.fsck_commit(current).await {
            return Ok(RepairReport {
                sync: SyncReport {
                    source_head: Some(current),
                    already_present: 1,
                    ..SyncReport::default()
                },
                fsck,
            });
        }
        let _publication = self.lock_branch_publication(source_branch).await;
        let source_head = source.head(source_branch).await?;
        let (mapped, mut sync) = source
            .replay_physical_history_to(self, &[source_head], true)
            .await?;
        let repaired_head = *mapped.get(&source_head).ok_or_else(|| {
            Error::new(
                ErrorCode::MissingClosure,
                "physical repair did not return a mapped head",
            )
        })?;
        let loaded = self.load_ref(source_branch).await?;
        if loaded.value.target != current {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "destination branch moved during physical repair",
            ));
        }
        let movement = self
            .move_ref_inner(
                source_branch,
                loaded,
                repaired_head,
                "repair physical bindings from qualified source",
            )
            .await?;
        sync.source_head = Some(repaired_head);
        sync.ref_move = Some(movement);
        let fsck = self.fsck_commit(repaired_head).await?;
        Ok(RepairReport { sync, fsck })
    }

    /// Start a restartable deep verification job. Root enumeration itself is
    /// paged by the caller; attach later pages with `extend_resumable_fsck`.
    pub async fn start_resumable_fsck(
        &self,
        roots: &[CommitId],
        branch_count: usize,
        tag_count: usize,
    ) -> Result<ResumableFsckCursor> {
        let closure = self.start_commit_closure(roots).await?;
        Ok(ResumableFsckCursor {
            closure,
            report: FsckReport {
                branches: branch_count,
                tags: tag_count,
                ..FsckReport::default()
            },
            phase: ResumableFsckPhase::DiscoverCommits,
        })
    }

    pub async fn extend_resumable_fsck(
        &self,
        cursor: &mut ResumableFsckCursor,
        roots: &[CommitId],
        branch_count: usize,
        tag_count: usize,
    ) -> Result<()> {
        if cursor.phase != ResumableFsckPhase::DiscoverCommits {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "fsck roots cannot be extended after commit discovery starts",
            ));
        }
        self.extend_commit_closure(&mut cursor.closure, roots)
            .await?;
        cursor.report.branches = cursor
            .report
            .branches
            .checked_add(branch_count)
            .ok_or_else(|| Error::new(ErrorCode::EntityTooLarge, "fsck branch count overflow"))?;
        cursor.report.tags = cursor
            .report
            .tags
            .checked_add(tag_count)
            .ok_or_else(|| Error::new(ErrorCode::EntityTooLarge, "fsck tag count overflow"))?;
        Ok(())
    }

    /// Advance one bounded phase of deep verification. `max_items` bounds
    /// emitted commits, decoded nodes, or physical-version requests depending
    /// on the current phase; delete-marker pagination consumes one item per
    /// provider LIST page.
    pub async fn resumable_fsck_page(
        &self,
        cursor: &ResumableFsckCursor,
        max_steps: usize,
        max_items: usize,
    ) -> Result<ResumableFsckPage> {
        self.validate_commit_closure_cursor(&cursor.closure)?;
        if !(1..=100_000).contains(&max_steps) || !(1..=1_000).contains(&max_items) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "fsck page requires 1..=100,000 steps and 1..=1,000 items",
            ));
        }
        match cursor.phase {
            ResumableFsckPhase::DiscoverCommits => {
                self.resumable_fsck_discover_page(cursor, max_steps, max_items)
                    .await
            }
            ResumableFsckPhase::VerifyNodes => {
                self.resumable_fsck_node_page(cursor, max_items).await
            }
            ResumableFsckPhase::VerifyVersions => {
                self.resumable_fsck_version_page(cursor, max_items).await
            }
            ResumableFsckPhase::Complete => Ok(ResumableFsckPage {
                cursor: cursor.clone(),
                processed_commits: 0,
                processed_nodes: 0,
                processed_versions: 0,
                traversal_steps: 0,
                complete: true,
                budget_exhausted: false,
            }),
        }
    }

    async fn resumable_fsck_discover_page(
        &self,
        cursor: &ResumableFsckCursor,
        max_steps: usize,
        max_items: usize,
    ) -> Result<ResumableFsckPage> {
        let page = self
            .commit_closure_page(&cursor.closure, max_steps, max_items)
            .await?;
        let processed_commits = page.commits.len();
        let index = self.commit_closure_index(cursor.closure.traversal)?;
        index.install_root(page.cursor.state.root.clone())?;
        let mut tree = index.tree()?;
        let mut mutations = Vec::with_capacity(processed_commits.saturating_mul(3));
        let format_digest = tree_format_digest(&self.format.state_tree_format)?;
        for (_, commit) in page.commits {
            self.load_commit_delta(&commit).await?;
            for (kind, root) in [
                (FSCK_OBJECT_TREE, &commit.state.objects),
                (FSCK_VERSION_TREE, &commit.state.versions),
                (FSCK_OPERATION_TREE, &commit.state.operations),
            ] {
                if root.format_digest != format_digest {
                    return Err(Error::new(
                        ErrorCode::CorruptNode,
                        "fsck encountered a state root with an incompatible format",
                    ));
                }
                if let Some(cid) = root.root.clone() {
                    mutations.push(Mutation::Upsert {
                        key: fsck_node_queue_key(kind, &cid),
                        val: encode_canonical(&FsckNodeWork { kind, cid })?,
                    });
                }
            }
        }
        if !mutations.is_empty() {
            tree = index.engine.batch(&tree, mutations).await?;
        }
        let mut next = cursor.clone();
        next.closure = page.cursor;
        next.closure.state = TreeRootV1::from_tree(&tree)?;
        next.report.commits = next
            .report
            .commits
            .checked_add(processed_commits)
            .ok_or_else(|| Error::new(ErrorCode::EntityTooLarge, "fsck commit count overflow"))?;
        next.report.deltas = next
            .report
            .deltas
            .checked_add(processed_commits)
            .ok_or_else(|| Error::new(ErrorCode::EntityTooLarge, "fsck delta count overflow"))?;
        if page.complete {
            next.phase = ResumableFsckPhase::VerifyNodes;
        }
        Ok(ResumableFsckPage {
            cursor: next,
            processed_commits,
            processed_nodes: 0,
            processed_versions: 0,
            traversal_steps: page.steps,
            complete: false,
            budget_exhausted: !page.complete && page.budget_exhausted,
        })
    }

    async fn resumable_fsck_node_page(
        &self,
        cursor: &ResumableFsckCursor,
        max_items: usize,
    ) -> Result<ResumableFsckPage> {
        let index = self.commit_closure_index(cursor.closure.traversal)?;
        index.install_root(cursor.closure.state.root.clone())?;
        let mut tree = index.tree()?;
        let mut iter = index.engine.prefix(&tree, b"fq/").await?;
        let mut work = Vec::with_capacity(max_items);
        while work.len() < max_items {
            let Some(entry) = iter.next().await else {
                break;
            };
            work.push(entry?);
        }
        drop(iter);
        let mut next = cursor.clone();
        if work.is_empty() {
            next.phase = ResumableFsckPhase::VerifyVersions;
            return Ok(ResumableFsckPage {
                cursor: next,
                processed_commits: 0,
                processed_nodes: 0,
                processed_versions: 0,
                traversal_steps: 0,
                complete: false,
                budget_exhausted: false,
            });
        }
        let mut mutations = Vec::new();
        let mut globally_marked = BTreeSet::new();
        for (queue_key, encoded) in &work {
            let node_work: FsckNodeWork = decode_canonical(encoded)?;
            if !matches!(
                node_work.kind,
                FSCK_OBJECT_TREE | FSCK_VERSION_TREE | FSCK_OPERATION_TREE
            ) || fsck_node_queue_key(node_work.kind, &node_work.cid) != *queue_key
            {
                return Err(Error::new(
                    ErrorCode::CorruptNode,
                    "fsck node queue record is malformed",
                ));
            }
            let semantic_mark = fsck_node_seen_key(node_work.kind, &node_work.cid);
            mutations.push(Mutation::Delete {
                key: queue_key.clone(),
            });
            if index.engine.get(&tree, &semantic_mark).await?.is_some() {
                continue;
            }
            let bytes = self
                .node_store
                .get(node_work.cid.as_bytes())
                .await?
                .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "fsck node is missing"))?;
            let node = Node::from_bytes_with_format(&bytes, &self.format.state_tree_format)
                .map_err(|error| {
                    Error::new(
                        ErrorCode::CorruptNode,
                        format!("fsck could not decode a Prolly node: {error}"),
                    )
                })?;
            if node.leaf {
                for (key, value) in node.keys.iter().zip(&node.vals) {
                    match node_work.kind {
                        FSCK_OBJECT_TREE => {
                            let current: CurrentObjectV1 = decode_canonical(value)?;
                            current.version.validate()?;
                        }
                        FSCK_VERSION_TREE => {
                            let version: ObjectVersionV1 = decode_canonical(value)?;
                            version.validate()?;
                            let logical_key = decode_version_tree_logical_key(key)?;
                            let digest = derive_input_digest(&[
                                b"fsck-version-work-v1",
                                &logical_key,
                                value,
                            ]);
                            let seen_key = fsck_version_seen_key(&digest);
                            if index.engine.get(&tree, &seen_key).await?.is_none() {
                                mutations.push(Mutation::Upsert {
                                    key: fsck_version_queue_key(&digest),
                                    val: encode_canonical(&FsckVersionWork {
                                        key: logical_key,
                                        version,
                                        continuation: None,
                                    })?,
                                });
                            }
                        }
                        FSCK_OPERATION_TREE => {
                            let _: OperationRecordV1 = decode_canonical(value)?;
                        }
                        _ => {
                            return Err(Error::new(
                                ErrorCode::CorruptNode,
                                "fsck node work has an invalid tree kind",
                            ));
                        }
                    }
                }
            } else {
                for value in node.vals {
                    let child = prolly::Cid(value.as_slice().try_into().map_err(|_| {
                        Error::new(
                            ErrorCode::CorruptNode,
                            "fsck internal node contains an invalid child CID",
                        )
                    })?);
                    mutations.push(Mutation::Upsert {
                        key: fsck_node_queue_key(node_work.kind, &child),
                        val: encode_canonical(&FsckNodeWork {
                            kind: node_work.kind,
                            cid: child,
                        })?,
                    });
                }
            }
            mutations.push(Mutation::Upsert {
                key: semantic_mark,
                val: Vec::new(),
            });
            let global_mark = fsck_global_node_seen_key(&node_work.cid);
            if globally_marked.insert(node_work.cid.clone())
                && index.engine.get(&tree, &global_mark).await?.is_none()
            {
                mutations.push(Mutation::Upsert {
                    key: global_mark,
                    val: Vec::new(),
                });
                next.report.reachable_nodes =
                    next.report.reachable_nodes.checked_add(1).ok_or_else(|| {
                        Error::new(ErrorCode::EntityTooLarge, "fsck node count overflow")
                    })?;
                next.report.reachable_node_bytes = next
                    .report
                    .reachable_node_bytes
                    .checked_add(bytes.len())
                    .ok_or_else(|| {
                        Error::new(ErrorCode::EntityTooLarge, "fsck node byte count overflow")
                    })?;
            }
        }
        tree = index.engine.batch(&tree, mutations).await?;
        next.closure.state = TreeRootV1::from_tree(&tree)?;
        let mut remaining = index.engine.prefix(&tree, b"fq/").await?;
        let exhausted = remaining.next().await.is_some();
        if !exhausted {
            next.phase = ResumableFsckPhase::VerifyVersions;
        }
        Ok(ResumableFsckPage {
            cursor: next,
            processed_commits: 0,
            processed_nodes: work.len(),
            processed_versions: 0,
            traversal_steps: 0,
            complete: false,
            budget_exhausted: exhausted,
        })
    }

    async fn resumable_fsck_version_page(
        &self,
        cursor: &ResumableFsckCursor,
        max_items: usize,
    ) -> Result<ResumableFsckPage> {
        let index = self.commit_closure_index(cursor.closure.traversal)?;
        index.install_root(cursor.closure.state.root.clone())?;
        let mut tree = index.tree()?;
        let mut iter = index.engine.prefix(&tree, b"fvq/").await?;
        let mut work = Vec::with_capacity(max_items);
        while work.len() < max_items {
            let Some(entry) = iter.next().await else {
                break;
            };
            work.push(entry?);
        }
        drop(iter);
        let mut next = cursor.clone();
        if work.is_empty() {
            next.phase = ResumableFsckPhase::Complete;
            return Ok(ResumableFsckPage {
                cursor: next,
                processed_commits: 0,
                processed_nodes: 0,
                processed_versions: 0,
                traversal_steps: 0,
                complete: true,
                budget_exhausted: false,
            });
        }
        let mut mutations = Vec::new();
        for (queue_key, encoded) in &work {
            let mut version_work: FsckVersionWork = decode_canonical(encoded)?;
            let digest = queue_key
                .strip_prefix(b"fvq/")
                .and_then(|bytes| <&[u8; 32]>::try_from(bytes).ok())
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::CorruptCommit,
                        "fsck version queue key is malformed",
                    )
                })?;
            let seen_key = fsck_version_seen_key(digest);
            let expected_digest = derive_input_digest(&[
                b"fsck-version-work-v1",
                &version_work.key,
                &encode_canonical(&version_work.version)?,
            ]);
            if expected_digest.as_slice() != digest {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "fsck version queue record does not match its key",
                ));
            }
            if index.engine.get(&tree, &seen_key).await?.is_some() {
                mutations.push(Mutation::Delete {
                    key: queue_key.clone(),
                });
                continue;
            }
            match self.verify_physical_version_page(&mut version_work).await? {
                Some(verified_bytes) => {
                    mutations.push(Mutation::Delete {
                        key: queue_key.clone(),
                    });
                    mutations.push(Mutation::Upsert {
                        key: seen_key,
                        val: Vec::new(),
                    });
                    next.report.logical_versions =
                        next.report.logical_versions.checked_add(1).ok_or_else(|| {
                            Error::new(ErrorCode::EntityTooLarge, "fsck version count overflow")
                        })?;
                    next.report.content_bytes_verified = next
                        .report
                        .content_bytes_verified
                        .checked_add(verified_bytes)
                        .ok_or_else(|| {
                            Error::new(ErrorCode::EntityTooLarge, "fsck provider bytes overflow")
                        })?;
                }
                None => mutations.push(Mutation::Upsert {
                    key: queue_key.clone(),
                    val: encode_canonical(&version_work)?,
                }),
            }
        }
        tree = index.engine.batch(&tree, mutations).await?;
        next.closure.state = TreeRootV1::from_tree(&tree)?;
        let mut remaining = index.engine.prefix(&tree, b"fvq/").await?;
        let exhausted = remaining.next().await.is_some();
        if !exhausted {
            next.phase = ResumableFsckPhase::Complete;
        }
        Ok(ResumableFsckPage {
            cursor: next,
            processed_commits: 0,
            processed_nodes: 0,
            processed_versions: work.len(),
            traversal_steps: 0,
            complete: !exhausted,
            budget_exhausted: exhausted,
        })
    }

    async fn run_resumable_fsck(&self, mut cursor: ResumableFsckCursor) -> Result<FsckReport> {
        loop {
            let page = match self.resumable_fsck_page(&cursor, 4_096, 256).await {
                Ok(page) => page,
                Err(error) => {
                    // Compatibility fsck owns its internal cursor. A public
                    // workflow keeps the cursor and chooses its own retry or
                    // cleanup policy, but this synchronous wrapper must not
                    // leak abandoned immutable job state on verification
                    // failure.
                    loop {
                        match self.cleanup_commit_closure(&cursor.closure, 1_000).await {
                            Ok(cleanup) if cleanup.complete => break,
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                    return Err(error);
                }
            };
            cursor = page.cursor;
            if page.complete {
                break;
            }
        }
        let report = cursor.report.clone();
        loop {
            let cleanup = self.cleanup_commit_closure(&cursor.closure, 1_000).await?;
            if cleanup.complete {
                break;
            }
        }
        Ok(report)
    }

    async fn verify_physical_version_page(
        &self,
        work: &mut FsckVersionWork,
    ) -> Result<Option<u64>> {
        work.version.validate()?;
        let path = ObjectPath::new(std::str::from_utf8(&work.key).map_err(|_| {
            Error::new(
                ErrorCode::CorruptCommit,
                "physical logical key is not UTF-8",
            )
        })?)?;
        match &work.version.binding {
            crate::PhysicalObjectBindingV1::Live {
                version_id,
                checksum_sha256,
                ..
            } => {
                let spool = tempfile::NamedTempFile::new().map_err(|error| {
                    Error::new(
                        ErrorCode::Transport,
                        format!("could not create fsck spool: {error}"),
                    )
                })?;
                let object = self
                    .plane
                    .get_physical_file(crate::PhysicalFileGet {
                        path,
                        version_id: version_id.clone(),
                        body_path: spool.path().to_path_buf(),
                    })
                    .await?;
                let expected_size = match work.version.body.kind {
                    LogicalObjectVersionKindV1::Live { size, .. } => size,
                    LogicalObjectVersionKindV1::DeleteMarker => unreachable!("binding validated"),
                };
                if object.size != expected_size || object.checksum_sha256 != *checksum_sha256 {
                    return Err(Error::new(
                        ErrorCode::CorruptContent,
                        "retained physical object version checksum or size mismatch",
                    ));
                }
                Ok(Some(expected_size))
            }
            crate::PhysicalObjectBindingV1::DeleteMarker { version_id } => {
                let page = self
                    .plane
                    .list(ListRequest {
                        prefix: path.as_str().to_string(),
                        continuation: work.continuation.take(),
                        limit: 1_000,
                        include_versions: true,
                    })
                    .await?;
                if page.entries.iter().any(|entry| {
                    entry.path == path
                        && entry.metadata.delete_marker
                        && entry.metadata.token.version_id.as_deref() == Some(version_id.as_str())
                }) {
                    return Ok(Some(0));
                }
                work.continuation = page.continuation;
                if work.continuation.is_none() {
                    return Err(Error::new(
                        ErrorCode::MissingClosure,
                        "retained physical delete marker is missing",
                    ));
                }
                Ok(None)
            }
        }
    }

    async fn reconcile_physical_payload(
        &self,
        path: &ObjectPath,
        operation: OperationId,
        expected_sha256: [u8; 32],
    ) -> Result<Option<crate::PhysicalObjectWriteResult>> {
        let mut continuation = None;
        let mut matches = Vec::new();
        loop {
            let page = self
                .plane
                .list(ListRequest {
                    prefix: path.as_str().to_string(),
                    continuation,
                    limit: 1_000,
                    include_versions: true,
                })
                .await?;
            for entry in page.entries {
                if entry.path != *path || entry.metadata.delete_marker {
                    continue;
                }
                let Some(version_id) = entry.metadata.token.version_id else {
                    continue;
                };
                let Some(object) = self
                    .plane
                    .get(GetRequest {
                        path: path.clone(),
                        range: None,
                        physical_version: Some(PhysicalVersion::Versioned {
                            version_id: version_id.clone(),
                        }),
                    })
                    .await?
                else {
                    continue;
                };
                let metadata = &object.metadata.user_metadata;
                if metadata.get("prolly-repository-id")
                    != Some(&self.format.repository_id.to_string())
                    || metadata.get("prolly-operation-id") != Some(&operation.to_string())
                    || metadata
                        .get("prolly-sha256")
                        .is_some_and(|value| value != &hex::encode(expected_sha256))
                    || crate::codec::sha256(&object.bytes) != expected_sha256
                {
                    continue;
                }
                let md5: [u8; 16] = Md5::digest(&object.bytes).into();
                matches.push(crate::PhysicalObjectWriteResult {
                    binding: crate::PhysicalObjectBindingV1::Live {
                        version_id,
                        provider_etag: object.metadata.token.etag,
                        checksum_sha256: expected_sha256,
                    },
                    size: object.bytes.len() as u64,
                    logical_etag: format!("\"{}\"", hex::encode(md5)),
                    checksums: crate::Checksums {
                        md5: Some(md5),
                        sha256: Some(expected_sha256),
                        algorithm_values: BTreeMap::new(),
                    },
                });
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.pop()),
            _ => Err(Error::new(
                ErrorCode::OutcomeUnknown,
                "multiple physical versions match one operation; manual repair is required",
            )
            .retry(RetryAdvice::ReconcileOperation)
            .operation(operation.to_string())),
        }
    }

    async fn reconcile_physical_delete(
        &self,
        path: &ObjectPath,
    ) -> Result<Option<crate::PhysicalObjectBindingV1>> {
        let mut continuation = None;
        let mut matches = Vec::new();
        loop {
            let page = self
                .plane
                .list(ListRequest {
                    prefix: path.as_str().to_string(),
                    continuation,
                    limit: 1_000,
                    include_versions: true,
                })
                .await?;
            for entry in page.entries {
                if entry.path != *path || !entry.metadata.delete_marker || !entry.is_latest {
                    continue;
                }
                if let Some(version_id) = entry.metadata.token.version_id {
                    matches.push(version_id);
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        match matches.as_slice() {
            [] => Ok(None),
            [version_id] => Ok(Some(crate::PhysicalObjectBindingV1::DeleteMarker {
                version_id: version_id.clone(),
            })),
            _ => Err(Error::new(
                ErrorCode::OutcomeUnknown,
                "physical delete reconciliation found multiple current delete markers",
            )
            .retry(RetryAdvice::ReconcileOperation)),
        }
    }

    fn gc_epoch_index(&self, id: OperationId) -> Result<ProllyMetadataIndex<P>> {
        let path = gc_epoch_v2_tree_path(id);
        ProllyMetadataIndex::new(
            self.plane.clone(),
            &self.options.repository_prefix,
            self.format.repository_id,
            self.format.state_tree_format.clone(),
            self.node_cache.clone(),
            MetadataIndexSpec {
                path: &path,
                protocol_version: 5,
                name: "gc-epoch",
            },
        )
    }

    async fn load_gc_epoch_v2(&self, id: OperationId) -> Result<LoadedGcEpoch> {
        let stored = self
            .plane
            .load_mutable(&gc_epoch_v2_path(&self.options.repository_prefix, id)?)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::InvalidRevision, "GC epoch does not exist"))?;
        let value: GcEpochV2 = decode_canonical(&stored.bytes)?;
        if value.id != id || value.repository != self.format.repository_id {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "GC epoch belongs to another repository",
            ));
        }
        if value.root.format_digest != tree_format_digest(&self.format.state_tree_format)? {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "GC epoch tree format is invalid",
            ));
        }
        Ok(LoadedGcEpoch {
            value,
            token: stored.metadata.token,
        })
    }

    async fn restore_gc_coordinator_v2(&self) -> Result<()> {
        let active = match self
            .plane
            .load_mutable(&gc_coordinator_v2_path(&self.options.repository_prefix)?)
            .await?
        {
            Some(stored) => {
                let coordinator: GcCoordinatorV2 = decode_canonical(&stored.bytes)?;
                coordinator.validate(self.format.repository_id)?;
                coordinator.active_epoch
            }
            None => None,
        };
        *self
            .active_gc_epoch
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "active GC lock poisoned"))? =
            active;
        if let Some(epoch) = active {
            let mut continuation = None;
            let mut maximum_sequence = 0_u64;
            loop {
                let page = self
                    .plane
                    .list(ListRequest {
                        prefix: gc_dirty_root_v2_prefix(&self.options.repository_prefix, epoch),
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
                        .nth(1)
                        .and_then(|component| component.parse::<u64>().ok())
                        .ok_or_else(|| {
                            Error::new(
                                ErrorCode::CorruptCommit,
                                "GC dirty-root journal path has an invalid sequence",
                            )
                        })?;
                    maximum_sequence = maximum_sequence.max(sequence);
                }
                continuation = page.continuation;
                if continuation.is_none() {
                    break;
                }
            }
            self.gc_dirty_sequence
                .fetch_max(maximum_sequence, Ordering::AcqRel);
        }
        Ok(())
    }

    async fn activate_gc_coordinator_v2(&self, epoch: OperationId, now_millis: u64) -> Result<()> {
        let path = gc_coordinator_v2_path(&self.options.repository_prefix)?;
        let loaded = self.plane.load_mutable(&path).await?;
        let (generation, expected) = match loaded {
            Some(stored) => {
                let current: GcCoordinatorV2 = decode_canonical(&stored.bytes)?;
                current.validate(self.format.repository_id)?;
                if let Some(active) = current.active_epoch {
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        format!("GC epoch {active} is already active"),
                    ));
                }
                (
                    current.generation.checked_add(1).ok_or_else(|| {
                        Error::new(ErrorCode::InternalInvariant, "GC coordinator overflow")
                    })?,
                    Some(stored.metadata.token),
                )
            }
            None => (1, None),
        };
        let coordinator = GcCoordinatorV2 {
            repository: self.format.repository_id,
            generation,
            active_epoch: Some(epoch),
            updated_at_millis: now_millis,
        };
        match self
            .controls
            .compare_exchange(CompareExchange {
                path,
                expected,
                bytes: encode_canonical(&coordinator)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => {
                *self.active_gc_epoch.write().map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "active GC lock poisoned")
                })? = Some(epoch);
                Ok(())
            }
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::RefConflict,
                "GC coordinator changed concurrently",
            )
            .retry(RetryAdvice::ReloadHead)),
        }
    }

    async fn clear_gc_coordinator_v2(&self, epoch: OperationId) -> Result<()> {
        let path = gc_coordinator_v2_path(&self.options.repository_prefix)?;
        let Some(stored) = self.plane.load_mutable(&path).await? else {
            return Err(Error::new(
                ErrorCode::MissingClosure,
                "active GC coordinator is missing",
            ));
        };
        let current: GcCoordinatorV2 = decode_canonical(&stored.bytes)?;
        current.validate(self.format.repository_id)?;
        if current.active_epoch.is_none() {
            *self.active_gc_epoch.write().map_err(|_| {
                Error::new(ErrorCode::InternalInvariant, "active GC lock poisoned")
            })? = None;
            return Ok(());
        }
        if current.active_epoch != Some(epoch) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "another GC epoch owns the coordinator",
            ));
        }
        let next = GcCoordinatorV2 {
            repository: current.repository,
            generation: current.generation.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "GC coordinator overflow")
            })?,
            active_epoch: None,
            updated_at_millis: self.now_millis()?,
        };
        match self
            .controls
            .compare_exchange(CompareExchange {
                path,
                expected: Some(stored.metadata.token),
                bytes: encode_canonical(&next)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => {
                *self.active_gc_epoch.write().map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "active GC lock poisoned")
                })? = None;
                Ok(())
            }
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::RefConflict,
                "GC coordinator changed while completing an epoch",
            )
            .retry(RetryAdvice::ReloadHead)),
        }
    }

    /// Starts a partitioned GC epoch. Every later call processes a bounded
    /// amount of root discovery, graph marking, version marking, candidate
    /// enumeration, or exact-version deletion.
    pub async fn start_gc_epoch_v2(&self, grace_millis: u64) -> Result<GcEpochV2> {
        self.validate_gc_plan_limits(grace_millis, 1)?;
        let writer_fence_generation = self.system_writer_generation("gc").await?;
        let _publication = self.lock_global_publication().await;
        if let Some(active) = *self
            .active_gc_epoch
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "active GC lock poisoned"))?
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                format!("GC epoch {active} is already active"),
            ));
        }
        let planned_at_millis = self.now_millis()?;
        let cutoff_millis = planned_at_millis
            .checked_sub(grace_millis)
            .ok_or_else(|| Error::new(ErrorCode::InvalidLimit, "GC grace predates the epoch"))?;
        let id = self.new_operation();
        let index = self.gc_epoch_index(id)?;
        let epoch = GcEpochV2 {
            id,
            repository: self.format.repository_id,
            process_session: self.process_session,
            writer_fence_generation,
            publication_acquisition: self
                .performance
                .publication_acquisitions
                .load(Ordering::Relaxed),
            planned_at_millis,
            cutoff_millis,
            root: TreeRootV1::from_tree(&index.engine.create())?,
            phase: GcEpochPhaseV2::DiscoverRoots,
            root_namespace: 0,
            source_continuation: None,
            sweep_after: None,
            generation: 0,
            marked_commits: 0,
            marked_nodes: 0,
            marked_versions: 0,
            candidates: 0,
            candidate_bytes: 0,
            deleted_versions: 0,
            deleted_bytes: 0,
            skipped_reachable: 0,
            already_missing: 0,
            dirty_roots_marked: 0,
            dirty_catch_up_active: false,
            dirty_root_sequence: self.gc_dirty_sequence.load(Ordering::Acquire),
            dirty_root_target_sequence: 0,
            updated_at_millis: planned_at_millis,
            abort_reason: None,
        };
        match self
            .controls
            .compare_exchange(CompareExchange {
                path: gc_epoch_v2_path(&self.options.repository_prefix, id)?,
                expected: None,
                bytes: encode_canonical(&epoch)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => {
                self.activate_gc_coordinator_v2(id, planned_at_millis)
                    .await?;
                Ok(epoch)
            }
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::RefConflict,
                "generated GC epoch ID already exists",
            )),
        }
    }

    pub async fn gc_epoch_v2(&self, id: OperationId) -> Result<GcEpochV2> {
        Ok(self.load_gc_epoch_v2(id).await?.value)
    }

    pub async fn advance_gc_epoch_v2(
        &self,
        id: OperationId,
        max_items: usize,
    ) -> Result<GcEpochStepReport> {
        if !(1..=1_000).contains(&max_items) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "GC epoch step must process between 1 and 1,000 items",
            ));
        }
        let writer_fence_generation = self.system_writer_generation("gc").await?;
        let loaded = self.load_gc_epoch_v2(id).await?;
        if matches!(
            loaded.value.phase,
            GcEpochPhaseV2::Completed | GcEpochPhaseV2::Aborted
        ) {
            return Ok(GcEpochStepReport {
                epoch: loaded.value,
                processed: 0,
                restarted_for_new_roots: false,
            });
        }
        if loaded.value.writer_fence_generation != writer_fence_generation {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "GC epoch writer fence no longer matches",
            ));
        }
        let mut next = loaded.value;
        let restarted_for_new_roots = next.process_session != self.process_session
            && matches!(
                next.phase,
                GcEpochPhaseV2::CatchUpDirtyRoots
                    | GcEpochPhaseV2::Ready
                    | GcEpochPhaseV2::Sweeping
            );
        if restarted_for_new_roots {
            next.process_session = self.process_session;
            next.phase = GcEpochPhaseV2::CatchUpDirtyRoots;
            next.source_continuation = None;
            next.dirty_root_target_sequence = 0;
        }
        let index = self.gc_epoch_index(id)?;
        index.install_root(next.root.root.clone())?;
        let mut tree = index.tree()?;
        let processed = match next.phase {
            GcEpochPhaseV2::DiscoverRoots => {
                self.gc_v2_discover_roots(&index, &mut tree, &mut next, max_items)
                    .await?
            }
            GcEpochPhaseV2::MarkCommits => {
                self.gc_v2_mark_commits(&index, &mut tree, &mut next, max_items)
                    .await?
            }
            GcEpochPhaseV2::MarkNodes => {
                self.gc_v2_mark_nodes(&index, &mut tree, &mut next, max_items)
                    .await?
            }
            GcEpochPhaseV2::MarkVersions => {
                self.gc_v2_mark_versions(&index, &mut tree, &mut next, max_items)
                    .await?
            }
            GcEpochPhaseV2::ScanCandidates => {
                self.gc_v2_scan_candidates(&index, &mut tree, &mut next, max_items)
                    .await?
            }
            GcEpochPhaseV2::CatchUpDirtyRoots => {
                self.gc_v2_catch_up_dirty_roots(&index, &mut tree, &mut next, max_items)
                    .await?
            }
            GcEpochPhaseV2::CleanupDirtyRoots => {
                self.gc_v2_cleanup_dirty_roots(&mut next, max_items).await?
            }
            GcEpochPhaseV2::Ready | GcEpochPhaseV2::Sweeping => 0,
            GcEpochPhaseV2::Completed | GcEpochPhaseV2::Aborted => unreachable!(),
        };
        next.root = TreeRootV1::from_tree(&tree)?;
        next.generation = next.generation.checked_add(1).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "GC epoch generation overflow")
        })?;
        next.updated_at_millis = self.now_millis()?;
        match self
            .controls
            .compare_exchange(CompareExchange {
                path: gc_epoch_v2_path(&self.options.repository_prefix, id)?,
                expected: Some(loaded.token),
                bytes: encode_canonical(&next)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => Ok(GcEpochStepReport {
                epoch: next,
                processed,
                restarted_for_new_roots,
            }),
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::RefConflict,
                "GC epoch changed concurrently",
            )
            .retry(RetryAdvice::ReloadHead)),
        }
    }

    async fn gc_v2_discover_roots(
        &self,
        index: &ProllyMetadataIndex<P>,
        tree: &mut Tree,
        epoch: &mut GcEpochV2,
        max_items: usize,
    ) -> Result<usize> {
        let suffix = match epoch.root_namespace {
            0 => "refs/heads/",
            1 => "refs/tags/",
            2 => "retention/pins/",
            3 => "reflogs/tags/",
            _ => {
                epoch.phase = GcEpochPhaseV2::MarkCommits;
                epoch.source_continuation = None;
                return Ok(0);
            }
        };
        let page = self
            .plane
            .list(ListRequest {
                prefix: format!("{}/{suffix}", self.options.repository_prefix),
                continuation: epoch.source_continuation.clone(),
                limit: max_items,
                include_versions: false,
            })
            .await?;
        let mut roots = Vec::new();
        for listed in &page.entries {
            let Some(stored) = self
                .plane
                .get(GetRequest {
                    path: listed.path.clone(),
                    range: None,
                    physical_version: None,
                })
                .await?
            else {
                continue;
            };
            match epoch.root_namespace {
                0 => {
                    let value: crate::RefValueV1 = decode_canonical(&stored.bytes)?;
                    if !value.tombstone {
                        roots.push(value.target);
                        roots.extend(value.previous_target);
                    }
                }
                1 => {
                    let value: crate::TagValueV1 = decode_canonical(&stored.bytes)?;
                    if !value.tombstone {
                        roots.push(value.target);
                        roots.extend(value.previous_target);
                    }
                }
                2 => {
                    let value: RetentionPinV1 = decode_canonical(&stored.bytes)?;
                    if !value.tombstone
                        && (value.expires_at_millis == 0
                            || value.expires_at_millis > epoch.planned_at_millis)
                    {
                        roots.push(value.target);
                    }
                }
                3 => {
                    let value: ReflogEntryV1 = decode_canonical(&stored.bytes)?;
                    let retain_until = value
                        .created_at_millis
                        .saturating_add(self.options.reflog_retention_millis);
                    if self.options.reflog_retention_millis == 0
                        || retain_until > epoch.planned_at_millis
                    {
                        roots.push(value.new_target);
                        roots.extend(value.old_target);
                    }
                }
                _ => unreachable!(),
            }
        }
        roots.sort_unstable();
        roots.dedup();
        if !roots.is_empty() {
            let mut mutations = Vec::with_capacity(roots.len());
            for commit in roots {
                mutations.push(Mutation::Upsert {
                    key: gc_v2_commit_queue_key(commit),
                    val: encode_canonical(&GcCommitWorkV2 {
                        commit,
                        scan_versions: true,
                    })?,
                });
            }
            *tree = index.engine.batch(tree, mutations).await?;
        }
        if page.continuation.is_none() {
            epoch.root_namespace += 1;
            epoch.source_continuation = None;
            if epoch.root_namespace > 3 {
                epoch.phase = GcEpochPhaseV2::MarkCommits;
            }
        } else {
            epoch.source_continuation = page.continuation;
        }
        Ok(page.entries.len())
    }

    async fn gc_v2_mark_commits(
        &self,
        index: &ProllyMetadataIndex<P>,
        tree: &mut Tree,
        epoch: &mut GcEpochV2,
        max_items: usize,
    ) -> Result<usize> {
        let mut iter = index.engine.prefix(tree, b"qc/").await?;
        let mut work = Vec::with_capacity(max_items);
        while work.len() < max_items {
            let Some(entry) = iter.next().await else {
                break;
            };
            work.push(entry?);
        }
        drop(iter);
        if work.is_empty() {
            epoch.phase = GcEpochPhaseV2::MarkNodes;
            return Ok(0);
        }
        for (queue_key, encoded) in &work {
            let item: GcCommitWorkV2 = decode_canonical(encoded)?;
            let already_marked = index
                .engine
                .get(tree, &gc_v2_commit_mark_key(item.commit))
                .await?
                .is_some();
            let mut mutations = vec![Mutation::Delete {
                key: queue_key.clone(),
            }];
            if !already_marked || item.scan_versions {
                let commit = self.load_commit(item.commit).await?;
                mutations.push(Mutation::Upsert {
                    key: gc_v2_commit_mark_key(item.commit),
                    val: Vec::new(),
                });
                mutations.push(Mutation::Upsert {
                    key: gc_v2_path_mark_key(&commit_path(
                        &self.options.repository_prefix,
                        item.commit,
                    )?),
                    val: Vec::new(),
                });
                if item.scan_versions {
                    if let Some(key) = gc_v2_version_queue_key(&commit.state.versions) {
                        mutations.push(Mutation::Upsert {
                            key,
                            val: encode_canonical(&GcVersionWorkV2 {
                                root: commit.state.versions.clone(),
                                after: None,
                            })?,
                        });
                    }
                    for root in [
                        &commit.state.objects,
                        &commit.state.versions,
                        &commit.state.operations,
                    ] {
                        if let Some(cid) = root.root.as_ref() {
                            mutations.push(Mutation::Upsert {
                                key: gc_v2_node_queue_key(cid),
                                val: cid.as_bytes().to_vec(),
                            });
                        }
                    }
                }
                for parent in commit.parents {
                    if index
                        .engine
                        .get(tree, &gc_v2_commit_mark_key(parent))
                        .await?
                        .is_none()
                    {
                        let queue_key = gc_v2_commit_queue_key(parent);
                        let queued_direct_root = index
                            .engine
                            .get(tree, &queue_key)
                            .await?
                            .map(|bytes| decode_canonical::<GcCommitWorkV2>(&bytes))
                            .transpose()?
                            .is_some_and(|work| work.scan_versions);
                        if !queued_direct_root {
                            mutations.push(Mutation::Upsert {
                                key: queue_key,
                                val: encode_canonical(&GcCommitWorkV2 {
                                    commit: parent,
                                    scan_versions: false,
                                })?,
                            });
                        }
                    }
                }
                if !already_marked {
                    epoch.marked_commits = epoch.marked_commits.saturating_add(1);
                }
            }
            *tree = index.engine.batch(tree, mutations).await?;
        }
        Ok(work.len())
    }

    async fn gc_v2_mark_versions(
        &self,
        index: &ProllyMetadataIndex<P>,
        tree: &mut Tree,
        epoch: &mut GcEpochV2,
        max_items: usize,
    ) -> Result<usize> {
        let mut queue = index.engine.prefix(tree, b"qv/").await?;
        let Some(entry) = queue.next().await else {
            epoch.phase = if epoch.dirty_catch_up_active {
                epoch.dirty_catch_up_active = false;
                epoch.source_continuation = None;
                GcEpochPhaseV2::CatchUpDirtyRoots
            } else {
                GcEpochPhaseV2::ScanCandidates
            };
            return Ok(0);
        };
        let (queue_key, encoded) = entry?;
        drop(queue);
        let mut work: GcVersionWorkV2 = decode_canonical(&encoded)?;
        let versions = self.tree_from_root(&work.root, &self.format.state_tree_format)?;
        let start = work.after.as_deref().unwrap_or(&[]);
        let mut iter = self.engine.range(&versions, start, None).await?;
        let mut mutations = Vec::with_capacity(max_items + 1);
        let mut processed = 0usize;
        let mut last = None;
        while processed < max_items {
            let Some(entry) = iter.next().await else {
                break;
            };
            let (encoded_key, value) = entry?;
            if work
                .after
                .as_ref()
                .is_some_and(|after| encoded_key.as_slice() <= after.as_slice())
            {
                continue;
            }
            let version: ObjectVersionV1 = decode_canonical(&value)?;
            let key = decode_version_tree_logical_key(&encoded_key)?;
            let path =
                ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                    Error::new(ErrorCode::CorruptCommit, "logical key is not UTF-8")
                })?)?;
            let version_id = match version.binding {
                crate::PhysicalObjectBindingV1::Live { version_id, .. }
                | crate::PhysicalObjectBindingV1::DeleteMarker { version_id } => version_id,
            };
            mutations.push(Mutation::Upsert {
                key: gc_v2_physical_mark_key(&path, &version_id),
                val: Vec::new(),
            });
            last = Some(encoded_key);
            processed += 1;
        }
        if processed < max_items {
            mutations.push(Mutation::Delete { key: queue_key });
        } else {
            work.after = last;
            mutations.push(Mutation::Upsert {
                key: queue_key,
                val: encode_canonical(&work)?,
            });
        }
        if !mutations.is_empty() {
            *tree = index.engine.batch(tree, mutations).await?;
        }
        epoch.marked_versions = epoch
            .marked_versions
            .saturating_add(u64::try_from(processed).unwrap_or(u64::MAX));
        Ok(processed)
    }

    async fn gc_v2_mark_nodes(
        &self,
        index: &ProllyMetadataIndex<P>,
        tree: &mut Tree,
        epoch: &mut GcEpochV2,
        max_items: usize,
    ) -> Result<usize> {
        let mut iter = index.engine.prefix(tree, b"qn/").await?;
        let mut work = Vec::with_capacity(max_items);
        while work.len() < max_items {
            let Some(entry) = iter.next().await else {
                break;
            };
            work.push(entry?);
        }
        drop(iter);
        if work.is_empty() {
            epoch.phase = GcEpochPhaseV2::MarkVersions;
            return Ok(0);
        }
        for (queue_key, encoded_cid) in &work {
            let cid = prolly::Cid(encoded_cid.as_slice().try_into().map_err(|_| {
                Error::new(ErrorCode::CorruptNode, "GC node work has an invalid CID")
            })?);
            let mark_key = gc_v2_node_mark_key(&cid);
            if index.engine.get(tree, &mark_key).await?.is_some() {
                *tree = index
                    .engine
                    .batch(
                        tree,
                        vec![Mutation::Delete {
                            key: queue_key.clone(),
                        }],
                    )
                    .await?;
                continue;
            }
            let location = self
                .node_store
                .resolve_node_location(&cid)
                .await?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::MissingCapability,
                        "GC requires the v2 node index to cover every reachable node; advance the index and retry",
                    )
                })?;
            let bytes = self.node_store.get(cid.as_bytes()).await?.ok_or_else(|| {
                Error::new(ErrorCode::MissingClosure, "reachable node is missing")
            })?;
            let node = Node::from_bytes_with_format(&bytes, &self.format.state_tree_format)
                .map_err(|error| {
                    Error::new(
                        ErrorCode::CorruptNode,
                        format!("reachable node could not be decoded: {error}"),
                    )
                })?;
            let mut mutations = vec![
                Mutation::Delete {
                    key: queue_key.clone(),
                },
                Mutation::Upsert {
                    key: mark_key,
                    val: Vec::new(),
                },
                Mutation::Upsert {
                    key: gc_v2_path_mark_key(&commit_path(
                        &self.options.repository_prefix,
                        location.container,
                    )?),
                    val: Vec::new(),
                },
            ];
            if !node.leaf {
                for value in node.vals {
                    let child = prolly::Cid(value.as_slice().try_into().map_err(|_| {
                        Error::new(
                            ErrorCode::CorruptNode,
                            "internal reachable node contains an invalid child CID",
                        )
                    })?);
                    if index
                        .engine
                        .get(tree, &gc_v2_node_mark_key(&child))
                        .await?
                        .is_none()
                    {
                        mutations.push(Mutation::Upsert {
                            key: gc_v2_node_queue_key(&child),
                            val: child.as_bytes().to_vec(),
                        });
                    }
                }
            }
            *tree = index.engine.batch(tree, mutations).await?;
            epoch.marked_nodes = epoch.marked_nodes.saturating_add(1);
        }
        Ok(work.len())
    }

    async fn gc_v2_scan_candidates(
        &self,
        index: &ProllyMetadataIndex<P>,
        tree: &mut Tree,
        epoch: &mut GcEpochV2,
        max_items: usize,
    ) -> Result<usize> {
        let page = self
            .plane
            .list(ListRequest {
                prefix: String::new(),
                continuation: epoch.source_continuation.clone(),
                limit: max_items,
                include_versions: true,
            })
            .await?;
        for listed in &page.entries {
            let managed_data = is_gc_data_path(&self.options.repository_prefix, &listed.path)
                || !listed
                    .path
                    .as_str()
                    .starts_with(&format!("{}/", self.options.repository_prefix));
            if !managed_data || listed.metadata.last_modified_millis > epoch.cutoff_millis {
                continue;
            }
            let retained = if let Some(version_id) = listed.metadata.token.version_id.as_deref() {
                index
                    .engine
                    .get(tree, &gc_v2_physical_mark_key(&listed.path, version_id))
                    .await?
                    .is_some()
                    || index
                        .engine
                        .get(tree, &gc_v2_path_mark_key(&listed.path))
                        .await?
                        .is_some()
            } else {
                index
                    .engine
                    .get(tree, &gc_v2_path_mark_key(&listed.path))
                    .await?
                    .is_some()
            };
            if retained {
                continue;
            }
            let physical_version = listed
                .metadata
                .token
                .version_id
                .clone()
                .map(|version_id| PhysicalVersion::Versioned { version_id })
                .unwrap_or_else(|| PhysicalVersion::Unversioned {
                    token: Some(listed.metadata.token.clone()),
                });
            let candidate = GcCandidateV1 {
                path: listed.path.clone(),
                physical_version,
                len: listed.metadata.len,
                last_modified_millis: listed.metadata.last_modified_millis,
            };
            let candidate_key = gc_v2_candidate_key(&candidate)?;
            if index.engine.get(tree, &candidate_key).await?.is_none() {
                *tree = index
                    .engine
                    .batch(
                        tree,
                        vec![Mutation::Upsert {
                            key: candidate_key,
                            val: encode_canonical(&candidate)?,
                        }],
                    )
                    .await?;
                epoch.candidates = epoch.candidates.saturating_add(1);
                epoch.candidate_bytes = epoch.candidate_bytes.saturating_add(candidate.len);
            }
        }
        epoch.source_continuation = page.continuation;
        if epoch.source_continuation.is_none() {
            epoch.phase = GcEpochPhaseV2::CatchUpDirtyRoots;
            epoch.source_continuation = None;
        }
        Ok(page.entries.len())
    }

    async fn gc_v2_catch_up_dirty_roots(
        &self,
        index: &ProllyMetadataIndex<P>,
        tree: &mut Tree,
        epoch: &mut GcEpochV2,
        max_items: usize,
    ) -> Result<usize> {
        if epoch.dirty_root_target_sequence == 0 {
            let stable_sequence = {
                let barrier = self.lock_global_publication().await;
                let sequence = self.gc_dirty_sequence.load(Ordering::Acquire);
                drop(barrier);
                sequence
            };
            epoch.dirty_root_target_sequence = stable_sequence;
            epoch.publication_acquisition = self
                .performance
                .publication_acquisitions
                .load(Ordering::Acquire);
        }
        let next_sequence = epoch.dirty_root_sequence.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "GC dirty-root sequence overflow",
            )
        })?;
        if next_sequence > epoch.dirty_root_target_sequence {
            epoch.phase = GcEpochPhaseV2::Ready;
            epoch.source_continuation = None;
            return Ok(0);
        }
        let page = self
            .plane
            .list(ListRequest {
                prefix: gc_dirty_root_v2_sequence_prefix(
                    &self.options.repository_prefix,
                    epoch.id,
                    next_sequence,
                ),
                continuation: epoch.source_continuation.clone(),
                limit: max_items,
                include_versions: false,
            })
            .await?;
        let mut mutations = Vec::new();
        let mut newly_marked = 0_u64;
        for listed in &page.entries {
            let stored = self
                .plane
                .get(GetRequest {
                    path: listed.path.clone(),
                    range: None,
                    physical_version: None,
                })
                .await?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::MissingClosure,
                        "GC dirty-root journal event disappeared",
                    )
                })?;
            let event: GcDirtyRootV2 = decode_canonical(&stored.bytes)?;
            event.validate()?;
            let event_id = event.id()?;
            if event.repository != self.format.repository_id
                || event.epoch != epoch.id
                || gc_dirty_root_v2_path(&self.options.repository_prefix, &event, event_id)?
                    != listed.path
            {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "GC dirty-root event does not match its journal path",
                ));
            }
            let mark_key = gc_v2_dirty_root_mark_key(event_id);
            if index.engine.get(tree, &mark_key).await?.is_none() {
                mutations.push(Mutation::Upsert {
                    key: mark_key,
                    val: Vec::new(),
                });
                let mut roots = vec![event.target];
                roots.extend(event.previous_target);
                roots.sort_unstable();
                roots.dedup();
                for commit in roots {
                    mutations.push(Mutation::Upsert {
                        key: gc_v2_commit_queue_key(commit),
                        val: encode_canonical(&GcCommitWorkV2 {
                            commit,
                            scan_versions: true,
                        })?,
                    });
                    newly_marked = newly_marked.saturating_add(1);
                }
            }
        }
        if !mutations.is_empty() {
            *tree = index.engine.batch(tree, mutations).await?;
        }
        epoch.dirty_roots_marked = epoch.dirty_roots_marked.saturating_add(newly_marked);
        epoch.source_continuation = page.continuation;
        if epoch.source_continuation.is_none() {
            epoch.dirty_root_sequence = next_sequence;
            if newly_marked > 0 {
                epoch.dirty_catch_up_active = true;
                epoch.phase = GcEpochPhaseV2::MarkCommits;
            }
        }
        Ok(page.entries.len())
    }

    async fn gc_v2_cleanup_dirty_roots(
        &self,
        epoch: &mut GcEpochV2,
        max_items: usize,
    ) -> Result<usize> {
        self.clear_gc_coordinator_v2(epoch.id).await?;
        let page = self
            .plane
            .list(ListRequest {
                prefix: gc_dirty_root_v2_prefix(&self.options.repository_prefix, epoch.id),
                continuation: None,
                limit: max_items,
                include_versions: false,
            })
            .await?;
        if page.entries.is_empty() {
            epoch.phase = GcEpochPhaseV2::Completed;
            return Ok(0);
        }
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
            if matches!(outcome, DeleteOutcome::TokenMismatch) {
                return Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "GC dirty-root event changed during exact cleanup",
                ));
            }
        }
        Ok(processed)
    }

    /// Deletes at most `max_candidates` exact physical versions from a ready
    /// epoch. Intervening publications schedule ordered dirty-root catch-up;
    /// they never restart the stable root-namespace scan.
    pub async fn sweep_gc_epoch_v2(
        &self,
        id: OperationId,
        max_candidates: usize,
    ) -> Result<GcEpochStepReport> {
        if !(1..=1_000).contains(&max_candidates) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "GC sweep batch must contain between 1 and 1,000 candidates",
            ));
        }
        let writer_fence_generation = self.system_writer_generation("gc").await?;
        let _publication = self.lock_global_publication().await;
        let acquisition = self
            .performance
            .publication_acquisitions
            .load(Ordering::Relaxed);
        let loaded = self.load_gc_epoch_v2(id).await?;
        if loaded.value.writer_fence_generation != writer_fence_generation {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "GC epoch writer fence no longer matches",
            ));
        }
        if matches!(loaded.value.phase, GcEpochPhaseV2::Completed) {
            self.clear_gc_coordinator_v2(id).await?;
            return Ok(GcEpochStepReport {
                epoch: loaded.value,
                processed: 0,
                restarted_for_new_roots: false,
            });
        }
        if matches!(loaded.value.phase, GcEpochPhaseV2::Aborted) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "GC epoch is aborted",
            ));
        }
        let mut next = loaded.value;
        let dirty_target = self.gc_dirty_sequence.load(Ordering::Acquire);
        let restarted_for_new_roots =
            next.process_session != self.process_session || dirty_target > next.dirty_root_sequence;
        if restarted_for_new_roots {
            next.process_session = self.process_session;
            next.phase = GcEpochPhaseV2::CatchUpDirtyRoots;
            next.source_continuation = None;
            next.dirty_root_target_sequence = dirty_target;
            next.publication_acquisition = acquisition;
            next.generation = next.generation.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "GC epoch generation overflow")
            })?;
            next.updated_at_millis = self.now_millis()?;
            match self
                .controls
                .compare_exchange(CompareExchange {
                    path: gc_epoch_v2_path(&self.options.repository_prefix, id)?,
                    expected: Some(loaded.token),
                    bytes: encode_canonical(&next)?,
                })
                .await?
            {
                CompareExchangeOutcome::Applied(_) => {
                    return Ok(GcEpochStepReport {
                        epoch: next,
                        processed: 0,
                        restarted_for_new_roots: true,
                    });
                }
                CompareExchangeOutcome::Conflict(_) => {
                    return Err(Error::new(
                        ErrorCode::RefConflict,
                        "GC epoch changed while scheduling dirty-root catch-up",
                    )
                    .retry(RetryAdvice::ReloadHead));
                }
            }
        }
        if !matches!(next.phase, GcEpochPhaseV2::Ready | GcEpochPhaseV2::Sweeping) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "GC epoch must finish marking and candidate discovery before sweeping",
            ));
        }
        let index = self.gc_epoch_index(id)?;
        index.install_root(next.root.root.clone())?;
        let tree = index.tree()?;
        let start = next.sweep_after.as_deref().unwrap_or(b"d/");
        let mut iter = index.engine.range(&tree, start, None).await?;
        let mut processed = 0usize;
        let mut exhausted = true;
        while processed < max_candidates {
            let Some(entry) = iter.next().await else {
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
            let candidate: GcCandidateV1 = decode_canonical(&encoded)?;
            if candidate.last_modified_millis > next.cutoff_millis {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "GC v2 candidate is newer than its cutoff",
                ));
            }
            let version_id = match &candidate.physical_version {
                PhysicalVersion::Versioned { version_id } => Some(version_id.as_str()),
                PhysicalVersion::Unversioned { .. } => None,
            };
            let retained = index
                .engine
                .get(&tree, &gc_v2_path_mark_key(&candidate.path))
                .await?
                .is_some()
                || match version_id {
                    Some(version_id) => index
                        .engine
                        .get(&tree, &gc_v2_physical_mark_key(&candidate.path, version_id))
                        .await?
                        .is_some(),
                    None => false,
                };
            if retained {
                next.skipped_reachable = next.skipped_reachable.saturating_add(1);
            } else {
                if self.options.gc_delete_rate_limit_per_second > 0 && processed > 0 {
                    let millis =
                        1_000_u64.div_ceil(u64::from(self.options.gc_delete_rate_limit_per_second));
                    tokio::time::sleep(Duration::from_millis(millis)).await;
                }
                match self
                    .plane
                    .delete_exact(&candidate.path, candidate.physical_version)
                    .await?
                {
                    DeleteOutcome::Deleted => {
                        next.deleted_versions = next.deleted_versions.saturating_add(1);
                        next.deleted_bytes = next.deleted_bytes.saturating_add(candidate.len);
                    }
                    DeleteOutcome::NotFound => {
                        next.already_missing = next.already_missing.saturating_add(1);
                    }
                    DeleteOutcome::TokenMismatch => {
                        return Err(Error::new(
                            ErrorCode::PreconditionFailed,
                            "GC v2 candidate physical version no longer matches",
                        ));
                    }
                }
            }
            next.sweep_after = Some(key);
            processed += 1;
        }
        // Probe one entry on the next call when the batch fills exactly. This
        // keeps memory bounded without retaining a lookahead candidate.
        if processed < max_candidates && (processed == 0 || exhausted) {
            next.phase = GcEpochPhaseV2::CleanupDirtyRoots;
        } else {
            next.phase = GcEpochPhaseV2::Sweeping;
        }
        next.publication_acquisition = acquisition;
        next.generation = next.generation.checked_add(1).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "GC epoch generation overflow")
        })?;
        next.updated_at_millis = self.now_millis()?;
        match self
            .controls
            .compare_exchange(CompareExchange {
                path: gc_epoch_v2_path(&self.options.repository_prefix, id)?,
                expected: Some(loaded.token),
                bytes: encode_canonical(&next)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => {
                if matches!(next.phase, GcEpochPhaseV2::CleanupDirtyRoots) {
                    self.clear_gc_coordinator_v2(id).await?;
                }
                Ok(GcEpochStepReport {
                    epoch: next,
                    processed,
                    restarted_for_new_roots: false,
                })
            }
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::RefConflict,
                "GC epoch changed while publishing a sweep checkpoint",
            )
            .retry(RetryAdvice::ReloadHead)),
        }
    }

    /// Persist a deterministic, immutable GC dry-run. Only objects older than
    /// `grace_millis` and outside the complete retained set become candidates.
    pub async fn plan_gc(&self, grace_millis: u64, max_candidates: usize) -> Result<GcDryRun> {
        self.validate_gc_plan_limits(grace_millis, max_candidates)?;
        self.plan_gc_at(self.now_millis()?, grace_millis, max_candidates)
            .await
    }

    fn validate_gc_plan_limits(&self, grace_millis: u64, max_candidates: usize) -> Result<()> {
        let minimum_grace = self
            .options
            .writer_lease_millis
            .checked_mul(2)
            .ok_or_else(|| Error::new(ErrorCode::InvalidLimit, "GC grace overflow"))?;
        if grace_millis < minimum_grace || max_candidates == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "GC requires a nonzero candidate limit and grace at least twice the writer lease",
            ));
        }
        Ok(())
    }

    async fn plan_gc_at(
        &self,
        planned_at_millis: u64,
        grace_millis: u64,
        max_candidates: usize,
    ) -> Result<GcDryRun> {
        let cutoff_millis = planned_at_millis
            .checked_sub(grace_millis)
            .ok_or_else(|| Error::new(ErrorCode::InvalidLimit, "GC grace predates the epoch"))?;
        let (retained, branches, tags) = self.retained_paths(planned_at_millis).await?;
        let mut candidates = Vec::new();
        let mut continuation = None;
        let prefix = String::new();
        loop {
            let page = self
                .plane
                .list(ListRequest {
                    prefix: prefix.clone(),
                    continuation,
                    limit: 1_000,
                    include_versions: true,
                })
                .await?;
            for entry in page.entries {
                let version_id = entry.metadata.token.version_id.as_deref();
                let managed_data = is_gc_data_path(&self.options.repository_prefix, &entry.path)
                    || !entry
                        .path
                        .as_str()
                        .starts_with(&format!("{}/", self.options.repository_prefix));
                if !managed_data
                    || retained.contains(&entry.path, version_id)
                    || entry.metadata.last_modified_millis > cutoff_millis
                {
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
                candidates.push(GcCandidateV1 {
                    path: entry.path,
                    physical_version,
                    len: entry.metadata.len,
                    last_modified_millis: entry.metadata.last_modified_millis,
                });
                if candidates.len() > max_candidates {
                    return Err(Error::new(
                        ErrorCode::InvalidLimit,
                        "GC candidate count exceeds the configured bound",
                    ));
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        candidates.sort_by(|left, right| {
            left.path.cmp(&right.path).then_with(|| {
                physical_version_key(&left.physical_version)
                    .cmp(&physical_version_key(&right.physical_version))
            })
        });
        let candidate_bytes = candidates.iter().try_fold(0u64, |total, candidate| {
            total
                .checked_add(candidate.len)
                .ok_or_else(|| Error::new(ErrorCode::EntityTooLarge, "GC candidate bytes overflow"))
        })?;
        let mut candidates_by_kind = BTreeMap::new();
        let mut candidate_bytes_by_kind = BTreeMap::new();
        for candidate in &candidates {
            let kind = gc_object_kind(&self.options.repository_prefix, &candidate.path);
            *candidates_by_kind.entry(kind.clone()).or_insert(0) += 1;
            let bytes = candidate_bytes_by_kind.entry(kind).or_insert(0u64);
            *bytes = bytes.checked_add(candidate.len).ok_or_else(|| {
                Error::new(ErrorCode::EntityTooLarge, "GC kind byte counter overflow")
            })?;
        }
        let plan = GcPlanV1::derive(GcPlanBodyV1 {
            repository: self.format.repository_id,
            fence: GcFenceV1 {
                branches,
                tags,
                cutoff_millis,
                planned_at_millis,
            },
            candidates,
        })?;
        self.store_immutable(
            gc_plan_path(&self.options.repository_prefix, plan.id)?,
            encode_canonical(&plan)?,
        )
        .await?;
        Ok(GcDryRun {
            plan,
            retained_paths: retained.len(),
            candidate_bytes,
            candidates_by_kind,
            candidate_bytes_by_kind,
        })
    }

    /// Creates or resumes a bounded GC mark checkpoint. A restarted worker
    /// recomputes reachability using the checkpoint's fixed planning time and
    /// current canonical roots, then CAS-publishes the immutable plan ID.
    pub async fn plan_gc_checkpointed(
        &self,
        run: Option<OperationId>,
        grace_millis: u64,
        max_candidates: usize,
    ) -> Result<GcMarkRunV1> {
        self.validate_gc_plan_limits(grace_millis, max_candidates)?;
        self.system_writer_generation("gc").await?;
        let max_candidates_u64 = u64::try_from(max_candidates).map_err(|_| {
            Error::new(
                ErrorCode::InvalidLimit,
                "GC candidate bound cannot be represented in its checkpoint",
            )
        })?;
        let id = run.unwrap_or_else(|| self.new_operation());
        let path = gc_mark_run_path(&self.options.repository_prefix, id)?;
        let existing = self.load_gc_mark_run_optional(id).await?;
        let mut loaded = match existing {
            Some(loaded) => loaded,
            None => {
                let now = self.now_millis()?;
                let initial = GcMarkRunV1 {
                    id,
                    repository: self.format.repository_id,
                    grace_millis,
                    max_candidates: max_candidates_u64,
                    planned_at_millis: now,
                    generation: 0,
                    state: GcMarkRunStateV1::Running,
                    plan: None,
                    updated_at_millis: now,
                };
                match self
                    .controls
                    .compare_exchange(CompareExchange {
                        path: path.clone(),
                        expected: None,
                        bytes: encode_canonical(&initial)?,
                    })
                    .await
                {
                    Ok(CompareExchangeOutcome::Applied(metadata)) => LoadedGcMarkRun {
                        value: initial,
                        token: metadata.token,
                    },
                    Ok(CompareExchangeOutcome::Conflict(_)) => self.load_gc_mark_run(id).await?,
                    Err(error) => {
                        if let Some(current) = self.load_gc_mark_run_optional(id).await? {
                            current
                        } else {
                            return Err(Error::new(
                                ErrorCode::OutcomeUnknown,
                                format!("GC mark checkpoint creation outcome is unknown: {error}"),
                            )
                            .retry(RetryAdvice::ReconcileOperation)
                            .operation(id.to_string()));
                        }
                    }
                }
            }
        };
        validate_gc_mark_run(
            &loaded.value,
            id,
            self.format.repository_id,
            grace_millis,
            max_candidates_u64,
        )?;
        if matches!(loaded.value.state, GcMarkRunStateV1::Completed) {
            self.load_gc_plan(loaded.value.plan.ok_or_else(|| {
                Error::new(
                    ErrorCode::CorruptCommit,
                    "completed GC mark checkpoint has no immutable plan",
                )
            })?)
            .await?;
            return Ok(loaded.value);
        }

        let dry_run = self
            .plan_gc_at(loaded.value.planned_at_millis, grace_millis, max_candidates)
            .await?;
        let mut next = loaded.value.clone();
        next.plan = Some(dry_run.plan.id);
        next.state = GcMarkRunStateV1::Completed;
        next.generation = next.generation.checked_add(1).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "GC mark generation overflow")
        })?;
        next.updated_at_millis = self.now_millis()?;
        match self
            .controls
            .compare_exchange(CompareExchange {
                path,
                expected: Some(loaded.token),
                bytes: encode_canonical(&next)?,
            })
            .await
        {
            Ok(CompareExchangeOutcome::Applied(_)) => Ok(next),
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                ErrorCode::RefConflict,
                "GC mark checkpoint changed concurrently",
            )
            .retry(RetryAdvice::ReloadHead)),
            Err(error) => {
                loaded = self.load_gc_mark_run(id).await?;
                if loaded.value == next {
                    Ok(next)
                } else {
                    Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        format!("GC mark checkpoint update outcome is unknown: {error}"),
                    )
                    .retry(RetryAdvice::ReconcileOperation)
                    .operation(id.to_string()))
                }
            }
        }
    }

    pub async fn gc_mark_run(&self, id: OperationId) -> Result<GcMarkRunV1> {
        Ok(self.load_gc_mark_run(id).await?.value)
    }

    /// Sweep an immutable dry-run plan after rechecking both the ref fence and
    /// the current retained set. Deletion always names the exact physical
    /// version selected by the plan.
    pub async fn sweep_gc(&self, id: GcPlanId) -> Result<GcSweepReport> {
        self.sweep_gc_batch(id, usize::MAX).await
    }

    /// Sweeps at most `max_candidates` entries and checkpoints progress in a
    /// CAS-protected mutable run record. A crash after physical deletion but
    /// before checkpoint publication is harmless: exact deletion is retried
    /// and recorded as already missing.
    pub async fn sweep_gc_batch(
        &self,
        id: GcPlanId,
        max_candidates: usize,
    ) -> Result<GcSweepReport> {
        if max_candidates == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "GC batch size must be greater than zero",
            ));
        }
        self.system_writer_generation("gc").await?;
        let plan = self.load_gc_plan(id).await?;
        let path = gc_run_path(&self.options.repository_prefix, id)?;
        if self.plane.load_mutable(&path).await?.is_none() {
            let run = GcRunV1 {
                plan: id,
                next_index: 0,
                generation: 0,
                state: GcRunStateV1::Running,
                deleted_versions: 0,
                deleted_bytes: 0,
                skipped_reachable: 0,
                already_missing: 0,
                deleted_by_kind: BTreeMap::new(),
                deleted_bytes_by_kind: BTreeMap::new(),
                updated_at_millis: self.now_millis()?,
                abort_reason: None,
                delete_rate_limit_per_second: self.options.gc_delete_rate_limit_per_second,
                last_delete_at_millis: 0,
            };
            let _ = self
                .controls
                .compare_exchange(CompareExchange {
                    path: path.clone(),
                    expected: None,
                    bytes: encode_canonical(&run)?,
                })
                .await?;
        }
        let initial_index = usize::try_from(self.load_gc_run(id).await?.value.next_index)
            .map_err(|_| Error::new(ErrorCode::CorruptCommit, "GC cursor exceeds usize"))?;
        let invocation_end = initial_index
            .saturating_add(max_candidates)
            .min(plan.body.candidates.len());
        for _ in 0..=MAX_GC_CAS_RETRIES {
            let loaded = self.load_gc_run(id).await?;
            if matches!(loaded.value.state, GcRunStateV1::Completed) {
                return Ok(gc_report(&loaded.value));
            }
            if matches!(loaded.value.state, GcRunStateV1::Aborted) {
                return Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "GC run was aborted after its root fence changed",
                ));
            }
            if matches!(loaded.value.state, GcRunStateV1::Paused) {
                let mut running = loaded.value;
                running.state = GcRunStateV1::Running;
                running.generation = running.generation.checked_add(1).ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "GC run generation overflow")
                })?;
                running.updated_at_millis = self.now_millis()?;
                if matches!(
                    self.controls
                        .compare_exchange(CompareExchange {
                            path: path.clone(),
                            expected: Some(loaded.token),
                            bytes: encode_canonical(&running)?,
                        })
                        .await?,
                    CompareExchangeOutcome::Conflict(_)
                ) {
                    continue;
                }
                // Reload the acquired fence and its storage token.
                continue;
            }
            let (retained, branches, tags) = self.retained_paths(self.now_millis()?).await?;
            if branches != plan.body.fence.branches || tags != plan.body.fence.tags {
                let mut aborted = loaded.value;
                aborted.state = GcRunStateV1::Aborted;
                aborted.generation = aborted.generation.saturating_add(1);
                aborted.updated_at_millis = self.now_millis()?;
                let _ = self
                    .controls
                    .compare_exchange(CompareExchange {
                        path: path.clone(),
                        expected: Some(loaded.token),
                        bytes: encode_canonical(&aborted)?,
                    })
                    .await?;
                return Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "GC ref fence changed after planning; create a new dry-run",
                ));
            }
            let next_index = usize::try_from(loaded.value.next_index)
                .map_err(|_| Error::new(ErrorCode::CorruptCommit, "GC cursor exceeds usize"))?;
            if next_index >= invocation_end && next_index < plan.body.candidates.len() {
                return Ok(gc_report(&loaded.value));
            }
            let end = invocation_end;
            let mut next = loaded.value;
            for candidate in &plan.body.candidates[next_index..end] {
                if candidate.last_modified_millis > plan.body.fence.cutoff_millis {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "GC plan contains a candidate newer than its cutoff",
                    ));
                }
                let version_id = match &candidate.physical_version {
                    PhysicalVersion::Versioned { version_id } => Some(version_id.as_str()),
                    PhysicalVersion::Unversioned { .. } => None,
                };
                if retained.contains(&candidate.path, version_id) {
                    next.skipped_reachable += 1;
                    continue;
                }
                self.pace_gc_delete(&next).await?;
                match self
                    .plane
                    .delete_exact(&candidate.path, candidate.physical_version.clone())
                    .await?
                {
                    DeleteOutcome::Deleted => {
                        next.last_delete_at_millis = self.now_millis()?;
                        next.deleted_versions += 1;
                        next.deleted_bytes = next
                            .deleted_bytes
                            .checked_add(candidate.len)
                            .ok_or_else(|| {
                                Error::new(ErrorCode::EntityTooLarge, "GC deleted bytes overflow")
                            })?;
                        let kind = gc_object_kind(&self.options.repository_prefix, &candidate.path);
                        *next.deleted_by_kind.entry(kind.clone()).or_insert(0) += 1;
                        let bytes = next.deleted_bytes_by_kind.entry(kind).or_insert(0);
                        *bytes = bytes.checked_add(candidate.len).ok_or_else(|| {
                            Error::new(
                                ErrorCode::EntityTooLarge,
                                "GC kind deleted byte counter overflow",
                            )
                        })?;
                    }
                    DeleteOutcome::NotFound => {
                        next.last_delete_at_millis = self.now_millis()?;
                        next.already_missing += 1;
                    }
                    DeleteOutcome::TokenMismatch => {
                        return Err(Error::new(
                            ErrorCode::PreconditionFailed,
                            "GC candidate physical version no longer matches",
                        ));
                    }
                }
            }
            next.next_index = u64::try_from(end)
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "GC cursor exceeds u64"))?;
            next.generation = next.generation.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "GC run generation overflow")
            })?;
            next.updated_at_millis = self.now_millis()?;
            if next.next_index
                == u64::try_from(plan.body.candidates.len()).map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "GC plan length exceeds u64")
                })?
            {
                next.state = GcRunStateV1::Completed;
            } else {
                next.state = GcRunStateV1::Paused;
            }
            match self
                .controls
                .compare_exchange(CompareExchange {
                    path: path.clone(),
                    expected: Some(loaded.token),
                    bytes: encode_canonical(&next)?,
                })
                .await?
            {
                CompareExchangeOutcome::Applied(_) => return Ok(gc_report(&next)),
                CompareExchangeOutcome::Conflict(_) => {
                    // Another worker advanced the checkpoint. Reload and
                    // continue from the published boundary.
                    continue;
                }
            }
        }
        Err(Error::new(
            ErrorCode::RefConflict,
            "GC checkpoint changed beyond retry budget",
        )
        .retry(RetryAdvice::ReloadHead))
    }

    pub async fn load_gc_plan(&self, id: GcPlanId) -> Result<GcPlanV1> {
        let object = self
            .plane
            .get(GetRequest {
                path: gc_plan_path(&self.options.repository_prefix, id)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::InvalidRevision, "GC plan does not exist"))?;
        let plan: GcPlanV1 = decode_canonical(&object.bytes)?;
        plan.validate_id()?;
        if plan.id != id || plan.body.repository != self.format.repository_id {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "GC plan belongs to another repository",
            ));
        }
        Ok(plan)
    }

    pub async fn gc_run(&self, id: GcPlanId) -> Result<GcRunV1> {
        Ok(self.load_gc_run(id).await?.value)
    }

    async fn pace_gc_delete(&self, run: &GcRunV1) -> Result<()> {
        let rate = run.delete_rate_limit_per_second;
        if rate == 0 || run.last_delete_at_millis == 0 {
            return Ok(());
        }
        let interval_millis = 1_000_u64.div_ceil(u64::from(rate));
        let earliest = run
            .last_delete_at_millis
            .checked_add(interval_millis)
            .ok_or_else(|| Error::new(ErrorCode::InternalInvariant, "GC pacing overflow"))?;
        let now = self.now_millis()?;
        if earliest > now {
            tokio::time::sleep(Duration::from_millis(earliest - now)).await;
        }
        Ok(())
    }

    /// Explicitly releases a failed-closed GC publication fence. Operators
    /// must establish that no worker still owns an in-flight delete before
    /// invoking this generation-checked transition.
    pub async fn abort_gc_run(
        &self,
        id: GcPlanId,
        expected_generation: u64,
        reason: &str,
    ) -> Result<GcRunV1> {
        if reason.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "GC abort requires a non-empty operator reason",
            ));
        }
        self.system_writer_generation("gc").await?;
        let loaded = self.load_gc_run(id).await?;
        if loaded.value.generation != expected_generation
            || matches!(loaded.value.state, GcRunStateV1::Completed)
        {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "GC run generation/state does not match abort expectation",
            ));
        }
        if matches!(loaded.value.state, GcRunStateV1::Aborted) {
            return Ok(loaded.value);
        }
        let mut aborted = loaded.value;
        aborted.state = GcRunStateV1::Aborted;
        aborted.generation = aborted.generation.checked_add(1).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "GC run generation overflow")
        })?;
        aborted.updated_at_millis = self.now_millis()?;
        aborted.abort_reason = Some(reason.to_string());
        match self
            .controls
            .compare_exchange(CompareExchange {
                path: gc_run_path(&self.options.repository_prefix, id)?,
                expected: Some(loaded.token),
                bytes: encode_canonical(&aborted)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => Ok(aborted),
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::PreconditionFailed,
                "GC run changed while aborting",
            )),
        }
    }

    async fn retained_paths(
        &self,
        at_millis: u64,
    ) -> Result<(
        RetainedClosure,
        BTreeMap<String, CommitId>,
        BTreeMap<String, CommitId>,
    )> {
        let branches = self
            .list_branches()
            .await?
            .into_iter()
            .map(|branch| (branch.name, branch.target))
            .collect::<BTreeMap<_, _>>();
        let tags = self
            .list_tags()
            .await?
            .into_iter()
            .map(|tag| (tag.name, tag.target))
            .collect::<BTreeMap<_, _>>();
        let mut retained = RetainedClosure::default();
        let mut commit_roots = Vec::new();
        commit_roots.extend(branches.values().copied());
        commit_roots.extend(tags.values().copied());
        let prefix = format!("{}/", self.options.repository_prefix);
        let mut continuation = None;
        loop {
            let page = self
                .plane
                .list(ListRequest {
                    prefix: prefix.clone(),
                    continuation,
                    limit: 1_000,
                    include_versions: false,
                })
                .await?;
            for listed in page.entries {
                if !is_gc_data_path(&self.options.repository_prefix, &listed.path) {
                    retained.paths.insert(listed.path.clone());
                }
                let path = listed.path.as_str();
                let Some(object) = self
                    .plane
                    .get(GetRequest {
                        path: listed.path.clone(),
                        range: None,
                        physical_version: None,
                    })
                    .await?
                else {
                    continue;
                };
                if path.contains("/refs/heads/") {
                    let value: crate::RefValueV1 = decode_canonical(&object.bytes)?;
                    if !value.tombstone {
                        commit_roots.push(value.target);
                    }
                } else if path.contains("/refs/tags/") {
                    let value: crate::TagValueV1 = decode_canonical(&object.bytes)?;
                    if !value.tombstone {
                        commit_roots.push(value.target);
                    }
                } else if path.contains("/retention/pins/") {
                    let value: RetentionPinV1 = decode_canonical(&object.bytes)?;
                    if !value.tombstone
                        && (value.expires_at_millis == 0 || value.expires_at_millis > at_millis)
                    {
                        commit_roots.push(value.target);
                    }
                } else if path.contains("/reflogs/") {
                    let value: ReflogEntryV1 = decode_canonical(&object.bytes)?;
                    let retain_until = value
                        .created_at_millis
                        .saturating_add(self.options.reflog_retention_millis);
                    if self.options.reflog_retention_millis == 0 || retain_until > at_millis {
                        commit_roots.push(value.new_target);
                        commit_roots.extend(value.old_target);
                    }
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }

        let mut seen_commits = BTreeSet::new();
        let mut state_roots = Vec::new();
        while let Some(id) = commit_roots.pop() {
            if !seen_commits.insert(id) {
                continue;
            }
            if seen_commits.len() > self.options.history_traversal_limit {
                return Err(Error::new(
                    ErrorCode::HistoryLimitExceeded,
                    "GC retained commit traversal exceeded its configured limit",
                ));
            }
            let commit = self.load_commit(id).await?;
            retained
                .paths
                .insert(commit_path(&self.options.repository_prefix, id)?);
            if commit.writer_fence_generation == 0 {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "commit has a zero writer fence generation",
                ));
            }
            let objects =
                self.tree_from_root(&commit.state.objects, &self.format.state_tree_format)?;
            let versions =
                self.tree_from_root(&commit.state.versions, &self.format.state_tree_format)?;
            let operations =
                self.tree_from_root(&commit.state.operations, &self.format.state_tree_format)?;
            state_roots.extend([objects, versions.clone(), operations]);
            let mut iter = self.engine.range(&versions, &[], None).await?;
            while let Some(entry) = iter.next().await {
                let (encoded_key, value) = entry?;
                let version: ObjectVersionV1 = decode_canonical(&value)?;
                let key = decode_version_tree_logical_key(&encoded_key)?;
                let path = ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                    Error::new(ErrorCode::CorruptCommit, "logical key is not UTF-8")
                })?)?;
                let version_id = match &version.binding {
                    crate::PhysicalObjectBindingV1::Live { version_id, .. }
                    | crate::PhysicalObjectBindingV1::DeleteMarker { version_id } => version_id,
                };
                retained
                    .physical_versions
                    .insert((path, version_id.clone()));
            }
            commit_roots.extend(commit.parents);
        }
        let nodes = self.engine.mark_reachable(&state_roots).await?;
        let _ = nodes;
        Ok((retained, branches, tags))
    }

    async fn find_version(
        &self,
        commit: &BucketCommitV1,
        key: &[u8],
        selected: ObjectVersionId,
    ) -> Result<ObjectVersionV1> {
        let versions =
            self.tree_from_root(&commit.state.versions, &self.format.state_tree_format)?;
        let prefix = version_tree_prefix(key);
        let mut iter = self.engine.prefix(&versions, &prefix).await?;
        while let Some(entry) = iter.next().await {
            let (_, value) = entry?;
            let version: ObjectVersionV1 = decode_canonical(&value)?;
            if version.id == selected {
                version.validate()?;
                return Ok(version);
            }
        }
        Err(Error::new(
            ErrorCode::NoSuchVersion,
            "object version is not reachable",
        ))
    }

    async fn latest_physical_delete_binding(
        &self,
        commit: &BucketCommitV1,
        key: &[u8],
    ) -> Result<Option<crate::PhysicalObjectBindingV1>> {
        let versions =
            self.tree_from_root(&commit.state.versions, &self.format.state_tree_format)?;
        let mut iter = self
            .engine
            .prefix(&versions, &version_tree_prefix(key))
            .await?;
        while let Some(entry) = iter.next().await {
            let (_, value) = entry?;
            let version: ObjectVersionV1 = decode_canonical(&value)?;
            if matches!(version.body.kind, LogicalObjectVersionKindV1::DeleteMarker) {
                version.validate()?;
                return Ok(Some(version.binding));
            }
        }
        Ok(None)
    }

    async fn load_ref(&self, branch: &str) -> Result<LoadedRef> {
        let loaded = self.load_ref_including_tombstone(branch).await?;
        if loaded.value.tombstone {
            return Err(Error::new(ErrorCode::NoSuchBranch, "branch is tombstoned"));
        }
        Ok(loaded)
    }

    async fn load_ref_including_tombstone(&self, branch: &str) -> Result<LoadedRef> {
        validate_branch(branch)?;
        if let Some(warm) = self
            .warm_branches
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "branch-cache lock poisoned"))?
            .get(&branch.to_string())
        {
            return Ok(LoadedRef {
                value: warm.reference,
                token: warm.token,
            });
        }
        let object = self
            .plane
            .load_mutable(&branch_path(&self.options.repository_prefix, branch)?)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::NoSuchBranch, "branch does not exist"))?;
        let value: crate::RefValueV1 = decode_canonical(&object.bytes)?;
        Ok(LoadedRef {
            value,
            token: object.metadata.token,
        })
    }

    async fn load_gc_run(&self, id: GcPlanId) -> Result<LoadedGcRun> {
        let object = self
            .plane
            .load_mutable(&gc_run_path(&self.options.repository_prefix, id)?)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::InvalidRevision, "GC run does not exist"))?;
        let value: GcRunV1 = decode_canonical(&object.bytes)?;
        let candidate_count = self.load_gc_plan(id).await?.body.candidates.len();
        if value.plan != id
            || value.next_index
                > u64::try_from(candidate_count).map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "GC plan length exceeds u64")
                })?
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "GC run checkpoint is inconsistent with its plan",
            ));
        }
        Ok(LoadedGcRun {
            value,
            token: object.metadata.token,
        })
    }

    async fn load_gc_mark_run_optional(&self, id: OperationId) -> Result<Option<LoadedGcMarkRun>> {
        let Some(object) = self
            .plane
            .load_mutable(&gc_mark_run_path(&self.options.repository_prefix, id)?)
            .await?
        else {
            return Ok(None);
        };
        let value: GcMarkRunV1 = decode_canonical(&object.bytes)?;
        if value.id != id || value.repository != self.format.repository_id {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "GC mark checkpoint identity mismatch",
            ));
        }
        validate_gc_mark_run_shape(&value)?;
        Ok(Some(LoadedGcMarkRun {
            value,
            token: object.metadata.token,
        }))
    }

    async fn load_gc_mark_run(&self, id: OperationId) -> Result<LoadedGcMarkRun> {
        self.load_gc_mark_run_optional(id)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::InvalidRevision, "GC mark run does not exist"))
    }

    async fn load_commit(&self, id: CommitId) -> Result<BucketCommitV1> {
        if let Some(commit) = self
            .commit_cache
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "commit-cache lock poisoned"))?
            .get(&id)
        {
            return Ok(commit);
        }
        let object = self
            .plane
            .get(GetRequest {
                path: commit_path(&self.options.repository_prefix, id)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "commit object is missing"))?;
        let stored = CommitObjectV1::decode_object(&object.bytes)?;
        let commit = stored.commit.clone();
        if commit.id()? != id {
            return Err(Error::new(ErrorCode::CorruptCommit, "commit ID mismatch"));
        }
        self.node_store
            .register_commit_object(id, &stored, &object.bytes)?;
        self.commit_cache
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "commit-cache lock poisoned"))?
            .insert(id, commit.clone());
        Ok(commit)
    }

    async fn load_commit_metadata(&self, id: CommitId) -> Result<BucketCommitV1> {
        if let Some(commit) = self
            .commit_cache
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "commit-cache lock poisoned"))?
            .get(&id)
        {
            return Ok(commit);
        }
        let path = commit_path(&self.options.repository_prefix, id)?;
        let header = self
            .plane
            .get(GetRequest {
                path: path.clone(),
                range: Some(0..=19),
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "commit object is missing"))?;
        let (commit_len, _) = CommitObjectV1::header_lengths(&header.bytes)?;
        if commit_len == 0 {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "commit object has an empty canonical commit",
            ));
        }
        let end = 20_u64
            .checked_add(u64::from(commit_len))
            .and_then(|exclusive| exclusive.checked_sub(1))
            .ok_or_else(|| Error::new(ErrorCode::CorruptCommit, "commit range overflow"))?;
        let encoded = self
            .plane
            .get(GetRequest {
                path,
                range: Some(20..=end),
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "commit object is missing"))?;
        if encoded.bytes.len() != commit_len as usize {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "ranged commit metadata has the wrong length",
            ));
        }
        let commit: BucketCommitV1 = decode_canonical(&encoded.bytes)?;
        if commit.id()? != id {
            return Err(Error::new(ErrorCode::CorruptCommit, "commit ID mismatch"));
        }
        self.commit_cache
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "commit-cache lock poisoned"))?
            .insert(id, commit.clone());
        Ok(commit)
    }

    async fn load_commit_delta(&self, commit: &BucketCommitV1) -> Result<BucketDeltaV1> {
        if commit.writer_fence_generation == 0 {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "commit has a zero writer fence generation",
            ));
        }
        Ok(commit.delta.clone())
    }

    async fn store_commit(
        &self,
        commit: &BucketCommitV1,
        prepared: Option<PreparedNodePack>,
    ) -> Result<StoredCommit> {
        let id = commit.id()?;
        let stored = CommitObjectV1::new(
            commit.clone(),
            prepared.as_ref().map(|prepared| prepared.pack().clone()),
        )?;
        let bytes = stored.encode_object()?;
        let payload_offset = CommitObjectV1::node_payload_offset(&bytes)?;
        self.store_immutable(commit_path(&self.options.repository_prefix, id)?, bytes)
            .await?;
        let pending_pack = prepared
            .map(|prepared| {
                payload_offset
                    .map(|payload_offset| (prepared, payload_offset))
                    .ok_or_else(|| {
                        Error::new(
                            ErrorCode::InternalInvariant,
                            "prepared commit node pack has no payload offset",
                        )
                    })
            })
            .transpose()?;
        self.commit_cache
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "commit-cache lock poisoned"))?
            .insert(id, commit.clone());
        Ok(StoredCommit { id, pending_pack })
    }

    async fn finalize_stored_commit(&self, stored: StoredCommit) -> Result<()> {
        if let Some((prepared, payload_offset)) = stored.pending_pack {
            self.node_store
                .commit_node_pack(stored.id, prepared, payload_offset)
                .await?;
        }
        Ok(())
    }

    async fn store_tag_reflog(&self, entry: &ReflogEntryV1) -> Result<crate::ReflogEntryId> {
        let bytes = encode_canonical(entry)?;
        let id = entry.id()?;
        self.store_immutable(
            tag_reflog_path(&self.options.repository_prefix, &entry.branch, id)?,
            bytes,
        )
        .await?;
        Ok(id)
    }

    async fn store_immutable(&self, path: ObjectPath, bytes: Vec<u8>) -> Result<()> {
        self.plane
            .put_immutable(ImmutablePut {
                path,
                expected_sha256: crate::codec::sha256(&bytes),
                bytes,
            })
            .await?;
        Ok(())
    }

    fn tree_from_root(&self, root: &TreeRootV1, format: &TreeFormat) -> Result<Tree> {
        if root.format_digest != tree_format_digest(format)? {
            return Err(Error::new(
                ErrorCode::UnsupportedRepositoryFormat,
                "state tree format digest mismatch",
            ));
        }
        Ok(Tree {
            root: root.root.clone(),
            config: Config {
                format: format.clone(),
                runtime: RuntimeConfig::default(),
            },
        })
    }

    fn validate_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() || key.len() > self.format.canonical_limits.max_key_bytes as usize {
            return Err(Error::new(
                ErrorCode::InvalidKey,
                "logical key must contain 1 to 1,024 UTF-8 bytes",
            ));
        }
        let key = std::str::from_utf8(key)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8"))?;
        if key == self.options.repository_prefix
            || key.starts_with(&format!("{}/", self.options.repository_prefix))
        {
            return Err(Error::new(
                ErrorCode::InvalidKey,
                "logical key overlaps the prolly-s3 repository metadata prefix",
            ));
        }
        Ok(())
    }
}

fn predicate_matches(predicate: &EtagPredicateV1, current: Option<&str>) -> bool {
    match predicate {
        EtagPredicateV1::Any => current.is_some(),
        EtagPredicateV1::OneOf(values) => current.is_some_and(|current| values.contains(current)),
    }
}

fn validate_write_condition(
    condition: &ObjectWriteConditionV1,
    current_etag: Option<&str>,
) -> Result<()> {
    if condition
        .if_match
        .as_ref()
        .is_some_and(|predicate| !predicate_matches(predicate, current_etag))
        || condition
            .if_none_match
            .as_ref()
            .is_some_and(|predicate| predicate_matches(predicate, current_etag))
    {
        return Err(Error::new(
            ErrorCode::PreconditionFailed,
            "logical object ETag precondition failed at the publication head",
        ));
    }
    Ok(())
}

fn validate_options(options: &RepositoryOptions) -> Result<()> {
    let prefix = &options.repository_prefix;
    if prefix.is_empty()
        || prefix.len() > 384
        || prefix.starts_with('/')
        || prefix.ends_with('/')
        || prefix
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(Error::new(
            ErrorCode::InvalidKey,
            "repository prefix violates the canonical path contract",
        ));
    }
    validate_branch(&options.default_branch)?;
    options.state_tree_format.validate()?;
    if !(10_000..=24 * 60 * 60 * 1_000).contains(&options.writer_lease_millis) {
        return Err(Error::new(
            ErrorCode::InvalidLimit,
            "writer lease must be between 10 seconds and 24 hours",
        ));
    }
    if options.history_traversal_limit == 0 {
        return Err(Error::new(
            ErrorCode::InvalidLimit,
            "history traversal limit must be greater than zero",
        ));
    }
    if !(1..=1_024).contains(&options.max_parallel_payload_writes) {
        return Err(Error::new(
            ErrorCode::InvalidLimit,
            "parallel payload write limit must be between 1 and 1,024",
        ));
    }
    if options.max_cached_commits == 0
        || options.max_cached_branches == 0
        || options.max_cached_node_pack_bytes == 0
        || options.max_cached_node_locations == 0
        || options.max_cached_node_bytes == 0
    {
        return Err(Error::new(
            ErrorCode::InvalidLimit,
            "metadata and node-pack cache bounds must be greater than zero",
        ));
    }
    if options.branch_ref_compaction_interval != 0
        && (options.branch_ref_compaction_interval < 100
            || options.branch_ref_versions_to_retain == 0
            || options.branch_ref_versions_to_retain as u64
                >= options.branch_ref_compaction_interval)
    {
        return Err(Error::new(
            ErrorCode::InvalidLimit,
            "branch-ref compaction requires an interval of at least 100 and a smaller nonzero retention count",
        ));
    }
    if !(2..=10_000).contains(&options.mutable_control_versions_to_retain) {
        return Err(Error::new(
            ErrorCode::InvalidLimit,
            "mutable-control retention must keep between 2 and 10,000 versions",
        ));
    }
    if options.branch_ref_versions_to_retain > options.mutable_control_versions_to_retain {
        return Err(Error::new(
            ErrorCode::InvalidLimit,
            "branch-ref retention cannot exceed the repository mutable-control bound",
        ));
    }
    if options.gc_delete_rate_limit_per_second > 1_000 {
        return Err(Error::new(
            ErrorCode::InvalidLimit,
            "GC delete rate limit must be zero or at most 1,000 per second",
        ));
    }
    Ok(())
}

fn validate_gc_mark_run(
    run: &GcMarkRunV1,
    id: OperationId,
    repository: RepositoryId,
    grace_millis: u64,
    max_candidates: u64,
) -> Result<()> {
    if run.id != id
        || run.repository != repository
        || run.grace_millis != grace_millis
        || run.max_candidates != max_candidates
    {
        return Err(Error::new(
            ErrorCode::IdempotencyConflict,
            "GC mark checkpoint does not match the selected planning request",
        )
        .operation(id.to_string()));
    }
    Ok(())
}

fn validate_gc_mark_run_shape(run: &GcMarkRunV1) -> Result<()> {
    let state_is_valid = match run.state {
        GcMarkRunStateV1::Running => run.plan.is_none() && run.generation == 0,
        GcMarkRunStateV1::Completed => run.plan.is_some() && run.generation == 1,
    };
    if run.grace_millis == 0
        || run.max_candidates == 0
        || run.updated_at_millis < run.planned_at_millis
        || !state_is_valid
    {
        return Err(Error::new(
            ErrorCode::CorruptCommit,
            "GC mark checkpoint state is inconsistent",
        ));
    }
    Ok(())
}

fn validate_format_compatibility(
    format: &RepositoryFormatV1,
    options: &RepositoryOptions,
) -> Result<()> {
    if format.format_version != RepositoryFormatV1::VERSION {
        return Err(Error::new(
            ErrorCode::UnsupportedRepositoryFormat,
            format!(
                "repository format version {} is not supported by format version {}",
                format.format_version,
                RepositoryFormatV1::VERSION
            ),
        ));
    }
    if format.required_capability_profile != RepositoryFormatV1::PROLLY_S3_CAPABILITY_PROFILE {
        return Err(Error::new(
            ErrorCode::UnsupportedRepositoryFormat,
            "repository is not a Prolly S3 repository",
        ));
    }
    if format.min_reader_version == 0
        || format.min_writer_version == 0
        || format.min_reader_version > RepositoryFormatV1::CURRENT_READER_VERSION
        || format.min_writer_version > RepositoryFormatV1::CURRENT_WRITER_VERSION
    {
        return Err(Error::new(
            ErrorCode::UnsupportedRepositoryFormat,
            format!(
                "repository requires reader/writer protocol {}/{}, client supports {}/{}",
                format.min_reader_version,
                format.min_writer_version,
                RepositoryFormatV1::CURRENT_READER_VERSION,
                RepositoryFormatV1::CURRENT_WRITER_VERSION
            ),
        ));
    }
    if format.state_tree_format != options.state_tree_format
        || format.canonical_limits != options.limits
    {
        return Err(Error::new(
            ErrorCode::RepositoryFormatConflict,
            "repository format does not match requested canonical settings",
        ));
    }
    Ok(())
}

fn decode_repository_format(bytes: &[u8]) -> Result<RepositoryFormatV1> {
    decode_canonical(bytes)
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

fn version_tree_key(key: &[u8], order: ObjectVersionOrder, version: ObjectVersionId) -> Vec<u8> {
    let mut output = version_tree_prefix(key);
    output.extend(order.commit_generation.0.to_be_bytes().map(|byte| !byte));
    output.extend(order.mutation_ordinal.to_be_bytes().map(|byte| !byte));
    output.extend(version.as_bytes().iter().map(|byte| !byte));
    output
}

fn ref_catalog_key(tag: bool, name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(name.len() + 2);
    key.extend_from_slice(if tag { b"t\0" } else { b"h\0" });
    key.extend_from_slice(name.as_bytes());
    key
}

fn commit_id_from_path(path: &ObjectPath) -> Result<CommitId> {
    let encoded = path.as_str().rsplit('/').next().unwrap_or_default();
    let raw = hex::decode(encoded)
        .map_err(|_| Error::new(ErrorCode::CorruptCommit, "commit path has an invalid ID"))?;
    Ok(CommitId::from_hash(raw.try_into().map_err(|_| {
        Error::new(ErrorCode::CorruptCommit, "commit ID has the wrong length")
    })?))
}

fn gc_v2_commit_queue_key(id: CommitId) -> Vec<u8> {
    [b"qc/".as_slice(), id.as_bytes()].concat()
}

fn gc_v2_commit_mark_key(id: CommitId) -> Vec<u8> {
    [b"mc/".as_slice(), id.as_bytes()].concat()
}

fn gc_v2_node_queue_key(cid: &prolly::Cid) -> Vec<u8> {
    [b"qn/".as_slice(), cid.as_bytes()].concat()
}

fn gc_v2_node_mark_key(cid: &prolly::Cid) -> Vec<u8> {
    [b"mn/".as_slice(), cid.as_bytes()].concat()
}

fn gc_v2_dirty_root_mark_key(id: GcDirtyRootIdV2) -> Vec<u8> {
    [b"mr/".as_slice(), id.as_bytes()].concat()
}

fn gc_v2_version_queue_key(root: &TreeRootV1) -> Option<Vec<u8>> {
    root.root
        .as_ref()
        .map(|cid| [b"qv/".as_slice(), cid.as_bytes()].concat())
}

fn gc_v2_path_mark_key(path: &ObjectPath) -> Vec<u8> {
    [b"mp/".as_slice(), path.as_str().as_bytes()].concat()
}

fn gc_v2_physical_mark_key(path: &ObjectPath, version_id: &str) -> Vec<u8> {
    let path_bytes = path.as_str().as_bytes();
    let mut key = Vec::with_capacity(3 + 4 + path_bytes.len() + version_id.len());
    key.extend_from_slice(b"mv/");
    key.extend_from_slice(&(path_bytes.len() as u32).to_be_bytes());
    key.extend_from_slice(path_bytes);
    key.extend_from_slice(version_id.as_bytes());
    key
}

fn gc_v2_candidate_key(candidate: &GcCandidateV1) -> Result<Vec<u8>> {
    let digest = crate::codec::sha256(&encode_canonical(candidate)?);
    Ok([b"d/".as_slice(), digest.as_slice()].concat())
}

fn format_path(prefix: &str) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/format/v1.cbor"))
}

fn intent_path(prefix: &str) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/format/initialization.cbor"))
}

fn writer_lease_path(prefix: &str) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/writers/lease.cbor"))
}

fn branch_path(prefix: &str, branch: &str) -> Result<ObjectPath> {
    validate_branch(branch)?;
    ObjectPath::new(format!(
        "{prefix}/refs/heads/{}",
        hex::encode(branch.as_bytes())
    ))
}

fn tag_path(prefix: &str, tag: &str) -> Result<ObjectPath> {
    validate_branch(tag)?;
    ObjectPath::new(format!(
        "{prefix}/refs/tags/{}",
        hex::encode(tag.as_bytes())
    ))
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

fn tag_reflog_path(prefix: &str, tag: &str, id: crate::ReflogEntryId) -> Result<ObjectPath> {
    ObjectPath::new(format!(
        "{prefix}/reflogs/tags/{}/{}",
        hex::encode(tag.as_bytes()),
        hex::encode(id.as_bytes())
    ))
}

fn node_checkpoint_path(
    prefix: &str,
    generation: CommitGeneration,
    id: crate::NodeIndexCheckpointId,
) -> Result<ObjectPath> {
    ObjectPath::new(format!(
        "{prefix}/node-index/checkpoints/{:020}-{}.cbor",
        generation.0,
        hex::encode(id.as_bytes())
    ))
}

fn node_index_head_path(prefix: &str) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/node-index/latest.cbor"))
}

fn node_index_v2_head_path(prefix: &str) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/node-index/v2/head.cbor"))
}

fn ref_catalog_v2_head_path(prefix: &str) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/ref-catalog/v2/head.cbor"))
}

fn commit_graph_v2_head_path(prefix: &str) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/commit-graph/v2/head.cbor"))
}

fn gc_plan_path(prefix: &str, id: GcPlanId) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/gc/plans/{id}.cbor"))
}

fn gc_run_path(prefix: &str, id: GcPlanId) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/gc/runs/{id}.cbor"))
}

fn gc_mark_run_path(prefix: &str, id: OperationId) -> Result<ObjectPath> {
    ObjectPath::new(format!(
        "{prefix}/gc/mark-runs/{}.cbor",
        hex::encode(id.as_bytes())
    ))
}

fn gc_epoch_v2_path(prefix: &str, id: OperationId) -> Result<ObjectPath> {
    ObjectPath::new(format!(
        "{prefix}/gc/v2/epochs/{}/head.cbor",
        hex::encode(id.as_bytes())
    ))
}

fn gc_coordinator_v2_path(prefix: &str) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/gc/v2/coordinator.cbor"))
}

fn commit_closure_stack_key(sequence: u64) -> Vec<u8> {
    format!("q/{sequence:020}").into_bytes()
}

fn commit_closure_seen_key(commit: CommitId) -> Vec<u8> {
    let mut key = b"s/".to_vec();
    key.extend_from_slice(commit.as_bytes());
    key
}

fn commit_closure_mapping_key(commit: CommitId) -> Vec<u8> {
    let mut key = b"m/".to_vec();
    key.extend_from_slice(commit.as_bytes());
    key
}

fn fsck_node_queue_key(kind: u8, cid: &prolly::Cid) -> Vec<u8> {
    let mut key = b"fq/".to_vec();
    key.push(kind);
    key.extend_from_slice(cid.as_bytes());
    key
}

fn fsck_node_seen_key(kind: u8, cid: &prolly::Cid) -> Vec<u8> {
    let mut key = b"fs/".to_vec();
    key.push(kind);
    key.extend_from_slice(cid.as_bytes());
    key
}

fn fsck_global_node_seen_key(cid: &prolly::Cid) -> Vec<u8> {
    let mut key = b"fg/".to_vec();
    key.extend_from_slice(cid.as_bytes());
    key
}

fn fsck_version_queue_key(digest: &[u8; 32]) -> Vec<u8> {
    let mut key = b"fvq/".to_vec();
    key.extend_from_slice(digest);
    key
}

fn fsck_version_seen_key(digest: &[u8; 32]) -> Vec<u8> {
    let mut key = b"fvs/".to_vec();
    key.extend_from_slice(digest);
    key
}

fn physical_transfer_mapping_path(prefix: &str, source: CommitId) -> Result<ObjectPath> {
    let encoded = hex::encode(source.as_bytes());
    ObjectPath::new(format!(
        "{prefix}/administration/v2/transfer-mappings/sha256/{}/{}/{}",
        &encoded[..2],
        &encoded[2..4],
        encoded
    ))
}

fn physical_transfer_destination_scope<Q: ObjectPlane>(target: &Repository<Q>) -> [u8; 32] {
    derive_input_digest(&[
        b"physical-transfer-destination-v1",
        target.format.repository_id.as_bytes(),
        target.options.repository_prefix.as_bytes(),
    ])
}

fn gc_dirty_root_v2_prefix(prefix: &str, epoch: OperationId) -> String {
    format!(
        "{prefix}/gc/v2/dirty-roots/{}/",
        hex::encode(epoch.as_bytes())
    )
}

fn gc_dirty_root_v2_sequence_prefix(prefix: &str, epoch: OperationId, sequence: u64) -> String {
    format!(
        "{}{:020}/",
        gc_dirty_root_v2_prefix(prefix, epoch),
        sequence
    )
}

fn gc_dirty_root_v2_path(
    prefix: &str,
    event: &GcDirtyRootV2,
    id: GcDirtyRootIdV2,
) -> Result<ObjectPath> {
    ObjectPath::new(format!(
        "{}{:020}/{}",
        gc_dirty_root_v2_prefix(prefix, event.epoch),
        event.publication_sequence,
        hex::encode(id.as_bytes())
    ))
}

fn gc_epoch_v2_tree_path(id: OperationId) -> String {
    format!("gc/v2/epochs/{}/tree", hex::encode(id.as_bytes()))
}

fn gc_object_kind(prefix: &str, path: &ObjectPath) -> String {
    path.as_str()
        .strip_prefix(prefix)
        .and_then(|value| value.strip_prefix('/'))
        .and_then(|value| value.split('/').next())
        .unwrap_or("unknown")
        .to_string()
}

fn gc_report(run: &GcRunV1) -> GcSweepReport {
    GcSweepReport {
        plan: run.plan,
        deleted_versions: run.deleted_versions,
        deleted_bytes: run.deleted_bytes,
        skipped_reachable: run.skipped_reachable,
        already_missing: run.already_missing,
        complete: matches!(run.state, GcRunStateV1::Completed),
        next_index: run.next_index,
        deleted_by_kind: run.deleted_by_kind.clone(),
        deleted_bytes_by_kind: run.deleted_bytes_by_kind.clone(),
    }
}

fn retention_pin_path(prefix: &str, name: &str) -> Result<ObjectPath> {
    validate_branch(name)?;
    ObjectPath::new(format!(
        "{prefix}/retention/pins/{}",
        hex::encode(name.as_bytes())
    ))
}

fn is_gc_data_path(prefix: &str, path: &ObjectPath) -> bool {
    let relative = path
        .as_str()
        .strip_prefix(prefix)
        .and_then(|value| value.strip_prefix('/'));
    relative.is_some_and(|value| value.starts_with("commits/"))
}

fn is_portable_clone_path(relative: &str) -> bool {
    relative.starts_with("format/")
        || relative.starts_with("commits/")
        || relative.starts_with("reflogs/")
        || relative.starts_with("refs/")
}

fn physical_version_key(version: &PhysicalVersion) -> String {
    match version {
        PhysicalVersion::Versioned { version_id } => format!("v:{version_id}"),
        PhysicalVersion::Unversioned { token: Some(token) } => {
            format!("u:{}:{:?}", token.etag, token.version_id)
        }
        PhysicalVersion::Unversioned { token: None } => "u:".to_string(),
    }
}

fn object_diff_from_prolly(diff: prolly::Diff) -> Result<ObjectDiff> {
    match diff {
        prolly::Diff::Added { key, val } => Ok(ObjectDiff {
            key,
            from: None,
            to: Some(decode_canonical::<CurrentObjectV1>(&val)?.version.id),
        }),
        prolly::Diff::Removed { key, val } => Ok(ObjectDiff {
            key,
            from: Some(decode_canonical::<CurrentObjectV1>(&val)?.version.id),
            to: None,
        }),
        prolly::Diff::Changed { key, old, new } => Ok(ObjectDiff {
            key,
            from: Some(decode_canonical::<CurrentObjectV1>(&old)?.version.id),
            to: Some(decode_canonical::<CurrentObjectV1>(&new)?.version.id),
        }),
    }
}

impl From<prolly::Error> for Error {
    fn from(error: prolly::Error) -> Self {
        Error::new(
            ErrorCode::CorruptNode,
            format!("Prolly operation failed: {error}"),
        )
    }
}
