use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    io::Write as _,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock, Weak,
    },
    time::Duration,
};

use crate::store::PreparedNodePack;
use crate::{
    decode_canonical, derive_input_digest, derive_repository_id, encode_canonical,
    tree_format_digest, BatchId, BucketCommitV1, BucketDeltaV1, BucketStateV1, CanonicalLimits,
    CanonicalOperationResult, ChecksumExpectation, Clock, CommitGeneration, CommitId,
    CommitObjectV1, CommitReceipt, CompareExchange, CompareExchangeOutcome, CurrentObjectV1,
    DeleteOutcome, Error, ErrorCode, EtagPredicateV1, GcCandidateV1, GcFenceV1, GcMarkRunStateV1,
    GcMarkRunV1, GcPlanBodyV1, GcPlanId, GcPlanV1, GcRunStateV1, GcRunV1, GetRequest, IdSource,
    ImmutablePut, InitializationIntentV1, ListRequest, LogicalObjectVersionBodyV1,
    LogicalObjectVersionKindV1, NativeBatchV1, NativePreparedMutationV1, ObjectData, ObjectHeaders,
    ObjectPath, ObjectPlane, ObjectTransition, ObjectVersionId, ObjectVersionOrder,
    ObjectVersionV1, ObjectWriteConditionV1, OperationId, OperationKind, OperationRecordV1,
    PhysicalVersion, ProllyObjectStore, RandomIdSource, RefGeneration, ReflogEntryV1,
    RepositoryFormatV1, RepositoryId, Result, RetentionPinV1, RetryAdvice, StorageToken,
    SystemClock, TreeRootV1,
};
use futures_util::{stream::BoxStream, Stream, StreamExt};
use md5::{Digest as _, Md5};
use prolly::{AsyncProlly, Config, RuntimeConfig, Tree, TreeFormat};
use sha2::Sha256;

const MIN_NONFINAL_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024;
const MAX_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const MAX_GC_CAS_RETRIES: usize = 16;

#[derive(Clone)]
pub struct RepositoryOptions {
    pub repository_prefix: String,
    pub default_branch: String,
    pub writer: String,
    pub limits: CanonicalLimits,
    pub state_tree_format: TreeFormat,
    /// Duration of the repository-scoped native writer lease. Renewal is
    /// amortized and is not part of an ordinary operation's foreground calls.
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
    /// Maximum exact physical deletions per second during GC. Zero disables
    /// pacing. The native format accepts 1..=1,000 when configured.
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairReport {
    pub sync: SyncReport,
    pub fsck: FsckReport,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RepositoryPerformanceSnapshot {
    pub publication_acquisitions: u64,
    pub publication_wait_nanos: u64,
    pub publication_queue_depth: u64,
    pub publication_max_queue_depth: u64,
}

#[derive(Default)]
struct RepositoryPerformanceCounters {
    publication_acquisitions: AtomicU64,
    publication_wait_nanos: AtomicU64,
    publication_queue_depth: AtomicU64,
    publication_max_queue_depth: AtomicU64,
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
    native_versions: BTreeSet<(ObjectPath, String)>,
}

impl RetainedClosure {
    fn contains(&self, path: &ObjectPath, version: Option<&str>) -> bool {
        self.paths.contains(path)
            || version.is_some_and(|version| {
                self.native_versions
                    .contains(&(path.clone(), version.to_string()))
            })
    }

    fn len(&self) -> usize {
        self.paths.len() + self.native_versions.len()
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

pub struct WriterLeaseMaintenance {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for WriterLeaseMaintenance {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct Repository<P: ObjectPlane> {
    plane: Arc<P>,
    options: RepositoryOptions,
    format: RepositoryFormatV1,
    node_store: ProllyObjectStore<P>,
    engine: AsyncProlly<ProllyObjectStore<P>>,
    writer_lease: Arc<RwLock<Option<HeldWriterLease>>>,
    warm_branches: Arc<RwLock<BoundedCache<String, WarmBranchState>>>,
    commit_cache: Arc<RwLock<BoundedCache<CommitId, BucketCommitV1>>>,
    native_publication: Arc<tokio::sync::Mutex<()>>,
    payload_writes: Arc<tokio::sync::Semaphore>,
    operation_locks: Arc<std::sync::Mutex<BTreeMap<OperationId, Weak<tokio::sync::Mutex<()>>>>>,
    lease_renewal: Arc<tokio::sync::Mutex<()>>,
    performance: Arc<RepositoryPerformanceCounters>,
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
            min_reader_version: RepositoryFormatV1::NATIVE_VERSIONED_PROTOCOL_VERSION,
            min_writer_version: RepositoryFormatV1::NATIVE_VERSIONED_PROTOCOL_VERSION,
            created_at_millis,
            required_capability_profile: RepositoryFormatV1::NATIVE_VERSIONED_S3_CAPABILITY_PROFILE,
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
        let mut repository =
            Self::from_format(plane.clone(), options.clone(), intent.format.clone())?;
        repository.acquire_native_writer().await?;

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
        let writer_fence_generation = repository.writer_fence_generation()?;
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
        repository.finalize_stored_commit(stored)?;

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
        match plane
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
        let mut repository = Self::from_format(plane, options, format)?;
        repository.load_latest_node_index_checkpoint().await?;
        repository.acquire_native_writer().await?;
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
        let node_store = ProllyObjectStore::new_packed_with_cache_limit(
            plane.clone(),
            options.repository_prefix.clone(),
            options.max_cached_node_pack_bytes,
        );
        let engine = AsyncProlly::new(node_store.clone(), config);
        let max_cached_branches = options.max_cached_branches;
        let max_cached_commits = options.max_cached_commits;
        let max_parallel_payload_writes = options.max_parallel_payload_writes;
        Ok(Self {
            plane,
            options,
            format,
            node_store,
            engine,
            writer_lease: Arc::new(RwLock::new(None)),
            warm_branches: Arc::new(RwLock::new(BoundedCache::new(max_cached_branches))),
            commit_cache: Arc::new(RwLock::new(BoundedCache::new(max_cached_commits))),
            native_publication: Arc::new(tokio::sync::Mutex::new(())),
            payload_writes: Arc::new(tokio::sync::Semaphore::new(max_parallel_payload_writes)),
            operation_locks: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            lease_renewal: Arc::new(tokio::sync::Mutex::new(())),
            performance: Arc::new(RepositoryPerformanceCounters::default()),
        })
    }

    pub fn performance_snapshot(&self) -> RepositoryPerformanceSnapshot {
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
        }
    }

    async fn lock_publication(&self) -> tokio::sync::MutexGuard<'_, ()> {
        let depth = self
            .performance
            .publication_queue_depth
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        self.performance
            .publication_max_queue_depth
            .fetch_max(depth, Ordering::Relaxed);
        let started = std::time::Instant::now();
        let guard = self.native_publication.lock().await;
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
        guard
    }

    /// Serialize requests that reuse an idempotency key before they touch the
    /// data plane. This prevents concurrent retries in one writer process from
    /// creating duplicate, unreachable native S3 versions.
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
            .plane
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

    async fn load_latest_node_index_checkpoint(&self) -> Result<()> {
        let Some(head_object) = self
            .plane
            .load_mutable(&node_index_head_path(&self.options.repository_prefix)?)
            .await?
        else {
            return Ok(());
        };
        let head = match decode_canonical::<crate::NodeIndexHeadV1>(&head_object.bytes) {
            Ok(head) => head,
            Err(_) => return self.node_store.rebuild_node_index().await,
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
            return self.node_store.rebuild_node_index().await;
        };
        let checkpoint = match decode_canonical::<crate::NodeIndexCheckpointV1>(&checkpoint.bytes) {
            Ok(checkpoint) => checkpoint,
            Err(_) => return self.node_store.rebuild_node_index().await,
        };
        if checkpoint.repository != self.format.repository_id
            || checkpoint.validate().is_err()
            || head.validate(&checkpoint).is_err()
        {
            return self.node_store.rebuild_node_index().await;
        }
        self.node_store.import_node_index(&checkpoint.entries)
    }

    fn now_millis(&self) -> Result<u64> {
        self.options.clock.now_millis()
    }

    async fn acquire_native_writer(&mut self) -> Result<()> {
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
                        "native repository is owned by another writer; takeover requires an explicit credential-isolated handoff",
                    ));
                }
                if current.expires_at_millis <= now {
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "native writer lease expired; automatic reacquisition is forbidden",
                    ));
                }
                let mut renewed = current;
                renewed.updated_at_millis = now;
                renewed.expires_at_millis = expires_at_millis;
                (renewed, Some(stored.metadata.token))
            }
        };
        match self
            .plane
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
                "native writer lease changed during acquisition",
            )),
        }
    }

    /// Renew the repository-scoped exclusive writer lease. Services should
    /// call this from an independent maintenance loop; mutations also renew
    /// opportunistically near the deadline.
    pub async fn renew_writer_lease(&self) -> Result<()> {
        let _renewal = self.lease_renewal.lock().await;
        self.renew_writer_lease_inner().await
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
                "native writer lease expired; publication is fenced",
            ));
        }
        let mut renewed = held.value;
        renewed.updated_at_millis = now;
        renewed.expires_at_millis = now
            .checked_add(self.options.writer_lease_millis)
            .ok_or_else(|| Error::new(ErrorCode::InvalidLimit, "writer lease expiry overflow"))?;
        let renewal = self
            .plane
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
                    "native writer lease was lost; publication is fenced",
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

    /// Run independent native-writer lease renewal until the returned handle
    /// is dropped. A failed or ambiguous renewal fences this repository before
    /// the task exits.
    pub fn start_writer_lease_maintenance(self: &Arc<Self>) -> Result<WriterLeaseMaintenance> {
        if self.options.read_only {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "writer lease maintenance requires a writable native repository",
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
                if repository.renew_writer_lease().await.is_err() {
                    break;
                }
            }
        });
        Ok(WriterLeaseMaintenance { task })
    }

    /// Explicitly take over an expired or credential-revoked native writer.
    /// The caller must have independently stopped/revoked the old writer; S3
    /// cannot make ref CAS conditional on this separate lease object.
    pub async fn takeover_native_writer(
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
        let path = writer_lease_path(&self.options.repository_prefix)?;
        let stored = self.plane.load_mutable(&path).await?.ok_or_else(|| {
            Error::new(ErrorCode::MissingClosure, "native writer lease is missing")
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
                .plane
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
                .plane
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

    async fn native_writer_generation_for_mutation(&self) -> Result<u64> {
        let held = self
            .writer_lease
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "writer-lease lock poisoned"))?
            .clone()
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::PreconditionFailed,
                    "native repository has no exclusive writer authority",
                )
            })?;
        let now = self.now_millis()?;
        if held.value.expires_at_millis <= now {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "native writer lease expired; publication is fenced",
            ));
        }
        let renew_at = held
            .value
            .expires_at_millis
            .saturating_sub(self.options.writer_lease_millis / 3);
        if now >= renew_at {
            let _renewal = self.lease_renewal.lock().await;
            let current = self
                .writer_lease
                .read()
                .map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "writer-lease lock poisoned")
                })?
                .clone()
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::PreconditionFailed,
                        "native repository has no exclusive writer authority",
                    )
                })?;
            let now = self.now_millis()?;
            let renew_at = current
                .value
                .expires_at_millis
                .saturating_sub(self.options.writer_lease_millis / 3);
            if now >= renew_at {
                self.renew_writer_lease_inner().await?;
            }
        }
        self.writer_fence_generation()
    }

    fn writer_fence_generation(&self) -> Result<u64> {
        self.writer_lease
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "writer-lease lock poisoned"))?
            .as_ref()
            .map(|lease| lease.value.generation)
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::PreconditionFailed,
                    "native repository has no exclusive writer authority",
                )
            })
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

    /// Replay the complete logical history into an empty native-versioned
    /// destination. Provider attestations and maintenance state remain local.
    pub async fn clone_to<Q: ObjectPlane>(
        &self,
        destination: Arc<Q>,
        destination_prefix: &str,
    ) -> Result<CloneReport> {
        self.clone_native_to(destination, destination_prefix).await
    }

    async fn clone_native_to<Q: ObjectPlane>(
        &self,
        destination: Arc<Q>,
        destination_prefix: &str,
    ) -> Result<CloneReport> {
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
                    "native clone destination has a different repository format",
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
                        "native clone destination contains repository data without a format marker",
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
                        "native clone destination format was created concurrently",
                    ))
                }
            }
        };

        let mut target_options = self.options.clone();
        target_options.repository_prefix = destination_prefix.to_string();
        target_options.read_only = false;
        let mut target =
            Repository::<Q>::from_format(destination, target_options, self.format.clone())?;
        target.acquire_native_writer().await?;
        let target = Arc::new(target);
        let _lease_maintenance = target.start_writer_lease_maintenance()?;

        let branches = self.list_branches().await?;
        let tags = self.list_tags().await?;
        let roots = branches
            .iter()
            .map(|branch| branch.target)
            .chain(tags.iter().map(|tag| tag.target))
            .collect::<Vec<_>>();
        let (commit_map, sync) = self
            .replay_native_history_to(target.as_ref(), &roots, false)
            .await?;
        let writer_fence_generation = target.native_writer_generation_for_mutation().await?;
        let mut report = CloneReport {
            immutable_objects: sync.copied_objects + usize::from(format_created),
            immutable_bytes: sync.copied_bytes,
            refs: 0,
        };

        for branch in branches {
            let target_id = *commit_map.get(&branch.target).ok_or_else(|| {
                Error::new(
                    ErrorCode::MissingClosure,
                    "native clone branch target was not replayed",
                )
            })?;
            let path = branch_path(destination_prefix, &branch.name)?;
            if let Some(existing) = target.plane.load_mutable(&path).await? {
                let value: crate::RefValueV1 = decode_canonical(&existing.bytes)?;
                if value.target != target_id || value.tombstone {
                    return Err(Error::new(
                        ErrorCode::RefConflict,
                        "native clone destination branch has a divergent target",
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
                message: "native logical clone".to_string(),
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
                .plane
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
                            "native clone destination branch was created concurrently",
                        ));
                    }
                    report.refs += 1;
                }
                CompareExchangeOutcome::Conflict(None) => {
                    return Err(Error::new(
                        ErrorCode::RefConflict,
                        "native clone branch create returned an empty conflict",
                    ))
                }
            }
        }
        for tag in tags {
            let target_id = *commit_map.get(&tag.target).ok_or_else(|| {
                Error::new(
                    ErrorCode::MissingClosure,
                    "native clone tag target was not replayed",
                )
            })?;
            let path = tag_path(destination_prefix, &tag.name)?;
            if let Some(existing) = target.plane.load_mutable(&path).await? {
                let value: crate::TagValueV1 = decode_canonical(&existing.bytes)?;
                if value.target != target_id || value.tombstone {
                    return Err(Error::new(
                        ErrorCode::RefConflict,
                        "native clone destination tag has a divergent target",
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
                message: "native logical clone tag".to_string(),
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
                .plane
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
                            "native clone destination tag was created concurrently",
                        ));
                    }
                    report.refs += 1;
                }
                CompareExchangeOutcome::Conflict(None) => {
                    return Err(Error::new(
                        ErrorCode::RefConflict,
                        "native clone tag create returned an empty conflict",
                    ))
                }
            }
        }
        target.fsck().await?;
        Ok(report)
    }

    async fn clone_native_version_binding<Q: ObjectPlane>(
        &self,
        target: &Repository<Q>,
        key: &[u8],
        version: &ObjectVersionV1,
        operation: OperationId,
        writer_fence_generation: u64,
    ) -> Result<crate::NativeObjectBindingV1> {
        let path =
            ObjectPath::new(std::str::from_utf8(key).map_err(|_| {
                Error::new(ErrorCode::CorruptCommit, "native clone key is not UTF-8")
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
                crate::NativeObjectBindingV1::Live {
                    version_id,
                    checksum_sha256,
                    ..
                },
            ) => {
                let spool = tempfile::NamedTempFile::new().map_err(|error| {
                    Error::new(
                        ErrorCode::Transport,
                        format!("could not create native clone spool: {error}"),
                    )
                })?;
                let source = self
                    .plane
                    .get_native_file(crate::NativeFileGet {
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
                        "native clone source object failed logical checksum verification",
                    ));
                }
                let _payload_permit = target.payload_write_permit().await;
                let write = target
                    .plane
                    .put_native_file(crate::NativeFilePut {
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
                    || !matches!(write.binding, crate::NativeObjectBindingV1::Live { .. })
                {
                    return Err(Error::new(
                        ErrorCode::ChecksumMismatch,
                        "native clone destination object failed logical checksum verification",
                    ));
                }
                Ok(write.binding)
            }
            (
                LogicalObjectVersionKindV1::DeleteMarker,
                crate::NativeObjectBindingV1::DeleteMarker { .. },
            ) => {
                let _payload_permit = target.payload_write_permit().await;
                match target
                    .plane
                    .delete_native(crate::NativeDelete {
                        path: path.clone(),
                        repository: target.format.repository_id,
                        operation,
                        writer_fence_generation,
                    })
                    .await
                {
                    Ok(binding) => Ok(binding),
                    Err(error) => match target.reconcile_native_delete(&path).await? {
                        Some(binding) => Ok(binding),
                        None => Err(error),
                    },
                }
            }
            _ => Err(Error::new(
                ErrorCode::CorruptCommit,
                "native clone source version has an invalid binding",
            )),
        }
    }

    async fn ordered_native_commit_closure(
        &self,
        roots: &[CommitId],
    ) -> Result<Vec<(CommitId, BucketCommitV1)>> {
        let mut pending = roots.to_vec();
        let mut commits = BTreeMap::new();
        while let Some(id) = pending.pop() {
            if commits.contains_key(&id) {
                continue;
            }
            if commits.len() >= self.options.history_traversal_limit {
                return Err(Error::new(
                    ErrorCode::HistoryLimitExceeded,
                    "native transfer commit closure exceeded its configured history limit",
                ));
            }
            let commit = self.load_commit(id).await?;
            pending.extend(commit.parents.iter().copied());
            commits.insert(id, commit);
        }
        let mut ordered = commits.into_iter().collect::<Vec<_>>();
        ordered.sort_by(|(left_id, left), (right_id, right)| {
            left.generation
                .cmp(&right.generation)
                .then_with(|| left_id.cmp(right_id))
        });
        Ok(ordered)
    }

    async fn all_native_commits(&self) -> Result<Vec<(CommitId, BucketCommitV1)>> {
        let prefix = format!("{}/commits/sha256/", self.options.repository_prefix);
        let mut continuation = None;
        let mut commits = BTreeMap::new();
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
            for entry in page.entries {
                let encoded = entry.path.as_str().rsplit('/').next().unwrap_or_default();
                let raw = hex::decode(encoded).map_err(|_| {
                    Error::new(
                        ErrorCode::CorruptCommit,
                        "native transfer commit path is not canonical hex",
                    )
                })?;
                let id = CommitId::from_hash(raw.try_into().map_err(|_| {
                    Error::new(
                        ErrorCode::CorruptCommit,
                        "native transfer commit path has the wrong ID length",
                    )
                })?);
                let commit = self.load_commit(id).await?;
                commits.insert(id, commit);
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        let mut ordered = commits.into_iter().collect::<Vec<_>>();
        ordered.sort_by(|(left_id, left), (right_id, right)| {
            left.generation
                .cmp(&right.generation)
                .then_with(|| left_id.cmp(right_id))
        });
        Ok(ordered)
    }

    async fn native_logical_commit_fingerprints(
        &self,
        ordered: &[(CommitId, BucketCommitV1)],
    ) -> Result<BTreeMap<CommitId, [u8; 32]>> {
        let mut fingerprints: BTreeMap<CommitId, [u8; 32]> = BTreeMap::new();
        for (id, commit) in ordered {
            let mut parent_bytes = Vec::with_capacity(commit.parents.len() * 32);
            for parent in &commit.parents {
                parent_bytes.extend_from_slice(fingerprints.get(parent).ok_or_else(|| {
                    Error::new(
                        ErrorCode::CorruptCommit,
                        "native logical fingerprint encountered a child before its parent",
                    )
                })?);
            }
            // Physical S3 version bindings are intentionally stored inline in
            // the current-object tree. They differ after clone or push, so a
            // transfer fingerprint must describe logical object identity
            // rather than the provider-specific tree root.
            let objects = encode_canonical(&self.current_object_map(commit).await?)?;
            let operations = encode_canonical(&commit.state.operations)?;
            let generation = commit.generation.0.to_be_bytes();
            let delta = encode_canonical(&commit.delta)?;
            let message = encode_canonical(&commit.message)?;
            let metadata = encode_canonical(&commit.metadata)?;
            let fingerprint = derive_input_digest(&[
                b"native-logical-commit-v1",
                &parent_bytes,
                &objects,
                &operations,
                &generation,
                &delta,
                commit.author.as_bytes(),
                &message,
                &commit.created_at_millis.to_be_bytes(),
                &metadata,
            ]);
            fingerprints.insert(*id, fingerprint);
        }
        Ok(fingerprints)
    }

    async fn replay_native_history_to<Q: ObjectPlane>(
        &self,
        target: &Repository<Q>,
        source_roots: &[CommitId],
        force_rebind: bool,
    ) -> Result<(BTreeMap<CommitId, CommitId>, SyncReport)> {
        let source_ordered = self.ordered_native_commit_closure(source_roots).await?;
        let source_fingerprints = self
            .native_logical_commit_fingerprints(&source_ordered)
            .await?;
        let target_by_fingerprint = if force_rebind {
            BTreeMap::new()
        } else {
            let target_ordered = target.all_native_commits().await?;
            let target_fingerprints = target
                .native_logical_commit_fingerprints(&target_ordered)
                .await?;
            target_fingerprints
                .into_iter()
                .map(|(id, fingerprint)| (fingerprint, id))
                .collect::<BTreeMap<_, _>>()
        };
        let mut commit_map = BTreeMap::new();
        let mut report = SyncReport::default();
        for (source_id, _) in &source_ordered {
            if let Some(target_id) = target_by_fingerprint.get(
                source_fingerprints
                    .get(source_id)
                    .expect("source fingerprint exists"),
            ) {
                commit_map.insert(*source_id, *target_id);
                report.already_present += 1;
            }
        }
        let writer_fence_generation = target.native_writer_generation_for_mutation().await?;
        let mut binding_map: BTreeMap<(Vec<u8>, ObjectVersionId), crate::NativeObjectBindingV1> =
            BTreeMap::new();
        for (source_id, source_commit) in source_ordered {
            if commit_map.contains_key(&source_id) {
                continue;
            }
            let mut mapped_parents = Vec::with_capacity(source_commit.parents.len());
            for parent in &source_commit.parents {
                mapped_parents.push(*commit_map.get(parent).ok_or_else(|| {
                    Error::new(
                        ErrorCode::MissingClosure,
                        "native transfer parent was not mapped",
                    )
                })?);
            }
            let base = match mapped_parents.first() {
                Some(parent) => Some(target.load_commit(*parent).await?),
                None => None,
            };
            let empty = target.engine.create();
            let mut objects = match &base {
                Some(commit) => target
                    .tree_from_root(&commit.state.objects, &target.format.state_tree_format)?,
                None => empty.clone(),
            };
            let mut versions = match &base {
                Some(commit) => target
                    .tree_from_root(&commit.state.versions, &target.format.state_tree_format)?,
                None => empty.clone(),
            };
            let mut operations = match &base {
                Some(commit) => target
                    .tree_from_root(&commit.state.operations, &target.format.state_tree_format)?,
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
            for transition in &delta.changes {
                let mut version = self
                    .find_version(&source_commit, &transition.key, transition.next)
                    .await?;
                let binding_key = (transition.key.clone(), version.id);
                let (binding, copied_payload) = if let Some(binding) = binding_map.get(&binding_key)
                {
                    (binding.clone(), false)
                } else if let Some(base) = &base {
                    match target
                        .find_version(base, &transition.key, transition.next)
                        .await
                    {
                        Ok(existing) => (existing.binding, false),
                        Err(error) if error.code == ErrorCode::NoSuchVersion => (
                            self.clone_native_version_binding(
                                target,
                                &transition.key,
                                &version,
                                physical_operation,
                                writer_fence_generation,
                            )
                            .await?,
                            true,
                        ),
                        Err(error) => return Err(error),
                    }
                } else {
                    (
                        self.clone_native_version_binding(
                            target,
                            &transition.key,
                            &version,
                            physical_operation,
                            writer_fence_generation,
                        )
                        .await?,
                        true,
                    )
                };
                if copied_payload {
                    let size = match &version.body.kind {
                        LogicalObjectVersionKindV1::Live { size, .. } => *size,
                        LogicalObjectVersionKindV1::DeleteMarker => 0,
                    };
                    report.copied_bytes =
                        report.copied_bytes.checked_add(size).ok_or_else(|| {
                            Error::new(
                                ErrorCode::EntityTooLarge,
                                "native transfer byte count overflow",
                            )
                        })?;
                    report.copied_objects += 1;
                }
                binding_map.insert(binding_key, binding.clone());
                version.binding = binding;
                version.validate()?;
                versions = target
                    .engine
                    .put(
                        &versions,
                        version_tree_key(&transition.key, version.body.order, version.id),
                        encode_canonical(&version)?,
                    )
                    .await?;
                objects = if transition.delete_marker {
                    target.engine.delete(&objects, &transition.key).await?
                } else {
                    target
                        .engine
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
                            "native transfer delta names a missing operation",
                        )
                    })?;
                operations = target
                    .engine
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
                    "native transfer replay did not reproduce the logical operation state",
                ));
            }
            let prepared = target.node_store.prepare_node_pack(
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
            let stored = target.store_commit(&destination_commit, prepared).await?;
            let destination_id = stored.id;
            target.finalize_stored_commit(stored)?;
            report.copied_objects += 1;
            commit_map.insert(source_id, destination_id);
        }
        Ok((commit_map, report))
    }

    /// Import portable immutable repository objects without moving a local
    /// ref. The returned source head may then be inspected or merged.
    pub async fn fetch_from<Q: ObjectPlane>(
        &self,
        source: &Repository<Q>,
        source_branch: &str,
    ) -> Result<SyncReport> {
        self.validate_sync_identity(source)?;
        let _publication = self.lock_publication().await;
        let source_head = source.head(source_branch).await?;
        let (mapped, mut report) = source
            .replay_native_history_to(self, &[source_head], false)
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
        let source_head = self.head(source_branch).await?;
        let _publication = destination.lock_publication().await;
        let (mapped, mut report) = self
            .replay_native_history_to(destination, &[source_head], false)
            .await?;
        let mapped_head = *mapped.get(&source_head).ok_or_else(|| {
            Error::new(
                ErrorCode::MissingClosure,
                "native push did not map its selected source head",
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
        let commit = self.load_commit(from).await?;
        let operation = self.new_operation();
        let _native_publication = self.lock_publication().await;
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
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
            .plane
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
        let _native_publication = self.lock_publication().await;
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
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
            .plane
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
            .native_reflog_history(branch)
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
        let mut entries = self.native_reflog_history(branch).await?;
        entries.sort_by(|left, right| {
            left.1
                .created_at_millis
                .cmp(&right.1.created_at_millis)
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(entries)
    }

    async fn native_reflog_history(
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
                    "native first-parent history contains a cycle",
                ));
            }
            if seen.len() > self.options.history_traversal_limit {
                return Err(Error::new(
                    ErrorCode::HistoryLimitExceeded,
                    "native reflog traversal exceeded its configured limit",
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
        let _native_publication = self.lock_publication().await;
        self.move_ref_inner(branch, loaded, target, reason).await
    }

    async fn move_ref_inner(
        &self,
        branch: &str,
        loaded: LoadedRef,
        target: CommitId,
        reason: &str,
    ) -> Result<RefMoveReceipt> {
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
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
            .plane
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

    pub async fn list_branches(&self) -> Result<Vec<BranchHead>> {
        let prefix = format!("{}/refs/heads/", self.options.repository_prefix);
        let mut continuation = None;
        let mut result = Vec::new();
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
                    result.push(BranchHead {
                        name,
                        target: value.target,
                        generation: value.generation,
                    });
                }
            }
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
        self.load_commit(target).await?;
        let _publication = self.lock_publication().await;
        self.native_writer_generation_for_mutation().await?;
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
            .plane
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

    pub async fn list_tags(&self) -> Result<Vec<Tag>> {
        let prefix = format!("{}/refs/tags/", self.options.repository_prefix);
        let mut continuation = None;
        let mut result = Vec::new();
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
            for entry in page.entries {
                let encoded = entry.path.as_str().strip_prefix(&prefix).ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "tag list escaped prefix")
                })?;
                let name =
                    String::from_utf8(hex::decode(encoded).map_err(|_| {
                        Error::new(ErrorCode::CorruptCommit, "tag path is not hex")
                    })?)
                    .map_err(|_| Error::new(ErrorCode::CorruptCommit, "tag name is not UTF-8"))?;
                let Some(stored) = self.plane.load_mutable(&entry.path).await? else {
                    continue;
                };
                let value: crate::TagValueV1 = decode_canonical(&stored.bytes)?;
                if !value.tombstone {
                    result.push(Tag {
                        name,
                        target: value.target,
                    });
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        result.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(result)
    }

    pub async fn delete_tag(&self, name: &str, expected: CommitId) -> Result<()> {
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
            .plane
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
        self.load_commit(target).await?;
        let _publication = self.lock_publication().await;
        self.native_writer_generation_for_mutation().await?;
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
            .plane
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
            .plane
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
        let prefix = format!("{}/retention/pins/", self.options.repository_prefix);
        let now = self.now_millis()?;
        let mut continuation = None;
        let mut pins = Vec::new();
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
            for entry in page.entries {
                let Some(stored) = self.plane.load_mutable(&entry.path).await? else {
                    continue;
                };
                let pin: RetentionPinV1 = decode_canonical(&stored.bytes)?;
                if !pin.tombstone && (pin.expires_at_millis == 0 || pin.expires_at_millis > now) {
                    pins.push(pin);
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        pins.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(pins)
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
            .plane
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
        self.put_native_bytes_checked(
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

    /// Spool a stream once, then upload it as one native S3 object version.
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
                format!("could not create native upload spool: {error}"),
            )
        })?;
        let mut size = 0_u64;
        let mut sha256 = Sha256::new();
        let mut md5 = Md5::new();
        while let Some(next) = stream.next().await {
            let next = next.map_err(|error| {
                Error::new(
                    ErrorCode::Transport,
                    format!("native object input stream failed: {error}"),
                )
            })?;
            let next = next.as_ref();
            size = size.checked_add(next.len() as u64).ok_or_else(|| {
                Error::new(ErrorCode::EntityTooLarge, "native object length overflow")
            })?;
            if size > self.format.canonical_limits.max_object_bytes {
                return Err(Error::new(
                    ErrorCode::EntityTooLarge,
                    "native object exceeds the repository size limit",
                ));
            }
            spool.write_all(next).map_err(|error| {
                Error::new(
                    ErrorCode::Transport,
                    format!("native upload spool write failed: {error}"),
                )
            })?;
            sha256.update(next);
            md5.update(next);
        }
        spool.flush().map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("native upload spool flush failed: {error}"),
            )
        })?;
        let checksum_sha256: [u8; 32] = sha256.finalize().into();
        let checksum_md5: [u8; 16] = md5.finalize().into();
        self.put_native_file_checked(
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
    async fn put_native_bytes_checked(
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
                "request checksum does not match the native object body",
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
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
        let path =
            ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let _payload_permit = self.payload_write_permit().await;
        let native = self
            .plane
            .put_native(crate::NativePut {
                path: path.clone(),
                bytes,
                headers: headers.clone(),
                user_metadata: user_metadata.clone(),
                repository: self.format.repository_id,
                operation,
                writer_fence_generation,
            })
            .await;
        let native = match native {
            Ok(value) => value,
            Err(error) => match self
                .reconcile_native_payload(&path, operation, expected_sha256)
                .await?
            {
                Some(value) => value,
                None => return Err(error),
            },
        };
        drop(_payload_permit);
        if native.size != expected_size
            || native.checksums.sha256 != Some(expected_sha256)
            || native.checksums.md5 != Some(expected_md5)
        {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "native provider result disagrees with the uploaded object identity",
            ));
        }
        let _publication = self.lock_publication().await;
        self.commit_one(
            branch,
            key,
            kind,
            native.binding,
            OperationKind::Put,
            operation,
            input_digest,
            "PutObject",
            condition,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_native_file_checked(
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
                "request checksum does not match the native object body",
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
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
        let path =
            ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let _payload_permit = self.payload_write_permit().await;
        let native = self
            .plane
            .put_native_file(crate::NativeFilePut {
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
        let native = match native {
            Ok(value) => value,
            Err(error) => match self
                .reconcile_native_payload(&path, operation, expected_sha256)
                .await?
            {
                Some(value) => value,
                None => return Err(error),
            },
        };
        drop(_payload_permit);
        if native.size != expected_size
            || native.checksums.sha256 != Some(expected_sha256)
            || native.checksums.md5 != Some(expected_md5)
        {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "native provider result disagrees with the uploaded object identity",
            ));
        }
        let _publication = self.lock_publication().await;
        self.commit_one(
            branch,
            key,
            kind,
            native.binding,
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
        let crate::NativeObjectBindingV1::Live {
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
                .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "native object version is missing"))?;
            if range.is_none() && object.metadata.sha256 != checksum_sha256 {
                Err(Error::new(
                    ErrorCode::CorruptContent,
                    "native object bytes do not match the committed checksum",
                ))?;
            }
            yield bytes::Bytes::from(object.bytes);
        })
    }

    pub async fn create_native_multipart_upload(
        &self,
        branch: &str,
        key: Vec<u8>,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        operation: Option<OperationId>,
    ) -> Result<crate::NativeMultipartSessionV1> {
        validate_branch(branch)?;
        self.validate_key(&key)?;
        let operation = operation.unwrap_or_else(|| self.new_operation());
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
        let path =
            ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let provider_upload_id = self
            .plane
            .create_native_multipart(crate::NativeMultipartCreate {
                path,
                headers: headers.clone(),
                user_metadata: user_metadata.clone(),
                repository: self.format.repository_id,
                operation,
                writer_fence_generation,
            })
            .await?;
        let session = crate::NativeMultipartSessionV1 {
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

    pub async fn upload_native_multipart_part(
        &self,
        session: &crate::NativeMultipartSessionV1,
        part_number: u32,
        bytes: Vec<u8>,
    ) -> Result<crate::NativeMultipartPartResult> {
        session.validate_address(self.format.repository_id)?;
        if !(1..=10_000).contains(&part_number) || bytes.len() as u64 > MAX_MULTIPART_PART_BYTES {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "native multipart part number or size is invalid",
            ));
        }
        let path =
            ObjectPath::new(std::str::from_utf8(&session.key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let _payload_permit = self.payload_write_permit().await;
        self.plane
            .upload_native_multipart_part(crate::NativeMultipartUploadPart {
                path,
                upload_id: session.provider_upload_id.clone(),
                part_number,
                bytes,
            })
            .await
    }

    pub async fn upload_native_multipart_part_stream<S, B, E>(
        &self,
        session: &crate::NativeMultipartSessionV1,
        part_number: u32,
        stream: S,
    ) -> Result<crate::NativeMultipartPartResult>
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
                format!("could not create native multipart spool: {error}"),
            )
        })?;
        let mut size = 0_u64;
        let mut checksum = Sha256::new();
        while let Some(next) = stream.next().await {
            let next = next.map_err(|error| {
                Error::new(
                    ErrorCode::Transport,
                    format!("native multipart part body failed: {error}"),
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
                    format!("native multipart spool write failed: {error}"),
                )
            })?;
            checksum.update(next);
        }
        spool.flush().map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("native multipart spool flush failed: {error}"),
            )
        })?;
        let path =
            ObjectPath::new(std::str::from_utf8(&session.key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let _payload_permit = self.payload_write_permit().await;
        self.plane
            .upload_native_multipart_file_part(crate::NativeMultipartFilePart {
                path,
                upload_id: session.provider_upload_id.clone(),
                part_number,
                body_path: spool.path().to_path_buf(),
                size,
                checksum_sha256: checksum.finalize().into(),
            })
            .await
    }

    pub async fn upload_native_multipart_part_copy(
        &self,
        session: &crate::NativeMultipartSessionV1,
        part_number: u32,
        source_branch: &str,
        source_key: &[u8],
        source_version: Option<ObjectVersionId>,
        range: Option<(u64, u64)>,
    ) -> Result<crate::NativeMultipartPartResult> {
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
        let crate::NativeObjectBindingV1::Live { version_id, .. } = &source.version.binding else {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "native multipart live source has a delete-marker binding",
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
            .upload_native_multipart_part_copy(crate::NativeMultipartUploadPartCopy {
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
                "native multipart copied part checksum differs from its source",
            ));
        }
        Ok(result)
    }

    pub async fn complete_native_multipart_upload(
        &self,
        session: crate::NativeMultipartSessionV1,
        parts: Vec<crate::NativeMultipartCompletedPart>,
        checksum_sha256: [u8; 32],
        checksum_md5: [u8; 16],
        size: u64,
        operation: Option<OperationId>,
    ) -> Result<CommitReceipt> {
        session.validate(self.format.repository_id)?;
        if operation.is_some_and(|operation| operation != session.operation) {
            return Err(Error::new(
                ErrorCode::IdempotencyConflict,
                "native multipart completion must reuse its create operation ID",
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
                "native multipart completion has invalid ordering, count, or nonfinal part size",
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
                "native multipart declared size does not match its part receipts",
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
            b"native-multipart-complete",
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
        if self.native_writer_generation_for_mutation().await? != session.writer_fence_generation {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "native multipart upload belongs to an older writer fence",
            ));
        }
        let path =
            ObjectPath::new(std::str::from_utf8(&session.key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let _payload_permit = self.payload_write_permit().await;
        let completed = self
            .plane
            .complete_native_multipart(crate::NativeMultipartComplete {
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
                .reconcile_native_payload(&path, session.operation, checksum_sha256)
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
                "native multipart result disagrees with its declared object identity",
            ));
        }
        let _publication = self.lock_publication().await;
        self.commit_one(
            &session.branch,
            session.key,
            kind,
            completed.binding,
            OperationKind::Put,
            session.operation,
            input_digest,
            "CompleteMultipartUpload",
            ObjectWriteConditionV1::default(),
        )
        .await
    }

    pub async fn abort_native_multipart_upload(
        &self,
        session: &crate::NativeMultipartSessionV1,
    ) -> Result<()> {
        session.validate_address(self.format.repository_id)?;
        let path =
            ObjectPath::new(std::str::from_utf8(&session.key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        self.plane
            .abort_native_multipart(crate::NativeMultipartAbort {
                path,
                upload_id: session.provider_upload_id.clone(),
            })
            .await
    }

    pub async fn begin_native_batch(
        &self,
        branch: &str,
        message: impl Into<String>,
        expires_after_millis: u64,
    ) -> Result<NativeBatchV1> {
        validate_branch(branch)?;
        let base_commit = self.warm_branch_state(branch).await?.reference.target;
        let now = self.now_millis()?;
        Ok(NativeBatchV1 {
            id: self.new_batch(),
            branch: branch.to_string(),
            base_commit,
            operation: self.new_operation(),
            message: message.into(),
            created_at_millis: now,
            expires_at_millis: now.checked_add(expires_after_millis).ok_or_else(|| {
                Error::new(ErrorCode::InvalidRequest, "native batch expiry overflow")
            })?,
        })
    }

    pub async fn publish_native_batch(
        &self,
        batch: NativeBatchV1,
        mutations: Vec<crate::NativeBatchMutationV1>,
    ) -> Result<CommitReceipt> {
        if mutations.is_empty()
            || mutations.len() > self.format.canonical_limits.max_mutations_per_commit as usize
            || batch.expires_at_millis < self.now_millis()?
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "native batch is empty, expired, or exceeds the mutation limit",
            ));
        }
        let mut unique_keys = BTreeSet::new();
        for mutation in &mutations {
            self.validate_key(mutation.key())?;
            if !unique_keys.insert(mutation.key().to_vec()) {
                return Err(Error::new(
                    ErrorCode::InvalidRequest,
                    "native batch contains the same key more than once",
                ));
            }
        }
        let request_digest = derive_input_digest(&[
            b"native-batch",
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
                "branch moved since native batch creation",
            ));
        }
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
        let results =
            futures_util::stream::iter(mutations.into_iter().map(|mutation| async move {
                self.prepare_native_batch_mutation(
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
        let _publication = self.lock_publication().await;
        self.commit_batch(&batch, &prepared, request_digest).await
    }

    async fn prepare_native_batch_mutation(
        &self,
        mutation: crate::NativeBatchMutationV1,
        operation: OperationId,
        writer_fence_generation: u64,
    ) -> Result<(Vec<u8>, NativePreparedMutationV1)> {
        match mutation {
            crate::NativeBatchMutationV1::Put {
                key,
                bytes,
                headers,
                user_metadata,
            } => {
                let path = ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                    Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
                })?)?;
                let _payload_permit = self.payload_write_permit().await;
                let native = self
                    .plane
                    .put_native(crate::NativePut {
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
                    NativePreparedMutationV1::NativePut {
                        key,
                        size: native.size,
                        logical_etag: native.logical_etag,
                        checksums: native.checksums,
                        headers,
                        user_metadata,
                        binding: native.binding,
                    },
                ))
            }
            crate::NativeBatchMutationV1::Delete { key } => {
                let path = ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                    Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
                })?)?;
                let _payload_permit = self.payload_write_permit().await;
                let binding = match self
                    .plane
                    .delete_native(crate::NativeDelete {
                        path: path.clone(),
                        repository: self.format.repository_id,
                        operation,
                        writer_fence_generation,
                    })
                    .await
                {
                    Ok(binding) => binding,
                    Err(error) => match self.reconcile_native_delete(&path).await? {
                        Some(binding) => binding,
                        None => return Err(error),
                    },
                };
                Ok((
                    key.clone(),
                    NativePreparedMutationV1::NativeDelete { key, binding },
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
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
        let path =
            ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
        let _payload_permit = self.payload_write_permit().await;
        let binding = match self
            .plane
            .delete_native(crate::NativeDelete {
                path: path.clone(),
                repository: self.format.repository_id,
                operation,
                writer_fence_generation,
            })
            .await
        {
            Ok(binding) => binding,
            Err(error) => match self.reconcile_native_delete(&path).await? {
                Some(binding) => binding,
                None => return Err(error),
            },
        };
        drop(_payload_permit);
        let _publication = self.lock_publication().await;
        self.commit_one(
            branch,
            key,
            kind,
            binding,
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
        self.delete_objects_native(branch, keys, operation, input_digest)
            .await
    }

    async fn delete_objects_native(
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
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
        let results = futures_util::stream::iter(keys.iter().map(|key| async move {
            let path = ObjectPath::new(std::str::from_utf8(key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
            let _payload_permit = self.payload_write_permit().await;
            let binding = match self
                .plane
                .delete_native(crate::NativeDelete {
                    path: path.clone(),
                    repository: self.format.repository_id,
                    operation,
                    writer_fence_generation,
                })
                .await
            {
                Ok(binding) => binding,
                Err(error) => match self.reconcile_native_delete(&path).await? {
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

        let _publication = self.lock_publication().await;
        let warm = self.warm_branch_state(branch).await?;
        let loaded_ref = LoadedRef {
            value: warm.reference,
            token: warm.token,
        };
        let base = warm.commit;
        let engine = AsyncProlly::new(
            self.node_store.clone(),
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
        let prepared = self.node_store.prepare_node_pack(
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
        if self.native_writer_generation_for_mutation().await? != writer_fence_generation {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "native writer fence changed during multi-delete publication",
            ));
        }
        match self
            .plane
            .compare_exchange(CompareExchange {
                path: branch_path(&self.options.repository_prefix, branch)?,
                expected: Some(loaded_ref.token),
                bytes: encode_canonical(&next_ref)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(metadata) => {
                self.finalize_stored_commit(stored)?;
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
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::PreconditionFailed,
                "native branch CAS conflicted; writer is fenced and must reopen",
            )),
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
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
        let crate::NativeObjectBindingV1::Live {
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
            .copy_native(crate::NativeCopy {
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
                .reconcile_native_payload(&destination_path, operation, checksum_sha256)
                .await?
            {
                Some(result) => result.binding,
                None => return Err(error),
            },
        };
        drop(_payload_permit);
        let _publication = self.lock_publication().await;
        self.commit_one(
            branch,
            destination_key,
            kind,
            binding,
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
        binding: crate::NativeObjectBindingV1,
        operation_kind: OperationKind,
        operation: OperationId,
        input_digest: [u8; 32],
        message: &str,
        condition: ObjectWriteConditionV1,
    ) -> Result<CommitReceipt> {
        validate_branch(branch)?;
        let created_at_millis = self.now_millis()?;
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
        let engine = AsyncProlly::new(
            self.node_store.clone(),
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
        let prepared = self.node_store.prepare_node_pack(
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
        let current_fence = self.native_writer_generation_for_mutation().await?;
        if current_fence != writer_fence_generation {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "native writer fence changed during publication",
            ));
        }
        let publication = self
            .plane
            .compare_exchange(CompareExchange {
                path: branch_path(&self.options.repository_prefix, branch)?,
                expected: Some(loaded_ref.token),
                bytes: encode_canonical(&next_ref)?,
            })
            .await;
        match publication {
            Ok(CompareExchangeOutcome::Applied(metadata)) => {
                self.finalize_stored_commit(stored)?;
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
                self.warm_branches
                    .write()
                    .map_err(|_| {
                        Error::new(ErrorCode::InternalInvariant, "branch-cache lock poisoned")
                    })?
                    .remove(&branch.to_string());
                Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "native branch CAS conflicted; writer is fenced and must reopen",
                ))
            }
            Err(error) => {
                if let Some(receipt) = self
                    .reconcile_operation(branch, operation, input_digest)
                    .await?
                {
                    self.finalize_stored_commit(stored)?;
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
        batch: &NativeBatchV1,
        mutations: &BTreeMap<Vec<u8>, NativePreparedMutationV1>,
        input_digest: [u8; 32],
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
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
        let engine = AsyncProlly::new(
            self.node_store.clone(),
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
        let generation = CommitGeneration(base.generation.0.checked_add(1).ok_or_else(|| {
            Error::new(ErrorCode::InternalInvariant, "commit generation overflow")
        })?);
        let now = self.now_millis()?;
        let mut transitions = Vec::with_capacity(mutations.len());
        let mut version_ids = Vec::with_capacity(mutations.len());
        for (ordinal, mutation) in mutations.values().enumerate() {
            let key = mutation.key();
            let previous = engine
                .get(&objects, key)
                .await?
                .map(|bytes| decode_canonical::<CurrentObjectV1>(&bytes))
                .transpose()?
                .map(|current| current.version.id);
            let (kind, binding) = match mutation {
                NativePreparedMutationV1::NativePut {
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
                NativePreparedMutationV1::NativeDelete { binding, .. } => {
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
            objects = if matches!(version.body.kind, LogicalObjectVersionKindV1::DeleteMarker) {
                engine.delete(&objects, key).await?
            } else {
                engine
                    .put(
                        &objects,
                        key.to_vec(),
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
                key: key.to_vec(),
                previous,
                next: version.id,
                delete_marker: matches!(
                    version.body.kind,
                    LogicalObjectVersionKindV1::DeleteMarker
                ),
            });
            version_ids.push(version.id);
        }
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
        let prepared = self.node_store.prepare_node_pack(
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
                crate::NativeObjectBindingV1::Live {
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
                            "native object version is missing",
                        )
                    })?;
                if object.metadata.sha256 != *checksum_sha256 {
                    return Err(Error::new(
                        ErrorCode::CorruptContent,
                        "native object bytes do not match the committed checksum",
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
        let _native_publication = self.lock_publication().await;
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
        let engine = AsyncProlly::new(
            self.node_store.clone(),
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
                        .latest_native_delete_binding(&theirs_commit, &change.key)
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
                                .delete_native(crate::NativeDelete {
                                    path: path.clone(),
                                    repository: self.format.repository_id,
                                    operation,
                                    writer_fence_generation,
                                })
                                .await
                            {
                                Ok(binding) => binding,
                                Err(error) => match self.reconcile_native_delete(&path).await? {
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
        let prepared = self.node_store.prepare_node_pack(
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
        let source_commit = self.load_commit(source).await?;
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
        let _native_publication = self.lock_publication().await;
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
        let engine = AsyncProlly::new(
            self.node_store.clone(),
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
                        .latest_native_delete_binding(&source_commit, key)
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
                                .delete_native(crate::NativeDelete {
                                    path: path.clone(),
                                    repository: self.format.repository_id,
                                    operation,
                                    writer_fence_generation,
                                })
                                .await
                            {
                                Ok(binding) => binding,
                                Err(error) => match self.reconcile_native_delete(&path).await? {
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
        let prepared = self.node_store.prepare_node_pack(
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
        if self.native_writer_generation_for_mutation().await? != writer_fence_generation {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "native writer fence changed during prepared publication",
            ));
        }
        let publication = self
            .plane
            .compare_exchange(CompareExchange {
                path: branch_path(&self.options.repository_prefix, branch)?,
                expected: Some(loaded_ref.token),
                bytes: encode_canonical(&next_ref)?,
            })
            .await;
        match publication {
            Ok(CompareExchangeOutcome::Applied(metadata)) => {
                self.finalize_stored_commit(stored)?;
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
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                ErrorCode::PreconditionFailed,
                "native branch CAS conflicted; writer is fenced and must reopen",
            )
            .retry(RetryAdvice::ReloadHead)
            .operation(operation.to_string())),
            Err(error) => {
                if let Some(receipt) = self.lookup_operation(branch, operation).await? {
                    self.finalize_stored_commit(stored)?;
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
        let branches = self.list_branches().await?;
        let tags = self.list_tags().await?;
        let mut roots = branches
            .iter()
            .map(|branch| branch.target)
            .collect::<Vec<_>>();
        roots.extend(tags.iter().map(|tag| tag.target));
        self.fsck_roots(roots, branches.len(), tags.len()).await
    }

    /// Verifies one selected commit closure. This is the incremental fsck
    /// primitive used after fetch/push or by a caller walking new heads.
    pub async fn fsck_commit(&self, head: CommitId) -> Result<FsckReport> {
        self.fsck_roots(vec![head], 0, 0).await
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
        let _publication = self.lock_publication().await;
        let source_head = source.head(source_branch).await?;
        let (mapped, mut sync) = source
            .replay_native_history_to(self, &[source_head], true)
            .await?;
        let repaired_head = *mapped.get(&source_head).ok_or_else(|| {
            Error::new(
                ErrorCode::MissingClosure,
                "native repair did not return a mapped head",
            )
        })?;
        let loaded = self.load_ref(source_branch).await?;
        if loaded.value.target != current {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "destination branch moved during native repair",
            ));
        }
        let movement = self
            .move_ref_inner(
                source_branch,
                loaded,
                repaired_head,
                "repair native bindings from qualified source",
            )
            .await?;
        sync.source_head = Some(repaired_head);
        sync.ref_move = Some(movement);
        let fsck = self.fsck_commit(repaired_head).await?;
        Ok(RepairReport { sync, fsck })
    }

    async fn fsck_roots(
        &self,
        root_ids: Vec<CommitId>,
        branch_count: usize,
        tag_count: usize,
    ) -> Result<FsckReport> {
        let mut report = FsckReport {
            branches: branch_count,
            tags: tag_count,
            ..FsckReport::default()
        };
        let mut seen_commits = HashSet::new();
        let mut stack = root_ids.clone();
        let mut roots = Vec::new();
        while let Some(id) = stack.pop() {
            if !seen_commits.insert(id) {
                continue;
            }
            let commit = self.load_commit(id).await?;
            self.load_commit_delta(&commit).await?;
            report.commits += 1;
            report.deltas += 1;
            roots.push(self.tree_from_root(&commit.state.objects, &self.format.state_tree_format)?);
            roots
                .push(self.tree_from_root(&commit.state.versions, &self.format.state_tree_format)?);
            roots.push(
                self.tree_from_root(&commit.state.operations, &self.format.state_tree_format)?,
            );
            stack.extend(commit.parents);
        }
        let reachability = self.engine.mark_reachable(&roots).await?;
        report.reachable_nodes = reachability.live_nodes;
        report.reachable_node_bytes = reachability.live_bytes;

        let mut versions_seen = BTreeSet::new();
        for root in root_ids {
            let commit = self.load_commit(root).await?;
            let versions =
                self.tree_from_root(&commit.state.versions, &self.format.state_tree_format)?;
            let mut iter = self.engine.range(&versions, &[], None).await?;
            while let Some(entry) = iter.next().await {
                let (encoded_key, bytes) = entry?;
                let version: ObjectVersionV1 = decode_canonical(&bytes)?;
                if !versions_seen.insert(version.id) {
                    continue;
                }
                report.logical_versions += 1;
                let key = decode_version_tree_logical_key(&encoded_key)?;
                let verified = self.verify_native_version(&key, &version).await?;
                report.content_bytes_verified = report
                    .content_bytes_verified
                    .checked_add(verified)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorCode::EntityTooLarge,
                            "fsck provider byte counter overflow",
                        )
                    })?;
            }
        }
        Ok(report)
    }

    async fn verify_native_version(&self, key: &[u8], version: &ObjectVersionV1) -> Result<u64> {
        version.validate()?;
        let path = ObjectPath::new(std::str::from_utf8(key).map_err(|_| {
            Error::new(ErrorCode::CorruptCommit, "native logical key is not UTF-8")
        })?)?;
        match &version.binding {
            crate::NativeObjectBindingV1::Live {
                version_id,
                checksum_sha256,
                ..
            } => {
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
                            "retained native object version is missing",
                        )
                    })?;
                if crate::codec::sha256(&object.bytes) != *checksum_sha256 {
                    return Err(Error::new(
                        ErrorCode::CorruptContent,
                        "retained native object version checksum mismatch",
                    ));
                }
                let expected_size = match version.body.kind {
                    LogicalObjectVersionKindV1::Live { size, .. } => size,
                    LogicalObjectVersionKindV1::DeleteMarker => {
                        unreachable!("binding was validated")
                    }
                };
                if object.bytes.len() as u64 != expected_size {
                    return Err(Error::new(
                        ErrorCode::CorruptContent,
                        "retained native object version size mismatch",
                    ));
                }
                Ok(expected_size)
            }
            crate::NativeObjectBindingV1::DeleteMarker { version_id } => {
                let mut continuation = None;
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
                    if page.entries.iter().any(|entry| {
                        entry.path == path
                            && entry.metadata.delete_marker
                            && entry.metadata.token.version_id.as_deref()
                                == Some(version_id.as_str())
                    }) {
                        return Ok(0);
                    }
                    continuation = page.continuation;
                    if continuation.is_none() {
                        return Err(Error::new(
                            ErrorCode::MissingClosure,
                            "retained native delete marker is missing",
                        ));
                    }
                }
            }
        }
    }

    async fn reconcile_native_payload(
        &self,
        path: &ObjectPath,
        operation: OperationId,
        expected_sha256: [u8; 32],
    ) -> Result<Option<crate::NativeObjectWriteResult>> {
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
                matches.push(crate::NativeObjectWriteResult {
                    binding: crate::NativeObjectBindingV1::Live {
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
                "multiple native versions match one operation; manual repair is required",
            )
            .retry(RetryAdvice::ReconcileOperation)
            .operation(operation.to_string())),
        }
    }

    async fn reconcile_native_delete(
        &self,
        path: &ObjectPath,
    ) -> Result<Option<crate::NativeObjectBindingV1>> {
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
            [version_id] => Ok(Some(crate::NativeObjectBindingV1::DeleteMarker {
                version_id: version_id.clone(),
            })),
            _ => Err(Error::new(
                ErrorCode::OutcomeUnknown,
                "native delete reconciliation found multiple current delete markers",
            )
            .retry(RetryAdvice::ReconcileOperation)),
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
                    .plane
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
            .plane
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
                .plane
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
                    self.plane
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
                    .plane
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
                .plane
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
            .plane
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
                    crate::NativeObjectBindingV1::Live { version_id, .. }
                    | crate::NativeObjectBindingV1::DeleteMarker { version_id } => version_id,
                };
                retained.native_versions.insert((path, version_id.clone()));
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

    async fn latest_native_delete_binding(
        &self,
        commit: &BucketCommitV1,
        key: &[u8],
    ) -> Result<Option<crate::NativeObjectBindingV1>> {
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

    fn finalize_stored_commit(&self, stored: StoredCommit) -> Result<()> {
        if let Some((prepared, payload_offset)) = stored.pending_pack {
            self.node_store
                .commit_node_pack(stored.id, prepared, payload_offset)?;
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
                "logical key overlaps the native-versioned repository metadata prefix",
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
    {
        return Err(Error::new(
            ErrorCode::InvalidLimit,
            "metadata and node-pack cache bounds must be greater than zero",
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
    if format.required_capability_profile
        != RepositoryFormatV1::NATIVE_VERSIONED_S3_CAPABILITY_PROFILE
    {
        return Err(Error::new(
            ErrorCode::UnsupportedRepositoryFormat,
            "repository is not a native-versioned S3 repository",
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
