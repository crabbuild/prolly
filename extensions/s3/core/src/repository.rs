use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    io::Write as _,
    sync::{Arc, RwLock},
    time::Duration,
};

use crate::{
    decode_canonical, derive_input_digest, derive_repository_id, encode_canonical,
    tree_format_digest, BucketCommitV1, BucketDeltaV1, BucketStateV1, CanonicalLimits,
    CanonicalOperationResult, ChecksumExpectation, Clock, CommitGeneration, CommitId,
    CommitReceipt, CompareExchange, CompareExchangeOutcome, ContentStore, CurrentObjectV1,
    DeleteOutcome, DeltaId, Error, ErrorCode, EtagPredicateV1, GcCandidateV1, GcFenceV1,
    GcMarkRunStateV1, GcMarkRunV1, GcPlanBodyV1, GcPlanId, GcPlanV1, GcRunStateV1, GcRunV1,
    GetRequest, IdSource, ImmutablePut, ImmutablePutOutcome, InitializationIntentV1, ListRequest,
    MultipartCatalogEntryV1, MultipartCatalogSnapshotBodyV1, MultipartCatalogSnapshotId,
    MultipartCatalogSnapshotV1, MultipartPartV1, MultipartStateV1, MultipartUploadV1, ObjectData,
    ObjectHeaders, ObjectPath, ObjectPlane, ObjectTransition, ObjectVersionBodyV1, ObjectVersionId,
    ObjectVersionKindV1, ObjectVersionOrder, ObjectVersionV1, ObjectWriteConditionV1, OperationId,
    OperationKind, OperationRecordV1, PhysicalVersion, ProllyObjectStore, ProtectionSink,
    PublicationLease, RandomIdSource, RefGeneration, ReflogEntryV1, RepositoryFormatV1,
    RepositoryId, RepositoryStorageProfile, Result, RetentionPinV1, RetryAdvice, StorageToken,
    StoredContent, SyncRunStateV1, SyncRunV1, SystemClock, TreeRootV1, UploadId, WorkspaceId,
    WorkspaceManifestV1, WorkspaceMutationV1, WorkspaceStateV1,
};
use futures_util::{stream::BoxStream, Stream, StreamExt};
use md5::{Digest as _, Md5};
use prolly::{AsyncProlly, Cid, Config, RuntimeConfig, Tree, TreeFormat};
use sha2::Sha256;

const MIN_NONFINAL_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024;
const MAX_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;
pub const MAX_LOGICAL_RETRY_LIMIT: u8 = 16;

#[derive(Clone)]
pub struct RepositoryOptions {
    pub repository_prefix: String,
    pub default_branch: String,
    pub writer: String,
    pub storage_profile: RepositoryStorageProfile,
    pub logical_retry_limit: u8,
    pub limits: CanonicalLimits,
    pub state_tree_format: TreeFormat,
    pub content_index_format: TreeFormat,
    pub publication_lease_millis: u64,
    /// Duration of the repository-scoped native writer lease. Renewal is
    /// amortized and is not part of an ordinary operation's foreground calls.
    pub writer_lease_millis: u64,
    /// Open without acquiring mutation authority.
    pub read_only: bool,
    pub multipart_upload_ttl_millis: u64,
    pub reflog_retention_millis: u64,
    pub history_traversal_limit: usize,
    /// Maximum exact physical deletions per second during GC. Zero disables
    /// pacing. V1 accepts 1..=1,000 when configured.
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
            storage_profile: RepositoryStorageProfile::DistributedContentAddressedV1,
            logical_retry_limit: 3,
            limits: CanonicalLimits::default(),
            state_tree_format: TreeFormat::default(),
            content_index_format: TreeFormat::default(),
            publication_lease_millis: 60 * 60 * 1_000,
            writer_lease_millis: 60_000,
            read_only: false,
            multipart_upload_ttl_millis: 7 * 24 * 60 * 60 * 1_000,
            reflog_retention_millis: 90 * 24 * 60 * 60 * 1_000,
            history_traversal_limit: 100_000,
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
pub struct MultipartUploadSummary {
    pub id: UploadId,
    pub branch: String,
    pub key: Vec<u8>,
    pub created_at_millis: u64,
    pub expires_at_millis: u64,
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
    pub deleted_versions: usize,
    pub deleted_bytes: u64,
    pub skipped_reachable: usize,
    pub already_missing: usize,
    pub complete: bool,
    pub next_index: usize,
    pub deleted_by_kind: BTreeMap<String, usize>,
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
    pub content_manifests: usize,
    pub content_bytes_verified: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairReport {
    pub sync: SyncReport,
    pub fsck: FsckReport,
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

struct LoadedUpload {
    value: MultipartUploadV1,
    token: StorageToken,
}

struct LoadedWorkspace {
    value: WorkspaceManifestV1,
    token: StorageToken,
}

struct LoadedGcRun {
    value: GcRunV1,
    token: StorageToken,
}

struct LoadedGcMarkRun {
    value: GcMarkRunV1,
    token: StorageToken,
}

struct LoadedSyncRun {
    value: SyncRunV1,
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
    content: ContentStore<P>,
    writer_lease: Arc<RwLock<Option<HeldWriterLease>>>,
    warm_branches: Arc<RwLock<BTreeMap<String, WarmBranchState>>>,
    commit_cache: Arc<RwLock<BTreeMap<CommitId, BucketCommitV1>>>,
    native_publication: Arc<tokio::sync::Mutex<()>>,
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
            content_index_format: options.content_index_format.clone(),
            canonical_limits: options.limits.clone(),
            min_reader_version: options.storage_profile.minimum_protocol_version(),
            min_writer_version: options.storage_profile.minimum_protocol_version(),
            created_at_millis,
            #[cfg(not(prolly_s3_legacy_v1_codec))]
            required_capability_profile: options.storage_profile.capability_profile(),
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
        let native_profile = options.storage_profile == RepositoryStorageProfile::NativeVersionedV1;
        let delta_id = if native_profile {
            delta.id()?
        } else {
            repository.store_delta(&delta).await?
        };
        let writer_fence_generation = repository.writer_fence_generation()?;
        let commit = BucketCommitV1 {
            state: empty_state,
            parents: Vec::new(),
            generation: CommitGeneration(0),
            delta: delta_id,
            author: options.writer.clone(),
            message: Some("initialize versioned S3 repository".to_string()),
            created_at_millis: intent.format.created_at_millis,
            metadata: BTreeMap::new(),
            native: native_profile.then(|| crate::NativeCommitExtensionV1 {
                node_pack: None,
                inline_delta: delta.clone(),
                writer_fence_generation,
            }),
        };
        let commit_id = repository.store_commit(&commit).await?;

        let reflog = ReflogEntryV1 {
            branch: options.default_branch.clone(),
            old_target: None,
            new_target: commit_id,
            operation: intent.operation,
            actor: options.writer.clone(),
            message: "initialize".to_string(),
            created_at_millis: intent.format.created_at_millis,
        };
        let reflog_id = if native_profile {
            reflog.id()?
        } else {
            repository.store_reflog(&reflog).await?
        };

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
            native: native_profile.then_some(crate::NativeRefExtensionV1 {
                writer_fence_generation,
                inline_reflog: reflog,
            }),
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
        let node_store = match format.storage_profile()? {
            RepositoryStorageProfile::DistributedContentAddressedV1 => {
                ProllyObjectStore::new(plane.clone(), options.repository_prefix.clone())
            }
            RepositoryStorageProfile::NativeVersionedV1 => {
                ProllyObjectStore::new_packed(plane.clone(), options.repository_prefix.clone())
            }
        };
        let engine = AsyncProlly::new(node_store.clone(), config);
        let content = ContentStore::new(
            plane.clone(),
            options.repository_prefix.clone(),
            format.canonical_limits.content_chunk_bytes as usize,
            format.content_index_format.clone(),
        );
        Ok(Self {
            plane,
            options,
            format,
            node_store,
            engine,
            content,
            writer_lease: Arc::new(RwLock::new(None)),
            warm_branches: Arc::new(RwLock::new(BTreeMap::new())),
            commit_cache: Arc::new(RwLock::new(BTreeMap::new())),
            native_publication: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub fn repository_id(&self) -> RepositoryId {
        self.format.repository_id
    }

    pub fn format(&self) -> &RepositoryFormatV1 {
        &self.format
    }

    pub fn storage_profile(&self) -> RepositoryStorageProfile {
        self.format
            .storage_profile()
            .expect("repository format was validated before construction")
    }

    pub fn plane(&self) -> Arc<P> {
        self.plane.clone()
    }

    /// Persist an advisory packed-node locator. It is safe to delete: cold
    /// reads and repair can deterministically rebuild it from immutable packs.
    pub async fn create_node_index_checkpoint(
        &self,
        branch: &str,
    ) -> Result<crate::NodeIndexCheckpointV1> {
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "node-index checkpoints apply only to native-versioned repositories",
            ));
        }
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
        Ok(checkpoint)
    }

    async fn load_latest_node_index_checkpoint(&self) -> Result<()> {
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            return Ok(());
        }
        let prefix = format!("{}/node-index/checkpoints/", self.options.repository_prefix);
        let mut continuation = None;
        let mut paths = Vec::new();
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
            paths.extend(page.entries.into_iter().map(|entry| entry.path));
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        paths.sort_by(|left, right| right.cmp(left));
        for path in paths {
            let Some(object) = self
                .plane
                .get(GetRequest {
                    path,
                    range: None,
                    physical_version: None,
                })
                .await?
            else {
                continue;
            };
            let Ok(checkpoint) = decode_canonical::<crate::NodeIndexCheckpointV1>(&object.bytes)
            else {
                continue;
            };
            if checkpoint.repository != self.format.repository_id || checkpoint.validate().is_err()
            {
                continue;
            }
            self.node_store.import_node_index(&checkpoint.entries)?;
            return Ok(());
        }
        Ok(())
    }

    fn now_millis(&self) -> Result<u64> {
        self.options.clock.now_millis()
    }

    async fn acquire_native_writer(&mut self) -> Result<()> {
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1
            || self.options.read_only
        {
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
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            return Ok(());
        }
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
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1
            || self.options.read_only
        {
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
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "exclusive writer takeover applies only to native-versioned repositories",
            ));
        }
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
            let observed_fence = loaded
                .value
                .native
                .as_ref()
                .map(|native| native.writer_fence_generation)
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::CorruptCommit,
                        "native branch ref is missing its writer fence",
                    )
                })?;
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
            barrier.writer = self.options.writer.clone();
            barrier.updated_at_millis = now;
            barrier.native = Some(crate::NativeRefExtensionV1 {
                writer_fence_generation: next_generation,
                inline_reflog: reflog,
            });
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
            self.renew_writer_lease().await?;
        }
        self.writer_fence_generation()
    }

    fn writer_fence_generation(&self) -> Result<u64> {
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            return Ok(0);
        }
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
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "branch-cache lock poisoned"))?
            .get(branch)
            .cloned()
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

    fn new_workspace(&self) -> WorkspaceId {
        self.options.ids.workspace()
    }

    fn new_upload(&self) -> UploadId {
        self.options.ids.upload()
    }

    /// Copy the complete portable repository into an empty object namespace.
    /// Provider attestations, probes, leases, maintenance state, uploads, and
    /// workspaces are deliberately excluded. Refs are created last through
    /// destination-local CAS so no source storage token crosses providers.
    pub async fn clone_to<Q: ObjectPlane>(
        &self,
        destination: Arc<Q>,
        destination_prefix: &str,
    ) -> Result<CloneReport> {
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
            return self.clone_native_to(destination, destination_prefix).await;
        }
        let probe_prefix = format!("{destination_prefix}/");
        ObjectPath::new(format!("{destination_prefix}/format/v1.cbor"))?;
        let mut destination_continuation = None;
        loop {
            let page = destination
                .list(ListRequest {
                    prefix: probe_prefix.clone(),
                    continuation: destination_continuation,
                    limit: 1_000,
                    include_versions: false,
                })
                .await?;
            if page.entries.iter().any(|entry| {
                entry
                    .path
                    .as_str()
                    .strip_prefix(&probe_prefix)
                    .is_some_and(is_portable_clone_path)
            }) {
                return Err(Error::new(
                    ErrorCode::RepositoryFormatConflict,
                    "clone destination portable namespace is not empty",
                ));
            }
            destination_continuation = page.continuation;
            if destination_continuation.is_none() {
                break;
            }
        }
        let mut report = CloneReport::default();
        for refs_only in [false, true] {
            let mut continuation = None;
            let source_prefix = format!("{}/", self.options.repository_prefix);
            loop {
                let page = self
                    .plane
                    .list(ListRequest {
                        prefix: source_prefix.clone(),
                        continuation,
                        limit: 1_000,
                        include_versions: false,
                    })
                    .await?;
                for listed in page.entries {
                    let relative = listed
                        .path
                        .as_str()
                        .strip_prefix(&source_prefix)
                        .ok_or_else(|| {
                            Error::new(
                                ErrorCode::InternalInvariant,
                                "clone listing escaped its source prefix",
                            )
                        })?
                        .to_string();
                    let is_ref = relative.starts_with("refs/");
                    if is_ref != refs_only || !is_portable_clone_path(&relative) {
                        continue;
                    }
                    let source = self
                        .plane
                        .get(GetRequest {
                            path: listed.path,
                            range: None,
                            physical_version: None,
                        })
                        .await?
                        .ok_or_else(|| {
                            Error::new(
                                ErrorCode::MissingClosure,
                                "clone source object disappeared during copy",
                            )
                        })?;
                    let path = ObjectPath::new(format!("{destination_prefix}/{relative}"))?;
                    if is_ref {
                        match destination
                            .compare_exchange(CompareExchange {
                                path,
                                expected: None,
                                bytes: source.bytes,
                            })
                            .await?
                        {
                            CompareExchangeOutcome::Applied(_) => report.refs += 1,
                            CompareExchangeOutcome::Conflict(_) => {
                                return Err(Error::new(
                                    ErrorCode::RefConflict,
                                    "clone destination ref was created concurrently",
                                ))
                            }
                        }
                    } else {
                        report.immutable_bytes = report
                            .immutable_bytes
                            .checked_add(source.bytes.len() as u64)
                            .ok_or_else(|| {
                                Error::new(ErrorCode::EntityTooLarge, "clone byte count overflow")
                            })?;
                        destination
                            .put_immutable(ImmutablePut {
                                path,
                                expected_sha256: crate::codec::sha256(&source.bytes),
                                bytes: source.bytes,
                            })
                            .await?;
                        report.immutable_objects += 1;
                    }
                }
                continuation = page.continuation;
                if continuation.is_none() {
                    break;
                }
            }
        }
        Ok(report)
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
                            is_portable_clone_path(relative)
                                || relative.starts_with("node-packs/")
                                || relative.starts_with("writers/")
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
        target_options.storage_profile = RepositoryStorageProfile::NativeVersionedV1;
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
                native: Some(crate::NativeRefExtensionV1 {
                    writer_fence_generation,
                    inline_reflog: reflog,
                }),
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
        match (&version.body.kind, &version.native_binding) {
            (
                ObjectVersionKindV1::Live {
                    size,
                    headers,
                    checksums,
                    user_metadata,
                    ..
                },
                Some(crate::NativeObjectBindingV1::Live {
                    version_id,
                    checksum_sha256,
                    ..
                }),
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
                ObjectVersionKindV1::DeleteMarker,
                Some(crate::NativeObjectBindingV1::DeleteMarker { .. }),
            ) => {
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
            let objects = encode_canonical(&commit.state.objects)?;
            let operations = encode_canonical(&commit.state.operations)?;
            let generation = commit.generation.0.to_be_bytes();
            let delta = commit.delta.as_bytes();
            let message = encode_canonical(&commit.message)?;
            let metadata = encode_canonical(&commit.metadata)?;
            let fingerprint = derive_input_digest(&[
                b"native-logical-commit-v1",
                &parent_bytes,
                &objects,
                &operations,
                &generation,
                delta,
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
                        Ok(existing) => (
                            existing.native_binding.ok_or_else(|| {
                                Error::new(
                                    ErrorCode::CorruptCommit,
                                    "native transfer base version omitted its binding",
                                )
                            })?,
                            false,
                        ),
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
                        ObjectVersionKindV1::Live { size, .. } => *size,
                        ObjectVersionKindV1::DeleteMarker => 0,
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
                version.native_binding = Some(binding);
                version.validate_native()?;
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
                                version: transition.next,
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
            if state.objects != source_commit.state.objects
                || state.operations != source_commit.state.operations
            {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "native transfer replay did not reproduce the logical commit state",
                ));
            }
            let node_pack = target
                .node_store
                .flush_node_pack(
                    tree_format_digest(&target.format.state_tree_format)?,
                    Vec::new(),
                )
                .await?;
            if node_pack.is_some() {
                report.copied_objects += 1;
            }
            let destination_commit = BucketCommitV1 {
                state,
                parents: mapped_parents,
                generation: source_commit.generation,
                delta: delta.id()?,
                author: source_commit.author,
                message: source_commit.message,
                created_at_millis: source_commit.created_at_millis,
                metadata: source_commit.metadata,
                native: Some(crate::NativeCommitExtensionV1 {
                    node_pack,
                    inline_delta: delta,
                    writer_fence_generation,
                }),
            };
            let destination_id = target.store_commit(&destination_commit).await?;
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
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
            let _publication = self.native_publication.lock().await;
            let source_head = source.head(source_branch).await?;
            let (mapped, mut report) = source
                .replay_native_history_to(self, &[source_head], false)
                .await?;
            report.source_head = Some(*mapped.get(&source_head).ok_or_else(|| {
                Error::new(
                    ErrorCode::MissingClosure,
                    "native fetch did not map its selected source head",
                )
            })?);
            return Ok(report);
        }
        let source_head = source.head(source_branch).await?;
        let mut report = source
            .copy_commit_closure_to(
                self.plane.clone(),
                &self.options.repository_prefix,
                source_head,
            )
            .await?;
        report.source_head = Some(source_head);
        Ok(report)
    }

    /// Copies a bounded portion of one immutable reachable closure and stores
    /// a destination-local CAS checkpoint. Repeating the same run after a
    /// crash safely rechecks already-created immutable objects and resumes
    /// after the last checkpointed relative path.
    pub async fn sync_closure_batch_to<Q: ObjectPlane>(
        &self,
        destination: &Repository<Q>,
        source_branch: &str,
        run: Option<OperationId>,
        max_objects: usize,
    ) -> Result<SyncRunV1> {
        if max_objects == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "sync checkpoint batch must contain at least one object",
            ));
        }
        self.validate_sync_identity(destination)?;
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "native resumable sync requires the logical transfer journal API",
            ));
        }
        let id = run.unwrap_or_else(|| self.new_operation());
        let path = sync_run_path(&destination.options.repository_prefix, id)?;
        let existing = destination.load_sync_run_optional(id).await?;
        let source_head = match &existing {
            Some(loaded) => loaded.value.source_head,
            None => self.head(source_branch).await?,
        };
        let mut loaded = match existing {
            Some(loaded) => loaded,
            None => {
                let initial = SyncRunV1 {
                    id,
                    repository: self.format.repository_id,
                    source_head,
                    source_branch: source_branch.to_string(),
                    after_relative_path: None,
                    generation: 0,
                    state: SyncRunStateV1::Running,
                    copied_objects: 0,
                    copied_bytes: 0,
                    already_present: 0,
                    updated_at_millis: destination.now_millis()?,
                };
                match destination
                    .plane
                    .compare_exchange(CompareExchange {
                        path: path.clone(),
                        expected: None,
                        bytes: encode_canonical(&initial)?,
                    })
                    .await
                {
                    Ok(CompareExchangeOutcome::Applied(metadata)) => LoadedSyncRun {
                        value: initial,
                        token: metadata.token,
                    },
                    Ok(CompareExchangeOutcome::Conflict(_)) => {
                        destination.load_sync_run(id).await?
                    }
                    Err(error) => {
                        if let Some(current) = destination.load_sync_run_optional(id).await? {
                            current
                        } else {
                            return Err(Error::new(
                                ErrorCode::OutcomeUnknown,
                                format!("sync checkpoint creation outcome is unknown: {error}"),
                            )
                            .retry(RetryAdvice::ReconcileOperation)
                            .operation(id.to_string()));
                        }
                    }
                }
            }
        };
        validate_sync_run(
            &loaded.value,
            id,
            self.format.repository_id,
            source_branch,
            source_head,
        )?;
        if matches!(loaded.value.state, SyncRunStateV1::Completed) {
            return Ok(loaded.value);
        }

        let source_prefix = format!("{}/", self.options.repository_prefix);
        let mut next = loaded.value.clone();
        let mut processed = 0usize;
        let mut has_more = false;
        let closure = self.commit_closure_paths(source_head).await?;
        if let Some(after) = next.after_relative_path.as_deref() {
            let checkpoint_exists = closure.iter().any(|path| {
                path.as_str()
                    .strip_prefix(&source_prefix)
                    .is_some_and(|relative| relative == after)
            });
            if !checkpoint_exists {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "sync checkpoint path is absent from its pinned closure",
                )
                .operation(id.to_string()));
            }
        }
        for source_path in closure {
            let relative = source_path
                .as_str()
                .strip_prefix(&source_prefix)
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "sync checkpoint closure escaped its source prefix",
                    )
                })?
                .to_string();
            if next
                .after_relative_path
                .as_ref()
                .is_some_and(|after| relative <= *after)
            {
                continue;
            }
            if processed == max_objects {
                has_more = true;
                break;
            }
            let source = self
                .plane
                .get(GetRequest {
                    path: source_path,
                    range: None,
                    physical_version: None,
                })
                .await?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::MissingClosure,
                        "sync checkpoint source object is missing",
                    )
                })?;
            let destination_path = ObjectPath::new(format!(
                "{}/{relative}",
                destination.options.repository_prefix
            ))?;
            match destination
                .plane
                .put_immutable(ImmutablePut {
                    path: destination_path,
                    expected_sha256: crate::codec::sha256(&source.bytes),
                    bytes: source.bytes,
                })
                .await?
            {
                ImmutablePutOutcome::Created(metadata) => {
                    next.copied_objects = next.copied_objects.checked_add(1).ok_or_else(|| {
                        Error::new(ErrorCode::EntityTooLarge, "sync object count overflow")
                    })?;
                    next.copied_bytes =
                        next.copied_bytes.checked_add(metadata.len).ok_or_else(|| {
                            Error::new(ErrorCode::EntityTooLarge, "sync byte count overflow")
                        })?;
                }
                ImmutablePutOutcome::AlreadyPresent(_) => {
                    next.already_present =
                        next.already_present.checked_add(1).ok_or_else(|| {
                            Error::new(ErrorCode::EntityTooLarge, "sync existing count overflow")
                        })?;
                }
            }
            next.after_relative_path = Some(relative);
            processed += 1;
        }
        if !has_more {
            next.state = SyncRunStateV1::Completed;
        }
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::new(ErrorCode::InternalInvariant, "sync generation overflow"))?;
        next.updated_at_millis = destination.now_millis()?;
        match destination
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
                "sync checkpoint changed concurrently",
            )
            .retry(RetryAdvice::ReloadHead)),
            Err(error) => {
                loaded = destination.load_sync_run(id).await?;
                if loaded.value == next {
                    Ok(next)
                } else {
                    Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        format!("sync checkpoint update outcome is unknown: {error}"),
                    )
                    .retry(RetryAdvice::ReconcileOperation)
                    .operation(id.to_string()))
                }
            }
        }
    }

    pub async fn sync_run(&self, id: OperationId) -> Result<SyncRunV1> {
        Ok(self.load_sync_run(id).await?.value)
    }

    /// Copy immutable objects, then move an existing destination branch only
    /// when its exact expected head still matches.
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
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
            let _publication = destination.native_publication.lock().await;
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
            return Ok(report);
        }
        let mut report = self
            .copy_commit_closure_to(
                destination.plane.clone(),
                &destination.options.repository_prefix,
                source_head,
            )
            .await?;
        let movement = destination
            .reset_branch(
                destination_branch,
                source_head,
                expected_destination,
                reason,
            )
            .await?;
        report.source_head = Some(source_head);
        report.ref_move = Some(movement);
        Ok(report)
    }

    fn validate_sync_identity<Q: ObjectPlane>(&self, other: &Repository<Q>) -> Result<()> {
        if self.storage_profile() != other.storage_profile() {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "sync cannot cross repository storage profiles",
            ));
        }
        if self.format != other.format {
            return Err(Error::new(
                ErrorCode::RepositoryFormatConflict,
                "sync requires identical repository identity and canonical format",
            ));
        }
        Ok(())
    }

    async fn copy_commit_closure_to<Q: ObjectPlane>(
        &self,
        destination: Arc<Q>,
        destination_prefix: &str,
        head: CommitId,
    ) -> Result<SyncReport> {
        let mut report = SyncReport::default();
        let source_prefix = format!("{}/", self.options.repository_prefix);
        for source_path in self.commit_closure_paths(head).await? {
            let relative = source_path
                .as_str()
                .strip_prefix(&source_prefix)
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "commit closure escaped its source prefix",
                    )
                })?
                .to_string();
            let source = self
                .plane
                .get(GetRequest {
                    path: source_path,
                    range: None,
                    physical_version: None,
                })
                .await?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::MissingClosure,
                        "sync source closure object is missing",
                    )
                })?;
            let path = ObjectPath::new(format!("{destination_prefix}/{relative}"))?;
            match destination
                .put_immutable(ImmutablePut {
                    path,
                    expected_sha256: crate::codec::sha256(&source.bytes),
                    bytes: source.bytes,
                })
                .await?
            {
                ImmutablePutOutcome::Created(metadata) => {
                    report.copied_objects += 1;
                    report.copied_bytes = report
                        .copied_bytes
                        .checked_add(metadata.len)
                        .ok_or_else(|| {
                            Error::new(ErrorCode::EntityTooLarge, "sync byte count overflow")
                        })?;
                }
                ImmutablePutOutcome::AlreadyPresent(_) => report.already_present += 1,
            }
        }
        Ok(report)
    }

    async fn commit_closure_paths(&self, head: CommitId) -> Result<BTreeSet<ObjectPath>> {
        let mut paths = BTreeSet::new();
        let mut commits = vec![head];
        let mut seen = BTreeSet::new();
        let mut state_roots = Vec::new();
        let mut content = BTreeSet::new();
        while let Some(id) = commits.pop() {
            if !seen.insert(id) {
                continue;
            }
            if seen.len() > self.options.history_traversal_limit {
                return Err(Error::new(
                    ErrorCode::HistoryLimitExceeded,
                    "sync commit closure exceeded its configured history limit",
                ));
            }
            let commit = self.load_commit(id).await?;
            paths.insert(commit_path(&self.options.repository_prefix, id)?);
            paths.insert(delta_path(&self.options.repository_prefix, commit.delta)?);
            let objects =
                self.tree_from_root(&commit.state.objects, &self.format.state_tree_format)?;
            let versions =
                self.tree_from_root(&commit.state.versions, &self.format.state_tree_format)?;
            let operations =
                self.tree_from_root(&commit.state.operations, &self.format.state_tree_format)?;
            state_roots.extend([objects, versions.clone(), operations]);
            let mut entries = self.engine.range(&versions, &[], None).await?;
            while let Some(entry) = entries.next().await {
                let (_, value) = entry?;
                let version: ObjectVersionV1 = decode_canonical(&value)?;
                if let ObjectVersionKindV1::Live {
                    content: crate::ContentRef::Chunks(reference),
                    ..
                } = version.body.kind
                {
                    content.insert(reference);
                }
            }
            commits.extend(commit.parents);
        }
        let nodes = self.engine.mark_reachable(&state_roots).await?;
        for cid in nodes.cids() {
            paths.insert(node_path(&self.options.repository_prefix, cid)?);
        }
        for reference in content {
            paths.extend(
                self.content
                    .retained_paths(&crate::ContentRef::Chunks(reference))
                    .await?,
            );
        }
        Ok(paths)
    }

    pub async fn head(&self, branch: &str) -> Result<CommitId> {
        Ok(self.load_ref(branch).await?.value.target)
    }

    pub async fn create_branch(&self, name: &str, from: CommitId) -> Result<BranchHead> {
        validate_branch(name)?;
        let commit = self.load_commit(from).await?;
        let operation = self.new_operation();
        let native_profile = self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1;
        let _native_publication = if native_profile {
            Some(self.native_publication.lock().await)
        } else {
            None
        };
        let writer_fence_generation = if native_profile {
            self.native_writer_generation_for_mutation().await?
        } else {
            0
        };
        let lease = if native_profile {
            None
        } else {
            let lease = self.publication_lease(operation).await?;
            lease.set_proposal(from).await?;
            Some(lease)
        };
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
        let reflog_id = self.store_reflog(&reflog).await?;
        let value = crate::RefValueV1 {
            target: from,
            previous_target: None,
            generation: RefGeneration(0),
            operation,
            reflog: reflog_id,
            writer: self.options.writer.clone(),
            updated_at_millis: created_at_millis,
            tombstone: false,
            native: native_profile.then_some(crate::NativeRefExtensionV1 {
                writer_fence_generation,
                inline_reflog: reflog,
            }),
        };
        if let Some(lease) = &lease {
            self.ensure_publication_allowed(lease).await?;
        }
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
                if native_profile {
                    self.cache_branch(name, value.clone(), metadata.token, commit)?;
                }
                if let Some(lease) = &lease {
                    let _ = lease.complete(from).await;
                }
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
        let native_profile = self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1;
        let _native_publication = if native_profile {
            Some(self.native_publication.lock().await)
        } else {
            None
        };
        let writer_fence_generation = if native_profile {
            self.native_writer_generation_for_mutation().await?
        } else {
            0
        };
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
        let reflog = self.store_reflog(&reflog_entry).await?;
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
            native: native_profile.then_some(crate::NativeRefExtensionV1 {
                writer_fence_generation,
                inline_reflog: reflog_entry,
            }),
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
                    .remove(name);
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
        let object = self
            .plane
            .get(GetRequest {
                path: reflog_path(&self.options.repository_prefix, branch, id)?,
                range: None,
                physical_version: None,
            })
            .await?;
        if let Some(object) = object {
            let entry: ReflogEntryV1 = decode_canonical(&object.bytes)?;
            if entry.id()? != id || entry.branch != branch {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "reflog entry identity mismatch",
                ));
            }
            return Ok(entry);
        }
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
            if let Some((_, entry)) = self
                .native_reflog_history(branch)
                .await?
                .into_iter()
                .find(|(entry_id, _)| *entry_id == id)
            {
                return Ok(entry);
            }
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
        validate_branch(branch)?;
        let prefix = format!(
            "{}/reflogs/heads/{}/",
            self.options.repository_prefix,
            hex::encode(branch.as_bytes())
        );
        let mut continuation = None;
        let mut entries = if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
            self.native_reflog_history(branch).await?
        } else {
            Vec::new()
        };
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
                        Error::new(
                            ErrorCode::MissingClosure,
                            "listed reflog entry disappeared during read",
                        )
                    })?;
                let entry: ReflogEntryV1 = decode_canonical(&object.bytes)?;
                if entry.branch != branch {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "reflog entry escaped its branch namespace",
                    ));
                }
                let id = entry.id()?;
                if !entries.iter().any(|(existing, _)| *existing == id) {
                    entries.push((id, entry));
                }
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

    async fn native_reflog_history(
        &self,
        branch: &str,
    ) -> Result<Vec<(crate::ReflogEntryId, ReflogEntryV1)>> {
        validate_branch(branch)?;
        let loaded = self.load_ref_including_tombstone(branch).await?;
        let mut entries = BTreeMap::new();
        if let Some(native) = &loaded.value.native {
            if native.inline_reflog.branch != branch
                || native.inline_reflog.id()? != loaded.value.reflog
            {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "native ref inline reflog identity mismatch",
                ));
            }
            entries.insert(loaded.value.reflog, native.inline_reflog.clone());
        }
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
        let native_profile = self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1;
        let _native_publication = if native_profile {
            Some(self.native_publication.lock().await)
        } else {
            None
        };
        self.move_ref_inner(branch, loaded, target, reason).await
    }

    async fn move_ref_inner(
        &self,
        branch: &str,
        loaded: LoadedRef,
        target: CommitId,
        reason: &str,
    ) -> Result<RefMoveReceipt> {
        let native_profile = self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1;
        let writer_fence_generation = if native_profile {
            self.native_writer_generation_for_mutation().await?
        } else {
            0
        };
        let operation = self.new_operation();
        let created_at_millis = self.now_millis()?;
        let lease = if native_profile {
            None
        } else {
            let lease = self.publication_lease(operation).await?;
            lease
                .protect(commit_path(&self.options.repository_prefix, target)?)
                .await?;
            Some(lease)
        };
        let reflog_entry = ReflogEntryV1 {
            branch: branch.to_string(),
            old_target: Some(loaded.value.target),
            new_target: target,
            operation,
            actor: self.options.writer.clone(),
            message: reason.to_string(),
            created_at_millis,
        };
        let reflog = self.store_reflog(&reflog_entry).await?;
        if let Some(lease) = &lease {
            lease
                .protect(reflog_path(
                    &self.options.repository_prefix,
                    branch,
                    reflog,
                )?)
                .await?;
        }
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
            native: native_profile.then_some(crate::NativeRefExtensionV1 {
                writer_fence_generation,
                inline_reflog: reflog_entry,
            }),
        };
        if let Some(lease) = &lease {
            lease.set_proposal(target).await?;
            self.ensure_publication_allowed(lease).await?;
        }
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
                if native_profile {
                    let commit = self.load_commit(target).await?;
                    self.cache_branch(branch, value, metadata.token, commit)?;
                }
                if let Some(lease) = &lease {
                    let _ = lease.complete(target).await;
                }
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
                    if let Some(lease) = &lease {
                        let _ = lease.complete(target).await;
                    }
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
        let operation = self.new_operation();
        let lease = self.publication_lease(operation).await?;
        lease.set_proposal(target).await?;
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
        self.ensure_publication_allowed(&lease).await?;
        let publication = self
            .plane
            .compare_exchange(CompareExchange {
                path: tag_path(&self.options.repository_prefix, name)?,
                expected: None,
                bytes: encode_canonical(&value)?,
            })
            .await;
        match publication {
            Ok(CompareExchangeOutcome::Applied(_)) => {
                let _ = lease.complete(target).await;
                Ok(Tag {
                    name: name.to_string(),
                    target,
                })
            }
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
        let operation = self.new_operation();
        let lease = self.publication_lease(operation).await?;
        lease.set_proposal(target).await?;
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
        self.ensure_publication_allowed(&lease).await?;
        match self
            .plane
            .compare_exchange(CompareExchange {
                path,
                expected,
                bytes: encode_canonical(&pin)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => {
                let _ = lease.complete(target).await;
                Ok(pin)
            }
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
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
            return self
                .put_native_bytes_checked(
                    branch,
                    key,
                    bytes,
                    headers,
                    user_metadata,
                    operation,
                    ObjectWriteConditionV1::default(),
                    ChecksumExpectation::default(),
                    None,
                )
                .await;
        }
        let lease = self.publication_lease(operation).await?;
        let stored = self
            .content
            .clone()
            .with_protection_sink(Arc::new(lease.clone()))
            .write_stream(
                futures_util::stream::once(async move { Ok::<_, std::convert::Infallible>(bytes) }),
                self.format.canonical_limits.max_object_bytes,
            )
            .await;
        let stored = match stored {
            Ok(value) => value,
            Err(error) => {
                let _ = lease.abandon().await;
                return Err(error);
            }
        };
        self.put_stored(
            branch,
            key,
            stored,
            headers,
            user_metadata,
            operation,
            Some(lease),
            ObjectWriteConditionV1::default(),
            None,
        )
        .await
    }

    /// Stage a body once; ref-conflict retries reuse its immutable content reference.
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
        self.put_stream_checked_with_retry_limit(
            branch,
            key,
            stream,
            headers,
            user_metadata,
            operation,
            condition,
            expected_checksums,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn put_stream_checked_with_retry_limit<S, B, E>(
        &self,
        branch: &str,
        key: Vec<u8>,
        stream: S,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        operation: Option<OperationId>,
        condition: ObjectWriteConditionV1,
        expected_checksums: ChecksumExpectation,
        logical_retry_limit: Option<u8>,
    ) -> Result<CommitReceipt>
    where
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]>,
        E: std::fmt::Display,
    {
        if logical_retry_limit.is_some_and(|limit| limit > MAX_LOGICAL_RETRY_LIMIT) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "logical retry limit must be at most 16",
            ));
        }
        self.validate_key(&key)?;
        let operation = operation.unwrap_or_else(|| self.new_operation());
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
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
            return self
                .put_native_file_checked(
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
                    logical_retry_limit,
                )
                .await;
        }
        let lease = self.publication_lease(operation).await?;
        let stored = self
            .content
            .clone()
            .with_protection_sink(Arc::new(lease.clone()))
            .write_stream(stream, self.format.canonical_limits.max_object_bytes)
            .await;
        let stored = match stored {
            Ok(value) => value,
            Err(error) => {
                let _ = lease.abandon().await;
                return Err(error);
            }
        };
        if let Err(error) = validate_expected_checksums(&stored, &expected_checksums) {
            let _ = lease.abandon().await;
            return Err(error);
        }
        self.put_stored(
            branch,
            key,
            stored,
            headers,
            user_metadata,
            operation,
            Some(lease),
            condition,
            logical_retry_limit,
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
        logical_retry_limit: Option<u8>,
    ) -> Result<CommitReceipt> {
        let _publication = self.native_publication.lock().await;
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
        let kind = ObjectVersionKindV1::Live {
            content: crate::ContentRef::Empty,
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
        if native.size != expected_size
            || native.checksums.sha256 != Some(expected_sha256)
            || native.checksums.md5 != Some(expected_md5)
        {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "native provider result disagrees with the uploaded object identity",
            ));
        }
        self.commit_one(
            branch,
            key,
            kind,
            Some(native.binding),
            OperationKind::Put,
            operation,
            input_digest,
            "PutObject",
            None,
            condition,
            logical_retry_limit,
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
        logical_retry_limit: Option<u8>,
    ) -> Result<CommitReceipt> {
        let _publication = self.native_publication.lock().await;
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
        let kind = ObjectVersionKindV1::Live {
            content: crate::ContentRef::Empty,
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
        if native.size != expected_size
            || native.checksums.sha256 != Some(expected_sha256)
            || native.checksums.md5 != Some(expected_md5)
        {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "native provider result disagrees with the uploaded object identity",
            ));
        }
        self.commit_one(
            branch,
            key,
            kind,
            Some(native.binding),
            OperationKind::Put,
            operation,
            input_digest,
            "PutObject",
            None,
            condition,
            logical_retry_limit,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_stored(
        &self,
        branch: &str,
        key: Vec<u8>,
        stored: StoredContent,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        operation: OperationId,
        lease: Option<PublicationLease<P>>,
        condition: ObjectWriteConditionV1,
        logical_retry_limit: Option<u8>,
    ) -> Result<CommitReceipt> {
        let lease = match lease {
            Some(value) => value,
            None => self.publication_lease(operation).await?,
        };
        let kind = ObjectVersionKindV1::Live {
            content: stored.reference,
            size: stored.size,
            logical_etag: stored.logical_etag,
            headers,
            checksums: stored.checksums,
            user_metadata,
            tags: BTreeMap::new(),
        };
        let kind_bytes = encode_canonical(&kind)?;
        let input_digest = derive_input_digest(&[
            self.format.repository_id.as_bytes(),
            branch.as_bytes(),
            b"put",
            &key,
            &kind_bytes,
            &encode_canonical(&condition)?,
        ]);
        self.commit_one(
            branch,
            key,
            kind,
            None,
            OperationKind::Put,
            operation,
            input_digest,
            "PutObject",
            Some(lease),
            condition,
            logical_retry_limit,
        )
        .await
    }

    pub fn read_content_stream(
        &self,
        reference: crate::ContentRef,
        range: Option<(u64, u64)>,
    ) -> BoxStream<'static, Result<bytes::Bytes>> {
        self.content.read_stream(reference, range)
    }

    pub fn read_version_stream(
        &self,
        key: &[u8],
        version: ObjectVersionV1,
        range: Option<(u64, u64)>,
    ) -> BoxStream<'static, Result<bytes::Bytes>> {
        let Some(crate::NativeObjectBindingV1::Live {
            version_id,
            checksum_sha256,
            ..
        }) = version.native_binding
        else {
            return match version.body.kind {
                ObjectVersionKindV1::Live { content, .. } => {
                    self.content.read_stream(content, range)
                }
                ObjectVersionKindV1::DeleteMarker => Box::pin(futures_util::stream::empty()),
            };
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

    pub async fn create_multipart_upload(
        &self,
        branch: &str,
        key: Vec<u8>,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<UploadId> {
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "native-versioned multipart requires direct provider multipart completion and VersionId capture",
            ));
        }
        validate_branch(branch)?;
        self.validate_key(&key)?;
        let id = self.new_upload();
        let now = self.now_millis()?;
        let upload = MultipartUploadV1 {
            id,
            branch: branch.to_string(),
            key,
            headers,
            user_metadata,
            parts: BTreeMap::new(),
            generation: 0,
            state: MultipartStateV1::Active,
            created_at_millis: now,
            updated_at_millis: now,
            expires_at_millis: if self.options.multipart_upload_ttl_millis == 0 {
                0
            } else {
                now.checked_add(self.options.multipart_upload_ttl_millis)
                    .ok_or_else(|| {
                        Error::new(ErrorCode::InvalidRequest, "multipart expiry overflows")
                    })?
            },
        };
        match self
            .plane
            .compare_exchange(CompareExchange {
                path: upload_path(&self.options.repository_prefix, id)?,
                expected: None,
                bytes: encode_canonical(&upload)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => Ok(id),
            CompareExchangeOutcome::Conflict(_) => {
                Err(Error::new(ErrorCode::UploadConflict, "upload ID collision"))
            }
        }
    }

    pub async fn create_native_multipart_upload(
        &self,
        branch: &str,
        key: Vec<u8>,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        operation: Option<OperationId>,
    ) -> Result<crate::NativeMultipartSessionV1> {
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "direct native multipart requires the native-versioned profile",
            ));
        }
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
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "direct native multipart requires the native-versioned profile",
            ));
        }
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
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "direct native multipart requires the native-versioned profile",
            ));
        }
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
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "direct native multipart requires the native-versioned profile",
            ));
        }
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
        let ObjectVersionKindV1::Live {
            size, checksums, ..
        } = &source.version.body.kind
        else {
            return Err(Error::new(
                ErrorCode::NoSuchKey,
                "multipart copy source is a delete marker",
            ));
        };
        let crate::NativeObjectBindingV1::Live { version_id, .. } =
            source.version.native_binding.as_ref().ok_or_else(|| {
                Error::new(
                    ErrorCode::CorruptCommit,
                    "native multipart copy source has no provider binding",
                )
            })?
        else {
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
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "direct native multipart requires the native-versioned profile",
            ));
        }
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
        let kind = ObjectVersionKindV1::Live {
            content: crate::ContentRef::Empty,
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
        let _publication = self.native_publication.lock().await;
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
        self.commit_one(
            &session.branch,
            session.key,
            kind,
            Some(completed.binding),
            OperationKind::Put,
            session.operation,
            input_digest,
            "CompleteMultipartUpload",
            None,
            ObjectWriteConditionV1::default(),
            None,
        )
        .await
    }

    pub async fn abort_native_multipart_upload(
        &self,
        session: &crate::NativeMultipartSessionV1,
    ) -> Result<()> {
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "direct native multipart requires the native-versioned profile",
            ));
        }
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

    pub async fn upload_part_stream<S, B, E>(
        &self,
        upload: UploadId,
        part_number: u32,
        stream: S,
    ) -> Result<MultipartPartV1>
    where
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]>,
        E: std::fmt::Display,
    {
        if !(1..=10_000).contains(&part_number) {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "part number must be between 1 and 10000",
            ));
        }
        let content = self
            .content
            .write_stream(stream, MAX_MULTIPART_PART_BYTES)
            .await?;
        let part = MultipartPartV1 {
            part_number,
            etag: content.logical_etag.clone(),
            content,
            updated_at_millis: self.now_millis()?,
        };
        self.store_multipart_part(upload, part).await
    }

    /// Uses an existing logical object's content as a multipart part. A full
    /// source copy reuses the immutable content reference; a byte range is
    /// streamed into a canonical new content value.
    pub async fn upload_part_copy(
        &self,
        upload: UploadId,
        part_number: u32,
        source_branch: &str,
        source_key: &[u8],
        source_version: Option<ObjectVersionId>,
        range: Option<(u64, u64)>,
    ) -> Result<MultipartPartV1> {
        if !(1..=10_000).contains(&part_number) {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "part number must be between 1 and 10000",
            ));
        }
        let (snapshot, source) = match source_version {
            Some(version) => {
                self.head_version(source_branch, source_key, version)
                    .await?
            }
            None => self.head_current_at(source_branch, source_key).await?,
        };
        let ObjectVersionKindV1::Live {
            content,
            size,
            logical_etag,
            checksums,
            ..
        } = source.version.body.kind
        else {
            return Err(Error::new(
                ErrorCode::NoSuchKey,
                "multipart copy source is a delete marker",
            ));
        };
        let source = StoredContent {
            reference: content,
            size,
            logical_etag,
            checksums,
        };
        let operation = self.new_operation();
        let lease = self.publication_lease(operation).await?;
        lease.set_proposal(snapshot).await?;
        let stored = match range {
            None if source.size <= MAX_MULTIPART_PART_BYTES => source,
            None => {
                return Err(Error::new(
                    ErrorCode::EntityTooLarge,
                    "multipart part exceeds 5 GiB",
                ));
            }
            Some((start, end)) if start <= end && end < source.size => {
                let range_len = end
                    .checked_sub(start)
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| {
                        Error::new(ErrorCode::EntityTooLarge, "multipart copy range overflow")
                    })?;
                if range_len > MAX_MULTIPART_PART_BYTES {
                    return Err(Error::new(
                        ErrorCode::EntityTooLarge,
                        "multipart copy part exceeds 5 GiB",
                    ));
                }
                self.content
                    .clone()
                    .with_protection_sink(Arc::new(lease.clone()))
                    .write_stream(
                        self.content
                            .read_stream(source.reference, Some((start, end))),
                        MAX_MULTIPART_PART_BYTES,
                    )
                    .await?
            }
            Some(_) => {
                return Err(Error::new(
                    ErrorCode::InvalidRequest,
                    "multipart copy range is not satisfiable",
                ));
            }
        };
        self.ensure_publication_allowed(&lease).await?;
        let part = MultipartPartV1 {
            part_number,
            etag: stored.logical_etag.clone(),
            content: stored,
            updated_at_millis: self.now_millis()?,
        };
        let result = self.store_multipart_part(upload, part).await;
        if result.is_ok() {
            let _ = lease.complete(snapshot).await;
        }
        result
    }

    async fn store_multipart_part(
        &self,
        upload: UploadId,
        part: MultipartPartV1,
    ) -> Result<MultipartPartV1> {
        for _ in 0..=self.options.logical_retry_limit {
            let loaded = self.load_upload(upload).await?;
            if !matches!(loaded.value.state, MultipartStateV1::Active) {
                return Err(Error::new(
                    ErrorCode::UploadConflict,
                    "multipart upload is not active",
                ));
            }
            let mut next = loaded.value;
            next.parts.insert(part.part_number, part.clone());
            next.generation = next.generation.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "upload generation overflow")
            })?;
            next.updated_at_millis = self.now_millis()?;
            let publication = self
                .plane
                .compare_exchange(CompareExchange {
                    path: upload_path(&self.options.repository_prefix, upload)?,
                    expected: Some(loaded.token),
                    bytes: encode_canonical(&next)?,
                })
                .await;
            match publication {
                Ok(CompareExchangeOutcome::Applied(_)) => return Ok(part),
                Ok(CompareExchangeOutcome::Conflict(_)) => continue,
                Err(error) => {
                    let current = self.load_upload(upload).await?;
                    if current.value.parts.get(&part.part_number) == Some(&part) {
                        return Ok(part);
                    }
                    return Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        format!("multipart part CAS outcome is unknown: {error}"),
                    )
                    .retry(RetryAdvice::Safe));
                }
            }
        }
        Err(Error::new(
            ErrorCode::UploadConflict,
            "upload changed beyond retry budget",
        )
        .retry(RetryAdvice::ReloadHead))
    }

    pub async fn list_parts(&self, upload: UploadId) -> Result<Vec<MultipartPartV1>> {
        let loaded = self.load_upload(upload).await?;
        if matches!(loaded.value.state, MultipartStateV1::Aborted) {
            return Err(Error::new(
                ErrorCode::NoSuchUpload,
                "multipart upload is aborted",
            ));
        }
        Ok(loaded.value.parts.into_values().collect())
    }

    pub async fn multipart_upload(&self, upload: UploadId) -> Result<MultipartUploadV1> {
        Ok(self.load_upload(upload).await?.value)
    }

    /// Lists active, unexpired uploads in stable `(key, upload_id)` order.
    /// The `after` tuple is exclusive and can be formed from the final item of
    /// the prior page. Completed and aborted records remain durable for
    /// idempotency but are deliberately absent from this catalog.
    pub async fn list_multipart_uploads(
        &self,
        branch: &str,
        key_prefix: &[u8],
        after: Option<(&[u8], UploadId)>,
        limit: usize,
    ) -> Result<(Vec<MultipartUploadSummary>, bool)> {
        validate_branch(branch)?;
        std::str::from_utf8(key_prefix)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "prefix is not valid UTF-8"))?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        let mut uploads = self
            .collect_multipart_uploads(branch, key_prefix, self.now_millis()?)
            .await?;
        if let Some((key, id)) = after {
            uploads.retain(|upload| (upload.key.as_slice(), upload.id) > (key, id));
        }
        let truncated = uploads.len() > limit;
        uploads.truncate(limit);
        Ok((uploads, truncated))
    }

    /// Captures the active upload catalog in an immutable, content-addressed
    /// object. This projection is used only for stable pagination; mutable
    /// upload manifests remain authoritative for upload lifecycle operations.
    pub async fn create_multipart_catalog_snapshot(
        &self,
        branch: &str,
        key_prefix: &[u8],
        expires_at_millis: u64,
    ) -> Result<MultipartCatalogSnapshotV1> {
        validate_branch(branch)?;
        std::str::from_utf8(key_prefix)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "prefix is not valid UTF-8"))?;
        let now = self.now_millis()?;
        if expires_at_millis <= now {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "multipart catalog snapshot expiry must be in the future",
            ));
        }
        let uploads = self
            .collect_multipart_uploads(branch, key_prefix, now)
            .await?;
        if uploads.len() > self.options.history_traversal_limit {
            return Err(Error::new(
                ErrorCode::HistoryLimitExceeded,
                "multipart catalog snapshot exceeds its configured entry bound",
            ));
        }
        let snapshot = MultipartCatalogSnapshotV1::derive(MultipartCatalogSnapshotBodyV1 {
            repository: self.format.repository_id,
            branch: branch.to_string(),
            key_prefix: key_prefix.to_vec(),
            created_at_millis: now,
            expires_at_millis,
            entries: uploads
                .into_iter()
                .map(|upload| MultipartCatalogEntryV1 {
                    id: upload.id,
                    key: upload.key,
                    created_at_millis: upload.created_at_millis,
                    expires_at_millis: upload.expires_at_millis,
                })
                .collect(),
        })?;
        self.store_immutable(
            multipart_catalog_snapshot_path(&self.options.repository_prefix, snapshot.id)?,
            encode_canonical(&snapshot)?,
        )
        .await?;
        Ok(snapshot)
    }

    /// Reads a stable page from an immutable multipart catalog snapshot.
    pub async fn list_multipart_catalog_snapshot(
        &self,
        id: MultipartCatalogSnapshotId,
        branch: &str,
        key_prefix: &[u8],
        expires_at_millis: u64,
        offset: u64,
        limit: usize,
    ) -> Result<(Vec<MultipartUploadSummary>, bool)> {
        let snapshot = self.load_multipart_catalog_snapshot(id).await?;
        if snapshot.body.branch != branch
            || snapshot.body.key_prefix != key_prefix
            || snapshot.body.expires_at_millis != expires_at_millis
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "multipart catalog snapshot does not match the listing request",
            ));
        }
        if snapshot.body.expires_at_millis < self.now_millis()? {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "multipart catalog snapshot is expired",
            ));
        }
        let offset = usize::try_from(offset).map_err(|_| {
            Error::new(
                ErrorCode::InvalidContinuationToken,
                "multipart catalog cursor offset is out of range",
            )
        })?;
        if offset > snapshot.body.entries.len() {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "multipart catalog cursor offset exceeds the snapshot",
            ));
        }
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        let end = offset
            .saturating_add(limit)
            .min(snapshot.body.entries.len());
        let uploads = snapshot.body.entries[offset..end]
            .iter()
            .map(|entry| MultipartUploadSummary {
                id: entry.id,
                branch: snapshot.body.branch.clone(),
                key: entry.key.clone(),
                created_at_millis: entry.created_at_millis,
                expires_at_millis: entry.expires_at_millis,
            })
            .collect();
        Ok((uploads, end < snapshot.body.entries.len()))
    }

    pub async fn load_multipart_catalog_snapshot(
        &self,
        id: MultipartCatalogSnapshotId,
    ) -> Result<MultipartCatalogSnapshotV1> {
        let object = self
            .plane
            .get(GetRequest {
                path: multipart_catalog_snapshot_path(&self.options.repository_prefix, id)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::MissingClosure,
                    "multipart catalog snapshot is missing",
                )
            })?;
        let snapshot: MultipartCatalogSnapshotV1 = decode_canonical(&object.bytes)?;
        snapshot.validate_id()?;
        if snapshot.id != id || snapshot.body.repository != self.format.repository_id {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "multipart catalog snapshot belongs to another repository",
            ));
        }
        validate_branch(&snapshot.body.branch)?;
        std::str::from_utf8(&snapshot.body.key_prefix).map_err(|_| {
            Error::new(
                ErrorCode::CorruptCommit,
                "multipart catalog snapshot prefix is not UTF-8",
            )
        })?;
        if snapshot.body.expires_at_millis <= snapshot.body.created_at_millis
            || snapshot.body.entries.len() > self.options.history_traversal_limit
            || snapshot.body.entries.iter().any(|entry| {
                !entry.key.starts_with(&snapshot.body.key_prefix)
                    || (entry.expires_at_millis != 0
                        && entry.expires_at_millis <= snapshot.body.created_at_millis)
            })
            || snapshot.body.entries.windows(2).any(|entries| {
                (entries[0].key.as_slice(), entries[0].id)
                    >= (entries[1].key.as_slice(), entries[1].id)
            })
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "multipart catalog snapshot violates its canonical invariants",
            ));
        }
        Ok(snapshot)
    }

    async fn collect_multipart_uploads(
        &self,
        branch: &str,
        key_prefix: &[u8],
        now: u64,
    ) -> Result<Vec<MultipartUploadSummary>> {
        let prefix = format!("{}/multipart/uploads/", self.options.repository_prefix);
        let mut continuation = None;
        let mut uploads = Vec::new();
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
                let Some(object) = self
                    .plane
                    .get(GetRequest {
                        path: entry.path,
                        range: None,
                        physical_version: None,
                    })
                    .await?
                else {
                    continue;
                };
                let upload: MultipartUploadV1 = decode_canonical(&object.bytes)?;
                let unexpired = upload.expires_at_millis == 0 || upload.expires_at_millis > now;
                if upload.branch == branch
                    && upload.key.starts_with(key_prefix)
                    && matches!(upload.state, MultipartStateV1::Active)
                    && unexpired
                {
                    uploads.push(MultipartUploadSummary {
                        id: upload.id,
                        branch: upload.branch,
                        key: upload.key,
                        created_at_millis: upload.created_at_millis,
                        expires_at_millis: upload.expires_at_millis,
                    });
                    if uploads.len() > self.options.history_traversal_limit {
                        return Err(Error::new(
                            ErrorCode::HistoryLimitExceeded,
                            "multipart catalog scan exceeds its configured entry bound",
                        ));
                    }
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        uploads.sort_by(|left, right| {
            (left.key.as_slice(), left.id).cmp(&(right.key.as_slice(), right.id))
        });
        Ok(uploads)
    }

    /// Atomically marks at most `limit` expired active uploads as aborted.
    /// It is safe for multiple sweepers to race: each transition is fenced by
    /// the mutable object's storage token.
    pub async fn expire_multipart_uploads(&self, limit: usize) -> Result<usize> {
        let now = self.now_millis()?;
        let prefix = format!("{}/multipart/uploads/", self.options.repository_prefix);
        let mut continuation = None;
        let mut expired = Vec::new();
        while expired.len() < limit {
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
                let Some(object) = self
                    .plane
                    .get(GetRequest {
                        path: entry.path,
                        range: None,
                        physical_version: None,
                    })
                    .await?
                else {
                    continue;
                };
                let upload: MultipartUploadV1 = decode_canonical(&object.bytes)?;
                if matches!(upload.state, MultipartStateV1::Active)
                    && upload.expires_at_millis != 0
                    && upload.expires_at_millis <= now
                {
                    expired.push(upload.id);
                    if expired.len() == limit {
                        break;
                    }
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        let mut transitioned = 0;
        for id in expired {
            let loaded = self.load_upload(id).await?;
            if !matches!(loaded.value.state, MultipartStateV1::Active)
                || loaded.value.expires_at_millis == 0
                || loaded.value.expires_at_millis > now
            {
                continue;
            }
            let mut aborted = loaded.value;
            aborted.state = MultipartStateV1::Aborted;
            aborted.generation = aborted.generation.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "upload generation overflow")
            })?;
            aborted.updated_at_millis = now;
            if matches!(
                self.plane
                    .compare_exchange(CompareExchange {
                        path: upload_path(&self.options.repository_prefix, id)?,
                        expected: Some(loaded.token),
                        bytes: encode_canonical(&aborted)?,
                    })
                    .await?,
                CompareExchangeOutcome::Applied(_)
            ) {
                transitioned += 1;
            }
        }
        Ok(transitioned)
    }

    pub async fn complete_multipart_upload(
        &self,
        upload: UploadId,
        requested: Vec<(u32, String)>,
        operation: Option<OperationId>,
    ) -> Result<CommitReceipt> {
        if requested.is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "multipart completion has no parts",
            ));
        }
        if requested.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "completed parts must be strictly increasing",
            ));
        }
        if requested.len() > 10_000 {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "multipart completion exceeds 10000 parts",
            ));
        }
        let operation = operation.unwrap_or_else(|| self.new_operation());
        let request_digest =
            derive_input_digest(&[b"complete-multipart", &encode_canonical(&requested)?]);
        let mut loaded = self.load_upload(upload).await?;
        match &loaded.value.state {
            MultipartStateV1::Completed {
                operation: prior,
                request_digest: prior_digest,
                receipt,
            } if *prior == operation && *prior_digest == request_digest => {
                return Ok(receipt.clone())
            }
            MultipartStateV1::Completed {
                operation: prior, ..
            } if *prior == operation => {
                return Err(Error::new(
                    ErrorCode::IdempotencyConflict,
                    "multipart completion operation was reused with different input",
                )
                .operation(operation.to_string()));
            }
            MultipartStateV1::Completed { .. } | MultipartStateV1::Aborted => {
                return Err(Error::new(
                    ErrorCode::NoSuchUpload,
                    "multipart upload is no longer active",
                ));
            }
            MultipartStateV1::Completing {
                operation: prior,
                request_digest: prior_digest,
            } if *prior == operation && *prior_digest != request_digest => {
                return Err(Error::new(
                    ErrorCode::IdempotencyConflict,
                    "multipart completion operation was reused with different input",
                )
                .operation(operation.to_string()));
            }
            MultipartStateV1::Completing {
                operation: prior, ..
            } if *prior != operation => {
                return Err(Error::new(
                    ErrorCode::UploadConflict,
                    "another completion owns this upload",
                ));
            }
            MultipartStateV1::Completing { .. } => {}
            MultipartStateV1::Active => {
                validate_completed_multipart_parts(
                    &loaded.value,
                    &requested,
                    self.format.canonical_limits.max_object_bytes,
                )?;
                let mut completing = loaded.value.clone();
                completing.state = MultipartStateV1::Completing {
                    operation,
                    request_digest,
                };
                completing.generation = completing.generation.checked_add(1).ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "upload generation overflow")
                })?;
                completing.updated_at_millis = self.now_millis()?;
                match self
                    .plane
                    .compare_exchange(CompareExchange {
                        path: upload_path(&self.options.repository_prefix, upload)?,
                        expected: Some(loaded.token),
                        bytes: encode_canonical(&completing)?,
                    })
                    .await?
                {
                    CompareExchangeOutcome::Applied(_) => loaded = self.load_upload(upload).await?,
                    CompareExchangeOutcome::Conflict(_) => {
                        loaded = self.load_upload(upload).await?;
                        if !matches!(loaded.value.state, MultipartStateV1::Completing { operation: prior, request_digest: prior_digest } if prior == operation && prior_digest == request_digest)
                        {
                            return Err(Error::new(
                                ErrorCode::UploadConflict,
                                "multipart completion race lost",
                            ));
                        }
                    }
                }
            }
        }
        let parts = validate_completed_multipart_parts(
            &loaded.value,
            &requested,
            self.format.canonical_limits.max_object_bytes,
        )?;
        let lease = self.publication_lease(operation).await?;
        let stored = self
            .content
            .clone()
            .with_protection_sink(Arc::new(lease.clone()))
            .compose(&parts)
            .await?;
        let receipt = self
            .put_stored(
                &loaded.value.branch,
                loaded.value.key.clone(),
                stored,
                loaded.value.headers.clone(),
                loaded.value.user_metadata.clone(),
                operation,
                Some(lease),
                ObjectWriteConditionV1::default(),
                None,
            )
            .await?;
        for _ in 0..=self.options.logical_retry_limit {
            let current = self.load_upload(upload).await?;
            if let MultipartStateV1::Completed {
                operation: prior,
                request_digest: prior_digest,
                receipt: prior_receipt,
            } = &current.value.state
            {
                if *prior == operation && *prior_digest == request_digest {
                    return Ok(prior_receipt.clone());
                }
                return Err(Error::new(
                    ErrorCode::UploadConflict,
                    "upload completed with a different request",
                ));
            }
            if !matches!(current.value.state, MultipartStateV1::Completing { operation: prior, request_digest: prior_digest } if prior == operation && prior_digest == request_digest)
            {
                return Err(Error::new(
                    ErrorCode::UploadConflict,
                    "upload state changed after publication",
                ));
            }
            let mut completed = current.value;
            completed.state = MultipartStateV1::Completed {
                operation,
                request_digest,
                receipt: receipt.clone(),
            };
            completed.generation = completed.generation.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "upload generation overflow")
            })?;
            completed.updated_at_millis = self.now_millis()?;
            let publication = self
                .plane
                .compare_exchange(CompareExchange {
                    path: upload_path(&self.options.repository_prefix, upload)?,
                    expected: Some(current.token),
                    bytes: encode_canonical(&completed)?,
                })
                .await;
            match publication {
                Ok(CompareExchangeOutcome::Applied(_)) => return Ok(receipt),
                Ok(CompareExchangeOutcome::Conflict(_)) => continue,
                Err(error) => {
                    let observed = self.load_upload(upload).await?;
                    if let MultipartStateV1::Completed {
                        operation: prior,
                        request_digest: prior_digest,
                        receipt: prior_receipt,
                    } = observed.value.state
                    {
                        if prior == operation && prior_digest == request_digest {
                            return Ok(prior_receipt);
                        }
                    }
                    return Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        format!("multipart completion marker outcome is unknown: {error}"),
                    )
                    .retry(RetryAdvice::ReconcileOperation)
                    .operation(operation.to_string()));
                }
            }
        }
        Err(Error::new(
            ErrorCode::OutcomeUnknown,
            "bucket commit succeeded but upload completion marker did not converge",
        )
        .retry(RetryAdvice::ReconcileOperation)
        .operation(operation.to_string()))
    }

    pub async fn abort_multipart_upload(&self, upload: UploadId) -> Result<()> {
        for _ in 0..=self.options.logical_retry_limit {
            let loaded = self.load_upload(upload).await?;
            match loaded.value.state {
                MultipartStateV1::Aborted => return Ok(()),
                MultipartStateV1::Active => {}
                _ => {
                    return Err(Error::new(
                        ErrorCode::UploadConflict,
                        "cannot abort an upload during or after completion",
                    ))
                }
            }
            let mut aborted = loaded.value;
            aborted.state = MultipartStateV1::Aborted;
            aborted.generation = aborted.generation.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "upload generation overflow")
            })?;
            aborted.updated_at_millis = self.now_millis()?;
            if matches!(
                self.plane
                    .compare_exchange(CompareExchange {
                        path: upload_path(&self.options.repository_prefix, upload)?,
                        expected: Some(loaded.token),
                        bytes: encode_canonical(&aborted)?,
                    })
                    .await?,
                CompareExchangeOutcome::Applied(_)
            ) {
                return Ok(());
            }
        }
        Err(Error::new(
            ErrorCode::UploadConflict,
            "upload changed beyond retry budget",
        ))
    }

    pub async fn begin_workspace(
        &self,
        branch: &str,
        message: impl Into<String>,
        expires_after_millis: u64,
    ) -> Result<WorkspaceManifestV1> {
        validate_branch(branch)?;
        let base_commit = self.head(branch).await?;
        let now = self.now_millis()?;
        let workspace = WorkspaceManifestV1 {
            id: self.new_workspace(),
            branch: branch.to_string(),
            base_commit,
            operation: self.new_operation(),
            message: message.into(),
            mutations: BTreeMap::new(),
            generation: 0,
            state: WorkspaceStateV1::Active,
            created_at_millis: now,
            updated_at_millis: now,
            expires_at_millis: now.checked_add(expires_after_millis).ok_or_else(|| {
                Error::new(ErrorCode::InvalidRequest, "workspace expiry overflow")
            })?,
        };
        match self
            .plane
            .compare_exchange(CompareExchange {
                path: workspace_path(&self.options.repository_prefix, workspace.id)?,
                expected: None,
                bytes: encode_canonical(&workspace)?,
            })
            .await?
        {
            CompareExchangeOutcome::Applied(_) => Ok(workspace),
            CompareExchangeOutcome::Conflict(_) => Err(Error::new(
                ErrorCode::WorkspaceConflict,
                "workspace ID collision",
            )),
        }
    }

    /// Create an in-memory atomic session descriptor for the native profile.
    /// Staging remains local so publishing N objects costs N payload calls plus
    /// one pack, one commit, and one ref CAS.
    pub async fn begin_native_batch(
        &self,
        branch: &str,
        message: impl Into<String>,
        expires_after_millis: u64,
    ) -> Result<WorkspaceManifestV1> {
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "ephemeral native batches require the native-versioned profile",
            ));
        }
        validate_branch(branch)?;
        let base_commit = self.warm_branch_state(branch).await?.reference.target;
        let now = self.now_millis()?;
        Ok(WorkspaceManifestV1 {
            id: self.new_workspace(),
            branch: branch.to_string(),
            base_commit,
            operation: self.new_operation(),
            message: message.into(),
            mutations: BTreeMap::new(),
            generation: 0,
            state: WorkspaceStateV1::Active,
            created_at_millis: now,
            updated_at_millis: now,
            expires_at_millis: now.checked_add(expires_after_millis).ok_or_else(|| {
                Error::new(ErrorCode::InvalidRequest, "native batch expiry overflow")
            })?,
        })
    }

    pub async fn publish_native_batch(
        &self,
        mut batch: WorkspaceManifestV1,
        mutations: Vec<crate::NativeBatchMutationV1>,
    ) -> Result<CommitReceipt> {
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "native batch publication requires the native-versioned profile",
            ));
        }
        if mutations.is_empty()
            || mutations.len() > self.format.canonical_limits.max_mutations_per_commit as usize
            || batch.expires_at_millis < self.now_millis()?
            || !matches!(batch.state, WorkspaceStateV1::Active)
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "native batch is empty, expired, closed, or exceeds the mutation limit",
            ));
        }
        for mutation in &mutations {
            self.validate_key(mutation.key())?;
        }
        let request_digest = derive_input_digest(&[
            b"native-batch",
            &encode_canonical(&mutations)?,
            batch.base_commit.as_bytes(),
        ]);
        let _publication = self.native_publication.lock().await;
        let warm = self.warm_branch_state(&batch.branch).await?;
        if warm.reference.target != batch.base_commit {
            if let Some(receipt) = self
                .reconcile_operation(&batch.branch, batch.operation, request_digest)
                .await?
            {
                return Ok(receipt);
            }
            return Err(Error::new(
                ErrorCode::WorkspaceConflict,
                "branch moved since native batch creation",
            ));
        }
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
        for mutation in mutations {
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
                    let native = self
                        .plane
                        .put_native(crate::NativePut {
                            path,
                            bytes,
                            headers: headers.clone(),
                            user_metadata: user_metadata.clone(),
                            repository: self.format.repository_id,
                            operation: batch.operation,
                            writer_fence_generation,
                        })
                        .await?;
                    batch.mutations.insert(
                        key.clone(),
                        WorkspaceMutationV1::NativePut {
                            key,
                            content: StoredContent {
                                reference: crate::ContentRef::Empty,
                                size: native.size,
                                logical_etag: native.logical_etag,
                                checksums: native.checksums,
                            },
                            headers,
                            user_metadata,
                            binding: native.binding,
                        },
                    );
                }
                crate::NativeBatchMutationV1::Delete { key } => {
                    let path = ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                        Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
                    })?)?;
                    let binding = match self
                        .plane
                        .delete_native(crate::NativeDelete {
                            path: path.clone(),
                            repository: self.format.repository_id,
                            operation: batch.operation,
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
                    batch.mutations.insert(
                        key.clone(),
                        WorkspaceMutationV1::NativeDelete { key, binding },
                    );
                }
            }
        }
        batch.state = WorkspaceStateV1::Publishing { request_digest };
        self.commit_workspace(&batch, request_digest, None).await
    }

    pub async fn resume_workspace(&self, id: WorkspaceId) -> Result<WorkspaceManifestV1> {
        let workspace = self.load_workspace(id).await?.value;
        if workspace.expires_at_millis < self.now_millis()?
            && matches!(workspace.state, WorkspaceStateV1::Active)
        {
            return Err(Error::new(
                ErrorCode::WorkspaceExpired,
                "workspace has expired",
            ));
        }
        Ok(workspace)
    }

    pub async fn workspace_put_stream<S, B, E>(
        &self,
        workspace: WorkspaceId,
        key: Vec<u8>,
        stream: S,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<WorkspaceManifestV1>
    where
        S: Stream<Item = std::result::Result<B, E>>,
        B: AsRef<[u8]>,
        E: std::fmt::Display,
    {
        self.validate_key(&key)?;
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
            let manifest = self.resume_workspace(workspace).await?;
            let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
            futures_util::pin_mut!(stream);
            let mut bytes = Vec::new();
            while let Some(next) = stream.next().await {
                let next = next.map_err(|error| {
                    Error::new(
                        ErrorCode::Transport,
                        format!("native workspace input stream failed: {error}"),
                    )
                })?;
                let next = next.as_ref();
                if bytes.len().saturating_add(next.len())
                    > self.format.canonical_limits.max_object_bytes as usize
                {
                    return Err(Error::new(
                        ErrorCode::EntityTooLarge,
                        "native workspace object exceeds the repository size limit",
                    ));
                }
                bytes.extend_from_slice(next);
            }
            let path = ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
            let native = self
                .plane
                .put_native(crate::NativePut {
                    path,
                    bytes,
                    headers: headers.clone(),
                    user_metadata: user_metadata.clone(),
                    repository: self.format.repository_id,
                    operation: manifest.operation,
                    writer_fence_generation,
                })
                .await?;
            return self
                .update_workspace_mutation(
                    workspace,
                    WorkspaceMutationV1::NativePut {
                        key,
                        content: StoredContent {
                            reference: crate::ContentRef::Empty,
                            size: native.size,
                            logical_etag: native.logical_etag,
                            checksums: native.checksums,
                        },
                        headers,
                        user_metadata,
                        binding: native.binding,
                    },
                )
                .await;
        }
        let content = self
            .content
            .write_stream(stream, self.format.canonical_limits.max_object_bytes)
            .await?;
        self.update_workspace_mutation(
            workspace,
            WorkspaceMutationV1::Put {
                key,
                content,
                headers,
                user_metadata,
            },
        )
        .await
    }

    pub async fn workspace_delete(
        &self,
        workspace: WorkspaceId,
        key: Vec<u8>,
    ) -> Result<WorkspaceManifestV1> {
        self.validate_key(&key)?;
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
            let manifest = self.resume_workspace(workspace).await?;
            let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
            let path = ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
            let binding = match self
                .plane
                .delete_native(crate::NativeDelete {
                    path: path.clone(),
                    repository: self.format.repository_id,
                    operation: manifest.operation,
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
            return self
                .update_workspace_mutation(
                    workspace,
                    WorkspaceMutationV1::NativeDelete { key, binding },
                )
                .await;
        }
        self.update_workspace_mutation(workspace, WorkspaceMutationV1::Delete { key })
            .await
    }

    async fn update_workspace_mutation(
        &self,
        workspace: WorkspaceId,
        mutation: WorkspaceMutationV1,
    ) -> Result<WorkspaceManifestV1> {
        for _ in 0..=self.options.logical_retry_limit {
            let loaded = self.load_workspace(workspace).await?;
            if loaded.value.expires_at_millis < self.now_millis()? {
                return Err(Error::new(
                    ErrorCode::WorkspaceExpired,
                    "workspace has expired",
                ));
            }
            if !matches!(loaded.value.state, WorkspaceStateV1::Active) {
                return Err(Error::new(
                    ErrorCode::WorkspaceConflict,
                    "workspace is not active",
                ));
            }
            let mut next = loaded.value;
            next.mutations
                .insert(mutation.key().to_vec(), mutation.clone());
            if next.mutations.len() > self.format.canonical_limits.max_mutations_per_commit as usize
            {
                return Err(Error::new(
                    ErrorCode::InvalidLimit,
                    "workspace mutation limit exceeded",
                ));
            }
            next.generation = next.generation.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "workspace generation overflow",
                )
            })?;
            next.updated_at_millis = self.now_millis()?;
            match self
                .plane
                .compare_exchange(CompareExchange {
                    path: workspace_path(&self.options.repository_prefix, workspace)?,
                    expected: Some(loaded.token),
                    bytes: encode_canonical(&next)?,
                })
                .await?
            {
                CompareExchangeOutcome::Applied(_) => return Ok(next),
                CompareExchangeOutcome::Conflict(_) => continue,
            }
        }
        Err(Error::new(
            ErrorCode::WorkspaceConflict,
            "workspace changed beyond retry budget",
        ))
    }

    pub async fn publish_workspace(&self, workspace: WorkspaceId) -> Result<CommitReceipt> {
        let mut loaded = self.load_workspace(workspace).await?;
        let request_digest =
            derive_input_digest(&[b"workspace", &encode_canonical(&loaded.value.mutations)?]);
        match &loaded.value.state {
            WorkspaceStateV1::Completed {
                request_digest: prior,
                receipt,
            } if *prior == request_digest => return Ok(receipt.clone()),
            WorkspaceStateV1::Completed { .. } | WorkspaceStateV1::Aborted => {
                return Err(Error::new(
                    ErrorCode::WorkspaceConflict,
                    "workspace is closed",
                ))
            }
            WorkspaceStateV1::Publishing {
                request_digest: prior,
            } if *prior != request_digest => {
                return Err(Error::new(
                    ErrorCode::WorkspaceConflict,
                    "workspace digest changed",
                ))
            }
            WorkspaceStateV1::Publishing { .. } => {}
            WorkspaceStateV1::Active => {
                if loaded.value.expires_at_millis < self.now_millis()? {
                    return Err(Error::new(
                        ErrorCode::WorkspaceExpired,
                        "workspace has expired",
                    ));
                }
                if loaded.value.mutations.is_empty() {
                    return Err(Error::new(
                        ErrorCode::InvalidRequest,
                        "workspace has no mutations",
                    ));
                }
                let mut publishing = loaded.value.clone();
                publishing.state = WorkspaceStateV1::Publishing { request_digest };
                publishing.generation = publishing.generation.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "workspace generation overflow",
                    )
                })?;
                publishing.updated_at_millis = self.now_millis()?;
                let _ = self
                    .plane
                    .compare_exchange(CompareExchange {
                        path: workspace_path(&self.options.repository_prefix, workspace)?,
                        expected: Some(loaded.token),
                        bytes: encode_canonical(&publishing)?,
                    })
                    .await?;
                loaded = self.load_workspace(workspace).await?;
            }
        }
        if !matches!(loaded.value.state, WorkspaceStateV1::Publishing { request_digest: prior } if prior == request_digest)
        {
            return Err(Error::new(
                ErrorCode::WorkspaceConflict,
                "workspace publication race lost",
            ));
        }
        let native_profile = self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1;
        let _native_publication = if native_profile {
            Some(self.native_publication.lock().await)
        } else {
            None
        };
        let lease = if native_profile {
            None
        } else {
            Some(self.publication_lease(loaded.value.operation).await?)
        };
        let receipt = self
            .commit_workspace(&loaded.value, request_digest, lease)
            .await?;
        for _ in 0..=self.options.logical_retry_limit {
            let current = self.load_workspace(workspace).await?;
            if let WorkspaceStateV1::Completed {
                request_digest: prior,
                receipt: prior_receipt,
            } = &current.value.state
            {
                if *prior == request_digest {
                    return Ok(prior_receipt.clone());
                }
                return Err(Error::new(
                    ErrorCode::WorkspaceConflict,
                    "workspace completed differently",
                ));
            }
            let mut completed = current.value;
            completed.state = WorkspaceStateV1::Completed {
                request_digest,
                receipt: receipt.clone(),
            };
            completed.generation = completed.generation.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "workspace generation overflow",
                )
            })?;
            completed.updated_at_millis = self.now_millis()?;
            if matches!(
                self.plane
                    .compare_exchange(CompareExchange {
                        path: workspace_path(&self.options.repository_prefix, workspace)?,
                        expected: Some(current.token),
                        bytes: encode_canonical(&completed)?,
                    })
                    .await?,
                CompareExchangeOutcome::Applied(_)
            ) {
                return Ok(receipt);
            }
        }
        Err(Error::new(
            ErrorCode::OutcomeUnknown,
            "workspace commit published but marker is ambiguous",
        )
        .retry(RetryAdvice::ReconcileOperation)
        .operation(loaded.value.operation.to_string()))
    }

    pub async fn abort_workspace(&self, workspace: WorkspaceId) -> Result<()> {
        for _ in 0..=self.options.logical_retry_limit {
            let loaded = self.load_workspace(workspace).await?;
            match loaded.value.state {
                WorkspaceStateV1::Aborted => return Ok(()),
                WorkspaceStateV1::Active => {}
                _ => {
                    return Err(Error::new(
                        ErrorCode::WorkspaceConflict,
                        "cannot abort publishing workspace",
                    ))
                }
            }
            let mut aborted = loaded.value;
            aborted.state = WorkspaceStateV1::Aborted;
            aborted.generation = aborted.generation.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "workspace generation overflow",
                )
            })?;
            aborted.updated_at_millis = self.now_millis()?;
            if matches!(
                self.plane
                    .compare_exchange(CompareExchange {
                        path: workspace_path(&self.options.repository_prefix, workspace)?,
                        expected: Some(loaded.token),
                        bytes: encode_canonical(&aborted)?,
                    })
                    .await?,
                CompareExchangeOutcome::Applied(_)
            ) {
                return Ok(());
            }
        }
        Err(Error::new(
            ErrorCode::WorkspaceConflict,
            "workspace changed beyond retry budget",
        ))
    }

    pub async fn delete_object(
        &self,
        branch: &str,
        key: Vec<u8>,
        operation: Option<OperationId>,
    ) -> Result<CommitReceipt> {
        self.validate_key(&key)?;
        let operation = operation.unwrap_or_else(|| self.new_operation());
        let kind = ObjectVersionKindV1::DeleteMarker;
        let input_digest = derive_input_digest(&[
            self.format.repository_id.as_bytes(),
            branch.as_bytes(),
            b"delete",
            &key,
        ]);
        let native_profile = self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1;
        let _native_publication = if native_profile {
            Some(self.native_publication.lock().await)
        } else {
            None
        };
        if native_profile {
            if let Some(receipt) = self
                .replay_warm_operation(branch, operation, input_digest)
                .await?
            {
                return Ok(receipt);
            }
        }
        let lease = if native_profile {
            None
        } else {
            Some(self.publication_lease(operation).await?)
        };
        let native_binding = if native_profile {
            let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
            let path = ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
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
                Ok(binding) => Some(binding),
                Err(error) => match self.reconcile_native_delete(&path).await? {
                    Some(binding) => Some(binding),
                    None => return Err(error),
                },
            }
        } else {
            None
        };
        self.commit_one(
            branch,
            key,
            kind,
            native_binding,
            OperationKind::Delete,
            operation,
            input_digest,
            "DeleteObject",
            lease,
            ObjectWriteConditionV1::default(),
            None,
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
        let operation = operation.unwrap_or_else(|| self.new_operation());
        let encoded_keys = encode_canonical(&keys)?;
        let input_digest = derive_input_digest(&[
            self.format.repository_id.as_bytes(),
            branch.as_bytes(),
            b"multi-delete",
            &encoded_keys,
        ]);
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
            return self
                .delete_objects_native(branch, keys, operation, input_digest)
                .await;
        }
        let lease = self.publication_lease(operation).await?;
        let engine = self.protected_engine(Arc::new(lease.clone()));
        let created_at_millis = self.now_millis()?;
        for _attempt in 0..=self.options.logical_retry_limit {
            let loaded_ref = self.load_ref(branch).await?;
            let base = self.load_commit(loaded_ref.value.target).await?;
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
                let receipt = CommitReceipt {
                    id: loaded_ref.value.target,
                    operation,
                    branch: branch.to_string(),
                    parents: base.parents,
                    changed_keys: existing.result.changed_keys,
                    object_versions: existing.result.object_versions,
                    idempotent_replay: true,
                };
                let _ = lease.complete(receipt.id).await;
                return Ok(receipt);
            }
            let generation =
                CommitGeneration(base.generation.0.checked_add(1).ok_or_else(|| {
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
                    .map(|current| current.version);
                let body = ObjectVersionBodyV1 {
                    order: ObjectVersionOrder {
                        commit_generation: generation,
                        mutation_ordinal: u32::try_from(ordinal).map_err(|_| {
                            Error::new(ErrorCode::InvalidLimit, "mutation ordinal overflow")
                        })?,
                    },
                    created_at_millis,
                    kind: ObjectVersionKindV1::DeleteMarker,
                };
                let version =
                    ObjectVersionV1::derive(self.format.repository_id, key, operation, body)?;
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
            let delta_id = self
                .store_delta(&BucketDeltaV1 {
                    operation_ids: vec![operation],
                    changes: transitions,
                })
                .await?;
            lease
                .protect(delta_path(&self.options.repository_prefix, delta_id)?)
                .await?;
            let commit = BucketCommitV1 {
                state: BucketStateV1 {
                    objects: TreeRootV1::from_tree(&objects)?,
                    versions: TreeRootV1::from_tree(&versions)?,
                    operations: TreeRootV1::from_tree(&operations)?,
                },
                parents: vec![loaded_ref.value.target],
                generation,
                delta: delta_id,
                author: self.options.writer.clone(),
                message: Some("DeleteObjects".to_string()),
                created_at_millis,
                metadata: BTreeMap::new(),
                native: None,
            };
            let commit_id = self.store_commit(&commit).await?;
            lease
                .protect(commit_path(&self.options.repository_prefix, commit_id)?)
                .await?;
            lease.set_proposal(commit_id).await?;
            let reflog_id = self
                .store_reflog(&ReflogEntryV1 {
                    branch: branch.to_string(),
                    old_target: Some(loaded_ref.value.target),
                    new_target: commit_id,
                    operation,
                    actor: self.options.writer.clone(),
                    message: "DeleteObjects".to_string(),
                    created_at_millis,
                })
                .await?;
            lease
                .protect(reflog_path(
                    &self.options.repository_prefix,
                    branch,
                    reflog_id,
                )?)
                .await?;
            let next_ref = crate::RefValueV1 {
                target: commit_id,
                previous_target: Some(loaded_ref.value.target),
                generation: RefGeneration(
                    loaded_ref
                        .value
                        .generation
                        .0
                        .checked_add(1)
                        .ok_or_else(|| {
                            Error::new(ErrorCode::InternalInvariant, "ref generation overflow")
                        })?,
                ),
                operation,
                reflog: reflog_id,
                writer: self.options.writer.clone(),
                updated_at_millis: created_at_millis,
                tombstone: false,
                native: None,
            };
            self.ensure_publication_allowed(&lease).await?;
            let publication = self
                .plane
                .compare_exchange(CompareExchange {
                    path: branch_path(&self.options.repository_prefix, branch)?,
                    expected: Some(loaded_ref.token),
                    bytes: encode_canonical(&next_ref)?,
                })
                .await;
            match publication {
                Ok(CompareExchangeOutcome::Applied(_)) => {
                    let receipt = CommitReceipt {
                        id: commit_id,
                        operation,
                        branch: branch.to_string(),
                        parents: commit.parents.clone(),
                        changed_keys: keys.len() as u64,
                        object_versions,
                        idempotent_replay: false,
                    };
                    let _ = lease.complete(commit_id).await;
                    return Ok(receipt);
                }
                Ok(CompareExchangeOutcome::Conflict(_)) => continue,
                Err(error) => {
                    if let Some(receipt) = self
                        .reconcile_operation(branch, operation, input_digest)
                        .await?
                    {
                        let _ = lease.complete(receipt.id).await;
                        return Ok(receipt);
                    }
                    return Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        format!("branch publication outcome is unknown: {error}"),
                    )
                    .retry(RetryAdvice::ReconcileOperation)
                    .operation(operation.to_string()));
                }
            }
        }
        Err(Error::new(
            ErrorCode::RefConflict,
            "branch moved beyond the logical retry budget",
        )
        .retry(RetryAdvice::ReloadHead)
        .operation(operation.to_string()))
    }

    async fn delete_objects_native(
        &self,
        branch: &str,
        keys: Vec<Vec<u8>>,
        operation: OperationId,
        input_digest: [u8; 32],
    ) -> Result<CommitReceipt> {
        let _publication = self.native_publication.lock().await;
        let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
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
                .map(|current| current.version);
            let path = ObjectPath::new(std::str::from_utf8(key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
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
            let body = ObjectVersionBodyV1 {
                order: ObjectVersionOrder {
                    commit_generation: generation,
                    mutation_ordinal: u32::try_from(ordinal).map_err(|_| {
                        Error::new(ErrorCode::InvalidLimit, "mutation ordinal overflow")
                    })?,
                },
                created_at_millis,
                kind: ObjectVersionKindV1::DeleteMarker,
            };
            let version = ObjectVersionV1::derive_native(
                self.format.repository_id,
                key,
                operation,
                body,
                binding,
            )?;
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
        let node_pack = self
            .node_store
            .flush_node_pack(
                tree_format_digest(&self.format.state_tree_format)?,
                Vec::new(),
            )
            .await?;
        let commit = BucketCommitV1 {
            state: BucketStateV1 {
                objects: TreeRootV1::from_tree(&objects)?,
                versions: TreeRootV1::from_tree(&versions)?,
                operations: TreeRootV1::from_tree(&operations)?,
            },
            parents: vec![loaded_ref.value.target],
            generation,
            delta: delta.id()?,
            author: self.options.writer.clone(),
            message: Some("DeleteObjects".to_string()),
            created_at_millis,
            metadata: BTreeMap::new(),
            native: Some(crate::NativeCommitExtensionV1 {
                node_pack,
                inline_delta: delta,
                writer_fence_generation,
            }),
        };
        let commit_id = self.store_commit(&commit).await?;
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
            native: Some(crate::NativeRefExtensionV1 {
                writer_fence_generation,
                inline_reflog: reflog,
            }),
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
        let ObjectVersionKindV1::Live { .. } = &source.version.body.kind else {
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
        let native_profile = self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1;
        let _native_publication = if native_profile {
            Some(self.native_publication.lock().await)
        } else {
            None
        };
        if native_profile {
            if let Some(receipt) = self
                .replay_warm_operation(branch, operation, input_digest)
                .await?
            {
                return Ok(receipt);
            }
        }
        let lease = if native_profile {
            None
        } else {
            Some(self.publication_lease(operation).await?)
        };
        let native_binding = if native_profile {
            let writer_fence_generation = self.native_writer_generation_for_mutation().await?;
            let crate::NativeObjectBindingV1::Live {
                version_id,
                checksum_sha256,
                ..
            } = source.version.native_binding.clone().ok_or_else(|| {
                Error::new(
                    ErrorCode::CorruptCommit,
                    "native copy source is missing its physical binding",
                )
            })?
            else {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "native copy source points to a delete marker",
                ));
            };
            let ObjectVersionKindV1::Live {
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
            let source_path = ObjectPath::new(std::str::from_utf8(source_key).map_err(|_| {
                Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
            })?)?;
            let destination_path =
                ObjectPath::new(std::str::from_utf8(&destination_key).map_err(|_| {
                    Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
                })?)?;
            match self
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
                Ok(result) => Some(result.binding),
                Err(error) => match self
                    .reconcile_native_payload(&destination_path, operation, checksum_sha256)
                    .await?
                {
                    Some(result) => Some(result.binding),
                    None => return Err(error),
                },
            }
        } else {
            None
        };
        self.commit_one(
            branch,
            destination_key,
            kind,
            native_binding,
            OperationKind::Copy,
            operation,
            input_digest,
            "CopyObject",
            lease,
            ObjectWriteConditionV1::default(),
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_one(
        &self,
        branch: &str,
        key: Vec<u8>,
        kind: ObjectVersionKindV1,
        native_binding: Option<crate::NativeObjectBindingV1>,
        operation_kind: OperationKind,
        operation: OperationId,
        input_digest: [u8; 32],
        message: &str,
        lease: Option<PublicationLease<P>>,
        condition: ObjectWriteConditionV1,
        logical_retry_limit: Option<u8>,
    ) -> Result<CommitReceipt> {
        validate_branch(branch)?;
        let native_profile = self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1;
        match (self.storage_profile(), native_binding.as_ref()) {
            (RepositoryStorageProfile::DistributedContentAddressedV1, None)
            | (RepositoryStorageProfile::NativeVersionedV1, Some(_)) => {}
            (RepositoryStorageProfile::DistributedContentAddressedV1, Some(_)) => {
                return Err(Error::new(
                    ErrorCode::InternalInvariant,
                    "distributed-profile publication received a native binding",
                ))
            }
            (RepositoryStorageProfile::NativeVersionedV1, None) => {
                return Err(Error::new(
                    ErrorCode::InternalInvariant,
                    "native-profile publication omitted its native binding",
                ))
            }
        }
        let created_at_millis = self.now_millis()?;
        let writer_fence_generation = if native_profile {
            self.native_writer_generation_for_mutation().await?
        } else {
            0
        };
        let engine = match lease.as_ref() {
            Some(lease) => self.protected_engine(Arc::new(lease.clone())),
            None => AsyncProlly::new(
                self.node_store.clone(),
                Config {
                    format: self.format.state_tree_format.clone(),
                    runtime: RuntimeConfig::default(),
                },
            ),
        };
        let retry_limit = if native_profile {
            0
        } else {
            effective_logical_retry_limit(self.options.logical_retry_limit, logical_retry_limit)
        };
        for _attempt in 0..=retry_limit {
            let (loaded_ref, base) = if native_profile {
                let warm = self.warm_branch_state(branch).await?;
                (
                    LoadedRef {
                        value: warm.reference,
                        token: warm.token,
                    },
                    warm.commit,
                )
            } else {
                let loaded = self.load_ref(branch).await?;
                let base = self.load_commit(loaded.value.target).await?;
                (loaded, base)
            };
            if condition
                .expected_head
                .is_some_and(|expected| expected != loaded_ref.value.target)
            {
                return Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "branch head does not match the atomic write expectation",
                ));
            }
            let objects =
                self.tree_from_root(&base.state.objects, &self.format.state_tree_format)?;
            let versions =
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
                let receipt = CommitReceipt {
                    id: loaded_ref.value.target,
                    operation,
                    branch: branch.to_string(),
                    parents: base.parents,
                    changed_keys: existing.result.changed_keys,
                    object_versions: existing.result.object_versions,
                    idempotent_replay: true,
                };
                if let Some(lease) = &lease {
                    let _ = lease.complete(receipt.id).await;
                }
                return Ok(receipt);
            }

            let generation =
                CommitGeneration(base.generation.0.checked_add(1).ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "commit generation overflow")
                })?);
            let previous_current = engine
                .get(&objects, &key)
                .await?
                .map(|bytes| decode_canonical::<CurrentObjectV1>(&bytes))
                .transpose()?;
            let current_etag = match previous_current.as_ref() {
                Some(current) => {
                    let version = self.find_version(&base, &key, current.version).await?;
                    match version.body.kind {
                        ObjectVersionKindV1::Live { logical_etag, .. } => Some(logical_etag),
                        ObjectVersionKindV1::DeleteMarker => None,
                    }
                }
                None => None,
            };
            validate_write_condition(&condition, current_etag.as_deref())?;
            let previous = previous_current.map(|current| current.version);
            let body = ObjectVersionBodyV1 {
                order: ObjectVersionOrder {
                    commit_generation: generation,
                    mutation_ordinal: 0,
                },
                created_at_millis,
                kind: kind.clone(),
            };
            let version = match native_binding.clone() {
                Some(binding) => ObjectVersionV1::derive_native(
                    self.format.repository_id,
                    &key,
                    operation,
                    body,
                    binding,
                )?,
                None => ObjectVersionV1::derive(self.format.repository_id, &key, operation, body)?,
            };
            let version_key = version_tree_key(&key, version.body.order, version.id);

            let objects = match &version.body.kind {
                ObjectVersionKindV1::Live { .. } => {
                    engine
                        .put(
                            &objects,
                            key.clone(),
                            encode_canonical(&CurrentObjectV1 {
                                version: version.id,
                            })?,
                        )
                        .await?
                }
                ObjectVersionKindV1::DeleteMarker => engine.delete(&objects, &key).await?,
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
                    delete_marker: matches!(version.body.kind, ObjectVersionKindV1::DeleteMarker),
                }],
            };
            let delta_id = if native_profile {
                delta.id()?
            } else {
                let id = self.store_delta(&delta).await?;
                lease
                    .as_ref()
                    .expect("distributed publication has a lease")
                    .protect(delta_path(&self.options.repository_prefix, id)?)
                    .await?;
                id
            };
            let node_pack = if native_profile {
                self.node_store
                    .flush_node_pack(
                        tree_format_digest(&self.format.state_tree_format)?,
                        Vec::new(),
                    )
                    .await?
            } else {
                None
            };
            let commit = BucketCommitV1 {
                state,
                parents: vec![loaded_ref.value.target],
                generation,
                delta: delta_id,
                author: self.options.writer.clone(),
                message: Some(message.to_string()),
                created_at_millis,
                metadata: BTreeMap::new(),
                native: native_profile.then(|| crate::NativeCommitExtensionV1 {
                    node_pack,
                    inline_delta: delta.clone(),
                    writer_fence_generation,
                }),
            };
            let commit_id = self.store_commit(&commit).await?;
            if let Some(lease) = &lease {
                lease
                    .protect(commit_path(&self.options.repository_prefix, commit_id)?)
                    .await?;
                lease.set_proposal(commit_id).await?;
            }
            let reflog = ReflogEntryV1 {
                branch: branch.to_string(),
                old_target: Some(loaded_ref.value.target),
                new_target: commit_id,
                operation,
                actor: self.options.writer.clone(),
                message: message.to_string(),
                created_at_millis,
            };
            let reflog_id = if native_profile {
                reflog.id()?
            } else {
                let id = self.store_reflog(&reflog).await?;
                lease
                    .as_ref()
                    .expect("distributed publication has a lease")
                    .protect(reflog_path(&self.options.repository_prefix, branch, id)?)
                    .await?;
                id
            };
            let next_ref = crate::RefValueV1 {
                target: commit_id,
                previous_target: Some(loaded_ref.value.target),
                generation: RefGeneration(
                    loaded_ref
                        .value
                        .generation
                        .0
                        .checked_add(1)
                        .ok_or_else(|| {
                            Error::new(ErrorCode::InternalInvariant, "ref generation overflow")
                        })?,
                ),
                operation,
                reflog: reflog_id,
                writer: self.options.writer.clone(),
                updated_at_millis: created_at_millis,
                tombstone: false,
                native: native_profile.then_some(crate::NativeRefExtensionV1 {
                    writer_fence_generation,
                    inline_reflog: reflog,
                }),
            };
            if native_profile {
                let current_fence = self.native_writer_generation_for_mutation().await?;
                if current_fence != writer_fence_generation {
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "native writer fence changed during publication",
                    ));
                }
            } else {
                self.ensure_publication_allowed(
                    lease.as_ref().expect("distributed publication has a lease"),
                )
                .await?;
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
                    let receipt = CommitReceipt {
                        id: commit_id,
                        operation,
                        branch: branch.to_string(),
                        parents: commit.parents.clone(),
                        changed_keys: 1,
                        object_versions: operation_result.object_versions,
                        idempotent_replay: false,
                    };
                    if native_profile {
                        self.cache_branch(branch, next_ref, metadata.token, commit.clone())?;
                    }
                    if let Some(lease) = &lease {
                        let _ = lease.complete(commit_id).await;
                    }
                    return Ok(receipt);
                }
                Ok(CompareExchangeOutcome::Conflict(_)) if native_profile => {
                    self.warm_branches
                        .write()
                        .map_err(|_| {
                            Error::new(ErrorCode::InternalInvariant, "branch-cache lock poisoned")
                        })?
                        .remove(branch);
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "native branch CAS conflicted; writer is fenced and must reopen",
                    ));
                }
                Ok(CompareExchangeOutcome::Conflict(_)) => continue,
                Err(error) => {
                    if let Some(receipt) = self
                        .reconcile_operation(branch, operation, input_digest)
                        .await?
                    {
                        if let Some(lease) = &lease {
                            let _ = lease.complete(receipt.id).await;
                        }
                        return Ok(receipt);
                    }
                    return Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        format!("branch publication outcome is unknown: {error}"),
                    )
                    .retry(RetryAdvice::ReconcileOperation)
                    .operation(operation.to_string()));
                }
            }
        }
        Err(Error::new(
            ErrorCode::RefConflict,
            "branch moved beyond the logical retry budget",
        )
        .retry(RetryAdvice::ReloadHead)
        .operation(operation.to_string()))
    }

    async fn commit_workspace(
        &self,
        workspace: &WorkspaceManifestV1,
        input_digest: [u8; 32],
        lease: Option<PublicationLease<P>>,
    ) -> Result<CommitReceipt> {
        let native_profile = self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1;
        let (loaded_ref, base) = if native_profile {
            let warm = self.warm_branch_state(&workspace.branch).await?;
            (
                LoadedRef {
                    value: warm.reference,
                    token: warm.token,
                },
                warm.commit,
            )
        } else {
            let loaded = self.load_ref(&workspace.branch).await?;
            let base = self.load_commit(workspace.base_commit).await?;
            (loaded, base)
        };
        if loaded_ref.value.target != workspace.base_commit {
            if let Some(receipt) = self
                .reconcile_operation(&workspace.branch, workspace.operation, input_digest)
                .await?
            {
                if let Some(lease) = &lease {
                    let _ = lease.complete(receipt.id).await;
                }
                return Ok(receipt);
            }
            return Err(Error::new(
                ErrorCode::WorkspaceConflict,
                "branch moved since workspace creation",
            ));
        }
        let writer_fence_generation = if native_profile {
            self.native_writer_generation_for_mutation().await?
        } else {
            0
        };
        let engine = match &lease {
            Some(lease) => self.protected_engine(Arc::new(lease.clone())),
            None => AsyncProlly::new(
                self.node_store.clone(),
                Config {
                    format: self.format.state_tree_format.clone(),
                    runtime: RuntimeConfig::default(),
                },
            ),
        };
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
        let mut transitions = Vec::with_capacity(workspace.mutations.len());
        let mut version_ids = Vec::with_capacity(workspace.mutations.len());
        for (ordinal, mutation) in workspace.mutations.values().enumerate() {
            let key = mutation.key();
            let previous = engine
                .get(&objects, key)
                .await?
                .map(|bytes| decode_canonical::<CurrentObjectV1>(&bytes))
                .transpose()?
                .map(|current| current.version);
            let (kind, native_binding) = match mutation {
                WorkspaceMutationV1::Put {
                    content,
                    headers,
                    user_metadata,
                    ..
                } => (
                    ObjectVersionKindV1::Live {
                        content: content.reference.clone(),
                        size: content.size,
                        logical_etag: content.logical_etag.clone(),
                        headers: headers.clone(),
                        checksums: content.checksums.clone(),
                        user_metadata: user_metadata.clone(),
                        tags: BTreeMap::new(),
                    },
                    None,
                ),
                WorkspaceMutationV1::Delete { .. } => (ObjectVersionKindV1::DeleteMarker, None),
                WorkspaceMutationV1::NativePut {
                    content,
                    headers,
                    user_metadata,
                    binding,
                    ..
                } => (
                    ObjectVersionKindV1::Live {
                        content: crate::ContentRef::Empty,
                        size: content.size,
                        logical_etag: content.logical_etag.clone(),
                        headers: headers.clone(),
                        checksums: content.checksums.clone(),
                        user_metadata: user_metadata.clone(),
                        tags: BTreeMap::new(),
                    },
                    Some(binding.clone()),
                ),
                WorkspaceMutationV1::NativeDelete { binding, .. } => {
                    (ObjectVersionKindV1::DeleteMarker, Some(binding.clone()))
                }
            };
            let body = ObjectVersionBodyV1 {
                order: ObjectVersionOrder {
                    commit_generation: generation,
                    mutation_ordinal: u32::try_from(ordinal).map_err(|_| {
                        Error::new(ErrorCode::InvalidLimit, "workspace ordinal overflow")
                    })?,
                },
                created_at_millis: now,
                kind,
            };
            let version = match (native_profile, native_binding) {
                (true, Some(binding)) => ObjectVersionV1::derive_native(
                    self.format.repository_id,
                    key,
                    workspace.operation,
                    body,
                    binding,
                )?,
                (false, None) => ObjectVersionV1::derive(
                    self.format.repository_id,
                    key,
                    workspace.operation,
                    body,
                )?,
                _ => {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "workspace mutation does not match the repository storage profile",
                    ))
                }
            };
            objects = if matches!(version.body.kind, ObjectVersionKindV1::DeleteMarker) {
                engine.delete(&objects, key).await?
            } else {
                engine
                    .put(
                        &objects,
                        key.to_vec(),
                        encode_canonical(&CurrentObjectV1 {
                            version: version.id,
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
                delete_marker: matches!(version.body.kind, ObjectVersionKindV1::DeleteMarker),
            });
            version_ids.push(version.id);
        }
        let result = CanonicalOperationResult {
            kind: OperationKind::CommitSession,
            object_versions: version_ids.clone(),
            changed_keys: workspace.mutations.len() as u64,
        };
        let operations = engine
            .put(
                &operations,
                workspace.operation.as_bytes().to_vec(),
                encode_canonical(&OperationRecordV1 {
                    input_digest,
                    result: result.clone(),
                    commit_generation: generation,
                    created_at_millis: now,
                })?,
            )
            .await?;
        if native_profile {
            let delta = BucketDeltaV1 {
                operation_ids: vec![workspace.operation],
                changes: transitions.clone(),
            };
            let node_pack = self
                .node_store
                .flush_node_pack(
                    tree_format_digest(&self.format.state_tree_format)?,
                    Vec::new(),
                )
                .await?;
            let commit = BucketCommitV1 {
                state: BucketStateV1 {
                    objects: TreeRootV1::from_tree(&objects)?,
                    versions: TreeRootV1::from_tree(&versions)?,
                    operations: TreeRootV1::from_tree(&operations)?,
                },
                parents: vec![workspace.base_commit],
                generation,
                delta: delta.id()?,
                author: self.options.writer.clone(),
                message: Some(workspace.message.clone()),
                created_at_millis: now,
                metadata: BTreeMap::new(),
                native: Some(crate::NativeCommitExtensionV1 {
                    node_pack,
                    inline_delta: delta,
                    writer_fence_generation,
                }),
            };
            return self
                .publish_prepared_commit(
                    &workspace.branch,
                    loaded_ref,
                    workspace.operation,
                    commit,
                    result,
                    None,
                    &workspace.message,
                )
                .await;
        }
        let lease = lease.expect("distributed workspace has a publication lease");
        let delta = self
            .store_delta(&BucketDeltaV1 {
                operation_ids: vec![workspace.operation],
                changes: transitions,
            })
            .await?;
        lease
            .protect(delta_path(&self.options.repository_prefix, delta)?)
            .await?;
        let commit = BucketCommitV1 {
            state: BucketStateV1 {
                objects: TreeRootV1::from_tree(&objects)?,
                versions: TreeRootV1::from_tree(&versions)?,
                operations: TreeRootV1::from_tree(&operations)?,
            },
            parents: vec![workspace.base_commit],
            generation,
            delta,
            author: self.options.writer.clone(),
            message: Some(workspace.message.clone()),
            created_at_millis: now,
            metadata: BTreeMap::new(),
            native: None,
        };
        let commit_id = self.store_commit(&commit).await?;
        lease
            .protect(commit_path(&self.options.repository_prefix, commit_id)?)
            .await?;
        lease.set_proposal(commit_id).await?;
        let reflog = self
            .store_reflog(&ReflogEntryV1 {
                branch: workspace.branch.clone(),
                old_target: Some(workspace.base_commit),
                new_target: commit_id,
                operation: workspace.operation,
                actor: self.options.writer.clone(),
                message: workspace.message.clone(),
                created_at_millis: now,
            })
            .await?;
        lease
            .protect(reflog_path(
                &self.options.repository_prefix,
                &workspace.branch,
                reflog,
            )?)
            .await?;
        let next_ref = crate::RefValueV1 {
            target: commit_id,
            previous_target: Some(workspace.base_commit),
            generation: RefGeneration(loaded_ref.value.generation.0.checked_add(1).ok_or_else(
                || Error::new(ErrorCode::InternalInvariant, "ref generation overflow"),
            )?),
            operation: workspace.operation,
            reflog,
            writer: self.options.writer.clone(),
            updated_at_millis: now,
            tombstone: false,
            native: None,
        };
        self.ensure_publication_allowed(&lease).await?;
        let publication = self
            .plane
            .compare_exchange(CompareExchange {
                path: branch_path(&self.options.repository_prefix, &workspace.branch)?,
                expected: Some(loaded_ref.token),
                bytes: encode_canonical(&next_ref)?,
            })
            .await;
        match publication {
            Ok(CompareExchangeOutcome::Applied(_)) => {
                let receipt = CommitReceipt {
                    id: commit_id,
                    operation: workspace.operation,
                    branch: workspace.branch.clone(),
                    parents: commit.parents,
                    changed_keys: workspace.mutations.len() as u64,
                    object_versions: version_ids,
                    idempotent_replay: false,
                };
                let _ = lease.complete(commit_id).await;
                Ok(receipt)
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => {
                if let Some(receipt) = self
                    .reconcile_operation(&workspace.branch, workspace.operation, input_digest)
                    .await?
                {
                    let _ = lease.complete(receipt.id).await;
                    Ok(receipt)
                } else {
                    Err(Error::new(
                        ErrorCode::WorkspaceConflict,
                        "branch moved during workspace publication",
                    ))
                }
            }
            Err(error) => {
                if let Some(receipt) = self
                    .reconcile_operation(&workspace.branch, workspace.operation, input_digest)
                    .await?
                {
                    let _ = lease.complete(receipt.id).await;
                    Ok(receipt)
                } else {
                    Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        format!("workspace branch publication outcome is unknown: {error}"),
                    )
                    .retry(RetryAdvice::ReconcileOperation)
                    .operation(workspace.operation.to_string()))
                }
            }
        }
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
    /// required and a matching lease is terminalized when present.
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
                    if crate::load_publication_lease(
                        self.plane.as_ref(),
                        &self.options.repository_prefix,
                        operation,
                    )
                    .await?
                    .is_some()
                    {
                        let lease = self.publication_lease(operation).await?;
                        let _ = lease.complete(id).await;
                    }
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
        let version = self.find_version(&commit, key, current.version).await?;
        Ok(ObjectSummary {
            key: key.to_vec(),
            version,
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
            self.find_version(&commit, key, current.version).await?
        };
        let bytes = match (&version.body.kind, &version.native_binding) {
            (
                ObjectVersionKindV1::Live { .. },
                Some(crate::NativeObjectBindingV1::Live {
                    version_id,
                    checksum_sha256,
                    ..
                }),
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
            (ObjectVersionKindV1::Live { content, .. }, None) => {
                self.content.read_all(content).await?
            }
            (ObjectVersionKindV1::Live { .. }, Some(_)) => {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "live logical object has a delete-marker native binding",
                ))
            }
            (ObjectVersionKindV1::DeleteMarker, _) if selected.is_some() => Vec::new(),
            (ObjectVersionKindV1::DeleteMarker, _) => {
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
            let version = self.find_version(&commit, &key, current.version).await?;
            result.push(ObjectSummary { key, version });
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
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1 {
            if let Some(receipt) = self
                .reconcile_operation(target, operation, input_digest)
                .await?
            {
                return Ok(receipt);
            }
        }
        let native_profile = self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1;
        let _native_publication = if native_profile {
            Some(self.native_publication.lock().await)
        } else {
            None
        };
        let writer_fence_generation = if native_profile {
            self.native_writer_generation_for_mutation().await?
        } else {
            0
        };
        let lease = if native_profile {
            None
        } else {
            Some(self.publication_lease(operation).await?)
        };
        let engine = match &lease {
            Some(lease) => self.protected_engine(Arc::new(lease.clone())),
            None => AsyncProlly::new(
                self.node_store.clone(),
                Config {
                    format: self.format.state_tree_format.clone(),
                    runtime: RuntimeConfig::default(),
                },
            ),
        };
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
                            encode_canonical(&CurrentObjectV1 { version })?,
                        )
                        .await?;
                    version
                }
                None => {
                    objects = engine.delete(&objects, &change.key).await?;
                    let body = ObjectVersionBodyV1 {
                        order: ObjectVersionOrder {
                            commit_generation: generation,
                            mutation_ordinal: u32::try_from(ordinal).map_err(|_| {
                                Error::new(ErrorCode::InvalidLimit, "merge ordinal overflow")
                            })?,
                        },
                        created_at_millis,
                        kind: ObjectVersionKindV1::DeleteMarker,
                    };
                    let version = if native_profile {
                        let binding = match self
                            .latest_native_delete_binding(&theirs_commit, &change.key)
                            .await?
                        {
                            Some(binding) => binding,
                            None => {
                                let path = ObjectPath::new(
                                    std::str::from_utf8(&change.key).map_err(|_| {
                                        Error::new(
                                            ErrorCode::InvalidKey,
                                            "logical key is not valid UTF-8",
                                        )
                                    })?,
                                )?;
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
                                    Err(error) => {
                                        match self.reconcile_native_delete(&path).await? {
                                            Some(binding) => binding,
                                            None => return Err(error),
                                        }
                                    }
                                }
                            }
                        };
                        ObjectVersionV1::derive_native(
                            self.format.repository_id,
                            &change.key,
                            operation,
                            body,
                            binding,
                        )?
                    } else {
                        ObjectVersionV1::derive(
                            self.format.repository_id,
                            &change.key,
                            operation,
                            body,
                        )?
                    };
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
        let delta_id = if native_profile {
            delta.id()?
        } else {
            let id = self.store_delta(&delta).await?;
            lease
                .as_ref()
                .expect("distributed merge has a lease")
                .protect(delta_path(&self.options.repository_prefix, id)?)
                .await?;
            id
        };
        let node_pack = if native_profile {
            self.node_store
                .flush_node_pack(
                    tree_format_digest(&self.format.state_tree_format)?,
                    Vec::new(),
                )
                .await?
        } else {
            None
        };
        let commit = BucketCommitV1 {
            state: BucketStateV1 {
                objects: TreeRootV1::from_tree(&objects)?,
                versions: TreeRootV1::from_tree(&versions)?,
                operations: TreeRootV1::from_tree(&operations)?,
            },
            parents: vec![plan.ours, plan.theirs],
            generation,
            delta: delta_id,
            author: self.options.writer.clone(),
            message: Some(message.unwrap_or_else(|| "merge".to_string())),
            created_at_millis,
            metadata: BTreeMap::new(),
            native: native_profile.then_some(crate::NativeCommitExtensionV1 {
                node_pack,
                inline_delta: delta,
                writer_fence_generation,
            }),
        };
        self.publish_prepared_commit(
            target,
            loaded_ref,
            operation,
            commit,
            operation_result,
            lease,
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
        if self.storage_profile() != RepositoryStorageProfile::NativeVersionedV1
            || supplied_operation.is_some()
        {
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
        let native_profile = self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1;
        let _native_publication = if native_profile {
            Some(self.native_publication.lock().await)
        } else {
            None
        };
        let writer_fence_generation = if native_profile {
            self.native_writer_generation_for_mutation().await?
        } else {
            0
        };
        let lease = if native_profile {
            None
        } else {
            Some(self.publication_lease(operation).await?)
        };
        let engine = match &lease {
            Some(lease) => self.protected_engine(Arc::new(lease.clone())),
            None => AsyncProlly::new(
                self.node_store.clone(),
                Config {
                    format: self.format.state_tree_format.clone(),
                    runtime: RuntimeConfig::default(),
                },
            ),
        };
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
            let (kind, native_binding) = match source_map.get(key).copied() {
                Some(source_version) => {
                    let source_version = self
                        .find_version(&source_commit, key, source_version)
                        .await?;
                    match &source_version.body.kind {
                        ObjectVersionKindV1::Live { .. } => (
                            source_version.body.kind.clone(),
                            source_version.native_binding.clone(),
                        ),
                        ObjectVersionKindV1::DeleteMarker => {
                            return Err(Error::new(
                                ErrorCode::CorruptCommit,
                                "source current-object root points to a delete marker",
                            ))
                        }
                    }
                }
                None => {
                    let binding = if native_profile {
                        match self
                            .latest_native_delete_binding(&source_commit, key)
                            .await?
                        {
                            Some(binding) => Some(binding),
                            None => {
                                let path =
                                    ObjectPath::new(std::str::from_utf8(key).map_err(|_| {
                                        Error::new(
                                            ErrorCode::InvalidKey,
                                            "logical key is not valid UTF-8",
                                        )
                                    })?)?;
                                Some(
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
                                        Err(error) => {
                                            match self.reconcile_native_delete(&path).await? {
                                                Some(binding) => binding,
                                                None => return Err(error),
                                            }
                                        }
                                    },
                                )
                            }
                        }
                    } else {
                        None
                    };
                    (ObjectVersionKindV1::DeleteMarker, binding)
                }
            };
            let body = ObjectVersionBodyV1 {
                order: ObjectVersionOrder {
                    commit_generation: generation,
                    mutation_ordinal: u32::try_from(ordinal).map_err(|_| {
                        Error::new(ErrorCode::InvalidLimit, "restore ordinal overflow")
                    })?,
                },
                created_at_millis,
                kind,
            };
            let version = if native_profile {
                ObjectVersionV1::derive_native(
                    self.format.repository_id,
                    key,
                    operation,
                    body,
                    native_binding.ok_or_else(|| {
                        Error::new(
                            ErrorCode::CorruptCommit,
                            "native restore source omitted its physical binding",
                        )
                    })?,
                )?
            } else {
                ObjectVersionV1::derive(self.format.repository_id, key, operation, body)?
            };
            objects = if matches!(version.body.kind, ObjectVersionKindV1::DeleteMarker) {
                engine.delete(&objects, key).await?
            } else {
                engine
                    .put(
                        &objects,
                        key.clone(),
                        encode_canonical(&CurrentObjectV1 {
                            version: version.id,
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
                delete_marker: matches!(version.body.kind, ObjectVersionKindV1::DeleteMarker),
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
        let delta_id = if native_profile {
            delta.id()?
        } else {
            let id = self.store_delta(&delta).await?;
            lease
                .as_ref()
                .expect("distributed restore has a lease")
                .protect(delta_path(&self.options.repository_prefix, id)?)
                .await?;
            id
        };
        let node_pack = if native_profile {
            self.node_store
                .flush_node_pack(
                    tree_format_digest(&self.format.state_tree_format)?,
                    Vec::new(),
                )
                .await?
        } else {
            None
        };
        let commit = BucketCommitV1 {
            state: BucketStateV1 {
                objects: TreeRootV1::from_tree(&objects)?,
                versions: TreeRootV1::from_tree(&versions)?,
                operations: TreeRootV1::from_tree(&operations)?,
            },
            parents: vec![expected_head],
            generation,
            delta: delta_id,
            author: self.options.writer.clone(),
            message: Some(message.unwrap_or_else(|| format!("restore {source}"))),
            created_at_millis,
            metadata: BTreeMap::new(),
            native: native_profile.then_some(crate::NativeCommitExtensionV1 {
                node_pack,
                inline_delta: delta,
                writer_fence_generation,
            }),
        };
        self.publish_prepared_commit(
            branch,
            loaded_ref,
            operation,
            commit,
            operation_result,
            lease,
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
            result.insert(key, current.version);
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
        operation_result: CanonicalOperationResult,
        lease: Option<PublicationLease<P>>,
        reflog_message: &str,
    ) -> Result<CommitReceipt> {
        let native_profile = self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1;
        let writer_fence_generation = if native_profile {
            commit
                .native
                .as_ref()
                .map(|native| native.writer_fence_generation)
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "native prepared commit omitted its publication extension",
                    )
                })?
        } else {
            if commit.native.is_some() {
                return Err(Error::new(
                    ErrorCode::InternalInvariant,
                    "distributed prepared commit contains a native extension",
                ));
            }
            0
        };
        let commit_id = self.store_commit(&commit).await?;
        if let Some(lease) = &lease {
            lease
                .protect(commit_path(&self.options.repository_prefix, commit_id)?)
                .await?;
            lease.set_proposal(commit_id).await?;
        }
        let reflog = ReflogEntryV1 {
            branch: branch.to_string(),
            old_target: Some(loaded_ref.value.target),
            new_target: commit_id,
            operation,
            actor: self.options.writer.clone(),
            message: reflog_message.to_string(),
            created_at_millis: commit.created_at_millis,
        };
        let reflog_id = if native_profile {
            reflog.id()?
        } else {
            let id = self.store_reflog(&reflog).await?;
            lease
                .as_ref()
                .expect("distributed prepared publication has a lease")
                .protect(reflog_path(&self.options.repository_prefix, branch, id)?)
                .await?;
            id
        };
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
            native: native_profile.then_some(crate::NativeRefExtensionV1 {
                writer_fence_generation,
                inline_reflog: reflog,
            }),
        };
        if native_profile {
            if self.native_writer_generation_for_mutation().await? != writer_fence_generation {
                return Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "native writer fence changed during prepared publication",
                ));
            }
        } else {
            self.ensure_publication_allowed(
                lease
                    .as_ref()
                    .expect("distributed prepared publication has a lease"),
            )
            .await?;
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
                let receipt = CommitReceipt {
                    id: commit_id,
                    operation,
                    branch: branch.to_string(),
                    parents: commit.parents.clone(),
                    changed_keys: operation_result.changed_keys,
                    object_versions: operation_result.object_versions,
                    idempotent_replay: false,
                };
                if native_profile {
                    self.cache_branch(branch, next_ref, metadata.token, commit)?;
                }
                if let Some(lease) = &lease {
                    let _ = lease.complete(commit_id).await;
                }
                Ok(receipt)
            }
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                if native_profile {
                    ErrorCode::PreconditionFailed
                } else {
                    ErrorCode::RefConflict
                },
                if native_profile {
                    "native branch CAS conflicted; writer is fenced and must reopen"
                } else {
                    "branch moved during publication"
                },
            )
            .retry(RetryAdvice::ReloadHead)
            .operation(operation.to_string())),
            Err(error) => {
                if let Some(receipt) = self.lookup_operation(branch, operation).await? {
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
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
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
            let _publication = self.native_publication.lock().await;
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
            return Ok(RepairReport { sync, fsck });
        }
        let head = source.head(source_branch).await?;
        let mut sync = source
            .copy_commit_closure_to(self.plane.clone(), &self.options.repository_prefix, head)
            .await?;
        sync.source_head = Some(head);
        let fsck = self.fsck_commit(head).await?;
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
        let mut content_seen = BTreeSet::new();
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
                if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
                    let key = decode_version_tree_logical_key(&encoded_key)?;
                    let verified = self.verify_native_version(&key, &version).await?;
                    report.content_bytes_verified = report
                        .content_bytes_verified
                        .checked_add(verified)
                        .ok_or_else(|| {
                            Error::new(
                                ErrorCode::EntityTooLarge,
                                "fsck native byte counter overflow",
                            )
                        })?;
                    continue;
                }
                let ObjectVersionKindV1::Live { content, size, .. } = version.body.kind else {
                    continue;
                };
                let crate::ContentRef::Chunks(reference) = content else {
                    if size != 0 {
                        return Err(Error::new(
                            ErrorCode::CorruptContent,
                            "empty content has nonzero size",
                        ));
                    }
                    continue;
                };
                if !content_seen.insert(reference) {
                    continue;
                }
                report.content_manifests += 1;
                let mut stream = self
                    .content
                    .read_stream(crate::ContentRef::Chunks(reference), None);
                let mut verified = 0u64;
                while let Some(chunk) = stream.next().await {
                    verified = verified.checked_add(chunk?.len() as u64).ok_or_else(|| {
                        Error::new(ErrorCode::EntityTooLarge, "fsck byte counter overflow")
                    })?;
                }
                if verified != size {
                    return Err(Error::new(
                        ErrorCode::CorruptContent,
                        "logical size disagrees with verified content",
                    ));
                }
                report.content_bytes_verified = report
                    .content_bytes_verified
                    .checked_add(verified)
                    .ok_or_else(|| {
                        Error::new(ErrorCode::EntityTooLarge, "fsck byte counter overflow")
                    })?;
            }
        }
        Ok(report)
    }

    async fn verify_native_version(&self, key: &[u8], version: &ObjectVersionV1) -> Result<u64> {
        version.validate_native()?;
        let path = ObjectPath::new(std::str::from_utf8(key).map_err(|_| {
            Error::new(ErrorCode::CorruptCommit, "native logical key is not UTF-8")
        })?)?;
        match version.native_binding.as_ref().expect("validated binding") {
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
                    ObjectVersionKindV1::Live { size, .. } => size,
                    ObjectVersionKindV1::DeleteMarker => unreachable!("binding was validated"),
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
        let lease_window = if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1
        {
            self.options.writer_lease_millis
        } else {
            self.options.publication_lease_millis
        };
        let minimum_grace = lease_window
            .checked_mul(2)
            .ok_or_else(|| Error::new(ErrorCode::InvalidLimit, "GC grace overflow"))?;
        if grace_millis < minimum_grace || max_candidates == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "GC requires a nonzero candidate limit and grace at least twice the publication lease",
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
        let prefix = if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
            String::new()
        } else {
            format!("{}/", self.options.repository_prefix)
        };
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
                    || (self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1
                        && !entry
                            .path
                            .as_str()
                            .starts_with(&format!("{}/", self.options.repository_prefix)));
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
        let initial_index = self.load_gc_run(id).await?.value.next_index;
        let invocation_end = initial_index
            .saturating_add(max_candidates)
            .min(plan.body.candidates.len());
        for _ in 0..=self.options.logical_retry_limit {
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
            if loaded.value.next_index >= invocation_end
                && loaded.value.next_index < plan.body.candidates.len()
            {
                return Ok(gc_report(&loaded.value));
            }
            let end = invocation_end;
            let mut next = loaded.value;
            for candidate in &plan.body.candidates[next.next_index..end] {
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
            next.next_index = end;
            next.generation = next.generation.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "GC run generation overflow")
            })?;
            next.updated_at_millis = self.now_millis()?;
            if next.next_index == plan.body.candidates.len() {
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
        let mut content = BTreeSet::new();
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
                } else if path.contains("/multipart/uploads/") {
                    let value: MultipartUploadV1 = decode_canonical(&object.bytes)?;
                    for part in value.parts.values() {
                        if let crate::ContentRef::Chunks(reference) = part.content.reference {
                            content.insert(reference);
                        }
                    }
                } else if path.contains("/multipart/catalog-snapshots/") {
                    let value: MultipartCatalogSnapshotV1 = decode_canonical(&object.bytes)?;
                    value.validate_id()?;
                    if value.body.repository != self.format.repository_id {
                        return Err(Error::new(
                            ErrorCode::CorruptCommit,
                            "multipart catalog snapshot belongs to another repository",
                        ));
                    }
                    if value.body.expires_at_millis >= at_millis {
                        retained.paths.insert(listed.path.clone());
                    }
                } else if path.contains("/workspaces/") {
                    let value: WorkspaceManifestV1 = decode_canonical(&object.bytes)?;
                    commit_roots.push(value.base_commit);
                    for mutation in value.mutations.values() {
                        match mutation {
                            WorkspaceMutationV1::Put {
                                content: stored, ..
                            } => {
                                if let crate::ContentRef::Chunks(reference) = stored.reference {
                                    content.insert(reference);
                                }
                            }
                            WorkspaceMutationV1::NativePut { key, binding, .. }
                            | WorkspaceMutationV1::NativeDelete { key, binding } => {
                                let path =
                                    ObjectPath::new(std::str::from_utf8(key).map_err(|_| {
                                        Error::new(
                                            ErrorCode::CorruptCommit,
                                            "native workspace key is not UTF-8",
                                        )
                                    })?)?;
                                let version_id = match binding {
                                    crate::NativeObjectBindingV1::Live { version_id, .. }
                                    | crate::NativeObjectBindingV1::DeleteMarker { version_id } => {
                                        version_id
                                    }
                                };
                                retained.native_versions.insert((path, version_id.clone()));
                            }
                            WorkspaceMutationV1::Delete { .. } => {}
                        }
                    }
                } else if path.ends_with("/lease") && path.contains("/publications/") {
                    let lease: crate::PublicationLeaseV1 = decode_canonical(&object.bytes)?;
                    if matches!(lease.state, crate::PublicationLeaseStateV1::Active)
                        && lease.expires_at_millis > at_millis
                    {
                        commit_roots.extend(lease.proposal);
                        let mut segment = lease.protection_head;
                        while let Some(id) = segment {
                            retained.paths.insert(publication_segment_path(
                                &self.options.repository_prefix,
                                id,
                            )?);
                            let loaded = crate::load_protection_segment(
                                self.plane.as_ref(),
                                &self.options.repository_prefix,
                                id,
                            )
                            .await?
                            .ok_or_else(|| {
                                Error::new(
                                    ErrorCode::MissingClosure,
                                    "active publication lease segment is missing",
                                )
                            })?;
                            retained.paths.extend(loaded.paths);
                            segment = loaded.previous;
                        }
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
            if let Some(native) = &commit.native {
                if native.inline_delta.id()? != commit.delta {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "native commit inline delta identity mismatch",
                    ));
                }
                if let Some(pack) = &native.node_pack {
                    retained
                        .paths
                        .insert(node_pack_path(&self.options.repository_prefix, pack.id)?);
                }
            } else {
                retained
                    .paths
                    .insert(delta_path(&self.options.repository_prefix, commit.delta)?);
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
                if let Some(binding) = &version.native_binding {
                    let key = decode_version_tree_logical_key(&encoded_key)?;
                    let path = ObjectPath::new(std::str::from_utf8(&key).map_err(|_| {
                        Error::new(ErrorCode::CorruptCommit, "native logical key is not UTF-8")
                    })?)?;
                    let version_id = match binding {
                        crate::NativeObjectBindingV1::Live { version_id, .. }
                        | crate::NativeObjectBindingV1::DeleteMarker { version_id } => version_id,
                    };
                    retained.native_versions.insert((path, version_id.clone()));
                    continue;
                }
                if let ObjectVersionKindV1::Live {
                    content: crate::ContentRef::Chunks(reference),
                    ..
                } = version.body.kind
                {
                    content.insert(reference);
                }
            }
            commit_roots.extend(commit.parents);
        }
        let nodes = self.engine.mark_reachable(&state_roots).await?;
        if self.storage_profile() == RepositoryStorageProfile::DistributedContentAddressedV1 {
            for cid in nodes.cids() {
                retained
                    .paths
                    .insert(node_path(&self.options.repository_prefix, cid)?);
            }
        }
        for reference in content {
            retained.paths.extend(
                self.content
                    .retained_paths(&crate::ContentRef::Chunks(reference))
                    .await?,
            );
        }
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
                match self.storage_profile() {
                    RepositoryStorageProfile::DistributedContentAddressedV1 => {
                        if version.native_binding.is_some() {
                            return Err(Error::new(
                                ErrorCode::CorruptCommit,
                                "distributed-profile object version contains a native binding",
                            ));
                        }
                    }
                    RepositoryStorageProfile::NativeVersionedV1 => version.validate_native()?,
                }
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
            if matches!(version.body.kind, ObjectVersionKindV1::DeleteMarker) {
                version.validate_native()?;
                return Ok(version.native_binding);
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
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1 {
            if let Some(warm) = self
                .warm_branches
                .read()
                .map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "branch-cache lock poisoned")
                })?
                .get(branch)
                .cloned()
            {
                return Ok(LoadedRef {
                    value: warm.reference,
                    token: warm.token,
                });
            }
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

    async fn load_upload(&self, id: UploadId) -> Result<LoadedUpload> {
        let object = self
            .plane
            .load_mutable(&upload_path(&self.options.repository_prefix, id)?)
            .await?
            .ok_or_else(|| {
                Error::new(ErrorCode::NoSuchUpload, "multipart upload does not exist")
            })?;
        let value: MultipartUploadV1 = decode_canonical(&object.bytes)?;
        if value.id != id {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "multipart upload ID mismatch",
            ));
        }
        Ok(LoadedUpload {
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
        if value.plan != id || value.next_index > self.load_gc_plan(id).await?.body.candidates.len()
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

    async fn load_sync_run_optional(&self, id: OperationId) -> Result<Option<LoadedSyncRun>> {
        let Some(object) = self
            .plane
            .load_mutable(&sync_run_path(&self.options.repository_prefix, id)?)
            .await?
        else {
            return Ok(None);
        };
        let value: SyncRunV1 = decode_canonical(&object.bytes)?;
        if value.id != id || value.repository != self.format.repository_id {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "sync checkpoint identity mismatch",
            ));
        }
        validate_sync_run_shape(&value)?;
        Ok(Some(LoadedSyncRun {
            value,
            token: object.metadata.token,
        }))
    }

    async fn load_sync_run(&self, id: OperationId) -> Result<LoadedSyncRun> {
        self.load_sync_run_optional(id)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::InvalidRevision, "sync checkpoint does not exist"))
    }

    async fn load_workspace(&self, id: WorkspaceId) -> Result<LoadedWorkspace> {
        let object = self
            .plane
            .load_mutable(&workspace_path(&self.options.repository_prefix, id)?)
            .await?
            .ok_or_else(|| Error::new(ErrorCode::NoSuchWorkspace, "workspace does not exist"))?;
        let value: WorkspaceManifestV1 = decode_canonical(&object.bytes)?;
        if value.id != id {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "workspace ID mismatch",
            ));
        }
        Ok(LoadedWorkspace {
            value,
            token: object.metadata.token,
        })
    }

    async fn load_commit(&self, id: CommitId) -> Result<BucketCommitV1> {
        if let Some(commit) = self
            .commit_cache
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "commit-cache lock poisoned"))?
            .get(&id)
            .cloned()
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
        let commit: BucketCommitV1 = decode_canonical(&object.bytes)?;
        if commit.id()? != id {
            return Err(Error::new(ErrorCode::CorruptCommit, "commit ID mismatch"));
        }
        self.commit_cache
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "commit-cache lock poisoned"))?
            .insert(id, commit.clone());
        Ok(commit)
    }

    async fn load_delta(&self, id: DeltaId) -> Result<BucketDeltaV1> {
        let object = self
            .plane
            .get(GetRequest {
                path: delta_path(&self.options.repository_prefix, id)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| Error::new(ErrorCode::MissingClosure, "delta object is missing"))?;
        let delta: BucketDeltaV1 = decode_canonical(&object.bytes)?;
        if delta.id()? != id {
            return Err(Error::new(ErrorCode::CorruptCommit, "delta ID mismatch"));
        }
        Ok(delta)
    }

    async fn load_commit_delta(&self, commit: &BucketCommitV1) -> Result<BucketDeltaV1> {
        if let Some(native) = &commit.native {
            if native.inline_delta.id()? != commit.delta {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "native commit inline delta identity mismatch",
                ));
            }
            if native.writer_fence_generation == 0 {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "native commit has a zero writer fence generation",
                ));
            }
            return Ok(native.inline_delta.clone());
        }
        self.load_delta(commit.delta).await
    }

    async fn store_commit(&self, commit: &BucketCommitV1) -> Result<CommitId> {
        let bytes = encode_canonical(commit)?;
        let id = commit.id()?;
        self.store_immutable(commit_path(&self.options.repository_prefix, id)?, bytes)
            .await?;
        self.commit_cache
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "commit-cache lock poisoned"))?
            .insert(id, commit.clone());
        Ok(id)
    }

    async fn store_delta(&self, delta: &BucketDeltaV1) -> Result<DeltaId> {
        let bytes = encode_canonical(delta)?;
        let id = delta.id()?;
        self.store_immutable(delta_path(&self.options.repository_prefix, id)?, bytes)
            .await?;
        Ok(id)
    }

    async fn store_reflog(&self, entry: &ReflogEntryV1) -> Result<crate::ReflogEntryId> {
        let bytes = encode_canonical(entry)?;
        let id = entry.id()?;
        self.store_immutable(
            reflog_path(&self.options.repository_prefix, &entry.branch, id)?,
            bytes,
        )
        .await?;
        Ok(id)
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

    async fn publication_lease(&self, operation: OperationId) -> Result<PublicationLease<P>> {
        PublicationLease::create_or_resume_with_clock(
            self.plane.clone(),
            self.options.repository_prefix.clone(),
            operation,
            self.options.writer.clone(),
            self.options.publication_lease_millis,
            self.options.clock.clone(),
        )
        .await
    }

    async fn ensure_publication_allowed(&self, lease: &PublicationLease<P>) -> Result<()> {
        lease.flush_protection().await?;
        self.ensure_gc_idle().await?;
        lease.ensure_active().await
    }

    async fn ensure_gc_idle(&self) -> Result<()> {
        let prefix = format!("{}/gc/runs/", self.options.repository_prefix);
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
            for entry in page.entries {
                let Some(stored) = self.plane.load_mutable(&entry.path).await? else {
                    continue;
                };
                let run: GcRunV1 = decode_canonical(&stored.bytes)?;
                if matches!(run.state, GcRunStateV1::Running) {
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "a garbage-collection sweep currently fences ref publication",
                    )
                    .retry(RetryAdvice::ReloadHead));
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                return Ok(());
            }
        }
    }

    fn protected_engine(&self, sink: Arc<dyn ProtectionSink>) -> AsyncProlly<ProllyObjectStore<P>> {
        if self.node_store.is_packed() {
            return AsyncProlly::new(
                self.node_store.clone(),
                Config {
                    format: self.format.state_tree_format.clone(),
                    runtime: RuntimeConfig::default(),
                },
            );
        }
        AsyncProlly::new(
            ProllyObjectStore::new(self.plane.clone(), self.options.repository_prefix.clone())
                .with_protection_sink(sink),
            Config {
                format: self.format.state_tree_format.clone(),
                runtime: RuntimeConfig::default(),
            },
        )
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
        if self.storage_profile() == RepositoryStorageProfile::NativeVersionedV1
            && (key == self.options.repository_prefix
                || key.starts_with(&format!("{}/", self.options.repository_prefix)))
        {
            return Err(Error::new(
                ErrorCode::InvalidKey,
                "logical key overlaps the native-versioned repository metadata prefix",
            ));
        }
        Ok(())
    }
}

fn validate_expected_checksums(
    stored: &StoredContent,
    expected: &ChecksumExpectation,
) -> Result<()> {
    if expected
        .md5
        .is_some_and(|expected| stored.checksums.md5 != Some(expected))
        || expected
            .sha256
            .is_some_and(|expected| stored.checksums.sha256 != Some(expected))
    {
        return Err(Error::new(
            ErrorCode::ChecksumMismatch,
            "request checksum does not match the staged logical body",
        ));
    }
    Ok(())
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
    if options.logical_retry_limit > MAX_LOGICAL_RETRY_LIMIT {
        return Err(Error::new(
            ErrorCode::InvalidLimit,
            "logical retry limit must be at most 16",
        ));
    }
    options.state_tree_format.validate()?;
    options.content_index_format.validate()?;
    if !(5 * 60 * 1_000..=24 * 60 * 60 * 1_000).contains(&options.publication_lease_millis) {
        return Err(Error::new(
            ErrorCode::InvalidLimit,
            "publication lease must be between 5 minutes and 24 hours",
        ));
    }
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
    if options.gc_delete_rate_limit_per_second > 1_000 {
        return Err(Error::new(
            ErrorCode::InvalidLimit,
            "GC delete rate limit must be zero or at most 1,000 per second",
        ));
    }
    Ok(())
}

fn validate_completed_multipart_parts(
    upload: &MultipartUploadV1,
    requested: &[(u32, String)],
    max_object_bytes: u64,
) -> Result<Vec<StoredContent>> {
    let mut total = 0_u64;
    let mut parts = Vec::with_capacity(requested.len());
    for (index, (part_number, etag)) in requested.iter().enumerate() {
        let part = upload
            .parts
            .get(part_number)
            .ok_or_else(|| Error::new(ErrorCode::InvalidRequest, "completed part is missing"))?;
        if &part.etag != etag {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "completed part ETag mismatch",
            ));
        }
        if part.content.size > MAX_MULTIPART_PART_BYTES {
            return Err(Error::new(
                ErrorCode::EntityTooLarge,
                "multipart part exceeds 5 GiB",
            ));
        }
        if index + 1 < requested.len() && part.content.size < MIN_NONFINAL_MULTIPART_PART_BYTES {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "nonfinal multipart part is smaller than 5 MiB",
            ));
        }
        total = total.checked_add(part.content.size).ok_or_else(|| {
            Error::new(
                ErrorCode::EntityTooLarge,
                "multipart object length overflow",
            )
        })?;
        if total > max_object_bytes {
            return Err(Error::new(
                ErrorCode::EntityTooLarge,
                "completed multipart object exceeds the repository size limit",
            ));
        }
        parts.push(part.content.clone());
    }
    Ok(parts)
}

fn validate_sync_run(
    run: &SyncRunV1,
    id: OperationId,
    repository: RepositoryId,
    source_branch: &str,
    source_head: CommitId,
) -> Result<()> {
    if run.id != id
        || run.repository != repository
        || run.source_branch != source_branch
        || run.source_head != source_head
    {
        return Err(Error::new(
            ErrorCode::IdempotencyConflict,
            "sync checkpoint does not match the selected source closure",
        )
        .operation(id.to_string()));
    }
    Ok(())
}

fn validate_sync_run_shape(run: &SyncRunV1) -> Result<()> {
    if run.source_branch.is_empty()
        || run
            .after_relative_path
            .as_deref()
            .is_some_and(|path| path.is_empty() || path.starts_with('/') || path.contains("/../"))
        || (matches!(run.state, SyncRunStateV1::Completed) && run.generation == 0)
    {
        return Err(Error::new(
            ErrorCode::CorruptCommit,
            "sync checkpoint state is inconsistent",
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
    let storage_profile = format.storage_profile()?;
    if storage_profile != options.storage_profile {
        return Err(Error::new(
            ErrorCode::RepositoryFormatConflict,
            format!(
                "repository uses storage profile {storage_profile:?}, requested {requested:?}",
                requested = options.storage_profile
            ),
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
        || format.content_index_format != options.content_index_format
        || format.canonical_limits != options.limits
    {
        return Err(Error::new(
            ErrorCode::RepositoryFormatConflict,
            "repository format does not match requested canonical settings",
        ));
    }
    Ok(())
}

#[cfg(not(prolly_s3_legacy_v1_codec))]
fn decode_repository_format(bytes: &[u8]) -> Result<RepositoryFormatV1> {
    decode_canonical(bytes)
}

#[cfg(prolly_s3_legacy_v1_codec)]
fn decode_repository_format(bytes: &[u8]) -> Result<RepositoryFormatV1> {
    match decode_canonical(bytes) {
        Ok(format) => Ok(format),
        Err(error) => {
            if canonical_legacy_format_has_appended_fields(bytes) {
                return Err(Error::new(
                    ErrorCode::UnsupportedRepositoryFormat,
                    "repository format contains fields this client does not support",
                ));
            }
            Err(error)
        }
    }
}

#[cfg(prolly_s3_legacy_v1_codec)]
fn canonical_legacy_format_has_appended_fields(bytes: &[u8]) -> bool {
    let Ok(value) = serde_cbor::from_slice::<serde_cbor::Value>(bytes) else {
        return false;
    };
    let Ok(canonical) = serde_cbor::to_vec(&value) else {
        return false;
    };
    if canonical != bytes
        || serde_cbor::value::from_value::<RepositoryFormatV1>(value.clone()).is_err()
    {
        return false;
    }
    let serde_cbor::Value::Map(fields) = value else {
        return false;
    };
    let mut known = [false; 8];
    let mut appended = false;
    for key in fields.keys() {
        match key {
            serde_cbor::Value::Integer(index) if (0..8).contains(index) => {
                known[*index as usize] = true;
            }
            serde_cbor::Value::Integer(index) if *index >= 8 => appended = true,
            _ => return false,
        }
    }
    appended && known.into_iter().all(|present| present)
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

fn upload_path(prefix: &str, id: UploadId) -> Result<ObjectPath> {
    ObjectPath::new(format!(
        "{prefix}/multipart/uploads/{}",
        hex::encode(id.as_bytes())
    ))
}

fn multipart_catalog_snapshot_path(
    prefix: &str,
    id: MultipartCatalogSnapshotId,
) -> Result<ObjectPath> {
    let encoded = id.to_string();
    let digest = encoded
        .strip_prefix(MultipartCatalogSnapshotId::PREFIX)
        .ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "multipart catalog snapshot ID prefix is invalid",
            )
        })?;
    ObjectPath::new(format!(
        "{prefix}/multipart/catalog-snapshots/{}/{}/{}.cbor",
        &digest[..2],
        &digest[2..4],
        encoded
    ))
}

fn workspace_path(prefix: &str, id: WorkspaceId) -> Result<ObjectPath> {
    ObjectPath::new(format!(
        "{prefix}/workspaces/{}",
        hex::encode(id.as_bytes())
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

fn delta_path(prefix: &str, id: DeltaId) -> Result<ObjectPath> {
    let encoded = hex::encode(id.as_bytes());
    ObjectPath::new(format!(
        "{prefix}/deltas/sha256/{}/{}/{}",
        &encoded[..2],
        &encoded[2..4],
        encoded
    ))
}

fn reflog_path(prefix: &str, branch: &str, id: crate::ReflogEntryId) -> Result<ObjectPath> {
    ObjectPath::new(format!(
        "{prefix}/reflogs/heads/{}/{}",
        hex::encode(branch.as_bytes()),
        hex::encode(id.as_bytes())
    ))
}

fn tag_reflog_path(prefix: &str, tag: &str, id: crate::ReflogEntryId) -> Result<ObjectPath> {
    ObjectPath::new(format!(
        "{prefix}/reflogs/tags/{}/{}",
        hex::encode(tag.as_bytes()),
        hex::encode(id.as_bytes())
    ))
}

fn node_path(prefix: &str, cid: &Cid) -> Result<ObjectPath> {
    let encoded = hex::encode(cid.as_bytes());
    ObjectPath::new(format!(
        "{prefix}/nodes/sha256/{}/{}/{}",
        &encoded[..2],
        &encoded[2..4],
        encoded
    ))
}

fn node_pack_path(prefix: &str, id: crate::NodePackId) -> Result<ObjectPath> {
    let encoded = hex::encode(id.as_bytes());
    ObjectPath::new(format!(
        "{prefix}/node-packs/sha256/{}/{}/{}.pack",
        &encoded[..2],
        &encoded[2..4],
        encoded
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

fn publication_segment_path(prefix: &str, id: crate::ProtectionSegmentId) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/publications/segments/{id}.cbor"))
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

fn sync_run_path(prefix: &str, id: OperationId) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/sync/runs/{}", hex::encode(id.as_bytes())))
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
    relative.is_some_and(|value| {
        value.starts_with("nodes/")
            || value.starts_with("chunks/")
            || value.starts_with("content-manifests/")
            || value.starts_with("commits/")
            || value.starts_with("deltas/")
            || value.starts_with("multipart/catalog-snapshots/")
    })
}

fn is_portable_clone_path(relative: &str) -> bool {
    relative.starts_with("format/")
        || relative.starts_with("chunks/")
        || relative.starts_with("content-manifests/")
        || relative.starts_with("nodes/")
        || relative.starts_with("deltas/")
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

fn effective_logical_retry_limit(default: u8, operation_override: Option<u8>) -> u8 {
    operation_override.unwrap_or(default)
}

fn object_diff_from_prolly(diff: prolly::Diff) -> Result<ObjectDiff> {
    match diff {
        prolly::Diff::Added { key, val } => Ok(ObjectDiff {
            key,
            from: None,
            to: Some(decode_canonical::<CurrentObjectV1>(&val)?.version),
        }),
        prolly::Diff::Removed { key, val } => Ok(ObjectDiff {
            key,
            from: Some(decode_canonical::<CurrentObjectV1>(&val)?.version),
            to: None,
        }),
        prolly::Diff::Changed { key, old, new } => Ok(ObjectDiff {
            key,
            from: Some(decode_canonical::<CurrentObjectV1>(&old)?.version),
            to: Some(decode_canonical::<CurrentObjectV1>(&new)?.version),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn synthetic_upload_with_part(size: u64) -> MultipartUploadV1 {
        let id = UploadId::new();
        MultipartUploadV1 {
            id,
            branch: "main".to_string(),
            key: b"boundary".to_vec(),
            headers: ObjectHeaders::default(),
            user_metadata: BTreeMap::new(),
            parts: BTreeMap::from([(
                1,
                MultipartPartV1 {
                    part_number: 1,
                    content: StoredContent {
                        reference: crate::ContentRef::Empty,
                        size,
                        logical_etag: "\"synthetic\"".to_string(),
                        checksums: crate::Checksums::default(),
                    },
                    etag: "\"synthetic\"".to_string(),
                    updated_at_millis: 1,
                },
            )]),
            generation: 1,
            state: MultipartStateV1::Active,
            created_at_millis: 1,
            updated_at_millis: 1,
            expires_at_millis: 0,
        }
    }

    #[test]
    fn multipart_part_upper_bound_is_exact_without_allocating_payload() {
        let at_limit = synthetic_upload_with_part(MAX_MULTIPART_PART_BYTES);
        assert!(validate_completed_multipart_parts(
            &at_limit,
            &[(1, "\"synthetic\"".to_string())],
            RepositoryOptions::default().limits.max_object_bytes,
        )
        .is_ok());

        let above_limit = synthetic_upload_with_part(MAX_MULTIPART_PART_BYTES + 1);
        assert_eq!(
            validate_completed_multipart_parts(
                &above_limit,
                &[(1, "\"synthetic\"".to_string())],
                RepositoryOptions::default().limits.max_object_bytes,
            )
            .unwrap_err()
            .code,
            ErrorCode::EntityTooLarge
        );
    }

    #[test]
    fn operation_retry_limit_overrides_the_repository_default() {
        assert_eq!(effective_logical_retry_limit(3, None), 3);
        assert_eq!(effective_logical_retry_limit(3, Some(0)), 0);
        assert_eq!(effective_logical_retry_limit(3, Some(16)), 16);

        let options = RepositoryOptions {
            logical_retry_limit: 17,
            ..RepositoryOptions::default()
        };
        assert_eq!(
            validate_options(&options).unwrap_err().code,
            ErrorCode::InvalidLimit
        );
    }

    #[cfg(prolly_s3_legacy_v1_codec)]
    #[test]
    fn legacy_codec_classifies_canonical_appended_format_fields_as_unsupported() {
        let options = RepositoryOptions::default();
        let format = RepositoryFormatV1 {
            repository_id: RepositoryId::from_hash([0x11; 32]),
            format_version: RepositoryFormatV1::VERSION,
            state_tree_format: options.state_tree_format,
            content_index_format: options.content_index_format,
            canonical_limits: options.limits,
            min_reader_version: RepositoryFormatV1::CURRENT_READER_VERSION,
            min_writer_version: RepositoryFormatV1::CURRENT_WRITER_VERSION,
            created_at_millis: 1,
        };
        let encoded = encode_canonical(&format).unwrap();
        let serde_cbor::Value::Map(mut fields) =
            serde_cbor::from_slice::<serde_cbor::Value>(&encoded).unwrap()
        else {
            panic!("repository format must be a packed CBOR map");
        };
        fields.insert(serde_cbor::Value::Integer(8), serde_cbor::Value::Integer(2));
        let future = serde_cbor::to_vec(&serde_cbor::Value::Map(fields)).unwrap();
        assert!(canonical_legacy_format_has_appended_fields(&future));
        assert_eq!(
            decode_repository_format(&future).unwrap_err().code,
            ErrorCode::UnsupportedRepositoryFormat
        );
        assert_eq!(
            decode_repository_format(&[0xff]).unwrap_err().code,
            ErrorCode::CorruptCommit
        );
    }
}
