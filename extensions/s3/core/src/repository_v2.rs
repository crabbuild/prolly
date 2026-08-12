use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock, Weak,
    },
    time::Duration,
};

use futures_util::{stream, StreamExt};
use md5::{Digest as _, Md5};
use prolly::{AsyncProlly, Config, Diff, Mutation, RuntimeConfig, Tree, TreeFormat};
use sha2::Sha256;

use crate::merge_v2::{
    MergeBaseCandidateV2, MergePlanEntryV2, MergeQueueEntryV2, MergeSeenEntryV2,
};
use crate::store::{LocatedPackedNode, NodeCacheNamespace, NodeLocator, PreparedNodePack};
use crate::{
    decode_canonical, encode_canonical, tree_format_digest, AuthorityPermitV2, AuthorityScopeV2,
    BucketCommitV1, BucketCommitV2, BucketDeltaV2, BucketStateV2, CanonicalLimits, Checksums,
    Clock, CommitGeneration, CommitId, CommitIdV2, CommitObjectV2, CommitPublicationV2,
    CommitSessionCheckpointV2, CommitSessionCleanupReportV2, CommitSessionStateV2,
    CommitSessionStoreV2, CompareExchange, CompareExchangeOutcome, CurrentObjectV2, DeleteOutcome,
    Error, ErrorCode, GetRequest, IdSource, IdempotencyRetentionV2, ImmutablePayloadStoreV2,
    ImportedJournalIndexStateV2, InitializationIntentV2, JournalCommitGraphEntryV2,
    JournalDerivedIndexesV2, JournalIndexAdvanceReportV2, JournalIndexRebuildCleanupV2,
    JournalIndexRebuildCursorV2, JournalIndexRebuildPhaseV2, JournalIndexRebuildStepV2,
    ListRequest, LoadedRefV2, LogicalObjectVersionBodyV1, LogicalObjectVersionKindV1,
    MemoryNodeCache, MergeAdvancePageV2, MergeBaseCursorV2, MergeBasePageV2, MergeChangeCursorV2,
    MergeChangePageV2, MergeChangeV2, MergeCleanupCursorV2, MergeCleanupPageV2,
    MergeConflictCursorV2, MergeConflictPageV2, MergeConflictV2, MergeCursorV2, MergePhaseV2,
    MergePolicyV2, MergeReceiptV2, NodeCache, ObjectHeaders, ObjectPath, ObjectPlane,
    ObjectTransitionV2, ObjectVersionIdV2, ObjectVersionOrder, ObjectVersionV1, ObjectVersionV2,
    OperationId, OperationIndexAdvanceReportV2, OperationIndexRebuildCursorV2,
    OperationIndexRebuildStepV2, PhysicalBatchV2, PhysicalMutationIdentityV2, PhysicalVersion,
    ProllyObjectStore, ProviderPerKeyVersionLimitV2, RandomIdSource, RefCatalogCursorV2,
    RefGeneration, RefKindV2, RepositoryFormatV2, Result, SegmentedOperationIndexV2,
    ShardWriterAuthorityV2, ShardedBranchPublisherV2, ShardedRefCatalogV2, StagedMutationBodyV2,
    StagedMutationV2, StagedPutV2, SystemClock, TagStoreV2, TakeoverRequestV2, TreeRootV1,
};

#[derive(Clone)]
pub struct RepositoryV2Options {
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
    pub idempotency_retention: IdempotencyRetentionV2,
    pub provider_per_key_version_limit: ProviderPerKeyVersionLimitV2,
    pub clock: Arc<dyn Clock>,
    pub ids: Arc<dyn IdSource>,
}

impl Default for RepositoryV2Options {
    fn default() -> Self {
        Self {
            repository_prefix: ".prolly/v2".to_string(),
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
            idempotency_retention: IdempotencyRetentionV2::default(),
            provider_per_key_version_limit: ProviderPerKeyVersionLimitV2::Unknown,
            clock: Arc::new(SystemClock),
            ids: Arc::new(RandomIdSource),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitReceiptV2 {
    pub id: CommitIdV2,
    pub operation: OperationId,
    pub branch: String,
    pub parents: Vec<CommitIdV2>,
    pub changed_keys: u64,
    pub object_versions: Vec<ObjectVersionIdV2>,
    pub idempotent_replay: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectDataV2 {
    pub key: Vec<u8>,
    pub version: ObjectVersionV2,
    pub bytes: Vec<u8>,
    pub snapshot: CommitIdV2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectSummaryV2 {
    pub key: Vec<u8>,
    pub version: ObjectVersionV2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionSummaryV2 {
    pub key: Vec<u8>,
    pub version: ObjectVersionV2,
    pub cursor: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchHeadV2 {
    pub name: String,
    pub target: CommitIdV2,
    pub generation: RefGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagV2 {
    pub name: String,
    pub target: CommitIdV2,
    pub generation: RefGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchCatalogPageV2 {
    pub branches: Vec<BranchHeadV2>,
    pub continuation: Option<RefCatalogCursorV2>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagCatalogPageV2 {
    pub tags: Vec<TagV2>,
    pub continuation: Option<RefCatalogCursorV2>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RefCatalogRepairPageV2 {
    pub scanned: usize,
    pub indexed: usize,
    pub continuation: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct V1MigrationChangeV2 {
    pub key: Vec<u8>,
    pub version: ObjectVersionV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportedCommitReceiptV2 {
    pub source: CommitId,
    pub destination: CommitIdV2,
    pub index: ImportedJournalIndexStateV2,
    pub payloads: usize,
    pub payload_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchIndexAdvanceReportV2 {
    pub operations: OperationIndexAdvanceReportV2,
    pub journal: JournalIndexAdvanceReportV2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchIndexHealthV2 {
    pub branch: String,
    pub target: CommitIdV2,
    pub ref_generation: RefGeneration,
    pub indexed_target: Option<CommitIdV2>,
    pub indexed_generation: Option<RefGeneration>,
    pub lag_generations: u64,
    pub ready: bool,
    pub locally_registered: bool,
    pub last_error: Option<String>,
}

pub struct BranchIndexMaintenance {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for BranchIndexMaintenance {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct JournalNodeLocator<P: ObjectPlane> {
    indexes: Arc<JournalDerivedIndexesV2<P>>,
    branches: RwLock<BTreeSet<String>>,
    imports: RwLock<BTreeMap<OperationId, ImportedJournalIndexStateV2>>,
}

impl<P: ObjectPlane> JournalNodeLocator<P> {
    fn register(&self, branch: &str) -> Result<()> {
        self.branches
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 locator lock poisoned"))?
            .insert(branch.to_string());
        Ok(())
    }

    fn registered_branches(&self) -> Result<Vec<String>> {
        Ok(self
            .branches
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 locator lock poisoned"))?
            .iter()
            .cloned()
            .collect())
    }

    fn register_import(&self, state: ImportedJournalIndexStateV2) -> Result<()> {
        self.imports
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 locator lock poisoned"))?
            .insert(state.job, state);
        Ok(())
    }

    fn remove_import(&self, job: OperationId) -> Result<()> {
        self.imports
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 locator lock poisoned"))?
            .remove(&job);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<P: ObjectPlane> NodeLocator for JournalNodeLocator<P> {
    async fn locate(&self, cid: &prolly::Cid) -> Result<Option<LocatedPackedNode>> {
        let branches = self
            .branches
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 locator lock poisoned"))?
            .clone();
        for branch in branches {
            if let Some(entry) = self.indexes.node_location(&branch, cid).await? {
                return Ok(Some(entry.into()));
            }
        }
        let imports = self
            .imports
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 locator lock poisoned"))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for state in imports {
            if let Some(entry) = self.indexes.imported_node_location(&state, cid).await? {
                return Ok(Some(entry.into()));
            }
        }
        Ok(None)
    }
}

/// Native protocol-v2 repository.
///
/// This type never reads or writes v1 format markers, refs, commits, payload
/// bindings, or operation trees. Migration is implemented as an explicit
/// logical clone into a separately initialized v2 repository.
pub struct RepositoryV2<P: ObjectPlane> {
    plane: Arc<P>,
    options: RepositoryV2Options,
    format: RepositoryFormatV2,
    node_store: ProllyObjectStore<P>,
    authority: Arc<ShardWriterAuthorityV2<P>>,
    publisher: ShardedBranchPublisherV2<P>,
    payloads: ImmutablePayloadStoreV2<P>,
    commit_sessions: CommitSessionStoreV2<P>,
    tags: TagStoreV2<P>,
    ref_catalog: Arc<ShardedRefCatalogV2<P>>,
    operation_index: SegmentedOperationIndexV2<P>,
    journal_indexes: Arc<JournalDerivedIndexesV2<P>>,
    locator: Arc<JournalNodeLocator<P>>,
    permits: RwLock<BTreeMap<AuthorityScopeV2, AuthorityPermitV2>>,
    fenced_scopes: RwLock<BTreeSet<AuthorityScopeV2>>,
    authority_renewal: tokio::sync::Mutex<()>,
    publication_lanes: std::sync::Mutex<BTreeMap<String, Weak<tokio::sync::Mutex<()>>>>,
    index_lanes: std::sync::Mutex<BTreeMap<String, Weak<tokio::sync::Mutex<()>>>>,
    local_index_heads: RwLock<BTreeMap<String, CommitIdV2>>,
    index_errors: RwLock<BTreeMap<String, String>>,
    writable: AtomicBool,
}

impl<P: ObjectPlane> RepositoryV2<P> {
    pub async fn initialize(plane: Arc<P>, options: RepositoryV2Options) -> Result<Self> {
        validate_options(&options)?;
        if options.read_only {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "cannot initialize a protocol-v2 repository read-only",
            ));
        }
        let format_path = format_path(&options.repository_prefix)?;
        let operation = options.ids.operation();
        let created_at_millis = options.clock.now_millis()?;
        let repository_id = crate::model::derive_repository_id_v2(operation);
        let proposed_format = RepositoryFormatV2 {
            repository_id,
            format_version: RepositoryFormatV2::VERSION,
            state_tree_format: options.state_tree_format.clone(),
            canonical_limits: options.limits.clone(),
            idempotency_retention: options.idempotency_retention,
            provider_per_key_version_limit: options.provider_per_key_version_limit,
            min_reader_version: RepositoryFormatV2::PROLLY_S3_PROTOCOL_VERSION,
            min_writer_version: RepositoryFormatV2::PROLLY_S3_PROTOCOL_VERSION,
            created_at_millis,
            required_capability_profile: RepositoryFormatV2::PROLLY_S3_CAPABILITY_PROFILE,
        };
        let proposed_intent = InitializationIntentV2 {
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
                    "v2 initialization intent create returned an empty conflict",
                ))
            }
        };
        validate_format_compatibility(&intent.format, &options)?;

        // Advertise the v2 repository before creating any v2 ref or commit.
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
                    "a different protocol-v2 repository format already exists",
                ))
            }
        }

        let repository = Self::from_format(plane, options, intent.format)?;
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
                AuthorityScopeV2::Branch {
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
        let commit = BucketCommitV2 {
            state: BucketStateV2 {
                objects: TreeRootV1::from_tree(&empty)?,
                versions: TreeRootV1::from_tree(&empty)?,
            },
            parents: Vec::new(),
            generation: CommitGeneration(0),
            delta: BucketDeltaV2 {
                input_digest: crate::model::derive_input_digest_v2(&[b"initialize"]),
                changes: Vec::new(),
                changes_root: None,
                change_count: 0,
            },
            node_pack: None,
            authority: permit.stamp(),
            author: repository.options.writer.clone(),
            message: Some("initialize native protocol-v2 repository".to_string()),
            created_at_millis: repository_created_at,
            metadata: BTreeMap::new(),
        };
        repository
            .publisher
            .create(CommitPublicationV2 {
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

    pub async fn open(plane: Arc<P>, options: RepositoryV2Options) -> Result<Self> {
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
                    "protocol-v2 repository format marker does not exist",
                )
            })?;
        let format: RepositoryFormatV2 = decode_canonical(&stored.bytes)?;
        validate_format_compatibility(&format, &options)?;
        let repository = Self::from_format(plane, options, format)?;
        let branch = repository.options.default_branch.clone();
        repository.locator.register(&branch)?;
        if !repository.options.read_only {
            let permit = repository
                .authority
                .acquire(
                    AuthorityScopeV2::Branch {
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
        options: RepositoryV2Options,
        format: RepositoryFormatV2,
    ) -> Result<Self> {
        let node_cache = options.node_cache.clone().unwrap_or_else(|| {
            Arc::new(MemoryNodeCache::new(options.max_cached_node_bytes)) as Arc<dyn NodeCache>
        });
        let node_store = ProllyObjectStore::new_packed_v2_with_node_cache(
            plane.clone(),
            options.repository_prefix.clone(),
            options.max_cached_node_pack_bytes,
            options.max_cached_node_locations,
            NodeCacheNamespace {
                repository: format.repository_id,
                protocol_version: RepositoryFormatV2::PROLLY_S3_PROTOCOL_VERSION,
                tree_format: tree_format_digest(&format.state_tree_format)?,
            },
            node_cache.clone(),
        );
        let authority = Arc::new(ShardWriterAuthorityV2::new_with_control_retention(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            Duration::from_millis(options.authority_lease_millis),
            options.mutable_control_versions_to_retain,
        )?);
        let publisher = ShardedBranchPublisherV2::new_with_control_retention(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            authority.clone(),
            options.mutable_control_versions_to_retain,
        )?;
        let payloads = ImmutablePayloadStoreV2::new(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
        );
        let tags = TagStoreV2::new_with_control_retention(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            authority.clone(),
            options.mutable_control_versions_to_retain,
        )?;
        let ref_catalog = Arc::new(ShardedRefCatalogV2::new_with_limits(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            format.state_tree_format.clone(),
            node_cache.clone(),
            options.mutable_control_versions_to_retain,
        )?);
        let commit_sessions = CommitSessionStoreV2::new(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            format.canonical_limits.max_mutations_per_commit as usize,
        )?;
        let operation_index = SegmentedOperationIndexV2::new_with_limits(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            format.idempotency_retention,
            options.operation_index_leaf_entries,
            options.operation_index_merge_fanout,
            options.operation_index_max_unindexed_events,
            options.mutable_control_versions_to_retain,
        )?;
        let journal_indexes = Arc::new(JournalDerivedIndexesV2::new_with_limits(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            format.state_tree_format.clone(),
            node_cache,
            options.journal_index_max_unindexed_events,
            options.mutable_control_versions_to_retain,
        )?);
        let locator = Arc::new(JournalNodeLocator {
            indexes: journal_indexes.clone(),
            branches: RwLock::new(BTreeSet::new()),
            imports: RwLock::new(BTreeMap::new()),
        });
        node_store.set_node_locator(locator.clone())?;
        let writable = !options.read_only;
        Ok(Self {
            plane,
            options,
            format,
            node_store,
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
            publication_lanes: std::sync::Mutex::new(BTreeMap::new()),
            index_lanes: std::sync::Mutex::new(BTreeMap::new()),
            local_index_heads: RwLock::new(BTreeMap::new()),
            index_errors: RwLock::new(BTreeMap::new()),
            writable: AtomicBool::new(writable),
        })
    }

    pub fn format(&self) -> &RepositoryFormatV2 {
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

    pub async fn head(&self, branch: &str) -> Result<CommitIdV2> {
        self.locator.register(branch)?;
        Ok(self.publisher.load(branch).await?.value.target)
    }

    pub(crate) async fn start_v1_migration(
        &self,
        source_repository: crate::RepositoryId,
        source_head: CommitId,
        destination_branch: &str,
        job: OperationId,
    ) -> Result<(ImportedJournalIndexStateV2, [u8; 32])> {
        crate::repository::validate_branch(destination_branch)?;
        match self
            .publisher
            .load_including_tombstone(destination_branch)
            .await
        {
            Ok(_) => {
                return Err(Error::new(
                    ErrorCode::RefConflict,
                    "v1 migration destination branch already exists",
                ))
            }
            Err(error) if error.code == ErrorCode::InvalidRevision => {}
            Err(error) => return Err(error),
        }
        let now = self.options.clock.now_millis()?;
        self.active_permit(destination_branch, now).await?;
        let state = self.journal_indexes.start_imported_closure(job)?;
        self.locator.register_import(state.clone())?;
        Ok((
            state,
            self.v1_migration_destination_scope(source_repository, source_head, destination_branch),
        ))
    }

    pub(crate) fn validate_v1_migration_destination(
        &self,
        source_repository: crate::RepositoryId,
        source_head: CommitId,
        destination_branch: &str,
        scope: [u8; 32],
        index: &ImportedJournalIndexStateV2,
    ) -> Result<()> {
        if scope
            != self.v1_migration_destination_scope(
                source_repository,
                source_head,
                destination_branch,
            )
            || index.repository != self.format.repository_id
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "v1 migration cursor belongs to another destination",
            ));
        }
        self.locator.register_import(index.clone())
    }

    fn v1_migration_destination_scope(
        &self,
        source_repository: crate::RepositoryId,
        source_head: CommitId,
        destination_branch: &str,
    ) -> [u8; 32] {
        crate::model::derive_input_digest_v2(&[
            b"v1-to-v2-migration-destination",
            source_repository.as_bytes(),
            source_head.as_bytes(),
            self.format.repository_id.as_bytes(),
            destination_branch.as_bytes(),
        ])
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn import_v1_commit<S: ObjectPlane>(
        &self,
        source_plane: Arc<S>,
        source_repository: crate::RepositoryId,
        source_id: CommitId,
        source_commit: BucketCommitV1,
        mapped_parents: Vec<CommitIdV2>,
        changes: Vec<V1MigrationChangeV2>,
        destination_branch: &str,
        index: &ImportedJournalIndexStateV2,
    ) -> Result<ImportedCommitReceiptV2> {
        crate::repository::validate_branch(destination_branch)?;
        let changes_match_delta = source_commit.delta.changes.len() == changes.len()
            && source_commit
                .delta
                .changes
                .iter()
                .zip(&changes)
                .all(|(transition, change)| {
                    transition.key == change.key
                        && transition.next == change.version.id
                        && transition.delete_marker
                            == matches!(
                                change.version.body.kind,
                                LogicalObjectVersionKindV1::DeleteMarker
                            )
                });
        if source_commit.parents.len() != mapped_parents.len() || !changes_match_delta {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "v1 migration changes do not match the source commit delta",
            ));
        }
        self.locator.register_import(index.clone())?;
        let now = self.options.clock.now_millis()?;
        let permit = self.active_system_permit("v1-migration", now).await?;
        self.authority.validate_active(&permit, now).await?;
        let write_store = self.node_store.isolated_write_session();
        let engine = self.engine(write_store.clone());
        let empty = engine.create();
        let mut parent_commits = Vec::with_capacity(mapped_parents.len());
        for parent in &mapped_parents {
            parent_commits.push(self.load_commit_object(*parent).await?.commit);
        }
        let base = parent_commits.first();
        let mut objects = match &base {
            Some(base) => self.tree_from_root(&base.state.objects)?,
            None => empty.clone(),
        };
        let mut versions = match &base {
            Some(base) => self.tree_from_root(&base.state.versions)?,
            None => empty,
        };
        let mut object_mutations = Vec::with_capacity(changes.len());
        let mut version_mutations = Vec::with_capacity(changes.len());
        let mut transitions = Vec::with_capacity(changes.len());
        let mut payloads = 0usize;
        let mut payload_bytes = 0u64;
        for change in changes {
            self.validate_key(&change.key)?;
            change.version.validate()?;
            let previous = engine
                .get(&objects, &change.key)
                .await?
                .map(|encoded| decode_canonical::<CurrentObjectV2>(&encoded))
                .transpose()?
                .map(|current| current.version.id);
            let operation =
                migration_version_operation(source_repository, &change.key, change.version.id);
            let expected_version = ObjectVersionV2::derive_id(
                self.format.repository_id,
                &change.key,
                operation,
                &change.version.body,
            )?;
            let mut reusable = self
                .find_v2_version_in_tree(&versions, &change.key, expected_version)
                .await?;
            if reusable.is_none() {
                for parent in parent_commits.iter().skip(1) {
                    let parent_versions = self.tree_from_root(&parent.state.versions)?;
                    reusable = self
                        .find_v2_version_in_tree(&parent_versions, &change.key, expected_version)
                        .await?;
                    if reusable.is_some() {
                        break;
                    }
                }
            }
            if reusable
                .as_ref()
                .is_some_and(|version| version.body != change.version.body)
            {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "v1 migration version identity collided with different content",
                ));
            }
            let binding = match (
                &change.version.body.kind,
                &change.version.binding,
                reusable
                    .as_ref()
                    .and_then(|version| version.binding.clone()),
            ) {
                (LogicalObjectVersionKindV1::Live { .. }, _, Some(binding)) => Some(binding),
                (
                    LogicalObjectVersionKindV1::Live {
                        size, checksums, ..
                    },
                    crate::PhysicalObjectBindingV1::Live {
                        version_id,
                        checksum_sha256,
                        ..
                    },
                    None,
                ) => {
                    if checksums.sha256 != Some(*checksum_sha256) {
                        return Err(Error::new(
                            ErrorCode::ChecksumMismatch,
                            "v1 migration source checksum is inconsistent",
                        ));
                    }
                    let spool = tempfile::NamedTempFile::new().map_err(|error| {
                        Error::new(
                            ErrorCode::Transport,
                            format!("could not create v1 migration spool: {error}"),
                        )
                    })?;
                    let path =
                        ObjectPath::new(std::str::from_utf8(&change.key).map_err(|_| {
                            Error::new(ErrorCode::InvalidKey, "v1 migration key is not UTF-8")
                        })?)?;
                    let fetched = source_plane
                        .get_physical_file(crate::PhysicalFileGet {
                            path,
                            version_id: version_id.clone(),
                            body_path: spool.path().to_path_buf(),
                        })
                        .await?;
                    if fetched.size != *size || fetched.checksum_sha256 != *checksum_sha256 {
                        return Err(Error::new(
                            ErrorCode::ChecksumMismatch,
                            "v1 migration source payload failed verification",
                        ));
                    }
                    let binding = self
                        .payloads
                        .put_file(spool.path().to_path_buf(), *size, *checksum_sha256)
                        .await?;
                    payloads = payloads.checked_add(1).ok_or_else(|| {
                        Error::new(
                            ErrorCode::InternalInvariant,
                            "migration payload count overflow",
                        )
                    })?;
                    payload_bytes = payload_bytes.checked_add(*size).ok_or_else(|| {
                        Error::new(
                            ErrorCode::EntityTooLarge,
                            "migration payload bytes overflow",
                        )
                    })?;
                    Some(binding)
                }
                (
                    LogicalObjectVersionKindV1::DeleteMarker,
                    crate::PhysicalObjectBindingV1::DeleteMarker { .. },
                    _,
                ) => None,
                _ => {
                    return Err(Error::new(
                        ErrorCode::CorruptCommit,
                        "v1 migration source version has an invalid binding",
                    ))
                }
            };
            let version = ObjectVersionV2::derive(
                self.format.repository_id,
                &change.key,
                operation,
                change.version.body,
                binding,
            )?;
            let delete_marker =
                matches!(version.body.kind, LogicalObjectVersionKindV1::DeleteMarker);
            object_mutations.push(if delete_marker {
                Mutation::Delete {
                    key: change.key.clone(),
                }
            } else {
                Mutation::Upsert {
                    key: change.key.clone(),
                    val: encode_canonical(&CurrentObjectV2 {
                        version: version.clone(),
                    })?,
                }
            });
            version_mutations.push(Mutation::Upsert {
                key: version_tree_key(&change.key, version.body.order, version.id),
                val: encode_canonical(&version)?,
            });
            transitions.push(ObjectTransitionV2 {
                key: change.key,
                previous,
                next: version.id,
                delete_marker,
            });
        }
        objects = engine.batch(&objects, object_mutations).await?;
        versions = engine.batch(&versions, version_mutations).await?;
        let prepared = write_store.prepare_node_pack(
            tree_format_digest(&self.format.state_tree_format)?,
            Vec::new(),
        )?;
        let commit = BucketCommitV2 {
            state: BucketStateV2 {
                objects: TreeRootV1::from_tree(&objects)?,
                versions: TreeRootV1::from_tree(&versions)?,
            },
            parents: mapped_parents,
            generation: source_commit.generation,
            delta: BucketDeltaV2 {
                input_digest: crate::model::derive_input_digest_v2(&[
                    b"v1-to-v2-migration",
                    source_repository.as_bytes(),
                    source_id.as_bytes(),
                ]),
                changes: transitions,
                changes_root: None,
                change_count: 0,
            },
            node_pack: prepared.as_ref().map(PreparedNodePack::reference),
            authority: permit.stamp(),
            author: source_commit.author,
            message: source_commit.message,
            created_at_millis: source_commit.created_at_millis,
            metadata: source_commit.metadata,
        };
        let destination = self
            .publisher
            .store_unpublished_commit(
                &permit,
                &commit,
                prepared.as_ref().map(PreparedNodePack::pack).cloned(),
                now,
            )
            .await?;
        self.finalize_pack(destination, &commit, prepared).await?;
        let next_index = self
            .journal_indexes
            .index_imported_commit(&self.publisher, index, destination)
            .await?;
        self.locator.register_import(next_index.clone())?;
        Ok(ImportedCommitReceiptV2 {
            source: source_id,
            destination,
            index: next_index,
            payloads,
            payload_bytes,
        })
    }

    pub(crate) async fn finish_v1_migration(
        &self,
        source_repository: crate::RepositoryId,
        source_head: CommitId,
        destination_branch: &str,
        destination_head: CommitIdV2,
        destination_scope: [u8; 32],
        index: &ImportedJournalIndexStateV2,
    ) -> Result<BranchHeadV2> {
        self.validate_v1_migration_destination(
            source_repository,
            source_head,
            destination_branch,
            destination_scope,
            index,
        )?;
        let branch = match self.publisher.load(destination_branch).await {
            Ok(reference) if reference.value.target == destination_head => BranchHeadV2 {
                name: destination_branch.to_string(),
                target: reference.value.target,
                generation: reference.value.generation,
            },
            Ok(_) => {
                return Err(Error::new(
                    ErrorCode::RefConflict,
                    "v1 migration destination branch points to another commit",
                ))
            }
            Err(error) if error.code == ErrorCode::InvalidRevision => {
                self.create_branch(destination_branch, destination_head)
                    .await?
            }
            Err(error) => return Err(error),
        };
        let now = self.options.clock.now_millis()?;
        self.journal_indexes
            .publish_imported_closure(&self.publisher, destination_branch, index, now)
            .await?;
        let reference = self.publisher.load(destination_branch).await?;
        self.record_branch_catalog(&reference).await?;
        self.mark_local_index_head(destination_branch, destination_head)?;
        self.locator.register(destination_branch)?;
        self.locator.remove_import(index.job)?;
        Ok(branch)
    }

    pub(crate) fn abandon_v1_migration(
        &self,
        source_repository: crate::RepositoryId,
        source_head: CommitId,
        destination_branch: &str,
        destination_scope: [u8; 32],
        index: &ImportedJournalIndexStateV2,
    ) -> Result<()> {
        self.validate_v1_migration_destination(
            source_repository,
            source_head,
            destination_branch,
            destination_scope,
            index,
        )?;
        self.locator.remove_import(index.job)
    }

    pub async fn create_branch(&self, name: &str, from: CommitIdV2) -> Result<BranchHeadV2> {
        crate::repository::validate_branch(name)?;
        let _lane = self.lock_branch(name).await;
        self.load_commit_object(from).await?;
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
        self.advance_branch_indexes(name).await?;
        Ok(BranchHeadV2 {
            name: name.to_string(),
            target: reference.value.target,
            generation: reference.value.generation,
        })
    }

    pub async fn delete_branch(&self, name: &str, expected: CommitIdV2) -> Result<()> {
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
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 local-index lock poisoned"))?
            .remove(name);
        Ok(())
    }

    pub async fn tag(&self, name: &str) -> Result<TagV2> {
        let loaded = self.tags.load(name).await?;
        Ok(TagV2 {
            name: name.to_string(),
            target: loaded.value.target,
            generation: loaded.value.generation,
        })
    }

    pub async fn create_tag(&self, name: &str, target: CommitIdV2) -> Result<TagV2> {
        crate::repository::validate_branch(name)?;
        let _lane = self.lock_branch(&format!("tag:{name}")).await;
        self.load_commit_object(target).await?;
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
        Ok(TagV2 {
            name: name.to_string(),
            target,
            generation: tag.value.generation,
        })
    }

    pub async fn delete_tag(&self, name: &str, expected: CommitIdV2) -> Result<()> {
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

    pub async fn list_branch_catalog_page(
        &self,
        cursor: Option<RefCatalogCursorV2>,
        limit: usize,
    ) -> Result<BranchCatalogPageV2> {
        let page = self
            .ref_catalog
            .list(RefKindV2::Branch, cursor, limit)
            .await?;
        Ok(BranchCatalogPageV2 {
            branches: page
                .entries
                .into_iter()
                .map(|entry| BranchHeadV2 {
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
        cursor: Option<RefCatalogCursorV2>,
        limit: usize,
    ) -> Result<TagCatalogPageV2> {
        let page = self.ref_catalog.list(RefKindV2::Tag, cursor, limit).await?;
        Ok(TagCatalogPageV2 {
            tags: page
                .entries
                .into_iter()
                .map(|entry| TagV2 {
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
        kind: RefKindV2,
        continuation: Option<String>,
        limit: usize,
    ) -> Result<RefCatalogRepairPageV2> {
        if !(1..=1_000).contains(&limit) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "v2 ref-catalog repair page must contain 1 to 1,000 refs",
            ));
        }
        let namespace = match kind {
            RefKindV2::Branch => "heads",
            RefKindV2::Tag => "tags",
        };
        let prefix = format!("{}/refs/v2/{namespace}/", self.options.repository_prefix);
        let page = self
            .plane
            .list(ListRequest {
                prefix: prefix.clone(),
                continuation,
                limit,
                include_versions: false,
            })
            .await?;
        let mut report = RefCatalogRepairPageV2 {
            continuation: page.continuation,
            ..RefCatalogRepairPageV2::default()
        };
        for entry in page.entries {
            report.scanned += 1;
            let encoded_name = entry.path.as_str().strip_prefix(&prefix).ok_or_else(|| {
                Error::new(
                    ErrorCode::CorruptCommit,
                    "v2 ref repair escaped its namespace",
                )
            })?;
            let name = String::from_utf8(hex::decode(encoded_name).map_err(|_| {
                Error::new(
                    ErrorCode::CorruptCommit,
                    "v2 ref path name is not canonical hex",
                )
            })?)
            .map_err(|_| Error::new(ErrorCode::CorruptCommit, "v2 ref path name is not UTF-8"))?;
            let Some(stored) = self.plane.load_mutable(&entry.path).await? else {
                continue;
            };
            match kind {
                RefKindV2::Branch => {
                    let value: crate::RefValueV2 = decode_canonical(&stored.bytes)?;
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
                RefKindV2::Tag => {
                    let value: crate::TagValueV2 = decode_canonical(&stored.bytes)?;
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
                "protocol-v2 takeover requires a read-only repository handle",
            ));
        }
        let _lane = self.lock_branch(branch).await;
        let now = self.options.clock.now_millis()?;
        let pending = self
            .authority
            .begin_takeover(TakeoverRequestV2 {
                scope: AuthorityScopeV2::Branch {
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
    ) -> Result<PhysicalBatchV2> {
        crate::repository::validate_branch(branch)?;
        let message = message.into();
        let now = self.options.clock.now_millis()?;
        let expires_at_millis = now.checked_add(expires_after_millis).ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidLimit,
                "protocol-v2 commit-session expiry overflow",
            )
        })?;
        let permit = self.active_permit(branch, now).await?;
        let session = PhysicalBatchV2 {
            id: self.options.ids.batch(),
            branch: branch.to_string(),
            base_commit: self.publisher.load(branch).await?.value.target,
            identity: PhysicalMutationIdentityV2 {
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
    ) -> Result<CommitSessionCheckpointV2> {
        let session = self
            .begin_commit_session(branch, message, expires_after_millis)
            .await?;
        let checkpoint = CommitSessionCheckpointV2 {
            session,
            sequence: 0,
            mutations: Vec::new(),
            state: CommitSessionStateV2::Open,
        };
        self.commit_sessions.save(&checkpoint).await?;
        Ok(checkpoint)
    }

    pub async fn checkpoint_commit_session(
        &self,
        session: &PhysicalBatchV2,
        mutations: Vec<StagedMutationV2>,
        sequence: u64,
    ) -> Result<CommitSessionCheckpointV2> {
        self.validate_commit_session(session).await?;
        let checkpoint = CommitSessionCheckpointV2 {
            session: session.clone(),
            sequence,
            mutations: self.canonical_session_mutations(mutations, true)?,
            state: CommitSessionStateV2::Open,
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
    ) -> Result<CommitSessionCheckpointV2> {
        let mut checkpoint = self.commit_sessions.latest(batch).await?.ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidRequest,
                "protocol-v2 commit session does not exist",
            )
        })?;
        if checkpoint.state != CommitSessionStateV2::Open {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "protocol-v2 commit session was aborted",
            ));
        }
        let now = self.options.clock.now_millis()?;
        if checkpoint.session.expires_at_millis < now {
            return Err(Error::new(
                ErrorCode::BatchExpired,
                "protocol-v2 commit session expired",
            ));
        }
        let permit = self.active_permit(&checkpoint.session.branch, now).await?;
        if checkpoint.session.identity.authority.writer_id != permit.stamp().writer_id {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "another writer cannot adopt a durable protocol-v2 commit session",
            )
            .operation(checkpoint.session.identity.operation.to_string()));
        }
        let current = self.publisher.load(&checkpoint.session.branch).await?;
        if current.value.target != checkpoint.session.base_commit {
            return Err(Error::new(
                ErrorCode::BatchConflict,
                "protocol-v2 branch moved since the durable session checkpoint",
            )
            .operation(checkpoint.session.identity.operation.to_string()));
        }
        if checkpoint.session.identity.authority != permit.stamp() {
            checkpoint.sequence = checkpoint.sequence.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InvalidLimit, "v2 checkpoint sequence overflow")
            })?;
            checkpoint.session.identity.authority = permit.stamp();
            self.commit_sessions.save(&checkpoint).await?;
        }
        Ok(checkpoint)
    }

    pub async fn abort_commit_session(
        &self,
        session: PhysicalBatchV2,
        mutations: Vec<StagedMutationV2>,
        sequence: u64,
    ) -> Result<()> {
        self.validate_commit_session(&session).await?;
        let checkpoint = CommitSessionCheckpointV2 {
            session,
            sequence,
            mutations: self.canonical_session_mutations(mutations, true)?,
            state: CommitSessionStateV2::Aborted,
        };
        self.commit_sessions.save(&checkpoint).await
    }

    pub async fn cleanup_expired_commit_sessions(
        &self,
        continuation: Option<String>,
        limit: usize,
    ) -> Result<CommitSessionCleanupReportV2> {
        self.commit_sessions
            .cleanup_expired_page(self.options.clock.now_millis()?, continuation, limit)
            .await
    }

    pub async fn stage_commit_session_put(
        &self,
        session: &PhysicalBatchV2,
        key: Vec<u8>,
        bytes: Vec<u8>,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<StagedMutationV2> {
        self.validate_commit_session(session).await?;
        self.validate_key(&key)?;
        if bytes.len() as u64 > self.format.canonical_limits.max_object_bytes {
            return Err(Error::new(
                ErrorCode::EntityTooLarge,
                "v2 object exceeds the repository object-size limit",
            ));
        }
        let size = bytes.len() as u64;
        let checksum_md5: [u8; 16] = Md5::digest(&bytes).into();
        let checksum_sha256: [u8; 32] = Sha256::digest(&bytes).into();
        let binding = self.payloads.put(bytes).await?;
        Ok(StagedMutationV2 {
            body: StagedMutationBodyV2::Put(Box::new(StagedPutV2 {
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
                binding,
            })),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn stage_commit_session_file(
        &self,
        session: &PhysicalBatchV2,
        key: Vec<u8>,
        body_path: PathBuf,
        size: u64,
        checksum_sha256: [u8; 32],
        checksum_md5: [u8; 16],
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<StagedMutationV2> {
        self.validate_commit_session(session).await?;
        self.validate_key(&key)?;
        if size > self.format.canonical_limits.max_object_bytes {
            return Err(Error::new(
                ErrorCode::EntityTooLarge,
                "v2 object exceeds the repository object-size limit",
            ));
        }
        let binding = self
            .payloads
            .put_file(body_path, size, checksum_sha256)
            .await?;
        Ok(StagedMutationV2 {
            body: StagedMutationBodyV2::Put(Box::new(StagedPutV2 {
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
                binding,
            })),
        })
    }

    pub async fn publish_commit_session(
        &self,
        session: PhysicalBatchV2,
        mutations: Vec<StagedMutationV2>,
    ) -> Result<CommitReceiptV2> {
        session.validate(self.format.repository_id)?;
        if session.expires_at_millis < self.options.clock.now_millis()? {
            return Err(Error::new(
                ErrorCode::BatchExpired,
                "protocol-v2 commit session is expired",
            ));
        }
        let canonical_mutations = self.canonical_session_mutations(mutations, false)?;
        let ordered = canonical_mutations
            .iter()
            .cloned()
            .map(|mutation| (mutation.key().to_vec(), mutation))
            .collect::<BTreeMap<_, _>>();
        let input_digest = crate::model::derive_input_digest_v2(&[
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
                "protocol-v2 commit session belongs to another authority epoch",
            ));
        }
        self.require_branch_indexes_ready(&session.branch).await?;
        let current = self.publisher.load(&session.branch).await?;
        if current.value.target != session.base_commit {
            return Err(Error::new(
                ErrorCode::BatchConflict,
                "protocol-v2 branch moved since commit-session creation",
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
            Error::new(
                ErrorCode::InternalInvariant,
                "v2 commit generation overflow",
            )
        })?);
        let mut object_mutations = Vec::with_capacity(ordered.len());
        let mut version_mutations = Vec::with_capacity(ordered.len());
        let mut transitions = Vec::with_capacity(ordered.len());
        let mut object_versions = Vec::with_capacity(ordered.len());
        for (ordinal, ((key, mutation), previous)) in
            ordered.iter().zip(previous_values).enumerate()
        {
            let previous = previous
                .map(|encoded| decode_canonical::<CurrentObjectV2>(&encoded))
                .transpose()?
                .map(|current| current.version.id);
            let (kind, binding) = match &mutation.body {
                StagedMutationBodyV2::Put(staged) => {
                    let StagedPutV2 {
                        size,
                        logical_etag,
                        checksums,
                        headers,
                        user_metadata,
                        binding,
                        ..
                    } = staged.as_ref();
                    (
                        LogicalObjectVersionKindV1::Live {
                            size: *size,
                            logical_etag: logical_etag.clone(),
                            headers: headers.clone(),
                            checksums: checksums.clone(),
                            user_metadata: user_metadata.clone(),
                            tags: BTreeMap::new(),
                        },
                        Some(binding.clone()),
                    )
                }
                StagedMutationBodyV2::Delete { .. } => {
                    (LogicalObjectVersionKindV1::DeleteMarker, None)
                }
            };
            let version = ObjectVersionV2::derive(
                self.format.repository_id,
                key,
                session.identity.operation,
                LogicalObjectVersionBodyV1 {
                    order: ObjectVersionOrder {
                        commit_generation: generation,
                        mutation_ordinal: u32::try_from(ordinal).map_err(|_| {
                            Error::new(ErrorCode::InvalidLimit, "v2 mutation ordinal overflow")
                        })?,
                    },
                    created_at_millis: now,
                    kind,
                },
                binding,
            )?;
            let delete_marker =
                matches!(version.body.kind, LogicalObjectVersionKindV1::DeleteMarker);
            if delete_marker {
                object_mutations.push(Mutation::Delete { key: key.clone() });
            } else {
                object_mutations.push(Mutation::Upsert {
                    key: key.clone(),
                    val: encode_canonical(&CurrentObjectV2 {
                        version: version.clone(),
                    })?,
                });
            }
            version_mutations.push(Mutation::Upsert {
                key: version_tree_key(key, version.body.order, version.id),
                val: encode_canonical(&version)?,
            });
            transitions.push(ObjectTransitionV2 {
                key: key.clone(),
                previous,
                next: version.id,
                delete_marker,
            });
            object_versions.push(version.id);
        }
        let objects = engine.batch(&objects, object_mutations).await?;
        let versions = engine.batch(&versions, version_mutations).await?;
        let prepared = write_store.prepare_node_pack(
            tree_format_digest(&self.format.state_tree_format)?,
            Vec::new(),
        )?;
        let commit = BucketCommitV2 {
            state: BucketStateV2 {
                objects: TreeRootV1::from_tree(&objects)?,
                versions: TreeRootV1::from_tree(&versions)?,
            },
            parents: vec![current.value.target],
            generation,
            delta: BucketDeltaV2 {
                input_digest,
                changes: transitions,
                changes_root: None,
                change_count: 0,
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
                CommitPublicationV2 {
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
                Ok(CommitReceiptV2 {
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
    ) -> Result<CommitReceiptV2> {
        let operation = self.options.ids.operation();
        self.put_object_with_operation(branch, key, bytes, headers, user_metadata, operation)
            .await
    }

    async fn validate_commit_session(&self, session: &PhysicalBatchV2) -> Result<()> {
        session.validate(self.format.repository_id)?;
        let now = self.options.clock.now_millis()?;
        if session.expires_at_millis < now {
            return Err(Error::new(
                ErrorCode::BatchExpired,
                "protocol-v2 commit session expired",
            ));
        }
        let permit = self.active_permit(&session.branch, now).await?;
        if permit.stamp() != session.identity.authority {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "protocol-v2 commit session belongs to another authority epoch",
            ));
        }
        Ok(())
    }

    fn canonical_session_mutations(
        &self,
        mutations: Vec<StagedMutationV2>,
        allow_empty: bool,
    ) -> Result<Vec<StagedMutationV2>> {
        if (!allow_empty && mutations.is_empty())
            || mutations.len() > self.format.canonical_limits.max_mutations_per_commit as usize
        {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "protocol-v2 commit session has an invalid mutation count",
            ));
        }
        let mut ordered = BTreeMap::new();
        for mutation in mutations {
            self.validate_key(mutation.key())?;
            if let StagedMutationBodyV2::Put(staged) = &mutation.body {
                self.validate_staged_put(staged)?;
            }
            if ordered.insert(mutation.key().to_vec(), mutation).is_some() {
                return Err(Error::new(
                    ErrorCode::InvalidRequest,
                    "protocol-v2 commit session contains the same key more than once",
                ));
            }
        }
        Ok(ordered.into_values().collect())
    }

    fn validate_staged_put(&self, staged: &StagedPutV2) -> Result<()> {
        staged.binding.validate()?;
        let expected_etag = staged
            .checksums
            .md5
            .map(|md5| format!("\"{}\"", hex::encode(md5)));
        if staged.size > self.format.canonical_limits.max_object_bytes
            || staged.binding.path != self.payloads.path(staged.binding.checksum_sha256)?
            || staged.checksums.sha256 != Some(staged.binding.checksum_sha256)
            || expected_etag.as_deref() != Some(staged.logical_etag.as_str())
        {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "staged v2 payload identity does not match its immutable binding",
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
    ) -> Result<CommitReceiptV2> {
        if !self.writable.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "protocol-v2 repository is read-only",
            ));
        }
        self.validate_key(&key)?;
        if bytes.len() as u64 > self.format.canonical_limits.max_object_bytes {
            return Err(Error::new(
                ErrorCode::EntityTooLarge,
                "v2 object exceeds the repository object-size limit",
            ));
        }
        let metadata_bytes = encode_canonical(&user_metadata)?;
        let headers_bytes = encode_canonical(&headers)?;
        let size = bytes.len() as u64;
        let checksum_md5: [u8; 16] = Md5::digest(&bytes).into();
        let checksum_sha256: [u8; 32] = Sha256::digest(&bytes).into();
        let input_digest = crate::model::derive_input_digest_v2(&[
            b"put",
            branch.as_bytes(),
            &key,
            &checksum_sha256,
            &headers_bytes,
            &metadata_bytes,
        ]);
        let _lane = self.lock_branch(branch).await;
        let now = self.options.clock.now_millis()?;
        if let Some(receipt) = self
            .reconcile_operation(branch, operation, input_digest, now)
            .await?
        {
            return Ok(receipt);
        }
        let permit = self.active_permit(branch, now).await?;
        // The authority check happens before payload bytes enter the object
        // plane. A stale process therefore fails before its payload PUT.
        self.authority.validate_active(&permit, now).await?;
        self.require_branch_indexes_ready(branch).await?;
        let binding = self.payloads.put(bytes).await?;

        let current = self.publisher.load(branch).await?;
        let base = self.load_commit_object(current.value.target).await?.commit;
        let write_store = self.node_store.isolated_write_session();
        let engine = self.engine(write_store.clone());
        let mut objects = self.tree_from_root(&base.state.objects)?;
        let mut versions = self.tree_from_root(&base.state.versions)?;
        let previous = engine
            .get(&objects, &key)
            .await?
            .map(|encoded| decode_canonical::<CurrentObjectV2>(&encoded))
            .transpose()?
            .map(|current| current.version.id);
        let generation = CommitGeneration(base.generation.0.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "v2 commit generation overflow",
            )
        })?);
        let body = LogicalObjectVersionBodyV1 {
            order: ObjectVersionOrder {
                commit_generation: generation,
                mutation_ordinal: 0,
            },
            created_at_millis: now,
            kind: LogicalObjectVersionKindV1::Live {
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
        let version = ObjectVersionV2::derive(
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
                encode_canonical(&CurrentObjectV2 {
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
        let commit = BucketCommitV2 {
            state: BucketStateV2 {
                objects: TreeRootV1::from_tree(&objects)?,
                versions: TreeRootV1::from_tree(&versions)?,
            },
            parents: vec![current.value.target],
            generation,
            delta: BucketDeltaV2 {
                input_digest,
                changes: vec![ObjectTransitionV2 {
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
                CommitPublicationV2 {
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
        Ok(CommitReceiptV2 {
            id: published.value.target,
            operation,
            branch: branch.to_string(),
            parents: commit.parents,
            changed_keys: 1,
            object_versions: vec![version.id],
            idempotent_replay: false,
        })
    }

    pub async fn get_object(&self, branch: &str, key: &[u8]) -> Result<Option<ObjectDataV2>> {
        self.validate_key(key)?;
        self.locator.register(branch)?;
        let reference = self.publisher.load(branch).await?;
        self.require_branch_indexes_ready_for(branch, &reference)
            .await?;
        let commit = self
            .load_commit_object(reference.value.target)
            .await?
            .commit;
        let objects = self.tree_from_root(&commit.state.objects)?;
        let Some(encoded) = self
            .engine(self.node_store.clone())
            .get(&objects, key)
            .await?
        else {
            return Ok(None);
        };
        let current: CurrentObjectV2 = decode_canonical(&encoded)?;
        current.version.validate()?;
        let binding = current.version.binding.as_ref().ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "live v2 object has no immutable payload binding",
            )
        })?;
        let bytes = self.payloads.get(binding).await?;
        Ok(Some(ObjectDataV2 {
            key: key.to_vec(),
            version: current.version,
            bytes,
            snapshot: reference.value.target,
        }))
    }

    pub async fn get_object_at(
        &self,
        branch: &str,
        snapshot: CommitIdV2,
        key: &[u8],
    ) -> Result<Option<ObjectDataV2>> {
        self.validate_key(key)?;
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        let commit = self.load_commit_object(snapshot).await?.commit;
        let objects = self.tree_from_root(&commit.state.objects)?;
        let Some(encoded) = self
            .engine(self.node_store.clone())
            .get(&objects, key)
            .await?
        else {
            return Ok(None);
        };
        let current: CurrentObjectV2 = decode_canonical(&encoded)?;
        current.version.validate()?;
        let binding = current.version.binding.as_ref().ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptCommit,
                "live historical v2 object has no immutable payload binding",
            )
        })?;
        let bytes = self.payloads.get(binding).await?;
        Ok(Some(ObjectDataV2 {
            key: key.to_vec(),
            version: current.version,
            bytes,
            snapshot,
        }))
    }

    pub async fn delete_object(&self, branch: &str, key: Vec<u8>) -> Result<CommitReceiptV2> {
        let operation = self.options.ids.operation();
        self.delete_object_with_operation(branch, key, operation)
            .await
    }

    pub async fn delete_object_with_operation(
        &self,
        branch: &str,
        key: Vec<u8>,
        operation: OperationId,
    ) -> Result<CommitReceiptV2> {
        if !self.writable.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "protocol-v2 repository is read-only",
            ));
        }
        self.validate_key(&key)?;
        let input_digest =
            crate::model::derive_input_digest_v2(&[b"delete", branch.as_bytes(), &key]);
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
            .map(|encoded| decode_canonical::<CurrentObjectV2>(&encoded))
            .transpose()?
            .map(|current| current.version.id);
        let generation = CommitGeneration(base.generation.0.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "v2 commit generation overflow",
            )
        })?);
        let version = ObjectVersionV2::derive(
            self.format.repository_id,
            &key,
            operation,
            LogicalObjectVersionBodyV1 {
                order: ObjectVersionOrder {
                    commit_generation: generation,
                    mutation_ordinal: 0,
                },
                created_at_millis: now,
                kind: LogicalObjectVersionKindV1::DeleteMarker,
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
        let commit = BucketCommitV2 {
            state: BucketStateV2 {
                objects: TreeRootV1::from_tree(&objects)?,
                versions: TreeRootV1::from_tree(&versions)?,
            },
            parents: vec![current.value.target],
            generation,
            delta: BucketDeltaV2 {
                input_digest,
                changes: vec![ObjectTransitionV2 {
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
                CommitPublicationV2 {
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
        Ok(CommitReceiptV2 {
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
    ) -> Result<(CommitIdV2, Vec<ObjectSummaryV2>, bool)> {
        std::str::from_utf8(prefix)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "v2 list prefix is not UTF-8"))?;
        let snapshot = self.head(branch).await?;
        let (objects, truncated) = self
            .list_objects_at(branch, snapshot, prefix, after, limit)
            .await?;
        Ok((snapshot, objects, truncated))
    }

    pub async fn list_objects_at(
        &self,
        branch: &str,
        snapshot: CommitIdV2,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<ObjectSummaryV2>, bool)> {
        std::str::from_utf8(prefix)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "v2 list prefix is not UTF-8"))?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        self.locator.register(branch)?;
        self.require_branch_indexes_ready(branch).await?;
        let commit = self.load_commit_object(snapshot).await?.commit;
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
            let current: CurrentObjectV2 = decode_canonical(&encoded)?;
            current.version.validate()?;
            result.push(ObjectSummaryV2 {
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
    ) -> Result<(CommitIdV2, Vec<ObjectVersionV2>)> {
        self.validate_key(key)?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        let snapshot = self.head(branch).await?;
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
            let version: ObjectVersionV2 = decode_canonical(&encoded)?;
            version.validate()?;
            result.push(version);
        }
        Ok((snapshot, result))
    }

    pub async fn list_versions_prefix(
        &self,
        branch: &str,
        prefix: &[u8],
        limit: usize,
    ) -> Result<(CommitIdV2, Vec<VersionSummaryV2>)> {
        std::str::from_utf8(prefix)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "v2 version prefix is not UTF-8"))?;
        let snapshot = self.head(branch).await?;
        let (versions, _) = self
            .list_versions_at(branch, snapshot, prefix, None, limit)
            .await?;
        Ok((snapshot, versions))
    }

    pub async fn list_versions_at(
        &self,
        branch: &str,
        snapshot: CommitIdV2,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<VersionSummaryV2>, bool)> {
        std::str::from_utf8(prefix)
            .map_err(|_| Error::new(ErrorCode::InvalidKey, "v2 version prefix is not UTF-8"))?;
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
            let version: ObjectVersionV2 = decode_canonical(&encoded)?;
            version.validate()?;
            result.push(VersionSummaryV2 {
                key,
                version,
                cursor: encoded_key,
            });
        }
        let truncated = result.len() > limit;
        result.truncate(limit);
        Ok((result, truncated))
    }

    /// Start a durable native-v2 merge between two branch snapshots.
    ///
    /// The returned cursor is process-independent. Callers must persist the
    /// cursor returned by every successful `advance_merge` call before
    /// discarding the previous one.
    pub async fn start_merge(
        &self,
        target_branch: &str,
        source_branch: &str,
        requested_base: Option<CommitIdV2>,
        policy: MergePolicyV2,
        message: impl Into<String>,
    ) -> Result<MergeCursorV2> {
        crate::repository::validate_branch(target_branch)?;
        crate::repository::validate_branch(source_branch)?;
        if target_branch == source_branch {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "v2 merge source and target branches must differ",
            ));
        }
        let message = message.into();
        if message.trim().is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "v2 merge message is empty",
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
        let mut cursor = MergeCursorV2 {
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
            phase: MergePhaseV2::DiscoveringBases,
            plan_root: TreeRootV1::from_tree(&tree)?,
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
            .is_first_parent_ancestor_v2(target_branch, source_branch, ours, theirs)
            .await?
        {
            Some(ours)
        } else if self
            .is_first_parent_ancestor_v2(target_branch, source_branch, theirs, ours)
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
            cursor.plan_root = TreeRootV1::from_tree(&tree)?;
            cursor.best_base_count = 1;
            cursor.selected_base = Some(base);
            self.validate_requested_merge_base(&cursor, base)?;
            cursor.phase = MergePhaseV2::Planning;
            self.seal_merge_cursor(&mut cursor).await?;
            return Ok(cursor);
        }

        let mut mutations = Vec::new();
        self.seed_merge_frontier(&mut mutations, ours_entry, MERGE_LEFT)?;
        self.seed_merge_frontier(&mut mutations, theirs_entry, MERGE_RIGHT)?;
        tree = engine.batch(&tree, mutations).await?;
        cursor.plan_root = TreeRootV1::from_tree(&tree)?;
        self.seal_merge_cursor(&mut cursor).await?;
        Ok(cursor)
    }

    /// Advance a durable merge by at most `max_steps` graph or tree records.
    pub async fn advance_merge(
        &self,
        cursor: &MergeCursorV2,
        max_steps: usize,
    ) -> Result<MergeAdvancePageV2> {
        if !(1..=10_000).contains(&max_steps) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "v2 merge advance must process 1 to 10,000 records",
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
                MergePhaseV2::DiscoveringBases => self.advance_merge_base_one(&mut next).await?,
                MergePhaseV2::CollectingBases => self.collect_merge_base_one(&mut next).await?,
                MergePhaseV2::Planning => {
                    self.advance_merge_plan(
                        &mut next,
                        max_steps - processed,
                        &mut changes,
                        &mut conflicts,
                    )
                    .await?
                }
                MergePhaseV2::BuildingVersions => {
                    self.advance_merge_version_union(&mut next, max_steps - processed)
                        .await?
                }
                MergePhaseV2::BuildingObjects => {
                    self.advance_merge_object_build(&mut next, max_steps - processed)
                        .await?
                }
                MergePhaseV2::AwaitingBase
                | MergePhaseV2::Conflicted
                | MergePhaseV2::ReadyToPublish => 0,
            };
            processed = processed.checked_add(advanced).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "v2 merge work counter overflow",
                )
            })?;
            if advanced == 0 && next.phase == before {
                break;
            }
        }
        self.seal_merge_cursor(&mut next).await?;
        Ok(MergeAdvancePageV2 {
            cursor: next,
            processed,
            changes,
            conflicts,
        })
    }

    /// Select one of several best merge bases discovered by the frontier.
    pub async fn select_merge_base(
        &self,
        cursor: &MergeCursorV2,
        base: CommitIdV2,
    ) -> Result<MergeCursorV2> {
        self.validate_merge_cursor(cursor).await?;
        if cursor.phase != MergePhaseV2::AwaitingBase {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "v2 merge is not awaiting an explicit merge base",
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
                "selected v2 merge base is not a best common ancestor",
            ));
        }
        let mut next = cursor.clone();
        next.selected_base = Some(base);
        next.phase = MergePhaseV2::Planning;
        self.seal_merge_cursor(&mut next).await?;
        Ok(next)
    }

    pub async fn merge_changes_page(
        &self,
        cursor: &MergeCursorV2,
        continuation: Option<&MergeChangeCursorV2>,
        limit: usize,
    ) -> Result<MergeChangePageV2> {
        self.merge_change_page(cursor, continuation, limit).await
    }

    pub async fn merge_bases_page(
        &self,
        cursor: &MergeCursorV2,
        continuation: Option<&MergeBaseCursorV2>,
        limit: usize,
    ) -> Result<MergeBasePageV2> {
        self.merge_base_page(cursor, continuation, limit).await
    }

    pub async fn merge_conflicts_page(
        &self,
        cursor: &MergeCursorV2,
        continuation: Option<&MergeConflictCursorV2>,
        limit: usize,
    ) -> Result<MergeConflictPageV2> {
        self.merge_conflict_page(cursor, continuation, limit).await
    }

    /// CAS-publish a completely built merge plan. Replaying this call with the
    /// same cursor and operation ID reconciles an ambiguous prior publication.
    pub async fn publish_merge(&self, cursor: &MergeCursorV2) -> Result<MergeReceiptV2> {
        self.validate_merge_cursor(cursor).await?;
        if cursor.phase != MergePhaseV2::ReadyToPublish {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "v2 merge plan is not ready to publish",
            ));
        }
        if cursor.policy == MergePolicyV2::Fail && cursor.conflicts != 0 {
            return Err(Error::new(
                ErrorCode::MergeConflict,
                "v2 merge plan contains unresolved conflicts",
            ));
        }
        let base = cursor.selected_base.ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "v2 merge plan has no selected base",
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
                "v2 merge target moved after planning",
            ));
        }
        let permit = self.active_permit(&cursor.target_branch, now).await?;
        let ours = self.load_commit_object(cursor.ours).await?.commit;
        let theirs = self.load_commit_object(cursor.theirs).await?.commit;
        let generation = CommitGeneration(
            ours.generation
                .0
                .max(theirs.generation.0)
                .checked_add(1)
                .ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "v2 merge generation overflow")
                })?,
        );
        let commit = BucketCommitV2 {
            state: BucketStateV2 {
                objects: cursor.final_objects.clone().ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "v2 merge object root is absent",
                    )
                })?,
                versions: cursor.final_versions.clone().ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "v2 merge version root is absent",
                    )
                })?,
            },
            parents: vec![cursor.ours, cursor.theirs],
            generation,
            delta: BucketDeltaV2 {
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
                CommitPublicationV2 {
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
                Ok(MergeReceiptV2 {
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
        cursor: &MergeCursorV2,
        continuation: Option<&MergeCleanupCursorV2>,
        limit: usize,
    ) -> Result<MergeCleanupPageV2> {
        if continuation.is_none() {
            self.validate_merge_cursor(cursor).await?;
        } else if cursor.repository != self.format.repository_id || cursor.job.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "v2 merge cleanup cursor belongs to another repository",
            ));
        }
        if !(1..=1_000).contains(&limit) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "v2 merge cleanup page must contain 1 to 1,000 objects",
            ));
        }
        if continuation.is_some_and(|continuation| {
            continuation.repository != cursor.repository || continuation.job != cursor.job
        }) {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "v2 merge cleanup cursor belongs to another job",
            ));
        }
        let prefix = format!(
            "{}/administration/v2/merge/{}/plan/",
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
                        "v2 merge cleanup object changed concurrently",
                    ))
                }
            }
        }
        Ok(MergeCleanupPageV2 {
            deleted,
            continuation: page
                .continuation
                .map(|provider_continuation| MergeCleanupCursorV2 {
                    repository: cursor.repository,
                    job: cursor.job,
                    provider_continuation,
                }),
        })
    }

    pub async fn advance_branch_indexes(&self, branch: &str) -> Result<BranchIndexAdvanceReportV2> {
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
        let report = BranchIndexAdvanceReportV2 {
            operations,
            journal,
        };
        self.index_errors
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 index-error lock poisoned"))?
            .remove(branch);
        let reference = self.publisher.load(branch).await?;
        self.record_branch_catalog(&reference).await?;
        Ok(report)
    }

    pub async fn start_branch_index_rebuild(
        &self,
        branch: &str,
    ) -> Result<JournalIndexRebuildCursorV2> {
        self.locator.register(branch)?;
        let _lane = self.lock_index_branch(branch).await;
        self.journal_indexes
            .start_rebuild(&self.publisher, branch, self.options.ids.operation())
            .await
    }

    pub async fn advance_branch_index_rebuild(
        &self,
        cursor: &JournalIndexRebuildCursorV2,
        max_events: usize,
    ) -> Result<JournalIndexRebuildStepV2> {
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
                .map_err(|_| {
                    Error::new(ErrorCode::InternalInvariant, "v2 index-error lock poisoned")
                })?
                .remove(&cursor.branch);
        }
        Ok(step)
    }

    pub async fn cleanup_branch_index_rebuild(
        &self,
        journal: &JournalIndexRebuildCursorV2,
        operations: &OperationIndexRebuildCursorV2,
        limit: usize,
    ) -> Result<JournalIndexRebuildCleanupV2> {
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
        journal: &JournalIndexRebuildCursorV2,
    ) -> Result<OperationIndexRebuildCursorV2> {
        if journal.phase != JournalIndexRebuildPhaseV2::Complete {
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
        cursor: &OperationIndexRebuildCursorV2,
        max_events: usize,
    ) -> Result<OperationIndexRebuildStepV2> {
        if cursor.complete {
            return Ok(OperationIndexRebuildStepV2 {
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

    pub async fn branch_index_health(&self, branch: &str) -> Result<BranchIndexHealthV2> {
        self.locator.register(branch)?;
        let reference = self.publisher.load(branch).await?;
        self.branch_index_health_for(branch, &reference).await
    }

    async fn branch_index_health_for(
        &self,
        branch: &str,
        reference: &LoadedRefV2,
    ) -> Result<BranchIndexHealthV2> {
        let indexed = self.journal_indexes.head(branch).await?;
        if indexed
            .as_ref()
            .is_some_and(|head| head.checkpoint_generation.0 > reference.value.generation.0)
        {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "v2 journal index is ahead of the branch ref",
            ));
        }
        let locally_registered = self
            .local_index_heads
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 local-index lock poisoned"))?
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
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 index-error lock poisoned"))?
            .get(branch)
            .cloned();
        Ok(BranchIndexHealthV2 {
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
                "v2 branch-index maintenance interval must be at least 10 milliseconds",
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
                "shard-authority renewal requires a writable protocol-v2 repository",
            ));
        }
        let _renewal = self.authority_renewal.lock().await;
        let now = self.options.clock.now_millis()?;
        let permits = self
            .permits
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 permit lock poisoned"))?
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
                "shard-authority maintenance requires a writable protocol-v2 repository",
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
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "v2 fenced-branch lock poisoned",
                )
            })?
            .iter()
            .filter_map(|scope| match scope {
                AuthorityScopeV2::Branch { name } => Some(name.clone()),
                AuthorityScopeV2::System { .. } => None,
            })
            .collect())
    }

    async fn reconcile_operation(
        &self,
        branch: &str,
        operation: OperationId,
        input_digest: [u8; 32],
        now: u64,
    ) -> Result<Option<CommitReceiptV2>> {
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
                "protocol-v2 operation ID was reused with different input",
            )
            .operation(operation.to_string()));
        }
        Ok(Some(CommitReceiptV2 {
            id: indexed.target,
            operation,
            branch: branch.to_string(),
            parents: commit.parents,
            changed_keys: commit.delta.changes.len() as u64,
            object_versions: commit
                .delta
                .changes
                .iter()
                .map(|change| change.next)
                .collect(),
            idempotent_replay: true,
        }))
    }

    async fn active_permit(&self, branch: &str, now: u64) -> Result<AuthorityPermitV2> {
        self.active_scope_permit(
            AuthorityScopeV2::Branch {
                name: branch.to_string(),
            },
            now,
        )
        .await
    }

    async fn active_system_permit(&self, namespace: &str, now: u64) -> Result<AuthorityPermitV2> {
        self.active_scope_permit(
            AuthorityScopeV2::System {
                namespace: namespace.to_string(),
            },
            now,
        )
        .await
    }

    async fn active_scope_permit(
        &self,
        scope: AuthorityScopeV2,
        now: u64,
    ) -> Result<AuthorityPermitV2> {
        if !self.writable.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "protocol-v2 repository is read-only",
            ));
        }
        if self.is_scope_fenced(&scope)? {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "protocol-v2 branch authority is fenced in this repository instance",
            ));
        }
        let cached = self
            .permits
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 permit lock poisoned"))?
            .get(&scope)
            .cloned();
        let permit = if let Some(permit) = cached.filter(|permit| permit.expires_at_millis() > now)
        {
            permit
        } else {
            let _renewal = self.authority_renewal.lock().await;
            if self.is_scope_fenced(&scope)? {
                return Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "protocol-v2 branch authority is fenced in this repository instance",
                ));
            }
            let current = self
                .permits
                .read()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 permit lock poisoned"))?
                .get(&scope)
                .cloned();
            let acquired = match current {
                Some(permit) if permit.expires_at_millis() > now => permit,
                Some(_) => {
                    self.fence_scope(&scope)?;
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "protocol-v2 branch authority expired; explicit takeover is required",
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

    fn install_permit(&self, permit: AuthorityPermitV2) -> Result<()> {
        let scope = permit.stamp().scope;
        self.permits
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 permit lock poisoned"))?
            .insert(scope.clone(), permit);
        self.fenced_scopes
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "v2 fenced-branch lock poisoned",
                )
            })?
            .remove(&scope);
        Ok(())
    }

    fn fence_branch(&self, branch: &str) -> Result<()> {
        self.fence_scope(&AuthorityScopeV2::Branch {
            name: branch.to_string(),
        })
    }

    fn fence_scope(&self, scope: &AuthorityScopeV2) -> Result<()> {
        self.permits
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 permit lock poisoned"))?
            .remove(scope);
        self.fenced_scopes
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "v2 fenced-branch lock poisoned",
                )
            })?
            .insert(scope.clone());
        Ok(())
    }

    fn is_scope_fenced(&self, scope: &AuthorityScopeV2) -> Result<bool> {
        Ok(self
            .fenced_scopes
            .read()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "v2 fenced-branch lock poisoned",
                )
            })?
            .contains(scope))
    }

    async fn require_branch_indexes_ready(&self, branch: &str) -> Result<()> {
        let health = self.branch_index_health(branch).await?;
        Self::check_branch_index_health(health)
    }

    async fn require_branch_indexes_ready_for(
        &self,
        branch: &str,
        reference: &LoadedRefV2,
    ) -> Result<()> {
        let health = self.branch_index_health_for(branch, reference).await?;
        Self::check_branch_index_health(health)
    }

    fn check_branch_index_health(health: BranchIndexHealthV2) -> Result<()> {
        if health.ready {
            return Ok(());
        }
        Err(Error::new(
            ErrorCode::MissingClosure,
            format!(
                "protocol-v2 branch indexes lag {} generation(s); background catch-up is required",
                health.lag_generations
            ),
        )
        .retry(crate::RetryAdvice::After(Duration::from_millis(250))))
    }

    fn mark_local_index_head(&self, branch: &str, target: CommitIdV2) -> Result<()> {
        self.local_index_heads
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 local-index lock poisoned"))?
            .insert(branch.to_string(), target);
        Ok(())
    }

    async fn record_branch_catalog(&self, reference: &LoadedRefV2) -> Result<()> {
        self.ref_catalog
            .record(
                RefKindV2::Branch,
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

    async fn record_tag_catalog(&self, name: &str, value: &crate::TagValueV2) -> Result<()> {
        self.ref_catalog
            .record(
                RefKindV2::Tag,
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

    async fn load_commit_object(&self, id: CommitIdV2) -> Result<CommitObjectV2> {
        let object = self.publisher.load_commit_object(id).await?;
        let encoded = object.encode_object()?;
        self.node_store
            .register_commit_object_v2(id, &object, &encoded)?;
        Ok(object)
    }

    async fn find_v2_version_in_tree(
        &self,
        versions: &Tree,
        key: &[u8],
        selected: ObjectVersionIdV2,
    ) -> Result<Option<ObjectVersionV2>> {
        let engine = self.engine(self.node_store.clone());
        let mut entries = engine.prefix(versions, &version_tree_prefix(key)).await?;
        while let Some(entry) = entries.next().await {
            let (_, encoded) = entry?;
            let version: ObjectVersionV2 = decode_canonical(&encoded)?;
            if version.id == selected {
                version.validate()?;
                return Ok(Some(version));
            }
        }
        Ok(None)
    }

    async fn finalize_pack(
        &self,
        id: CommitIdV2,
        commit: &BucketCommitV2,
        prepared: Option<PreparedNodePack>,
    ) -> Result<()> {
        let Some(prepared) = prepared else {
            return Ok(());
        };
        let object = CommitObjectV2::new(commit.clone(), Some(prepared.pack().clone()))?;
        let encoded = object.encode_object()?;
        let offset = CommitObjectV2::node_payload_offset(&encoded)?.ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "prepared v2 node pack has no payload offset",
            )
        })?;
        self.node_store
            .commit_node_pack_v2(id, prepared, offset)
            .await
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

    fn tree_from_root(&self, root: &TreeRootV1) -> Result<Tree> {
        if root.format_digest != tree_format_digest(&self.format.state_tree_format)? {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "protocol-v2 state root uses another tree format",
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

    fn merge_plan_engine(&self, job: OperationId) -> Result<AsyncProlly<ProllyObjectStore<P>>> {
        if job.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "v2 merge job ID is nil",
            ));
        }
        Ok(self.engine(ProllyObjectStore::new(
            self.plane.clone(),
            format!(
                "{}/administration/v2/merge/{job}/plan",
                self.options.repository_prefix
            ),
        )))
    }

    fn merge_state_engine(&self) -> AsyncProlly<ProllyObjectStore<P>> {
        self.engine(self.node_store.durable_direct_write_session())
    }

    fn tree_from_merge_root(&self, root: &TreeRootV1) -> Result<Tree> {
        self.tree_from_root(root).map_err(|_| {
            Error::new(
                ErrorCode::InvalidContinuationToken,
                "v2 merge cursor uses another tree format",
            )
        })
    }

    async fn validate_merge_cursor(&self, cursor: &MergeCursorV2) -> Result<()> {
        crate::repository::validate_branch(&cursor.target_branch)?;
        crate::repository::validate_branch(&cursor.source_branch)?;
        self.tree_from_merge_root(&cursor.plan_root)?;
        if cursor.repository != self.format.repository_id
            || cursor.job.is_nil()
            || cursor.operation.is_nil()
            || cursor.target_branch == cursor.source_branch
            || cursor.message.trim().is_empty()
            || cursor.best_base_count == 0
                && !matches!(
                    cursor.phase,
                    MergePhaseV2::DiscoveringBases | MergePhaseV2::CollectingBases
                )
            || cursor.selected_base.is_none()
                && matches!(
                    cursor.phase,
                    MergePhaseV2::Planning
                        | MergePhaseV2::BuildingVersions
                        | MergePhaseV2::BuildingObjects
                        | MergePhaseV2::Conflicted
                        | MergePhaseV2::ReadyToPublish
                )
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "v2 merge cursor is malformed or belongs to another repository",
            ));
        }
        let engine = self.merge_plan_engine(cursor.job)?;
        let tree = self.tree_from_merge_root(&cursor.plan_root)?;
        let stored = engine
            .get(&tree, MERGE_CURSOR_KEY)
            .await?
            .map(|bytes| decode_canonical::<MergeCursorV2>(&bytes))
            .transpose()?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidContinuationToken,
                    "v2 merge cursor is not anchored by its durable plan",
                )
            })?;
        if normalized_merge_cursor(&stored)? != normalized_merge_cursor(cursor)? {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "v2 merge cursor state disagrees with its durable plan",
            ));
        }
        Ok(())
    }

    async fn seal_merge_cursor(&self, cursor: &mut MergeCursorV2) -> Result<()> {
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
        cursor.plan_root = TreeRootV1::from_tree(&tree)?;
        Ok(())
    }

    fn validate_requested_merge_base(
        &self,
        cursor: &MergeCursorV2,
        discovered: CommitIdV2,
    ) -> Result<()> {
        if cursor
            .requested_base
            .is_some_and(|requested| requested != discovered)
        {
            return Err(Error::new(
                ErrorCode::InvalidRevision,
                "requested v2 merge base is not a best common ancestor",
            ));
        }
        Ok(())
    }

    async fn merge_graph_entry(
        &self,
        target_branch: &str,
        source_branch: &str,
        commit: CommitIdV2,
    ) -> Result<JournalCommitGraphEntryV2> {
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
        let commit_object = self.load_commit_object(commit).await?.commit;
        Ok(JournalCommitGraphEntryV2 {
            commit,
            generation: commit_object.generation,
            parents: commit_object.parents,
            first_parent_jumps: Vec::new(),
        })
    }

    async fn is_first_parent_ancestor_v2(
        &self,
        target_branch: &str,
        source_branch: &str,
        ancestor: CommitIdV2,
        mut descendant: CommitIdV2,
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
        entry: JournalCommitGraphEntryV2,
        flags: u8,
    ) -> Result<()> {
        let seen_key = merge_seen_key(entry.commit);
        let queue_key = merge_queue_key(entry.generation.0, entry.commit);
        mutations.push(Mutation::Upsert {
            key: seen_key,
            val: encode_canonical(&MergeSeenEntryV2 {
                generation: entry.generation.0,
                flags,
            })?,
        });
        mutations.push(Mutation::Upsert {
            key: queue_key,
            val: encode_canonical(&MergeQueueEntryV2 {
                commit: entry.commit,
                generation: entry.generation.0,
            })?,
        });
        Ok(())
    }

    async fn advance_merge_base_one(&self, cursor: &mut MergeCursorV2) -> Result<usize> {
        let engine = self.merge_plan_engine(cursor.job)?;
        let mut tree = self.tree_from_merge_root(&cursor.plan_root)?;
        let mut queue = engine.prefix(&tree, MERGE_QUEUE_PREFIX).await?;
        let Some(entry) = queue.next().await else {
            cursor.phase = MergePhaseV2::CollectingBases;
            return Ok(0);
        };
        let (queue_key, encoded) = entry?;
        let queued: MergeQueueEntryV2 = decode_canonical(&encoded)?;
        let seen_key = merge_seen_key(queued.commit);
        let seen: MergeSeenEntryV2 = engine
            .get(&tree, &seen_key)
            .await?
            .map(|bytes| decode_canonical(&bytes))
            .transpose()?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::CorruptCommit,
                    "v2 merge queue has no seen record",
                )
            })?;
        if seen.generation != queued.generation {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "v2 merge queue generation disagrees with seen state",
            ));
        }
        let candidate_key = merge_base_candidate_key(queued.commit);
        let candidate = engine
            .get(&tree, &candidate_key)
            .await?
            .map(|bytes| decode_canonical::<MergeBaseCandidateV2>(&bytes))
            .transpose()?;
        let is_common = seen.flags & MERGE_BOTH == MERGE_BOTH;
        let is_stale = seen.flags & MERGE_STALE != 0;
        let mut mutations = vec![Mutation::Delete { key: queue_key }];
        if is_common && !is_stale && candidate.is_none() {
            mutations.push(Mutation::Upsert {
                key: candidate_key.clone(),
                val: encode_canonical(&MergeBaseCandidateV2 {
                    generation: seen.generation,
                    stale: false,
                })?,
            });
        } else if is_stale && candidate.as_ref().is_some_and(|candidate| !candidate.stale) {
            mutations.push(Mutation::Upsert {
                key: candidate_key,
                val: encode_canonical(&MergeBaseCandidateV2 {
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
                .map(|bytes| decode_canonical::<MergeSeenEntryV2>(&bytes))
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
                val: encode_canonical(&MergeSeenEntryV2 {
                    generation: parent_graph.generation.0,
                    flags: next_flags,
                })?,
            });
            mutations.push(Mutation::Upsert {
                key: merge_queue_key(parent_graph.generation.0, parent),
                val: encode_canonical(&MergeQueueEntryV2 {
                    commit: parent,
                    generation: parent_graph.generation.0,
                })?,
            });
            if next_flags & MERGE_STALE != 0 {
                let key = merge_base_candidate_key(parent);
                if let Some(candidate) = engine
                    .get(&tree, &key)
                    .await?
                    .map(|bytes| decode_canonical::<MergeBaseCandidateV2>(&bytes))
                    .transpose()?
                {
                    if !candidate.stale {
                        mutations.push(Mutation::Upsert {
                            key,
                            val: encode_canonical(&MergeBaseCandidateV2 {
                                generation: candidate.generation,
                                stale: true,
                            })?,
                        });
                    }
                }
            }
        }
        tree = engine.batch(&tree, mutations).await?;
        cursor.plan_root = TreeRootV1::from_tree(&tree)?;
        cursor.visited_commits = cursor.visited_commits.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "v2 merge visited count overflow",
            )
        })?;
        Ok(1)
    }

    async fn collect_merge_base_one(&self, cursor: &mut MergeCursorV2) -> Result<usize> {
        let engine = self.merge_plan_engine(cursor.job)?;
        let mut tree = self.tree_from_merge_root(&cursor.plan_root)?;
        let mut candidates = engine.prefix(&tree, MERGE_BASE_CANDIDATE_PREFIX).await?;
        let Some(entry) = candidates.next().await else {
            if cursor.best_base_count == 0 {
                return Err(Error::new(
                    ErrorCode::NoMergeBase,
                    "v2 commits have no common ancestor",
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
                        "requested v2 merge base is not a best common ancestor",
                    ));
                }
                cursor.selected_base = Some(requested);
                cursor.phase = MergePhaseV2::Planning;
            } else if cursor.best_base_count == 1 {
                let mut bases = engine.prefix(&tree, MERGE_BASE_RESULT_PREFIX).await?;
                let (key, _) = bases.next().await.ok_or_else(|| {
                    Error::new(ErrorCode::CorruptCommit, "v2 best-base result is absent")
                })??;
                cursor.selected_base = Some(commit_from_suffix(&key, MERGE_BASE_RESULT_PREFIX)?);
                cursor.phase = MergePhaseV2::Planning;
            } else {
                cursor.phase = MergePhaseV2::AwaitingBase;
            }
            return Ok(0);
        };
        let (key, encoded) = entry?;
        let candidate: MergeBaseCandidateV2 = decode_canonical(&encoded)?;
        let commit = commit_from_suffix(&key, MERGE_BASE_CANDIDATE_PREFIX)?;
        let mut mutations = vec![Mutation::Delete { key }];
        if !candidate.stale {
            mutations.push(Mutation::Upsert {
                key: merge_base_result_key(commit),
                val: Vec::new(),
            });
            cursor.best_base_count = cursor.best_base_count.checked_add(1).ok_or_else(|| {
                Error::new(ErrorCode::InternalInvariant, "v2 best-base count overflow")
            })?;
        }
        tree = engine.batch(&tree, mutations).await?;
        cursor.plan_root = TreeRootV1::from_tree(&tree)?;
        Ok(1)
    }

    async fn advance_merge_plan(
        &self,
        cursor: &mut MergeCursorV2,
        max_steps: usize,
        emitted_changes: &mut Vec<MergeChangeV2>,
        emitted_conflicts: &mut Vec<MergeConflictV2>,
    ) -> Result<usize> {
        let base = cursor.selected_base.ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "v2 merge plan has no selected base",
            )
        })?;
        let base_commit = self.load_commit_object(base).await?.commit;
        let ours_commit = self.load_commit_object(cursor.ours).await?.commit;
        let theirs_commit = self.load_commit_object(cursor.theirs).await?.commit;
        let base_tree = self.tree_from_root(&base_commit.state.objects)?;
        let ours_tree = self.tree_from_root(&ours_commit.state.objects)?;
        let theirs_tree = self.tree_from_root(&theirs_commit.state.objects)?;
        let state_engine = self.engine(self.node_store.clone());
        let plan_engine = self.merge_plan_engine(cursor.job)?;
        let mut plan_tree = self.tree_from_merge_root(&cursor.plan_root)?;
        let mut processed = 0usize;
        let mut page_mutations = Vec::new();
        while processed < max_steps {
            if cursor.ours_pending.is_none() && !cursor.ours_finished {
                let page = state_engine
                    .structural_diff_page(&base_tree, &ours_tree, cursor.ours_diff.as_ref(), 1)
                    .await?;
                cursor.ours_pending = page.diffs.into_iter().next();
                cursor.ours_diff = page.next_cursor;
                if cursor.ours_diff.is_none() {
                    cursor.ours_finished = true;
                }
            }
            if cursor.theirs_pending.is_none() && !cursor.theirs_finished {
                let page = state_engine
                    .structural_diff_page(&base_tree, &theirs_tree, cursor.theirs_diff.as_ref(), 1)
                    .await?;
                cursor.theirs_pending = page.diffs.into_iter().next();
                cursor.theirs_diff = page.next_cursor;
                if cursor.theirs_diff.is_none() {
                    cursor.theirs_finished = true;
                }
            }
            let key_order = match (&cursor.ours_pending, &cursor.theirs_pending) {
                (None, None) => break,
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some(ours), Some(theirs)) => ours.key().cmp(theirs.key()),
            };
            let (key, base_value, ours_value, theirs_value) = match key_order {
                std::cmp::Ordering::Less => {
                    let ours = cursor.ours_pending.take().expect("matched pending ours");
                    let (key, base, ours) = merge_diff_values(ours);
                    (key, base.clone(), ours, base)
                }
                std::cmp::Ordering::Greater => {
                    let theirs = cursor
                        .theirs_pending
                        .take()
                        .expect("matched pending theirs");
                    let (key, base, theirs) = merge_diff_values(theirs);
                    (key, base.clone(), base, theirs)
                }
                std::cmp::Ordering::Equal => {
                    let ours = cursor.ours_pending.take().expect("matched pending ours");
                    let theirs = cursor
                        .theirs_pending
                        .take()
                        .expect("matched pending theirs");
                    let (key, ours_base, ours_value) = merge_diff_values(ours);
                    let (theirs_key, theirs_base, theirs_value) = merge_diff_values(theirs);
                    if key != theirs_key || ours_base != theirs_base {
                        return Err(Error::new(
                            ErrorCode::CorruptCommit,
                            "v2 structural merge streams disagree on their base value",
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
                    MergePolicyV2::Fail | MergePolicyV2::Ours => ours_value.clone(),
                    MergePolicyV2::Theirs => theirs_value.clone(),
                }
            };
            let record = MergePlanEntryV2 {
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
                        Error::new(
                            ErrorCode::InternalInvariant,
                            "v2 merge change count overflow",
                        )
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
                        "v2 merge conflict count overflow",
                    )
                })?;
            }
            processed += 1;
        }
        if !page_mutations.is_empty() {
            plan_tree = plan_engine.batch(&plan_tree, page_mutations).await?;
            cursor.plan_root = TreeRootV1::from_tree(&plan_tree)?;
        }
        if cursor.ours_pending.is_none()
            && cursor.theirs_pending.is_none()
            && cursor.ours_finished
            && cursor.theirs_finished
        {
            if cursor.policy == MergePolicyV2::Fail && cursor.conflicts != 0 {
                cursor.phase = MergePhaseV2::Conflicted;
            } else {
                cursor.final_objects = Some(ours_commit.state.objects);
                cursor.final_versions = Some(ours_commit.state.versions);
                let empty_delta = self.merge_state_engine().create();
                cursor.delta_root = Some(TreeRootV1::from_tree(&empty_delta)?);
                cursor.phase = MergePhaseV2::BuildingVersions;
            }
        }
        Ok(processed)
    }

    async fn advance_merge_version_union(
        &self,
        cursor: &mut MergeCursorV2,
        max_steps: usize,
    ) -> Result<usize> {
        let ours_commit = self.load_commit_object(cursor.ours).await?.commit;
        let theirs_commit = self.load_commit_object(cursor.theirs).await?.commit;
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
                        "same v2 version-tree key has unequal immutable values",
                    ))
                }
            }
        }
        if !mutations.is_empty() {
            let state_engine = self.merge_state_engine();
            let versions =
                self.tree_from_root(cursor.final_versions.as_ref().ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "v2 merge version root is absent",
                    )
                })?)?;
            let versions = state_engine.batch(&versions, mutations).await?;
            cursor.final_versions = Some(TreeRootV1::from_tree(&versions)?);
        }
        cursor.version_diff = page.next_cursor;
        if cursor.version_diff.is_none() {
            cursor.version_diff_finished = true;
            cursor.phase = MergePhaseV2::BuildingObjects;
            cursor.build_after = None;
        }
        Ok(processed)
    }

    async fn advance_merge_object_build(
        &self,
        cursor: &mut MergeCursorV2,
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
            let record: MergePlanEntryV2 = decode_canonical(&encoded)?;
            if merge_change_key(&record.key) != key {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "v2 merge-plan change key disagrees with its record",
                ));
            }
            records.push((key, record));
        }
        if records.is_empty() {
            if cursor.built_changes != cursor.planned_changes {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "v2 merge build did not consume every planned change",
                ));
            }
            if cursor.built_changes == 0 {
                cursor.delta_root = None;
            }
            cursor.phase = MergePhaseV2::ReadyToPublish;
            return Ok(0);
        }
        let ours = self.load_commit_object(cursor.ours).await?.commit;
        let theirs = self.load_commit_object(cursor.theirs).await?.commit;
        let generation = CommitGeneration(
            ours.generation
                .0
                .max(theirs.generation.0)
                .checked_add(1)
                .ok_or_else(|| {
                    Error::new(ErrorCode::InternalInvariant, "v2 merge generation overflow")
                })?,
        );
        let mut object_mutations = Vec::with_capacity(records.len());
        let mut version_mutations = Vec::new();
        let mut delta_mutations = Vec::with_capacity(records.len());
        for (_, record) in &records {
            let previous = current_v2_id(record.ours.as_deref())?;
            let (next, delete_marker) = if let Some(selected) = &record.selected {
                let current: CurrentObjectV2 = decode_canonical(selected)?;
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
                        Error::new(
                            ErrorCode::InvalidLimit,
                            "v2 merge delete ordinal exceeds u32",
                        )
                    })?;
                let version = ObjectVersionV2::derive(
                    self.format.repository_id,
                    &record.key,
                    cursor.operation,
                    LogicalObjectVersionBodyV1 {
                        order: ObjectVersionOrder {
                            commit_generation: generation,
                            mutation_ordinal: ordinal,
                        },
                        created_at_millis: cursor.created_at_millis,
                        kind: LogicalObjectVersionKindV1::DeleteMarker,
                    },
                    None,
                )?;
                version_mutations.push(Mutation::Upsert {
                    key: version_tree_key(&record.key, version.body.order, version.id),
                    val: encode_canonical(&version)?,
                });
                (version.id, true)
            };
            let transition = ObjectTransitionV2 {
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
            Error::new(
                ErrorCode::InternalInvariant,
                "v2 merge object root is absent",
            )
        })?)?;
        let versions = self.tree_from_root(cursor.final_versions.as_ref().ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "v2 merge version root is absent",
            )
        })?)?;
        let delta = self.tree_from_root(cursor.delta_root.as_ref().ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "v2 merge delta root is absent",
            )
        })?)?;
        let objects = state_engine.batch(&objects, object_mutations).await?;
        let versions = if version_mutations.is_empty() {
            versions
        } else {
            state_engine.batch(&versions, version_mutations).await?
        };
        let delta = state_engine.batch(&delta, delta_mutations).await?;
        cursor.final_objects = Some(TreeRootV1::from_tree(&objects)?);
        cursor.final_versions = Some(TreeRootV1::from_tree(&versions)?);
        cursor.delta_root = Some(TreeRootV1::from_tree(&delta)?);
        cursor.built_changes = cursor
            .built_changes
            .checked_add(records.len() as u64)
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "v2 merge build count overflow",
                )
            })?;
        cursor.build_after = records.last().map(|(key, _)| key.clone());
        Ok(records.len())
    }

    async fn merge_base_page(
        &self,
        cursor: &MergeCursorV2,
        continuation: Option<&MergeBaseCursorV2>,
        limit: usize,
    ) -> Result<MergeBasePageV2> {
        self.validate_merge_cursor(cursor).await?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "v2 merge-base page limit must be greater than zero",
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
                "v2 merge-base cursor belongs to another plan",
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
        Ok(MergeBasePageV2 {
            continuation: (bases.len() == limit).then(|| MergeBaseCursorV2 {
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
        cursor: &MergeCursorV2,
        continuation: Option<&MergeChangeCursorV2>,
        limit: usize,
    ) -> Result<MergeChangePageV2> {
        self.validate_merge_cursor(cursor).await?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "v2 merge change page limit must be greater than zero",
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
                "v2 merge change cursor belongs to another plan",
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
            let record: MergePlanEntryV2 = decode_canonical(&encoded)?;
            changes.push(merge_change_from_record(&record)?);
            last = Some(key);
        }
        Ok(MergeChangePageV2 {
            continuation: (changes.len() == limit).then(|| MergeChangeCursorV2 {
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
        cursor: &MergeCursorV2,
        continuation: Option<&MergeConflictCursorV2>,
        limit: usize,
    ) -> Result<MergeConflictPageV2> {
        self.validate_merge_cursor(cursor).await?;
        let limit = limit.min(self.format.canonical_limits.max_list_page as usize);
        if limit == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "v2 merge conflict page limit must be greater than zero",
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
                "v2 merge conflict cursor belongs to another plan",
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
            let record: MergePlanEntryV2 = decode_canonical(&encoded)?;
            conflicts.push(merge_conflict_from_record(&record)?);
            last = Some(key);
        }
        Ok(MergeConflictPageV2 {
            continuation: (conflicts.len() == limit).then(|| MergeConflictCursorV2 {
                repository: cursor.repository,
                job: cursor.job,
                plan_root: cursor.plan_root.clone(),
                after: last.expect("full page has a last merge conflict"),
            }),
            conflicts,
        })
    }

    fn merge_input_digest(&self, cursor: &MergeCursorV2, base: CommitIdV2) -> [u8; 32] {
        let policy = match cursor.policy {
            MergePolicyV2::Fail => [0],
            MergePolicyV2::Ours => [1],
            MergePolicyV2::Theirs => [2],
        };
        crate::model::derive_input_digest_v2(&[
            b"merge-v2",
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
        cursor: &MergeCursorV2,
        input_digest: [u8; 32],
        now: u64,
    ) -> Result<Option<MergeReceiptV2>> {
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
                "v2 merge operation ID was reused with different input",
            )
            .operation(cursor.operation.to_string()));
        }
        Ok(Some(MergeReceiptV2 {
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
                "logical key violates the protocol-v2 key contract",
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

fn normalized_merge_cursor(cursor: &MergeCursorV2) -> Result<Vec<u8>> {
    let mut normalized = cursor.clone();
    normalized.plan_root.root = None;
    encode_canonical(&normalized)
}

fn merge_queue_key(generation: u64, commit: CommitIdV2) -> Vec<u8> {
    let mut key = Vec::with_capacity(MERGE_QUEUE_PREFIX.len() + 8 + 32);
    key.extend_from_slice(MERGE_QUEUE_PREFIX);
    key.extend_from_slice(&(u64::MAX - generation).to_be_bytes());
    key.extend_from_slice(commit.as_bytes());
    key
}

fn merge_seen_key(commit: CommitIdV2) -> Vec<u8> {
    merge_commit_key(MERGE_SEEN_PREFIX, commit)
}

fn merge_base_candidate_key(commit: CommitIdV2) -> Vec<u8> {
    merge_commit_key(MERGE_BASE_CANDIDATE_PREFIX, commit)
}

fn merge_base_result_key(commit: CommitIdV2) -> Vec<u8> {
    merge_commit_key(MERGE_BASE_RESULT_PREFIX, commit)
}

fn merge_commit_key(prefix: &[u8], commit: CommitIdV2) -> Vec<u8> {
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

fn commit_from_suffix(key: &[u8], prefix: &[u8]) -> Result<CommitIdV2> {
    let suffix = key.strip_prefix(prefix).ok_or_else(|| {
        Error::new(
            ErrorCode::CorruptCommit,
            "v2 merge-state key uses the wrong namespace",
        )
    })?;
    let hash: [u8; 32] = suffix.try_into().map_err(|_| {
        Error::new(
            ErrorCode::CorruptCommit,
            "v2 merge-state commit key has the wrong length",
        )
    })?;
    Ok(CommitIdV2::from_hash(hash))
}

fn merge_diff_values(diff: Diff) -> (Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>) {
    match diff {
        Diff::Added { key, val } => (key, None, Some(val)),
        Diff::Removed { key, val } => (key, Some(val), None),
        Diff::Changed { key, old, new } => (key, Some(old), Some(new)),
    }
}

fn current_v2_id(value: Option<&[u8]>) -> Result<Option<ObjectVersionIdV2>> {
    value
        .map(|value| {
            let current: CurrentObjectV2 = decode_canonical(value)?;
            current.version.validate()?;
            Ok(current.version.id)
        })
        .transpose()
}

fn merge_change_from_record(record: &MergePlanEntryV2) -> Result<MergeChangeV2> {
    Ok(MergeChangeV2 {
        key: record.key.clone(),
        from: current_v2_id(record.ours.as_deref())?,
        to: current_v2_id(record.selected.as_deref())?,
    })
}

fn merge_conflict_from_record(record: &MergePlanEntryV2) -> Result<MergeConflictV2> {
    if !record.conflict {
        return Err(Error::new(
            ErrorCode::CorruptCommit,
            "v2 merge conflict index points to a non-conflict record",
        ));
    }
    Ok(MergeConflictV2 {
        key: record.key.clone(),
        base: current_v2_id(record.base.as_deref())?,
        ours: current_v2_id(record.ours.as_deref())?,
        theirs: current_v2_id(record.theirs.as_deref())?,
    })
}

fn validate_options(options: &RepositoryV2Options) -> Result<()> {
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
            "protocol-v2 repository options are invalid",
        ));
    }
    Ok(())
}

fn validate_format_compatibility(
    format: &RepositoryFormatV2,
    options: &RepositoryV2Options,
) -> Result<()> {
    if format.format_version != RepositoryFormatV2::VERSION
        || format.required_capability_profile != RepositoryFormatV2::PROLLY_S3_CAPABILITY_PROFILE
        || format.min_reader_version == 0
        || format.min_writer_version == 0
        || format.min_reader_version > RepositoryFormatV2::CURRENT_READER_VERSION
        || format.min_writer_version > RepositoryFormatV2::CURRENT_WRITER_VERSION
    {
        return Err(Error::new(
            ErrorCode::UnsupportedRepositoryFormat,
            "repository is not a supported native protocol-v2 repository",
        ));
    }
    format.idempotency_retention.validate()?;
    if format.state_tree_format != options.state_tree_format
        || format.canonical_limits != options.limits
        || format.idempotency_retention != options.idempotency_retention
        || format.provider_per_key_version_limit != options.provider_per_key_version_limit
    {
        return Err(Error::new(
            ErrorCode::RepositoryFormatConflict,
            "protocol-v2 format does not match the requested canonical settings",
        ));
    }
    Ok(())
}

fn format_path(prefix: &str) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/format/v2.cbor"))
}

fn intent_path(prefix: &str) -> Result<ObjectPath> {
    ObjectPath::new(format!("{prefix}/format/initialization-v2.cbor"))
}

fn version_tree_key(key: &[u8], order: ObjectVersionOrder, version: ObjectVersionIdV2) -> Vec<u8> {
    let mut output = version_tree_prefix(key);
    output.reserve(8 + 4 + 32);
    output.extend(order.commit_generation.0.to_be_bytes().map(|byte| !byte));
    output.extend(order.mutation_ordinal.to_be_bytes().map(|byte| !byte));
    output.extend(version.as_bytes().iter().map(|byte| !byte));
    output
}

fn migration_version_operation(
    source_repository: crate::RepositoryId,
    key: &[u8],
    version: crate::ObjectVersionId,
) -> OperationId {
    let digest = crate::codec::sha256(
        &[
            b"prolly-s3/v1-to-v2-version".as_slice(),
            source_repository.as_bytes().as_slice(),
            key,
            version.as_bytes().as_slice(),
        ]
        .concat(),
    );
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    OperationId(uuid::Uuid::from_bytes(bytes))
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
                    "noncanonical v2 version-tree key escape",
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
        "unterminated v2 version-tree logical key",
    ))
}
