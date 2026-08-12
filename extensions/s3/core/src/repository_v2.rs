use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock, Weak,
    },
    time::Duration,
};

use md5::{Digest as _, Md5};
use prolly::{AsyncProlly, Config, Mutation, RuntimeConfig, Tree, TreeFormat};
use sha2::Sha256;

use crate::store::{LocatedPackedNode, NodeCacheNamespace, NodeLocator, PreparedNodePack};
use crate::{
    decode_canonical, encode_canonical, tree_format_digest, AuthorityPermitV2, AuthorityScopeV2,
    BucketCommitV2, BucketDeltaV2, BucketStateV2, CanonicalLimits, Checksums, Clock,
    CommitGeneration, CommitIdV2, CommitObjectV2, CommitPublicationV2, CompareExchange,
    CompareExchangeOutcome, CurrentObjectV2, Error, ErrorCode, GetRequest, IdSource,
    IdempotencyRetentionV2, ImmutablePayloadStoreV2, InitializationIntentV2,
    JournalDerivedIndexesV2, JournalIndexAdvanceReportV2, LogicalObjectVersionBodyV1,
    LogicalObjectVersionKindV1, MemoryNodeCache, NodeCache, ObjectHeaders, ObjectPath, ObjectPlane,
    ObjectTransitionV2, ObjectVersionIdV2, ObjectVersionOrder, ObjectVersionV2, OperationId,
    OperationIndexAdvanceReportV2, PhysicalBatchV2, PhysicalMutationIdentityV2, ProllyObjectStore,
    ProviderPerKeyVersionLimitV2, RandomIdSource, RepositoryFormatV2, Result,
    SegmentedOperationIndexV2, ShardWriterAuthorityV2, ShardedBranchPublisherV2,
    StagedMutationBodyV2, StagedMutationV2, StagedPutV2, SystemClock, TakeoverRequestV2,
    TreeRootV1,
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
pub struct BranchIndexAdvanceReportV2 {
    pub operations: OperationIndexAdvanceReportV2,
    pub journal: JournalIndexAdvanceReportV2,
}

struct JournalNodeLocator<P: ObjectPlane> {
    indexes: Arc<JournalDerivedIndexesV2<P>>,
    branches: RwLock<BTreeSet<String>>,
}

impl<P: ObjectPlane> JournalNodeLocator<P> {
    fn register(&self, branch: &str) -> Result<()> {
        self.branches
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 locator lock poisoned"))?
            .insert(branch.to_string());
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
    operation_index: SegmentedOperationIndexV2<P>,
    journal_indexes: Arc<JournalDerivedIndexesV2<P>>,
    locator: Arc<JournalNodeLocator<P>>,
    permits: RwLock<BTreeMap<String, AuthorityPermitV2>>,
    fenced_branches: RwLock<BTreeSet<String>>,
    authority_renewal: tokio::sync::Mutex<()>,
    publication_lanes: std::sync::Mutex<BTreeMap<String, Weak<tokio::sync::Mutex<()>>>>,
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
                repository.register_unindexed_tail(&default_branch).await?;
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
        repository.install_permit(&default_branch, permit.clone())?;

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
        repository.register_unindexed_tail(&branch).await?;
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
            repository.install_permit(&branch, permit)?;
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
        let operation_index = SegmentedOperationIndexV2::new_with_limits(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            format.idempotency_retention,
            crate::DEFAULT_OPERATION_INDEX_LEAF_ENTRIES,
            crate::DEFAULT_OPERATION_INDEX_MERGE_FANOUT,
            crate::DEFAULT_OPERATION_INDEX_MAX_UNINDEXED_EVENTS,
            options.mutable_control_versions_to_retain,
        )?;
        let journal_indexes = Arc::new(JournalDerivedIndexesV2::new_with_limits(
            plane.clone(),
            options.repository_prefix.clone(),
            format.repository_id,
            format.state_tree_format.clone(),
            node_cache,
            crate::DEFAULT_JOURNAL_INDEX_MAX_UNINDEXED_EVENTS,
            options.mutable_control_versions_to_retain,
        )?);
        let locator = Arc::new(JournalNodeLocator {
            indexes: journal_indexes.clone(),
            branches: RwLock::new(BTreeSet::new()),
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
            operation_index,
            journal_indexes,
            locator,
            permits: RwLock::new(BTreeMap::new()),
            fenced_branches: RwLock::new(BTreeSet::new()),
            authority_renewal: tokio::sync::Mutex::new(()),
            publication_lanes: std::sync::Mutex::new(BTreeMap::new()),
            writable: AtomicBool::new(writable),
        })
    }

    pub fn format(&self) -> &RepositoryFormatV2 {
        &self.format
    }

    pub fn repository_id(&self) -> crate::RepositoryId {
        self.format.repository_id
    }

    pub fn plane(&self) -> Arc<P> {
        self.plane.clone()
    }

    pub async fn head(&self, branch: &str) -> Result<CommitIdV2> {
        self.locator.register(branch)?;
        Ok(self.publisher.load(branch).await?.value.target)
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
        self.install_permit(branch, permit)?;
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

    pub async fn publish_commit_session(
        &self,
        session: PhysicalBatchV2,
        mutations: Vec<StagedMutationV2>,
    ) -> Result<CommitReceiptV2> {
        session.validate(self.format.repository_id)?;
        if mutations.is_empty()
            || mutations.len() > self.format.canonical_limits.max_mutations_per_commit as usize
            || session.expires_at_millis < self.options.clock.now_millis()?
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "protocol-v2 commit session is empty, expired, or exceeds the mutation limit",
            ));
        }
        let mut ordered = BTreeMap::new();
        for mutation in mutations {
            self.validate_key(mutation.key())?;
            if ordered.insert(mutation.key().to_vec(), mutation).is_some() {
                return Err(Error::new(
                    ErrorCode::InvalidRequest,
                    "protocol-v2 commit session contains the same key more than once",
                ));
            }
        }
        let canonical_mutations = ordered.values().cloned().collect::<Vec<_>>();
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
                    binding.validate()?;
                    let expected_etag =
                        checksums.md5.map(|md5| format!("\"{}\"", hex::encode(md5)));
                    if *size > self.format.canonical_limits.max_object_bytes
                        || binding.path != self.payloads.path(binding.checksum_sha256)?
                        || checksums.sha256 != Some(binding.checksum_sha256)
                        || expected_etag.as_deref() != Some(logical_etag)
                    {
                        return Err(Error::new(
                            ErrorCode::ChecksumMismatch,
                            "staged v2 payload identity does not match its immutable binding",
                        ));
                    }
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
        self.register_unindexed_tail(branch).await?;
        let reference = self.publisher.load(branch).await?;
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
        self.register_unindexed_tail(branch).await?;
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
        self.register_unindexed_tail(branch).await?;
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
        self.register_unindexed_tail(branch).await?;
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
        self.register_unindexed_tail(branch).await?;
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

    pub async fn advance_branch_indexes(&self, branch: &str) -> Result<BranchIndexAdvanceReportV2> {
        self.locator.register(branch)?;
        let now = self.options.clock.now_millis()?;
        let operations = self
            .operation_index
            .advance(&self.publisher, branch, now)
            .await?;
        let journal = self
            .journal_indexes
            .advance(&self.publisher, branch, now)
            .await?;
        Ok(BranchIndexAdvanceReportV2 {
            operations,
            journal,
        })
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
        let mut first_error = None;
        for (branch, permit) in permits {
            match self.authority.renew(permit, now).await {
                Ok(renewed) => self.install_permit(&branch, renewed)?,
                Err(error) => {
                    self.fence_branch(&branch)?;
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
            .fenced_branches
            .read()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "v2 fenced-branch lock poisoned",
                )
            })?
            .iter()
            .cloned()
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
        if !self.writable.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "protocol-v2 repository is read-only",
            ));
        }
        if self.is_branch_fenced(branch)? {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "protocol-v2 branch authority is fenced in this repository instance",
            ));
        }
        let cached = self
            .permits
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 permit lock poisoned"))?
            .get(branch)
            .cloned();
        let permit = if let Some(permit) = cached.filter(|permit| permit.expires_at_millis() > now)
        {
            permit
        } else {
            let _renewal = self.authority_renewal.lock().await;
            if self.is_branch_fenced(branch)? {
                return Err(Error::new(
                    ErrorCode::PreconditionFailed,
                    "protocol-v2 branch authority is fenced in this repository instance",
                ));
            }
            let current = self
                .permits
                .read()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 permit lock poisoned"))?
                .get(branch)
                .cloned();
            let acquired = match current {
                Some(permit) if permit.expires_at_millis() > now => permit,
                Some(_) => {
                    self.fence_branch(branch)?;
                    return Err(Error::new(
                        ErrorCode::PreconditionFailed,
                        "protocol-v2 branch authority expired; explicit takeover is required",
                    ));
                }
                None => match self
                    .authority
                    .acquire(
                        AuthorityScopeV2::Branch {
                            name: branch.to_string(),
                        },
                        &self.options.writer,
                        now,
                        self.options.ids.operation(),
                    )
                    .await
                {
                    Ok(permit) => permit,
                    Err(error) => {
                        self.fence_branch(branch)?;
                        return Err(error);
                    }
                },
            };
            self.install_permit(branch, acquired.clone())?;
            acquired
        };
        match self.authority.validate_active(&permit, now).await {
            Ok(_) => Ok(permit),
            Err(error) => {
                self.fence_branch(branch)?;
                Err(error)
            }
        }
    }

    fn install_permit(&self, branch: &str, permit: AuthorityPermitV2) -> Result<()> {
        self.permits
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 permit lock poisoned"))?
            .insert(branch.to_string(), permit);
        self.fenced_branches
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "v2 fenced-branch lock poisoned",
                )
            })?
            .remove(branch);
        Ok(())
    }

    fn fence_branch(&self, branch: &str) -> Result<()> {
        self.permits
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 permit lock poisoned"))?
            .remove(branch);
        self.fenced_branches
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "v2 fenced-branch lock poisoned",
                )
            })?
            .insert(branch.to_string());
        Ok(())
    }

    fn is_branch_fenced(&self, branch: &str) -> Result<bool> {
        Ok(self
            .fenced_branches
            .read()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "v2 fenced-branch lock poisoned",
                )
            })?
            .contains(branch))
    }

    async fn register_unindexed_tail(&self, branch: &str) -> Result<()> {
        let reference = self.publisher.load(branch).await?;
        let checkpoint = self
            .journal_indexes
            .head(branch)
            .await?
            .map(|head| head.checkpoint);
        let mut cursor = Some(self.publisher.open_journal(branch).await?);
        let mut reached_checkpoint = checkpoint.is_none();
        let mut visited = 0usize;
        while let Some(current) = cursor {
            let page = self.publisher.read_journal_page(&current, 256).await?;
            for entry in page.entries {
                if checkpoint == Some(entry.id) {
                    reached_checkpoint = true;
                    break;
                }
                self.load_commit_object(entry.event.new_target).await?;
                visited += 1;
                if visited > crate::DEFAULT_JOURNAL_INDEX_MAX_UNINDEXED_EVENTS {
                    return Err(Error::new(
                        ErrorCode::HistoryLimitExceeded,
                        "v2 node-index journal tail exceeds its bounded foreground recovery limit",
                    ));
                }
            }
            if reached_checkpoint {
                break;
            }
            cursor = page.continuation;
        }
        if checkpoint.is_some() && !reached_checkpoint {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "v2 journal index checkpoint is not reachable from the branch ref",
            ));
        }
        // A generation-zero repository intentionally has no earlier indexed
        // history. Register its commit pack directly.
        if checkpoint.is_none() && reference.value.generation.0 == 0 {
            self.load_commit_object(reference.value.target).await?;
        } else if checkpoint.is_none() {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "v2 journal indexes were not initialized with branch creation",
            ));
        }
        Ok(())
    }

    async fn load_commit_object(&self, id: CommitIdV2) -> Result<CommitObjectV2> {
        let object = self.publisher.load_commit_object(id).await?;
        let encoded = object.encode_object()?;
        self.node_store
            .register_commit_object_v2(id, &object, &encoded)?;
        Ok(object)
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
