use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock, Weak},
    time::Duration,
};

use md5::{Digest as _, Md5};
use prolly::{AsyncProlly, Config, RuntimeConfig, Tree, TreeFormat};
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
    OperationIndexAdvanceReportV2, ProllyObjectStore, RandomIdSource, RepositoryFormatV2, Result,
    SegmentedOperationIndexV2, ShardWriterAuthorityV2, ShardedBranchPublisherV2, SystemClock,
    TakeoverRequestV2, TreeRootV1,
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
    publication_lanes: std::sync::Mutex<BTreeMap<String, Weak<tokio::sync::Mutex<()>>>>,
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
        Ok(Self {
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
            publication_lanes: std::sync::Mutex::new(BTreeMap::new()),
        })
    }

    pub fn format(&self) -> &RepositoryFormatV2 {
        &self.format
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
        &mut self,
        branch: &str,
        expected_writer: &str,
        expected_generation: u64,
        handoff_evidence: &str,
    ) -> Result<u64> {
        if !self.options.read_only {
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
        self.options.read_only = false;
        Ok(generation)
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

    pub async fn put_object_with_operation(
        &self,
        branch: &str,
        key: Vec<u8>,
        bytes: Vec<u8>,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        operation: OperationId,
    ) -> Result<CommitReceiptV2> {
        if self.options.read_only {
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
        let permit = self
            .permits
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 permit lock poisoned"))?
            .get(branch)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::PreconditionFailed,
                    "no active local authority permit for this v2 branch; explicitly take over or create the branch",
                )
            })?;
        self.authority.validate_active(&permit, now).await?;
        Ok(permit)
    }

    fn install_permit(&self, branch: &str, permit: AuthorityPermitV2) -> Result<()> {
        self.permits
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "v2 permit lock poisoned"))?
            .insert(branch.to_string(), permit);
        Ok(())
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
    let mut output = Vec::with_capacity(key.len() + 2 + 8 + 4 + 32);
    for byte in key {
        if *byte == 0 {
            output.extend_from_slice(&[0, 0xff]);
        } else {
            output.push(*byte);
        }
    }
    output.extend_from_slice(&[0, 0]);
    output.extend(order.commit_generation.0.to_be_bytes().map(|byte| !byte));
    output.extend(order.mutation_ordinal.to_be_bytes().map(|byte| !byte));
    output.extend(version.as_bytes().iter().map(|byte| !byte));
    output
}
