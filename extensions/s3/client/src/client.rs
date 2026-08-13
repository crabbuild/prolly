use std::{
    collections::BTreeMap,
    io::Write as _,
    sync::{Arc, Mutex},
    time::Duration,
};

use aws_sdk_s3::primitives::ByteStream;
use md5::Md5;
use prolly_s3_core::{
    BatchId, BranchCatalogPage, BranchHead, BranchIndexAdvanceReport, BranchIndexHealth, CommitId,
    CommitReceipt, CommitSessionManifest, Error, ErrorCode, JournalIndexRebuildCleanup,
    JournalIndexRebuildCursor, JournalIndexRebuildStep, MergeAdvancePage, MergeBaseCursor,
    MergeBasePage, MergeChangeCursor, MergeChangePage, MergeCleanupCursor, MergeCleanupPage,
    MergeConflictCursor, MergeConflictPage, MergeCursor, MergePolicy, MergeReceipt, ObjectData,
    ObjectHeaders, ObjectSummary, ObjectVersion, OperationId, OperationIndexRebuildCursor,
    OperationIndexRebuildStep, ProviderAttestation, ProviderPerKeyVersionLimit, ProviderProfileId,
    RefCatalogCursor, RefCatalogRepairPage, RefKind, Repository, RepositoryOptions, Result,
    StagedMutation, Tag, TagCatalogPage, VersionSummary,
};
use sha2::{Digest as _, Sha256};

use crate::{
    ensure_attestation_current, load_valid_attestation, qualify_and_store,
    validate_provider_bucket, AttestationSigner, AwsS3ObjectPlane, ProviderIdentity,
    ProviderQualificationOptions, S3OperationMetrics,
};

/// Application-facing repository client.
///
/// This client is the sole authority for the repository prefix and never
/// dual-writes another format.
#[derive(Clone)]
pub struct Client {
    repository: Arc<Repository<AwsS3ObjectPlane>>,
    bucket: String,
    branch: String,
    provider_attestation: ProviderAttestation,
    shard_authority_maintenance: Arc<Mutex<Option<prolly_s3_core::ShardAuthorityMaintenance>>>,
    _branch_index_maintenance: Arc<Mutex<Option<prolly_s3_core::BranchIndexMaintenance>>>,
}

#[derive(Default)]
pub struct ClientBuilder {
    aws_client: Option<aws_sdk_s3::Client>,
    bucket: Option<String>,
    repository_prefix: Option<String>,
    default_branch: Option<String>,
    writer: Option<String>,
    authority_lease_duration: Option<Duration>,
    read_only: bool,
    max_cached_node_pack_bytes: Option<usize>,
    max_cached_node_locations: Option<usize>,
    max_cached_node_bytes: Option<usize>,
    node_cache: Option<Arc<dyn prolly_s3_core::NodeCache>>,
    mutable_control_versions_to_retain: Option<usize>,
    journal_index_max_unindexed_events: Option<usize>,
    operation_index_leaf_entries: Option<usize>,
    operation_index_merge_fanout: Option<usize>,
    operation_index_max_unindexed_events: Option<usize>,
    provider_identity: Option<ProviderIdentity>,
    attestation_signer: Option<Arc<dyn AttestationSigner>>,
    provider_attestation: Option<ProviderProfileId>,
    qualification_options: Option<ProviderQualificationOptions>,
    provider_per_key_version_limit: Option<ProviderPerKeyVersionLimit>,
    background_index_maintenance: Option<bool>,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn branch(&self) -> &str {
        &self.branch
    }

    pub fn repository_id(&self) -> prolly_s3_core::RepositoryId {
        self.repository.repository_id()
    }

    pub fn for_branch(&self, branch: impl Into<String>) -> Result<Self> {
        let branch = branch.into();
        prolly_s3_core::validate_branch(&branch)?;
        let mut client = self.clone();
        client.branch = branch;
        Ok(client)
    }

    pub async fn head(&self) -> Result<CommitId> {
        self.ensure_provider_qualified()?;
        self.repository.head(&self.branch).await
    }

    pub async fn create_branch(
        &self,
        name: impl AsRef<str>,
        from: Option<CommitId>,
    ) -> Result<BranchHead> {
        self.ensure_provider_qualified()?;
        let from = match from {
            Some(from) => from,
            None => self.repository.head(&self.branch).await?,
        };
        self.repository.create_branch(name.as_ref(), from).await
    }

    pub async fn delete_branch(&self, name: impl AsRef<str>, expected: CommitId) -> Result<()> {
        self.ensure_provider_qualified()?;
        self.repository.delete_branch(name.as_ref(), expected).await
    }

    pub async fn create_tag(&self, name: impl AsRef<str>, target: CommitId) -> Result<Tag> {
        self.ensure_provider_qualified()?;
        self.repository.create_tag(name.as_ref(), target).await
    }

    pub async fn tag(&self, name: impl AsRef<str>) -> Result<Tag> {
        self.ensure_provider_qualified()?;
        self.repository.tag(name.as_ref()).await
    }

    pub async fn delete_tag(&self, name: impl AsRef<str>, expected: CommitId) -> Result<()> {
        self.ensure_provider_qualified()?;
        self.repository.delete_tag(name.as_ref(), expected).await
    }

    /// Start a restartable structural merge from `source_branch` into this
    /// client's selected branch.
    pub async fn start_merge(
        &self,
        source_branch: impl AsRef<str>,
        selected_base: Option<CommitId>,
        policy: MergePolicy,
        message: impl Into<String>,
    ) -> Result<MergeCursor> {
        self.ensure_provider_qualified()?;
        self.repository
            .start_merge(
                &self.branch,
                source_branch.as_ref(),
                selected_base,
                policy,
                message,
            )
            .await
    }

    pub async fn advance_merge(
        &self,
        cursor: &MergeCursor,
        max_steps: usize,
    ) -> Result<MergeAdvancePage> {
        self.ensure_provider_qualified()?;
        self.repository.advance_merge(cursor, max_steps).await
    }

    pub async fn select_merge_base(
        &self,
        cursor: &MergeCursor,
        base: CommitId,
    ) -> Result<MergeCursor> {
        self.ensure_provider_qualified()?;
        self.repository.select_merge_base(cursor, base).await
    }

    pub async fn merge_bases_page(
        &self,
        cursor: &MergeCursor,
        continuation: Option<&MergeBaseCursor>,
        limit: usize,
    ) -> Result<MergeBasePage> {
        self.ensure_provider_qualified()?;
        self.repository
            .merge_bases_page(cursor, continuation, limit)
            .await
    }

    pub async fn merge_changes_page(
        &self,
        cursor: &MergeCursor,
        continuation: Option<&MergeChangeCursor>,
        limit: usize,
    ) -> Result<MergeChangePage> {
        self.ensure_provider_qualified()?;
        self.repository
            .merge_changes_page(cursor, continuation, limit)
            .await
    }

    pub async fn merge_conflicts_page(
        &self,
        cursor: &MergeCursor,
        continuation: Option<&MergeConflictCursor>,
        limit: usize,
    ) -> Result<MergeConflictPage> {
        self.ensure_provider_qualified()?;
        self.repository
            .merge_conflicts_page(cursor, continuation, limit)
            .await
    }

    pub async fn publish_merge(&self, cursor: &MergeCursor) -> Result<MergeReceipt> {
        self.ensure_provider_qualified()?;
        self.repository.publish_merge(cursor).await
    }

    pub async fn cleanup_merge(
        &self,
        cursor: &MergeCursor,
        continuation: Option<&MergeCleanupCursor>,
        limit: usize,
    ) -> Result<MergeCleanupPage> {
        self.ensure_provider_qualified()?;
        self.repository
            .cleanup_merge(cursor, continuation, limit)
            .await
    }

    pub async fn list_branch_catalog_page(
        &self,
        cursor: Option<RefCatalogCursor>,
        limit: usize,
    ) -> Result<BranchCatalogPage> {
        self.ensure_provider_qualified()?;
        self.repository
            .list_branch_catalog_page(cursor, limit)
            .await
    }

    pub async fn list_tag_catalog_page(
        &self,
        cursor: Option<RefCatalogCursor>,
        limit: usize,
    ) -> Result<TagCatalogPage> {
        self.ensure_provider_qualified()?;
        self.repository.list_tag_catalog_page(cursor, limit).await
    }

    pub async fn repair_branch_catalog_page(
        &self,
        continuation: Option<String>,
        limit: usize,
    ) -> Result<RefCatalogRepairPage> {
        self.ensure_provider_qualified()?;
        self.repository
            .repair_ref_catalog_page(RefKind::Branch, continuation, limit)
            .await
    }

    pub async fn repair_tag_catalog_page(
        &self,
        continuation: Option<String>,
        limit: usize,
    ) -> Result<RefCatalogRepairPage> {
        self.ensure_provider_qualified()?;
        self.repository
            .repair_ref_catalog_page(RefKind::Tag, continuation, limit)
            .await
    }

    pub async fn put_object(
        &self,
        key: impl Into<String>,
        bytes: Vec<u8>,
    ) -> Result<CommitReceipt> {
        self.put_object_with_metadata(key, bytes, ObjectHeaders::default(), BTreeMap::new())
            .await
    }

    pub async fn put_object_with_metadata(
        &self,
        key: impl Into<String>,
        bytes: Vec<u8>,
        headers: ObjectHeaders,
        metadata: BTreeMap<String, String>,
    ) -> Result<CommitReceipt> {
        self.ensure_provider_qualified()?;
        self.repository
            .put_object(
                &self.branch,
                key.into().into_bytes(),
                bytes,
                headers,
                metadata,
            )
            .await
    }

    /// Put one object with a caller-stable operation ID. Reuse the same ID and
    /// exact input after an ambiguous response to reconcile the committed
    /// result without uploading the payload again.
    pub async fn put_object_with_operation(
        &self,
        key: impl Into<String>,
        bytes: Vec<u8>,
        operation: OperationId,
    ) -> Result<CommitReceipt> {
        self.ensure_provider_qualified()?;
        self.repository
            .put_object_with_operation(
                &self.branch,
                key.into().into_bytes(),
                bytes,
                ObjectHeaders::default(),
                BTreeMap::new(),
                operation,
            )
            .await
    }

    pub async fn get_object(&self, key: impl AsRef<str>) -> Result<Option<ObjectData>> {
        self.ensure_provider_qualified()?;
        self.repository
            .get_object(&self.branch, key.as_ref().as_bytes())
            .await
    }

    pub async fn get_object_at(
        &self,
        snapshot: CommitId,
        key: impl AsRef<str>,
    ) -> Result<Option<ObjectData>> {
        self.ensure_provider_qualified()?;
        self.repository
            .get_object_at(&self.branch, snapshot, key.as_ref().as_bytes())
            .await
    }

    pub async fn delete_object(&self, key: impl Into<String>) -> Result<CommitReceipt> {
        self.ensure_provider_qualified()?;
        self.repository
            .delete_object(&self.branch, key.into().into_bytes())
            .await
    }

    /// Begin an atomic repository commit session. Payloads are uploaded as
    /// they are staged, so the session retains metadata rather than complete
    /// object bodies in process memory.
    pub fn begin_commit(&self) -> CommitSessionBuilder {
        CommitSessionBuilder {
            client: self.clone(),
            message: "atomic repository commit".to_string(),
            expires_after: Duration::from_secs(60 * 60),
            durable: true,
            checkpoint_every: 256,
        }
    }

    /// Resume the latest canonical remote checkpoint for a session. Verified
    /// immutable payload bindings are reused; bodies are not uploaded again.
    pub async fn resume_commit(&self, batch: BatchId) -> Result<CommitSession> {
        self.ensure_provider_qualified()?;
        let checkpoint = self.repository.resume_commit_session(batch).await?;
        Ok(CommitSession {
            client: self.clone(),
            manifest: checkpoint.session,
            staged: checkpoint
                .mutations
                .into_iter()
                .map(|mutation| (mutation.key().to_vec(), mutation))
                .collect(),
            durable: true,
            checkpoint_every: 256,
            checkpoint_sequence: checkpoint.sequence,
            dirty_mutations: 0,
        })
    }

    pub async fn list_objects(
        &self,
        prefix: impl AsRef<str>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(CommitId, Vec<ObjectSummary>, bool)> {
        self.ensure_provider_qualified()?;
        self.repository
            .list_objects(
                &self.branch,
                prefix.as_ref().as_bytes(),
                after.map(str::as_bytes),
                limit,
            )
            .await
    }

    pub async fn list_objects_at(
        &self,
        snapshot: CommitId,
        prefix: impl AsRef<str>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<ObjectSummary>, bool)> {
        self.ensure_provider_qualified()?;
        self.repository
            .list_objects_at(
                &self.branch,
                snapshot,
                prefix.as_ref().as_bytes(),
                after.map(str::as_bytes),
                limit,
            )
            .await
    }

    pub async fn list_object_versions(
        &self,
        key: impl AsRef<str>,
        limit: usize,
    ) -> Result<(CommitId, Vec<ObjectVersion>)> {
        self.ensure_provider_qualified()?;
        self.repository
            .list_object_versions(&self.branch, key.as_ref().as_bytes(), limit)
            .await
    }

    pub async fn list_versions_prefix(
        &self,
        prefix: impl AsRef<str>,
        limit: usize,
    ) -> Result<(CommitId, Vec<VersionSummary>)> {
        self.ensure_provider_qualified()?;
        self.repository
            .list_versions_prefix(&self.branch, prefix.as_ref().as_bytes(), limit)
            .await
    }

    pub async fn list_versions_at(
        &self,
        snapshot: CommitId,
        prefix: impl AsRef<str>,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<VersionSummary>, bool)> {
        self.ensure_provider_qualified()?;
        self.repository
            .list_versions_at(
                &self.branch,
                snapshot,
                prefix.as_ref().as_bytes(),
                after,
                limit,
            )
            .await
    }

    pub async fn takeover_branch_writer(
        &self,
        branch: impl AsRef<str>,
        expected_writer: &str,
        expected_generation: u64,
        handoff_evidence: &str,
    ) -> Result<u64> {
        self.ensure_provider_qualified()?;
        let generation = self
            .repository
            .takeover_branch_writer(
                branch.as_ref(),
                expected_writer,
                expected_generation,
                handoff_evidence,
            )
            .await?;
        self.ensure_shard_authority_maintenance()?;
        Ok(generation)
    }

    pub async fn advance_branch_indexes(&self) -> Result<BranchIndexAdvanceReport> {
        self.ensure_provider_qualified()?;
        self.repository.advance_branch_indexes(&self.branch).await
    }

    pub async fn branch_index_health(&self) -> Result<BranchIndexHealth> {
        self.ensure_provider_qualified()?;
        self.repository.branch_index_health(&self.branch).await
    }

    pub async fn start_branch_index_rebuild(&self) -> Result<JournalIndexRebuildCursor> {
        self.ensure_provider_qualified()?;
        self.repository
            .start_branch_index_rebuild(&self.branch)
            .await
    }

    pub async fn advance_branch_index_rebuild(
        &self,
        cursor: &JournalIndexRebuildCursor,
        max_events: usize,
    ) -> Result<JournalIndexRebuildStep> {
        self.ensure_provider_qualified()?;
        self.repository
            .advance_branch_index_rebuild(cursor, max_events)
            .await
    }

    pub async fn cleanup_branch_index_rebuild(
        &self,
        journal: &JournalIndexRebuildCursor,
        operations: &OperationIndexRebuildCursor,
        limit: usize,
    ) -> Result<JournalIndexRebuildCleanup> {
        self.ensure_provider_qualified()?;
        self.repository
            .cleanup_branch_index_rebuild(journal, operations, limit)
            .await
    }

    pub async fn start_operation_index_rebuild(
        &self,
        journal: &JournalIndexRebuildCursor,
    ) -> Result<OperationIndexRebuildCursor> {
        self.ensure_provider_qualified()?;
        self.repository.start_operation_index_rebuild(journal).await
    }

    pub async fn advance_operation_index_rebuild(
        &self,
        cursor: &OperationIndexRebuildCursor,
        max_events: usize,
    ) -> Result<OperationIndexRebuildStep> {
        self.ensure_provider_qualified()?;
        self.repository
            .advance_operation_index_rebuild(cursor, max_events)
            .await
    }

    pub async fn wait_for_branch_indexes(&self, timeout: Duration) -> Result<BranchIndexHealth> {
        self.ensure_provider_qualified()?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let health = self.repository.branch_index_health(&self.branch).await?;
            if health.ready {
                return Ok(health);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::new(
                    ErrorCode::MissingClosure,
                    health.last_error.unwrap_or_else(|| {
                        format!(
                            "repository branch indexes remain {} generation(s) behind",
                            health.lag_generations
                        )
                    }),
                ));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn cleanup_expired_commit_sessions(
        &self,
        continuation: Option<String>,
        limit: usize,
    ) -> Result<prolly_s3_core::CommitSessionCleanupReport> {
        self.ensure_provider_qualified()?;
        self.repository
            .cleanup_expired_commit_sessions(continuation, limit)
            .await
    }

    pub fn s3_operation_metrics(&self) -> S3OperationMetrics {
        self.repository.plane().metrics()
    }

    pub fn reset_s3_operation_metrics(&self) -> S3OperationMetrics {
        self.repository.plane().reset_metrics()
    }

    pub fn fenced_branches(&self) -> Result<Vec<String>> {
        self.repository.fenced_branches()
    }

    pub(crate) fn ensure_provider_qualified(&self) -> Result<()> {
        ensure_attestation_current(&self.provider_attestation)?;
        self.provider_attestation
            .body
            .capabilities
            .validate_prolly_s3()
    }

    fn ensure_shard_authority_maintenance(&self) -> Result<()> {
        let mut maintenance = self
            .shard_authority_maintenance
            .lock()
            .map_err(|_| invalid("shard-authority maintenance lock is poisoned"))?;
        if maintenance.is_none() {
            *maintenance = Some(self.repository.start_shard_authority_maintenance()?);
        }
        Ok(())
    }
}

pub struct CommitSessionBuilder {
    client: Client,
    message: String,
    expires_after: Duration,
    durable: bool,
    checkpoint_every: usize,
}

impl CommitSessionBuilder {
    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }

    pub fn expires_after(mut self, expires_after: Duration) -> Self {
        self.expires_after = expires_after;
        self
    }

    /// Disable remote checkpoints for the minimum N + 3 S3 PUT shape. The
    /// session cannot then be resumed after process loss.
    pub fn ephemeral(mut self) -> Self {
        self.durable = false;
        self
    }

    /// Checkpoint after this many staged mutations. The final state is always
    /// checkpointed before publication for durable sessions.
    pub fn checkpoint_every(mut self, mutations: usize) -> Self {
        self.checkpoint_every = mutations;
        self
    }

    pub async fn start(self) -> Result<CommitSession> {
        self.client.ensure_provider_qualified()?;
        let expires_after_millis = u64::try_from(self.expires_after.as_millis())
            .map_err(|_| invalid("commit-session expiry exceeds u64 milliseconds"))?;
        if self.durable && self.checkpoint_every == 0 {
            return Err(invalid("checkpoint interval must be positive"));
        }
        let (manifest, checkpoint_sequence) = if self.durable {
            let checkpoint = self
                .client
                .repository
                .begin_durable_commit_session(
                    &self.client.branch,
                    self.message,
                    expires_after_millis,
                )
                .await?;
            (checkpoint.session, checkpoint.sequence)
        } else {
            (
                self.client
                    .repository
                    .begin_commit_session(&self.client.branch, self.message, expires_after_millis)
                    .await?,
                0,
            )
        };
        Ok(CommitSession {
            client: self.client,
            manifest,
            staged: BTreeMap::new(),
            durable: self.durable,
            checkpoint_every: self.checkpoint_every,
            checkpoint_sequence,
            dirty_mutations: 0,
        })
    }
}

pub struct CommitSession {
    client: Client,
    manifest: CommitSessionManifest,
    staged: BTreeMap<Vec<u8>, StagedMutation>,
    durable: bool,
    checkpoint_every: usize,
    checkpoint_sequence: u64,
    dirty_mutations: usize,
}

impl CommitSession {
    pub fn id(&self) -> prolly_s3_core::BatchId {
        self.manifest.id
    }

    pub fn operation(&self) -> prolly_s3_core::OperationId {
        self.manifest.identity.operation
    }

    pub fn base_commit(&self) -> CommitId {
        self.manifest.base_commit
    }

    pub fn staged_objects(&self) -> usize {
        self.staged.len()
    }

    pub fn is_durable(&self) -> bool {
        self.durable
    }

    pub async fn put_object(&mut self, key: impl Into<String>, bytes: Vec<u8>) -> Result<()> {
        self.put_object_with_metadata(key, bytes, ObjectHeaders::default(), BTreeMap::new())
            .await
    }

    pub async fn put_object_with_metadata(
        &mut self,
        key: impl Into<String>,
        bytes: Vec<u8>,
        headers: ObjectHeaders,
        metadata: BTreeMap<String, String>,
    ) -> Result<()> {
        self.client.ensure_provider_qualified()?;
        let staged = self
            .client
            .repository
            .stage_commit_session_put(
                &self.manifest,
                key.into().into_bytes(),
                bytes,
                headers,
                metadata,
            )
            .await?;
        self.staged.insert(staged.key().to_vec(), staged);
        self.mark_staged_and_checkpoint_if_due().await?;
        Ok(())
    }

    /// Stage a streamed object through a bounded-memory disk spool. The spool
    /// is removed after its immutable content-addressed upload completes.
    pub async fn put_stream(&mut self, key: impl Into<String>, body: ByteStream) -> Result<()> {
        self.put_stream_with_metadata(key, body, ObjectHeaders::default(), BTreeMap::new())
            .await
    }

    pub async fn put_stream_with_metadata(
        &mut self,
        key: impl Into<String>,
        mut body: ByteStream,
        headers: ObjectHeaders,
        metadata: BTreeMap<String, String>,
    ) -> Result<()> {
        self.client.ensure_provider_qualified()?;
        let key = key.into().into_bytes();
        if key.is_empty() {
            return Err(invalid("commit-session put key is empty"));
        }
        let mut spool = tempfile::NamedTempFile::new().map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("could not create upload spool: {error}"),
            )
        })?;
        let max_object_bytes = self.client.repository.max_object_bytes();
        let mut size = 0_u64;
        let mut sha256 = Sha256::new();
        let mut md5 = Md5::new();
        while let Some(next) = body.next().await {
            let next = next.map_err(|error| {
                Error::new(
                    ErrorCode::Transport,
                    format!("object input stream failed: {error}"),
                )
            })?;
            size = size
                .checked_add(next.len() as u64)
                .ok_or_else(|| Error::new(ErrorCode::EntityTooLarge, "object length overflow"))?;
            if size > max_object_bytes {
                return Err(Error::new(
                    ErrorCode::EntityTooLarge,
                    "object exceeds the repository object-size limit",
                ));
            }
            spool.write_all(&next).map_err(|error| {
                Error::new(
                    ErrorCode::Transport,
                    format!("upload spool write failed: {error}"),
                )
            })?;
            sha256.update(&next);
            md5.update(&next);
        }
        spool.flush().map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("upload spool flush failed: {error}"),
            )
        })?;
        let staged = self
            .client
            .repository
            .stage_commit_session_file(
                &self.manifest,
                key,
                spool.path().to_path_buf(),
                size,
                sha256.finalize().into(),
                md5.finalize().into(),
                headers,
                metadata,
            )
            .await?;
        self.staged.insert(staged.key().to_vec(), staged);
        self.mark_staged_and_checkpoint_if_due().await?;
        Ok(())
    }

    pub fn delete_object(&mut self, key: impl Into<String>) -> Result<()> {
        let key = key.into().into_bytes();
        if key.is_empty() {
            return Err(invalid("commit-session delete key is empty"));
        }
        self.staged.insert(key.clone(), StagedMutation::delete(key));
        self.dirty_mutations = self.dirty_mutations.saturating_add(1);
        Ok(())
    }

    pub async fn checkpoint(&mut self) -> Result<()> {
        if !self.durable || self.dirty_mutations == 0 {
            return Ok(());
        }
        let sequence = self
            .checkpoint_sequence
            .checked_add(1)
            .ok_or_else(|| invalid("commit-session checkpoint sequence overflow"))?;
        let checkpoint = self
            .client
            .repository
            .checkpoint_commit_session(
                &self.manifest,
                self.staged.values().cloned().collect(),
                sequence,
            )
            .await?;
        self.manifest = checkpoint.session;
        self.checkpoint_sequence = checkpoint.sequence;
        self.dirty_mutations = 0;
        Ok(())
    }

    pub async fn publish(mut self) -> Result<CommitReceipt> {
        self.client.ensure_provider_qualified()?;
        self.checkpoint().await?;
        self.client
            .repository
            .publish_commit_session(self.manifest, self.staged.into_values().collect())
            .await
    }

    /// Mark a durable session aborted. Immutable payload candidates remain
    /// deduplicated and bounded staging cleanup removes expired checkpoints.
    pub async fn abort(self) -> Result<()> {
        if !self.durable {
            return Ok(());
        }
        let sequence = self
            .checkpoint_sequence
            .checked_add(1)
            .ok_or_else(|| invalid("commit-session checkpoint sequence overflow"))?;
        self.client
            .repository
            .abort_commit_session(self.manifest, self.staged.into_values().collect(), sequence)
            .await
    }

    async fn mark_staged_and_checkpoint_if_due(&mut self) -> Result<()> {
        self.dirty_mutations = self.dirty_mutations.saturating_add(1);
        if self.durable && self.dirty_mutations >= self.checkpoint_every {
            self.checkpoint().await?;
        }
        Ok(())
    }
}

impl ClientBuilder {
    pub fn aws_client(mut self, client: aws_sdk_s3::Client) -> Self {
        self.aws_client = Some(client);
        self
    }

    /// Enable or disable automatic branch-index catch-up. It is enabled by
    /// default; disabling it is intended for isolated request-shape probes.
    pub fn background_index_maintenance(mut self, enabled: bool) -> Self {
        self.background_index_maintenance = Some(enabled);
        self
    }

    pub fn journal_index_max_unindexed_events(mut self, events: usize) -> Self {
        self.journal_index_max_unindexed_events = Some(events);
        self
    }

    /// Configure the bounded operation-index shape. Production deployments
    /// normally use the defaults; smaller limits are useful for deterministic
    /// recovery qualification.
    pub fn operation_index_limits(
        mut self,
        leaf_entries: usize,
        merge_fanout: usize,
        max_unindexed_events: usize,
    ) -> Self {
        self.operation_index_leaf_entries = Some(leaf_entries);
        self.operation_index_merge_fanout = Some(merge_fanout);
        self.operation_index_max_unindexed_events = Some(max_unindexed_events);
        self
    }

    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    pub fn repository_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.repository_prefix = Some(prefix.into());
        self
    }

    pub fn default_branch(mut self, branch: impl Into<String>) -> Self {
        self.default_branch = Some(branch.into());
        self
    }

    pub fn writer(mut self, writer: impl Into<String>) -> Self {
        self.writer = Some(writer.into());
        self
    }

    pub fn authority_lease_duration(mut self, duration: Duration) -> Self {
        self.authority_lease_duration = Some(duration);
        self
    }

    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    pub fn node_cache(mut self, cache: Arc<dyn prolly_s3_core::NodeCache>) -> Self {
        self.node_cache = Some(cache);
        self
    }

    pub fn max_cached_node_pack_bytes(mut self, bytes: usize) -> Self {
        self.max_cached_node_pack_bytes = Some(bytes);
        self
    }

    pub fn max_cached_node_locations(mut self, locations: usize) -> Self {
        self.max_cached_node_locations = Some(locations);
        self
    }

    pub fn max_cached_node_bytes(mut self, bytes: usize) -> Self {
        self.max_cached_node_bytes = Some(bytes);
        self
    }

    pub fn mutable_control_version_retention(mut self, versions: usize) -> Self {
        self.mutable_control_versions_to_retain = Some(versions);
        self
    }

    /// Provider-attested maximum number of physical versions for one key.
    /// Unknown limits fail closed for repository initialization and open.
    pub fn provider_per_key_version_limit(mut self, limit: ProviderPerKeyVersionLimit) -> Self {
        self.provider_per_key_version_limit = Some(limit);
        self
    }

    pub fn provider_identity(mut self, identity: ProviderIdentity) -> Self {
        self.provider_identity = Some(identity);
        self
    }

    pub fn attestation_signer(mut self, signer: Arc<dyn AttestationSigner>) -> Self {
        self.attestation_signer = Some(signer);
        self
    }

    pub fn provider_attestation(mut self, id: ProviderProfileId) -> Self {
        self.provider_attestation = Some(id);
        self
    }

    pub fn provider_attestation_validity(mut self, validity: Duration) -> Self {
        self.qualification_options = Some(ProviderQualificationOptions { validity });
        self
    }

    pub async fn initialize(self) -> Result<Client> {
        self.finish(true).await
    }

    pub async fn open(self) -> Result<Client> {
        self.finish(false).await
    }

    async fn finish(self, initialize: bool) -> Result<Client> {
        if initialize && self.read_only {
            return Err(invalid(
                "repository initialization requires a writable client",
            ));
        }
        let background_index_maintenance = self.background_index_maintenance.unwrap_or(true);
        let aws = self
            .aws_client
            .ok_or_else(|| invalid("aws_client is required"))?;
        let bucket = self.bucket.ok_or_else(|| invalid("bucket is required"))?;
        let identity = self
            .provider_identity
            .ok_or_else(|| invalid("provider_identity is required"))?;
        validate_provider_bucket(&identity, &bucket)?;
        let signer = self
            .attestation_signer
            .ok_or_else(|| invalid("attestation_signer is required"))?;
        let provider_per_key_version_limit =
            self.provider_per_key_version_limit.ok_or_else(|| {
                Error::new(
                    ErrorCode::ProviderNotQualified,
                    "repository open requires an explicit provider per-key version-limit attestation",
                )
            })?;
        let prefix = self
            .repository_prefix
            .unwrap_or_else(|| RepositoryOptions::default().repository_prefix);
        let plane = Arc::new(AwsS3ObjectPlane::new(aws, bucket.clone()));
        let attestation = if initialize {
            match load_valid_attestation(
                plane.clone(),
                &prefix,
                &identity,
                signer.as_ref(),
                self.provider_attestation,
            )
            .await
            {
                Ok(attestation) => attestation,
                Err(error) if error.code == ErrorCode::ProviderNotQualified => {
                    qualify_and_store(
                        plane.clone(),
                        &prefix,
                        &identity,
                        signer.as_ref(),
                        &self.qualification_options.unwrap_or_default(),
                    )
                    .await?
                }
                Err(error) => return Err(error),
            }
        } else {
            load_valid_attestation(
                plane.clone(),
                &prefix,
                &identity,
                signer.as_ref(),
                self.provider_attestation,
            )
            .await?
        };
        attestation.body.capabilities.validate_prolly_s3()?;

        let mut options = RepositoryOptions {
            repository_prefix: prefix,
            read_only: self.read_only,
            provider_per_key_version_limit,
            ..RepositoryOptions::default()
        };
        if let Some(branch) = self.default_branch {
            options.default_branch = branch;
        }
        if let Some(writer) = self.writer {
            options.writer = writer;
        }
        if let Some(duration) = self.authority_lease_duration {
            options.authority_lease_millis = u64::try_from(duration.as_millis())
                .map_err(|_| invalid("authority lease duration exceeds u64 milliseconds"))?;
        }
        if let Some(bytes) = self.max_cached_node_pack_bytes {
            options.max_cached_node_pack_bytes = bytes;
        }
        if let Some(locations) = self.max_cached_node_locations {
            options.max_cached_node_locations = locations;
        }
        if let Some(bytes) = self.max_cached_node_bytes {
            options.max_cached_node_bytes = bytes;
        }
        if let Some(versions) = self.mutable_control_versions_to_retain {
            options.mutable_control_versions_to_retain = versions;
        }
        if let Some(events) = self.journal_index_max_unindexed_events {
            options.journal_index_max_unindexed_events = events;
        }
        if let Some(entries) = self.operation_index_leaf_entries {
            options.operation_index_leaf_entries = entries;
        }
        if let Some(fanout) = self.operation_index_merge_fanout {
            options.operation_index_merge_fanout = fanout;
        }
        if let Some(events) = self.operation_index_max_unindexed_events {
            options.operation_index_max_unindexed_events = events;
        }
        options.node_cache = self.node_cache;
        let branch = options.default_branch.clone();
        let repository = if initialize {
            Repository::initialize(plane, options).await?
        } else {
            Repository::open(plane, options).await?
        };
        let repository = Arc::new(repository);
        let shard_authority_maintenance = if self.read_only {
            None
        } else {
            Some(repository.start_shard_authority_maintenance()?)
        };
        let branch_index_maintenance = if background_index_maintenance {
            Some(repository.start_branch_index_maintenance(Duration::from_secs(5))?)
        } else {
            None
        };
        let client = Client {
            repository,
            bucket,
            branch,
            provider_attestation: attestation,
            shard_authority_maintenance: Arc::new(Mutex::new(shard_authority_maintenance)),
            _branch_index_maintenance: Arc::new(Mutex::new(branch_index_maintenance)),
        };
        if background_index_maintenance {
            client
                .wait_for_branch_indexes(Duration::from_secs(30))
                .await?;
        }
        Ok(client)
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::InvalidRequest, message)
}
