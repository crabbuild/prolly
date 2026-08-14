use std::{
    collections::{BTreeMap, VecDeque},
    str::FromStr as _,
    sync::{Arc, Mutex},
    time::Duration,
};

use aws_sdk_s3::primitives::ByteStream;
use futures_util::{stream, Stream, StreamExt};
use md5::Md5;
use prolly_s3_core::{
    BackupVerificationCursor, BackupVerificationPage, BatchId, BranchCatalogPage, BranchHead,
    BranchIndexAdvanceReport, BranchIndexHealth, CommitId, CommitPage, CommitReceipt,
    CommitSessionManifest, DelimitedObjectPage, Error, ErrorCode, FsckCursor, FsckPage, GcCursor,
    GcPage, HistoryCursor, HistoryTransferCursor, HistoryTransferMapping, HistoryTransferPage,
    JournalIndexRebuildCleanup, JournalIndexRebuildCursor, JournalIndexRebuildStep,
    ListObjectsPage, LogicalObjectVersionKind, MergeAdvancePage, MergeBaseCursor, MergeBasePage,
    MergeChangeCursor, MergeChangePage, MergeCleanupCursor, MergeCleanupPage, MergeConflictCursor,
    MergeConflictPage, MergeCursor, MergePolicy, MergeReceipt, NodeCachePrewarmReport, ObjectData,
    ObjectDiff, ObjectDiffCursor, ObjectDiffPage, ObjectHeaders, ObjectRangeData, ObjectSummary,
    ObjectVersion, OperationId, OperationIndexRebuildCursor, OperationIndexRebuildStep,
    PayloadPackStatsCursor, PayloadPackStatsPage, ProviderAttestation, ProviderPerKeyVersionLimit,
    ProviderProfileId, PublicationJournalCursor, PublicationJournalPage, RefCatalogCursor,
    RefCatalogRepairPage, RefKind, RefMoveReceipt, RepairCursor, RepairPage, Repository,
    RepositoryOptions, RestoreCursor, RestorePage, Result, RetentionPin, RetentionPinPage,
    StagedMutation, Tag, TagCatalogPage, TraversalBudget, VersionSummary,
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
    /// Branch used to resolve branch-local packed-node indexes. For a detached
    /// checkout this remains the branch from which checkout was requested.
    branch: String,
    checked_out: CheckedOutRef,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutObjectInput {
    pub key: String,
    pub bytes: Vec<u8>,
    pub headers: ObjectHeaders,
    pub user_metadata: BTreeMap<String, String>,
}

/// Bounded resource controls for [`Client::put_object_stream`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BulkWriteOptions {
    /// Maximum logical mutations published by one atomic commit.
    pub batch_size: usize,
    /// Maximum payload uploads in flight at once.
    pub concurrency: usize,
    /// Successfully staged mutations durably checkpointed as one window.
    pub checkpoint_every: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PayloadRepackOptions {
    pub page_size: usize,
    pub max_object_bytes: u64,
    pub max_batch_bytes: u64,
    pub concurrency: usize,
}

impl Default for PayloadRepackOptions {
    fn default() -> Self {
        Self {
            page_size: 1_000,
            max_object_bytes: 4 * 1_024,
            max_batch_bytes: 4 * 1_024 * 1_024,
            concurrency: 32,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadRepackPage {
    pub snapshot: CommitId,
    pub scanned_objects: usize,
    pub repacked_objects: usize,
    pub repacked_bytes: u64,
    pub receipt: Option<CommitReceipt>,
    pub continuation: Option<String>,
}

impl Default for BulkWriteOptions {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
            concurrency: 32,
            checkpoint_every: 1_000,
        }
    }
}

/// A revision accepted by [`Client::checkout`].
///
/// Unqualified names resolve branches before tags. Use `Branch` or `Tag` (or
/// the `refs/heads/` and `refs/tags/` string forms) when both have the same
/// name. Commit IDs always create a detached checkout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckoutRef {
    Name(String),
    Branch(String),
    Tag(String),
    Commit(CommitId),
}

impl From<String> for CheckoutRef {
    fn from(value: String) -> Self {
        Self::Name(value)
    }
}

impl From<&str> for CheckoutRef {
    fn from(value: &str) -> Self {
        Self::Name(value.to_string())
    }
}

impl From<&String> for CheckoutRef {
    fn from(value: &String) -> Self {
        Self::Name(value.clone())
    }
}

impl From<CommitId> for CheckoutRef {
    fn from(value: CommitId) -> Self {
        Self::Commit(value)
    }
}

impl From<&CommitId> for CheckoutRef {
    fn from(value: &CommitId) -> Self {
        Self::Commit(*value)
    }
}

/// The resolved revision selected by a client handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckedOutRef {
    Branch(String),
    Tag { name: String, target: CommitId },
    Commit(CommitId),
}

impl CheckedOutRef {
    pub fn target(&self) -> Option<CommitId> {
        match self {
            Self::Branch(_) => None,
            Self::Tag { target, .. } | Self::Commit(target) => Some(*target),
        }
    }
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Return the attached branch, or `None` for a tag/commit checkout.
    pub fn branch(&self) -> Option<&str> {
        match &self.checked_out {
            CheckedOutRef::Branch(branch) => Some(branch),
            CheckedOutRef::Tag { .. } | CheckedOutRef::Commit(_) => None,
        }
    }

    pub fn checked_out_ref(&self) -> &CheckedOutRef {
        &self.checked_out
    }

    pub fn repository_id(&self) -> prolly_s3_core::RepositoryId {
        self.repository.repository_id()
    }

    pub fn node_cache_snapshot(&self) -> prolly_s3_core::NodeCacheSnapshot {
        self.repository.node_cache_snapshot()
    }

    pub async fn prewarm_node_cache(&self, snapshot: CommitId) -> Result<NodeCachePrewarmReport> {
        self.ensure_provider_qualified()?;
        self.repository
            .prewarm_node_cache(&self.branch, snapshot)
            .await
    }

    /// Select a branch, tag, or immutable commit, following Git-like ref
    /// precedence. Branch checkouts are writable; tag and commit checkouts are
    /// detached and reject mutation APIs with `InvalidRevision`.
    pub async fn checkout(&self, reference: impl Into<CheckoutRef>) -> Result<Self> {
        self.ensure_provider_qualified()?;
        match reference.into() {
            CheckoutRef::Commit(id) => self.checkout_commit(id).await,
            CheckoutRef::Branch(name) => self.checkout_branch(name).await,
            CheckoutRef::Tag(name) => self.checkout_tag(name).await,
            CheckoutRef::Name(name) => {
                if let Some(name) = name.strip_prefix("refs/heads/") {
                    return self.checkout_branch(name.to_string()).await;
                }
                if let Some(name) = name.strip_prefix("refs/tags/") {
                    return self.checkout_tag(name.to_string()).await;
                }
                if let Ok(id) = CommitId::from_str(&name) {
                    return self.checkout_commit(id).await;
                }
                match self.checkout_branch(name.clone()).await {
                    Ok(client) => Ok(client),
                    Err(error) if error.code == ErrorCode::InvalidRevision => {
                        self.checkout_tag(name).await
                    }
                    Err(error) => Err(error),
                }
            }
        }
    }

    async fn checkout_branch(&self, branch: String) -> Result<Self> {
        prolly_s3_core::validate_branch(&branch)?;
        self.repository.head(&branch).await?;
        let mut client = self.clone();
        client.branch = branch.clone();
        client.checked_out = CheckedOutRef::Branch(branch);
        Ok(client)
    }

    async fn checkout_tag(&self, name: String) -> Result<Self> {
        prolly_s3_core::validate_branch(&name)?;
        let tag = self.repository.tag(&name).await?;
        self.repository.commit(tag.target).await?;
        let mut client = self.clone();
        client.checked_out = CheckedOutRef::Tag {
            name,
            target: tag.target,
        };
        Ok(client)
    }

    async fn checkout_commit(&self, id: CommitId) -> Result<Self> {
        self.repository.commit(id).await?;
        let mut client = self.clone();
        client.checked_out = CheckedOutRef::Commit(id);
        Ok(client)
    }

    fn attached_branch(&self) -> Result<&str> {
        self.branch().ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidRevision,
                "mutation or branch-ref operation requires an attached branch checkout",
            )
        })
    }

    pub async fn head(&self) -> Result<CommitId> {
        self.ensure_provider_qualified()?;
        match self.checked_out.target() {
            Some(target) => Ok(target),
            None => self.repository.head(&self.branch).await,
        }
    }

    pub async fn create_branch(
        &self,
        name: impl AsRef<str>,
        from: Option<CommitId>,
    ) -> Result<BranchHead> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        let from = match from {
            Some(from) => from,
            None => self.head().await?,
        };
        self.repository
            .create_branch_from(self.attached_branch()?, name.as_ref(), from)
            .await
    }

    pub async fn delete_branch(&self, name: impl AsRef<str>, expected: CommitId) -> Result<()> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository.delete_branch(name.as_ref(), expected).await
    }

    pub async fn create_tag(&self, name: impl AsRef<str>, target: CommitId) -> Result<Tag> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository.create_tag(name.as_ref(), target).await
    }

    pub async fn tag(&self, name: impl AsRef<str>) -> Result<Tag> {
        self.ensure_provider_qualified()?;
        self.repository.tag(name.as_ref()).await
    }

    pub async fn delete_tag(&self, name: impl AsRef<str>, expected: CommitId) -> Result<()> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository.delete_tag(name.as_ref(), expected).await
    }

    pub async fn create_retention_pin(
        &self,
        name: impl AsRef<str>,
        target: CommitId,
    ) -> Result<RetentionPin> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository
            .create_retention_pin(name.as_ref(), target)
            .await
    }

    pub async fn retention_pin(&self, name: impl AsRef<str>) -> Result<RetentionPin> {
        self.ensure_provider_qualified()?;
        self.repository.retention_pin(name.as_ref()).await
    }

    pub async fn delete_retention_pin(
        &self,
        name: impl AsRef<str>,
        expected: CommitId,
    ) -> Result<()> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository
            .delete_retention_pin(name.as_ref(), expected)
            .await
    }

    pub async fn list_retention_pins_page(
        &self,
        cursor: Option<RefCatalogCursor>,
        limit: usize,
    ) -> Result<RetentionPinPage> {
        self.ensure_provider_qualified()?;
        self.repository
            .list_retention_pins_page(cursor, limit)
            .await
    }

    pub async fn commit(&self, id: CommitId) -> Result<prolly_s3_core::BucketCommit> {
        self.ensure_provider_qualified()?;
        self.repository.commit(id).await
    }

    pub async fn log(&self, limit: usize) -> Result<Vec<(CommitId, prolly_s3_core::BucketCommit)>> {
        self.ensure_provider_qualified()?;
        match self.checked_out.target() {
            Some(target) => Ok(self
                .repository
                .log_page_bounded(
                    &self.branch,
                    target,
                    None,
                    limit,
                    TraversalBudget::default(),
                )
                .await?
                .commits),
            None => self.repository.log(&self.branch, limit).await,
        }
    }

    pub async fn log_bounded(
        &self,
        start: CommitId,
        cursor: Option<&HistoryCursor>,
        limit: usize,
        budget: TraversalBudget,
    ) -> Result<CommitPage> {
        self.ensure_provider_qualified()?;
        self.repository
            .log_page_bounded(&self.branch, start, cursor, limit, budget)
            .await
    }

    pub async fn diff(&self, from: CommitId, to: CommitId) -> Result<Vec<ObjectDiff>> {
        self.ensure_provider_qualified()?;
        self.repository.diff(&self.branch, from, to).await
    }

    pub async fn diff_bounded(
        &self,
        from: CommitId,
        to: CommitId,
        cursor: Option<&ObjectDiffCursor>,
        limit: usize,
    ) -> Result<ObjectDiffPage> {
        self.ensure_provider_qualified()?;
        self.repository
            .diff_page_bounded(&self.branch, from, to, cursor, limit)
            .await
    }

    pub async fn open_reflog(&self) -> Result<PublicationJournalCursor> {
        self.ensure_provider_qualified()?;
        self.repository.open_reflog(self.attached_branch()?).await
    }

    pub async fn read_reflog_page(
        &self,
        cursor: &PublicationJournalCursor,
        limit: usize,
    ) -> Result<PublicationJournalPage> {
        self.ensure_provider_qualified()?;
        self.repository.read_reflog_page(cursor, limit).await
    }

    pub async fn reset_branch(
        &self,
        to: CommitId,
        expected_head: CommitId,
        reason: impl AsRef<str>,
    ) -> Result<RefMoveReceipt> {
        self.ensure_provider_qualified()?;
        let branch = self.attached_branch()?;
        self.repository
            .reset_branch(branch, to, expected_head, reason.as_ref())
            .await
    }

    pub async fn recover_branch(
        &self,
        reflog: prolly_s3_core::ReflogEntryId,
        expected_head: CommitId,
        reason: impl AsRef<str>,
    ) -> Result<RefMoveReceipt> {
        self.ensure_provider_qualified()?;
        let branch = self.attached_branch()?;
        self.repository
            .recover_branch(branch, reflog, expected_head, reason.as_ref())
            .await
    }

    pub async fn start_fsck(&self, deep: bool) -> Result<FsckCursor> {
        self.ensure_provider_qualified()?;
        self.repository
            .start_fsck(self.attached_branch()?, deep)
            .await
    }

    pub async fn advance_fsck(&self, cursor: &FsckCursor, max_steps: usize) -> Result<FsckPage> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository.advance_fsck(cursor, max_steps).await
    }

    pub async fn start_payload_pack_stats(&self) -> Result<PayloadPackStatsCursor> {
        self.ensure_provider_qualified()?;
        self.repository
            .start_payload_pack_stats(self.attached_branch()?)
            .await
    }

    pub async fn advance_payload_pack_stats(
        &self,
        cursor: &PayloadPackStatsCursor,
        max_objects: usize,
    ) -> Result<PayloadPackStatsPage> {
        self.ensure_provider_qualified()?;
        self.repository
            .advance_payload_pack_stats(cursor, max_objects)
            .await
    }

    /// Repack one snapshot-bound page of direct payloads no larger than the
    /// configured threshold. Each page publishes one deterministic commit;
    /// historical versions remain readable until ordinary retention/GC makes
    /// their physical payloads unreachable.
    pub async fn repack_payloads_page(
        &self,
        prefix: impl AsRef<str>,
        continuation: Option<&str>,
        options: PayloadRepackOptions,
    ) -> Result<PayloadRepackPage> {
        self.ensure_provider_qualified()?;
        let branch = self.attached_branch()?;
        if !(1..=1_000).contains(&options.page_size)
            || options.max_object_bytes == 0
            || options.max_object_bytes > 4 * 1_024
            || options.concurrency == 0
            || options.concurrency > 1_024
            || options
                .max_object_bytes
                .checked_mul(options.page_size as u64)
                .is_none_or(|maximum| maximum > options.max_batch_bytes)
        {
            return Err(invalid("payload repack resource limits are invalid"));
        }
        let page = self
            .repository
            .list_objects_page(
                branch,
                prefix.as_ref().as_bytes(),
                continuation,
                options.page_size,
            )
            .await?;
        let selected = page
            .objects
            .iter()
            .filter_map(|object| {
                let LogicalObjectVersionKind::Live { size, .. } = &object.version.body.kind else {
                    return None;
                };
                let binding = object.version.binding.as_ref()?;
                (!binding.is_packed() && *size <= options.max_object_bytes)
                    .then(|| object.key.clone())
            })
            .collect::<Vec<_>>();
        let loaded = stream::iter(selected)
            .map(|key| async move {
                self.repository
                    .get_object_at(branch, page.snapshot, &key)
                    .await?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorCode::MissingClosure,
                            "repack snapshot object disappeared",
                        )
                    })
            })
            .buffer_unordered(options.concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let mut inputs = Vec::with_capacity(loaded.len());
        let mut repacked_bytes = 0_u64;
        for object in loaded {
            let LogicalObjectVersionKind::Live {
                size,
                headers,
                user_metadata,
                tags,
                ..
            } = object.version.body.kind
            else {
                return Err(invalid("repack selected a non-live object"));
            };
            repacked_bytes = repacked_bytes
                .checked_add(size)
                .ok_or_else(|| invalid("payload repack byte count overflow"))?;
            inputs.push((object.key, object.bytes, headers, user_metadata, tags));
        }
        let repacked_objects = inputs.len();
        let receipt = if inputs.is_empty() {
            None
        } else {
            let mut session = self
                .begin_commit()
                .message("payload pack maintenance")
                .start()
                .await?;
            let staged = self
                .repository
                .stage_commit_session_repack_batch(&session.manifest, inputs, options.concurrency)
                .await?;
            for mutation in staged {
                session.insert_staged(mutation);
            }
            Some(session.publish().await?)
        };
        Ok(PayloadRepackPage {
            snapshot: page.snapshot,
            scanned_objects: page.objects.len(),
            repacked_objects,
            repacked_bytes,
            receipt,
            continuation: page.continuation,
        })
    }

    pub async fn start_gc(&self, grace_millis: u64) -> Result<GcCursor> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository.start_gc(grace_millis).await
    }

    pub async fn resume_gc(&self) -> Result<Option<GcCursor>> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository.resume_gc().await
    }

    pub async fn abandon_gc(&self, expected_epoch: prolly_s3_core::OperationId) -> Result<()> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository.abandon_gc(expected_epoch).await
    }

    pub async fn abandon_incomplete_gc(&self) -> Result<prolly_s3_core::OperationId> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository.abandon_incomplete_gc().await
    }

    pub async fn advance_gc(&self, cursor: &GcCursor, max_steps: usize) -> Result<GcPage> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository.advance_gc(cursor, max_steps).await
    }

    pub async fn sweep_gc(&self, cursor: &GcCursor, max_candidates: usize) -> Result<GcPage> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository.sweep_gc(cursor, max_candidates).await
    }

    pub async fn start_repair_from(
        &self,
        source: &Client,
        source_snapshot: CommitId,
        expected_head: CommitId,
        message: impl Into<String>,
    ) -> Result<RepairCursor> {
        self.ensure_provider_qualified()?;
        source.ensure_provider_qualified()?;
        let destination_branch = self.attached_branch()?;
        self.repository
            .start_repair_from(
                source.repository.as_ref(),
                &source.branch,
                source_snapshot,
                destination_branch,
                expected_head,
                message,
            )
            .await
    }

    pub async fn advance_repair_from(
        &self,
        source: &Client,
        cursor: &RepairCursor,
        max_steps: usize,
    ) -> Result<RepairPage> {
        self.ensure_provider_qualified()?;
        source.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository
            .advance_repair_from(source.repository.as_ref(), cursor, max_steps)
            .await
    }

    pub async fn start_history_transfer_from(
        &self,
        source: &Client,
        source_snapshot: CommitId,
        expected_head: CommitId,
    ) -> Result<HistoryTransferCursor> {
        self.ensure_provider_qualified()?;
        source.ensure_provider_qualified()?;
        let destination_branch = self.attached_branch()?;
        self.repository
            .start_history_transfer_from(
                source.repository.as_ref(),
                &source.branch,
                source_snapshot,
                destination_branch,
                expected_head,
            )
            .await
    }

    pub async fn advance_history_transfer_from(
        &self,
        source: &Client,
        cursor: &HistoryTransferCursor,
        max_steps: usize,
    ) -> Result<HistoryTransferPage> {
        self.ensure_provider_qualified()?;
        source.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository
            .advance_history_transfer_from(source.repository.as_ref(), cursor, max_steps)
            .await
    }

    pub async fn publish_history_transfer(
        &self,
        cursor: &HistoryTransferCursor,
        reason: impl AsRef<str>,
    ) -> Result<RefMoveReceipt> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository
            .publish_history_transfer(cursor, reason.as_ref())
            .await
    }

    pub async fn history_transfer_mapping(
        &self,
        cursor: &HistoryTransferCursor,
        source: CommitId,
    ) -> Result<Option<HistoryTransferMapping>> {
        self.ensure_provider_qualified()?;
        self.repository
            .history_transfer_mapping(cursor, source)
            .await
    }

    pub async fn start_fetch_from(
        &self,
        source: &Client,
        source_snapshot: CommitId,
        expected_head: CommitId,
    ) -> Result<RepairCursor> {
        self.start_repair_from(source, source_snapshot, expected_head, "fetch snapshot")
            .await
    }

    pub async fn start_clone_from(
        &self,
        source: &Client,
        source_snapshot: CommitId,
        expected_head: CommitId,
    ) -> Result<RepairCursor> {
        self.start_repair_from(source, source_snapshot, expected_head, "clone snapshot")
            .await
    }

    pub async fn start_push_to(
        &self,
        destination: &Client,
        source_snapshot: CommitId,
        destination_expected_head: CommitId,
    ) -> Result<RepairCursor> {
        destination
            .start_repair_from(
                self,
                source_snapshot,
                destination_expected_head,
                "push snapshot",
            )
            .await
    }

    /// Start a fetch that preserves the complete source commit DAG.
    pub async fn start_history_fetch_from(
        &self,
        source: &Client,
        source_snapshot: CommitId,
        expected_head: CommitId,
    ) -> Result<HistoryTransferCursor> {
        self.start_history_transfer_from(source, source_snapshot, expected_head)
            .await
    }

    /// Start a clone that preserves the complete source commit DAG.
    pub async fn start_history_clone_from(
        &self,
        source: &Client,
        source_snapshot: CommitId,
        expected_head: CommitId,
    ) -> Result<HistoryTransferCursor> {
        self.start_history_transfer_from(source, source_snapshot, expected_head)
            .await
    }

    /// Start a push that preserves the complete source commit DAG.
    pub async fn start_history_push_to(
        &self,
        destination: &Client,
        source_snapshot: CommitId,
        destination_expected_head: CommitId,
    ) -> Result<HistoryTransferCursor> {
        destination
            .start_history_transfer_from(self, source_snapshot, destination_expected_head)
            .await
    }

    pub async fn start_backup_verification(
        &self,
        destination: &Client,
        source_snapshot: CommitId,
        destination_snapshot: CommitId,
    ) -> Result<BackupVerificationCursor> {
        self.ensure_provider_qualified()?;
        destination.ensure_provider_qualified()?;
        self.repository
            .start_backup_verification(
                destination.repository.as_ref(),
                &self.branch,
                source_snapshot,
                &destination.branch,
                destination_snapshot,
            )
            .await
    }

    pub async fn advance_backup_verification(
        &self,
        destination: &Client,
        cursor: &BackupVerificationCursor,
        limit: usize,
    ) -> Result<BackupVerificationPage> {
        self.ensure_provider_qualified()?;
        destination.ensure_provider_qualified()?;
        self.repository
            .advance_backup_verification(destination.repository.as_ref(), cursor, limit)
            .await
    }

    pub async fn start_restore(
        &self,
        source: CommitId,
        expected_head: CommitId,
        message: impl Into<String>,
    ) -> Result<RestoreCursor> {
        self.ensure_provider_qualified()?;
        let branch = self.attached_branch()?;
        self.repository
            .start_restore(branch, source, expected_head, message)
            .await
    }

    pub async fn advance_restore(
        &self,
        cursor: &RestoreCursor,
        max_steps: usize,
    ) -> Result<RestorePage> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository.advance_restore(cursor, max_steps).await
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
        let branch = self.attached_branch()?;
        self.repository
            .start_merge(
                branch,
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
        self.attached_branch()?;
        self.repository.advance_merge(cursor, max_steps).await
    }

    pub async fn select_merge_base(
        &self,
        cursor: &MergeCursor,
        base: CommitId,
    ) -> Result<MergeCursor> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
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
        self.attached_branch()?;
        self.repository.publish_merge(cursor).await
    }

    pub async fn cleanup_merge(
        &self,
        cursor: &MergeCursor,
        continuation: Option<&MergeCleanupCursor>,
        limit: usize,
    ) -> Result<MergeCleanupPage> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
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
        self.attached_branch()?;
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
        self.attached_branch()?;
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
        let branch = self.attached_branch()?;
        self.repository
            .put_object(branch, key.into().into_bytes(), bytes, headers, metadata)
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
        let branch = self.attached_branch()?;
        self.repository
            .put_object_with_operation(
                branch,
                key.into().into_bytes(),
                bytes,
                ObjectHeaders::default(),
                BTreeMap::new(),
                operation,
            )
            .await
    }

    /// Put objects through durable atomic batches. This is the recommended
    /// path for bulk loading because publication and checkpoint costs are
    /// amortized across each batch.
    pub async fn put_objects(
        &self,
        objects: Vec<PutObjectInput>,
        batch_size: usize,
    ) -> Result<Vec<CommitReceipt>> {
        self.put_object_stream(
            stream::iter(objects.into_iter().map(Ok)),
            BulkWriteOptions {
                batch_size,
                checkpoint_every: batch_size.min(1_000),
                ..BulkWriteOptions::default()
            },
        )
        .await
    }

    /// Consume a fallible object stream using bounded memory and concurrent
    /// payload uploads. Each checkpoint window is fully resolved before its
    /// canonical mutation metadata is saved, so cancellation or a staging
    /// failure leaves the last completed window resumable by batch ID.
    pub async fn put_object_stream<S>(
        &self,
        objects: S,
        options: BulkWriteOptions,
    ) -> Result<Vec<CommitReceipt>>
    where
        S: Stream<Item = Result<PutObjectInput>> + Send,
    {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        let max = self
            .repository
            .format()
            .canonical_limits
            .max_mutations_per_commit as usize;
        if options.batch_size == 0 || options.batch_size > max {
            return Err(invalid(
                "bulk-write batch size exceeds the canonical mutation limit",
            ));
        }
        if options.concurrency == 0 || options.concurrency > 1_024 {
            return Err(invalid("bulk-write concurrency must be 1..=1024"));
        }
        if options.checkpoint_every == 0 || options.checkpoint_every > options.batch_size {
            return Err(invalid(
                "bulk-write checkpoint interval must be within the batch size",
            ));
        }

        futures_util::pin_mut!(objects);
        let mut receipts = Vec::new();
        let mut next = objects.next().await.transpose()?;
        while next.is_some() {
            let mut session = self
                .begin_commit()
                .message("bulk object write")
                .checkpoint_every(options.checkpoint_every)
                .start()
                .await?;
            let mut batch_items = 0_usize;
            while batch_items < options.batch_size && next.is_some() {
                let window_limit = options
                    .checkpoint_every
                    .min(options.batch_size - batch_items);
                let mut window = Vec::with_capacity(window_limit);
                while window.len() < window_limit {
                    let Some(object) = next.take() else {
                        break;
                    };
                    window.push(object);
                    next = match objects.next().await {
                        Some(Ok(object)) => Some(object),
                        Some(Err(mut error)) => {
                            session.checkpoint().await?;
                            error.message = format!(
                                "bulk input failed after {} staged objects: {}",
                                session.staged_objects(),
                                error.message
                            );
                            error.operation_id = Some(session.id().to_string());
                            return Err(error);
                        }
                        None => None,
                    };
                }

                batch_items = batch_items.saturating_add(window.len());
                let staged = self
                    .repository
                    .stage_commit_session_put_batch(
                        &session.manifest,
                        window
                            .into_iter()
                            .map(|object| {
                                (
                                    object.key.into_bytes(),
                                    object.bytes,
                                    object.headers,
                                    object.user_metadata,
                                )
                            })
                            .collect(),
                        options.concurrency,
                    )
                    .await;
                let staged = match staged {
                    Ok(staged) => staged,
                    Err(mut error) => {
                        session.checkpoint().await?;
                        error.message = format!("bulk payload staging failed: {}", error.message);
                        error.operation_id = Some(session.id().to_string());
                        return Err(error);
                    }
                };
                for mutation in staged {
                    session.insert_staged(mutation);
                }
                session.checkpoint().await?;
            }
            receipts.push(session.publish().await?);
        }
        Ok(receipts)
    }

    pub async fn get_object(&self, key: impl AsRef<str>) -> Result<Option<ObjectData>> {
        self.ensure_provider_qualified()?;
        match self.checked_out.target() {
            Some(snapshot) => {
                self.repository
                    .get_object_at(&self.branch, snapshot, key.as_ref().as_bytes())
                    .await
            }
            None => {
                self.repository
                    .get_object(&self.branch, key.as_ref().as_bytes())
                    .await
            }
        }
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

    pub async fn head_object(
        &self,
        key: impl AsRef<str>,
    ) -> Result<Option<(CommitId, ObjectSummary)>> {
        self.ensure_provider_qualified()?;
        match self.checked_out.target() {
            Some(snapshot) => Ok(self
                .repository
                .head_object_at(&self.branch, snapshot, key.as_ref().as_bytes())
                .await?
                .map(|summary| (snapshot, summary))),
            None => {
                self.repository
                    .head_object(&self.branch, key.as_ref().as_bytes())
                    .await
            }
        }
    }

    pub async fn get_object_range(
        &self,
        snapshot: CommitId,
        key: impl AsRef<str>,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<Option<ObjectRangeData>> {
        self.ensure_provider_qualified()?;
        self.repository
            .get_object_range(&self.branch, snapshot, key.as_ref().as_bytes(), range)
            .await
    }

    pub async fn copy_object(
        &self,
        source_snapshot: CommitId,
        source_key: impl AsRef<str>,
        destination_key: impl Into<String>,
    ) -> Result<CommitReceipt> {
        self.ensure_provider_qualified()?;
        let branch = self.attached_branch()?;
        self.repository
            .copy_object(
                branch,
                source_snapshot,
                source_key.as_ref().as_bytes(),
                destination_key.into().into_bytes(),
            )
            .await
    }

    pub async fn delete_objects<I, K>(&self, keys: I) -> Result<CommitReceipt>
    where
        I: IntoIterator<Item = K>,
        K: Into<String>,
    {
        self.ensure_provider_qualified()?;
        let branch = self.attached_branch()?;
        let keys = keys
            .into_iter()
            .map(|key| key.into().into_bytes())
            .collect();
        self.repository.delete_objects(branch, keys).await
    }

    pub async fn delete_object(&self, key: impl Into<String>) -> Result<CommitReceipt> {
        self.ensure_provider_qualified()?;
        let branch = self.attached_branch()?;
        self.repository
            .delete_object(branch, key.into().into_bytes())
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
        self.attached_branch()?;
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
        match self.checked_out.target() {
            Some(snapshot) => {
                let (objects, truncated) = self
                    .repository
                    .list_objects_at(
                        &self.branch,
                        snapshot,
                        prefix.as_ref().as_bytes(),
                        after.map(str::as_bytes),
                        limit,
                    )
                    .await?;
                Ok((snapshot, objects, truncated))
            }
            None => {
                self.repository
                    .list_objects(
                        &self.branch,
                        prefix.as_ref().as_bytes(),
                        after.map(str::as_bytes),
                        limit,
                    )
                    .await
            }
        }
    }

    /// List one cursor page from the selected revision. Attached checkouts pin
    /// the current branch head on page one; detached commit and tag checkouts
    /// pin their immutable target. Continuations cannot cross those revisions.
    pub async fn list_objects_page(
        &self,
        prefix: impl AsRef<str>,
        continuation: Option<&str>,
        limit: usize,
    ) -> Result<ListObjectsPage> {
        self.ensure_provider_qualified()?;
        match self.checked_out.target() {
            Some(snapshot) => {
                self.repository
                    .list_objects_page_at(
                        &self.branch,
                        snapshot,
                        prefix.as_ref().as_bytes(),
                        continuation,
                        limit,
                    )
                    .await
            }
            None => {
                self.repository
                    .list_objects_page(
                        &self.branch,
                        prefix.as_ref().as_bytes(),
                        continuation,
                        limit,
                    )
                    .await
            }
        }
    }

    /// Lazily stream a snapshot-bound prefix listing. An attached checkout
    /// captures the branch head on its first page; a detached checkout starts
    /// from its immutable commit. Later pages seek through the opaque cursor.
    pub fn stream_objects(
        &self,
        prefix: impl Into<String>,
        page_size: usize,
    ) -> impl Stream<Item = Result<ObjectSummary>> + Send + 'static {
        struct State {
            client: Client,
            prefix: String,
            page_size: usize,
            continuation: Option<String>,
            buffered: VecDeque<ObjectSummary>,
            complete: bool,
        }
        stream::try_unfold(
            State {
                client: self.clone(),
                prefix: prefix.into(),
                page_size,
                continuation: None,
                buffered: VecDeque::new(),
                complete: false,
            },
            |mut state| async move {
                loop {
                    if let Some(object) = state.buffered.pop_front() {
                        return Ok(Some((object, state)));
                    }
                    if state.complete {
                        return Ok(None);
                    }
                    let page = state
                        .client
                        .list_objects_page(
                            &state.prefix,
                            state.continuation.as_deref(),
                            state.page_size,
                        )
                        .await?;
                    state.continuation = page.continuation;
                    state.complete = state.continuation.is_none();
                    state.buffered = page.objects.into();
                    if state.buffered.is_empty() && state.complete {
                        return Ok(None);
                    }
                }
            },
        )
    }

    pub async fn list_objects_delimited(
        &self,
        prefix: impl AsRef<str>,
        delimiter: impl AsRef<str>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<DelimitedObjectPage> {
        self.ensure_provider_qualified()?;
        match self.checked_out.target() {
            Some(snapshot) => {
                self.repository
                    .list_objects_delimited_at(
                        &self.branch,
                        snapshot,
                        prefix.as_ref().as_bytes(),
                        delimiter.as_ref().as_bytes(),
                        after.map(str::as_bytes),
                        limit,
                    )
                    .await
            }
            None => {
                self.repository
                    .list_objects_delimited(
                        &self.branch,
                        prefix.as_ref().as_bytes(),
                        delimiter.as_ref().as_bytes(),
                        after.map(str::as_bytes),
                        limit,
                    )
                    .await
            }
        }
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
        match self.checked_out.target() {
            Some(snapshot) => Ok((
                snapshot,
                self.repository
                    .list_object_versions_at(&self.branch, snapshot, key.as_ref().as_bytes(), limit)
                    .await?,
            )),
            None => {
                self.repository
                    .list_object_versions(&self.branch, key.as_ref().as_bytes(), limit)
                    .await
            }
        }
    }

    pub async fn list_versions_prefix(
        &self,
        prefix: impl AsRef<str>,
        limit: usize,
    ) -> Result<(CommitId, Vec<VersionSummary>)> {
        self.ensure_provider_qualified()?;
        match self.checked_out.target() {
            Some(snapshot) => Ok((
                snapshot,
                self.repository
                    .list_versions_at(
                        &self.branch,
                        snapshot,
                        prefix.as_ref().as_bytes(),
                        None,
                        limit,
                    )
                    .await?
                    .0,
            )),
            None => {
                self.repository
                    .list_versions_prefix(&self.branch, prefix.as_ref().as_bytes(), limit)
                    .await
            }
        }
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
        self.attached_branch()?;
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
        self.repository
            .advance_branch_indexes(self.attached_branch()?)
            .await
    }

    pub async fn branch_index_health(&self) -> Result<BranchIndexHealth> {
        self.ensure_provider_qualified()?;
        self.repository
            .branch_index_health(self.attached_branch()?)
            .await
    }

    pub async fn start_branch_index_rebuild(&self) -> Result<JournalIndexRebuildCursor> {
        self.ensure_provider_qualified()?;
        self.repository
            .start_branch_index_rebuild(self.attached_branch()?)
            .await
    }

    pub async fn advance_branch_index_rebuild(
        &self,
        cursor: &JournalIndexRebuildCursor,
        max_events: usize,
    ) -> Result<JournalIndexRebuildStep> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
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
        self.attached_branch()?;
        self.repository
            .cleanup_branch_index_rebuild(journal, operations, limit)
            .await
    }

    pub async fn start_operation_index_rebuild(
        &self,
        journal: &JournalIndexRebuildCursor,
    ) -> Result<OperationIndexRebuildCursor> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository.start_operation_index_rebuild(journal).await
    }

    pub async fn advance_operation_index_rebuild(
        &self,
        cursor: &OperationIndexRebuildCursor,
        max_events: usize,
    ) -> Result<OperationIndexRebuildStep> {
        self.ensure_provider_qualified()?;
        self.attached_branch()?;
        self.repository
            .advance_operation_index_rebuild(cursor, max_events)
            .await
    }

    pub async fn wait_for_branch_indexes(&self, timeout: Duration) -> Result<BranchIndexHealth> {
        self.ensure_provider_qualified()?;
        let branch = self.attached_branch()?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let health = self.repository.branch_index_health(branch).await?;
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
        self.attached_branch()?;
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
        let branch = self.client.attached_branch()?.to_string();
        let expires_after_millis = u64::try_from(self.expires_after.as_millis())
            .map_err(|_| invalid("commit-session expiry exceeds u64 milliseconds"))?;
        if self.durable && self.checkpoint_every == 0 {
            return Err(invalid("checkpoint interval must be positive"));
        }
        let (manifest, checkpoint_sequence) = if self.durable {
            let checkpoint = self
                .client
                .repository
                .begin_durable_commit_session(&branch, self.message, expires_after_millis)
                .await?;
            (checkpoint.session, checkpoint.sequence)
        } else {
            (
                self.client
                    .repository
                    .begin_commit_session(&branch, self.message, expires_after_millis)
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

    fn insert_staged(&mut self, staged: StagedMutation) {
        self.staged.insert(staged.key().to_vec(), staged);
        self.dirty_mutations = self.dirty_mutations.saturating_add(1);
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
        self.insert_staged(staged);
        self.mark_staged_and_checkpoint_if_due().await?;
        Ok(())
    }

    /// Stage a streamed object as independently deduplicated 8 MiB chunks and
    /// one content-addressed manifest. At most eight chunks are buffered.
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
        const CHUNK_BYTES: usize = 8 * 1024 * 1024;
        const CHUNK_CONCURRENCY: usize = 8;
        let max_object_bytes = self.client.repository.max_object_bytes();
        let mut size = 0_u64;
        let mut sha256 = Sha256::new();
        let mut md5 = Md5::new();
        let mut current = Vec::with_capacity(CHUNK_BYTES);
        let mut window = Vec::with_capacity(CHUNK_CONCURRENCY);
        let mut chunks = Vec::new();
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
            sha256.update(&next);
            md5.update(&next);
            current.extend_from_slice(&next);
            while current.len() >= CHUNK_BYTES {
                let remainder = current.split_off(CHUNK_BYTES);
                window.push(std::mem::replace(&mut current, remainder));
                if window.len() == CHUNK_CONCURRENCY {
                    chunks.extend(
                        self.upload_chunk_window(std::mem::take(&mut window))
                            .await?,
                    );
                }
            }
        }
        let checksum_sha256 = sha256.finalize().into();
        let checksum_md5 = md5.finalize().into();
        let staged = if chunks.is_empty() && window.is_empty() {
            self.client
                .repository
                .stage_commit_session_put(&self.manifest, key, current, headers, metadata)
                .await?
        } else {
            if !current.is_empty() {
                window.push(current);
            }
            if !window.is_empty() {
                chunks.extend(self.upload_chunk_window(window).await?);
            }
            self.client
                .repository
                .stage_commit_session_chunk_manifest(
                    &self.manifest,
                    key,
                    chunks,
                    size,
                    checksum_sha256,
                    checksum_md5,
                    headers,
                    metadata,
                )
                .await?
        };
        self.insert_staged(staged);
        self.mark_staged_and_checkpoint_if_due().await?;
        Ok(())
    }

    async fn upload_chunk_window(
        &self,
        window: Vec<Vec<u8>>,
    ) -> Result<Vec<prolly_s3_core::PayloadChunk>> {
        stream::iter(window.into_iter().map(|bytes| async move {
            self.client
                .repository
                .upload_commit_session_chunk(&self.manifest, bytes)
                .await
        }))
        .buffered(8)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect()
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
            branch: branch.clone(),
            checked_out: CheckedOutRef::Branch(branch),
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
