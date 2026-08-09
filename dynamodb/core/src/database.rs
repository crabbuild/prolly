use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::{lock::Mutex, stream, StreamExt, TryStreamExt};
use prolly::{
    AsyncManifestStore, AsyncManifestStoreScan, AsyncPreparedIndexedUpdate, AsyncProlly,
    AsyncProllyTransaction, AsyncStore, AsyncTransactionalStore, AsyncVersionedMapsTransaction,
    BlobRef, CollectionIndexPolicy, IndexedSnapshotId, IndexedSnapshotManifest, MapVersion,
    MapVersionCursor, MapVersionId, MapVersionPage, Mutation, MutationBudget, NamedRootUpdate,
    RootManifest, SnapshotExportLimits, StructuralDiffCursor, StructuralDiffPage,
    TransactionUpdate, Tree, ValueRef, VersionedMapUpdate, DEFAULT_VERSIONED_MAP_RETRIES,
};
use serde::{Deserialize, Serialize};

use crate::blob::BlobLayer;
use crate::index::{index_registry, prepare_index_source_record};
use crate::{
    decode_item, encode_item, encode_key_schema, encode_partition_prefix, encode_primary_key,
    item_size, AttributeValue, BlobStorage, Condition, DatabaseFormatRecord, Error, Item,
    KeyCondition, LargeValueConfig, Result, SecondaryIndexDefinition, SecondaryIndexDescription,
    SecondaryIndexId, SecondaryIndexStatus, SortKeyCondition, StoragePublicationMode, TableArchive,
    TableArchiveBlob, TableArchiveLimits, TableDescription, TableId, TableStatus, UpdatePlan,
};

const CATALOG_MAP_ID: &[u8] = b"dynamodb/catalog/v1";
const TABLE_DESCRIPTOR_MAP_ID: &[u8] = b"dynamodb/table-descriptors/v1";
const FORMAT_MAP_ID: &[u8] = b"dynamodb/format/v1";
const FORMAT_RECORD_KEY: &[u8] = b"database";
const TABLE_MAP_PREFIX: &[u8] = b"dynamodb/table/v1/";
const TABLE_INDEXED_SOURCE_PREFIX: &[u8] = b"dynamodb/table-indexed/v1/";
const TABLE_SNAPSHOT_CATALOG_PREFIX: &[u8] = b"\0dynamodb/table-snapshot-catalog/v1/";
const TABLE_BLOB_REGISTRY_PREFIX: &[u8] = b"\0dynamodb/table-blob-registry/v1/";
const TABLE_BLOB_REGISTRY_VALUE_MAGIC: &[u8] = b"DDBR\x01";
const TABLE_SNAPSHOT_LOCATOR_MAGIC: &[u8] = b"DDBL\x02";
const TABLE_SNAPSHOT_RECORD_KEY: &[u8] = b"snapshot";
const TABLE_SNAPSHOT_MANIFEST_FORMAT: u32 = 1;
const MAX_TABLE_SNAPSHOT_MANIFEST_BYTES: usize = 1024 * 1024;
const MAX_TABLE_SNAPSHOT_LOCATOR_BYTES: usize = 64 * 1024;
const TABLE_SCHEMA_RECORD_KEY: &[u8] = b"\xffdynamodb/schema/v1";
const TABLE_SCHEMA_RECORD_MAGIC: &[u8] = b"DDBS\x01";
const COMMIT_CATALOG_ROOT_NAME: &[u8] = b"\0dynamodb/commit-catalog/v1";
const TABLE_COMMIT_LOG_PREFIX: &[u8] = b"\0dynamodb/table-commit-log/v1/";
const COMMIT_SEQUENCE_KEY: &[u8] = &[0];
const IDEMPOTENCY_MAP_ID: &[u8] = b"dynamodb/idempotency/v1";
const MAINTENANCE_AUDIT_MAP_ID: &[u8] = b"dynamodb/maintenance-audit/v1";
const IMPORT_AUDIT_MAP_ID: &[u8] = b"dynamodb/import-audit/v1";
const INDEX_RECONFIG_AUDIT_MAP_ID: &[u8] = b"dynamodb/index-reconfiguration-audit/v1";
const MAINTENANCE_CONTROL_MAP_ID: &[u8] = b"dynamodb/maintenance-control/v1";
const MAINTENANCE_LEASE_KEY: &[u8] = b"global";
const MAINTENANCE_LEASE_AUDIT_MAP_ID: &[u8] = b"dynamodb/maintenance-lease-audit/v1";
const GC_EXECUTION_MAP_ID: &[u8] = b"dynamodb/gc-execution/v1";
const WORKER_LEASE_MAP_ID: &[u8] = b"dynamodb/worker-leases/v1";
const WORKER_FENCE_MAP_ID: &[u8] = b"dynamodb/worker-fences/v1";
const WORKER_CHECKPOINT_MAP_ID: &[u8] = b"dynamodb/worker-checkpoints/v1";
const WORKER_LEASE_AUDIT_MAP_ID: &[u8] = b"dynamodb/worker-lease-audit/v1";
const GC_ACTIVE_KEY: &[u8] = b"\0active";
const IDEMPOTENCY_WINDOW_MILLIS: u64 = 10 * 60 * 1000;
const MAX_READ_PAGE_BYTES: usize = 1024 * 1024;
const READ_CHUNK_ITEMS: usize = 128;
const IMPORT_INDEX_BATCH_ITEMS: usize = 1_024;
const IMPORT_INDEX_BATCH_BYTES: usize = 16 * 1024 * 1024;
/// DynamoDB TTL eligibility excludes timestamps older than five 365-day years.
pub const TTL_MAX_PAST_SECONDS: u64 = 5 * 365 * 24 * 60 * 60;

pub const MAX_BATCH_GET_ITEMS: usize = 100;
pub const MAX_BATCH_GET_PARTITION_BYTES: usize = 1024 * 1024;
pub const MAX_BATCH_GET_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BATCH_WRITE_ITEMS: usize = 25;
pub const MAX_TRANSACTION_ITEMS: usize = 100;
pub const MAX_TRANSACTION_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_COMMIT_PAGE_ITEMS: usize = 1_000;
/// Maximum number of changes returned by one resumable diff page.
pub const MAX_DIFF_PAGE_ITEMS: usize = 1_000;
/// Safety ceiling for the legacy convenience method that collects a diff.
pub const MAX_COLLECTED_DIFF_ITEMS: usize = 10_000;
/// Maximum number of immutable versions returned by one discovery page.
pub const MAX_VERSION_PAGE_ITEMS: usize = 1_000;
/// Safety ceiling for the convenience method that collects and sorts versions.
pub const MAX_COLLECTED_VERSIONS: usize = 10_000;
/// Default retries after the first optimistic logical attempt.
pub const DEFAULT_LOGICAL_RETRY_LIMIT: usize = DEFAULT_VERSIONED_MAP_RETRIES - 1;
/// Hard ceiling for retries after the first optimistic logical attempt.
pub const MAX_LOGICAL_RETRY_LIMIT: usize = 63;
/// Maximum explicit protected versions accepted by one retention policy.
pub const MAX_RETENTION_PROTECTED_VERSIONS: usize = 10_000;
/// Maximum version roots removed by one atomic audited retention execution.
///
/// This reserves provider transaction actions for catalog, head, commit-log,
/// managed-map fences, and the durable audit record.
pub const MAX_RETENTION_REMOVALS: usize = 80;
pub const MIN_MAINTENANCE_LEASE_MILLIS: u64 = 60_000;
pub const MAX_MAINTENANCE_LEASE_MILLIS: u64 = 24 * 60 * 60 * 1_000;
pub const MAX_GC_PLAN_DELETES: usize = 2_000;
pub const MIN_WORKER_LEASE_MILLIS: u64 = 10_000;
pub const MAX_WORKER_LEASE_MILLIS: u64 = 5 * 60 * 1_000;

/// Explicit background worker family. Opening a client never acquires one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WorkerKind {
    Stream,
    Ttl,
}

/// Canonical identity for one worker configuration/subscription.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WorkerJobId(pub [u8; 32]);

impl WorkerJobId {
    pub fn for_configuration(kind: WorkerKind, canonical_configuration: &[u8]) -> Self {
        Self::for_digest(kind, Self::configuration_digest(canonical_configuration))
    }

    pub fn configuration_digest(canonical_configuration: &[u8]) -> [u8; 32] {
        let cid = prolly::Cid::from_bytes(canonical_configuration);
        let mut digest = [0; 32];
        digest.copy_from_slice(cid.as_bytes());
        digest
    }

    fn for_digest(kind: WorkerKind, configuration_digest: [u8; 32]) -> Self {
        let mut bytes = b"DDB-WorkerJob-v1".to_vec();
        bytes.push(match kind {
            WorkerKind::Stream => 1,
            WorkerKind::Ttl => 2,
        });
        bytes.extend_from_slice(&configuration_digest);
        let cid = prolly::Cid::from_bytes(&bytes);
        let mut id = [0; 32];
        id.copy_from_slice(cid.as_bytes());
        Self(id)
    }
}

impl std::fmt::Display for WorkerJobId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Renewable single-owner lease with an ABA-safe monotonically increasing
/// fencing token. Expiry allows takeover because worker effects are required
/// to be idempotent or conditionally race-safe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLease {
    pub job_id: WorkerJobId,
    pub kind: WorkerKind,
    pub configuration_digest: [u8; 32],
    pub owner_id: String,
    pub fence: u64,
    pub acquired_at_millis: u64,
    pub renewed_at_millis: u64,
    pub expires_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLeaseRelease {
    pub lease: WorkerLease,
    pub released_at_millis: u64,
    pub replayed: bool,
}

/// Durable worker progress. Counters are cumulative so checkpoint updates are
/// monotonic even when a TTL scan begins another cycle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerProgress {
    Stream {
        table_id: TableId,
        delivered_through_sequence: u64,
    },
    Ttl {
        table_id: TableId,
        cycle: u64,
        last_evaluated_key: Option<Item>,
        evaluated_total: u64,
        deleted_total: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCheckpoint {
    pub job_id: WorkerJobId,
    pub kind: WorkerKind,
    pub configuration_digest: [u8; 32],
    pub revision: u64,
    pub fence: u64,
    pub progress: WorkerProgress,
    pub updated_at_millis: u64,
}

/// Collision-resistant identity for one global fail-closed maintenance fence.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MaintenanceLeaseId(pub [u8; 32]);

impl std::fmt::Display for MaintenanceLeaseId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Durable global writer fence used by destructive physical maintenance.
///
/// Expiry permits an authorized break operation; it never automatically
/// admits writers, because doing so could race a paused sweeper.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceLease {
    pub id: MaintenanceLeaseId,
    pub context: MaintenanceContext,
    pub acquired_at_millis: u64,
    pub expires_at_millis: u64,
}

/// Durable evidence that a maintenance fence was explicitly released or
/// force-broken after expiry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceLeaseRelease {
    pub lease: MaintenanceLease,
    pub context: MaintenanceContext,
    pub released_at_millis: u64,
    pub forced_after_expiry: bool,
    pub replayed: bool,
}

/// Durable state of one exact physical GC execution page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GcExecutionState {
    InProgress,
    Complete,
}

/// Provider-neutral audit/progress record that keeps the maintenance fence
/// pinned across partial physical deletion and process restart.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcExecutionRecord {
    pub plan_id: [u8; 32],
    pub lease_id: MaintenanceLeaseId,
    pub roots_digest: [u8; 32],
    pub node_deletes: usize,
    pub blob_deletes: usize,
    pub context: MaintenanceContext,
    pub state: GcExecutionState,
    pub started_at_millis: u64,
    pub completed_at_millis: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcExecutionResult {
    pub record: GcExecutionRecord,
    pub replayed: bool,
}

/// Content identity for one exact, dry-run retention plan.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RetentionPlanId(pub [u8; 32]);

impl std::fmt::Display for RetentionPlanId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Union-style version retention policy. The current head is always retained.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub keep_last: usize,
    pub keep_since_millis: Option<u64>,
    pub protected_versions: BTreeSet<MapVersionId>,
}

impl RetentionPolicy {
    pub fn keep_last(count: usize) -> Self {
        Self {
            keep_last: count,
            keep_since_millis: None,
            protected_versions: BTreeSet::new(),
        }
    }

    pub fn keep_since_millis(mut self, cutoff: u64) -> Self {
        self.keep_since_millis = Some(cutoff);
        self
    }

    pub fn protect(mut self, version: MapVersionId) -> Self {
        self.protected_versions.insert(version);
        self
    }
}

/// Exact bounded deletion set computed without mutating storage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPlan {
    pub id: RetentionPlanId,
    pub table_name: String,
    pub table_id: TableId,
    pub expected_head: MapVersionId,
    pub expected_commit_sequence: u64,
    pub policy: RetentionPolicy,
    pub remove: Vec<MapVersionId>,
    pub examined_versions: u64,
    pub more_removable: bool,
    pub planned_at_millis: u64,
}

/// Required operator attribution for a destructive maintenance call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceContext {
    pub actor: String,
    pub reason: String,
    pub change_ticket: Option<String>,
}

impl MaintenanceContext {
    pub fn new(actor: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            actor: actor.into(),
            reason: reason.into(),
            change_ticket: None,
        }
    }

    pub fn change_ticket(mut self, ticket: impl Into<String>) -> Self {
        self.change_ticket = Some(ticket.into());
        self
    }

    fn validate(&self) -> Result<()> {
        if self.actor.is_empty() || self.actor.len() > 256 {
            return Err(Error::Validation(
                "maintenance actor must contain 1..=256 bytes".into(),
            ));
        }
        if self.reason.is_empty() || self.reason.len() > 1_024 {
            return Err(Error::Validation(
                "maintenance reason must contain 1..=1024 bytes".into(),
            ));
        }
        if self
            .change_ticket
            .as_ref()
            .is_some_and(|ticket| ticket.is_empty() || ticket.len() > 256)
        {
            return Err(Error::Validation(
                "maintenance change ticket must contain 1..=256 bytes when supplied".into(),
            ));
        }
        Ok(())
    }
}

/// Durable result of one atomic retention execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionResult {
    pub plan_id: RetentionPlanId,
    pub removed: Vec<MapVersionId>,
    pub completed_at_millis: u64,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionAuditRecord {
    pub plan: RetentionPlan,
    pub context: MaintenanceContext,
    pub completed_at_millis: u64,
}

/// Content identity for one exact, dry-run table import plan.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ImportPlanId(pub [u8; 32]);

impl std::fmt::Display for ImportPlanId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Immutable dry-run plan binding an archive to a fresh target incarnation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportPlan {
    pub id: ImportPlanId,
    pub target_table_name: String,
    pub target_table_id: TableId,
    pub archive_digest: [u8; 32],
    pub source_table_name: String,
    pub source_table_id: TableId,
    pub source_version: MapVersionId,
    pub required_database_format: Vec<u8>,
    pub planned_at_millis: u64,
}

/// Durable outcome of one atomically published table import.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportResult {
    pub plan_id: ImportPlanId,
    pub description: TableDescription,
    pub version: MapVersionId,
    pub commit_id: CommitId,
    pub completed_at_millis: u64,
    pub replayed: bool,
}

/// Durable operator-attributed evidence for a completed import.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportAuditRecord {
    pub plan: ImportPlan,
    pub context: MaintenanceContext,
    pub description: TableDescription,
    pub commit_id: CommitId,
    pub completed_at_millis: u64,
}

/// Content identity for one exact online secondary-index reconfiguration.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexReconfigurationPlanId(pub [u8; 32]);

impl std::fmt::Display for IndexReconfigurationPlanId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Immutable dry-run plan for a clean shadow rebuild and atomic activation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexReconfigurationPlan {
    pub id: IndexReconfigurationPlanId,
    pub table_name: String,
    pub table_id: TableId,
    pub expected_head: MapVersionId,
    pub expected_commit_sequence: u64,
    pub before: TableDescription,
    pub after: TableDescription,
    pub planned_at_millis: u64,
}

/// Durable result of one activated secondary-index generation set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexReconfigurationResult {
    pub plan_id: IndexReconfigurationPlanId,
    pub description: TableDescription,
    pub version: MapVersionId,
    pub indexed_source_version: MapVersionId,
    pub indexed_snapshot_id: IndexedSnapshotId,
    pub commit_id: CommitId,
    pub completed_at_millis: u64,
    pub replayed: bool,
}

/// Operator-attributed evidence for an index activation transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexReconfigurationAuditRecord {
    pub plan: IndexReconfigurationPlan,
    pub context: MaintenanceContext,
    pub result: IndexReconfigurationResult,
}

/// Immutable per-base-version schema/index locator stored in the table's
/// current-only snapshot catalog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct TableSnapshotManifestRecord {
    format_version: u32,
    table_id: TableId,
    base_version: MapVersionId,
    description: TableDescription,
    indexed: IndexedSnapshotManifest,
}

#[derive(Clone, Debug, PartialEq)]
struct TableSnapshotLocator {
    manifest_tree: Tree,
    indexed_snapshot_id: IndexedSnapshotId,
}

/// Database-owned trees whose leaf values are covered by an exact durable blob
/// registry. GC must still retain their nodes, but need not rediscover blob
/// references by rescanning every historical leaf value.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotCatalogProtection {
    pub protected_trees: Vec<Tree>,
    pub covered_value_trees: Vec<MapVersionId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadPage {
    pub items: Vec<Item>,
    pub last_evaluated_key: Option<Item>,
    pub version_id: MapVersionId,
}

/// One race-safe TTL deletion candidate planned from a pinned scan page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TtlCandidate {
    pub key: Item,
    pub observed_expiration: crate::DynamoNumber,
}

/// Bounded TTL scan output. `evaluated` counts every scanned item, including
/// entries ignored because their TTL value is missing or ineligible.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TtlCandidatePage {
    pub candidates: Vec<TtlCandidate>,
    pub evaluated: usize,
    pub last_evaluated_key: Option<Item>,
    pub version_id: MapVersionId,
}

/// One bounded secondary-index page pinned to an exact base/index snapshot pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexReadPage {
    pub items: Vec<Item>,
    pub last_evaluated_key: Option<Item>,
    pub base_version_id: MapVersionId,
    pub indexed_source_version_id: MapVersionId,
    pub indexed_snapshot_id: IndexedSnapshotId,
}

/// Borrowed bounded query options for one exact secondary-index snapshot.
#[derive(Clone, Copy, Debug)]
pub struct IndexQueryRequest<'a> {
    pub base_version: Option<&'a MapVersionId>,
    pub condition: &'a KeyCondition,
    pub exclusive_start_key: Option<&'a Item>,
    pub limit: usize,
    pub scan_forward: bool,
}

impl<'a> IndexQueryRequest<'a> {
    pub fn new(condition: &'a KeyCondition, limit: usize) -> Self {
        Self {
            base_version: None,
            condition,
            exclusive_start_key: None,
            limit,
            scan_forward: true,
        }
    }

    pub fn at(mut self, version: Option<&'a MapVersionId>) -> Self {
        self.base_version = version;
        self
    }

    pub fn after(mut self, key: Option<&'a Item>) -> Self {
        self.exclusive_start_key = key;
        self
    }

    pub fn forward(mut self, forward: bool) -> Self {
        self.scan_forward = forward;
        self
    }
}

/// Result of a point read pinned to one immutable table version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ItemRead {
    pub item: Option<Item>,
    pub version_id: MapVersionId,
}

/// One table's validated request within a multi-table batch read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchGetTableRequest {
    pub keys: Vec<Item>,
    pub projection: Option<crate::Projection>,
    pub version: Option<MapVersionId>,
}

/// One table's response from a batch read pinned to one immutable version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchGetTableResult {
    pub items: Vec<Item>,
    pub unprocessed_keys: Vec<Item>,
    pub version_id: MapVersionId,
}

/// Deterministic multi-table batch result and its logical response size.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchGetResult {
    pub tables: BTreeMap<String, BatchGetTableResult>,
    pub response_bytes: usize,
}

/// One independently atomic operation in a non-atomic BatchWriteItem request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchWriteAction {
    Put(Item),
    Delete(Item),
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatchWriteTransition {
    pub table_name: String,
    pub table_id: TableId,
    pub action_index: usize,
    pub commit_id: CommitId,
    pub update: VersionedMapUpdate,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatchWriteResult {
    pub transitions: Vec<BatchWriteTransition>,
}

/// One ordered, strongly consistent item read in `TransactGetItems`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactGetRequest {
    pub table_name: String,
    pub key: Item,
    pub projection: Option<crate::Projection>,
}

/// One response slot corresponding exactly to its request position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactGetResponse {
    pub item: Option<Item>,
}

/// Atomic read result and every table version validated at commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactGetResult {
    pub responses: Vec<TransactGetResponse>,
    pub table_versions: BTreeMap<String, MapVersionId>,
    pub response_bytes: usize,
}

/// One write/check action in an atomic `TransactWriteItems` request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactWriteAction {
    Put {
        table_name: String,
        item: Item,
        condition: Option<Condition>,
        return_failure_old: bool,
    },
    Delete {
        table_name: String,
        key: Item,
        condition: Option<Condition>,
        return_failure_old: bool,
    },
    Update {
        table_name: String,
        key: Item,
        condition: Option<Condition>,
        plan: UpdatePlan,
        return_failure_old: bool,
    },
    ConditionCheck {
        table_name: String,
        key: Item,
        condition: Condition,
        return_failure_old: bool,
    },
}

impl TransactWriteAction {
    pub fn table_name(&self) -> &str {
        match self {
            Self::Put { table_name, .. }
            | Self::Delete { table_name, .. }
            | Self::Update { table_name, .. }
            | Self::ConditionCheck { table_name, .. } => table_name,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionCancellationCode {
    ConditionalCheckFailed,
    TransactionConflict,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionCancellationReason {
    pub code: Option<TransactionCancellationCode>,
    pub message: Option<String>,
    pub item: Option<Item>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionTableTransition {
    pub table_name: String,
    pub table_id: TableId,
    pub before: Option<MapVersionId>,
    pub after: Option<MapVersionId>,
    pub applied: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactWriteResult {
    pub commit_id: CommitId,
    pub transitions: Vec<TransactionTableTransition>,
    pub table_versions: BTreeMap<String, MapVersionId>,
}

/// Durable identity for one accepted write event, independent of state hashes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CommitId(pub [u8; 32]);

impl std::fmt::Display for CommitId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IdempotencyRecord {
    fingerprint: [u8; 32],
    completed_at_millis: u64,
    result: TransactWriteResult,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCommit {
    pub commit_id: CommitId,
    pub sequence: u64,
    pub committed_at_millis: u64,
    pub transition: TransactionTableTransition,
}

/// Bounded ascending page of one table incarnation's accepted events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableCommitPage {
    pub commits: Vec<TableCommit>,
    pub last_sequence: Option<u64>,
    pub log_version: MapVersionId,
}

#[derive(Debug)]
pub enum BatchWriteExecutionError {
    Validation {
        source: Error,
    },
    Partial {
        table_name: String,
        action_index: usize,
        applied_transitions: Vec<BatchWriteTransition>,
        source: Error,
    },
}

impl std::fmt::Display for BatchWriteExecutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation { .. } => formatter.write_str("BatchWriteItem validation failed"),
            Self::Partial {
                table_name,
                action_index,
                applied_transitions,
                ..
            } => write!(
                formatter,
                "BatchWriteItem failed at table {table_name:?} action {action_index} after {} accepted transitions",
                applied_transitions.len()
            ),
        }
    }
}

impl std::error::Error for BatchWriteExecutionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Validation { source } | Self::Partial { source, .. } => source,
        })
    }
}

/// Atomic UpdateItem result, including the exact old and new logical images.
#[derive(Clone, Debug, PartialEq)]
pub struct ItemUpdate {
    pub commit_id: Option<CommitId>,
    pub table_id: Option<TableId>,
    pub update: VersionedMapUpdate,
    pub old_item: Option<Item>,
    pub new_item: Option<Item>,
}

/// Atomic PutItem/DeleteItem result with the exact pre-write image.
#[derive(Clone, Debug, PartialEq)]
pub struct ItemWrite {
    pub commit_id: Option<CommitId>,
    pub table_id: Option<TableId>,
    pub update: VersionedMapUpdate,
    pub old_item: Option<Item>,
}

/// CAS restore result with a durable accepted-event identity.
#[derive(Clone, Debug, PartialEq)]
pub struct RestoreResult {
    pub commit_id: Option<CommitId>,
    pub table_id: TableId,
    pub update: VersionedMapUpdate,
}

/// Logical table lifecycle mutation and its durable accepted-event identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableLifecycleResult {
    pub description: TableDescription,
    pub commit_id: CommitId,
    pub transition: TransactionTableTransition,
}

/// Injectable source of collision-resistant table-incarnation identifiers.
pub trait IdGenerator: Send + Sync {
    fn generate(&self) -> Result<TableId>;
}

/// Injectable wall clock used only for durable metadata.
pub trait Clock: Send + Sync {
    fn now_millis(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct SystemIdGenerator;

impl IdGenerator for SystemIdGenerator {
    fn generate(&self) -> Result<TableId> {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes).map_err(|error| Error::Random(error.to_string()))?;
        Ok(TableId(bytes))
    }
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }
}

/// Transport-independent logical database over one async Prolly store.
pub struct Database<S: AsyncStore> {
    engine: Arc<AsyncProlly<S>>,
    blobs: BlobLayer,
    publication_mode: StoragePublicationMode,
    ids: Arc<dyn IdGenerator>,
    clock: Arc<dyn Clock>,
    logical_retry_limit: usize,
    /// Prevent cloned in-process clients from doing expensive speculative
    /// tree work against the same stale logical heads. Provider CAS and
    /// logical retries remain authoritative across processes.
    write_admission: Arc<Mutex<()>>,
}

impl<S> Database<S>
where
    S: AsyncStore + Clone,
    <S as AsyncStore>::Error: Send + Sync,
{
    async fn prefetch_transaction_global_roots(
        &self,
        tx: &AsyncProllyTransaction<'_, S>,
    ) -> Result<()>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        // Pin every global root used by replay, schema lookup, the maintenance
        // fence, and the idempotency-map authority guard in one strongly
        // consistent batch. This makes no fence decision, so a completed
        // durable-token replay remains observable during maintenance.
        let maintenance_head = self
            .engine
            .versioned_map(MAINTENANCE_CONTROL_MAP_ID)
            .head_name()
            .to_vec();
        let catalog_head = self
            .engine
            .versioned_map(CATALOG_MAP_ID)
            .head_name()
            .to_vec();
        let idempotency_head = self
            .engine
            .versioned_map(IDEMPOTENCY_MAP_ID)
            .head_name()
            .to_vec();
        let idempotency_index_root = prolly::indexed_collection_root_name(IDEMPOTENCY_MAP_ID)?;
        tx.load_named_roots_ordered(&[
            maintenance_head.as_slice(),
            catalog_head.as_slice(),
            idempotency_head.as_slice(),
            idempotency_index_root.as_slice(),
        ])
        .await?;
        Ok(())
    }

    async fn ensure_writes_unfenced(
        &self,
        tx: &AsyncProllyTransaction<'_, S>,
        maps: &AsyncVersionedMapsTransaction<'_, '_, S>,
    ) -> Result<()>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        let maintenance_head = self
            .engine
            .versioned_map(MAINTENANCE_CONTROL_MAP_ID)
            .head_name()
            .to_vec();
        let catalog_head = self
            .engine
            .versioned_map(CATALOG_MAP_ID)
            .head_name()
            .to_vec();
        tx.load_named_roots_ordered(&[maintenance_head.as_slice(), catalog_head.as_slice()])
            .await?;
        if let Some(bytes) = maps
            .get(MAINTENANCE_CONTROL_MAP_ID, MAINTENANCE_LEASE_KEY)
            .await?
        {
            let lease = decode_maintenance_lease(&bytes)?;
            return Err(Error::MaintenanceInProgress { lease_id: lease.id });
        }
        Ok(())
    }

    /// Return the active fail-closed writer fence, if any. An expired lease is
    /// still active until explicitly released or force-broken.
    pub async fn maintenance_lease(&self) -> Result<Option<MaintenanceLease>>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        self.engine
            .versioned_map(MAINTENANCE_CONTROL_MAP_ID)
            .get(MAINTENANCE_LEASE_KEY)
            .await?
            .map(|bytes| decode_maintenance_lease(&bytes))
            .transpose()
    }

    /// Atomically acquire the global maintenance writer fence.
    pub async fn acquire_maintenance_lease(
        &self,
        context: MaintenanceContext,
        duration_millis: u64,
    ) -> Result<MaintenanceLease>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        context.validate()?;
        if !(MIN_MAINTENANCE_LEASE_MILLIS..=MAX_MAINTENANCE_LEASE_MILLIS).contains(&duration_millis)
        {
            return Err(Error::Validation(format!(
                "maintenance lease duration must be {MIN_MAINTENANCE_LEASE_MILLIS}..={MAX_MAINTENANCE_LEASE_MILLIS} milliseconds"
            )));
        }
        let acquired_at_millis = self.clock.now_millis();
        let expires_at_millis = acquired_at_millis
            .checked_add(duration_millis)
            .ok_or_else(|| Error::Validation("maintenance lease expiry overflow".into()))?;
        let lease = MaintenanceLease {
            id: MaintenanceLeaseId(self.ids.generate()?.0),
            context,
            acquired_at_millis,
            expires_at_millis,
        };
        let encoded = encode_maintenance_lease(&lease)?;
        for _ in 0..=self.logical_retry_limit {
            let tx = self.engine.begin_transaction()?;
            let maps = tx.versioned_maps_at_millis(acquired_at_millis);
            if let Some(bytes) = maps
                .get(MAINTENANCE_CONTROL_MAP_ID, MAINTENANCE_LEASE_KEY)
                .await?
            {
                let current = decode_maintenance_lease(&bytes)?;
                tx.rollback();
                return Err(Error::MaintenanceInProgress {
                    lease_id: current.id,
                });
            }
            maps.put(
                MAINTENANCE_CONTROL_MAP_ID,
                MAINTENANCE_LEASE_KEY.to_vec(),
                encoded.clone(),
            )
            .await?;
            match tx.commit().await {
                Ok(TransactionUpdate::Applied { .. }) => return Ok(lease),
                Ok(TransactionUpdate::Conflict(_)) => continue,
                Err(source) => match self.maintenance_lease().await {
                    Ok(Some(current)) if current == lease => return Ok(current),
                    _ => return Err(Error::Storage(source)),
                },
            }
        }
        Err(Error::ConflictExhausted)
    }

    /// Release a held fence. This is valid before or after expiry and records
    /// durable operator evidence in the same transaction as writer admission.
    pub async fn release_maintenance_lease(
        &self,
        id: &MaintenanceLeaseId,
        context: MaintenanceContext,
    ) -> Result<MaintenanceLeaseRelease>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        self.finish_maintenance_lease(id, context, false).await
    }

    /// Force-break a crashed holder's fence, but only after its durable expiry.
    pub async fn break_expired_maintenance_lease(
        &self,
        id: &MaintenanceLeaseId,
        context: MaintenanceContext,
    ) -> Result<MaintenanceLeaseRelease>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        self.finish_maintenance_lease(id, context, true).await
    }

    async fn finish_maintenance_lease(
        &self,
        id: &MaintenanceLeaseId,
        context: MaintenanceContext,
        force_after_expiry: bool,
    ) -> Result<MaintenanceLeaseRelease>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        context.validate()?;
        if let Some(mut release) = self.maintenance_lease_release(id).await? {
            if release.context != context || release.forced_after_expiry != force_after_expiry {
                return Err(Error::IdempotentParameterMismatch);
            }
            release.replayed = true;
            return Ok(release);
        }
        let released_at_millis = self.clock.now_millis();
        let tx = self.engine.begin_transaction()?;
        let maps = tx.versioned_maps_at_millis(released_at_millis);
        if let Some(bytes) = maps.get(MAINTENANCE_LEASE_AUDIT_MAP_ID, &id.0).await? {
            let mut release = decode_maintenance_lease_release(&bytes)?;
            tx.rollback();
            if release.context != context || release.forced_after_expiry != force_after_expiry {
                return Err(Error::IdempotentParameterMismatch);
            }
            release.replayed = true;
            return Ok(release);
        }
        let bytes = maps
            .get(MAINTENANCE_CONTROL_MAP_ID, MAINTENANCE_LEASE_KEY)
            .await?
            .ok_or_else(|| Error::MaintenancePlanStale("maintenance lease is absent".into()))?;
        let lease = decode_maintenance_lease(&bytes)?;
        if lease.id != *id {
            tx.rollback();
            return Err(Error::MaintenancePlanStale(
                "another maintenance lease is active".into(),
            ));
        }
        if force_after_expiry && released_at_millis < lease.expires_at_millis {
            tx.rollback();
            return Err(Error::Validation(
                "maintenance lease cannot be force-broken before expiry".into(),
            ));
        }
        if maps
            .get(GC_EXECUTION_MAP_ID, GC_ACTIVE_KEY)
            .await?
            .is_some()
        {
            tx.rollback();
            return Err(Error::MaintenancePlanStale(
                "maintenance lease is pinned by an active GC execution".into(),
            ));
        }
        maps.delete(MAINTENANCE_CONTROL_MAP_ID, MAINTENANCE_LEASE_KEY)
            .await?;
        let release = MaintenanceLeaseRelease {
            lease,
            context,
            released_at_millis,
            forced_after_expiry: force_after_expiry,
            replayed: false,
        };
        maps.put(
            MAINTENANCE_LEASE_AUDIT_MAP_ID,
            id.0.to_vec(),
            encode_maintenance_lease_release(&release)?,
        )
        .await?;
        match tx.commit().await {
            Ok(TransactionUpdate::Applied { .. }) => Ok(release),
            Ok(TransactionUpdate::Conflict(_)) => Err(Error::MaintenancePlanStale(
                "maintenance lease changed concurrently".into(),
            )),
            Err(source) => match self.maintenance_lease_release(id).await {
                Ok(Some(mut stored)) if stored == release => {
                    stored.replayed = true;
                    Ok(stored)
                }
                _ => Err(Error::Storage(source)),
            },
        }
    }

    pub async fn maintenance_lease_release(
        &self,
        id: &MaintenanceLeaseId,
    ) -> Result<Option<MaintenanceLeaseRelease>>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        self.engine
            .versioned_map(MAINTENANCE_LEASE_AUDIT_MAP_ID)
            .get(&id.0)
            .await?
            .map(|bytes| decode_maintenance_lease_release(&bytes))
            .transpose()
    }

    /// Inspect the currently active lease for one explicit worker job.
    pub async fn worker_lease(&self, job_id: &WorkerJobId) -> Result<Option<WorkerLease>>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        self.engine
            .versioned_map(WORKER_LEASE_MAP_ID)
            .get(&job_id.0)
            .await?
            .map(|bytes| decode_worker_lease(&bytes))
            .transpose()
    }

    /// Acquire or renew a worker lease. A live lease held by another owner is
    /// never stolen; an expired takeover increments the durable fence.
    pub async fn acquire_worker_lease(
        &self,
        job_id: WorkerJobId,
        kind: WorkerKind,
        configuration_digest: [u8; 32],
        owner_id: impl Into<String>,
        duration_millis: u64,
    ) -> Result<WorkerLease>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        let owner_id = owner_id.into();
        validate_worker_identity(
            &job_id,
            kind,
            configuration_digest,
            &owner_id,
            duration_millis,
        )?;
        for _ in 0..=self.logical_retry_limit {
            let now = self.clock.now_millis();
            let expires_at_millis = now
                .checked_add(duration_millis)
                .ok_or_else(|| Error::Validation("worker lease expiry overflow".into()))?;
            let tx = self.engine.begin_transaction()?;
            let maps = tx.versioned_maps_at_millis(now);
            let current = maps
                .get(WORKER_LEASE_MAP_ID, &job_id.0)
                .await?
                .map(|bytes| decode_worker_lease(&bytes))
                .transpose()?;
            let persisted_fence = maps
                .get(WORKER_FENCE_MAP_ID, &job_id.0)
                .await?
                .map(|bytes| decode_worker_fence(&bytes))
                .transpose()?;
            let lease = match current {
                Some(current) => {
                    if current.kind != kind
                        || current.configuration_digest != configuration_digest
                        || current.job_id != job_id
                    {
                        return Err(Error::CorruptData(
                            "worker lease identity/configuration mismatch".into(),
                        ));
                    }
                    let durable_fence = persisted_fence.unwrap_or(current.fence);
                    if durable_fence != current.fence {
                        return Err(Error::CorruptData(
                            "active worker lease disagrees with its durable fence counter".into(),
                        ));
                    }
                    if now < current.expires_at_millis && current.owner_id != owner_id {
                        tx.rollback();
                        return Err(Error::WorkerLeaseHeld {
                            job_id,
                            expires_at_millis: current.expires_at_millis,
                        });
                    }
                    if now < current.expires_at_millis {
                        WorkerLease {
                            renewed_at_millis: now,
                            expires_at_millis,
                            ..current
                        }
                    } else {
                        WorkerLease {
                            job_id: job_id.clone(),
                            kind,
                            configuration_digest,
                            owner_id: owner_id.clone(),
                            fence: durable_fence.checked_add(1).ok_or_else(|| {
                                Error::Validation("worker fencing token exhausted".into())
                            })?,
                            acquired_at_millis: now,
                            renewed_at_millis: now,
                            expires_at_millis,
                        }
                    }
                }
                None => {
                    let fence = persisted_fence.unwrap_or(0).checked_add(1).ok_or_else(|| {
                        Error::Validation("worker fencing token exhausted".into())
                    })?;
                    WorkerLease {
                        job_id: job_id.clone(),
                        kind,
                        configuration_digest,
                        owner_id: owner_id.clone(),
                        fence,
                        acquired_at_millis: now,
                        renewed_at_millis: now,
                        expires_at_millis,
                    }
                }
            };
            maps.put(
                WORKER_FENCE_MAP_ID,
                job_id.0.to_vec(),
                encode_worker_fence(lease.fence),
            )
            .await?;
            maps.put(
                WORKER_LEASE_MAP_ID,
                job_id.0.to_vec(),
                encode_worker_lease(&lease)?,
            )
            .await?;
            match tx.commit().await {
                Ok(TransactionUpdate::Applied { .. }) => return Ok(lease),
                Ok(TransactionUpdate::Conflict(_)) => continue,
                Err(source) => match self.worker_lease(&job_id).await {
                    Ok(Some(stored)) if stored == lease => return Ok(stored),
                    _ => return Err(Error::Storage(source)),
                },
            }
        }
        Err(Error::ConflictExhausted)
    }

    /// Renew only the exact live fencing generation; expiry or takeover is a
    /// lease-lost error and never silently reacquires under a new fence.
    pub async fn renew_worker_lease(
        &self,
        expected: &WorkerLease,
        duration_millis: u64,
    ) -> Result<WorkerLease>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        validate_worker_identity(
            &expected.job_id,
            expected.kind,
            expected.configuration_digest,
            &expected.owner_id,
            duration_millis,
        )?;
        let now = self.clock.now_millis();
        let expires_at_millis = now
            .checked_add(duration_millis)
            .ok_or_else(|| Error::Validation("worker lease expiry overflow".into()))?;
        let tx = self.engine.begin_transaction()?;
        let maps = tx.versioned_maps_at_millis(now);
        let current = maps
            .get(WORKER_LEASE_MAP_ID, &expected.job_id.0)
            .await?
            .map(|bytes| decode_worker_lease(&bytes))
            .transpose()?;
        if current.as_ref() != Some(expected) || now >= expected.expires_at_millis {
            tx.rollback();
            return Err(Error::WorkerLeaseLost {
                job_id: expected.job_id.clone(),
            });
        }
        let renewed = WorkerLease {
            renewed_at_millis: now,
            expires_at_millis,
            ..expected.clone()
        };
        maps.put(
            WORKER_LEASE_MAP_ID,
            expected.job_id.0.to_vec(),
            encode_worker_lease(&renewed)?,
        )
        .await?;
        match tx.commit().await {
            Ok(TransactionUpdate::Applied { .. }) => Ok(renewed),
            Ok(TransactionUpdate::Conflict(_)) => Err(Error::WorkerLeaseLost {
                job_id: expected.job_id.clone(),
            }),
            Err(source) => match self.worker_lease(&expected.job_id).await {
                Ok(Some(stored)) if stored == renewed => Ok(stored),
                _ => Err(Error::Storage(source)),
            },
        }
    }

    /// Release an exact lease generation and persist replayable evidence.
    pub async fn release_worker_lease(&self, expected: &WorkerLease) -> Result<WorkerLeaseRelease>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        let key = worker_lease_audit_key(&expected.job_id, expected.fence);
        if let Some(mut release) = self
            .worker_lease_release(&expected.job_id, expected.fence)
            .await?
        {
            if release.lease != *expected {
                return Err(Error::IdempotentParameterMismatch);
            }
            release.replayed = true;
            return Ok(release);
        }
        let now = self.clock.now_millis();
        let tx = self.engine.begin_transaction()?;
        let maps = tx.versioned_maps_at_millis(now);
        if let Some(bytes) = maps.get(WORKER_LEASE_AUDIT_MAP_ID, &key).await? {
            let mut release = decode_worker_lease_release(&bytes)?;
            tx.rollback();
            if release.lease != *expected {
                return Err(Error::IdempotentParameterMismatch);
            }
            release.replayed = true;
            return Ok(release);
        }
        let current = maps
            .get(WORKER_LEASE_MAP_ID, &expected.job_id.0)
            .await?
            .map(|bytes| decode_worker_lease(&bytes))
            .transpose()?;
        if current.as_ref() != Some(expected) {
            tx.rollback();
            return Err(Error::WorkerLeaseLost {
                job_id: expected.job_id.clone(),
            });
        }
        maps.delete(WORKER_LEASE_MAP_ID, &expected.job_id.0).await?;
        let release = WorkerLeaseRelease {
            lease: expected.clone(),
            released_at_millis: now,
            replayed: false,
        };
        maps.put(
            WORKER_LEASE_AUDIT_MAP_ID,
            key,
            encode_worker_lease_release(&release)?,
        )
        .await?;
        match tx.commit().await {
            Ok(TransactionUpdate::Applied { .. }) => Ok(release),
            Ok(TransactionUpdate::Conflict(_)) => Err(Error::WorkerLeaseLost {
                job_id: expected.job_id.clone(),
            }),
            Err(source) => match self
                .worker_lease_release(&expected.job_id, expected.fence)
                .await
            {
                Ok(Some(mut stored)) if stored == release => {
                    stored.replayed = true;
                    Ok(stored)
                }
                _ => Err(Error::Storage(source)),
            },
        }
    }

    pub async fn worker_lease_release(
        &self,
        job_id: &WorkerJobId,
        fence: u64,
    ) -> Result<Option<WorkerLeaseRelease>>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        self.engine
            .versioned_map(WORKER_LEASE_AUDIT_MAP_ID)
            .get(&worker_lease_audit_key(job_id, fence))
            .await?
            .map(|bytes| decode_worker_lease_release(&bytes))
            .transpose()
    }

    pub async fn worker_checkpoint(&self, job_id: &WorkerJobId) -> Result<Option<WorkerCheckpoint>>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        self.engine
            .versioned_map(WORKER_CHECKPOINT_MAP_ID)
            .get(&job_id.0)
            .await?
            .map(|bytes| decode_worker_checkpoint(&bytes))
            .transpose()
    }

    /// CAS one monotonic checkpoint while validating the exact unexpired
    /// worker lease in the same strict transaction.
    pub async fn update_worker_checkpoint(
        &self,
        lease: &WorkerLease,
        expected_revision: Option<u64>,
        progress: WorkerProgress,
    ) -> Result<WorkerCheckpoint>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        validate_worker_progress(lease.kind, &progress)?;
        let now = self.clock.now_millis();
        let tx = self.engine.begin_transaction()?;
        let maps = tx.versioned_maps_at_millis(now);
        let current_lease = maps
            .get(WORKER_LEASE_MAP_ID, &lease.job_id.0)
            .await?
            .map(|bytes| decode_worker_lease(&bytes))
            .transpose()?;
        if current_lease.as_ref() != Some(lease) || now >= lease.expires_at_millis {
            tx.rollback();
            return Err(Error::WorkerLeaseLost {
                job_id: lease.job_id.clone(),
            });
        }
        let current = maps
            .get(WORKER_CHECKPOINT_MAP_ID, &lease.job_id.0)
            .await?
            .map(|bytes| decode_worker_checkpoint(&bytes))
            .transpose()?;
        if current.as_ref().map(|checkpoint| checkpoint.revision) != expected_revision {
            tx.rollback();
            return Err(Error::WorkerCheckpointConflict {
                job_id: lease.job_id.clone(),
            });
        }
        if let Some(current) = &current {
            if current.kind != lease.kind
                || current.configuration_digest != lease.configuration_digest
                || current.job_id != lease.job_id
            {
                return Err(Error::CorruptData(
                    "worker checkpoint identity/configuration mismatch".into(),
                ));
            }
            validate_worker_progress_transition(&current.progress, &progress)?;
        }
        let revision = expected_revision
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| Error::Validation("worker checkpoint revision exhausted".into()))?;
        let checkpoint = WorkerCheckpoint {
            job_id: lease.job_id.clone(),
            kind: lease.kind,
            configuration_digest: lease.configuration_digest,
            revision,
            fence: lease.fence,
            progress,
            updated_at_millis: now,
        };
        maps.put(
            WORKER_CHECKPOINT_MAP_ID,
            lease.job_id.0.to_vec(),
            encode_worker_checkpoint(&checkpoint)?,
        )
        .await?;
        match tx.commit().await {
            Ok(TransactionUpdate::Applied { .. }) => Ok(checkpoint),
            Ok(TransactionUpdate::Conflict(_)) => Err(Error::WorkerCheckpointConflict {
                job_id: lease.job_id.clone(),
            }),
            Err(source) => match self.worker_checkpoint(&lease.job_id).await {
                Ok(Some(stored)) if stored == checkpoint => Ok(stored),
                _ => Err(Error::Storage(source)),
            },
        }
    }

    /// Resolve durable progress for one exact GC plan page.
    pub async fn gc_execution(&self, plan_id: &[u8; 32]) -> Result<Option<GcExecutionRecord>>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        let record = self
            .engine
            .versioned_map(GC_EXECUTION_MAP_ID)
            .get(plan_id)
            .await?
            .map(|bytes| decode_gc_execution_record(&bytes))
            .transpose()?;
        if record
            .as_ref()
            .is_some_and(|record| record.plan_id != *plan_id)
        {
            return Err(Error::CorruptData(
                "GC execution record key disagrees with its plan ID".into(),
            ));
        }
        Ok(record)
    }

    /// Atomically pin the maintenance fence to one exact physical GC plan.
    #[allow(clippy::too_many_arguments)]
    pub async fn begin_gc_execution(
        &self,
        plan_id: [u8; 32],
        lease_id: &MaintenanceLeaseId,
        roots_digest: [u8; 32],
        node_deletes: usize,
        blob_deletes: usize,
        context: MaintenanceContext,
    ) -> Result<GcExecutionResult>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        context.validate()?;
        validate_gc_delete_counts(node_deletes, blob_deletes)?;
        if let Some(record) = self.gc_execution(&plan_id).await? {
            validate_gc_execution_parameters(
                &record,
                lease_id,
                &roots_digest,
                node_deletes,
                blob_deletes,
                &context,
            )?;
            return Ok(GcExecutionResult {
                record,
                replayed: true,
            });
        }
        let started_at_millis = self.clock.now_millis();
        let record = GcExecutionRecord {
            plan_id,
            lease_id: lease_id.clone(),
            roots_digest,
            node_deletes,
            blob_deletes,
            context,
            state: GcExecutionState::InProgress,
            started_at_millis,
            completed_at_millis: None,
        };
        let tx = self.engine.begin_transaction()?;
        let maps = tx.versioned_maps_at_millis(started_at_millis);
        if let Some(bytes) = maps.get(GC_EXECUTION_MAP_ID, &plan_id).await? {
            let existing = decode_gc_execution_record(&bytes)?;
            tx.rollback();
            validate_gc_execution_parameters(
                &existing,
                lease_id,
                &roots_digest,
                node_deletes,
                blob_deletes,
                &record.context,
            )?;
            return Ok(GcExecutionResult {
                record: existing,
                replayed: true,
            });
        }
        let lease = maps
            .get(MAINTENANCE_CONTROL_MAP_ID, MAINTENANCE_LEASE_KEY)
            .await?
            .map(|bytes| decode_maintenance_lease(&bytes))
            .transpose()?
            .ok_or_else(|| Error::MaintenancePlanStale("maintenance lease is absent".into()))?;
        if lease.id != *lease_id {
            tx.rollback();
            return Err(Error::MaintenancePlanStale(
                "GC plan belongs to another maintenance lease".into(),
            ));
        }
        if let Some(active) = maps.get(GC_EXECUTION_MAP_ID, GC_ACTIVE_KEY).await? {
            tx.rollback();
            let active: [u8; 32] = active
                .try_into()
                .map_err(|_| Error::CorruptData("active GC execution ID is malformed".into()))?;
            return Err(Error::MaintenancePlanStale(format!(
                "another GC execution {} is active",
                hex_id(&active)
            )));
        }
        maps.apply(
            GC_EXECUTION_MAP_ID,
            vec![
                Mutation::Upsert {
                    key: plan_id.to_vec(),
                    val: encode_gc_execution_record(&record)?,
                },
                Mutation::Upsert {
                    key: GC_ACTIVE_KEY.to_vec(),
                    val: plan_id.to_vec(),
                },
            ],
        )
        .await?;
        match tx.commit().await {
            Ok(TransactionUpdate::Applied { .. }) => Ok(GcExecutionResult {
                record,
                replayed: false,
            }),
            Ok(TransactionUpdate::Conflict(_)) => Err(Error::MaintenancePlanStale(
                "GC execution start conflicted with maintenance state".into(),
            )),
            Err(source) => match self.gc_execution(&plan_id).await {
                Ok(Some(stored)) if stored == record => Ok(GcExecutionResult {
                    record: stored,
                    replayed: true,
                }),
                _ => Err(Error::Storage(source)),
            },
        }
    }

    /// Mark one exact GC execution complete and release its pin on the lease.
    pub async fn complete_gc_execution(
        &self,
        plan_id: &[u8; 32],
        lease_id: &MaintenanceLeaseId,
    ) -> Result<GcExecutionResult>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        if let Some(record) = self.gc_execution(plan_id).await? {
            if record.lease_id != *lease_id {
                return Err(Error::IdempotentParameterMismatch);
            }
            if record.state == GcExecutionState::Complete {
                return Ok(GcExecutionResult {
                    record,
                    replayed: true,
                });
            }
        }
        let completed_at_millis = self.clock.now_millis();
        let tx = self.engine.begin_transaction()?;
        let maps = tx.versioned_maps_at_millis(completed_at_millis);
        let bytes = maps
            .get(GC_EXECUTION_MAP_ID, plan_id)
            .await?
            .ok_or_else(|| Error::MaintenancePlanStale("GC execution is absent".into()))?;
        let mut record = decode_gc_execution_record(&bytes)?;
        if record.plan_id != *plan_id {
            tx.rollback();
            return Err(Error::CorruptData(
                "GC execution record key disagrees with its plan ID".into(),
            ));
        }
        if record.lease_id != *lease_id {
            tx.rollback();
            return Err(Error::IdempotentParameterMismatch);
        }
        if record.state == GcExecutionState::Complete {
            tx.rollback();
            return Ok(GcExecutionResult {
                record,
                replayed: true,
            });
        }
        let lease = maps
            .get(MAINTENANCE_CONTROL_MAP_ID, MAINTENANCE_LEASE_KEY)
            .await?
            .map(|bytes| decode_maintenance_lease(&bytes))
            .transpose()?
            .ok_or_else(|| Error::MaintenancePlanStale("maintenance lease is absent".into()))?;
        if lease.id != *lease_id {
            tx.rollback();
            return Err(Error::MaintenancePlanStale(
                "GC maintenance lease changed before completion".into(),
            ));
        }
        let active = maps
            .get(GC_EXECUTION_MAP_ID, GC_ACTIVE_KEY)
            .await?
            .ok_or_else(|| Error::CorruptData("GC execution lost its active pin".into()))?;
        if active.as_slice() != plan_id {
            tx.rollback();
            return Err(Error::CorruptData(
                "GC active pin identifies another plan".into(),
            ));
        }
        record.state = GcExecutionState::Complete;
        record.completed_at_millis = Some(completed_at_millis);
        maps.apply(
            GC_EXECUTION_MAP_ID,
            vec![
                Mutation::Upsert {
                    key: plan_id.to_vec(),
                    val: encode_gc_execution_record(&record)?,
                },
                Mutation::Delete {
                    key: GC_ACTIVE_KEY.to_vec(),
                },
            ],
        )
        .await?;
        match tx.commit().await {
            Ok(TransactionUpdate::Applied { .. }) => Ok(GcExecutionResult {
                record,
                replayed: false,
            }),
            Ok(TransactionUpdate::Conflict(_)) => Err(Error::MaintenancePlanStale(
                "GC completion conflicted with maintenance state".into(),
            )),
            Err(source) => match self.gc_execution(plan_id).await {
                Ok(Some(stored)) if stored == record => Ok(GcExecutionResult {
                    record: stored,
                    replayed: true,
                }),
                _ => Err(Error::Storage(source)),
            },
        }
    }

    /// Export one exact immutable table version as a self-contained, bounded,
    /// canonical archive. `None` pins the current head once at method entry.
    pub async fn export_table(
        &self,
        table: &str,
        version: Option<&MapVersionId>,
        limits: TableArchiveLimits,
    ) -> Result<TableArchive>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        let limits = limits.validate()?;
        let current_description = self.describe_table(table).await?;
        let map = self
            .engine
            .versioned_map(Self::table_map_id(&current_description.id));
        let selected = match version {
            Some(id) => map.version(id).await?,
            None => map.head().await?,
        }
        .ok_or_else(|| {
            Error::Validation(match version {
                Some(id) => format!("unknown table version {id}"),
                None => format!("table {table:?} has no head"),
            })
        })?;
        let description = self
            .schema_at_version(&current_description.id, &selected.id)
            .await?;
        if description.name != table {
            return Err(Error::CorruptData(
                "historical schema descriptor has another table name".into(),
            ));
        }
        let snapshot = self
            .engine
            .export_snapshot_with_limits(
                &selected.tree,
                SnapshotExportLimits::new(limits.max_nodes, limits.max_node_bytes),
            )
            .await?;
        let references = crate::archive::referenced_blobs(&snapshot)?;
        if references.len() > limits.max_blobs {
            return Err(Error::Validation(format!(
                "table archive exceeded blobs limit: limit={}, actual={}",
                limits.max_blobs,
                references.len()
            )));
        }
        let expected_blob_bytes = references.values().try_fold(0usize, |total, length| {
            let length = usize::try_from(*length).map_err(|_| {
                Error::Validation("table archive blob length exceeds platform limits".into())
            })?;
            total
                .checked_add(length)
                .ok_or_else(|| Error::Validation("table archive blob byte count overflow".into()))
        })?;
        if expected_blob_bytes > limits.max_blob_bytes {
            return Err(Error::Validation(format!(
                "table archive exceeded blob bytes limit: limit={}, actual={expected_blob_bytes}",
                limits.max_blob_bytes
            )));
        }
        let mut blobs = Vec::with_capacity(references.len());
        for (cid, len) in references {
            let reference = prolly::BlobRef { cid, len };
            let bytes = self.blobs.get_verified(&reference).await?;
            blobs.push(TableArchiveBlob { reference, bytes });
        }
        let archive = TableArchive {
            format_version: crate::TABLE_ARCHIVE_FORMAT_VERSION,
            source: description,
            version: selected.id,
            version_created_at_millis: selected.created_at_millis,
            database_format: self.format_record()?,
            snapshot,
            blobs,
        };
        archive.verify(limits)?;
        Ok(archive)
    }

    /// Open and negotiate the durable database/tree format.
    pub async fn open(store: S, config: prolly::Config) -> Result<Self>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        let database = Self::new(store, config);
        database.ensure_format().await?;
        Ok(database)
    }

    /// Open with a durable large-value policy and negotiate it as part of the
    /// database format. Clients sharing a namespace must use the same policy.
    pub async fn open_with_blob_storage(
        store: S,
        config: prolly::Config,
        storage: Arc<dyn BlobStorage>,
        large_value_config: LargeValueConfig,
    ) -> Result<Self>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        Self::open_with_blob_storage_and_mode(
            store,
            config,
            storage,
            large_value_config,
            StoragePublicationMode::AtomicNodesAndRoots,
        )
        .await
    }

    /// Open with an explicit provider publication mode included in durable
    /// format negotiation.
    pub async fn open_with_blob_storage_and_mode(
        store: S,
        config: prolly::Config,
        storage: Arc<dyn BlobStorage>,
        large_value_config: LargeValueConfig,
        publication_mode: StoragePublicationMode,
    ) -> Result<Self>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        Self::open_with_blob_storage_and_mode_and_sources(
            store,
            config,
            storage,
            large_value_config,
            publication_mode,
            Arc::new(SystemIdGenerator),
            Arc::new(SystemClock),
        )
        .await
    }

    /// Open with explicit publication, ID, and metadata-clock policies.
    ///
    /// Supplying deterministic sources is useful for conformance and replay
    /// tests. Production ID sources must remain collision-resistant across all
    /// writers sharing a namespace, and production clocks must satisfy the
    /// documented lease and retention assumptions.
    pub async fn open_with_blob_storage_and_mode_and_sources(
        store: S,
        config: prolly::Config,
        storage: Arc<dyn BlobStorage>,
        large_value_config: LargeValueConfig,
        publication_mode: StoragePublicationMode,
        ids: Arc<dyn IdGenerator>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        let mut database = Self::new_with_blob_storage(store, config, storage, large_value_config)?;
        database.publication_mode = publication_mode;
        database = database.with_sources(ids, clock);
        database.ensure_format().await?;
        Ok(database)
    }

    async fn ensure_format(&self) -> Result<()>
    where
        S: AsyncManifestStore + AsyncTransactionalStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        let expected_record = self.format_record()?;
        let expected = expected_record.encode();
        let map = self.engine.versioned_map(FORMAT_MAP_ID);
        for _ in 0..=self.logical_retry_limit {
            let head = map.head().await?;
            if let Some(stored) = map.get(FORMAT_RECORD_KEY).await? {
                let stored_record = DatabaseFormatRecord::decode(&stored)?;
                if stored_record == expected_record {
                    return Ok(());
                }
                return Err(Error::FormatMismatch(format!(
                    "stored format {stored_record:?} differs from required format {expected_record:?}"
                )));
            }
            match map
                .apply_if_at_millis(
                    head.as_ref().map(|version| &version.id),
                    vec![Mutation::Upsert {
                        key: FORMAT_RECORD_KEY.to_vec(),
                        val: expected.clone(),
                    }],
                    self.clock.now_millis(),
                )
                .await?
            {
                VersionedMapUpdate::Applied { .. } | VersionedMapUpdate::Unchanged { .. } => {
                    return Ok(())
                }
                VersionedMapUpdate::Conflict { .. } => continue,
            }
        }
        Err(Error::ConflictExhausted)
    }

    pub fn new(store: S, config: prolly::Config) -> Self {
        Self::from_engine(AsyncProlly::new(store, config))
    }

    /// Construct without format negotiation using explicit blob storage.
    /// Intended for tests and bootstrap code; production clients should use
    /// [`Self::open_with_blob_storage`].
    pub fn new_with_blob_storage(
        store: S,
        config: prolly::Config,
        storage: Arc<dyn BlobStorage>,
        large_value_config: LargeValueConfig,
    ) -> Result<Self> {
        let mut database = Self::new(store, config);
        database.blobs = BlobLayer::new(storage, large_value_config)?;
        Ok(database)
    }

    pub fn from_engine(engine: AsyncProlly<S>) -> Self {
        Self {
            engine: Arc::new(engine),
            blobs: BlobLayer::inline_only(),
            publication_mode: StoragePublicationMode::AtomicNodesAndRoots,
            ids: Arc::new(SystemIdGenerator),
            clock: Arc::new(SystemClock),
            logical_retry_limit: DEFAULT_LOGICAL_RETRY_LIMIT,
            write_admission: Arc::new(Mutex::new(())),
        }
    }

    pub fn with_sources(mut self, ids: Arc<dyn IdGenerator>, clock: Arc<dyn Clock>) -> Self {
        self.ids = ids;
        self.clock = clock;
        self
    }

    /// Configure retries after the first optimistic logical attempt.
    ///
    /// This is runtime-only tuning and never participates in persisted format
    /// identity. A value of zero makes exactly one attempt.
    pub fn with_logical_retry_limit(mut self, retries: usize) -> Result<Self> {
        if retries > MAX_LOGICAL_RETRY_LIMIT {
            return Err(Error::Validation(format!(
                "logical retry limit must be <={MAX_LOGICAL_RETRY_LIMIT}"
            )));
        }
        self.logical_retry_limit = retries;
        Ok(self)
    }

    pub fn logical_retry_limit(&self) -> usize {
        self.logical_retry_limit
    }

    pub fn engine(&self) -> &AsyncProlly<S> {
        &self.engine
    }

    /// Required durable format for this configured database handle.
    pub fn format_record(&self) -> Result<DatabaseFormatRecord> {
        Ok(DatabaseFormatRecord::current(
            self.engine.config().format.digest()?,
            self.publication_mode,
            self.blobs.inline_threshold(),
        ))
    }

    fn table_map_id(id: &TableId) -> Vec<u8> {
        let mut map_id = TABLE_MAP_PREFIX.to_vec();
        map_id.extend_from_slice(&id.0);
        map_id
    }

    fn table_commit_log_root_name(id: &TableId) -> Vec<u8> {
        let mut name = Vec::with_capacity(TABLE_COMMIT_LOG_PREFIX.len() + id.0.len());
        name.extend_from_slice(TABLE_COMMIT_LOG_PREFIX);
        name.extend_from_slice(&id.0);
        name
    }

    fn table_indexed_source_id(id: &TableId) -> Vec<u8> {
        let mut map_id = TABLE_INDEXED_SOURCE_PREFIX.to_vec();
        map_id.extend_from_slice(&id.0);
        map_id
    }

    fn table_snapshot_catalog_root_name(id: &TableId) -> Vec<u8> {
        let mut name = Vec::with_capacity(TABLE_SNAPSHOT_CATALOG_PREFIX.len() + id.0.len());
        name.extend_from_slice(TABLE_SNAPSHOT_CATALOG_PREFIX);
        name.extend_from_slice(&id.0);
        name
    }

    fn table_blob_registry_root_name(id: &TableId) -> Vec<u8> {
        let mut name = Vec::with_capacity(TABLE_BLOB_REGISTRY_PREFIX.len() + id.0.len());
        name.extend_from_slice(TABLE_BLOB_REGISTRY_PREFIX);
        name.extend_from_slice(&id.0);
        name
    }

    async fn schema_at_version(
        &self,
        table_id: &TableId,
        version: &MapVersionId,
    ) -> Result<TableDescription>
    where
        S: AsyncManifestStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        let (_, manifest) = self
            .table_snapshot_manifest_at_version(table_id, version)
            .await?
            .ok_or_else(|| {
                Error::Validation(format!(
                    "table version {version} has no retained schema descriptor"
                ))
            })?;
        Ok(manifest.description)
    }

    async fn table_snapshot_manifest_at_version(
        &self,
        table_id: &TableId,
        version: &MapVersionId,
    ) -> Result<Option<(Tree, TableSnapshotManifestRecord)>>
    where
        S: AsyncManifestStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        let name = Self::table_snapshot_catalog_root_name(table_id);
        let Some(tree) = self.engine.load_named_root(&name).await? else {
            return Ok(None);
        };
        let locator = self.engine.get(&tree, version.as_cid().as_bytes()).await?;
        let Some(locator) = locator else {
            return Ok(None);
        };
        let locator = decode_table_snapshot_locator(&locator)?;
        let bytes = self
            .engine
            .get(&locator.manifest_tree, TABLE_SNAPSHOT_RECORD_KEY)
            .await?
            .ok_or_else(|| Error::CorruptData("detached snapshot manifest is absent".into()))?;
        let record = decode_table_snapshot_manifest(&bytes)?;
        if record.table_id != *table_id
            || record.base_version != *version
            || record.indexed.snapshot_id != locator.indexed_snapshot_id
        {
            return Err(Error::CorruptData(
                "snapshot manifest root identity mismatch".into(),
            ));
        }
        Ok(Some((tree, record)))
    }

    /// Expand one current-only table snapshot catalog into the source/index
    /// tree roots it protects. Non-catalog named roots return `None`.
    #[doc(hidden)]
    pub async fn expand_snapshot_catalog_root(
        &self,
        root_name: &[u8],
        tree: &Tree,
        max_trees: usize,
    ) -> Result<Option<SnapshotCatalogProtection>> {
        let Some(raw_table_id) = root_name.strip_prefix(TABLE_SNAPSHOT_CATALOG_PREFIX) else {
            return Ok(None);
        };
        let table_id = TableId(raw_table_id.try_into().map_err(|_| {
            Error::CorruptData("snapshot catalog root has an invalid table identity".into())
        })?);
        let mut versions = Vec::new();
        let mut detached = Vec::new();
        let mut entries = self.engine.range(tree, b"", None).await?;
        while let Some(entry) = entries.next().await {
            let (key, locator) = entry?;
            let version = MapVersionId::from_bytes(&key).map_err(|_| {
                Error::CorruptData("snapshot catalog contains an invalid version key".into())
            })?;
            if detached.len() >= max_trees {
                return Err(Error::Validation(format!(
                    "snapshot catalog expands beyond GC tree limit {max_trees}"
                )));
            }
            versions.push(version);
            detached.push(decode_table_snapshot_locator(&locator)?);
        }
        let detached_trees = detached
            .iter()
            .map(|locator| locator.manifest_tree.clone())
            .collect::<Vec<_>>();
        let records = self
            .engine
            .load_single_leaf_values_ordered(&detached_trees, TABLE_SNAPSHOT_RECORD_KEY)
            .await?;
        let mut protected = Vec::new();
        let mut covered = Vec::with_capacity(versions.len());
        for ((version, locator), bytes) in versions.into_iter().zip(detached).zip(records) {
            let manifest = decode_table_snapshot_manifest(&bytes)?;
            if manifest.table_id != table_id
                || manifest.base_version != version
                || manifest.indexed.snapshot_id != locator.indexed_snapshot_id
            {
                return Err(Error::CorruptData(
                    "snapshot catalog entry identity mismatch".into(),
                ));
            }
            covered.push(version);
            let added = 2usize
                .checked_add(manifest.indexed.record.indexes.len())
                .ok_or_else(|| Error::Validation("snapshot tree count overflow".into()))?;
            if protected
                .len()
                .checked_add(added)
                .is_none_or(|count| count > max_trees)
            {
                return Err(Error::Validation(format!(
                    "snapshot catalog expands beyond GC tree limit {max_trees}"
                )));
            }
            protected.push(locator.manifest_tree);
            protected.push(manifest.indexed.record.source.tree);
            protected.extend(
                manifest
                    .indexed
                    .record
                    .indexes
                    .into_iter()
                    .map(|index| index.tree),
            );
        }
        for tree in &protected {
            covered.push(MapVersionId::for_tree(tree)?);
        }
        covered.sort();
        covered.dedup();
        Ok(Some(SnapshotCatalogProtection {
            protected_trees: protected,
            covered_value_trees: covered,
        }))
    }

    /// Decode one table blob registry into canonical references. A registry is
    /// append-only between explicit compactions, so it is always a safe
    /// reachability superset for every retained table snapshot.
    #[doc(hidden)]
    pub async fn expand_blob_registry_root(
        &self,
        root_name: &[u8],
        tree: &Tree,
        max_blobs: usize,
    ) -> Result<Option<Vec<BlobRef>>> {
        let Some(raw_table_id) = root_name.strip_prefix(TABLE_BLOB_REGISTRY_PREFIX) else {
            return Ok(None);
        };
        let _table_id = TableId(raw_table_id.try_into().map_err(|_| {
            Error::CorruptData("blob registry root has an invalid table identity".into())
        })?);
        let mut references = Vec::new();
        let mut entries = self.engine.range(tree, b"", None).await?;
        while let Some(entry) = entries.next().await {
            let (key, value) = entry?;
            if references.len() >= max_blobs {
                return Err(Error::Validation(format!(
                    "blob registries expand beyond GC blob limit {max_blobs}"
                )));
            }
            let cid =
                prolly::Cid(key.try_into().map_err(|_| {
                    Error::CorruptData("blob registry contains an invalid CID".into())
                })?);
            references.push(decode_table_blob_registry_value(&cid, &value)?);
        }
        Ok(Some(references))
    }

    async fn reconcile_idempotency_record(
        &self,
        key: &[u8],
        fingerprint: &[u8; 32],
        now_millis: u64,
    ) -> Result<Option<TransactWriteResult>>
    where
        S: AsyncManifestStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        let Some(bytes) = self
            .engine
            .versioned_map(IDEMPOTENCY_MAP_ID)
            .get(key)
            .await?
        else {
            return Ok(None);
        };
        let record = decode_idempotency_record(&bytes)?;
        if now_millis.saturating_sub(record.completed_at_millis) > IDEMPOTENCY_WINDOW_MILLIS {
            return Ok(None);
        }
        if &record.fingerprint != fingerprint {
            return Err(Error::IdempotentParameterMismatch);
        }
        Ok(Some(record.result))
    }

    async fn description_for_transition(
        &self,
        transition: &TransactionTableTransition,
    ) -> Result<TableDescription>
    where
        S: AsyncManifestStore,
        <S as AsyncManifestStore>::Error: Send + Sync,
    {
        let version = transition
            .after
            .as_ref()
            .or(transition.before.as_ref())
            .ok_or_else(|| {
                Error::CorruptData("table transition has neither before nor after version".into())
            })?;
        self.schema_at_version(&transition.table_id, version).await
    }
}

impl<S> Database<S>
where
    S: AsyncStore + AsyncManifestStore + AsyncTransactionalStore + Clone,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    async fn stage_table_blob_references<'a>(
        &self,
        tx: &AsyncProllyTransaction<'_, S>,
        table_id: &TableId,
        references: impl IntoIterator<Item = &'a BlobRef>,
        timestamp_millis: u64,
    ) -> Result<()> {
        let mut unique = BTreeMap::new();
        for reference in references {
            match unique.insert(reference.cid.clone(), reference.len) {
                Some(existing) if existing != reference.len => {
                    return Err(Error::CorruptData(
                        "one blob CID was observed with conflicting lengths".into(),
                    ));
                }
                _ => {}
            }
        }
        if unique.is_empty() {
            return Ok(());
        }
        let name = Self::table_blob_registry_root_name(table_id);
        let current = tx.load_named_root(&name).await?;
        let base = current.clone().unwrap_or_else(|| tx.create());
        let mut mutations = Vec::with_capacity(unique.len());
        for (cid, len) in unique {
            if let Some(existing) = tx.get(&base, cid.as_bytes()).await? {
                let stored = decode_table_blob_registry_value(&cid, &existing)?;
                if stored.len != len {
                    return Err(Error::CorruptData(
                        "blob registry contains a conflicting length".into(),
                    ));
                }
                continue;
            }
            mutations.push(Mutation::Upsert {
                key: cid.as_bytes().to_vec(),
                val: encode_table_blob_registry_value(len),
            });
        }
        if mutations.is_empty() {
            return Ok(());
        }
        let next = tx.batch(&base, mutations).await?;
        tx.publish_named_root_at_millis(&name, &next, timestamp_millis)
            .await?;
        Ok(())
    }

    async fn prefetch_table_write_roots(
        &self,
        tx: &AsyncProllyTransaction<'_, S>,
        description: &TableDescription,
    ) -> Result<()> {
        let map_id = Self::table_map_id(&description.id);
        let table_head_name = self.engine.versioned_map(&map_id).head_name().to_vec();
        let snapshot_catalog_name = Self::table_snapshot_catalog_root_name(&description.id);
        let indexed_source_id = Self::table_indexed_source_id(&description.id);
        let indexed_root_name = prolly::indexed_collection_root_name(&indexed_source_id)?;
        let commit_log_name = Self::table_commit_log_root_name(&description.id);
        let managed_map_guard_name = prolly::indexed_collection_root_name(&map_id)?;
        let prefetched = tx
            .load_named_roots_ordered(&[
                table_head_name.as_slice(),
                snapshot_catalog_name.as_slice(),
                indexed_root_name.as_slice(),
                COMMIT_CATALOG_ROOT_NAME,
                commit_log_name.as_slice(),
                managed_map_guard_name.as_slice(),
            ])
            .await?;
        if prefetched[..5].iter().any(Option::is_none) {
            return Err(Error::CorruptData(
                "table write metadata root set is incomplete".into(),
            ));
        }
        Ok(())
    }

    async fn stage_table_snapshot_manifest(
        &self,
        tx: &AsyncProllyTransaction<'_, S>,
        description: &TableDescription,
        base_version: &MapVersionId,
        indexed: &IndexedSnapshotManifest,
        timestamp_millis: u64,
    ) -> Result<Tree> {
        let record = TableSnapshotManifestRecord {
            format_version: TABLE_SNAPSHOT_MANIFEST_FORMAT,
            table_id: description.id.clone(),
            base_version: base_version.clone(),
            description: description.clone(),
            indexed: indexed.clone(),
        };
        let bytes = encode_table_snapshot_manifest(&record)?;
        let name = Self::table_snapshot_catalog_root_name(&description.id);
        let current = tx.load_named_root(&name).await?;
        let base = current.clone().unwrap_or_else(|| tx.create());
        if let Some(locator) = tx.get(&base, base_version.as_cid().as_bytes()).await? {
            let locator = decode_table_snapshot_locator(&locator)?;
            let existing = tx
                .get(&locator.manifest_tree, TABLE_SNAPSHOT_RECORD_KEY)
                .await?
                .ok_or_else(|| Error::CorruptData("detached snapshot manifest is absent".into()))?;
            let existing = decode_table_snapshot_manifest(&existing)?;
            if existing.indexed.snapshot_id != locator.indexed_snapshot_id {
                return Err(Error::CorruptData(
                    "snapshot locator and detached manifest disagree".into(),
                ));
            }
            if !table_snapshot_manifests_semantically_equal(&existing, &record) {
                return Err(Error::CorruptData(
                    "base version is bound to a different snapshot manifest".into(),
                ));
            }
            if encode_table_snapshot_manifest(&existing)? == bytes {
                return Ok(base);
            }
        }
        let detached = tx
            .put(&tx.create(), TABLE_SNAPSHOT_RECORD_KEY.to_vec(), bytes)
            .await?;
        let locator = encode_table_snapshot_locator(&TableSnapshotLocator {
            manifest_tree: detached,
            indexed_snapshot_id: record.indexed.snapshot_id,
        })?;
        let tree = tx
            .put(&base, base_version.as_cid().as_bytes().to_vec(), locator)
            .await?;
        tx.publish_named_root_at_millis(&name, &tree, timestamp_millis)
            .await?;
        Ok(tree)
    }

    async fn transaction_table_snapshot_manifest(
        &self,
        tx: &AsyncProllyTransaction<'_, S>,
        table_id: &TableId,
        version: &MapVersionId,
    ) -> Result<Option<TableSnapshotManifestRecord>> {
        let name = Self::table_snapshot_catalog_root_name(table_id);
        let Some(tree) = tx.load_named_root(&name).await? else {
            return Ok(None);
        };
        let Some(locator) = tx.get(&tree, version.as_cid().as_bytes()).await? else {
            return Ok(None);
        };
        let locator = decode_table_snapshot_locator(&locator)?;
        let bytes = tx
            .get(&locator.manifest_tree, TABLE_SNAPSHOT_RECORD_KEY)
            .await?
            .ok_or_else(|| Error::CorruptData("detached snapshot manifest is absent".into()))?;
        let record = decode_table_snapshot_manifest(&bytes)?;
        if record.table_id != *table_id
            || record.base_version != *version
            || record.indexed.snapshot_id != locator.indexed_snapshot_id
        {
            return Err(Error::CorruptData(
                "transaction-visible snapshot manifest identity mismatch".into(),
            ));
        }
        Ok(Some(record))
    }

    async fn transaction_table_indexed_snapshot_id(
        &self,
        tx: &AsyncProllyTransaction<'_, S>,
        table_id: &TableId,
        version: &MapVersionId,
    ) -> Result<Option<IndexedSnapshotId>> {
        let name = Self::table_snapshot_catalog_root_name(table_id);
        let Some(tree) = tx.load_named_root(&name).await? else {
            return Ok(None);
        };
        let Some(bytes) = tx.get(&tree, version.as_cid().as_bytes()).await? else {
            return Ok(None);
        };
        Ok(Some(
            decode_table_snapshot_locator(&bytes)?.indexed_snapshot_id,
        ))
    }

    async fn transaction_required_table_snapshot_manifests(
        &self,
        tx: &AsyncProllyTransaction<'_, S>,
        table_id: &TableId,
        versions: &[MapVersionId],
    ) -> Result<Vec<TableSnapshotManifestRecord>> {
        if versions.is_empty() {
            return Ok(Vec::new());
        }
        let name = Self::table_snapshot_catalog_root_name(table_id);
        let tree = tx
            .load_named_root(&name)
            .await?
            .ok_or_else(|| Error::CorruptData("table snapshot catalog is absent".into()))?;
        let keys = versions
            .iter()
            .map(|version| version.as_cid().as_bytes())
            .collect::<Vec<_>>();
        let locators = tx.get_many(&tree, &keys).await?;
        let detached = versions
            .iter()
            .zip(locators)
            .map(|(version, locator)| {
                let locator = locator.ok_or_else(|| {
                    Error::CorruptData(format!("base version {version} has no snapshot manifest"))
                })?;
                decode_table_snapshot_locator(&locator)
            })
            .collect::<Result<Vec<_>>>()?;
        let detached_trees = detached
            .iter()
            .map(|locator| locator.manifest_tree.clone())
            .collect::<Vec<_>>();
        // The catalog root above is transaction-pinned. Its detached targets are
        // immutable content-addressed nodes, so loading those targets through the
        // base engine preserves the transaction's logical read snapshot while
        // retaining the provider's ordered batch-read path.
        let records = self
            .engine
            .load_single_leaf_values_ordered(&detached_trees, TABLE_SNAPSHOT_RECORD_KEY)
            .await?;
        versions
            .iter()
            .zip(detached)
            .zip(records)
            .map(|((version, locator), bytes)| {
                let record = decode_table_snapshot_manifest(&bytes)?;
                if record.table_id != *table_id
                    || record.base_version != *version
                    || record.indexed.snapshot_id != locator.indexed_snapshot_id
                {
                    return Err(Error::CorruptData(
                        "transaction-visible snapshot manifest identity mismatch".into(),
                    ));
                }
                Ok(record)
            })
            .collect()
    }

    async fn stage_commit_records(
        &self,
        tx: &AsyncProllyTransaction<'_, S>,
        descriptions: &BTreeMap<String, TableDescription>,
        result: &TransactWriteResult,
        committed_at_millis: u64,
    ) -> Result<()> {
        let encoded_commit = encode_commit_result(result)?;
        let commit_catalog = tx
            .load_named_root(COMMIT_CATALOG_ROOT_NAME)
            .await?
            .unwrap_or_else(|| tx.create());
        if tx
            .get(&commit_catalog, &result.commit_id.0)
            .await?
            .is_some()
        {
            return Err(Error::CorruptData(format!(
                "generated commit ID {} already exists",
                result.commit_id
            )));
        }
        let next = tx
            .batch(
                &commit_catalog,
                vec![Mutation::Upsert {
                    key: result.commit_id.0.to_vec(),
                    val: encoded_commit,
                }],
            )
            .await?;
        tx.publish_named_root_at_millis(COMMIT_CATALOG_ROOT_NAME, &next, committed_at_millis)
            .await?;
        let mut seen_tables = BTreeSet::new();
        for transition in &result.transitions {
            let description = descriptions.get(&transition.table_name).ok_or_else(|| {
                Error::CorruptData(format!(
                    "commit transition references unvalidated table {:?}",
                    transition.table_name
                ))
            })?;
            if !seen_tables.insert(description.id.clone()) {
                return Err(Error::CorruptData(
                    "commit contains duplicate table transitions".into(),
                ));
            }
            let log_name = Self::table_commit_log_root_name(&description.id);
            let log = tx
                .load_named_root(&log_name)
                .await?
                .unwrap_or_else(|| tx.create());
            let sequence = match tx.get(&log, COMMIT_SEQUENCE_KEY).await? {
                Some(bytes) => decode_commit_sequence(&bytes)?
                    .checked_add(1)
                    .ok_or_else(|| {
                        Error::Validation("table commit sequence exhausted u64".into())
                    })?,
                None => 1,
            };
            let record = TableCommit {
                commit_id: result.commit_id.clone(),
                sequence,
                committed_at_millis,
                transition: transition.clone(),
            };
            let mut record_key = vec![1];
            record_key.extend_from_slice(&sequence.to_be_bytes());
            let next = tx
                .batch(
                    &log,
                    vec![
                        Mutation::Upsert {
                            key: COMMIT_SEQUENCE_KEY.to_vec(),
                            val: sequence.to_be_bytes().to_vec(),
                        },
                        Mutation::Upsert {
                            key: record_key,
                            val: encode_table_commit_record(&record)?,
                        },
                    ],
                )
                .await?;
            tx.publish_named_root_at_millis(&log_name, &next, committed_at_millis)
                .await?;
        }
        Ok(())
    }

    /// Stage one logical table mutation together with its derived-index source
    /// mutation and durable historical pairing. `None` means that the indexed
    /// collection changed concurrently and the owning transaction must retry.
    ///
    /// Immutable nodes may be published before this method returns, but no
    /// base head, indexed head, pairing record, or audit record becomes visible
    /// until the caller commits `tx` successfully.
    async fn stage_indexed_table_mutations(
        &self,
        tx: &AsyncProllyTransaction<'_, S>,
        maps: &AsyncVersionedMapsTransaction<'_, '_, S>,
        description: &TableDescription,
        before: &MapVersionId,
        base_mutations: Vec<Mutation>,
        indexed_mutations: Vec<Mutation>,
    ) -> Result<Option<MapVersion>> {
        if base_mutations.len() != indexed_mutations.len() {
            return Err(Error::CorruptData(
                "base/index mutation batches have different cardinality".into(),
            ));
        }
        let introduced_blobs = blob_references_from_mutations(&base_mutations)?;

        let map_id = Self::table_map_id(&description.id);
        let indexed_source_id = Self::table_indexed_source_id(&description.id);
        let indexed_root_name = prolly::indexed_collection_root_name(&indexed_source_id)?;
        self.prefetch_table_write_roots(tx, description).await?;
        let current = maps.head(&map_id).await?.ok_or_else(|| {
            Error::CorruptData(format!("table {:?} has no head", description.name))
        })?;
        if current.id != *before {
            return Err(Error::CorruptData(
                "staged base-table head changed before indexed mutation".into(),
            ));
        }

        let indexed_snapshot_before = self
            .transaction_table_indexed_snapshot_id(tx, &description.id, before)
            .await?
            .ok_or_else(|| {
                Error::CorruptData("base version has no paired indexed source version".into())
            })?;
        let indexed_state_tree = tx
            .load_named_root(&indexed_root_name)
            .await?
            .ok_or_else(|| Error::CorruptData("indexed collection root is absent".into()))?;

        let after = maps.apply(&map_id, base_mutations).await?;
        match self
            .engine
            .prepare_indexed_apply_at_snapshot_with_policy(
                indexed_source_id,
                index_registry(description)?,
                table_index_policy(),
                indexed_state_tree,
                &indexed_snapshot_before,
                indexed_mutations,
                &MutationBudget::default(),
            )
            .await?
        {
            AsyncPreparedIndexedUpdate::Prepared(update) => {
                if !matches!(
                    tx.compare_and_swap_named_root_at_millis(
                        update.root_name(),
                        update.expected_state_tree(),
                        Some(update.candidate_state_tree()),
                        maps.timestamp_millis(),
                    )
                    .await?,
                    NamedRootUpdate::Applied
                ) {
                    return Ok(None);
                }
                self.stage_table_snapshot_manifest(
                    tx,
                    description,
                    &after.id,
                    update.manifest(),
                    maps.timestamp_millis(),
                )
                .await?;
                self.stage_table_blob_references(
                    tx,
                    &description.id,
                    introduced_blobs.values(),
                    maps.timestamp_millis(),
                )
                .await?;
            }
            AsyncPreparedIndexedUpdate::Unchanged { current } => {
                if *before != after.id {
                    return Err(Error::CorruptData(
                        "base table changed while its indexed source did not".into(),
                    ));
                }
                if current.snapshot_id != indexed_snapshot_before {
                    return Err(Error::CorruptData(
                        "unchanged indexed mutation selected another snapshot".into(),
                    ));
                }
            }
            AsyncPreparedIndexedUpdate::Conflict { .. } => return Ok(None),
        }
        Ok(Some(after))
    }

    /// Convert a verified base-table snapshot into the compact, pre-extracted
    /// source consumed by `AsyncIndexedMap`. Work is streamed and applied in
    /// deterministic bounded batches so import memory is independent of table
    /// cardinality.
    async fn build_index_source(
        &self,
        description: &TableDescription,
        base: &Tree,
        budget: &prolly::MaintenanceBudget,
    ) -> Result<Tree> {
        budget.validate().map_err(Error::Storage)?;
        let mut source = self.engine.create();
        let mut range = self
            .engine
            .range(base, b"", Some(TABLE_SCHEMA_RECORD_KEY))
            .await?;
        let mut batch = Vec::with_capacity(IMPORT_INDEX_BATCH_ITEMS);
        let mut batch_bytes = 0usize;
        let mut entries = 0usize;
        while let Some(entry) = range.next().await {
            let (key, stored_item) = entry?;
            entries = entries.checked_add(1).ok_or_else(|| {
                Error::Validation("import index source entry count overflow".into())
            })?;
            if entries > budget.max_source_entries {
                return Err(Error::Validation(format!(
                    "import index source exceeds {} entries",
                    budget.max_source_entries
                )));
            }
            let item = decode_item(&self.blobs.resolve(&stored_item).await?)?;
            let logical_key = key_from_item(description, &item)?;
            if encode_primary_key(description, &logical_key)? != key {
                return Err(Error::CorruptData(
                    "imported item does not match its canonical primary key".into(),
                ));
            }
            let indexed =
                prepare_index_source_record(description, &item, stored_item, &self.blobs).await?;
            batch_bytes = batch_bytes
                .checked_add(key.len())
                .and_then(|bytes| bytes.checked_add(indexed.len()))
                .ok_or_else(|| Error::Validation("import index batch size overflow".into()))?;
            batch.push(Mutation::Upsert { key, val: indexed });
            if batch.len() >= IMPORT_INDEX_BATCH_ITEMS || batch_bytes >= IMPORT_INDEX_BATCH_BYTES {
                source = self
                    .engine
                    .batch(&source, std::mem::take(&mut batch))
                    .await?;
                batch_bytes = 0;
            }
        }
        if !batch.is_empty() {
            source = self.engine.batch(&source, batch).await?;
        }
        Ok(source)
    }

    async fn stage_single_table_commit(
        &self,
        tx: &AsyncProllyTransaction<'_, S>,
        description: &TableDescription,
        commit_id: CommitId,
        before: Option<MapVersionId>,
        after: Option<MapVersionId>,
        committed_at_millis: u64,
    ) -> Result<TransactWriteResult> {
        let applied = before != after;
        let result = TransactWriteResult {
            commit_id,
            transitions: vec![TransactionTableTransition {
                table_name: description.name.clone(),
                table_id: description.id.clone(),
                before,
                after: after.clone(),
                applied,
            }],
            table_versions: after
                .into_iter()
                .map(|after| (description.name.clone(), after))
                .collect(),
        };
        self.stage_commit_records(
            tx,
            &BTreeMap::from([(description.name.clone(), description.clone())]),
            &result,
            committed_at_millis,
        )
        .await?;
        Ok(result)
    }

    async fn lifecycle_result_from_commit(
        &self,
        expected_name: &str,
        result: TransactWriteResult,
    ) -> Result<TableLifecycleResult> {
        let transition = single_table_transition(expected_name, &result)?.clone();
        let description = self.description_for_transition(&transition).await?;
        if description.name != expected_name {
            return Err(Error::CorruptData(
                "lifecycle commit resolved another table name".into(),
            ));
        }
        Ok(TableLifecycleResult {
            description,
            commit_id: result.commit_id,
            transition,
        })
    }

    /// Validate an archive and produce a read-only, content-addressed import
    /// plan for a fresh target table incarnation.
    pub async fn plan_import(
        &self,
        archive: &TableArchive,
        target_table_name: impl Into<String>,
        limits: TableArchiveLimits,
    ) -> Result<ImportPlan> {
        let target_table_name = target_table_name.into();
        let summary = archive.verify(limits)?;
        let required_database_format = self.format_record()?.encode();
        if archive.database_format.encode() != required_database_format {
            return Err(Error::FormatMismatch(
                "archive database format differs from the destination namespace".into(),
            ));
        }
        let candidate = TableDescription {
            name: target_table_name.clone(),
            id: TableId([0; 32]),
            partition_key: archive.source.partition_key.clone(),
            sort_key: archive.source.sort_key.clone(),
            attribute_definitions: archive.source.attribute_definitions.clone(),
            secondary_indexes: archive.source.secondary_indexes.clone(),
            status: TableStatus::Active,
            created_at_millis: 0,
        };
        candidate.validate()?;
        if self
            .engine
            .versioned_map(CATALOG_MAP_ID)
            .get(target_table_name.as_bytes())
            .await?
            .is_some()
        {
            return Err(Error::TableAlreadyExists(target_table_name));
        }
        let mut plan = ImportPlan {
            id: ImportPlanId([0; 32]),
            target_table_name,
            target_table_id: self.ids.generate()?,
            archive_digest: summary.archive_digest.0,
            source_table_name: archive.source.name.clone(),
            source_table_id: archive.source.id.clone(),
            source_version: archive.version.clone(),
            required_database_format,
            planned_at_millis: self.clock.now_millis(),
        };
        plan.id = import_plan_id(&plan)?;
        Ok(plan)
    }

    /// Publish a fully verified archive according to an exact dry-run plan.
    /// Immutable objects are prepublished; all logical visibility and audit
    /// state changes atomically in one strict root transaction.
    pub async fn apply_import(
        &self,
        archive: &TableArchive,
        plan: &ImportPlan,
        context: MaintenanceContext,
        limits: TableArchiveLimits,
    ) -> Result<ImportResult> {
        context.validate()?;
        validate_import_plan(plan)?;
        let summary = archive.verify(limits)?;
        if summary.archive_digest.as_bytes() != plan.archive_digest
            || archive.source.name != plan.source_table_name
            || archive.source.id != plan.source_table_id
            || archive.version != plan.source_version
        {
            return Err(Error::IdempotentParameterMismatch);
        }
        let destination_format = self.format_record()?.encode();
        if plan.required_database_format != destination_format
            || archive.database_format.encode() != destination_format
        {
            return Err(Error::FormatMismatch(
                "import plan/archive format differs from the destination namespace".into(),
            ));
        }
        if let Some(record) = self.import_audit(&plan.id).await? {
            if record.plan != *plan || record.context != context {
                return Err(Error::IdempotentParameterMismatch);
            }
            return Ok(import_result_from_audit(record, true));
        }
        if self
            .engine
            .versioned_map(CATALOG_MAP_ID)
            .get(plan.target_table_name.as_bytes())
            .await?
            .is_some()
        {
            return Err(Error::ImportPlanStale(
                "target table name is no longer absent".into(),
            ));
        }

        for blob in &archive.blobs {
            self.blobs
                .put_verified(&blob.reference, &blob.bytes)
                .await?;
        }
        let imported_tree = self.engine.import_snapshot(&archive.snapshot).await?;
        if MapVersionId::for_tree(&imported_tree)? != archive.version {
            return Err(Error::CorruptData(
                "imported snapshot changed table version identity".into(),
            ));
        }

        let completed_at_millis = self.clock.now_millis();
        let description = TableDescription {
            name: plan.target_table_name.clone(),
            id: plan.target_table_id.clone(),
            partition_key: archive.source.partition_key.clone(),
            sort_key: archive.source.sort_key.clone(),
            attribute_definitions: archive.source.attribute_definitions.clone(),
            secondary_indexes: archive.source.secondary_indexes.clone(),
            status: TableStatus::Active,
            created_at_millis: completed_at_millis,
        };
        description.validate()?;
        let imported_schema = self
            .engine
            .get(&imported_tree, TABLE_SCHEMA_RECORD_KEY)
            .await?
            .ok_or_else(|| {
                Error::CorruptData("imported table has no schema-version record".into())
            })?;
        if imported_schema != encode_table_schema_record(&description)? {
            return Err(Error::CorruptData(
                "imported table schema-version record disagrees with its descriptor".into(),
            ));
        }
        let index_budget = prolly::MaintenanceBudget::default();
        let index_source = self
            .build_index_source(&description, &imported_tree, &index_budget)
            .await?;
        let prepared_indexes = self
            .engine
            .prepare_indexed_map_from_source_with_policy(
                Self::table_indexed_source_id(&description.id),
                index_registry(&description)?,
                index_source,
                &index_budget,
                table_index_policy(),
            )
            .await?;
        let encoded_description = encode_description(&description)?;
        let commit_id = CommitId(self.ids.generate()?.0);
        let tx = self.engine.begin_transaction()?;
        let maps = tx.versioned_maps_at_millis(completed_at_millis);
        self.ensure_writes_unfenced(&tx, &maps).await?;
        if let Some(bytes) = maps.get(IMPORT_AUDIT_MAP_ID, &plan.id.0).await? {
            let record = decode_import_audit_record(&bytes)?;
            tx.rollback();
            if record.plan != *plan || record.context != context {
                return Err(Error::IdempotentParameterMismatch);
            }
            return Ok(import_result_from_audit(record, true));
        }
        if maps
            .get(CATALOG_MAP_ID, plan.target_table_name.as_bytes())
            .await?
            .is_some()
        {
            tx.rollback();
            return Err(Error::ImportPlanStale(
                "target table name was claimed after planning".into(),
            ));
        }
        if maps
            .get(TABLE_DESCRIPTOR_MAP_ID, &plan.target_table_id.0)
            .await?
            .is_some()
        {
            tx.rollback();
            return Err(Error::ImportPlanStale(
                "target table incarnation ID already exists".into(),
            ));
        }

        let table_map = self
            .engine
            .versioned_map(Self::table_map_id(&plan.target_table_id));
        if maps.head(table_map.id()).await?.is_some() {
            tx.rollback();
            return Err(Error::ImportPlanStale(
                "target table incarnation already has a head".into(),
            ));
        }
        let version_root = table_map.version_root_name(&archive.version);
        if tx.load_named_root(&version_root).await?.is_some() {
            tx.rollback();
            return Err(Error::ImportPlanStale(
                "target table incarnation already has a version root".into(),
            ));
        }
        if !matches!(
            tx.compare_and_swap_named_root_at_millis(
                prepared_indexes.root_name(),
                prepared_indexes.expected_state_tree(),
                Some(prepared_indexes.candidate_state_tree()),
                completed_at_millis,
            )
            .await?,
            NamedRootUpdate::Applied
        ) {
            tx.rollback();
            return Err(Error::ImportPlanStale(
                "target indexed collection was claimed after planning".into(),
            ));
        }
        tx.publish_named_root_at_millis(&version_root, &imported_tree, completed_at_millis)
            .await?;
        tx.publish_named_root_at_millis(table_map.head_name(), &imported_tree, completed_at_millis)
            .await?;
        self.stage_table_blob_references(
            &tx,
            &description.id,
            archive.blobs.iter().map(|blob| &blob.reference),
            completed_at_millis,
        )
        .await?;
        self.stage_table_snapshot_manifest(
            &tx,
            &description,
            &archive.version,
            prepared_indexes.manifest(),
            completed_at_millis,
        )
        .await?;
        maps.put(
            CATALOG_MAP_ID,
            description.name.as_bytes(),
            encoded_description.clone(),
        )
        .await?;
        maps.put(
            TABLE_DESCRIPTOR_MAP_ID,
            description.id.0.to_vec(),
            encoded_description,
        )
        .await?;
        let commit = self
            .stage_single_table_commit(
                &tx,
                &description,
                commit_id.clone(),
                None,
                Some(archive.version.clone()),
                completed_at_millis,
            )
            .await?;
        let audit = ImportAuditRecord {
            plan: plan.clone(),
            context,
            description: description.clone(),
            commit_id,
            completed_at_millis,
        };
        maps.apply(
            IMPORT_AUDIT_MAP_ID,
            vec![Mutation::Upsert {
                key: plan.id.0.to_vec(),
                val: encode_import_audit_record(&audit)?,
            }],
        )
        .await?;

        match tx.commit().await {
            Ok(TransactionUpdate::Applied { .. }) => {
                debug_assert_eq!(commit.commit_id, audit.commit_id);
                Ok(import_result_from_audit(audit, false))
            }
            Ok(TransactionUpdate::Conflict(_)) => Err(Error::ImportPlanStale(
                "import transaction conflicted with concurrent state".into(),
            )),
            Err(source) => match self.import_audit(&plan.id).await {
                Ok(Some(stored)) if stored == audit => Ok(import_result_from_audit(stored, true)),
                _ => Err(Error::Storage(source)),
            },
        }
    }

    /// Resolve durable operator evidence for an import plan.
    pub async fn import_audit(&self, id: &ImportPlanId) -> Result<Option<ImportAuditRecord>> {
        self.engine
            .versioned_map(IMPORT_AUDIT_MAP_ID)
            .get(&id.0)
            .await?
            .map(|bytes| decode_import_audit_record(&bytes))
            .transpose()
    }

    /// Create one active table and its empty initial version atomically.
    pub async fn create_table(
        &self,
        name: impl Into<String>,
        partition_key: crate::KeyAttribute,
        sort_key: Option<crate::KeyAttribute>,
    ) -> Result<TableDescription> {
        Ok(self
            .create_table_result(name, partition_key, sort_key)
            .await?
            .description)
    }

    /// Create the catalog entry, initial table state, and lifecycle commit in
    /// one strict root transaction.
    pub async fn create_table_result(
        &self,
        name: impl Into<String>,
        partition_key: crate::KeyAttribute,
        sort_key: Option<crate::KeyAttribute>,
    ) -> Result<TableLifecycleResult> {
        let attribute_definitions = table_key_definitions(&partition_key, sort_key.as_ref());
        self.create_table_result_with_token(
            name,
            partition_key,
            sort_key,
            attribute_definitions,
            Vec::new(),
            None,
        )
        .await
    }

    /// Idempotent table creation extension. Replay resolves the original
    /// immutable descriptor and creation commit rather than allocating a new
    /// incarnation.
    pub async fn create_table_idempotent_result(
        &self,
        name: impl Into<String>,
        partition_key: crate::KeyAttribute,
        sort_key: Option<crate::KeyAttribute>,
        request_token: &str,
    ) -> Result<TableLifecycleResult> {
        let attribute_definitions = table_key_definitions(&partition_key, sort_key.as_ref());
        self.create_table_result_with_token(
            name,
            partition_key,
            sort_key,
            attribute_definitions,
            Vec::new(),
            Some(request_token),
        )
        .await
    }

    /// Create a table with its complete LSI/GSI schema and empty synchronous
    /// indexed collection in one strict visibility transaction.
    pub async fn create_table_with_indexes_result(
        &self,
        name: impl Into<String>,
        partition_key: crate::KeyAttribute,
        sort_key: Option<crate::KeyAttribute>,
        attribute_definitions: BTreeMap<String, crate::KeyKind>,
        secondary_indexes: Vec<SecondaryIndexDefinition>,
        request_token: Option<&str>,
    ) -> Result<TableLifecycleResult> {
        self.create_table_result_with_token(
            name,
            partition_key,
            sort_key,
            attribute_definitions,
            secondary_indexes,
            request_token,
        )
        .await
    }

    async fn create_table_result_with_token(
        &self,
        name: impl Into<String>,
        partition_key: crate::KeyAttribute,
        sort_key: Option<crate::KeyAttribute>,
        attribute_definitions: BTreeMap<String, crate::KeyKind>,
        mut secondary_indexes: Vec<SecondaryIndexDefinition>,
        request_token: Option<&str>,
    ) -> Result<TableLifecycleResult> {
        validate_client_request_token(request_token)?;
        let name = name.into();
        secondary_indexes.sort_by(|left, right| left.name.cmp(&right.name));
        let fingerprint = canonical_fingerprint(
            b"DDB-CreateTable-extension-v2-indexes",
            &(
                name.as_str(),
                &partition_key,
                &sort_key,
                &attribute_definitions,
                &secondary_indexes,
            ),
        )?;
        let idempotency_key = request_token.map(idempotency_key);
        let table_id = self.ids.generate()?;
        let secondary_indexes = secondary_indexes
            .into_iter()
            .map(|definition| {
                let id = SecondaryIndexId(canonical_fingerprint(
                    b"DDB-SecondaryIndexId-v1",
                    &(&table_id, definition.name.as_str(), 1_u64),
                )?);
                Ok(SecondaryIndexDescription {
                    name: definition.name,
                    id,
                    generation: 1,
                    kind: definition.kind,
                    partition_key: definition.partition_key,
                    sort_key: definition.sort_key,
                    projection: definition.projection,
                    status: SecondaryIndexStatus::Active,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let description = TableDescription {
            name,
            id: table_id,
            attribute_definitions,
            partition_key,
            sort_key,
            secondary_indexes,
            status: TableStatus::Active,
            created_at_millis: self.clock.now_millis(),
        };
        let commit_id = CommitId(self.ids.generate()?.0);
        description.validate()?;
        let encoded = encode_description(&description)?;

        for _ in 0..=self.logical_retry_limit {
            let committed_at_millis = self.clock.now_millis();
            let tx = self.engine.begin_transaction()?;
            let maps = tx.versioned_maps_at_millis(committed_at_millis);
            self.prefetch_transaction_global_roots(&tx).await?;
            if let Some(key) = &idempotency_key {
                if let Some(bytes) = maps.get(IDEMPOTENCY_MAP_ID, key).await? {
                    let record = decode_idempotency_record(&bytes)?;
                    if committed_at_millis.saturating_sub(record.completed_at_millis)
                        <= IDEMPOTENCY_WINDOW_MILLIS
                    {
                        tx.rollback();
                        if record.fingerprint != fingerprint {
                            return Err(Error::IdempotentParameterMismatch);
                        }
                        return self
                            .lifecycle_result_from_commit(&description.name, record.result)
                            .await;
                    }
                }
            }
            self.ensure_writes_unfenced(&tx, &maps).await?;
            if maps
                .get(CATALOG_MAP_ID, description.name.as_bytes())
                .await?
                .is_some()
            {
                tx.rollback();
                return Err(Error::TableAlreadyExists(description.name));
            }
            if maps
                .get(TABLE_DESCRIPTOR_MAP_ID, &description.id.0)
                .await?
                .is_some()
            {
                return Err(Error::Random(
                    "generated table incarnation ID already exists".into(),
                ));
            }
            let prepared_indexes = self
                .engine
                .prepare_indexed_map_with_policy(
                    Self::table_indexed_source_id(&description.id),
                    index_registry(&description)?,
                    table_index_policy(),
                )
                .await?;
            if !matches!(
                tx.compare_and_swap_named_root_at_millis(
                    prepared_indexes.root_name(),
                    prepared_indexes.expected_state_tree(),
                    Some(prepared_indexes.candidate_state_tree()),
                    committed_at_millis,
                )
                .await?,
                NamedRootUpdate::Applied
            ) {
                tx.rollback();
                continue;
            }
            maps.put(CATALOG_MAP_ID, description.name.as_bytes(), encoded.clone())
                .await?;
            maps.put(
                TABLE_DESCRIPTOR_MAP_ID,
                description.id.0.to_vec(),
                encoded.clone(),
            )
            .await?;
            let initial = maps
                .apply(
                    Self::table_map_id(&description.id),
                    vec![Mutation::Upsert {
                        key: TABLE_SCHEMA_RECORD_KEY.to_vec(),
                        val: encode_table_schema_record(&description)?,
                    }],
                )
                .await?;
            self.stage_table_snapshot_manifest(
                &tx,
                &description,
                &initial.id,
                prepared_indexes.manifest(),
                committed_at_millis,
            )
            .await?;
            let commit = self
                .stage_single_table_commit(
                    &tx,
                    &description,
                    commit_id.clone(),
                    None,
                    Some(initial.id),
                    committed_at_millis,
                )
                .await?;
            if let Some(key) = &idempotency_key {
                maps.apply(
                    IDEMPOTENCY_MAP_ID,
                    vec![Mutation::Upsert {
                        key: key.clone(),
                        val: encode_idempotency_record(&IdempotencyRecord {
                            fingerprint,
                            completed_at_millis: committed_at_millis,
                            result: commit.clone(),
                        })?,
                    }],
                )
                .await?;
            }
            match tx.commit().await {
                Ok(TransactionUpdate::Applied { .. }) => {
                    return self
                        .lifecycle_result_from_commit(&description.name, commit)
                        .await
                }
                Ok(TransactionUpdate::Conflict(_)) => continue,
                Err(source) => {
                    if let Some(key) = &idempotency_key {
                        match self
                            .reconcile_idempotency_record(
                                key,
                                &fingerprint,
                                self.clock.now_millis(),
                            )
                            .await
                        {
                            Ok(Some(result)) => {
                                return self
                                    .lifecycle_result_from_commit(&description.name, result)
                                    .await
                            }
                            Ok(None) | Err(Error::Storage(_)) => {}
                            Err(error) => return Err(error),
                        }
                    }
                    return Err(Error::Storage(source));
                }
            }
        }
        Err(Error::ConflictExhausted)
    }

    pub async fn describe_table(&self, name: &str) -> Result<TableDescription> {
        let catalog = self.engine.versioned_map(CATALOG_MAP_ID);
        let bytes = catalog
            .get(name.as_bytes())
            .await?
            .ok_or_else(|| Error::TableNotFound(name.to_string()))?;
        let description = decode_description(&bytes)?;
        if description.status != TableStatus::Active {
            return Err(Error::TableNotActive(name.to_string()));
        }
        Ok(description)
    }

    /// Inspect the canonical source/index closure for one table incarnation.
    pub async fn index_health(&self, name: &str) -> Result<prolly::IndexedMapHealth> {
        let description = self.describe_table(name).await?;
        let indexed = self
            .engine
            .indexed_map_with_policy(
                Self::table_indexed_source_id(&description.id),
                index_registry(&description)?,
                table_index_policy(),
            )
            .await?;
        Ok(indexed.health().await?)
    }

    /// Semantically rebuild and compare every active index at the current
    /// paired source version under the engine's finite maintenance budget.
    pub async fn verify_indexes(&self, name: &str) -> Result<Vec<prolly::IndexVerification>> {
        let description = self.describe_table(name).await?;
        let indexed = self
            .engine
            .indexed_map_with_policy(
                Self::table_indexed_source_id(&description.id),
                index_registry(&description)?,
                table_index_policy(),
            )
            .await?;
        let version = indexed.snapshot().await?.source_version().clone();
        Ok(indexed.verify_all(&version).await?)
    }

    /// Query a current or historical secondary index through the exact indexed
    /// snapshot paired with the requested base-table version.
    pub async fn query_index(
        &self,
        table: &str,
        index_name: &str,
        request: IndexQueryRequest<'_>,
    ) -> Result<IndexReadPage> {
        let IndexQueryRequest {
            base_version,
            condition,
            exclusive_start_key,
            limit,
            scan_forward,
        } = request;
        if limit == 0 || limit > 4_096 {
            return Err(Error::Validation(
                "secondary-index page limit must be 1..=4096".into(),
            ));
        }
        let current_description = self.describe_table(table).await?;
        let base_version = match base_version {
            Some(version) => version.clone(),
            None => self.head(table).await?.id,
        };
        let description = self
            .schema_at_version(&current_description.id, &base_version)
            .await?;
        if description.name != table {
            return Err(Error::CorruptData(
                "historical schema descriptor has another table name".into(),
            ));
        }
        let index = description
            .secondary_indexes
            .iter()
            .find(|index| index.name == index_name)
            .ok_or_else(|| Error::Validation(format!("unknown index {index_name:?}")))?;
        if index.status != SecondaryIndexStatus::Active {
            return Err(Error::Validation(format!(
                "index {index_name:?} is not active"
            )));
        }
        let indexed = self
            .engine
            .indexed_map_with_policy(
                Self::table_indexed_source_id(&description.id),
                index_registry(&description)?,
                table_index_policy(),
            )
            .await?;
        let (tree, manifest) = self
            .table_snapshot_manifest_at_version(&description.id, &base_version)
            .await?
            .ok_or_else(|| {
                Error::Validation(format!(
                    "table version {base_version} has no retained index snapshot"
                ))
            })?;
        let indexed_snapshot_id = manifest.indexed.snapshot_id.clone();
        let snapshot = indexed.snapshot_from_manifest(tree, manifest.indexed)?;
        let indexed_source_version = snapshot.source_version().clone();
        let selected = snapshot.index(index_name.as_bytes())?;
        let bounds = index_condition_bounds(index, condition)?;
        let direction = if scan_forward {
            prolly::SecondaryIndexDirection::Forward
        } else {
            prolly::SecondaryIndexDirection::Reverse
        };
        let cursor = exclusive_start_key
            .map(|start| {
                let primary_key =
                    encode_primary_key(&description, &key_from_item(&description, start)?)?;
                let term = crate::index::index_term(index, start)?.ok_or_else(|| {
                    Error::Validation(
                        "ExclusiveStartKey is missing a secondary-index key attribute".into(),
                    )
                })?;
                match &bounds {
                    IndexQueryBounds::Exact(query_term) => {
                        selected.exact_cursor_after(query_term, &term, &primary_key, direction)
                    }
                    IndexQueryBounds::Prefix(prefix) => {
                        selected.prefix_cursor_after(prefix, &term, &primary_key, direction)
                    }
                    IndexQueryBounds::Range(start, end) => selected.range_cursor_after(
                        start,
                        end.as_deref(),
                        &term,
                        &primary_key,
                        direction,
                    ),
                }
                .map_err(Error::Storage)
            })
            .transpose()?;
        let page = match (bounds, scan_forward) {
            (IndexQueryBounds::Exact(term), true) => {
                selected.exact_page(&term, cursor.as_ref(), limit).await?
            }
            (IndexQueryBounds::Exact(term), false) => {
                selected
                    .exact_reverse_page(&term, cursor.as_ref(), limit)
                    .await?
            }
            (IndexQueryBounds::Prefix(prefix), true) => {
                selected
                    .prefix_page(&prefix, cursor.as_ref(), limit)
                    .await?
            }
            (IndexQueryBounds::Prefix(prefix), false) => {
                selected
                    .prefix_reverse_page(&prefix, cursor.as_ref(), limit)
                    .await?
            }
            (IndexQueryBounds::Range(start, end), true) => {
                selected
                    .range_page(&start, end.as_deref(), cursor.as_ref(), limit)
                    .await?
            }
            (IndexQueryBounds::Range(start, end), false) => {
                selected
                    .range_reverse_page(&start, end.as_deref(), cursor.as_ref(), limit)
                    .await?
            }
        };
        let mut has_more = page.next_cursor.is_some();
        let mut items = Vec::with_capacity(page.matches.len());
        let mut logical_bytes = 0usize;
        for matched in page.matches {
            let stored = match &index.projection {
                crate::SecondaryIndexProjection::KeysOnly => {
                    let source = self
                        .engine
                        .get(snapshot.source_tree(), &matched.primary_key)
                        .await?
                        .ok_or_else(|| {
                            Error::CorruptData(
                                "secondary index references a missing source record".into(),
                            )
                        })?;
                    crate::index::stored_item_from_index_source(&source)?
                }
                crate::SecondaryIndexProjection::Include(_)
                | crate::SecondaryIndexProjection::All => matched.projection.ok_or_else(|| {
                    Error::CorruptData(
                        "secondary-index projection is absent from a projected index".into(),
                    )
                })?,
            };
            let item = decode_item(&self.blobs.resolve(&stored).await?)?;
            let item = match &index.projection {
                crate::SecondaryIndexProjection::KeysOnly => {
                    project_index_keys(&description, index, &item)
                }
                crate::SecondaryIndexProjection::Include(_)
                | crate::SecondaryIndexProjection::All => item,
            };
            let bytes = item_size(&item)?;
            if logical_bytes
                .checked_add(bytes)
                .is_none_or(|total| total > MAX_READ_PAGE_BYTES)
            {
                has_more = true;
                break;
            }
            logical_bytes += bytes;
            items.push(item);
        }
        let last_evaluated_key = has_more
            .then(|| items.last())
            .flatten()
            .map(|item| project_index_keys(&description, index, item));
        Ok(IndexReadPage {
            items,
            last_evaluated_key,
            base_version_id: base_version,
            indexed_source_version_id: indexed_source_version,
            indexed_snapshot_id,
        })
    }

    /// Scan a current or historical secondary index in canonical index order.
    pub async fn scan_index(
        &self,
        table: &str,
        index_name: &str,
        base_version: Option<&MapVersionId>,
        exclusive_start_key: Option<&Item>,
        limit: usize,
    ) -> Result<IndexReadPage> {
        if limit == 0 || limit > 4_096 {
            return Err(Error::Validation(
                "secondary-index page limit must be 1..=4096".into(),
            ));
        }
        let current_description = self.describe_table(table).await?;
        let base_version = match base_version {
            Some(version) => version.clone(),
            None => self.head(table).await?.id,
        };
        let description = self
            .schema_at_version(&current_description.id, &base_version)
            .await?;
        if description.name != table {
            return Err(Error::CorruptData(
                "historical schema descriptor has another table name".into(),
            ));
        }
        let index = description
            .secondary_indexes
            .iter()
            .find(|index| index.name == index_name)
            .ok_or_else(|| Error::Validation(format!("unknown index {index_name:?}")))?;
        if index.status != SecondaryIndexStatus::Active {
            return Err(Error::Validation(format!(
                "index {index_name:?} is not active"
            )));
        }
        let indexed = self
            .engine
            .indexed_map_with_policy(
                Self::table_indexed_source_id(&description.id),
                index_registry(&description)?,
                table_index_policy(),
            )
            .await?;
        let (tree, manifest) = self
            .table_snapshot_manifest_at_version(&description.id, &base_version)
            .await?
            .ok_or_else(|| {
                Error::Validation(format!(
                    "table version {base_version} has no retained index snapshot"
                ))
            })?;
        let indexed_snapshot_id = manifest.indexed.snapshot_id.clone();
        let snapshot = indexed.snapshot_from_manifest(tree, manifest.indexed)?;
        let indexed_source_version = snapshot.source_version().clone();
        let selected = snapshot.index(index_name.as_bytes())?;
        let cursor = exclusive_start_key
            .map(|start| {
                let primary_key =
                    encode_primary_key(&description, &key_from_item(&description, start)?)?;
                let term = crate::index::index_term(index, start)?.ok_or_else(|| {
                    Error::Validation(
                        "ExclusiveStartKey is missing a secondary-index key attribute".into(),
                    )
                })?;
                selected
                    .range_cursor_after(
                        b"",
                        None,
                        &term,
                        &primary_key,
                        prolly::SecondaryIndexDirection::Forward,
                    )
                    .map_err(Error::Storage)
            })
            .transpose()?;
        let page = selected
            .range_page(b"", None, cursor.as_ref(), limit)
            .await?;
        let mut has_more = page.next_cursor.is_some();
        let mut items = Vec::with_capacity(page.matches.len());
        let mut logical_bytes = 0usize;
        for matched in page.matches {
            let stored = match &index.projection {
                crate::SecondaryIndexProjection::KeysOnly => {
                    let source = self
                        .engine
                        .get(snapshot.source_tree(), &matched.primary_key)
                        .await?
                        .ok_or_else(|| {
                            Error::CorruptData(
                                "secondary index references a missing source record".into(),
                            )
                        })?;
                    crate::index::stored_item_from_index_source(&source)?
                }
                crate::SecondaryIndexProjection::Include(_)
                | crate::SecondaryIndexProjection::All => matched.projection.ok_or_else(|| {
                    Error::CorruptData(
                        "secondary-index projection is absent from a projected index".into(),
                    )
                })?,
            };
            let item = decode_item(&self.blobs.resolve(&stored).await?)?;
            let item = match &index.projection {
                crate::SecondaryIndexProjection::KeysOnly => {
                    project_index_keys(&description, index, &item)
                }
                crate::SecondaryIndexProjection::Include(_)
                | crate::SecondaryIndexProjection::All => item,
            };
            let bytes = item_size(&item)?;
            if logical_bytes
                .checked_add(bytes)
                .is_none_or(|total| total > MAX_READ_PAGE_BYTES)
            {
                has_more = true;
                break;
            }
            logical_bytes += bytes;
            items.push(item);
        }
        let last_evaluated_key = has_more
            .then(|| items.last())
            .flatten()
            .map(|item| project_index_keys(&description, index, item));
        Ok(IndexReadPage {
            items,
            last_evaluated_key,
            base_version_id: base_version,
            indexed_source_version_id: indexed_source_version,
            indexed_snapshot_id,
        })
    }

    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let catalog = self.engine.versioned_map(CATALOG_MAP_ID);
        let Some(snapshot) = catalog.snapshot().await? else {
            return Ok(Vec::new());
        };
        let mut range = snapshot.range(&[], None).await?;
        let mut names = Vec::new();
        while let Some(entry) = range.next().await {
            let (name, encoded) = entry?;
            let description = decode_description(&encoded)?;
            if description.status == TableStatus::Active {
                names.push(String::from_utf8(name).map_err(|error| {
                    Error::CorruptData(format!("catalog contains non-UTF-8 table name: {error}"))
                })?);
            }
        }
        Ok(names)
    }

    /// Delete the name-to-incarnation fence. Historical roots remain isolated
    /// under the old random table ID and cannot become visible after recreate.
    pub async fn delete_table(&self, name: &str) -> Result<TableDescription> {
        Ok(self.delete_table_result(name).await?.description)
    }

    /// Remove the active name fence and record the final table state in one
    /// strict root transaction. Historical content remains immutable.
    pub async fn delete_table_result(&self, name: &str) -> Result<TableLifecycleResult> {
        self.delete_table_result_with_token(name, None).await
    }

    /// Idempotent logical table deletion extension.
    pub async fn delete_table_idempotent_result(
        &self,
        name: &str,
        request_token: &str,
    ) -> Result<TableLifecycleResult> {
        self.delete_table_result_with_token(name, Some(request_token))
            .await
    }

    async fn delete_table_result_with_token(
        &self,
        name: &str,
        request_token: Option<&str>,
    ) -> Result<TableLifecycleResult> {
        validate_client_request_token(request_token)?;
        let fingerprint = canonical_fingerprint(b"DDB-DeleteTable-extension-v1", &name)?;
        let idempotency_key = request_token.map(idempotency_key);
        let commit_id = CommitId(self.ids.generate()?.0);
        for _ in 0..=self.logical_retry_limit {
            let committed_at_millis = self.clock.now_millis();
            let tx = self.engine.begin_transaction()?;
            let maps = tx.versioned_maps_at_millis(committed_at_millis);
            self.prefetch_transaction_global_roots(&tx).await?;
            if let Some(key) = &idempotency_key {
                if let Some(bytes) = maps.get(IDEMPOTENCY_MAP_ID, key).await? {
                    let record = decode_idempotency_record(&bytes)?;
                    if committed_at_millis.saturating_sub(record.completed_at_millis)
                        <= IDEMPOTENCY_WINDOW_MILLIS
                    {
                        tx.rollback();
                        if record.fingerprint != fingerprint {
                            return Err(Error::IdempotentParameterMismatch);
                        }
                        return self.lifecycle_result_from_commit(name, record.result).await;
                    }
                }
            }
            self.ensure_writes_unfenced(&tx, &maps).await?;
            let bytes = maps
                .get(CATALOG_MAP_ID, name.as_bytes())
                .await?
                .ok_or_else(|| Error::TableNotFound(name.to_string()))?;
            let description = decode_description(&bytes)?;
            let map_id = Self::table_map_id(&description.id);
            let head = maps
                .head(&map_id)
                .await?
                .ok_or_else(|| Error::CorruptData(format!("table {name:?} has no head")))?;
            let indexed = self
                .engine
                .indexed_map_with_policy(
                    Self::table_indexed_source_id(&description.id),
                    index_registry(&description)?,
                    table_index_policy(),
                )
                .await?;
            let indexed_head = indexed.snapshot().await?.id().clone();
            let paired = self
                .transaction_table_snapshot_manifest(&tx, &description.id, &head.id)
                .await?
                .ok_or_else(|| {
                    Error::CorruptData("deleted table head has no paired index snapshot".into())
                })?;
            if paired.indexed.snapshot_id != indexed_head {
                return Err(Error::CorruptData(
                    "deleted table base/index heads disagree".into(),
                ));
            }
            let indexed_root_name = prolly::indexed_collection_root_name(
                &Self::table_indexed_source_id(&description.id),
            )?;
            let indexed_state = tx
                .load_named_root(&indexed_root_name)
                .await?
                .ok_or_else(|| Error::CorruptData("indexed collection root is absent".into()))?;
            if !matches!(
                tx.compare_and_swap_named_root_at_millis(
                    &indexed_root_name,
                    Some(&indexed_state),
                    None,
                    committed_at_millis,
                )
                .await?,
                NamedRootUpdate::Applied
            ) {
                tx.rollback();
                continue;
            }
            maps.delete(CATALOG_MAP_ID, name.as_bytes()).await?;
            let commit = self
                .stage_single_table_commit(
                    &tx,
                    &description,
                    commit_id.clone(),
                    Some(head.id),
                    None,
                    committed_at_millis,
                )
                .await?;
            if let Some(key) = &idempotency_key {
                maps.apply(
                    IDEMPOTENCY_MAP_ID,
                    vec![Mutation::Upsert {
                        key: key.clone(),
                        val: encode_idempotency_record(&IdempotencyRecord {
                            fingerprint,
                            completed_at_millis: committed_at_millis,
                            result: commit.clone(),
                        })?,
                    }],
                )
                .await?;
            }
            match tx.commit().await {
                Ok(TransactionUpdate::Applied { .. }) => {
                    return self.lifecycle_result_from_commit(name, commit).await
                }
                Ok(TransactionUpdate::Conflict(_)) => continue,
                Err(source) => {
                    if let Some(key) = &idempotency_key {
                        match self
                            .reconcile_idempotency_record(
                                key,
                                &fingerprint,
                                self.clock.now_millis(),
                            )
                            .await
                        {
                            Ok(Some(result)) => {
                                return self.lifecycle_result_from_commit(name, result).await
                            }
                            Ok(None) | Err(Error::Storage(_)) => {}
                            Err(error) => return Err(error),
                        }
                    }
                    return Err(Error::Storage(source));
                }
            }
        }
        Err(Error::ConflictExhausted)
    }

    pub async fn get_item(&self, table: &str, key: &Item) -> Result<Option<Item>> {
        Ok(self.get_item_with_version(table, key).await?.item)
    }

    /// Read an item and return the exact immutable version used for the read.
    ///
    /// This must be used whenever version metadata is exposed. Reading the
    /// item and head separately can associate data with a later concurrent
    /// version.
    pub async fn get_item_with_version(&self, table: &str, key: &Item) -> Result<ItemRead> {
        let description = self.describe_table(table).await?;
        let key = encode_primary_key(&description, key)?;
        let map = self
            .engine
            .versioned_map(Self::table_map_id(&description.id));
        let snapshot = map
            .snapshot()
            .await?
            .ok_or_else(|| Error::CorruptData(format!("table {table:?} has no head")))?;
        let item = match snapshot.get(&key).await? {
            Some(bytes) => Some(decode_item(&self.blobs.resolve(&bytes).await?)?),
            None => None,
        };
        Ok(ItemRead {
            item,
            version_id: snapshot.version().id.clone(),
        })
    }

    pub async fn get_item_at(
        &self,
        table: &str,
        version: &MapVersionId,
        key: &Item,
    ) -> Result<Option<Item>> {
        let description = self.describe_table(table).await?;
        let key = encode_primary_key(&description, key)?;
        let map = self
            .engine
            .versioned_map(Self::table_map_id(&description.id));
        let snapshot = map
            .snapshot_at(version)
            .await?
            .ok_or_else(|| Error::Validation(format!("unknown table version {version}")))?;
        match snapshot.get(&key).await? {
            Some(bytes) => Ok(Some(decode_item(&self.blobs.resolve(&bytes).await?)?)),
            None => Ok(None),
        }
    }

    /// Read up to 100 keys using exactly one immutable snapshot per table.
    ///
    /// All table names, keys, projections, and duplicate canonical keys are
    /// validated before item-tree reads begin. Tables are processed in lexical
    /// order to make partial responses deterministic; item order follows the
    /// request even though DynamoDB callers must not rely on response order.
    pub async fn batch_get(
        &self,
        requests: BTreeMap<String, BatchGetTableRequest>,
    ) -> Result<BatchGetResult> {
        if requests.is_empty() {
            return Err(Error::Validation(
                "BatchGetItem.request_items must not be empty".into(),
            ));
        }
        let total_keys = requests.values().try_fold(0_usize, |total, request| {
            if request.keys.is_empty() {
                return Err(Error::Validation(
                    "BatchGetItem table keys must not be empty".into(),
                ));
            }
            total
                .checked_add(request.keys.len())
                .ok_or_else(|| Error::Validation("too many BatchGetItem keys".into()))
        })?;
        if total_keys > MAX_BATCH_GET_ITEMS {
            return Err(Error::Validation(format!(
                "BatchGetItem supports at most {MAX_BATCH_GET_ITEMS} keys"
            )));
        }

        let mut prepared = Vec::with_capacity(requests.len());
        for (table, request) in requests {
            let description = self.describe_table(&table).await?;
            let mut unique = BTreeSet::new();
            let mut encoded = Vec::with_capacity(request.keys.len());
            let mut partitions = Vec::with_capacity(request.keys.len());
            for key in &request.keys {
                let canonical = encode_primary_key(&description, key)?;
                if !unique.insert(canonical.clone()) {
                    return Err(Error::Validation(format!(
                        "BatchGetItem contains a duplicate key for table {table:?}"
                    )));
                }
                encoded.push(canonical);
                let partition_key = Item::from([(
                    description.partition_key.name.clone(),
                    key.get(&description.partition_key.name)
                        .ok_or_else(|| {
                            Error::Validation(format!(
                                "missing partition key attribute {:?}",
                                description.partition_key.name
                            ))
                        })?
                        .clone(),
                )]);
                partitions.push(encode_partition_prefix(&description, &partition_key)?);
            }
            prepared.push((table, description, request, encoded, partitions));
        }

        let mut response_bytes = 0_usize;
        let mut tables = BTreeMap::new();
        let mut partition_bytes = BTreeMap::<(String, Vec<u8>), usize>::new();
        let mut exhausted_partitions = BTreeSet::<(String, Vec<u8>)>::new();
        for (table, description, request, encoded, partitions) in prepared {
            let map = self
                .engine
                .versioned_map(Self::table_map_id(&description.id));
            let snapshot = match &request.version {
                Some(version) => map.snapshot_at(version).await?.ok_or_else(|| {
                    Error::Validation(format!(
                        "unknown version {version} for BatchGetItem table {table:?}"
                    ))
                })?,
                None => map
                    .snapshot()
                    .await?
                    .ok_or_else(|| Error::CorruptData(format!("table {table:?} has no head")))?,
            };
            let stored = snapshot.get_many(&encoded).await?;
            let resolved: Vec<Option<Item>> = stream::iter(stored)
                .map(|stored| async {
                    match stored {
                        Some(bytes) => Ok::<Option<Item>, Error>(Some(decode_item(
                            &self.blobs.resolve(&bytes).await?,
                        )?)),
                        None => Ok::<Option<Item>, Error>(None),
                    }
                })
                .buffered(16)
                .try_collect()
                .await?;

            let mut items = Vec::new();
            let mut unprocessed_keys = Vec::new();
            let mut exhausted = false;
            for ((key, partition), item) in request.keys.into_iter().zip(partitions).zip(resolved) {
                if exhausted {
                    unprocessed_keys.push(key);
                    continue;
                }
                let partition = (table.clone(), partition);
                if exhausted_partitions.contains(&partition) {
                    unprocessed_keys.push(key);
                    continue;
                }
                let Some(item) = item else {
                    continue;
                };
                let item = match &request.projection {
                    Some(projection) => projection.apply(&item),
                    None => item,
                };
                let bytes = item_size(&item)?;
                let current_partition_bytes = partition_bytes.get(&partition).copied().unwrap_or(0);
                if current_partition_bytes
                    .checked_add(bytes)
                    .is_none_or(|total| total > MAX_BATCH_GET_PARTITION_BYTES)
                {
                    exhausted_partitions.insert(partition);
                    unprocessed_keys.push(key);
                    continue;
                }
                if response_bytes
                    .checked_add(bytes)
                    .is_none_or(|total| total > MAX_BATCH_GET_RESPONSE_BYTES)
                {
                    exhausted = true;
                    unprocessed_keys.push(key);
                    continue;
                }
                response_bytes += bytes;
                partition_bytes.insert(partition, current_partition_bytes + bytes);
                items.push(item);
            }
            tables.insert(
                table,
                BatchGetTableResult {
                    items,
                    unprocessed_keys,
                    version_id: snapshot.version().id.clone(),
                },
            );
        }
        Ok(BatchGetResult {
            tables,
            response_bytes,
        })
    }

    /// Atomically read up to 100 ordered item slots from one validated
    /// multi-table transaction read set.
    pub async fn transact_get(
        &self,
        requests: Vec<TransactGetRequest>,
    ) -> Result<TransactGetResult> {
        if requests.is_empty() || requests.len() > MAX_TRANSACTION_ITEMS {
            return Err(Error::Validation(format!(
                "TransactGetItems requires 1..={MAX_TRANSACTION_ITEMS} items"
            )));
        }

        for _ in 0..=self.logical_retry_limit {
            let tx = self.engine.begin_transaction()?;
            let maps = tx.versioned_maps();
            let mut responses = Vec::with_capacity(requests.len());
            let mut table_versions = BTreeMap::new();
            let mut response_bytes = 0_usize;

            for request in &requests {
                let encoded_description = maps
                    .get(CATALOG_MAP_ID, request.table_name.as_bytes())
                    .await?
                    .ok_or_else(|| Error::TableNotFound(request.table_name.clone()))?;
                let description = decode_description(&encoded_description)?;
                if description.status != TableStatus::Active {
                    return Err(Error::TableNotActive(request.table_name.clone()));
                }
                let encoded_key = encode_primary_key(&description, &request.key)?;
                let map_id = Self::table_map_id(&description.id);
                let version = maps.head(&map_id).await?.ok_or_else(|| {
                    Error::CorruptData(format!(
                        "table {:?} has no transaction-visible head",
                        request.table_name
                    ))
                })?;
                match table_versions.get(&request.table_name) {
                    Some(existing) if existing != &version.id => {
                        return Err(Error::CorruptData(format!(
                            "table {:?} resolved multiple versions in one transaction",
                            request.table_name
                        )))
                    }
                    _ => {
                        table_versions.insert(request.table_name.clone(), version.id);
                    }
                }
                let item = match maps.get(&map_id, &encoded_key).await? {
                    Some(bytes) => {
                        let item = decode_item(&self.blobs.resolve(&bytes).await?)?;
                        response_bytes =
                            response_bytes
                                .checked_add(item_size(&item)?)
                                .ok_or_else(|| {
                                    Error::Validation(
                                        "TransactGetItems response size overflow".into(),
                                    )
                                })?;
                        if response_bytes > MAX_TRANSACTION_BYTES {
                            return Err(Error::Validation(format!(
                                "TransactGetItems response exceeds {MAX_TRANSACTION_BYTES} bytes"
                            )));
                        }
                        Some(match &request.projection {
                            Some(projection) => projection.apply(&item),
                            None => item,
                        })
                    }
                    None => None,
                };
                responses.push(TransactGetResponse { item });
            }

            match tx.commit().await? {
                TransactionUpdate::Applied { .. } => {
                    return Ok(TransactGetResult {
                        responses,
                        table_versions,
                        response_bytes,
                    })
                }
                TransactionUpdate::Conflict(_) => continue,
            }
        }
        Err(Error::ConflictExhausted)
    }

    /// Atomically evaluate and apply up to 100 distinct item actions through
    /// one multi-map root transaction. Every condition and update operand reads
    /// the same pre-transaction table heads.
    pub async fn transact_write(
        &self,
        actions: Vec<TransactWriteAction>,
    ) -> Result<TransactWriteResult> {
        self.transact_write_idempotent(actions, None).await
    }

    /// Execute an atomic write with an optional standard 10-minute
    /// `ClientRequestToken` replay record.
    pub async fn transact_write_idempotent(
        &self,
        actions: Vec<TransactWriteAction>,
        client_request_token: Option<&str>,
    ) -> Result<TransactWriteResult> {
        self.transact_write_idempotent_at_heads(actions, client_request_token, &BTreeMap::new())
            .await
    }

    /// Execute an idempotent transaction with additional whole-table CAS
    /// fences. Expected heads participate in the canonical token fingerprint.
    pub async fn transact_write_idempotent_at_heads(
        &self,
        actions: Vec<TransactWriteAction>,
        client_request_token: Option<&str>,
        expected_heads: &BTreeMap<String, MapVersionId>,
    ) -> Result<TransactWriteResult> {
        if actions.is_empty() || actions.len() > MAX_TRANSACTION_ITEMS {
            return Err(Error::Validation(format!(
                "TransactWriteItems requires 1..={MAX_TRANSACTION_ITEMS} actions"
            )));
        }
        validate_client_request_token(client_request_token)?;
        let action_tables = actions
            .iter()
            .map(|action| action.table_name())
            .collect::<BTreeSet<_>>();
        if let Some(table) = expected_heads
            .keys()
            .find(|table| !action_tables.contains(table.as_str()))
        {
            return Err(Error::Validation(format!(
                "expected head supplied for non-participating table {table:?}"
            )));
        }
        let fingerprint = transaction_fingerprint(&actions, expected_heads)?;
        let _admission = self.write_admission.lock().await;
        let idempotency_key = client_request_token.map(idempotency_key);
        let commit_id = CommitId(self.ids.generate()?.0);

        enum PendingMutation {
            Upsert { key: Vec<u8>, item: Item },
            Delete { key: Vec<u8> },
        }

        'attempt: for _ in 0..=self.logical_retry_limit {
            // Sample on every optimistic attempt. A conflict must not consume
            // part of the caller's ten-minute replay window.
            let committed_at_millis = self.clock.now_millis();
            let tx = self.engine.begin_transaction()?;
            let maps = tx.versioned_maps_at_millis(committed_at_millis);
            self.prefetch_transaction_global_roots(&tx).await?;
            if let Some(key) = &idempotency_key {
                if let Some(bytes) = maps.get(IDEMPOTENCY_MAP_ID, key).await? {
                    let record = decode_idempotency_record(&bytes)?;
                    if committed_at_millis.saturating_sub(record.completed_at_millis)
                        <= IDEMPOTENCY_WINDOW_MILLIS
                    {
                        tx.rollback();
                        if record.fingerprint != fingerprint {
                            return Err(Error::IdempotentParameterMismatch);
                        }
                        return Ok(record.result);
                    }
                }
            }
            self.ensure_writes_unfenced(&tx, &maps).await?;
            let mut descriptions = BTreeMap::<String, TableDescription>::new();
            let mut before_versions = BTreeMap::<String, MapVersionId>::new();
            let mut pending = BTreeMap::<String, Vec<PendingMutation>>::new();
            let mut targets = BTreeSet::<(String, Vec<u8>)>::new();
            let mut reasons = vec![
                TransactionCancellationReason {
                    code: None,
                    message: None,
                    item: None,
                };
                actions.len()
            ];
            let mut transaction_bytes = 0_usize;

            for (index, action) in actions.iter().enumerate() {
                let table = action.table_name();
                if !descriptions.contains_key(table) {
                    let bytes = maps
                        .get(CATALOG_MAP_ID, table.as_bytes())
                        .await?
                        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
                    let description = decode_description(&bytes)?;
                    if description.status != TableStatus::Active {
                        return Err(Error::TableNotActive(table.to_string()));
                    }
                    let map_id = Self::table_map_id(&description.id);
                    self.prefetch_table_write_roots(&tx, &description).await?;
                    let head = maps.head(&map_id).await?.ok_or_else(|| {
                        Error::CorruptData(format!(
                            "table {table:?} has no transaction-visible head"
                        ))
                    })?;
                    if let Some(expected) = expected_heads.get(table) {
                        if &head.id != expected {
                            tx.rollback();
                            return Err(Error::ExpectedHeadMismatch {
                                table: table.to_string(),
                                expected: expected.clone(),
                                current: head.id,
                            });
                        }
                    }
                    before_versions.insert(table.to_string(), head.id);
                    descriptions.insert(table.to_string(), description);
                }
                let description = descriptions.get(table).expect("description inserted");
                let (key_item, condition, return_failure_old) = match action {
                    TransactWriteAction::Put {
                        item,
                        condition,
                        return_failure_old,
                        ..
                    } => (
                        key_from_item(description, item)?,
                        condition.as_ref(),
                        *return_failure_old,
                    ),
                    TransactWriteAction::Delete {
                        key,
                        condition,
                        return_failure_old,
                        ..
                    }
                    | TransactWriteAction::Update {
                        key,
                        condition,
                        return_failure_old,
                        ..
                    } => (
                        key_from_item(description, key)?,
                        condition.as_ref(),
                        *return_failure_old,
                    ),
                    TransactWriteAction::ConditionCheck {
                        key,
                        condition,
                        return_failure_old,
                        ..
                    } => (
                        key_from_item(description, key)?,
                        Some(condition),
                        *return_failure_old,
                    ),
                };
                let encoded_key = encode_primary_key(description, &key_item)?;
                if !targets.insert((table.to_string(), encoded_key.clone())) {
                    return Err(Error::Validation(format!(
                        "TransactWriteItems contains multiple actions for table {table:?} and one item"
                    )));
                }
                let map_id = Self::table_map_id(&description.id);
                let old_item = match maps.get(&map_id, &encoded_key).await? {
                    Some(bytes) => Some(decode_item(&self.blobs.resolve(&bytes).await?)?),
                    None => None,
                };
                if let Some(condition) = condition {
                    if !condition.evaluate(old_item.as_ref())? {
                        reasons[index] = TransactionCancellationReason {
                            code: Some(TransactionCancellationCode::ConditionalCheckFailed),
                            message: Some("The conditional request failed".into()),
                            item: return_failure_old.then(|| old_item.clone()).flatten(),
                        };
                        continue;
                    }
                }

                let (logical_bytes, mutation) = match action {
                    TransactWriteAction::Put { item, .. } => (
                        item_size(item)?,
                        Some(PendingMutation::Upsert {
                            key: encoded_key,
                            item: item.clone(),
                        }),
                    ),
                    TransactWriteAction::Delete { .. } => (
                        item_size(&key_item)?,
                        Some(PendingMutation::Delete { key: encoded_key }),
                    ),
                    TransactWriteAction::Update { plan, .. } => {
                        let base = old_item.unwrap_or_else(|| key_item.clone());
                        let key_names = std::iter::once(description.partition_key.name.as_str())
                            .chain(description.sort_key.as_ref().map(|key| key.name.as_str()));
                        let item = plan.apply(&base, key_names)?;
                        (
                            item_size(&item)?,
                            Some(PendingMutation::Upsert {
                                key: encoded_key,
                                item,
                            }),
                        )
                    }
                    TransactWriteAction::ConditionCheck { .. } => (item_size(&key_item)?, None),
                };
                transaction_bytes = transaction_bytes
                    .checked_add(logical_bytes)
                    .ok_or_else(|| Error::Validation("transaction item size overflow".into()))?;
                if transaction_bytes > MAX_TRANSACTION_BYTES {
                    return Err(Error::Validation(format!(
                        "TransactWriteItems aggregate item size exceeds {MAX_TRANSACTION_BYTES} bytes"
                    )));
                }
                if let Some(mutation) = mutation {
                    pending.entry(table.to_string()).or_default().push(mutation);
                }
            }

            if reasons.iter().any(|reason| reason.code.is_some()) {
                tx.rollback();
                return Err(Error::TransactionCanceled { reasons });
            }
            // The provider folds a root's expected-value condition into that
            // root's Put/Delete action. Therefore this is one catalog condition,
            // one data-root action and one commit-log action per participating
            // table, one indexed-collection CAS, one snapshot-catalog action,
            // and at most one blob-registry action per mutated table, one
            // global commit action, one global maintenance-fence condition,
            // and optionally one idempotency action. In
            // AtomicNodesAndRoots mode the provider additionally
            // counts staged node writes exactly before issuing the AWS request.
            let root_actions = descriptions
                .len()
                .checked_mul(2)
                .and_then(|count| count.checked_add(3))
                .and_then(|count| count.checked_add(pending.len()))
                .and_then(|count| count.checked_add(pending.len()))
                .and_then(|count| count.checked_add(pending.len()))
                .and_then(|count| count.checked_add(usize::from(client_request_token.is_some())))
                .ok_or_else(|| Error::Validation("transaction root-action size overflow".into()))?;
            if root_actions > 100 {
                return Err(Error::Validation(format!(
                    "TransactWriteItems requires {root_actions} physical root actions, exceeding 100"
                )));
            }

            let mut transitions = Vec::with_capacity(descriptions.len());
            let mut transitioned_tables = BTreeSet::new();
            let mut table_versions = before_versions.clone();
            for (table, mutations) in pending {
                let description = descriptions
                    .get(&table)
                    .expect("validated table description");
                let mut prepared = Vec::with_capacity(mutations.len());
                let mut indexed_mutations = Vec::with_capacity(mutations.len());
                for mutation in mutations {
                    match mutation {
                        PendingMutation::Upsert { key, item } => {
                            let stored_item = self.blobs.prepare(encode_item(&item)?).await?;
                            let index_source = prepare_index_source_record(
                                description,
                                &item,
                                stored_item.clone(),
                                &self.blobs,
                            )
                            .await?;
                            prepared.push(Mutation::Upsert {
                                key: key.clone(),
                                val: stored_item,
                            });
                            indexed_mutations.push(Mutation::Upsert {
                                key,
                                val: index_source,
                            });
                        }
                        PendingMutation::Delete { key } => {
                            prepared.push(Mutation::Delete { key: key.clone() });
                            indexed_mutations.push(Mutation::Delete { key });
                        }
                    }
                }
                let before = before_versions[&table].clone();
                let Some(after) = self
                    .stage_indexed_table_mutations(
                        &tx,
                        &maps,
                        description,
                        &before,
                        prepared,
                        indexed_mutations,
                    )
                    .await?
                else {
                    tx.rollback();
                    continue 'attempt;
                };
                let after = after.id;
                table_versions.insert(table.clone(), after.clone());
                transitions.push(TransactionTableTransition {
                    table_name: table.clone(),
                    table_id: description.id.clone(),
                    applied: before != after,
                    before: Some(before),
                    after: Some(after),
                });
                transitioned_tables.insert(table);
            }
            // ConditionCheck is still a participant in an accepted transaction.
            // Record its before=after transition so table-local audit order can
            // reconstruct the complete cross-table event, including no-op work.
            for table in descriptions.keys() {
                if transitioned_tables.contains(table) {
                    continue;
                }
                let version = before_versions[table].clone();
                transitions.push(TransactionTableTransition {
                    table_name: table.clone(),
                    table_id: descriptions[table].id.clone(),
                    before: Some(version.clone()),
                    after: Some(version),
                    applied: false,
                });
            }
            transitions.sort_by(|left, right| left.table_name.cmp(&right.table_name));

            let result = TransactWriteResult {
                commit_id: commit_id.clone(),
                transitions,
                table_versions,
            };
            self.stage_commit_records(&tx, &descriptions, &result, committed_at_millis)
                .await?;
            if let Some(key) = &idempotency_key {
                maps.apply(
                    IDEMPOTENCY_MAP_ID,
                    vec![Mutation::Upsert {
                        key: key.clone(),
                        val: encode_idempotency_record(&IdempotencyRecord {
                            fingerprint,
                            completed_at_millis: committed_at_millis,
                            result: result.clone(),
                        })?,
                    }],
                )
                .await?;
            }

            match tx.commit().await {
                Ok(TransactionUpdate::Applied { .. }) => return Ok(result),
                Ok(TransactionUpdate::Conflict(_)) => continue,
                Err(source) => {
                    if let Some(key) = &idempotency_key {
                        // A provider error may arrive after DynamoDB accepted
                        // the atomic root transaction. A strongly visible token
                        // record is authoritative evidence of that outcome.
                        match self
                            .reconcile_idempotency_record(
                                key,
                                &fingerprint,
                                self.clock.now_millis(),
                            )
                            .await
                        {
                            Ok(Some(reconciled)) => return Ok(reconciled),
                            Ok(None) | Err(Error::Storage(_)) => {}
                            Err(error) => return Err(error),
                        }
                    }
                    return Err(Error::Storage(source));
                }
            }
        }

        Err(Error::TransactionCanceled {
            reasons: actions
                .iter()
                .map(|_| TransactionCancellationReason {
                    code: Some(TransactionCancellationCode::TransactionConflict),
                    message: Some("Transaction is ongoing for the item".into()),
                    item: None,
                })
                .collect(),
        })
    }

    /// Idempotent single-item put used by the client extension token surface.
    pub async fn put_item_idempotent(
        &self,
        table: &str,
        item: Item,
        expected: Option<&MapVersionId>,
        condition: Option<&Condition>,
        client_request_token: &str,
        capture_old: bool,
    ) -> Result<ItemWrite> {
        let key_source = item.clone();
        let result = self
            .single_action_transaction(
                TransactWriteAction::Put {
                    table_name: table.to_string(),
                    item,
                    condition: condition.cloned(),
                    return_failure_old: true,
                },
                expected,
                client_request_token,
            )
            .await?;
        self.item_write_from_commit(table, &key_source, result, capture_old)
            .await
    }

    /// Idempotent single-item delete used by the client extension token surface.
    pub async fn delete_item_idempotent(
        &self,
        table: &str,
        key: &Item,
        expected: Option<&MapVersionId>,
        condition: Option<&Condition>,
        client_request_token: &str,
        capture_old: bool,
    ) -> Result<ItemWrite> {
        let result = self
            .single_action_transaction(
                TransactWriteAction::Delete {
                    table_name: table.to_string(),
                    key: key.clone(),
                    condition: condition.cloned(),
                    return_failure_old: true,
                },
                expected,
                client_request_token,
            )
            .await?;
        self.item_write_from_commit(table, key, result, capture_old)
            .await
    }

    /// Idempotent single-item update used by the client extension token surface.
    pub async fn update_item_idempotent(
        &self,
        table: &str,
        key: &Item,
        expected: Option<&MapVersionId>,
        condition: Option<&Condition>,
        plan: &UpdatePlan,
        client_request_token: &str,
    ) -> Result<ItemUpdate> {
        let result = self
            .single_action_transaction(
                TransactWriteAction::Update {
                    table_name: table.to_string(),
                    key: key.clone(),
                    condition: condition.cloned(),
                    plan: plan.clone(),
                    return_failure_old: true,
                },
                expected,
                client_request_token,
            )
            .await?;
        let transition = single_table_transition(table, &result)?;
        let description = self.description_for_transition(transition).await?;
        if description.name != table {
            return Err(Error::CorruptData(
                "idempotent update resolved another table name".into(),
            ));
        }
        let key = encode_primary_key(&description, key)?;
        let old_item = self
            .item_at_table_version(&description, transition.before.as_ref(), &key)
            .await?;
        let new_item = self
            .item_at_table_version(&description, transition.after.as_ref(), &key)
            .await?;
        Ok(ItemUpdate {
            commit_id: Some(result.commit_id.clone()),
            table_id: Some(description.id.clone()),
            update: self
                .version_update_from_transition(&description, transition)
                .await?,
            old_item,
            new_item,
        })
    }

    async fn single_action_transaction(
        &self,
        action: TransactWriteAction,
        expected: Option<&MapVersionId>,
        client_request_token: &str,
    ) -> Result<TransactWriteResult> {
        let table = action.table_name().to_string();
        let expected_heads = expected
            .cloned()
            .map(|expected| BTreeMap::from([(table, expected)]))
            .unwrap_or_default();
        match self
            .transact_write_idempotent_at_heads(
                vec![action],
                Some(client_request_token),
                &expected_heads,
            )
            .await
        {
            Err(Error::TransactionCanceled { mut reasons }) if reasons.len() == 1 => {
                let reason = reasons.pop().expect("length checked");
                if reason.code == Some(TransactionCancellationCode::ConditionalCheckFailed) {
                    Err(Error::ConditionalCheckFailed {
                        old_item: reason.item,
                    })
                } else {
                    Err(Error::TransactionCanceled {
                        reasons: vec![reason],
                    })
                }
            }
            result => result,
        }
    }

    async fn item_write_from_commit(
        &self,
        table: &str,
        key_source: &Item,
        result: TransactWriteResult,
        capture_old: bool,
    ) -> Result<ItemWrite> {
        let transition = single_table_transition(table, &result)?;
        let description = self.description_for_transition(transition).await?;
        if description.name != table {
            return Err(Error::CorruptData(
                "idempotent write resolved another table name".into(),
            ));
        }
        let key_item = key_from_item(&description, key_source)?;
        let key = encode_primary_key(&description, &key_item)?;
        let old_item = if capture_old {
            self.item_at_table_version(&description, transition.before.as_ref(), &key)
                .await?
        } else {
            None
        };
        Ok(ItemWrite {
            commit_id: Some(result.commit_id.clone()),
            table_id: Some(description.id.clone()),
            update: self
                .version_update_from_transition(&description, transition)
                .await?,
            old_item,
        })
    }

    async fn item_at_table_version(
        &self,
        description: &TableDescription,
        version: Option<&MapVersionId>,
        key: &[u8],
    ) -> Result<Option<Item>> {
        let Some(version) = version else {
            return Ok(None);
        };
        let map = self
            .engine
            .versioned_map(Self::table_map_id(&description.id));
        let snapshot = map.snapshot_at(version).await?.ok_or_else(|| {
            Error::CorruptData(format!(
                "idempotent result references missing table version {version}"
            ))
        })?;
        match snapshot.get(key).await? {
            Some(bytes) => Ok(Some(decode_item(&self.blobs.resolve(&bytes).await?)?)),
            None => Ok(None),
        }
    }

    async fn version_update_from_transition(
        &self,
        description: &TableDescription,
        transition: &TransactionTableTransition,
    ) -> Result<VersionedMapUpdate> {
        let after = transition.after.as_ref().ok_or_else(|| {
            Error::CorruptData("item write commit has no resulting table version".into())
        })?;
        let current = self
            .engine
            .versioned_map(Self::table_map_id(&description.id))
            .version(after)
            .await?
            .ok_or_else(|| {
                Error::CorruptData(format!(
                    "item write commit references missing table version {after}"
                ))
            })?;
        if transition.applied {
            Ok(VersionedMapUpdate::Applied {
                previous: transition.before.clone(),
                current,
            })
        } else {
            Ok(VersionedMapUpdate::Unchanged {
                current: Some(current),
            })
        }
    }

    /// Validate an entire BatchWriteItem request before any item-tree write.
    ///
    /// Duplicate targets use canonical encoded primary keys, so equivalent
    /// number spellings cannot bypass duplicate detection.
    pub async fn validate_batch_write(
        &self,
        requests: &BTreeMap<String, Vec<BatchWriteAction>>,
    ) -> Result<()> {
        if requests.is_empty() {
            return Err(Error::Validation(
                "BatchWriteItem.request_items must not be empty".into(),
            ));
        }
        let total = requests.values().try_fold(0_usize, |total, actions| {
            if actions.is_empty() {
                return Err(Error::Validation(
                    "BatchWriteItem table actions must not be empty".into(),
                ));
            }
            total
                .checked_add(actions.len())
                .ok_or_else(|| Error::Validation("too many BatchWriteItem actions".into()))
        })?;
        if total > MAX_BATCH_WRITE_ITEMS {
            return Err(Error::Validation(format!(
                "BatchWriteItem supports at most {MAX_BATCH_WRITE_ITEMS} actions"
            )));
        }

        for (table, actions) in requests {
            let description = self.describe_table(table).await?;
            let mut targets = BTreeSet::new();
            for action in actions {
                let key = match action {
                    BatchWriteAction::Put(item) => {
                        encode_item(item)?;
                        key_from_item(&description, item)?
                    }
                    BatchWriteAction::Delete(key) => key.clone(),
                };
                let encoded = encode_primary_key(&description, &key)?;
                if !targets.insert(encoded) {
                    return Err(Error::Validation(format!(
                        "BatchWriteItem contains duplicate operations for table {table:?}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Execute a validated BatchWriteItem as independently atomic logical
    /// writes. The batch as a whole is intentionally non-atomic.
    pub async fn batch_write(
        &self,
        requests: BTreeMap<String, Vec<BatchWriteAction>>,
    ) -> std::result::Result<BatchWriteResult, BatchWriteExecutionError> {
        if let Err(source) = self.validate_batch_write(&requests).await {
            return Err(BatchWriteExecutionError::Validation { source });
        }
        let mut transitions = Vec::new();
        for (table, actions) in requests {
            for (action_index, action) in actions.into_iter().enumerate() {
                let update = match action {
                    BatchWriteAction::Put(item) => self.put_item_result(&table, item, None).await,
                    BatchWriteAction::Delete(key) => {
                        self.delete_item_result(&table, &key, None).await
                    }
                };
                match update {
                    Ok(result) => {
                        let commit_id =
                            result
                                .commit_id
                                .ok_or_else(|| BatchWriteExecutionError::Partial {
                                    table_name: table.clone(),
                                    action_index,
                                    applied_transitions: transitions.clone(),
                                    source: Error::CorruptData(
                                        "accepted BatchWriteItem action has no commit identity"
                                            .into(),
                                    ),
                                })?;
                        transitions.push(BatchWriteTransition {
                            table_name: table.clone(),
                            table_id: result.table_id.ok_or_else(|| {
                                BatchWriteExecutionError::Partial {
                                    table_name: table.clone(),
                                    action_index,
                                    applied_transitions: transitions.clone(),
                                    source: Error::CorruptData(
                                        "accepted BatchWriteItem action has no table identity"
                                            .into(),
                                    ),
                                }
                            })?,
                            action_index,
                            commit_id,
                            update: result.update,
                        });
                    }
                    Err(source) => {
                        return Err(BatchWriteExecutionError::Partial {
                            table_name: table,
                            action_index,
                            applied_transitions: transitions,
                            source,
                        })
                    }
                }
            }
        }
        Ok(BatchWriteResult { transitions })
    }

    /// Read one partition in canonical sort-key order from a pinned head.
    pub async fn query_partition(
        &self,
        table: &str,
        partition_key: &Item,
        exclusive_start_key: Option<&Item>,
        limit: usize,
    ) -> Result<ReadPage> {
        if limit == 0 {
            return Err(Error::Validation("query limit must be nonzero".into()));
        }
        self.query_partition_at_version(table, None, partition_key, exclusive_start_key, limit)
            .await
    }

    /// Read one partition from an exact immutable historical version.
    pub async fn query_partition_at(
        &self,
        table: &str,
        version: &MapVersionId,
        partition_key: &Item,
        exclusive_start_key: Option<&Item>,
        limit: usize,
    ) -> Result<ReadPage> {
        self.query_partition_at_version(
            table,
            Some(version),
            partition_key,
            exclusive_start_key,
            limit,
        )
        .await
    }

    /// Execute a typed base-table key condition using bounded Prolly ranges.
    pub async fn query_key_condition(
        &self,
        table: &str,
        condition: &KeyCondition,
        exclusive_start_key: Option<&Item>,
        limit: usize,
    ) -> Result<ReadPage> {
        self.query_key_condition_at_version(
            table,
            None,
            condition,
            exclusive_start_key,
            limit,
            true,
        )
        .await
    }

    pub async fn query_key_condition_ordered(
        &self,
        table: &str,
        condition: &KeyCondition,
        exclusive_start_key: Option<&Item>,
        limit: usize,
        scan_forward: bool,
    ) -> Result<ReadPage> {
        self.query_key_condition_at_version(
            table,
            None,
            condition,
            exclusive_start_key,
            limit,
            scan_forward,
        )
        .await
    }

    /// Execute a typed key condition against an exact historical version.
    pub async fn query_key_condition_at(
        &self,
        table: &str,
        version: &MapVersionId,
        condition: &KeyCondition,
        exclusive_start_key: Option<&Item>,
        limit: usize,
    ) -> Result<ReadPage> {
        self.query_key_condition_at_version(
            table,
            Some(version),
            condition,
            exclusive_start_key,
            limit,
            true,
        )
        .await
    }

    pub async fn query_key_condition_at_ordered(
        &self,
        table: &str,
        version: &MapVersionId,
        condition: &KeyCondition,
        exclusive_start_key: Option<&Item>,
        limit: usize,
        scan_forward: bool,
    ) -> Result<ReadPage> {
        self.query_key_condition_at_version(
            table,
            Some(version),
            condition,
            exclusive_start_key,
            limit,
            scan_forward,
        )
        .await
    }

    async fn query_key_condition_at_version(
        &self,
        table: &str,
        version: Option<&MapVersionId>,
        condition: &KeyCondition,
        exclusive_start_key: Option<&Item>,
        limit: usize,
        scan_forward: bool,
    ) -> Result<ReadPage> {
        if limit == 0 {
            return Err(Error::Validation("query limit must be nonzero".into()));
        }
        let description = self.describe_table(table).await?;
        let (default_cursor, end) = key_condition_bounds(&description, condition)?;
        let lower = default_cursor.after().unwrap_or_default().to_vec();
        let cursor = match exclusive_start_key {
            Some(key) => {
                let key = encode_primary_key(&description, key)?;
                match default_cursor.after() {
                    Some(lower) if lower >= key.as_slice() => default_cursor,
                    _ => prolly::RangeCursor::after_key(key),
                }
            }
            None => default_cursor,
        };
        let map = self
            .engine
            .versioned_map(Self::table_map_id(&description.id));
        let snapshot = match version {
            Some(version) => map
                .snapshot_at(version)
                .await?
                .ok_or_else(|| Error::Validation(format!("unknown table version {version}")))?,
            None => map
                .snapshot()
                .await?
                .ok_or_else(|| Error::CorruptData(format!("table {table:?} has no head")))?,
        };
        if scan_forward {
            self.collect_read_page(&description, &snapshot, None, cursor, end.as_deref(), limit)
                .await
        } else {
            let reverse_cursor = match exclusive_start_key {
                Some(key) => {
                    prolly::ReverseCursor::before_key(encode_primary_key(&description, key)?)
                }
                None => prolly::ReverseCursor::end(),
            };
            self.collect_reverse_read_page(
                &description,
                &snapshot,
                reverse_cursor,
                &lower,
                end.as_deref(),
                limit,
            )
            .await
        }
    }

    async fn query_partition_at_version(
        &self,
        table: &str,
        version: Option<&MapVersionId>,
        partition_key: &Item,
        exclusive_start_key: Option<&Item>,
        limit: usize,
    ) -> Result<ReadPage> {
        if limit == 0 {
            return Err(Error::Validation("query limit must be nonzero".into()));
        }
        let description = self.describe_table(table).await?;
        let prefix = encode_partition_prefix(&description, partition_key)?;
        let map = self
            .engine
            .versioned_map(Self::table_map_id(&description.id));
        let snapshot = match version {
            Some(version) => map
                .snapshot_at(version)
                .await?
                .ok_or_else(|| Error::Validation(format!("unknown table version {version}")))?,
            None => map
                .snapshot()
                .await?
                .ok_or_else(|| Error::CorruptData(format!("table {table:?} has no head")))?,
        };
        let cursor = match exclusive_start_key {
            Some(key) => prolly::RangeCursor::after_key(encode_primary_key(&description, key)?),
            None => prolly::RangeCursor::start(),
        };
        self.collect_read_page(&description, &snapshot, Some(&prefix), cursor, None, limit)
            .await
    }

    /// Scan one pinned head in canonical primary-key order.
    pub async fn scan(
        &self,
        table: &str,
        exclusive_start_key: Option<&Item>,
        limit: usize,
    ) -> Result<ReadPage> {
        self.scan_at_version(table, None, exclusive_start_key, limit)
            .await
    }

    /// Scan an exact immutable historical version.
    pub async fn scan_at(
        &self,
        table: &str,
        version: &MapVersionId,
        exclusive_start_key: Option<&Item>,
        limit: usize,
    ) -> Result<ReadPage> {
        self.scan_at_version(table, Some(version), exclusive_start_key, limit)
            .await
    }

    /// Validate and resolve one TTL configuration without scanning table data.
    pub async fn validate_ttl_configuration(
        &self,
        table: &str,
        ttl_attribute: &str,
    ) -> Result<TableDescription> {
        let description = self.describe_table(table).await?;
        validate_ttl_attribute(&description, ttl_attribute)?;
        Ok(description)
    }

    /// Plan one DynamoDB-compatible TTL page without mutating table state.
    pub async fn ttl_candidates(
        &self,
        table: &str,
        expected_table_id: &TableId,
        ttl_attribute: &str,
        exclusive_start_key: Option<&Item>,
        limit: usize,
        now_epoch_seconds: u64,
    ) -> Result<TtlCandidatePage> {
        let description = self
            .validate_ttl_configuration(table, ttl_attribute)
            .await?;
        ensure_table_incarnation(table, &description, expected_table_id)?;
        let page = self
            .scan_at_version_for_incarnation(
                table,
                None,
                Some(expected_table_id),
                exclusive_start_key,
                limit,
            )
            .await?;
        let evaluated = page.items.len();
        let mut candidates = Vec::new();
        for item in &page.items {
            let Some(AttributeValue::N(expiration)) = item.get(ttl_attribute) else {
                continue;
            };
            if ttl_expiration_is_eligible(expiration, now_epoch_seconds) {
                candidates.push(TtlCandidate {
                    key: key_from_item(&description, item)?,
                    observed_expiration: expiration.clone(),
                });
            }
        }
        Ok(TtlCandidatePage {
            candidates,
            evaluated,
            last_evaluated_key: page.last_evaluated_key,
            version_id: page.version_id,
        })
    }

    /// Delete one planned candidate only if the exact observed TTL number is
    /// still present and eligible at the supplied time. `false` means a
    /// concurrent writer changed or removed the item/TTL value.
    pub async fn expire_ttl_candidate(
        &self,
        table: &str,
        expected_table_id: &TableId,
        ttl_attribute: &str,
        candidate: &TtlCandidate,
        now_epoch_seconds: u64,
    ) -> Result<bool> {
        let description = self
            .validate_ttl_configuration(table, ttl_attribute)
            .await?;
        ensure_table_incarnation(table, &description, expected_table_id)?;
        encode_primary_key(&description, &candidate.key)?;
        if !ttl_expiration_is_eligible(&candidate.observed_expiration, now_epoch_seconds) {
            return Err(Error::Validation(
                "TTL candidate expiration is not eligible at the supplied time".into(),
            ));
        }
        let condition = Condition::Equals {
            name: crate::AttributePath::top_level(ttl_attribute),
            value: AttributeValue::N(candidate.observed_expiration.clone()),
        };
        match self
            .write_item_for_incarnation(
                table,
                &candidate.key,
                None,
                Some(&condition),
                None,
                false,
                Some(expected_table_id),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(Error::ConditionalCheckFailed { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn scan_at_version(
        &self,
        table: &str,
        version: Option<&MapVersionId>,
        exclusive_start_key: Option<&Item>,
        limit: usize,
    ) -> Result<ReadPage> {
        self.scan_at_version_for_incarnation(table, version, None, exclusive_start_key, limit)
            .await
    }

    async fn scan_at_version_for_incarnation(
        &self,
        table: &str,
        version: Option<&MapVersionId>,
        expected_table_id: Option<&TableId>,
        exclusive_start_key: Option<&Item>,
        limit: usize,
    ) -> Result<ReadPage> {
        if limit == 0 {
            return Err(Error::Validation("scan limit must be nonzero".into()));
        }
        let description = self.describe_table(table).await?;
        if let Some(expected_table_id) = expected_table_id {
            ensure_table_incarnation(table, &description, expected_table_id)?;
        }
        let map = self
            .engine
            .versioned_map(Self::table_map_id(&description.id));
        let snapshot = match version {
            Some(version) => map
                .snapshot_at(version)
                .await?
                .ok_or_else(|| Error::Validation(format!("unknown table version {version}")))?,
            None => map
                .snapshot()
                .await?
                .ok_or_else(|| Error::CorruptData(format!("table {table:?} has no head")))?,
        };
        let cursor = match exclusive_start_key {
            Some(key) => prolly::RangeCursor::after_key(encode_primary_key(&description, key)?),
            None => prolly::RangeCursor::start(),
        };
        self.collect_read_page(
            &description,
            &snapshot,
            None,
            cursor,
            Some(TABLE_SCHEMA_RECORD_KEY),
            limit,
        )
        .await
    }

    async fn collect_read_page(
        &self,
        description: &TableDescription,
        snapshot: &prolly::AsyncMapSnapshot<'_, S>,
        prefix: Option<&[u8]>,
        mut cursor: prolly::RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<ReadPage> {
        let mut items = Vec::new();
        let mut logical_bytes = 0_usize;
        let mut has_more = false;

        while items.len() < limit {
            let request_items = (limit - items.len()).min(READ_CHUNK_ITEMS);
            let page = match prefix {
                Some(prefix) => snapshot.prefix_page(prefix, &cursor, request_items).await?,
                None => snapshot.range_page(&cursor, end, request_items).await?,
            };
            let page_has_more = page.next_cursor.is_some();
            let entry_count = page.entries.len();
            if entry_count == 0 {
                break;
            }

            for (index, (_, bytes)) in page.entries.into_iter().enumerate() {
                let item = decode_item(&self.blobs.resolve(&bytes).await?)?;
                let bytes = item_size(&item)?;
                if logical_bytes
                    .checked_add(bytes)
                    .is_none_or(|total| total > MAX_READ_PAGE_BYTES)
                {
                    has_more = true;
                    break;
                }
                logical_bytes += bytes;
                items.push(item);
                if items.len() == limit {
                    has_more = index + 1 < entry_count || page_has_more;
                    break;
                }
            }
            if has_more || items.len() == limit {
                break;
            }
            match page.next_cursor {
                Some(next) => cursor = next,
                None => break,
            }
        }

        let last_evaluated_key = has_more
            .then(|| items.last())
            .flatten()
            .map(|item| key_from_item(description, item))
            .transpose()?;
        Ok(ReadPage {
            items,
            last_evaluated_key,
            version_id: snapshot.version().id.clone(),
        })
    }

    async fn collect_reverse_read_page(
        &self,
        description: &TableDescription,
        snapshot: &prolly::AsyncMapSnapshot<'_, S>,
        mut cursor: prolly::ReverseCursor,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<ReadPage> {
        let mut items = Vec::new();
        let mut logical_bytes = 0_usize;
        let mut has_more = false;
        while items.len() < limit {
            let request_items = (limit - items.len()).min(READ_CHUNK_ITEMS);
            let page = snapshot
                .reverse_range_page(&cursor, start, end, request_items)
                .await?;
            let page_has_more = page.next_cursor.is_some();
            let entry_count = page.entries.len();
            if entry_count == 0 {
                break;
            }
            for (index, (_, bytes)) in page.entries.into_iter().enumerate() {
                let item = decode_item(&self.blobs.resolve(&bytes).await?)?;
                let bytes = item_size(&item)?;
                if logical_bytes
                    .checked_add(bytes)
                    .is_none_or(|total| total > MAX_READ_PAGE_BYTES)
                {
                    has_more = true;
                    break;
                }
                logical_bytes += bytes;
                items.push(item);
                if items.len() == limit {
                    has_more = index + 1 < entry_count || page_has_more;
                    break;
                }
            }
            if has_more || items.len() == limit {
                break;
            }
            match page.next_cursor {
                Some(next) => cursor = next,
                None => break,
            }
        }
        let last_evaluated_key = has_more
            .then(|| items.last())
            .flatten()
            .map(|item| key_from_item(description, item))
            .transpose()?;
        Ok(ReadPage {
            items,
            last_evaluated_key,
            version_id: snapshot.version().id.clone(),
        })
    }

    pub async fn put_item(
        &self,
        table: &str,
        item: Item,
        expected: Option<&MapVersionId>,
    ) -> Result<VersionedMapUpdate> {
        let encoded_item = encode_item(&item)?;
        Ok(self
            .write_item(table, &item, expected, None, Some(encoded_item), false)
            .await?
            .update)
    }

    /// Put an item and retain the durable accepted-event identity without
    /// paying to materialize the old image.
    pub async fn put_item_result(
        &self,
        table: &str,
        item: Item,
        expected: Option<&MapVersionId>,
    ) -> Result<ItemWrite> {
        let encoded_item = encode_item(&item)?;
        self.write_item(table, &item, expected, None, Some(encoded_item), false)
            .await
    }

    pub async fn put_item_with_old(
        &self,
        table: &str,
        item: Item,
        expected: Option<&MapVersionId>,
    ) -> Result<ItemWrite> {
        let encoded_item = encode_item(&item)?;
        self.write_item(table, &item, expected, None, Some(encoded_item), true)
            .await
    }

    pub async fn put_item_conditionally(
        &self,
        table: &str,
        item: Item,
        expected: Option<&MapVersionId>,
        condition: &Condition,
    ) -> Result<VersionedMapUpdate> {
        let encoded_item = encode_item(&item)?;
        Ok(self
            .write_item(
                table,
                &item,
                expected,
                Some(condition),
                Some(encoded_item),
                false,
            )
            .await?
            .update)
    }

    /// Conditional put with commit metadata but without returning the old image.
    pub async fn put_item_conditionally_result(
        &self,
        table: &str,
        item: Item,
        expected: Option<&MapVersionId>,
        condition: &Condition,
    ) -> Result<ItemWrite> {
        let encoded_item = encode_item(&item)?;
        self.write_item(
            table,
            &item,
            expected,
            Some(condition),
            Some(encoded_item),
            false,
        )
        .await
    }

    pub async fn put_item_conditionally_with_old(
        &self,
        table: &str,
        item: Item,
        expected: Option<&MapVersionId>,
        condition: &Condition,
    ) -> Result<ItemWrite> {
        let encoded_item = encode_item(&item)?;
        self.write_item(
            table,
            &item,
            expected,
            Some(condition),
            Some(encoded_item),
            true,
        )
        .await
    }

    pub async fn delete_item(
        &self,
        table: &str,
        key: &Item,
        expected: Option<&MapVersionId>,
    ) -> Result<VersionedMapUpdate> {
        Ok(self
            .write_item(table, key, expected, None, None, false)
            .await?
            .update)
    }

    /// Delete an item and retain the durable accepted-event identity without
    /// paying to materialize the old image.
    pub async fn delete_item_result(
        &self,
        table: &str,
        key: &Item,
        expected: Option<&MapVersionId>,
    ) -> Result<ItemWrite> {
        self.write_item(table, key, expected, None, None, false)
            .await
    }

    pub async fn delete_item_with_old(
        &self,
        table: &str,
        key: &Item,
        expected: Option<&MapVersionId>,
    ) -> Result<ItemWrite> {
        self.write_item(table, key, expected, None, None, true)
            .await
    }

    pub async fn delete_item_conditionally(
        &self,
        table: &str,
        key: &Item,
        expected: Option<&MapVersionId>,
        condition: &Condition,
    ) -> Result<VersionedMapUpdate> {
        Ok(self
            .write_item(table, key, expected, Some(condition), None, false)
            .await?
            .update)
    }

    /// Conditional delete with commit metadata but without returning the old image.
    pub async fn delete_item_conditionally_result(
        &self,
        table: &str,
        key: &Item,
        expected: Option<&MapVersionId>,
        condition: &Condition,
    ) -> Result<ItemWrite> {
        self.write_item(table, key, expected, Some(condition), None, false)
            .await
    }

    pub async fn delete_item_conditionally_with_old(
        &self,
        table: &str,
        key: &Item,
        expected: Option<&MapVersionId>,
        condition: &Condition,
    ) -> Result<ItemWrite> {
        self.write_item(table, key, expected, Some(condition), None, true)
            .await
    }

    /// Evaluate and publish a typed update plan atomically with an optional
    /// condition. On an optimistic conflict the entire plan is re-evaluated
    /// against the new immutable old item.
    pub async fn update_item(
        &self,
        table: &str,
        key: &Item,
        expected: Option<&MapVersionId>,
        condition: Option<&Condition>,
        plan: &UpdatePlan,
    ) -> Result<ItemUpdate> {
        let _admission = self.write_admission.lock().await;
        let commit_id = CommitId(self.ids.generate()?.0);
        for _ in 0..=self.logical_retry_limit {
            let committed_at_millis = self.clock.now_millis();
            let tx = self.engine.begin_transaction()?;
            let maps = tx.versioned_maps_at_millis(committed_at_millis);
            self.ensure_writes_unfenced(&tx, &maps).await?;
            let descriptor = maps
                .get(CATALOG_MAP_ID, table.as_bytes())
                .await?
                .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
            let description = decode_description(&descriptor)?;
            if description.status != TableStatus::Active {
                tx.rollback();
                return Err(Error::TableNotActive(table.to_string()));
            }
            let key_item = key_from_item(&description, key)?;
            let encoded_key = encode_primary_key(&description, &key_item)?;
            let map_id = Self::table_map_id(&description.id);
            self.prefetch_table_write_roots(&tx, &description).await?;
            let current = maps.head(&map_id).await?;
            if expected.is_some() && current.as_ref().map(|version| &version.id) != expected {
                tx.rollback();
                return Ok(ItemUpdate {
                    commit_id: None,
                    table_id: Some(description.id),
                    update: VersionedMapUpdate::Conflict { current },
                    old_item: None,
                    new_item: None,
                });
            }
            let old_item = match maps.get(&map_id, &encoded_key).await? {
                Some(bytes) => Some(decode_item(&self.blobs.resolve(&bytes).await?)?),
                None => None,
            };
            if let Some(condition) = condition {
                if !condition.evaluate(old_item.as_ref())? {
                    tx.rollback();
                    return Err(Error::ConditionalCheckFailed {
                        old_item: old_item.clone(),
                    });
                }
            }
            let base = old_item.clone().unwrap_or_else(|| key_item.clone());
            let key_names = std::iter::once(description.partition_key.name.as_str())
                .chain(description.sort_key.as_ref().map(|key| key.name.as_str()));
            let new_item = plan.apply(&base, key_names)?;
            let encoded_item = encode_item(&new_item)?;
            let prepared_item = self.blobs.prepare(encoded_item).await?;
            let indexed_item = prepare_index_source_record(
                &description,
                &new_item,
                prepared_item.clone(),
                &self.blobs,
            )
            .await?;
            let before = current
                .map(|version| version.id)
                .ok_or_else(|| Error::CorruptData(format!("table {table:?} has no head")))?;
            let Some(current) = self
                .stage_indexed_table_mutations(
                    &tx,
                    &maps,
                    &description,
                    &before,
                    vec![Mutation::Upsert {
                        key: encoded_key.clone(),
                        val: prepared_item,
                    }],
                    vec![Mutation::Upsert {
                        key: encoded_key,
                        val: indexed_item,
                    }],
                )
                .await?
            else {
                tx.rollback();
                continue;
            };
            let update = if before == current.id {
                VersionedMapUpdate::Unchanged {
                    current: Some(current.clone()),
                }
            } else {
                VersionedMapUpdate::Applied {
                    previous: Some(before.clone()),
                    current: current.clone(),
                }
            };
            self.stage_single_table_commit(
                &tx,
                &description,
                commit_id.clone(),
                Some(before),
                Some(current.id),
                committed_at_millis,
            )
            .await?;
            match tx.commit().await? {
                TransactionUpdate::Applied { .. } => {
                    return Ok(ItemUpdate {
                        commit_id: Some(commit_id),
                        table_id: Some(description.id),
                        update,
                        old_item,
                        new_item: Some(new_item),
                    })
                }
                TransactionUpdate::Conflict(_) if expected.is_some() => {
                    let current = self.engine.versioned_map(&map_id).head().await?;
                    return Ok(ItemUpdate {
                        commit_id: None,
                        table_id: Some(description.id),
                        update: VersionedMapUpdate::Conflict { current },
                        old_item: None,
                        new_item: None,
                    });
                }
                TransactionUpdate::Conflict(_) => continue,
            }
        }
        Err(Error::ConflictExhausted)
    }

    async fn write_item(
        &self,
        table: &str,
        item_or_key: &Item,
        expected: Option<&MapVersionId>,
        condition: Option<&Condition>,
        encoded_item: Option<Vec<u8>>,
        capture_old: bool,
    ) -> Result<ItemWrite> {
        self.write_item_for_incarnation(
            table,
            item_or_key,
            expected,
            condition,
            encoded_item,
            capture_old,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_item_for_incarnation(
        &self,
        table: &str,
        item_or_key: &Item,
        expected: Option<&MapVersionId>,
        condition: Option<&Condition>,
        encoded_item: Option<Vec<u8>>,
        capture_old: bool,
        expected_table_id: Option<&TableId>,
    ) -> Result<ItemWrite> {
        let _admission = self.write_admission.lock().await;
        let mut prepared_item = None;
        let commit_id = CommitId(self.ids.generate()?.0);
        for _ in 0..=self.logical_retry_limit {
            let committed_at_millis = self.clock.now_millis();
            let tx = self.engine.begin_transaction()?;
            let maps = tx.versioned_maps_at_millis(committed_at_millis);
            self.ensure_writes_unfenced(&tx, &maps).await?;
            let descriptor = maps
                .get(CATALOG_MAP_ID, table.as_bytes())
                .await?
                .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
            let description = decode_description(&descriptor)?;
            if let Some(expected_table_id) = expected_table_id {
                ensure_table_incarnation(table, &description, expected_table_id)?;
            }
            if description.status != TableStatus::Active {
                tx.rollback();
                return Err(Error::TableNotActive(table.to_string()));
            }
            let key_item = key_from_item(&description, item_or_key)?;
            let key = encode_primary_key(&description, &key_item)?;
            let map_id = Self::table_map_id(&description.id);
            self.prefetch_table_write_roots(&tx, &description).await?;
            let current = maps.head(&map_id).await?;
            if current.as_ref().map(|version| &version.id) != expected && expected.is_some() {
                tx.rollback();
                return Ok(ItemWrite {
                    commit_id: None,
                    table_id: Some(description.id),
                    update: VersionedMapUpdate::Conflict { current },
                    old_item: None,
                });
            }
            let old_item = if capture_old || condition.is_some() {
                match maps.get(&map_id, &key).await? {
                    Some(bytes) => Some(decode_item(&self.blobs.resolve(&bytes).await?)?),
                    None => None,
                }
            } else {
                None
            };
            if let Some(condition) = condition {
                if !condition.evaluate(old_item.as_ref())? {
                    tx.rollback();
                    return Err(Error::ConditionalCheckFailed {
                        old_item: old_item.clone(),
                    });
                }
            }
            let before = current
                .map(|version| version.id)
                .ok_or_else(|| Error::CorruptData(format!("table {table:?} has no head")))?;
            if prepared_item.is_none() {
                if let Some(value) = encoded_item.clone() {
                    prepared_item = Some(self.blobs.prepare(value).await?);
                }
            }
            let (base_mutation, indexed_mutation) = match prepared_item.clone() {
                Some(value) => {
                    let indexed = prepare_index_source_record(
                        &description,
                        item_or_key,
                        value.clone(),
                        &self.blobs,
                    )
                    .await?;
                    (
                        Mutation::Upsert {
                            key: key.clone(),
                            val: value,
                        },
                        Mutation::Upsert { key, val: indexed },
                    )
                }
                None => (
                    Mutation::Delete { key: key.clone() },
                    Mutation::Delete { key },
                ),
            };
            let Some(current) = self
                .stage_indexed_table_mutations(
                    &tx,
                    &maps,
                    &description,
                    &before,
                    vec![base_mutation],
                    vec![indexed_mutation],
                )
                .await?
            else {
                tx.rollback();
                continue;
            };
            let update = if before == current.id {
                VersionedMapUpdate::Unchanged {
                    current: Some(current.clone()),
                }
            } else {
                VersionedMapUpdate::Applied {
                    previous: Some(before.clone()),
                    current: current.clone(),
                }
            };
            self.stage_single_table_commit(
                &tx,
                &description,
                commit_id.clone(),
                Some(before),
                Some(current.id),
                committed_at_millis,
            )
            .await?;
            match tx.commit().await? {
                TransactionUpdate::Applied { .. } => {
                    return Ok(ItemWrite {
                        commit_id: Some(commit_id),
                        table_id: Some(description.id),
                        update,
                        old_item: if capture_old { old_item } else { None },
                    })
                }
                TransactionUpdate::Conflict(_) if expected.is_some() => {
                    let current = self.engine.versioned_map(&map_id).head().await?;
                    return Ok(ItemWrite {
                        commit_id: None,
                        table_id: Some(description.id),
                        update: VersionedMapUpdate::Conflict { current },
                        old_item: None,
                    });
                }
                TransactionUpdate::Conflict(_) => continue,
            }
        }
        Err(Error::ConflictExhausted)
    }

    pub async fn head(&self, table: &str) -> Result<MapVersion> {
        let description = self.describe_table(table).await?;
        self.engine
            .versioned_map(Self::table_map_id(&description.id))
            .head()
            .await?
            .ok_or_else(|| Error::CorruptData(format!("table {table:?} has no head")))
    }

    /// Resolve one durable accepted transaction event by its immutable ID.
    ///
    /// A missing ID is distinct from a transaction whose table transitions
    /// were all no-ops; accepted no-op transactions still have a stored record.
    pub async fn commit(&self, id: &CommitId) -> Result<Option<TransactWriteResult>> {
        let result = match self
            .engine
            .load_named_root(COMMIT_CATALOG_ROOT_NAME)
            .await?
        {
            Some(tree) => self
                .engine
                .get(&tree, &id.0)
                .await?
                .map(|bytes| decode_commit_result(&bytes))
                .transpose()?,
            None => None,
        };
        if result
            .as_ref()
            .is_some_and(|result| &result.commit_id != id)
        {
            return Err(Error::CorruptData(format!(
                "commit key {id} contains another commit identity"
            )));
        }
        Ok(result)
    }

    /// List accepted events for the current table incarnation in sequence order.
    ///
    /// `after_sequence` is exclusive. A continuation is returned only when the
    /// pinned commit-log snapshot contains another record.
    pub async fn commits(
        &self,
        table: &str,
        after_sequence: Option<u64>,
        limit: usize,
    ) -> Result<TableCommitPage> {
        if limit == 0 || limit > MAX_COMMIT_PAGE_ITEMS {
            return Err(Error::Validation(format!(
                "commit page limit must be 1..={MAX_COMMIT_PAGE_ITEMS}"
            )));
        }
        let description = self.describe_table(table).await?;
        self.commits_for_description(table, &description, after_sequence, limit)
            .await
    }

    /// List one exact table incarnation's events. A name reused by a recreated
    /// table is rejected before selecting a commit-log map.
    pub async fn commits_for_incarnation(
        &self,
        table: &str,
        expected_table_id: &TableId,
        after_sequence: Option<u64>,
        limit: usize,
    ) -> Result<TableCommitPage> {
        if limit == 0 || limit > MAX_COMMIT_PAGE_ITEMS {
            return Err(Error::Validation(format!(
                "commit page limit must be 1..={MAX_COMMIT_PAGE_ITEMS}"
            )));
        }
        let description = self.describe_table(table).await?;
        ensure_table_incarnation(table, &description, expected_table_id)?;
        self.commits_for_description(table, &description, after_sequence, limit)
            .await
    }

    async fn commits_for_description(
        &self,
        table: &str,
        description: &TableDescription,
        after_sequence: Option<u64>,
        limit: usize,
    ) -> Result<TableCommitPage> {
        let log_name = Self::table_commit_log_root_name(&description.id);
        let tree = self
            .engine
            .load_named_root(&log_name)
            .await?
            .ok_or_else(|| Error::CorruptData(format!("table {table:?} has no commit log")))?;
        let cursor = after_sequence.map_or_else(
            || prolly::RangeCursor::after_key(COMMIT_SEQUENCE_KEY.to_vec()),
            |sequence| {
                let mut key = vec![1];
                key.extend_from_slice(&sequence.to_be_bytes());
                prolly::RangeCursor::after_key(key)
            },
        );
        let end = prolly::prefix_range([1]).1;
        let page = self
            .engine
            .range_page(&tree, &cursor, end.as_deref(), limit + 1)
            .await?;
        let has_more = page.entries.len() > limit;
        let mut entries = page.entries;
        if has_more {
            entries.pop();
        }
        let mut commits = Vec::with_capacity(entries.len());
        for (key, bytes) in entries {
            let encoded_sequence: [u8; 8] = key
                .get(1..)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| Error::CorruptData("malformed table commit key".into()))?;
            let record = decode_table_commit_record(&bytes)?;
            if record.sequence != u64::from_be_bytes(encoded_sequence) {
                return Err(Error::CorruptData(
                    "table commit key/record sequence mismatch".into(),
                ));
            }
            if record.transition.table_name != table || record.transition.table_id != description.id
            {
                return Err(Error::CorruptData(
                    "table commit record belongs to another table incarnation".into(),
                ));
            }
            commits.push(record);
        }
        let last_sequence = has_more
            .then(|| commits.last().map(|commit| commit.sequence))
            .flatten();
        Ok(TableCommitPage {
            commits,
            last_sequence,
            log_version: MapVersionId::for_tree(&tree)?,
        })
    }

    pub async fn restore(
        &self,
        table: &str,
        expected: &MapVersionId,
        target: &MapVersionId,
    ) -> Result<VersionedMapUpdate> {
        Ok(self.restore_result(table, expected, target).await?.update)
    }

    /// Restore a retained state and its audit event in one root transaction.
    pub async fn restore_result(
        &self,
        table: &str,
        expected: &MapVersionId,
        target: &MapVersionId,
    ) -> Result<RestoreResult> {
        self.restore_result_with_token(table, expected, target, None)
            .await
    }

    /// Idempotent restore extension. Replay returns the original transition,
    /// even if the table name is subsequently deleted or recreated.
    pub async fn restore_idempotent_result(
        &self,
        table: &str,
        expected: &MapVersionId,
        target: &MapVersionId,
        request_token: &str,
    ) -> Result<RestoreResult> {
        self.restore_result_with_token(table, expected, target, Some(request_token))
            .await
    }

    async fn restore_result_with_token(
        &self,
        table: &str,
        expected: &MapVersionId,
        target: &MapVersionId,
        request_token: Option<&str>,
    ) -> Result<RestoreResult> {
        validate_client_request_token(request_token)?;
        let fingerprint =
            canonical_fingerprint(b"DDB-RestoreTable-extension-v1", &(table, expected, target))?;
        let idempotency_key = request_token.map(idempotency_key);
        let commit_id = CommitId(self.ids.generate()?.0);
        let committed_at_millis = self.clock.now_millis();
        let tx = self.engine.begin_transaction()?;
        let maps = tx.versioned_maps_at_millis(committed_at_millis);
        self.prefetch_transaction_global_roots(&tx).await?;
        if let Some(key) = &idempotency_key {
            if let Some(bytes) = maps.get(IDEMPOTENCY_MAP_ID, key).await? {
                let record = decode_idempotency_record(&bytes)?;
                if committed_at_millis.saturating_sub(record.completed_at_millis)
                    <= IDEMPOTENCY_WINDOW_MILLIS
                {
                    tx.rollback();
                    if record.fingerprint != fingerprint {
                        return Err(Error::IdempotentParameterMismatch);
                    }
                    return self.restore_result_from_commit(table, record.result).await;
                }
            }
        }
        self.ensure_writes_unfenced(&tx, &maps).await?;
        let descriptor = maps
            .get(CATALOG_MAP_ID, table.as_bytes())
            .await?
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        let description = decode_description(&descriptor)?;
        if description.status != TableStatus::Active {
            tx.rollback();
            return Err(Error::TableNotActive(table.to_string()));
        }
        let map_id = Self::table_map_id(&description.id);
        let expected_manifest = self
            .transaction_table_snapshot_manifest(&tx, &description.id, expected)
            .await?
            .ok_or_else(|| Error::CorruptData("restore source has no snapshot manifest".into()))?;
        if expected_manifest.description != description {
            return Err(Error::CorruptData(
                "catalog schema disagrees with the current table version".into(),
            ));
        }
        let target_manifest = self
            .transaction_table_snapshot_manifest(&tx, &description.id, target)
            .await?
            .ok_or_else(|| {
                Error::Validation(format!(
                    "restore target {target} has no retained snapshot manifest"
                ))
            })?;
        let target_description = target_manifest.description.clone();
        if target_description.id != description.id
            || target_description.name != description.name
            || target_description.status != TableStatus::Active
        {
            return Err(Error::CorruptData(
                "restore target schema belongs to another or inactive table".into(),
            ));
        }
        let indexed_expected = expected_manifest.indexed.snapshot_id.clone();
        let indexed_target = target_manifest.indexed.snapshot_id.clone();
        let indexed = self
            .engine
            .indexed_map_with_policy(
                Self::table_indexed_source_id(&description.id),
                index_registry(&target_description)?,
                table_index_policy(),
            )
            .await?;
        let prepared_restore = indexed
            .prepare_restore_manifest(&indexed_expected, &target_manifest.indexed)
            .await?;
        match prepared_restore {
            AsyncPreparedIndexedUpdate::Prepared(index_update) => {
                if index_update.current().snapshot_id != indexed_target {
                    return Err(Error::CorruptData(
                        "indexed restore candidate selected another target".into(),
                    ));
                }
                if !matches!(
                    tx.compare_and_swap_named_root_at_millis(
                        index_update.root_name(),
                        index_update.expected_state_tree(),
                        Some(index_update.candidate_state_tree()),
                        committed_at_millis,
                    )
                    .await?,
                    NamedRootUpdate::Applied
                ) {
                    tx.rollback();
                    let current = self.engine.versioned_map(&map_id).head().await?;
                    return Ok(RestoreResult {
                        commit_id: None,
                        table_id: description.id,
                        update: VersionedMapUpdate::Conflict { current },
                    });
                }
            }
            AsyncPreparedIndexedUpdate::Unchanged { current } => {
                if current.snapshot_id != indexed_target || indexed_expected != indexed_target {
                    return Err(Error::CorruptData(
                        "indexed restore reported an inconsistent unchanged result".into(),
                    ));
                }
            }
            AsyncPreparedIndexedUpdate::Conflict { .. } => {
                tx.rollback();
                let current = self.engine.versioned_map(&map_id).head().await?;
                return Ok(RestoreResult {
                    commit_id: None,
                    table_id: description.id,
                    update: VersionedMapUpdate::Conflict { current },
                });
            }
        }
        let update = maps.restore_if(&map_id, Some(expected), target).await?;
        let (before, after) = match &update {
            VersionedMapUpdate::Applied { previous, current } => {
                let before = previous.clone().ok_or_else(|| {
                    Error::CorruptData(format!("table {table:?} has no previous head"))
                })?;
                (before, current.id.clone())
            }
            VersionedMapUpdate::Unchanged {
                current: Some(current),
            } => (current.id.clone(), current.id.clone()),
            VersionedMapUpdate::Unchanged { current: None } => {
                return Err(Error::CorruptData(format!("table {table:?} has no head")))
            }
            VersionedMapUpdate::Conflict { .. } => {
                tx.rollback();
                return Ok(RestoreResult {
                    commit_id: None,
                    table_id: description.id,
                    update,
                });
            }
        };
        let commit = self
            .stage_single_table_commit(
                &tx,
                &target_description,
                commit_id,
                Some(before),
                Some(after),
                committed_at_millis,
            )
            .await?;
        let encoded_target = encode_description(&target_description)?;
        maps.put(CATALOG_MAP_ID, table.as_bytes(), encoded_target.clone())
            .await?;
        maps.put(
            TABLE_DESCRIPTOR_MAP_ID,
            target_description.id.0.to_vec(),
            encoded_target,
        )
        .await?;
        if let Some(key) = &idempotency_key {
            maps.apply(
                IDEMPOTENCY_MAP_ID,
                vec![Mutation::Upsert {
                    key: key.clone(),
                    val: encode_idempotency_record(&IdempotencyRecord {
                        fingerprint,
                        completed_at_millis: committed_at_millis,
                        result: commit.clone(),
                    })?,
                }],
            )
            .await?;
        }
        match tx.commit().await {
            Ok(TransactionUpdate::Applied { .. }) => {
                self.restore_result_from_commit(table, commit).await
            }
            Ok(TransactionUpdate::Conflict(_)) => {
                let current = self.engine.versioned_map(&map_id).head().await?;
                Ok(RestoreResult {
                    commit_id: None,
                    table_id: description.id,
                    update: VersionedMapUpdate::Conflict { current },
                })
            }
            Err(source) => {
                if let Some(key) = &idempotency_key {
                    match self
                        .reconcile_idempotency_record(key, &fingerprint, self.clock.now_millis())
                        .await
                    {
                        Ok(Some(result)) => {
                            return self.restore_result_from_commit(table, result).await
                        }
                        Ok(None) | Err(Error::Storage(_)) => {}
                        Err(error) => return Err(error),
                    }
                }
                Err(Error::Storage(source))
            }
        }
    }

    async fn restore_result_from_commit(
        &self,
        table: &str,
        result: TransactWriteResult,
    ) -> Result<RestoreResult> {
        let transition = single_table_transition(table, &result)?;
        let description = self.description_for_transition(transition).await?;
        if description.name != table {
            return Err(Error::CorruptData(
                "restore commit resolved another table name".into(),
            ));
        }
        Ok(RestoreResult {
            commit_id: Some(result.commit_id.clone()),
            table_id: description.id.clone(),
            update: self
                .version_update_from_transition(&description, transition)
                .await?,
        })
    }
}

impl<S> Database<S>
where
    S: AsyncStore + AsyncManifestStore + AsyncManifestStoreScan + AsyncTransactionalStore + Clone,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    /// Plan a complete clean secondary-index generation set without mutating
    /// table or index visibility.
    pub async fn plan_index_reconfiguration(
        &self,
        table: &str,
        desired: Vec<SecondaryIndexDefinition>,
    ) -> Result<IndexReconfigurationPlan> {
        for _ in 0..=self.logical_retry_limit {
            let before = self.describe_table(table).await?;
            let head = self.head(table).await?;
            let sequence = self.table_commit_sequence(&before.id).await?;
            let after = reconfigured_description(&before, desired.clone())?;
            if after == before {
                return Err(Error::Validation(
                    "secondary-index reconfiguration does not change the table schema".into(),
                ));
            }
            if self.describe_table(table).await? != before
                || self.head(table).await?.id != head.id
                || self.table_commit_sequence(&before.id).await? != sequence
            {
                continue;
            }
            let mut plan = IndexReconfigurationPlan {
                id: IndexReconfigurationPlanId([0; 32]),
                table_name: table.to_string(),
                table_id: before.id.clone(),
                expected_head: head.id,
                expected_commit_sequence: sequence,
                before,
                after,
                planned_at_millis: self.clock.now_millis(),
            };
            plan.id = index_reconfiguration_plan_id(&plan)?;
            return Ok(plan);
        }
        Err(Error::ConflictExhausted)
    }

    /// Build, verify, and atomically activate an exact planned index set.
    /// Shadow nodes are unreachable until the final strict root transaction.
    pub async fn apply_index_reconfiguration(
        &self,
        plan: &IndexReconfigurationPlan,
        context: MaintenanceContext,
    ) -> Result<IndexReconfigurationResult> {
        context.validate()?;
        validate_index_reconfiguration_plan(plan)?;
        if let Some(record) = self.index_reconfiguration_audit(&plan.id).await? {
            if record.plan != *plan || record.context != context {
                return Err(Error::IdempotentParameterMismatch);
            }
            let mut result = record.result;
            result.replayed = true;
            return Ok(result);
        }
        if self.describe_table(&plan.table_name).await? != plan.before
            || self.head(&plan.table_name).await?.id != plan.expected_head
            || self.table_commit_sequence(&plan.table_id).await? != plan.expected_commit_sequence
        {
            return Err(Error::MaintenancePlanStale(
                "table schema, head, or commit sequence changed after planning".into(),
            ));
        }

        let base = self
            .engine
            .versioned_map(Self::table_map_id(&plan.table_id))
            .snapshot_at(&plan.expected_head)
            .await?
            .ok_or_else(|| {
                Error::MaintenancePlanStale("planned table version is no longer retained".into())
            })?;
        let marker = base
            .get(TABLE_SCHEMA_RECORD_KEY)
            .await?
            .ok_or_else(|| Error::CorruptData("table schema-version record is absent".into()))?;
        if marker != encode_table_schema_record(&plan.before)? {
            return Err(Error::MaintenancePlanStale(
                "table schema-version record changed after planning".into(),
            ));
        }
        let budget = prolly::MaintenanceBudget::default();
        let source = self
            .build_index_source(&plan.after, base.tree(), &budget)
            .await?;
        let planned_manifest = self
            .table_snapshot_manifest_at_version(&plan.table_id, &plan.expected_head)
            .await?
            .ok_or_else(|| {
                Error::CorruptData("planned base version has no snapshot manifest".into())
            })?;
        if planned_manifest.1.description != plan.before {
            return Err(Error::MaintenancePlanStale(
                "planned snapshot manifest schema changed".into(),
            ));
        }
        let indexed_before = planned_manifest.1.indexed.snapshot_id;
        let indexed = self
            .engine
            .indexed_map_with_policy(
                Self::table_indexed_source_id(&plan.table_id),
                index_registry(&plan.after)?,
                table_index_policy(),
            )
            .await?;
        let prepared = match indexed
            .prepare_rebuild_from_source_at(
                &indexed_before,
                source,
                index_registry(&plan.after)?,
                &budget,
            )
            .await?
        {
            AsyncPreparedIndexedUpdate::Prepared(prepared) => prepared,
            AsyncPreparedIndexedUpdate::Conflict { .. } => {
                return Err(Error::MaintenancePlanStale(
                    "indexed collection changed after planning".into(),
                ))
            }
            AsyncPreparedIndexedUpdate::Unchanged { .. } => {
                return Err(Error::CorruptData(
                    "changed index schema produced an unchanged indexed closure".into(),
                ))
            }
        };
        let indexed_after = prepared.current().source.id.clone();
        let indexed_snapshot_after = prepared.current().snapshot_id.clone();
        let completed_at_millis = self.clock.now_millis().max(plan.planned_at_millis);
        let commit_id = CommitId(self.ids.generate()?.0);
        let tx = self.engine.begin_transaction()?;
        let maps = tx.versioned_maps_at_millis(completed_at_millis);
        self.ensure_writes_unfenced(&tx, &maps).await?;
        if let Some(bytes) = maps.get(INDEX_RECONFIG_AUDIT_MAP_ID, &plan.id.0).await? {
            let record = decode_index_reconfiguration_audit(&bytes)?;
            tx.rollback();
            if record.plan != *plan || record.context != context {
                return Err(Error::IdempotentParameterMismatch);
            }
            let mut result = record.result;
            result.replayed = true;
            return Ok(result);
        }
        let descriptor = maps
            .get(CATALOG_MAP_ID, plan.table_name.as_bytes())
            .await?
            .ok_or_else(|| Error::MaintenancePlanStale("table name no longer exists".into()))?;
        if decode_description(&descriptor)? != plan.before {
            tx.rollback();
            return Err(Error::MaintenancePlanStale(
                "table descriptor changed after planning".into(),
            ));
        }
        let sequence = self
            .transaction_table_commit_sequence(&tx, &plan.table_id)
            .await?
            .ok_or_else(|| Error::CorruptData("table commit log has no sequence".into()))?;
        if sequence != plan.expected_commit_sequence {
            tx.rollback();
            return Err(Error::MaintenancePlanStale(
                "table commit sequence advanced after planning".into(),
            ));
        }
        let table_map_id = Self::table_map_id(&plan.table_id);
        let head = maps
            .head(&table_map_id)
            .await?
            .ok_or_else(|| Error::CorruptData("table has no transaction-visible head".into()))?;
        if head.id != plan.expected_head {
            tx.rollback();
            return Err(Error::MaintenancePlanStale(
                "table head changed after planning".into(),
            ));
        }
        let manifest = self
            .transaction_table_snapshot_manifest(&tx, &plan.table_id, &plan.expected_head)
            .await?
            .ok_or_else(|| Error::CorruptData("table head has no snapshot manifest".into()))?;
        if manifest.indexed.snapshot_id != indexed_before {
            tx.rollback();
            return Err(Error::MaintenancePlanStale(
                "base/index pairing changed after planning".into(),
            ));
        }
        if manifest.description != plan.before {
            tx.rollback();
            return Err(Error::MaintenancePlanStale(
                "base/schema pairing changed after planning".into(),
            ));
        }
        if !matches!(
            tx.compare_and_swap_named_root_at_millis(
                prepared.root_name(),
                prepared.expected_state_tree(),
                Some(prepared.candidate_state_tree()),
                completed_at_millis,
            )
            .await?,
            NamedRootUpdate::Applied
        ) {
            tx.rollback();
            return Err(Error::MaintenancePlanStale(
                "indexed collection changed during activation".into(),
            ));
        }
        let after = maps
            .apply(
                &table_map_id,
                vec![Mutation::Upsert {
                    key: TABLE_SCHEMA_RECORD_KEY.to_vec(),
                    val: encode_table_schema_record(&plan.after)?,
                }],
            )
            .await?;
        if after.id == plan.expected_head {
            return Err(Error::CorruptData(
                "index schema activation did not create a distinct table version".into(),
            ));
        }
        self.stage_table_snapshot_manifest(
            &tx,
            &plan.after,
            &after.id,
            prepared.manifest(),
            completed_at_millis,
        )
        .await?;
        let encoded = encode_description(&plan.after)?;
        maps.put(CATALOG_MAP_ID, plan.table_name.as_bytes(), encoded.clone())
            .await?;
        maps.put(TABLE_DESCRIPTOR_MAP_ID, plan.table_id.0.to_vec(), encoded)
            .await?;
        let commit = self
            .stage_single_table_commit(
                &tx,
                &plan.after,
                commit_id.clone(),
                Some(plan.expected_head.clone()),
                Some(after.id.clone()),
                completed_at_millis,
            )
            .await?;
        let result = IndexReconfigurationResult {
            plan_id: plan.id.clone(),
            description: plan.after.clone(),
            version: after.id,
            indexed_source_version: indexed_after,
            indexed_snapshot_id: indexed_snapshot_after,
            commit_id,
            completed_at_millis,
            replayed: false,
        };
        debug_assert_eq!(commit.commit_id, result.commit_id);
        let audit = IndexReconfigurationAuditRecord {
            plan: plan.clone(),
            context,
            result: result.clone(),
        };
        maps.put(
            INDEX_RECONFIG_AUDIT_MAP_ID,
            plan.id.0.to_vec(),
            encode_index_reconfiguration_audit(&audit)?,
        )
        .await?;
        match tx.commit().await {
            Ok(TransactionUpdate::Applied { .. }) => Ok(result),
            Ok(TransactionUpdate::Conflict(_)) => Err(Error::MaintenancePlanStale(
                "index activation transaction conflicted".into(),
            )),
            Err(source) => match self.index_reconfiguration_audit(&plan.id).await {
                Ok(Some(stored)) if stored == audit => {
                    let mut result = stored.result;
                    result.replayed = true;
                    Ok(result)
                }
                _ => Err(Error::Storage(source)),
            },
        }
    }

    /// Resolve durable evidence for an index reconfiguration plan.
    pub async fn index_reconfiguration_audit(
        &self,
        id: &IndexReconfigurationPlanId,
    ) -> Result<Option<IndexReconfigurationAuditRecord>> {
        self.engine
            .versioned_map(INDEX_RECONFIG_AUDIT_MAP_ID)
            .get(&id.0)
            .await?
            .map(|bytes| decode_index_reconfiguration_audit(&bytes))
            .transpose()
    }

    /// Compute an exact bounded retention batch without mutating storage.
    ///
    /// Planning is linear in the version count but memory is bounded by the
    /// policy's `keep_last` value plus [`MAX_RETENTION_REMOVALS`]. The table's
    /// durable commit sequence is sampled before and after enumeration so an
    /// intervening write, including a head ABA, causes a complete retry.
    pub async fn plan_retention(
        &self,
        table: &str,
        policy: RetentionPolicy,
    ) -> Result<RetentionPlan> {
        if policy.keep_last > MAX_COLLECTED_VERSIONS {
            return Err(Error::Validation(format!(
                "retention keep_last must be <={MAX_COLLECTED_VERSIONS}"
            )));
        }
        if policy.protected_versions.len() > MAX_RETENTION_PROTECTED_VERSIONS {
            return Err(Error::Validation(format!(
                "retention protected version count must be <={MAX_RETENTION_PROTECTED_VERSIONS}"
            )));
        }

        for _ in 0..=self.logical_retry_limit {
            let description = self.describe_table(table).await?;
            let map = self
                .engine
                .versioned_map(Self::table_map_id(&description.id));
            let head = map
                .head()
                .await?
                .ok_or_else(|| Error::CorruptData(format!("table {table:?} has no head")))?;
            let sequence_before = self.table_commit_sequence(&description.id).await?;
            // The current head is always retained and consumes one keep-last
            // slot. Only select additional historical versions here.
            let additional_keep = policy.keep_last.saturating_sub(1);
            let mut newest = Vec::<MapVersion>::with_capacity(additional_keep);
            let mut candidates = BTreeMap::<MapVersionId, ()>::new();
            let mut seen_protected = BTreeSet::new();
            let mut removable_count = 0_u64;
            let mut examined_versions = 0_u64;
            let mut cursor = None;

            loop {
                let page = map
                    .versions_page(cursor.as_ref(), MAX_VERSION_PAGE_ITEMS)
                    .await?;
                for version in page.versions {
                    examined_versions = examined_versions.checked_add(1).ok_or_else(|| {
                        Error::Validation("retention version count exhausted u64".into())
                    })?;
                    if policy.protected_versions.contains(&version.id) {
                        seen_protected.insert(version.id.clone());
                    }
                    if version.id == head.id {
                        continue;
                    }
                    if additional_keep == 0 {
                        consider_retention_candidate(
                            &policy,
                            &head.id,
                            version,
                            &mut candidates,
                            &mut removable_count,
                        )?;
                        continue;
                    }

                    newest.push(version);
                    newest.sort_by(version_newest_cmp);
                    if newest.len() > additional_keep {
                        let evicted = newest.pop().expect("length exceeded keep_last");
                        consider_retention_candidate(
                            &policy,
                            &head.id,
                            evicted,
                            &mut candidates,
                            &mut removable_count,
                        )?;
                    }
                }
                match page.next_cursor {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }

            // Extension-token replays reconstruct exact return images from the
            // transition's immutable before/after versions. Retention must not
            // collect either side while the durable token remains inside the
            // advertised ten-minute replay window. Filtering only the bounded
            // candidate set keeps planner memory independent of token volume.
            let planned_at_millis = self.clock.now_millis();
            self.protect_live_idempotency_versions(
                &description.id,
                planned_at_millis,
                &mut candidates,
            )
            .await?;

            let missing = policy
                .protected_versions
                .difference(&seen_protected)
                .cloned()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                return Err(Error::Validation(format!(
                    "retention protects unknown versions: {missing:?}"
                )));
            }

            let current_description = self.describe_table(table).await?;
            let current_head = self.head(table).await?;
            let sequence_after = self.table_commit_sequence(&description.id).await?;
            if current_description.id != description.id
                || current_head.id != head.id
                || sequence_after != sequence_before
            {
                continue;
            }

            let remove = candidates.into_keys().collect::<Vec<_>>();
            let mut plan = RetentionPlan {
                id: RetentionPlanId([0; 32]),
                table_name: table.to_string(),
                table_id: description.id,
                expected_head: head.id,
                expected_commit_sequence: sequence_before,
                policy: policy.clone(),
                remove,
                examined_versions,
                more_removable: removable_count
                    > u64::try_from(MAX_RETENTION_REMOVALS).expect("constant fits u64"),
                planned_at_millis,
            };
            plan.id = retention_plan_id(&plan)?;
            return Ok(plan);
        }
        Err(Error::ConflictExhausted)
    }

    /// Execute one exact dry-run plan and durably audit it in the same root
    /// transaction as the version-root deletions.
    pub async fn apply_retention(
        &self,
        plan: &RetentionPlan,
        context: MaintenanceContext,
    ) -> Result<RetentionResult> {
        context.validate()?;
        validate_retention_plan(plan)?;
        if let Some(record) = self.retention_audit(&plan.id).await? {
            if record.plan != *plan || record.context != context {
                return Err(Error::IdempotentParameterMismatch);
            }
            return Ok(RetentionResult {
                plan_id: plan.id.clone(),
                removed: plan.remove.clone(),
                completed_at_millis: record.completed_at_millis,
                replayed: true,
            });
        }

        let completed_at_millis = self.clock.now_millis();
        let tx = self.engine.begin_transaction()?;
        let maps = tx.versioned_maps_at_millis(completed_at_millis);
        self.ensure_writes_unfenced(&tx, &maps).await?;
        if let Some(bytes) = maps.get(MAINTENANCE_AUDIT_MAP_ID, &plan.id.0).await? {
            let record = decode_retention_audit_record(&bytes)?;
            tx.rollback();
            if record.plan != *plan || record.context != context {
                return Err(Error::IdempotentParameterMismatch);
            }
            return Ok(RetentionResult {
                plan_id: plan.id.clone(),
                removed: plan.remove.clone(),
                completed_at_millis: record.completed_at_millis,
                replayed: true,
            });
        }

        let descriptor = maps
            .get(CATALOG_MAP_ID, plan.table_name.as_bytes())
            .await?
            .ok_or_else(|| Error::MaintenancePlanStale("table name no longer exists".into()))?;
        let description = decode_description(&descriptor)?;
        if description.id != plan.table_id {
            tx.rollback();
            return Err(Error::MaintenancePlanStale(
                "table name now identifies another incarnation".into(),
            ));
        }
        let sequence = self
            .transaction_table_commit_sequence(&tx, &plan.table_id)
            .await?
            .ok_or_else(|| Error::CorruptData("table commit log has no sequence".into()))?;
        if sequence != plan.expected_commit_sequence {
            tx.rollback();
            return Err(Error::MaintenancePlanStale(
                "table commit sequence advanced after planning".into(),
            ));
        }
        let table_map = self
            .engine
            .versioned_map(Self::table_map_id(&plan.table_id));
        let head = maps
            .head(table_map.id())
            .await?
            .ok_or_else(|| Error::CorruptData("table has no transaction-visible head".into()))?;
        if head.id != plan.expected_head {
            tx.rollback();
            return Err(Error::MaintenancePlanStale(
                "table head changed after planning".into(),
            ));
        }

        let mut manifest_versions = Vec::with_capacity(plan.remove.len() + 1);
        manifest_versions.push(head.id.clone());
        manifest_versions.extend(plan.remove.iter().cloned());
        let mut manifests = self
            .transaction_required_table_snapshot_manifests(&tx, &plan.table_id, &manifest_versions)
            .await?
            .into_iter();
        let current_manifest = manifests.next().ok_or_else(|| {
            Error::CorruptData("retention table head has no snapshot manifest".into())
        })?;
        if current_manifest.description != description {
            return Err(Error::CorruptData(
                "retention table head and catalog schemas disagree".into(),
            ));
        }
        let current_indexed = current_manifest.indexed.snapshot_id;
        for (id, manifest) in plan.remove.iter().zip(manifests) {
            if manifest.description.id != plan.table_id {
                return Err(Error::CorruptData(format!(
                    "retained base version {id} manifest belongs to another table"
                )));
            }
        }
        let indexed = self
            .engine
            .indexed_map_with_policy(
                Self::table_indexed_source_id(&description.id),
                index_registry(&description)?,
                table_index_policy(),
            )
            .await?;
        if indexed.snapshot().await?.id() != &current_indexed {
            return Err(Error::CorruptData(
                "retention table and indexed heads disagree".into(),
            ));
        }

        for id in &plan.remove {
            let name = table_map.version_root_name(id);
            let tree = tx.load_named_root(&name).await?.ok_or_else(|| {
                Error::MaintenancePlanStale(format!("planned version {id} is already absent"))
            })?;
            if MapVersionId::for_tree(&tree)? != *id {
                tx.rollback();
                return Err(Error::CorruptData(format!(
                    "planned version root {id} contains different content"
                )));
            }
            tx.delete_named_root(&name).await?;
        }
        if !plan.remove.is_empty() {
            let catalog_name = Self::table_snapshot_catalog_root_name(&plan.table_id);
            if let Some(catalog) = tx.load_named_root(&catalog_name).await? {
                let next = tx
                    .batch(
                        &catalog,
                        plan.remove
                            .iter()
                            .map(|id| Mutation::Delete {
                                key: id.as_cid().as_bytes().to_vec(),
                            })
                            .collect(),
                    )
                    .await?;
                if next != catalog {
                    tx.publish_named_root_at_millis(&catalog_name, &next, completed_at_millis)
                        .await?;
                }
            }
        }
        let record = RetentionAuditRecord {
            plan: plan.clone(),
            context,
            completed_at_millis,
        };
        maps.apply(
            MAINTENANCE_AUDIT_MAP_ID,
            vec![Mutation::Upsert {
                key: plan.id.0.to_vec(),
                val: encode_retention_audit_record(&record)?,
            }],
        )
        .await?;

        match tx.commit().await {
            Ok(TransactionUpdate::Applied { .. }) => Ok(RetentionResult {
                plan_id: plan.id.clone(),
                removed: plan.remove.clone(),
                completed_at_millis,
                replayed: false,
            }),
            Ok(TransactionUpdate::Conflict(_)) => Err(Error::MaintenancePlanStale(
                "retention transaction conflicted with concurrent state".into(),
            )),
            Err(source) => match self.retention_audit(&plan.id).await {
                Ok(Some(stored)) if stored == record => Ok(RetentionResult {
                    plan_id: plan.id.clone(),
                    removed: plan.remove.clone(),
                    completed_at_millis: stored.completed_at_millis,
                    replayed: true,
                }),
                _ => Err(Error::Storage(source)),
            },
        }
    }

    /// Resolve one durable retention audit record by plan identity.
    pub async fn retention_audit(
        &self,
        id: &RetentionPlanId,
    ) -> Result<Option<RetentionAuditRecord>> {
        let record = self
            .engine
            .versioned_map(MAINTENANCE_AUDIT_MAP_ID)
            .get(&id.0)
            .await?
            .map(|bytes| decode_retention_audit_record(&bytes))
            .transpose()?;
        if record.as_ref().is_some_and(|record| &record.plan.id != id) {
            return Err(Error::CorruptData(
                "retention audit key contains another plan identity".into(),
            ));
        }
        Ok(record)
    }

    async fn table_commit_sequence(&self, table_id: &TableId) -> Result<u64> {
        let name = Self::table_commit_log_root_name(table_id);
        let tree = self
            .engine
            .load_named_root(&name)
            .await?
            .ok_or_else(|| Error::CorruptData("table commit log has no sequence".into()))?;
        let bytes = self
            .engine
            .get(&tree, COMMIT_SEQUENCE_KEY)
            .await?
            .ok_or_else(|| Error::CorruptData("table commit log has no sequence".into()))?;
        decode_commit_sequence(&bytes)
    }

    async fn transaction_table_commit_sequence(
        &self,
        tx: &AsyncProllyTransaction<'_, S>,
        table_id: &TableId,
    ) -> Result<Option<u64>> {
        let name = Self::table_commit_log_root_name(table_id);
        let Some(tree) = tx.load_named_root(&name).await? else {
            return Ok(None);
        };
        tx.get(&tree, COMMIT_SEQUENCE_KEY)
            .await?
            .map(|bytes| decode_commit_sequence(&bytes))
            .transpose()
    }

    async fn protect_live_idempotency_versions(
        &self,
        table_id: &TableId,
        now_millis: u64,
        candidates: &mut BTreeMap<MapVersionId, ()>,
    ) -> Result<()> {
        if candidates.is_empty() {
            return Ok(());
        }
        let Some(snapshot) = self
            .engine
            .versioned_map(IDEMPOTENCY_MAP_ID)
            .snapshot()
            .await?
        else {
            return Ok(());
        };
        let mut cursor = prolly::RangeCursor::start();
        loop {
            let page = snapshot
                .range_page(&cursor, None, MAX_VERSION_PAGE_ITEMS)
                .await?;
            for (_, bytes) in page.entries {
                let record = decode_idempotency_record(&bytes)?;
                if now_millis.saturating_sub(record.completed_at_millis) > IDEMPOTENCY_WINDOW_MILLIS
                {
                    continue;
                }
                for transition in record
                    .result
                    .transitions
                    .iter()
                    .filter(|transition| &transition.table_id == table_id)
                {
                    if let Some(before) = &transition.before {
                        candidates.remove(before);
                    }
                    if let Some(after) = &transition.after {
                        candidates.remove(after);
                    }
                }
                if candidates.is_empty() {
                    return Ok(());
                }
            }
            match page.next_cursor {
                Some(next) => cursor = next,
                None => return Ok(()),
            }
        }
    }

    pub async fn versions(&self, table: &str) -> Result<Vec<MapVersion>> {
        let mut cursor = None;
        let mut versions = Vec::new();
        loop {
            let page = self
                .versions_page(table, cursor.as_ref(), MAX_VERSION_PAGE_ITEMS)
                .await?;
            if versions.len().saturating_add(page.versions.len()) > MAX_COLLECTED_VERSIONS {
                return Err(Error::Validation(format!(
                    "version list exceeds the bounded collection limit of {MAX_COLLECTED_VERSIONS}; use versions_page or the client versions paginator"
                )));
            }
            versions.extend(page.versions);
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => {
                    versions.sort_by(|left, right| {
                        right
                            .created_at_millis
                            .cmp(&left.created_at_millis)
                            .then_with(|| {
                                left.id
                                    .as_cid()
                                    .as_bytes()
                                    .cmp(right.id.as_cid().as_bytes())
                            })
                    });
                    return Ok(versions);
                }
            }
        }
    }

    /// List one bounded page of this table incarnation's immutable versions.
    pub async fn versions_page(
        &self,
        table: &str,
        cursor: Option<&MapVersionCursor>,
        limit: usize,
    ) -> Result<MapVersionPage> {
        if limit == 0 || limit > MAX_VERSION_PAGE_ITEMS {
            return Err(Error::Validation(format!(
                "version page limit must be 1..={MAX_VERSION_PAGE_ITEMS}"
            )));
        }
        let description = self.describe_table(table).await?;
        Ok(self
            .engine
            .versioned_map(Self::table_map_id(&description.id))
            .versions_page(cursor, limit)
            .await?)
    }

    pub async fn diff(
        &self,
        table: &str,
        base: &MapVersionId,
        target: &MapVersionId,
    ) -> Result<Vec<prolly::Diff>> {
        let mut cursor = None;
        let mut diffs = Vec::new();
        loop {
            let page = self
                .structural_diff_page(table, base, target, cursor.as_ref(), MAX_DIFF_PAGE_ITEMS)
                .await?;
            if diffs.len().saturating_add(page.diffs.len()) > MAX_COLLECTED_DIFF_ITEMS {
                return Err(Error::Validation(format!(
                    "diff exceeds the bounded collection limit of {MAX_COLLECTED_DIFF_ITEMS}; use structural_diff_page or the client diff paginator"
                )));
            }
            diffs.extend(page.diffs);
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => return Ok(diffs),
            }
        }
    }

    /// Read one bounded, resumable structural diff page.
    ///
    /// The cursor is tied cryptographically to `base` and `target` by their
    /// immutable roots. Reusing it for another pair fails closed in the engine.
    pub async fn structural_diff_page(
        &self,
        table: &str,
        base: &MapVersionId,
        target: &MapVersionId,
        cursor: Option<&StructuralDiffCursor>,
        limit: usize,
    ) -> Result<StructuralDiffPage> {
        if limit == 0 || limit > MAX_DIFF_PAGE_ITEMS {
            return Err(Error::Validation(format!(
                "diff page limit must be 1..={MAX_DIFF_PAGE_ITEMS}"
            )));
        }
        let description = self.describe_table(table).await?;
        let mut page = self
            .engine
            .versioned_map(Self::table_map_id(&description.id))
            .structural_diff_page(base, target, cursor, limit)
            .await?;
        page.diffs
            .retain(|diff| diff.key() != TABLE_SCHEMA_RECORD_KEY);
        Ok(page)
    }
}

fn version_newest_cmp(left: &MapVersion, right: &MapVersion) -> std::cmp::Ordering {
    right
        .created_at_millis
        .cmp(&left.created_at_millis)
        .then_with(|| left.id.cmp(&right.id))
}

fn table_key_definitions(
    partition_key: &crate::KeyAttribute,
    sort_key: Option<&crate::KeyAttribute>,
) -> BTreeMap<String, crate::KeyKind> {
    let mut definitions = BTreeMap::from([(partition_key.name.clone(), partition_key.kind)]);
    if let Some(sort_key) = sort_key {
        definitions.insert(sort_key.name.clone(), sort_key.kind);
    }
    definitions
}

fn consider_retention_candidate(
    policy: &RetentionPolicy,
    head: &MapVersionId,
    version: MapVersion,
    candidates: &mut BTreeMap<MapVersionId, ()>,
    removable_count: &mut u64,
) -> Result<()> {
    if &version.id == head || policy.protected_versions.contains(&version.id) {
        return Ok(());
    }
    let Some(created_at_millis) = version.created_at_millis else {
        // Missing timestamps are retained because age cannot be proven.
        return Ok(());
    };
    if policy
        .keep_since_millis
        .is_some_and(|cutoff| created_at_millis >= cutoff)
    {
        return Ok(());
    }
    *removable_count = removable_count
        .checked_add(1)
        .ok_or_else(|| Error::Validation("retention removable count exhausted u64".into()))?;
    candidates.insert(version.id, ());
    if candidates.len() > MAX_RETENTION_REMOVALS {
        candidates.pop_last();
    }
    Ok(())
}

fn key_from_item(description: &TableDescription, item: &Item) -> Result<Item> {
    let mut key = Item::new();
    let partition = item.get(&description.partition_key.name).ok_or_else(|| {
        Error::Validation(format!(
            "item is missing partition key {:?}",
            description.partition_key.name
        ))
    })?;
    key.insert(description.partition_key.name.clone(), partition.clone());
    if let Some(sort) = &description.sort_key {
        let value = item.get(&sort.name).ok_or_else(|| {
            Error::Validation(format!("item is missing sort key {:?}", sort.name))
        })?;
        key.insert(sort.name.clone(), value.clone());
    }
    Ok(key)
}

fn validate_ttl_attribute(description: &TableDescription, ttl_attribute: &str) -> Result<()> {
    if ttl_attribute.is_empty() || ttl_attribute.len() > 255 {
        return Err(Error::Validation(
            "TTL attribute name must contain 1..=255 bytes".into(),
        ));
    }
    if ttl_attribute == description.partition_key.name
        || description
            .sort_key
            .as_ref()
            .is_some_and(|key| ttl_attribute == key.name)
    {
        return Err(Error::Validation(
            "TTL attribute cannot be a table primary-key attribute".into(),
        ));
    }
    Ok(())
}

fn ensure_table_incarnation(
    table: &str,
    description: &TableDescription,
    expected_table_id: &TableId,
) -> Result<()> {
    if &description.id != expected_table_id {
        return Err(Error::TableIncarnationChanged {
            table: table.to_owned(),
        });
    }
    Ok(())
}

fn ttl_expiration_is_eligible(expiration: &crate::DynamoNumber, now_epoch_seconds: u64) -> bool {
    let text = expiration.as_str();
    if text.contains('.') || text.starts_with('-') {
        return false;
    }
    let Ok(expiry) = text.parse::<u64>() else {
        return false;
    };
    expiry <= now_epoch_seconds && expiry >= now_epoch_seconds.saturating_sub(TTL_MAX_PAST_SECONDS)
}

fn key_condition_bounds(
    description: &TableDescription,
    condition: &KeyCondition,
) -> Result<(prolly::RangeCursor, Option<Vec<u8>>)> {
    if condition.partition_name != description.partition_key.name {
        return Err(Error::Validation(format!(
            "key condition partition attribute {:?} does not match table partition key {:?}",
            condition.partition_name, description.partition_key.name
        )));
    }
    let partition = Item::from([(
        condition.partition_name.clone(),
        condition.partition_value.clone(),
    )]);
    let prefix = encode_partition_prefix(description, &partition)?;
    let (_, partition_end) = prolly::prefix_range(&prefix);
    let before = |key: &[u8]| {
        let mut predecessor = key.to_vec();
        predecessor.pop();
        prolly::RangeCursor::after_key(predecessor)
    };
    let Some((sort_name, sort_condition)) = &condition.sort else {
        if description.sort_key.is_some() {
            return Ok((before(&prefix), partition_end));
        }
        return Ok((before(&prefix), prolly::prefix_range(&prefix).1));
    };
    let sort_key = description
        .sort_key
        .as_ref()
        .ok_or_else(|| Error::Validation("sort-key condition used on a hash-only table".into()))?;
    if sort_name != &sort_key.name {
        return Err(Error::Validation(format!(
            "key condition sort attribute {sort_name:?} does not match table sort key {:?}",
            sort_key.name
        )));
    }
    let full_key = |value: &AttributeValue| {
        encode_primary_key(
            description,
            &Item::from([
                (
                    description.partition_key.name.clone(),
                    condition.partition_value.clone(),
                ),
                (sort_key.name.clone(), value.clone()),
            ]),
        )
    };
    Ok(match sort_condition {
        SortKeyCondition::Equal(value) => {
            let key = full_key(value)?;
            (before(&key), prolly::prefix_range(&key).1)
        }
        SortKeyCondition::LessThan(value) => (before(&prefix), Some(full_key(value)?)),
        SortKeyCondition::LessThanOrEqual(value) => {
            let key = full_key(value)?;
            (before(&prefix), prolly::prefix_range(&key).1)
        }
        SortKeyCondition::GreaterThan(value) => (
            prolly::RangeCursor::after_key(full_key(value)?),
            partition_end,
        ),
        SortKeyCondition::GreaterThanOrEqual(value) => (before(&full_key(value)?), partition_end),
        SortKeyCondition::Between(lower, upper) => {
            let lower = full_key(lower)?;
            let upper = full_key(upper)?;
            if lower > upper {
                return Err(Error::Validation(
                    "sort-key BETWEEN lower bound exceeds upper bound".into(),
                ));
            }
            (before(&lower), prolly::prefix_range(&upper).1)
        }
        SortKeyCondition::BeginsWith(value) => {
            if !matches!(value, AttributeValue::S(value) if !value.is_empty())
                && !matches!(value, AttributeValue::B(value) if !value.is_empty())
            {
                return Err(Error::Validation(
                    "begins_with sort-key operand must be a non-empty string or binary value"
                        .into(),
                ));
            }
            let mut key_prefix = full_key(value)?;
            if !key_prefix.ends_with(&[0, 0]) {
                return Err(Error::CorruptData(
                    "encoded sort key is missing its terminator".into(),
                ));
            }
            key_prefix.truncate(key_prefix.len() - 2);
            let end = prolly::prefix_range(&key_prefix).1;
            (prolly::RangeCursor::after_key(key_prefix), end)
        }
    })
}

enum IndexQueryBounds {
    Exact(Vec<u8>),
    Prefix(Vec<u8>),
    Range(Vec<u8>, Option<Vec<u8>>),
}

fn index_condition_bounds(
    index: &SecondaryIndexDescription,
    condition: &KeyCondition,
) -> Result<IndexQueryBounds> {
    if condition.partition_name != index.partition_key.name {
        return Err(Error::Validation(format!(
            "key condition partition attribute {:?} does not match index partition key {:?}",
            condition.partition_name, index.partition_key.name
        )));
    }
    let partition_item = Item::from([(
        index.partition_key.name.clone(),
        condition.partition_value.clone(),
    )]);
    let partition = encode_key_schema(&index.partition_key, None, &partition_item)?;
    let Some((sort_name, sort_condition)) = &condition.sort else {
        return Ok(IndexQueryBounds::Prefix(partition));
    };
    let sort = index.sort_key.as_ref().ok_or_else(|| {
        Error::Validation("sort-key condition used on a hash-only secondary index".into())
    })?;
    if sort_name != &sort.name {
        return Err(Error::Validation(format!(
            "key condition sort attribute {sort_name:?} does not match index sort key {:?}",
            sort.name
        )));
    }
    let full = |value: &AttributeValue| {
        encode_key_schema(
            &index.partition_key,
            Some(sort),
            &Item::from([
                (
                    index.partition_key.name.clone(),
                    condition.partition_value.clone(),
                ),
                (sort.name.clone(), value.clone()),
            ]),
        )
    };
    Ok(match sort_condition {
        SortKeyCondition::Equal(value) => IndexQueryBounds::Exact(full(value)?),
        SortKeyCondition::LessThan(value) => IndexQueryBounds::Range(partition, Some(full(value)?)),
        SortKeyCondition::LessThanOrEqual(value) => {
            let key = full(value)?;
            IndexQueryBounds::Range(partition, prolly::prefix_range(&key).1)
        }
        SortKeyCondition::GreaterThan(value) => {
            let key = full(value)?;
            let start = prolly::prefix_range(&key).1.ok_or_else(|| {
                Error::Validation("sort-key lower bound has no byte successor".into())
            })?;
            IndexQueryBounds::Range(start, prolly::prefix_range(&partition).1)
        }
        SortKeyCondition::GreaterThanOrEqual(value) => {
            IndexQueryBounds::Range(full(value)?, prolly::prefix_range(&partition).1)
        }
        SortKeyCondition::Between(lower, upper) => {
            let lower = full(lower)?;
            let upper = full(upper)?;
            if lower > upper {
                return Err(Error::Validation(
                    "sort-key BETWEEN lower bound exceeds upper bound".into(),
                ));
            }
            IndexQueryBounds::Range(lower, prolly::prefix_range(&upper).1)
        }
        SortKeyCondition::BeginsWith(value) => {
            if !matches!(value, AttributeValue::S(value) if !value.is_empty())
                && !matches!(value, AttributeValue::B(value) if !value.is_empty())
            {
                return Err(Error::Validation(
                    "begins_with sort-key operand must be a non-empty string or binary value"
                        .into(),
                ));
            }
            let mut prefix = full(value)?;
            if !prefix.ends_with(&[0, 0]) {
                return Err(Error::CorruptData(
                    "encoded secondary-index sort key is missing its terminator".into(),
                ));
            }
            prefix.truncate(prefix.len() - 2);
            IndexQueryBounds::Prefix(prefix)
        }
    })
}

fn project_index_keys(
    table: &TableDescription,
    index: &SecondaryIndexDescription,
    item: &Item,
) -> Item {
    let names = std::iter::once(table.partition_key.name.as_str())
        .chain(table.sort_key.as_ref().map(|key| key.name.as_str()))
        .chain(std::iter::once(index.partition_key.name.as_str()))
        .chain(index.sort_key.as_ref().map(|key| key.name.as_str()))
        .collect::<BTreeSet<_>>();
    names
        .into_iter()
        .filter_map(|name| {
            item.get(name)
                .cloned()
                .map(|value| (name.to_string(), value))
        })
        .collect()
}

fn transaction_fingerprint(
    actions: &[TransactWriteAction],
    expected_heads: &BTreeMap<String, MapVersionId>,
) -> Result<[u8; 32]> {
    canonical_fingerprint(
        b"DDB-TransactWriteItems-fingerprint-v1",
        &(actions, expected_heads),
    )
}

fn canonical_fingerprint<T: Serialize>(domain: &[u8], value: &T) -> Result<[u8; 32]> {
    let mut bytes = domain.to_vec();
    bytes.extend(
        serde_cbor::ser::to_vec_packed(value)
            .map_err(|error| Error::Serialization(error.to_string()))?,
    );
    let cid = prolly::Cid::from_bytes(&bytes);
    let mut fingerprint = [0; 32];
    fingerprint.copy_from_slice(cid.as_bytes());
    Ok(fingerprint)
}

fn table_index_policy() -> CollectionIndexPolicy {
    CollectionIndexPolicy {
        max_retained_snapshots: 1,
        ..CollectionIndexPolicy::default()
    }
}

fn index_definition_matches(
    description: &SecondaryIndexDescription,
    definition: &SecondaryIndexDefinition,
) -> bool {
    description.name == definition.name
        && description.kind == definition.kind
        && description.partition_key == definition.partition_key
        && description.sort_key == definition.sort_key
        && description.projection == definition.projection
}

fn reconfigured_description(
    before: &TableDescription,
    mut desired: Vec<SecondaryIndexDefinition>,
) -> Result<TableDescription> {
    before.validate()?;
    if before.status != TableStatus::Active {
        return Err(Error::Validation(
            "secondary indexes can only be reconfigured on an active table".into(),
        ));
    }
    if before
        .secondary_indexes
        .iter()
        .any(|index| index.status != SecondaryIndexStatus::Active)
    {
        return Err(Error::Validation(
            "secondary-index reconfiguration requires every current generation to be active".into(),
        ));
    }
    desired.sort_by(|left, right| left.name.cmp(&right.name));
    if desired.windows(2).any(|pair| pair[0].name == pair[1].name) {
        return Err(Error::Validation(
            "secondary index names must be unique within a table".into(),
        ));
    }

    let existing = before
        .secondary_indexes
        .iter()
        .map(|index| (index.name.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let mut secondary_indexes = Vec::with_capacity(desired.len());
    for definition in desired {
        let (id, generation) = match existing.get(definition.name.as_str()) {
            Some(current) if index_definition_matches(current, &definition) => {
                (current.id.clone(), current.generation)
            }
            Some(current) => {
                let generation = current.generation.checked_add(1).ok_or_else(|| {
                    Error::Validation("secondary index generation is exhausted".into())
                })?;
                let id = SecondaryIndexId(canonical_fingerprint(
                    b"DDB-SecondaryIndexId-v1",
                    &(&before.id, definition.name.as_str(), generation),
                )?);
                (id, generation)
            }
            None => {
                let generation = 1;
                let id = SecondaryIndexId(canonical_fingerprint(
                    b"DDB-SecondaryIndexId-v1",
                    &(&before.id, definition.name.as_str(), generation),
                )?);
                (id, generation)
            }
        };
        secondary_indexes.push(SecondaryIndexDescription {
            name: definition.name,
            id,
            generation,
            kind: definition.kind,
            partition_key: definition.partition_key,
            sort_key: definition.sort_key,
            projection: definition.projection,
            status: SecondaryIndexStatus::Active,
        });
    }

    let mut attribute_definitions = BTreeMap::new();
    let mut add_key = |key: &crate::KeyAttribute| -> Result<()> {
        if attribute_definitions
            .get(&key.name)
            .is_some_and(|kind| *kind != key.kind)
        {
            return Err(Error::Validation(format!(
                "attribute definition for {:?} has inconsistent scalar types",
                key.name
            )));
        }
        attribute_definitions.insert(key.name.clone(), key.kind);
        Ok(())
    };
    add_key(&before.partition_key)?;
    if let Some(sort) = &before.sort_key {
        add_key(sort)?;
    }
    for index in &secondary_indexes {
        add_key(&index.partition_key)?;
        if let Some(sort) = &index.sort_key {
            add_key(sort)?;
        }
    }

    let mut after = before.clone();
    after.attribute_definitions = attribute_definitions;
    after.secondary_indexes = secondary_indexes;
    after.validate()?;
    Ok(after)
}

fn index_reconfiguration_plan_id(
    plan: &IndexReconfigurationPlan,
) -> Result<IndexReconfigurationPlanId> {
    Ok(IndexReconfigurationPlanId(canonical_fingerprint(
        b"DDB-IndexReconfigurationPlan-v1",
        &(
            &plan.table_name,
            &plan.table_id,
            &plan.expected_head,
            plan.expected_commit_sequence,
            &plan.before,
            &plan.after,
            plan.planned_at_millis,
        ),
    )?))
}

fn validate_index_reconfiguration_plan(plan: &IndexReconfigurationPlan) -> Result<()> {
    plan.before.validate()?;
    plan.after.validate()?;
    if plan.table_name != plan.before.name
        || plan.table_name != plan.after.name
        || plan.table_id != plan.before.id
        || plan.table_id != plan.after.id
    {
        return Err(Error::Validation(
            "index reconfiguration plan table identity is inconsistent".into(),
        ));
    }
    if plan.before.partition_key != plan.after.partition_key
        || plan.before.sort_key != plan.after.sort_key
        || plan.before.status != plan.after.status
        || plan.before.created_at_millis != plan.after.created_at_millis
    {
        return Err(Error::Validation(
            "index reconfiguration may only change secondary indexes and their key definitions"
                .into(),
        ));
    }
    if plan.before == plan.after {
        return Err(Error::Validation(
            "index reconfiguration plan must change the table schema".into(),
        ));
    }
    let definitions = plan
        .after
        .secondary_indexes
        .iter()
        .map(|index| SecondaryIndexDefinition {
            name: index.name.clone(),
            kind: index.kind,
            partition_key: index.partition_key.clone(),
            sort_key: index.sort_key.clone(),
            projection: index.projection.clone(),
        })
        .collect();
    if reconfigured_description(&plan.before, definitions)? != plan.after {
        return Err(Error::Validation(
            "index reconfiguration generations or identities are not canonical".into(),
        ));
    }
    if index_reconfiguration_plan_id(plan)? != plan.id {
        return Err(Error::Validation(
            "index reconfiguration plan ID does not match its canonical contents".into(),
        ));
    }
    Ok(())
}

fn encode_index_reconfiguration_audit(record: &IndexReconfigurationAuditRecord) -> Result<Vec<u8>> {
    validate_index_reconfiguration_plan(&record.plan)?;
    record.context.validate()?;
    if record.result.plan_id != record.plan.id
        || record.result.description != record.plan.after
        || record.result.version == record.plan.expected_head
        || record.result.completed_at_millis < record.plan.planned_at_millis
        || record.result.replayed
    {
        return Err(Error::Validation(
            "index reconfiguration audit result is inconsistent with its plan".into(),
        ));
    }
    encode_record(b"DDBJ\x01", record)
}

fn decode_index_reconfiguration_audit(bytes: &[u8]) -> Result<IndexReconfigurationAuditRecord> {
    let record: IndexReconfigurationAuditRecord = decode_record(b"DDBJ\x01", bytes)?;
    encode_index_reconfiguration_audit(&record)
        .map_err(|error| Error::CorruptData(error.to_string()))?;
    Ok(record)
}

fn validate_client_request_token(token: Option<&str>) -> Result<()> {
    if token.is_some_and(|token| token.is_empty() || token.chars().count() > 36) {
        return Err(Error::Validation(
            "request token length must be 1..=36 characters".into(),
        ));
    }
    Ok(())
}

fn single_table_transition<'a>(
    table: &str,
    result: &'a TransactWriteResult,
) -> Result<&'a TransactionTableTransition> {
    if result.transitions.len() != 1 || result.transitions[0].table_name != table {
        return Err(Error::CorruptData(format!(
            "single-item commit does not contain exactly one transition for table {table:?}"
        )));
    }
    Ok(&result.transitions[0])
}

fn idempotency_key(token: &str) -> Vec<u8> {
    let mut bytes = b"DDB-TransactWriteItems-token-v1".to_vec();
    bytes.extend_from_slice(token.as_bytes());
    prolly::Cid::from_bytes(&bytes).as_bytes().to_vec()
}

fn encode_commit_result(result: &TransactWriteResult) -> Result<Vec<u8>> {
    encode_record(b"DDBM\x01", result)
}

fn decode_commit_result(bytes: &[u8]) -> Result<TransactWriteResult> {
    let result = decode_record(b"DDBM\x01", bytes)?;
    validate_commit_result(&result)?;
    Ok(result)
}

fn encode_table_commit_record(record: &TableCommit) -> Result<Vec<u8>> {
    encode_record(b"DDBL\x01", record)
}

fn decode_table_commit_record(bytes: &[u8]) -> Result<TableCommit> {
    let record: TableCommit = decode_record(b"DDBL\x01", bytes)?;
    if record.sequence == 0
        || record.transition.before.is_none() && record.transition.after.is_none()
        || record.transition.applied != (record.transition.before != record.transition.after)
    {
        return Err(Error::CorruptData(
            "table commit record has inconsistent transition metadata".into(),
        ));
    }
    Ok(record)
}

fn validate_commit_result(result: &TransactWriteResult) -> Result<()> {
    if result.transitions.is_empty() {
        return Err(Error::CorruptData(
            "commit record contains no table transitions".into(),
        ));
    }
    let mut names = BTreeSet::new();
    let mut ids = BTreeSet::new();
    let mut expected_versions = BTreeMap::new();
    for transition in &result.transitions {
        if !names.insert(transition.table_name.as_str()) || !ids.insert(&transition.table_id) {
            return Err(Error::CorruptData(
                "commit record contains duplicate table transitions".into(),
            ));
        }
        if transition.before.is_none() && transition.after.is_none()
            || transition.applied != (transition.before != transition.after)
        {
            return Err(Error::CorruptData(
                "commit record has inconsistent transition metadata".into(),
            ));
        }
        if let Some(after) = &transition.after {
            expected_versions.insert(transition.table_name.clone(), after.clone());
        }
    }
    if result.table_versions != expected_versions {
        return Err(Error::CorruptData(
            "commit record table versions disagree with its transitions".into(),
        ));
    }
    Ok(())
}

fn encode_idempotency_record(record: &IdempotencyRecord) -> Result<Vec<u8>> {
    encode_record(b"DDBR\x01", record)
}

fn decode_idempotency_record(bytes: &[u8]) -> Result<IdempotencyRecord> {
    let record: IdempotencyRecord = decode_record(b"DDBR\x01", bytes)?;
    validate_commit_result(&record.result)?;
    Ok(record)
}

fn encode_maintenance_lease(lease: &MaintenanceLease) -> Result<Vec<u8>> {
    lease.context.validate()?;
    if lease.expires_at_millis <= lease.acquired_at_millis {
        return Err(Error::Validation(
            "maintenance lease expiry must follow acquisition".into(),
        ));
    }
    encode_record(b"DDBL\x01", lease)
}

fn validate_worker_identity(
    job_id: &WorkerJobId,
    kind: WorkerKind,
    configuration_digest: [u8; 32],
    owner_id: &str,
    duration_millis: u64,
) -> Result<()> {
    if *job_id != WorkerJobId::for_digest(kind, configuration_digest) {
        return Err(Error::Validation(
            "worker job ID does not match its kind/configuration digest".into(),
        ));
    }
    if owner_id.is_empty() || owner_id.len() > 256 {
        return Err(Error::Validation(
            "worker owner ID must contain 1..=256 bytes".into(),
        ));
    }
    if !(MIN_WORKER_LEASE_MILLIS..=MAX_WORKER_LEASE_MILLIS).contains(&duration_millis) {
        return Err(Error::Validation(format!(
            "worker lease duration must be {MIN_WORKER_LEASE_MILLIS}..={MAX_WORKER_LEASE_MILLIS} milliseconds"
        )));
    }
    Ok(())
}

fn validate_worker_lease(lease: &WorkerLease) -> Result<()> {
    validate_worker_identity(
        &lease.job_id,
        lease.kind,
        lease.configuration_digest,
        &lease.owner_id,
        MIN_WORKER_LEASE_MILLIS,
    )?;
    let duration = lease
        .expires_at_millis
        .saturating_sub(lease.renewed_at_millis);
    if lease.fence == 0
        || lease.acquired_at_millis > lease.renewed_at_millis
        || lease.renewed_at_millis >= lease.expires_at_millis
        || !(MIN_WORKER_LEASE_MILLIS..=MAX_WORKER_LEASE_MILLIS).contains(&duration)
    {
        return Err(Error::Validation(
            "worker lease has invalid fence or timestamps".into(),
        ));
    }
    Ok(())
}

fn validate_worker_progress(kind: WorkerKind, progress: &WorkerProgress) -> Result<()> {
    match (kind, progress) {
        (WorkerKind::Stream, WorkerProgress::Stream { .. }) => Ok(()),
        (
            WorkerKind::Ttl,
            WorkerProgress::Ttl {
                last_evaluated_key,
                evaluated_total,
                deleted_total,
                ..
            },
        ) => {
            if deleted_total > evaluated_total {
                return Err(Error::Validation(
                    "TTL worker deleted count cannot exceed evaluated count".into(),
                ));
            }
            if let Some(key) = last_evaluated_key {
                item_size(key)?;
            }
            Ok(())
        }
        _ => Err(Error::Validation(
            "worker progress kind does not match the lease kind".into(),
        )),
    }
}

fn validate_worker_progress_transition(
    current: &WorkerProgress,
    candidate: &WorkerProgress,
) -> Result<()> {
    let monotonic = match (current, candidate) {
        (
            WorkerProgress::Stream {
                table_id: current_table,
                delivered_through_sequence: current_sequence,
            },
            WorkerProgress::Stream {
                table_id: candidate_table,
                delivered_through_sequence: candidate_sequence,
            },
        ) => current_table == candidate_table && candidate_sequence >= current_sequence,
        (
            WorkerProgress::Ttl {
                table_id: current_table,
                cycle: current_cycle,
                evaluated_total: current_evaluated,
                deleted_total: current_deleted,
                ..
            },
            WorkerProgress::Ttl {
                table_id: candidate_table,
                cycle: candidate_cycle,
                evaluated_total: candidate_evaluated,
                deleted_total: candidate_deleted,
                ..
            },
        ) => {
            current_table == candidate_table
                && candidate_cycle >= current_cycle
                && candidate_evaluated >= current_evaluated
                && candidate_deleted >= current_deleted
        }
        _ => false,
    };
    if !monotonic {
        return Err(Error::Validation(
            "worker checkpoint progress must be monotonic for one table and kind".into(),
        ));
    }
    Ok(())
}

fn worker_lease_audit_key(job_id: &WorkerJobId, fence: u64) -> Vec<u8> {
    let mut key = job_id.0.to_vec();
    key.extend_from_slice(&fence.to_be_bytes());
    key
}

fn encode_worker_lease(lease: &WorkerLease) -> Result<Vec<u8>> {
    validate_worker_lease(lease)?;
    encode_record(b"DDBW\x01", lease)
}

fn encode_worker_fence(fence: u64) -> Vec<u8> {
    fence.to_be_bytes().to_vec()
}

fn decode_worker_fence(bytes: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::CorruptData("worker fence counter must contain 8 bytes".into()))?;
    let fence = u64::from_be_bytes(bytes);
    if fence == 0 {
        return Err(Error::CorruptData(
            "worker fence counter must be greater than zero".into(),
        ));
    }
    Ok(fence)
}

fn decode_worker_lease(bytes: &[u8]) -> Result<WorkerLease> {
    let lease: WorkerLease = decode_record(b"DDBW\x01", bytes)?;
    validate_worker_lease(&lease).map_err(|error| Error::CorruptData(error.to_string()))?;
    Ok(lease)
}

fn encode_worker_lease_release(release: &WorkerLeaseRelease) -> Result<Vec<u8>> {
    validate_worker_lease(&release.lease)?;
    if release.replayed || release.released_at_millis < release.lease.acquired_at_millis {
        return Err(Error::Validation(
            "persisted worker lease release has invalid replay or timestamp state".into(),
        ));
    }
    encode_record(b"DDBY\x01", release)
}

fn decode_worker_lease_release(bytes: &[u8]) -> Result<WorkerLeaseRelease> {
    let release: WorkerLeaseRelease = decode_record(b"DDBY\x01", bytes)?;
    encode_worker_lease_release(&release).map_err(|error| Error::CorruptData(error.to_string()))?;
    Ok(release)
}

fn encode_worker_checkpoint(checkpoint: &WorkerCheckpoint) -> Result<Vec<u8>> {
    if checkpoint.revision == 0 || checkpoint.fence == 0 {
        return Err(Error::Validation(
            "worker checkpoint revision and fence must be greater than zero".into(),
        ));
    }
    if checkpoint.job_id
        != WorkerJobId::for_digest(checkpoint.kind, checkpoint.configuration_digest)
    {
        return Err(Error::Validation(
            "worker checkpoint identity/configuration mismatch".into(),
        ));
    }
    validate_worker_progress(checkpoint.kind, &checkpoint.progress)?;
    encode_record(b"DDBQ\x01", checkpoint)
}

fn decode_worker_checkpoint(bytes: &[u8]) -> Result<WorkerCheckpoint> {
    let checkpoint: WorkerCheckpoint = decode_record(b"DDBQ\x01", bytes)?;
    encode_worker_checkpoint(&checkpoint).map_err(|error| Error::CorruptData(error.to_string()))?;
    Ok(checkpoint)
}

fn decode_maintenance_lease(bytes: &[u8]) -> Result<MaintenanceLease> {
    let lease: MaintenanceLease = decode_record(b"DDBL\x01", bytes)?;
    encode_maintenance_lease(&lease).map_err(|error| Error::CorruptData(error.to_string()))?;
    Ok(lease)
}

fn encode_maintenance_lease_release(release: &MaintenanceLeaseRelease) -> Result<Vec<u8>> {
    if release.replayed {
        return Err(Error::Validation(
            "persisted maintenance release cannot be marked replayed".into(),
        ));
    }
    encode_maintenance_lease(&release.lease)?;
    release.context.validate()?;
    if release.released_at_millis < release.lease.acquired_at_millis
        || release.forced_after_expiry
            && release.released_at_millis < release.lease.expires_at_millis
    {
        return Err(Error::Validation(
            "maintenance release timestamp is inconsistent with its lease".into(),
        ));
    }
    encode_record(b"DDBQ\x01", release)
}

fn decode_maintenance_lease_release(bytes: &[u8]) -> Result<MaintenanceLeaseRelease> {
    let release: MaintenanceLeaseRelease = decode_record(b"DDBQ\x01", bytes)?;
    encode_maintenance_lease_release(&release)
        .map_err(|error| Error::CorruptData(error.to_string()))?;
    Ok(release)
}

fn validate_gc_delete_counts(node_deletes: usize, blob_deletes: usize) -> Result<()> {
    let total = node_deletes
        .checked_add(blob_deletes)
        .ok_or_else(|| Error::Validation("GC delete count overflow".into()))?;
    if total == 0 || total > MAX_GC_PLAN_DELETES {
        return Err(Error::Validation(format!(
            "GC execution must contain 1..={MAX_GC_PLAN_DELETES} physical deletes"
        )));
    }
    Ok(())
}

fn validate_gc_execution_parameters(
    record: &GcExecutionRecord,
    lease_id: &MaintenanceLeaseId,
    roots_digest: &[u8; 32],
    node_deletes: usize,
    blob_deletes: usize,
    context: &MaintenanceContext,
) -> Result<()> {
    if record.lease_id != *lease_id
        || record.roots_digest != *roots_digest
        || record.node_deletes != node_deletes
        || record.blob_deletes != blob_deletes
        || record.context != *context
    {
        return Err(Error::IdempotentParameterMismatch);
    }
    Ok(())
}

fn encode_gc_execution_record(record: &GcExecutionRecord) -> Result<Vec<u8>> {
    record.context.validate()?;
    validate_gc_delete_counts(record.node_deletes, record.blob_deletes)?;
    match (record.state, record.completed_at_millis) {
        (GcExecutionState::InProgress, None) => {}
        (GcExecutionState::Complete, Some(completed)) if completed >= record.started_at_millis => {}
        _ => {
            return Err(Error::Validation(
                "GC execution state and timestamps are inconsistent".into(),
            ));
        }
    }
    encode_record(b"DDBG\x01", record)
}

fn decode_gc_execution_record(bytes: &[u8]) -> Result<GcExecutionRecord> {
    let record: GcExecutionRecord = decode_record(b"DDBG\x01", bytes)?;
    encode_gc_execution_record(&record).map_err(|error| Error::CorruptData(error.to_string()))?;
    Ok(record)
}

fn hex_id(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn import_plan_id(plan: &ImportPlan) -> Result<ImportPlanId> {
    let mut identity = plan.clone();
    identity.id = ImportPlanId([0; 32]);
    Ok(ImportPlanId(canonical_fingerprint(
        b"DDB-ImportPlan-v1",
        &identity,
    )?))
}

fn validate_import_plan(plan: &ImportPlan) -> Result<()> {
    let candidate = TableDescription {
        name: plan.target_table_name.clone(),
        id: plan.target_table_id.clone(),
        partition_key: crate::KeyAttribute {
            name: "placeholder".into(),
            kind: crate::KeyKind::String,
        },
        sort_key: None,
        attribute_definitions: BTreeMap::from([("placeholder".into(), crate::KeyKind::String)]),
        secondary_indexes: Vec::new(),
        status: TableStatus::Active,
        created_at_millis: 0,
    };
    candidate.validate()?;
    DatabaseFormatRecord::decode(&plan.required_database_format).map_err(|error| {
        Error::Validation(format!("import plan database format is invalid: {error}"))
    })?;
    if plan.source_table_name.is_empty() || plan.source_table_name.len() > 255 {
        return Err(Error::Validation(
            "import plan source table name must contain 1..=255 bytes".into(),
        ));
    }
    if import_plan_id(plan)? != plan.id {
        return Err(Error::Validation(
            "import plan identity does not match its canonical contents".into(),
        ));
    }
    Ok(())
}

fn encode_import_audit_record(record: &ImportAuditRecord) -> Result<Vec<u8>> {
    validate_import_plan(&record.plan)?;
    record.context.validate()?;
    record.description.validate()?;
    if record.description.name != record.plan.target_table_name
        || record.description.id != record.plan.target_table_id
    {
        return Err(Error::Validation(
            "import audit description disagrees with its plan".into(),
        ));
    }
    encode_record(b"DDBJ\x01", record)
}

fn decode_import_audit_record(bytes: &[u8]) -> Result<ImportAuditRecord> {
    let record: ImportAuditRecord = decode_record(b"DDBJ\x01", bytes)?;
    encode_import_audit_record(&record).map_err(|error| Error::CorruptData(error.to_string()))?;
    Ok(record)
}

fn import_result_from_audit(record: ImportAuditRecord, replayed: bool) -> ImportResult {
    ImportResult {
        plan_id: record.plan.id,
        description: record.description,
        version: record.plan.source_version,
        commit_id: record.commit_id,
        completed_at_millis: record.completed_at_millis,
        replayed,
    }
}

fn retention_plan_id(plan: &RetentionPlan) -> Result<RetentionPlanId> {
    let mut identity = plan.clone();
    identity.id = RetentionPlanId([0; 32]);
    Ok(RetentionPlanId(canonical_fingerprint(
        b"DDB-RetentionPlan-v1",
        &identity,
    )?))
}

fn validate_retention_plan(plan: &RetentionPlan) -> Result<()> {
    if plan.table_name.is_empty() || plan.table_name.len() > 255 {
        return Err(Error::Validation(
            "retention plan table name must contain 1..=255 bytes".into(),
        ));
    }
    if plan.policy.keep_last > MAX_COLLECTED_VERSIONS
        || plan.policy.protected_versions.len() > MAX_RETENTION_PROTECTED_VERSIONS
    {
        return Err(Error::Validation(
            "retention plan policy exceeds advertised limits".into(),
        ));
    }
    if plan.remove.len() > MAX_RETENTION_REMOVALS
        || plan.remove.windows(2).any(|ids| ids[0] >= ids[1])
        || plan.remove.contains(&plan.expected_head)
    {
        return Err(Error::Validation(
            "retention plan removal set is oversized, unordered, duplicated, or contains head"
                .into(),
        ));
    }
    if plan.examined_versions < u64::try_from(plan.remove.len()).unwrap_or(u64::MAX) {
        return Err(Error::Validation(
            "retention plan examined count is smaller than its removal set".into(),
        ));
    }
    if retention_plan_id(plan)? != plan.id {
        return Err(Error::Validation(
            "retention plan identity does not match its canonical contents".into(),
        ));
    }
    Ok(())
}

fn encode_retention_audit_record(record: &RetentionAuditRecord) -> Result<Vec<u8>> {
    validate_retention_plan(&record.plan)?;
    record.context.validate()?;
    encode_record(b"DDBA\x01", record)
}

fn decode_retention_audit_record(bytes: &[u8]) -> Result<RetentionAuditRecord> {
    let record: RetentionAuditRecord = decode_record(b"DDBA\x01", bytes)?;
    validate_retention_plan(&record.plan).map_err(|error| Error::CorruptData(error.to_string()))?;
    record
        .context
        .validate()
        .map_err(|error| Error::CorruptData(error.to_string()))?;
    Ok(record)
}

fn encode_record<T: Serialize>(magic: &[u8; 5], value: &T) -> Result<Vec<u8>> {
    let mut bytes = magic.to_vec();
    bytes.extend(
        serde_cbor::ser::to_vec_packed(value)
            .map_err(|error| Error::Serialization(error.to_string()))?,
    );
    Ok(bytes)
}

fn decode_record<T>(magic: &[u8; 5], bytes: &[u8]) -> Result<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    if !bytes.starts_with(magic) {
        return Err(Error::CorruptData(
            "durable transaction record has an invalid envelope".into(),
        ));
    }
    let value: T = serde_cbor::from_slice(&bytes[magic.len()..])
        .map_err(|error| Error::CorruptData(error.to_string()))?;
    if encode_record(magic, &value).map_err(|error| Error::CorruptData(error.to_string()))? != bytes
    {
        return Err(Error::CorruptData(
            "durable transaction record is not canonical".into(),
        ));
    }
    Ok(value)
}

fn decode_commit_sequence(bytes: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
        Error::CorruptData("table commit sequence must contain exactly 8 bytes".into())
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn validate_table_snapshot_manifest(record: &TableSnapshotManifestRecord) -> Result<()> {
    if record.format_version != TABLE_SNAPSHOT_MANIFEST_FORMAT {
        return Err(Error::CorruptData(
            "unsupported table snapshot manifest format".into(),
        ));
    }
    record.description.validate()?;
    if record.description.id != record.table_id {
        return Err(Error::CorruptData(
            "table snapshot manifest description belongs to another table".into(),
        ));
    }
    record
        .indexed
        .validate()
        .map_err(|error| Error::CorruptData(error.to_string()))?;
    let mut expected_source = TABLE_INDEXED_SOURCE_PREFIX.to_vec();
    expected_source.extend_from_slice(&record.table_id.0);
    if record.indexed.record.source_map_id != expected_source {
        return Err(Error::CorruptData(
            "table snapshot manifest indexed source belongs to another table".into(),
        ));
    }
    let registry = index_registry(&record.description)?;
    let expected = registry
        .iter()
        .map(|definition| {
            let descriptor = prolly::IndexDescriptor::from_runtime(
                &record.indexed.record.source_map_id,
                definition,
            )?;
            Ok((descriptor.name, descriptor.fingerprint))
        })
        .collect::<std::result::Result<Vec<_>, prolly::Error>>()?;
    let actual = record
        .indexed
        .record
        .indexes
        .iter()
        .map(|index| (index.name.clone(), index.descriptor_fingerprint.clone()))
        .collect::<Vec<_>>();
    if actual != expected {
        return Err(Error::CorruptData(
            "table snapshot manifest schema/index closure mismatch".into(),
        ));
    }
    Ok(())
}

fn table_snapshot_manifests_semantically_equal(
    left: &TableSnapshotManifestRecord,
    right: &TableSnapshotManifestRecord,
) -> bool {
    left.format_version == right.format_version
        && left.table_id == right.table_id
        && left.base_version == right.base_version
        && left.description == right.description
        && left.indexed.record.source_map_id == right.indexed.record.source_map_id
        && left.indexed.record.source == right.indexed.record.source
        && left.indexed.record.indexes == right.indexed.record.indexes
        && left.indexed.descriptors == right.indexed.descriptors
}

fn encode_table_snapshot_locator(locator: &TableSnapshotLocator) -> Result<Vec<u8>> {
    if locator.manifest_tree.root.is_none() {
        return Err(Error::CorruptData(
            "detached snapshot manifest tree is empty".into(),
        ));
    }
    let manifest = RootManifest::from_tree(&locator.manifest_tree)
        .to_bytes()
        .map_err(|error| Error::Serialization(error.to_string()))?;
    let manifest_len = u32::try_from(manifest.len())
        .map_err(|_| Error::Validation("table snapshot locator tree handle is too large".into()))?;
    let mut bytes = TABLE_SNAPSHOT_LOCATOR_MAGIC.to_vec();
    bytes.extend_from_slice(&manifest_len.to_be_bytes());
    bytes.extend(manifest);
    bytes.extend_from_slice(locator.indexed_snapshot_id.as_cid().as_bytes());
    if bytes.len() > MAX_TABLE_SNAPSHOT_LOCATOR_BYTES {
        return Err(Error::Validation(format!(
            "table snapshot locator exceeds {MAX_TABLE_SNAPSHOT_LOCATOR_BYTES} bytes"
        )));
    }
    Ok(bytes)
}

fn decode_table_snapshot_locator(bytes: &[u8]) -> Result<TableSnapshotLocator> {
    if bytes.len() > MAX_TABLE_SNAPSHOT_LOCATOR_BYTES
        || !bytes.starts_with(TABLE_SNAPSHOT_LOCATOR_MAGIC)
    {
        return Err(Error::CorruptData(
            "malformed table snapshot locator envelope".into(),
        ));
    }
    let length_start = TABLE_SNAPSHOT_LOCATOR_MAGIC.len();
    let length_end = length_start + 4;
    let manifest_len = bytes
        .get(length_start..length_end)
        .and_then(|value| value.try_into().ok())
        .map(u32::from_be_bytes)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| Error::CorruptData("malformed table snapshot locator length".into()))?;
    let manifest_end = length_end
        .checked_add(manifest_len)
        .ok_or_else(|| Error::CorruptData("table snapshot locator length overflow".into()))?;
    let snapshot_end = manifest_end
        .checked_add(32)
        .ok_or_else(|| Error::CorruptData("table snapshot locator length overflow".into()))?;
    if snapshot_end != bytes.len() {
        return Err(Error::CorruptData(
            "malformed table snapshot locator length".into(),
        ));
    }
    let manifest = RootManifest::from_bytes(&bytes[length_end..manifest_end])
        .map_err(|error| Error::CorruptData(error.to_string()))?;
    if manifest.root.is_none()
        || manifest.created_at_millis.is_some()
        || manifest.updated_at_millis.is_some()
    {
        return Err(Error::CorruptData(
            "table snapshot locator is not an immutable nonempty tree".into(),
        ));
    }
    let locator = TableSnapshotLocator {
        manifest_tree: manifest.into_tree(),
        indexed_snapshot_id: IndexedSnapshotId(prolly::Cid(
            bytes[manifest_end..snapshot_end]
                .try_into()
                .expect("validated snapshot identifier length"),
        )),
    };
    if encode_table_snapshot_locator(&locator)
        .map_err(|error| Error::CorruptData(error.to_string()))?
        != bytes
    {
        return Err(Error::CorruptData(
            "table snapshot locator is not canonical".into(),
        ));
    }
    Ok(locator)
}

fn encode_table_snapshot_manifest(record: &TableSnapshotManifestRecord) -> Result<Vec<u8>> {
    validate_table_snapshot_manifest(record)?;
    let mut bytes = b"DDBM\x01".to_vec();
    bytes.extend(
        serde_cbor::ser::to_vec_packed(record)
            .map_err(|error| Error::Serialization(error.to_string()))?,
    );
    if bytes.len() > MAX_TABLE_SNAPSHOT_MANIFEST_BYTES {
        return Err(Error::Validation(format!(
            "table snapshot manifest exceeds {MAX_TABLE_SNAPSHOT_MANIFEST_BYTES} bytes"
        )));
    }
    Ok(bytes)
}

fn decode_table_snapshot_manifest(bytes: &[u8]) -> Result<TableSnapshotManifestRecord> {
    if bytes.len() > MAX_TABLE_SNAPSHOT_MANIFEST_BYTES || !bytes.starts_with(b"DDBM\x01") {
        return Err(Error::CorruptData(
            "malformed table snapshot manifest envelope".into(),
        ));
    }
    let record: TableSnapshotManifestRecord = serde_cbor::from_slice(&bytes[5..])
        .map_err(|error| Error::CorruptData(error.to_string()))?;
    validate_table_snapshot_manifest(&record)?;
    if encode_table_snapshot_manifest(&record)
        .map_err(|error| Error::CorruptData(error.to_string()))?
        != bytes
    {
        return Err(Error::CorruptData(
            "table snapshot manifest is not canonical".into(),
        ));
    }
    Ok(record)
}

fn blob_references_from_mutations(
    mutations: &[Mutation],
) -> Result<BTreeMap<prolly::Cid, BlobRef>> {
    let mut references = BTreeMap::new();
    for mutation in mutations {
        let Mutation::Upsert { val, .. } = mutation else {
            continue;
        };
        let ValueRef::Blob(reference) = ValueRef::from_stored_bytes(val)? else {
            continue;
        };
        match references.insert(reference.cid.clone(), reference.clone()) {
            Some(existing) if existing.len != reference.len => {
                return Err(Error::CorruptData(
                    "one blob CID was observed with conflicting lengths".into(),
                ));
            }
            _ => {}
        }
    }
    Ok(references)
}

fn encode_table_blob_registry_value(len: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(TABLE_BLOB_REGISTRY_VALUE_MAGIC.len() + 8);
    bytes.extend_from_slice(TABLE_BLOB_REGISTRY_VALUE_MAGIC);
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes
}

fn decode_table_blob_registry_value(cid: &prolly::Cid, bytes: &[u8]) -> Result<BlobRef> {
    let expected_len = TABLE_BLOB_REGISTRY_VALUE_MAGIC.len() + 8;
    if bytes.len() != expected_len || !bytes.starts_with(TABLE_BLOB_REGISTRY_VALUE_MAGIC) {
        return Err(Error::CorruptData(
            "malformed table blob registry value".into(),
        ));
    }
    let len = u64::from_be_bytes(
        bytes[TABLE_BLOB_REGISTRY_VALUE_MAGIC.len()..]
            .try_into()
            .expect("validated blob registry value length"),
    );
    Ok(BlobRef {
        cid: cid.clone(),
        len,
    })
}

fn encode_table_schema_record(description: &TableDescription) -> Result<Vec<u8>> {
    let digest = canonical_fingerprint(
        b"DDB-TableSchemaRecord-v1",
        &(
            &description.partition_key,
            &description.sort_key,
            &description.attribute_definitions,
            &description.secondary_indexes,
        ),
    )?;
    let mut bytes = TABLE_SCHEMA_RECORD_MAGIC.to_vec();
    bytes.extend_from_slice(&digest);
    Ok(prolly::ValueRef::Inline(bytes).to_bytes())
}

fn encode_description(description: &TableDescription) -> Result<Vec<u8>> {
    let mut bytes = b"DDBT\x01".to_vec();
    bytes.extend(
        serde_cbor::ser::to_vec_packed(description)
            .map_err(|error| Error::Serialization(error.to_string()))?,
    );
    Ok(bytes)
}

fn decode_description(bytes: &[u8]) -> Result<TableDescription> {
    if !bytes.starts_with(b"DDBT\x01") {
        return Err(Error::CorruptData(
            "unsupported table descriptor envelope".into(),
        ));
    }
    let description: TableDescription = serde_cbor::from_slice(&bytes[5..])
        .map_err(|error| Error::CorruptData(error.to_string()))?;
    description.validate()?;
    Ok(description)
}

#[cfg(test)]
mod maintenance_format_tests {
    use super::*;

    #[test]
    fn detached_snapshot_locator_is_compact_canonical_and_timestamp_free() {
        let tree = Tree {
            root: Some(prolly::Cid::from_bytes(b"snapshot-manifest-node")),
            config: prolly::Config::default(),
        };
        let locator_record = TableSnapshotLocator {
            manifest_tree: tree.clone(),
            indexed_snapshot_id: IndexedSnapshotId(prolly::Cid::from_bytes(b"indexed-snapshot")),
        };
        let locator = encode_table_snapshot_locator(&locator_record).unwrap();

        assert!(locator.starts_with(TABLE_SNAPSHOT_LOCATOR_MAGIC));
        assert!(!locator.starts_with(b"DDBM\x01"));
        assert!(
            locator.len() < 512,
            "locator unexpectedly grew to {} bytes",
            locator.len()
        );
        assert_eq!(
            decode_table_snapshot_locator(&locator).unwrap(),
            locator_record
        );

        assert!(decode_table_snapshot_locator(b"DDBM\x01inline-manifest").is_err());
        assert!(encode_table_snapshot_locator(&TableSnapshotLocator {
            manifest_tree: Tree::new(prolly::Config::default()),
            indexed_snapshot_id: IndexedSnapshotId(prolly::Cid::from_bytes(b"indexed-snapshot")),
        })
        .is_err());

        let timestamped = RootManifest::from_tree(&tree)
            .with_created_at_millis(42)
            .to_bytes()
            .unwrap();
        let mut timestamped_locator = TABLE_SNAPSHOT_LOCATOR_MAGIC.to_vec();
        timestamped_locator.extend_from_slice(&(timestamped.len() as u32).to_be_bytes());
        timestamped_locator.extend(timestamped);
        timestamped_locator.extend_from_slice(prolly::Cid::from_bytes(b"indexed").as_bytes());
        assert!(decode_table_snapshot_locator(&timestamped_locator).is_err());
    }

    #[test]
    fn database_index_coordinator_retains_only_the_active_snapshot() {
        let policy = table_index_policy();
        assert_eq!(policy.max_retained_snapshots, 1);
        assert_eq!(policy.max_active_indexes, 32);
        policy.validate().unwrap();
    }

    #[test]
    fn retention_plan_and_audit_have_frozen_canonical_digests() {
        let head = MapVersionId::from_bytes(&[1; 32]).unwrap();
        let removed = MapVersionId::from_bytes(&[2; 32]).unwrap();
        let protected = MapVersionId::from_bytes(&[4; 32]).unwrap();
        let mut plan = RetentionPlan {
            id: RetentionPlanId([0; 32]),
            table_name: "Evidence".into(),
            table_id: TableId([3; 32]),
            expected_head: head,
            expected_commit_sequence: 17,
            policy: RetentionPolicy::keep_last(365)
                .keep_since_millis(1_700_000_000_000)
                .protect(protected),
            remove: vec![removed],
            examined_versions: 900,
            more_removable: true,
            planned_at_millis: 1_700_000_123_456,
        };
        plan.id = retention_plan_id(&plan).unwrap();
        let record = RetentionAuditRecord {
            plan: plan.clone(),
            context: MaintenanceContext::new("records-officer", "annual schedule")
                .change_ticket("LEGAL-42"),
            completed_at_millis: 1_700_000_234_567,
        };
        let encoded = encode_retention_audit_record(&record).unwrap();

        assert_eq!(
            plan.id.to_string(),
            "0d8f323c8c4f7733d6839841c601ad0210314acb8939ffe2b73e74c40928fc55"
        );
        assert_eq!(
            hex(prolly::Cid::from_bytes(&encoded).as_bytes()),
            "981f763b128723d0e204054a8ed0e067fd284884ee5fc37e3ebfcdea0c120bf1"
        );
        assert_eq!(decode_retention_audit_record(&encoded).unwrap(), record);
        assert!(decode_retention_audit_record(&[encoded, vec![0]].concat()).is_err());
    }

    #[test]
    fn import_plan_and_audit_have_frozen_canonical_digests() {
        let mut plan = ImportPlan {
            id: ImportPlanId([0; 32]),
            target_table_name: "EvidenceRestored".into(),
            target_table_id: TableId([7; 32]),
            archive_digest: [8; 32],
            source_table_name: "Evidence".into(),
            source_table_id: TableId([9; 32]),
            source_version: MapVersionId::from_bytes(&[10; 32]).unwrap(),
            required_database_format: DatabaseFormatRecord::current(
                prolly::Cid::from_bytes(b"tree"),
                StoragePublicationMode::AtomicNodesAndRoots,
                65_536,
            )
            .encode(),
            planned_at_millis: 1_700_000_345_678,
        };
        plan.id = import_plan_id(&plan).unwrap();
        let record = ImportAuditRecord {
            plan: plan.clone(),
            context: MaintenanceContext::new("records-officer", "court restoration")
                .change_ticket("LEGAL-42"),
            description: TableDescription {
                name: "EvidenceRestored".into(),
                id: TableId([7; 32]),
                partition_key: crate::KeyAttribute {
                    name: "case_id".into(),
                    kind: crate::KeyKind::String,
                },
                sort_key: None,
                attribute_definitions: BTreeMap::from([("case_id".into(), crate::KeyKind::String)]),
                secondary_indexes: Vec::new(),
                status: TableStatus::Active,
                created_at_millis: 1_700_000_456_789,
            },
            commit_id: CommitId([11; 32]),
            completed_at_millis: 1_700_000_456_789,
        };
        let encoded = encode_import_audit_record(&record).unwrap();

        assert_eq!(
            plan.id.to_string(),
            "17493c4bdbc539a107506ab3919764255d05a1754c3c3bd41da6b716b998d245"
        );
        assert_eq!(
            hex(prolly::Cid::from_bytes(&encoded).as_bytes()),
            "5c47109161be157ff1b146e0705ef5fe23637c0ee3f2fefd5c550c82dbfd7ac1"
        );
        assert_eq!(decode_import_audit_record(&encoded).unwrap(), record);
        assert!(decode_import_audit_record(&[encoded, vec![0]].concat()).is_err());
    }

    #[test]
    fn index_reconfiguration_plan_and_audit_have_frozen_canonical_digests() {
        let before = TableDescription {
            name: "Evidence".into(),
            id: TableId([21; 32]),
            partition_key: crate::KeyAttribute {
                name: "case_id".into(),
                kind: crate::KeyKind::String,
            },
            sort_key: None,
            attribute_definitions: BTreeMap::from([
                ("case_id".into(), crate::KeyKind::String),
                ("status".into(), crate::KeyKind::String),
            ]),
            secondary_indexes: vec![SecondaryIndexDescription {
                name: "ByStatus".into(),
                id: SecondaryIndexId([22; 32]),
                generation: 1,
                kind: crate::SecondaryIndexKind::Global,
                partition_key: crate::KeyAttribute {
                    name: "status".into(),
                    kind: crate::KeyKind::String,
                },
                sort_key: None,
                projection: crate::SecondaryIndexProjection::All,
                status: SecondaryIndexStatus::Active,
            }],
            status: TableStatus::Active,
            created_at_millis: 1_700_000_000_000,
        };
        let after = reconfigured_description(
            &before,
            vec![SecondaryIndexDefinition {
                name: "ByStatus".into(),
                kind: crate::SecondaryIndexKind::Global,
                partition_key: crate::KeyAttribute {
                    name: "owner".into(),
                    kind: crate::KeyKind::String,
                },
                sort_key: None,
                projection: crate::SecondaryIndexProjection::KeysOnly,
            }],
        )
        .unwrap();
        let mut plan = IndexReconfigurationPlan {
            id: IndexReconfigurationPlanId([0; 32]),
            table_name: before.name.clone(),
            table_id: before.id.clone(),
            expected_head: MapVersionId::from_bytes(&[23; 32]).unwrap(),
            expected_commit_sequence: 7,
            before,
            after: after.clone(),
            planned_at_millis: 1_700_000_100_000,
        };
        plan.id = index_reconfiguration_plan_id(&plan).unwrap();
        let record = IndexReconfigurationAuditRecord {
            plan: plan.clone(),
            context: MaintenanceContext::new("index-admin", "approved index replacement")
                .change_ticket("DB-42"),
            result: IndexReconfigurationResult {
                plan_id: plan.id.clone(),
                description: after,
                version: MapVersionId::from_bytes(&[24; 32]).unwrap(),
                indexed_source_version: MapVersionId::from_bytes(&[25; 32]).unwrap(),
                indexed_snapshot_id: IndexedSnapshotId(prolly::Cid([26; 32])),
                commit_id: CommitId([27; 32]),
                completed_at_millis: 1_700_000_200_000,
                replayed: false,
            },
        };
        let encoded = encode_index_reconfiguration_audit(&record).unwrap();

        assert_eq!(
            plan.id.to_string(),
            "b8627ac5e06059919df5c5ba50a47fd670d2362c80a63959ef9024633cc4a0cc"
        );
        assert_eq!(
            hex(prolly::Cid::from_bytes(&encoded).as_bytes()),
            "ede0e368ce4f7f0c0499bc793088a699dc8bfe2713d3ac45e8428fe25f317a26"
        );
        assert_eq!(
            decode_index_reconfiguration_audit(&encoded).unwrap(),
            record
        );
        assert!(decode_index_reconfiguration_audit(&[encoded, vec![0]].concat()).is_err());
    }

    #[test]
    fn maintenance_lease_and_release_have_frozen_canonical_digests() {
        let lease = MaintenanceLease {
            id: MaintenanceLeaseId([12; 32]),
            context: MaintenanceContext::new("gc-worker", "verified global sweep")
                .change_ticket("OPS-42"),
            acquired_at_millis: 1_700_000_500_000,
            expires_at_millis: 1_700_004_100_000,
        };
        let release = MaintenanceLeaseRelease {
            lease: lease.clone(),
            context: MaintenanceContext::new("gc-worker", "sweep completed")
                .change_ticket("OPS-42"),
            released_at_millis: 1_700_000_600_000,
            forced_after_expiry: false,
            replayed: false,
        };
        let encoded_lease = encode_maintenance_lease(&lease).unwrap();
        let encoded_release = encode_maintenance_lease_release(&release).unwrap();
        assert_eq!(
            hex(prolly::Cid::from_bytes(&encoded_lease).as_bytes()),
            "c341294fe8f5b9cf98ba4c860719358827036f2486d69f9999c5f6070c0756bb"
        );
        assert_eq!(
            hex(prolly::Cid::from_bytes(&encoded_release).as_bytes()),
            "6b52a902719c7155cecd0528bad5b035a4258c00db1c72838e90f9d5d27d1ee9"
        );
        assert_eq!(decode_maintenance_lease(&encoded_lease).unwrap(), lease);
        assert_eq!(
            decode_maintenance_lease_release(&encoded_release).unwrap(),
            release
        );
    }

    #[test]
    fn worker_lease_checkpoint_and_release_have_frozen_canonical_digests() {
        let configuration_digest = [31; 32];
        let job_id = WorkerJobId::for_digest(WorkerKind::Stream, configuration_digest);
        let lease = WorkerLease {
            job_id: job_id.clone(),
            kind: WorkerKind::Stream,
            configuration_digest,
            owner_id: "stream-worker-a".into(),
            fence: 7,
            acquired_at_millis: 1_700_000_000_000,
            renewed_at_millis: 1_700_000_010_000,
            expires_at_millis: 1_700_000_040_000,
        };
        let checkpoint = WorkerCheckpoint {
            job_id: job_id.clone(),
            kind: WorkerKind::Stream,
            configuration_digest,
            revision: 19,
            fence: lease.fence,
            progress: WorkerProgress::Stream {
                table_id: TableId([32; 32]),
                delivered_through_sequence: 4_096,
            },
            updated_at_millis: 1_700_000_020_000,
        };
        let release = WorkerLeaseRelease {
            lease: lease.clone(),
            released_at_millis: 1_700_000_030_000,
            replayed: false,
        };
        let encoded_lease = encode_worker_lease(&lease).unwrap();
        let encoded_checkpoint = encode_worker_checkpoint(&checkpoint).unwrap();
        let encoded_release = encode_worker_lease_release(&release).unwrap();

        assert_eq!(
            hex(prolly::Cid::from_bytes(&encoded_lease).as_bytes()),
            "a7552de2259d0681e10dda0f69b33e0820fcbaf427208fdb5fea1e7355001970"
        );
        assert_eq!(
            hex(prolly::Cid::from_bytes(&encoded_checkpoint).as_bytes()),
            "520afee9780953f302c98782c7c34a5a4353463bb3ea471137135df47b5456d2"
        );
        assert_eq!(
            hex(prolly::Cid::from_bytes(&encoded_release).as_bytes()),
            "dccd4429c544f79a7d1c72b8d5bad93bf49827282279dfea2df0454df5be7a70"
        );
        assert_eq!(decode_worker_lease(&encoded_lease).unwrap(), lease);
        assert_eq!(
            decode_worker_checkpoint(&encoded_checkpoint).unwrap(),
            checkpoint
        );
        assert_eq!(
            decode_worker_lease_release(&encoded_release).unwrap(),
            release
        );
        assert!(decode_worker_lease(&[encoded_lease, vec![0]].concat()).is_err());
        assert!(decode_worker_checkpoint(&[encoded_checkpoint, vec![0]].concat()).is_err());
        assert!(decode_worker_lease_release(&[encoded_release, vec![0]].concat()).is_err());
    }

    #[test]
    fn gc_execution_record_has_a_frozen_canonical_digest() {
        let record = GcExecutionRecord {
            plan_id: [13; 32],
            lease_id: MaintenanceLeaseId([14; 32]),
            roots_digest: [15; 32],
            node_deletes: 123,
            blob_deletes: 45,
            context: MaintenanceContext::new("gc-worker", "verified bounded sweep")
                .change_ticket("OPS-43"),
            state: GcExecutionState::Complete,
            started_at_millis: 1_700_000_700_000,
            completed_at_millis: Some(1_700_000_800_000),
        };
        let encoded = encode_gc_execution_record(&record).unwrap();
        assert_eq!(decode_gc_execution_record(&encoded).unwrap(), record);
        assert!(decode_gc_execution_record(&[encoded.clone(), vec![0]].concat()).is_err());

        assert_eq!(
            hex(prolly::Cid::from_bytes(&encoded).as_bytes()),
            "b145fce74b306a5e9b626cdedf00e5b861dde1c11cbbca55559986ea1e03ead7"
        );
    }

    fn hex(bytes: &[u8]) -> String {
        use std::fmt::Write;
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut output, "{byte:02x}").unwrap();
        }
        output
    }
}
