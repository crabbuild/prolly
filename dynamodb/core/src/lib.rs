//! Transport-independent DynamoDB-compatible logical data model over Prolly.

mod archive;
mod blob;
mod database;
mod error;
mod expression;
mod format;
mod index;
mod model;

pub use blob::{BlobFuture, BlobStorage, InlineBlobStorage};
pub use database::{
    BatchGetResult, BatchGetTableRequest, BatchGetTableResult, BatchWriteAction,
    BatchWriteExecutionError, BatchWriteResult, BatchWriteTransition, Clock, CommitId, Database,
    GcExecutionRecord, GcExecutionResult, GcExecutionState, IdGenerator, ImportAuditRecord,
    ImportPlan, ImportPlanId, ImportResult, IndexQueryRequest, IndexReadPage,
    IndexReconfigurationAuditRecord, IndexReconfigurationPlan, IndexReconfigurationPlanId,
    IndexReconfigurationResult, ItemRead, ItemUpdate, ItemWrite, MaintenanceContext,
    MaintenanceLease, MaintenanceLeaseId, MaintenanceLeaseRelease, ReadPage, RestoreResult,
    RetentionAuditRecord, RetentionPlan, RetentionPlanId, RetentionPolicy, RetentionResult,
    SystemClock, SystemIdGenerator, TableCommit, TableCommitPage, TableLifecycleResult,
    TransactGetRequest, TransactGetResponse, TransactGetResult, TransactWriteAction,
    TransactWriteResult, TransactionCancellationCode, TransactionCancellationReason,
    TransactionTableTransition, TtlCandidate, TtlCandidatePage, WorkerCheckpoint, WorkerJobId,
    WorkerKind, WorkerLease, WorkerLeaseRelease, WorkerProgress, DEFAULT_LOGICAL_RETRY_LIMIT,
    MAX_BATCH_GET_ITEMS, MAX_BATCH_GET_PARTITION_BYTES, MAX_BATCH_GET_RESPONSE_BYTES,
    MAX_BATCH_WRITE_ITEMS, MAX_COLLECTED_DIFF_ITEMS, MAX_COLLECTED_VERSIONS, MAX_COMMIT_PAGE_ITEMS,
    MAX_DIFF_PAGE_ITEMS, MAX_GC_PLAN_DELETES, MAX_LOGICAL_RETRY_LIMIT,
    MAX_MAINTENANCE_LEASE_MILLIS, MAX_RETENTION_PROTECTED_VERSIONS, MAX_RETENTION_REMOVALS,
    MAX_TRANSACTION_BYTES, MAX_TRANSACTION_ITEMS, MAX_VERSION_PAGE_ITEMS, MAX_WORKER_LEASE_MILLIS,
    MIN_MAINTENANCE_LEASE_MILLIS, MIN_WORKER_LEASE_MILLIS, TTL_MAX_PAST_SECONDS,
};
pub use error::{Error, Result};
pub use expression::{
    parse_condition, parse_key_condition, parse_key_equality, parse_projection,
    parse_read_expressions, parse_update, ArithmeticOperator, AttributePath, ComparisonOperator,
    Condition, KeyCondition, ParsedReadExpressions, ParsedUpdate, PathElement, Projection,
    SetOperand, SortKeyCondition, UpdateAction, UpdateOperand, UpdatePlan,
};
pub use format::{DatabaseFormatRecord, StoragePublicationMode};
pub use model::{
    canonicalize_attribute_value, decode_item, encode_item, encode_key_schema,
    encode_partition_prefix, encode_primary_key, item_size, AttributeValue, DynamoNumber, Item,
    KeyAttribute, KeyKind, SecondaryIndexDefinition, SecondaryIndexDescription, SecondaryIndexId,
    SecondaryIndexKind, SecondaryIndexProjection, SecondaryIndexStatus, TableDescription, TableId,
    TableStatus, MAX_ITEM_BYTES,
};
pub use prolly::LargeValueConfig;

/// Persistent database format written before public client use.
pub const DATABASE_FORMAT_VERSION: u32 = 12;
pub use archive::{
    TableArchive, TableArchiveBlob, TableArchiveLimits, TableArchiveSummary,
    TABLE_ARCHIVE_FORMAT_VERSION,
};
