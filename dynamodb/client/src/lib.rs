//! AWS SDK-shaped Rust client for logical versioned DynamoDB tables.
#![recursion_limit = "256"]

mod blob;
mod capabilities;
mod client;
mod conversion;
mod error;
mod gc;
mod metadata;
pub mod operation;
mod table;
mod worker;

pub use capabilities::{CapabilityReport, CompatibilityLevel, OperationCapability};
pub use client::{Client, ClientBuilder, DEFAULT_NODE_CACHE_MAX_BYTES};
pub use error::{BatchWriteFailure, BatchWriteFailureDisposition, Error, Result};
pub use gc::{
    GcApplyOptions, GcApplyResult, GcBlobCandidate, GcCursor, GcPlan, GcPlanId, GcPlanLimits,
    MAX_GC_BLOB_DELETE_PARALLELISM,
};
pub use metadata::{TableTransitionMetadata, WithMetadata};
pub use prolly_dynamodb_core::{
    CommitId, ImportAuditRecord, ImportPlan, ImportPlanId, ImportResult,
    IndexReconfigurationAuditRecord, IndexReconfigurationPlan, IndexReconfigurationPlanId,
    IndexReconfigurationResult, KeyAttribute, KeyKind, MaintenanceContext, MaintenanceLease,
    MaintenanceLeaseId, MaintenanceLeaseRelease, RetentionAuditRecord, RetentionPlan,
    RetentionPlanId, RetentionPolicy, RetentionResult, SecondaryIndexDefinition,
    SecondaryIndexKind, SecondaryIndexProjection, TableArchive, TableArchiveBlob,
    TableArchiveLimits, TableArchiveSummary, TableCommit, TableCommitPage,
    TransactWriteResult as Commit, WorkerCheckpoint, WorkerJobId, WorkerKind, WorkerLease,
    WorkerLeaseRelease, WorkerProgress, DEFAULT_LOGICAL_RETRY_LIMIT, MAX_GC_PLAN_DELETES,
    MAX_LOGICAL_RETRY_LIMIT, MAX_MAINTENANCE_LEASE_MILLIS, MAX_RETENTION_PROTECTED_VERSIONS,
    MAX_RETENTION_REMOVALS, MAX_WORKER_LEASE_MILLIS, MIN_MAINTENANCE_LEASE_MILLIS,
    MIN_WORKER_LEASE_MILLIS, TABLE_ARCHIVE_FORMAT_VERSION,
};
pub use table::{
    ConditionalTable, DiffPaginator, Import, Indexes, Restore, RetentionPlanner, Snapshot, Table,
    VersionsPaginator,
};
pub use tokio_util::sync::CancellationToken;
pub use worker::{
    MaintenanceWorker, StreamRun, StreamWorker, StreamWorkerOptions, TtlRun, TtlWorker,
    TtlWorkerOptions, Worker, WorkerExit, Workers, MAX_WORKER_PAGE_ITEMS, MAX_WORKER_SLEEP,
    MIN_WORKER_SLEEP,
};

pub use aws_sdk_dynamodb::types::AttributeValue;
pub use prolly::ProllyCacheUsage;
