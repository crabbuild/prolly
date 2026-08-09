use thiserror::Error;

/// Stable logical error categories returned by the transport-independent core.
#[derive(Debug, Error)]
pub enum Error {
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("unsupported operation or expression: {0}")]
    Unsupported(String),
    #[error("table {0:?} already exists")]
    TableAlreadyExists(String),
    #[error("table {0:?} was not found")]
    TableNotFound(String),
    #[error("table {0:?} is not active")]
    TableNotActive(String),
    #[error("table {table:?} no longer refers to the expected incarnation")]
    TableIncarnationChanged { table: String },
    #[error("conditional check failed")]
    ConditionalCheckFailed { old_item: Option<crate::Item> },
    #[error("transaction canceled")]
    TransactionCanceled {
        reasons: Vec<crate::TransactionCancellationReason>,
    },
    #[error("idempotent parameter mismatch for client request token")]
    IdempotentParameterMismatch,
    #[error("expected head for table {table:?} was {expected}, current head is {current}")]
    ExpectedHeadMismatch {
        table: String,
        expected: prolly::MapVersionId,
        current: prolly::MapVersionId,
    },
    #[error("optimistic conflict retry budget exhausted")]
    ConflictExhausted,
    #[error("maintenance plan is stale: {0}")]
    MaintenancePlanStale(String),
    #[error("import plan is stale: {0}")]
    ImportPlanStale(String),
    #[error("writes are fenced by maintenance lease {lease_id}")]
    MaintenanceInProgress { lease_id: crate::MaintenanceLeaseId },
    #[error("worker job {job_id} is leased by another owner until {expires_at_millis}")]
    WorkerLeaseHeld {
        job_id: crate::WorkerJobId,
        expires_at_millis: u64,
    },
    #[error("worker job {job_id} lease or fencing token is no longer valid")]
    WorkerLeaseLost { job_id: crate::WorkerJobId },
    #[error("worker job {job_id} checkpoint revision changed")]
    WorkerCheckpointConflict { job_id: crate::WorkerJobId },
    #[error("serialization failed: {0}")]
    Serialization(String),
    #[error("stored data is corrupt: {0}")]
    CorruptData(String),
    #[error("database format is incompatible: {0}")]
    FormatMismatch(String),
    #[error("blob storage failed: {0}")]
    Blob(String),
    #[error("storage operation failed: {0}")]
    Storage(#[from] prolly::Error),
    #[error("secure random identifier generation failed: {0}")]
    Random(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
