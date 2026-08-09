use thiserror::Error;

/// Errors produced by the Prolly-backed SQL storage engine.
#[derive(Debug, Error)]
pub enum Error {
    /// The underlying Prolly engine rejected an operation.
    #[error("prolly operation failed: {0}")]
    Prolly(#[from] prolly::Error),

    /// A persisted GlueSQL value could not be encoded or decoded.
    #[error("record codec failed: {0}")]
    Codec(#[from] bincode::Error),

    /// Persisted data uses an unsupported on-disk format.
    #[error("unsupported record format: {0}")]
    UnsupportedFormat(String),

    /// The requested storage operation is invalid in the current transaction state.
    #[error("invalid transaction state: {0}")]
    TransactionState(&'static str),

    /// A concurrent writer advanced the selected branch.
    #[error("serialization conflict: branch advanced while the transaction was active")]
    SerializationConflict,

    /// A generated row identifier exceeded the supported range.
    #[error("row identifier sequence overflowed for table {0:?}")]
    SequenceOverflow(String),

    /// Persisted bytes violate an internal storage invariant.
    #[error("corrupt database record: {0}")]
    Corrupt(String),

    /// A requested branch operation could not be completed.
    #[error("branch operation failed: {0}")]
    Branch(String),

    /// A merged logical state could not be materialized as SQL storage records.
    #[error("merge materialization failed: {0}")]
    Merge(String),

    /// A redb-backed storage engine could not be opened.
    #[cfg(feature = "redb")]
    #[error("redb store failed: {0}")]
    Redb(#[from] prolly_store_redb::RedbStoreError),

    /// A SQLite-backed storage engine could not be opened.
    #[cfg(feature = "sqlite")]
    #[error("sqlite store failed: {0}")]
    Sqlite(#[from] prolly_store_sqlite::SqliteStoreError),
}

/// Result type used by the public storage API.
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn glue_error(error: impl std::fmt::Display) -> gluesql_core::error::Error {
    gluesql_core::error::Error::StorageMsg(format!("[ProllyStorage] {error}"))
}
