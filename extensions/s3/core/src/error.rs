use std::time::Duration;

/// Stable error categories exposed by the transport-independent core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorCode {
    InvalidRequest,
    UnsupportedParameter,
    InvalidBucket,
    InvalidKey,
    InvalidBranch,
    InvalidRevision,
    InvalidLimit,
    EntityTooLarge,
    IncompleteBody,
    RepositoryNotInitialized,
    RepositoryFormatConflict,
    UnsupportedRepositoryFormat,
    ProviderNotQualified,
    MissingCapability,
    NoSuchKey,
    NoSuchVersion,
    NoSuchBranch,
    NoSuchUpload,
    UploadConflict,
    NoSuchBatch,
    BatchExpired,
    BatchConflict,
    PreconditionFailed,
    NotModified,
    RefConflict,
    IdempotencyConflict,
    NoMergeBase,
    AmbiguousMergeBase,
    MergeConflict,
    HistoryLimitExceeded,
    InvalidContinuationToken,
    InvalidRange,
    ChecksumMismatch,
    CorruptNode,
    CorruptContent,
    CorruptCommit,
    MissingClosure,
    PermissionDenied,
    Throttled,
    Timeout,
    OperationCanceled,
    OutcomeUnknown,
    Transport,
    InternalInvariant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryAdvice {
    Never,
    Safe,
    After(Duration),
    ReloadHead,
    ReconcileOperation,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct Error {
    pub code: ErrorCode,
    pub retry: RetryAdvice,
    pub message: String,
    pub operation_id: Option<String>,
    pub provider_code: Option<Box<str>>,
    pub provider_message: Option<Box<str>>,
    pub provider_request_id: Option<Box<str>>,
}

impl Error {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            retry: RetryAdvice::Never,
            message: message.into(),
            operation_id: None,
            provider_code: None,
            provider_message: None,
            provider_request_id: None,
        }
    }

    pub fn retry(mut self, retry: RetryAdvice) -> Self {
        self.retry = retry;
        self
    }

    pub fn operation(mut self, operation: impl Into<String>) -> Self {
        self.operation_id = Some(operation.into());
        self
    }

    pub fn provider_metadata(
        mut self,
        code: Option<impl Into<String>>,
        message: Option<impl Into<String>>,
    ) -> Self {
        self.provider_code = code.map(|value| value.into().into_boxed_str());
        self.provider_message = message.map(|value| value.into().into_boxed_str());
        self
    }

    pub fn provider_request_id(mut self, request_id: Option<impl Into<String>>) -> Self {
        self.provider_request_id = request_id.map(|value| value.into().into_boxed_str());
        self
    }

    pub(crate) fn serialization(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InternalInvariant, message)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<prolly::Error> for Error {
    fn from(error: prolly::Error) -> Self {
        Self::new(
            ErrorCode::CorruptNode,
            format!("Prolly operation failed: {error}"),
        )
    }
}
