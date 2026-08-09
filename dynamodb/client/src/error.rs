use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;

use aws_sdk_dynamodb::types::WriteRequest;

use crate::TableTransitionMetadata;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchWriteFailureDisposition {
    Terminal,
    OutcomeUnknown,
}

/// Structured partial failure. Successfully applied transitions are never
/// hidden, and an uncertain failed request is kept separate from requests that
/// were definitely never attempted.
#[derive(Debug)]
pub struct BatchWriteFailure {
    pub disposition: BatchWriteFailureDisposition,
    pub failed_table: String,
    pub failed_request: WriteRequest,
    pub unattempted_items: HashMap<String, Vec<WriteRequest>>,
    pub applied_transitions: Vec<TableTransitionMetadata>,
    pub transaction_token: Option<String>,
    cause: Box<Error>,
}

impl BatchWriteFailure {
    pub(crate) fn new(
        disposition: BatchWriteFailureDisposition,
        failed_table: String,
        failed_request: WriteRequest,
        unattempted_items: HashMap<String, Vec<WriteRequest>>,
        applied_transitions: Vec<TableTransitionMetadata>,
        transaction_token: Option<String>,
        cause: Error,
    ) -> Self {
        Self {
            disposition,
            failed_table,
            failed_request,
            unattempted_items,
            applied_transitions,
            transaction_token,
            cause: Box::new(cause),
        }
    }

    pub fn cause(&self) -> &Error {
        &self.cause
    }
}

impl fmt::Display for BatchWriteFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "BatchWriteItem stopped at table {:?} after {} accepted transitions ({:?})",
            self.failed_table,
            self.applied_transitions.len(),
            self.disposition
        )
    }
}

impl StdError for BatchWriteFailure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.cause.as_ref())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Core(#[from] prolly_dynamodb_core::Error),
    #[error(transparent)]
    Store(#[from] prolly_store_dynamodb::dynamodb::DynamoDbBackendError),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("unsupported request field or value: {0}")]
    Unsupported(String),
    #[error("conditional check failed")]
    ConditionalCheckFailed {
        item: Option<std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>,
    },
    #[error("logical table head conflict (expected {expected:?}, current {current:?})")]
    HeadConflict {
        expected: prolly::MapVersionId,
        current: Option<prolly::MapVersionId>,
    },
    #[error("transaction canceled")]
    TransactionCanceled {
        cancellation_reasons: Vec<aws_sdk_dynamodb::types::CancellationReason>,
    },
    #[error(transparent)]
    BatchWrite(#[from] Box<BatchWriteFailure>),
    #[error("stream worker sink rejected a commit: {source}")]
    WorkerSink {
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
}

impl Error {
    pub fn conditional_failure_item(
        &self,
    ) -> Option<&std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>> {
        match self {
            Self::ConditionalCheckFailed { item } => item.as_ref(),
            _ => None,
        }
    }

    pub fn batch_write_failure(&self) -> Option<&BatchWriteFailure> {
        match self {
            Self::BatchWrite(failure) => Some(failure),
            _ => None,
        }
    }

    pub fn cancellation_reasons(&self) -> Option<&[aws_sdk_dynamodb::types::CancellationReason]> {
        match self {
            Self::TransactionCanceled {
                cancellation_reasons,
            } => Some(cancellation_reasons),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WriteErrorDisposition {
    RetryableNotApplied,
    OutcomeUnknown { token: Option<String> },
    Terminal,
}

pub(crate) fn classify_write_error(error: &Error) -> WriteErrorDisposition {
    if matches!(
        error,
        Error::Core(prolly_dynamodb_core::Error::ConflictExhausted)
    ) {
        return WriteErrorDisposition::RetryableNotApplied;
    }
    let mut current: Option<&(dyn StdError + 'static)> = Some(error);
    while let Some(source) = current {
        if let Some(backend) =
            source.downcast_ref::<prolly_store_dynamodb::dynamodb::DynamoDbBackendError>()
        {
            use prolly_store_dynamodb::dynamodb::WriteFailureDisposition;
            return match backend.write_disposition() {
                WriteFailureDisposition::RetryableNotApplied => {
                    WriteErrorDisposition::RetryableNotApplied
                }
                WriteFailureDisposition::OutcomeUnknown => WriteErrorDisposition::OutcomeUnknown {
                    token: backend.transaction_token().map(str::to_owned),
                },
                WriteFailureDisposition::Terminal => WriteErrorDisposition::Terminal,
            };
        }
        current = source.source();
    }
    WriteErrorDisposition::Terminal
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    fn wrapped_backend(error: prolly_store_dynamodb::dynamodb::DynamoDbBackendError) -> Error {
        Error::Core(prolly_dynamodb_core::Error::Storage(prolly::Error::Store(
            Box::new(prolly::RemoteAdapterError::Backend(error)),
        )))
    }

    #[test]
    fn write_classification_survives_every_error_wrapper() {
        let retryable = wrapped_backend(
            prolly_store_dynamodb::dynamodb::DynamoDbBackendError::RetryableTransaction {
                token: "retry".into(),
                source: "throttled".into(),
            },
        );
        assert_eq!(
            classify_write_error(&retryable),
            WriteErrorDisposition::RetryableNotApplied
        );

        let unknown = wrapped_backend(
            prolly_store_dynamodb::dynamodb::DynamoDbBackendError::OutcomeUnknown {
                token: "reconcile-me".into(),
                source: "timeout".into(),
            },
        );
        assert_eq!(
            classify_write_error(&unknown),
            WriteErrorDisposition::OutcomeUnknown {
                token: Some("reconcile-me".into())
            }
        );

        assert_eq!(
            classify_write_error(&Error::Core(prolly_dynamodb_core::Error::ConflictExhausted)),
            WriteErrorDisposition::RetryableNotApplied
        );
    }
}
