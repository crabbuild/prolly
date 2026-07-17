use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

#[repr(u32)]
// The complete stable code table is consumed incrementally as each facade
// family lands; keeping the numeric assignments together prevents renumbering.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ErrorCode {
    InvalidArgument = 1,
    InvalidHandle = 2,
    Closed = 3,
    Conflict = 4,
    StaleIndex = 5,
    InvalidProximity = 6,
    Verification = 7,
    Cancelled = 8,
    DeadlineExceeded = 9,
    Unsupported = 10,
    MalformedTransport = 11,
    Callback = 12,
    Internal = 255,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BindingError {
    pub(crate) code: ErrorCode,
    pub(crate) message: String,
    pub(crate) details: BTreeMap<String, String>,
}

impl BindingError {
    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: BTreeMap::new(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }

    pub(crate) fn invalid_handle(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidHandle, message)
    }

    pub(crate) fn malformed_transport(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::MalformedTransport, message)
    }
}

impl Display for BindingError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} (code {})", self.message, self.code as u32)
    }
}

impl Error for BindingError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_error_preserves_stable_code_and_details() {
        let error = BindingError::new(ErrorCode::StaleIndex, "index is stale")
            .with_detail("index", "by_team")
            .with_detail("expected_generation", "4");

        assert_eq!(error.code, ErrorCode::StaleIndex);
        assert_eq!(error.message, "index is stale");
        assert_eq!(error.details["index"], "by_team");
        assert_eq!(error.details["expected_generation"], "4");
    }

    #[test]
    fn stable_error_codes_do_not_renumber() {
        assert_eq!(
            [
                ErrorCode::InvalidArgument as u32,
                ErrorCode::InvalidHandle as u32,
                ErrorCode::Closed as u32,
                ErrorCode::Conflict as u32,
                ErrorCode::StaleIndex as u32,
                ErrorCode::InvalidProximity as u32,
                ErrorCode::Verification as u32,
                ErrorCode::Cancelled as u32,
                ErrorCode::DeadlineExceeded as u32,
                ErrorCode::Unsupported as u32,
                ErrorCode::MalformedTransport as u32,
                ErrorCode::Callback as u32,
                ErrorCode::Internal as u32,
            ],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 255],
        );
    }
}
