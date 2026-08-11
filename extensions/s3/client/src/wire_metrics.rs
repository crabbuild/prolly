use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        interceptors::{
            context::{
                BeforeSerializationInterceptorContextRef, BeforeTransmitInterceptorContextRef,
                FinalizerInterceptorContextRef,
            },
            Intercept,
        },
        runtime_components::RuntimeComponents,
    },
};
use aws_smithy_types::config_bag::ConfigBag;

/// Smithy execution and HTTP-transmission counters for an instrumented S3
/// client. Attach [`S3WireAttemptInterceptor`] while constructing the caller's
/// `aws_sdk_s3::Client`; the adapter cannot retrofit an interceptor afterward.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct S3WireAttemptMetrics {
    pub executions: u64,
    pub transmissions: u64,
    pub completed_attempts: u64,
    pub informational_responses: u64,
    pub successful_responses: u64,
    pub redirection_responses: u64,
    pub client_error_responses: u64,
    pub server_error_responses: u64,
    pub unclassified_responses: u64,
    pub attempts_without_response: u64,
}

impl S3WireAttemptMetrics {
    /// Transmissions beyond the first transmission of each SDK execution.
    /// Read this only across a quiescent measurement interval.
    pub fn retry_transmissions(self) -> u64 {
        self.transmissions.saturating_sub(self.executions)
    }
}

#[derive(Debug, Default)]
struct WireAttemptCounters {
    executions: AtomicU64,
    transmissions: AtomicU64,
    completed_attempts: AtomicU64,
    informational_responses: AtomicU64,
    successful_responses: AtomicU64,
    redirection_responses: AtomicU64,
    client_error_responses: AtomicU64,
    server_error_responses: AtomicU64,
    unclassified_responses: AtomicU64,
    attempts_without_response: AtomicU64,
}

/// Cloneable AWS SDK interceptor that counts executions, actual transmit hooks,
/// retry attempts, and response status classes without inspecting credentials or
/// payloads.
#[derive(Clone, Debug, Default)]
pub struct S3WireAttemptInterceptor {
    counters: Arc<WireAttemptCounters>,
}

impl S3WireAttemptInterceptor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn metrics(&self) -> S3WireAttemptMetrics {
        S3WireAttemptMetrics {
            executions: self.counters.executions.load(Ordering::Relaxed),
            transmissions: self.counters.transmissions.load(Ordering::Relaxed),
            completed_attempts: self.counters.completed_attempts.load(Ordering::Relaxed),
            informational_responses: self
                .counters
                .informational_responses
                .load(Ordering::Relaxed),
            successful_responses: self.counters.successful_responses.load(Ordering::Relaxed),
            redirection_responses: self.counters.redirection_responses.load(Ordering::Relaxed),
            client_error_responses: self.counters.client_error_responses.load(Ordering::Relaxed),
            server_error_responses: self.counters.server_error_responses.load(Ordering::Relaxed),
            unclassified_responses: self.counters.unclassified_responses.load(Ordering::Relaxed),
            attempts_without_response: self
                .counters
                .attempts_without_response
                .load(Ordering::Relaxed),
        }
    }

    /// Resets the interval counters. Call only when no request using this
    /// interceptor is in flight; individual atomic swaps are not a transaction.
    pub fn reset(&self) -> S3WireAttemptMetrics {
        S3WireAttemptMetrics {
            executions: self.counters.executions.swap(0, Ordering::AcqRel),
            transmissions: self.counters.transmissions.swap(0, Ordering::AcqRel),
            completed_attempts: self.counters.completed_attempts.swap(0, Ordering::AcqRel),
            informational_responses: self
                .counters
                .informational_responses
                .swap(0, Ordering::AcqRel),
            successful_responses: self.counters.successful_responses.swap(0, Ordering::AcqRel),
            redirection_responses: self
                .counters
                .redirection_responses
                .swap(0, Ordering::AcqRel),
            client_error_responses: self
                .counters
                .client_error_responses
                .swap(0, Ordering::AcqRel),
            server_error_responses: self
                .counters
                .server_error_responses
                .swap(0, Ordering::AcqRel),
            unclassified_responses: self
                .counters
                .unclassified_responses
                .swap(0, Ordering::AcqRel),
            attempts_without_response: self
                .counters
                .attempts_without_response
                .swap(0, Ordering::AcqRel),
        }
    }
}

impl Intercept for S3WireAttemptInterceptor {
    fn name(&self) -> &'static str {
        "ProllyS3WireAttemptMetrics"
    }

    fn read_before_execution(
        &self,
        _context: &BeforeSerializationInterceptorContextRef<'_>,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        self.counters.executions.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn read_before_transmit(
        &self,
        _context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        self.counters.transmissions.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn read_after_attempt(
        &self,
        context: &FinalizerInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        self.counters
            .completed_attempts
            .fetch_add(1, Ordering::Relaxed);
        let Some(response) = context.response() else {
            self.counters
                .attempts_without_response
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        };
        let counter = match response.status().as_u16() {
            100..=199 => &self.counters.informational_responses,
            200..=299 => &self.counters.successful_responses,
            300..=399 => &self.counters.redirection_responses,
            400..=499 => &self.counters.client_error_responses,
            500..=599 => &self.counters.server_error_responses,
            _ => &self.counters.unclassified_responses,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
