use std::time::Duration;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use std::time::Instant;

use super::super::error::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationBudget {
    pub max_input_records: usize,
    pub max_input_bytes: usize,
    pub max_derived_entries: usize,
    pub max_derived_bytes: usize,
    pub max_accounted_memory_bytes: usize,
    pub max_cas_attempts: usize,
    pub max_elapsed: Duration,
}

impl Default for MutationBudget {
    fn default() -> Self {
        Self {
            max_input_records: 10_000,
            max_input_bytes: 64 * 1024 * 1024,
            max_derived_entries: 100_000,
            max_derived_bytes: 64 * 1024 * 1024,
            max_accounted_memory_bytes: 128 * 1024 * 1024,
            max_cas_attempts: 8,
            max_elapsed: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryBudget {
    pub max_page_entries: usize,
    pub max_returned_entries: usize,
    pub max_returned_bytes: usize,
    pub max_scanned_entries: usize,
    pub max_source_fetches: usize,
    pub max_accounted_memory_bytes: usize,
    pub max_elapsed: Duration,
}

impl Default for QueryBudget {
    fn default() -> Self {
        Self {
            max_page_entries: 4_096,
            max_returned_entries: 4_096,
            max_returned_bytes: 64 * 1024 * 1024,
            max_scanned_entries: 1_000_000,
            max_source_fetches: 4_096,
            max_accounted_memory_bytes: 128 * 1024 * 1024,
            max_elapsed: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaintenanceBudget {
    pub max_source_entries: usize,
    pub max_derived_entries: usize,
    pub max_verification_findings: usize,
    pub max_accounted_memory_bytes: usize,
    pub max_spill_bytes: usize,
    pub max_spill_runs: usize,
    pub max_merge_fan_in: usize,
    pub max_cas_attempts: usize,
    pub max_elapsed: Duration,
}

impl Default for MaintenanceBudget {
    fn default() -> Self {
        Self {
            max_source_entries: 10_000_000,
            max_derived_entries: 10_000_000,
            max_verification_findings: 10_000,
            max_accounted_memory_bytes: 256 * 1024 * 1024,
            max_spill_bytes: 2 * 1024 * 1024 * 1024,
            max_spill_runs: 4_096,
            max_merge_fan_in: 64,
            max_cas_attempts: 8,
            max_elapsed: Duration::from_secs(600),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferBudget {
    pub max_encoded_bytes: usize,
    pub max_nodes: usize,
    pub max_decoded_bytes: usize,
    pub max_verification_work: usize,
    pub max_accounted_memory_bytes: usize,
    pub max_elapsed: Duration,
}

impl Default for TransferBudget {
    fn default() -> Self {
        Self {
            max_encoded_bytes: 1024 * 1024 * 1024,
            max_nodes: 1_000_000,
            max_decoded_bytes: 1024 * 1024 * 1024,
            max_verification_work: 10_000_000,
            max_accounted_memory_bytes: 256 * 1024 * 1024,
            max_elapsed: Duration::from_secs(600),
        }
    }
}

fn require_nonzero(values: &[(&'static str, usize)]) -> Result<(), Error> {
    if let Some((field, value)) = values.iter().find(|(_, value)| *value == 0) {
        return Err(Error::InvalidExecutionConfig {
            field,
            value: *value,
        });
    }
    Ok(())
}

impl MutationBudget {
    pub fn validate(&self) -> Result<(), Error> {
        require_nonzero(&[
            ("mutation.max_input_records", self.max_input_records),
            ("mutation.max_input_bytes", self.max_input_bytes),
            ("mutation.max_derived_entries", self.max_derived_entries),
            ("mutation.max_derived_bytes", self.max_derived_bytes),
            (
                "mutation.max_accounted_memory_bytes",
                self.max_accounted_memory_bytes,
            ),
            ("mutation.max_cas_attempts", self.max_cas_attempts),
            (
                "mutation.max_elapsed_millis",
                usize::try_from(self.max_elapsed.as_millis()).unwrap_or(usize::MAX),
            ),
        ])
    }
}

impl QueryBudget {
    pub fn validate(&self) -> Result<(), Error> {
        require_nonzero(&[
            ("query.max_page_entries", self.max_page_entries),
            ("query.max_returned_entries", self.max_returned_entries),
            ("query.max_returned_bytes", self.max_returned_bytes),
            ("query.max_scanned_entries", self.max_scanned_entries),
            ("query.max_source_fetches", self.max_source_fetches),
            (
                "query.max_accounted_memory_bytes",
                self.max_accounted_memory_bytes,
            ),
            (
                "query.max_elapsed_millis",
                usize::try_from(self.max_elapsed.as_millis()).unwrap_or(usize::MAX),
            ),
        ])
    }
}

impl MaintenanceBudget {
    pub fn validate(&self) -> Result<(), Error> {
        require_nonzero(&[
            ("maintenance.max_source_entries", self.max_source_entries),
            ("maintenance.max_derived_entries", self.max_derived_entries),
            (
                "maintenance.max_verification_findings",
                self.max_verification_findings,
            ),
            (
                "maintenance.max_accounted_memory_bytes",
                self.max_accounted_memory_bytes,
            ),
            ("maintenance.max_spill_bytes", self.max_spill_bytes),
            ("maintenance.max_spill_runs", self.max_spill_runs),
            ("maintenance.max_merge_fan_in", self.max_merge_fan_in),
            ("maintenance.max_cas_attempts", self.max_cas_attempts),
            (
                "maintenance.max_elapsed_millis",
                usize::try_from(self.max_elapsed.as_millis()).unwrap_or(usize::MAX),
            ),
        ])
    }
}

impl TransferBudget {
    pub fn validate(&self) -> Result<(), Error> {
        require_nonzero(&[
            ("transfer.max_encoded_bytes", self.max_encoded_bytes),
            ("transfer.max_nodes", self.max_nodes),
            ("transfer.max_decoded_bytes", self.max_decoded_bytes),
            ("transfer.max_verification_work", self.max_verification_work),
            (
                "transfer.max_accounted_memory_bytes",
                self.max_accounted_memory_bytes,
            ),
            (
                "transfer.max_elapsed_millis",
                usize::try_from(self.max_elapsed.as_millis()).unwrap_or(usize::MAX),
            ),
        ])
    }
}

#[allow(dead_code)]
pub(crate) struct BudgetCounter {
    started: Deadline,
}

pub(crate) struct Deadline {
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    started: Instant,
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    started_millis: f64,
}

impl Deadline {
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
            started: Instant::now(),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            started_millis: js_sys::Date::now(),
        }
    }

    pub(crate) fn elapsed_millis(&self) -> u128 {
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        {
            self.started.elapsed().as_millis()
        }
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        {
            (js_sys::Date::now() - self.started_millis).max(0.0) as u128
        }
    }

    pub(crate) fn exceeded(&self, limit: Duration) -> bool {
        self.elapsed_millis() > limit.as_millis()
    }
}

#[allow(dead_code)]
impl BudgetCounter {
    pub(crate) fn new() -> Self {
        Self {
            started: Deadline::new(),
        }
    }

    pub(crate) fn charge(
        &self,
        resource: &'static str,
        current: &mut usize,
        additional: usize,
        limit: usize,
    ) -> Result<(), Error> {
        let actual = current
            .checked_add(additional)
            .ok_or(Error::IndexResourceLimitExceeded {
                resource,
                limit,
                actual: usize::MAX,
            })?;
        if actual > limit {
            return Err(Error::IndexResourceLimitExceeded {
                resource,
                limit,
                actual,
            });
        }
        *current = actual;
        Ok(())
    }

    pub(crate) fn check_elapsed(
        &self,
        resource: &'static str,
        limit: Duration,
    ) -> Result<(), Error> {
        if self.started.exceeded(limit) {
            return Err(Error::IndexResourceLimitExceeded {
                resource,
                limit: usize::try_from(limit.as_millis()).unwrap_or(usize::MAX),
                actual: usize::try_from(self.started.elapsed_millis()).unwrap_or(usize::MAX),
            });
        }
        Ok(())
    }
}
