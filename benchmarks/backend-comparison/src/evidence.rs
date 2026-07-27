use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::cli::is_hex;

pub const RESULT_SCHEMA: &str = "backend-comparison-v1";
pub const TIMED_SCOPE_VERSION: &str = "public-prolly-operation-v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    Postgres,
    DynamoDbLocal,
}

impl Backend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::DynamoDbLocal => "dynamodb_local",
        }
    }
}

impl fmt::Display for Backend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Backend {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "postgres" => Ok(Self::Postgres),
            "dynamodb_local" | "dynamodb" => Ok(Self::DynamoDbLocal),
            _ => Err(format!("unsupported backend: {value}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Build,
    Batch,
    Query,
    ConcurrentQuery,
    Diff,
    Merge,
}

impl Operation {
    pub const ALL: [Self; 6] = [
        Self::Build,
        Self::Batch,
        Self::Query,
        Self::ConcurrentQuery,
        Self::Diff,
        Self::Merge,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Batch => "batch",
            Self::Query => "query",
            Self::ConcurrentQuery => "concurrent_query",
            Self::Diff => "diff",
            Self::Merge => "merge",
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EvidenceRow {
    pub schema: String,
    pub timed_scope_version: String,
    pub contract_version: String,
    pub run_id: String,
    pub backend: Backend,
    pub repetition: u32,
    pub operation: Operation,
    pub revision: String,
    pub tree_hash: String,
    pub binary_sha256: String,
    pub records: u64,
    pub value_bytes: u64,
    pub changes: u64,
    pub samples: u64,
    pub concurrency: u64,
    pub seed: u64,
    pub logical_operations: u64,
    pub observed_items: u64,
    pub total_ns: u128,
    pub ops_per_sec: f64,
    pub root: String,
    pub workload_digest: String,
    pub outcome_digest: String,
    pub validated: bool,
    pub error: String,
}

impl EvidenceRow {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema != RESULT_SCHEMA {
            return Err(format!("unsupported result schema: {}", self.schema));
        }
        if self.timed_scope_version != TIMED_SCOPE_VERSION {
            return Err(format!(
                "unsupported timed scope: {}",
                self.timed_scope_version
            ));
        }
        if self.contract_version.is_empty() || self.run_id.is_empty() {
            return Err("contract version and run ID must be present".to_string());
        }
        if self.repetition == 0
            || self.records == 0
            || self.value_bytes == 0
            || self.changes == 0
            || self.samples == 0
            || self.concurrency == 0
            || self.logical_operations == 0
            || self.total_ns == 0
        {
            return Err("measurement dimensions and timing must be positive".to_string());
        }
        if !self.validated || !self.error.is_empty() {
            return Err(format!(
                "row did not validate: {}",
                if self.error.is_empty() {
                    "unspecified validation failure"
                } else {
                    &self.error
                }
            ));
        }
        if !is_hex(&self.revision, 40)
            || !is_hex(&self.tree_hash, 40)
            || !is_hex(&self.binary_sha256, 64)
        {
            return Err("source and binary provenance must be hexadecimal".to_string());
        }
        if !is_hex(&self.root, 64) {
            return Err("root must be a 64-character hexadecimal CID".to_string());
        }
        if !is_hex(&self.workload_digest, 64) {
            return Err("workload digest must be a 64-character SHA-256 value".to_string());
        }
        if !is_hex(&self.outcome_digest, 64) {
            return Err("outcome digest must be a 64-character SHA-256 value".to_string());
        }
        let expected = self.logical_operations as f64 * 1_000_000_000.0 / self.total_ns as f64;
        if !self.ops_per_sec.is_finite()
            || (self.ops_per_sec - expected).abs() > expected.abs().max(1.0) * 1e-9
        {
            return Err("throughput does not match elapsed time".to_string());
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn example() -> Self {
        Self {
            schema: RESULT_SCHEMA.to_string(),
            timed_scope_version: TIMED_SCOPE_VERSION.to_string(),
            contract_version: "backend-workload-v1".to_string(),
            run_id: "run-1".to_string(),
            backend: Backend::Postgres,
            repetition: 1,
            operation: Operation::Build,
            revision: "a".repeat(40),
            tree_hash: "b".repeat(40),
            binary_sha256: "c".repeat(64),
            records: 100,
            value_bytes: 27,
            changes: 10,
            samples: 10,
            concurrency: 4,
            seed: 1,
            logical_operations: 100,
            observed_items: 100,
            total_ns: 1_000,
            ops_per_sec: 100_000_000.0,
            root: "d".repeat(64),
            workload_digest: "e".repeat(64),
            outcome_digest: "f".repeat(64),
            validated: true,
            error: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_row_checks_all_evidence() {
        let row = EvidenceRow::example();
        row.validate().unwrap();

        let mut broken = row.clone();
        broken.ops_per_sec = 1.0;
        assert!(broken.validate().unwrap_err().contains("throughput"));

        let mut broken = row.clone();
        broken.outcome_digest.clear();
        assert!(broken.validate().unwrap_err().contains("outcome digest"));

        let mut broken = row;
        broken.validated = false;
        assert!(broken.validate().is_err());
    }
}
