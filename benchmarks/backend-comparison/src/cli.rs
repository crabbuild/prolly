use std::path::PathBuf;

use prolly_backend_workload_contract::WorkloadSpec;

use crate::Backend;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunConfig {
    pub backend: Backend,
    pub output: PathBuf,
    pub run_id: String,
    pub repetition: u32,
    pub revision: String,
    pub tree_hash: String,
    pub binary_sha256: String,
    pub workload: WorkloadSpec,
}

impl RunConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.run_id.is_empty() {
            return Err("run ID cannot be empty".to_string());
        }
        if self.repetition == 0 {
            return Err("repetition must be positive".to_string());
        }
        if !is_hex(&self.revision, 40) {
            return Err("revision must be a 40-character hexadecimal commit".to_string());
        }
        if !is_hex(&self.tree_hash, 40) {
            return Err("tree hash must be a 40-character hexadecimal value".to_string());
        }
        if !is_hex(&self.binary_sha256, 64) {
            return Err("binary hash must be a 64-character SHA-256 value".to_string());
        }
        if self.output.as_os_str().is_empty() {
            return Err("output path cannot be empty".to_string());
        }
        Ok(())
    }
}

pub(crate) fn is_hex(value: &str, length: usize) -> bool {
    value.len() == length && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prolly_backend_workload_contract::DEFAULT_SEED;

    #[test]
    fn run_config_rejects_unattributable_provenance() {
        let mut config = RunConfig {
            backend: Backend::Postgres,
            output: PathBuf::from("row.csv"),
            run_id: "run-1".to_string(),
            repetition: 1,
            revision: "a".repeat(40),
            tree_hash: "b".repeat(40),
            binary_sha256: "c".repeat(64),
            workload: WorkloadSpec {
                records: 100,
                value_bytes: 27,
                changes: 10,
                samples: 10,
                concurrency: 4,
                seed: DEFAULT_SEED,
            },
        };
        config.validate().unwrap();
        config.binary_sha256 = "unknown".to_string();
        assert!(config.validate().unwrap_err().contains("binary hash"));
    }
}
