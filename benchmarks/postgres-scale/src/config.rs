use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::model::{Operation, Pattern, RANDOM_SEED};

pub const WORKLOAD_SCHEMA: u32 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadConfig {
    pub schema: u32,
    pub seed: u64,
    pub service: ServiceConfig,
    pub scale: ScaleConfig,
    #[serde(default)]
    pub regression: RegressionConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    pub enabled: bool,
    pub records: usize,
    pub value_bytes: usize,
    pub clients: Vec<usize>,
    pub pool_sizes: Vec<u32>,
    pub tenants: usize,
    pub hot_roots: usize,
    pub hot_root_share: f64,
    pub warmup_ms: u64,
    pub duration_ms: u64,
    pub operation_timeout_ms: u64,
    pub multi_read_keys: usize,
    pub commit_keys: usize,
    pub retained_versions: usize,
    pub cas_retries: u32,
    pub adapter_batch_items: usize,
    pub operation_mix: OperationMix,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationMix {
    pub point_read: u32,
    pub multi_read: u32,
    pub commit: u32,
    pub diff: u32,
    pub merge: u32,
}

impl OperationMix {
    pub const fn total(&self) -> u32 {
        self.point_read + self.multi_read + self.commit + self.diff + self.merge
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScaleConfig {
    pub enabled: bool,
    pub sizes: Vec<usize>,
    pub value_bytes: usize,
    pub runs: u32,
    pub changes: Option<usize>,
    pub read_samples: usize,
    pub concurrency: usize,
    pub operations: Vec<Operation>,
    pub patterns: Vec<Pattern>,
    pub min_free_gb: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RegressionConfig {
    pub max_throughput_loss_percent: f64,
    pub max_p99_increase_percent: f64,
    pub max_conflict_rate: f64,
    pub max_error_rate: f64,
    pub max_pg_statements_per_operation: f64,
    pub minimum_percentile_samples: u64,
}

impl Default for RegressionConfig {
    fn default() -> Self {
        Self {
            max_throughput_loss_percent: 10.0,
            max_p99_increase_percent: 20.0,
            max_conflict_rate: 1.0,
            max_error_rate: 0.0,
            max_pg_statements_per_operation: f64::MAX,
            minimum_percentile_samples: 1_000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuiteSelection {
    Service,
    Scale,
    Both,
}

impl SuiteSelection {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "service" => Ok(Self::Service),
            "scale" => Ok(Self::Scale),
            "both" => Ok(Self::Both),
            _ => Err(format!("unknown suite: {value}")),
        }
    }

    pub const fn runs_service(self) -> bool {
        matches!(self, Self::Service | Self::Both)
    }

    pub const fn runs_scale(self) -> bool {
        matches!(self, Self::Scale | Self::Both)
    }
}

#[derive(Clone, Debug)]
pub struct CommandConfig {
    pub workload: WorkloadConfig,
    pub workload_path: PathBuf,
    pub suites: SuiteSelection,
    pub url: String,
    pub output: PathBuf,
    pub revision: String,
    pub dirty: bool,
    pub baseline: Option<PathBuf>,
    pub allow_environment_mismatch: bool,
}

impl WorkloadConfig {
    pub fn load(path: &Path) -> Result<Self, String> {
        let source = std::fs::read_to_string(path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        Self::parse(&source)
    }

    pub fn parse(source: &str) -> Result<Self, String> {
        let config: Self =
            toml::from_str(source).map_err(|error| format!("invalid workload TOML: {error}"))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema != WORKLOAD_SCHEMA {
            return Err(format!(
                "unsupported workload schema {}, expected {WORKLOAD_SCHEMA}",
                self.schema
            ));
        }
        if self.service.enabled {
            self.service.validate()?;
        }
        if self.scale.enabled {
            self.scale.validate()?;
        }
        if !self.service.enabled && !self.scale.enabled {
            return Err("at least one suite must be enabled".to_string());
        }
        self.regression.validate()
    }

    pub fn canonical_toml(&self) -> Result<String, String> {
        toml::to_string(self).map_err(|error| format!("failed to serialize workload: {error}"))
    }

    pub fn configuration_hash(&self) -> Result<String, String> {
        let digest = Sha256::digest(self.canonical_toml()?.as_bytes());
        let mut output = String::with_capacity(digest.len() * 2);
        for byte in digest {
            write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
        }
        Ok(output)
    }

    pub fn default_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("workloads")
            .join("default.toml")
    }
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            schema: WORKLOAD_SCHEMA,
            seed: RANDOM_SEED,
            service: ServiceConfig::default(),
            scale: ScaleConfig::default(),
            regression: RegressionConfig::default(),
        }
    }
}

impl ServiceConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.records == 0
            || self.value_bytes == 0
            || self.clients.is_empty()
            || self.clients.contains(&0)
            || self.pool_sizes.is_empty()
            || self.pool_sizes.contains(&0)
            || self.tenants == 0
            || self.hot_roots > self.tenants
            || self.duration_ms == 0
            || self.operation_timeout_ms == 0
            || self.multi_read_keys == 0
            || self.commit_keys == 0
            || self.commit_keys.saturating_mul(2) > self.records
            || self.retained_versions < 3
            || self.adapter_batch_items == 0
        {
            return Err("service counts, durations, and batch sizes must be positive".to_string());
        }
        if !(0.0..=1.0).contains(&self.hot_root_share) || !self.hot_root_share.is_finite() {
            return Err("hot_root_share must be between 0 and 1".to_string());
        }
        if self.hot_root_share > 0.0 && self.hot_roots == 0 {
            return Err("positive hot_root_share requires at least one hot root".to_string());
        }
        if self.hot_root_share < 1.0 && self.hot_roots >= self.tenants {
            return Err(
                "hot_root_share below 1 requires at least one independent tenant".to_string(),
            );
        }
        if self.operation_mix.total() != 100 {
            return Err(format!(
                "service operation weights must total 100, observed {}",
                self.operation_mix.total()
            ));
        }
        Ok(())
    }
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            records: 1_000_000,
            value_bytes: 256,
            clients: vec![1, 8, 32, 64],
            pool_sizes: vec![8, 32],
            tenants: 64,
            hot_roots: 1,
            hot_root_share: 0.20,
            warmup_ms: 15_000,
            duration_ms: 60_000,
            operation_timeout_ms: 30_000,
            multi_read_keys: 32,
            commit_keys: 16,
            retained_versions: 32,
            cas_retries: 3,
            adapter_batch_items: 1_024,
            operation_mix: OperationMix {
                point_read: 45,
                multi_read: 15,
                commit: 25,
                diff: 10,
                merge: 5,
            },
        }
    }
}

impl ScaleConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.sizes.is_empty()
            || self.sizes.contains(&0)
            || self.value_bytes == 0
            || self.runs == 0
            || self.changes == Some(0)
            || self.read_samples == 0
            || self.concurrency == 0
            || self.operations.is_empty()
            || self.patterns.is_empty()
        {
            return Err("scale counts and filters must be positive".to_string());
        }
        if self.operations.contains(&Operation::Merge)
            && self.changes.is_some_and(|changes| changes % 2 != 0)
        {
            return Err("scale merge requires an even change count".to_string());
        }
        Ok(())
    }
}

impl Default for ScaleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sizes: vec![1_000_000, 10_000_000],
            value_bytes: 27,
            runs: 3,
            changes: None,
            read_samples: 10_000,
            concurrency: 32,
            operations: Operation::ALL.to_vec(),
            patterns: Pattern::ALL.to_vec(),
            min_free_gb: 3,
        }
    }
}

impl RegressionConfig {
    fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            (
                "max_throughput_loss_percent",
                self.max_throughput_loss_percent,
            ),
            ("max_p99_increase_percent", self.max_p99_increase_percent),
            ("max_conflict_rate", self.max_conflict_rate),
            ("max_error_rate", self.max_error_rate),
            (
                "max_pg_statements_per_operation",
                self.max_pg_statements_per_operation,
            ),
        ] {
            if value.is_nan() || value < 0.0 {
                return Err(format!("{name} must be nonnegative"));
            }
        }
        if self.minimum_percentile_samples == 0 {
            return Err("minimum_percentile_samples must be positive".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_service_contract() {
        let config = WorkloadConfig::load(&WorkloadConfig::default_path()).unwrap();
        config.validate().unwrap();
        assert_eq!(config.service.clients, vec![1, 8, 32, 64]);
        assert_eq!(config.service.pool_sizes, vec![8, 32]);
        assert_eq!(config.service.operation_mix.total(), 100);
        assert_eq!(config.service.value_bytes, 256);
        assert_eq!(config.service.adapter_batch_items, 1_024);
    }

    #[test]
    fn canonical_hash_tracks_resolved_values() {
        let first = WorkloadConfig::default();
        let mut second = first.clone();
        assert_eq!(
            first.configuration_hash().unwrap(),
            second.configuration_hash().unwrap()
        );
        second.service.clients.push(128);
        assert_ne!(
            first.configuration_hash().unwrap(),
            second.configuration_hash().unwrap()
        );
    }

    #[test]
    fn rejects_invalid_mix_and_zero_batch() {
        let mut config = WorkloadConfig::default();
        config.service.operation_mix.commit = 24;
        assert!(config.validate().unwrap_err().contains("total 100"));
        config.service.operation_mix.commit = 25;
        config.service.adapter_batch_items = 0;
        assert!(config.validate().unwrap_err().contains("positive"));
    }
}
