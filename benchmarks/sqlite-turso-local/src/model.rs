//! Deterministic benchmark matrix and workload generation.

use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

pub const RANDOM_SEED: u64 = 0x6a09_e667_f3bc_c909;
pub const FULL_SIZES: &[usize] = &[10_000, 50_000, 100_000, 500_000, 1_000_000, 2_000_000];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Adapter {
    SqliteSync,
    TursoAsync,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Api {
    Put,
    Batch,
    Diff,
    Merge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Pattern {
    Append,
    Random,
    Clustered,
}

impl Adapter {
    pub const ALL: [Self; 2] = [Self::SqliteSync, Self::TursoAsync];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SqliteSync => "sqlite-sync",
            Self::TursoAsync => "turso-async",
        }
    }
}

impl Api {
    pub const ALL: [Self; 4] = [Self::Put, Self::Batch, Self::Diff, Self::Merge];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Batch => "batch",
            Self::Diff => "diff",
            Self::Merge => "merge",
        }
    }
}

impl Pattern {
    pub const ALL: [Self; 3] = [Self::Append, Self::Random, Self::Clustered];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Random => "random",
            Self::Clustered => "clustered",
        }
    }
}

impl FromStr for Adapter {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "sqlite-sync" => Ok(Self::SqliteSync),
            "turso-async" => Ok(Self::TursoAsync),
            _ => Err(format!("unknown adapter: {value}")),
        }
    }
}

impl FromStr for Api {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "put" => Ok(Self::Put),
            "batch" => Ok(Self::Batch),
            "diff" => Ok(Self::Diff),
            "merge" => Ok(Self::Merge),
            _ => Err(format!("unknown API: {value}")),
        }
    }
}

impl FromStr for Pattern {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "append" => Ok(Self::Append),
            "random" => Ok(Self::Random),
            "clustered" => Ok(Self::Clustered),
            _ => Err(format!("unknown pattern: {value}")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunConfig {
    pub output: PathBuf,
    pub revision: String,
    pub dirty: bool,
    pub adapters: Vec<Adapter>,
    pub sizes: Vec<usize>,
    pub runs: usize,
    pub apis: Vec<Api>,
    pub patterns: Vec<Pattern>,
    pub explicit_changes: Option<usize>,
    pub max_seconds: Option<u64>,
    pub min_free_bytes: u64,
    pub keep_fixtures: bool,
    pub tokio_workers: usize,
    pub build_batch_size: usize,
    pub measurement_samples: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixtureSpec {
    pub adapter: Adapter,
    pub records: usize,
    pub repetition: usize,
    pub revision: String,
    pub dirty: bool,
    pub build_batch_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellSpec {
    pub adapter: Adapter,
    pub records: usize,
    pub repetition: usize,
    pub api: Api,
    pub pattern: Pattern,
    pub changes: usize,
    pub revision: String,
    pub dirty: bool,
    pub measurement_samples: usize,
}

impl CellSpec {
    pub fn expected_operations(&self) -> usize {
        match self.api {
            Api::Merge => self.changes.saturating_mul(2),
            Api::Put | Api::Batch | Api::Diff => self.changes,
        }
    }

    pub fn expected_records(&self) -> usize {
        match (self.api, self.pattern) {
            (Api::Merge, Pattern::Append) => {
                self.records.saturating_add(self.changes.saturating_mul(2))
            }
            (_, Pattern::Append) => self.records.saturating_add(self.changes),
            _ => self.records,
        }
    }
}

impl RunConfig {
    pub fn smoke(output: PathBuf) -> Self {
        Self {
            output,
            revision: "unknown".to_string(),
            dirty: true,
            adapters: Adapter::ALL.to_vec(),
            sizes: vec![100],
            runs: 1,
            apis: Api::ALL.to_vec(),
            patterns: Pattern::ALL.to_vec(),
            explicit_changes: Some(10),
            max_seconds: None,
            min_free_bytes: 0,
            keep_fixtures: false,
            tokio_workers: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
            build_batch_size: 50_000,
            measurement_samples: 1,
        }
    }

    pub fn full(output: PathBuf, revision: String, dirty: bool) -> Self {
        Self {
            output,
            revision,
            dirty,
            adapters: Adapter::ALL.to_vec(),
            sizes: FULL_SIZES.to_vec(),
            runs: 3,
            apis: Api::ALL.to_vec(),
            patterns: Pattern::ALL.to_vec(),
            explicit_changes: None,
            max_seconds: None,
            min_free_bytes: 0,
            keep_fixtures: false,
            tokio_workers: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
            build_batch_size: 50_000,
            measurement_samples: 1,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.output.as_os_str().is_empty() {
            return Err("output path must not be empty".to_string());
        }
        if self.revision.is_empty() {
            return Err("revision must not be empty".to_string());
        }
        if self.runs == 0 {
            return Err("runs must be positive".to_string());
        }
        if self.sizes.is_empty() || self.sizes.contains(&0) {
            return Err("sizes must be non-empty and positive".to_string());
        }
        if self.adapters.is_empty() || self.apis.is_empty() || self.patterns.is_empty() {
            return Err("adapter, API, and pattern filters must be non-empty".to_string());
        }
        ensure_unique(&self.adapters)?;
        ensure_unique(&self.sizes)?;
        ensure_unique(&self.apis)?;
        ensure_unique(&self.patterns)?;
        if self.explicit_changes == Some(0) {
            return Err("explicit changes must be positive".to_string());
        }
        if let Some(changes) = self.explicit_changes {
            if self
                .sizes
                .iter()
                .any(|records| changes.saturating_mul(2) > *records)
            {
                return Err("explicit changes must leave two disjoint merge branches".to_string());
            }
        }
        if self.max_seconds == Some(0) {
            return Err("maximum seconds must be positive".to_string());
        }
        if self.tokio_workers == 0 {
            return Err("Tokio worker count must be positive".to_string());
        }
        if self.build_batch_size == 0 {
            return Err("fixture build batch size must be positive".to_string());
        }
        if !(1..=254).contains(&self.measurement_samples) {
            return Err("measurement samples must be between 1 and 254".to_string());
        }
        Ok(())
    }
}

fn ensure_unique<T: Ord + Copy>(values: &[T]) -> Result<(), String> {
    if values.iter().copied().collect::<BTreeSet<_>>().len() != values.len() {
        return Err("filter contains a duplicate value".to_string());
    }
    Ok(())
}

pub fn change_count(records: usize) -> usize {
    (records / 100).clamp(100, 10_000).min(records)
}

pub fn key(id: usize) -> Vec<u8> {
    format!("key-{id:020}").into_bytes()
}

pub fn value(id: usize, generation: u8) -> Vec<u8> {
    format!("value-{id:020}-{generation:02}-payload").into_bytes()
}

pub fn mutation_ids(pattern: Pattern, records: usize, count: usize, seed: u64) -> Vec<usize> {
    match pattern {
        Pattern::Append => (records..records.saturating_add(count)).collect(),
        Pattern::Random => random_ids(records, count, seed),
        Pattern::Clustered => clustered_ids(records, count),
    }
}

pub fn merge_ids(
    pattern: Pattern,
    records: usize,
    count: usize,
    seed: u64,
) -> (Vec<usize>, Vec<usize>) {
    let combined = match pattern {
        Pattern::Append => (records..records.saturating_add(count.saturating_mul(2))).collect(),
        Pattern::Random => random_ids(records, count.saturating_mul(2), seed),
        Pattern::Clustered => clustered_ids(records, count.saturating_mul(2)),
    };
    let split = combined.len() / 2;
    (combined[..split].to_vec(), combined[split..].to_vec())
}

fn random_ids(records: usize, count: usize, mut state: u64) -> Vec<usize> {
    let wanted = count.min(records);
    let mut seen = HashSet::with_capacity(wanted);
    let mut ids = Vec::with_capacity(wanted);
    while ids.len() < wanted {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let id = (state as usize) % records;
        if seen.insert(id) {
            ids.push(id);
        }
    }
    ids
}

fn clustered_ids(records: usize, count: usize) -> Vec<usize> {
    let wanted = count.min(records);
    let start = records.saturating_sub(wanted) / 2;
    (start..start + wanted).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_count_uses_approved_bounds() {
        assert_eq!(change_count(10_000), 100);
        assert_eq!(change_count(50_000), 500);
        assert_eq!(change_count(1_000_000), 10_000);
        assert_eq!(change_count(2_000_000), 10_000);
    }

    #[test]
    fn keys_and_values_have_frozen_width_and_order() {
        assert_eq!(key(7).len(), 24);
        assert_eq!(value(7, 1).len(), 37);
        assert!(key(7) < key(8));
    }

    #[test]
    fn random_and_clustered_inputs_are_deterministic_and_disjoint() {
        let first = mutation_ids(Pattern::Random, 10_000, 100, RANDOM_SEED);
        assert_eq!(
            first,
            mutation_ids(Pattern::Random, 10_000, 100, RANDOM_SEED)
        );
        assert_eq!(first.iter().collect::<BTreeSet<_>>().len(), 100);

        let (left, right) = merge_ids(Pattern::Clustered, 10_000, 100, RANDOM_SEED);
        assert!(left.iter().all(|id| !right.contains(id)));
    }

    #[test]
    fn full_profile_freezes_provenance_and_build_settings() {
        let config = RunConfig::full(PathBuf::from("output"), "abc123".to_string(), true);
        assert_eq!(config.sizes, FULL_SIZES);
        assert_eq!(config.runs, 3);
        assert_eq!(config.revision, "abc123");
        assert!(config.dirty);
        assert_eq!(config.build_batch_size, 50_000);
        config.validate().unwrap();
    }

    #[test]
    fn enum_filters_parse_frozen_names_and_reject_unknown_values() {
        assert_eq!("sqlite-sync".parse(), Ok(Adapter::SqliteSync));
        assert_eq!("turso-async".parse(), Ok(Adapter::TursoAsync));
        assert_eq!("batch".parse(), Ok(Api::Batch));
        assert_eq!("clustered".parse(), Ok(Pattern::Clustered));
        assert_eq!(Adapter::TursoAsync.as_str(), "turso-async");
        assert_eq!(Api::Merge.as_str(), "merge");
        assert_eq!(Pattern::Random.as_str(), "random");
        assert!(Adapter::from_str("sqlite").is_err());
    }

    #[test]
    fn smoke_configuration_explicitly_overrides_change_floor() {
        let config = RunConfig::smoke(PathBuf::from("smoke"));
        assert_eq!(config.sizes, vec![100]);
        assert_eq!(config.runs, 1);
        assert_eq!(config.explicit_changes, Some(10));
        assert_eq!(config.adapters.len(), 2);
        assert_eq!(config.apis.len(), 4);
        assert_eq!(config.patterns.len(), 3);
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn configuration_rejects_empty_duplicate_and_zero_dimensions() {
        let mut config = RunConfig::smoke(PathBuf::from("smoke"));
        config.runs = 0;
        assert!(config.validate().unwrap_err().contains("runs"));

        let mut config = RunConfig::smoke(PathBuf::from("smoke"));
        config.adapters.push(Adapter::SqliteSync);
        assert!(config.validate().unwrap_err().contains("duplicate"));

        let mut config = RunConfig::smoke(PathBuf::from("smoke"));
        config.sizes.clear();
        assert!(config.validate().unwrap_err().contains("sizes"));
    }
}
