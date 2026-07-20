use std::collections::BTreeSet;
use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

pub const RANDOM_SEED: u64 = 0x6a09_e667_f3bc_c909;
pub const FULL_SIZES: &[usize] = &[1_000_000];

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Build,
    Put,
    Batch,
    GetCold,
    GetWarm,
    Query,
    Scan,
    FullScan,
    Diff,
    Merge,
}

impl Operation {
    pub const ALL: [Self; 9] = [
        Self::Put,
        Self::Batch,
        Self::GetCold,
        Self::GetWarm,
        Self::Query,
        Self::Scan,
        Self::FullScan,
        Self::Diff,
        Self::Merge,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Put => "put",
            Self::Batch => "batch",
            Self::GetCold => "get_cold",
            Self::GetWarm => "get_warm",
            Self::Query => "query",
            Self::Scan => "scan",
            Self::FullScan => "full_scan",
            Self::Diff => "diff",
            Self::Merge => "merge",
        }
    }
}

impl FromStr for Operation {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "build" => Ok(Self::Build),
            "put" => Ok(Self::Put),
            "batch" => Ok(Self::Batch),
            "get_cold" => Ok(Self::GetCold),
            "get_warm" => Ok(Self::GetWarm),
            "query" => Ok(Self::Query),
            "scan" => Ok(Self::Scan),
            "full_scan" => Ok(Self::FullScan),
            "diff" => Ok(Self::Diff),
            "merge" => Ok(Self::Merge),
            _ => Err(format!("unknown operation: {value}")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Pattern {
    Append,
    Random,
    Clustered,
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

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CacheState {
    NotApplicable,
    ColdManager,
    WarmManager,
}

impl CacheState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotApplicable => "n/a",
            Self::ColdManager => "cold-manager",
            Self::WarmManager => "warm-manager",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunConfig {
    pub output: PathBuf,
    pub redis_url: String,
    pub revision: String,
    pub dirty: bool,
    pub sizes: Vec<usize>,
    pub runs: usize,
    pub operations: Vec<Operation>,
    pub patterns: Vec<Pattern>,
    pub changes: Option<usize>,
    pub read_samples: usize,
    pub min_free_bytes: u64,
    pub keep_fixtures: bool,
    pub tokio_workers: usize,
}

impl RunConfig {
    pub fn full(output: PathBuf, revision: String, dirty: bool) -> Self {
        Self {
            output,
            redis_url: "redis://127.0.0.1:6379/".to_string(),
            revision,
            dirty,
            sizes: FULL_SIZES.to_vec(),
            runs: 3,
            operations: Operation::ALL.to_vec(),
            patterns: Pattern::ALL.to_vec(),
            changes: None,
            read_samples: 10_000,
            min_free_bytes: 3 * 1024 * 1024 * 1024,
            keep_fixtures: false,
            tokio_workers: 4,
        }
    }

    pub fn smoke(output: PathBuf) -> Self {
        Self {
            output,
            redis_url: "redis://127.0.0.1:6379/".to_string(),
            revision: "unknown".to_string(),
            dirty: true,
            sizes: vec![100],
            runs: 1,
            operations: Operation::ALL.to_vec(),
            patterns: Pattern::ALL.to_vec(),
            changes: Some(10),
            read_samples: 10,
            min_free_bytes: 0,
            keep_fixtures: false,
            tokio_workers: 2,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.output.as_os_str().is_empty()
            || self.revision.is_empty()
            || self.redis_url.trim().is_empty()
        {
            return Err("output, revision, and Redis URL must be non-empty".to_string());
        }
        if self.sizes.is_empty()
            || self.sizes.contains(&0)
            || self.runs == 0
            || self.tokio_workers == 0
        {
            return Err("sizes and runs must be positive".to_string());
        }
        if self.operations.is_empty() || self.patterns.is_empty() {
            return Err("operation and pattern filters must be non-empty".to_string());
        }
        ensure_unique(&self.sizes, "sizes")?;
        ensure_unique(&self.operations, "operations")?;
        ensure_unique(&self.patterns, "patterns")?;
        if self.changes == Some(0) || self.read_samples == 0 {
            return Err("changes and read samples must be positive".to_string());
        }
        if self.operations.contains(&Operation::Build) {
            return Err("build is implicit and cannot be selected as a workload cell".to_string());
        }
        for records in &self.sizes {
            let changes = self.changes_for(*records);
            if changes > *records {
                return Err("changes must not exceed records".to_string());
            }
            if self.operations.contains(&Operation::Merge) && !changes.is_multiple_of(2) {
                return Err("merge requires an even total change count".to_string());
            }
            if self.read_samples > *records {
                return Err("read samples must not exceed records".to_string());
            }
        }
        Ok(())
    }

    pub fn changes_for(&self, records: usize) -> usize {
        self.changes.unwrap_or_else(|| change_count(records))
    }
}

fn ensure_unique<T: Copy + Ord>(values: &[T], name: &str) -> Result<(), String> {
    if values.iter().copied().collect::<BTreeSet<_>>().len() != values.len() {
        return Err(format!("{name} contains a duplicate"));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureSpec {
    pub records: usize,
    pub repetition: usize,
    pub revision: String,
    pub dirty: bool,
}

impl FixtureSpec {
    pub fn from_config(config: &RunConfig, records: usize, repetition: usize) -> Self {
        Self {
            records,
            repetition,
            revision: config.revision.clone(),
            dirty: config.dirty,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CellSpec {
    pub records: usize,
    pub repetition: usize,
    pub operation: Operation,
    pub pattern: Pattern,
    pub cache_state: CacheState,
    pub changes: usize,
    pub read_samples: usize,
    pub revision: String,
    pub dirty: bool,
}

impl CellSpec {
    pub fn logical_operations(&self) -> usize {
        match self.operation {
            Operation::Put => 1,
            Operation::Batch | Operation::Diff | Operation::Merge => self.changes,
            Operation::GetCold | Operation::GetWarm | Operation::Query | Operation::Scan => {
                self.read_samples
            }
            Operation::FullScan => self.records,
            Operation::Build => self.records,
        }
    }

    pub fn expected_entries(&self) -> usize {
        if self.pattern != Pattern::Append {
            return self.records;
        }
        match self.operation {
            Operation::Put => self.records.saturating_add(1),
            Operation::Batch | Operation::Diff | Operation::Merge => {
                self.records.saturating_add(self.changes)
            }
            _ => self.records,
        }
    }
}

pub fn enumerate_cells(config: &RunConfig, records: usize, repetition: usize) -> Vec<CellSpec> {
    let mut cells = Vec::new();
    for &operation in &config.operations {
        let patterns: &[Pattern] = if operation == Operation::FullScan {
            &config.patterns[..1]
        } else {
            &config.patterns
        };
        for &pattern in patterns {
            cells.push(CellSpec {
                records,
                repetition,
                operation,
                pattern,
                cache_state: match operation {
                    Operation::GetCold => CacheState::ColdManager,
                    Operation::GetWarm => CacheState::WarmManager,
                    _ => CacheState::NotApplicable,
                },
                changes: config.changes_for(records),
                read_samples: config.read_samples,
                revision: config.revision.clone(),
                dirty: config.dirty,
            });
        }
    }
    cells
}

pub fn key(id: usize) -> Vec<u8> {
    format!("key-{id:020}").into_bytes()
}

pub fn value(id: usize, generation: u8) -> Vec<u8> {
    let prefix = format!("value-{id:020}-{generation:02}-");
    let mut bytes = prefix.into_bytes();
    bytes.resize(100, b'x');
    bytes
}

pub fn change_count(records: usize) -> usize {
    records.saturating_mul(30).div_ceil(100).max(1).min(records)
}

pub fn mutation_ids(pattern: Pattern, records: usize, count: usize, salt: u64) -> Vec<usize> {
    match pattern {
        Pattern::Append => (records..records.saturating_add(count)).collect(),
        Pattern::Random => random_ids(records, count, salt),
        Pattern::Clustered => clustered_ids(records, count),
    }
}

pub fn merge_ids(records: usize, count: usize, pattern: Pattern) -> (Vec<usize>, Vec<usize>) {
    let branch_count = count / 2;
    match pattern {
        Pattern::Append => (
            (records..records.saturating_add(branch_count)).collect(),
            (records.saturating_add(branch_count)..records.saturating_add(count)).collect(),
        ),
        Pattern::Clustered => {
            let ids = clustered_ids(records, count);
            (ids[..branch_count].to_vec(), ids[branch_count..].to_vec())
        }
        Pattern::Random => {
            let ids = random_ids(records, count, 0x006d_6572_6765);
            let mut left = Vec::with_capacity(branch_count);
            let mut right = Vec::with_capacity(branch_count);
            for (index, id) in ids.into_iter().enumerate() {
                if index % 2 == 0 {
                    left.push(id);
                } else {
                    right.push(id);
                }
            }
            (left, right)
        }
    }
}

pub fn read_ids(pattern: Pattern, records: usize, count: usize) -> Vec<usize> {
    match pattern {
        Pattern::Append => right_edge_ids(records, count),
        Pattern::Random => random_ids(records, count, 0x7265_6164),
        Pattern::Clustered => clustered_ids(records, count),
    }
}

pub fn range_ids(pattern: Pattern, records: usize, count: usize) -> Vec<usize> {
    let wanted = count.min(records);
    if wanted == 0 {
        return Vec::new();
    }
    let start = match pattern {
        Pattern::Append => records - wanted,
        Pattern::Clustered => records.saturating_sub(wanted) / 2,
        Pattern::Random => {
            let mut state = RANDOM_SEED ^ 0x7363_616e;
            (next_random(&mut state) as usize) % (records - wanted + 1)
        }
    };
    (start..start + wanted).collect()
}

pub fn range_bounds(pattern: Pattern, records: usize, count: usize) -> (Vec<u8>, Vec<u8>) {
    let ids = range_ids(pattern, records, count);
    let start = ids.first().copied().unwrap_or(0);
    (key(start), key(start.saturating_add(ids.len())))
}

fn clustered_ids(records: usize, count: usize) -> Vec<usize> {
    let wanted = count.min(records);
    let start = records.saturating_sub(wanted) / 2;
    (start..start + wanted).collect()
}

fn right_edge_ids(records: usize, count: usize) -> Vec<usize> {
    let wanted = count.min(records);
    (records - wanted..records).collect()
}

fn random_ids(records: usize, count: usize, salt: u64) -> Vec<usize> {
    let wanted = count.min(records);
    let mut state = RANDOM_SEED ^ (records as u64).rotate_left(29) ^ salt.rotate_left(11);
    let mut ids = BTreeSet::new();
    while ids.len() < wanted {
        ids.insert((next_random(&mut state) as usize) % records);
    }
    ids.into_iter().collect()
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
