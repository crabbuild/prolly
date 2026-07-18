use std::collections::HashSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const RANDOM_SEED: u64 = 0x6a09_e667_f3bc_c909;
pub const FULL_SIZES: &[usize] = &[10_000, 50_000, 100_000, 500_000, 1_000_000];

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Operation {
    Put,
    Batch,
    PointRead,
    RangeScan,
}

impl Operation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Batch => "batch",
            Self::PointRead => "point-read",
            Self::RangeScan => "range-scan",
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
    pub revision: String,
    pub dirty: bool,
    pub sizes: Vec<usize>,
    pub runs: usize,
    pub explicit_operations: Option<usize>,
    pub keep_fixtures: bool,
}

impl RunConfig {
    pub fn full(output: PathBuf, revision: String, dirty: bool) -> Self {
        Self {
            output,
            revision,
            dirty,
            sizes: FULL_SIZES.to_vec(),
            runs: 3,
            explicit_operations: None,
            keep_fixtures: false,
        }
    }

    pub fn smoke(output: PathBuf) -> Self {
        Self {
            output,
            revision: "unknown".to_string(),
            dirty: true,
            sizes: vec![100],
            runs: 1,
            explicit_operations: Some(10),
            keep_fixtures: false,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.output.as_os_str().is_empty() {
            return Err("output path must not be empty".to_string());
        }
        if self.revision.is_empty() {
            return Err("revision must not be empty".to_string());
        }
        if self.sizes.is_empty() || self.sizes.contains(&0) {
            return Err("sizes must be non-empty and positive".to_string());
        }
        if self.runs == 0 {
            return Err("runs must be positive".to_string());
        }
        if self.explicit_operations == Some(0) {
            return Err("operations must be positive".to_string());
        }
        Ok(())
    }

    pub fn operations_for(&self, records: usize) -> usize {
        self.explicit_operations
            .unwrap_or_else(|| change_count(records))
            .min(records)
    }
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
    pub operations: usize,
    pub revision: String,
    pub dirty: bool,
}

impl CellSpec {
    pub fn expected_entries(&self) -> usize {
        if matches!(self.operation, Operation::Put | Operation::Batch)
            && self.pattern == Pattern::Append
        {
            self.records.saturating_add(self.operations)
        } else {
            self.records
        }
    }
}

pub fn enumerate_cells(config: &RunConfig, records: usize, repetition: usize) -> Vec<CellSpec> {
    let operations = config.operations_for(records);
    let mut cells = Vec::with_capacity(15);
    for operation in [Operation::Put, Operation::Batch] {
        for pattern in Pattern::ALL {
            cells.push(CellSpec {
                records,
                repetition,
                operation,
                pattern,
                cache_state: CacheState::NotApplicable,
                operations,
                revision: config.revision.clone(),
                dirty: config.dirty,
            });
        }
    }
    for pattern in Pattern::ALL {
        for cache_state in [CacheState::ColdManager, CacheState::WarmManager] {
            cells.push(CellSpec {
                records,
                repetition,
                operation: Operation::PointRead,
                pattern,
                cache_state,
                operations,
                revision: config.revision.clone(),
                dirty: config.dirty,
            });
        }
    }
    for pattern in Pattern::ALL {
        cells.push(CellSpec {
            records,
            repetition,
            operation: Operation::RangeScan,
            pattern,
            cache_state: CacheState::NotApplicable,
            operations,
            revision: config.revision.clone(),
            dirty: config.dirty,
        });
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
    (records / 100).clamp(100, 10_000).min(records)
}

pub fn mutation_ids(pattern: Pattern, records: usize, count: usize) -> Vec<usize> {
    match pattern {
        Pattern::Append => (records..records.saturating_add(count)).collect(),
        Pattern::Random => random_ids(records, count, RANDOM_SEED),
        Pattern::Clustered => clustered_ids(records, count),
    }
}

pub fn read_ids(pattern: Pattern, records: usize, count: usize) -> Vec<usize> {
    match pattern {
        Pattern::Append => right_edge_ids(records, count),
        Pattern::Random => random_ids(records, count, RANDOM_SEED),
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
            let mut state = RANDOM_SEED;
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

fn random_ids(records: usize, count: usize, seed: u64) -> Vec<usize> {
    let wanted = count.min(records);
    if wanted == 0 {
        return Vec::new();
    }
    let mut state = seed;
    let mut seen = HashSet::with_capacity(wanted);
    let mut ids = Vec::with_capacity(wanted);
    while ids.len() < wanted {
        let id = (next_random(&mut state) as usize) % records;
        if seen.insert(id) {
            ids.push(id);
        }
    }
    ids
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
