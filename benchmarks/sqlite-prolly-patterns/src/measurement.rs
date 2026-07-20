use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::model::{CacheState, Operation, Pattern};

pub const SCHEMA_VERSION: &str = "sqlite-prolly-patterns-v1";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RawRow {
    pub schema: String,
    pub revision: String,
    pub dirty: bool,
    pub records: usize,
    pub repetition: usize,
    pub operation: Operation,
    pub pattern: Pattern,
    pub cache_state: CacheState,
    pub configured_operations: usize,
    pub observed_operations: usize,
    pub total_ns: u128,
    pub ns_per_operation: f64,
    pub operations_per_sec: f64,
    pub p50_ns: Option<u128>,
    pub p95_ns: Option<u128>,
    pub p99_ns: Option<u128>,
    pub max_ns: Option<u128>,
    pub nodes_read: u64,
    pub nodes_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_evictions: u64,
    pub result_entries: usize,
    pub num_nodes: usize,
    pub num_leaves: usize,
    pub num_internal: usize,
    pub height: usize,
    pub tree_bytes: usize,
    pub db_bytes: u64,
    pub wal_bytes: u64,
    pub shm_bytes: u64,
    pub total_database_bytes: u64,
    pub expected_entries: usize,
    pub observed_entries: usize,
    pub error: String,
    pub validated: bool,
}

impl RawRow {
    pub fn key(&self) -> CellKey {
        CellKey {
            records: self.records,
            repetition: self.repetition,
            operation: self.operation,
            pattern: self.pattern,
            cache_state: self.cache_state,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FixtureRow {
    pub schema: String,
    pub revision: String,
    pub dirty: bool,
    pub records: usize,
    pub repetition: usize,
    pub build_ns: u128,
    pub records_per_sec: f64,
    pub num_nodes: usize,
    pub num_leaves: usize,
    pub num_internal: usize,
    pub height: usize,
    pub tree_bytes: usize,
    pub database_bytes: u64,
    pub observed_records: usize,
    pub error: String,
    pub validated: bool,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CellKey {
    pub records: usize,
    pub repetition: usize,
    pub operation: Operation,
    pub pattern: Pattern,
    pub cache_state: CacheState,
}

pub fn nearest_rank(samples: &[u128], quantile: f64) -> Option<u128> {
    if samples.is_empty() || !quantile.is_finite() || !(0.0..=1.0).contains(&quantile) {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = (quantile * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted.get(rank - 1).copied()
}

pub fn rate(operations: usize, total_ns: u128) -> f64 {
    if total_ns == 0 {
        0.0
    } else {
        operations as f64 / (total_ns as f64 / 1_000_000_000.0)
    }
}

pub struct CsvSink<T> {
    writer: csv::Writer<File>,
    marker: PhantomData<T>,
}

impl<T> CsvSink<T>
where
    T: Serialize,
{
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
        }
        let has_rows = path
            .metadata()
            .map(|metadata| metadata.len() > 0)
            .unwrap_or(false);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
        Ok(Self {
            writer: csv::WriterBuilder::new()
                .has_headers(!has_rows)
                .from_writer(file),
            marker: PhantomData,
        })
    }

    pub fn append(&mut self, row: &T) -> Result<(), String> {
        self.writer
            .serialize(row)
            .map_err(|error| format!("failed to serialize CSV row: {error}"))?;
        self.writer
            .flush()
            .map_err(|error| format!("failed to flush CSV row: {error}"))?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|error| format!("failed to sync CSV row: {error}"))
    }
}

pub fn read_csv<T>(path: &Path) -> Result<Vec<T>, String>
where
    T: DeserializeOwned,
{
    if !path.exists() {
        return Ok(Vec::new());
    }
    csv::Reader::from_path(path)
        .map_err(|error| format!("failed to open {}: {error}", path.display()))?
        .deserialize()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to read {}: {error}", path.display()))
}
