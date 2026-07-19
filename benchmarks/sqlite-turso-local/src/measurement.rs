//! Durable benchmark measurements and resumable cell keys.

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::model::{Adapter, Api, Pattern};

pub const SCHEMA_VERSION: &str = "sqlite-turso-local-v1";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RawRow {
    pub schema: String,
    pub revision: String,
    pub dirty: bool,
    pub adapter: Adapter,
    pub records: usize,
    pub repetition: usize,
    pub api: Api,
    pub pattern: Pattern,
    pub configured_changes: usize,
    pub observed_changes: usize,
    pub total_ns: u128,
    pub operations_per_sec: f64,
    pub p50_ns: Option<u128>,
    pub p95_ns: Option<u128>,
    pub p99_ns: Option<u128>,
    pub max_ns: Option<u128>,
    pub db_bytes_before: u64,
    pub db_bytes_after: u64,
    pub expected_records: usize,
    pub observed_records: usize,
    pub validated: bool,
    pub error: String,
}

impl RawRow {
    pub fn example() -> Self {
        Self {
            schema: SCHEMA_VERSION.to_string(),
            revision: "revision".to_string(),
            dirty: false,
            adapter: Adapter::SqliteSync,
            records: 100,
            repetition: 1,
            api: Api::Put,
            pattern: Pattern::Append,
            configured_changes: 10,
            observed_changes: 10,
            total_ns: 1_000,
            operations_per_sec: 10_000_000.0,
            p50_ns: Some(100),
            p95_ns: Some(150),
            p99_ns: Some(175),
            max_ns: Some(200),
            db_bytes_before: 4_096,
            db_bytes_after: 8_192,
            expected_records: 110,
            observed_records: 110,
            validated: true,
            error: String::new(),
        }
    }

    pub fn key(&self) -> CellKey {
        CellKey {
            adapter: self.adapter,
            records: self.records,
            repetition: self.repetition,
            api: self.api,
            pattern: self.pattern,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FixtureRow {
    pub schema: String,
    pub revision: String,
    pub dirty: bool,
    pub adapter: Adapter,
    pub records: usize,
    pub repetition: usize,
    pub build_ns: u128,
    pub records_per_sec: f64,
    pub database_bytes: u64,
    pub observed_records: usize,
    pub validated: bool,
    pub error: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CellKey {
    pub adapter: Adapter,
    pub records: usize,
    pub repetition: usize,
    pub api: Api,
    pub pattern: Pattern,
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

#[derive(Clone, Debug, Default)]
pub struct ResumeState {
    completed: BTreeSet<CellKey>,
    failed: BTreeSet<CellKey>,
}

impl ResumeState {
    pub fn from_rows(rows: &[RawRow]) -> Result<Self, String> {
        let mut state = Self::default();
        let mut seen = BTreeSet::new();
        for row in rows {
            if row.schema != SCHEMA_VERSION {
                return Err(format!("unexpected benchmark schema: {}", row.schema));
            }
            if row.records == 0 || row.repetition == 0 {
                return Err("measurement keys must be positive".to_string());
            }
            let key = row.key();
            if !seen.insert(key) {
                return Err(format!("duplicate measurement row: {key:?}"));
            }
            if row.validated {
                if !row.error.is_empty() {
                    return Err(format!("validated row contains an error: {key:?}"));
                }
                state.completed.insert(key);
            } else {
                if row.error.is_empty() {
                    return Err(format!("failed row is missing an error: {key:?}"));
                }
                state.failed.insert(key);
            }
        }
        Ok(state)
    }

    pub fn contains(&self, key: &CellKey) -> bool {
        self.completed.contains(key)
    }

    pub fn failed(&self, key: &CellKey) -> bool {
        self.failed.contains(key)
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
        let writer = csv::WriterBuilder::new()
            .has_headers(!has_rows)
            .from_writer(file);
        Ok(Self {
            writer,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_uses_one_based_ceiling() {
        let samples = vec![10, 20, 30, 40, 50];
        assert_eq!(nearest_rank(&samples, 0.50), Some(30));
        assert_eq!(nearest_rank(&samples, 0.95), Some(50));
        assert_eq!(nearest_rank(&[], 0.99), None);
    }

    #[test]
    fn resume_rejects_duplicates_and_skips_exact_successful_cells() {
        let row = RawRow::example();
        let state = ResumeState::from_rows(std::slice::from_ref(&row)).unwrap();
        assert!(state.contains(&row.key()));
        assert!(ResumeState::from_rows(&[row.clone(), row]).is_err());
    }

    #[test]
    fn failed_rows_are_visible_but_not_complete() {
        let mut row = RawRow::example();
        row.validated = false;
        row.error = "database, said \"busy\"\nretry".to_string();
        let state = ResumeState::from_rows(std::slice::from_ref(&row)).unwrap();
        assert!(!state.contains(&row.key()));
        assert!(state.failed(&row.key()));
    }

    #[test]
    fn csv_round_trip_preserves_escaped_errors() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("rows.csv");
        let mut row = RawRow::example();
        row.error = "database, said \"busy\"\nretry".to_string();

        let mut sink = CsvSink::open(&path).unwrap();
        sink.append(&row).unwrap();
        drop(sink);

        let loaded = csv::Reader::from_path(path)
            .unwrap()
            .deserialize::<RawRow>()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(loaded, vec![row]);
    }
}

