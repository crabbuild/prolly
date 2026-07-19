use std::fs::{File, OpenOptions};
use std::path::Path;

use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: &str = "postgres-scale-v1";

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PgMetrics {
    pub statement_calls: u64,
    pub execution_ms: f64,
    pub shared_blks_hit: u64,
    pub shared_blks_read: u64,
    pub shared_blks_dirtied: u64,
    pub shared_blks_written: u64,
    pub temp_blks_read: u64,
    pub temp_blks_written: u64,
    pub wal_bytes: u64,
    pub commits: u64,
    pub rollbacks: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalSize {
    pub database_bytes: u64,
    pub prolly_table_bytes: u64,
    pub prolly_index_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RawRow {
    pub schema: String,
    pub revision: String,
    pub dirty: bool,
    pub timestamp_ms: u128,
    pub records: u64,
    pub repetition: u32,
    pub operation: String,
    pub pattern: String,
    pub cache_state: String,
    pub sample_count: u64,
    pub logical_operations: u64,
    pub observed_items: u64,
    pub total_ns: u128,
    pub ns_per_op: f64,
    pub ops_per_sec: f64,
    pub p50_ns: Option<u128>,
    pub p95_ns: Option<u128>,
    pub p99_ns: Option<u128>,
    pub max_ns: Option<u128>,
    pub node_cache_hits: u64,
    pub node_cache_misses: u64,
    pub node_cache_evictions: u64,
    pub nodes_read: u64,
    pub bytes_read: u64,
    pub nodes_written: u64,
    pub bytes_written: u64,
    pub store_get_calls: u64,
    pub store_batch_get_calls: u64,
    pub store_batch_get_keys: u64,
    pub store_put_calls: u64,
    pub store_batch_put_calls: u64,
    pub store_batch_put_nodes: u64,
    pub tree_nodes: u64,
    pub tree_leaves: u64,
    pub tree_internal_nodes: u64,
    pub tree_height: u8,
    pub tree_records: u64,
    pub tree_bytes: u64,
    pub pg_statement_calls: u64,
    pub pg_execution_ms: f64,
    pub pg_shared_blks_hit: u64,
    pub pg_shared_blks_read: u64,
    pub pg_shared_blks_dirtied: u64,
    pub pg_shared_blks_written: u64,
    pub pg_temp_blks_read: u64,
    pub pg_temp_blks_written: u64,
    pub pg_wal_bytes: u64,
    pub pg_commits: u64,
    pub pg_rollbacks: u64,
    pub database_bytes_before: u64,
    pub database_bytes_after: u64,
    pub prolly_table_bytes_before: u64,
    pub prolly_table_bytes_after: u64,
    pub prolly_index_bytes_before: u64,
    pub prolly_index_bytes_after: u64,
    pub validated: bool,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CellKey {
    pub records: u64,
    pub repetition: u32,
    pub operation: String,
    pub pattern: String,
    pub cache_state: String,
}

impl RawRow {
    pub fn example() -> Self {
        Self {
            schema: SCHEMA_VERSION.to_string(),
            revision: "revision".to_string(),
            dirty: true,
            timestamp_ms: 1,
            records: 1_000,
            repetition: 1,
            operation: "get".to_string(),
            pattern: "random".to_string(),
            cache_state: "warm".to_string(),
            sample_count: 10,
            logical_operations: 10,
            observed_items: 10,
            total_ns: 1_000,
            ns_per_op: 100.0,
            ops_per_sec: 10_000_000.0,
            p50_ns: Some(100),
            p95_ns: Some(150),
            p99_ns: Some(175),
            max_ns: Some(200),
            node_cache_hits: 1,
            node_cache_misses: 1,
            node_cache_evictions: 0,
            nodes_read: 1,
            bytes_read: 100,
            nodes_written: 0,
            bytes_written: 0,
            store_get_calls: 1,
            store_batch_get_calls: 0,
            store_batch_get_keys: 0,
            store_put_calls: 0,
            store_batch_put_calls: 0,
            store_batch_put_nodes: 0,
            tree_nodes: 1,
            tree_leaves: 1,
            tree_internal_nodes: 0,
            tree_height: 0,
            tree_records: 1_000,
            tree_bytes: 100,
            pg_statement_calls: 1,
            pg_execution_ms: 0.1,
            pg_shared_blks_hit: 1,
            pg_shared_blks_read: 0,
            pg_shared_blks_dirtied: 0,
            pg_shared_blks_written: 0,
            pg_temp_blks_read: 0,
            pg_temp_blks_written: 0,
            pg_wal_bytes: 0,
            pg_commits: 0,
            pg_rollbacks: 0,
            database_bytes_before: 1,
            database_bytes_after: 1,
            prolly_table_bytes_before: 1,
            prolly_table_bytes_after: 1,
            prolly_index_bytes_before: 1,
            prolly_index_bytes_after: 1,
            validated: true,
            error: String::new(),
        }
    }

    pub fn key(&self) -> CellKey {
        CellKey {
            records: self.records,
            repetition: self.repetition,
            operation: self.operation.clone(),
            pattern: self.pattern.clone(),
            cache_state: self.cache_state.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema != SCHEMA_VERSION {
            return Err("unexpected schema".to_string());
        }
        if !self.validated {
            return Err(self.error.clone());
        }
        if self.logical_operations == 0 || self.total_ns == 0 {
            return Err("timing inputs must be positive".to_string());
        }
        let expected_ns = self.total_ns as f64 / self.logical_operations as f64;
        let expected_rate = self.logical_operations as f64 * 1_000_000_000.0 / self.total_ns as f64;
        if !self.ns_per_op.is_finite()
            || (self.ns_per_op - expected_ns).abs() > expected_ns.abs().max(1.0) * 1e-9
        {
            return Err("per-operation latency is inconsistent".to_string());
        }
        if !self.ops_per_sec.is_finite()
            || (self.ops_per_sec - expected_rate).abs() > expected_rate.abs().max(1.0) * 1e-9
        {
            return Err("throughput is inconsistent".to_string());
        }
        Ok(())
    }
}

pub fn percentile(samples: &[u128], quantile: f64) -> Option<u128> {
    if samples.is_empty() || !quantile.is_finite() || !(0.0..=1.0).contains(&quantile) {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = (quantile * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted.get(rank - 1).copied()
}

pub struct CsvSink {
    writer: csv::Writer<File>,
}

impl CsvSink {
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
        }
        let has_rows = path.metadata().is_ok_and(|metadata| metadata.len() > 0);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
        Ok(Self {
            writer: csv::WriterBuilder::new()
                .has_headers(!has_rows)
                .from_writer(file),
        })
    }

    pub fn append(&mut self, row: &RawRow) -> Result<(), String> {
        self.writer
            .serialize(row)
            .map_err(|error| format!("failed to serialize row: {error}"))?;
        self.writer
            .flush()
            .map_err(|error| format!("failed to flush row: {error}"))?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|error| format!("failed to sync row: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles_are_stable() {
        let values = [50, 10, 40, 20, 30];
        assert_eq!(percentile(&values, 0.50), Some(30));
        assert_eq!(percentile(&values, 0.95), Some(50));
        assert_eq!(percentile(&values, 0.99), Some(50));
        assert_eq!(percentile(&[], 0.50), None);
    }

    #[test]
    fn row_validation_checks_timing_arithmetic() {
        let row = RawRow::example();
        assert!(row.validate().is_ok());
        let mut broken = row;
        broken.ops_per_sec = 1.0;
        assert!(broken.validate().unwrap_err().contains("throughput"));
    }

    #[test]
    fn cell_key_tracks_exact_measurement_identity() {
        let mut second = RawRow::example();
        second.repetition = 2;
        assert_ne!(RawRow::example().key(), second.key());
    }

    #[test]
    fn csv_sink_round_trips_escaped_error_text() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("rows.csv");
        let mut row = RawRow::example();
        row.validated = false;
        row.error = "postgres, said \"no\"\nretry".to_string();
        let mut sink = CsvSink::open(&path).unwrap();
        sink.append(&row).unwrap();
        drop(sink);

        let rows = csv::Reader::from_path(path)
            .unwrap()
            .deserialize::<RawRow>()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows, vec![row]);
    }
}
