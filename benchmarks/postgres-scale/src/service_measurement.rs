use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;
use prolly::ProllyMetricsSnapshot;
use serde::{Deserialize, Serialize};

use crate::measurement::{PgMetrics, PhysicalSize};
use crate::service_model::{ServiceOperation, TenantClass};

pub const SERVICE_SCHEMA_VERSION: &str = "postgres-service-v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SampleOutcome {
    Success,
    ExhaustedRetries,
    SemanticConflict,
    Timeout,
    SqlError,
    ValidationError,
}

#[derive(Clone, Debug)]
pub struct OperationSample {
    pub operation: ServiceOperation,
    pub tenant_class: TenantClass,
    pub latency_ns: u64,
    pub attempts: u64,
    pub conflicts: u64,
    pub retries: u64,
    pub outcome: SampleOutcome,
    pub prolly: ProllyMetricsSnapshot,
}

impl OperationSample {
    pub fn success(
        operation: ServiceOperation,
        tenant_class: TenantClass,
        latency_ns: u64,
        attempts: u64,
        conflicts: u64,
    ) -> Self {
        Self {
            operation,
            tenant_class,
            latency_ns,
            attempts,
            conflicts,
            retries: attempts.saturating_sub(1),
            outcome: SampleOutcome::Success,
            prolly: ProllyMetricsSnapshot::default(),
        }
    }

    pub fn with_prolly_metrics(mut self, prolly: ProllyMetricsSnapshot) -> Self {
        self.prolly = prolly;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationSummary {
    pub operation: ServiceOperation,
    pub tenant_class: TenantClass,
    pub attempted: u64,
    pub completed: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
    pub cas_attempts: u64,
    pub conflicts: u64,
    pub retries: u64,
    pub exhausted_retries: u64,
    pub semantic_conflicts: u64,
    pub timeouts: u64,
    pub sql_errors: u64,
    pub validation_errors: u64,
    pub prolly: ProllyMetricsSnapshot,
}

impl Default for OperationSummary {
    fn default() -> Self {
        Self {
            operation: ServiceOperation::PointRead,
            tenant_class: TenantClass::Independent,
            attempted: 0,
            completed: 0,
            p50_ns: 0,
            p95_ns: 0,
            p99_ns: 0,
            p999_ns: 0,
            max_ns: 0,
            cas_attempts: 0,
            conflicts: 0,
            retries: 0,
            exhausted_retries: 0,
            semantic_conflicts: 0,
            timeouts: 0,
            sql_errors: 0,
            validation_errors: 0,
            prolly: ProllyMetricsSnapshot::default(),
        }
    }
}

struct Bucket {
    histogram: Histogram<u64>,
    summary: OperationSummary,
}

pub struct ServiceAccumulator {
    max_latency_ns: u64,
    buckets: BTreeMap<(ServiceOperation, TenantClass), Bucket>,
}

impl ServiceAccumulator {
    pub fn new(max_latency_ns: u64) -> Result<Self, String> {
        if max_latency_ns == 0 {
            return Err("maximum latency must be positive".to_string());
        }
        Ok(Self {
            max_latency_ns,
            buckets: BTreeMap::new(),
        })
    }

    pub fn record(&mut self, sample: OperationSample) -> Result<(), String> {
        let key = (sample.operation, sample.tenant_class);
        let bucket = self.buckets.entry(key).or_insert_with(|| Bucket {
            histogram: Histogram::new_with_max(self.max_latency_ns, 3)
                .expect("validated histogram bounds"),
            summary: OperationSummary {
                operation: sample.operation,
                tenant_class: sample.tenant_class,
                ..OperationSummary::default()
            },
        });
        bucket
            .histogram
            .record(sample.latency_ns.min(self.max_latency_ns))
            .map_err(|error| format!("failed to record service latency: {error}"))?;
        bucket.summary.attempted += 1;
        bucket.summary.cas_attempts += sample.attempts;
        bucket.summary.conflicts += sample.conflicts;
        bucket.summary.retries += sample.retries;
        add_prolly_metrics(&mut bucket.summary.prolly, &sample.prolly);
        match sample.outcome {
            SampleOutcome::Success => bucket.summary.completed += 1,
            SampleOutcome::ExhaustedRetries => bucket.summary.exhausted_retries += 1,
            SampleOutcome::SemanticConflict => bucket.summary.semantic_conflicts += 1,
            SampleOutcome::Timeout => bucket.summary.timeouts += 1,
            SampleOutcome::SqlError => bucket.summary.sql_errors += 1,
            SampleOutcome::ValidationError => bucket.summary.validation_errors += 1,
        }
        Ok(())
    }

    pub fn merge(&mut self, other: Self) -> Result<(), String> {
        for (key, other_bucket) in other.buckets {
            let bucket = self.buckets.entry(key).or_insert_with(|| Bucket {
                histogram: Histogram::new_with_max(self.max_latency_ns, 3)
                    .expect("validated histogram bounds"),
                summary: OperationSummary {
                    operation: key.0,
                    tenant_class: key.1,
                    ..OperationSummary::default()
                },
            });
            bucket
                .histogram
                .add(&other_bucket.histogram)
                .map_err(|error| format!("failed to merge service histograms: {error}"))?;
            add_summary(&mut bucket.summary, &other_bucket.summary);
        }
        Ok(())
    }

    pub fn summaries(&self) -> Vec<OperationSummary> {
        self.buckets
            .values()
            .map(|bucket| {
                let mut summary = bucket.summary.clone();
                summary.p50_ns = bucket.histogram.value_at_quantile(0.50);
                summary.p95_ns = bucket.histogram.value_at_quantile(0.95);
                summary.p99_ns = bucket.histogram.value_at_quantile(0.99);
                summary.p999_ns = bucket.histogram.value_at_quantile(0.999);
                summary.max_ns = bucket.histogram.max();
                summary
            })
            .collect()
    }
}

fn add_summary(target: &mut OperationSummary, source: &OperationSummary) {
    target.attempted += source.attempted;
    target.completed += source.completed;
    target.cas_attempts += source.cas_attempts;
    target.conflicts += source.conflicts;
    target.retries += source.retries;
    target.exhausted_retries += source.exhausted_retries;
    target.semantic_conflicts += source.semantic_conflicts;
    target.timeouts += source.timeouts;
    target.sql_errors += source.sql_errors;
    target.validation_errors += source.validation_errors;
    add_prolly_metrics(&mut target.prolly, &source.prolly);
}

fn add_prolly_metrics(target: &mut ProllyMetricsSnapshot, source: &ProllyMetricsSnapshot) {
    target.node_cache_hits += source.node_cache_hits;
    target.node_cache_misses += source.node_cache_misses;
    target.node_cache_evictions += source.node_cache_evictions;
    target.nodes_read += source.nodes_read;
    target.bytes_read += source.bytes_read;
    target.nodes_written += source.nodes_written;
    target.bytes_written += source.bytes_written;
    target.store_get_calls += source.store_get_calls;
    target.store_batch_get_calls += source.store_batch_get_calls;
    target.store_batch_get_keys += source.store_batch_get_keys;
    target.store_put_calls += source.store_put_calls;
    target.store_batch_put_calls += source.store_batch_put_calls;
    target.store_batch_put_nodes += source.store_batch_put_nodes;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServiceRawRow {
    pub schema: String,
    pub config_hash: String,
    pub revision: String,
    pub dirty: bool,
    pub timestamp_ms: u128,
    pub records: u64,
    pub value_bytes: u64,
    pub clients: u64,
    pub pool_size: u32,
    pub operation: String,
    pub tenant_class: String,
    pub duration_ns: u128,
    pub sample_count: u64,
    pub attempted: u64,
    pub completed: u64,
    pub cell_attempted: u64,
    pub cell_completed: u64,
    pub attempted_ops_per_sec: f64,
    pub successful_ops_per_sec: f64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
    pub cas_attempts: u64,
    pub conflicts: u64,
    pub retries: u64,
    pub exhausted_retries: u64,
    pub semantic_conflicts: u64,
    pub timeouts: u64,
    pub sql_errors: u64,
    pub validation_errors: u64,
    pub worker_panics: u64,
    pub prolly_node_cache_hits: u64,
    pub prolly_node_cache_misses: u64,
    pub prolly_node_cache_evictions: u64,
    pub prolly_nodes_read: u64,
    pub prolly_bytes_read: u64,
    pub prolly_nodes_written: u64,
    pub prolly_bytes_written: u64,
    pub prolly_store_get_calls: u64,
    pub prolly_store_batch_get_calls: u64,
    pub prolly_store_batch_get_keys: u64,
    pub prolly_store_put_calls: u64,
    pub prolly_store_batch_put_calls: u64,
    pub prolly_store_batch_put_nodes: u64,
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

impl ServiceRawRow {
    #[allow(clippy::too_many_arguments)]
    pub fn from_summary(
        config_hash: &str,
        revision: &str,
        dirty: bool,
        records: usize,
        value_bytes: usize,
        clients: usize,
        pool_size: u32,
        duration_ns: u128,
        summary: &OperationSummary,
        pg: &PgMetrics,
        before: &PhysicalSize,
        after: &PhysicalSize,
    ) -> Self {
        let seconds = duration_ns.max(1) as f64 / 1_000_000_000.0;
        Self {
            schema: SERVICE_SCHEMA_VERSION.to_string(),
            config_hash: config_hash.to_string(),
            revision: revision.to_string(),
            dirty,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            records: records as u64,
            value_bytes: value_bytes as u64,
            clients: clients as u64,
            pool_size,
            operation: summary.operation.as_str().to_string(),
            tenant_class: summary.tenant_class.as_str().to_string(),
            duration_ns,
            sample_count: summary.attempted,
            attempted: summary.attempted,
            completed: summary.completed,
            cell_attempted: summary.attempted,
            cell_completed: summary.completed,
            attempted_ops_per_sec: summary.attempted as f64 / seconds,
            successful_ops_per_sec: summary.completed as f64 / seconds,
            p50_ns: summary.p50_ns,
            p95_ns: summary.p95_ns,
            p99_ns: summary.p99_ns,
            p999_ns: summary.p999_ns,
            max_ns: summary.max_ns,
            cas_attempts: summary.cas_attempts,
            conflicts: summary.conflicts,
            retries: summary.retries,
            exhausted_retries: summary.exhausted_retries,
            semantic_conflicts: summary.semantic_conflicts,
            timeouts: summary.timeouts,
            sql_errors: summary.sql_errors,
            validation_errors: summary.validation_errors,
            worker_panics: 0,
            prolly_node_cache_hits: summary.prolly.node_cache_hits,
            prolly_node_cache_misses: summary.prolly.node_cache_misses,
            prolly_node_cache_evictions: summary.prolly.node_cache_evictions,
            prolly_nodes_read: summary.prolly.nodes_read,
            prolly_bytes_read: summary.prolly.bytes_read,
            prolly_nodes_written: summary.prolly.nodes_written,
            prolly_bytes_written: summary.prolly.bytes_written,
            prolly_store_get_calls: summary.prolly.store_get_calls,
            prolly_store_batch_get_calls: summary.prolly.store_batch_get_calls,
            prolly_store_batch_get_keys: summary.prolly.store_batch_get_keys,
            prolly_store_put_calls: summary.prolly.store_put_calls,
            prolly_store_batch_put_calls: summary.prolly.store_batch_put_calls,
            prolly_store_batch_put_nodes: summary.prolly.store_batch_put_nodes,
            pg_statement_calls: pg.statement_calls,
            pg_execution_ms: pg.execution_ms,
            pg_shared_blks_hit: pg.shared_blks_hit,
            pg_shared_blks_read: pg.shared_blks_read,
            pg_shared_blks_dirtied: pg.shared_blks_dirtied,
            pg_shared_blks_written: pg.shared_blks_written,
            pg_temp_blks_read: pg.temp_blks_read,
            pg_temp_blks_written: pg.temp_blks_written,
            pg_wal_bytes: pg.wal_bytes,
            pg_commits: pg.commits,
            pg_rollbacks: pg.rollbacks,
            database_bytes_before: before.database_bytes,
            database_bytes_after: after.database_bytes,
            prolly_table_bytes_before: before.prolly_table_bytes,
            prolly_table_bytes_after: after.prolly_table_bytes,
            prolly_index_bytes_before: before.prolly_index_bytes,
            prolly_index_bytes_after: after.prolly_index_bytes,
            validated: true,
            error: String::new(),
        }
    }

    pub fn key(&self) -> (String, u64, u32, String, String) {
        (
            self.config_hash.clone(),
            self.clients,
            self.pool_size,
            self.operation.clone(),
            self.tenant_class.clone(),
        )
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema != SERVICE_SCHEMA_VERSION {
            return Err("unexpected service schema".to_string());
        }
        if !self.validated || !self.error.is_empty() {
            return Err(format!("failed service cell: {}", self.error));
        }
        if self.duration_ns == 0 || self.attempted == 0 || self.sample_count != self.attempted {
            return Err("service timing inputs must be positive and consistent".to_string());
        }
        if self.completed > self.attempted
            || self.attempted > self.cell_attempted
            || self.completed > self.cell_completed
            || self.conflicts > self.cas_attempts
            || self.p50_ns > self.p95_ns
            || self.p95_ns > self.p99_ns
            || self.p99_ns > self.p999_ns
            || self.p999_ns > self.max_ns
        {
            return Err("service counters or percentiles are inconsistent".to_string());
        }
        let seconds = self.duration_ns as f64 / 1_000_000_000.0;
        let attempted_rate = self.attempted as f64 / seconds;
        let successful_rate = self.completed as f64 / seconds;
        if !close(self.attempted_ops_per_sec, attempted_rate)
            || !close(self.successful_ops_per_sec, successful_rate)
        {
            return Err("service throughput is inconsistent".to_string());
        }
        Ok(())
    }
}

fn close(actual: f64, expected: f64) -> bool {
    actual.is_finite() && (actual - expected).abs() <= expected.abs().max(1.0) * f64::EPSILON * 8.0
}

pub struct ServiceCsvSink {
    writer: csv::Writer<File>,
}

impl ServiceCsvSink {
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

    pub fn append(&mut self, row: &ServiceRawRow) -> Result<(), String> {
        self.writer
            .serialize(row)
            .map_err(|error| format!("failed to serialize service row: {error}"))?;
        self.writer
            .flush()
            .map_err(|error| format!("failed to flush service row: {error}"))?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|error| format!("failed to sync service row: {error}"))
    }
}

pub fn read_service_rows(path: &Path) -> Result<Vec<ServiceRawRow>, String> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    csv::Reader::from_path(path)
        .map_err(|error| format!("failed to open {}: {error}", path.display()))?
        .deserialize()
        .map(|row| row.map_err(|error| format!("failed to parse service row: {error}")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulator_reports_tail_latency_and_retries() {
        let mut accumulator = ServiceAccumulator::new(10_000_000).unwrap();
        for latency in 1..=1_000_u64 {
            accumulator
                .record(OperationSample::success(
                    ServiceOperation::Commit,
                    TenantClass::Hot,
                    latency * 1_000,
                    2,
                    1,
                ))
                .unwrap();
        }
        let summary = accumulator.summaries().pop().unwrap();
        assert_eq!(summary.completed, 1_000);
        assert_eq!(summary.conflicts, 1_000);
        assert!(summary.p99_ns >= 990_000);
        assert!(summary.p999_ns >= summary.p99_ns);
    }

    #[test]
    fn csv_round_trips_service_rows() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("service.csv");
        let summary = OperationSummary {
            operation: ServiceOperation::PointRead,
            tenant_class: TenantClass::Independent,
            attempted: 1,
            completed: 1,
            p50_ns: 10,
            p95_ns: 10,
            p99_ns: 10,
            p999_ns: 10,
            max_ns: 10,
            ..OperationSummary::default()
        };
        let row = ServiceRawRow::from_summary(
            "hash",
            "revision",
            true,
            1_000,
            64,
            4,
            2,
            1_000,
            &summary,
            &PgMetrics::default(),
            &PhysicalSize::default(),
            &PhysicalSize::default(),
        );
        row.validate().unwrap();
        ServiceCsvSink::open(&path).unwrap().append(&row).unwrap();
        assert_eq!(read_service_rows(&path).unwrap(), vec![row]);
    }
}
