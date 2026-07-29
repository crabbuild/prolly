use std::fs::OpenOptions;
use std::path::Path;
use std::time::Instant;

use futures_util::stream::{self, StreamExt, TryStreamExt};
use prolly::{RemoteManifestUpdate, RemoteStoreBackend};
use serde::{Deserialize, Serialize};

use crate::{Backend, RunConfig};

pub const SERVICE_SCHEMA: &str = "sql-service-comparison-v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceOperation {
    BatchPut,
    BatchGet,
    ConcurrentGet,
    ContendedRootCas,
}

impl ServiceOperation {
    pub const ALL: [Self; 4] = [
        Self::BatchPut,
        Self::BatchGet,
        Self::ConcurrentGet,
        Self::ContendedRootCas,
    ];
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServiceEvidenceRow {
    pub schema: String,
    pub run_id: String,
    pub backend: Backend,
    pub repetition: u32,
    pub operation: ServiceOperation,
    pub revision: String,
    pub tree_hash: String,
    pub binary_sha256: String,
    pub clients: u64,
    pub pool_size: u32,
    pub adapter_batch_items: u64,
    pub logical_operations: u64,
    pub total_ns: u128,
    pub ops_per_sec: f64,
    pub p50_ns: u128,
    pub p95_ns: u128,
    pub p99_ns: u128,
    pub p999_ns: u128,
    pub max_ns: u128,
    pub applied: u64,
    pub conflicts: u64,
    pub validated: bool,
    pub error: String,
}

impl ServiceEvidenceRow {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema != SERVICE_SCHEMA
            || self.run_id.is_empty()
            || self.repetition == 0
            || self.clients == 0
            || self.pool_size == 0
            || self.adapter_batch_items == 0
            || self.logical_operations == 0
            || self.total_ns == 0
            || !self.validated
            || !self.error.is_empty()
        {
            return Err("service evidence is incomplete or invalid".to_string());
        }
        if self.p50_ns == 0
            || self.p50_ns > self.p95_ns
            || self.p95_ns > self.p99_ns
            || self.p99_ns > self.p999_ns
            || self.p999_ns > self.max_ns
        {
            return Err("service latency distribution is invalid".to_string());
        }
        let expected = self.logical_operations as f64 * 1_000_000_000.0 / self.total_ns as f64;
        if !self.ops_per_sec.is_finite()
            || (self.ops_per_sec - expected).abs() > expected.abs().max(1.0) * 1e-9
        {
            return Err("service throughput does not match elapsed time".to_string());
        }
        Ok(())
    }
}

pub async fn run_service_workload<B>(
    backend: B,
    config: &RunConfig,
) -> Result<Vec<ServiceEvidenceRow>, String>
where
    B: RemoteStoreBackend + Clone,
{
    config.validate()?;
    let sample_count = config.workload.samples;
    let entries = (0..sample_count)
        .map(|id| {
            (
                service_key(id),
                service_value(id, config.workload.value_bytes),
            )
        })
        .collect::<Vec<_>>();
    let borrowed = entries
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect::<Vec<_>>();

    let started = Instant::now();
    backend
        .batch_put_nodes(&borrowed)
        .await
        .map_err(|error| format!("service batch put failed: {error}"))?;
    let batch_put_ns = started.elapsed().as_nanos();
    let requested = entries
        .iter()
        .map(|(key, _)| key.as_slice())
        .collect::<Vec<_>>();
    validate_values(
        &entries,
        &backend
            .batch_get_nodes_ordered(&requested)
            .await
            .map_err(|error| format!("service batch-put validation failed: {error}"))?,
    )?;
    let mut rows = vec![row(
        config,
        ServiceOperation::BatchPut,
        sample_count,
        batch_put_ns,
        Distribution::total(batch_put_ns),
        0,
        0,
    )?];

    let started = Instant::now();
    let batch_values = backend
        .batch_get_nodes_ordered(&requested)
        .await
        .map_err(|error| format!("service batch get failed: {error}"))?;
    let batch_get_ns = started.elapsed().as_nanos();
    validate_values(&entries, &batch_values)?;
    rows.push(row(
        config,
        ServiceOperation::BatchGet,
        sample_count,
        batch_get_ns,
        Distribution::total(batch_get_ns),
        0,
        0,
    )?);

    let started = Instant::now();
    let reads = stream::iter(entries.iter())
        .map(|(key, expected)| {
            let backend = &backend;
            async move {
                let sample_started = Instant::now();
                let actual = backend
                    .get_node(key)
                    .await
                    .map_err(|error| format!("service concurrent get failed: {error}"))?;
                if actual.as_deref() != Some(expected.as_slice()) {
                    return Err("service concurrent get returned the wrong value".to_string());
                }
                Ok(sample_started.elapsed().as_nanos())
            }
        })
        .buffer_unordered(config.workload.concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    let concurrent_ns = started.elapsed().as_nanos();
    rows.push(row(
        config,
        ServiceOperation::ConcurrentGet,
        sample_count,
        concurrent_ns,
        Distribution::from_samples(&reads)?,
        0,
        0,
    )?);

    let root_name = format!("service:{}:{}", config.run_id, config.repetition).into_bytes();
    let started = Instant::now();
    let outcomes = stream::iter(0..config.workload.concurrency)
        .map(|contender| {
            let backend = &backend;
            let root_name = &root_name;
            async move {
                let manifest = (contender as u64).to_be_bytes();
                let sample_started = Instant::now();
                let outcome = backend
                    .compare_and_swap_root_manifest(root_name, None, Some(&manifest))
                    .await
                    .map_err(|error| format!("service root CAS failed: {error}"))?;
                Ok::<_, String>((outcome, sample_started.elapsed().as_nanos()))
            }
        })
        .buffer_unordered(config.workload.concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    let cas_ns = started.elapsed().as_nanos();
    let applied = outcomes
        .iter()
        .filter(|(outcome, _)| *outcome == RemoteManifestUpdate::Applied)
        .count();
    let conflicts = outcomes.len() - applied;
    if applied != 1 {
        return Err(format!(
            "contended root CAS expected one winner, observed {applied}"
        ));
    }
    let cas_latencies = outcomes
        .iter()
        .map(|(_, elapsed)| *elapsed)
        .collect::<Vec<_>>();
    rows.push(row(
        config,
        ServiceOperation::ContendedRootCas,
        config.workload.concurrency,
        cas_ns,
        Distribution::from_samples(&cas_latencies)?,
        applied,
        conflicts,
    )?);
    Ok(rows)
}

pub fn write_service_rows_new(path: &Path, rows: &[ServiceEvidenceRow]) -> Result<(), String> {
    if rows.is_empty() {
        return Err("cannot write empty service evidence".to_string());
    }
    for row in rows {
        row.validate()?;
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("refusing to overwrite {}: {error}", path.display()))?;
    let mut writer = csv::WriterBuilder::new()
        .terminator(csv::Terminator::Any(b'\n'))
        .from_writer(file);
    for row in rows {
        writer
            .serialize(row)
            .map_err(|error| format!("failed to serialize service evidence: {error}"))?;
    }
    writer
        .flush()
        .map_err(|error| format!("failed to flush service evidence: {error}"))?;
    writer
        .get_ref()
        .sync_all()
        .map_err(|error| format!("failed to sync service evidence: {error}"))
}

#[derive(Clone, Copy)]
struct Distribution {
    p50: u128,
    p95: u128,
    p99: u128,
    p999: u128,
    max: u128,
}

impl Distribution {
    fn total(total: u128) -> Self {
        Self {
            p50: total,
            p95: total,
            p99: total,
            p999: total,
            max: total,
        }
    }

    fn from_samples(samples: &[u128]) -> Result<Self, String> {
        if samples.is_empty() {
            return Err("service latency distribution requires samples".to_string());
        }
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        Ok(Self {
            p50: percentile(&sorted, 0.50),
            p95: percentile(&sorted, 0.95),
            p99: percentile(&sorted, 0.99),
            p999: percentile(&sorted, 0.999),
            max: *sorted.last().expect("nonempty samples"),
        })
    }
}

fn row(
    config: &RunConfig,
    operation: ServiceOperation,
    logical_operations: usize,
    total_ns: u128,
    distribution: Distribution,
    applied: usize,
    conflicts: usize,
) -> Result<ServiceEvidenceRow, String> {
    let row = ServiceEvidenceRow {
        schema: SERVICE_SCHEMA.to_string(),
        run_id: config.run_id.clone(),
        backend: config.backend,
        repetition: config.repetition,
        operation,
        revision: config.revision.clone(),
        tree_hash: config.tree_hash.clone(),
        binary_sha256: config.binary_sha256.clone(),
        clients: config.workload.concurrency as u64,
        pool_size: config.pool_size,
        adapter_batch_items: config.adapter_batch_items as u64,
        logical_operations: logical_operations as u64,
        total_ns,
        ops_per_sec: logical_operations as f64 * 1_000_000_000.0 / total_ns as f64,
        p50_ns: distribution.p50,
        p95_ns: distribution.p95,
        p99_ns: distribution.p99,
        p999_ns: distribution.p999,
        max_ns: distribution.max,
        applied: applied as u64,
        conflicts: conflicts as u64,
        validated: true,
        error: String::new(),
    };
    row.validate()?;
    Ok(row)
}

fn service_key(id: usize) -> Vec<u8> {
    let mut key = vec![0_u8; 32];
    key[..8].copy_from_slice(&(id as u64).to_be_bytes());
    for (position, byte) in key[8..].iter_mut().enumerate() {
        *byte = (id as u8).wrapping_add(position as u8);
    }
    key
}

fn service_value(id: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|position| (id as u8).wrapping_mul(31).wrapping_add(position as u8))
        .collect()
}

fn validate_values(
    entries: &[(Vec<u8>, Vec<u8>)],
    values: &[Option<Vec<u8>>],
) -> Result<(), String> {
    if entries.len() != values.len() {
        return Err("service batch result length differs".to_string());
    }
    for (position, ((_, expected), actual)) in entries.iter().zip(values).enumerate() {
        if actual.as_deref() != Some(expected.as_slice()) {
            return Err(format!(
                "service batch value differs at position {position}"
            ));
        }
    }
    Ok(())
}

fn percentile(sorted: &[u128], quantile: f64) -> u128 {
    let rank = (quantile * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_keys_are_fixed_width_and_unique() {
        assert_eq!(service_key(1).len(), 32);
        assert_ne!(service_key(1), service_key(2));
    }

    #[test]
    fn service_percentiles_use_nearest_rank() {
        assert_eq!(percentile(&[10, 20, 30, 40, 50], 0.50), 30);
        assert_eq!(percentile(&[10, 20, 30, 40, 50], 0.99), 50);
    }
}
