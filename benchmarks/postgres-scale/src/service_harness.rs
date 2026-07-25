use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use prolly_store_postgres::{PostgresBackend, PostgresBackendOptions};
use sqlx::postgres::PgPoolOptions;

use crate::config::{CommandConfig, ServiceConfig};
use crate::harness::RunStats;
use crate::postgres::{read_pg_metrics, read_physical_size, reset_pg_stats};
use crate::service_measurement::{
    read_service_rows, OperationSample, SampleOutcome, ServiceAccumulator, ServiceCsvSink,
    ServiceRawRow,
};
use crate::service_model::{enumerate_service_cells, trace_item, ServiceCell, ServiceOperation};
use crate::service_workloads::{execute_operation, ServiceFixture};

pub async fn run_service_suite(command: &CommandConfig) -> Result<RunStats, String> {
    command.workload.service.validate()?;
    std::fs::create_dir_all(&command.output)
        .map_err(|error| format!("failed to create {}: {error}", command.output.display()))?;
    let config_hash = command.workload.configuration_hash()?;
    let raw_path = command.output.join("service-raw.csv");
    let mut existing = read_service_rows(&raw_path)?;
    for row in &existing {
        row.validate()?;
        if row.config_hash != config_hash
            || row.revision != command.revision
            || row.dirty != command.dirty
        {
            return Err(format!("service row provenance differs: {:?}", row.key()));
        }
    }
    let required_rows = required_row_count(&command.workload.service);
    let grouped = group_rows(&existing);
    let partial = grouped
        .iter()
        .filter_map(|(cell, count)| (*count != required_rows).then_some(*cell))
        .collect::<BTreeSet<_>>();
    if !partial.is_empty() {
        existing.retain(|row| !partial.contains(&(row.clients as usize, row.pool_size)));
        rewrite_rows(&raw_path, &existing)?;
    }
    let completed = group_rows(&existing)
        .into_iter()
        .filter_map(|(cell, count)| (count == required_rows).then_some(cell))
        .collect::<BTreeSet<_>>();
    let cells = enumerate_service_cells(&command.workload.service);
    if cells
        .iter()
        .all(|cell| completed.contains(&cell_key(*cell)))
    {
        return Ok(RunStats {
            measured: 0,
            skipped: cells.len() * required_rows,
            fixtures_built: 0,
        });
    }

    let bootstrap_connections = command
        .workload
        .service
        .pool_sizes
        .iter()
        .copied()
        .max()
        .unwrap_or(1);
    let bootstrap_pool = PgPoolOptions::new()
        .max_connections(bootstrap_connections)
        .connect(&command.url)
        .await
        .map_err(error)?;
    let options = PostgresBackendOptions::new(
        NonZeroUsize::new(command.workload.service.adapter_batch_items)
            .ok_or_else(|| "adapter batch size must be nonzero".to_string())?,
    );
    ServiceFixture::build_snapshot(
        PostgresBackend::new_with_options(bootstrap_pool, options),
        &command.workload.service,
    )
    .await?;

    let mut sink = ServiceCsvSink::open(&raw_path)?;
    let mut stats = RunStats {
        fixtures_built: 1,
        ..RunStats::default()
    };
    for cell in cells {
        if completed.contains(&cell_key(cell)) {
            stats.skipped += required_rows;
            continue;
        }
        eprintln!(
            "measuring service: clients={} pool_size={}",
            cell.clients, cell.pool_size
        );
        let pool = PgPoolOptions::new()
            .max_connections(cell.pool_size)
            .connect(&command.url)
            .await
            .map_err(error)?;
        let backend = PostgresBackend::new_with_options(pool, options);

        if command.workload.service.warmup_ms > 0 {
            let warmup_fixture =
                ServiceFixture::load(backend.clone(), &command.workload.service).await?;
            run_workers(
                warmup_fixture,
                &command.workload.service,
                command.workload.seed ^ 0x7761_726d_7570,
                cell.clients,
                Duration::from_millis(command.workload.service.warmup_ms),
            )
            .await?;
        }

        let fixture = ServiceFixture::load(backend.clone(), &command.workload.service).await?;
        let before = read_physical_size(backend.pool()).await.map_err(error)?;
        reset_pg_stats(backend.pool()).await.map_err(error)?;
        let (accumulator, elapsed) = run_workers(
            fixture.clone(),
            &command.workload.service,
            command.workload.seed,
            cell.clients,
            Duration::from_millis(command.workload.service.duration_ms),
        )
        .await?;
        fixture.validate().await?;
        let pg = read_pg_metrics(backend.pool()).await.map_err(error)?;
        let after = read_physical_size(backend.pool()).await.map_err(error)?;
        let summaries = accumulator.summaries();
        if summaries.len() != required_rows {
            return Err(format!(
                "service cell {}/{} observed {} operation classes, expected {required_rows}; increase duration",
                cell.clients,
                cell.pool_size,
                summaries.len()
            ));
        }
        let cell_attempted = summaries.iter().map(|summary| summary.attempted).sum();
        let cell_completed = summaries.iter().map(|summary| summary.completed).sum();
        for summary in summaries {
            let mut row = ServiceRawRow::from_summary(
                &config_hash,
                &command.revision,
                command.dirty,
                command.workload.service.records,
                command.workload.service.value_bytes,
                cell.clients,
                cell.pool_size,
                elapsed.as_nanos(),
                &summary,
                &pg,
                &before,
                &after,
            );
            row.cell_attempted = cell_attempted;
            row.cell_completed = cell_completed;
            row.validate()?;
            sink.append(&row)?;
            stats.measured += 1;
        }
    }
    Ok(stats)
}

async fn run_workers(
    fixture: ServiceFixture,
    config: &ServiceConfig,
    seed: u64,
    clients: usize,
    duration: Duration,
) -> Result<(ServiceAccumulator, Duration), String> {
    let fixture = Arc::new(fixture);
    let config = Arc::new(config.clone());
    let sequence = Arc::new(AtomicU64::new(0));
    let deadline = tokio::time::Instant::now() + duration;
    let started = Instant::now();
    let mut workers = tokio::task::JoinSet::new();
    for _ in 0..clients {
        let fixture = fixture.clone();
        let config = config.clone();
        let sequence = sequence.clone();
        workers.spawn(async move {
            let max_latency_ns = Duration::from_millis(config.operation_timeout_ms)
                .as_nanos()
                .min(u64::MAX as u128) as u64;
            let mut accumulator = ServiceAccumulator::new(max_latency_ns)?;
            while tokio::time::Instant::now() < deadline {
                let sequence = sequence.fetch_add(1, Ordering::Relaxed);
                let item = trace_item(&config, seed, sequence);
                let operation_started = Instant::now();
                let result = tokio::time::timeout(
                    Duration::from_millis(config.operation_timeout_ms),
                    execute_operation(&fixture, &item),
                )
                .await;
                match result {
                    Ok(Ok(sample)) => accumulator.record(sample)?,
                    Ok(Err(error)) => return Err(error),
                    Err(_) => {
                        accumulator.record(OperationSample {
                            operation: item.operation,
                            tenant_class: item.tenant_class,
                            latency_ns: operation_started.elapsed().as_nanos().min(u64::MAX as u128)
                                as u64,
                            attempts: 0,
                            conflicts: 0,
                            retries: 0,
                            outcome: SampleOutcome::Timeout,
                            prolly: Default::default(),
                        })?;
                        return Err(format!(
                            "service operation {} timed out after {} ms",
                            item.operation.as_str(),
                            config.operation_timeout_ms
                        ));
                    }
                }
            }
            Ok::<_, String>(accumulator)
        });
    }
    let mut combined = ServiceAccumulator::new(
        Duration::from_millis(config.operation_timeout_ms)
            .as_nanos()
            .min(u64::MAX as u128) as u64,
    )?;
    while let Some(worker) = workers.join_next().await {
        let accumulator = match worker {
            Ok(Ok(accumulator)) => accumulator,
            Ok(Err(error)) => {
                workers.abort_all();
                while workers.join_next().await.is_some() {}
                return Err(error);
            }
            Err(error) => {
                workers.abort_all();
                while workers.join_next().await.is_some() {}
                return Err(format!("service worker panic: {error}"));
            }
        };
        if let Err(error) = combined.merge(accumulator) {
            workers.abort_all();
            while workers.join_next().await.is_some() {}
            return Err(error);
        }
    }
    Ok((combined, started.elapsed()))
}

fn required_row_count(config: &ServiceConfig) -> usize {
    let classes =
        usize::from(config.hot_root_share < 1.0) + usize::from(config.hot_root_share > 0.0);
    ServiceOperation::ALL.len() * classes
}

fn cell_key(cell: ServiceCell) -> (usize, u32) {
    (cell.clients, cell.pool_size)
}

fn group_rows(rows: &[ServiceRawRow]) -> BTreeMap<(usize, u32), usize> {
    let mut groups = BTreeMap::new();
    for row in rows {
        *groups
            .entry((row.clients as usize, row.pool_size))
            .or_default() += 1;
    }
    groups
}

fn rewrite_rows(path: &std::path::Path, rows: &[ServiceRawRow]) -> Result<(), String> {
    if path.exists() {
        std::fs::remove_file(path)
            .map_err(|error| format!("failed to replace {}: {error}", path.display()))?;
    }
    if rows.is_empty() {
        return Ok(());
    }
    let mut sink = ServiceCsvSink::open(path)?;
    for row in rows {
        sink.append(row)?;
    }
    Ok(())
}

fn error(value: impl std::fmt::Display) -> String {
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WorkloadConfig;
    use crate::service_model::TenantClass;

    #[test]
    fn required_rows_follow_enabled_tenant_classes() {
        let mut config = WorkloadConfig::default().service;
        assert_eq!(required_row_count(&config), 10);
        config.hot_root_share = 0.0;
        assert_eq!(required_row_count(&config), 5);
        config.hot_root_share = 1.0;
        assert_eq!(required_row_count(&config), 5);
    }

    #[test]
    fn partial_cells_are_not_complete() {
        let mut row = service_row();
        let rows = vec![row.clone()];
        assert_eq!(group_rows(&rows).get(&(4, 2)), Some(&1));
        row.operation = TenantClass::Hot.as_str().to_string();
        assert_ne!(rows[0].key(), row.key());
    }

    fn service_row() -> ServiceRawRow {
        let mut accumulator = ServiceAccumulator::new(1_000).unwrap();
        accumulator
            .record(OperationSample::success(
                ServiceOperation::PointRead,
                TenantClass::Independent,
                10,
                0,
                0,
            ))
            .unwrap();
        ServiceRawRow::from_summary(
            "hash",
            "revision",
            true,
            1_000,
            64,
            4,
            2,
            1_000,
            &accumulator.summaries()[0],
            &Default::default(),
            &Default::default(),
            &Default::default(),
        )
    }
}
