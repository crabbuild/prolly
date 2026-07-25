use std::collections::VecDeque;
use std::time::Instant;

use prolly::{AsyncProlly, Config, Diff, Mutation, NamedRootUpdate, RemoteProllyStore, Tree};
use prolly_store_postgres::{PostgresBackend, PostgresStore};
use tokio::sync::RwLock;

use crate::config::ServiceConfig;
use crate::postgres::{clear_all, initialize_benchmark_schema, restore_base, snapshot_base};
use crate::service_measurement::{OperationSample, SampleOutcome};
use crate::service_model::{root_name, value_sized, ServiceOperation, TraceItem};

type Manager = AsyncProlly<PostgresStore>;

#[derive(Clone)]
struct TenantState {
    history: std::sync::Arc<RwLock<VecDeque<Tree>>>,
}

#[derive(Clone)]
pub struct ServiceFixture {
    backend: PostgresBackend,
    config: ServiceConfig,
    tenants: std::sync::Arc<Vec<TenantState>>,
}

impl ServiceFixture {
    pub async fn build_snapshot(
        backend: PostgresBackend,
        config: &ServiceConfig,
    ) -> Result<(), String> {
        backend.initialize_schema().await.map_err(error)?;
        initialize_benchmark_schema(backend.pool())
            .await
            .map_err(error)?;
        clear_all(backend.pool()).await.map_err(error)?;

        let manager = manager(&backend);
        let mutations = (0..config.records)
            .map(|id| Mutation::Upsert {
                key: service_key(id),
                val: value_sized(id, 0, config.value_bytes),
            })
            .collect::<Vec<_>>();
        let base = manager
            .batch(&manager.create(), mutations)
            .await
            .map_err(error)?;

        for tenant in 0..config.tenants {
            let left_ids = branch_ids(config.records, config.commit_keys, 0);
            let right_ids = branch_ids(config.records, config.commit_keys, config.commit_keys);
            let left = manager
                .batch(
                    &base,
                    mutations_for(&left_ids, 1 + tenant as u64 * 2, config.value_bytes),
                )
                .await
                .map_err(error)?;
            let right = manager
                .batch(
                    &base,
                    mutations_for(&right_ids, 2 + tenant as u64 * 2, config.value_bytes),
                )
                .await
                .map_err(error)?;
            manager
                .publish_named_root(&root_name(tenant, "main"), &base)
                .await
                .map_err(error)?;
            manager
                .publish_named_root(&root_name(tenant, "left"), &left)
                .await
                .map_err(error)?;
            manager
                .publish_named_root(&root_name(tenant, "right"), &right)
                .await
                .map_err(error)?;
        }
        validate_tree(&manager, &base, config).await?;
        snapshot_base(backend.pool()).await.map_err(error)
    }

    pub async fn load(backend: PostgresBackend, config: &ServiceConfig) -> Result<Self, String> {
        restore_base(backend.pool()).await.map_err(error)?;
        let manager = manager(&backend);
        let mut tenants = Vec::with_capacity(config.tenants);
        for tenant in 0..config.tenants {
            let base = load_required(&manager, &root_name(tenant, "main")).await?;
            let left = load_required(&manager, &root_name(tenant, "left")).await?;
            let right = load_required(&manager, &root_name(tenant, "right")).await?;
            tenants.push(TenantState {
                history: std::sync::Arc::new(RwLock::new(VecDeque::from([
                    base.clone(),
                    left.clone(),
                    right.clone(),
                ]))),
            });
        }
        let fixture = Self {
            backend,
            config: config.clone(),
            tenants: std::sync::Arc::new(tenants),
        };
        fixture.validate().await?;
        Ok(fixture)
    }

    pub async fn validate(&self) -> Result<(), String> {
        let manager = manager(&self.backend);
        for tenant in validation_tenants(self.config.tenants) {
            let tree = load_required(&manager, &root_name(tenant, "main")).await?;
            validate_tree(&manager, &tree, &self.config).await?;
        }
        Ok(())
    }

    async fn retain(&self, tenant: usize, tree: Tree) {
        let mut history = self.tenants[tenant].history.write().await;
        history.push_back(tree);
        while history.len() > self.config.retained_versions {
            history.pop_front();
        }
    }
}

pub async fn execute_operation(
    fixture: &ServiceFixture,
    item: &TraceItem,
) -> Result<OperationSample, String> {
    match item.operation {
        ServiceOperation::PointRead => point_read(fixture, item).await,
        ServiceOperation::MultiRead => multi_read(fixture, item).await,
        ServiceOperation::Commit => commit(fixture, item).await,
        ServiceOperation::Diff => diff(fixture, item).await,
        ServiceOperation::Merge => merge(fixture, item).await,
    }
}

async fn point_read(fixture: &ServiceFixture, item: &TraceItem) -> Result<OperationSample, String> {
    let manager = manager(&fixture.backend);
    let started = Instant::now();
    let tree = load_required(&manager, &item.root_name).await?;
    let value = manager
        .get(&tree, &service_key(item.key_ids[0]))
        .await
        .map_err(error)?;
    require_value_size(value.as_deref(), fixture.config.value_bytes)?;
    Ok(success_sample(item, started, 0, 0, &manager))
}

async fn multi_read(fixture: &ServiceFixture, item: &TraceItem) -> Result<OperationSample, String> {
    let manager = manager(&fixture.backend);
    let keys = item
        .key_ids
        .iter()
        .map(|id| service_key(*id))
        .collect::<Vec<_>>();
    let started = Instant::now();
    let tree = load_required(&manager, &item.root_name).await?;
    let values = manager.get_many(&tree, &keys).await.map_err(error)?;
    if values.len() != keys.len() {
        return Err("multi-read returned the wrong number of values".to_string());
    }
    for value in values {
        require_value_size(value.as_deref(), fixture.config.value_bytes)?;
    }
    Ok(success_sample(item, started, 0, 0, &manager))
}

async fn commit(fixture: &ServiceFixture, item: &TraceItem) -> Result<OperationSample, String> {
    let manager = manager(&fixture.backend);
    let started = Instant::now();
    let mut attempts = 0u64;
    let mut conflicts = 0u64;
    while attempts <= fixture.config.cas_retries as u64 {
        attempts += 1;
        let expected = load_required(&manager, &item.root_name).await?;
        let changed = manager
            .batch(
                &expected,
                mutations_for(&item.key_ids, item.generation, fixture.config.value_bytes),
            )
            .await
            .map_err(error)?;
        match manager
            .compare_and_swap_named_root(&item.root_name, Some(&expected), Some(&changed))
            .await
            .map_err(error)?
        {
            NamedRootUpdate::Applied => {
                fixture.retain(item.tenant, changed).await;
                return Ok(success_sample(item, started, attempts, conflicts, &manager));
            }
            NamedRootUpdate::Conflict { .. } => conflicts += 1,
        }
    }
    Ok(OperationSample {
        operation: item.operation,
        tenant_class: item.tenant_class,
        latency_ns: elapsed_ns(started),
        attempts,
        conflicts,
        retries: attempts.saturating_sub(1),
        outcome: SampleOutcome::ExhaustedRetries,
        prolly: manager.metrics(),
    })
}

async fn diff(fixture: &ServiceFixture, item: &TraceItem) -> Result<OperationSample, String> {
    let manager = manager(&fixture.backend);
    let tenant = &fixture.tenants[item.tenant];
    let (older, newer) = {
        let history = tenant.history.read().await;
        let newer = history
            .back()
            .cloned()
            .ok_or_else(|| "tenant version history is empty".to_string())?;
        let older_index = (item.sequence as usize) % history.len().saturating_sub(1).max(1);
        let older = history
            .get(older_index)
            .cloned()
            .ok_or_else(|| "tenant version history is incomplete".to_string())?;
        (older, newer)
    };
    let started = Instant::now();
    let differences = manager.diff(&older, &newer).await.map_err(error)?;
    if differences.is_empty() {
        return Err("retained-version diff unexpectedly returned no changes".to_string());
    }
    let mut previous: Option<&[u8]> = None;
    for difference in &differences {
        let key = match difference {
            Diff::Added { key, .. } | Diff::Removed { key, .. } | Diff::Changed { key, .. } => {
                key.as_slice()
            }
        };
        if previous.is_some_and(|prior| prior >= key) {
            return Err("diff keys are not strictly ordered".to_string());
        }
        previous = Some(key);
    }
    Ok(success_sample(item, started, 0, 0, &manager))
}

async fn merge(fixture: &ServiceFixture, item: &TraceItem) -> Result<OperationSample, String> {
    let manager = manager(&fixture.backend);
    let started = Instant::now();
    let expected = load_required(&manager, &item.root_name).await?;
    let split = item.key_ids.len() / 2;
    let left = manager
        .batch(
            &expected,
            mutations_for(
                &item.key_ids[..split],
                item.generation.saturating_mul(2),
                fixture.config.value_bytes,
            ),
        )
        .await
        .map_err(error)?;
    let right = manager
        .batch(
            &expected,
            mutations_for(
                &item.key_ids[split..],
                item.generation.saturating_mul(2).saturating_add(1),
                fixture.config.value_bytes,
            ),
        )
        .await
        .map_err(error)?;
    let merged = manager
        .merge(&expected, &left, &right, None)
        .await
        .map_err(error)?;
    let (outcome, conflicts) = match manager
        .compare_and_swap_named_root(&item.root_name, Some(&expected), Some(&merged))
        .await
        .map_err(error)?
    {
        NamedRootUpdate::Applied => {
            fixture.retain(item.tenant, merged).await;
            (SampleOutcome::Success, 0)
        }
        NamedRootUpdate::Conflict { .. } => (SampleOutcome::ExhaustedRetries, 1),
    };
    Ok(OperationSample {
        operation: item.operation,
        tenant_class: item.tenant_class,
        latency_ns: elapsed_ns(started),
        attempts: 1,
        conflicts,
        retries: 0,
        outcome,
        prolly: manager.metrics(),
    })
}

fn success_sample(
    item: &TraceItem,
    started: Instant,
    attempts: u64,
    conflicts: u64,
    manager: &Manager,
) -> OperationSample {
    OperationSample::success(
        item.operation,
        item.tenant_class,
        elapsed_ns(started),
        attempts,
        conflicts,
    )
    .with_prolly_metrics(manager.metrics())
}

fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

fn manager(backend: &PostgresBackend) -> Manager {
    AsyncProlly::new(RemoteProllyStore::new(backend.clone()), Config::default())
}

fn service_key(id: usize) -> Vec<u8> {
    format!("key-{id:020}").into_bytes()
}

fn branch_ids(records: usize, count: usize, start: usize) -> Vec<usize> {
    (start..start.saturating_add(count))
        .map(|id| id % records)
        .collect()
}

fn mutations_for(ids: &[usize], generation: u64, value_bytes: usize) -> Vec<Mutation> {
    ids.iter()
        .map(|id| Mutation::Upsert {
            key: service_key(*id),
            val: value_sized(*id, generation, value_bytes),
        })
        .collect()
}

async fn load_required(manager: &Manager, name: &[u8]) -> Result<Tree, String> {
    manager
        .load_named_root(name)
        .await
        .map_err(error)?
        .ok_or_else(|| format!("named root {} is missing", String::from_utf8_lossy(name)))
}

async fn validate_tree(
    manager: &Manager,
    tree: &Tree,
    config: &ServiceConfig,
) -> Result<(), String> {
    let stats = manager.collect_stats(tree).await.map_err(error)?;
    if stats.total_key_value_pairs != config.records {
        return Err(format!(
            "service tree count mismatch: expected {}, observed {}",
            config.records, stats.total_key_value_pairs
        ));
    }
    for id in [0, config.records / 2, config.records - 1] {
        let value = manager.get(tree, &service_key(id)).await.map_err(error)?;
        require_value_size(value.as_deref(), config.value_bytes)?;
    }
    Ok(())
}

fn require_value_size(value: Option<&[u8]>, expected: usize) -> Result<(), String> {
    match value {
        Some(value) if value.len() == expected => Ok(()),
        Some(value) => Err(format!(
            "service value length mismatch: expected {expected}, observed {}",
            value.len()
        )),
        None => Err("service key is missing".to_string()),
    }
}

fn validation_tenants(tenants: usize) -> Vec<usize> {
    [0, tenants / 2, tenants - 1]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn error(value: impl std::fmt::Display) -> String {
    value.to_string()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use prolly_store_postgres::PostgresBackendOptions;

    use super::*;
    use crate::config::WorkloadConfig;
    use crate::service_model::trace_item;

    #[tokio::test]
    #[ignore = "requires PROLLY_STORE_POSTGRES_URL"]
    async fn postgres_service_fixture_exercises_every_operation() {
        let Ok(url) = std::env::var("PROLLY_STORE_POSTGRES_URL") else {
            return;
        };
        let workload = WorkloadConfig::load(
            &WorkloadConfig::default_path()
                .parent()
                .unwrap()
                .join("smoke.toml"),
        )
        .unwrap();
        let options =
            PostgresBackendOptions::new(NonZeroUsize::new(32).expect("nonzero batch size"));
        let backend = PostgresBackend::connect_with_options(&url, options)
            .await
            .unwrap();
        ServiceFixture::build_snapshot(backend.clone(), &workload.service)
            .await
            .unwrap();
        let fixture = ServiceFixture::load(backend, &workload.service)
            .await
            .unwrap();

        let mut observed = std::collections::BTreeSet::new();
        for sequence in 0..100 {
            let item = trace_item(&workload.service, workload.seed, sequence);
            if observed.insert(item.operation) {
                execute_operation(&fixture, &item).await.unwrap();
            }
        }
        assert_eq!(
            observed,
            ServiceOperation::ALL
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        );
        fixture.validate().await.unwrap();
    }
}
