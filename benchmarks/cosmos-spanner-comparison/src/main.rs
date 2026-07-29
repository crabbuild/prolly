use std::env;
use std::error::Error;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use prolly::{RemoteBatchOp, RemoteManifestUpdate, RemoteStoreBackend};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let provider = env::args()
        .nth(1)
        .ok_or("usage: benchmark <cosmos|spanner>")?;
    let items = env::var("PROLLY_CLOUD_BENCH_ITEMS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_000usize)
        .max(1);
    let value_bytes = env::var("PROLLY_CLOUD_BENCH_VALUE_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_024usize)
        .max(1);
    let cas_iterations = env::var("PROLLY_CLOUD_BENCH_CAS_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100usize)
        .max(1);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .to_be_bytes();
    let keys = benchmark_keys(items, nonce);
    let values = benchmark_values(items, value_bytes);

    match provider.as_str() {
        "cosmos" => {
            use prolly_store_cosmosdb::{CosmosDbBackend, CosmosDbBackendOptions};

            let endpoint = required_env("PROLLY_STORE_COSMOS_ENDPOINT")?;
            let key = required_env("PROLLY_STORE_COSMOS_KEY")?;
            let database = required_env("PROLLY_STORE_COSMOS_DATABASE")?;
            let container = required_env("PROLLY_STORE_COSMOS_CONTAINER")?;
            let options = CosmosDbBackendOptions::default().with_max_concurrency(
                env::var("PROLLY_CLOUD_BENCH_CONCURRENCY")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(16),
            );
            let prefix = format!("prolly:bench:{}:", hex_string(&nonce)).into_bytes();
            let backend = CosmosDbBackend::with_http_client_and_options(
                reqwest::Client::new(),
                endpoint,
                &key,
                database,
                container,
                options,
            )?
            .with_key_prefix(prefix);
            let result = run_workload("cosmos", &backend, &keys, &values, cas_iterations).await?;
            let metrics = backend.metrics();
            println!(
                "{}",
                serde_json::json!({
                    "provider": "cosmos",
                    "items": items,
                    "value_bytes": value_bytes,
                    "cas_iterations": cas_iterations,
                    "batch_put_ms": result.batch_put_ms,
                    "batch_get_ms": result.batch_get_ms,
                    "cas_ms": result.cas_ms,
                    "requests": metrics.requests,
                    "retries": metrics.retries,
                    "request_charge": metrics.request_charge,
                })
            );
            backend.clear_namespace().await?;
        }
        "spanner" => {
            use google_cloud_spanner::client::ClientConfig;
            use prolly_store_spanner::{SpannerBackend, SpannerBackendOptions};

            let database = required_env("PROLLY_STORE_SPANNER_DATABASE")?;
            let mut config = ClientConfig::default();
            if env::var_os("SPANNER_EMULATOR_HOST").is_none() {
                config = config.with_auth().await?;
            }
            let options = SpannerBackendOptions::default().with_batch_read_items(
                env::var("PROLLY_CLOUD_BENCH_BATCH_READ_ITEMS")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(5_000),
            );
            let backend = SpannerBackend::connect_with_options(&database, config, options).await?;
            let result = run_workload("spanner", &backend, &keys, &values, cas_iterations).await?;
            println!(
                "{}",
                serde_json::json!({
                    "provider": "spanner",
                    "items": items,
                    "value_bytes": value_bytes,
                    "cas_iterations": cas_iterations,
                    "batch_put_ms": result.batch_put_ms,
                    "batch_get_ms": result.batch_get_ms,
                    "cas_ms": result.cas_ms,
                })
            );
            backend.client().clone().close().await;
        }
        _ => return Err(format!("unknown provider {provider:?}; use cosmos or spanner").into()),
    }
    Ok(())
}

struct WorkloadResult {
    batch_put_ms: f64,
    batch_get_ms: f64,
    cas_ms: f64,
}

async fn run_workload<B: RemoteStoreBackend>(
    provider: &str,
    backend: &B,
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
    cas_iterations: usize,
) -> Result<WorkloadResult, B::Error> {
    let entries = keys
        .iter()
        .zip(values)
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect::<Vec<_>>();
    let started = Instant::now();
    backend.batch_put_nodes(&entries).await?;
    let batch_put_ms = elapsed_ms(started);

    let key_refs = keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let started = Instant::now();
    let fetched = backend.batch_get_nodes_ordered(&key_refs).await?;
    let batch_get_ms = elapsed_ms(started);
    assert_eq!(fetched.len(), values.len());
    for (actual, expected) in fetched.iter().zip(values) {
        assert_eq!(actual.as_deref(), Some(expected.as_slice()));
    }

    let root_name = format!("benchmark/{provider}").into_bytes();
    let mut expected = None;
    let started = Instant::now();
    for iteration in 0..cas_iterations {
        let next = (iteration as u64).to_be_bytes();
        let update = backend
            .compare_and_swap_root_manifest(&root_name, expected.as_deref(), Some(next.as_slice()))
            .await?;
        assert_eq!(update, RemoteManifestUpdate::Applied);
        expected = Some(next.to_vec());
    }
    let cas_ms = elapsed_ms(started);

    let deletes = keys
        .iter()
        .map(|key| RemoteBatchOp::Delete {
            key: key.as_slice(),
        })
        .collect::<Vec<_>>();
    backend.batch_nodes(&deletes).await?;
    backend
        .compare_and_swap_root_manifest(&root_name, expected.as_deref(), None)
        .await?;

    Ok(WorkloadResult {
        batch_put_ms,
        batch_get_ms,
        cas_ms,
    })
}

fn benchmark_keys(items: usize, nonce: [u8; 16]) -> Vec<Vec<u8>> {
    (0..items)
        .map(|index| {
            let mut key = vec![0u8; 32];
            key[..16].copy_from_slice(&nonce);
            key[24..].copy_from_slice(&(index as u64).to_be_bytes());
            key
        })
        .collect()
}

fn benchmark_values(items: usize, value_bytes: usize) -> Vec<Vec<u8>> {
    (0..items)
        .map(|index| vec![(index % 251) as u8; value_bytes])
        .collect()
}

fn required_env(name: &str) -> Result<String, Box<dyn Error>> {
    env::var(name).map_err(|_| format!("{name} must be set").into())
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000.0
}

fn hex_string(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
