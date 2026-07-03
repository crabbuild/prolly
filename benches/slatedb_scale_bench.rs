use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use prolly::{append_batch, Config, Mutation, Prolly, SlateDbStore, SlateDbStoreConfig, Tree};
use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::path::Path as ObjectPath;
use slatedb::object_store::{ObjectStore, ObjectStoreExt};

const DEFAULT_STAGES: &str = "1000,5000";
const DEFAULT_BATCH_SIZE: usize = 1_000;
const DEFAULT_MAX_SECONDS: u64 = 900;
const DEFAULT_MAX_OBJECT_GB: u64 = 70;

#[derive(Debug, Default)]
struct ObjectStats {
    count: usize,
    bytes: u64,
}

fn main() {
    let stages = parse_stages();
    let batch_size = env_usize("PROLLY_SLATEDB_SCALE_BATCH").unwrap_or(DEFAULT_BATCH_SIZE);
    let max_duration = Duration::from_secs(
        env_u64("PROLLY_SLATEDB_SCALE_MAX_SECONDS").unwrap_or(DEFAULT_MAX_SECONDS),
    );
    let max_object_bytes = env_u64("PROLLY_SLATEDB_SCALE_MAX_OBJECT_GB")
        .unwrap_or(DEFAULT_MAX_OBJECT_GB)
        * 1024
        * 1024
        * 1024;
    let keep_db = env_bool("PROLLY_SLATEDB_SCALE_KEEP_DB").unwrap_or(false);
    let path = db_path();
    let object_store = build_slatedb_object_store();

    remove_slatedb_prefix(object_store.clone(), &path);

    println!("slatedb scale bench");
    println!("path={path}");
    println!("endpoint={}", slatedb_endpoint());
    println!("bucket={}", slatedb_bucket());
    println!(
        "flush_after_write={}",
        slatedb_store_config().flush_after_write
    );
    println!("stages={stages:?}");
    println!("batch_size={batch_size}");
    println!("max_seconds={}", max_duration.as_secs());
    println!("max_object_bytes={max_object_bytes}");
    println!(
        "name,target_records,total_records,stage_ms,total_ms,object_count,object_bytes,bytes_per_record,records_per_sec,verified,status"
    );

    let config = bench_config();
    let mut store = Some(Arc::new(open_store(&path, object_store.clone())));
    let mut prolly = Some(Prolly::new(store.as_ref().unwrap().clone(), config.clone()));
    let mut tree = prolly.as_ref().unwrap().create();
    let mut total_records = 0usize;
    let total_start = Instant::now();

    for target in stages {
        if target <= total_records {
            continue;
        }

        let stage_start = Instant::now();
        let stage_start_records = total_records;
        let mut status = "ok";

        while total_records < target {
            let remaining = target - total_records;
            let count = remaining.min(batch_size);
            let mutations = append_mutations(total_records, count);
            tree = append_batch(prolly.as_ref().unwrap(), &tree, mutations).unwrap();
            total_records += count;

            if total_start.elapsed() >= max_duration {
                status = "hit-max-seconds";
                break;
            }
        }

        drop(prolly.take());
        drop(store.take());

        let stats = object_stats(object_store.clone(), &path);
        if stats.bytes >= max_object_bytes {
            status = "hit-max-object-bytes";
        }

        let stage_ms = stage_start.elapsed().as_secs_f64() * 1_000.0;
        let total_ms = total_start.elapsed().as_secs_f64() * 1_000.0;
        let stage_records = total_records - stage_start_records;
        let records_per_sec = if stage_ms > 0.0 {
            stage_records as f64 / (stage_ms / 1_000.0)
        } else {
            0.0
        };
        let bytes_per_record = if total_records == 0 {
            0.0
        } else {
            stats.bytes as f64 / total_records as f64
        };
        let verified =
            verify_reopen_reads(&path, object_store.clone(), &config, &tree, total_records);

        println!(
            "slatedb_scale_append,{target},{total_records},{stage_ms:.3},{total_ms:.3},{},{},{bytes_per_record:.2},{records_per_sec:.0},{verified},{status}",
            stats.count,
            stats.bytes
        );

        if status != "ok" {
            break;
        }

        store = Some(Arc::new(open_store(&path, object_store.clone())));
        prolly = Some(Prolly::new(store.as_ref().unwrap().clone(), config.clone()));
    }

    drop(prolly);
    drop(store);

    if !keep_db {
        remove_slatedb_prefix(object_store, &path);
    }
}

fn parse_stages() -> Vec<usize> {
    let raw =
        std::env::var("PROLLY_SLATEDB_SCALE_STAGES").unwrap_or_else(|_| DEFAULT_STAGES.to_string());
    let mut stages = raw
        .split(',')
        .filter_map(|part| part.trim().parse::<usize>().ok())
        .collect::<Vec<_>>();
    stages.sort_unstable();
    stages.dedup();
    stages
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}

fn env_bool(name: &str) -> Option<bool> {
    match std::env::var(name).ok()?.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn db_path() -> String {
    if let Ok(path) = std::env::var("PROLLY_SLATEDB_SCALE_PATH") {
        return path.trim().trim_matches('/').to_string();
    }

    let prefix = std::env::var("PROLLY_SLATEDB_PATH_PREFIX")
        .unwrap_or_else(|_| "crabdb/prolly-bench".to_string());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{}/slatedb-scale-{}-{nanos}",
        prefix.trim().trim_matches('/'),
        std::process::id()
    )
}

fn open_store(path: &str, object_store: Arc<dyn ObjectStore>) -> SlateDbStore {
    SlateDbStore::open_with_config(path.to_string(), object_store, slatedb_store_config()).unwrap()
}

fn slatedb_store_config() -> SlateDbStoreConfig {
    SlateDbStoreConfig {
        flush_after_write: env_bool("PROLLY_SLATEDB_FLUSH_AFTER_WRITE").unwrap_or(true),
        ..SlateDbStoreConfig::default()
    }
}

fn build_slatedb_object_store() -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(slatedb_endpoint().trim_end_matches('/'))
            .with_bucket_name(slatedb_bucket().trim())
            .with_region(slatedb_region().trim())
            .with_access_key_id(slatedb_access_key_id())
            .with_secret_access_key(slatedb_secret_access_key())
            .with_allow_http(env_bool("PROLLY_SLATEDB_ALLOW_HTTP").unwrap_or(true))
            .with_virtual_hosted_style_request(false)
            .build()
            .unwrap(),
    )
}

fn slatedb_endpoint() -> String {
    std::env::var("PROLLY_SLATEDB_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string())
}

fn slatedb_bucket() -> String {
    std::env::var("PROLLY_SLATEDB_BUCKET").unwrap_or_else(|_| "crab".to_string())
}

fn slatedb_region() -> String {
    std::env::var("PROLLY_SLATEDB_REGION").unwrap_or_else(|_| "us-east-1".to_string())
}

fn slatedb_access_key_id() -> String {
    std::env::var("PROLLY_SLATEDB_ACCESS_KEY_ID").unwrap_or_else(|_| "crab".to_string())
}

fn slatedb_secret_access_key() -> String {
    std::env::var("PROLLY_SLATEDB_SECRET_ACCESS_KEY").unwrap_or_else(|_| "crab".to_string())
}

fn append_mutations(start: usize, count: usize) -> Vec<Mutation> {
    (start..start + count)
        .map(|i| Mutation::Upsert {
            key: key_for_index(i),
            val: value_for_index(i),
        })
        .collect()
}

fn key_for_index(i: usize) -> Vec<u8> {
    format!("key-{i:012}").into_bytes()
}

fn value_for_index(i: usize) -> Vec<u8> {
    format!("value-{i:012}-payload").into_bytes()
}

fn verify_reopen_reads(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    config: &Config,
    tree: &Tree,
    total_records: usize,
) -> bool {
    if total_records == 0 {
        return tree.root.is_none();
    }

    let Ok(store) =
        SlateDbStore::open_with_config(path.to_string(), object_store, slatedb_store_config())
    else {
        return false;
    };
    let prolly = Prolly::new(store, config.clone());
    let probes = [0, total_records / 2, total_records - 1];

    probes.iter().all(|idx| {
        prolly
            .get(tree, &key_for_index(*idx))
            .ok()
            .flatten()
            .as_deref()
            == Some(value_for_index(*idx).as_slice())
    })
}

fn object_stats(object_store: Arc<dyn ObjectStore>, path: &str) -> ObjectStats {
    let runtime = runtime();
    let prefix = ObjectPath::from(path);
    runtime.block_on(async move {
        let mut stats = ObjectStats::default();
        let mut list = object_store.list(Some(&prefix));
        while let Some(meta) = list.next().await.transpose().unwrap() {
            stats.count += 1;
            stats.bytes += meta.size;
        }
        stats
    })
}

fn remove_slatedb_prefix(object_store: Arc<dyn ObjectStore>, path: &str) {
    let runtime = runtime();
    let prefix = ObjectPath::from(path);
    runtime.block_on(async move {
        let mut locations = Vec::new();
        let mut list = object_store.list(Some(&prefix));
        while let Some(meta) = list.next().await.transpose().unwrap() {
            locations.push(meta.location);
        }
        drop(list);
        for location in locations {
            let _ = object_store.delete(&location).await;
        }
    });
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("prolly-slatedb-bench")
        .enable_all()
        .build()
        .unwrap()
}

fn bench_config() -> Config {
    Config::builder()
        .min_chunk_size(64)
        .max_chunk_size(512)
        .chunking_factor(256)
        .hash_seed(0xC0DA)
        .build()
}
