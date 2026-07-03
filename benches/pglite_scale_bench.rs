use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prolly::{append_batch, Config, Mutation, PgliteStore, PgliteStoreConfig, Prolly, Tree};

const DEFAULT_STAGES: &str = "10000,50000";
const DEFAULT_BATCH_SIZE: usize = 5_000;
const DEFAULT_MAX_SECONDS: u64 = 900;
const DEFAULT_MAX_DB_GB: u64 = 70;

fn main() {
    let stages = parse_stages();
    let batch_size = env_usize("PROLLY_PGLITE_SCALE_BATCH").unwrap_or(DEFAULT_BATCH_SIZE);
    let max_duration = Duration::from_secs(
        env_u64("PROLLY_PGLITE_SCALE_MAX_SECONDS").unwrap_or(DEFAULT_MAX_SECONDS),
    );
    let max_db_bytes =
        env_u64("PROLLY_PGLITE_SCALE_MAX_DB_GB").unwrap_or(DEFAULT_MAX_DB_GB) * 1024 * 1024 * 1024;
    let keep_db = std::env::var("PROLLY_PGLITE_SCALE_KEEP_DB").ok().as_deref() == Some("1");
    let path = db_path();

    remove_pglite_dir(&path);

    println!("pglite scale bench");
    println!("data_dir={}", path.display());
    println!("node_cwd={}", node_working_dir_display());
    println!("stages={stages:?}");
    println!("batch_size={batch_size}");
    println!("max_seconds={}", max_duration.as_secs());
    println!("max_db_bytes={max_db_bytes}");
    println!(
        "name,target_records,total_records,stage_ms,total_ms,db_bytes,bytes_per_record,records_per_sec,verified,status"
    );

    let config = bench_config();
    let mut store = Arc::new(open_store(&path));
    let mut prolly = Prolly::new(store.clone(), config.clone());
    let mut tree = prolly.create();
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
            tree = append_batch(&prolly, &tree, mutations).unwrap();
            total_records += count;

            let db_bytes = pglite_dir_bytes(&path);
            if db_bytes >= max_db_bytes {
                status = "hit-max-db-bytes";
                break;
            }
            if total_start.elapsed() >= max_duration {
                status = "hit-max-seconds";
                break;
            }
        }

        let db_bytes = pglite_dir_bytes(&path);
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
            db_bytes as f64 / total_records as f64
        };
        drop(prolly);
        drop(store);
        let verified = verify_reopen_reads(&path, &config, &tree, total_records);
        store = Arc::new(open_store(&path));
        prolly = Prolly::new(store.clone(), config.clone());

        println!(
            "pglite_scale_append,{target},{total_records},{stage_ms:.3},{total_ms:.3},{db_bytes},{bytes_per_record:.2},{records_per_sec:.0},{verified},{status}"
        );

        if status != "ok" {
            break;
        }
    }

    drop(prolly);
    drop(store);

    if !keep_db {
        remove_pglite_dir(&path);
    }
}

fn parse_stages() -> Vec<usize> {
    let raw =
        std::env::var("PROLLY_PGLITE_SCALE_STAGES").unwrap_or_else(|_| DEFAULT_STAGES.to_string());
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

fn db_path() -> PathBuf {
    if let Ok(path) = std::env::var("PROLLY_PGLITE_SCALE_DB") {
        return PathBuf::from(path);
    }

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crabdb-prolly-pglite-scale-{}-{nanos}",
        std::process::id()
    ))
}

fn open_store(path: &Path) -> PgliteStore {
    PgliteStore::open_with_config(PgliteStoreConfig {
        data_dir: path.to_string_lossy().to_string(),
        node_working_dir: node_working_dir(),
        ..PgliteStoreConfig::default()
    })
    .unwrap()
}

fn node_working_dir() -> Option<PathBuf> {
    std::env::var("PROLLY_PGLITE_NODE_CWD")
        .ok()
        .map(PathBuf::from)
}

fn node_working_dir_display() -> String {
    node_working_dir()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "<current>".to_string())
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

fn verify_reopen_reads(path: &Path, config: &Config, tree: &Tree, total_records: usize) -> bool {
    if total_records == 0 {
        return tree.root.is_none();
    }

    let Ok(store) = PgliteStore::open_with_config(PgliteStoreConfig {
        data_dir: path.to_string_lossy().to_string(),
        node_working_dir: node_working_dir(),
        ..PgliteStoreConfig::default()
    }) else {
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

fn pglite_dir_bytes(path: &Path) -> u64 {
    dir_bytes(path).unwrap_or(0)
}

fn dir_bytes(path: &Path) -> std::io::Result<u64> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    if metadata.is_file() {
        return Ok(metadata.len());
    }

    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        total += dir_bytes(&entry.path())?;
    }
    Ok(total)
}

fn remove_pglite_dir(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
}

fn bench_config() -> Config {
    Config::builder()
        .min_chunk_size(64)
        .max_chunk_size(512)
        .chunking_factor(256)
        .hash_seed(0xC0DA)
        .build()
}
