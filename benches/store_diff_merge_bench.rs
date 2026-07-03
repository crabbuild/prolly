use std::time::{Duration, Instant};

use prolly::{
    append_batch, Config, Error, MemStore, Mutation, Prolly, Resolution, Resolver, Store, Tree,
};

#[cfg(feature = "slatedb")]
use futures_util::StreamExt;
#[cfg(any(feature = "sqlite", feature = "pglite"))]
use std::path::{Path, PathBuf};
#[cfg(feature = "slatedb")]
use std::sync::Arc;
#[cfg(any(feature = "sqlite", feature = "pglite", feature = "slatedb"))]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "pglite")]
use prolly::{PgliteStore, PgliteStoreConfig};
#[cfg(feature = "slatedb")]
use prolly::{SlateDbStore, SlateDbStoreConfig};
#[cfg(feature = "sqlite")]
use prolly::{SqliteStore, SqliteStoreConfig};
#[cfg(feature = "slatedb")]
use slatedb::object_store::aws::AmazonS3Builder;
#[cfg(feature = "slatedb")]
use slatedb::object_store::path::Path as ObjectPath;
#[cfg(feature = "slatedb")]
use slatedb::object_store::{ObjectStore, ObjectStoreExt};

const DEFAULT_STAGES: &str = "10000";
const DEFAULT_CHANGES: usize = 1_000;
const DEFAULT_MAX_SECONDS: u64 = 300;

fn main() {
    let stages = parse_stages();
    let requested_changes = env_usize("PROLLY_DIFF_MERGE_CHANGES").unwrap_or(DEFAULT_CHANGES);
    let max_duration = Duration::from_secs(
        env_u64("PROLLY_DIFF_MERGE_MAX_SECONDS").unwrap_or(DEFAULT_MAX_SECONDS),
    );
    let total_start = Instant::now();

    println!("prolly store diff/merge bench");
    println!("stages={stages:?}");
    println!("requested_changes={requested_changes}");
    println!("max_seconds={}", max_duration.as_secs());
    println!("store,operation,records,changes,total_ms,items_per_sec,diff_count,verified,status");

    for records in stages {
        let changes = requested_changes.min((records / 4).max(1));

        run_mem(records, changes);

        #[cfg(feature = "sqlite")]
        run_sqlite(records, changes);

        #[cfg(feature = "pglite")]
        run_pglite(records, changes);

        #[cfg(feature = "slatedb")]
        run_slatedb(records, changes);

        if total_start.elapsed() >= max_duration {
            eprintln!("hit max duration after records={records}");
            break;
        }
    }
}

fn run_mem(records: usize, changes: usize) {
    run_store("mem", MemStore::new(), records, changes);
}

#[cfg(feature = "sqlite")]
fn run_sqlite(records: usize, changes: usize) {
    let path = sqlite_path(records);
    remove_sqlite_files(&path);
    let store = SqliteStore::open_with_config(&path, durable_sqlite_config()).unwrap();
    run_store("sqlite", store, records, changes);
    remove_sqlite_files(&path);
}

#[cfg(feature = "pglite")]
fn run_pglite(records: usize, changes: usize) {
    let path = pglite_path(records);
    remove_pglite_dir(&path);
    let store = PgliteStore::open_with_config(PgliteStoreConfig {
        data_dir: path.to_string_lossy().to_string(),
        node_working_dir: node_working_dir(),
        ..PgliteStoreConfig::default()
    })
    .unwrap();
    run_store("pglite", store, records, changes);
    remove_pglite_dir(&path);
}

#[cfg(feature = "slatedb")]
fn run_slatedb(records: usize, changes: usize) {
    let path = slatedb_path("diff-merge", records);
    let object_store = build_slatedb_object_store();
    remove_slatedb_prefix(object_store.clone(), &path);
    let store =
        SlateDbStore::open_with_config(path.clone(), object_store.clone(), slatedb_store_config())
            .unwrap();
    run_store("slatedb", store, records, changes);
    remove_slatedb_prefix(object_store, &path);
}

fn run_store<S>(store_name: &str, store: S, records: usize, changes: usize)
where
    S: Store,
{
    let config = bench_config();
    let prolly = Prolly::new(store, config);

    let start = Instant::now();
    let base = append_batch(&prolly, &prolly.create(), base_mutations(records)).unwrap();
    print_row(BenchRow {
        store: store_name,
        operation: "build_base",
        records,
        changes: records,
        elapsed: start.elapsed(),
        diff_count: 0,
        verified: verify_base(&prolly, &base, records),
        status: "ok",
    });

    let left_mutations = left_update_mutations(changes);
    let right_mutations = right_update_mutations(records, changes);

    let left = prolly.batch(&base, left_mutations.clone()).unwrap();
    let right = prolly.batch(&base, right_mutations.clone()).unwrap();

    let start = Instant::now();
    let left_diff = prolly.diff(&base, &left).unwrap();
    let left_diff_elapsed = start.elapsed();
    let left_diff_verified =
        left_diff.len() == changes && verify_left_updates(&prolly, &left, changes);
    print_row(BenchRow {
        store: store_name,
        operation: "diff_sparse_left",
        records,
        changes,
        elapsed: left_diff_elapsed,
        diff_count: left_diff.len(),
        verified: left_diff_verified,
        status: "ok",
    });

    let start = Instant::now();
    let streaming = prolly
        .stream_diff(&base, &left)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let streaming_elapsed = start.elapsed();
    let streaming_verified = streaming == left_diff;
    print_row(BenchRow {
        store: store_name,
        operation: "stream_diff_sparse_left",
        records,
        changes,
        elapsed: streaming_elapsed,
        diff_count: streaming.len(),
        verified: streaming_verified,
        status: "ok",
    });

    let range_start = key_for_index(0);
    let range_end = key_for_index(changes);
    let start = Instant::now();
    let range_diff = prolly
        .range_diff(&base, &left, &range_start, Some(&range_end))
        .unwrap();
    let range_elapsed = start.elapsed();
    let range_verified = range_diff.len() == changes;
    print_row(BenchRow {
        store: store_name,
        operation: "range_diff_left_window",
        records,
        changes,
        elapsed: range_elapsed,
        diff_count: range_diff.len(),
        verified: range_verified,
        status: "ok",
    });

    let start = Instant::now();
    let merged = prolly.merge(&base, &left, &right, None).unwrap();
    let merge_elapsed = start.elapsed();
    let merge_verified = verify_left_updates(&prolly, &merged, changes)
        && verify_right_updates(&prolly, &merged, records, changes)
        && verify_base_unchanged_sample(&prolly, &merged, records, changes);
    print_row(BenchRow {
        store: store_name,
        operation: "merge_disjoint",
        records,
        changes: changes * 2,
        elapsed: merge_elapsed,
        diff_count: changes * 2,
        verified: merge_verified,
        status: "ok",
    });

    let conflict_changes = changes.min(128);
    let conflict_left = prolly
        .batch(&base, conflict_mutations(conflict_changes, "left-conflict"))
        .unwrap();
    let conflict_right = prolly
        .batch(
            &base,
            conflict_mutations(conflict_changes, "right-conflict"),
        )
        .unwrap();

    let start = Instant::now();
    let conflict_detected = matches!(
        prolly.merge(&base, &conflict_left, &conflict_right, None),
        Err(Error::Conflict(_))
    );
    print_row(BenchRow {
        store: store_name,
        operation: "merge_conflict_detect",
        records,
        changes: conflict_changes,
        elapsed: start.elapsed(),
        diff_count: 1,
        verified: conflict_detected,
        status: "ok",
    });

    let resolver: Resolver = Box::new(|conflict| {
        let mut value = conflict.left.clone().expect("left value");
        value.extend_from_slice(b"+");
        value.extend_from_slice(conflict.right.as_ref().expect("right value"));
        Resolution::value(value)
    });
    let start = Instant::now();
    let resolved = prolly
        .merge(&base, &conflict_left, &conflict_right, Some(resolver))
        .unwrap();
    let resolved_elapsed = start.elapsed();
    let resolved_verified = verify_conflict_resolution(&prolly, &resolved, conflict_changes);
    print_row(BenchRow {
        store: store_name,
        operation: "merge_conflict_resolved",
        records,
        changes: conflict_changes,
        elapsed: resolved_elapsed,
        diff_count: conflict_changes,
        verified: resolved_verified,
        status: "ok",
    });
}

struct BenchRow<'a> {
    store: &'a str,
    operation: &'a str,
    records: usize,
    changes: usize,
    elapsed: Duration,
    diff_count: usize,
    verified: bool,
    status: &'a str,
}

fn print_row(row: BenchRow<'_>) {
    let total_ms = row.elapsed.as_secs_f64() * 1_000.0;
    let items_per_sec = if total_ms > 0.0 {
        row.changes as f64 / (total_ms / 1_000.0)
    } else {
        0.0
    };
    let BenchRow {
        store,
        operation,
        records,
        changes,
        diff_count,
        verified,
        status,
        ..
    } = row;
    println!(
        "{store},{operation},{records},{changes},{total_ms:.3},{items_per_sec:.0},{diff_count},{verified},{status}"
    );
}

fn parse_stages() -> Vec<usize> {
    let raw =
        std::env::var("PROLLY_DIFF_MERGE_STAGES").unwrap_or_else(|_| DEFAULT_STAGES.to_string());
    let mut stages = raw
        .split(',')
        .filter_map(|part| part.trim().parse::<usize>().ok())
        .filter(|items| *items >= 4)
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

fn base_mutations(records: usize) -> Vec<Mutation> {
    (0..records)
        .map(|i| Mutation::Upsert {
            key: key_for_index(i),
            val: base_value_for_index(i),
        })
        .collect()
}

fn left_update_mutations(changes: usize) -> Vec<Mutation> {
    (0..changes)
        .map(|i| Mutation::Upsert {
            key: key_for_index(i),
            val: format!("left-update-{i:012}").into_bytes(),
        })
        .collect()
}

fn right_update_mutations(records: usize, changes: usize) -> Vec<Mutation> {
    let start = records / 2;
    (start..start + changes)
        .map(|i| Mutation::Upsert {
            key: key_for_index(i),
            val: format!("right-update-{i:012}").into_bytes(),
        })
        .collect()
}

fn conflict_mutations(changes: usize, prefix: &str) -> Vec<Mutation> {
    (0..changes)
        .map(|i| Mutation::Upsert {
            key: key_for_index(i),
            val: format!("{prefix}-{i:012}").into_bytes(),
        })
        .collect()
}

fn key_for_index(i: usize) -> Vec<u8> {
    format!("key-{i:012}").into_bytes()
}

fn base_value_for_index(i: usize) -> Vec<u8> {
    format!("base-value-{i:012}").into_bytes()
}

fn verify_base<S: Store>(prolly: &Prolly<S>, tree: &Tree, records: usize) -> bool {
    sample_indices(records).into_iter().all(|idx| {
        prolly
            .get(tree, &key_for_index(idx))
            .ok()
            .flatten()
            .as_deref()
            == Some(base_value_for_index(idx).as_slice())
    })
}

fn verify_left_updates<S: Store>(prolly: &Prolly<S>, tree: &Tree, changes: usize) -> bool {
    sample_indices(changes).into_iter().all(|idx| {
        prolly
            .get(tree, &key_for_index(idx))
            .ok()
            .flatten()
            .as_deref()
            == Some(format!("left-update-{idx:012}").as_bytes())
    })
}

fn verify_right_updates<S: Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    records: usize,
    changes: usize,
) -> bool {
    let start = records / 2;
    sample_indices(changes).into_iter().all(|offset| {
        let idx = start + offset;
        prolly
            .get(tree, &key_for_index(idx))
            .ok()
            .flatten()
            .as_deref()
            == Some(format!("right-update-{idx:012}").as_bytes())
    })
}

fn verify_base_unchanged_sample<S: Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    records: usize,
    changes: usize,
) -> bool {
    let idx = (records - 1).saturating_sub(changes / 2);
    prolly
        .get(tree, &key_for_index(idx))
        .ok()
        .flatten()
        .as_deref()
        == Some(base_value_for_index(idx).as_slice())
}

fn verify_conflict_resolution<S: Store>(prolly: &Prolly<S>, tree: &Tree, changes: usize) -> bool {
    sample_indices(changes).into_iter().all(|idx| {
        prolly
            .get(tree, &key_for_index(idx))
            .ok()
            .flatten()
            .as_deref()
            == Some(format!("left-conflict-{idx:012}+right-conflict-{idx:012}").as_bytes())
    })
}

fn sample_indices(len: usize) -> Vec<usize> {
    if len == 0 {
        return Vec::new();
    }
    let mut indices = vec![0, len / 2, len - 1];
    indices.sort_unstable();
    indices.dedup();
    indices
}

fn bench_config() -> Config {
    Config::builder()
        .min_chunk_size(64)
        .max_chunk_size(512)
        .chunking_factor(256)
        .hash_seed(0xC0DA)
        .build()
}

#[cfg(feature = "sqlite")]
fn durable_sqlite_config() -> SqliteStoreConfig {
    SqliteStoreConfig {
        busy_timeout_ms: 5_000,
        enable_wal: true,
        synchronous_normal: false,
    }
}

#[cfg(feature = "sqlite")]
fn sqlite_path(records: usize) -> PathBuf {
    if let Ok(path) = std::env::var("PROLLY_DIFF_MERGE_SQLITE_DB") {
        return PathBuf::from(path);
    }
    temp_path(format!("crabdb-prolly-diff-merge-sqlite-{records}"), ".db")
}

#[cfg(feature = "sqlite")]
fn sqlite_paths(path: &Path) -> [PathBuf; 3] {
    [
        path.to_path_buf(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ]
}

#[cfg(feature = "sqlite")]
fn remove_sqlite_files(path: &Path) {
    for path in sqlite_paths(path) {
        let _ = std::fs::remove_file(path);
    }
}

#[cfg(feature = "pglite")]
fn pglite_path(records: usize) -> PathBuf {
    if let Ok(path) = std::env::var("PROLLY_DIFF_MERGE_PGLITE_DB") {
        return PathBuf::from(path);
    }
    temp_path(format!("crabdb-prolly-diff-merge-pglite-{records}"), "")
}

#[cfg(feature = "pglite")]
fn node_working_dir() -> Option<PathBuf> {
    std::env::var("PROLLY_PGLITE_NODE_CWD")
        .ok()
        .map(PathBuf::from)
}

#[cfg(feature = "pglite")]
fn remove_pglite_dir(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
}

#[cfg(feature = "slatedb")]
fn slatedb_store_config() -> SlateDbStoreConfig {
    SlateDbStoreConfig {
        flush_after_write: env_bool("PROLLY_SLATEDB_FLUSH_AFTER_WRITE").unwrap_or(true),
        ..SlateDbStoreConfig::default()
    }
}

#[cfg(feature = "slatedb")]
fn build_slatedb_object_store() -> Arc<dyn ObjectStore> {
    let endpoint = std::env::var("PROLLY_SLATEDB_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:9000".to_string());
    let bucket = std::env::var("PROLLY_SLATEDB_BUCKET").unwrap_or_else(|_| "crab".to_string());
    let region = std::env::var("PROLLY_SLATEDB_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let access_key =
        std::env::var("PROLLY_SLATEDB_ACCESS_KEY_ID").unwrap_or_else(|_| "crab".to_string());
    let secret_key =
        std::env::var("PROLLY_SLATEDB_SECRET_ACCESS_KEY").unwrap_or_else(|_| "crab".to_string());
    let allow_http = env_bool("PROLLY_SLATEDB_ALLOW_HTTP").unwrap_or(true);

    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint.trim_end_matches('/'))
            .with_bucket_name(bucket.trim())
            .with_region(region.trim())
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_allow_http(allow_http)
            .with_virtual_hosted_style_request(false)
            .build()
            .unwrap(),
    )
}

#[cfg(feature = "slatedb")]
fn slatedb_path(label: &str, records: usize) -> String {
    if let Ok(path) = std::env::var("PROLLY_DIFF_MERGE_SLATEDB_PATH") {
        return path.trim().trim_matches('/').to_string();
    }
    let prefix = std::env::var("PROLLY_SLATEDB_PATH_PREFIX")
        .unwrap_or_else(|_| "crabdb/prolly-bench".to_string());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{}/{label}-{records}-{}-{nanos}",
        prefix.trim().trim_matches('/'),
        std::process::id()
    )
}

#[cfg(feature = "slatedb")]
fn remove_slatedb_prefix(object_store: Arc<dyn ObjectStore>, path: &str) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
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

#[cfg(feature = "slatedb")]
fn env_bool(name: &str) -> Option<bool> {
    match std::env::var(name).ok()?.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(any(feature = "sqlite", feature = "pglite"))]
fn temp_path(prefix: String, suffix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}{suffix}", std::process::id()))
}
