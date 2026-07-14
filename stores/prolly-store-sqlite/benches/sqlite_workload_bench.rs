mod sqlite_workload_support;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use prolly::{
    BatchBuilder, Config, ManifestStore, Prolly, ProllyMetricsSnapshot, RootManifest,
    SortedBatchBuilder, Tree, TreeStats,
};
use prolly_store_sqlite::{SqliteStore, SqliteStoreConfig};
use rusqlite::Connection;

use sqlite_workload_support::{
    key, shuffled_ids, value, BenchArgs, CsvRow, DurabilityProfile, Workload, RANDOM_SEED,
};

const BASE_ROOT_NAME: &[u8] = b"sqlite-workload-base";

fn main() {
    if let Err(err) = run() {
        eprintln!("sqlite workload benchmark failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = BenchArgs::from_env()?;
    let row = match args.workload {
        Workload::SortedStreamBuild => run_sorted_build(&args)?,
        Workload::ShuffledBatchBuild => run_shuffled_build(&args)?,
        workload => return Err(format!("workload {} is not implemented", workload.as_str())),
    };
    println!("{}", CsvRow::header());
    println!("{}", row.to_csv());
    Ok(())
}

fn run_sorted_build(args: &BenchArgs) -> Result<CsvRow, String> {
    remove_sqlite_files(&args.db_path);
    let db_before = sqlite_files(&args.db_path).total;
    let config = bench_config();
    let store = Arc::new(open_store(&args.db_path, args.profile)?);
    let started = Instant::now();
    let mut builder = SortedBatchBuilder::new(store.clone(), config.clone());
    for id in 0..args.records {
        builder
            .add(key(id), value(id, 0))
            .map_err(|err| err.to_string())?;
    }
    let tree = builder.build().map_err(|err| err.to_string())?;
    store
        .put_root(BASE_ROOT_NAME, &RootManifest::from_tree(&tree))
        .map_err(|err| err.to_string())?;
    let elapsed = started.elapsed();
    finalize_build_row(args, store, tree, elapsed.as_nanos(), db_before)
}

fn run_shuffled_build(args: &BenchArgs) -> Result<CsvRow, String> {
    remove_sqlite_files(&args.db_path);
    let db_before = sqlite_files(&args.db_path).total;
    let config = bench_config();
    let store = Arc::new(open_store(&args.db_path, args.profile)?);
    let started = Instant::now();
    let mut builder = BatchBuilder::new(store.clone(), config.clone());
    for id in shuffled_ids(args.records, RANDOM_SEED) {
        builder.add(key(id), value(id, 0));
    }
    let tree = builder.build().map_err(|err| err.to_string())?;
    store
        .put_root(BASE_ROOT_NAME, &RootManifest::from_tree(&tree))
        .map_err(|err| err.to_string())?;
    let elapsed = started.elapsed();
    finalize_build_row(args, store, tree, elapsed.as_nanos(), db_before)
}

fn finalize_build_row(
    args: &BenchArgs,
    store: Arc<SqliteStore>,
    tree: Tree,
    total_ns: u128,
    db_before: u64,
) -> Result<CsvRow, String> {
    let manager = Prolly::new(store.clone(), tree.config.clone());
    let stats = manager.collect_stats(&tree).map_err(|err| err.to_string())?;
    if stats.total_key_value_pairs != args.records {
        return Err(format!(
            "build cardinality mismatch: expected {}, observed {}",
            args.records, stats.total_key_value_pairs
        ));
    }
    drop(manager);
    drop(store);
    verify_reopened_tree(&args.db_path, args.profile, args.records)?;
    make_row(
        args,
        args.records,
        total_ns,
        ProllyMetricsSnapshot::default(),
        &stats,
        db_before,
    )
}

fn make_row(
    args: &BenchArgs,
    operations: usize,
    total_ns: u128,
    metrics: ProllyMetricsSnapshot,
    stats: &TreeStats,
    db_before: u64,
) -> Result<CsvRow, String> {
    let files = sqlite_files(&args.db_path);
    let (sqlite_node_count, sqlite_node_payload_bytes) = sqlite_node_stats(&args.db_path)?;
    let ns_per_op = if operations == 0 {
        0.0
    } else {
        total_ns as f64 / operations as f64
    };
    let ops_per_sec = if total_ns == 0 {
        0.0
    } else {
        operations as f64 / (total_ns as f64 / 1_000_000_000.0)
    };
    Ok(CsvRow {
        version: args.version.clone(),
        profile: args.profile.as_str().to_string(),
        records: args.records,
        run: args.run,
        workload: args.workload.as_str().to_string(),
        operations,
        total_ns,
        ns_per_op,
        ops_per_sec,
        nodes_read: metrics.nodes_read,
        nodes_written: metrics.nodes_written,
        bytes_read: metrics.bytes_read,
        bytes_written: metrics.bytes_written,
        cache_hits: metrics.node_cache_hits,
        cache_misses: metrics.node_cache_misses,
        cache_evictions: metrics.node_cache_evictions,
        result_entries: stats.total_key_value_pairs,
        num_nodes: stats.num_nodes,
        num_leaves: stats.num_leaves,
        num_internal: stats.num_internal_nodes,
        height: usize::from(stats.tree_height),
        tree_bytes: stats.total_tree_size_bytes,
        db_bytes_before: db_before,
        db_bytes_after: files.db,
        wal_bytes_after: files.wal,
        shm_bytes_after: files.shm,
        fixture_bytes_after: files.total,
        sqlite_node_count,
        sqlite_node_payload_bytes,
        validated: true,
        status: "ok".to_string(),
    })
}

fn verify_reopened_tree(
    path: &Path,
    profile: DurabilityProfile,
    records: usize,
) -> Result<(), String> {
    let store = Arc::new(open_store(path, profile)?);
    let manifest = store
        .get_root(BASE_ROOT_NAME)
        .map_err(|err| err.to_string())?
        .ok_or_else(|| "prepared database is missing the named base root".to_string())?;
    let tree = manifest.to_tree();
    let manager = Prolly::new(store, tree.config.clone());
    for id in [0, records / 2, records - 1] {
        let observed = manager.get(&tree, &key(id)).map_err(|err| err.to_string())?;
        if observed.as_deref() != Some(value(id, 0).as_slice()) {
            return Err(format!("reopen verification failed for record {id}"));
        }
    }
    Ok(())
}

fn bench_config() -> Config {
    Config::builder()
        .min_chunk_size(64)
        .max_chunk_size(512)
        .chunking_factor(256)
        .hash_seed(0xC0DA)
        .build()
}

fn open_store(path: &Path, profile: DurabilityProfile) -> Result<SqliteStore, String> {
    SqliteStore::open_with_config(
        path,
        SqliteStoreConfig {
            busy_timeout_ms: 5_000,
            enable_wal: true,
            synchronous_normal: matches!(profile, DurabilityProfile::Normal),
        },
    )
    .map_err(|err| err.to_string())
}

#[derive(Clone, Copy, Debug, Default)]
struct SqliteFiles {
    db: u64,
    wal: u64,
    shm: u64,
    total: u64,
}

fn sqlite_files(path: &Path) -> SqliteFiles {
    let size = |path: &Path| std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0);
    let wal_path = PathBuf::from(format!("{}-wal", path.display()));
    let shm_path = PathBuf::from(format!("{}-shm", path.display()));
    let db = size(path);
    let wal = size(&wal_path);
    let shm = size(&shm_path);
    SqliteFiles {
        db,
        wal,
        shm,
        total: db.saturating_add(wal).saturating_add(shm),
    }
}

fn sqlite_node_stats(path: &Path) -> Result<(u64, u64), String> {
    let connection = Connection::open(path).map_err(|err| err.to_string())?;
    connection
        .query_row(
            "SELECT count(*), COALESCE(sum(length(node)), 0) FROM prolly_nodes",
            [],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
        )
        .map_err(|err| err.to_string())
}

fn remove_sqlite_files(path: &Path) {
    for path in [
        path.to_path_buf(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ] {
        let _ = std::fs::remove_file(path);
    }
}
