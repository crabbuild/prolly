use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use prolly::{append_batch, Config, ManifestStore, Mutation, Prolly, Store};
use prolly_store_redb::{Durability, RedbStore, RedbStoreConfig, RedbStoreOptions};
use prolly_store_sqlite::{SqliteStore, SqliteStoreConfig};

const RECORDS: usize = 1_000_000;
const BUILD_BATCH: usize = 50_000;
const READS: usize = 10_000;
const UPDATES: usize = 10_000;
const ROOT: &[u8] = b"perf/main";

fn main() {
    let mut args = std::env::args().skip(1);
    let adapter = args.next().expect("adapter: redb or sqlite");
    let repetition = args
        .next()
        .expect("repetition")
        .parse::<usize>()
        .expect("numeric repetition");
    let base_dir = std::env::var_os("PROLLY_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("prolly-redb-sqlite-1m"));
    std::fs::create_dir_all(&base_dir).expect("create benchmark directory");
    let base = base_dir.join(format!("{adapter}-{repetition}.db"));

    match adapter.as_str() {
        "redb" => {
            cleanup_redb(&base);
            let result = run(
                "redb",
                repetition,
                &base,
                |path| {
                    RedbStore::open_with_options(
                        path,
                        RedbStoreOptions {
                            database: RedbStoreConfig {
                                cache_size_bytes: 192 * 1024 * 1024,
                                durability: Durability::Immediate,
                            },
                            node_read_cache_size_bytes: 0,
                            compress_nodes: true,
                        },
                    )
                    .unwrap()
                },
                redb_bytes,
            );
            println!("{result}");
            if std::env::var_os("KEEP_DB").is_none() {
                cleanup_redb(&base);
            }
        }
        "sqlite" => {
            cleanup_sqlite(&base);
            let result = run(
                "sqlite",
                repetition,
                &base,
                |path| SqliteStore::open_with_config(path, sqlite_config()).unwrap(),
                sqlite_bytes,
            );
            println!("{result}");
            if std::env::var_os("KEEP_DB").is_none() {
                cleanup_sqlite(&base);
            }
        }
        other => panic!("unknown adapter {other}"),
    }
}

fn run<S, O, B>(adapter: &str, repetition: usize, path: &Path, open: O, database_bytes: B) -> String
where
    S: Store + ManifestStore + Send + Sync + 'static,
    O: Fn(&Path) -> S,
    B: Fn(&Path) -> u64,
{
    let config = tree_config();
    let store = Arc::new(open(path));
    let manager = Prolly::new(store.clone(), config.clone());
    let mut tree = manager.create();
    let build_started = Instant::now();
    for start in (0..RECORDS).step_by(BUILD_BATCH) {
        let count = (RECORDS - start).min(BUILD_BATCH);
        tree = append_batch(&manager, &tree, append_mutations(start, count)).unwrap();
        if (start + count).is_multiple_of(100_000) {
            eprintln!("progress,{adapter},{repetition},{}", start + count);
        }
    }
    manager.publish_named_root(ROOT, &tree).unwrap();
    let build_seconds = build_started.elapsed().as_secs_f64();
    let expected_root = tree.root;
    drop(manager);
    drop(store);

    let build_db_bytes = database_bytes(path);
    let store = Arc::new(open(path));
    let manager = Prolly::new(store.clone(), config);
    let tree = manager
        .load_named_root(ROOT)
        .unwrap()
        .expect("published root");
    assert_eq!(tree.root, expected_root);
    assert_eq!(manager.len(&tree).unwrap() as usize, RECORDS);

    manager.clear_cache();
    let read_ids = deterministic_ids(READS, RECORDS, 0x7e57_1a5e);
    let read_started = Instant::now();
    let mut read_checksum = 0u64;
    for id in &read_ids {
        let value = manager
            .get(&tree, &key_for_index(*id))
            .unwrap()
            .expect("random read value");
        assert_eq!(value, value_for_index(*id, 0));
        read_checksum = read_checksum.wrapping_add(u64::from(value[0]));
    }
    let read_seconds = read_started.elapsed().as_secs_f64();

    manager.clear_cache();
    let scan_started = Instant::now();
    let mut scan_count = 0usize;
    let mut scan_checksum = 0u64;
    for item in manager.range(&tree, &[], None).unwrap() {
        let (key, value) = item.unwrap();
        scan_count += 1;
        scan_checksum = scan_checksum
            .wrapping_add(key.len() as u64)
            .wrapping_add(value.len() as u64);
    }
    let scan_seconds = scan_started.elapsed().as_secs_f64();
    assert_eq!(scan_count, RECORDS);

    manager.clear_cache();
    let update_ids = deterministic_ids(UPDATES, RECORDS, 0xb47c_4a11);
    let updates = update_ids
        .iter()
        .map(|id| Mutation::Upsert {
            key: key_for_index(*id),
            val: value_for_index(*id, 1),
        })
        .collect::<Vec<_>>();
    let update_started = Instant::now();
    let updated = manager.batch(&tree, updates).unwrap();
    manager.publish_named_root(ROOT, &updated).unwrap();
    let update_seconds = update_started.elapsed().as_secs_f64();
    assert_eq!(manager.len(&updated).unwrap() as usize, RECORDS);
    for id in [
        update_ids[0],
        update_ids[UPDATES / 2],
        update_ids[UPDATES - 1],
    ] {
        assert_eq!(
            manager.get(&updated, &key_for_index(id)).unwrap(),
            Some(value_for_index(id, 1))
        );
    }
    drop(manager);
    drop(store);
    let final_db_bytes = database_bytes(path);

    format!(
        "RESULT,{adapter},{repetition},{RECORDS},{build_seconds:.6},{:.0},{build_db_bytes},{:.2},{READS},{read_seconds:.6},{:.0},{scan_count},{scan_seconds:.6},{:.0},{UPDATES},{update_seconds:.6},{:.0},{final_db_bytes},{read_checksum},{scan_checksum}",
        RECORDS as f64 / build_seconds,
        build_db_bytes as f64 / RECORDS as f64,
        READS as f64 / read_seconds,
        scan_count as f64 / scan_seconds,
        UPDATES as f64 / update_seconds,
    )
}

fn tree_config() -> Config {
    Config::builder()
        .min_chunk_size(64)
        .max_chunk_size(512)
        .chunking_factor(256)
        .hash_seed(0xC0DA)
        .build()
}

// Start from adapter defaults so newly added, unrelated tuning fields remain
// forward-compatible with this benchmark.
#[allow(clippy::field_reassign_with_default)]
fn sqlite_config() -> SqliteStoreConfig {
    let mut config = SqliteStoreConfig::default();
    config.busy_timeout_ms = 5_000;
    config.enable_wal = true;
    config.synchronous_normal = false;
    config
}

fn append_mutations(start: usize, count: usize) -> Vec<Mutation> {
    (start..start + count)
        .map(|id| Mutation::Upsert {
            key: key_for_index(id),
            val: value_for_index(id, 0),
        })
        .collect()
}

fn deterministic_ids(count: usize, ceiling: usize, seed: u64) -> Vec<usize> {
    let mut state = seed;
    (0..count)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state as usize) % ceiling
        })
        .collect()
}

fn key_for_index(id: usize) -> Vec<u8> {
    format!("key-{id:012}").into_bytes()
}

fn value_for_index(id: usize, generation: u8) -> Vec<u8> {
    format!("value-{id:012}-g{generation}-payload").into_bytes()
}

fn redb_bytes(path: &Path) -> u64 {
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn sqlite_bytes(path: &Path) -> u64 {
    sqlite_paths(path)
        .iter()
        .filter_map(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn sqlite_paths(path: &Path) -> [PathBuf; 3] {
    [
        path.to_path_buf(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ]
}

fn cleanup_redb(path: &Path) {
    let _ = std::fs::remove_file(path);
}

fn cleanup_sqlite(path: &Path) {
    for path in sqlite_paths(path) {
        let _ = std::fs::remove_file(path);
    }
}
