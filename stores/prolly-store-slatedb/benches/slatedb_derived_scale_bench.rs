use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use prolly::{
    BuildParallelism, Config, DistanceMetric, Prolly, ProximityConfig, ProximityMap,
    ProximityMutation, ProximityRecord, SearchRequest, SecondaryIndex, SecondaryIndexRegistry,
};
use prolly_store_slatedb::{SlateDbStore, SlateDbStoreConfig};
use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::path::Path as ObjectPath;
use slatedb::object_store::{ObjectStore, ObjectStoreExt};

const DEFAULT_RECORDS: usize = 100_000;
const DEFAULT_CHANGES: usize = 10_000;
const DEFAULT_DIMENSIONS: usize = 16;
const DEFAULT_BUILD_THREADS: usize = 8;

fn main() {
    let kind = env_string("PROLLY_SLATEDB_DERIVED_KIND", "all");
    let records = env_usize("PROLLY_SLATEDB_DERIVED_RECORDS", DEFAULT_RECORDS).max(1_000);
    let changes =
        env_usize("PROLLY_SLATEDB_DERIVED_CHANGES", DEFAULT_CHANGES).clamp(1, records / 2);
    let dimensions = env_usize("PROLLY_SLATEDB_DERIVED_DIMENSIONS", DEFAULT_DIMENSIONS).max(1);
    let build_threads = env_usize(
        "PROLLY_SLATEDB_DERIVED_BUILD_THREADS",
        DEFAULT_BUILD_THREADS,
    )
    .max(1);
    let path = benchmark_path();
    let keep_data = env_bool("PROLLY_SLATEDB_DERIVED_KEEP_DATA").unwrap_or(false);
    let object_store = build_object_store();

    println!("SlateDB derived-map scale benchmark");
    println!("kind={kind}");
    println!("path={path}");
    println!("records={records}");
    println!("changes={changes}");
    println!("dimensions={dimensions}");
    println!("build_threads={build_threads}");
    println!("flush_policy=one explicit durability flush per measured write stage");
    println!("kind,operation,records,items,total_ms,items_per_sec,objects,bytes,verified");

    match kind.as_str() {
        "index" => run_index_workload(
            object_store.clone(),
            &format!("{path}/index"),
            records,
            changes,
            keep_data,
        ),
        "proximity" => run_proximity_workload(
            object_store.clone(),
            &format!("{path}/proximity"),
            records,
            changes,
            dimensions,
            build_threads,
            keep_data,
        ),
        "all" => {
            run_index_workload(
                object_store.clone(),
                &format!("{path}/index"),
                records,
                changes,
                keep_data,
            );
            run_proximity_workload(
                object_store,
                &format!("{path}/proximity"),
                records,
                changes,
                dimensions,
                build_threads,
                keep_data,
            );
        }
        other => panic!(
            "invalid PROLLY_SLATEDB_DERIVED_KIND {other:?}; expected all, index, or proximity"
        ),
    }
}

fn run_index_workload(
    object_store: Arc<dyn ObjectStore>,
    path: &str,
    records: usize,
    changes: usize,
    keep_data: bool,
) {
    remove_prefix(object_store.clone(), path);
    let context = RowContext::new(&object_store, path, "index", records);
    {
        let registry = index_registry();
        let store = Arc::new(open_store(path, object_store.clone()));
        let engine = Prolly::new(store.clone(), Config::default());
        let source = engine.versioned_map(b"users");

        timed(&context, "source_build", records, || {
            source
                .edit(|edit| {
                    for id in 0..records {
                        edit.put(key(id), value(id, 0));
                    }
                })
                .unwrap();
            store.flush().unwrap();
            source.get(&key(records - 1)).unwrap().is_some()
        });

        let indexed = engine.indexed_map(b"users", registry.clone()).unwrap();
        timed(&context, "index_build", records, || {
            indexed.ensure_index(b"by-status").unwrap();
            store.flush().unwrap();
            indexed.health().unwrap().active_indexes.len() == 1
        });

        let random = random_indices(records, changes, 0x61c8_8646_80b5_83eb);
        timed(&context, "random_update", changes, || {
            indexed
                .edit(|edit| {
                    for &id in &random {
                        edit.put(key(id), value(id, 1));
                    }
                })
                .unwrap();
            store.flush().unwrap();
            indexed.get(&key(random[0])).unwrap() == Some(value(random[0], 1))
        });

        let cluster_start = records / 2 - changes / 2;
        timed(&context, "clustered_update", changes, || {
            indexed
                .edit(|edit| {
                    for id in cluster_start..cluster_start + changes {
                        edit.put(key(id), value(id, 2));
                    }
                })
                .unwrap();
            store.flush().unwrap();
            indexed.get(&key(cluster_start)).unwrap() == Some(value(cluster_start, 2))
        });

        let spread = spread_indices(records, changes);
        timed(&context, "spread_batch_update", changes, || {
            indexed
                .edit(|edit| {
                    for &id in &spread {
                        edit.put(key(id), value(id, 3));
                    }
                })
                .unwrap();
            store.flush().unwrap();
            indexed.get(&key(spread[changes - 1])).unwrap() == Some(value(spread[changes - 1], 3))
        });

        timed(&context, "index_query_exact", 1, || {
            let snapshot = indexed.snapshot().unwrap();
            !snapshot
                .index(b"by-status")
                .unwrap()
                .exact(b"status-03")
                .unwrap()
                .is_empty()
        });

        timed(&context, "verify_all", records, || {
            let snapshot = indexed.snapshot().unwrap();
            let checks = indexed.verify_all(&snapshot.id().source_version).unwrap();
            checks.len() == 1 && checks.iter().all(prolly::IndexVerification::is_valid)
        });

        drop(indexed);
        drop(source);
        drop(engine);
        drop(store);

        timed(&context, "reopen_verify", records, || {
            let reopened_store = Arc::new(open_store(path, object_store.clone()));
            let reopened = Prolly::new(reopened_store, Config::default());
            let indexed = reopened.indexed_map(b"users", registry).unwrap();
            let health = indexed.health().unwrap();
            let snapshot = indexed.snapshot().unwrap();
            let checks = indexed.verify_all(&snapshot.id().source_version).unwrap();
            health.active_indexes.len() == 1
                && checks.len() == 1
                && checks.iter().all(prolly::IndexVerification::is_valid)
                && indexed.get(&key(spread[0])).unwrap() == Some(value(spread[0], 3))
        });
    }
    if !keep_data {
        remove_prefix(object_store, path);
    }
}

fn run_proximity_workload(
    object_store: Arc<dyn ObjectStore>,
    path: &str,
    records: usize,
    changes: usize,
    dimensions: usize,
    build_threads: usize,
    keep_data: bool,
) {
    remove_prefix(object_store.clone(), path);
    let context = RowContext::new(&object_store, path, "proximity", records);
    {
        let store = Arc::new(open_store(path, object_store.clone()));
        let mut config = ProximityConfig::new(dimensions as u32);
        config.metric = DistanceMetric::L2Squared;
        config.hierarchy.level_hash_seed = 42;
        let input = (0..records)
            .map(|id| ProximityRecord {
                key: key(id),
                vector: vector(id, 0, dimensions),
                value: value(id, 0),
            })
            .collect::<Vec<_>>();

        let started = Instant::now();
        let (mut map, build_stats) = ProximityMap::build_with_parallelism(
            store.clone(),
            config,
            input,
            BuildParallelism::new(build_threads).unwrap(),
        )
        .unwrap();
        store.flush().unwrap();
        row(
            &context,
            "build",
            records,
            started,
            map.tree().count as usize == records
                && build_stats.proximity_objects_written > 0
                && map.get(&key(records - 1)).unwrap().is_some(),
        );

        timed(&context, "search_exact_100", 100, || {
            for query_id in 0..100 {
                let query = vector(query_id * records / 100, 0, dimensions);
                let result = map.search(SearchRequest::exact(&query, 10)).unwrap();
                if result.neighbors.len() != 10 {
                    return false;
                }
            }
            true
        });

        let random = random_indices(records, changes, 0xdea1_106c_94f5_7b21);
        let started = Instant::now();
        let (next, random_stats) = map
            .mutate_batch(random.iter().map(|&id| ProximityMutation {
                key: key(id),
                value: Some((vector(id, 1, dimensions), value(id, 1))),
            }))
            .unwrap();
        map = next;
        store.flush().unwrap();
        row(
            &context,
            "random_update",
            changes,
            started,
            random_stats.records_rebuilt > 0
                && map.get(&key(random[0])).unwrap().unwrap().1 == value(random[0], 1),
        );

        let cluster_start = records / 2 - changes / 2;
        let started = Instant::now();
        let (next, cluster_stats) = map
            .mutate_batch(
                (cluster_start..cluster_start + changes).map(|id| ProximityMutation {
                    key: key(id),
                    value: Some((vector(id, 2, dimensions), value(id, 2))),
                }),
            )
            .unwrap();
        map = next;
        store.flush().unwrap();
        row(
            &context,
            "clustered_update",
            changes,
            started,
            cluster_stats.records_rebuilt > 0
                && map.get(&key(cluster_start)).unwrap().unwrap().1 == value(cluster_start, 2),
        );

        let spread = spread_indices(records, changes);
        let started = Instant::now();
        let (next, spread_stats) = map
            .mutate_batch(spread.iter().map(|&id| ProximityMutation {
                key: key(id),
                value: Some((vector(id, 3, dimensions), value(id, 3))),
            }))
            .unwrap();
        map = next;
        store.flush().unwrap();
        row(
            &context,
            "spread_batch_update",
            changes,
            started,
            spread_stats.records_rebuilt > 0
                && map.get(&key(spread[changes - 1])).unwrap().unwrap().1
                    == value(spread[changes - 1], 3),
        );

        timed(&context, "verify", records, || {
            map.verify().unwrap().record_count as usize == records
        });

        let descriptor = map.tree().descriptor.clone();
        drop(map);
        drop(store);
        timed(&context, "reopen_verify_search", records, || {
            let reopened_store = Arc::new(open_store(path, object_store.clone()));
            let reopened = ProximityMap::load(reopened_store, descriptor).unwrap();
            let verification = reopened.verify().unwrap();
            let query = vector(spread[0], 3, dimensions);
            let search = reopened.search(SearchRequest::exact(&query, 10)).unwrap();
            verification.record_count as usize == records
                && search.neighbors.len() == 10
                && reopened.get(&key(spread[0])).unwrap().unwrap().1 == value(spread[0], 3)
        });
    }
    if !keep_data {
        remove_prefix(object_store, path);
    }
}

struct RowContext<'a> {
    object_store: &'a Arc<dyn ObjectStore>,
    path: &'a str,
    kind: &'a str,
    records: usize,
}

impl<'a> RowContext<'a> {
    fn new(
        object_store: &'a Arc<dyn ObjectStore>,
        path: &'a str,
        kind: &'a str,
        records: usize,
    ) -> Self {
        Self {
            object_store,
            path,
            kind,
            records,
        }
    }
}

fn timed(context: &RowContext<'_>, operation: &str, items: usize, action: impl FnOnce() -> bool) {
    let started = Instant::now();
    let verified = action();
    row(context, operation, items, started, verified);
}

fn row(context: &RowContext<'_>, operation: &str, items: usize, started: Instant, verified: bool) {
    let elapsed = started.elapsed();
    let (objects, bytes) = object_stats(context.object_store.clone(), context.path);
    println!(
        "{},{operation},{},{items},{:.3},{:.0},{objects},{bytes},{verified}",
        context.kind,
        context.records,
        elapsed.as_secs_f64() * 1000.0,
        items as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
    );
    assert!(
        verified,
        "verification failed for {}/{operation}",
        context.kind
    );
}

fn open_store(path: &str, object_store: Arc<dyn ObjectStore>) -> SlateDbStore {
    let config = SlateDbStoreConfig {
        flush_after_write: false,
        ..Default::default()
    };
    SlateDbStore::open_with_config(path.to_owned(), object_store, config).unwrap()
}

fn index_registry() -> SecondaryIndexRegistry {
    SecondaryIndexRegistry::new()
        .register(
            SecondaryIndex::non_unique("by-status", 1, "bench.by-status/v1", |_, value| {
                Ok(vec![value[..9.min(value.len())].to_vec()])
            })
            .unwrap(),
        )
        .unwrap()
}

fn key(id: usize) -> Vec<u8> {
    format!("user-{id:012}").into_bytes()
}

fn value(id: usize, generation: usize) -> Vec<u8> {
    format!(
        "status-{:02}|generation-{generation}|user-{id:012}",
        (id + generation) % 10
    )
    .into_bytes()
}

fn vector(id: usize, generation: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|component| {
            let mixed = id
                .wrapping_mul(1_000_003)
                .wrapping_add(component.wrapping_mul(97_409))
                .wrapping_add(generation.wrapping_mul(65_537));
            ((mixed % 20_003) as f32 - 10_001.0) / 1_000.0
        })
        .collect()
}

fn spread_indices(records: usize, changes: usize) -> Vec<usize> {
    (0..changes)
        .map(|index| index * records / changes)
        .collect()
}

fn random_indices(records: usize, changes: usize, seed: u64) -> Vec<usize> {
    let mut selected = BTreeSet::new();
    let mut state = seed;
    while selected.len() < changes {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        selected.insert((state as usize) % records);
    }
    selected.into_iter().collect()
}

fn build_object_store() -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(env_string(
                "PROLLY_SLATEDB_ENDPOINT",
                "http://127.0.0.1:9000",
            ))
            .with_bucket_name(env_string("PROLLY_SLATEDB_BUCKET", "crab"))
            .with_region(env_string("PROLLY_SLATEDB_REGION", "us-east-1"))
            .with_access_key_id(env_string("PROLLY_SLATEDB_ACCESS_KEY_ID", "crab"))
            .with_secret_access_key(env_string("PROLLY_SLATEDB_SECRET_ACCESS_KEY", "crab"))
            .with_allow_http(env_bool("PROLLY_SLATEDB_ALLOW_HTTP").unwrap_or(true))
            .with_virtual_hosted_style_request(false)
            .build()
            .unwrap(),
    )
}

fn object_stats(object_store: Arc<dyn ObjectStore>, path: &str) -> (usize, u64) {
    runtime().block_on(async move {
        let mut count = 0;
        let mut bytes = 0;
        let mut list = object_store.list(Some(&ObjectPath::from(path)));
        while let Some(meta) = list.next().await.transpose().unwrap() {
            count += 1;
            bytes += meta.size;
        }
        (count, bytes)
    })
}

fn remove_prefix(object_store: Arc<dyn ObjectStore>, path: &str) {
    runtime().block_on(async move {
        let mut locations = Vec::new();
        let mut list = object_store.list(Some(&ObjectPath::from(path)));
        while let Some(meta) = list.next().await.transpose().unwrap() {
            locations.push(meta.location);
        }
        drop(list);
        for location in locations {
            object_store.delete(&location).await.unwrap();
        }
    });
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("prolly-slatedb-derived-bench")
        .enable_all()
        .build()
        .unwrap()
}

fn benchmark_path() -> String {
    if let Ok(path) = std::env::var("PROLLY_SLATEDB_DERIVED_PATH") {
        return path.trim().trim_matches('/').to_owned();
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("perf/derived-{}-{nanos}", std::process::id())
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str) -> Option<bool> {
    match std::env::var(name).ok()?.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}
