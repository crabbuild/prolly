use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use prolly::{
    compare_and_swap_named_content_root_async, load_named_content_root_async,
    put_named_content_root_async, AsyncProximityHead, AsyncProximityHeadCommit, AsyncProximityMap,
    AsyncSearchControl, BuildParallelism, ContentGraphLimits, ContentManifestUpdate,
    ContentObjectKind, ContentRootManifest, ProximityConfig, ProximityMap, ProximityMutation,
    ProximityRecord, ScalarQuantizationConfig, SearchIo, SearchRequest, SearchRuntime,
    TypedContentRoot,
};
use prolly_store_postgres::{PostgresBackend, PostgresStore};
use sqlx::Row;

const DIMENSIONS: usize = 1_836;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn records(count: usize) -> Vec<ProximityRecord> {
    (0..count)
        .map(|index| ProximityRecord {
            key: format!("embedding-{index:08}").into_bytes(),
            vector: vector(index),
            value: format!("document-{index:08}").into_bytes(),
        })
        .collect()
}

fn vector(index: usize) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|component| {
            let mixed = index
                .wrapping_mul(1_000_003)
                .wrapping_add(component.wrapping_mul(97_409));
            ((mixed % 20_003) as f32 - 10_001.0) / 1_000.0
        })
        .collect()
}

fn config() -> ProximityConfig {
    let mut config = ProximityConfig::new(DIMENSIONS as u32);
    config.hierarchy.log_chunk_size = 3;
    config.hierarchy.level_hash_seed = 42;
    config.overflow.min_page_bytes = 16 * 1024;
    config.overflow.target_page_bytes = 64 * 1024;
    config.overflow.max_page_bytes = 256 * 1024;
    // Exercise the remote-object path used when embedding vectors are larger
    // than the desired PRXN page payload.
    config.vector_storage.inline_threshold_bytes = 4 * 1024;
    config.scalar_quantization = Some(ScalarQuantizationConfig { group_size: 32 });
    config
}

async fn clear(backend: &PostgresBackend) {
    sqlx::query("TRUNCATE prolly_roots, prolly_hints, prolly_nodes")
        .execute(backend.pool())
        .await
        .unwrap();
}

async fn reset_sql_stats(backend: &PostgresBackend) {
    sqlx::query("SELECT pg_stat_statements_reset()")
        .execute(backend.pool())
        .await
        .unwrap();
}

async fn report(backend: &PostgresBackend, operation: &str, elapsed: Duration, samples: usize) {
    let row = sqlx::query(
        "SELECT COALESCE(sum(calls), 0)::bigint AS calls, \
                COALESCE(sum(total_exec_time), 0)::double precision AS exec_ms \
         FROM pg_stat_statements \
         WHERE query LIKE '%prolly_%' AND query NOT LIKE '%pg_stat_statements%'",
    )
    .fetch_one(backend.pool())
    .await
    .unwrap();
    let calls: i64 = row.try_get("calls").unwrap();
    let exec_ms: f64 = row.try_get("exec_ms").unwrap();
    println!(
        "PROXIMITY_PERF operation={operation} dimensions={DIMENSIONS} samples={samples} \
         client_ms={:.3} per_sample_ms={:.3} sql_calls={calls} postgres_exec_ms={exec_ms:.3}",
        elapsed.as_secs_f64() * 1_000.0,
        elapsed.as_secs_f64() * 1_000.0 / samples.max(1) as f64,
    );
}

async fn measured<F, T>(backend: &PostgresBackend, operation: &str, samples: usize, future: F) -> T
where
    F: Future<Output = T>,
{
    reset_sql_stats(backend).await;
    let started = Instant::now();
    let value = future.await;
    report(backend, operation, started.elapsed(), samples).await;
    value
}

#[test]
fn postgres_async_proximity_1836_end_to_end_and_performance() {
    let Some(database_url) = std::env::var("PROLLY_STORE_POSTGRES_URL").ok() else {
        return;
    };
    let record_count = env_usize("PROLLY_POSTGRES_PROXIMITY_RECORDS", 256);
    let search_samples = env_usize("PROLLY_POSTGRES_PROXIMITY_SEARCH_SAMPLES", 10);
    assert!(record_count >= 32);
    assert!(search_samples > 0);

    runtime().block_on(async {
        let backend = PostgresBackend::connect(&database_url).await.unwrap();
        backend.initialize_schema().await.unwrap();
        sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
            .execute(backend.pool())
            .await
            .unwrap();
        clear(&backend).await;

        let source = records(record_count);
        let oracle =
            ProximityMap::build(Arc::new(prolly::MemStore::new()), config(), source.clone())
                .unwrap();
        let store = PostgresStore::new(backend.clone());
        let (map, build_stats) = measured(&backend, "build", 1, async {
            AsyncProximityMap::build_with_parallelism(
                store.clone(),
                config(),
                source,
                BuildParallelism::new(4).unwrap(),
            )
            .await
            .unwrap()
        })
        .await;
        assert_eq!(map.tree(), oracle.tree());
        assert_eq!(map.tree().config.dimensions, DIMENSIONS as u32);
        assert_eq!(map.tree().count, record_count as u64);
        assert!(build_stats.proximity_objects_written > 0);

        let verification = measured(&backend, "verify", 1, map.verify()).await.unwrap();
        assert_eq!(verification.record_count, record_count as u64);
        assert!(verification.external_vector_count > 0);
        assert!(verification.scalar_quantizer_count > 0);
        assert!(verification.maximum_node_bytes <= config().overflow.max_page_bytes as usize);

        let descriptor = map.tree().descriptor.clone();
        let read_map = AsyncProximityMap::load(store.clone(), descriptor.clone())
            .await
            .unwrap();
        let key = format!("embedding-{:08}", record_count / 3).into_bytes();
        let exact = measured(&backend, "get_cold", 1, read_map.get(&key))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exact.0.len(), DIMENSIONS);
        assert_eq!(
            exact.1,
            format!("document-{:08}", record_count / 3).into_bytes()
        );

        let warm_exact = measured(&backend, "get_warm", 1, read_map.get(&key))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(warm_exact, exact);

        let scan_map = AsyncProximityMap::load(store.clone(), descriptor.clone())
            .await
            .unwrap();
        let scanned = measured(&backend, "scan_cold", 1, scan_map.scan_records(|_, _| {}))
            .await
            .unwrap();
        assert_eq!(scanned, record_count as u64);

        let query = vector(record_count / 3);
        let expected = oracle.search(SearchRequest::exact(&query, 10)).unwrap();
        let runtime_store = SearchIo::new(store.clone(), Arc::new(SearchRuntime::default()));
        let runtime_map = AsyncProximityMap::load_with_search_io(runtime_store, descriptor.clone())
            .await
            .unwrap();
        let cold = measured(
            &backend,
            "search_cold",
            1,
            runtime_map.search_with_runtime(
                SearchRequest::exact(&query, 10),
                AsyncSearchControl::default(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(cold.neighbors, expected.neighbors);
        assert!(cold.stats.physical_bytes_read > 0);

        reset_sql_stats(&backend).await;
        let started = Instant::now();
        for _ in 0..search_samples {
            let warm = runtime_map
                .search_with_runtime(
                    SearchRequest::exact(&query, 10),
                    AsyncSearchControl::default(),
                )
                .await
                .unwrap();
            assert_eq!(warm.neighbors, expected.neighbors);
            assert_eq!(warm.stats.physical_bytes_read, 0);
        }
        report(&backend, "search_warm", started.elapsed(), search_samples).await;

        let membership = measured(&backend, "membership_proof", 1, map.prove_membership(&key))
            .await
            .unwrap();
        assert!(membership
            .verify_for(&map.tree().descriptor)
            .unwrap()
            .record
            .is_some());

        let limits = ContentGraphLimits::default();
        let structural = measured(
            &backend,
            "structural_proof",
            1,
            map.prove_structure(&limits),
        )
        .await
        .unwrap();
        assert_eq!(
            structural
                .verify_for(&map.tree().descriptor, &limits)
                .unwrap()
                .summary,
            verification
        );

        let search_proof = measured(
            &backend,
            "search_proof",
            1,
            map.prove_search(SearchRequest::exact(&query, 10), &limits),
        )
        .await
        .unwrap();
        assert_eq!(
            search_proof
                .verify_for_source(&map.tree().descriptor, &limits)
                .unwrap()
                .result
                .neighbors,
            expected.neighbors
        );

        let named = ContentRootManifest {
            root: TypedContentRoot::new(ContentObjectKind::ProximityDescriptor, descriptor.clone()),
            logical_version: 1,
            created_at_millis: 1,
            metadata: BTreeMap::new(),
        };
        let published = measured(
            &backend,
            "named_root_publish",
            1,
            put_named_content_root_async(&store, b"proximity/e2e", named.clone()),
        )
        .await
        .unwrap();
        let loaded_named = measured(
            &backend,
            "named_root_load",
            1,
            load_named_content_root_async(&store, b"proximity/e2e"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(loaded_named, published);
        let mut next_named = named;
        next_named.logical_version = 2;
        assert!(matches!(
            measured(
                &backend,
                "named_root_cas",
                1,
                compare_and_swap_named_content_root_async(
                    &store,
                    b"proximity/e2e",
                    Some(&published.manifest_cid),
                    next_named,
                ),
            )
            .await
            .unwrap(),
            ContentManifestUpdate::Applied(_)
        ));

        let managed = AsyncProximityHead::new(store.clone(), b"proximity/managed".to_vec());
        let managed_publication = measured(
            &backend,
            "managed_head_publish",
            1,
            managed.publish_descriptor_if_absent(descriptor.clone(), 1, 2, BTreeMap::new()),
        )
        .await
        .unwrap();
        assert!(matches!(
            managed_publication,
            AsyncProximityHeadCommit::Applied { .. }
        ));
        let managed_snapshot = measured(&backend, "managed_head_open_cached", 1, managed.open())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(managed_snapshot.map().tree().descriptor, descriptor);
        let managed_key = format!("embedding-{:08}", record_count / 2).into_bytes();
        let managed_update = measured(
            &backend,
            "managed_head_mutate_vector",
            1,
            managed.mutate_with_retry(
                [ProximityMutation {
                    key: managed_key,
                    value: Some((vector(record_count + 11), b"managed-vector-update".to_vec())),
                }],
                3,
                BTreeMap::new(),
            ),
        )
        .await
        .unwrap();
        let AsyncProximityHeadCommit::Applied { stats, .. } = managed_update else {
            panic!("uncontended managed PostgreSQL mutation conflicted");
        };
        assert!(!stats.full_proximity_rebuild);

        let same_vector = vector(record_count / 3);
        let (value_map, value_stats) = measured(
            &backend,
            "mutate_value",
            1,
            map.mutate_batch([ProximityMutation {
                key: key.clone(),
                value: Some((same_vector, b"value-only-update".to_vec())),
            }]),
        )
        .await
        .unwrap();
        assert!(!value_stats.full_proximity_rebuild);
        assert_eq!(value_map.tree().proximity_root, map.tree().proximity_root);

        let moved_vector = vector(record_count + 7);
        let mutation = ProximityMutation {
            key: key.clone(),
            value: Some((moved_vector, b"vector-update".to_vec())),
        };
        let (mutated, mutation_stats) = measured(
            &backend,
            "mutate_vector",
            1,
            value_map.mutate_batch([mutation.clone()]),
        )
        .await
        .unwrap();
        assert!(!mutation_stats.full_proximity_rebuild);
        assert!(mutation_stats.records_rebuilt < record_count);
        let rebuilt = measured(
            &backend,
            "rebuild_oracle",
            1,
            value_map.rebuild_batch([mutation]),
        )
        .await
        .unwrap();
        assert_eq!(mutated.tree(), rebuilt.tree());

        let reopened = measured(
            &backend,
            "reopen",
            1,
            AsyncProximityMap::load(store, mutated.tree().descriptor.clone()),
        )
        .await
        .unwrap();
        reopened.verify().await.unwrap();
        assert_eq!(
            reopened.get(&key).await.unwrap().unwrap().1,
            b"vector-update"
        );

        let node_row = sqlx::query(
            "SELECT count(*)::bigint AS objects, \
                    COALESCE(sum(octet_length(node)), 0)::bigint AS bytes \
             FROM prolly_nodes",
        )
        .fetch_one(backend.pool())
        .await
        .unwrap();
        let objects: i64 = node_row.try_get("objects").unwrap();
        let bytes: i64 = node_row.try_get("bytes").unwrap();
        println!(
            "PROXIMITY_STORAGE dimensions={DIMENSIONS} records={record_count} \
             objects={objects} bytes={bytes}"
        );
    });
}
