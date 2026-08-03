fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn env_var(primary: &str, legacy: &str) -> Option<String> {
    std::env::var(primary)
        .or_else(|_| std::env::var(legacy))
        .ok()
}

fn database_test_guard() -> std::sync::MutexGuard<'static, ()> {
    MYSQL_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[test]
fn mysql_backend_satisfies_remote_backend_contract_when_url_is_set() {
    let Some(database_url) = env_var("PROLLY_STORE_MYSQL_URL", "PROLLY_ADAPTERS_MYSQL_URL") else {
        return;
    };
    let _database_guard = database_test_guard();

    runtime().block_on(async {
        use prolly::remote_conformance::{
            assert_remote_backend_async_indexed_map_contract, assert_remote_backend_contract,
            assert_remote_backend_indexed_map_contract, assert_remote_backend_transaction_contract,
        };
        let backend = MySqlBackend::connect(&database_url).await.unwrap();
        backend.initialize_schema().await.unwrap();
        clear_mysql(backend.pool()).await.unwrap();
        assert_remote_backend_contract(&backend).await;
        assert_remote_backend_transaction_contract(&backend).await;
        clear_mysql(backend.pool()).await.unwrap();
        assert_remote_backend_async_indexed_map_contract(backend.clone()).await;
        clear_mysql(backend.pool()).await.unwrap();
        assert_remote_backend_indexed_map_contract(backend.clone());
        clear_mysql(backend.pool()).await.unwrap();
    });
}

#[test]
fn mysql_options_are_additive_and_default_to_bounded_batches() {
    assert_eq!(MySqlBackendOptions::default().max_batch_items(), 1_000);
    let options = MySqlBackendOptions::new(NonZeroUsize::new(7).unwrap());
    assert_eq!(options.max_batch_items(), 7);
}

#[test]
fn mysql_set_based_batches_and_root_locking_work_when_url_is_set() {
    let Some(database_url) = env_var("PROLLY_STORE_MYSQL_URL", "PROLLY_ADAPTERS_MYSQL_URL") else {
        return;
    };
    let _database_guard = database_test_guard();

    runtime().block_on(async {
        let pool = MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        let options = MySqlBackendOptions::new(NonZeroUsize::new(2).unwrap());
        let backend = MySqlBackend::new_with_options(pool, options);
        backend.initialize_schema().await.unwrap();
        clear_mysql(backend.pool()).await.unwrap();

        let before = session_counter(backend.pool(), "Com_insert").await;
        let entries = (0_u8..5)
            .map(|id| (vec![id; 32], vec![id; 8]))
            .collect::<Vec<_>>();
        let borrowed = entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect::<Vec<_>>();
        backend.batch_put_nodes(&borrowed).await.unwrap();
        let after = session_counter(backend.pool(), "Com_insert").await;
        assert_eq!(after - before, 3);

        let missing = vec![99; 32];
        let requested = [
            entries[4].0.as_slice(),
            missing.as_slice(),
            entries[1].0.as_slice(),
            entries[4].0.as_slice(),
            entries[0].0.as_slice(),
        ];
        let values = backend.batch_get_nodes_ordered(&requested).await.unwrap();
        assert_eq!(
            values,
            vec![
                Some(entries[4].1.clone()),
                None,
                Some(entries[1].1.clone()),
                Some(entries[4].1.clone()),
                Some(entries[0].1.clone()),
            ]
        );

        let concurrent_pool = MySqlPoolOptions::new()
            .max_connections(16)
            .connect(&database_url)
            .await
            .unwrap();
        let concurrent_backend = MySqlBackend::new_with_options(concurrent_pool, options);
        let mut contenders = tokio::task::JoinSet::new();
        for id in 0_u8..16 {
            let backend = concurrent_backend.clone();
            contenders.spawn(async move {
                backend
                    .compare_and_swap_root_manifest(b"main", None, Some(&[id]))
                    .await
                    .unwrap()
            });
        }
        let mut applied = 0;
        let mut conflicts = 0;
        while let Some(result) = contenders.join_next().await {
            match result.unwrap() {
                RemoteManifestUpdate::Applied => applied += 1,
                RemoteManifestUpdate::Conflict { .. } => conflicts += 1,
            }
        }
        assert_eq!((applied, conflicts), (1, 15));
        clear_mysql(concurrent_backend.pool()).await.unwrap();
    });
}

#[test]
fn mysql_multichunk_batch_rolls_back_when_a_later_chunk_fails() {
    let Some(database_url) = env_var("PROLLY_STORE_MYSQL_URL", "PROLLY_ADAPTERS_MYSQL_URL") else {
        return;
    };
    let _database_guard = database_test_guard();

    runtime().block_on(async {
        let backend = MySqlBackend::connect_with_options(
            &database_url,
            MySqlBackendOptions::new(NonZeroUsize::new(2).unwrap()),
        )
        .await
        .unwrap();
        backend.initialize_schema().await.unwrap();
        clear_mysql(backend.pool()).await.unwrap();

        let entries = [
            (vec![1; 32], vec![1]),
            (vec![2; 32], vec![2]),
            (vec![0xff; 33], vec![3]),
        ];
        let borrowed = entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect::<Vec<_>>();
        assert!(backend.batch_put_nodes(&borrowed).await.is_err());
        assert_eq!(
            backend
                .batch_get_nodes_ordered(
                    &entries
                        .iter()
                        .map(|(key, _)| key.as_slice())
                        .collect::<Vec<_>>()
                )
                .await
                .unwrap(),
            vec![None, None, None]
        );

        clear_mysql(backend.pool()).await.unwrap();
    });
}

async fn session_counter(pool: &sqlx::MySqlPool, name: &str) -> u64 {
    let row = sqlx::query("SHOW SESSION STATUS WHERE Variable_name = ?")
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap();
    row.try_get::<String, _>("Value").unwrap().parse().unwrap()
}

async fn clear_mysql(pool: &sqlx::MySqlPool) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM prolly_hints")
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM prolly_roots")
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM prolly_nodes")
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM prolly_root_locks")
        .execute(pool)
        .await?;
    Ok(())
}
use std::num::NonZeroUsize;

use prolly_store_mysql::{
    MySqlBackend, MySqlBackendOptions, RemoteManifestUpdate, RemoteStoreBackend,
};
use sqlx::{mysql::MySqlPoolOptions, Row};

static MYSQL_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
