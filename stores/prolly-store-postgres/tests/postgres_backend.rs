use std::num::NonZeroUsize;
use std::time::Duration;

use prolly::{
    RemoteBatchOp, RemoteManifestUpdate, RemoteRootCondition, RemoteRootWrite, RemoteStoreBackend,
    RemoteTransactionUpdate,
};
use prolly_store_postgres::{PostgresBackend, PostgresBackendOptions};
use sqlx::Row;

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

#[test]
fn postgres_backend_options_have_a_safe_default() {
    assert_eq!(PostgresBackendOptions::default().max_batch_items(), 1_024);
}

#[test]
fn postgres_backend_satisfies_remote_backend_contract_when_url_is_set() {
    let Some(database_url) = env_var("PROLLY_STORE_POSTGRES_URL", "PROLLY_ADAPTERS_POSTGRES_URL")
    else {
        return;
    };

    runtime().block_on(async {
        use prolly::remote_conformance::{
            assert_remote_backend_async_indexed_map_contract, assert_remote_backend_contract,
            assert_remote_backend_indexed_map_contract, assert_remote_backend_transaction_contract,
        };

        let backend = PostgresBackend::connect(&database_url).await.unwrap();
        backend.initialize_schema().await.unwrap();
        clear_postgres(backend.pool()).await.unwrap();
        assert_remote_backend_contract(&backend).await;
        assert_remote_backend_transaction_contract(&backend).await;
        clear_postgres(backend.pool()).await.unwrap();
        assert_remote_backend_async_indexed_map_contract(backend.clone()).await;
        clear_postgres(backend.pool()).await.unwrap();
        assert_remote_backend_indexed_map_contract(backend.clone());
        clear_postgres(backend.pool()).await.unwrap();
        assert_set_based_batches(&backend).await;
        clear_postgres(backend.pool()).await.unwrap();
        assert_root_concurrency(&backend).await;
        clear_postgres(backend.pool()).await.unwrap();
    });
}

async fn assert_set_based_batches(backend: &PostgresBackend) {
    let options = PostgresBackendOptions::new(NonZeroUsize::new(2).unwrap());
    let backend = PostgresBackend::new_with_options(backend.pool().clone(), options);
    assert_eq!(backend.options(), options);

    for statement in [
        "DROP TRIGGER IF EXISTS prolly_count_node_inserts ON prolly_nodes",
        "DROP FUNCTION IF EXISTS prolly_count_node_insert_statements()",
        "DROP TABLE IF EXISTS prolly_test_statement_counts",
        "CREATE TABLE prolly_test_statement_counts (id boolean PRIMARY KEY, calls integer NOT NULL)",
        "INSERT INTO prolly_test_statement_counts(id, calls) VALUES(true, 0)",
        "CREATE FUNCTION prolly_count_node_insert_statements() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE prolly_test_statement_counts SET calls = calls + 1 WHERE id = true; RETURN NULL; END $$",
        "CREATE TRIGGER prolly_count_node_inserts AFTER INSERT ON prolly_nodes FOR EACH STATEMENT EXECUTE FUNCTION prolly_count_node_insert_statements()",
    ] {
        sqlx::query(statement)
            .execute(backend.pool())
            .await
            .unwrap();
    }

    let entries: Vec<(&[u8], &[u8])> = vec![
        (b"a", b"A"),
        (b"b", b"B"),
        (b"c", b"C"),
        (b"d", b"D"),
        (b"e", b"E"),
        (b"a", b"A-last"),
    ];
    backend.batch_put_nodes(&entries).await.unwrap();
    let calls: i32 = sqlx::query("SELECT calls FROM prolly_test_statement_counts WHERE id = true")
        .fetch_one(backend.pool())
        .await
        .unwrap()
        .try_get("calls")
        .unwrap();
    assert_eq!(
        calls, 3,
        "five unique entries with chunk size two use three inserts"
    );

    let keys: Vec<&[u8]> = vec![b"e", b"missing", b"a", b"e", b"c"];
    assert_eq!(
        backend.batch_get_nodes_ordered(&keys).await.unwrap(),
        vec![
            Some(b"E".to_vec()),
            None,
            Some(b"A-last".to_vec()),
            Some(b"E".to_vec()),
            Some(b"C".to_vec()),
        ]
    );
    assert!(backend
        .batch_get_nodes_ordered(&[])
        .await
        .unwrap()
        .is_empty());

    for statement in [
        "DROP TRIGGER IF EXISTS prolly_fail_node_insert ON prolly_nodes",
        "DROP FUNCTION IF EXISTS prolly_fail_selected_node_insert()",
        "CREATE FUNCTION prolly_fail_selected_node_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.cid = decode('6661696c2d7468697264', 'hex') THEN RAISE EXCEPTION 'forced node insert failure'; END IF; RETURN NEW; END $$",
        "CREATE TRIGGER prolly_fail_node_insert BEFORE INSERT ON prolly_nodes FOR EACH ROW EXECUTE FUNCTION prolly_fail_selected_node_insert()",
    ] {
        sqlx::query(statement)
            .execute(backend.pool())
            .await
            .unwrap();
    }
    let atomic_entries: Vec<(&[u8], &[u8])> = vec![
        (b"atomic-a", b"A"),
        (b"atomic-b", b"B"),
        (b"fail-third", b"C"),
    ];
    assert!(backend.batch_put_nodes(&atomic_entries).await.is_err());
    assert_eq!(backend.get_node(b"atomic-a").await.unwrap(), None);
    assert_eq!(backend.get_node(b"atomic-b").await.unwrap(), None);

    backend
        .batch_nodes(&[
            RemoteBatchOp::Upsert {
                key: b"a",
                value: b"A2",
            },
            RemoteBatchOp::Delete { key: b"a" },
            RemoteBatchOp::Upsert {
                key: b"a",
                value: b"A3",
            },
            RemoteBatchOp::Upsert {
                key: b"b",
                value: b"B2",
            },
            RemoteBatchOp::Delete { key: b"b" },
        ])
        .await
        .unwrap();
    assert_eq!(backend.get_node(b"a").await.unwrap(), Some(b"A3".to_vec()));
    assert_eq!(backend.get_node(b"b").await.unwrap(), None);

    for statement in [
        "DROP TRIGGER prolly_fail_node_insert ON prolly_nodes",
        "DROP FUNCTION prolly_fail_selected_node_insert()",
        "DROP TRIGGER prolly_count_node_inserts ON prolly_nodes",
        "DROP FUNCTION prolly_count_node_insert_statements()",
        "DROP TABLE prolly_test_statement_counts",
    ] {
        sqlx::query(statement)
            .execute(backend.pool())
            .await
            .unwrap();
    }
}

async fn assert_root_concurrency(backend: &PostgresBackend) {
    let contenders = (0..16)
        .map(|index| {
            let backend = backend.clone();
            tokio::spawn(async move {
                backend
                    .compare_and_swap_root_manifest(
                        b"hot/main",
                        None,
                        Some(format!("manifest-{index}").as_bytes()),
                    )
                    .await
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let mut applied = 0;
    for contender in contenders {
        if matches!(contender.await.unwrap(), RemoteManifestUpdate::Applied) {
            applied += 1;
        }
    }
    assert_eq!(applied, 1);

    let mut locking_tx = backend.pool().begin().await.unwrap();
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtextextended('prolly-root-v1:' || encode($1::bytea, 'hex'), 0))",
    )
    .bind(b"blocked/main".as_slice())
    .execute(&mut *locking_tx)
    .await
    .unwrap();
    let blocked_backend = backend.clone();
    let mut blocked = tokio::spawn(async move {
        blocked_backend
            .compare_and_swap_root_manifest(b"blocked/main", None, Some(b"blocked"))
            .await
    });
    let free_backend = backend.clone();
    let mut free = tokio::spawn(async move {
        free_backend
            .compare_and_swap_root_manifest(b"free/main", None, Some(b"free"))
            .await
    });
    let free_result = tokio::time::timeout(Duration::from_millis(500), &mut free)
        .await
        .expect("an unrelated root must not wait for the held root lock")
        .unwrap()
        .unwrap();
    assert!(matches!(free_result, RemoteManifestUpdate::Applied));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut blocked)
            .await
            .is_err(),
        "the same root must wait for its advisory lock"
    );
    locking_tx.rollback().await.unwrap();
    assert!(matches!(
        blocked.await.unwrap().unwrap(),
        RemoteManifestUpdate::Applied
    ));

    backend.put_root_manifest(b"tx/a", b"base").await.unwrap();
    backend.put_root_manifest(b"tx/b", b"base").await.unwrap();
    let left = backend.clone();
    let right = backend.clone();
    let first = tokio::spawn(async move {
        left.commit_transaction(
            &[],
            &[
                RemoteRootCondition::new(b"tx/a".to_vec(), Some(b"base".to_vec())),
                RemoteRootCondition::new(b"tx/b".to_vec(), Some(b"base".to_vec())),
            ],
            &[
                RemoteRootWrite::Put {
                    name: b"tx/a".to_vec(),
                    manifest: b"first".to_vec(),
                },
                RemoteRootWrite::Put {
                    name: b"tx/b".to_vec(),
                    manifest: b"first".to_vec(),
                },
            ],
        )
        .await
    });
    let second = tokio::spawn(async move {
        right
            .commit_transaction(
                &[],
                &[
                    RemoteRootCondition::new(b"tx/b".to_vec(), Some(b"base".to_vec())),
                    RemoteRootCondition::new(b"tx/a".to_vec(), Some(b"base".to_vec())),
                ],
                &[
                    RemoteRootWrite::Put {
                        name: b"tx/b".to_vec(),
                        manifest: b"second".to_vec(),
                    },
                    RemoteRootWrite::Put {
                        name: b"tx/a".to_vec(),
                        manifest: b"second".to_vec(),
                    },
                ],
            )
            .await
    });
    let (first, second) = tokio::time::timeout(Duration::from_secs(2), async {
        (
            first.await.unwrap().unwrap(),
            second.await.unwrap().unwrap(),
        )
    })
    .await
    .expect("sorted root locks must prevent a deadlock");
    assert_eq!(
        usize::from(matches!(first, RemoteTransactionUpdate::Applied))
            + usize::from(matches!(second, RemoteTransactionUpdate::Applied)),
        1
    );
}

async fn clear_postgres(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM prolly_hints")
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM prolly_roots")
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM prolly_nodes")
        .execute(pool)
        .await?;
    Ok(())
}
