use std::time::{SystemTime, UNIX_EPOCH};

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

fn unique_prefix(label: &str) -> Vec<u8> {
    format!(
        "prolly:test:{label}:{}:",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
    .into_bytes()
}

#[test]
fn redis_backend_satisfies_remote_backend_contract_when_url_is_set() {
    let Some(redis_url) = env_var("PROLLY_STORE_REDIS_URL", "PROLLY_ADAPTERS_REDIS_URL") else {
        return;
    };

    runtime().block_on(async {
        use prolly::remote_conformance::{
            assert_remote_backend_contract, assert_remote_backend_transaction_contract,
        };
        use prolly_store_redis::RedisBackend;

        let backend = RedisBackend::connect(&redis_url)
            .await
            .unwrap()
            .with_key_prefix(unique_prefix("contract"));

        backend.clear_namespace().await.unwrap();
        assert_remote_backend_contract(&backend).await;
        assert_remote_backend_transaction_contract(&backend).await;
        backend.clear_namespace().await.unwrap();
    });
}

#[test]
fn redis_backend_bounds_bulk_commands_and_preserves_semantics_when_url_is_set() {
    let Some(redis_url) = env_var("PROLLY_STORE_REDIS_URL", "PROLLY_ADAPTERS_REDIS_URL") else {
        return;
    };

    runtime().block_on(async {
        use std::time::Duration;

        use prolly::{
            RemoteBatchOp, RemoteManifestUpdate, RemoteRootWrite, RemoteStoreBackend,
            RemoteTransactionUpdate,
        };
        use prolly_store_redis::{RedisBackend, RedisBackendOptions};

        let options = RedisBackendOptions::default()
            .with_max_batch_items(2)
            .with_max_batch_bytes(64)
            .with_scan_count(1)
            .with_delete_chunk_size(1)
            .with_read_parallelism(3)
            .with_response_timeout(Duration::from_secs(5))
            .with_connection_timeout(Duration::from_secs(5));
        let backend = RedisBackend::connect_with_options(&redis_url, options)
            .await
            .unwrap()
            .with_key_prefix(unique_prefix("bounded"));

        assert_eq!(backend.options().max_batch_items(), 2);
        assert_eq!(backend.read_parallelism(), 3);
        assert!(backend.prefers_rightmost_path_hints());

        let mut control = backend.connection().clone();
        let mut bulk = backend.bulk_connection().clone();
        let control_id: i64 = redis_client::cmd("CLIENT")
            .arg("ID")
            .query_async(&mut control)
            .await
            .unwrap();
        let bulk_id: i64 = redis_client::cmd("CLIENT")
            .arg("ID")
            .query_async(&mut bulk)
            .await
            .unwrap();
        assert_ne!(control_id, bulk_id);

        let entries = [
            (b"a".as_slice(), b"one".as_slice()),
            (b"b".as_slice(), b"two".as_slice()),
            (b"c".as_slice(), b"three".as_slice()),
            (b"d".as_slice(), b"four".as_slice()),
            (b"e".as_slice(), b"five".as_slice()),
        ];
        backend.batch_put_nodes(&entries).await.unwrap();
        let keys = [
            b"e".as_slice(),
            b"missing".as_slice(),
            b"a".as_slice(),
            b"c".as_slice(),
            b"b".as_slice(),
        ];
        assert_eq!(
            backend.batch_get_nodes_ordered(&keys).await.unwrap(),
            vec![
                Some(b"five".to_vec()),
                None,
                Some(b"one".to_vec()),
                Some(b"three".to_vec()),
                Some(b"two".to_vec()),
            ]
        );

        backend
            .batch_nodes(&[
                RemoteBatchOp::Upsert {
                    key: b"a",
                    value: b"stale",
                },
                RemoteBatchOp::Delete { key: b"a" },
                RemoteBatchOp::Delete { key: b"b" },
                RemoteBatchOp::Upsert {
                    key: b"b",
                    value: b"latest",
                },
            ])
            .await
            .unwrap();
        assert_eq!(backend.get_node(b"a").await.unwrap(), None);
        assert_eq!(
            backend.get_node(b"b").await.unwrap(),
            Some(b"latest".to_vec())
        );

        for index in (0..5).rev() {
            backend
                .put_root_manifest(
                    format!("root-{index}").as_bytes(),
                    format!("manifest-{index}").as_bytes(),
                )
                .await
                .unwrap();
        }
        let roots = backend.list_root_manifests().await.unwrap();
        assert_eq!(
            roots
                .iter()
                .map(|root| root.name.as_slice())
                .collect::<Vec<_>>(),
            vec![
                b"root-0".as_slice(),
                b"root-1".as_slice(),
                b"root-2".as_slice(),
                b"root-3".as_slice(),
                b"root-4".as_slice(),
            ]
        );

        let mut contenders = Vec::new();
        for index in 0..32 {
            let contender = backend.clone();
            contenders.push(tokio::spawn(async move {
                contender
                    .compare_and_swap_root_manifest(
                        b"contended-root",
                        None,
                        Some(format!("contender-{index}").as_bytes()),
                    )
                    .await
                    .unwrap()
            }));
        }
        let mut applied = 0;
        let mut conflicts = 0;
        for contender in contenders {
            match contender.await.unwrap() {
                RemoteManifestUpdate::Applied => applied += 1,
                RemoteManifestUpdate::Conflict { current: Some(_) } => conflicts += 1,
                other => panic!("unexpected contended CAS result: {other:?}"),
            }
        }
        assert_eq!(applied, 1);
        assert_eq!(conflicts, 31);

        assert_eq!(
            backend
                .commit_transaction(
                    &[RemoteBatchOp::Upsert {
                        key: b"transaction-node",
                        value: b"value",
                    }],
                    &[],
                    &[RemoteRootWrite::Put {
                        name: b"transaction-root".to_vec(),
                        manifest: b"manifest".to_vec(),
                    }],
                )
                .await
                .unwrap(),
            RemoteTransactionUpdate::Applied
        );

        backend.clear_namespace().await.unwrap();
        assert!(backend.list_node_cids().await.unwrap().is_empty());
        assert!(backend.list_root_manifests().await.unwrap().is_empty());
    });
}
