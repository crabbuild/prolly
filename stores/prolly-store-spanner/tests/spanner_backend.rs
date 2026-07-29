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
fn spanner_backend_satisfies_remote_backend_contract_when_database_is_set() {
    let Some(database) = env_var(
        "PROLLY_STORE_SPANNER_DATABASE",
        "PROLLY_ADAPTERS_SPANNER_DATABASE",
    ) else {
        return;
    };

    runtime().block_on(async {
        use google_cloud_spanner::client::ClientConfig;
        use prolly::remote_conformance::{
            assert_remote_backend_contract, assert_remote_backend_transaction_contract,
        };
        use prolly::{RemoteManifestUpdate, RemoteStoreBackend};
        use prolly_store_spanner::{SpannerBackend, SpannerBackendOptions};

        let mut config = ClientConfig::default();
        if env_var("PROLLY_STORE_SPANNER_AUTH", "PROLLY_ADAPTERS_SPANNER_AUTH").is_some() {
            config = config.with_auth().await.unwrap();
        }

        let backend = SpannerBackend::connect(&database, config).await.unwrap();
        clear_spanner(backend.client()).await.unwrap();
        assert_remote_backend_contract(&backend).await;
        assert_remote_backend_transaction_contract(&backend).await;

        let mut contenders = tokio::task::JoinSet::new();
        for contender in 0..32u8 {
            let backend = backend.clone();
            contenders.spawn(async move {
                backend
                    .compare_and_swap_root_manifest(b"contention/main", None, Some(&[contender]))
                    .await
                    .unwrap()
            });
        }
        let mut applied = 0;
        while let Some(result) = contenders.join_next().await {
            if result.unwrap() == RemoteManifestUpdate::Applied {
                applied += 1;
            }
        }
        assert_eq!(applied, 1, "exactly one concurrent root CAS must win");

        clear_spanner(backend.client()).await.unwrap();
        let chunked_backend = SpannerBackend::new_with_options(
            backend.client().clone(),
            SpannerBackendOptions::default()
                .with_batch_read_items(7)
                .with_read_parallelism(4),
        );
        assert_eq!(chunked_backend.read_parallelism(), 4);
        assert!(chunked_backend.prefers_batch_reads());

        let keys = (0..257u32)
            .map(|index| {
                let mut key = [0u8; 32];
                key[..4].copy_from_slice(&index.to_be_bytes());
                key[4..8].copy_from_slice(&(index ^ 0xa5a5_5a5a).to_be_bytes());
                key
            })
            .collect::<Vec<_>>();
        let values = (0..keys.len())
            .map(|index| {
                let length = 48 + (index % 113);
                (0..length)
                    .map(|offset| ((index * 31 + offset * 17) & 0xff) as u8)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let entries = keys
            .iter()
            .zip(&values)
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect::<Vec<_>>();
        chunked_backend.batch_put_nodes(&entries).await.unwrap();
        chunked_backend.batch_put_nodes(&[]).await.unwrap();
        assert!(chunked_backend
            .batch_get_nodes_ordered(&[])
            .await
            .unwrap()
            .is_empty());

        let missing = [0xff; 32];
        let mut requested = keys
            .iter()
            .rev()
            .map(<[u8; 32]>::as_slice)
            .collect::<Vec<_>>();
        requested.insert(3, keys[42].as_slice());
        requested.insert(129, missing.as_slice());
        requested.push(keys[42].as_slice());
        let fetched = chunked_backend
            .batch_get_nodes_ordered(&requested)
            .await
            .unwrap();
        assert_eq!(fetched.len(), requested.len());
        for (key, value) in requested.iter().zip(&fetched) {
            let expected = keys
                .iter()
                .position(|candidate| candidate.as_slice() == *key)
                .map(|index| values[index].clone());
            assert_eq!(*value, expected);
        }

        let large_key = [0xfe; 32];
        let large_value = vec![0x5a; 1024 * 1024];
        chunked_backend
            .put_node(&large_key, &large_value)
            .await
            .unwrap();
        assert_eq!(
            chunked_backend.get_node(&large_key).await.unwrap(),
            Some(large_value)
        );

        let listed = chunked_backend.list_node_cids().await.unwrap();
        assert_eq!(listed.len(), keys.len() + 1);
        assert!(listed.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(listed.contains(&large_key.to_vec()));

        clear_spanner(backend.client()).await.unwrap();
        backend.client().clone().close().await;
    });
}

async fn clear_spanner(
    client: &google_cloud_spanner::client::Client,
) -> Result<(), google_cloud_spanner::client::Error> {
    use google_cloud_spanner::key::all_keys;
    use google_cloud_spanner::mutation::delete;

    client
        .apply(vec![
            delete("ProllyHints", all_keys()),
            delete("ProllyRoots", all_keys()),
            delete("ProllyNodes", all_keys()),
        ])
        .await?;
    Ok(())
}
