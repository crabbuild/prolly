use prolly::{
    AsyncProlly, AsyncStore, Cid, Config, NodePublication, NodePublicationHint, PublicationOrigin,
    RemoteAdapterError, RemoteBatchOp, RemoteManifestUpdate, RemoteRootWrite, RemoteStoreBackend,
};
#[cfg(feature = "turso-cloud-sync")]
use prolly_store_turso::TursoStoreError;
use prolly_store_turso::{TursoBackend, TursoStore};

#[tokio::test(flavor = "multi_thread")]
async fn local_backend_constructs_store() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("local.db");

    let backend = TursoBackend::open(&path).await.unwrap();

    assert!(!backend.is_synced());
    let _store = TursoStore::new(backend);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn local_backend_rejects_non_utf8_paths_without_mangling_them() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(OsString::from_vec(vec![0xff]));

    let error = TursoBackend::open(&path).await.unwrap_err();

    assert!(matches!(
        error,
        prolly_store_turso::TursoStoreError::InvalidPath(invalid) if invalid == path
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn local_backend_satisfies_remote_contract() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TursoBackend::open(temp.path().join("contract.db"))
        .await
        .unwrap();

    prolly::remote_conformance::assert_remote_backend_contract(&backend).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn local_backend_satisfies_transaction_contract() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TursoBackend::open(temp.path().join("transaction.db"))
        .await
        .unwrap();

    prolly::remote_conformance::assert_remote_backend_transaction_contract(&backend).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_root_cas_has_one_winner_and_one_conflict() {
    async fn cas_with_busy_retry(
        backend: &TursoBackend,
        manifest: &'static [u8],
    ) -> RemoteManifestUpdate {
        for _ in 0..100 {
            match backend
                .compare_and_swap_root_manifest(b"main", None, Some(manifest))
                .await
            {
                Ok(update) => return update,
                Err(prolly_store_turso::TursoStoreError::Turso(
                    turso::Error::Busy(_) | turso::Error::BusySnapshot(_),
                )) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected CAS error: {error}"),
            }
        }

        panic!("CAS remained busy after 100 retries");
    }

    let temp = tempfile::tempdir().unwrap();
    let backend = TursoBackend::open(temp.path().join("concurrent-cas.db"))
        .await
        .unwrap();
    let left = backend.clone();
    let right = backend.clone();

    let (left_result, right_result) = tokio::join!(
        cas_with_busy_retry(&left, b"left"),
        cas_with_busy_retry(&right, b"right"),
    );

    assert!(
        matches!(
            (&left_result, &right_result),
            (
                RemoteManifestUpdate::Applied,
                RemoteManifestUpdate::Conflict { current: Some(current) }
            ) if current == b"left"
        ) || matches!(
            (&left_result, &right_result),
            (
                RemoteManifestUpdate::Conflict { current: Some(current) },
                RemoteManifestUpdate::Applied
            ) if current == b"right"
        )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_batch_rolls_back_every_node_write() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("rollback.db");
    let database = turso::Builder::new_local(path.to_str().unwrap())
        .build()
        .await
        .unwrap();
    let backend = TursoBackend::from_local_database(database.clone())
        .await
        .unwrap();
    database
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_short_node
             BEFORE INSERT ON prolly_nodes
             WHEN length(NEW.cid) = 1
             BEGIN
               SELECT RAISE(ABORT, 'rejected by test');
             END;",
        )
        .await
        .unwrap();

    let result = backend
        .batch_nodes(&[
            RemoteBatchOp::Upsert {
                key: b"valid",
                value: b"written-first",
            },
            RemoteBatchOp::Upsert {
                key: b"x",
                value: b"must-fail",
            },
        ])
        .await;

    assert!(result.is_err());
    assert_eq!(backend.get_node(b"valid").await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn hint_round_trip_and_root_cas_delete_work() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TursoBackend::open(temp.path().join("hints-and-delete.db"))
        .await
        .unwrap();

    backend
        .put_hint(b"scan", b"rightmost", b"hint-v1")
        .await
        .unwrap();
    assert_eq!(
        backend.get_hint(b"scan", b"rightmost").await.unwrap(),
        Some(b"hint-v1".to_vec())
    );

    backend
        .put_root_manifest(b"delete-me", b"manifest")
        .await
        .unwrap();
    assert_eq!(
        backend
            .compare_and_swap_root_manifest(b"delete-me", Some(b"manifest"), None)
            .await
            .unwrap(),
        RemoteManifestUpdate::Applied
    );
    assert_eq!(backend.get_root_manifest(b"delete-me").await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn point_publications_preserve_nodes_and_optional_hints() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TursoBackend::open(temp.path().join("point-publication.db"))
        .await
        .unwrap();
    let store = TursoStore::new(backend.clone());

    let plain_bytes = b"unhinted-point-node";
    let plain_cid = Cid::from_bytes(plain_bytes);
    let plain_entries = [(plain_cid.as_bytes(), plain_bytes.as_slice())];
    store
        .publish_nodes(NodePublication::new(
            &plain_entries,
            PublicationOrigin::PointUpsert,
        ))
        .await
        .unwrap();

    let hinted_bytes = b"hinted-point-node";
    let hinted_cid = Cid::from_bytes(hinted_bytes);
    let hinted_entries = [(hinted_cid.as_bytes(), hinted_bytes.as_slice())];
    let hint = NodePublicationHint::new(b"point", b"rightmost", hinted_cid.as_bytes());
    store
        .publish_nodes(NodePublication::with_hint(
            &hinted_entries,
            hint,
            PublicationOrigin::PointUpsert,
        ))
        .await
        .unwrap();

    assert_eq!(
        backend.get_node(plain_cid.as_bytes()).await.unwrap(),
        Some(plain_bytes.to_vec())
    );
    assert_eq!(
        backend.get_node(hinted_cid.as_bytes()).await.unwrap(),
        Some(hinted_bytes.to_vec())
    );
    assert_eq!(
        backend
            .get_hint(b"point", b"rightmost")
            .await
            .unwrap(),
        Some(hinted_cid.as_bytes().to_vec())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_node_plus_hint_batch_rolls_back_node_writes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("node-hint-rollback.db");
    let database = turso::Builder::new_local(path.to_str().unwrap())
        .build()
        .await
        .unwrap();
    let backend = TursoBackend::from_local_database(database.clone())
        .await
        .unwrap();
    database
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_hint
             BEFORE INSERT ON prolly_hints
             BEGIN
               SELECT RAISE(ABORT, 'rejected by test');
             END;",
        )
        .await
        .unwrap();

    let store = TursoStore::new(backend.clone());
    let bytes = b"point-node-must-roll-back";
    let cid = Cid::from_bytes(bytes);
    let entries = [(cid.as_bytes(), bytes.as_slice())];
    let result = store
        .publish_nodes(NodePublication::with_hint(
            &entries,
            NodePublicationHint::new(b"ns", b"key", b"hint"),
            PublicationOrigin::PointUpsert,
        ))
        .await;

    assert!(result.is_err());
    assert_eq!(backend.get_node(cid.as_bytes()).await.unwrap(), None);
    assert_eq!(backend.get_hint(b"ns", b"key").await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_coordinated_transaction_rolls_back_prior_node_writes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("transaction-sql-rollback.db");
    let database = turso::Builder::new_local(path.to_str().unwrap())
        .build()
        .await
        .unwrap();
    let backend = TursoBackend::from_local_database(database.clone())
        .await
        .unwrap();
    database
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_root
             BEFORE INSERT ON prolly_roots
             WHEN NEW.name = X'72656A656374'
             BEGIN
               SELECT RAISE(ABORT, 'rejected by test');
             END;",
        )
        .await
        .unwrap();

    let result = backend
        .commit_transaction(
            &[RemoteBatchOp::Upsert {
                key: b"node",
                value: b"must-roll-back",
            }],
            &[],
            &[RemoteRootWrite::Put {
                name: b"reject".to_vec(),
                manifest: b"manifest".to_vec(),
            }],
        )
        .await;

    assert!(result.is_err());
    assert_eq!(backend.get_node(b"node").await.unwrap(), None);
    assert_eq!(backend.get_root_manifest(b"reject").await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn local_store_persists_named_root_across_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("reopen.db");
    let expected = {
        let backend = TursoBackend::open(&path).await.unwrap();
        let prolly = AsyncProlly::new(TursoStore::new(backend), Config::default());
        let tree = prolly
            .put(&prolly.create(), b"user/1".to_vec(), b"Ada".to_vec())
            .await
            .unwrap();
        prolly.publish_named_root(b"main", &tree).await.unwrap();
        tree
    };

    let backend = TursoBackend::open(&path).await.unwrap();
    let prolly = AsyncProlly::new(TursoStore::new(backend), Config::default());
    let loaded = prolly
        .load_named_root(b"main")
        .await
        .unwrap()
        .expect("main root");

    assert_eq!(loaded, expected);
    assert_eq!(
        prolly.get(&loaded, b"user/1").await.unwrap(),
        Some(b"Ada".to_vec())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_point_upserts_remain_readable_after_reopen() {
    fn successful_or_busy(
        result: Result<prolly::Tree, prolly::Error>,
    ) -> Option<prolly::Tree> {
        match result {
            Ok(tree) => Some(tree),
            Err(prolly::Error::Store(error)) => {
                let adapter = error
                    .downcast_ref::<RemoteAdapterError<prolly_store_turso::TursoStoreError>>()
                    .expect("point publication returned an unexpected store error type");
                assert!(matches!(
                    adapter,
                    RemoteAdapterError::Backend(prolly_store_turso::TursoStoreError::Turso(
                        turso::Error::Busy(_) | turso::Error::BusySnapshot(_)
                    ))
                ));
                None
            }
            Err(error) => panic!("point publication returned an unexpected error: {error}"),
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("concurrent-point-upserts.db");
    let backend = TursoBackend::open(&path).await.unwrap();
    let left = AsyncProlly::new(TursoStore::new(backend.clone()), Config::default());
    let right = AsyncProlly::new(TursoStore::new(backend.clone()), Config::default());
    let base = left.create();

    let (left_result, right_result) = tokio::join!(
        left.put(&base, b"left".to_vec(), b"one".to_vec()),
        right.put(&base, b"right".to_vec(), b"two".to_vec()),
    );
    let successful = [
        successful_or_busy(left_result).map(|tree| (tree, b"left".as_slice(), b"one".as_slice())),
        successful_or_busy(right_result)
            .map(|tree| (tree, b"right".as_slice(), b"two".as_slice())),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    assert!(!successful.is_empty());

    drop(left);
    drop(right);
    drop(backend);

    let reopened = AsyncProlly::new(
        TursoStore::new(TursoBackend::open(&path).await.unwrap()),
        Config::default(),
    );
    for (tree, key, value) in successful {
        assert_eq!(reopened.get(&tree, key).await.unwrap(), Some(value.to_vec()));
    }
}

#[cfg(feature = "turso-cloud-sync")]
#[tokio::test(flavor = "multi_thread")]
async fn local_backend_rejects_cloud_sync_operations() {
    let temp = tempfile::tempdir().unwrap();
    let backend = TursoBackend::open(temp.path().join("local-only.db"))
        .await
        .unwrap();

    assert!(matches!(
        backend.push().await,
        Err(TursoStoreError::NotSynced)
    ));
    assert!(matches!(
        backend.pull().await,
        Err(TursoStoreError::NotSynced)
    ));
}

#[cfg(feature = "turso-cloud-sync")]
#[tokio::test(flavor = "multi_thread")]
async fn synced_backend_pushes_and_pulls_when_credentials_are_set() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let Ok(remote_url) = std::env::var("TURSO_DATABASE_URL") else {
        return;
    };
    let Ok(auth_token) = std::env::var("TURSO_AUTH_TOKEN") else {
        return;
    };

    let temp = tempfile::tempdir().unwrap();
    let writer_backend = TursoBackend::open_synced(
        temp.path().join("writer.db"),
        remote_url.clone(),
        auth_token.clone(),
    )
    .await
    .unwrap();
    let reader_backend =
        TursoBackend::open_synced(temp.path().join("reader.db"), remote_url, auth_token)
            .await
            .unwrap();
    assert!(writer_backend.is_synced());
    assert!(reader_backend.is_synced());
    let writer_sync = writer_backend.clone();
    let reader_sync = reader_backend.clone();
    let writer = AsyncProlly::new(TursoStore::new(writer_backend), Config::default());
    let reader = AsyncProlly::new(TursoStore::new(reader_backend), Config::default());
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root_name = format!("prolly/test/{unique}").into_bytes();
    let key = format!("sync/{unique}").into_bytes();
    let tree = writer
        .put(&writer.create(), key.clone(), b"synced".to_vec())
        .await
        .unwrap();
    writer.publish_named_root(&root_name, &tree).await.unwrap();

    assert_eq!(reader.load_named_root(&root_name).await.unwrap(), None);

    writer_sync.push().await.unwrap();
    assert!(reader_sync.pull().await.unwrap());

    let loaded = reader
        .load_named_root(&root_name)
        .await
        .unwrap()
        .expect("synced root");
    assert_eq!(
        reader.get(&loaded, &key).await.unwrap(),
        Some(b"synced".to_vec())
    );
}
