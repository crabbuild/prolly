use std::{collections::BTreeMap, sync::Arc};

use prolly_s3_core::{
    decode_canonical, encode_canonical, MemoryObjectPlane, MergePolicy, ObjectHeaders,
    ProviderPerKeyVersionLimitV2, Repository, RepositoryOptions, RepositoryV2, RepositoryV2Options,
    V1ToV2MigrationCursor,
};

fn source_options() -> RepositoryOptions {
    RepositoryOptions {
        repository_prefix: ".tests/migration-v1-source".to_string(),
        writer: "migration-source-writer".to_string(),
        ..RepositoryOptions::default()
    }
}

fn destination_options() -> RepositoryV2Options {
    RepositoryV2Options {
        repository_prefix: ".tests/migration-v2-destination".to_string(),
        writer: "migration-destination-writer".to_string(),
        provider_per_key_version_limit: ProviderPerKeyVersionLimitV2::Finite(10_000),
        ..RepositoryV2Options::default()
    }
}

#[tokio::test]
async fn v1_history_migrates_in_restartable_parent_first_pages() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(source_plane, source_options())
        .await
        .unwrap();
    let first = source
        .put_bytes(
            "main",
            b"docs/history.txt".to_vec(),
            b"first".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    source.create_branch("feature", first.id).await.unwrap();
    let feature = source
        .put_bytes(
            "feature",
            b"docs/feature.txt".to_vec(),
            b"from feature".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    source
        .put_bytes(
            "main",
            b"docs/history.txt".to_vec(),
            b"second".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    source
        .merge(
            "main",
            feature.id,
            None,
            MergePolicy::Fail,
            None,
            Some("merge feature before migration".to_string()),
        )
        .await
        .unwrap();
    source
        .put_bytes(
            "main",
            b"docs/other.txt".to_vec(),
            b"other".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let destination_plane = Arc::new(MemoryObjectPlane::new(true));
    let mut destination =
        RepositoryV2::initialize(destination_plane.clone(), destination_options())
            .await
            .unwrap();
    let initial = source
        .start_v1_to_v2_migration(&destination, "main", "imported-main")
        .await
        .unwrap();

    let first_page = source
        .v1_to_v2_migration_page(&destination, &initial, 100, 1)
        .await
        .unwrap();
    assert_eq!(first_page.processed_commits, 1);
    assert!(!first_page.complete);

    // Replay the previous durable cursor after reopening the destination.
    // Immutable payload/commit writes and index roots must reconcile exactly.
    drop(destination);
    destination = RepositoryV2::open(destination_plane.clone(), destination_options())
        .await
        .unwrap();
    let replay = source
        .v1_to_v2_migration_page(&destination, &initial, 100, 1)
        .await
        .unwrap();
    assert_eq!(replay.cursor.index, first_page.cursor.index);

    let mut cursor = replay.cursor;
    loop {
        let encoded = encode_canonical(&cursor).unwrap();
        cursor = decode_canonical::<V1ToV2MigrationCursor>(&encoded).unwrap();
        drop(destination);
        destination = RepositoryV2::open(destination_plane.clone(), destination_options())
            .await
            .unwrap();
        let page = source
            .v1_to_v2_migration_page(&destination, &cursor, 100, 1)
            .await
            .unwrap();
        cursor = page.cursor;
        if page.complete {
            break;
        }
    }

    assert_eq!(cursor.migrated_commits, 6);
    assert_eq!(cursor.migrated_payloads, 4);
    assert!(cursor.mapped_head.is_some());
    assert!(source.list_retention_pins().await.unwrap().is_empty());
    assert!(
        source
            .v1_to_v2_migration_page(&destination, &cursor, 100, 1)
            .await
            .unwrap()
            .complete
    );
    let imported_feature = source
        .v1_to_v2_migration_mapping(&cursor, feature.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        destination
            .create_tag("imported-feature", imported_feature)
            .await
            .unwrap()
            .target,
        imported_feature
    );
    assert_eq!(
        destination
            .get_object("imported-main", b"docs/history.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"second"
    );
    assert_eq!(
        destination
            .get_object("imported-main", b"docs/other.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"other"
    );
    assert_eq!(
        destination
            .get_object("imported-main", b"docs/feature.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"from feature"
    );
    let imported_head = destination.head("imported-main").await.unwrap();
    let (versions, truncated) = destination
        .list_versions_at("imported-main", imported_head, b"docs/history", None, 100)
        .await
        .unwrap();
    assert!(!truncated);
    assert_eq!(versions.len(), 2);
    loop {
        let cleanup = source.cleanup_v1_to_v2_migration(&cursor, 1).await.unwrap();
        if cleanup.complete {
            break;
        }
    }

    let abandoned = source
        .start_v1_to_v2_migration(&destination, "main", "abandoned-import")
        .await
        .unwrap();
    let abandoned = source
        .v1_to_v2_migration_page(&destination, &abandoned, 100, 2)
        .await
        .unwrap()
        .cursor;
    loop {
        if source
            .abort_v1_to_v2_migration(&destination, &abandoned, 1)
            .await
            .unwrap()
            .complete
        {
            break;
        }
    }
    assert!(source.list_retention_pins().await.unwrap().is_empty());
    assert_eq!(
        destination.head("abandoned-import").await.unwrap_err().code,
        prolly_s3_core::ErrorCode::InvalidRevision
    );

    // Prove a completely cold reader resolves ancestor-packed nodes through
    // the imported durable index rather than process-local migration state.
    drop(destination);
    let cold = RepositoryV2::open(
        destination_plane,
        RepositoryV2Options {
            read_only: true,
            ..destination_options()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        cold.get_object("imported-main", b"docs/history.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"second"
    );
}
