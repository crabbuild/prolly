use std::{collections::BTreeMap, sync::Arc};

use prolly_s3_core::{
    decode_canonical, encode_canonical, FixedClock, HistoryTransferCursor, MemoryObjectPlane,
    MergePhase, MergePolicy, ObjectHeaders, ProviderPerKeyVersionLimit, Repository,
    RepositoryOptions, SequenceIdSource,
};

fn options(prefix: &str, writer: &str, seed: u64, clock: Arc<FixedClock>) -> RepositoryOptions {
    RepositoryOptions {
        repository_prefix: prefix.to_string(),
        writer: writer.to_string(),
        clock,
        ids: Arc::new(SequenceIdSource::new(seed, 1)),
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    }
}

async fn put(repository: &Repository<MemoryObjectPlane>, branch: &str, key: &str, value: &str) {
    repository
        .put_object(
            branch,
            key.as_bytes().to_vec(),
            value.as_bytes().to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn history_transfer_rebinds_payloads_and_preserves_merge_topology() {
    let source_clock = Arc::new(FixedClock::new(100_000));
    let destination_clock = Arc::new(FixedClock::new(200_000));
    let source = Repository::initialize(
        Arc::new(MemoryObjectPlane::new(true)),
        options(
            ".tests/history-transfer-source",
            "source-writer",
            0xc1,
            source_clock.clone(),
        ),
    )
    .await
    .unwrap();
    let destination = Repository::initialize(
        Arc::new(MemoryObjectPlane::new(false)),
        options(
            ".tests/history-transfer-destination",
            "destination-writer",
            0xd2,
            destination_clock,
        ),
    )
    .await
    .unwrap();

    source_clock.advance(1).unwrap();
    put(&source, "main", "base.txt", "base").await;
    let base = source.head("main").await.unwrap();
    source.create_branch("feature", base).await.unwrap();
    source_clock.advance(1).unwrap();
    put(&source, "main", "main.txt", "main").await;
    source_clock.advance(1).unwrap();
    put(&source, "feature", "feature.txt", "feature").await;

    let mut merge = source
        .start_merge("main", "feature", None, MergePolicy::Fail, "merge feature")
        .await
        .unwrap();
    while merge.phase != MergePhase::ReadyToPublish {
        merge = source.advance_merge(&merge, 1).await.unwrap().cursor;
    }
    let source_head = source.publish_merge(&merge).await.unwrap().id;
    source.advance_branch_indexes("main").await.unwrap();
    let destination_root = destination.head("main").await.unwrap();

    let mut transfer = destination
        .start_history_transfer_from(&source, "main", source_head, "main", destination_root)
        .await
        .unwrap();
    while !transfer.complete {
        let persisted = encode_canonical(&transfer).unwrap();
        let restored: HistoryTransferCursor = decode_canonical(&persisted).unwrap();
        let page = destination
            .advance_history_transfer_from(&source, &restored, 1)
            .await
            .unwrap();
        assert!(page.traversal_steps <= 1);
        assert!(page.mutation_steps <= 1);
        transfer = page.cursor;
    }
    assert!(transfer.report.imported_commits >= 5);
    assert!(transfer.report.copied_payloads >= 3);
    let mapped_head = transfer.mapped_head.unwrap();
    assert_ne!(mapped_head, source_head);
    destination
        .publish_history_transfer(&transfer, "publish imported history")
        .await
        .unwrap();
    assert_eq!(destination.head("main").await.unwrap(), mapped_head);

    for (key, value) in [
        ("base.txt", b"base".as_slice()),
        ("main.txt", b"main".as_slice()),
        ("feature.txt", b"feature".as_slice()),
    ] {
        assert_eq!(
            destination
                .get_object("main", key.as_bytes())
                .await
                .unwrap()
                .unwrap()
                .bytes,
            value
        );
    }

    let source_merge = source.commit(source_head).await.unwrap();
    let destination_merge = destination.commit(mapped_head).await.unwrap();
    assert_eq!(source_merge.parents.len(), 2);
    assert_eq!(destination_merge.parents.len(), 2);
    for (source_parent, destination_parent) in
        source_merge.parents.iter().zip(&destination_merge.parents)
    {
        assert_eq!(
            destination
                .history_transfer_mapping(&transfer, *source_parent)
                .await
                .unwrap()
                .unwrap()
                .destination,
            *destination_parent
        );
    }

    let mut closure = source.start_commit_closure(&[source_head]).await.unwrap();
    loop {
        let page = source.commit_closure_page(&closure, 1, 1).await.unwrap();
        for (source_id, source_commit) in page.commits {
            let mapped = destination
                .history_transfer_mapping(&transfer, source_id)
                .await
                .unwrap()
                .unwrap()
                .destination;
            let imported = destination.commit(mapped).await.unwrap();
            let mut mapped_parents = Vec::new();
            for source_parent in &source_commit.parents {
                mapped_parents.push(
                    destination
                        .history_transfer_mapping(&transfer, *source_parent)
                        .await
                        .unwrap()
                        .unwrap()
                        .destination,
                );
            }
            assert_eq!(imported.parents, mapped_parents);
            assert_eq!(imported.generation, source_commit.generation);
            assert_eq!(imported.message, source_commit.message);
            assert_eq!(imported.created_at_millis, source_commit.created_at_millis);
        }
        closure = page.cursor;
        if page.complete {
            break;
        }
    }

    let mut verification = source
        .start_backup_verification(&destination, "main", source_head, "main", mapped_head)
        .await
        .unwrap();
    while !verification.complete {
        verification = source
            .advance_backup_verification(&destination, &verification, 1)
            .await
            .unwrap()
            .cursor;
    }
    assert_eq!(verification.report.objects_verified, 3);
    assert_eq!(verification.report.content_bytes_verified, 15);
}
