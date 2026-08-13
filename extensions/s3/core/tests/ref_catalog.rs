use std::sync::Arc;

use prolly::TreeFormat;
use prolly_s3_core::{
    ref_catalog_shard, CommitId, MemoryObjectPlane, OperationId, RefGeneration, RefKind,
    RepositoryId, ShardedRefCatalog,
};

fn operation(value: u128) -> OperationId {
    OperationId(uuid::Uuid::from_u128(value))
}

#[tokio::test]
async fn ref_catalog_shards_merge_concurrent_events_and_reject_regression() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = RepositoryId::from_hash([0x31; 32]);
    let catalog = Arc::new(
        ShardedRefCatalog::new(
            plane.clone(),
            ".tests/ref-catalog-",
            repository,
            TreeFormat::default(),
        )
        .unwrap(),
    );
    let first = "branch-0".to_string();
    let shard = ref_catalog_shard(RefKind::Branch, &first);
    let second = (1..10_000)
        .map(|ordinal| format!("branch-{ordinal}"))
        .find(|name| ref_catalog_shard(RefKind::Branch, name) == shard)
        .unwrap();
    let left = {
        let catalog = catalog.clone();
        let first = first.clone();
        tokio::spawn(async move {
            catalog
                .record(
                    RefKind::Branch,
                    &first,
                    CommitId::from_hash([0x41; 32]),
                    RefGeneration(0),
                    operation(1),
                    false,
                    1_000,
                )
                .await
        })
    };
    let right = {
        let catalog = catalog.clone();
        let second = second.clone();
        tokio::spawn(async move {
            catalog
                .record(
                    RefKind::Branch,
                    &second,
                    CommitId::from_hash([0x42; 32]),
                    RefGeneration(0),
                    operation(2),
                    false,
                    1_001,
                )
                .await
        })
    };
    let left = left.await.unwrap().unwrap();
    let right = right.await.unwrap().unwrap();
    let left_event = catalog.load_event(left.event).await.unwrap();
    let right_event = catalog.load_event(right.event).await.unwrap();
    assert!(
        left_event.previous == Some(right.event)
            || right_event.previous == Some(left.event)
            || left.event == right.event,
        "successful same-shard updates must form one linked history"
    );

    let page = catalog.list(RefKind::Branch, None, 100).await.unwrap();
    let mut names = page
        .entries
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    names.sort();
    let mut expected = vec![first.clone(), second];
    expected.sort();
    assert_eq!(names, expected);

    catalog
        .record(
            RefKind::Branch,
            &first,
            CommitId::from_hash([0x51; 32]),
            RefGeneration(1),
            operation(3),
            false,
            2_000,
        )
        .await
        .unwrap();
    let stale = catalog
        .record(
            RefKind::Branch,
            &first,
            CommitId::from_hash([0x41; 32]),
            RefGeneration(0),
            operation(1),
            false,
            1_000,
        )
        .await
        .unwrap();
    assert!(stale.already_indexed);
    let current = catalog
        .list(RefKind::Branch, None, 100)
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.name == first)
        .unwrap();
    assert_eq!(current.target, CommitId::from_hash([0x51; 32]));
    assert_eq!(current.generation, RefGeneration(1));
}
