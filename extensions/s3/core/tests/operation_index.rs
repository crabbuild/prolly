use std::{collections::BTreeMap, sync::Arc, time::Duration};

use prolly_s3_core::{
    AuthorityScope, AuthorityStamp, BucketCommit, BucketDelta, BucketState, CommitGeneration,
    CommitId, CommitPublication, IdempotencyRetention, MemoryObjectPlane, ObjectPath, ObjectPlane,
    OperationId, OperationIndexHead, RefGeneration, RepositoryId, RootManifest,
    SegmentedOperationIndex, ShardWriterAuthority, ShardedBranchPublisher, TreeFormatDigest,
};

fn operation(value: u128) -> OperationId {
    OperationId(uuid::Uuid::from_u128(value))
}

fn commit(authority: AuthorityStamp, parents: Vec<CommitId>, generation: u64) -> BucketCommit {
    let root = RootManifest {
        root: None,
        format_digest: TreeFormatDigest::from_hash([0x31; 32]),
    };
    BucketCommit {
        state: BucketState {
            objects: root.clone(),
            versions: root,
        },
        parents,
        generation: CommitGeneration(generation),
        delta: BucketDelta {
            input_digest: [0; 32],
            changes: Vec::new(),
            changes_root: None,
            change_count: 0,
        },
        node_pack: None,
        authority,
        author: "writer-a".to_string(),
        message: Some(format!("generation {generation}")),
        created_at_millis: 1_000 + generation,
        metadata: BTreeMap::new(),
    }
}

#[tokio::test]
async fn branch_local_lsm_index_catches_up_tail_merges_segments_and_prunes_retention() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = RepositoryId::from_hash([0x41; 32]);
    let authority = Arc::new(
        ShardWriterAuthority::new(
            plane.clone(),
            ".prolly",
            repository,
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let publisher =
        ShardedBranchPublisher::new(plane.clone(), ".prolly", repository, authority.clone())
            .unwrap();
    let retention = IdempotencyRetention {
        max_generations: 3,
        max_age_millis: 60_000,
    };
    let index = SegmentedOperationIndex::new_with_limits(
        plane.clone(),
        ".prolly",
        repository,
        retention,
        2,
        2,
        32,
        8,
    )
    .unwrap();
    let permit = authority
        .acquire(
            AuthorityScope::Branch {
                name: "main".to_string(),
            },
            "writer-a",
            1_000,
            operation(1),
        )
        .await
        .unwrap();
    let root = commit(permit.stamp(), Vec::new(), 0);
    let mut current = publisher
        .create(CommitPublication {
            permit: &permit,
            branch: "main",
            commit: &root,
            node_pack: None,
            operation: operation(100),
            message: "root",
            now_millis: 1_000,
        })
        .await
        .unwrap();
    let initialized = index.advance(&publisher, "main", 1_000).await.unwrap();
    assert!(initialized.initialized);
    assert_eq!(initialized.indexed_events, 1);

    for generation in 1..=10 {
        let next = commit(permit.stamp(), vec![current.value.target], generation);
        current = publisher
            .store_and_publish(
                current,
                CommitPublication {
                    permit: &permit,
                    branch: "main",
                    commit: &next,
                    node_pack: None,
                    operation: operation(100 + u128::from(generation)),
                    message: "advance",
                    now_millis: 1_000 + generation,
                },
            )
            .await
            .unwrap();
        plane.reset_request_counts();
        assert!(index
            .lookup(
                &publisher,
                "main",
                operation(10_000 + u128::from(generation)),
                1_000 + generation,
            )
            .await
            .unwrap()
            .is_none());
        assert!(
            plane.request_snapshot().get <= 6,
            "one new journal event should require bounded lookup I/O"
        );
        if generation == 2 {
            let tail = index
                .lookup(&publisher, "main", operation(101), 1_002)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(tail.generation, RefGeneration(1));
        }
    }

    let advanced = index.advance(&publisher, "main", 1_010).await.unwrap();
    assert_eq!(advanced.indexed_events, 10);
    assert!(advanced.segments_written >= 3);
    for generation in 7..=10 {
        let found = index
            .lookup(
                &publisher,
                "main",
                operation(100 + u128::from(generation)),
                1_010,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.generation, RefGeneration(generation));
    }
    assert!(index
        .lookup(&publisher, "main", operation(100), 1_010)
        .await
        .unwrap()
        .is_none());
    assert!(index
        .lookup(&publisher, "main", operation(110), 61_011)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        index
            .advance(&publisher, "main", 1_010)
            .await
            .unwrap()
            .indexed_events,
        0
    );
    let head = plane
        .load_mutable(&ObjectPath::new(".prolly/operation-index/heads/6d61696e.cbor").unwrap())
        .await
        .unwrap()
        .unwrap();
    let head: OperationIndexHead = prolly_s3_core::decode_canonical(&head.bytes).unwrap();
    assert!(head.levels.len() <= 2);
    assert!(head.levels.iter().all(|level| level.len() < 2));
}
