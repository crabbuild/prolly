use std::{collections::BTreeMap, sync::Arc, time::Duration};

use prolly_s3_core::{
    AuthorityScopeV2, AuthorityStampV2, BucketCommitV2, BucketDeltaV1, BucketStateV1,
    CommitGeneration, CommitIdV2, CommitObjectV2, CommitPublicationV2, ErrorCode, GetRequest,
    MemoryObjectPlane, NodePackEntryV1, NodePackV1, ObjectPath, ObjectPlane, OperationId,
    RepositoryId, ShardWriterAuthorityV2, ShardedBranchPublisherV2, TakeoverRequestV2,
    TreeFormatDigest, TreeRootV1,
};

fn operation(value: u128) -> OperationId {
    OperationId(uuid::Uuid::from_u128(value))
}

fn scope(branch: &str) -> AuthorityScopeV2 {
    AuthorityScopeV2::Branch {
        name: branch.to_string(),
    }
}

fn commit(
    authority: AuthorityStampV2,
    parents: Vec<CommitIdV2>,
    generation: u64,
    message: &str,
) -> BucketCommitV2 {
    let root = TreeRootV1 {
        root: None,
        format_digest: TreeFormatDigest::from_hash([0x66; 32]),
    };
    BucketCommitV2 {
        state: BucketStateV1 {
            objects: root.clone(),
            versions: root.clone(),
            operations: root,
        },
        parents,
        generation: CommitGeneration(generation),
        delta: BucketDeltaV1 {
            operation_ids: Vec::new(),
            changes: Vec::new(),
        },
        node_pack: None,
        authority,
        author: String::new(),
        message: Some(message.to_string()),
        created_at_millis: 1_000 + generation,
        metadata: BTreeMap::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn separate_writers_publish_independent_branch_shards_concurrently() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = RepositoryId::from_hash([0x77; 32]);
    let authority_a = Arc::new(
        ShardWriterAuthorityV2::new(
            plane.clone(),
            ".prolly/v2",
            repository,
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let authority_b = Arc::new(
        ShardWriterAuthorityV2::new(
            plane.clone(),
            ".prolly/v2",
            repository,
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let publisher_a =
        ShardedBranchPublisherV2::new(plane.clone(), ".prolly/v2", repository, authority_a.clone());
    let publisher_b =
        ShardedBranchPublisherV2::new(plane.clone(), ".prolly/v2", repository, authority_b.clone());
    let (main_permit, ingest_permit) = tokio::join!(
        authority_a.acquire(scope("main"), "writer-a", 1_000, operation(1)),
        authority_b.acquire(scope("ingest"), "writer-b", 1_000, operation(2)),
    );
    let main_permit = main_permit.unwrap();
    let ingest_permit = ingest_permit.unwrap();
    let mut main_root = commit(main_permit.stamp(), Vec::new(), 0, "main root");
    main_root.author = "writer-a".to_string();
    let mut ingest_root = commit(ingest_permit.stamp(), Vec::new(), 0, "ingest root");
    ingest_root.author = "writer-b".to_string();

    plane.set_compare_exchange_delay_millis(25);
    plane.reset_compare_exchange_concurrency();
    let (main, ingest) = tokio::join!(
        publisher_a.create(CommitPublicationV2 {
            permit: &main_permit,
            branch: "main",
            commit: &main_root,
            node_pack: None,
            operation: operation(3),
            message: "create main",
            now_millis: 1_001,
        }),
        publisher_b.create(CommitPublicationV2 {
            permit: &ingest_permit,
            branch: "ingest",
            commit: &ingest_root,
            node_pack: None,
            operation: operation(4),
            message: "create ingest",
            now_millis: 1_001,
        }),
    );
    let main = main.unwrap();
    let ingest = ingest.unwrap();
    assert_eq!(plane.max_compare_exchanges_in_flight(), 2);
    assert_eq!(main.value.authority.writer_id, "writer-a");
    assert_eq!(ingest.value.authority.writer_id, "writer-b");

    let mut main_child = commit(
        main_permit.stamp(),
        vec![main.value.target],
        1,
        "main child",
    );
    main_child.author = "writer-a".to_string();
    let mut ingest_child = commit(
        ingest_permit.stamp(),
        vec![ingest.value.target],
        1,
        "ingest child",
    );
    ingest_child.author = "writer-b".to_string();
    plane.reset_compare_exchange_concurrency();
    let (main, ingest) = tokio::join!(
        publisher_a.store_and_publish(
            main,
            CommitPublicationV2 {
                permit: &main_permit,
                branch: "main",
                commit: &main_child,
                node_pack: None,
                operation: operation(5),
                message: "publish main",
                now_millis: 1_002,
            },
        ),
        publisher_b.store_and_publish(
            ingest,
            CommitPublicationV2 {
                permit: &ingest_permit,
                branch: "ingest",
                commit: &ingest_child,
                node_pack: None,
                operation: operation(6),
                message: "publish ingest",
                now_millis: 1_002,
            },
        ),
    );
    assert_eq!(plane.max_compare_exchanges_in_flight(), 2);
    assert_eq!(main.unwrap().value.target, main_child.id().unwrap());
    assert_eq!(ingest.unwrap().value.target, ingest_child.id().unwrap());
}

#[tokio::test]
async fn branch_barrier_fences_the_old_writer_and_activates_the_new_writer() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = RepositoryId::from_hash([0x88; 32]);
    let old_authority = Arc::new(
        ShardWriterAuthorityV2::new(
            plane.clone(),
            ".prolly/v2",
            repository,
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let new_authority = Arc::new(
        ShardWriterAuthorityV2::new(
            plane.clone(),
            ".prolly/v2",
            repository,
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let old_publisher = ShardedBranchPublisherV2::new(
        plane.clone(),
        ".prolly/v2",
        repository,
        old_authority.clone(),
    );
    let new_publisher =
        ShardedBranchPublisherV2::new(plane, ".prolly/v2", repository, new_authority.clone());
    let old_permit = old_authority
        .acquire(scope("main"), "writer-a", 1_000, operation(10))
        .await
        .unwrap();
    let mut root = commit(old_permit.stamp(), Vec::new(), 0, "root");
    root.author = "writer-a".to_string();
    let current = old_publisher
        .create(CommitPublicationV2 {
            permit: &old_permit,
            branch: "main",
            commit: &root,
            node_pack: None,
            operation: operation(11),
            message: "create main",
            now_millis: 1_001,
        })
        .await
        .unwrap();
    let pending = new_authority
        .begin_takeover(TakeoverRequestV2 {
            scope: scope("main"),
            expected_writer: "writer-a".to_string(),
            expected_generation: 1,
            next_writer: "writer-b".to_string(),
            handoff_evidence: "old credentials revoked".to_string(),
            now_millis: 2_000,
            nonce: operation(12),
        })
        .await
        .unwrap();
    let applied = new_publisher
        .publish_takeover_barrier(
            "main",
            current,
            &pending,
            operation(13),
            "take over main",
            2_001,
        )
        .await
        .unwrap();
    let barrier_ref = applied.reference.clone();
    let new_permit = new_authority
        .activate_after_barrier(pending, applied.into_barrier(), 2_002)
        .await
        .unwrap();

    let mut stale_child = commit(
        old_permit.stamp(),
        vec![barrier_ref.value.target],
        1,
        "stale",
    );
    stale_child.author = "writer-a".to_string();
    let stale = old_publisher
        .store_and_publish(
            barrier_ref.clone(),
            CommitPublicationV2 {
                permit: &old_permit,
                branch: "main",
                commit: &stale_child,
                node_pack: None,
                operation: operation(14),
                message: "stale publish",
                now_millis: 2_003,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(stale.code, ErrorCode::PreconditionFailed);

    let mut child = commit(
        new_permit.stamp(),
        vec![barrier_ref.value.target],
        1,
        "new writer",
    );
    child.author = "writer-b".to_string();
    let published = new_publisher
        .store_and_publish(
            barrier_ref,
            CommitPublicationV2 {
                permit: &new_permit,
                branch: "main",
                commit: &child,
                node_pack: None,
                operation: operation(15),
                message: "publish after takeover",
                now_millis: 2_004,
            },
        )
        .await
        .unwrap();
    assert_eq!(published.value.target, child.id().unwrap());
    assert_eq!(published.value.authority.writer_id, "writer-b");
}

#[tokio::test]
async fn applied_ref_cas_with_lost_response_reconciles_by_exact_value() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = RepositoryId::from_hash([0x99; 32]);
    let authority = Arc::new(
        ShardWriterAuthorityV2::new(
            plane.clone(),
            ".prolly/v2",
            repository,
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let publisher =
        ShardedBranchPublisherV2::new(plane.clone(), ".prolly/v2", repository, authority.clone());
    let permit = authority
        .acquire(scope("main"), "writer-a", 1_000, operation(20))
        .await
        .unwrap();
    let mut root = commit(permit.stamp(), Vec::new(), 0, "root");
    root.author = "writer-a".to_string();

    plane.conflict_after_next_compare_exchange();
    let created = publisher
        .create(CommitPublicationV2 {
            permit: &permit,
            branch: "main",
            commit: &root,
            node_pack: None,
            operation: operation(21),
            message: "create main",
            now_millis: 1_001,
        })
        .await
        .unwrap();

    assert_eq!(created.value.target, root.id().unwrap());
    assert_eq!(publisher.load("main").await.unwrap().value, created.value);
}

#[tokio::test]
async fn publication_stores_real_prolly_nodes_in_the_v2_commit_envelope() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = RepositoryId::from_hash([0xaa; 32]);
    let authority = Arc::new(
        ShardWriterAuthorityV2::new(
            plane.clone(),
            ".prolly/v2",
            repository,
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let publisher =
        ShardedBranchPublisherV2::new(plane.clone(), ".prolly/v2", repository, authority.clone());
    let permit = authority
        .acquire(scope("main"), "writer-a", 1_000, operation(30))
        .await
        .unwrap();
    let node = b"prolly-root-node".to_vec();
    let cid = prolly_s3_core::Cid::from_bytes(&node);
    let pack = NodePackV1 {
        format_digest: TreeFormatDigest::from_hash([0x66; 32]),
        entries: vec![NodePackEntryV1 {
            cid: cid.clone(),
            offset: 0,
            len: node.len() as u32,
            sha256: cid.0,
        }],
        attachments: Vec::new(),
        payload: node,
    };
    let mut root = commit(permit.stamp(), Vec::new(), 0, "packed root");
    root.author = "writer-a".to_string();
    root.state.objects.root = Some(cid);
    root.node_pack = Some(pack.reference().unwrap());

    let created = publisher
        .create(CommitPublicationV2 {
            permit: &permit,
            branch: "main",
            commit: &root,
            node_pack: Some(&pack),
            operation: operation(31),
            message: "create packed main",
            now_millis: 1_001,
        })
        .await
        .unwrap();
    let encoded_id = hex::encode(created.value.target.as_bytes());
    let stored = plane
        .get(GetRequest {
            path: ObjectPath::new(format!(
                ".prolly/v2/commits/v2/sha256/{}/{}/{}",
                &encoded_id[..2],
                &encoded_id[2..4],
                encoded_id
            ))
            .unwrap(),
            range: None,
            physical_version: None,
        })
        .await
        .unwrap()
        .unwrap();
    let envelope = CommitObjectV2::decode_object(&stored.bytes).unwrap();

    assert_eq!(envelope.commit, root);
    assert_eq!(envelope.node_pack, Some(pack));
}
