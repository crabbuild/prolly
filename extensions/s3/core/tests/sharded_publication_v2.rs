use std::{collections::BTreeMap, sync::Arc, time::Duration};

use prolly_s3_core::{
    AuthorityScopeV2, AuthorityStampV2, BucketCommitV2, BucketDeltaV2, BucketStateV2,
    CommitGeneration, CommitIdV2, CommitObjectV2, CommitPublicationV2, ErrorCode, GetRequest,
    ListRequest, MemoryObjectPlane, NodePackEntryV1, NodePackV1, ObjectPath, ObjectPlane,
    OperationId, RefGeneration, RepositoryId, ShardWriterAuthorityV2, ShardedBranchPublisherV2,
    TakeoverRequestV2, TreeFormatDigest, TreeRootV1,
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
        state: BucketStateV2 {
            objects: root.clone(),
            versions: root,
        },
        parents,
        generation: CommitGeneration(generation),
        delta: BucketDeltaV2 {
            input_digest: [0; 32],
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

async fn exact_version_count(plane: &MemoryObjectPlane, path: &ObjectPath) -> usize {
    let mut continuation = None;
    let mut count = 0;
    loop {
        let page = plane
            .list(ListRequest {
                prefix: path.as_str().to_string(),
                continuation,
                limit: 1_000,
                include_versions: true,
            })
            .await
            .unwrap();
        count += page
            .entries
            .iter()
            .filter(|entry| entry.path == *path)
            .count();
        continuation = page.continuation;
        if continuation.is_none() {
            return count;
        }
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
        ShardedBranchPublisherV2::new(plane.clone(), ".prolly/v2", repository, authority_a.clone())
            .unwrap();
    let publisher_b =
        ShardedBranchPublisherV2::new(plane.clone(), ".prolly/v2", repository, authority_b.clone())
            .unwrap();
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
    )
    .unwrap();
    let new_publisher =
        ShardedBranchPublisherV2::new(plane, ".prolly/v2", repository, new_authority.clone())
            .unwrap();
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
        ShardedBranchPublisherV2::new(plane.clone(), ".prolly/v2", repository, authority.clone())
            .unwrap();
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
        ShardedBranchPublisherV2::new(plane.clone(), ".prolly/v2", repository, authority.clone())
            .unwrap();
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

#[tokio::test]
async fn v2_lease_and_ref_updates_remain_within_the_same_control_version_bound() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = RepositoryId::from_hash([0xbb; 32]);
    let authority = Arc::new(
        ShardWriterAuthorityV2::new_with_control_retention(
            plane.clone(),
            ".prolly/v2",
            repository,
            Duration::from_secs(60),
            4,
        )
        .unwrap(),
    );
    let mut permit = authority
        .acquire(scope("main"), "writer-a", 1_000, operation(40))
        .await
        .unwrap();
    for now_millis in 1_001..1_013 {
        permit = authority.renew(permit, now_millis).await.unwrap();
    }
    let authority_path =
        ObjectPath::new(".prolly/v2/authority/v2/branches/6d61696e/lease.cbor").unwrap();
    assert!(exact_version_count(&plane, &authority_path).await <= 4);

    let publisher = ShardedBranchPublisherV2::new_with_control_retention(
        plane.clone(),
        ".prolly/v2",
        repository,
        authority.clone(),
        4,
    )
    .unwrap();
    let alternate_publisher = ShardedBranchPublisherV2::new_with_control_retention(
        plane.clone(),
        ".prolly/v2",
        repository,
        authority,
        4,
    )
    .unwrap();
    let mut root = commit(permit.stamp(), Vec::new(), 0, "root");
    root.author = "writer-a".to_string();
    let mut current = publisher
        .create(CommitPublicationV2 {
            permit: &permit,
            branch: "main",
            commit: &root,
            node_pack: None,
            operation: operation(41),
            message: "create main",
            now_millis: 1_020,
        })
        .await
        .unwrap();
    for generation in 1..13 {
        let mut next = commit(
            permit.stamp(),
            vec![current.value.target],
            generation,
            "advance",
        );
        next.author = "writer-a".to_string();
        let selected = if generation.is_multiple_of(2) {
            &publisher
        } else {
            &alternate_publisher
        };
        current = selected
            .store_and_publish(
                current,
                CommitPublicationV2 {
                    permit: &permit,
                    branch: "main",
                    commit: &next,
                    node_pack: None,
                    operation: operation(41 + u128::from(generation)),
                    message: "advance main",
                    now_millis: 1_020 + generation,
                },
            )
            .await
            .unwrap();
    }
    let ref_path = ObjectPath::new(".prolly/v2/refs/v2/heads/6d61696e").unwrap();
    assert!(exact_version_count(&plane, &ref_path).await <= 4);
}

#[tokio::test]
async fn publication_journal_pages_a_stable_branch_snapshot_without_listing() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = RepositoryId::from_hash([0xcc; 32]);
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
        ShardedBranchPublisherV2::new(plane, ".prolly/v2", repository, authority.clone()).unwrap();
    let permit = authority
        .acquire(scope("main"), "writer-a", 1_000, operation(100))
        .await
        .unwrap();
    let mut root = commit(permit.stamp(), Vec::new(), 0, "root");
    root.author = "writer-a".to_string();
    let mut current = publisher
        .create(CommitPublicationV2 {
            permit: &permit,
            branch: "main",
            commit: &root,
            node_pack: None,
            operation: operation(101),
            message: "root",
            now_millis: 1_001,
        })
        .await
        .unwrap();
    for generation in 1..=2 {
        let mut next = commit(
            permit.stamp(),
            vec![current.value.target],
            generation,
            "advance",
        );
        next.author = "writer-a".to_string();
        current = publisher
            .store_and_publish(
                current,
                CommitPublicationV2 {
                    permit: &permit,
                    branch: "main",
                    commit: &next,
                    node_pack: None,
                    operation: operation(101 + u128::from(generation)),
                    message: "advance",
                    now_millis: 1_001 + generation,
                },
            )
            .await
            .unwrap();
    }

    let snapshot = publisher.open_journal("main").await.unwrap();
    assert_eq!(snapshot.next_generation, Some(RefGeneration(2)));
    let snapshot_bytes = prolly_s3_core::encode_canonical(&snapshot).unwrap();
    let snapshot = prolly_s3_core::decode_canonical(&snapshot_bytes).unwrap();

    let mut next = commit(
        permit.stamp(),
        vec![current.value.target],
        3,
        "after snapshot",
    );
    next.author = "writer-a".to_string();
    let latest = publisher
        .store_and_publish(
            current,
            CommitPublicationV2 {
                permit: &permit,
                branch: "main",
                commit: &next,
                node_pack: None,
                operation: operation(104),
                message: "after snapshot",
                now_millis: 1_004,
            },
        )
        .await
        .unwrap();

    let first = publisher.read_journal_page(&snapshot, 2).await.unwrap();
    assert_eq!(
        first
            .entries
            .iter()
            .map(|entry| entry.event.generation)
            .collect::<Vec<_>>(),
        vec![RefGeneration(2), RefGeneration(1)]
    );
    let second = publisher
        .read_journal_page(first.continuation.as_ref().unwrap(), 2)
        .await
        .unwrap();
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].event.generation, RefGeneration(0));
    assert!(second.continuation.is_none());

    let latest_event = publisher
        .load_publication(latest.value.publication)
        .await
        .unwrap();
    assert!(latest_event.matches_ref(&latest.value).unwrap());
    assert_eq!(
        publisher
            .open_journal("main")
            .await
            .unwrap()
            .next_generation,
        Some(RefGeneration(3))
    );
}
