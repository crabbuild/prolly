use std::{collections::BTreeMap, sync::Arc, time::Duration};

use prolly::TreeFormat;
use prolly_s3_core::{
    AuthorityScopeV2, AuthorityStampV2, BucketCommitV2, BucketDeltaV1, BucketStateV1,
    CommitGeneration, CommitIdV2, CommitPublicationV2, JournalDerivedIndexesV2, MemoryObjectPlane,
    NodePackEntryV1, NodePackV1, OperationId, RepositoryId, ShardWriterAuthorityV2,
    ShardedBranchPublisherV2, TreeFormatDigest, TreeRootV1,
};

fn operation(value: u128) -> OperationId {
    OperationId(uuid::Uuid::from_u128(value))
}

fn commit(
    authority: AuthorityStampV2,
    parents: Vec<CommitIdV2>,
    generation: u64,
    pack: Option<&NodePackV1>,
) -> BucketCommitV2 {
    let root = TreeRootV1 {
        root: None,
        format_digest: TreeFormatDigest::from_hash([0x51; 32]),
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
        node_pack: pack.map(|pack| pack.reference().unwrap()),
        authority,
        author: "writer-a".to_string(),
        message: Some(format!("generation {generation}")),
        created_at_millis: 1_000 + generation,
        metadata: BTreeMap::new(),
    }
}

fn pack(payload: &[u8]) -> NodePackV1 {
    let cid = prolly_s3_core::Cid::from_bytes(payload);
    NodePackV1 {
        format_digest: TreeFormatDigest::from_hash([0x51; 32]),
        entries: vec![NodePackEntryV1 {
            cid: cid.clone(),
            offset: 0,
            len: payload.len() as u32,
            sha256: cid.0,
        }],
        attachments: Vec::new(),
        payload: payload.to_vec(),
    }
}

#[tokio::test]
async fn node_and_graph_indexes_advance_only_from_the_branch_journal() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = RepositoryId::from_hash([0x52; 32]);
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
    let indexes = JournalDerivedIndexesV2::new(
        plane.clone(),
        ".prolly/v2",
        repository,
        TreeFormat::default(),
    )
    .unwrap();
    let permit = authority
        .acquire(
            AuthorityScopeV2::Branch {
                name: "main".to_string(),
            },
            "writer-a",
            1_000,
            operation(1),
        )
        .await
        .unwrap();
    let root = commit(permit.stamp(), Vec::new(), 0, None);
    let mut current = publisher
        .create(CommitPublicationV2 {
            permit: &permit,
            branch: "main",
            commit: &root,
            node_pack: None,
            operation: operation(2),
            message: "root",
            now_millis: 1_001,
        })
        .await
        .unwrap();
    let initialized = indexes.advance(&publisher, "main", 1_001).await.unwrap();
    assert!(initialized.initialized);
    assert_eq!(initialized.indexed_commits, 1);

    let first_pack = pack(b"first journal node");
    let first_cid = first_pack.entries[0].cid.clone();
    let first = commit(
        permit.stamp(),
        vec![current.value.target],
        1,
        Some(&first_pack),
    );
    current = publisher
        .store_and_publish(
            current,
            CommitPublicationV2 {
                permit: &permit,
                branch: "main",
                commit: &first,
                node_pack: Some(&first_pack),
                operation: operation(3),
                message: "first",
                now_millis: 1_002,
            },
        )
        .await
        .unwrap();
    let second_pack = pack(b"second journal node");
    let second_cid = second_pack.entries[0].cid.clone();
    let second = commit(
        permit.stamp(),
        vec![current.value.target],
        2,
        Some(&second_pack),
    );
    current = publisher
        .store_and_publish(
            current,
            CommitPublicationV2 {
                permit: &permit,
                branch: "main",
                commit: &second,
                node_pack: Some(&second_pack),
                operation: operation(4),
                message: "second",
                now_millis: 1_003,
            },
        )
        .await
        .unwrap();

    plane.reset_request_counts();
    let report = indexes.advance(&publisher, "main", 1_003).await.unwrap();
    let requests = plane.request_snapshot();
    assert_eq!(requests.list, 0, "journal maintenance must never LIST S3");
    assert_eq!(report.indexed_publications, 2);
    assert_eq!(report.indexed_commits, 2);
    assert_eq!(report.indexed_nodes, 2);
    assert_eq!(report.checkpoint, current.value.publication);

    let first_location = indexes
        .node_location("main", &first_cid)
        .await
        .unwrap()
        .unwrap();
    let second_location = indexes
        .node_location("main", &second_cid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_location.container, first.id().unwrap());
    assert_eq!(second_location.container, second.id().unwrap());
    let graph = indexes
        .commit_graph_entry("main", second.id().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(graph.parents, vec![first.id().unwrap()]);
    assert_eq!(graph.first_parent_jumps[0], first.id().unwrap());
    assert_eq!(graph.first_parent_jumps[1], root.id().unwrap());

    let no_op = indexes.advance(&publisher, "main", 1_004).await.unwrap();
    assert_eq!(no_op.indexed_publications, 0);
}

#[tokio::test]
async fn late_initialization_fails_closed_instead_of_scanning_commit_namespaces() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = RepositoryId::from_hash([0x53; 32]);
    let authority = Arc::new(
        ShardWriterAuthorityV2::new(
            plane.clone(),
            ".prolly/v2-late",
            repository,
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let publisher = ShardedBranchPublisherV2::new(
        plane.clone(),
        ".prolly/v2-late",
        repository,
        authority.clone(),
    )
    .unwrap();
    let indexes = JournalDerivedIndexesV2::new(
        plane.clone(),
        ".prolly/v2-late",
        repository,
        TreeFormat::default(),
    )
    .unwrap();
    let permit = authority
        .acquire(
            AuthorityScopeV2::Branch {
                name: "main".to_string(),
            },
            "writer-a",
            1_000,
            operation(10),
        )
        .await
        .unwrap();
    let root = commit(permit.stamp(), Vec::new(), 0, None);
    let current = publisher
        .create(CommitPublicationV2 {
            permit: &permit,
            branch: "main",
            commit: &root,
            node_pack: None,
            operation: operation(11),
            message: "root",
            now_millis: 1_001,
        })
        .await
        .unwrap();
    let child = commit(permit.stamp(), vec![current.value.target], 1, None);
    publisher
        .store_and_publish(
            current,
            CommitPublicationV2 {
                permit: &permit,
                branch: "main",
                commit: &child,
                node_pack: None,
                operation: operation(12),
                message: "child",
                now_millis: 1_002,
            },
        )
        .await
        .unwrap();

    plane.reset_request_counts();
    let error = indexes
        .advance(&publisher, "main", 1_002)
        .await
        .unwrap_err();
    assert_eq!(error.code, prolly_s3_core::ErrorCode::PreconditionFailed);
    assert_eq!(plane.request_snapshot().list, 0);
}
