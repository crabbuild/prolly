use prolly_s3_core::{
    encode_canonical, AuthorityLease, AuthorityLeaseState, AuthorityScope, AuthorityStamp,
    BucketCommit, BucketDelta, BucketState, CommitGeneration, CommitId, CommitObject, ErrorCode,
    NodePack, NodePackEntry, OperationId, PublicationEvent, RefGeneration, RefValue, ReflogEntry,
    RepositoryId, RootManifest, TreeFormatDigest,
};
use sha2::{Digest as _, Sha256};

fn lease() -> AuthorityLease {
    AuthorityLease {
        repository: RepositoryId::from_hash([0x11; 32]),
        scope: AuthorityScope::Branch {
            name: "main".to_string(),
        },
        generation: 2,
        writer_id: "writer-b".to_string(),
        fencing_token: [0x22; 32],
        state: AuthorityLeaseState::BarrierPending {
            previous_generation: 1,
        },
        expires_at_millis: 70_000,
        updated_at_millis: 10_000,
    }
}

#[test]
fn authority_records_have_frozen_canonical_encodings() {
    let lease = lease();
    let stamp = AuthorityStamp {
        repository: lease.repository,
        scope: lease.scope.clone(),
        generation: lease.generation,
        writer_id: lease.writer_id.clone(),
        fencing_token_digest: [0x33; 32],
    };
    let lease_hex = hex::encode(encode_canonical(&lease).unwrap());
    let stamp_hex = hex::encode(encode_canonical(&stamp).unwrap());
    assert_eq!(
        lease_hex,
        "a8009820111111111111111111111111111111111111111111111111111111111111111101a100a100646d61696e020203687772697465722d620498201822182218221822182218221822182218221822182218221822182218221822182218221822182218221822182218221822182218221822182218221822182205a101a10001061a0001117007192710"
    );
    assert_eq!(
        stamp_hex,
        "a5009820111111111111111111111111111111111111111111111111111111111111111101a100a100646d61696e020203687772697465722d6204982018331833183318331833183318331833183318331833183318331833183318331833183318331833183318331833183318331833183318331833183318331833"
    );
}

#[test]
fn publication_records_have_frozen_content_identities() {
    let lease = lease();
    let authority = lease.stamp();
    let target = CommitId::from_hash([0x55; 32]);
    let operation = OperationId(uuid::Uuid::from_u128(7));
    let reflog = ReflogEntry {
        branch: "main".to_string(),
        old_target: None,
        new_target: target,
        operation,
        actor: "writer-b".to_string(),
        message: "initialize".to_string(),
        created_at_millis: 10_000,
    };
    let publication = PublicationEvent {
        repository: lease.repository,
        branch: "main".to_string(),
        generation: RefGeneration(0),
        previous: None,
        old_target: None,
        new_target: target,
        operation,
        reflog: reflog.id().unwrap(),
        authority: authority.clone(),
        created_at_millis: 10_000,
    };
    let reference = RefValue {
        target,
        previous_target: None,
        generation: RefGeneration(0),
        operation,
        reflog: reflog.id().unwrap(),
        publication: publication.id().unwrap(),
        inline_reflog: reflog.clone(),
        authority: authority.clone(),
        updated_at_millis: 10_000,
        tombstone: false,
    };
    reference.validate(lease.repository, "main").unwrap();
    assert!(publication.matches_ref(&reference).unwrap());

    let root = RootManifest {
        root: None,
        format_digest: TreeFormatDigest::from_hash([0x44; 32]),
    };
    let commit = BucketCommit {
        state: BucketState {
            objects: root.clone(),
            versions: root,
        },
        parents: Vec::new(),
        generation: CommitGeneration(0),
        delta: BucketDelta {
            input_digest: [0; 32],
            changes: Vec::new(),
            changes_root: None,
            change_count: 0,
        },
        node_pack: None,
        authority: authority.clone(),
        author: "writer-b".to_string(),
        message: Some("initialize".to_string()),
        created_at_millis: 10_000,
        metadata: Default::default(),
    };
    commit.validate_authority(lease.repository, "main").unwrap();

    assert_eq!(
        reflog.id().unwrap().to_string(),
        "prl_62hxrlpfj3pjthyw6ef3t7p3ouc2lxonxs55zohuqzggqsn5d6tq"
    );
    assert_eq!(
        commit.id().unwrap().to_string(),
        "pbc_wp7bxsopbxi65drfkqv57g3aek37qyqf5fqu6ikhg4m2ggiuc33q"
    );
    assert_eq!(
        publication.id().unwrap().to_string(),
        "ppe_obv7p6wzvdznn3gqckrocmgh2pvltdq3ya7fxbnslnxoxiohv42a"
    );
    assert_eq!(
        hex::encode(Sha256::digest(encode_canonical(&reference).unwrap())),
        "6e03605be00ce52254192c7c8051b2102269b0dd90034e8841f00610917281bc"
    );
}

#[test]
fn commit_envelope_is_range_readable_and_rejects_invalid_magic() {
    let node = b"authority-stamped-node".to_vec();
    let cid = prolly_s3_core::Cid::from_bytes(&node);
    let pack = NodePack {
        format_digest: TreeFormatDigest::from_hash([0x44; 32]),
        entries: vec![NodePackEntry {
            cid: cid.clone(),
            offset: 0,
            len: node.len() as u32,
            sha256: cid.0,
        }],
        attachments: Vec::new(),
        payload: node,
    };
    let root = RootManifest {
        root: Some(cid),
        format_digest: pack.format_digest,
    };
    let mut commit = BucketCommit {
        state: BucketState {
            objects: root.clone(),
            versions: root,
        },
        parents: Vec::new(),
        generation: CommitGeneration(0),
        delta: BucketDelta {
            input_digest: [0; 32],
            changes: Vec::new(),
            changes_root: None,
            change_count: 0,
        },
        node_pack: None,
        authority: lease().stamp(),
        author: "writer-b".to_string(),
        message: Some("packed".to_string()),
        created_at_millis: 10_000,
        metadata: Default::default(),
    };
    commit.node_pack = Some(pack.reference().unwrap());
    let encoded = CommitObject::new(commit.clone(), Some(pack.clone()))
        .unwrap()
        .encode_object()
        .unwrap();
    let decoded = CommitObject::decode_object(&encoded).unwrap();

    assert_eq!(decoded.commit, commit);
    assert_eq!(decoded.node_pack, Some(pack));
    assert!(CommitObject::node_payload_offset(&encoded)
        .unwrap()
        .is_some());
    let mut invalid_magic = encoded.clone();
    invalid_magic[0] ^= 0xff;
    assert_eq!(
        CommitObject::decode_object(&invalid_magic)
            .unwrap_err()
            .code,
        ErrorCode::CorruptCommit
    );

    let mut external_delta = commit;
    external_delta.node_pack = None;
    external_delta.delta.changes_root = Some(external_delta.state.objects.clone());
    external_delta.delta.change_count = 1;
    CommitObject::new(external_delta.clone(), None).unwrap();

    external_delta.delta.changes_root.as_mut().unwrap().root = None;
    assert_eq!(
        CommitObject::new(external_delta, None).unwrap_err().code,
        ErrorCode::CorruptCommit
    );
}
