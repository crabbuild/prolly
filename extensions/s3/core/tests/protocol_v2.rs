use prolly_s3_core::{
    encode_canonical, AuthorityLeaseStateV2, AuthorityLeaseV2, AuthorityScopeV2, AuthorityStampV2,
    BucketCommitV2, BucketDeltaV1, BucketStateV1, CommitGeneration, CommitIdV2, CommitObjectV1,
    CommitObjectV2, ErrorCode, NodePackEntryV1, NodePackV1, ObjectHeaders, OperationId,
    PhysicalMultipartSessionV2, PhysicalMutationIdentityV2, PublicationEventV2, RefGeneration,
    RefValueV2, ReflogEntryV2, RepositoryId, TreeFormatDigest, TreeRootV1,
};
use sha2::{Digest as _, Sha256};

fn lease() -> AuthorityLeaseV2 {
    AuthorityLeaseV2 {
        repository: RepositoryId::from_hash([0x11; 32]),
        scope: AuthorityScopeV2::Branch {
            name: "main".to_string(),
        },
        generation: 2,
        writer_id: "writer-b".to_string(),
        fencing_token: [0x22; 32],
        state: AuthorityLeaseStateV2::BarrierPending {
            previous_generation: 1,
        },
        expires_at_millis: 70_000,
        updated_at_millis: 10_000,
    }
}

#[test]
fn v2_authority_records_have_frozen_canonical_encodings() {
    let lease = lease();
    let stamp = AuthorityStampV2 {
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
fn v2_publication_records_have_frozen_content_identities() {
    let lease = lease();
    let authority = lease.stamp();
    let target = CommitIdV2::from_hash([0x55; 32]);
    let operation = OperationId(uuid::Uuid::from_u128(7));
    let reflog = ReflogEntryV2 {
        branch: "main".to_string(),
        old_target: None,
        new_target: target,
        operation,
        actor: "writer-b".to_string(),
        message: "initialize".to_string(),
        created_at_millis: 10_000,
    };
    let publication = PublicationEventV2 {
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
    let reference = RefValueV2 {
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

    let root = TreeRootV1 {
        root: None,
        format_digest: TreeFormatDigest::from_hash([0x44; 32]),
    };
    let commit = BucketCommitV2 {
        state: BucketStateV1 {
            objects: root.clone(),
            versions: root.clone(),
            operations: root,
        },
        parents: Vec::new(),
        generation: CommitGeneration(0),
        delta: BucketDeltaV1 {
            operation_ids: Vec::new(),
            changes: Vec::new(),
        },
        node_pack: None,
        authority: authority.clone(),
        author: "writer-b".to_string(),
        message: Some("initialize".to_string()),
        created_at_millis: 10_000,
        metadata: Default::default(),
    };
    commit.validate_authority(lease.repository, "main").unwrap();

    let multipart = PhysicalMultipartSessionV2 {
        identity: PhysicalMutationIdentityV2 {
            repository: lease.repository,
            operation,
            authority,
        },
        branch: "main".to_string(),
        key: b"large.bin".to_vec(),
        headers: ObjectHeaders::default(),
        user_metadata: Default::default(),
        provider_upload_id: "upload-1".to_string(),
        created_at_millis: 10_000,
        discovered: false,
    };
    multipart.validate(lease.repository).unwrap();

    assert_eq!(
        reflog.id().unwrap().to_string(),
        "prl2_wsbmbda2a72ivk7ng765y2nrt2lumfjvjqcgicab7bgdjokqf35q"
    );
    assert_eq!(
        commit.id().unwrap().to_string(),
        "pbc2_k5quoflouuch4crelfudtng432li2tenqar65viagucs3iuzompa"
    );
    assert_eq!(
        publication.id().unwrap().to_string(),
        "ppe2_fue4s3c3apahgop7zerujuxkjk5pjqnurr7psr4nwxnishz2pz4q"
    );
    assert_eq!(
        hex::encode(Sha256::digest(encode_canonical(&reference).unwrap())),
        "c4df5e499727013a5e5eba5403632a9931ddd467fa4acc0c1f98f5cb200c7d83"
    );
}

#[test]
fn v2_commit_envelope_is_range_readable_and_wire_separated_from_v1() {
    let node = b"authority-stamped-node".to_vec();
    let cid = prolly_s3_core::Cid::from_bytes(&node);
    let pack = NodePackV1 {
        format_digest: TreeFormatDigest::from_hash([0x44; 32]),
        entries: vec![NodePackEntryV1 {
            cid: cid.clone(),
            offset: 0,
            len: node.len() as u32,
            sha256: cid.0,
        }],
        attachments: Vec::new(),
        payload: node,
    };
    let root = TreeRootV1 {
        root: Some(cid),
        format_digest: pack.format_digest,
    };
    let mut commit = BucketCommitV2 {
        state: BucketStateV1 {
            objects: root.clone(),
            versions: root.clone(),
            operations: root,
        },
        parents: Vec::new(),
        generation: CommitGeneration(0),
        delta: BucketDeltaV1 {
            operation_ids: Vec::new(),
            changes: Vec::new(),
        },
        node_pack: None,
        authority: lease().stamp(),
        author: "writer-b".to_string(),
        message: Some("packed".to_string()),
        created_at_millis: 10_000,
        metadata: Default::default(),
    };
    commit.node_pack = Some(pack.reference().unwrap());
    let encoded = CommitObjectV2::new(commit.clone(), Some(pack.clone()))
        .unwrap()
        .encode_object()
        .unwrap();
    let decoded = CommitObjectV2::decode_object(&encoded).unwrap();

    assert_eq!(decoded.commit, commit);
    assert_eq!(decoded.node_pack, Some(pack));
    assert!(CommitObjectV2::node_payload_offset(&encoded)
        .unwrap()
        .is_some());
    assert_eq!(
        CommitObjectV1::decode_object(&encoded).unwrap_err().code,
        ErrorCode::CorruptCommit
    );
}
