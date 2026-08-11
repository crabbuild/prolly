use prolly::{Tree, TreeFormat};
use prolly_s3_core::{
    encode_canonical, tree_format_digest, BucketCommitV1, BucketDeltaV1, BucketStateV1,
    CanonicalLimits, CommitGeneration, ObjectTransition, ObjectVersionBodyV1, ObjectVersionKindV1,
    ObjectVersionOrder, ObjectVersionV1, OperationId, RefGeneration, RefValueV1, ReflogEntryV1,
    RepositoryFormatV1, RepositoryId, TreeRootV1,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

#[test]
fn canonical_v1_fixture_is_stable() {
    let repository = RepositoryId::from_hash([0x11; 32]);
    let operation = OperationId(Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap());
    let version = ObjectVersionV1::derive(
        repository,
        b"fixtures/object.txt",
        operation,
        ObjectVersionBodyV1 {
            order: ObjectVersionOrder {
                commit_generation: CommitGeneration(42),
                mutation_ordinal: 7,
            },
            created_at_millis: 1_725_000_000_123,
            kind: ObjectVersionKindV1::DeleteMarker,
        },
    )
    .unwrap();
    let delta = BucketDeltaV1 {
        operation_ids: vec![operation],
        changes: vec![ObjectTransition {
            key: b"fixtures/object.txt".to_vec(),
            previous: None,
            next: version.id,
            delete_marker: true,
        }],
    };
    let created_at_millis = 1_725_000_000_123;
    let format = RepositoryFormatV1 {
        repository_id: repository,
        format_version: RepositoryFormatV1::VERSION,
        state_tree_format: TreeFormat::default(),
        content_index_format: TreeFormat::default(),
        canonical_limits: CanonicalLimits::default(),
        min_reader_version: RepositoryFormatV1::DISTRIBUTED_PROTOCOL_VERSION,
        min_writer_version: RepositoryFormatV1::DISTRIBUTED_PROTOCOL_VERSION,
        created_at_millis,
        required_capability_profile: RepositoryFormatV1::DISTRIBUTED_S3_CAPABILITY_PROFILE,
    };
    let empty_root = TreeRootV1::from_tree(&Tree::default()).unwrap();
    let state = BucketStateV1 {
        objects: empty_root.clone(),
        versions: empty_root.clone(),
        operations: empty_root,
    };
    let initial_delta = BucketDeltaV1 {
        operation_ids: Vec::new(),
        changes: Vec::new(),
    };
    let initial_delta_id = initial_delta.id().unwrap();
    let initial_commit = BucketCommitV1 {
        state,
        parents: Vec::new(),
        generation: CommitGeneration(0),
        delta: initial_delta_id,
        author: "fixture-writer".to_string(),
        message: Some("initialize versioned S3 repository".to_string()),
        created_at_millis,
        metadata: BTreeMap::new(),
        native: None,
    };
    let initial_commit_id = initial_commit.id().unwrap();
    let initial_reflog = ReflogEntryV1 {
        branch: "main".to_string(),
        old_target: None,
        new_target: initial_commit_id,
        operation,
        actor: "fixture-writer".to_string(),
        message: "initialize".to_string(),
        created_at_millis,
    };
    let initial_reflog_id = initial_reflog.id().unwrap();
    let initial_ref = RefValueV1 {
        target: initial_commit_id,
        previous_target: None,
        generation: RefGeneration(0),
        operation,
        reflog: initial_reflog_id,
        writer: "fixture-writer".to_string(),
        updated_at_millis: created_at_millis,
        tombstone: false,
        native: None,
    };
    let actual = json!({
        "schema": "prolly-s3-canonical-fixtures/v1",
        "repository_format_cbor_hex": hex::encode(encode_canonical(&format).unwrap()),
        "initial_state_cbor_hex": hex::encode(encode_canonical(&initial_commit.state).unwrap()),
        "initial_delta_cbor_hex": hex::encode(encode_canonical(&initial_delta).unwrap()),
        "initial_delta_id": initial_delta_id.to_string(),
        "initial_commit_cbor_hex": hex::encode(encode_canonical(&initial_commit).unwrap()),
        "initial_commit_id": initial_commit_id.to_string(),
        "initial_reflog_cbor_hex": hex::encode(encode_canonical(&initial_reflog).unwrap()),
        "initial_reflog_id": initial_reflog_id.to_string(),
        "initial_ref_cbor_hex": hex::encode(encode_canonical(&initial_ref).unwrap()),
        "object_version_cbor_hex": hex::encode(encode_canonical(&version).unwrap()),
        "object_version_id": version.id.to_string(),
        "delta_cbor_hex": hex::encode(encode_canonical(&delta).unwrap()),
        "delta_id": delta.id().unwrap().to_string(),
        "tree_format_digest": tree_format_digest(&TreeFormat::default()).unwrap().to_string(),
    });
    let expected: Value = serde_json::from_str(include_str!("../../fixtures/canonical-v1.json"))
        .expect("fixture JSON is valid");
    if actual != expected {
        eprintln!("{}", serde_json::to_string_pretty(&actual).unwrap());
    }
    assert_eq!(actual, expected);
}
