use std::collections::BTreeMap;

use prolly::{Cid, TreeFormat};
use prolly_s3_core::{
    decode_canonical, encode_canonical, BucketCommitV1, BucketDeltaV1, BucketStateV1,
    CanonicalLimits, CommitGeneration, CommitId, ErrorCode, ExclusiveWriterLeaseV1,
    LogicalObjectVersionBodyV1, LogicalObjectVersionKindV1, NodePackEntryV1, NodePackV1,
    ObjectHeaders, ObjectTransition, ObjectVersionOrder, ObjectVersionV1, OperationId,
    PhysicalObjectBindingV1, RefGeneration, RefValueV1, ReflogEntryV1, RepositoryFormatV1,
    RepositoryId, TreeFormatDigest, TreeRootV1,
};
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use uuid::Uuid;

const FIXTURE: &str = include_str!("../../spec/prolly-s3/v1/conformance/canonical-records.json");
// Keep the fixture embedded so any wire change is reviewed as source.
const CASES: &str = include_str!("../../spec/prolly-s3/v1/conformance/cases.json");

fn encoded<T: Serialize>(value: &T) -> String {
    hex::encode(encode_canonical(value).unwrap())
}

fn fixed_digest(byte: u8) -> [u8; 32] {
    [byte; 32]
}

fn actual_fixture() -> JsonValue {
    let repository = RepositoryId::from_hash(fixed_digest(0x11));
    let operation = OperationId(Uuid::from_bytes([
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ]));
    let tree_digest = TreeFormatDigest::from_hash(fixed_digest(0x22));
    let format = RepositoryFormatV1 {
        repository_id: repository,
        format_version: 1,
        state_tree_format: TreeFormat::default(),
        canonical_limits: CanonicalLimits::default(),
        min_reader_version: 1,
        min_writer_version: 1,
        created_at_millis: 1_700_000_000_123,
        required_capability_profile: 1,
    };
    let checksum = fixed_digest(0x33);
    let version = ObjectVersionV1::derive(
        repository,
        b"docs/readme.txt",
        operation,
        LogicalObjectVersionBodyV1 {
            order: ObjectVersionOrder {
                commit_generation: CommitGeneration(7),
                mutation_ordinal: 2,
            },
            created_at_millis: 1_700_000_000_456,
            kind: LogicalObjectVersionKindV1::Live {
                size: 5,
                logical_etag: "5d41402abc4b2a76b9719d911017c592".into(),
                headers: ObjectHeaders {
                    content_type: Some("text/plain".into()),
                    ..ObjectHeaders::default()
                },
                checksums: prolly_s3_core::Checksums {
                    md5: Some([0x5d; 16]),
                    sha256: Some(checksum),
                    algorithm_values: BTreeMap::new(),
                },
                user_metadata: BTreeMap::from([
                    ("aa".into(), "second".into()),
                    ("z".into(), "first".into()),
                ]),
                tags: BTreeMap::from([("env".into(), "test".into())]),
            },
        },
        PhysicalObjectBindingV1::Live {
            version_id: "provider-version-1".into(),
            provider_etag: "provider-etag-1".into(),
            checksum_sha256: checksum,
        },
    )
    .unwrap();
    let delta = BucketDeltaV1 {
        operation_ids: vec![operation],
        changes: vec![ObjectTransition {
            key: b"docs/readme.txt".to_vec(),
            previous: None,
            next: version.id,
            delete_marker: false,
        }],
    };
    let root = TreeRootV1 {
        root: None,
        format_digest: tree_digest,
    };
    let commit = BucketCommitV1 {
        state: BucketStateV1 {
            objects: root.clone(),
            versions: root.clone(),
            operations: root,
        },
        parents: vec![CommitId::from_hash(fixed_digest(0x44))],
        generation: CommitGeneration(7),
        delta,
        node_pack: None,
        writer_fence_generation: 3,
        author: "fixture".into(),
        message: Some("v1 canonical fixture".into()),
        created_at_millis: 1_700_000_000_789,
        metadata: BTreeMap::from([("aa".into(), vec![2]), ("z".into(), vec![1])]),
    };
    let commit_id = commit.id().unwrap();
    let reflog = ReflogEntryV1 {
        branch: "main".into(),
        old_target: None,
        new_target: commit_id,
        operation,
        actor: "fixture".into(),
        message: "publish".into(),
        created_at_millis: 1_700_000_000_800,
    };
    let reference = RefValueV1 {
        target: commit_id,
        previous_target: None,
        generation: RefGeneration(8),
        operation,
        reflog: reflog.id().unwrap(),
        inline_reflog: reflog,
        writer: "writer-a".into(),
        writer_fence_generation: 3,
        updated_at_millis: 1_700_000_000_801,
        tombstone: false,
    };
    let lease = ExclusiveWriterLeaseV1 {
        repository,
        writer_id: "writer-a".into(),
        generation: 3,
        fencing_token: fixed_digest(0x55),
        expires_at_millis: 1_700_000_060_000,
        updated_at_millis: 1_700_000_000_000,
    };

    json!({
        "schema": "prolly-s3-canonical-records/v1",
        "version": 1,
        "records": {
            "repository_format_v1": {"cbor_hex": encoded(&format)},
            "object_version_v1": {
                "cbor_hex": encoded(&version),
                "id": version.id.to_string()
            },
            "bucket_commit_v1": {
                "cbor_hex": encoded(&commit),
                "id": commit_id.to_string()
            },
            "ref_value_v1": {"cbor_hex": encoded(&reference)},
            "exclusive_writer_lease_v1": {"cbor_hex": encoded(&lease)}
        }
    })
}

#[test]
fn canonical_v1_records_are_frozen() {
    let actual = actual_fixture();
    if std::env::var_os("PROLLY_BLESS_V1").is_some() {
        println!("{}", serde_json::to_string_pretty(&actual).unwrap());
        return;
    }
    let expected: JsonValue = serde_json::from_str(FIXTURE).unwrap();
    assert_eq!(actual["schema"], expected["schema"]);
    assert_eq!(actual["version"], expected["version"]);
    for (name, actual_record) in actual["records"].as_object().unwrap() {
        let expected_record = &expected["records"][name];
        assert_ne!(expected_record, &JsonValue::Null, "missing fixture {name}");
        for (field, actual_value) in actual_record.as_object().unwrap() {
            assert_eq!(
                actual_value, &expected_record[field],
                "v1 fixture changed at {name}.{field}; a released v1 is immutable"
            );
        }
    }
}

#[test]
fn v1_decoder_rejects_all_negative_cbor_vectors() {
    let cases: JsonValue = serde_json::from_str(CASES).unwrap();
    for case in cases["invalid_cbor"].as_array().unwrap() {
        let bytes = hex::decode(case["hex"].as_str().unwrap()).unwrap();
        assert!(
            decode_canonical::<serde_cbor::Value>(&bytes).is_err(),
            "negative vector was accepted: {}",
            case["name"]
        );
    }
}

#[test]
fn every_protocol_default_is_v1() {
    assert_eq!(RepositoryFormatV1::VERSION, 1);
    assert_eq!(RepositoryFormatV1::PROLLY_S3_CAPABILITY_PROFILE, 1);
    assert_eq!(RepositoryFormatV1::PROLLY_S3_PROTOCOL_VERSION, 1);
    assert_eq!(RepositoryFormatV1::CURRENT_READER_VERSION, 1);
    assert_eq!(RepositoryFormatV1::CURRENT_WRITER_VERSION, 1);
    assert_eq!(CommitId::PREFIX, "pbc1_");
    assert_eq!(prolly_s3_core::ObjectVersionId::PREFIX, "pov1_");
}

#[test]
fn v1_node_pack_rejects_overlapping_payload_ranges() {
    let mut entries = [(0_u64, b"ab".as_slice()), (1_u64, b"b".as_slice())]
        .into_iter()
        .map(|(offset, bytes)| {
            let cid = Cid::from_bytes(bytes);
            NodePackEntryV1 {
                sha256: cid.as_bytes().try_into().unwrap(),
                cid,
                offset,
                len: u32::try_from(bytes.len()).unwrap(),
            }
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.cid.cmp(&right.cid));
    let pack = NodePackV1 {
        format_digest: TreeFormatDigest::from_hash(fixed_digest(0x22)),
        entries,
        attachments: Vec::new(),
        payload: b"ab".to_vec(),
    };
    assert_eq!(pack.validate().unwrap_err().code, ErrorCode::CorruptNode);
}
