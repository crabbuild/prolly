use std::sync::Arc;

use prolly_s3_core::{
    classify_mutable_control_path, CompareExchange, CompareExchangeOutcome, ErrorCode, ListRequest,
    MemoryObjectPlane, MutableControlKind, MutableControlStore, ObjectPath, ObjectPlane,
};

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
            break;
        }
    }
    count
}

#[test]
fn every_mutable_control_family_has_one_canonical_classification() {
    let prefix = ".prolly/repository";
    let cases = [
        ("writers/lease.cbor", MutableControlKind::WriterLeaseV1),
        (
            "authority/v2/branches/6d61696e/lease.cbor",
            MutableControlKind::AuthorityLeaseV2,
        ),
        (
            "authority/v2/system/6763/lease.cbor",
            MutableControlKind::AuthorityLeaseV2,
        ),
        (
            "authority/v2/maintenance/gate.cbor",
            MutableControlKind::MaintenanceGateV2,
        ),
        ("refs/heads/6d61696e", MutableControlKind::BranchRefV1),
        ("refs/v2/heads/6d61696e", MutableControlKind::BranchRefV2),
        ("refs/tags/7631", MutableControlKind::TagRefV1),
        (
            "retention/pins/6c6567616c",
            MutableControlKind::RetentionPinV1,
        ),
        (
            "node-index/latest.cbor",
            MutableControlKind::NodeIndexHeadV1,
        ),
        (
            "node-index/v2/head.cbor",
            MutableControlKind::NodeIndexHeadV2,
        ),
        (
            "ref-catalog/v2/head.cbor",
            MutableControlKind::RefCatalogHeadV2,
        ),
        (
            "commit-graph/v2/head.cbor",
            MutableControlKind::CommitGraphHeadV2,
        ),
        (
            "operation-index/v2/heads/6d61696e.cbor",
            MutableControlKind::OperationIndexHeadV2,
        ),
        ("gc/mark-runs/01.cbor", MutableControlKind::GcMarkRunV1),
        ("gc/runs/pgc1_x.cbor", MutableControlKind::GcRunV1),
        ("gc/v2/epochs/01/head.cbor", MutableControlKind::GcEpochV2),
        (
            "gc/v2/coordinator.cbor",
            MutableControlKind::GcCoordinatorV2,
        ),
    ];
    for (relative, expected) in cases {
        let path = ObjectPath::new(format!("{prefix}/{relative}")).unwrap();
        assert_eq!(
            classify_mutable_control_path(prefix, &path),
            Some(expected),
            "{relative}"
        );
    }
    for relative in [
        "format/v1.cbor",
        "commits/sha256/00/00/id",
        "reflogs/tags/7631/id",
        "probes/id/mutable",
    ] {
        let path = ObjectPath::new(format!("{prefix}/{relative}")).unwrap();
        assert_eq!(classify_mutable_control_path(prefix, &path), None);
    }
}

#[tokio::test]
async fn one_hot_control_key_pages_past_one_thousand_and_compacts_exactly() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let path = ObjectPath::new(".prolly/repository/writers/lease.cbor").unwrap();
    let mut expected = None;
    for generation in 0..1_105_u64 {
        let outcome = plane
            .compare_exchange(CompareExchange {
                path: path.clone(),
                expected,
                bytes: generation.to_be_bytes().to_vec(),
            })
            .await
            .unwrap();
        let CompareExchangeOutcome::Applied(metadata) = outcome else {
            panic!("single writer must apply");
        };
        expected = Some(metadata.token);
    }
    assert_eq!(exact_version_count(&plane, &path).await, 1_105);

    let controls = MutableControlStore::new(plane.clone(), ".prolly/repository", 8).unwrap();
    let report = controls.compact_path(&path).await.unwrap();
    assert_eq!(report.scanned, 1_105);
    assert_eq!(report.retained, 8);
    assert_eq!(report.deleted, 1_097);
    assert_eq!(exact_version_count(&plane, &path).await, 8);
    assert_eq!(
        plane.load_mutable(&path).await.unwrap().unwrap().bytes,
        1_104_u64.to_be_bytes()
    );

    let current = plane.load_mutable(&path).await.unwrap().unwrap();
    assert!(matches!(
        controls
            .compare_exchange(CompareExchange {
                path: path.clone(),
                expected: Some(current.metadata.token),
                bytes: 1_105_u64.to_be_bytes().to_vec(),
            })
            .await
            .unwrap(),
        CompareExchangeOutcome::Applied(_)
    ));
    assert!(exact_version_count(&plane, &path).await <= 8);
}

#[tokio::test]
async fn control_store_rejects_unregistered_paths() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let controls = MutableControlStore::new(plane, ".prolly/repository", 8).unwrap();
    let error = controls
        .compare_exchange(CompareExchange {
            path: ObjectPath::new(".prolly/repository/commits/immutable").unwrap(),
            expected: None,
            bytes: vec![1],
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidRequest);
}
