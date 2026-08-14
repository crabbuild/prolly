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
        (
            "authority/branches/6d61696e/lease.cbor",
            MutableControlKind::AuthorityLease,
        ),
        (
            "authority/system/6763/lease.cbor",
            MutableControlKind::AuthorityLease,
        ),
        (
            "authority/maintenance/gate.cbor",
            MutableControlKind::MaintenanceGate,
        ),
        ("refs/heads/6d61696e", MutableControlKind::BranchRef),
        ("refs/tags/7631", MutableControlKind::TagRef),
        ("node-index/head.cbor", MutableControlKind::NodeIndexHead),
        ("ref-catalog/head.cbor", MutableControlKind::RefCatalogHead),
        (
            "ref-catalog/shards/0a/head.cbor",
            MutableControlKind::RefCatalogShardHead,
        ),
        (
            "commit-graph/head.cbor",
            MutableControlKind::CommitGraphHead,
        ),
        (
            "operation-index/heads/6d61696e.cbor",
            MutableControlKind::OperationIndexHead,
        ),
        (
            "journal-index/heads/6d61696e.cbor",
            MutableControlKind::JournalDerivedIndexHead,
        ),
        ("gc/coordinator.cbor", MutableControlKind::GcCoordinator),
        (
            "gc/epochs/00000000-0000-0000-0000-000000000001/cursor.cbor",
            MutableControlKind::GcCursor,
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
        "format/unregistered.cbor",
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
    let path = ObjectPath::new(".prolly/repository/authority/maintenance/gate.cbor").unwrap();
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
