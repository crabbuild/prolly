use std::{collections::BTreeMap, sync::Arc};

use prolly_s3_core::{
    Checksums, CommitGeneration, ErrorCode, ImmutablePayloadStore, ListRequest,
    LogicalObjectVersionBody, LogicalObjectVersionKind, MemoryObjectPlane, ObjectHeaders,
    ObjectPlane, ObjectVersion, ObjectVersionOrder, OperationId, ProviderPerKeyVersionLimit,
    RepositoryId,
};

fn live_body(checksum_sha256: [u8; 32], size: u64) -> LogicalObjectVersionBody {
    LogicalObjectVersionBody {
        order: ObjectVersionOrder {
            commit_generation: CommitGeneration(1),
            mutation_ordinal: 0,
        },
        created_at_millis: 1_000,
        kind: LogicalObjectVersionKind::Live {
            size,
            logical_etag: "\"etag\"".to_string(),
            headers: ObjectHeaders::default(),
            checksums: Checksums {
                md5: None,
                sha256: Some(checksum_sha256),
                algorithm_values: BTreeMap::new(),
            },
            user_metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
        },
    }
}

#[tokio::test]
async fn repeated_hot_key_content_uses_one_immutable_physical_version() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = RepositoryId::from_hash([0x31; 32]);
    let store = ImmutablePayloadStore::new(plane.clone(), ".prolly", repository);
    let bytes = b"same logical hot-key payload".to_vec();
    let first = store.put(bytes.clone()).await.unwrap();
    for _ in 0..250 {
        assert_eq!(store.put(bytes.clone()).await.unwrap(), first);
    }
    assert_eq!(store.get(&first).await.unwrap(), bytes);

    let page = plane
        .list(ListRequest {
            prefix: first.path.as_str().to_string(),
            continuation: None,
            limit: 1_000,
            include_versions: true,
        })
        .await
        .unwrap();
    assert_eq!(
        page.entries
            .iter()
            .filter(|entry| entry.path == first.path)
            .count(),
        1
    );
    assert!(page.continuation.is_none());
}

#[tokio::test]
async fn distinct_hot_key_history_spreads_across_one_version_payload_keys() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = RepositoryId::from_hash([0x32; 32]);
    let store = ImmutablePayloadStore::new(plane.clone(), ".prolly", repository);
    let mut paths = std::collections::BTreeSet::new();
    for generation in 0..256_u32 {
        let binding = store.put(generation.to_be_bytes().to_vec()).await.unwrap();
        assert!(paths.insert(binding.path));
    }

    let prefix = format!(".prolly/payloads/{}/", hex::encode(repository.as_bytes()));
    let page = plane
        .list(ListRequest {
            prefix,
            continuation: None,
            limit: 1_000,
            include_versions: true,
        })
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 256);
    assert!(page.entries.iter().all(|entry| entry.is_latest));
}

#[test]
fn logical_delete_has_no_physical_binding_and_provider_limits_fail_closed() {
    let repository = RepositoryId::from_hash([0x33; 32]);
    let operation = OperationId(uuid::Uuid::from_u128(1));
    let delete = LogicalObjectVersionBody {
        order: ObjectVersionOrder {
            commit_generation: CommitGeneration(2),
            mutation_ordinal: 0,
        },
        created_at_millis: 2_000,
        kind: LogicalObjectVersionKind::DeleteMarker,
    };
    ObjectVersion::derive(repository, b"hot.txt", operation, delete, None).unwrap();

    assert!(ProviderPerKeyVersionLimit::Unlimited
        .validate_immutable_payload_profile(100)
        .is_ok());
    assert!(ProviderPerKeyVersionLimit::Finite(102)
        .validate_immutable_payload_profile(100)
        .is_ok());
    assert_eq!(
        ProviderPerKeyVersionLimit::Finite(101)
            .validate_immutable_payload_profile(100)
            .unwrap_err()
            .code,
        ErrorCode::ProviderNotQualified
    );
    assert_eq!(
        ProviderPerKeyVersionLimit::Unknown
            .validate_immutable_payload_profile(100)
            .unwrap_err()
            .code,
        ErrorCode::ProviderNotQualified
    );
}

#[tokio::test]
async fn object_version_binds_the_immutable_payload_checksum() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = RepositoryId::from_hash([0x34; 32]);
    let store = ImmutablePayloadStore::new(plane, ".prolly", repository);
    let binding = store.put(b"bound".to_vec()).await.unwrap();
    let version = ObjectVersion::derive(
        repository,
        b"hot.txt",
        OperationId(uuid::Uuid::from_u128(2)),
        live_body(binding.checksum_sha256, 5),
        Some(binding),
    )
    .unwrap();
    version.validate().unwrap();
}
