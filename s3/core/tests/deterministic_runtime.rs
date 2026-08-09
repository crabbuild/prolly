use std::{collections::BTreeMap, sync::Arc};

use prolly_s3_core::{
    encode_canonical, CanonicalLimits, FixedClock, MemoryObjectPlane, ObjectHeaders, OperationId,
    Repository, RepositoryOptions, SequenceIdSource,
};
use uuid::Uuid;

fn deterministic_options(prefix: &str, clock: Arc<FixedClock>) -> RepositoryOptions {
    RepositoryOptions {
        repository_prefix: prefix.to_string(),
        writer: "determinism-fixture".to_string(),
        limits: CanonicalLimits {
            content_chunk_bytes: 7,
            ..CanonicalLimits::default()
        },
        clock,
        ids: Arc::new(SequenceIdSource::new(0xd37e_0000_0000_0001, 1)),
        ..RepositoryOptions::default()
    }
}

#[tokio::test]
async fn seeded_histories_are_identical_across_stores_and_restart() {
    const OPERATIONS: u128 = 10_000;
    let left_plane = Arc::new(MemoryObjectPlane::new(true));
    let right_plane = Arc::new(MemoryObjectPlane::new(false));
    let left_clock = Arc::new(FixedClock::new(1_725_000_000_000));
    let right_clock = Arc::new(FixedClock::new(1_725_000_000_000));
    let left_options = deterministic_options("deterministic-left", left_clock.clone());
    let right_options = deterministic_options("deterministic-right", right_clock.clone());
    let mut left = Repository::initialize(left_plane.clone(), left_options.clone())
        .await
        .unwrap();
    let right = Repository::initialize(right_plane, right_options)
        .await
        .unwrap();
    assert_eq!(left.repository_id(), right.repository_id());
    assert_eq!(
        left.head("main").await.unwrap(),
        right.head("main").await.unwrap()
    );

    let mut random = 0x6a09_e667_f3bc_c909_u64;
    for ordinal in 0..OPERATIONS {
        if ordinal > 0
            && ordinal % 1_000 == 0
            && std::env::var_os("PROLLY_S3_TRACE_PROGRESS").is_some()
        {
            eprintln!("deterministic corpus: {ordinal}/{OPERATIONS} operations");
        }
        random = random
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let key = format!("edge/κ/{:02}/\0suffix", random % 23).into_bytes();
        let operation = OperationId(Uuid::from_u128(
            0xa11c_e000_0000_0000_0000_0000_0000_0000 | ordinal,
        ));
        let timestamp = 1_725_000_001_000 + ordinal as u64;
        left_clock.set(timestamp);
        right_clock.set(timestamp);
        if random & 3 == 0 {
            left.delete_object("main", key.clone(), Some(operation))
                .await
                .unwrap();
            right
                .delete_object("main", key, Some(operation))
                .await
                .unwrap();
        } else {
            let bytes = format!("payload/{ordinal}/{random}").into_bytes();
            let metadata = BTreeMap::from([("ordinal".to_string(), ordinal.to_string())]);
            left.put_bytes(
                "main",
                key.clone(),
                bytes.clone(),
                ObjectHeaders::default(),
                metadata.clone(),
                Some(operation),
            )
            .await
            .unwrap();
            right
                .put_bytes(
                    "main",
                    key,
                    bytes,
                    ObjectHeaders::default(),
                    metadata,
                    Some(operation),
                )
                .await
                .unwrap();
        }
        assert_eq!(
            left.head("main").await.unwrap(),
            right.head("main").await.unwrap()
        );
        if ordinal % 127 == 63 {
            left = Repository::open(left_plane.clone(), left_options.clone())
                .await
                .unwrap();
        }
    }

    let head = left.head("main").await.unwrap();
    let left_commit = left.commit(head).await.unwrap();
    let right_commit = right.commit(head).await.unwrap();
    assert_eq!(left_commit, right_commit);
    assert_eq!(
        encode_canonical(&left_commit).unwrap(),
        encode_canonical(&right_commit).unwrap()
    );
    assert_eq!(
        left.list_versions_prefix("main", b"", 1_000).await.unwrap(),
        right
            .list_versions_prefix("main", b"", 1_000)
            .await
            .unwrap()
    );
    assert_eq!(left.fsck().await.unwrap(), right.fsck().await.unwrap());
}
