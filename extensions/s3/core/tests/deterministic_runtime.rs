use std::{collections::BTreeMap, sync::Arc};

use prolly_s3_core::{
    FixedClock, MemoryObjectPlane, ObjectHeaders, OperationId, Repository, RepositoryOptions,
    SequenceIdSource,
};
use uuid::Uuid;

fn deterministic_options(prefix: &str, clock: Arc<FixedClock>) -> RepositoryOptions {
    RepositoryOptions {
        repository_prefix: prefix.to_string(),
        writer: "determinism-fixture".to_string(),
        clock,
        ids: Arc::new(SequenceIdSource::new(0xd37e_0000_0000_0001, 1)),
        ..RepositoryOptions::default()
    }
}

#[tokio::test]
async fn logical_version_ids_are_stable_across_provider_sequences_and_restart() {
    let operations = std::env::var("PROLLY_S3_DETERMINISTIC_OPERATIONS")
        .ok()
        .map(|value| value.parse::<u128>().expect("valid operation count"))
        .unwrap_or(1_000);
    let left_plane = Arc::new(MemoryObjectPlane::new(true));
    let right_plane = Arc::new(MemoryObjectPlane::new(true));
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
    for ordinal in 0..operations {
        if ordinal > 0
            && ordinal % 1_000 == 0
            && std::env::var_os("PROLLY_S3_TRACE_PROGRESS").is_some()
        {
            eprintln!("deterministic corpus: {ordinal}/{operations} operations");
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
        let (left_receipt, right_receipt) = if random & 3 == 0 {
            let left_receipt = left
                .delete_object("main", key.clone(), Some(operation))
                .await
                .unwrap();
            let right_receipt = right
                .delete_object("main", key, Some(operation))
                .await
                .unwrap();
            (left_receipt, right_receipt)
        } else {
            let bytes = format!("payload/{ordinal}/{random}").into_bytes();
            let metadata = BTreeMap::from([("ordinal".to_string(), ordinal.to_string())]);
            let left_receipt = left
                .put_bytes(
                    "main",
                    key.clone(),
                    bytes.clone(),
                    ObjectHeaders::default(),
                    metadata.clone(),
                    Some(operation),
                )
                .await
                .unwrap();
            let right_receipt = right
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
            (left_receipt, right_receipt)
        };
        // Provider VersionIds are deliberately excluded from logical object
        // identity, so the logical history stays stable even when physical
        // version sequences diverge after a reopen.
        assert_eq!(left_receipt.object_versions, right_receipt.object_versions);
        if ordinal % 127 == 63 {
            let before_reopen = left.head("main").await.unwrap();
            left = Repository::open(left_plane.clone(), left_options.clone())
                .await
                .unwrap();
            assert_eq!(left.head("main").await.unwrap(), before_reopen);
        }
    }

    let left_fsck = left.fsck().await.unwrap();
    let right_fsck = right.fsck().await.unwrap();
    assert_eq!(left_fsck.branches, right_fsck.branches);
    assert_eq!(left_fsck.tags, right_fsck.tags);
    assert_eq!(left_fsck.commits, right_fsck.commits);
    assert_eq!(left_fsck.deltas, right_fsck.deltas);
    assert_eq!(left_fsck.reachable_nodes, right_fsck.reachable_nodes);
    assert_eq!(left_fsck.logical_versions, right_fsck.logical_versions);
    assert_eq!(
        left_fsck.content_bytes_verified,
        right_fsck.content_bytes_verified
    );
}
