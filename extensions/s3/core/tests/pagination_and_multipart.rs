use std::{collections::BTreeMap, sync::Arc};

use futures_util::StreamExt;
use prolly_s3_core::{
    ChecksumExpectation, ErrorCode, EtagPredicateV1, FixedClock, MemoryObjectPlane,
    MultipartStateV1, ObjectHeaders, ObjectVersionKindV1, ObjectWriteConditionV1, OperationId,
    Repository, RepositoryOptions, SequenceIdSource,
};
use sha2::{Digest, Sha256};

fn options(prefix: &str, clock: Arc<FixedClock>) -> RepositoryOptions {
    RepositoryOptions {
        repository_prefix: prefix.to_string(),
        clock,
        ids: Arc::new(SequenceIdSource::new(0xface_0000_0000_0001, 1)),
        multipart_upload_ttl_millis: 100,
        ..RepositoryOptions::default()
    }
}

#[tokio::test]
async fn all_bounded_cursors_are_exclusive_stable_and_complete() {
    let clock = Arc::new(FixedClock::new(10_000));
    let repository = Repository::initialize(
        Arc::new(MemoryObjectPlane::new(true)),
        options("pagination-corpus", clock),
    )
    .await
    .unwrap();

    let root = repository.head("main").await.unwrap();
    for ordinal in 0..41 {
        let key = match ordinal {
            0 => "edge/\0nul".to_string(),
            1 => "edge/κ/雪".to_string(),
            _ => format!("edge/{ordinal:03}"),
        };
        repository
            .put_bytes(
                "main",
                key.into_bytes(),
                format!("v1-{ordinal}").into_bytes(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    let snapshot = repository.head("main").await.unwrap();
    assert_eq!(
        repository
            .list_objects_at(snapshot, b"edge/", None, 0)
            .await
            .unwrap(),
        (Vec::new(), false)
    );
    assert_eq!(
        repository
            .list_versions_at(snapshot, b"edge/", None, 0)
            .await
            .unwrap(),
        (Vec::new(), false)
    );

    let mut object_cursor = None;
    let mut object_keys = Vec::new();
    let mut object_pages = 0;
    loop {
        let (page, truncated) = repository
            .list_objects_at(snapshot, b"edge/", object_cursor.as_deref(), 2)
            .await
            .unwrap();
        assert!(!page.is_empty());
        object_cursor = page.last().map(|item| item.key.clone());
        object_keys.extend(page.into_iter().map(|item| item.key));
        object_pages += 1;
        if !truncated {
            break;
        }
    }
    assert_eq!(object_pages, 21);
    assert_eq!(object_keys.len(), 41);
    assert!(object_keys.windows(2).all(|pair| pair[0] < pair[1]));

    // A later write cannot leak into pages bound to the earlier snapshot.
    repository
        .put_bytes(
            "main",
            b"edge/late".to_vec(),
            b"late".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    assert!(repository
        .list_objects_at(snapshot, b"edge/late", None, 10)
        .await
        .unwrap()
        .0
        .is_empty());

    let mut version_cursor = None;
    let mut version_cursors = Vec::new();
    loop {
        let (page, truncated) = repository
            .list_versions_at(snapshot, b"edge/", version_cursor.as_deref(), 2)
            .await
            .unwrap();
        version_cursor = page.last().map(|item| item.cursor.clone());
        version_cursors.extend(page.into_iter().map(|item| item.cursor));
        if !truncated {
            break;
        }
    }
    assert_eq!(version_cursors.len(), 41);
    assert!(version_cursors.windows(2).all(|pair| pair[0] < pair[1]));

    let head = repository.head("main").await.unwrap();
    let first_log_page = repository.log_at(head, None, 3).await.unwrap();
    let second_log_page = repository
        .log_at(head, Some(first_log_page.last().unwrap().0), 3)
        .await
        .unwrap();
    assert_eq!(first_log_page.len(), 3);
    assert_eq!(second_log_page.len(), 3);
    assert_ne!(first_log_page.last().unwrap().0, second_log_page[0].0);

    let mut diff_cursor = None;
    let mut diff_keys = Vec::new();
    loop {
        let (page, truncated) = repository
            .diff_at(root, snapshot, diff_cursor.as_deref(), 2)
            .await
            .unwrap();
        diff_cursor = page.last().map(|item| item.key.clone());
        diff_keys.extend(page.into_iter().map(|item| item.key));
        if !truncated {
            break;
        }
    }
    assert_eq!(diff_keys, object_keys);
}

#[tokio::test]
async fn multipart_catalog_is_paged_and_expiry_is_cas_safe() {
    let clock = Arc::new(FixedClock::new(10_000_000));
    let repository = Repository::initialize(
        Arc::new(MemoryObjectPlane::new(true)),
        options("multipart-catalog", clock.clone()),
    )
    .await
    .unwrap();
    for key in ["uploads/c", "uploads/a", "uploads/b"] {
        repository
            .create_multipart_upload(
                "main",
                key.as_bytes().to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
            )
            .await
            .unwrap();
    }
    let (first, truncated) = repository
        .list_multipart_uploads("main", b"uploads/", None, 2)
        .await
        .unwrap();
    assert_eq!(
        repository
            .list_multipart_uploads("main", b"uploads/", None, 0)
            .await
            .unwrap(),
        (Vec::new(), false)
    );
    assert!(truncated);
    assert_eq!(
        first
            .iter()
            .map(|item| item.key.as_slice())
            .collect::<Vec<_>>(),
        [b"uploads/a", b"uploads/b"]
    );
    let cursor = first.last().unwrap();
    let (second, truncated) = repository
        .list_multipart_uploads("main", b"uploads/", Some((&cursor.key, cursor.id)), 2)
        .await
        .unwrap();
    assert!(!truncated);
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].key, b"uploads/c");

    let snapshot = repository
        .create_multipart_catalog_snapshot("main", b"uploads/", 10_000_050)
        .await
        .unwrap();
    let (snapshot_first, truncated) = repository
        .list_multipart_catalog_snapshot(snapshot.id, "main", b"uploads/", 10_000_050, 0, 2)
        .await
        .unwrap();
    assert!(truncated);
    assert_eq!(snapshot_first[0].key, b"uploads/a");
    assert_eq!(snapshot_first[1].key, b"uploads/b");

    // Mutating the authoritative upload catalog between pages cannot alter
    // the immutable listing projection.
    repository
        .abort_multipart_upload(second[0].id)
        .await
        .unwrap();
    repository
        .create_multipart_upload(
            "main",
            b"uploads/aa".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let (snapshot_second, truncated) = repository
        .list_multipart_catalog_snapshot(snapshot.id, "main", b"uploads/", 10_000_050, 2, 2)
        .await
        .unwrap();
    assert!(!truncated);
    assert_eq!(snapshot_second.len(), 1);
    assert_eq!(snapshot_second[0].key, b"uploads/c");

    let retained = repository
        .plan_gc(2 * 60 * 60 * 1_000, 10_000)
        .await
        .unwrap();
    assert!(!retained.plan.body.candidates.iter().any(|candidate| {
        candidate
            .path
            .as_str()
            .contains("/multipart/catalog-snapshots/")
    }));

    clock.advance(101).unwrap();
    assert_eq!(
        repository
            .list_multipart_catalog_snapshot(snapshot.id, "main", b"uploads/", 10_000_050, 2, 2)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidContinuationToken
    );
    let collectible = repository
        .plan_gc(2 * 60 * 60 * 1_000, 10_000)
        .await
        .unwrap();
    assert!(collectible.plan.body.candidates.iter().any(|candidate| {
        candidate
            .path
            .as_str()
            .contains("/multipart/catalog-snapshots/")
    }));
    assert!(repository
        .list_multipart_uploads("main", b"uploads/", None, 10)
        .await
        .unwrap()
        .0
        .is_empty());
    assert_eq!(repository.expire_multipart_uploads(2).await.unwrap(), 2);
    assert_eq!(repository.expire_multipart_uploads(2).await.unwrap(), 1);
    assert_eq!(repository.expire_multipart_uploads(2).await.unwrap(), 0);
    assert_eq!(
        repository.list_parts(first[0].id).await.unwrap_err().code,
        ErrorCode::NoSuchUpload
    );

    let bounded = Repository::initialize(
        Arc::new(MemoryObjectPlane::new(true)),
        RepositoryOptions {
            history_traversal_limit: 2,
            ..options(
                "multipart-catalog-bound",
                Arc::new(FixedClock::new(20_000_000)),
            )
        },
    )
    .await
    .unwrap();
    for key in ["a", "b", "c"] {
        bounded
            .create_multipart_upload(
                "main",
                key.as_bytes().to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        bounded
            .create_multipart_catalog_snapshot("main", b"", 20_001_000)
            .await
            .unwrap_err()
            .code,
        ErrorCode::HistoryLimitExceeded
    );
}

#[tokio::test]
async fn multipart_catalog_enforces_the_thousand_entry_page_boundary() {
    let clock = Arc::new(FixedClock::new(30_000_000));
    let repository = Repository::initialize(
        Arc::new(MemoryObjectPlane::new(true)),
        options("multipart-catalog-thousand", clock),
    )
    .await
    .unwrap();
    for ordinal in 0..1_001 {
        repository
            .create_multipart_upload(
                "main",
                format!("uploads/{ordinal:04}").into_bytes(),
                ObjectHeaders::default(),
                BTreeMap::new(),
            )
            .await
            .unwrap();
    }
    let snapshot = repository
        .create_multipart_catalog_snapshot("main", b"uploads/", 30_001_000)
        .await
        .unwrap();
    let (first, truncated) = repository
        .list_multipart_catalog_snapshot(snapshot.id, "main", b"uploads/", 30_001_000, 0, 1_001)
        .await
        .unwrap();
    assert_eq!(first.len(), 1_000);
    assert!(truncated);
    assert_eq!(first.first().unwrap().key, b"uploads/0000");
    assert_eq!(first.last().unwrap().key, b"uploads/0999");
    let (second, truncated) = repository
        .list_multipart_catalog_snapshot(snapshot.id, "main", b"uploads/", 30_001_000, 1_000, 1_001)
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].key, b"uploads/1000");
    assert!(!truncated);
}

#[tokio::test]
async fn multipart_part_copy_supports_zero_copy_and_bounded_ranges() {
    let clock = Arc::new(FixedClock::new(30_000));
    let repository = Repository::initialize(
        Arc::new(MemoryObjectPlane::new(true)),
        options("multipart-copy", clock),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"source".to_vec(),
            b"0123456789".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let full_upload = repository
        .create_multipart_upload(
            "main",
            b"full-copy".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let full = repository
        .upload_part_copy(full_upload, 1, "main", b"source", None, None)
        .await
        .unwrap();
    repository
        .complete_multipart_upload(full_upload, vec![(1, full.etag)], None)
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_current("main", b"full-copy")
            .await
            .unwrap()
            .bytes,
        b"0123456789"
    );

    let ranged_upload = repository
        .create_multipart_upload(
            "main",
            b"range-copy".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let ranged = repository
        .upload_part_copy(ranged_upload, 1, "main", b"source", None, Some((2, 6)))
        .await
        .unwrap();
    repository
        .complete_multipart_upload(ranged_upload, vec![(1, ranged.etag)], None)
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_current("main", b"range-copy")
            .await
            .unwrap()
            .bytes,
        b"23456"
    );
}

#[tokio::test]
async fn multipart_range_stream_crosses_three_part_boundaries_without_assembly() {
    const FIVE_MIB: usize = 5 * 1024 * 1024;
    let repository = Repository::initialize(
        Arc::new(MemoryObjectPlane::new(true)),
        options(
            "multipart-three-part-range",
            Arc::new(FixedClock::new(40_000_000)),
        ),
    )
    .await
    .unwrap();
    let upload = repository
        .create_multipart_upload(
            "main",
            b"three-parts".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let first = repository
        .upload_part_stream(
            upload,
            1,
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(vec![b'A'; FIVE_MIB])
            }),
        )
        .await
        .unwrap();
    let second = repository
        .upload_part_stream(
            upload,
            2,
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(vec![b'B'; FIVE_MIB])
            }),
        )
        .await
        .unwrap();
    let third = repository
        .upload_part_stream(
            upload,
            3,
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"CCC".to_vec())
            }),
        )
        .await
        .unwrap();
    repository
        .complete_multipart_upload(
            upload,
            vec![(1, first.etag), (2, second.etag), (3, third.etag)],
            None,
        )
        .await
        .unwrap();
    let summary = repository
        .head_current("main", b"three-parts")
        .await
        .unwrap();
    let content = match summary.version.body.kind {
        ObjectVersionKindV1::Live { content, .. } => content,
        ObjectVersionKindV1::DeleteMarker => panic!("completed multipart object was deleted"),
    };
    let start = FIVE_MIB as u64 - 2;
    let end = (2 * FIVE_MIB) as u64 + 1;
    let mut stream = repository.read_content_stream(content, Some((start, end)));
    let mut observed = Vec::new();
    while let Some(chunk) = stream.next().await {
        observed.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(&observed[..2], b"AA");
    assert!(observed[2..2 + FIVE_MIB].iter().all(|byte| *byte == b'B'));
    assert_eq!(&observed[2 + FIVE_MIB..], b"CC");
}

#[tokio::test]
async fn invalid_completion_never_freezes_the_upload_and_replay_input_is_exact() {
    let mut configured = options(
        "multipart-validation-before-freeze",
        Arc::new(FixedClock::new(50_000_000)),
    );
    configured.limits.max_object_bytes = 10;
    let repository = Repository::initialize(Arc::new(MemoryObjectPlane::new(true)), configured)
        .await
        .unwrap();
    let upload = repository
        .create_multipart_upload(
            "main",
            b"validated".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let oversized = repository
        .upload_part_stream(
            upload,
            1,
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"eleven-byte".to_vec())
            }),
        )
        .await
        .unwrap();
    let operation = OperationId::new();
    assert_eq!(
        repository
            .complete_multipart_upload(
                upload,
                (1..=10_001)
                    .map(|part| (part, "unused".to_string()))
                    .collect(),
                None,
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        repository
            .complete_multipart_upload(upload, vec![(1, "wrong".to_string())], Some(operation))
            .await
            .unwrap_err()
            .code,
        ErrorCode::PreconditionFailed
    );
    assert!(matches!(
        repository.multipart_upload(upload).await.unwrap().state,
        MultipartStateV1::Active
    ));
    assert_eq!(
        repository
            .complete_multipart_upload(upload, vec![(1, oversized.etag.clone())], Some(operation),)
            .await
            .unwrap_err()
            .code,
        ErrorCode::EntityTooLarge
    );
    assert!(matches!(
        repository.multipart_upload(upload).await.unwrap().state,
        MultipartStateV1::Active
    ));
    let valid = repository
        .upload_part_stream(
            upload,
            1,
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"ten-bytes!".to_vec())
            }),
        )
        .await
        .unwrap();
    let receipt = repository
        .complete_multipart_upload(upload, vec![(1, valid.etag)], Some(operation))
        .await
        .unwrap();
    assert_eq!(
        repository
            .complete_multipart_upload(upload, vec![(1, oversized.etag)], Some(operation))
            .await
            .unwrap_err()
            .code,
        ErrorCode::IdempotencyConflict
    );
    assert_eq!(repository.head("main").await.unwrap(), receipt.id);

    let other = repository
        .create_multipart_upload(
            "main",
            b"part-number".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    for invalid_part in [0, 10_001] {
        assert_eq!(
            repository
                .upload_part_stream(
                    other,
                    invalid_part,
                    futures_util::stream::once(async {
                        Ok::<_, std::convert::Infallible>(b"not-polled".to_vec())
                    }),
                )
                .await
                .unwrap_err()
                .code,
            ErrorCode::InvalidRequest
        );
    }
    repository
        .upload_part_stream(
            other,
            10_000,
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"highest-valid-part".to_vec())
            }),
        )
        .await
        .unwrap();
    let too_small = repository
        .upload_part_stream(
            other,
            1,
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"small".to_vec())
            }),
        )
        .await
        .unwrap();
    let final_part = repository
        .upload_part_stream(
            other,
            2,
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"final".to_vec())
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .complete_multipart_upload(
                other,
                vec![(1, too_small.etag), (2, final_part.etag)],
                None,
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidRequest
    );
    assert!(matches!(
        repository.multipart_upload(other).await.unwrap().state,
        MultipartStateV1::Active
    ));
}

#[tokio::test]
async fn put_conditions_are_atomic_and_checksums_cover_the_staged_body() {
    let repository = Arc::new(
        Repository::initialize(
            Arc::new(MemoryObjectPlane::new(true)),
            options("conditional-put", Arc::new(FixedClock::new(40_000))),
        )
        .await
        .unwrap(),
    );
    repository
        .put_bytes(
            "main",
            b"conditional".to_vec(),
            b"old".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let etag = match repository
        .head_current("main", b"conditional")
        .await
        .unwrap()
        .version
        .body
        .kind
    {
        prolly_s3_core::ObjectVersionKindV1::Live { logical_etag, .. } => logical_etag,
        _ => unreachable!(),
    };
    let condition = ObjectWriteConditionV1 {
        if_match: Some(EtagPredicateV1::OneOf([etag].into_iter().collect())),
        if_none_match: None,
        expected_head: None,
    };
    let left = repository.clone();
    let right = repository.clone();
    let left_condition = condition.clone();
    let (left_result, right_result) = tokio::join!(
        left.put_stream_checked(
            "main",
            b"conditional".to_vec(),
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"left".to_vec())
            }),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
            left_condition,
            ChecksumExpectation::default(),
        ),
        right.put_stream_checked(
            "main",
            b"conditional".to_vec(),
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"right".to_vec())
            }),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
            condition,
            ChecksumExpectation::default(),
        )
    );
    assert_eq!(
        usize::from(left_result.is_ok()) + usize::from(right_result.is_ok()),
        1
    );
    let loser = left_result.err().or_else(|| right_result.err()).unwrap();
    assert_eq!(loser.code, ErrorCode::PreconditionFailed);

    let stale_head = repository.head("main").await.unwrap();
    repository
        .put_bytes(
            "main",
            b"unrelated".to_vec(),
            b"advance".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let stale_write = repository
        .put_stream_checked(
            "main",
            b"expected-head".to_vec(),
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"stale".to_vec())
            }),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
            ObjectWriteConditionV1 {
                expected_head: Some(stale_head),
                ..ObjectWriteConditionV1::default()
            },
            ChecksumExpectation::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(stale_write.code, ErrorCode::PreconditionFailed);

    let before_bad_checksum = repository.head("main").await.unwrap();
    let bad = repository
        .put_stream_checked(
            "main",
            b"checksum".to_vec(),
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"verified-body".to_vec())
            }),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
            ObjectWriteConditionV1::default(),
            ChecksumExpectation {
                sha256: Some([0; 32]),
                ..ChecksumExpectation::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(bad.code, ErrorCode::ChecksumMismatch);
    assert_eq!(repository.head("main").await.unwrap(), before_bad_checksum);

    let digest: [u8; 32] = Sha256::digest(b"verified-body").into();
    repository
        .put_stream_checked(
            "main",
            b"checksum".to_vec(),
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"verified-body".to_vec())
            }),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
            ObjectWriteConditionV1 {
                if_match: None,
                if_none_match: Some(EtagPredicateV1::Any),
                expected_head: None,
            },
            ChecksumExpectation {
                sha256: Some(digest),
                ..ChecksumExpectation::default()
            },
        )
        .await
        .unwrap();
}
