use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use md5::Md5;
use prolly::{Cid, TreeFormat};
use prolly_s3_core::{
    tree_format_digest, Checksums, CommitGeneration, CommitObjectV1, CompareExchange,
    CompareExchangeOutcome, ErrorCode, LogicalObjectVersionBodyV1, LogicalObjectVersionKindV1,
    MemoryNodeCache, MemoryObjectPlane, MergePolicy, NodeCache, NodeCacheError, NodeCacheKey,
    NodePackEntryV1, NodePackV1, ObjectHeaders, ObjectPath, ObjectPlane, ObjectVersionOrder,
    ObjectVersionV1, OperationId, PhysicalBatchMutationV1, PhysicalMultipartCompletedPart,
    PhysicalObjectBindingV1, PhysicalPut, PhysicalVersion, PhysicalVersioning,
    ProviderCapabilities, Repository, RepositoryId, RepositoryOptions, TraversalBudget,
};
use sha2::{Digest as _, Sha256};

#[derive(Default)]
struct CorruptNodeCache {
    removals: AtomicUsize,
}

#[async_trait::async_trait]
impl NodeCache for CorruptNodeCache {
    async fn get(
        &self,
        _key: &NodeCacheKey,
    ) -> std::result::Result<Option<Vec<u8>>, NodeCacheError> {
        Ok(Some(vec![0x5a; 17]))
    }

    async fn insert(
        &self,
        _key: NodeCacheKey,
        _value: Vec<u8>,
    ) -> std::result::Result<(), NodeCacheError> {
        Ok(())
    }

    async fn remove(&self, _key: &NodeCacheKey) -> std::result::Result<(), NodeCacheError> {
        self.removals.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn physical_options(prefix: &str) -> RepositoryOptions {
    RepositoryOptions {
        repository_prefix: prefix.to_string(),
        writer: "physical-writer".to_string(),
        ..RepositoryOptions::default()
    }
}

async fn corrupt_mutable_head(plane: &MemoryObjectPlane, path: &str) {
    let path = ObjectPath::new(path).unwrap();
    let current = plane.load_mutable(&path).await.unwrap().unwrap();
    let outcome = plane
        .compare_exchange(CompareExchange {
            path,
            expected: Some(current.metadata.token),
            bytes: b"invalid derived head".to_vec(),
        })
        .await
        .unwrap();
    assert!(matches!(outcome, CompareExchangeOutcome::Applied(_)));
}

#[tokio::test]
async fn physical_clone_replays_history_and_rebinds_physical_versions() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(
        source_plane,
        physical_options(".prolly/prolly-s3/clone-source"),
    )
    .await
    .unwrap();
    let first = source
        .put_bytes(
            "main",
            b"history.txt".to_vec(),
            b"first".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    source
        .put_bytes(
            "main",
            b"history.txt".to_vec(),
            b"second".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let destination_plane = Arc::new(MemoryObjectPlane::new(true));
    let report = source
        .clone_to(
            destination_plane.clone(),
            ".prolly/prolly-s3/clone-destination",
        )
        .await
        .unwrap();
    assert_eq!(report.immutable_objects, 6);
    assert_eq!(report.refs, 1);
    let resumed = source
        .clone_to(
            destination_plane.clone(),
            ".prolly/prolly-s3/clone-destination",
        )
        .await
        .unwrap();
    assert_eq!(resumed.immutable_objects, 0);
    assert_eq!(resumed.immutable_bytes, 0);
    assert_eq!(resumed.refs, 1);

    let destination = Repository::open(
        destination_plane,
        physical_options(".prolly/prolly-s3/clone-destination"),
    )
    .await
    .unwrap();
    assert_eq!(destination.repository_id(), source.repository_id());
    assert_eq!(
        destination
            .get_current("main", b"history.txt")
            .await
            .unwrap()
            .bytes,
        b"second"
    );
    assert_eq!(
        destination
            .get_version("main", b"history.txt", first.object_versions[0])
            .await
            .unwrap()
            .bytes,
        b"first"
    );
    let source_version = source
        .head_version("main", b"history.txt", first.object_versions[0])
        .await
        .unwrap()
        .1
        .version;
    let destination_version = destination
        .head_version("main", b"history.txt", first.object_versions[0])
        .await
        .unwrap()
        .1
        .version;
    assert_eq!(source_version.id, destination_version.id);
    assert_ne!(source_version.binding, destination_version.binding);
}

#[tokio::test]
async fn physical_push_replays_only_new_history_and_moves_destination_ref() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(
        source_plane,
        physical_options(".prolly/prolly-s3/push-source"),
    )
    .await
    .unwrap();
    source
        .put_bytes(
            "main",
            b"sync.txt".to_vec(),
            b"base".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let destination_plane = Arc::new(MemoryObjectPlane::new(true));
    source
        .clone_to(
            destination_plane.clone(),
            ".prolly/prolly-s3/push-destination",
        )
        .await
        .unwrap();
    let destination = Repository::open(
        destination_plane.clone(),
        physical_options(".prolly/prolly-s3/push-destination"),
    )
    .await
    .unwrap();
    let expected_destination = destination.head("main").await.unwrap();
    let next = source
        .put_bytes(
            "main",
            b"sync.txt".to_vec(),
            b"incremental".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    destination_plane.reset_request_counts();

    let report = source
        .push_to(
            &destination,
            "main",
            "main",
            expected_destination,
            "incremental physical push",
        )
        .await
        .unwrap();
    // One physical payload plus one commit envelope. The commit envelope
    // carries the Prolly node pack, so transfer no longer copies a third
    // standalone node-pack object.
    assert_eq!(report.copied_objects, 2);
    assert_eq!(report.copied_bytes, b"incremental".len() as u64);
    // The one LIST is bounded mutable-control version compaction for the ref
    // move. Transfer itself performs no destination commit-namespace LIST.
    assert_eq!(destination_plane.request_snapshot().list, 1);
    assert_eq!(
        destination
            .get_current("main", b"sync.txt")
            .await
            .unwrap()
            .bytes,
        b"incremental"
    );
    assert_eq!(
        destination
            .head_current("main", b"sync.txt")
            .await
            .unwrap()
            .version
            .id,
        next.object_versions[0]
    );
}

#[tokio::test]
async fn physical_transfer_pages_resume_across_source_processes() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source_options = physical_options(".prolly/prolly-s3/resumable-transfer-source");
    let source = Repository::initialize(source_plane.clone(), source_options.clone())
        .await
        .unwrap();
    source
        .put_bytes(
            "main",
            b"base.txt".to_vec(),
            b"base".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let destination_plane = Arc::new(MemoryObjectPlane::new(true));
    source
        .clone_to(
            destination_plane.clone(),
            ".prolly/prolly-s3/resumable-transfer-destination",
        )
        .await
        .unwrap();
    for ordinal in 0..2 {
        source
            .put_bytes(
                "main",
                format!("page-{ordinal}.txt").into_bytes(),
                format!("value-{ordinal}").into_bytes(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    let source_head = source.head("main").await.unwrap();
    let destination = Repository::open(
        destination_plane,
        physical_options(".prolly/prolly-s3/resumable-transfer-destination"),
    )
    .await
    .unwrap();
    let mut cursor = source
        .start_physical_transfer(&destination, &[source_head], false)
        .await
        .unwrap();
    assert!(prolly_s3_core::encode_canonical(&cursor).unwrap().len() < 320);
    let mut copied_objects = 0usize;
    let mut pages = 0usize;
    loop {
        let encoded = prolly_s3_core::encode_canonical(&cursor).unwrap();
        cursor = prolly_s3_core::decode_canonical(&encoded).unwrap();
        let reader = Repository::open(
            source_plane.clone(),
            RepositoryOptions {
                read_only: true,
                ..source_options.clone()
            },
        )
        .await
        .unwrap();
        let page = reader
            .physical_transfer_page(&destination, &cursor, 2, 1)
            .await
            .unwrap();
        assert!(page.processed_commits <= 1);
        assert!(page.traversal_steps <= 2);
        copied_objects += page.sync.copied_objects;
        pages += 1;
        cursor = page.cursor;
        if page.complete {
            break;
        }
        assert!(pages < 20);
    }
    assert!(pages > 1);
    assert_eq!(copied_objects, 4);
    let mapped_head = source
        .physical_transfer_mapping(&cursor, source_head)
        .await
        .unwrap()
        .unwrap();
    destination
        .create_branch("resumed", mapped_head)
        .await
        .unwrap();
    assert_eq!(
        destination
            .get_current("resumed", b"page-1.txt")
            .await
            .unwrap()
            .bytes,
        b"value-1"
    );
}

#[tokio::test]
async fn physical_repair_rebinds_a_missing_destination_payload() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(
        source_plane,
        physical_options(".prolly/prolly-s3/repair-source"),
    )
    .await
    .unwrap();
    source
        .put_bytes(
            "main",
            b"repair.txt".to_vec(),
            b"recoverable".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let destination_plane = Arc::new(MemoryObjectPlane::new(true));
    source
        .clone_to(
            destination_plane.clone(),
            ".prolly/prolly-s3/repair-destination",
        )
        .await
        .unwrap();
    let destination = Repository::open(
        destination_plane.clone(),
        physical_options(".prolly/prolly-s3/repair-destination"),
    )
    .await
    .unwrap();
    let damaged = destination
        .head_current("main", b"repair.txt")
        .await
        .unwrap()
        .version;
    let PhysicalObjectBindingV1::Live { version_id, .. } = damaged.binding.clone() else {
        panic!("expected live physical binding")
    };
    destination_plane
        .delete_exact(
            &ObjectPath::new("repair.txt").unwrap(),
            PhysicalVersion::Versioned { version_id },
        )
        .await
        .unwrap();
    assert_eq!(
        destination
            .get_current("main", b"repair.txt")
            .await
            .unwrap_err()
            .code,
        ErrorCode::MissingClosure
    );

    let repaired = destination
        .repair_missing_from(&source, "main")
        .await
        .unwrap();
    assert!(repaired.sync.ref_move.is_some());
    assert_eq!(
        destination
            .get_current("main", b"repair.txt")
            .await
            .unwrap()
            .bytes,
        b"recoverable"
    );
}

#[tokio::test]
async fn physical_fetch_returns_a_destination_local_mapped_head() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(
        source_plane,
        physical_options(".prolly/prolly-s3/fetch-source"),
    )
    .await
    .unwrap();
    source
        .put_bytes(
            "main",
            b"base.txt".to_vec(),
            b"base".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let destination_plane = Arc::new(MemoryObjectPlane::new(true));
    source
        .clone_to(
            destination_plane.clone(),
            ".prolly/prolly-s3/fetch-destination",
        )
        .await
        .unwrap();
    let destination = Repository::open(
        destination_plane,
        physical_options(".prolly/prolly-s3/fetch-destination"),
    )
    .await
    .unwrap();
    let source_base = source.head("main").await.unwrap();
    source.create_branch("feature", source_base).await.unwrap();
    source
        .put_bytes(
            "feature",
            b"feature.txt".to_vec(),
            b"feature".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let fetched = destination.fetch_from(&source, "feature").await.unwrap();
    let mapped_head = fetched.source_head.unwrap();
    assert_ne!(mapped_head, source.head("feature").await.unwrap());
    destination.fsck_commit(mapped_head).await.unwrap();
    destination
        .create_branch("imported", mapped_head)
        .await
        .unwrap();
    assert_eq!(
        destination
            .get_current("imported", b"feature.txt")
            .await
            .unwrap()
            .bytes,
        b"feature"
    );
}

#[tokio::test]
async fn physical_multipart_uses_n_plus_four_calls_and_replays_without_io() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/multipart-budget"),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"warmup.bin".to_vec(),
            b"warm".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let first_bytes = vec![7; 5 * 1024 * 1024];
    let second_bytes = vec![9; 1024];
    let mut whole = first_bytes.clone();
    whole.extend_from_slice(&second_bytes);
    let checksum_sha256: [u8; 32] = Sha256::digest(&whole).into();
    let checksum_md5: [u8; 16] = Md5::digest(&whole).into();

    plane.reset_request_counts();
    let session = repository
        .create_physical_multipart_upload(
            "main",
            b"multipart.bin".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let first = repository
        .upload_physical_multipart_part(&session, 1, first_bytes)
        .await
        .unwrap();
    let second = repository
        .upload_physical_multipart_part(&session, 2, second_bytes)
        .await
        .unwrap();
    let parts = [&first, &second]
        .into_iter()
        .map(|part| PhysicalMultipartCompletedPart {
            part_number: part.part_number,
            etag: part.etag.clone(),
            checksum_sha256: part.checksum_sha256.unwrap(),
            size: part.size,
        })
        .collect::<Vec<_>>();
    let receipt = repository
        .complete_physical_multipart_upload(
            session.clone(),
            parts.clone(),
            checksum_sha256,
            checksum_md5,
            whole.len() as u64,
            Some(session.operation),
        )
        .await
        .unwrap();
    assert_eq!(receipt.changed_keys, 1);
    let requests = plane.request_snapshot();
    assert_eq!(requests.physical_multipart_create, 1);
    assert_eq!(requests.physical_multipart_upload_part, 2);
    assert_eq!(requests.physical_multipart_complete, 1);
    assert_eq!(requests.immutable_put, 1);
    assert_eq!(requests.compare_exchange, 1);
    assert_eq!(requests.total(), 6, "unexpected calls: {requests:?}");
    assert_eq!(
        repository
            .get_current("main", b"multipart.bin")
            .await
            .unwrap()
            .bytes,
        whole
    );

    plane.reset_request_counts();
    let replay = repository
        .complete_physical_multipart_upload(
            session.clone(),
            parts,
            checksum_sha256,
            checksum_md5,
            first.size + second.size,
            Some(session.operation),
        )
        .await
        .unwrap();
    assert_eq!(replay.id, receipt.id);
    assert!(replay.idempotent_replay);
    assert_eq!(plane.request_snapshot().total(), 0);
}

#[tokio::test]
async fn two_object_physical_batch_is_exactly_four_calls() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/batch-budget"),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"warmup.bin".to_vec(),
            b"warm".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let batch = repository
        .begin_physical_batch("main", "two objects", 60_000)
        .await
        .unwrap();
    plane.reset_request_counts();
    let receipt = repository
        .publish_physical_batch(
            batch,
            vec![
                PhysicalBatchMutationV1::Put {
                    key: b"batch/a.bin".to_vec(),
                    bytes: b"a".to_vec(),
                    headers: ObjectHeaders::default(),
                    user_metadata: BTreeMap::new(),
                },
                PhysicalBatchMutationV1::Put {
                    key: b"batch/b.bin".to_vec(),
                    bytes: b"b".to_vec(),
                    headers: ObjectHeaders::default(),
                    user_metadata: BTreeMap::new(),
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(receipt.changed_keys, 2);
    let requests = plane.request_snapshot();
    assert_eq!(requests.physical_put, 2);
    assert_eq!(requests.immutable_put, 1);
    assert_eq!(requests.compare_exchange, 1);
    assert_eq!(requests.total(), 4, "unexpected calls: {requests:?}");
}

#[tokio::test]
async fn applied_put_cas_conflict_is_reconciled_by_operation_id() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/reconcile-applied-put"),
    )
    .await
    .unwrap();

    plane.conflict_after_next_compare_exchange();
    let receipt = repository
        .put_bytes(
            "main",
            b"reconciled.bin".to_vec(),
            b"published".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    assert!(receipt.idempotent_replay);
    assert_eq!(repository.head("main").await.unwrap(), receipt.id);
    assert_eq!(
        repository
            .get_current("main", b"reconciled.bin")
            .await
            .unwrap()
            .bytes,
        b"published"
    );
}

#[tokio::test]
async fn applied_batch_cas_conflict_is_reconciled_by_operation_id() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/reconcile-applied-batch"),
    )
    .await
    .unwrap();
    let batch = repository
        .begin_physical_batch("main", "reconciled batch", 60_000)
        .await
        .unwrap();

    plane.conflict_after_next_compare_exchange();
    let receipt = repository
        .publish_physical_batch(
            batch,
            vec![PhysicalBatchMutationV1::Put {
                key: b"reconciled-batch.bin".to_vec(),
                bytes: b"published".to_vec(),
                headers: ObjectHeaders::default(),
                user_metadata: BTreeMap::new(),
            }],
        )
        .await
        .unwrap();

    assert!(receipt.idempotent_replay);
    assert_eq!(repository.head("main").await.unwrap(), receipt.id);
}

#[tokio::test]
async fn applied_multi_delete_cas_conflict_is_reconciled_by_operation_id() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/reconcile-applied-multi-delete"),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"delete-me.bin".to_vec(),
            b"published".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    plane.conflict_after_next_compare_exchange();
    let receipt = repository
        .delete_objects("main", vec![b"delete-me.bin".to_vec()], None)
        .await
        .unwrap();

    assert!(receipt.idempotent_replay);
    assert_eq!(repository.head("main").await.unwrap(), receipt.id);
    assert!(repository
        .get_current("main", b"delete-me.bin")
        .await
        .is_err());
}

#[tokio::test]
async fn hot_branch_ref_versions_are_compacted_without_losing_history() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let mut options = physical_options(".prolly/prolly-s3/ref-version-compaction");
    options.branch_ref_compaction_interval = 100;
    options.branch_ref_versions_to_retain = 5;
    let repository = Repository::initialize(plane, options).await.unwrap();
    for index in 0..101 {
        repository
            .put_bytes(
                "main",
                format!("objects/{index:04}.bin").into_bytes(),
                vec![index as u8; 32],
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }

    let report = repository
        .compact_branch_ref_versions("main")
        .await
        .unwrap();
    assert_eq!(report.scanned, 6, "automatic compaction did not run");
    assert_eq!(report.retained, 5);
    assert_eq!(report.deleted, 1);
    assert_eq!(repository.list_reflog("main").await.unwrap().len(), 101);
    assert_eq!(
        repository
            .get_current("main", b"objects/0000.bin")
            .await
            .unwrap()
            .bytes,
        vec![0; 32]
    );
}

#[tokio::test]
async fn hundred_object_batch_packs_only_final_reachable_nodes() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let prefix = ".prolly/prolly-s3/final-batch-nodes";
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(prefix),
    )
    .await
    .unwrap();
    let batch = repository
        .begin_physical_batch("main", "one hundred objects", 60_000)
        .await
        .unwrap();
    let receipt = repository
        .publish_physical_batch(
            batch,
            (0..100)
                .map(|index| PhysicalBatchMutationV1::Put {
                    key: format!("objects/{index:04}.bin").into_bytes(),
                    bytes: vec![index as u8; 1024],
                    headers: ObjectHeaders::default(),
                    user_metadata: BTreeMap::new(),
                })
                .collect(),
        )
        .await
        .unwrap();
    let commit = repository.commit(receipt.id).await.unwrap();
    let packed_bytes = commit.node_pack.expect("batch node pack").object_len;

    assert!(
        packed_bytes < 2 * 1024 * 1024,
        "100-object commit packed {packed_bytes} bytes of transient nodes"
    );
    let warmed = repository.prewarm_internal_nodes(receipt.id).await.unwrap();
    assert_eq!(warmed.roots, 3);
    assert!(warmed.internal_nodes > 0);
    assert!(warmed.leaves_skipped > 0);

    repository.advance_node_index_v2(1_000).await.unwrap();
    let shared_cache = Arc::new(MemoryNodeCache::new(64 * 1024 * 1024));
    let mut reader_options = physical_options(prefix);
    reader_options.read_only = true;
    reader_options.node_cache = Some(shared_cache.clone());
    let reader = Repository::open(plane.clone(), reader_options.clone())
        .await
        .unwrap();
    plane.reset_request_counts();
    let cold = reader.prewarm_internal_nodes(receipt.id).await.unwrap();
    assert_eq!(plane.request_snapshot().list, 0);
    assert_eq!(
        reader.performance_snapshot().node_ranged_fetches,
        (cold.internal_nodes + cold.root_leaves) as u64
    );

    // A fresh repository process can reuse the same persistent/shared cache
    // without refetching any warmed routing node from S3.
    let reopened = Repository::open(plane.clone(), reader_options).await.unwrap();
    plane.reset_request_counts();
    let warm = reopened.prewarm_internal_nodes(receipt.id).await.unwrap();
    assert_eq!(warm, cold);
    assert_eq!(reopened.performance_snapshot().node_ranged_fetches, 0);
    assert_eq!(plane.request_snapshot().list, 0);
}

#[tokio::test]
async fn two_object_physical_multi_delete_is_exactly_four_calls() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/multi-delete-budget"),
    )
    .await
    .unwrap();
    for key in [b"delete/a.bin".to_vec(), b"delete/b.bin".to_vec()] {
        repository
            .put_bytes(
                "main",
                key,
                b"payload".to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }

    plane.reset_request_counts();
    let receipt = repository
        .delete_objects(
            "main",
            vec![b"delete/a.bin".to_vec(), b"delete/b.bin".to_vec()],
            None,
        )
        .await
        .unwrap();
    assert_eq!(receipt.changed_keys, 2);
    let requests = plane.request_snapshot();
    assert_eq!(requests.physical_delete, 2);
    assert_eq!(requests.immutable_put, 1);
    assert_eq!(requests.compare_exchange, 1);
    assert_eq!(requests.total(), 4, "unexpected calls: {requests:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn physical_batch_payload_uploads_are_bounded_and_parallel() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    plane.set_physical_put_delay_millis(20);
    let mut options = physical_options(".prolly/prolly-s3/parallel-batch");
    options.max_parallel_payload_writes = 3;
    let repository = Repository::initialize(plane.clone(), options)
        .await
        .unwrap();
    let batch = repository
        .begin_physical_batch("main", "parallel payloads", 60_000)
        .await
        .unwrap();
    plane.reset_physical_put_concurrency();
    repository
        .publish_physical_batch(
            batch,
            (0..9)
                .map(|index| PhysicalBatchMutationV1::Put {
                    key: format!("parallel/{index}.bin").into_bytes(),
                    bytes: vec![index; 1024],
                    headers: ObjectHeaders::default(),
                    user_metadata: BTreeMap::new(),
                })
                .collect(),
        )
        .await
        .unwrap();
    assert_eq!(plane.max_physical_puts_in_flight(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_branches_publish_their_refs_concurrently() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/independent-branch-lanes"),
    )
    .await
    .unwrap();
    let root = repository.head("main").await.unwrap();
    repository.create_branch("alpha", root).await.unwrap();
    repository.create_branch("beta", root).await.unwrap();

    plane.set_compare_exchange_delay_millis(50);
    plane.reset_compare_exchange_concurrency();
    let (alpha, beta) = tokio::join!(
        repository.put_bytes(
            "alpha",
            b"alpha.bin".to_vec(),
            b"alpha".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        ),
        repository.put_bytes(
            "beta",
            b"beta.bin".to_vec(),
            b"beta".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        ),
    );
    let alpha = alpha.unwrap();
    let beta = beta.unwrap();

    assert_eq!(
        plane.max_compare_exchanges_in_flight(),
        2,
        "independent branch refs were serialized"
    );
    repository.fsck_commit(alpha.id).await.unwrap();
    repository.fsck_commit(beta.id).await.unwrap();
}

#[tokio::test]
async fn warm_physical_merge_reuses_bindings_in_two_calls() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/merge-budget"),
    )
    .await
    .unwrap();
    let base = repository
        .put_bytes(
            "main",
            b"base.bin".to_vec(),
            b"base".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    repository.create_branch("feature", base.id).await.unwrap();
    let feature = repository
        .put_bytes(
            "feature",
            b"feature.bin".to_vec(),
            b"feature".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    repository
        .put_bytes(
            "main",
            b"main.bin".to_vec(),
            b"main".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    plane.reset_request_counts();
    repository
        .merge(
            "main",
            feature.id,
            Some(base.id),
            MergePolicy::Fail,
            None,
            None,
        )
        .await
        .unwrap();
    let requests = plane.request_snapshot();
    assert_eq!(requests.immutable_put, 1);
    assert_eq!(requests.compare_exchange, 1);
    assert_eq!(requests.total(), 2, "unexpected calls: {requests:?}");
    assert_eq!(
        repository
            .get_current("main", b"feature.bin")
            .await
            .unwrap()
            .bytes,
        b"feature"
    );
}

#[tokio::test]
async fn warm_physical_restore_reuses_live_binding_in_two_calls() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/restore-budget"),
    )
    .await
    .unwrap();
    let source = repository
        .put_bytes(
            "main",
            b"restore.bin".to_vec(),
            b"source".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let current = repository
        .put_bytes(
            "main",
            b"restore.bin".to_vec(),
            b"newer".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    plane.reset_request_counts();
    repository
        .restore("main", source.id, current.id, None, None)
        .await
        .unwrap();
    let requests = plane.request_snapshot();
    assert_eq!(requests.immutable_put, 1);
    assert_eq!(requests.compare_exchange, 1);
    assert_eq!(requests.total(), 2, "unexpected calls: {requests:?}");
    assert_eq!(
        repository
            .get_current("main", b"restore.bin")
            .await
            .unwrap()
            .bytes,
        b"source"
    );
}

#[tokio::test]
async fn lost_physical_payload_response_is_reconciled_without_duplicate_upload() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/lost-payload"),
    )
    .await
    .unwrap();
    plane.lose_next_physical_put_response();
    repository
        .put_bytes(
            "main",
            b"lost.bin".to_vec(),
            b"accepted-before-timeout".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_current("main", b"lost.bin")
            .await
            .unwrap()
            .bytes,
        b"accepted-before-timeout"
    );
    let versions = plane
        .list(prolly_s3_core::ListRequest {
            prefix: "lost.bin".to_string(),
            continuation: None,
            limit: 100,
            include_versions: true,
        })
        .await
        .unwrap();
    assert_eq!(versions.entries.len(), 1);
}

#[tokio::test]
async fn lost_physical_copy_response_is_reconciled_without_duplicate_upload() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/lost-copy"),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"source.txt".to_vec(),
            b"copy me".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let operation = OperationId::new();
    plane.lose_next_physical_put_response();
    let copied = repository
        .copy_object(
            "main",
            b"source.txt",
            None,
            b"destination.txt".to_vec(),
            Some(operation),
        )
        .await
        .unwrap();
    assert_eq!(copied.operation, operation);
    assert_eq!(
        repository
            .get_current("main", b"destination.txt")
            .await
            .unwrap()
            .bytes,
        b"copy me"
    );
    let replay = repository
        .copy_object(
            "main",
            b"source.txt",
            None,
            b"destination.txt".to_vec(),
            Some(operation),
        )
        .await
        .unwrap();
    assert!(replay.idempotent_replay);
}

#[tokio::test]
async fn lost_physical_delete_response_is_reconciled_to_the_current_marker() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/lost-delete"),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"deleted.txt".to_vec(),
            b"delete me".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let operation = OperationId::new();
    plane.lose_next_physical_delete_response();
    let deleted = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        repository.delete_object("main", b"deleted.txt".to_vec(), Some(operation)),
    )
    .await
    .expect("lost delete response reconciliation deadlocked")
    .unwrap();
    assert_eq!(deleted.operation, operation);
    assert_eq!(
        repository
            .get_current("main", b"deleted.txt")
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoSuchKey
    );
    let replay = repository
        .delete_object("main", b"deleted.txt".to_vec(), Some(operation))
        .await
        .unwrap();
    assert!(replay.idempotent_replay);
}

#[tokio::test]
async fn physical_idempotent_replay_does_not_upload_again() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/idempotency"),
    )
    .await
    .unwrap();
    let operation = OperationId::new();
    let first = repository
        .put_bytes(
            "main",
            b"idempotent.bin".to_vec(),
            b"once".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            Some(operation),
        )
        .await
        .unwrap();
    plane.reset_request_counts();
    let replay = repository
        .put_bytes(
            "main",
            b"idempotent.bin".to_vec(),
            b"once".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            Some(operation),
        )
        .await
        .unwrap();
    assert_eq!(replay.id, first.id);
    assert!(replay.idempotent_replay);
    assert_eq!(plane.request_snapshot().total(), 0);
}

fn physical_live_body(bytes: &[u8]) -> LogicalObjectVersionBodyV1 {
    let sha256 = Cid::from_bytes(bytes).0;
    LogicalObjectVersionBodyV1 {
        order: ObjectVersionOrder {
            commit_generation: CommitGeneration(1),
            mutation_ordinal: 0,
        },
        created_at_millis: 7,
        kind: LogicalObjectVersionKindV1::Live {
            size: bytes.len() as u64,
            logical_etag: "\"logical\"".to_string(),
            headers: ObjectHeaders::default(),
            checksums: Checksums {
                md5: None,
                sha256: Some(sha256),
                algorithm_values: BTreeMap::new(),
            },
            user_metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
        },
    }
}

#[test]
fn physical_object_identity_excludes_provider_binding() {
    let bytes = b"whole object";
    let repository = RepositoryId::from_hash([3; 32]);
    let operation = OperationId::new();
    let body = physical_live_body(bytes);
    let checksum_sha256 = Cid::from_bytes(bytes).0;
    let first = ObjectVersionV1::derive(
        repository,
        b"asset.bin",
        operation,
        body.clone(),
        PhysicalObjectBindingV1::Live {
            version_id: "version-a".to_string(),
            provider_etag: "etag-a".to_string(),
            checksum_sha256,
        },
    )
    .unwrap();
    let second = ObjectVersionV1::derive(
        repository,
        b"asset.bin",
        operation,
        body,
        PhysicalObjectBindingV1::Live {
            version_id: "version-b".to_string(),
            provider_etag: "etag-b".to_string(),
            checksum_sha256,
        },
    )
    .unwrap();

    assert_eq!(first.id, second.id);
    assert_ne!(first.binding, second.binding);
    first.validate().unwrap();
    second.validate().unwrap();
}

#[test]
fn physical_binding_rejects_kind_and_checksum_mismatch() {
    let error = ObjectVersionV1::derive(
        RepositoryId::from_hash([4; 32]),
        b"asset.bin",
        OperationId::new(),
        physical_live_body(b"expected"),
        PhysicalObjectBindingV1::DeleteMarker {
            version_id: "delete-version".to_string(),
        },
    )
    .err()
    .unwrap();
    assert_eq!(error.code, ErrorCode::CorruptCommit);

    let logical = LogicalObjectVersionKindV1::DeleteMarker;
    assert!(matches!(logical, LogicalObjectVersionKindV1::DeleteMarker));
}

#[test]
fn node_pack_verifies_cids_ranges_and_corruption() {
    let first_bytes = b"node-a".to_vec();
    let second_bytes = b"node-b".to_vec();
    let first = Cid::from_bytes(&first_bytes);
    let second = Cid::from_bytes(&second_bytes);
    let mut nodes = [(first.clone(), first_bytes), (second, second_bytes)];
    nodes.sort_by_key(|(cid, _)| cid.clone());
    let mut payload = Vec::new();
    let mut entries = Vec::new();
    for (cid, bytes) in nodes {
        let offset = payload.len() as u64;
        payload.extend_from_slice(&bytes);
        let checksum = cid.0;
        entries.push(NodePackEntryV1 {
            cid,
            offset,
            len: bytes.len() as u32,
            sha256: checksum,
        });
    }
    let pack = NodePackV1 {
        format_digest: tree_format_digest(&TreeFormat::default()).unwrap(),
        entries,
        attachments: Vec::new(),
        payload,
    };
    pack.validate().unwrap();
    assert_eq!(pack.node(&first).unwrap().unwrap(), b"node-a");
    assert_eq!(pack.reference().unwrap().node_count, 2);

    let mut corrupt = pack;
    corrupt.payload[0] ^= 1;
    assert_eq!(corrupt.validate().unwrap_err().code, ErrorCode::CorruptNode);
}

#[tokio::test]
async fn commit_envelope_carries_a_range_readable_node_pack() {
    let prefix = ".prolly/prolly-s3/commit-envelope";
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(plane.clone(), physical_options(prefix))
        .await
        .unwrap();
    let receipt = repository
        .put_bytes(
            "main",
            b"envelope.bin".to_vec(),
            b"payload".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let encoded_id = hex::encode(receipt.id.as_bytes());
    let object = plane
        .get(prolly_s3_core::GetRequest {
            path: ObjectPath::new(format!(
                "{prefix}/commits/sha256/{}/{}/{}",
                &encoded_id[..2],
                &encoded_id[2..4],
                encoded_id
            ))
            .unwrap(),
            range: None,
            physical_version: None,
        })
        .await
        .unwrap()
        .unwrap();

    let envelope = CommitObjectV1::decode_object(&object.bytes).unwrap();
    assert_eq!(envelope.commit.id().unwrap(), receipt.id);
    assert!(envelope
        .node_pack
        .as_ref()
        .is_some_and(|pack| !pack.entries.is_empty()));
    assert!(CommitObjectV1::node_payload_offset(&object.bytes)
        .unwrap()
        .is_some());

    let mut corrupt = object.bytes;
    corrupt[0] ^= 1;
    assert_eq!(
        CommitObjectV1::decode_object(&corrupt).unwrap_err().code,
        ErrorCode::CorruptCommit
    );
}

#[test]
fn provider_physical_profile_requires_enabled_versioning() {
    let mut capabilities = ProviderCapabilities {
        conditional_create: true,
        conditional_update: true,
        strong_get_after_put: true,
        strong_list_after_put: true,
        strong_list_after_delete: true,
        ranged_get: true,
        paged_list: true,
        list_physical_versions: true,
        exact_version_delete: true,
        physical_versioning: PhysicalVersioning::Suspended,
        conflicting_lifecycle_rule: false,
        default_object_lock_retention: false,
        max_object_bytes: 5 * 1024 * 1024,
        max_single_put_bytes: 5 * 1024 * 1024,
    };
    assert_eq!(
        capabilities.validate_prolly_s3().unwrap_err().code,
        ErrorCode::ProviderNotQualified
    );
    capabilities.physical_versioning = PhysicalVersioning::Enabled;
    capabilities.validate_prolly_s3().unwrap();
}

#[tokio::test]
async fn physical_repository_round_trips_exact_physical_versions() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = physical_options(".prolly/prolly-s3/test");
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    let first = repository
        .put_bytes(
            "main",
            b"notes/file.txt".to_vec(),
            b"first".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let first_version = first.object_versions[0];
    repository
        .put_bytes(
            "main",
            b"notes/file.txt".to_vec(),
            b"second".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        repository
            .get_version("main", b"notes/file.txt", first_version)
            .await
            .unwrap()
            .bytes,
        b"first"
    );
    assert_eq!(
        repository
            .get_current("main", b"notes/file.txt")
            .await
            .unwrap()
            .bytes,
        b"second"
    );

    repository
        .copy_object(
            "main",
            b"notes/file.txt",
            Some(first_version),
            b"notes/copied.txt".to_vec(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_current("main", b"notes/copied.txt")
            .await
            .unwrap()
            .bytes,
        b"first"
    );

    let deleted = repository
        .delete_object("main", b"notes/file.txt".to_vec(), None)
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_current("main", b"notes/file.txt")
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoSuchKey
    );
    let marker = repository
        .head_version("main", b"notes/file.txt", deleted.object_versions[0])
        .await
        .unwrap()
        .1;
    assert!(matches!(
        marker.version.binding,
        PhysicalObjectBindingV1::DeleteMarker { .. }
    ));

    let reserved = repository
        .put_bytes(
            "main",
            b".prolly/prolly-s3/test/internal".to_vec(),
            vec![1],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(reserved.code, ErrorCode::InvalidKey);
}

#[tokio::test]
async fn warm_physical_put_is_exactly_three_foreground_calls() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/call-budget"),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"warmup.bin".to_vec(),
            vec![1; 64 * 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    plane.reset_request_counts();
    repository
        .put_bytes(
            "main",
            b"measured.bin".to_vec(),
            vec![2; 64 * 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let requests = plane.request_snapshot();
    assert_eq!(requests.physical_put, 1);
    assert_eq!(requests.immutable_put, 1);
    assert_eq!(requests.compare_exchange, 1);
    assert_eq!(requests.total(), 3, "unexpected calls: {requests:?}");
}

#[tokio::test]
async fn physical_writer_queue_preserves_three_calls_at_1_8_and_32_callers() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/concurrent-budget"),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"warmup.bin".to_vec(),
            vec![0; 64 * 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    for writers in [1usize, 8, 32] {
        plane.reset_request_counts();
        let writes = (0..writers).map(|index| {
            repository.put_bytes(
                "main",
                format!("tier-{writers}/object-{index}.bin").into_bytes(),
                vec![index as u8; 64 * 1024],
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
        });
        for result in futures_util::future::join_all(writes).await {
            result.unwrap();
        }
        let requests = plane.request_snapshot();
        assert_eq!(
            requests.total(),
            (writers * 3) as u64,
            "{writers}-writer tier made unexpected calls: {requests:?}"
        );
        assert_eq!(requests.physical_put, writers as u64);
        assert_eq!(requests.immutable_put, writers as u64);
        assert_eq!(requests.compare_exchange, writers as u64);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_upload_payloads_before_serial_publication() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    plane.set_physical_put_delay_millis(20);
    let mut options = physical_options(".prolly/prolly-s3/parallel-writers");
    options.max_parallel_payload_writes = 3;
    let repository = Repository::initialize(plane.clone(), options)
        .await
        .unwrap();
    plane.set_compare_exchange_delay_millis(20);
    plane.reset_compare_exchange_concurrency();
    plane.reset_physical_put_concurrency();
    let writes = (0..8).map(|index| {
        repository.put_bytes(
            "main",
            format!("writers/{index}.bin").into_bytes(),
            vec![index; 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
    });
    for result in futures_util::future::join_all(writes).await {
        result.unwrap();
    }
    assert!(plane.max_physical_puts_in_flight() > 1);
    assert_eq!(plane.max_physical_puts_in_flight(), 3);
    assert_eq!(plane.max_compare_exchanges_in_flight(), 1);
    let performance = repository.performance_snapshot();
    assert_eq!(performance.publication_acquisitions, 8);
    assert!(performance.publication_max_queue_depth >= 1);
    assert_eq!(performance.publication_queue_depth, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_idempotent_retries_upload_only_one_physical_version() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    plane.set_physical_put_delay_millis(20);
    let repository = Repository::initialize(
        plane.clone(),
        physical_options(".prolly/prolly-s3/idempotent-singleflight"),
    )
    .await
    .unwrap();
    let operation = OperationId::new();
    plane.reset_request_counts();

    let writes = (0..8).map(|_| {
        repository.put_bytes(
            "main",
            b"same-operation.bin".to_vec(),
            vec![7; 64 * 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            Some(operation),
        )
    });
    let receipts = futures_util::future::join_all(writes)
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();

    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| !receipt.idempotent_replay)
            .count(),
        1
    );
    assert!(receipts
        .iter()
        .skip(1)
        .all(|receipt| receipt.id == receipts[0].id));
    let requests = plane.request_snapshot();
    assert_eq!(requests.physical_put, 1);
    assert_eq!(requests.immutable_put, 1);
    assert_eq!(requests.compare_exchange, 1);
}

#[tokio::test]
async fn physical_gc_deletes_only_unreachable_exact_versions() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository =
        Repository::initialize(plane.clone(), physical_options(".prolly/prolly-s3/gc"))
            .await
            .unwrap();
    repository
        .put_bytes(
            "main",
            b"retained.bin".to_vec(),
            b"retained".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let orphan = plane
        .put_physical(PhysicalPut {
            path: ObjectPath::new("orphan.bin").unwrap(),
            bytes: b"orphan".to_vec(),
            headers: ObjectHeaders::default(),
            user_metadata: BTreeMap::new(),
            repository: repository.repository_id(),
            operation: OperationId::new(),
            writer_fence_generation: 1,
        })
        .await
        .unwrap();
    let PhysicalObjectBindingV1::Live { version_id, .. } = orphan.binding else {
        unreachable!()
    };

    let plan = repository.plan_gc(120_000, 10).await.unwrap();
    assert_eq!(plan.plan.body.candidates.len(), 1);
    assert_eq!(plan.plan.body.candidates[0].path.as_str(), "orphan.bin");
    repository.sweep_gc(plan.plan.id).await.unwrap();
    assert!(plane
        .get(prolly_s3_core::GetRequest {
            path: ObjectPath::new("orphan.bin").unwrap(),
            range: None,
            physical_version: Some(PhysicalVersion::Versioned { version_id }),
        })
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        repository
            .get_current("main", b"retained.bin")
            .await
            .unwrap()
            .bytes,
        b"retained"
    );
}

#[tokio::test]
async fn checkpointed_reopen_uses_ranged_nodes_without_pack_listing() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = physical_options(".prolly/prolly-s3/checkpoint");
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    for index in 0..8 {
        repository
            .put_bytes(
                "main",
                format!("objects/{index}.bin").into_bytes(),
                vec![index; 4096],
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    let checkpoint = repository
        .create_node_index_checkpoint("main")
        .await
        .unwrap();
    assert!(!checkpoint.entries.is_empty());

    plane.reset_request_counts();
    let reopened = Repository::open(
        plane.clone(),
        RepositoryOptions {
            read_only: true,
            ..options
        },
    )
    .await
    .unwrap();
    assert_eq!(plane.request_snapshot().list, 0);
    plane.reset_request_counts();
    assert_eq!(
        reopened
            .get_current("main", b"objects/7.bin")
            .await
            .unwrap()
            .bytes,
        vec![7; 4096]
    );
    let requests = plane.request_snapshot();
    assert_eq!(
        requests.list, 0,
        "point read rebuilt by listing: {requests:?}"
    );
    assert!(requests.get >= 2, "cold read accounting was incomplete");
}

#[tokio::test]
async fn legacy_node_lookup_finds_entries_evicted_from_the_bounded_locator() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = RepositoryOptions {
        max_cached_node_locations: 1,
        max_cached_node_pack_bytes: 1,
        max_cached_node_bytes: 1,
        ..physical_options(".prolly/prolly-s3/bounded-legacy-locator")
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    for index in 0..8 {
        repository
            .put_bytes(
                "main",
                format!("objects/{index}.bin").into_bytes(),
                vec![index; 4096],
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    drop(repository);

    let reopened = Repository::open(
        plane,
        RepositoryOptions {
            read_only: true,
            ..options
        },
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .get_current("main", b"objects/0.bin")
            .await
            .unwrap()
            .bytes,
        vec![0; 4096]
    );
}

#[tokio::test]
async fn sharded_node_index_opens_lazily_and_node_cache_eliminates_repeat_ranges() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = RepositoryOptions {
        max_cached_node_pack_bytes: 1,
        ..physical_options(".prolly/prolly-s3/node-index-v2")
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    for index in 0..64 {
        repository
            .put_bytes(
                "main",
                format!("objects/{index:04}.bin").into_bytes(),
                vec![index; 4096],
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    let advance = repository.advance_node_index_v2(1_000).await.unwrap();
    assert!(advance.completed_scan);
    assert!(advance.indexed_commit_objects >= 65);
    assert!(advance.indexed_node_entries > 0);

    plane.reset_request_counts();
    let shared_node_cache = Arc::new(MemoryNodeCache::new(64 * 1024 * 1024));
    let reopened = Repository::open(
        plane.clone(),
        RepositoryOptions {
            read_only: true,
            node_cache: Some(shared_node_cache.clone()),
            ..options.clone()
        },
    )
    .await
    .unwrap();
    assert_eq!(plane.request_snapshot().list, 0);

    plane.reset_request_counts();
    assert_eq!(
        reopened
            .get_current("main", b"objects/0000.bin")
            .await
            .unwrap()
            .bytes,
        vec![0; 4096]
    );
    assert_eq!(plane.request_snapshot().list, 0);
    let after_cold = reopened.performance_snapshot();
    assert!(after_cold.node_ranged_fetches > 0);

    plane.reset_request_counts();
    assert_eq!(
        reopened
            .get_current("main", b"objects/0000.bin")
            .await
            .unwrap()
            .bytes,
        vec![0; 4096]
    );
    let after_warm = reopened.performance_snapshot();
    assert_eq!(
        after_warm.node_ranged_fetches,
        after_cold.node_ranged_fetches
    );
    assert_eq!(plane.request_snapshot().list, 0);

    let warm_reopen = Repository::open(
        plane.clone(),
        RepositoryOptions {
            read_only: true,
            node_cache: Some(shared_node_cache),
            ..options
        },
    )
    .await
    .unwrap();
    plane.reset_request_counts();
    assert_eq!(
        warm_reopen
            .get_current("main", b"objects/0000.bin")
            .await
            .unwrap()
            .bytes,
        vec![0; 4096]
    );
    let shared_warm = warm_reopen.performance_snapshot();
    assert!(shared_warm.node_cache_hits > 0);
    assert_eq!(shared_warm.node_ranged_fetches, 0);
    assert_eq!(plane.request_snapshot().list, 0);
}

#[tokio::test]
async fn writer_populates_shared_node_cache_for_a_zero_range_reopen() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let shared_node_cache = Arc::new(MemoryNodeCache::new(64 * 1024 * 1024));
    let options = RepositoryOptions {
        max_cached_node_pack_bytes: 1,
        node_cache: Some(shared_node_cache.clone()),
        ..physical_options(".prolly/prolly-s3/write-through-node-cache")
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    for index in 0..128 {
        repository
            .put_bytes(
                "main",
                format!("objects/{index:04}.bin").into_bytes(),
                vec![index as u8; 1024],
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    let head = repository.head("main").await.unwrap();
    drop(repository);

    let reader = Repository::open(
        plane.clone(),
        RepositoryOptions {
            read_only: true,
            node_cache: Some(shared_node_cache),
            ..options
        },
    )
    .await
    .unwrap();
    plane.reset_request_counts();
    let (objects, truncated) = reader
        .list_objects_at(head, b"objects/", None, 1_000)
        .await
        .unwrap();

    assert_eq!(objects.len(), 128);
    assert!(!truncated);
    assert_eq!(reader.performance_snapshot().node_ranged_fetches, 0);
    assert!(plane.request_snapshot().get <= 1);
}

#[tokio::test]
async fn corrupt_external_node_cache_fails_open_and_is_invalidated() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = RepositoryOptions {
        max_cached_node_pack_bytes: 1,
        ..physical_options(".prolly/prolly-s3/corrupt-node-cache")
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    for index in 0..64 {
        repository
            .put_bytes(
                "main",
                format!("objects/{index:04}.bin").into_bytes(),
                vec![index; 2048],
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    repository.advance_node_index_v2(1_000).await.unwrap();
    let cache = Arc::new(CorruptNodeCache::default());
    let reopened = Repository::open(
        plane,
        RepositoryOptions {
            read_only: true,
            node_cache: Some(cache.clone()),
            ..options
        },
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .get_current("main", b"objects/0000.bin")
            .await
            .unwrap()
            .bytes,
        vec![0; 2048]
    );
    assert!(cache.removals.load(Ordering::Relaxed) > 0);
    assert!(reopened.performance_snapshot().node_cache_corruptions > 0);
}

#[tokio::test]
async fn scale_indexes_and_history_are_bounded_and_resumable() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = RepositoryOptions {
        history_traversal_limit: 3,
        ..physical_options(".prolly/prolly-s3/scale-metadata-v2")
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    for index in 0..12 {
        repository
            .put_bytes(
                "main",
                format!("history/{index:02}").into_bytes(),
                vec![index; 64],
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    let root = repository.head("main").await.unwrap();
    for index in 0..7 {
        repository
            .create_branch(&format!("branch-{index:02}"), root)
            .await
            .unwrap();
        repository
            .create_tag(&format!("tag-{index:02}"), root)
            .await
            .unwrap();
    }

    let first = repository
        .log_page_bounded(
            root,
            None,
            4,
            TraversalBudget {
                max_commits: 4,
                max_decoded_bytes: 1024 * 1024,
                max_elapsed: Duration::from_secs(1),
            },
        )
        .await
        .unwrap();
    assert_eq!(first.commits.len(), 4);
    assert!(first.continuation.is_some());
    let second = repository
        .log_page_bounded(
            root,
            first.continuation.as_ref(),
            4,
            TraversalBudget::default(),
        )
        .await
        .unwrap();
    assert_eq!(second.commits.len(), 4);
    assert_ne!(first.commits[3].0, second.commits[0].0);
    let diff = repository
        .diff_page_bounded(first.commits[3].0, root, None, 1)
        .await
        .unwrap();
    assert_eq!(diff.changes.len(), 1);
    assert!(diff.continuation.is_some());
    let resumed_diff = repository
        .diff_page_bounded(first.commits[3].0, root, diff.continuation.as_ref(), 1)
        .await
        .unwrap();
    assert_eq!(resumed_diff.changes.len(), 1);

    let branches = repository.advance_ref_catalog_v2(1_000).await.unwrap();
    assert!(!branches.completed_scan);
    let tags = repository.advance_ref_catalog_v2(1_000).await.unwrap();
    assert!(tags.completed_scan);
    let first_branches = repository.list_branch_catalog_page(None, 3).await.unwrap();
    assert_eq!(first_branches.branches.len(), 3);
    assert!(first_branches.continuation.is_some());
    assert_eq!(first_branches.freshness.scan_epoch, 1);
    let next_branches = repository
        .list_branch_catalog_page(first_branches.continuation.as_deref(), 3)
        .await
        .unwrap();
    assert_eq!(next_branches.branches.len(), 3);
    assert!(first_branches.branches[2].name < next_branches.branches[0].name);
    assert_eq!(
        repository
            .list_tag_catalog_page(None, 1_000)
            .await
            .unwrap()
            .tags
            .len(),
        7
    );

    let mut graph = repository.advance_commit_graph_v2(1_000).await.unwrap();
    assert!(graph.completed_scan);
    assert!(graph.indexed_commit_objects >= 13);
    for _ in 0..4 {
        graph = repository.advance_commit_graph_v2(1_000).await.unwrap();
        assert!(graph.completed_scan);
    }
    let skipped = repository
        .first_parent_ancestor_bounded(root, 8, None, 4)
        .await
        .unwrap();
    assert!(skipped.ancestor.is_some());
    assert!(skipped.continuation.is_none());
    assert_eq!(skipped.fallback_commit_reads, 0);
    assert!(skipped.index_reads <= 4);

    plane.reset_request_counts();
    let reopened = Repository::open(
        plane.clone(),
        RepositoryOptions {
            read_only: true,
            ..options
        },
    )
    .await
    .unwrap();
    assert_eq!(plane.request_snapshot().list, 0);
    assert_eq!(
        reopened
            .list_branch_catalog_page(None, 2)
            .await
            .unwrap()
            .branches
            .len(),
        2
    );
}

#[tokio::test]
async fn corrupt_scale_indexes_fail_open_and_rebuild_from_authority() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = physical_options(".prolly/prolly-s3/corrupt-scale-metadata-v2");
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    repository
        .put_bytes(
            "main",
            b"authoritative.bin".to_vec(),
            b"still readable".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    repository.advance_node_index_v2(1_000).await.unwrap();
    repository.advance_ref_catalog_v2(1_000).await.unwrap();
    repository.advance_ref_catalog_v2(1_000).await.unwrap();
    repository.advance_commit_graph_v2(1_000).await.unwrap();

    let prefix = &options.repository_prefix;
    for suffix in [
        "node-index/v2/head.cbor",
        "ref-catalog/v2/head.cbor",
        "commit-graph/v2/head.cbor",
    ] {
        corrupt_mutable_head(&plane, &format!("{prefix}/{suffix}")).await;
    }

    let fail_open = Repository::open(
        plane.clone(),
        RepositoryOptions {
            read_only: true,
            ..options.clone()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        fail_open
            .get_current("main", b"authoritative.bin")
            .await
            .unwrap()
            .bytes,
        b"still readable"
    );

    repository.advance_node_index_v2(1_000).await.unwrap();
    repository.advance_ref_catalog_v2(1_000).await.unwrap();
    repository.advance_ref_catalog_v2(1_000).await.unwrap();
    repository.advance_commit_graph_v2(1_000).await.unwrap();

    let repaired = Repository::open(
        plane,
        RepositoryOptions {
            read_only: true,
            ..options
        },
    )
    .await
    .unwrap();
    assert_eq!(
        repaired
            .list_branch_catalog_page(None, 10)
            .await
            .unwrap()
            .branches
            .len(),
        1
    );
}

#[tokio::test]
async fn corrupt_v1_checkpoint_is_ignored_without_eager_namespace_scan() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = physical_options(".prolly/prolly-s3/checkpoint-corrupt");
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    repository
        .put_bytes(
            "main",
            b"rebuild.bin".to_vec(),
            b"canonical".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    repository
        .create_node_index_checkpoint("main")
        .await
        .unwrap();
    let listed = plane
        .list(prolly_s3_core::ListRequest {
            prefix: format!("{}/node-index/checkpoints/", options.repository_prefix),
            continuation: None,
            limit: 10,
            include_versions: false,
        })
        .await
        .unwrap();
    let checkpoint = listed.entries.into_iter().next().unwrap();
    plane
        .delete_exact(
            &checkpoint.path,
            PhysicalVersion::Versioned {
                version_id: checkpoint.metadata.token.version_id.clone().unwrap(),
            },
        )
        .await
        .unwrap();
    plane
        .put_immutable(prolly_s3_core::ImmutablePut {
            path: checkpoint.path,
            bytes: b"not canonical cbor".to_vec(),
            expected_sha256: Sha256::digest(b"not canonical cbor").into(),
        })
        .await
        .unwrap();

    plane.reset_request_counts();
    let reopened = Repository::open(
        plane.clone(),
        RepositoryOptions {
            read_only: true,
            ..options
        },
    )
    .await
    .unwrap();
    assert_eq!(plane.request_snapshot().list, 0);
    plane.reset_request_counts();
    assert_eq!(
        reopened
            .get_current("main", b"rebuild.bin")
            .await
            .unwrap()
            .bytes,
        b"canonical"
    );
    assert_eq!(plane.request_snapshot().list, 0);
}

#[tokio::test]
async fn explicit_takeover_barrier_fences_the_old_writer() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = physical_options(".prolly/prolly-s3/takeover");
    let old_writer = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    old_writer
        .put_bytes(
            "main",
            b"before.bin".to_vec(),
            b"before".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let mut new_writer = Repository::open(
        plane.clone(),
        RepositoryOptions {
            writer: "replacement-writer".to_string(),
            read_only: true,
            ..options
        },
    )
    .await
    .unwrap();
    assert_eq!(
        new_writer
            .takeover_physical_writer(
                "physical-writer",
                1,
                "old credentials revoked and process stopped",
            )
            .await
            .unwrap(),
        2
    );
    new_writer
        .put_bytes(
            "main",
            b"after.bin".to_vec(),
            b"after".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        old_writer.renew_writer_lease().await.unwrap_err().code,
        ErrorCode::PreconditionFailed
    );
    plane.reset_request_counts();

    let stale = old_writer
        .put_bytes(
            "main",
            b"stale.bin".to_vec(),
            b"stale".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(stale.code, ErrorCode::PreconditionFailed);
    assert_eq!(plane.request_snapshot().physical_put, 0);
}
