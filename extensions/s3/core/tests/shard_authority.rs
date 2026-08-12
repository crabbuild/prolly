use std::{sync::Arc, time::Duration};

use prolly_s3_core::{
    AuthorityScopeV2, ErrorCode, MemoryObjectPlane, OperationId, RepositoryId,
    ShardWriterAuthorityV2, TakeoverRequestV2,
};

fn branch(name: &str) -> AuthorityScopeV2 {
    AuthorityScopeV2::Branch {
        name: name.to_string(),
    }
}

fn operation(value: u128) -> OperationId {
    OperationId(uuid::Uuid::from_u128(value))
}

#[tokio::test]
async fn independent_branch_shards_have_independent_writers() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let authority = ShardWriterAuthorityV2::new(
        plane.clone(),
        ".prolly/v2",
        RepositoryId::from_hash([7; 32]),
        Duration::from_secs(60),
    )
    .unwrap();

    plane.set_compare_exchange_delay_millis(25);
    plane.reset_compare_exchange_concurrency();
    let (main, ingest) = tokio::join!(
        authority.acquire(branch("main"), "writer-a", 1_000, operation(1)),
        authority.acquire(branch("ingest"), "writer-b", 1_000, operation(2)),
    );
    let main = main.unwrap();
    let ingest = ingest.unwrap();
    assert_eq!(plane.max_compare_exchanges_in_flight(), 2);

    assert_ne!(main.stamp().scope, ingest.stamp().scope);
    assert_eq!(main.stamp().writer_id, "writer-a");
    assert_eq!(ingest.stamp().writer_id, "writer-b");
    authority.validate_active(&main, 1_001).await.unwrap();
    authority.validate_active(&ingest, 1_001).await.unwrap();

    let conflict = authority
        .acquire(branch("main"), "writer-b", 1_001, operation(3))
        .await
        .unwrap_err();
    assert_eq!(conflict.code, ErrorCode::PreconditionFailed);
}

#[tokio::test]
async fn takeover_is_pending_until_the_branch_ref_barrier_completes() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let authority = ShardWriterAuthorityV2::new(
        plane,
        ".prolly/v2",
        RepositoryId::from_hash([8; 32]),
        Duration::from_secs(60),
    )
    .unwrap();
    let old = authority
        .acquire(branch("main"), "writer-a", 1_000, operation(10))
        .await
        .unwrap();

    let pending = authority
        .begin_takeover(TakeoverRequestV2 {
            scope: branch("main"),
            expected_writer: "writer-a".to_string(),
            expected_generation: 1,
            next_writer: "writer-b".to_string(),
            handoff_evidence: "old credentials revoked".to_string(),
            now_millis: 2_000,
            nonce: operation(11),
        })
        .await
        .unwrap();
    assert_eq!(pending.stamp().generation, 2);
    assert_eq!(pending.stamp().writer_id, "writer-b");

    let stale = authority.validate_active(&old, 2_001).await.unwrap_err();
    assert_eq!(stale.code, ErrorCode::PreconditionFailed);

    let resumed = authority
        .begin_takeover(TakeoverRequestV2 {
            scope: branch("main"),
            expected_writer: "writer-a".to_string(),
            expected_generation: 1,
            next_writer: "writer-b".to_string(),
            handoff_evidence: "old credentials revoked".to_string(),
            now_millis: 2_001,
            nonce: operation(12),
        })
        .await
        .unwrap();
    assert_eq!(resumed.stamp(), pending.stamp());

    // Repository v2 performs the no-target-change branch-ref CAS here. The
    // activation method is crate-private so callers cannot skip that barrier.
}

#[tokio::test]
async fn ambiguous_or_concurrent_renewal_fences_only_that_shard() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let authority = ShardWriterAuthorityV2::new(
        plane,
        ".prolly/v2",
        RepositoryId::from_hash([9; 32]),
        Duration::from_secs(60),
    )
    .unwrap();
    let main = authority
        .acquire(branch("main"), "writer-a", 1_000, operation(20))
        .await
        .unwrap();
    let ingest = authority
        .acquire(branch("ingest"), "writer-b", 1_000, operation(21))
        .await
        .unwrap();

    let renewed = authority.renew(main.clone(), 2_000).await.unwrap();
    let stale_renewal = authority.renew(main, 2_001).await.unwrap_err();
    assert_eq!(stale_renewal.code, ErrorCode::PreconditionFailed);
    authority.validate_active(&renewed, 2_002).await.unwrap();
    authority.validate_active(&ingest, 2_002).await.unwrap();
}

#[tokio::test]
async fn applied_cas_with_lost_response_reconciles_to_the_same_permit() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let authority = ShardWriterAuthorityV2::new(
        plane.clone(),
        ".prolly/v2",
        RepositoryId::from_hash([10; 32]),
        Duration::from_secs(60),
    )
    .unwrap();
    plane.conflict_after_next_compare_exchange();
    let permit = authority
        .acquire(branch("main"), "writer-a", 1_000, operation(30))
        .await
        .unwrap();
    authority.validate_active(&permit, 1_001).await.unwrap();
}

#[tokio::test]
async fn branch_authority_uses_the_canonical_ref_name_contract() {
    let authority = ShardWriterAuthorityV2::new(
        Arc::new(MemoryObjectPlane::new(true)),
        ".prolly/v2",
        RepositoryId::from_hash([11; 32]),
        Duration::from_secs(60),
    )
    .unwrap();
    let error = authority
        .acquire(branch("invalid..branch"), "writer-a", 1_000, operation(40))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidBranch);
}
