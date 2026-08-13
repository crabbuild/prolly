//! Deep integrity checking, cache prewarming, metrics, retention pins, and GC.

mod common;

use std::time::Duration;

use common::ExampleResult;
use prolly_s3_client::{
    core::{FsckPhase, GcPhase},
    Client,
};

#[tokio::main]
async fn main() -> ExampleResult {
    let repository = common::initialize("integrity-gc-observability").await?;
    let client = repository.client;

    client
        .put_object("live/current.txt", b"current\n".to_vec())
        .await?;
    let main_head = client.head().await?;

    // Build two detached histories. The pin protects one; the other becomes a
    // valid GC candidate after its branch is deleted and the grace period ends.
    client.create_branch("pinned-work", Some(main_head)).await?;
    let pinned = client
        .for_branch("pinned-work")?
        .put_object("archive/pinned.txt", b"legal hold\n".to_vec())
        .await?
        .id;
    client.create_retention_pin("case-42", pinned).await?;
    client.delete_branch("pinned-work", pinned).await?;

    client
        .create_branch("abandoned-work", Some(main_head))
        .await?;
    let abandoned = client
        .for_branch("abandoned-work")?
        .put_object("scratch/abandoned.txt", b"collect me\n".to_vec())
        .await?
        .id;
    client.delete_branch("abandoned-work", abandoned).await?;

    // Deep fsck streams and hashes reachable payload bytes in addition to
    // checking commit, tree, version, and provider metadata.
    let mut fsck = client.start_fsck(true).await?;
    while fsck.phase != FsckPhase::Complete {
        fsck = client.advance_fsck(&fsck, 100).await?.cursor;
    }

    // Immutable nodes are ideal cache entries. Prewarming is optional and
    // affects performance only; cached bytes are always verified before use.
    let prewarm = client.prewarm_node_cache(main_head).await?;
    let cache = client.node_cache_snapshot();

    // The grace period must exceed the longest possible unpublished operation
    // in production. One millisecond is used only to keep this local demo fast.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let gc = run_gc(&client, 1).await?;
    client.commit(pinned).await?;

    let metrics = client.s3_operation_metrics();
    let health = client.branch_index_health().await?;
    println!("repository_prefix={}", repository.prefix);
    println!("fsck_commits={}", fsck.report.commits);
    println!("fsck_payloads={}", fsck.report.payloads_verified);
    println!("prewarmed_object_nodes={}", prewarm.object_nodes);
    println!("cache_hits={}", cache.hits);
    println!("gc_candidates={}", gc.report.candidates);
    println!("gc_deleted_versions={}", gc.report.deleted_versions);
    println!("retained_pinned_commit={pinned}");
    println!("s3_calls={}", metrics.total_calls());
    println!("index_lag_generations={}", health.lag_generations);
    Ok(())
}

async fn run_gc(
    client: &Client,
    grace_millis: u64,
) -> ExampleResult<prolly_s3_client::core::GcCursor> {
    let mut gc = client.start_gc(grace_millis).await?;
    loop {
        gc = match gc.phase {
            GcPhase::Ready | GcPhase::Sweeping => client.sweep_gc(&gc, 100).await?.cursor,
            GcPhase::Complete => return Ok(gc),
            _ => client.advance_gc(&gc, 100).await?.cursor,
        };
        // Persist `gc` after every page in an enterprise maintenance worker.
    }
}
