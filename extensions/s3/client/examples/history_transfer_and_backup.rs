//! Full commit-DAG transfer between repositories and logical backup checking.

mod common;

use common::ExampleResult;
use prolly_s3_client::core::{MergePhase, MergePolicy};

#[tokio::main]
async fn main() -> ExampleResult {
    let source_repository = common::initialize("history-transfer-source").await?;
    let destination_repository = common::initialize("history-transfer-destination").await?;
    let source = source_repository.client;
    let destination = destination_repository.client;

    // Build a source history containing a real two-parent merge.
    let base = source
        .put_object("data/base.txt", b"base\n".to_vec())
        .await?
        .id;
    source.create_branch("feature", Some(base)).await?;
    source
        .put_object("data/main.txt", b"main\n".to_vec())
        .await?;
    source
        .checkout("feature")
        .await?
        .put_object("data/feature.txt", b"feature\n".to_vec())
        .await?;
    let mut merge = source
        .start_merge("feature", None, MergePolicy::Fail, "merge feature history")
        .await?;
    while merge.phase != MergePhase::ReadyToPublish {
        merge = source.advance_merge(&merge, 100).await?.cursor;
    }
    let source_head = source.publish_merge(&merge).await?.id;

    // The history variant maps every source commit parent-first and preserves
    // merge topology. IDs change because payload bindings and repository
    // identity are destination-local.
    let destination_initial = destination.head().await?;
    let mut transfer = destination
        .start_history_clone_from(&source, source_head, destination_initial)
        .await?;
    while !transfer.complete {
        transfer = destination
            .advance_history_transfer_from(&source, &transfer, 100)
            .await?
            .cursor;
        // Persist `transfer` after every page for restartability.
    }
    let mapped_head = transfer.mapped_head.ok_or("source head was not mapped")?;
    destination
        .publish_history_transfer(&transfer, "publish imported source history")
        .await?;

    // Backup verification compares logical keys and downloads both sides to
    // verify bytes, rather than trusting provider metadata alone.
    let mut verification = source
        .start_backup_verification(&destination, source_head, mapped_head)
        .await?;
    while !verification.complete {
        verification = source
            .advance_backup_verification(&destination, &verification, 100)
            .await?
            .cursor;
    }

    let imported = destination.commit(mapped_head).await?;
    println!("source_prefix={}", source_repository.prefix);
    println!("destination_prefix={}", destination_repository.prefix);
    println!("source_head={source_head}");
    println!("mapped_head={mapped_head}");
    println!("mapped_merge_parents={}", imported.parents.len());
    println!("imported_commits={}", transfer.report.imported_commits);
    println!("verified_objects={}", verification.report.objects_verified);
    println!(
        "verified_content_bytes={}",
        verification.report.content_bytes_verified
    );
    Ok(())
}
