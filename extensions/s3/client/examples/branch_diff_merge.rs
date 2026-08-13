//! Branch isolation, bounded diff, structural merge, log, and reflog.

mod common;

use common::ExampleResult;
use prolly_s3_client::core::{MergePhase, MergePolicy};

#[tokio::main]
async fn main() -> ExampleResult {
    let repository = common::initialize("branch-diff-merge").await?;
    let main = repository.client;

    let base = main
        .put_object("config/app.toml", b"color = \"blue\"\n".to_vec())
        .await?
        .id;
    main.create_branch("feature", Some(base)).await?;
    let feature = main.checkout("feature").await?;

    // Branches publish through independent ref lanes. Here each branch changes
    // a different key, so Fail policy can merge without resolving conflicts.
    let feature_head = feature
        .put_object("features/search.txt", b"enabled\n".to_vec())
        .await?
        .id;
    let main_head = main
        .put_object("release/version.txt", b"2026.8\n".to_vec())
        .await?
        .id;

    let diff = feature.diff_bounded(base, feature_head, None, 100).await?;
    println!("feature_changed_keys={}", diff.changes.len());

    // Merge construction is restartable. Persist the returned cursor after
    // every page for large repositories or long-running merge workers.
    let mut merge = main
        .start_merge("feature", None, MergePolicy::Fail, "merge search feature")
        .await?;
    while merge.phase != MergePhase::ReadyToPublish {
        merge = main.advance_merge(&merge, 100).await?.cursor;
    }
    let planned = main.merge_changes_page(&merge, None, 100).await?;
    let merged = main.publish_merge(&merge).await?;

    // Tags and commit IDs produce detached, immutable checkouts. Current-read
    // APIs automatically use the selected snapshot; mutation APIs reject a
    // detached checkout instead of accidentally publishing to another branch.
    main.create_tag("release-2026.8", merged.id).await?;
    let release = main.checkout("refs/tags/release-2026.8").await?;
    let exact_commit = main.checkout(merged.id).await?;
    let released_feature = release
        .get_object("features/search.txt")
        .await?
        .ok_or("released feature is missing")?;
    assert_eq!(release.branch(), None);
    assert_eq!(exact_commit.head().await?, merged.id);

    let history = main.log(10).await?;
    let reflog = main.open_reflog().await?;
    let reflog_page = main.read_reflog_page(&reflog, 10).await?;

    println!("repository_prefix={}", repository.prefix);
    println!("main_before_merge={main_head}");
    println!("merge_commit={}", merged.id);
    println!("merge_parents={},{}", merged.parents[0], merged.parents[1]);
    println!("planned_changes={}", planned.changes.len());
    println!("log_entries={}", history.len());
    println!("reflog_entries={}", reflog_page.entries.len());
    println!(
        "released_feature={}",
        String::from_utf8_lossy(&released_feature.bytes).trim()
    );
    Ok(())
}
