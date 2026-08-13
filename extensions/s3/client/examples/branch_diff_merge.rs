//! Branch isolation, bounded diff, structural merge, log, and reflog.

mod common;

use common::ExampleResult;
use futures_util::StreamExt;
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
    feature
        .put_object("features/search.txt", b"enabled\n".to_vec())
        .await?;
    let selected_feature = feature
        .put_object("features/filters.txt", b"enabled\n".to_vec())
        .await?
        .id;
    let historical_feature = feature.checkout(selected_feature).await?;
    let feature_head = feature
        .put_object("features/post-snapshot.txt", b"not historical\n".to_vec())
        .await?
        .id;
    let main_head = main
        .put_object("release/version.txt", b"2026.8\n".to_vec())
        .await?
        .id;

    // Detached pagination and streaming remain pinned to the selected commit
    // even after the source branch advances. The checkout retains `feature`
    // as its branch-derived node-index context for efficient node resolution.
    let first_historical_page = historical_feature
        .list_objects_page("features/", None, 1)
        .await?;
    assert_eq!(first_historical_page.snapshot, selected_feature);
    let second_historical_page = historical_feature
        .list_objects_page(
            "features/",
            first_historical_page.continuation.as_deref(),
            1,
        )
        .await?;
    let paged_historical = first_historical_page
        .objects
        .into_iter()
        .chain(second_historical_page.objects)
        .map(|object| object.key)
        .collect::<Vec<_>>();
    assert_eq!(
        paged_historical,
        vec![
            b"features/filters.txt".to_vec(),
            b"features/search.txt".to_vec(),
        ]
    );
    assert!(second_historical_page.continuation.is_none());

    let historical_stream = historical_feature.stream_objects("features/", 1);
    futures_util::pin_mut!(historical_stream);
    let mut streamed_historical = Vec::new();
    while let Some(object) = historical_stream.next().await {
        streamed_historical.push(object?.key);
    }
    assert_eq!(streamed_historical, paged_historical);

    let diff = feature.diff_bounded(base, feature_head, None, 100).await?;
    println!("feature_changed_keys={}", diff.changes.len());
    println!("historical_listed_keys={}", paged_historical.len());

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
