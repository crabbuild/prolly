//! Runnable tour of Prolly S3 commits, branches, diffs, and merges.
//!
//! This uses the in-memory object plane, so it needs no AWS account or RustFS.
//! The repository operations are the same ones used by the S3-backed client.

use std::{collections::BTreeMap, sync::Arc};

use prolly_s3_core::{
    CommitReceipt, ErrorCode, MemoryObjectPlane, MergePolicy, ObjectHeaders, Repository,
    RepositoryOptions, Result, TraversalBudget,
};

#[tokio::main]
async fn main() -> Result<()> {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane,
        RepositoryOptions {
            repository_prefix: ".prolly/examples/branching".to_string(),
            writer: "walkthrough".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await?;

    println!("1. Build a shared history on main");
    put(&repository, "main", "README.md", "Welcome to the site\n").await?;
    let base = put(&repository, "main", "config/theme.txt", "light\n").await?;
    println!("   base commit: {}", base.id);

    println!("\n2. Create feature from that exact commit");
    repository.create_branch("feature", base.id).await?;
    put(
        &repository,
        "feature",
        "feature/banner.txt",
        "Try the new theme!\n",
    )
    .await?;
    let feature_head = put(&repository, "feature", "config/theme.txt", "dark\n").await?;
    println!("   feature head: {}", feature_head.id);

    println!("\n3. Let main diverge");
    put(&repository, "main", "release.txt", "1.0\n").await?;
    let main_head = put(&repository, "main", "config/theme.txt", "solarized\n").await?;
    println!("   main head:    {}", main_head.id);

    println!("\n4. Diff base..feature one result at a time");
    let mut cursor = None;
    loop {
        let page = repository
            .diff_page_bounded(base.id, feature_head.id, cursor.as_ref(), 1)
            .await?;

        for change in page.changes {
            println!(
                "   {:<20} {} -> {}",
                display_key(&change.key),
                display_version(change.from),
                display_version(change.to),
            );
        }

        cursor = page.continuation;
        if cursor.is_none() {
            break;
        }
    }

    println!("\n5. Plan the merge before changing main");
    let plan = repository
        .plan_merge("main", feature_head.id, Some(base.id), MergePolicy::Fail)
        .await?;
    for conflict in &plan.conflicts {
        println!("   conflict: {}", display_key(&conflict.key));
    }
    assert_eq!(plan.conflicts.len(), 1);

    println!("\n6. A fail-on-conflict merge leaves main unchanged");
    let error = repository
        .merge(
            "main",
            feature_head.id,
            Some(base.id),
            MergePolicy::Fail,
            None,
            Some("merge feature".to_string()),
        )
        .await
        .expect_err("the theme edits must conflict");
    assert_eq!(error.code, ErrorCode::MergeConflict);
    assert_eq!(repository.head("main").await?, main_head.id);
    println!("   rejected: {}", error.message);

    println!("\n7. Resolve explicitly with the feature branch's value");
    let merged = repository
        .merge(
            "main",
            feature_head.id,
            Some(base.id),
            MergePolicy::Theirs,
            None,
            Some("merge feature using its theme".to_string()),
        )
        .await?;
    println!("   merge commit: {}", merged.id);
    println!("   parents:      {}", merged.parents.len());

    let theme = repository.get_current("main", b"config/theme.txt").await?;
    let banner = repository
        .get_current("main", b"feature/banner.txt")
        .await?;
    let release = repository.get_current("main", b"release.txt").await?;
    assert_eq!(theme.bytes, b"dark\n");
    assert_eq!(banner.bytes, b"Try the new theme!\n");
    assert_eq!(release.bytes, b"1.0\n");

    println!("   config/theme.txt  = {}", display_bytes(&theme.bytes));
    println!("   feature/banner.txt = {}", display_bytes(&banner.bytes));
    println!("   release.txt        = {}", display_bytes(&release.bytes));

    println!("\n8. Walk main's first-parent history");
    let history = repository
        .log_page_bounded(merged.id, None, 10, TraversalBudget::default())
        .await?;
    for (id, commit) in history.commits {
        println!(
            "   {}  parents={}  {}",
            id,
            commit.parents.len(),
            commit.message.as_deref().unwrap_or("(no message)"),
        );
    }

    println!("\nWalkthrough complete.");
    Ok(())
}

async fn put(
    repository: &Repository<MemoryObjectPlane>,
    branch: &str,
    key: &str,
    value: &str,
) -> Result<CommitReceipt> {
    repository
        .put_bytes(
            branch,
            key.as_bytes().to_vec(),
            value.as_bytes().to_vec(),
            ObjectHeaders {
                content_type: Some("text/plain".to_string()),
                ..ObjectHeaders::default()
            },
            BTreeMap::new(),
            None,
        )
        .await
}

fn display_key(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

fn display_version(version: Option<prolly_s3_core::ObjectVersionId>) -> String {
    version.map_or_else(|| "(absent)".to_string(), |version| version.to_string())
}

fn display_bytes(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim_end().to_string()
}
