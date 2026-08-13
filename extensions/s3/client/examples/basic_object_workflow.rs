//! Basic whole-file CRUD, metadata, range reads, copies, and history.

mod common;

use std::collections::BTreeMap;

use common::ExampleResult;
use prolly_s3_client::core::{LogicalObjectVersionKind, ObjectHeaders};

#[tokio::main]
async fn main() -> ExampleResult {
    let repository = common::initialize("basic-object-workflow").await?;
    let client = repository.client;

    // A write publishes one immutable whole-file payload and one new commit.
    let mut metadata = BTreeMap::new();
    metadata.insert("owner".to_string(), "finance".to_string());
    let first = client
        .put_object_with_metadata(
            "reports/2026/summary.txt",
            b"revenue=42\nstatus=preliminary\n".to_vec(),
            ObjectHeaders {
                content_type: Some("text/plain; charset=utf-8".to_string()),
                ..ObjectHeaders::default()
            },
            metadata,
        )
        .await?;

    // Metadata and byte-range reads avoid downloading the complete body.
    let (_, head) = client
        .head_object("reports/2026/summary.txt")
        .await?
        .ok_or("new object is missing")?;
    let range = client
        .get_object_range(first.id, "reports/2026/summary.txt", 0..=9)
        .await?
        .ok_or("range source is missing")?;

    // Copying reuses the immutable payload binding instead of uploading the
    // body again. The destination still receives its own logical version.
    client
        .copy_object(
            first.id,
            "reports/2026/summary.txt",
            "archive/2026/summary.txt",
        )
        .await?;

    // A later write changes the current state while the first commit remains
    // an immutable historical snapshot.
    client
        .put_object(
            "reports/2026/summary.txt",
            b"revenue=43\nstatus=final\n".to_vec(),
        )
        .await?;
    let historical = client
        .get_object_at(first.id, "reports/2026/summary.txt")
        .await?
        .ok_or("historical object is missing")?;

    let page = client.list_objects_delimited("", "/", None, 100).await?;
    let (_, versions) = client
        .list_object_versions("reports/2026/summary.txt", 100)
        .await?;

    println!("bucket={}", repository.bucket);
    println!("repository_prefix={}", repository.prefix);
    println!("first_commit={}", first.id);
    let logical_etag = match &head.version.body.kind {
        LogicalObjectVersionKind::Live { logical_etag, .. } => logical_etag.as_str(),
        LogicalObjectVersionKind::DeleteMarker => "delete-marker",
    };
    println!("logical_etag={logical_etag}");
    println!("first_ten_bytes={}", String::from_utf8_lossy(&range.bytes));
    println!(
        "historical_body={}",
        String::from_utf8_lossy(&historical.bytes).trim()
    );
    println!("top_level_prefixes={}", page.common_prefixes.len());
    println!("logical_versions={}", versions.len());
    Ok(())
}
