//! Atomic multi-file writes with durable checkpoints and streaming input.

mod common;

use aws_sdk_s3::primitives::ByteStream;
use common::ExampleResult;

#[tokio::main]
async fn main() -> ExampleResult {
    let repository = common::initialize("atomic-batch-and-streaming").await?;
    let client = repository.client;

    // A durable session uploads payloads as they are staged and checkpoints
    // canonical metadata remotely. Persist `batch_id` in a real bulk job so
    // another process can call `client.resume_commit(batch_id)` after failure.
    let mut session = client
        .begin_commit()
        .message("publish one consistent daily data set")
        .checkpoint_every(2)
        .start()
        .await?;
    let batch_id = session.id();
    println!("batch_id={batch_id}");

    session
        .put_object("daily/customers.csv", b"id,name\n1,Ada\n".to_vec())
        .await?;
    session
        .put_object("daily/orders.csv", b"id,total\n10,42\n".to_vec())
        .await?;

    // Streaming uses a bounded-memory temporary-file spool. The complete file
    // remains one immutable S3 object; this API does not chunk the logical file.
    session
        .put_stream(
            "daily/events.ndjson",
            ByteStream::from_static(b"{\"event\":\"created\"}\n"),
        )
        .await?;

    // Explicit checkpointing is useful immediately before acknowledging an
    // upstream offset. Automatic checkpoints still run at the chosen interval.
    session.checkpoint().await?;
    println!("staged_objects={}", session.staged_objects());

    // This one ref CAS makes all staged mutations visible together.
    let receipt = session.publish().await?;
    let (_, objects, truncated) = client.list_objects("daily/", None, 100).await?;

    println!("repository_prefix={}", repository.prefix);
    println!("commit={}", receipt.id);
    println!("changed_keys={}", receipt.changed_keys);
    println!("visible_objects={}", objects.len());
    println!("listing_truncated={truncated}");
    Ok(())
}
