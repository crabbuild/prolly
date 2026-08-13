//! Historical reads, restartable restore, reset, and reflog recovery.

mod common;

use common::ExampleResult;

#[tokio::main]
async fn main() -> ExampleResult {
    let repository = common::initialize("restore-and-recovery").await?;
    let client = repository.client;

    let known_good = client
        .put_object("service/config.json", br#"{"mode":"safe"}"#.to_vec())
        .await?
        .id;
    let unwanted = client
        .put_object("service/config.json", br#"{"mode":"broken"}"#.to_vec())
        .await?
        .id;

    // Restore recreates the selected snapshot as new logical versions. Unlike
    // reset, it preserves the existing branch history and is suitable when an
    // auditable forward-moving correction is required.
    let mut restore = client
        .start_restore(known_good, unwanted, "restore known-good configuration")
        .await?;
    let mut restored_commit = None;
    while !restore.complete {
        let page = client.advance_restore(&restore, 100).await?;
        if let Some(receipt) = page.receipt {
            restored_commit = Some(receipt.id);
        }
        restore = page.cursor;
        // Persist `restore` here in a real recovery controller.
    }
    let restored_commit = restored_commit.ok_or("restore published no commit")?;
    let restored = client
        .get_object("service/config.json")
        .await?
        .ok_or("restored object is missing")?;

    // Reset is an administrative ref movement. It is CAS-protected, requires
    // the expected current head, and records an immutable reflog event.
    let reset = client
        .reset_branch(
            known_good,
            restored_commit,
            "demonstrate administrative reset",
        )
        .await?;
    let cursor = client.open_reflog().await?;
    let page = client.read_reflog_page(&cursor, 10).await?;
    let reset_event = page
        .entries
        .iter()
        .find(|entry| entry.event.operation == reset.operation)
        .ok_or("reset event is absent from reflog")?;

    // Recovering that event moves the branch back to its previous target.
    let recovered = client
        .recover_branch(
            reset_event.event.reflog,
            reset.new_target,
            "undo demonstration reset from immutable reflog",
        )
        .await?;

    println!("repository_prefix={}", repository.prefix);
    println!("known_good={known_good}");
    println!("unwanted={unwanted}");
    println!("restored_commit={restored_commit}");
    println!("restored_body={}", String::from_utf8_lossy(&restored.bytes));
    println!("reset_target={}", reset.new_target);
    println!("recovered_target={}", recovered.new_target);
    Ok(())
}
