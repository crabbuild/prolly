//! Shared backend connections and optimistic serialization conflicts.
//!
//! Run with: `cargo run --example concurrent_writers`

use {
    prolly::{ManifestStore, MemStore, Store},
    prolly_gluesql::{gluesql_core::prelude::Value, Glue, Payload, ProllyStorage},
    std::{error::Error, sync::Arc},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // ProllyStorage accepts any backend implementing Store + ManifestStore.
    let backend = Arc::new(MemStore::new());
    assert_backend(&backend);
    let mut first = Glue::new(ProllyStorage::new(Arc::clone(&backend))?);
    first
        .execute("CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER NOT NULL);")
        .await?;
    first.execute("INSERT INTO counters VALUES (1, 0);").await?;

    let mut second = Glue::new(ProllyStorage::new(Arc::clone(&backend))?);
    first.execute("START TRANSACTION;").await?;
    second.execute("START TRANSACTION;").await?;
    first
        .execute("UPDATE counters SET value = 1 WHERE id = 1;")
        .await?;
    second
        .execute("UPDATE counters SET value = 2 WHERE id = 1;")
        .await?;

    first.execute("COMMIT;").await?;
    let conflict = second
        .execute("COMMIT;")
        .await
        .expect_err("the stale writer must lose compare-and-swap");
    assert!(conflict.to_string().contains("serialization conflict"));

    let payloads = first.execute("SELECT value FROM counters;").await?;
    assert!(matches!(
        &payloads[0],
        Payload::Select { rows, .. } if rows == &vec![vec![Value::I64(1)]]
    ));
    println!("second writer rejected: {conflict}");
    println!("the first writer's value remains committed");
    Ok(())
}

fn assert_backend<S: Store + ManifestStore>(_backend: &S) {}
