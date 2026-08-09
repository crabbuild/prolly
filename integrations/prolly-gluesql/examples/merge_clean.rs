//! A clean, typed three-way merge with rebuilt derived state.
//!
//! Run with: `cargo run --example merge_clean`

use {
    prolly_gluesql::{gluesql_core::prelude::Value, Glue, MergeResult, Payload, ProllyStorage},
    std::error::Error,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let storage = ProllyStorage::in_memory()?;
    let mut db = Glue::new(storage);

    db.execute(
        "CREATE TABLE tickets (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            title TEXT NOT NULL
        );",
    )
    .await?;
    db.execute("INSERT INTO tickets VALUES (1, 'open', 'base ticket');")
        .await?;
    let base = db.storage.head()?.unwrap();
    db.storage.create_branch("feature")?;

    // Current/main changes one row and adds an index.
    db.execute("UPDATE tickets SET title = 'edited on main' WHERE id = 1;")
        .await?;
    db.execute("CREATE INDEX tickets_status ON tickets (status);")
        .await?;

    // Incoming/feature adds a disjoint row from the same base.
    db.storage.checkout_branch("feature")?;
    db.execute("INSERT INTO tickets VALUES (2, 'open', 'added on feature');")
        .await?;
    let incoming = db.storage.head()?.unwrap();

    db.storage.checkout_branch("main")?;
    let result = db.storage.merge(&base, &incoming).await?;
    let MergeResult::Applied { version, changes } = result else {
        return Err("disjoint changes should merge cleanly".into());
    };
    assert_eq!(changes.rows.len(), 1);
    assert!(changes.schemas.is_empty());

    // This predicate uses the main-side index, rebuilt with the incoming row.
    let payloads = db
        .execute("SELECT id, title FROM tickets WHERE status = 'open' ORDER BY id;")
        .await?;
    let Payload::Select { rows, .. } = &payloads[0] else {
        return Err("expected a SELECT payload".into());
    };
    assert_eq!(
        rows,
        &vec![
            vec![Value::I64(1), Value::Str("edited on main".to_owned())],
            vec![Value::I64(2), Value::Str("added on feature".to_owned())],
        ]
    );

    println!("merged version: {}", version.id().unwrap());
    println!("changes applied to main: {changes:#?}");
    println!("merged indexed rows: {rows:#?}");
    Ok(())
}
