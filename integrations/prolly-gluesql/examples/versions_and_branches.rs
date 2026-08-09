//! Opaque versions, typed diffs, branches, historical reads, and reset.
//!
//! Run with: `cargo run --example versions_and_branches`

use {
    prolly_gluesql::{
        gluesql_core::prelude::{Key, Value},
        Glue, Payload, ProllyStorage, RowChange,
    },
    std::error::Error,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let storage = ProllyStorage::in_memory()?;
    let mut db = Glue::new(storage);

    db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL);")
        .await?;
    db.execute("INSERT INTO accounts VALUES (1, 100);").await?;
    let base = db.storage.head()?.expect("main has been published");
    db.storage.create_branch("experiment")?;

    db.execute("UPDATE accounts SET balance = 125 WHERE id = 1;")
        .await?;
    let updated_main = db.storage.head()?.unwrap();
    let diff = db.storage.diff(&base, &updated_main)?;
    assert!(matches!(
        diff.rows.as_slice(),
        [RowChange::Modified {
            table,
            key: Key::I64(1),
            before,
            after,
        }] if table == "accounts"
            && before == &prolly_gluesql::gluesql_core::store::DataRow::Vec(vec![
                Value::I64(1),
                Value::I64(100),
            ])
            && after == &prolly_gluesql::gluesql_core::store::DataRow::Vec(vec![
                Value::I64(1),
                Value::I64(125),
            ])
    ));

    db.storage.checkout_branch("experiment")?;
    db.execute("INSERT INTO accounts VALUES (2, 50);").await?;
    let experiment = db.storage.head()?.unwrap();

    db.storage.checkout_branch("main")?;
    assert_eq!(
        selected_rows(
            &db.execute("SELECT id, balance FROM accounts ORDER BY id;")
                .await?
        )?,
        vec![vec![Value::I64(1), Value::I64(125)]]
    );

    // Historical checkout opens a transaction pinned to the immutable version.
    db.storage.checkout(&base)?;
    assert_eq!(
        selected_rows(
            &db.execute("SELECT id, balance FROM accounts ORDER BY id;")
                .await?
        )?,
        vec![vec![Value::I64(1), Value::I64(100)]]
    );
    db.execute("ROLLBACK;").await?;

    // Reset and restore demonstrate compare-and-swap branch movement.
    db.storage.reset(&base)?;
    assert_eq!(
        selected_rows(
            &db.execute("SELECT id, balance FROM accounts ORDER BY id;")
                .await?
        )?,
        vec![vec![Value::I64(1), Value::I64(100)]]
    );
    db.storage.reset(&updated_main)?;

    println!("base:       {}", base.id().unwrap());
    println!("main:       {}", updated_main.id().unwrap());
    println!("experiment: {}", experiment.id().unwrap());
    println!("typed row change: {:#?}", diff.rows[0]);
    Ok(())
}

fn selected_rows(payloads: &[Payload]) -> Result<Vec<Vec<Value>>, Box<dyn Error>> {
    let Payload::Select { rows, .. } = &payloads[0] else {
        return Err("expected a SELECT payload".into());
    };
    Ok(rows.clone())
}
