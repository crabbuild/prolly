//! Typed merge-conflict payloads and unchanged target branches.
//!
//! Run with: `cargo run --example merge_conflicts`

use {
    prolly_gluesql::{Glue, MergeConflict, MergeResult, ProllyStorage},
    std::error::Error,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    row_conflict().await?;
    schema_and_function_conflicts().await?;
    unique_constraint_conflict().await?;
    foreign_key_conflict().await?;
    Ok(())
}

async fn row_conflict() -> Result<(), Box<dyn Error>> {
    let mut db = Glue::new(ProllyStorage::in_memory()?);
    db.execute("CREATE TABLE settings (id INTEGER PRIMARY KEY, value TEXT NOT NULL);")
        .await?;
    db.execute("INSERT INTO settings VALUES (1, 'base');")
        .await?;
    let base = db.storage.head()?.unwrap();
    db.storage.create_branch("feature")?;

    db.execute("UPDATE settings SET value = 'current' WHERE id = 1;")
        .await?;
    let current = db.storage.head()?.unwrap();
    db.storage.checkout_branch("feature")?;
    db.execute("UPDATE settings SET value = 'incoming' WHERE id = 1;")
        .await?;
    let incoming = db.storage.head()?.unwrap();

    db.storage.checkout_branch("main")?;
    let result = db.storage.merge(&base, &incoming).await?;
    assert!(matches!(result.conflicts(), [MergeConflict::Row { .. }]));
    assert_eq!(db.storage.head()?.unwrap().id(), current.id());
    println!("row conflict: {:#?}", result.conflicts()[0]);
    Ok(())
}

async fn schema_and_function_conflicts() -> Result<(), Box<dyn Error>> {
    let mut db = Glue::new(ProllyStorage::in_memory()?);
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY);")
        .await?;
    db.execute("CREATE FUNCTION transform(n INT) RETURN n;")
        .await?;
    let base = db.storage.head()?.unwrap();
    db.storage.create_branch("feature")?;

    db.execute("ALTER TABLE items ADD COLUMN current_value TEXT NULL;")
        .await?;
    db.execute("DROP FUNCTION transform;").await?;
    db.execute("CREATE FUNCTION transform(n INT) RETURN n + 1;")
        .await?;
    let current = db.storage.head()?.unwrap();

    db.storage.checkout_branch("feature")?;
    db.execute("ALTER TABLE items ADD COLUMN incoming_value TEXT NULL;")
        .await?;
    db.execute("DROP FUNCTION transform;").await?;
    db.execute("CREATE FUNCTION transform(n INT) RETURN n + 2;")
        .await?;
    let incoming = db.storage.head()?.unwrap();

    db.storage.checkout_branch("main")?;
    let result = db.storage.merge(&base, &incoming).await?;
    assert!(result
        .conflicts()
        .iter()
        .any(|conflict| matches!(conflict, MergeConflict::Schema { .. })));
    assert!(result
        .conflicts()
        .iter()
        .any(|conflict| matches!(conflict, MergeConflict::Function { .. })));
    assert_eq!(db.storage.head()?.unwrap().id(), current.id());
    println!("schema and function conflicts: {:#?}", result.conflicts());
    Ok(())
}

async fn unique_constraint_conflict() -> Result<(), Box<dyn Error>> {
    let mut db = Glue::new(ProllyStorage::in_memory()?);
    db.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL UNIQUE
        );",
    )
    .await?;
    let base = db.storage.head()?.unwrap();
    db.storage.create_branch("feature")?;

    db.execute("INSERT INTO users VALUES (1, 'same@example.com');")
        .await?;
    let current = db.storage.head()?.unwrap();
    db.storage.checkout_branch("feature")?;
    db.execute("INSERT INTO users VALUES (2, 'same@example.com');")
        .await?;
    let incoming = db.storage.head()?.unwrap();

    db.storage.checkout_branch("main")?;
    let result = db.storage.merge(&base, &incoming).await?;
    assert!(matches!(
        result,
        MergeResult::Conflicted { ref conflicts }
            if matches!(conflicts.as_slice(), [MergeConflict::Constraint { reason, .. }] if reason.contains("unique column"))
    ));
    assert_eq!(db.storage.head()?.unwrap().id(), current.id());
    println!("unique constraint conflict: {:#?}", result.conflicts()[0]);
    Ok(())
}

async fn foreign_key_conflict() -> Result<(), Box<dyn Error>> {
    let mut db = Glue::new(ProllyStorage::in_memory()?);
    db.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY);")
        .await?;
    db.execute(
        "CREATE TABLE children (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            FOREIGN KEY (parent_id) REFERENCES parents (id)
        );",
    )
    .await?;
    db.execute("INSERT INTO parents VALUES (1);").await?;
    let base = db.storage.head()?.unwrap();
    db.storage.create_branch("feature")?;

    db.execute("DELETE FROM parents WHERE id = 1;").await?;
    let current = db.storage.head()?.unwrap();
    db.storage.checkout_branch("feature")?;
    db.execute("INSERT INTO children VALUES (1, 1);").await?;
    let incoming = db.storage.head()?.unwrap();

    db.storage.checkout_branch("main")?;
    let result = db.storage.merge(&base, &incoming).await?;
    assert!(matches!(
        result.conflicts(),
        [MergeConflict::Constraint { reason, .. }] if reason.contains("foreign key")
    ));
    assert_eq!(db.storage.head()?.unwrap().id(), current.id());
    println!("foreign-key conflict: {:#?}", result.conflicts()[0]);
    Ok(())
}
